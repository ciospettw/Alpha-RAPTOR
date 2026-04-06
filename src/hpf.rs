use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use chrono::Utc;
use flate2::read::{GzDecoder, ZlibDecoder};
use memmap2::Mmap;
use osmpbfreader::{
    NodeId, OsmObj, OsmPbfReader, Way,
    fileformat::{Blob, BlobHeader},
    osmformat::HeaderBlock,
};
use protobuf::Message;
use quick_xml::{Reader, events::{BytesStart, Event}};
use reqwest::blocking::Client as BlockingClient;
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    engine::{PolylinePoint, StopRecord},
    geo::{
        decode_morton_code, decode_morton_components, morton_code, morton_code_from_components,
    },
};

const OSM_HPF_STRATEGY: &str = "osm-pbf-cached-hpf";
const HPF_OVERLAY_SHIFT_BITS: u8 = 20;
const HPF_OVERLAY_AXIS_STEP: u32 = 1u32 << (HPF_OVERLAY_SHIFT_BITS / 2);
const HPF_OVERLAY_AXIS_CENTER: u32 = HPF_OVERLAY_AXIS_STEP / 2;
const HPF_OVERLAY_EPSILON_METERS: f64 = 0.5;
const HPF_LOCAL_PROPAGATION_LIMIT: usize = 2_048;
const HPF_RASTER_STEP_METERS: f64 = 12.0;
const HPF_TOPOLOGY_INF_METERS: f32 = 1_000_000_000.0;
const HPF_WAY_INDEX_MAGIC: &[u8; 8] = b"HPFWIDX1";
const HPF_OVERLAY_MAGIC: &[u8; 8] = b"HPFOVLY1";

pub struct HpfBuildResult {
    pub forest: HolographicPedestrianForest,
    pub strategy: &'static str,
    pub cache_hit: bool,
    pub covered_nodes: usize,
    pub anchored_stops: usize,
}

#[derive(Clone, Debug)]
pub struct HpfDiffConfig {
    pub state_url: String,
    pub diff_base_url: Option<String>,
    pub poll_interval_secs: u64,
    pub allow_invalid_tls: bool,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct HpfOverlaySnapshot {
    pub enabled: bool,
    pub state_url: Option<String>,
    pub diff_base_url: Option<String>,
    pub poll_interval_secs: Option<u64>,
    pub base_sequence: Option<u64>,
    pub base_timestamp: Option<String>,
    pub applied_sequence: Option<u64>,
    pub applied_timestamp: Option<String>,
    pub last_poll_timestamp: Option<String>,
    pub overlay_cells: usize,
    pub blocked_cells: usize,
    pub synthetic_cells: usize,
    pub way_overrides: usize,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct HolographicPedestrianForest {
    nodes: Arc<Vec<HpfNode>>,
    walk_speed_mps: f64,
    snap_tolerance_meters: f64,
    snap_quadratic_kappa_meters: f64,
    search_window: usize,
    overlay_runtime: Arc<ArcSwap<HpfOverlayRuntime>>,
    overlay_path: Arc<PathBuf>,
    way_index: Option<Arc<HpfWayIndex>>,
    base_metadata: HpfCacheMetadata,
    diff_config: Option<HpfDiffConfig>,
    pbf_replication: Option<HpfPbfReplicationAnchor>,
}

#[derive(Clone, Debug)]
pub struct HpfConnector {
    pub stop_index: usize,
    pub duration_secs: i32,
    pub distance_meters: f64,
    pub polyline: Vec<PolylinePoint>,
    pub used_asymptotic_penalty: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct HpfNode {
    morton: u64,
    parent_index: u32,
    root_stop_index: u32,
    cost_meters: f32,
}

#[derive(Serialize, Deserialize)]
struct HpfCache {
    metadata: HpfCacheMetadata,
    anchored_stops: usize,
    nodes: Vec<HpfNode>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct HpfCacheMetadata {
    osm_pbf_bytes: u64,
    osm_pbf_modified_unix_secs: Option<u64>,
    stop_fingerprint: u64,
    max_distance_bits: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct HpfOverlayCell {
    cell: u64,
    parent_cell: Option<u64>,
    root_stop_index: u32,
    cost_meters: f32,
    blocked: bool,
    synthetic: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct HpfWayOverride {
    way_id: i64,
    walkable: bool,
    cells: Vec<u64>,
}

#[derive(Serialize, Deserialize)]
struct HpfOverlayPersisted {
    magic: [u8; 8],
    metadata: HpfCacheMetadata,
    state_url: Option<String>,
    diff_base_url: Option<String>,
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    entries: Vec<HpfOverlayCell>,
    way_overrides: Vec<HpfWayOverride>,
}

#[derive(Clone)]
struct HpfOverlayRuntime {
    enabled: bool,
    state_url: Option<String>,
    diff_base_url: Option<String>,
    poll_interval_secs: Option<u64>,
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    last_poll_timestamp: Option<String>,
    last_error: Option<String>,
    entries: Arc<Vec<HpfOverlayCell>>,
    entry_lookup: Arc<HashMap<u64, usize>>,
    synthetic_cells: Arc<Vec<u64>>,
    way_overrides: Arc<HashMap<i64, HpfWayOverride>>,
}

#[derive(Clone)]
struct HpfPbfReplicationAnchor {
    sequence_number: Option<u64>,
    timestamp: Option<String>,
    base_url: Option<String>,
}

struct HpfWayIndex {
    mmap: Arc<Mmap>,
    record_count: usize,
    cells_offset: usize,
}

#[derive(Clone)]
enum CandidateLocation {
    BaseNode(usize),
    OverlayCell(u64),
}

#[derive(Clone)]
struct EffectiveCellState {
    cost_meters: f64,
    root_stop_index: u32,
    parent_cell: Option<u64>,
    blocked: bool,
    synthetic: bool,
}

struct OscDiff {
    ways: Vec<OscWayChange>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OscAction {
    Create,
    Modify,
    Delete,
}

struct OscWayChange {
    action: OscAction,
    way_id: i64,
    node_refs: Vec<i64>,
    coordinates: Vec<(f64, f64)>,
    tags: HashMap<String, String>,
}

#[derive(Clone, Copy)]
struct StopAnchor {
    node_index: usize,
    snap_distance_meters: f64,
}

#[derive(Clone, Copy)]
struct PedestrianEdge {
    to: usize,
    distance_meters: f64,
}

#[derive(Clone)]
struct IndexedPoint {
    index: usize,
    point: [f64; 2],
}

#[derive(Clone, Copy)]
struct HeapState {
    node_index: usize,
    distance_meters: f64,
}

#[derive(Clone)]
struct CandidateConnector {
    stop_index: usize,
    distance_meters: f64,
    used_asymptotic_penalty: bool,
    location: CandidateLocation,
}

impl RTreeObject for IndexedPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.point)
    }
}

impl PointDistance for IndexedPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.point[0] - point[0];
        let dy = self.point[1] - point[1];
        (dx * dx) + (dy * dy)
    }
}

impl PartialEq for HeapState {
    fn eq(&self, other: &Self) -> bool {
        self.node_index == other.node_index
            && self.distance_meters.to_bits() == other.distance_meters.to_bits()
    }
}

impl Eq for HeapState {}

impl PartialOrd for HeapState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance_meters
            .total_cmp(&self.distance_meters)
            .then_with(|| self.node_index.cmp(&other.node_index))
    }
}

impl HolographicPedestrianForest {
    fn from_cache(
        cache: HpfCache,
        walk_speed_mps: f64,
        snap_tolerance_meters: f64,
        snap_quadratic_kappa_meters: f64,
        search_window: usize,
        cache_dir: &Path,
        osm_pbf_path: &Path,
        base_metadata: HpfCacheMetadata,
        diff_config: Option<HpfDiffConfig>,
        pbf_replication: Option<HpfPbfReplicationAnchor>,
        way_index: Option<Arc<HpfWayIndex>>,
    ) -> Self {
        let overlay_path = Arc::new(hpf_overlay_path(cache_dir, osm_pbf_path));
        let overlay_runtime = Arc::new(ArcSwap::from_pointee(load_overlay_runtime(
            &overlay_path,
            &base_metadata,
            diff_config.as_ref(),
        )));
        Self {
            nodes: Arc::new(cache.nodes),
            walk_speed_mps,
            snap_tolerance_meters,
            snap_quadratic_kappa_meters,
            search_window: search_window.max(256),
            overlay_runtime,
            overlay_path,
            way_index,
            base_metadata,
            diff_config,
            pbf_replication,
        }
    }

    pub fn overlay_snapshot(&self) -> HpfOverlaySnapshot {
        let runtime = self.overlay_runtime.load();
        HpfOverlaySnapshot {
            enabled: runtime.enabled,
            state_url: runtime.state_url.clone(),
            diff_base_url: runtime.diff_base_url.clone(),
            poll_interval_secs: runtime.poll_interval_secs,
            base_sequence: self
                .pbf_replication
                .as_ref()
                .and_then(|anchor| anchor.sequence_number),
            base_timestamp: self
                .pbf_replication
                .as_ref()
                .and_then(|anchor| anchor.timestamp.clone()),
            applied_sequence: runtime.applied_sequence,
            applied_timestamp: runtime.applied_timestamp.clone(),
            last_poll_timestamp: runtime.last_poll_timestamp.clone(),
            overlay_cells: runtime.entries.len(),
            blocked_cells: runtime.entries.iter().filter(|entry| entry.blocked).count(),
            synthetic_cells: runtime.entries.iter().filter(|entry| entry.synthetic).count(),
            way_overrides: runtime.way_overrides.len(),
            last_error: runtime.last_error.clone(),
        }
    }

    pub fn poll_remote_updates(&self) -> Result<HpfOverlaySnapshot> {
        let Some(diff_config) = self.diff_config.as_ref() else {
            return Ok(self.overlay_snapshot());
        };
        let Some(way_index) = self.way_index.as_ref() else {
            let mut runtime = (*self.overlay_runtime.load_full()).clone();
            runtime.enabled = false;
            runtime.last_poll_timestamp = Some(Utc::now().to_rfc3339());
            runtime.last_error = Some("HPF way-raster index unavailable; diff polling is disabled".to_owned());
            self.overlay_runtime.store(Arc::new(runtime));
            return Ok(self.overlay_snapshot());
        };

        let client = BlockingClient::builder()
            .user_agent("alpha-raptor-engine/0.1")
            .danger_accept_invalid_certs(diff_config.allow_invalid_tls)
            .build()
            .context("failed to build OSM diff HTTP client")?;

        let runtime = (*self.overlay_runtime.load_full()).clone();
        let poll_timestamp = Utc::now().to_rfc3339();
        let configured_base_url = resolved_diff_base_url(diff_config);
        if let Some(anchor_base_url) = self
            .pbf_replication
            .as_ref()
            .and_then(|anchor| anchor.base_url.as_deref())
        {
            if normalize_diff_base_url(anchor_base_url) != configured_base_url {
                let mut next_runtime = runtime;
                next_runtime.enabled = false;
                next_runtime.last_poll_timestamp = Some(poll_timestamp);
                next_runtime.last_error = Some(format!(
                    "OSM diff base URL mismatch: PBF header expects {anchor_base_url}, config points to {}",
                    diff_config.state_url
                ));
                self.overlay_runtime.store(Arc::new(next_runtime));
                return Ok(self.overlay_snapshot());
            }
        }
        let state_body = client
            .get(&diff_config.state_url)
            .send()
            .and_then(|response| response.error_for_status())
            .with_context(|| format!("failed to fetch OSM diff state from {}", diff_config.state_url))?
            .text()
            .context("failed to read OSM diff state response body")?;
        let remote_state = parse_remote_state_file(&state_body)?;

        let mut applied_sequence = runtime.applied_sequence.or_else(|| {
            self.pbf_replication
                .as_ref()
                .and_then(|anchor| anchor.sequence_number)
        });

        if runtime.applied_sequence.is_none() && applied_sequence.is_none() {
            applied_sequence = Some(remote_state.sequence_number);
        }

        let Some(applied_sequence) = applied_sequence else {
            return Ok(self.overlay_snapshot());
        };

        if remote_state.sequence_number <= applied_sequence {
            let mut next_runtime = runtime;
            next_runtime.enabled = true;
            next_runtime.last_poll_timestamp = Some(poll_timestamp);
            next_runtime.last_error = None;
            if next_runtime.applied_sequence.is_none() {
                next_runtime.applied_sequence = Some(applied_sequence);
                next_runtime.applied_timestamp = remote_state.timestamp.clone();
                persist_overlay_runtime(
                    &self.overlay_path,
                    &self.base_metadata,
                    &next_runtime,
                )?;
            }
            self.overlay_runtime.store(Arc::new(next_runtime));
            return Ok(self.overlay_snapshot());
        }

        let mut draft = overlay_draft_from_runtime(&runtime);
        for sequence in (applied_sequence + 1)..=remote_state.sequence_number {
            let diff_url = sequence_diff_url(
                runtime.diff_base_url.as_deref().unwrap_or(&configured_base_url),
                sequence,
            );
            let diff_bytes = client
                .get(&diff_url)
                .send()
                .and_then(|response| response.error_for_status())
                .with_context(|| format!("failed to fetch OSM diff {sequence} from {diff_url}"))?
                .bytes()
                .with_context(|| format!("failed to read OSM diff body from {diff_url}"))?;
            let diff = parse_osc_diff(diff_bytes.as_ref())?;
            self.apply_osc_diff(&mut draft, &diff, way_index)?;
            draft.applied_sequence = Some(sequence);
        }
        draft.applied_timestamp = remote_state.timestamp;

        let mut next_runtime = overlay_runtime_from_draft(
            &draft,
            diff_config,
            Some(poll_timestamp),
            None,
        );
        next_runtime.enabled = true;
        persist_overlay_runtime(&self.overlay_path, &self.base_metadata, &next_runtime)?;
        self.overlay_runtime.store(Arc::new(next_runtime));
        Ok(self.overlay_snapshot())
    }

    pub fn query_connectors(
        &self,
        latitude: f64,
        longitude: f64,
        limit: usize,
        stops: &[StopRecord],
    ) -> Vec<HpfConnector> {
        if limit == 0 || self.nodes.is_empty() {
            return Vec::new();
        }

        let query_morton = morton_code(latitude, longitude);
        let query_cell = overlay_cell_from_morton(query_morton);
        let center = self
            .nodes
            .binary_search_by_key(&query_morton, |node| node.morton)
            .unwrap_or_else(|position| position);
        let overlay = self.overlay_runtime.load_full();
        let synthetic_center = overlay
            .synthetic_cells
            .binary_search(&query_cell)
            .unwrap_or_else(|position| position);

        let mut best_by_stop = HashMap::<usize, CandidateConnector>::new();
        let mut scanned_left = center;
        let mut scanned_right = center;
        let mut scanned_overlay_left = synthetic_center;
        let mut scanned_overlay_right = synthetic_center;
        let mut window = self.search_window.min(self.nodes.len()).max(limit * 64);

        loop {
            let left = center.saturating_sub(window);
            let right = (center + window).min(self.nodes.len());
            let overlay_window = (window / 16).max(limit * 8);
            let overlay_left_cell = query_cell.saturating_sub(overlay_window as u64);
            let overlay_right_cell = query_cell.saturating_add(overlay_window as u64);
            let overlay_left = overlay
                .synthetic_cells
                .binary_search(&overlay_left_cell)
                .unwrap_or_else(|position| position);
            let overlay_right = overlay
                .synthetic_cells
                .binary_search(&overlay_right_cell)
                .map(|position| position + 1)
                .unwrap_or_else(|position| position)
                .min(overlay.synthetic_cells.len());

            for index in left..scanned_left {
                self.consider_base_candidate(
                    &overlay,
                    index,
                    latitude,
                    longitude,
                    stops,
                    &mut best_by_stop,
                );
            }
            for index in scanned_right..right {
                self.consider_base_candidate(
                    &overlay,
                    index,
                    latitude,
                    longitude,
                    stops,
                    &mut best_by_stop,
                );
            }
            for position in overlay_left..scanned_overlay_left {
                if let Some(&cell) = overlay.synthetic_cells.get(position) {
                    self.consider_overlay_candidate(
                        &overlay,
                        cell,
                        latitude,
                        longitude,
                        stops,
                        &mut best_by_stop,
                    );
                }
            }
            for position in scanned_overlay_right..overlay_right {
                if let Some(&cell) = overlay.synthetic_cells.get(position) {
                    self.consider_overlay_candidate(
                        &overlay,
                        cell,
                        latitude,
                        longitude,
                        stops,
                        &mut best_by_stop,
                    );
                }
            }

            scanned_left = left;
            scanned_right = right;
            scanned_overlay_left = overlay_left;
            scanned_overlay_right = overlay_right;

            if best_by_stop.len() >= limit
                || ((left == 0 && right == self.nodes.len())
                    && (overlay_left == 0 && overlay_right == overlay.synthetic_cells.len()))
            {
                break;
            }
            window = (window * 2).min(self.nodes.len());
        }

        let mut connectors = best_by_stop
            .into_values()
            .filter_map(|candidate| {
                let stop = stops.get(candidate.stop_index)?;
                let (stop_lat, stop_lon) = (stop.latitude?, stop.longitude?);
                Some(HpfConnector {
                    stop_index: candidate.stop_index,
                    duration_secs: (candidate.distance_meters / self.walk_speed_mps).ceil() as i32,
                    distance_meters: candidate.distance_meters,
                    polyline: self.reconstruct_candidate_polyline(
                        &candidate,
                        latitude,
                        longitude,
                        stop_lat,
                        stop_lon,
                    ),
                    used_asymptotic_penalty: candidate.used_asymptotic_penalty,
                })
            })
            .collect::<Vec<_>>();

        connectors.sort_by(|left, right| {
            left.duration_secs
                .cmp(&right.duration_secs)
                .then_with(|| left.distance_meters.total_cmp(&right.distance_meters))
                .then_with(|| left.stop_index.cmp(&right.stop_index))
        });
        connectors.truncate(limit);
        connectors
    }

    fn consider_base_candidate(
        &self,
        overlay: &HpfOverlayRuntime,
        index: usize,
        latitude: f64,
        longitude: f64,
        stops: &[StopRecord],
        best_by_stop: &mut HashMap<usize, CandidateConnector>,
    ) {
        let node = &self.nodes[index];
        let cell = overlay_cell_from_morton(node.morton);
        let Some(state) = self.effective_cell_state(overlay, cell) else {
            return;
        };
        if state.blocked || !state.cost_meters.is_finite() || state.root_stop_index == u32::MAX {
            return;
        }
        let stop_index = state.root_stop_index as usize;
        let Some(stop) = stops.get(stop_index) else {
            return;
        };
        let (_stop_lat, _stop_lon) = match (stop.latitude, stop.longitude) {
            (Some(lat), Some(lon)) => (lat, lon),
            _ => return,
        };

        let (node_lat, node_lon) = decode_morton_code(node.morton);
        let snap_distance = haversine_meters(latitude, longitude, node_lat, node_lon);
        let used_asymptotic_penalty = snap_distance > self.snap_tolerance_meters;
        let snap_cost = snap_distance.mul_add(
            snap_distance / self.snap_quadratic_kappa_meters,
            snap_distance,
        );
        let total_distance = state.cost_meters + snap_cost;

        if !should_replace_candidate(best_by_stop.get(&stop_index), total_distance) {
            return;
        }

        best_by_stop.insert(
            stop_index,
            CandidateConnector {
                stop_index,
                distance_meters: total_distance,
                used_asymptotic_penalty,
                location: if overlay.entry_lookup.contains_key(&cell) {
                    CandidateLocation::OverlayCell(cell)
                } else {
                    CandidateLocation::BaseNode(index)
                },
            },
        );
    }

    fn consider_overlay_candidate(
        &self,
        overlay: &HpfOverlayRuntime,
        cell: u64,
        latitude: f64,
        longitude: f64,
        stops: &[StopRecord],
        best_by_stop: &mut HashMap<usize, CandidateConnector>,
    ) {
        let Some(state) = self.effective_cell_state(overlay, cell) else {
            return;
        };
        if state.blocked || !state.synthetic || !state.cost_meters.is_finite() || state.root_stop_index == u32::MAX {
            return;
        }

        let stop_index = state.root_stop_index as usize;
        let Some(stop) = stops.get(stop_index) else {
            return;
        };
        if stop.latitude.is_none() || stop.longitude.is_none() {
            return;
        }

        let (cell_lat, cell_lon) = overlay_cell_center(cell);
        let snap_distance = haversine_meters(latitude, longitude, cell_lat, cell_lon);
        let used_asymptotic_penalty = snap_distance > self.snap_tolerance_meters;
        let snap_cost = snap_distance.mul_add(
            snap_distance / self.snap_quadratic_kappa_meters,
            snap_distance,
        );
        let total_distance = state.cost_meters + snap_cost;
        if !should_replace_candidate(best_by_stop.get(&stop_index), total_distance) {
            return;
        }

        best_by_stop.insert(
            stop_index,
            CandidateConnector {
                stop_index,
                distance_meters: total_distance,
                used_asymptotic_penalty,
                location: CandidateLocation::OverlayCell(cell),
            },
        );
    }

    fn reconstruct_candidate_polyline(
        &self,
        candidate: &CandidateConnector,
        query_lat: f64,
        query_lon: f64,
        stop_lat: f64,
        stop_lon: f64,
    ) -> Vec<PolylinePoint> {
        let mut polyline = vec![PolylinePoint {
            lat: query_lat,
            lon: query_lon,
        }];

        match candidate.location {
            CandidateLocation::BaseNode(index) => self.reconstruct_base_tail(index, &mut polyline),
            CandidateLocation::OverlayCell(cell) => self.reconstruct_overlay_tail(cell, &mut polyline),
        }

        push_polyline_point(&mut polyline, stop_lat, stop_lon);
        polyline
    }

    fn reconstruct_base_tail(&self, start_index: usize, polyline: &mut Vec<PolylinePoint>) {
        let overlay = self.overlay_runtime.load_full();
        let start_cell = overlay_cell_from_morton(self.nodes[start_index].morton);
        if overlay.entry_lookup.contains_key(&start_cell) {
            self.reconstruct_overlay_tail_from_runtime(&overlay, start_cell, polyline);
            return;
        }

        self.reconstruct_base_tail_raw(start_index, polyline);
    }

    fn reconstruct_base_tail_raw(&self, start_index: usize, polyline: &mut Vec<PolylinePoint>) {

        let mut cursor = start_index;
        loop {
            let node = &self.nodes[cursor];
            let (lat, lon) = decode_morton_code(node.morton);
            push_polyline_point(polyline, lat, lon);
            if node.parent_index == u32::MAX {
                break;
            }
            cursor = node.parent_index as usize;
        }
    }

    fn reconstruct_overlay_tail(&self, start_cell: u64, polyline: &mut Vec<PolylinePoint>) {
        let overlay = self.overlay_runtime.load_full();
        self.reconstruct_overlay_tail_from_runtime(&overlay, start_cell, polyline);
    }

    fn reconstruct_overlay_tail_from_runtime(
        &self,
        overlay: &HpfOverlayRuntime,
        start_cell: u64,
        polyline: &mut Vec<PolylinePoint>,
    ) {
        let mut cursor = Some(start_cell);
        let mut visited = HashSet::<u64>::new();

        while let Some(cell) = cursor {
            if !visited.insert(cell) || visited.len() > HPF_LOCAL_PROPAGATION_LIMIT {
                break;
            }

            let (lat, lon) = overlay_cell_center(cell);
            push_polyline_point(polyline, lat, lon);

            let next = self.overlay_entry(overlay, cell).and_then(|entry| entry.parent_cell);
            let Some(next_cell) = next else {
                break;
            };

            if !overlay.entry_lookup.contains_key(&next_cell) {
                if let Some(base_index) = self.best_base_node_index_for_cell(next_cell) {
                    self.reconstruct_base_tail_raw(base_index, polyline);
                    break;
                }
            }
            cursor = Some(next_cell);
        }
    }

    fn overlay_entry(&self, overlay: &HpfOverlayRuntime, cell: u64) -> Option<HpfOverlayCell> {
        overlay
            .entry_lookup
            .get(&cell)
            .and_then(|index| overlay.entries.get(*index))
            .cloned()
    }

    fn effective_cell_state(&self, overlay: &HpfOverlayRuntime, cell: u64) -> Option<EffectiveCellState> {
        if let Some(entry) = self.overlay_entry(overlay, cell) {
            return Some(EffectiveCellState {
                cost_meters: f64::from(entry.cost_meters),
                root_stop_index: entry.root_stop_index,
                parent_cell: entry.parent_cell,
                blocked: entry.blocked,
                synthetic: entry.synthetic,
            });
        }

        self.best_base_node_for_cell(cell)
            .map(|(_, state)| EffectiveCellState {
                cost_meters: state.cost_meters,
                root_stop_index: state.root_stop_index,
                parent_cell: state.parent_cell,
                blocked: false,
                synthetic: false,
            })
    }

    fn best_base_node_index_for_cell(&self, cell: u64) -> Option<usize> {
        self.best_base_node_for_cell(cell).map(|(index, _)| index)
    }

    fn best_base_node_for_cell(&self, cell: u64) -> Option<(usize, EffectiveCellState)> {
        let start = cell << HPF_OVERLAY_SHIFT_BITS;
        let end = if cell == (u64::MAX >> HPF_OVERLAY_SHIFT_BITS) {
            u64::MAX
        } else {
            ((cell + 1) << HPF_OVERLAY_SHIFT_BITS).saturating_sub(1)
        };
        let mut position = self
            .nodes
            .binary_search_by_key(&start, |node| node.morton)
            .unwrap_or_else(|index| index);
        let mut best = None::<(usize, EffectiveCellState)>;

        while let Some(node) = self.nodes.get(position) {
            if node.morton > end {
                break;
            }
            let candidate = EffectiveCellState {
                cost_meters: f64::from(node.cost_meters),
                root_stop_index: node.root_stop_index,
                parent_cell: if node.parent_index == u32::MAX {
                    None
                } else {
                    Some(overlay_cell_from_morton(
                        self.nodes[node.parent_index as usize].morton,
                    ))
                },
                blocked: false,
                synthetic: false,
            };
            let should_replace = match &best {
                Some((_, current)) => {
                    candidate.cost_meters < current.cost_meters
                        || (candidate.cost_meters - current.cost_meters).abs() < HPF_OVERLAY_EPSILON_METERS
                            && candidate.root_stop_index < current.root_stop_index
                }
                None => true,
            };
            if should_replace {
                best = Some((position, candidate));
            }
            position += 1;
        }

        best
    }

    fn apply_osc_diff(
        &self,
        draft: &mut HpfOverlayDraft,
        diff: &OscDiff,
        way_index: &HpfWayIndex,
    ) -> Result<()> {
        for change in &diff.ways {
            let current_cells = current_way_cells(draft, way_index, change.way_id);
            if !current_cells.is_empty() {
                self.block_cells(draft, &current_cells);
                self.shadow_propagate(draft, &current_cells);
            }

            let walkable = change.action != OscAction::Delete && is_walkable_tags(&change.tags);
            let next_cells = if walkable {
                let rasterized = rasterize_change_cells(change, &current_cells)?;
                if rasterized.is_empty() && change.action == OscAction::Modify {
                    current_cells.clone()
                } else {
                    rasterized
                }
            } else {
                Vec::new()
            };

            update_way_override(draft, way_index, change.way_id, walkable, next_cells.clone());
            if walkable && !next_cells.is_empty() {
                self.cool_new_way(draft, &next_cells);
            }
        }

        Ok(())
    }

    fn block_cells(&self, draft: &mut HpfOverlayDraft, cells: &[u64]) {
        for &cell in cells {
            let root_stop_index = draft
                .entries
                .get(&cell)
                .map(|entry| entry.root_stop_index)
                .or_else(|| self.best_base_node_for_cell(cell).map(|(_, state)| state.root_stop_index))
                .unwrap_or(u32::MAX);
            draft.entries.insert(
                cell,
                HpfOverlayCell {
                    cell,
                    parent_cell: None,
                    root_stop_index,
                    cost_meters: HPF_TOPOLOGY_INF_METERS,
                    blocked: true,
                    synthetic: draft
                        .entries
                        .get(&cell)
                        .map(|entry| entry.synthetic)
                        .unwrap_or(false),
                },
            );
        }
    }

    fn shadow_propagate(&self, draft: &mut HpfOverlayDraft, blocked_cells: &[u64]) {
        let mut queue = VecDeque::<u64>::new();
        let mut enqueued = HashSet::<u64>::new();
        for &cell in blocked_cells {
            for neighbor in overlay_moore_neighbors(cell) {
                if enqueued.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
        self.propagate_overlay_queue(draft, queue, &mut enqueued, true);
    }

    fn cool_new_way(&self, draft: &mut HpfOverlayDraft, cells: &[u64]) {
        let Some(anchor) = self.select_way_anchor(draft, cells) else {
            return;
        };

        let mut ordered = cells.to_vec();
        if anchor.reverse {
            ordered.reverse();
        }

        let mut next_cell = anchor.anchor_cell;
        let mut next_cost = anchor.anchor_cost;
        let root_stop_index = anchor.root_stop_index;

        for &cell in ordered.iter().rev() {
            if cell == next_cell {
                continue;
            }
            let cost = next_cost + overlay_transition_cost(cell, next_cell);
            let current = self.effective_cell_state_from_draft(draft, cell);
            if current
                .as_ref()
                .is_none_or(|state| state.blocked || cost + HPF_OVERLAY_EPSILON_METERS < state.cost_meters)
            {
                draft.entries.insert(
                    cell,
                    HpfOverlayCell {
                        cell,
                        parent_cell: Some(next_cell),
                        root_stop_index,
                        cost_meters: cost as f32,
                        blocked: false,
                        synthetic: self.best_base_node_for_cell(cell).is_none(),
                    },
                );
            }
            next_cell = cell;
            next_cost = cost;
        }

        let mut queue = VecDeque::<u64>::new();
        let mut enqueued = HashSet::<u64>::new();
        for &cell in cells {
            if enqueued.insert(cell) {
                queue.push_back(cell);
            }
            for neighbor in overlay_moore_neighbors(cell) {
                if enqueued.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
        self.propagate_overlay_queue(draft, queue, &mut enqueued, false);
    }

    fn propagate_overlay_queue(
        &self,
        draft: &mut HpfOverlayDraft,
        mut queue: VecDeque<u64>,
        enqueued: &mut HashSet<u64>,
        allow_repair: bool,
    ) {
        let mut steps = 0usize;
        while let Some(cell) = queue.pop_front() {
            enqueued.remove(&cell);
            if steps >= HPF_LOCAL_PROPAGATION_LIMIT {
                break;
            }
            steps += 1;

            let current = self.effective_cell_state_from_draft(draft, cell);
            if current.as_ref().is_some_and(|state| state.blocked) {
                continue;
            }

            let needs_repair = current
                .as_ref()
                .and_then(|state| state.parent_cell)
                .is_some_and(|parent_cell| self.is_cell_unusable(draft, parent_cell));

            let Some(next_state) = self.best_neighbor_relaxation(draft, cell) else {
                continue;
            };

            let should_update = match current {
                Some(ref state) => {
                    (allow_repair && needs_repair)
                        || next_state.cost_meters + HPF_OVERLAY_EPSILON_METERS < state.cost_meters
                }
                None => true,
            };
            if !should_update {
                continue;
            }

            let synthetic = draft
                .entries
                .get(&cell)
                .map(|entry| entry.synthetic)
                .unwrap_or_else(|| self.best_base_node_for_cell(cell).is_none());
            draft.entries.insert(
                cell,
                HpfOverlayCell {
                    cell,
                    parent_cell: next_state.parent_cell,
                    root_stop_index: next_state.root_stop_index,
                    cost_meters: next_state.cost_meters as f32,
                    blocked: false,
                    synthetic,
                },
            );
            prune_overlay_entry(draft, self, cell);
            for neighbor in overlay_moore_neighbors(cell) {
                if enqueued.insert(neighbor) {
                    queue.push_back(neighbor);
                }
            }
        }
    }

    fn best_neighbor_relaxation(&self, draft: &HpfOverlayDraft, cell: u64) -> Option<EffectiveCellState> {
        let mut best = None::<(u64, EffectiveCellState)>;
        for neighbor in overlay_moore_neighbors(cell) {
            if neighbor == cell {
                continue;
            }
            let Some(state) = self.effective_cell_state_from_draft(draft, neighbor) else {
                continue;
            };
            if state.blocked || !state.cost_meters.is_finite() || state.root_stop_index == u32::MAX {
                continue;
            }

            let candidate_cost = state.cost_meters + overlay_transition_cost(cell, neighbor);
            let should_replace = match &best {
                Some((_, current)) => {
                    candidate_cost + HPF_OVERLAY_EPSILON_METERS < current.cost_meters
                }
                None => true,
            };
            if should_replace {
                best = Some((
                    neighbor,
                    EffectiveCellState {
                        cost_meters: candidate_cost,
                        root_stop_index: state.root_stop_index,
                        parent_cell: Some(neighbor),
                        blocked: false,
                        synthetic: true,
                    },
                ));
            }
        }

        best.map(|(_, state)| state)
    }

    fn effective_cell_state_from_draft(
        &self,
        draft: &HpfOverlayDraft,
        cell: u64,
    ) -> Option<EffectiveCellState> {
        if let Some(entry) = draft.entries.get(&cell) {
            return Some(EffectiveCellState {
                cost_meters: f64::from(entry.cost_meters),
                root_stop_index: entry.root_stop_index,
                parent_cell: entry.parent_cell,
                blocked: entry.blocked,
                synthetic: entry.synthetic,
            });
        }
        self.best_base_node_for_cell(cell).map(|(_, state)| state)
    }

    fn is_cell_unusable(&self, draft: &HpfOverlayDraft, cell: u64) -> bool {
        self.effective_cell_state_from_draft(draft, cell)
            .is_none_or(|state| state.blocked || !state.cost_meters.is_finite())
    }

    fn select_way_anchor(&self, draft: &HpfOverlayDraft, cells: &[u64]) -> Option<WayAnchor> {
        let first = *cells.first()?;
        let last = *cells.last()?;
        let first_anchor = self.resolve_anchor_state(draft, first);
        let last_anchor = self.resolve_anchor_state(draft, last);

        match (first_anchor, last_anchor) {
            (Some(first_anchor), Some(last_anchor)) => {
                if first_anchor.anchor_cost <= last_anchor.anchor_cost {
                    Some(WayAnchor {
                        anchor_cell: first_anchor.anchor_cell,
                        anchor_cost: first_anchor.anchor_cost,
                        root_stop_index: first_anchor.root_stop_index,
                        reverse: true,
                    })
                } else {
                    Some(WayAnchor {
                        anchor_cell: last_anchor.anchor_cell,
                        anchor_cost: last_anchor.anchor_cost,
                        root_stop_index: last_anchor.root_stop_index,
                        reverse: false,
                    })
                }
            }
            (Some(anchor), None) => Some(WayAnchor {
                anchor_cell: anchor.anchor_cell,
                anchor_cost: anchor.anchor_cost,
                root_stop_index: anchor.root_stop_index,
                reverse: true,
            }),
            (None, Some(anchor)) => Some(WayAnchor {
                anchor_cell: anchor.anchor_cell,
                anchor_cost: anchor.anchor_cost,
                root_stop_index: anchor.root_stop_index,
                reverse: false,
            }),
            (None, None) => None,
        }
    }

    fn resolve_anchor_state(&self, draft: &HpfOverlayDraft, cell: u64) -> Option<ResolvedAnchor> {
        let mut best = None::<ResolvedAnchor>;
        for neighbor in overlay_moore_neighbors(cell) {
            let Some(state) = self.effective_cell_state_from_draft(draft, neighbor) else {
                continue;
            };
            if state.blocked || !state.cost_meters.is_finite() || state.root_stop_index == u32::MAX {
                continue;
            }
            let cost = state.cost_meters + overlay_transition_cost(cell, neighbor);
            let candidate = ResolvedAnchor {
                anchor_cell: neighbor,
                anchor_cost: cost,
                root_stop_index: state.root_stop_index,
            };
            let should_replace = match &best {
                Some(current) => cost + HPF_OVERLAY_EPSILON_METERS < current.anchor_cost,
                None => true,
            };
            if should_replace {
                best = Some(candidate);
            }
        }
        best
    }
}

pub fn build_or_load_hpf(
    osm_pbf_path: &Path,
    cache_dir: &Path,
    stops: &[StopRecord],
    max_distance_meters: f64,
    walk_speed_mps: f64,
    snap_tolerance_meters: f64,
    snap_quadratic_kappa_meters: f64,
    search_window: usize,
    diff_config: Option<HpfDiffConfig>,
) -> Result<HpfBuildResult> {
    let metadata = build_cache_metadata(osm_pbf_path, stops, max_distance_meters)?;
    let cache_path = hpf_cache_path(cache_dir, osm_pbf_path);
    let way_index_path = hpf_way_index_path(cache_dir, osm_pbf_path);
    let pbf_replication = read_pbf_replication_anchor(osm_pbf_path).ok();
    let mut cache_hit = true;

    let mut cache = load_cache(&cache_path, &metadata)?;
    let mut way_index = if diff_config.is_some() {
        match load_way_index(&way_index_path, &metadata)? {
            Some(index) => Some(Arc::new(index)),
            None => None,
        }
    } else {
        None
    };

    if cache.is_none() || (diff_config.is_some() && way_index.is_none()) {
        let (node_coordinates, ways) = load_walkable_osm(osm_pbf_path)?;
        if cache.is_none() {
            let built_cache = build_hpf_cache_from_data(
                osm_pbf_path,
                stops,
                max_distance_meters,
                &node_coordinates,
                &ways,
            )?;
            store_cache(&cache_path, &built_cache)?;
            cache = Some(built_cache);
            cache_hit = false;
        }
        if diff_config.is_some() && way_index.is_none() {
            let way_entries = build_way_index_entries(&node_coordinates, &ways);
            store_way_index(&way_index_path, &metadata, &way_entries)?;
            way_index = load_way_index(&way_index_path, &metadata)?.map(Arc::new);
        }
    }

    let cache = cache.context("failed to materialize HPF cache")?;
    let covered_nodes = cache.nodes.len();
    let anchored_stops = cache.anchored_stops;
    info!(
        cache = %cache_path.display(),
        covered_nodes,
        anchored_stops,
        cache_hit,
        "loaded holographic pedestrian forest"
    );

    Ok(HpfBuildResult {
        forest: HolographicPedestrianForest::from_cache(
            cache,
            walk_speed_mps,
            snap_tolerance_meters,
            snap_quadratic_kappa_meters,
            search_window,
            cache_dir,
            osm_pbf_path,
            metadata,
            diff_config,
            pbf_replication,
            way_index,
        ),
        strategy: OSM_HPF_STRATEGY,
        cache_hit,
        covered_nodes,
        anchored_stops,
    })
}

fn load_walkable_osm(
    osm_pbf_path: &Path,
) -> Result<(HashMap<NodeId, (f64, f64)>, Vec<Way>)> {
    let file = File::open(osm_pbf_path)
        .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?;
    let mut pbf = OsmPbfReader::new(BufReader::new(file));
    let objects = pbf
        .get_objs_and_deps(|obj| matches!(obj, OsmObj::Way(way) if is_walkable_way(way)))
        .context("failed to extract walkable OSM ways and dependencies for HPF")?;

    let mut node_coordinates = HashMap::<NodeId, (f64, f64)>::new();
    let mut ways = Vec::<Way>::new();
    for object in objects.into_values() {
        match object {
            OsmObj::Node(node) => {
                node_coordinates.insert(node.id, (node.lat(), node.lon()));
            }
            OsmObj::Way(way) => ways.push(way),
            _ => {}
        }
    }

    Ok((node_coordinates, ways))
}

fn build_hpf_cache_from_data(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    max_distance_meters: f64,
    node_coordinates: &HashMap<NodeId, (f64, f64)>,
    ways: &[Way],
) -> Result<HpfCache> {
    let metadata = build_cache_metadata(osm_pbf_path, stops, max_distance_meters)?;
    let (graph_coordinates, graph_edges) = build_pedestrian_graph(node_coordinates, ways)?;
    if graph_coordinates.is_empty() {
        bail!("OSM pedestrian graph is empty after filtering walkable ways");
    }

    let graph_index = RTree::bulk_load(
        graph_coordinates
            .iter()
            .enumerate()
            .map(|(index, (lat, lon))| IndexedPoint {
                index,
                point: [*lon, *lat],
            })
            .collect(),
    );

    let max_snap_distance_meters = max_distance_meters.min(250.0).max(80.0);
    let stop_anchors = stops
        .iter()
        .map(|stop| {
            snap_stop_to_graph(
                stop,
                &graph_index,
                &graph_coordinates,
                max_snap_distance_meters,
            )
        })
        .collect::<Vec<_>>();
    let anchored_stops = stop_anchors.iter().filter(|anchor| anchor.is_some()).count();
    if anchored_stops == 0 {
        bail!("no GTFS stops could be anchored to the pedestrian graph for HPF")
    }

    let (best_distances, roots, parents) =
        multi_source_forest(&graph_edges, &stop_anchors, max_distance_meters);

    let mut graph_to_hpf = HashMap::<usize, u32>::new();
    let mut nodes = Vec::<(usize, HpfNode)>::new();
    for (graph_index, best_distance) in best_distances.iter().copied().enumerate() {
        let root_stop_index = roots[graph_index];
        if !best_distance.is_finite() || best_distance > max_distance_meters || root_stop_index == u32::MAX {
            continue;
        }
        nodes.push((
            graph_index,
            HpfNode {
                morton: morton_code(graph_coordinates[graph_index].0, graph_coordinates[graph_index].1),
                parent_index: u32::MAX,
                root_stop_index,
                cost_meters: best_distance as f32,
            },
        ));
    }

    nodes.sort_by(|left, right| {
        left.1
            .morton
            .cmp(&right.1.morton)
            .then_with(|| left.0.cmp(&right.0))
    });

    for (position, (graph_index, _)) in nodes.iter().enumerate() {
        graph_to_hpf.insert(*graph_index, position as u32);
    }

    let nodes = nodes
        .into_iter()
        .map(|(graph_index, mut node)| {
            if let Some(parent_graph_index) = parents.get(&graph_index).copied() {
                if let Some(parent_hpf_index) = graph_to_hpf.get(&parent_graph_index).copied() {
                    node.parent_index = parent_hpf_index;
                }
            }
            node
        })
        .collect::<Vec<_>>();

    Ok(HpfCache {
        metadata,
        anchored_stops,
        nodes,
    })
}

fn multi_source_forest(
    graph_edges: &[Vec<PedestrianEdge>],
    stop_anchors: &[Option<StopAnchor>],
    max_distance_meters: f64,
) -> (Vec<f64>, Vec<u32>, HashMap<usize, usize>) {
    let mut best_distances = vec![f64::INFINITY; graph_edges.len()];
    let mut roots = vec![u32::MAX; graph_edges.len()];
    let mut parents = HashMap::<usize, usize>::new();
    let mut heap = BinaryHeap::<HeapState>::new();

    for (stop_index, anchor) in stop_anchors.iter().enumerate() {
        let Some(anchor) = anchor else {
            continue;
        };
        let distance = anchor.snap_distance_meters;
        if distance > max_distance_meters {
            continue;
        }

        let should_update = match best_distances[anchor.node_index].partial_cmp(&distance) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => roots[anchor.node_index] > stop_index as u32,
            _ => false,
        };
        if should_update {
            best_distances[anchor.node_index] = distance;
            roots[anchor.node_index] = stop_index as u32;
            parents.remove(&anchor.node_index);
            heap.push(HeapState {
                node_index: anchor.node_index,
                distance_meters: distance,
            });
        }
    }

    while let Some(state) = heap.pop() {
        let known_distance = best_distances[state.node_index];
        if state.distance_meters > known_distance {
            continue;
        }
        if state.distance_meters > max_distance_meters {
            break;
        }

        let root_stop = roots[state.node_index];
        for edge in &graph_edges[state.node_index] {
            let candidate_distance = state.distance_meters + edge.distance_meters;
            if candidate_distance > max_distance_meters {
                continue;
            }

            let should_update = match best_distances[edge.to].partial_cmp(&candidate_distance) {
                Some(Ordering::Greater) => true,
                Some(Ordering::Equal) => root_stop < roots[edge.to],
                _ => false,
            };
            if should_update {
                best_distances[edge.to] = candidate_distance;
                roots[edge.to] = root_stop;
                parents.insert(edge.to, state.node_index);
                heap.push(HeapState {
                    node_index: edge.to,
                    distance_meters: candidate_distance,
                });
            }
        }
    }

    (best_distances, roots, parents)
}

fn build_pedestrian_graph(
    node_coordinates: &HashMap<NodeId, (f64, f64)>,
    ways: &[Way],
) -> Result<(Vec<(f64, f64)>, Vec<Vec<PedestrianEdge>>)> {
    let mut graph_coordinates = Vec::<(f64, f64)>::new();
    let mut graph_edges = Vec::<Vec<PedestrianEdge>>::new();
    let mut node_lookup = HashMap::<NodeId, usize>::new();

    for way in ways {
        for window in way.nodes.windows(2) {
            let from_id = window[0];
            let to_id = window[1];
            let Some(&(from_lat, from_lon)) = node_coordinates.get(&from_id) else {
                continue;
            };
            let Some(&(to_lat, to_lon)) = node_coordinates.get(&to_id) else {
                continue;
            };

            let from_index = graph_index_for(
                from_id,
                (from_lat, from_lon),
                &mut node_lookup,
                &mut graph_coordinates,
                &mut graph_edges,
            );
            let to_index = graph_index_for(
                to_id,
                (to_lat, to_lon),
                &mut node_lookup,
                &mut graph_coordinates,
                &mut graph_edges,
            );
            if from_index == to_index {
                continue;
            }

            let distance_meters = haversine_meters(from_lat, from_lon, to_lat, to_lon);
            if !(distance_meters.is_finite()) || distance_meters <= 0.0 {
                continue;
            }

            graph_edges[from_index].push(PedestrianEdge {
                to: to_index,
                distance_meters,
            });
            graph_edges[to_index].push(PedestrianEdge {
                to: from_index,
                distance_meters,
            });
        }
    }

    Ok((graph_coordinates, graph_edges))
}

fn graph_index_for(
    node_id: NodeId,
    coordinate: (f64, f64),
    node_lookup: &mut HashMap<NodeId, usize>,
    graph_coordinates: &mut Vec<(f64, f64)>,
    graph_edges: &mut Vec<Vec<PedestrianEdge>>,
) -> usize {
    if let Some(index) = node_lookup.get(&node_id).copied() {
        return index;
    }

    let index = graph_coordinates.len();
    graph_coordinates.push(coordinate);
    graph_edges.push(Vec::new());
    node_lookup.insert(node_id, index);
    index
}

fn snap_stop_to_graph(
    stop: &StopRecord,
    graph_index: &RTree<IndexedPoint>,
    graph_coordinates: &[(f64, f64)],
    max_snap_distance_meters: f64,
) -> Option<StopAnchor> {
    let lat = stop.latitude?;
    let lon = stop.longitude?;
    let mut best_anchor = None;
    let mut best_distance = f64::INFINITY;

    for candidate in graph_index.nearest_neighbor_iter(&[lon, lat]).take(8) {
        let (candidate_lat, candidate_lon) = graph_coordinates[candidate.index];
        let distance = haversine_meters(lat, lon, candidate_lat, candidate_lon);
        if distance < best_distance {
            best_distance = distance;
            best_anchor = Some(StopAnchor {
                node_index: candidate.index,
                snap_distance_meters: distance,
            });
        }
    }

    best_anchor.filter(|anchor| anchor.snap_distance_meters <= max_snap_distance_meters)
}

fn load_cache(cache_path: &Path, metadata: &HpfCacheMetadata) -> Result<Option<HpfCache>> {
    if !cache_path.exists() {
        return Ok(None);
    }

    let file = File::open(cache_path)
        .with_context(|| format!("unable to open HPF cache {}", cache_path.display()))?;
    let reader = BufReader::new(file);
    let cache: HpfCache = match bincode::deserialize_from(reader) {
        Ok(cache) => cache,
        Err(error) => {
            warn!(%error, cache = %cache_path.display(), "invalid HPF cache, rebuilding");
            return Ok(None);
        }
    };

    if cache.metadata == *metadata {
        Ok(Some(cache))
    } else {
        info!(cache = %cache_path.display(), "HPF cache metadata mismatch, rebuilding");
        Ok(None)
    }
}

fn store_cache(cache_path: &Path, cache: &HpfCache) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("unable to create HPF cache directory {}", parent.display()))?;
    }
    let file = File::create(cache_path)
        .with_context(|| format!("unable to create HPF cache {}", cache_path.display()))?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, cache).context("failed to serialize HPF cache")
}

fn build_cache_metadata(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    max_distance_meters: f64,
) -> Result<HpfCacheMetadata> {
    let metadata = fs::metadata(osm_pbf_path)
        .with_context(|| format!("unable to stat OSM PBF at {}", osm_pbf_path.display()))?;
    let modified_secs = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());

    Ok(HpfCacheMetadata {
        osm_pbf_bytes: metadata.len(),
        osm_pbf_modified_unix_secs: modified_secs,
        stop_fingerprint: stop_fingerprint(stops),
        max_distance_bits: max_distance_meters.to_bits(),
    })
}

fn stop_fingerprint(stops: &[StopRecord]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for stop in stops {
        stop.id.hash(&mut hasher);
        stop.code.hash(&mut hasher);
        stop.latitude.map(f64::to_bits).hash(&mut hasher);
        stop.longitude.map(f64::to_bits).hash(&mut hasher);
    }
    hasher.finish()
}

fn hpf_cache_path(cache_dir: &Path, osm_pbf_path: &Path) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-hpf");
    let file_name = format!("{stem}.hpf.bin");
    cache_dir.join(file_name)
}

fn hpf_way_index_path(cache_dir: &Path, osm_pbf_path: &Path) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-hpf");
    cache_dir.join(format!("{stem}.hpf.ways.bin"))
}

fn hpf_overlay_path(cache_dir: &Path, osm_pbf_path: &Path) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-hpf");
    cache_dir.join(format!("{stem}.hpf.overlay.bin"))
}

#[derive(Clone)]
struct HpfOverlayDraft {
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    entries: HashMap<u64, HpfOverlayCell>,
    way_overrides: HashMap<i64, HpfWayOverride>,
}

struct RemoteStateFile {
    sequence_number: u64,
    timestamp: Option<String>,
}

struct WayAnchor {
    anchor_cell: u64,
    anchor_cost: f64,
    root_stop_index: u32,
    reverse: bool,
}

struct ResolvedAnchor {
    anchor_cell: u64,
    anchor_cost: f64,
    root_stop_index: u32,
}

fn load_overlay_runtime(
    overlay_path: &Path,
    base_metadata: &HpfCacheMetadata,
    diff_config: Option<&HpfDiffConfig>,
) -> HpfOverlayRuntime {
    if diff_config.is_none() {
        return empty_overlay_runtime(None);
    }

    match load_overlay_persisted(overlay_path, base_metadata) {
        Ok(Some(persisted)) => overlay_runtime_from_parts(
            persisted.entries,
            persisted.way_overrides,
            diff_config,
            persisted.applied_sequence,
            persisted.applied_timestamp,
            None,
            None,
        ),
        Ok(None) => empty_overlay_runtime(diff_config),
        Err(error) => {
            warn!(
                %error,
                overlay = %overlay_path.display(),
                "failed to load HPF overlay state; starting from empty differential overlay"
            );
            let mut runtime = empty_overlay_runtime(diff_config);
            runtime.last_error = Some(error.to_string());
            runtime
        }
    }
}

fn load_overlay_persisted(
    overlay_path: &Path,
    base_metadata: &HpfCacheMetadata,
) -> Result<Option<HpfOverlayPersisted>> {
    if !overlay_path.exists() {
        return Ok(None);
    }

    let file = File::open(overlay_path)
        .with_context(|| format!("unable to open HPF overlay {}", overlay_path.display()))?;
    let reader = BufReader::new(file);
    let persisted: HpfOverlayPersisted = bincode::deserialize_from(reader)
        .with_context(|| format!("unable to deserialize HPF overlay {}", overlay_path.display()))?;
    if &persisted.magic != HPF_OVERLAY_MAGIC {
        return Ok(None);
    }
    if persisted.metadata != *base_metadata {
        return Ok(None);
    }
    Ok(Some(persisted))
}

fn persist_overlay_runtime(
    overlay_path: &Path,
    base_metadata: &HpfCacheMetadata,
    runtime: &HpfOverlayRuntime,
) -> Result<()> {
    if let Some(parent) = overlay_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("unable to create HPF overlay directory {}", parent.display()))?;
    }

    let persisted = HpfOverlayPersisted {
        magic: *HPF_OVERLAY_MAGIC,
        metadata: base_metadata.clone(),
        state_url: runtime.state_url.clone(),
        diff_base_url: runtime.diff_base_url.clone(),
        applied_sequence: runtime.applied_sequence,
        applied_timestamp: runtime.applied_timestamp.clone(),
        entries: runtime.entries.as_ref().clone(),
        way_overrides: runtime.way_overrides.values().cloned().collect(),
    };

    let tmp_path = overlay_path.with_extension("overlay.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("unable to create HPF overlay temp file {}", tmp_path.display()))?;
    bincode::serialize_into(BufWriter::new(file), &persisted)
        .context("failed to serialize HPF overlay state")?;
    if overlay_path.exists() {
        fs::remove_file(overlay_path)
            .with_context(|| format!("unable to replace HPF overlay {}", overlay_path.display()))?;
    }
    fs::rename(&tmp_path, overlay_path)
        .with_context(|| format!("unable to move HPF overlay into {}", overlay_path.display()))
}

fn overlay_runtime_from_parts(
    entries: Vec<HpfOverlayCell>,
    way_overrides: Vec<HpfWayOverride>,
    diff_config: Option<&HpfDiffConfig>,
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    last_poll_timestamp: Option<String>,
    last_error: Option<String>,
) -> HpfOverlayRuntime {
    let mut sorted_entries = entries;
    sorted_entries.sort_by_key(|entry| entry.cell);
    let entry_lookup = sorted_entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry.cell, index))
        .collect::<HashMap<_, _>>();
    let synthetic_cells = sorted_entries
        .iter()
        .filter(|entry| entry.synthetic)
        .map(|entry| entry.cell)
        .collect::<Vec<_>>();
    let way_override_map = way_overrides
        .into_iter()
        .map(|entry| (entry.way_id, entry))
        .collect::<HashMap<_, _>>();

    HpfOverlayRuntime {
        enabled: diff_config.is_some(),
        state_url: diff_config.map(|config| config.state_url.clone()),
        diff_base_url: diff_config.map(|config| resolved_diff_base_url(config)),
        poll_interval_secs: diff_config.map(|config| config.poll_interval_secs),
        applied_sequence,
        applied_timestamp,
        last_poll_timestamp,
        last_error,
        entries: Arc::new(sorted_entries),
        entry_lookup: Arc::new(entry_lookup),
        synthetic_cells: Arc::new(synthetic_cells),
        way_overrides: Arc::new(way_override_map),
    }
}

fn overlay_runtime_from_draft(
    draft: &HpfOverlayDraft,
    diff_config: &HpfDiffConfig,
    last_poll_timestamp: Option<String>,
    last_error: Option<String>,
) -> HpfOverlayRuntime {
    overlay_runtime_from_parts(
        draft.entries.values().cloned().collect(),
        draft.way_overrides.values().cloned().collect(),
        Some(diff_config),
        draft.applied_sequence,
        draft.applied_timestamp.clone(),
        last_poll_timestamp,
        last_error,
    )
}

fn overlay_draft_from_runtime(runtime: &HpfOverlayRuntime) -> HpfOverlayDraft {
    HpfOverlayDraft {
        applied_sequence: runtime.applied_sequence,
        applied_timestamp: runtime.applied_timestamp.clone(),
        entries: runtime.entries.iter().cloned().map(|entry| (entry.cell, entry)).collect(),
        way_overrides: runtime
            .way_overrides
            .iter()
            .map(|(way_id, entry)| (*way_id, entry.clone()))
            .collect(),
    }
}

fn empty_overlay_runtime(diff_config: Option<&HpfDiffConfig>) -> HpfOverlayRuntime {
    overlay_runtime_from_parts(Vec::new(), Vec::new(), diff_config, None, None, None, None)
}

fn resolved_diff_base_url(config: &HpfDiffConfig) -> String {
    match config.diff_base_url.as_deref() {
        Some(value) => normalize_diff_base_url(value),
        None => normalize_diff_base_url(&config.state_url),
    }
}

fn normalize_diff_base_url(value: &str) -> String {
    let trimmed = value.trim();
    let (scheme, remainder) = if let Some(value) = trimmed.strip_prefix("https://") {
        ("https://", value)
    } else if let Some(value) = trimmed.strip_prefix("http://") {
        ("http://", value)
    } else {
        ("", trimmed)
    };
    let without_state = remainder.strip_suffix("state.txt").unwrap_or(remainder);
    let mut segments = without_state
        .trim_end_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    while segments
        .last()
        .is_some_and(|segment| segment.len() == 3 && segment.chars().all(|ch| ch.is_ascii_digit()))
    {
        segments.pop();
    }

    let mut normalized = format!("{}{}/", scheme, segments.join("/"));
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    normalized
}

fn sequence_diff_url(base_url: &str, sequence_number: u64) -> String {
    let base_url = normalize_diff_base_url(base_url);
    let a = sequence_number / 1_000_000;
    let b = (sequence_number / 1_000) % 1_000;
    let c = sequence_number % 1_000;
    format!("{base_url}{a:03}/{b:03}/{c:03}.osc.gz")
}

fn parse_remote_state_file(body: &str) -> Result<RemoteStateFile> {
    let mut sequence_number = None;
    let mut timestamp = None;
    for line in body.lines() {
        let trimmed = line.trim();
        if let Some(value) = trimmed.strip_prefix("sequenceNumber=") {
            sequence_number = value.trim().parse::<u64>().ok();
        } else if let Some(value) = trimmed.strip_prefix("timestamp=") {
            timestamp = Some(value.replace("\\:", ":"));
        }
    }

    let sequence_number = sequence_number.context("OSM diff state file missing sequenceNumber")?;
    Ok(RemoteStateFile {
        sequence_number,
        timestamp,
    })
}

fn parse_osc_diff(bytes: &[u8]) -> Result<OscDiff> {
    let xml_bytes = maybe_decompress_osc(bytes)?;
    let mut reader = Reader::from_reader(std::io::Cursor::new(xml_bytes));
    reader.config_mut().trim_text(true);

    let mut buffer = Vec::<u8>::new();
    let mut current_action = OscAction::Modify;
    let mut nodes = HashMap::<i64, (f64, f64)>::new();
    let mut current_way = None::<OscWayChange>;
    let mut ways = Vec::<OscWayChange>::new();

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(event)) => match event.name().as_ref() {
                b"create" => current_action = OscAction::Create,
                b"modify" => current_action = OscAction::Modify,
                b"delete" => current_action = OscAction::Delete,
                b"node" => {
                    if let Some((id, lat, lon)) = parse_osc_node(&event)? {
                        nodes.insert(id, (lat, lon));
                    }
                }
                b"way" => {
                    current_way = Some(OscWayChange {
                        action: current_action,
                        way_id: parse_attr_i64(&event, b"id")
                            .context("OSM diff way missing id attribute")?,
                        node_refs: Vec::new(),
                        coordinates: Vec::new(),
                        tags: HashMap::new(),
                    });
                }
                _ => {}
            },
            Ok(Event::Empty(event)) => match event.name().as_ref() {
                b"node" => {
                    if let Some((id, lat, lon)) = parse_osc_node(&event)? {
                        nodes.insert(id, (lat, lon));
                    }
                }
                b"nd" => {
                    if let Some(way) = current_way.as_mut() {
                        if let Some(node_ref) = parse_attr_i64(&event, b"ref") {
                            way.node_refs.push(node_ref);
                        }
                    }
                }
                b"tag" => {
                    if let Some(way) = current_way.as_mut() {
                        if let (Some(key), Some(value)) =
                            (parse_attr_string(&event, b"k"), parse_attr_string(&event, b"v"))
                        {
                            way.tags.insert(key, value);
                        }
                    }
                }
                _ => {}
            },
            Ok(Event::End(event)) if event.name().as_ref() == b"way" => {
                if let Some(way) = current_way.take() {
                    let coordinates = way
                        .node_refs
                        .iter()
                        .filter_map(|node_id| nodes.get(node_id).copied())
                        .collect::<Vec<_>>();
                    ways.push(way.with_coordinates(coordinates));
                }
            }
            Ok(_) => {}
            Err(error) => return Err(error).context("failed to parse OSM change XML"),
        }
        buffer.clear();
    }

    Ok(OscDiff { ways })
}

fn maybe_decompress_osc(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() >= 2 && bytes[0] == 0x1f && bytes[1] == 0x8b {
        let mut decoder = GzDecoder::new(bytes);
        let mut xml = Vec::new();
        decoder
            .read_to_end(&mut xml)
            .context("failed to decompress OSM change payload")?;
        Ok(xml)
    } else {
        Ok(bytes.to_vec())
    }
}

fn parse_osc_node(event: &BytesStart<'_>) -> Result<Option<(i64, f64, f64)>> {
    let Some(id) = parse_attr_i64(event, b"id") else {
        return Ok(None);
    };
    let (Some(lat), Some(lon)) = (parse_attr_f64(event, b"lat"), parse_attr_f64(event, b"lon")) else {
        return Ok(None);
    };
    Ok(Some((id, lat, lon)))
}

trait OscWayWithCoordinates {
    fn with_coordinates(self, coordinates: Vec<(f64, f64)>) -> Self;
}

impl OscWayWithCoordinates for OscWayChange {
    fn with_coordinates(mut self, coordinates: Vec<(f64, f64)>) -> Self {
        self.coordinates = coordinates;
        self
    }
}

fn parse_attr_i64(event: &BytesStart<'_>, key: &[u8]) -> Option<i64> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| {
            std::str::from_utf8(attribute.value.as_ref())
                .ok()
                .map(str::to_owned)
        })
        .and_then(|value| value.parse::<i64>().ok())
}

fn parse_attr_f64(event: &BytesStart<'_>, key: &[u8]) -> Option<f64> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| {
            std::str::from_utf8(attribute.value.as_ref())
                .ok()
                .map(str::to_owned)
        })
        .and_then(|value| value.parse::<f64>().ok())
}

fn parse_attr_string(event: &BytesStart<'_>, key: &[u8]) -> Option<String> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| {
            std::str::from_utf8(attribute.value.as_ref())
                .ok()
                .map(str::to_owned)
        })
}

fn build_way_index_entries(
    node_coordinates: &HashMap<NodeId, (f64, f64)>,
    ways: &[Way],
) -> Vec<(i64, Vec<u64>)> {
    let mut entries = ways
        .iter()
        .map(|way| (way.id.0, rasterize_way_cells(node_coordinates, way)))
        .filter(|(_, cells)| !cells.is_empty())
        .collect::<Vec<_>>();
    entries.sort_by_key(|(way_id, _)| *way_id);
    entries
}

fn store_way_index(
    way_index_path: &Path,
    metadata: &HpfCacheMetadata,
    entries: &[(i64, Vec<u64>)],
) -> Result<()> {
    if let Some(parent) = way_index_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("unable to create HPF way index directory {}", parent.display()))?;
    }

    let tmp_path = way_index_path.with_extension("ways.tmp");
    let mut writer = BufWriter::new(
        File::create(&tmp_path)
            .with_context(|| format!("unable to create HPF way index {}", tmp_path.display()))?,
    );
    writer.write_all(HPF_WAY_INDEX_MAGIC)?;
    writer.write_all(&metadata.osm_pbf_bytes.to_le_bytes())?;
    writer.write_all(&metadata.osm_pbf_modified_unix_secs.unwrap_or(u64::MAX).to_le_bytes())?;
    writer.write_all(&metadata.stop_fingerprint.to_le_bytes())?;
    writer.write_all(&metadata.max_distance_bits.to_le_bytes())?;
    writer.write_all(&(entries.len() as u64).to_le_bytes())?;

    let mut cell_offset = 0u64;
    for (way_id, cells) in entries {
        writer.write_all(&way_id.to_le_bytes())?;
        writer.write_all(&cell_offset.to_le_bytes())?;
        writer.write_all(&(cells.len() as u32).to_le_bytes())?;
        writer.write_all(&0u32.to_le_bytes())?;
        cell_offset += cells.len() as u64;
    }
    for (_, cells) in entries {
        for cell in cells {
            writer.write_all(&cell.to_le_bytes())?;
        }
    }
    writer.flush()?;

    if way_index_path.exists() {
        fs::remove_file(way_index_path)
            .with_context(|| format!("unable to replace HPF way index {}", way_index_path.display()))?;
    }
    fs::rename(&tmp_path, way_index_path)
        .with_context(|| format!("unable to move HPF way index into {}", way_index_path.display()))
}

fn load_way_index(
    way_index_path: &Path,
    metadata: &HpfCacheMetadata,
) -> Result<Option<HpfWayIndex>> {
    if !way_index_path.exists() {
        return Ok(None);
    }

    let file = File::open(way_index_path)
        .with_context(|| format!("unable to open HPF way index {}", way_index_path.display()))?;
    let mmap = unsafe { Mmap::map(&file) }
        .with_context(|| format!("unable to mmap HPF way index {}", way_index_path.display()))?;
    if mmap.len() < 48 || &mmap[0..8] != HPF_WAY_INDEX_MAGIC {
        return Ok(None);
    }

    let stored_metadata = HpfCacheMetadata {
        osm_pbf_bytes: read_u64_le(&mmap[8..16]),
        osm_pbf_modified_unix_secs: match read_u64_le(&mmap[16..24]) {
            u64::MAX => None,
            value => Some(value),
        },
        stop_fingerprint: read_u64_le(&mmap[24..32]),
        max_distance_bits: read_u64_le(&mmap[32..40]),
    };
    if stored_metadata != *metadata {
        return Ok(None);
    }
    let record_count = read_u64_le(&mmap[40..48]) as usize;
    let cells_offset = 48 + (record_count * 24);
    if mmap.len() < cells_offset {
        return Ok(None);
    }

    Ok(Some(HpfWayIndex {
        mmap: Arc::new(mmap),
        record_count,
        cells_offset,
    }))
}

impl HpfWayIndex {
    fn cells_for_way(&self, way_id: i64) -> Option<Vec<u64>> {
        let mut left = 0usize;
        let mut right = self.record_count;
        while left < right {
            let mid = left + ((right - left) / 2);
            let record_offset = 48 + (mid * 24);
            let current_way_id = read_i64_le(&self.mmap[record_offset..record_offset + 8]);
            match current_way_id.cmp(&way_id) {
                Ordering::Less => left = mid + 1,
                Ordering::Greater => right = mid,
                Ordering::Equal => {
                    let cell_offset = read_u64_le(&self.mmap[record_offset + 8..record_offset + 16]) as usize;
                    let len = read_u32_le(&self.mmap[record_offset + 16..record_offset + 20]) as usize;
                    let start = self.cells_offset + (cell_offset * 8);
                    let end = start + (len * 8);
                    if end > self.mmap.len() {
                        return None;
                    }
                    return Some(
                        self.mmap[start..end]
                            .chunks_exact(8)
                            .map(read_u64_le)
                            .collect(),
                    );
                }
            }
        }
        None
    }
}

fn read_pbf_replication_anchor(osm_pbf_path: &Path) -> Result<HpfPbfReplicationAnchor> {
    let mut reader = BufReader::new(
        File::open(osm_pbf_path)
            .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?,
    );
    let header_len = read_u32_be_from_reader(&mut reader)? as usize;
    let mut header_bytes = vec![0; header_len];
    reader.read_exact(&mut header_bytes)?;
    let header = BlobHeader::parse_from_bytes(&header_bytes)
        .context("failed to parse OSM PBF header block header")?;
    if header.type_() != "OSMHeader" {
        bail!("OSM PBF does not start with an OSMHeader block")
    }

    let mut blob_bytes = vec![0; header.datasize() as usize];
    reader.read_exact(&mut blob_bytes)?;
    let blob = Blob::parse_from_bytes(&blob_bytes)
        .context("failed to parse OSM PBF header blob")?;
    let raw = if blob.has_raw() {
        blob.raw().to_vec()
    } else if blob.has_zlib_data() {
        let mut decoder = ZlibDecoder::new(blob.zlib_data());
        let mut bytes = Vec::new();
        decoder.read_to_end(&mut bytes)?;
        bytes
    } else {
        bail!("unsupported OSM PBF header compression")
    };
    let header_block = HeaderBlock::parse_from_bytes(&raw)
        .context("failed to parse OSM PBF header block")?;

    let timestamp = if header_block.has_osmosis_replication_timestamp() {
        chrono::DateTime::<Utc>::from_timestamp(header_block.osmosis_replication_timestamp(), 0)
            .map(|value| value.to_rfc3339())
    } else {
        None
    };

    Ok(HpfPbfReplicationAnchor {
        sequence_number: if header_block.has_osmosis_replication_sequence_number() {
            Some(header_block.osmosis_replication_sequence_number() as u64)
        } else {
            None
        },
        timestamp,
        base_url: if header_block.has_osmosis_replication_base_url() {
            Some(normalize_diff_base_url(
                header_block.osmosis_replication_base_url(),
            ))
        } else {
            None
        },
    })
}

fn current_way_cells(
    draft: &HpfOverlayDraft,
    way_index: &HpfWayIndex,
    way_id: i64,
) -> Vec<u64> {
    match draft.way_overrides.get(&way_id) {
        Some(override_state) if override_state.walkable => override_state.cells.clone(),
        Some(_) => Vec::new(),
        None => way_index.cells_for_way(way_id).unwrap_or_default(),
    }
}

fn update_way_override(
    draft: &mut HpfOverlayDraft,
    way_index: &HpfWayIndex,
    way_id: i64,
    walkable: bool,
    cells: Vec<u64>,
) {
    let base_cells = way_index.cells_for_way(way_id);
    let matches_base = match (&base_cells, walkable) {
        (Some(existing), true) => *existing == cells,
        (None, false) => true,
        _ => false,
    };
    if matches_base {
        draft.way_overrides.remove(&way_id);
        return;
    }
    draft.way_overrides.insert(
        way_id,
        HpfWayOverride {
            way_id,
            walkable,
            cells,
        },
    );
}

fn rasterize_change_cells(change: &OscWayChange, current_cells: &[u64]) -> Result<Vec<u64>> {
    if change.coordinates.len() >= 2 {
        Ok(rasterize_coordinate_chain(&change.coordinates))
    } else {
        Ok(current_cells.to_vec())
    }
}

fn rasterize_way_cells(
    node_coordinates: &HashMap<NodeId, (f64, f64)>,
    way: &Way,
) -> Vec<u64> {
    let mut coordinates = Vec::new();
    for node_id in &way.nodes {
        if let Some(&coordinate) = node_coordinates.get(node_id) {
            coordinates.push(coordinate);
        }
    }
    rasterize_coordinate_chain(&coordinates)
}

fn rasterize_coordinate_chain(coordinates: &[(f64, f64)]) -> Vec<u64> {
    let mut cells = Vec::<u64>::new();
    for window in coordinates.windows(2) {
        let (from_lat, from_lon) = window[0];
        let (to_lat, to_lon) = window[1];
        let distance = haversine_meters(from_lat, from_lon, to_lat, to_lon);
        let steps = ((distance / HPF_RASTER_STEP_METERS).ceil() as usize).max(1);
        for step in 0..=steps {
            let t = step as f64 / steps as f64;
            let lat = from_lat + ((to_lat - from_lat) * t);
            let lon = from_lon + ((to_lon - from_lon) * t);
            let cell = overlay_cell_from_morton(morton_code(lat, lon));
            if cells.last().copied() != Some(cell) {
                cells.push(cell);
            }
        }
    }
    cells
}

fn overlay_cell_from_morton(morton: u64) -> u64 {
    morton >> HPF_OVERLAY_SHIFT_BITS
}

fn overlay_cell_center(cell: u64) -> (f64, f64) {
    let prefix = cell << HPF_OVERLAY_SHIFT_BITS;
    let (lat_component, lon_component) = decode_morton_components(prefix);
    let lat_center = lat_component.saturating_add(HPF_OVERLAY_AXIS_CENTER);
    let lon_center = lon_component.saturating_add(HPF_OVERLAY_AXIS_CENTER);
    decode_morton_code(morton_code_from_components(lat_center, lon_center))
}

fn overlay_moore_neighbors(cell: u64) -> Vec<u64> {
    let prefix = cell << HPF_OVERLAY_SHIFT_BITS;
    let (lat_component, lon_component) = decode_morton_components(prefix);
    let mut neighbors = Vec::with_capacity(9);
    for lat_delta in -1i64..=1 {
        for lon_delta in -1i64..=1 {
            let next_lat = lat_component as i64 + (lat_delta * i64::from(HPF_OVERLAY_AXIS_STEP));
            let next_lon = lon_component as i64 + (lon_delta * i64::from(HPF_OVERLAY_AXIS_STEP));
            if next_lat < 0
                || next_lon < 0
                || next_lat > i64::from(u32::MAX)
                || next_lon > i64::from(u32::MAX)
            {
                continue;
            }
            neighbors.push(
                overlay_cell_from_morton(morton_code_from_components(next_lat as u32, next_lon as u32)),
            );
        }
    }
    neighbors
}

fn overlay_transition_cost(from_cell: u64, to_cell: u64) -> f64 {
    let (from_lat, from_lon) = overlay_cell_center(from_cell);
    let (to_lat, to_lon) = overlay_cell_center(to_cell);
    haversine_meters(from_lat, from_lon, to_lat, to_lon)
}

fn prune_overlay_entry(
    draft: &mut HpfOverlayDraft,
    forest: &HolographicPedestrianForest,
    cell: u64,
) {
    let Some(entry) = draft.entries.get(&cell).cloned() else {
        return;
    };
    if entry.blocked {
        return;
    }
    let Some((_, base_state)) = forest.best_base_node_for_cell(cell) else {
        return;
    };
    if entry.synthetic {
        return;
    }
    if entry.root_stop_index == base_state.root_stop_index
        && entry.parent_cell == base_state.parent_cell
        && (f64::from(entry.cost_meters) - base_state.cost_meters).abs() <= HPF_OVERLAY_EPSILON_METERS
    {
        draft.entries.remove(&cell);
    }
}

fn should_replace_candidate(current: Option<&CandidateConnector>, total_distance: f64) -> bool {
    match current {
        Some(existing) => total_distance + HPF_OVERLAY_EPSILON_METERS < existing.distance_meters,
        None => true,
    }
}

fn is_walkable_tags(tags: &HashMap<String, String>) -> bool {
    if tags.get("area").is_some_and(|value| value == "yes") {
        return false;
    }
    if matches!(
        tags.get("access").map(|value| value.as_str()),
        Some("no" | "private")
    ) {
        return false;
    }
    if matches!(
        tags.get("foot").map(|value| value.as_str()),
        Some("no" | "private" | "use_sidepath")
    ) {
        return false;
    }
    if matches!(
        tags.get("pedestrian").map(|value| value.as_str()),
        Some("no")
    ) {
        return false;
    }
    if let Some(highway) = tags.get("highway").map(|value| value.as_str()) {
        return !matches!(
            highway,
            "motorway"
                | "motorway_link"
                | "trunk"
                | "trunk_link"
                | "construction"
                | "proposed"
                | "raceway"
                | "bus_guideway"
        );
    }
    tags.get("railway")
        .is_some_and(|value| value == "platform")
        || tags
            .get("public_transport")
            .is_some_and(|value| value == "platform")
}

fn read_u32_be_from_reader(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(array)
}

fn read_i64_le(bytes: &[u8]) -> i64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    i64::from_le_bytes(array)
}

fn read_u32_le(bytes: &[u8]) -> u32 {
    let mut array = [0u8; 4];
    array.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(array)
}

fn push_polyline_point(polyline: &mut Vec<PolylinePoint>, lat: f64, lon: f64) {
    let should_push = polyline
        .last()
        .is_none_or(|last| (last.lat - lat).abs() > 1e-7 || (last.lon - lon).abs() > 1e-7);
    if should_push {
        polyline.push(PolylinePoint { lat, lon });
    }
}

fn is_walkable_way(way: &Way) -> bool {
    if way.tags.get("area").is_some_and(|value| value == "yes") {
        return false;
    }
    if matches!(
        way.tags.get("access").map(|value| value.as_str()),
        Some("no" | "private")
    ) {
        return false;
    }
    if matches!(
        way.tags.get("foot").map(|value| value.as_str()),
        Some("no" | "private" | "use_sidepath")
    ) {
        return false;
    }
    if matches!(
        way.tags.get("pedestrian").map(|value| value.as_str()),
        Some("no")
    ) {
        return false;
    }

    if let Some(highway) = way.tags.get("highway").map(|value| value.as_str()) {
        return !matches!(
            highway,
            "motorway"
                | "motorway_link"
                | "trunk"
                | "trunk_link"
                | "construction"
                | "proposed"
                | "raceway"
                | "bus_guideway"
        );
    }

    way.tags
        .get("railway")
        .is_some_and(|value| value == "platform")
        || way
            .tags
            .get("public_transport")
            .is_some_and(|value| value == "platform")
}

fn haversine_meters(lat_a: f64, lon_a: f64, lat_b: f64, lon_b: f64) -> f64 {
    let earth_radius_m = 6_371_000.0_f64;
    let delta_lat = (lat_b - lat_a).to_radians();
    let delta_lon = (lon_b - lon_a).to_radians();
    let lat_a = lat_a.to_radians();
    let lat_b = lat_b.to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat_a.cos() * lat_b.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    earth_radius_m * c
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::Instant};

    use arc_swap::ArcSwap;

    use super::{
        HolographicPedestrianForest, HpfCacheMetadata, HpfNode, HpfOverlayDraft,
        empty_overlay_runtime, normalize_diff_base_url, overlay_cell_from_morton,
        parse_remote_state_file, sequence_diff_url,
    };
    use crate::{engine::StopRecord, geo::morton_code};

    fn test_forest(mut nodes: Vec<HpfNode>) -> HolographicPedestrianForest {
        nodes.sort_by_key(|node| node.morton);
        HolographicPedestrianForest {
            nodes: Arc::new(nodes),
            walk_speed_mps: 1.35,
            snap_tolerance_meters: 140.0,
            snap_quadratic_kappa_meters: 40.0,
            search_window: 256,
            overlay_runtime: Arc::new(ArcSwap::from_pointee(empty_overlay_runtime(None))),
            overlay_path: Arc::new(PathBuf::from("overlay.bin")),
            way_index: None,
            base_metadata: HpfCacheMetadata {
                osm_pbf_bytes: 0,
                osm_pbf_modified_unix_secs: None,
                stop_fingerprint: 0,
                max_distance_bits: 0,
            },
            diff_config: None,
            pbf_replication: None,
        }
    }

    #[test]
    fn query_connectors_prefers_lowest_total_cost_per_stop() {
        let forest = test_forest(vec![
                HpfNode {
                    morton: morton_code(41.9000, 12.5000),
                    parent_index: u32::MAX,
                    root_stop_index: 0,
                    cost_meters: 100.0,
                },
                HpfNode {
                    morton: morton_code(41.9002, 12.5002),
                    parent_index: u32::MAX,
                    root_stop_index: 1,
                    cost_meters: 80.0,
                },
            ]);
        let stops = vec![
            StopRecord {
                global_id: 1,
                feed_index: 0,
                feed_id: "roma".to_owned(),
                local_id: "a".to_owned(),
                id: "roma:a".to_owned(),
                code: None,
                name: "A".to_owned(),
                latitude: Some(41.9000),
                longitude: Some(12.5000),
                search_blob: String::new(),
            },
            StopRecord {
                global_id: 2,
                feed_index: 0,
                feed_id: "roma".to_owned(),
                local_id: "b".to_owned(),
                id: "roma:b".to_owned(),
                code: None,
                name: "B".to_owned(),
                latitude: Some(41.9002),
                longitude: Some(12.5002),
                search_blob: String::new(),
            },
        ];

        let results = forest.query_connectors(41.90015, 12.50015, 2, &stops);
        assert_eq!(results.len(), 2);
        assert!(results[0].distance_meters <= results[1].distance_meters);
        assert_eq!(results[0].polyline.first().unwrap().lat, 41.90015);
    }

    #[test]
    fn query_connectors_prefers_network_coverage_over_long_asymptotic_snap() {
        let forest = test_forest(vec![
                HpfNode {
                    morton: morton_code(41.9000, 12.5000),
                    parent_index: u32::MAX,
                    root_stop_index: 0,
                    cost_meters: 17.79,
                },
                HpfNode {
                    morton: morton_code(41.9000, 12.5038),
                    parent_index: u32::MAX,
                    root_stop_index: 0,
                    cost_meters: 527.93,
                },
            ]);
        let stops = vec![StopRecord {
            global_id: 1,
            feed_index: 0,
            feed_id: "feed".to_owned(),
            local_id: "stop-1".to_owned(),
            id: "feed:stop-1".to_owned(),
            code: None,
            name: "Stop 1".to_owned(),
            latitude: Some(41.9000),
            longitude: Some(12.5000),
            search_blob: String::new(),
        }];

        let results = forest.query_connectors(41.9000, 12.5039, 1, &stops);
        assert_eq!(results.len(), 1);
        assert!(!results[0].used_asymptotic_penalty);
        assert!(results[0].distance_meters > 500.0);
        assert!(results[0].distance_meters < 600.0);
        assert!(results[0].polyline.iter().any(|point| {
            (point.lat - 41.9000).abs() < 0.0001 && (point.lon - 12.5038).abs() < 0.0001
        }));
    }

    #[test]
    fn parses_geofabrik_state_file() {
        let state = parse_remote_state_file(
            "# comment\ntimestamp=2026-04-05T20\\:20\\:45Z\nsequenceNumber=3806\n",
        )
        .expect("state file should parse");
        assert_eq!(state.sequence_number, 3806);
        assert_eq!(state.timestamp.as_deref(), Some("2026-04-05T20:20:45Z"));
    }

    #[test]
    fn normalizes_sequence_urls_from_folder_links() {
        let base = normalize_diff_base_url(
            "https://download.geofabrik.de/europe/italy/centro-updates/000/003/",
        );
        assert_eq!(
            sequence_diff_url(&base, 3_806),
            "https://download.geofabrik.de/europe/italy/centro-updates/000/003/806.osc.gz"
        );
    }

    #[test]
    fn topology_apply_stays_under_two_ms_in_release() {
        let forest = test_forest(vec![
            HpfNode {
                morton: morton_code(41.9000, 12.5000),
                parent_index: u32::MAX,
                root_stop_index: 0,
                cost_meters: 0.0,
            },
            HpfNode {
                morton: morton_code(41.9001, 12.5001),
                parent_index: 0,
                root_stop_index: 0,
                cost_meters: 12.0,
            },
            HpfNode {
                morton: morton_code(41.9002, 12.5002),
                parent_index: 1,
                root_stop_index: 0,
                cost_meters: 24.0,
            },
            HpfNode {
                morton: morton_code(41.9002, 12.5000),
                parent_index: 0,
                root_stop_index: 0,
                cost_meters: 18.0,
            },
        ]);

        let blocked_cell = overlay_cell_from_morton(morton_code(41.9001, 12.5001));
        let cooling_cells = vec![
            overlay_cell_from_morton(morton_code(41.90015, 12.50005)),
            overlay_cell_from_morton(morton_code(41.90018, 12.50008)),
        ];
        let mut draft = HpfOverlayDraft {
            applied_sequence: None,
            applied_timestamp: None,
            entries: Default::default(),
            way_overrides: Default::default(),
        };

        let started = Instant::now();
        forest.block_cells(&mut draft, &[blocked_cell]);
        forest.shadow_propagate(&mut draft, &[blocked_cell]);
        forest.cool_new_way(&mut draft, &cooling_cells);
        let elapsed = started.elapsed();

        assert!(!draft.entries.is_empty());
        if !cfg!(debug_assertions) {
            assert!(elapsed.as_micros() < 2_000, "elapsed: {elapsed:?}");
        }
    }
}