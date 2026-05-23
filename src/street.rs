use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fs::{self, File},
    io::{BufReader, BufWriter, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, anyhow, bail};
use arc_swap::ArcSwap;
use chrono::Utc;
use flate2::read::{GzDecoder, ZlibDecoder};
use osmpbfreader::{
    NodeId, OsmObj, OsmPbfReader, Way,
    fileformat::{Blob, BlobHeader},
    osmformat::HeaderBlock,
};
use protobuf::Message;
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use reqwest::blocking::Client as BlockingClient;
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    engine::PolylinePoint,
    hpf::HpfDiffConfig,
    progress::{progress_bar, progress_percent},
};

const STREET_GRAPH_SCHEMA_VERSION: u32 = 2;
const STREET_ROUTING_STRATEGY: &str = "flat-osm-bidirectional";
const WALK_CONNECTOR_SPEED_MPS: f64 = 1.35;
const DRIVE_CONNECTOR_SPEED_MPS: f64 = 8.33;
const WALK_MAX_SNAP_DISTANCE_METERS: f64 = 500.0;
const DRIVE_MAX_SNAP_DISTANCE_METERS: f64 = 2_000.0;
const SNAP_CANDIDATES: usize = 8;
const STREET_OVERLAY_MAGIC: &[u8; 8] = b"STROVLY1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreetMode {
    Walk,
    Drive,
}

impl StreetMode {
    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value
            .unwrap_or("drive")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "walk" | "foot" | "pedestrian" => Ok(Self::Walk),
            "drive" | "car" | "auto" => Ok(Self::Drive),
            other => bail!("unsupported street routing mode {other}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Walk => "walk",
            Self::Drive => "drive",
        }
    }

    fn connector_speed_mps(self) -> f64 {
        match self {
            Self::Walk => WALK_CONNECTOR_SPEED_MPS,
            Self::Drive => DRIVE_CONNECTOR_SPEED_MPS,
        }
    }

    fn max_snap_distance_meters(self) -> f64 {
        match self {
            Self::Walk => WALK_MAX_SNAP_DISTANCE_METERS,
            Self::Drive => DRIVE_MAX_SNAP_DISTANCE_METERS,
        }
    }
}

#[derive(Debug)]
pub struct StreetRoutePath {
    pub duration_seconds: u32,
    pub distance_meters: f64,
    pub polyline: Vec<PolylinePoint>,
    pub segment_way_ids: Vec<Option<i64>>,
    pub way_names: Arc<HashMap<i64, String>>,
    pub source_snap_distance_meters: f64,
    pub destination_snap_distance_meters: f64,
    pub explored_forward_nodes: usize,
    pub explored_backward_nodes: usize,
    pub strategy: &'static str,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct StreetOverlaySnapshot {
    pub mode: &'static str,
    pub enabled: bool,
    pub state_url: Option<String>,
    pub diff_base_url: Option<String>,
    pub poll_interval_secs: Option<u64>,
    pub base_sequence: Option<u64>,
    pub base_timestamp: Option<String>,
    pub applied_sequence: Option<u64>,
    pub applied_timestamp: Option<String>,
    pub last_poll_timestamp: Option<String>,
    pub overlay_nodes: usize,
    pub overlay_edges: usize,
    pub blocked_way_ids: usize,
    pub way_overrides: usize,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Default)]
pub struct StreetRouterOverlaySnapshot {
    pub walk: StreetOverlaySnapshot,
    pub drive: StreetOverlaySnapshot,
}

#[derive(Clone)]
pub struct StreetRouter {
    walk: Arc<StreetGraph>,
    drive: Arc<StreetGraph>,
}

#[derive(Clone)]
struct StreetGraph {
    mode: StreetMode,
    coordinates: Arc<Vec<(f64, f64)>>,
    node_lookup: Arc<HashMap<i64, usize>>,
    node_offsets: Arc<Vec<u32>>,
    forward_edges: Arc<Vec<StreetEdge>>,
    reverse_node_offsets: Arc<Vec<u32>>,
    reverse_edges: Arc<Vec<StreetEdge>>,
    way_names: Arc<HashMap<i64, String>>,
    index: Arc<RTree<IndexedPoint>>,
    overlay_runtime: Arc<ArcSwap<StreetOverlayRuntime>>,
    overlay_path: Arc<PathBuf>,
    base_metadata: StreetGraphCacheMetadata,
    diff_config: Option<HpfDiffConfig>,
    pbf_replication: Option<StreetPbfReplicationAnchor>,
}

#[derive(Clone)]
struct StreetOverlayRuntime {
    enabled: bool,
    state_url: Option<String>,
    diff_base_url: Option<String>,
    poll_interval_secs: Option<u64>,
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    last_poll_timestamp: Option<String>,
    last_error: Option<String>,
    blocked_way_ids: Arc<HashSet<i64>>,
    way_overrides: Arc<HashMap<i64, StreetOverlayWay>>,
    way_names: Arc<HashMap<i64, String>>,
    nodes: Arc<Vec<StreetOverlayNode>>,
    forward_adjacency: Arc<HashMap<usize, Vec<StreetEdge>>>,
    reverse_adjacency: Arc<HashMap<usize, Vec<StreetEdge>>>,
    index: Arc<RTree<IndexedPoint>>,
}

#[derive(Clone)]
struct StreetOverlayDraft {
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    blocked_way_ids: HashSet<i64>,
    way_overrides: HashMap<i64, StreetOverlayWay>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StreetOverlayPersisted {
    magic: [u8; 8],
    metadata: StreetGraphCacheMetadata,
    state_url: Option<String>,
    diff_base_url: Option<String>,
    applied_sequence: Option<u64>,
    applied_timestamp: Option<String>,
    last_poll_timestamp: Option<String>,
    last_error: Option<String>,
    blocked_way_ids: Vec<i64>,
    way_overrides: Vec<StreetOverlayWay>,
}

#[derive(Clone, Serialize, Deserialize)]
struct StreetOverlayWay {
    way_id: i64,
    name: Option<String>,
    node_refs: Vec<i64>,
    coordinates: Vec<(f64, f64)>,
    tags: HashMap<String, String>,
}

#[derive(Clone)]
struct StreetOverlayNode {
    coordinate: (f64, f64),
}

#[derive(Clone)]
struct StreetPbfReplicationAnchor {
    sequence_number: Option<u64>,
    timestamp: Option<String>,
    base_url: Option<String>,
}

struct RemoteStateFile {
    sequence_number: u64,
    timestamp: Option<String>,
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

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct StreetEdge {
    to: u32,
    duration_secs: u32,
    distance_meters: f32,
    way_id: i64,
}

#[derive(Serialize, Deserialize)]
struct StreetGraphCache {
    metadata: StreetGraphCacheMetadata,
    coordinates: Vec<(f64, f64)>,
    node_osm_ids: Vec<i64>,
    node_offsets: Vec<u32>,
    forward_edges: Vec<StreetEdge>,
    reverse_node_offsets: Vec<u32>,
    reverse_edges: Vec<StreetEdge>,
    way_names: HashMap<i64, String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StreetGraphCacheMetadata {
    schema_version: u32,
    mode: String,
    osm_pbf_bytes: u64,
    osm_pbf_modified_unix_secs: Option<u64>,
}

#[derive(Clone, Copy)]
struct SnapResult {
    node_index: usize,
    distance_meters: f64,
}

#[derive(Clone, Copy)]
struct ForwardParent {
    previous: usize,
    way_id: i64,
    distance_meters: f32,
}

#[derive(Clone, Copy)]
struct BackwardParent {
    next: usize,
    way_id: i64,
    distance_meters: f32,
}

#[derive(Clone, Copy)]
struct HeapState {
    node_index: usize,
    cost_secs: u32,
}

impl PartialEq for HeapState {
    fn eq(&self, other: &Self) -> bool {
        self.node_index == other.node_index && self.cost_secs == other.cost_secs
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
            .cost_secs
            .cmp(&self.cost_secs)
            .then_with(|| self.node_index.cmp(&other.node_index))
    }
}

#[derive(Clone)]
struct IndexedPoint {
    index: usize,
    point: [f64; 2],
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

enum TravelDirection {
    Both,
    ForwardOnly,
    ReverseOnly,
}

pub fn build_or_load_street_router(
    osm_pbf_path: &Path,
    cache_dir: &Path,
    diff_config: Option<HpfDiffConfig>,
) -> Result<StreetRouter> {
    let pbf_replication = read_pbf_replication_anchor(osm_pbf_path).ok();
    let walk = Arc::new(build_or_load_graph(
        osm_pbf_path,
        cache_dir,
        StreetMode::Walk,
        diff_config.clone(),
        pbf_replication.clone(),
    )?);
    let drive = Arc::new(build_or_load_graph(
        osm_pbf_path,
        cache_dir,
        StreetMode::Drive,
        diff_config,
        pbf_replication,
    )?);
    Ok(StreetRouter { walk, drive })
}

impl StreetRouter {
    pub fn route(
        &self,
        mode: StreetMode,
        from: (f64, f64),
        to: (f64, f64),
    ) -> Result<StreetRoutePath> {
        match mode {
            StreetMode::Walk => self.walk.route(from, to),
            StreetMode::Drive => self.drive.route(from, to),
        }
    }

    pub fn overlay_snapshot(&self) -> StreetRouterOverlaySnapshot {
        StreetRouterOverlaySnapshot {
            walk: self.walk.overlay_snapshot(),
            drive: self.drive.overlay_snapshot(),
        }
    }

    pub fn poll_remote_updates(&self) -> Result<StreetRouterOverlaySnapshot> {
        let walk = self.walk.poll_remote_updates()?;
        let drive = self.drive.poll_remote_updates()?;
        Ok(StreetRouterOverlaySnapshot { walk, drive })
    }
}

impl StreetGraph {
    fn from_cache(
        mode: StreetMode,
        cache: StreetGraphCache,
        overlay_path: PathBuf,
        diff_config: Option<HpfDiffConfig>,
        pbf_replication: Option<StreetPbfReplicationAnchor>,
    ) -> Self {
        let node_lookup = cache
            .node_osm_ids
            .iter()
            .enumerate()
            .map(|(index, node_id)| (*node_id, index))
            .collect::<HashMap<_, _>>();
        let index = RTree::bulk_load(
            cache
                .coordinates
                .iter()
                .enumerate()
                .map(|(index, (lat, lon))| IndexedPoint {
                    index,
                    point: [*lon, *lat],
                })
                .collect(),
        );

        let graph = Self {
            mode,
            coordinates: Arc::new(cache.coordinates),
            node_lookup: Arc::new(node_lookup),
            node_offsets: Arc::new(cache.node_offsets),
            forward_edges: Arc::new(cache.forward_edges),
            reverse_node_offsets: Arc::new(cache.reverse_node_offsets),
            reverse_edges: Arc::new(cache.reverse_edges),
            way_names: Arc::new(cache.way_names),
            index: Arc::new(index),
            overlay_runtime: Arc::new(ArcSwap::from_pointee(empty_overlay_runtime(
                diff_config.as_ref(),
            ))),
            overlay_path: Arc::new(overlay_path),
            base_metadata: cache.metadata,
            diff_config,
            pbf_replication,
        };
        let overlay_runtime = graph.load_overlay_runtime();
        graph.overlay_runtime.store(Arc::new(overlay_runtime));
        graph
    }

    fn route(&self, from: (f64, f64), to: (f64, f64)) -> Result<StreetRoutePath> {
        let overlay = self.overlay_runtime.load_full();
        let source = self.snap_coordinate(from.0, from.1, overlay.as_ref())?;
        let target = self.snap_coordinate(to.0, to.1, overlay.as_ref())?;

        let (best_cost, meeting, forward_parents, backward_parents, explored_forward_nodes, explored_backward_nodes) = self
            .bidirectional_search(source.node_index, target.node_index, overlay.as_ref())?;
        let (node_path, node_way_ids, graph_distance_meters) = self.reconstruct_path(
            source.node_index,
            target.node_index,
            meeting,
            &forward_parents,
            &backward_parents,
        )?;
        let (polyline, segment_way_ids) = build_street_geometry(
            from,
            to,
            &node_path,
            &node_way_ids,
            |node_index| self.coordinate_for_node(node_index, overlay.as_ref()),
        );

        let connector_distance = source.distance_meters + target.distance_meters;
        let connector_duration = if connector_distance <= f64::EPSILON {
            0
        } else {
            (connector_distance / self.mode.connector_speed_mps()).ceil() as u32
        };

        Ok(StreetRoutePath {
            duration_seconds: best_cost.saturating_add(connector_duration),
            distance_meters: graph_distance_meters + connector_distance,
            polyline,
            segment_way_ids,
            way_names: Arc::new(self.merged_way_names(overlay.as_ref())),
            source_snap_distance_meters: source.distance_meters,
            destination_snap_distance_meters: target.distance_meters,
            explored_forward_nodes,
            explored_backward_nodes,
            strategy: STREET_ROUTING_STRATEGY,
        })
    }

    fn overlay_snapshot(&self) -> StreetOverlaySnapshot {
        let runtime = self.overlay_runtime.load();
        StreetOverlaySnapshot {
            mode: self.mode.as_str(),
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
            overlay_nodes: runtime.nodes.len(),
            overlay_edges: runtime
                .forward_adjacency
                .values()
                .map(Vec::len)
                .sum(),
            blocked_way_ids: runtime.blocked_way_ids.len(),
            way_overrides: runtime.way_overrides.len(),
            last_error: runtime.last_error.clone(),
        }
    }

    fn poll_remote_updates(&self) -> Result<StreetOverlaySnapshot> {
        let Some(diff_config) = self.diff_config.as_ref() else {
            return Ok(self.overlay_snapshot());
        };

        let client = BlockingClient::builder()
            .user_agent("alpha-raptor-engine/0.1")
            .danger_accept_invalid_certs(diff_config.allow_invalid_tls)
            .build()
            .context("failed to build street OSM diff HTTP client")?;

        let runtime = (*self.overlay_runtime.load_full()).clone();
        let poll_timestamp = Utc::now().to_rfc3339();
        let configured_base_url = resolved_diff_base_url(diff_config);
        if let Some(anchor_base_url) = self
            .pbf_replication
            .as_ref()
            .and_then(|anchor| anchor.base_url.as_deref())
        {
            if normalize_diff_base_url(anchor_base_url) != configured_base_url {
                let next_runtime = self.overlay_runtime_from_parts(
                    runtime.blocked_way_ids.iter().copied().collect(),
                    runtime.way_overrides.values().cloned().collect(),
                    runtime.applied_sequence,
                    runtime.applied_timestamp.clone(),
                    Some(poll_timestamp),
                    Some(format!(
                        "street diff base URL mismatch: PBF header expects {anchor_base_url}, config points to {}",
                        diff_config.state_url
                    )),
                );
                self.overlay_runtime.store(Arc::new(next_runtime));
                return Ok(self.overlay_snapshot());
            }
        }

        let state_body = client
            .get(&diff_config.state_url)
            .send()
            .and_then(|response| response.error_for_status())
            .with_context(|| {
                format!(
                    "failed to fetch street OSM diff state from {}",
                    diff_config.state_url
                )
            })?
            .text()
            .context("failed to read street OSM diff state response body")?;
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
            let next_runtime = self.overlay_runtime_from_parts(
                runtime.blocked_way_ids.iter().copied().collect(),
                runtime.way_overrides.values().cloned().collect(),
                Some(applied_sequence),
                runtime
                    .applied_timestamp
                    .clone()
                    .or(remote_state.timestamp.clone()),
                Some(poll_timestamp),
                None,
            );
            if runtime.applied_sequence.is_none() {
                persist_overlay_runtime(&self.overlay_path, &self.base_metadata, &next_runtime)?;
            }
            self.overlay_runtime.store(Arc::new(next_runtime));
            return Ok(self.overlay_snapshot());
        }

        let mut draft = overlay_draft_from_runtime(&runtime);
        let pending_sequences = (remote_state.sequence_number - applied_sequence) as usize;
        for (sequence_offset, sequence) in ((applied_sequence + 1)..=remote_state.sequence_number)
            .enumerate()
        {
            let diff_url = sequence_diff_url(
                runtime
                    .diff_base_url
                    .as_deref()
                    .unwrap_or(&configured_base_url),
                sequence,
            );
            let diff_bytes = client
                .get(&diff_url)
                .send()
                .and_then(|response| response.error_for_status())
                .with_context(|| {
                    format!("failed to fetch street OSM diff {sequence} from {diff_url}")
                })?
                .bytes()
                .with_context(|| format!("failed to read street OSM diff body from {diff_url}"))?;
            let diff = parse_osc_diff(diff_bytes.as_ref())?;
            self.apply_osc_diff(&mut draft, &diff);
            draft.applied_sequence = Some(sequence);
            info!(
                phase = "street-diff-apply",
                mode = self.mode.as_str(),
                progress = %progress_bar(sequence_offset + 1, pending_sequences.max(1)),
                percent = progress_percent(sequence_offset + 1, pending_sequences.max(1)),
                completed = sequence_offset + 1,
                total = pending_sequences.max(1),
                sequence,
                "street diff apply progress"
            );
        }
        draft.applied_timestamp = remote_state.timestamp;

        let next_runtime = self.overlay_runtime_from_draft(&draft, Some(poll_timestamp), None);
        persist_overlay_runtime(&self.overlay_path, &self.base_metadata, &next_runtime)?;
        self.overlay_runtime.store(Arc::new(next_runtime));
        Ok(self.overlay_snapshot())
    }

    fn snap_coordinate(
        &self,
        lat: f64,
        lon: f64,
        overlay: &StreetOverlayRuntime,
    ) -> Result<SnapResult> {
        let mut best = None::<SnapResult>;
        for candidate in self.index.nearest_neighbor_iter(&[lon, lat]).take(SNAP_CANDIDATES) {
            let (candidate_lat, candidate_lon) = self.coordinates[candidate.index];
            let distance_meters = haversine_meters(lat, lon, candidate_lat, candidate_lon);
            match best {
                Some(current) if current.distance_meters <= distance_meters => {}
                _ => {
                    best = Some(SnapResult {
                        node_index: candidate.index,
                        distance_meters,
                    })
                }
            }
        }

        for candidate in overlay.index.nearest_neighbor_iter(&[lon, lat]).take(SNAP_CANDIDATES) {
            let (candidate_lat, candidate_lon) = self.coordinate_for_node(candidate.index, overlay);
            let distance_meters = haversine_meters(lat, lon, candidate_lat, candidate_lon);
            match best {
                Some(current) if current.distance_meters <= distance_meters => {}
                _ => {
                    best = Some(SnapResult {
                        node_index: candidate.index,
                        distance_meters,
                    })
                }
            }
        }

        let best = best.ok_or_else(|| anyhow!("{} street graph is empty", self.mode.as_str()))?;
        if best.distance_meters > self.mode.max_snap_distance_meters() {
            bail!(
                "no {} street node found within {:.0} meters of the requested coordinate",
                self.mode.as_str(),
                self.mode.max_snap_distance_meters()
            );
        }
        Ok(best)
    }

    fn bidirectional_search(
        &self,
        source: usize,
        target: usize,
        overlay: &StreetOverlayRuntime,
    ) -> Result<(
        u32,
        usize,
        HashMap<usize, ForwardParent>,
        HashMap<usize, BackwardParent>,
        usize,
        usize,
    )> {
        if source == target {
            return Ok((0, source, HashMap::new(), HashMap::new(), 1, 1));
        }

        let mut forward_distances = HashMap::<usize, u32>::new();
        let mut backward_distances = HashMap::<usize, u32>::new();
        let mut forward_parents = HashMap::<usize, ForwardParent>::new();
        let mut backward_parents = HashMap::<usize, BackwardParent>::new();
        let mut forward_heap = BinaryHeap::<HeapState>::new();
        let mut backward_heap = BinaryHeap::<HeapState>::new();
        let mut best_cost = u32::MAX;
        let mut meeting = None::<usize>;
        let mut explored_forward_nodes = 0usize;
        let mut explored_backward_nodes = 0usize;

        forward_distances.insert(source, 0);
        backward_distances.insert(target, 0);
        forward_heap.push(HeapState {
            node_index: source,
            cost_secs: 0,
        });
        backward_heap.push(HeapState {
            node_index: target,
            cost_secs: 0,
        });

        while !forward_heap.is_empty() || !backward_heap.is_empty() {
            let next_forward = forward_heap.peek().map(|state| state.cost_secs).unwrap_or(u32::MAX);
            let next_backward = backward_heap.peek().map(|state| state.cost_secs).unwrap_or(u32::MAX);
            if next_forward.saturating_add(next_backward) >= best_cost {
                break;
            }

            if next_forward <= next_backward {
                let Some(state) = forward_heap.pop() else {
                    continue;
                };
                let Some(&known_cost) = forward_distances.get(&state.node_index) else {
                    continue;
                };
                if state.cost_secs > known_cost {
                    continue;
                }

                explored_forward_nodes += 1;
                if let Some(&other_cost) = backward_distances.get(&state.node_index) {
                    let total_cost = state.cost_secs.saturating_add(other_cost);
                    if total_cost < best_cost {
                        best_cost = total_cost;
                        meeting = Some(state.node_index);
                    }
                }

                self.for_each_forward_edge(overlay, state.node_index, |edge| {
                    let candidate_cost = state.cost_secs.saturating_add(edge.duration_secs);
                    if candidate_cost >= best_cost {
                        return;
                    }
                    let target_index = edge.to as usize;
                    let should_update = match forward_distances.get(&target_index) {
                        Some(&current_cost) => candidate_cost < current_cost,
                        None => true,
                    };
                    if should_update {
                        forward_distances.insert(target_index, candidate_cost);
                        forward_parents.insert(
                            target_index,
                            ForwardParent {
                                previous: state.node_index,
                                way_id: edge.way_id,
                                distance_meters: edge.distance_meters,
                            },
                        );
                        forward_heap.push(HeapState {
                            node_index: target_index,
                            cost_secs: candidate_cost,
                        });
                    }
                });
            } else {
                let Some(state) = backward_heap.pop() else {
                    continue;
                };
                let Some(&known_cost) = backward_distances.get(&state.node_index) else {
                    continue;
                };
                if state.cost_secs > known_cost {
                    continue;
                }

                explored_backward_nodes += 1;
                if let Some(&other_cost) = forward_distances.get(&state.node_index) {
                    let total_cost = state.cost_secs.saturating_add(other_cost);
                    if total_cost < best_cost {
                        best_cost = total_cost;
                        meeting = Some(state.node_index);
                    }
                }

                self.for_each_reverse_edge(overlay, state.node_index, |edge| {
                    let candidate_cost = state.cost_secs.saturating_add(edge.duration_secs);
                    if candidate_cost >= best_cost {
                        return;
                    }
                    let target_index = edge.to as usize;
                    let should_update = match backward_distances.get(&target_index) {
                        Some(&current_cost) => candidate_cost < current_cost,
                        None => true,
                    };
                    if should_update {
                        backward_distances.insert(target_index, candidate_cost);
                        backward_parents.insert(
                            target_index,
                            BackwardParent {
                                next: state.node_index,
                                way_id: edge.way_id,
                                distance_meters: edge.distance_meters,
                            },
                        );
                        backward_heap.push(HeapState {
                            node_index: target_index,
                            cost_secs: candidate_cost,
                        });
                    }
                });
            }
        }

        let meeting = meeting.ok_or_else(|| {
            anyhow!(
                "no {} route found between the requested coordinates",
                self.mode.as_str()
            )
        })?;

        Ok((
            best_cost,
            meeting,
            forward_parents,
            backward_parents,
            explored_forward_nodes,
            explored_backward_nodes,
        ))
    }

    fn reconstruct_path(
        &self,
        source: usize,
        target: usize,
        meeting: usize,
        forward_parents: &HashMap<usize, ForwardParent>,
        backward_parents: &HashMap<usize, BackwardParent>,
    ) -> Result<(Vec<usize>, Vec<Option<i64>>, f64)> {
        let mut path_nodes = vec![meeting];
        let mut path_way_ids = Vec::<Option<i64>>::new();
        let mut graph_distance_meters = 0.0;
        let mut cursor = meeting;

        while cursor != source {
            let parent = forward_parents
                .get(&cursor)
                .copied()
                .ok_or_else(|| anyhow!("incomplete forward street path reconstruction"))?;
            path_nodes.push(parent.previous);
            path_way_ids.push(Some(parent.way_id));
            graph_distance_meters += parent.distance_meters as f64;
            cursor = parent.previous;
        }
        path_nodes.reverse();
        path_way_ids.reverse();

        cursor = meeting;
        while cursor != target {
            let parent = backward_parents
                .get(&cursor)
                .copied()
                .ok_or_else(|| anyhow!("incomplete backward street path reconstruction"))?;
            path_nodes.push(parent.next);
            path_way_ids.push(Some(parent.way_id));
            graph_distance_meters += parent.distance_meters as f64;
            cursor = parent.next;
        }

        Ok((path_nodes, path_way_ids, graph_distance_meters))
    }

    fn for_each_forward_edge(
        &self,
        overlay: &StreetOverlayRuntime,
        node_index: usize,
        mut apply: impl FnMut(&StreetEdge),
    ) {
        if node_index < self.coordinates.len() {
            for edge in edge_slice(node_index, &self.node_offsets, &self.forward_edges) {
                if !overlay.blocked_way_ids.contains(&edge.way_id) {
                    apply(edge);
                }
            }
        }
        if let Some(edges) = overlay.forward_adjacency.get(&node_index) {
            for edge in edges {
                apply(edge);
            }
        }
    }

    fn for_each_reverse_edge(
        &self,
        overlay: &StreetOverlayRuntime,
        node_index: usize,
        mut apply: impl FnMut(&StreetEdge),
    ) {
        if node_index < self.coordinates.len() {
            for edge in edge_slice(node_index, &self.reverse_node_offsets, &self.reverse_edges) {
                if !overlay.blocked_way_ids.contains(&edge.way_id) {
                    apply(edge);
                }
            }
        }
        if let Some(edges) = overlay.reverse_adjacency.get(&node_index) {
            for edge in edges {
                apply(edge);
            }
        }
    }

    fn coordinate_for_node(&self, node_index: usize, overlay: &StreetOverlayRuntime) -> (f64, f64) {
        if node_index < self.coordinates.len() {
            self.coordinates[node_index]
        } else {
            overlay
                .nodes
                .get(node_index.saturating_sub(self.coordinates.len()))
                .map(|node| node.coordinate)
                .unwrap_or_else(|| self.coordinates.last().copied().unwrap_or((0.0, 0.0)))
        }
    }

    fn merged_way_names(&self, overlay: &StreetOverlayRuntime) -> HashMap<i64, String> {
        let mut names = self.way_names.as_ref().clone();
        names.extend(
            overlay
                .way_names
                .iter()
                .map(|(way_id, name)| (*way_id, name.clone())),
        );
        names
    }

    fn apply_osc_diff(&self, draft: &mut StreetOverlayDraft, diff: &OscDiff) {
        for change in &diff.ways {
            draft.blocked_way_ids.insert(change.way_id);
            draft.way_overrides.remove(&change.way_id);

            if change.action == OscAction::Delete {
                continue;
            }
            if !matches_mode_osc_way(self.mode, change) {
                continue;
            }
            if change.coordinates.len() < 2 || change.node_refs.len() < 2 {
                continue;
            }

            draft.way_overrides.insert(
                change.way_id,
                StreetOverlayWay {
                    way_id: change.way_id,
                    name: change
                        .tags
                        .get("name")
                        .map(|value| value.trim())
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned),
                    node_refs: change.node_refs.clone(),
                    coordinates: change.coordinates.clone(),
                    tags: change.tags.clone(),
                },
            );
        }
    }

    fn load_overlay_runtime(&self) -> StreetOverlayRuntime {
        if self.diff_config.is_none() {
            return empty_overlay_runtime(None);
        }

        match load_overlay_persisted(&self.overlay_path, &self.base_metadata) {
            Ok(Some(persisted)) => self.overlay_runtime_from_parts(
                persisted.blocked_way_ids,
                persisted.way_overrides,
                persisted.applied_sequence,
                persisted.applied_timestamp,
                persisted.last_poll_timestamp,
                persisted.last_error,
            ),
            Ok(None) => empty_overlay_runtime(self.diff_config.as_ref()),
            Err(error) => {
                warn!(
                    %error,
                    overlay = %self.overlay_path.display(),
                    mode = self.mode.as_str(),
                    "failed to load street overlay state; starting from empty differential overlay"
                );
                let mut runtime = empty_overlay_runtime(self.diff_config.as_ref());
                runtime.last_error = Some(error.to_string());
                runtime
            }
        }
    }

    fn overlay_runtime_from_draft(
        &self,
        draft: &StreetOverlayDraft,
        last_poll_timestamp: Option<String>,
        last_error: Option<String>,
    ) -> StreetOverlayRuntime {
        self.overlay_runtime_from_parts(
            draft.blocked_way_ids.iter().copied().collect(),
            draft.way_overrides.values().cloned().collect(),
            draft.applied_sequence,
            draft.applied_timestamp.clone(),
            last_poll_timestamp,
            last_error,
        )
    }

    fn overlay_runtime_from_parts(
        &self,
        blocked_way_ids: Vec<i64>,
        way_overrides: Vec<StreetOverlayWay>,
        applied_sequence: Option<u64>,
        applied_timestamp: Option<String>,
        last_poll_timestamp: Option<String>,
        last_error: Option<String>,
    ) -> StreetOverlayRuntime {
        let blocked_way_ids = blocked_way_ids.into_iter().collect::<HashSet<_>>();
        let mut way_override_map = way_overrides
            .into_iter()
            .map(|entry| (entry.way_id, entry))
            .collect::<HashMap<_, _>>();
        let (nodes, forward_adjacency, reverse_adjacency, way_names) =
            self.build_overlay_graph(&way_override_map);
        way_override_map.retain(|_, entry| entry.coordinates.len() >= 2 && entry.node_refs.len() >= 2);
        let index = RTree::bulk_load(
            nodes
                .iter()
                .enumerate()
                .map(|(offset, node)| IndexedPoint {
                    index: self.coordinates.len() + offset,
                    point: [node.coordinate.1, node.coordinate.0],
                })
                .collect(),
        );

        StreetOverlayRuntime {
            enabled: self.diff_config.is_some(),
            state_url: self.diff_config.as_ref().map(|config| config.state_url.clone()),
            diff_base_url: self
                .diff_config
                .as_ref()
                .map(resolved_diff_base_url),
            poll_interval_secs: self
                .diff_config
                .as_ref()
                .map(|config| config.poll_interval_secs),
            applied_sequence,
            applied_timestamp,
            last_poll_timestamp,
            last_error,
            blocked_way_ids: Arc::new(blocked_way_ids),
            way_overrides: Arc::new(way_override_map),
            way_names: Arc::new(way_names),
            nodes: Arc::new(nodes),
            forward_adjacency: Arc::new(forward_adjacency),
            reverse_adjacency: Arc::new(reverse_adjacency),
            index: Arc::new(index),
        }
    }

    fn build_overlay_graph(
        &self,
        way_overrides: &HashMap<i64, StreetOverlayWay>,
    ) -> (
        Vec<StreetOverlayNode>,
        HashMap<usize, Vec<StreetEdge>>,
        HashMap<usize, Vec<StreetEdge>>,
        HashMap<i64, String>,
    ) {
        let mut nodes = Vec::<StreetOverlayNode>::new();
        let mut overlay_lookup = HashMap::<i64, usize>::new();
        let mut forward_adjacency = HashMap::<usize, Vec<StreetEdge>>::new();
        let mut reverse_adjacency = HashMap::<usize, Vec<StreetEdge>>::new();
        let mut way_names = HashMap::<i64, String>::new();

        for way in way_overrides.values() {
            if let Some(name) = way.name.as_ref() {
                way_names.insert(way.way_id, name.clone());
            }

            let direction = way_direction_for_tags(self.mode, |key| way.tags.get(key).cloned());
            let speed_mps = way_speed_mps_for_tags(self.mode, |key| way.tags.get(key).cloned());
            let window_len = way.node_refs.len().min(way.coordinates.len());
            if window_len < 2 {
                continue;
            }

            for index in 0..(window_len - 1) {
                let from_index = self.overlay_node_index_for(
                    way.node_refs[index],
                    way.coordinates[index],
                    &mut overlay_lookup,
                    &mut nodes,
                );
                let to_index = self.overlay_node_index_for(
                    way.node_refs[index + 1],
                    way.coordinates[index + 1],
                    &mut overlay_lookup,
                    &mut nodes,
                );
                if from_index == to_index {
                    continue;
                }

                let distance_meters = haversine_meters(
                    way.coordinates[index].0,
                    way.coordinates[index].1,
                    way.coordinates[index + 1].0,
                    way.coordinates[index + 1].1,
                );
                if !(distance_meters.is_finite()) || distance_meters <= 0.0 {
                    continue;
                }

                let duration_secs = (distance_meters / speed_mps).ceil().max(1.0) as u32;
                match direction {
                    TravelDirection::Both => {
                        push_overlay_directed_edge(
                            &mut forward_adjacency,
                            &mut reverse_adjacency,
                            from_index,
                            to_index,
                            duration_secs,
                            distance_meters,
                            way.way_id,
                        );
                        push_overlay_directed_edge(
                            &mut forward_adjacency,
                            &mut reverse_adjacency,
                            to_index,
                            from_index,
                            duration_secs,
                            distance_meters,
                            way.way_id,
                        );
                    }
                    TravelDirection::ForwardOnly => {
                        push_overlay_directed_edge(
                            &mut forward_adjacency,
                            &mut reverse_adjacency,
                            from_index,
                            to_index,
                            duration_secs,
                            distance_meters,
                            way.way_id,
                        );
                    }
                    TravelDirection::ReverseOnly => {
                        push_overlay_directed_edge(
                            &mut forward_adjacency,
                            &mut reverse_adjacency,
                            to_index,
                            from_index,
                            duration_secs,
                            distance_meters,
                            way.way_id,
                        );
                    }
                }
            }
        }

        (nodes, forward_adjacency, reverse_adjacency, way_names)
    }

    fn overlay_node_index_for(
        &self,
        node_ref: i64,
        coordinate: (f64, f64),
        overlay_lookup: &mut HashMap<i64, usize>,
        nodes: &mut Vec<StreetOverlayNode>,
    ) -> usize {
        if let Some(index) = self.node_lookup.get(&node_ref).copied() {
            return index;
        }
        if let Some(index) = overlay_lookup.get(&node_ref).copied() {
            return index;
        }

        let index = self.coordinates.len() + nodes.len();
        nodes.push(StreetOverlayNode { coordinate });
        overlay_lookup.insert(node_ref, index);
        index
    }
}

fn build_or_load_graph(
    osm_pbf_path: &Path,
    cache_dir: &Path,
    mode: StreetMode,
    diff_config: Option<HpfDiffConfig>,
    pbf_replication: Option<StreetPbfReplicationAnchor>,
) -> Result<StreetGraph> {
    let metadata = build_cache_metadata(osm_pbf_path, mode)?;
    let cache_path = street_cache_path(cache_dir, osm_pbf_path, mode);
    let overlay_path = street_overlay_path(cache_dir, osm_pbf_path, mode);

    if let Some(cache) = load_cache(&cache_path, &metadata)? {
        info!(
            mode = mode.as_str(),
            cache = %cache_path.display(),
            graph_nodes = cache.coordinates.len(),
            graph_edges = cache.forward_edges.len(),
            "loaded street graph from cache"
        );
        return Ok(StreetGraph::from_cache(
            mode,
            cache,
            overlay_path,
            diff_config,
            pbf_replication,
        ));
    }

    let file = File::open(osm_pbf_path)
        .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?;
    let mut pbf = OsmPbfReader::new(BufReader::new(file));
    let objects = pbf
        .get_objs_and_deps(|obj| matches!(obj, OsmObj::Way(way) if matches_mode_way(mode, way)))
        .context("failed to extract OSM ways for street routing")?;

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

    let cache = build_graph_cache(mode, &metadata, &node_coordinates, &ways)?;
    store_cache(&cache_path, &cache)?;
    Ok(StreetGraph::from_cache(
        mode,
        cache,
        overlay_path,
        diff_config,
        pbf_replication,
    ))
}

fn build_graph_cache(
    mode: StreetMode,
    metadata: &StreetGraphCacheMetadata,
    node_coordinates: &HashMap<NodeId, (f64, f64)>,
    ways: &[Way],
) -> Result<StreetGraphCache> {
    info!(mode = mode.as_str(), "building street graph from OSM PBF");

    let mut coordinates = Vec::<(f64, f64)>::new();
    let mut node_osm_ids = Vec::<i64>::new();
    let mut forward_adjacency = Vec::<Vec<StreetEdge>>::new();
    let mut reverse_adjacency = Vec::<Vec<StreetEdge>>::new();
    let mut node_lookup = HashMap::<NodeId, usize>::new();
    let mut way_names = HashMap::<i64, String>::new();

    let total_ways = ways.len().max(1);
    for (way_index, way) in ways.iter().enumerate() {
        if let Some(name) = way.tags.get("name").map(|value| value.trim()).filter(|value| !value.is_empty()) {
            way_names.insert(way.id.0, name.to_owned());
        }

        let direction = way_direction(mode, way);
        let speed_mps = way_speed_mps(mode, way);

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
                &mut coordinates,
                &mut node_osm_ids,
                &mut forward_adjacency,
                &mut reverse_adjacency,
            );
            let to_index = graph_index_for(
                to_id,
                (to_lat, to_lon),
                &mut node_lookup,
                &mut coordinates,
                &mut node_osm_ids,
                &mut forward_adjacency,
                &mut reverse_adjacency,
            );

            if from_index == to_index {
                continue;
            }

            let distance_meters = haversine_meters(from_lat, from_lon, to_lat, to_lon);
            if !(distance_meters.is_finite()) || distance_meters <= 0.0 {
                continue;
            }

            let duration_secs = (distance_meters / speed_mps).ceil().max(1.0) as u32;

            match direction {
                TravelDirection::Both => {
                    push_directed_edge(
                        &mut forward_adjacency,
                        &mut reverse_adjacency,
                        from_index,
                        to_index,
                        duration_secs,
                        distance_meters,
                        way.id.0,
                    )?;
                    push_directed_edge(
                        &mut forward_adjacency,
                        &mut reverse_adjacency,
                        to_index,
                        from_index,
                        duration_secs,
                        distance_meters,
                        way.id.0,
                    )?;
                }
                TravelDirection::ForwardOnly => {
                    push_directed_edge(
                        &mut forward_adjacency,
                        &mut reverse_adjacency,
                        from_index,
                        to_index,
                        duration_secs,
                        distance_meters,
                        way.id.0,
                    )?;
                }
                TravelDirection::ReverseOnly => {
                    push_directed_edge(
                        &mut forward_adjacency,
                        &mut reverse_adjacency,
                        to_index,
                        from_index,
                        duration_secs,
                        distance_meters,
                        way.id.0,
                    )?;
                }
            }
        }

        let completed = way_index + 1;
        if completed % 2048 == 0 || completed == ways.len() {
            info!(
                phase = "street-graph-build",
                mode = mode.as_str(),
                progress = %progress_bar(completed, total_ways),
                percent = progress_percent(completed, total_ways),
                completed,
                total = total_ways,
                "street graph build progress"
            );
        }
    }

    if coordinates.is_empty() {
        bail!("OSM street graph is empty after filtering {} ways", mode.as_str());
    }

    let (node_offsets, forward_edges) = flatten_edges(forward_adjacency)?;
    let (reverse_node_offsets, reverse_edges) = flatten_edges(reverse_adjacency)?;
    info!(
        mode = mode.as_str(),
        graph_nodes = coordinates.len(),
        graph_edges = forward_edges.len(),
        "constructed street graph"
    );

    Ok(StreetGraphCache {
        metadata: metadata.clone(),
        coordinates,
        node_osm_ids,
        node_offsets,
        forward_edges,
        reverse_node_offsets,
        reverse_edges,
        way_names,
    })
}

fn graph_index_for(
    node_id: NodeId,
    coordinate: (f64, f64),
    node_lookup: &mut HashMap<NodeId, usize>,
    coordinates: &mut Vec<(f64, f64)>,
    node_osm_ids: &mut Vec<i64>,
    forward_adjacency: &mut Vec<Vec<StreetEdge>>,
    reverse_adjacency: &mut Vec<Vec<StreetEdge>>,
) -> usize {
    if let Some(index) = node_lookup.get(&node_id).copied() {
        return index;
    }

    let index = coordinates.len();
    coordinates.push(coordinate);
    node_osm_ids.push(node_id.0);
    forward_adjacency.push(Vec::new());
    reverse_adjacency.push(Vec::new());
    node_lookup.insert(node_id, index);
    index
}

fn push_directed_edge(
    forward_adjacency: &mut [Vec<StreetEdge>],
    reverse_adjacency: &mut [Vec<StreetEdge>],
    from_index: usize,
    to_index: usize,
    duration_secs: u32,
    distance_meters: f64,
    way_id: i64,
) -> Result<()> {
    let to_index_u32 = u32::try_from(to_index).context("street graph node index exceeds u32")?;
    let from_index_u32 =
        u32::try_from(from_index).context("street graph node index exceeds u32")?;
    let edge = StreetEdge {
        to: to_index_u32,
        duration_secs,
        distance_meters: distance_meters as f32,
        way_id,
    };
    let reverse_edge = StreetEdge {
        to: from_index_u32,
        duration_secs,
        distance_meters: distance_meters as f32,
        way_id,
    };
    forward_adjacency[from_index].push(edge);
    reverse_adjacency[to_index].push(reverse_edge);
    Ok(())
}

fn push_overlay_directed_edge(
    forward_adjacency: &mut HashMap<usize, Vec<StreetEdge>>,
    reverse_adjacency: &mut HashMap<usize, Vec<StreetEdge>>,
    from_index: usize,
    to_index: usize,
    duration_secs: u32,
    distance_meters: f64,
    way_id: i64,
) {
    let (Ok(to_index_u32), Ok(from_index_u32)) = (u32::try_from(to_index), u32::try_from(from_index)) else {
        return;
    };
    forward_adjacency.entry(from_index).or_default().push(StreetEdge {
        to: to_index_u32,
        duration_secs,
        distance_meters: distance_meters as f32,
        way_id,
    });
    reverse_adjacency.entry(to_index).or_default().push(StreetEdge {
        to: from_index_u32,
        duration_secs,
        distance_meters: distance_meters as f32,
        way_id,
    });
}

fn flatten_edges(adjacency: Vec<Vec<StreetEdge>>) -> Result<(Vec<u32>, Vec<StreetEdge>)> {
    let mut offsets = Vec::with_capacity(adjacency.len() + 1);
    let mut flat_edges = Vec::<StreetEdge>::new();
    offsets.push(0);
    for edges in adjacency {
        flat_edges.extend(edges);
        offsets.push(u32::try_from(flat_edges.len()).context("street edge slab exceeds u32")?);
    }
    Ok((offsets, flat_edges))
}

fn edge_slice<'a>(node_index: usize, offsets: &'a [u32], edges: &'a [StreetEdge]) -> &'a [StreetEdge] {
    let start = offsets.get(node_index).copied().unwrap_or(0) as usize;
    let end = offsets.get(node_index + 1).copied().unwrap_or(start as u32) as usize;
    &edges[start..end]
}

fn build_street_geometry(
    from: (f64, f64),
    to: (f64, f64),
    node_path: &[usize],
    node_way_ids: &[Option<i64>],
    mut coordinate_for_node: impl FnMut(usize) -> (f64, f64),
) -> (Vec<PolylinePoint>, Vec<Option<i64>>) {
    let mut polyline = Vec::<PolylinePoint>::new();
    let mut segment_way_ids = Vec::<Option<i64>>::new();
    push_geometry_point(&mut polyline, &mut segment_way_ids, from.0, from.1, None);

    for (index, node_index) in node_path.iter().copied().enumerate() {
        let (lat, lon) = coordinate_for_node(node_index);
        let incoming_way_id = if index == 0 {
            None
        } else {
            node_way_ids.get(index - 1).copied().flatten()
        };
        push_geometry_point(
            &mut polyline,
            &mut segment_way_ids,
            lat,
            lon,
            incoming_way_id,
        );
    }

    push_geometry_point(&mut polyline, &mut segment_way_ids, to.0, to.1, None);

    if polyline.len() >= 2 {
        (polyline, segment_way_ids)
    } else {
        let polyline = vec![
            PolylinePoint { lat: from.0, lon: from.1 },
            PolylinePoint { lat: to.0, lon: to.1 },
        ];
        (polyline.clone(), empty_segment_way_ids(&polyline))
    }
}

fn push_geometry_point(
    polyline: &mut Vec<PolylinePoint>,
    segment_way_ids: &mut Vec<Option<i64>>,
    lat: f64,
    lon: f64,
    incoming_way_id: Option<i64>,
) {
    let should_push = match polyline.last() {
        Some(last) => (last.lat - lat).abs() > 1e-7 || (last.lon - lon).abs() > 1e-7,
        None => true,
    };
    if should_push {
        if !polyline.is_empty() {
            segment_way_ids.push(incoming_way_id);
        }
        polyline.push(PolylinePoint { lat, lon });
    }
}

fn empty_segment_way_ids(polyline: &[PolylinePoint]) -> Vec<Option<i64>> {
    vec![None; polyline.len().saturating_sub(1)]
}

fn build_cache_metadata(osm_pbf_path: &Path, mode: StreetMode) -> Result<StreetGraphCacheMetadata> {
    let metadata = fs::metadata(osm_pbf_path).with_context(|| {
        format!(
            "unable to read OSM PBF metadata at {}",
            osm_pbf_path.display()
        )
    })?;

    Ok(StreetGraphCacheMetadata {
        schema_version: STREET_GRAPH_SCHEMA_VERSION,
        mode: mode.as_str().to_owned(),
        osm_pbf_bytes: metadata.len(),
        osm_pbf_modified_unix_secs: metadata
            .modified()
            .ok()
            .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
            .map(|value| value.as_secs()),
    })
}

fn load_cache(
    cache_path: &Path,
    metadata: &StreetGraphCacheMetadata,
) -> Result<Option<StreetGraphCache>> {
    if !cache_path.exists() {
        return Ok(None);
    }

    let file = File::open(cache_path)
        .with_context(|| format!("unable to open street cache {}", cache_path.display()))?;
    let cache: StreetGraphCache = bincode::deserialize_from(BufReader::new(file))
        .with_context(|| format!("unable to deserialize street cache {}", cache_path.display()))?;
    if cache.metadata == *metadata {
        Ok(Some(cache))
    } else {
        info!(
            cache = %cache_path.display(),
            mode = metadata.mode,
            "street cache metadata mismatch, rebuilding"
        );
        Ok(None)
    }
}

fn store_cache(cache_path: &Path, cache: &StreetGraphCache) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("unable to create street cache directory {}", parent.display())
        })?;
    }
    let file = File::create(cache_path)
        .with_context(|| format!("unable to create street cache {}", cache_path.display()))?;
    bincode::serialize_into(BufWriter::new(file), cache)
        .with_context(|| format!("unable to serialize street cache {}", cache_path.display()))
}

fn load_overlay_persisted(
    overlay_path: &Path,
    metadata: &StreetGraphCacheMetadata,
) -> Result<Option<StreetOverlayPersisted>> {
    if !overlay_path.exists() {
        return Ok(None);
    }

    let file = File::open(overlay_path)
        .with_context(|| format!("unable to open street overlay {}", overlay_path.display()))?;
    let persisted: StreetOverlayPersisted = bincode::deserialize_from(BufReader::new(file))
        .with_context(|| format!("unable to deserialize street overlay {}", overlay_path.display()))?;
    if &persisted.magic != STREET_OVERLAY_MAGIC {
        return Ok(None);
    }
    if persisted.metadata != *metadata {
        return Ok(None);
    }
    Ok(Some(persisted))
}

fn persist_overlay_runtime(
    overlay_path: &Path,
    metadata: &StreetGraphCacheMetadata,
    runtime: &StreetOverlayRuntime,
) -> Result<()> {
    if let Some(parent) = overlay_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "unable to create street overlay directory {}",
                parent.display()
            )
        })?;
    }

    let mut blocked_way_ids = runtime.blocked_way_ids.iter().copied().collect::<Vec<_>>();
    blocked_way_ids.sort_unstable();
    let mut way_overrides = runtime
        .way_overrides
        .values()
        .cloned()
        .collect::<Vec<_>>();
    way_overrides.sort_by_key(|entry| entry.way_id);
    let persisted = StreetOverlayPersisted {
        magic: *STREET_OVERLAY_MAGIC,
        metadata: metadata.clone(),
        state_url: runtime.state_url.clone(),
        diff_base_url: runtime.diff_base_url.clone(),
        applied_sequence: runtime.applied_sequence,
        applied_timestamp: runtime.applied_timestamp.clone(),
        last_poll_timestamp: runtime.last_poll_timestamp.clone(),
        last_error: runtime.last_error.clone(),
        blocked_way_ids,
        way_overrides,
    };

    let tmp_path = overlay_path.with_extension("overlay.tmp");
    let file = File::create(&tmp_path).with_context(|| {
        format!(
            "unable to create street overlay temp file {}",
            tmp_path.display()
        )
    })?;
    bincode::serialize_into(BufWriter::new(file), &persisted)
        .context("failed to serialize street overlay state")?;
    if overlay_path.exists() {
        fs::remove_file(overlay_path).with_context(|| {
            format!("unable to replace street overlay {}", overlay_path.display())
        })?;
    }
    fs::rename(&tmp_path, overlay_path).with_context(|| {
        format!("unable to move street overlay into {}", overlay_path.display())
    })
}

fn street_cache_path(cache_dir: &Path, osm_pbf_path: &Path, mode: StreetMode) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-street");
    cache_dir.join(format!("{stem}.street-{}.bin", mode.as_str()))
}

fn street_overlay_path(cache_dir: &Path, osm_pbf_path: &Path, mode: StreetMode) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-street");
    cache_dir.join(format!("{stem}.street-{}.overlay.bin", mode.as_str()))
}

fn overlay_draft_from_runtime(runtime: &StreetOverlayRuntime) -> StreetOverlayDraft {
    StreetOverlayDraft {
        applied_sequence: runtime.applied_sequence,
        applied_timestamp: runtime.applied_timestamp.clone(),
        blocked_way_ids: runtime.blocked_way_ids.iter().copied().collect(),
        way_overrides: runtime
            .way_overrides
            .iter()
            .map(|(way_id, entry)| (*way_id, entry.clone()))
            .collect(),
    }
}

fn empty_overlay_runtime(diff_config: Option<&HpfDiffConfig>) -> StreetOverlayRuntime {
    StreetOverlayRuntime {
        enabled: diff_config.is_some(),
        state_url: diff_config.map(|config| config.state_url.clone()),
        diff_base_url: diff_config.map(resolved_diff_base_url),
        poll_interval_secs: diff_config.map(|config| config.poll_interval_secs),
        applied_sequence: None,
        applied_timestamp: None,
        last_poll_timestamp: None,
        last_error: None,
        blocked_way_ids: Arc::new(HashSet::new()),
        way_overrides: Arc::new(HashMap::new()),
        way_names: Arc::new(HashMap::new()),
        nodes: Arc::new(Vec::new()),
        forward_adjacency: Arc::new(HashMap::new()),
        reverse_adjacency: Arc::new(HashMap::new()),
        index: Arc::new(RTree::bulk_load(Vec::new())),
    }
}

fn matches_mode_way(mode: StreetMode, way: &Way) -> bool {
    let mut get = |key: &str| way.tags.get(key).map(|value| value.to_string());
    match mode {
        StreetMode::Walk => is_walkable_tags(&mut get),
        StreetMode::Drive => is_driveable_tags(&mut get),
    }
}

fn matches_mode_osc_way(mode: StreetMode, way: &OscWayChange) -> bool {
    let mut get = |key: &str| way.tags.get(key).cloned();
    match mode {
        StreetMode::Walk => is_walkable_tags(&mut get),
        StreetMode::Drive => is_driveable_tags(&mut get),
    }
}

fn way_speed_mps(mode: StreetMode, way: &Way) -> f64 {
    let mut get = |key: &str| way.tags.get(key).map(|value| value.to_string());
    way_speed_mps_for_tags(mode, &mut get)
}

fn way_speed_mps_for_tags(
    mode: StreetMode,
    mut get: impl FnMut(&str) -> Option<String>,
) -> f64 {
    way_speed_mps_from_lookup(mode, &mut get)
}

fn way_speed_mps_from_lookup(
    mode: StreetMode,
    get: &mut impl FnMut(&str) -> Option<String>,
) -> f64 {
    match mode {
        StreetMode::Walk => WALK_CONNECTOR_SPEED_MPS,
        StreetMode::Drive => drive_speed_mps_for_tags(get),
    }
}

fn way_direction(mode: StreetMode, way: &Way) -> TravelDirection {
    let mut get = |key: &str| way.tags.get(key).map(|value| value.to_string());
    way_direction_for_tags(mode, &mut get)
}

fn way_direction_for_tags(
    mode: StreetMode,
    mut get: impl FnMut(&str) -> Option<String>,
) -> TravelDirection {
    way_direction_from_lookup(mode, &mut get)
}

fn way_direction_from_lookup(
    mode: StreetMode,
    get: &mut impl FnMut(&str) -> Option<String>,
) -> TravelDirection {
    if mode == StreetMode::Walk {
        return TravelDirection::Both;
    }

    if get("junction").as_deref().is_some_and(|value| value == "roundabout") {
        return TravelDirection::ForwardOnly;
    }

    match get("oneway").as_deref() {
        Some("-1") => TravelDirection::ReverseOnly,
        Some("yes" | "true" | "1") => TravelDirection::ForwardOnly,
        _ => TravelDirection::Both,
    }
}

fn is_walkable_tags(get: &mut impl FnMut(&str) -> Option<String>) -> bool {
    if get("area").as_deref().is_some_and(|value| value == "yes") {
        return false;
    }
    if matches!(get("access").as_deref(), Some("no" | "private")) {
        return false;
    }
    if matches!(get("foot").as_deref(), Some("no" | "private" | "use_sidepath")) {
        return false;
    }
    if matches!(get("pedestrian").as_deref(), Some("no")) {
        return false;
    }

    if let Some(highway) = get("highway") {
        return !matches!(
            highway.as_str(),
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

    get("railway").as_deref().is_some_and(|value| value == "platform")
        || get("public_transport")
            .as_deref()
            .is_some_and(|value| value == "platform")
}

fn is_driveable_tags(get: &mut impl FnMut(&str) -> Option<String>) -> bool {
    if get("area").as_deref().is_some_and(|value| value == "yes") {
        return false;
    }
    if matches!(get("access").as_deref(), Some("no" | "private")) {
        return false;
    }
    if matches!(get("motor_vehicle").as_deref(), Some("no" | "private")) {
        return false;
    }
    if matches!(get("motorcar").as_deref(), Some("no" | "private")) {
        return false;
    }

    let Some(highway) = get("highway") else {
        return false;
    };

    !matches!(
        highway.as_str(),
        "footway"
            | "path"
            | "cycleway"
            | "steps"
            | "bridleway"
            | "corridor"
            | "pedestrian"
            | "platform"
            | "construction"
            | "proposed"
            | "raceway"
            | "bus_guideway"
    )
}

fn drive_speed_mps_for_tags(get: &mut impl FnMut(&str) -> Option<String>) -> f64 {
    if let Some(maxspeed) = get("maxspeed").as_deref().and_then(parse_osm_speed_mps) {
        return maxspeed.clamp(4.0, 40.0);
    }

    match get("highway").as_deref() {
        Some("motorway") => 33.33,
        Some("motorway_link") => 22.22,
        Some("trunk") => 27.78,
        Some("trunk_link") => 19.44,
        Some("primary") => 19.44,
        Some("primary_link") => 16.67,
        Some("secondary") => 16.67,
        Some("secondary_link") => 13.89,
        Some("tertiary") => 13.89,
        Some("tertiary_link") => 11.11,
        Some("residential") | Some("living_street") => 8.33,
        Some("service") => 5.56,
        Some("track") => 4.17,
        _ => 11.11,
    }
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

    let sequence_number = sequence_number.context("street OSM diff state file missing sequenceNumber")?;
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
                            .context("street OSM diff way missing id attribute")?,
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
                        if let (Some(key), Some(value)) = (
                            parse_attr_string(&event, b"k"),
                            parse_attr_string(&event, b"v"),
                        ) {
                            way.tags.insert(key, value);
                        }
                    }
                }
                _ => {}
            },
            Ok(Event::End(event)) if event.name().as_ref() == b"way" => {
                if let Some(mut way) = current_way.take() {
                    way.coordinates = way
                        .node_refs
                        .iter()
                        .filter_map(|node_id| nodes.get(node_id).copied())
                        .collect();
                    ways.push(way);
                }
            }
            Ok(_) => {}
            Err(error) => return Err(error).context("failed to parse street OSM change XML"),
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
            .context("failed to decompress street OSM change payload")?;
        Ok(xml)
    } else {
        Ok(bytes.to_vec())
    }
}

fn parse_osc_node(event: &BytesStart<'_>) -> Result<Option<(i64, f64, f64)>> {
    let Some(id) = parse_attr_i64(event, b"id") else {
        return Ok(None);
    };
    let (Some(lat), Some(lon)) = (parse_attr_f64(event, b"lat"), parse_attr_f64(event, b"lon"))
    else {
        return Ok(None);
    };
    Ok(Some((id, lat, lon)))
}

fn parse_attr_i64(event: &BytesStart<'_>, key: &[u8]) -> Option<i64> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| std::str::from_utf8(attribute.value.as_ref()).ok().map(str::to_owned))
        .and_then(|value| value.parse::<i64>().ok())
}

fn parse_attr_f64(event: &BytesStart<'_>, key: &[u8]) -> Option<f64> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| std::str::from_utf8(attribute.value.as_ref()).ok().map(str::to_owned))
        .and_then(|value| value.parse::<f64>().ok())
}

fn parse_attr_string(event: &BytesStart<'_>, key: &[u8]) -> Option<String> {
    event
        .attributes()
        .flatten()
        .find(|attribute| attribute.key.as_ref() == key)
        .and_then(|attribute| std::str::from_utf8(attribute.value.as_ref()).ok().map(str::to_owned))
}

fn read_pbf_replication_anchor(osm_pbf_path: &Path) -> Result<StreetPbfReplicationAnchor> {
    let mut reader = BufReader::new(
        File::open(osm_pbf_path)
            .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?,
    );
    let header_len = read_u32_be_from_reader(&mut reader)? as usize;
    let mut header_bytes = vec![0; header_len];
    reader.read_exact(&mut header_bytes)?;
    let header = BlobHeader::parse_from_bytes(&header_bytes)
        .context("failed to parse street OSM PBF header block header")?;
    if header.type_() != "OSMHeader" {
        bail!("OSM PBF does not start with an OSMHeader block")
    }

    let mut blob_bytes = vec![0; header.datasize() as usize];
    reader.read_exact(&mut blob_bytes)?;
    let blob = Blob::parse_from_bytes(&blob_bytes)
        .context("failed to parse street OSM PBF header blob")?;
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
        .context("failed to parse street OSM PBF header block")?;

    let timestamp = if header_block.has_osmosis_replication_timestamp() {
        chrono::DateTime::<Utc>::from_timestamp(header_block.osmosis_replication_timestamp(), 0)
            .map(|value| value.to_rfc3339())
    } else {
        None
    };

    Ok(StreetPbfReplicationAnchor {
        sequence_number: if header_block.has_osmosis_replication_sequence_number() {
            Some(header_block.osmosis_replication_sequence_number() as u64)
        } else {
            None
        },
        timestamp,
        base_url: if header_block.has_osmosis_replication_base_url() {
            Some(header_block.osmosis_replication_base_url().to_owned())
        } else {
            None
        },
    })
}

fn read_u32_be_from_reader(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn parse_osm_speed_mps(raw: &str) -> Option<f64> {
    let value = raw.split(';').next()?.trim().to_ascii_lowercase();
    if value.is_empty() || value.contains(':') {
        return None;
    }

    let mut numeric = String::new();
    for character in value.chars() {
        if character.is_ascii_digit() || character == '.' {
            numeric.push(character);
        } else if !numeric.is_empty() {
            break;
        }
    }

    let parsed = numeric.parse::<f64>().ok()?;
    if value.contains("mph") {
        Some(parsed * 0.44704)
    } else {
        Some(parsed / 3.6)
    }
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
    use super::*;

    #[test]
    fn drive_route_returns_ordered_way_ids() {
        let graph = test_drive_graph();
        let route = graph.route((41.9000, 12.5000), (41.9010, 12.5030)).unwrap();

        assert!(route.distance_meters > 0.0);
        assert_eq!(route.segment_way_ids, vec![Some(10), Some(20), Some(30)]);
        assert_eq!(route.polyline.first().unwrap().lon, 12.5000);
        assert_eq!(route.polyline.last().unwrap().lon, 12.5030);
    }

    #[test]
    fn drive_route_reports_missing_reverse_path_on_one_way_chain() {
        let graph = test_drive_graph();
        let error = graph
            .route((41.9010, 12.5030), (41.9000, 12.5000))
            .unwrap_err();

        assert!(error.to_string().contains("no drive route found"));
    }

    fn test_drive_graph() -> StreetGraph {
        let coordinates = vec![
            (41.9000, 12.5000),
            (41.9000, 12.5010),
            (41.9005, 12.5020),
            (41.9010, 12.5030),
        ];
        let mut way_names = HashMap::new();
        way_names.insert(10, "Via Uno".to_owned());
        way_names.insert(20, "Via Due".to_owned());
        way_names.insert(30, "Via Tre".to_owned());

        let forward_adjacency = vec![
            vec![StreetEdge {
                to: 1,
                duration_secs: 12,
                distance_meters: 80.0,
                way_id: 10,
            }],
            vec![StreetEdge {
                to: 2,
                duration_secs: 12,
                distance_meters: 85.0,
                way_id: 20,
            }],
            vec![StreetEdge {
                to: 3,
                duration_secs: 12,
                distance_meters: 90.0,
                way_id: 30,
            }],
            vec![],
        ];
        let reverse_adjacency = vec![
            vec![],
            vec![StreetEdge {
                to: 0,
                duration_secs: 12,
                distance_meters: 80.0,
                way_id: 10,
            }],
            vec![StreetEdge {
                to: 1,
                duration_secs: 12,
                distance_meters: 85.0,
                way_id: 20,
            }],
            vec![StreetEdge {
                to: 2,
                duration_secs: 12,
                distance_meters: 90.0,
                way_id: 30,
            }],
        ];
        let (node_offsets, forward_edges) = flatten_edges(forward_adjacency).unwrap();
        let (reverse_node_offsets, reverse_edges) = flatten_edges(reverse_adjacency).unwrap();
        let index = RTree::bulk_load(
            coordinates
                .iter()
                .enumerate()
                .map(|(index, (lat, lon))| IndexedPoint {
                    index,
                    point: [*lon, *lat],
                })
                .collect(),
        );

        StreetGraph {
            mode: StreetMode::Drive,
            coordinates: Arc::new(coordinates),
            node_lookup: Arc::new(HashMap::from([(1, 0), (2, 1), (3, 2), (4, 3)])),
            node_offsets: Arc::new(node_offsets),
            forward_edges: Arc::new(forward_edges),
            reverse_node_offsets: Arc::new(reverse_node_offsets),
            reverse_edges: Arc::new(reverse_edges),
            way_names: Arc::new(way_names),
            index: Arc::new(index),
            overlay_runtime: Arc::new(ArcSwap::from_pointee(empty_overlay_runtime(None))),
            overlay_path: Arc::new(PathBuf::from("test.street.overlay.bin")),
            base_metadata: StreetGraphCacheMetadata {
                schema_version: STREET_GRAPH_SCHEMA_VERSION,
                mode: StreetMode::Drive.as_str().to_owned(),
                osm_pbf_bytes: 0,
                osm_pbf_modified_unix_secs: None,
            },
            diff_config: None,
            pbf_replication: None,
        }
    }
}