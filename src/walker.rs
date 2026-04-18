use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering as AtomicOrdering},
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, anyhow};
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, Way};
use rayon::prelude::*;
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::engine::{PolylinePoint, StopRecord, WalkTransfer};

const OSM_WALK_STRATEGY: &str = "osm-pbf-cached-dijkstra";
const FALLBACK_WALK_STRATEGY: &str = "fallback-radius-haversine";

pub struct WalkerBuildResult {
    pub transfers: Vec<Vec<WalkTransfer>>,
    pub way_names: HashMap<i64, String>,
    pub strategy: &'static str,
    pub cache_hit: bool,
    pub graph_nodes: usize,
    pub graph_edges: usize,
    pub anchored_stops: usize,
}

impl WalkerBuildResult {
    pub fn fallback(transfers: Vec<Vec<WalkTransfer>>) -> Self {
        Self {
            transfers,
            way_names: HashMap::new(),
            strategy: FALLBACK_WALK_STRATEGY,
            cache_hit: false,
            graph_nodes: 0,
            graph_edges: 0,
            anchored_stops: 0,
        }
    }
}

pub struct WalkerSubsetBuildResult {
    pub updated_transfers: Vec<(usize, Vec<WalkTransfer>)>,
    pub graph_nodes: usize,
    pub graph_edges: usize,
    pub anchored_stops: usize,
}

#[derive(Serialize, Deserialize)]
struct WalkerCache {
    metadata: WalkerCacheMetadata,
    graph_nodes: usize,
    graph_edges: usize,
    anchored_stops: usize,
    transfers: Vec<Vec<WalkTransfer>>,
    way_names: HashMap<i64, String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct WalkerCacheMetadata {
    osm_pbf_bytes: u64,
    osm_pbf_modified_unix_secs: Option<u64>,
    stop_fingerprint: u64,
    walk_radius_bits: u64,
    walk_speed_bits: u64,
    max_transfer_candidates: usize,
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
    way_id: i64,
}

#[derive(Clone, Copy)]
struct PredecessorEdge {
    previous: usize,
    way_id: i64,
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

#[derive(Clone, Copy)]
struct HeapState {
    node_index: usize,
    distance_meters: f64,
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

pub fn build_or_load_walker_transfers(
    osm_pbf_path: &Path,
    cache_dir: &Path,
    stops: &[StopRecord],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Result<WalkerBuildResult> {
    let metadata = build_cache_metadata(
        osm_pbf_path,
        stops,
        walk_radius_meters,
        walk_speed_mps,
        max_transfer_candidates,
    )?;
    let cache_path = walker_cache_path(cache_dir, osm_pbf_path);

    if let Some(cache) = load_cache(&cache_path, &metadata)? {
        info!(
            cache = %cache_path.display(),
            transfers = cache.transfers.iter().map(Vec::len).sum::<usize>(),
            graph_nodes = cache.graph_nodes,
            graph_edges = cache.graph_edges,
            anchored_stops = cache.anchored_stops,
            "loaded walker matrix from cache"
        );
        return Ok(WalkerBuildResult {
            transfers: cache.transfers,
            way_names: cache.way_names,
            strategy: OSM_WALK_STRATEGY,
            cache_hit: true,
            graph_nodes: cache.graph_nodes,
            graph_edges: cache.graph_edges,
            anchored_stops: cache.anchored_stops,
        });
    }

    let built = build_walker_from_osm(
        osm_pbf_path,
        stops,
        walk_radius_meters,
        walk_speed_mps,
        max_transfer_candidates,
    )?;

    let cache = WalkerCache {
        metadata,
        graph_nodes: built.graph_nodes,
        graph_edges: built.graph_edges,
        anchored_stops: built.anchored_stops,
        transfers: built.transfers.clone(),
        way_names: built.way_names.clone(),
    };
    store_cache(&cache_path, &cache)?;

    Ok(WalkerBuildResult {
        transfers: built.transfers,
        way_names: built.way_names,
        strategy: OSM_WALK_STRATEGY,
        cache_hit: false,
        graph_nodes: built.graph_nodes,
        graph_edges: built.graph_edges,
        anchored_stops: built.anchored_stops,
    })
}

pub fn rebuild_walker_transfers_subset(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    affected_sources: &[usize],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Result<WalkerSubsetBuildResult> {
    if affected_sources.is_empty() {
        return Ok(WalkerSubsetBuildResult {
            updated_transfers: Vec::new(),
            graph_nodes: 0,
            graph_edges: 0,
            anchored_stops: 0,
        });
    }

    let file = File::open(osm_pbf_path)
        .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?;
    let mut pbf = OsmPbfReader::new(BufReader::new(file));
    let objects = pbf
        .get_objs_and_deps(|obj| matches!(obj, OsmObj::Way(way) if is_walkable_way(way)))
        .context("failed to extract walkable OSM ways and dependencies")?;

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

    let (graph_coordinates, graph_edges) = build_pedestrian_graph(&node_coordinates, &ways)?;
    if graph_coordinates.is_empty() {
        return Err(anyhow!(
            "OSM walker graph is empty after filtering walkable ways"
        ));
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
    let stop_index = RTree::bulk_load(
        stops
            .iter()
            .enumerate()
            .filter_map(|(index, stop)| {
                Some(IndexedPoint {
                    index,
                    point: [stop.longitude?, stop.latitude?],
                })
            })
            .collect(),
    );

    let max_snap_distance_meters = walk_radius_meters.min(250.0).max(80.0);
    let stop_anchors: Vec<_> = stops
        .iter()
        .map(|stop| {
            snap_stop_to_graph(
                stop,
                &graph_index,
                &graph_coordinates,
                max_snap_distance_meters,
            )
        })
        .collect();
    let anchored_stops = stop_anchors
        .iter()
        .filter(|anchor| anchor.is_some())
        .count();

    let updated_transfers = affected_sources
        .par_iter()
        .map(|source_index| {
            (
                *source_index,
                build_stop_transfers(
                    *source_index,
                    stops,
                    &stop_anchors,
                    &stop_index,
                    &graph_coordinates,
                    &graph_edges,
                    walk_radius_meters,
                    walk_speed_mps,
                    max_transfer_candidates,
                ),
            )
        })
        .collect();

    Ok(WalkerSubsetBuildResult {
        updated_transfers,
        graph_nodes: graph_coordinates.len(),
        graph_edges: graph_edges.iter().map(Vec::len).sum::<usize>(),
        anchored_stops,
    })
}

struct BuiltWalker {
    transfers: Vec<Vec<WalkTransfer>>,
    way_names: HashMap<i64, String>,
    graph_nodes: usize,
    graph_edges: usize,
    anchored_stops: usize,
}

fn build_walker_from_osm(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Result<BuiltWalker> {
    info!(osm = %osm_pbf_path.display(), "building walker matrix from OSM PBF");

    let file = File::open(osm_pbf_path)
        .with_context(|| format!("unable to open OSM PBF at {}", osm_pbf_path.display()))?;
    let mut pbf = OsmPbfReader::new(BufReader::new(file));
    let objects = pbf
        .get_objs_and_deps(|obj| matches!(obj, OsmObj::Way(way) if is_walkable_way(way)))
        .context("failed to extract walkable OSM ways and dependencies")?;

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

    let way_names = collect_way_names(&ways);
    let (graph_coordinates, graph_edges) = build_pedestrian_graph(&node_coordinates, &ways)?;
    if graph_coordinates.is_empty() {
        return Err(anyhow!(
            "OSM walker graph is empty after filtering walkable ways"
        ));
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
    let stop_index = RTree::bulk_load(
        stops
            .iter()
            .enumerate()
            .filter_map(|(index, stop)| {
                Some(IndexedPoint {
                    index,
                    point: [stop.longitude?, stop.latitude?],
                })
            })
            .collect(),
    );

    let max_snap_distance_meters = walk_radius_meters.min(250.0).max(80.0);
    let stop_anchors: Vec<_> = stops
        .iter()
        .map(|stop| {
            snap_stop_to_graph(
                stop,
                &graph_index,
                &graph_coordinates,
                max_snap_distance_meters,
            )
        })
        .collect();
    let anchored_stops = stop_anchors
        .iter()
        .filter(|anchor| anchor.is_some())
        .count();
    info!(
        graph_nodes = graph_coordinates.len(),
        graph_edges = graph_edges.iter().map(Vec::len).sum::<usize>(),
        anchored_stops,
        "constructed pedestrian graph and stop anchors"
    );

    let processed = AtomicUsize::new(0);
    let transfers: Vec<Vec<WalkTransfer>> = (0..stops.len())
        .into_par_iter()
        .map(|source_index| {
            let neighbours = build_stop_transfers(
                source_index,
                stops,
                &stop_anchors,
                &stop_index,
                &graph_coordinates,
                &graph_edges,
                walk_radius_meters,
                walk_speed_mps,
                max_transfer_candidates,
            );
            let completed = processed.fetch_add(1, AtomicOrdering::Relaxed) + 1;
            if completed % 512 == 0 || completed == stops.len() {
                info!(completed, total = stops.len(), "walker precompute progress");
            }
            neighbours
        })
        .collect();

    Ok(BuiltWalker {
        transfers,
        way_names,
        graph_nodes: graph_coordinates.len(),
        graph_edges: graph_edges.iter().map(Vec::len).sum::<usize>(),
        anchored_stops,
    })
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
                way_id: way.id.0,
            });
            graph_edges[to_index].push(PedestrianEdge {
                to: from_index,
                distance_meters,
                way_id: way.id.0,
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

fn build_stop_transfers(
    source_index: usize,
    stops: &[StopRecord],
    stop_anchors: &[Option<StopAnchor>],
    stop_index: &RTree<IndexedPoint>,
    graph_coordinates: &[(f64, f64)],
    graph_edges: &[Vec<PedestrianEdge>],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Vec<WalkTransfer> {
    let Some(source_stop) = stops.get(source_index) else {
        return Vec::new();
    };
    let (Some(source_lat), Some(source_lon), Some(source_anchor)) = (
        source_stop.latitude,
        source_stop.longitude,
        stop_anchors[source_index],
    ) else {
        return Vec::new();
    };

    let candidate_stops = nearby_stop_candidates(
        source_index,
        source_lat,
        source_lon,
        stop_index,
        stops,
        walk_radius_meters,
    );
    if candidate_stops.is_empty() {
        return Vec::new();
    }

    let mut target_nodes = HashSet::<usize>::new();
    let mut candidate_targets = Vec::<(usize, StopAnchor)>::new();
    for target_stop in candidate_stops {
        let Some(target_anchor) = stop_anchors[target_stop] else {
            continue;
        };
        target_nodes.insert(target_anchor.node_index);
        candidate_targets.push((target_stop, target_anchor));
    }
    if candidate_targets.is_empty() {
        return Vec::new();
    }

    let (best_distances, predecessors) = bounded_dijkstra(
        source_anchor.node_index,
        graph_edges,
        walk_radius_meters,
        &target_nodes,
    );

    let mut transfers = Vec::<WalkTransfer>::new();
    for (target_stop, target_anchor) in candidate_targets {
        let path_distance = if target_anchor.node_index == source_anchor.node_index {
            0.0
        } else if let Some(distance) = best_distances.get(&target_anchor.node_index).copied() {
            distance
        } else {
            continue;
        };

        let total_distance =
            source_anchor.snap_distance_meters + path_distance + target_anchor.snap_distance_meters;
        if total_distance > walk_radius_meters {
            continue;
        }

        let (polyline, segment_way_ids) = build_walk_geometry(
            source_stop,
            &stops[target_stop],
            source_anchor,
            target_anchor,
            graph_coordinates,
            &predecessors,
        );

        transfers.push(WalkTransfer {
            to_stop: target_stop,
            duration_secs: (total_distance / walk_speed_mps).ceil() as i32,
            distance_meters: total_distance,
            polyline,
            segment_way_ids,
        });
    }

    transfers.sort_by(|left, right| {
        left.duration_secs
            .cmp(&right.duration_secs)
            .then_with(|| left.to_stop.cmp(&right.to_stop))
    });
    transfers.truncate(max_transfer_candidates);
    transfers
}

fn bounded_dijkstra(
    source_node: usize,
    graph_edges: &[Vec<PedestrianEdge>],
    max_distance_meters: f64,
    target_nodes: &HashSet<usize>,
) -> (HashMap<usize, f64>, HashMap<usize, PredecessorEdge>) {
    let mut best_distances = HashMap::<usize, f64>::new();
    let mut predecessors = HashMap::<usize, PredecessorEdge>::new();
    let mut remaining_targets = target_nodes.clone();
    let mut heap = BinaryHeap::<HeapState>::new();

    best_distances.insert(source_node, 0.0);
    heap.push(HeapState {
        node_index: source_node,
        distance_meters: 0.0,
    });

    while let Some(state) = heap.pop() {
        let Some(&known_distance) = best_distances.get(&state.node_index) else {
            continue;
        };
        if state.distance_meters > known_distance {
            continue;
        }
        if state.distance_meters > max_distance_meters {
            break;
        }

        remaining_targets.remove(&state.node_index);
        if remaining_targets.is_empty() {
            break;
        }

        for edge in &graph_edges[state.node_index] {
            let candidate_distance = state.distance_meters + edge.distance_meters;
            if candidate_distance > max_distance_meters {
                continue;
            }

            let should_update = match best_distances.get(&edge.to) {
                Some(&current_distance) => candidate_distance < current_distance,
                None => true,
            };
            if should_update {
                best_distances.insert(edge.to, candidate_distance);
                predecessors.insert(
                    edge.to,
                    PredecessorEdge {
                        previous: state.node_index,
                        way_id: edge.way_id,
                    },
                );
                heap.push(HeapState {
                    node_index: edge.to,
                    distance_meters: candidate_distance,
                });
            }
        }
    }

    (best_distances, predecessors)
}

fn build_walk_geometry(
    source_stop: &StopRecord,
    target_stop: &StopRecord,
    source_anchor: StopAnchor,
    target_anchor: StopAnchor,
    graph_coordinates: &[(f64, f64)],
    predecessors: &HashMap<usize, PredecessorEdge>,
) -> (Vec<PolylinePoint>, Vec<Option<i64>>) {
    let mut polyline = Vec::<PolylinePoint>::new();
    let mut segment_way_ids = Vec::<Option<i64>>::new();

    if let (Some(lat), Some(lon)) = (source_stop.latitude, source_stop.longitude) {
        push_walk_geometry_point(&mut polyline, &mut segment_way_ids, lat, lon, None);
    }

    let mut node_path = Vec::<usize>::new();
    let mut node_way_ids = Vec::<Option<i64>>::new();
    let mut cursor = target_anchor.node_index;
    node_path.push(cursor);
    while cursor != source_anchor.node_index {
        let Some(predecessor) = predecessors.get(&cursor).copied() else {
            let polyline = straight_polyline(source_stop, target_stop);
            return (polyline.clone(), empty_segment_way_ids(&polyline));
        };
        cursor = predecessor.previous;
        node_path.push(cursor);
        node_way_ids.push(Some(predecessor.way_id));
    }
    node_path.reverse();
    node_way_ids.reverse();

    for (offset, node_index) in node_path.into_iter().enumerate() {
        let (lat, lon) = graph_coordinates[node_index];
        let incoming_way_id = if offset == 0 {
            None
        } else {
            node_way_ids.get(offset - 1).copied().flatten()
        };
        push_walk_geometry_point(
            &mut polyline,
            &mut segment_way_ids,
            lat,
            lon,
            incoming_way_id,
        );
    }

    if let (Some(lat), Some(lon)) = (target_stop.latitude, target_stop.longitude) {
        push_walk_geometry_point(&mut polyline, &mut segment_way_ids, lat, lon, None);
    }

    if polyline.len() >= 2 {
        (polyline, segment_way_ids)
    } else {
        let polyline = straight_polyline(source_stop, target_stop);
        (polyline.clone(), empty_segment_way_ids(&polyline))
    }
}

fn push_walk_geometry_point(
    polyline: &mut Vec<PolylinePoint>,
    segment_way_ids: &mut Vec<Option<i64>>,
    lat: f64,
    lon: f64,
    incoming_way_id: Option<i64>,
) {
    let should_push = polyline
        .last()
        .is_none_or(|last| (last.lat - lat).abs() > 1e-7 || (last.lon - lon).abs() > 1e-7);
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

fn collect_way_names(ways: &[Way]) -> HashMap<i64, String> {
    ways.iter()
        .filter_map(|way| way_display_name(way).map(|name| (way.id.0, name)))
        .collect()
}

fn way_display_name(way: &Way) -> Option<String> {
    ["name", "official_name", "ref"]
        .iter()
        .find_map(|tag| way.tags.get(*tag))
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn nearby_stop_candidates(
    source_index: usize,
    source_lat: f64,
    source_lon: f64,
    stop_index: &RTree<IndexedPoint>,
    stops: &[StopRecord],
    walk_radius_meters: f64,
) -> Vec<usize> {
    let lat_delta = walk_radius_meters / 111_320.0;
    let lon_denominator = 111_320.0 * source_lat.to_radians().cos().abs().max(0.25);
    let lon_delta = walk_radius_meters / lon_denominator;
    let envelope = AABB::from_corners(
        [source_lon - lon_delta, source_lat - lat_delta],
        [source_lon + lon_delta, source_lat + lat_delta],
    );

    stop_index
        .locate_in_envelope_intersecting(&envelope)
        .filter_map(|candidate| {
            if candidate.index == source_index {
                return None;
            }
            let stop = &stops[candidate.index];
            let distance =
                haversine_meters(source_lat, source_lon, stop.latitude?, stop.longitude?);
            (distance <= walk_radius_meters).then_some(candidate.index)
        })
        .collect()
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

fn load_cache(cache_path: &Path, metadata: &WalkerCacheMetadata) -> Result<Option<WalkerCache>> {
    if !cache_path.exists() {
        return Ok(None);
    }

    let file = File::open(cache_path)
        .with_context(|| format!("unable to open walker cache {}", cache_path.display()))?;
    let reader = BufReader::new(file);
    let cache: WalkerCache = match bincode::deserialize_from(reader) {
        Ok(cache) => cache,
        Err(error) => {
            warn!(%error, cache = %cache_path.display(), "invalid walker cache, rebuilding");
            return Ok(None);
        }
    };

    if cache.metadata == *metadata {
        Ok(Some(cache))
    } else {
        info!(cache = %cache_path.display(), "walker cache metadata mismatch, rebuilding");
        Ok(None)
    }
}

fn store_cache(cache_path: &Path, cache: &WalkerCache) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "unable to create walker cache directory {}",
                parent.display()
            )
        })?;
    }
    let file = File::create(cache_path)
        .with_context(|| format!("unable to create walker cache {}", cache_path.display()))?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, cache).context("failed to serialize walker cache")
}

fn build_cache_metadata(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Result<WalkerCacheMetadata> {
    let metadata = fs::metadata(osm_pbf_path)
        .with_context(|| format!("unable to stat OSM PBF at {}", osm_pbf_path.display()))?;
    let modified_secs = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());

    Ok(WalkerCacheMetadata {
        osm_pbf_bytes: metadata.len(),
        osm_pbf_modified_unix_secs: modified_secs,
        stop_fingerprint: stop_fingerprint(stops),
        walk_radius_bits: walk_radius_meters.to_bits(),
        walk_speed_bits: walk_speed_mps.to_bits(),
        max_transfer_candidates,
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

fn walker_cache_path(cache_dir: &Path, osm_pbf_path: &Path) -> PathBuf {
    let stem = osm_pbf_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-walker");
    let file_name = format!("{stem}.walker.bin");
    cache_dir.join(file_name)
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

fn straight_polyline(from: &StopRecord, to: &StopRecord) -> Vec<PolylinePoint> {
    match (from.latitude, from.longitude, to.latitude, to.longitude) {
        (Some(from_lat), Some(from_lon), Some(to_lat), Some(to_lon)) => vec![
            PolylinePoint {
                lat: from_lat,
                lon: from_lon,
            },
            PolylinePoint {
                lat: to_lat,
                lon: to_lon,
            },
        ],
        _ => Vec::new(),
    }
}
