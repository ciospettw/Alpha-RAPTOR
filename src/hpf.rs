use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::Arc,
    time::UNIX_EPOCH,
};

use anyhow::{Context, Result, bail};
use osmpbfreader::{NodeId, OsmObj, OsmPbfReader, Way};
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    engine::{PolylinePoint, StopRecord},
    geo::{decode_morton_code, morton_code},
};

const OSM_HPF_STRATEGY: &str = "osm-pbf-cached-hpf";

pub struct HpfBuildResult {
    pub forest: HolographicPedestrianForest,
    pub strategy: &'static str,
    pub cache_hit: bool,
    pub covered_nodes: usize,
    pub anchored_stops: usize,
}

#[derive(Clone)]
pub struct HolographicPedestrianForest {
    nodes: Arc<Vec<HpfNode>>,
    walk_speed_mps: f64,
    snap_tolerance_meters: f64,
    snap_quadratic_kappa_meters: f64,
    search_window: usize,
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
struct HpfNode {
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

#[derive(Serialize, Deserialize, PartialEq, Eq)]
struct HpfCacheMetadata {
    osm_pbf_bytes: u64,
    osm_pbf_modified_unix_secs: Option<u64>,
    stop_fingerprint: u64,
    max_distance_bits: u64,
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
    polyline: Vec<PolylinePoint>,
    used_asymptotic_penalty: bool,
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
    ) -> Self {
        Self {
            nodes: Arc::new(cache.nodes),
            walk_speed_mps,
            snap_tolerance_meters,
            snap_quadratic_kappa_meters,
            search_window: search_window.max(256),
        }
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
        let center = self
            .nodes
            .binary_search_by_key(&query_morton, |node| node.morton)
            .unwrap_or_else(|position| position);

        let mut best_by_stop = HashMap::<usize, CandidateConnector>::new();
        let mut scanned_left = center;
        let mut scanned_right = center;
        let mut window = self.search_window.min(self.nodes.len()).max(limit * 64);

        loop {
            let left = center.saturating_sub(window);
            let right = (center + window).min(self.nodes.len());

            for index in left..scanned_left {
                self.consider_candidate(
                    index,
                    latitude,
                    longitude,
                    stops,
                    &mut best_by_stop,
                );
            }
            for index in scanned_right..right {
                self.consider_candidate(
                    index,
                    latitude,
                    longitude,
                    stops,
                    &mut best_by_stop,
                );
            }

            scanned_left = left;
            scanned_right = right;

            if best_by_stop.len() >= limit || (left == 0 && right == self.nodes.len()) {
                break;
            }
            window = (window * 2).min(self.nodes.len());
        }

        let mut connectors = best_by_stop
            .into_values()
            .map(|candidate| HpfConnector {
                stop_index: candidate.stop_index,
                duration_secs: (candidate.distance_meters / self.walk_speed_mps).ceil() as i32,
                distance_meters: candidate.distance_meters,
                polyline: candidate.polyline,
                used_asymptotic_penalty: candidate.used_asymptotic_penalty,
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

    fn consider_candidate(
        &self,
        index: usize,
        latitude: f64,
        longitude: f64,
        stops: &[StopRecord],
        best_by_stop: &mut HashMap<usize, CandidateConnector>,
    ) {
        let node = &self.nodes[index];
        let stop_index = node.root_stop_index as usize;
        let Some(stop) = stops.get(stop_index) else {
            return;
        };
        let (Some(stop_lat), Some(stop_lon)) = (stop.latitude, stop.longitude) else {
            return;
        };

        let (node_lat, node_lon) = decode_morton_code(node.morton);
        let snap_distance = haversine_meters(latitude, longitude, node_lat, node_lon);
        let used_asymptotic_penalty = snap_distance > self.snap_tolerance_meters;
        let snap_cost = snap_distance.mul_add(
            snap_distance / self.snap_quadratic_kappa_meters,
            snap_distance,
        );
        let total_distance = f64::from(node.cost_meters) + snap_cost;

        let should_replace = match best_by_stop.get(&stop_index) {
            Some(current) => total_distance < current.distance_meters,
            None => true,
        };
        if !should_replace {
            return;
        }

        let polyline = self.reconstruct_polyline(index, latitude, longitude, stop_lat, stop_lon);
        best_by_stop.insert(
            stop_index,
            CandidateConnector {
                stop_index,
                distance_meters: total_distance,
                polyline,
                used_asymptotic_penalty,
            },
        );
    }

    fn reconstruct_polyline(
        &self,
        start_index: usize,
        query_lat: f64,
        query_lon: f64,
        stop_lat: f64,
        stop_lon: f64,
    ) -> Vec<PolylinePoint> {
        let mut polyline = vec![PolylinePoint {
            lat: query_lat,
            lon: query_lon,
        }];

        let mut cursor = start_index;
        loop {
            let node = &self.nodes[cursor];
            let (lat, lon) = decode_morton_code(node.morton);
            push_polyline_point(&mut polyline, lat, lon);
            if node.parent_index == u32::MAX {
                break;
            }
            cursor = node.parent_index as usize;
        }

        push_polyline_point(&mut polyline, stop_lat, stop_lon);
        polyline
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
) -> Result<HpfBuildResult> {
    let metadata = build_cache_metadata(osm_pbf_path, stops, max_distance_meters)?;
    let cache_path = hpf_cache_path(cache_dir, osm_pbf_path);

    if let Some(cache) = load_cache(&cache_path, &metadata)? {
        info!(
            cache = %cache_path.display(),
            covered_nodes = cache.nodes.len(),
            anchored_stops = cache.anchored_stops,
            "loaded holographic pedestrian forest from cache"
        );
        let covered_nodes = cache.nodes.len();
        let anchored_stops = cache.anchored_stops;
        return Ok(HpfBuildResult {
            forest: HolographicPedestrianForest::from_cache(
                cache,
                walk_speed_mps,
                snap_tolerance_meters,
                snap_quadratic_kappa_meters,
                search_window,
            ),
            strategy: OSM_HPF_STRATEGY,
            cache_hit: true,
            covered_nodes,
            anchored_stops,
        });
    }

    let cache = build_hpf_cache(osm_pbf_path, stops, max_distance_meters)?;
    let covered_nodes = cache.nodes.len();
    let anchored_stops = cache.anchored_stops;
    store_cache(&cache_path, &cache)?;

    Ok(HpfBuildResult {
        forest: HolographicPedestrianForest::from_cache(
            cache,
            walk_speed_mps,
            snap_tolerance_meters,
            snap_quadratic_kappa_meters,
            search_window,
        ),
        strategy: OSM_HPF_STRATEGY,
        cache_hit: false,
        covered_nodes,
        anchored_stops,
    })
}

fn build_hpf_cache(
    osm_pbf_path: &Path,
    stops: &[StopRecord],
    max_distance_meters: f64,
) -> Result<HpfCache> {
    let metadata = build_cache_metadata(osm_pbf_path, stops, max_distance_meters)?;
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

    let (graph_coordinates, graph_edges) = build_pedestrian_graph(&node_coordinates, &ways)?;
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
    use super::{HolographicPedestrianForest, HpfNode};
    use crate::{engine::StopRecord, geo::morton_code};

    #[test]
    fn query_connectors_prefers_lowest_total_cost_per_stop() {
        let forest = HolographicPedestrianForest {
            nodes: std::sync::Arc::new(vec![
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
            ]),
            walk_speed_mps: 1.35,
            snap_tolerance_meters: 50.0,
            snap_quadratic_kappa_meters: 40.0,
            search_window: 256,
        };
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
        let forest = HolographicPedestrianForest {
            nodes: std::sync::Arc::new(vec![
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
            ]),
            walk_speed_mps: 1.35,
            snap_tolerance_meters: 140.0,
            snap_quadratic_kappa_meters: 40.0,
            search_window: 256,
        };
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
}