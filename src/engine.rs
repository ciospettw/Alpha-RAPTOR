use std::{
    collections::{HashMap, HashSet},
    env,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Duration, NaiveDate, NaiveTime, Timelike};
use gtfs_structures::{Exception, Gtfs};
use reqwest::{
    blocking::Client as BlockingHttpClient,
    header::{ETAG, LAST_MODIFIED},
};
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::cold_storage::{ColdRouteRecord, ColdStore, ColdTripRecord, cold_store_paths};
use crate::geo::{
    destination_cell as geo_destination_cell,
    destination_cell_neighborhood as geo_destination_cell_neighborhood,
    destination_cell_window as geo_destination_cell_window,
};
use crate::hpf::{HolographicPedestrianForest, build_or_load_hpf};
use crate::profile_cache::{
    CachedLeg, PreparedSpatialLookup, ProfileCache, ProfileCacheStats, ProfileInsertionPoint,
    ProfileLookupDecision, SpatialProfileInsertionPoint,
};
use crate::realtime::{RealtimeDebugSnapshot, RealtimeStore};
use crate::walker::{
    WalkerBuildResult, build_or_load_walker_transfers, rebuild_walker_transfers_subset,
};

const INF_TIME: i32 = i32::MAX / 8;
const STATIC_CACHE_SCHEMA_VERSION: u32 = 6;
const CHRONOS_BUCKET_SECS: i32 = 15 * 60;
const DEFAULT_MANIFEST_NAME: &str = "alpha-raptor.toml";
const DEFAULT_STATIC_DIFF_TOLERANCE: f64 = 0.05;
const DEFAULT_DVNI_KNN_CANDIDATES: usize = 5;
const DEFAULT_DVNI_MAX_WALK_RADIUS_METERS: f64 = 1_500.0;
const DEFAULT_HPF_MAX_DISTANCE_METERS: f64 = 4_000.0;
const DEFAULT_HPF_SNAP_TOLERANCE_METERS: f64 = 140.0;
const DEFAULT_HPF_SNAP_QUADRATIC_KAPPA_METERS: f64 = 40.0;
const DEFAULT_HPF_SEARCH_WINDOW: usize = 512;
const GLOBAL_ID_LOCAL_MASK: u64 = (1u64 << 48) - 1;
const ENTITY_KIND_SHIFT: u64 = 44;
const ENTITY_ORDINAL_MASK: u64 = (1u64 << ENTITY_KIND_SHIFT) - 1;

#[derive(Clone)]
pub struct FeedConfig {
    pub feed_index: u16,
    pub id: String,
    pub static_gtfs_path: PathBuf,
    pub static_gtfs_source: String,
    pub static_gtfs_remote_url: Option<String>,
    pub static_gtfs_allow_invalid_tls: bool,
    pub trip_updates_url: Option<String>,
    pub vehicle_positions_url: Option<String>,
    pub depends_on: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawEngineManifest {
    osm_pbf: Option<String>,
    walk_radius_meters: Option<f64>,
    walk_speed_mps: Option<f64>,
    max_transfer_candidates: Option<usize>,
    refresh_interval_secs: Option<u64>,
    static_reload_interval_secs: Option<u64>,
    static_diff_tolerance: Option<f64>,
    default_max_transfers: Option<usize>,
    dvni: Option<RawDvniConfig>,
    hpf: Option<RawHpfConfig>,
    feeds: Vec<RawFeedConfig>,
}

#[derive(Debug, Deserialize)]
struct RawDvniConfig {
    knn_candidates: Option<usize>,
    max_walk_radius_meters: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawHpfConfig {
    max_distance_meters: Option<f64>,
    snap_tolerance_meters: Option<f64>,
    snap_quadratic_kappa_meters: Option<f64>,
    search_window: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawFeedConfig {
    id: String,
    static_gtfs: String,
    #[serde(default)]
    static_gtfs_allow_invalid_tls: bool,
    trip_updates_url: Option<String>,
    vehicle_positions_url: Option<String>,
    #[serde(default)]
    depends_on: Vec<String>,
}

#[derive(Copy, Clone)]
enum EntityKind {
    Stop = 1,
    Route = 2,
    Trip = 3,
}

#[derive(Clone)]
pub struct Engine {
    pub config: Arc<EngineConfig>,
    static_data: Arc<StaticData>,
    realtime: RealtimeStore,
    profile_cache: ProfileCache,
    hpf: Option<HolographicPedestrianForest>,
}

#[derive(Clone)]
pub struct EngineConfig {
    pub workspace_root: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub feeds: Vec<FeedConfig>,
    pub osm_pbf_path: PathBuf,
    pub walk_radius_meters: f64,
    pub walk_speed_mps: f64,
    pub max_transfer_candidates: usize,
    pub refresh_interval_secs: u64,
    pub static_reload_interval_secs: u64,
    pub static_diff_tolerance: f64,
    pub default_max_transfers: usize,
    pub dvni_knn_candidates: usize,
    pub dvni_max_walk_radius_meters: f64,
    pub hpf_max_distance_meters: f64,
    pub hpf_snap_tolerance_meters: f64,
    pub hpf_snap_quadratic_kappa_meters: f64,
    pub hpf_search_window: usize,
    static_inputs_metadata: StaticCacheMetadata,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FeedRecord {
    pub feed_index: u16,
    pub id: String,
    pub static_gtfs_path: String,
    pub depends_on: Vec<String>,
}

#[derive(Clone)]
pub struct StaticData {
    pub feeds: Vec<FeedRecord>,
    pub stops: Vec<StopRecord>,
    pub active_stop_indices: Vec<usize>,
    pub stop_lookup: HashMap<String, usize>,
    pub stop_lookup_by_global_id: HashMap<u64, usize>,
    pub routes: Vec<RouteRecord>,
    pub active_route_indices: Vec<usize>,
    pub trips: Vec<TripRecord>,
    pub active_trip_indices: Vec<usize>,
    pub trip_lookup_by_feed: Vec<HashMap<String, usize>>,
    pub service_to_trip_indices: HashMap<String, Vec<usize>>,
    pub lines: Vec<LineRecord>,
    pub stop_to_lines: Vec<Vec<StopLineRef>>,
    pub transfers: Vec<Vec<WalkTransfer>>,
    pub stop_cells: HashMap<u64, Vec<usize>>,
    pub service_by_date: HashMap<NaiveDate, HashSet<String>>,
    pub cold_store: Arc<ColdStore>,
    pub build_stats: BuildStats,
}

#[derive(Clone, Serialize, Deserialize)]
struct StaticCore {
    feeds: Vec<FeedRecord>,
    stops: Vec<StopRecord>,
    active_stop_indices: Vec<usize>,
    stop_lookup: HashMap<String, usize>,
    stop_lookup_by_global_id: HashMap<u64, usize>,
    routes: Vec<RouteRecord>,
    active_route_indices: Vec<usize>,
    trips: Vec<TripRecord>,
    active_trip_indices: Vec<usize>,
    trip_lookup_by_feed: Vec<HashMap<String, usize>>,
    service_to_trip_indices: HashMap<String, Vec<usize>>,
    lines: Vec<LineRecord>,
    stop_to_lines: Vec<Vec<StopLineRef>>,
    service_by_date: HashMap<NaiveDate, HashSet<String>>,
    shapes: HashMap<String, Vec<ShapePoint>>,
}

struct StaticCoreLoadResult {
    core: StaticCore,
    cache_hit: bool,
    cache_bytes: u64,
    timings: BuildTimings,
    update_strategy: &'static str,
    diff_summary: StaticDiffSummary,
}

#[derive(Clone, Serialize, Deserialize)]
struct StaticCache {
    metadata: StaticCacheMetadata,
    core: StaticCore,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StaticCacheMetadata {
    schema_version: u32,
    manifest_path: Option<String>,
    manifest_modified_unix_secs: Option<u64>,
    feed_sources: Vec<StaticFeedSourceMetadata>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StaticFeedSourceMetadata {
    feed_id: String,
    static_gtfs_source: String,
    static_gtfs_path: String,
    static_gtfs_allow_invalid_tls: bool,
    static_gtfs_bytes: u64,
    static_gtfs_modified_unix_secs: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StopRecord {
    pub global_id: u64,
    pub feed_index: u16,
    pub feed_id: String,
    pub local_id: String,
    pub id: String,
    pub code: Option<String>,
    pub name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub search_blob: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RouteRecord {
    pub global_id: u64,
    pub feed_index: u16,
    pub feed_id: String,
    pub local_id: String,
    pub id: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub route_type: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TripRecord {
    pub global_id: u64,
    pub feed_index: u16,
    pub feed_id: String,
    pub local_id: String,
    pub id: String,
    pub route_index: usize,
    pub shape_id: Option<String>,
    pub headsign: Option<String>,
    pub stop_times: Vec<TripStopRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TripStopRecord {
    pub stop_index: usize,
    pub arrival_secs: i32,
    pub departure_secs: i32,
    pub stop_sequence: u32,
    pub shape_dist_traveled: Option<f32>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LineRecord {
    pub stop_indices: Vec<usize>,
    pub trip_indices: Vec<usize>,
    pub scheduled_departures_by_stop: Vec<Vec<i32>>,
    pub chronos_bucket_start_indices_by_stop: Vec<Vec<u32>>,
    pub binary_searchable_by_stop: Vec<bool>,
    pub trip_order_indirection_by_stop: Vec<Vec<u32>>,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct StopLineRef {
    pub line_index: usize,
    pub stop_pos: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalkTransfer {
    pub to_stop: usize,
    pub duration_secs: i32,
    pub distance_meters: f64,
    pub polyline: Vec<PolylinePoint>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ShapePoint {
    pub lat: f64,
    pub lon: f64,
    pub dist_traveled: Option<f32>,
}

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub from: Option<String>,
    pub to: Option<String>,
    pub from_gid: Option<u64>,
    pub to_gid: Option<u64>,
    pub from_lat: Option<f64>,
    pub from_lon: Option<f64>,
    pub to_lat: Option<f64>,
    pub to_lon: Option<f64>,
    pub date: String,
    pub time: String,
    pub max_transfers: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct EngineStats {
    pub build: BuildStats,
    pub realtime: RealtimeDebugSnapshot,
    pub memoization: ProfileCacheStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct BuildStats {
    pub manifest_path: Option<String>,
    pub static_gtfs_path: String,
    pub feed_count: usize,
    pub feed_ids: Vec<String>,
    pub static_reload_interval_secs: u64,
    pub static_reload_strategy: &'static str,
    pub static_update_strategy: &'static str,
    pub static_diff_tolerance: f64,
    pub static_divergence_ratio: f64,
    pub static_mutated_entities: usize,
    pub static_total_entities: usize,
    pub cold_store_strategy: &'static str,
    pub osm_pbf_path: String,
    pub static_gtfs_bytes: u64,
    pub osm_pbf_bytes: u64,
    pub stops: usize,
    pub routes: usize,
    pub trips: usize,
    pub lines: usize,
    pub transfers: usize,
    pub service_days: usize,
    pub service_date_start: Option<String>,
    pub service_date_end: Option<String>,
    pub walk_radius_meters: f64,
    pub walk_speed_mps: f64,
    pub walk_strategy: &'static str,
    pub coordinate_query_strategy: &'static str,
    pub dvni_knn_candidates: usize,
    pub dvni_max_walk_radius_meters: f64,
    pub hpf_strategy: &'static str,
    pub hpf_cache_hit: bool,
    pub hpf_covered_nodes: usize,
    pub hpf_anchored_stops: usize,
    pub hpf_max_distance_meters: f64,
    pub hpf_snap_tolerance_meters: f64,
    pub hpf_snap_quadratic_kappa_meters: f64,
    pub static_cache_hit: bool,
    pub static_cache_bytes: u64,
    pub walk_cache_hit: bool,
    pub walk_graph_nodes: usize,
    pub walk_graph_edges: usize,
    pub walk_anchored_stops: usize,
    pub timings: BuildTimings,
    pub build_millis: u128,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct BuildTimings {
    pub static_cache_read_ms: u128,
    pub gtfs_parse_ms: u128,
    pub transit_model_ms: u128,
    pub static_cache_write_ms: u128,
    pub cold_store_ms: u128,
    pub walker_ms: u128,
    pub hpf_ms: u128,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct StaticDiffSummary {
    pub differential_applied: bool,
    pub divergence_ratio: f64,
    pub mutated_entities: usize,
    pub total_entities: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct StopSearchResult {
    pub global_id: u64,
    pub feed_id: String,
    pub local_id: String,
    pub id: String,
    pub code: Option<String>,
    pub name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub is_virtual: bool,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub from: StopSearchResult,
    pub to: StopSearchResult,
    pub departure_time: String,
    pub arrival_time: String,
    pub duration_seconds: i32,
    pub transfers: usize,
    pub legs: Vec<LegResponse>,
    pub deferred_hydration: DeferredHydrationResponse,
    pub trace: QueryTrace,
}

#[derive(Debug, Serialize)]
pub struct QueryTrace {
    pub service_date: String,
    pub query_runtime_ms: u128,
    pub active_services: usize,
    pub shadow_delta_count: usize,
    pub canceled_trip_count: usize,
    pub timings: QueryTimingBreakdown,
    pub round_timing_totals_us: RaptorRoundTimingBreakdownUs,
    pub coordinate_routing: Option<CoordinateRoutingTrace>,
    pub counters: QueryPerformanceCounters,
    pub rounds: Vec<QueryRoundTrace>,
}

#[derive(Debug, Serialize)]
pub struct CoordinateRoutingTrace {
    pub source_virtualized: bool,
    pub destination_virtualized: bool,
    pub source_seed_count: usize,
    pub destination_seed_count: usize,
    pub connector_strategy: String,
    pub source_asymptotic_connectors: usize,
    pub destination_asymptotic_connectors: usize,
}

#[derive(Debug, Serialize)]
pub struct QueryRoundTrace {
    pub round: usize,
    pub marked_stops: usize,
    pub queued_lines: usize,
    pub lines_scanned: usize,
    pub improvements: usize,
    pub best_before_clone_ms: u128,
    pub queue_build_ms: u128,
    pub line_scan_ms: u128,
    pub transfer_relax_ms: u128,
    pub timings_us: RaptorRoundTimingBreakdownUs,
    pub stop_positions_scanned: usize,
    pub onboard_arrival_evaluations: usize,
    pub actual_arrival_calls: usize,
    pub actual_departure_calls: usize,
    pub skipped_stop_checks: usize,
    pub profile_lookups: usize,
    pub profile_hits: usize,
    pub profile_bound_improvements: usize,
    pub trip_searches: usize,
    pub binary_trip_searches: usize,
    pub trip_departure_checks: usize,
    pub transfer_relaxations: usize,
    pub transfer_improvements: usize,
    pub destination_time: Option<String>,
}

#[derive(Debug, Serialize, Default)]
pub struct QueryTimingBreakdown {
    pub request_setup_ms: u128,
    pub coordinate_projection_ms: u128,
    pub pedestrian_lookup_ms: u128,
    pub trip_mask_ms: u128,
    pub flat_spatial_mask_ms: u128,
    pub state_init_ms: u128,
    pub initial_walk_ms: u128,
    pub profile_lookup_ms: u128,
    pub rounds_ms: u128,
    pub reconstruct_ms: u128,
    pub hydrate_ms: u128,
}

#[derive(Debug, Serialize, Default)]
pub struct QueryPerformanceCounters {
    pub rounds_executed: usize,
    pub queued_lines_total: usize,
    pub chronos_bucket_lookups: usize,
    pub chronos_indirection_lookups: usize,
    pub chronos_bucket_fallback_searches: usize,
    pub chronos_bucket_fallback_end_of_service: usize,
    pub chronos_bucket_fallback_non_monotonic: usize,
    pub chronos_bucket_lookback_secs_total: u64,
    pub chronos_bucket_lookback_secs_max: i32,
    pub flat_spatial_mask_populated_sources: usize,
    pub flat_spatial_mask_materialized_matches: usize,
    pub flat_spatial_mask_checks: usize,
    pub flat_spatial_mask_hits: usize,
    pub stop_positions_scanned: usize,
    pub onboard_arrival_evaluations: usize,
    pub actual_arrival_calls: usize,
    pub actual_departure_calls: usize,
    pub skipped_stop_checks: usize,
    pub trip_searches: usize,
    pub binary_trip_searches: usize,
    pub trip_departure_checks: usize,
    pub transfer_relaxations: usize,
    pub transfer_improvements: usize,
    pub profile_lookups: usize,
    pub profile_hits: usize,
    pub profile_bound_improvements: usize,
    pub destination_bound_prunes: usize,
}

#[derive(Debug, Serialize, Default, Clone, Copy)]
pub struct RaptorRoundTimingBreakdownUs {
    pub round_total_us: u128,
    pub round_other_us: u128,
    pub best_before_clone_us: u128,
    pub queue_build_us: u128,
    pub line_scan_us: u128,
    pub line_scan_other_us: u128,
    pub line_scan_onboard_us: u128,
    pub line_scan_trip_search_us: u128,
    pub line_scan_trip_search_binary_partition_us: u128,
    pub line_scan_trip_search_binary_scan_us: u128,
    pub line_scan_trip_search_linear_scan_us: u128,
    pub line_scan_candidate_compare_us: u128,
    pub destination_egress_pre_transfer_us: u128,
    pub profile_lookup_pre_transfer_us: u128,
    pub transfer_relax_us: u128,
    pub destination_egress_post_transfer_us: u128,
    pub profile_lookup_post_transfer_us: u128,
}

impl RaptorRoundTimingBreakdownUs {
    fn accumulate(&mut self, other: &Self) {
        self.round_total_us += other.round_total_us;
        self.round_other_us += other.round_other_us;
        self.best_before_clone_us += other.best_before_clone_us;
        self.queue_build_us += other.queue_build_us;
        self.line_scan_us += other.line_scan_us;
        self.line_scan_other_us += other.line_scan_other_us;
        self.line_scan_onboard_us += other.line_scan_onboard_us;
        self.line_scan_trip_search_us += other.line_scan_trip_search_us;
        self.line_scan_trip_search_binary_partition_us +=
            other.line_scan_trip_search_binary_partition_us;
        self.line_scan_trip_search_binary_scan_us += other.line_scan_trip_search_binary_scan_us;
        self.line_scan_trip_search_linear_scan_us += other.line_scan_trip_search_linear_scan_us;
        self.line_scan_candidate_compare_us += other.line_scan_candidate_compare_us;
        self.destination_egress_pre_transfer_us += other.destination_egress_pre_transfer_us;
        self.profile_lookup_pre_transfer_us += other.profile_lookup_pre_transfer_us;
        self.transfer_relax_us += other.transfer_relax_us;
        self.destination_egress_post_transfer_us += other.destination_egress_post_transfer_us;
        self.profile_lookup_post_transfer_us += other.profile_lookup_post_transfer_us;
    }
}

#[derive(Default)]
struct RoundMetrics {
    timings_us: RaptorRoundTimingBreakdownUs,
    queued_lines: usize,
    lines_scanned: usize,
    chronos_bucket_lookups: usize,
    chronos_indirection_lookups: usize,
    chronos_bucket_fallback_searches: usize,
    chronos_bucket_fallback_end_of_service: usize,
    chronos_bucket_fallback_non_monotonic: usize,
    chronos_bucket_lookback_secs_total: u64,
    chronos_bucket_lookback_secs_max: i32,
    flat_spatial_mask_checks: usize,
    flat_spatial_mask_hits: usize,
    stop_positions_scanned: usize,
    onboard_arrival_evaluations: usize,
    actual_arrival_calls: usize,
    actual_departure_calls: usize,
    skipped_stop_checks: usize,
    profile_lookups: usize,
    profile_hits: usize,
    profile_bound_improvements: usize,
    trip_searches: usize,
    binary_trip_searches: usize,
    trip_departure_checks: usize,
    transfer_relaxations: usize,
    transfer_improvements: usize,
    destination_bound_prunes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct LegResponse {
    pub kind: &'static str,
    pub departure_time: String,
    pub arrival_time: String,
    pub duration_seconds: i32,
    pub from_stop: StopSearchResult,
    pub to_stop: StopSearchResult,
    pub trip_gid: Option<u64>,
    pub route_gid: Option<u64>,
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub route_label: Option<String>,
    pub route_type: Option<String>,
    pub route_color: Option<String>,
    pub route_text_color: Option<String>,
    pub headsign: Option<String>,
    pub walk_distance_meters: Option<f64>,
    pub delay_applied_seconds: Option<i32>,
    pub polyline: Vec<PolylinePoint>,
}

#[derive(Debug, Serialize)]
pub struct DeferredHydrationResponse {
    pub legs: Vec<DeferredLegRef>,
    pub entities: HydrationEntityDictionary,
}

#[derive(Debug, Serialize)]
pub struct DeferredLegRef {
    pub kind: &'static str,
    pub departure_time: String,
    pub arrival_time: String,
    pub duration_seconds: i32,
    pub from_stop_gid: u64,
    pub to_stop_gid: u64,
    pub trip_gid: Option<u64>,
    pub route_gid: Option<u64>,
    pub headsign: Option<String>,
    pub walk_distance_meters: Option<f64>,
    pub delay_applied_seconds: Option<i32>,
    pub polyline_index: usize,
}

#[derive(Debug, Serialize, Default)]
pub struct HydrationEntityDictionary {
    pub stops: Vec<StopSearchResult>,
    pub routes: Vec<RouteHydration>,
    pub trips: Vec<TripHydration>,
    pub polylines: Vec<Vec<PolylinePoint>>,
}

#[derive(Debug, Serialize)]
pub struct RouteHydration {
    pub global_id: u64,
    pub id: String,
    pub label: String,
    pub route_type: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TripHydration {
    pub global_id: u64,
    pub id: String,
    pub headsign: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolylinePoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Clone, Debug)]
struct CoordinatePoint {
    latitude: f64,
    longitude: f64,
}

#[derive(Clone, Debug)]
enum QueryEndpointInput {
    Stop {
        stop_index: usize,
        display: StopSearchResult,
    },
    Coordinate(CoordinatePoint),
}

#[derive(Clone, Debug)]
struct ProjectedStopCandidate {
    stop_index: usize,
    distance_meters: f64,
}

#[derive(Clone, Debug)]
struct QueryVirtualWalk {
    from_stop: usize,
    to_stop: usize,
    duration_secs: i32,
    distance_meters: f64,
    polyline: Vec<PolylinePoint>,
}

#[derive(Clone, Debug, Default)]
struct QueryOverlay {
    virtual_stops: HashMap<usize, StopSearchResult>,
    virtual_walks: HashMap<(usize, usize), QueryVirtualWalk>,
}

#[derive(Clone, Debug)]
struct QueryPlan {
    from_index: usize,
    to_index: usize,
    display_from: StopSearchResult,
    display_to: StopSearchResult,
    exact_destination_stop: Option<usize>,
    destination_cells: Vec<u64>,
    destination_cell_for_insert: Option<u64>,
    source_access_edges: Vec<QueryVirtualWalk>,
    destination_egress_edges: Vec<QueryVirtualWalk>,
    overlay: QueryOverlay,
}

#[derive(Clone, Debug)]
struct QueryPlanMetrics {
    coordinate_projection_ms: u128,
    pedestrian_lookup_ms: u128,
    source_virtualized: bool,
    destination_virtualized: bool,
    source_seed_count: usize,
    destination_seed_count: usize,
    connector_strategy: String,
    source_asymptotic_connectors: usize,
    destination_asymptotic_connectors: usize,
}

impl Default for QueryPlanMetrics {
    fn default() -> Self {
        Self {
            coordinate_projection_ms: 0,
            pedestrian_lookup_ms: 0,
            source_virtualized: false,
            destination_virtualized: false,
            source_seed_count: 0,
            destination_seed_count: 0,
            connector_strategy: "discrete-stop-query".to_owned(),
            source_asymptotic_connectors: 0,
            destination_asymptotic_connectors: 0,
        }
    }
}

#[derive(Clone, Debug)]
enum ParentStep {
    Origin,
    Walk {
        from_stop: usize,
        duration_secs: i32,
        distance_meters: f64,
    },
    Transit {
        trip_index: usize,
        board_stop: usize,
        board_pos: usize,
        alight_stop: usize,
        alight_pos: usize,
    },
    Memoized {
        from_stop: usize,
        source_round: usize,
        legs: Arc<Vec<CachedLeg>>,
    },
}

#[derive(Clone, Debug)]
enum RawLeg {
    Walk {
        from_stop: usize,
        to_stop: usize,
        departure_secs: i32,
        arrival_secs: i32,
        duration_secs: i32,
        distance_meters: f64,
    },
    Transit {
        trip_index: usize,
        board_stop: usize,
        board_pos: usize,
        alight_stop: usize,
        alight_pos: usize,
        departure_secs: i32,
        arrival_secs: i32,
    },
}

#[derive(Clone)]
struct LocalSubqueryResult {
    arrival_secs: i32,
    transit_legs: usize,
    legs: Vec<CachedLeg>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LocalSubqueryKey {
    from_stop: usize,
    to_stop: usize,
    departure_secs: i32,
    remaining_transit_legs: usize,
}

#[derive(Clone, Eq)]
struct LineKey {
    route_index: usize,
    stop_indices: Vec<usize>,
}

impl PartialEq for LineKey {
    fn eq(&self, other: &Self) -> bool {
        self.route_index == other.route_index && self.stop_indices == other.stop_indices
    }
}

impl Hash for LineKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.route_index.hash(state);
        self.stop_indices.hash(state);
    }
}

#[derive(Clone)]
struct IndexedStopPoint {
    index: usize,
    point: [f64; 2],
}

impl RTreeObject for IndexedStopPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.point)
    }
}

impl PointDistance for IndexedStopPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.point[0] - point[0];
        let dy = self.point[1] - point[1];
        (dx * dx) + (dy * dy)
    }
}

impl EngineConfig {
    pub fn from_env(workspace_root: PathBuf) -> Result<Self> {
        if let Some(manifest_override) = env::var("ALPHA_CONFIG").ok() {
            return Self::from_manifest(
                &workspace_root,
                resolve_path(
                    &workspace_root,
                    Some(manifest_override),
                    DEFAULT_MANIFEST_NAME,
                ),
                StaticGtfsRefreshMode::Bootstrap,
            );
        }

        let default_manifest = workspace_root.join(DEFAULT_MANIFEST_NAME);
        if default_manifest.exists() {
            return Self::from_manifest(
                &workspace_root,
                default_manifest,
                StaticGtfsRefreshMode::Bootstrap,
            );
        }

        Self::from_legacy_env(workspace_root, StaticGtfsRefreshMode::Bootstrap)
    }

    pub fn reload_from_source(&self) -> Result<Self> {
        if let Some(manifest_path) = &self.manifest_path {
            return Self::from_manifest(
                &self.workspace_root,
                manifest_path.clone(),
                StaticGtfsRefreshMode::Poll,
            );
        }

        Self::from_legacy_env(self.workspace_root.clone(), StaticGtfsRefreshMode::Poll)
    }

    pub fn static_inputs_changed(&self, other: &Self) -> bool {
        self.static_inputs_metadata != other.static_inputs_metadata
    }

    fn from_manifest(
        workspace_root: &PathBuf,
        manifest_path: PathBuf,
        refresh_mode: StaticGtfsRefreshMode,
    ) -> Result<Self> {
        let manifest_body = fs::read_to_string(&manifest_path)
            .with_context(|| format!("unable to read manifest {}", manifest_path.display()))?;
        let manifest: RawEngineManifest = toml::from_str(&manifest_body)
            .with_context(|| format!("invalid manifest TOML at {}", manifest_path.display()))?;
        if manifest.feeds.is_empty() {
            bail!(
                "manifest {} does not define any feeds",
                manifest_path.display()
            );
        }
        if manifest.feeds.len() > usize::from(u16::MAX) + 1 {
            bail!("manifest defines too many feeds for 16-bit feed indices");
        }

        let manifest_dir = manifest_path
            .parent()
            .unwrap_or_else(|| workspace_root.as_path())
            .to_path_buf();
        let mut seen_ids = HashSet::<String>::new();
        let mut feeds = Vec::with_capacity(manifest.feeds.len());
        for (position, raw_feed) in manifest.feeds.into_iter().enumerate() {
            validate_feed_id(&raw_feed.id)?;
            if !seen_ids.insert(raw_feed.id.clone()) {
                bail!("duplicate feed id {} in manifest", raw_feed.id);
            }

            let prepared_static_gtfs = prepare_static_gtfs_source(
                workspace_root,
                &manifest_dir,
                &raw_feed.id,
                &raw_feed.static_gtfs,
                raw_feed.static_gtfs_allow_invalid_tls,
                refresh_mode,
            )?;

            feeds.push(FeedConfig {
                feed_index: position as u16,
                id: raw_feed.id,
                static_gtfs_path: prepared_static_gtfs.local_path,
                static_gtfs_source: prepared_static_gtfs.source,
                static_gtfs_remote_url: prepared_static_gtfs.remote_url,
                static_gtfs_allow_invalid_tls: prepared_static_gtfs.allow_invalid_tls,
                trip_updates_url: raw_feed.trip_updates_url,
                vehicle_positions_url: raw_feed.vehicle_positions_url,
                depends_on: raw_feed.depends_on,
            });
        }
        validate_feed_dependencies(&feeds)?;

        let mut config = Self {
            workspace_root: workspace_root.clone(),
            manifest_path: Some(manifest_path),
            feeds,
            osm_pbf_path: manifest
                .osm_pbf
                .map(|path| resolve_path_from(&manifest_dir, &path))
                .unwrap_or_else(|| default_osm_pbf_path(workspace_root)),
            walk_radius_meters: manifest.walk_radius_meters.unwrap_or(450.0),
            walk_speed_mps: manifest.walk_speed_mps.unwrap_or(1.35),
            max_transfer_candidates: manifest.max_transfer_candidates.unwrap_or(12),
            refresh_interval_secs: manifest.refresh_interval_secs.unwrap_or(45),
            static_reload_interval_secs: manifest.static_reload_interval_secs.unwrap_or(600),
            static_diff_tolerance: manifest
                .static_diff_tolerance
                .unwrap_or(DEFAULT_STATIC_DIFF_TOLERANCE),
            default_max_transfers: manifest.default_max_transfers.unwrap_or(4),
            dvni_knn_candidates: manifest
                .dvni
                .as_ref()
                .and_then(|dvni| dvni.knn_candidates)
                .unwrap_or(DEFAULT_DVNI_KNN_CANDIDATES)
                .clamp(1, 16),
            dvni_max_walk_radius_meters: manifest
                .dvni
                .as_ref()
                .and_then(|dvni| dvni.max_walk_radius_meters)
                .unwrap_or(DEFAULT_DVNI_MAX_WALK_RADIUS_METERS)
                .clamp(50.0, 5_000.0),
            hpf_max_distance_meters: manifest
                .hpf
                .as_ref()
                .and_then(|hpf| hpf.max_distance_meters)
                .unwrap_or(DEFAULT_HPF_MAX_DISTANCE_METERS)
                .clamp(250.0, 20_000.0),
            hpf_snap_tolerance_meters: manifest
                .hpf
                .as_ref()
                .and_then(|hpf| hpf.snap_tolerance_meters)
                .unwrap_or(DEFAULT_HPF_SNAP_TOLERANCE_METERS)
                .clamp(25.0, 1_000.0),
            hpf_snap_quadratic_kappa_meters: manifest
                .hpf
                .as_ref()
                .and_then(|hpf| hpf.snap_quadratic_kappa_meters)
                .unwrap_or(DEFAULT_HPF_SNAP_QUADRATIC_KAPPA_METERS)
                .clamp(5.0, 5_000.0),
            hpf_search_window: manifest
                .hpf
                .as_ref()
                .and_then(|hpf| hpf.search_window)
                .unwrap_or(DEFAULT_HPF_SEARCH_WINDOW)
                .clamp(64, 16_384),
            static_inputs_metadata: StaticCacheMetadata {
                schema_version: STATIC_CACHE_SCHEMA_VERSION,
                manifest_path: None,
                manifest_modified_unix_secs: None,
                feed_sources: Vec::new(),
            },
        };
        config.static_inputs_metadata =
            capture_static_inputs_metadata(config.manifest_path.as_ref(), &config.feeds)?;
        Ok(config)
    }

    fn from_legacy_env(
        workspace_root: PathBuf,
        refresh_mode: StaticGtfsRefreshMode,
    ) -> Result<Self> {
        let feed_id = env::var("ALPHA_DEFAULT_FEED_ID").unwrap_or_else(|_| "roma".to_owned());
        validate_feed_id(&feed_id)?;

        let static_gtfs_value = env::var("ALPHA_STATIC_GTFS").ok();
        let static_gtfs_allow_invalid_tls = env::var("ALPHA_STATIC_GTFS_ALLOW_INVALID_TLS")
            .ok()
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let prepared_static_gtfs = prepare_static_gtfs_source_legacy(
            &workspace_root,
            &feed_id,
            static_gtfs_value,
            "data/gtfs/rome_static_gtfs.zip",
            static_gtfs_allow_invalid_tls,
            refresh_mode,
        )?;

        let mut config = Self {
            workspace_root: workspace_root.clone(),
            manifest_path: None,
            feeds: vec![FeedConfig {
                feed_index: 0,
                id: feed_id,
                static_gtfs_path: prepared_static_gtfs.local_path,
                static_gtfs_source: prepared_static_gtfs.source,
                static_gtfs_remote_url: prepared_static_gtfs.remote_url,
                static_gtfs_allow_invalid_tls: prepared_static_gtfs.allow_invalid_tls,
                trip_updates_url: Some(env::var("ALPHA_TRIP_UPDATES_URL").unwrap_or_else(|_| {
                    "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb"
                        .to_owned()
                })),
                vehicle_positions_url: Some(
                    env::var("ALPHA_VEHICLE_POSITIONS_URL").unwrap_or_else(|_| {
                        "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"
                            .to_owned()
                    }),
                ),
                depends_on: Vec::new(),
            }],
            osm_pbf_path: resolve_path(
                &workspace_root,
                env::var("ALPHA_OSM_PBF").ok(),
                "data/osm/lazio-latest.osm.pbf",
            ),
            walk_radius_meters: env::var("ALPHA_WALK_RADIUS_M")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(450.0),
            walk_speed_mps: env::var("ALPHA_WALK_SPEED_MPS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(1.35),
            max_transfer_candidates: env::var("ALPHA_MAX_WALK_NEIGHBORS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(12),
            refresh_interval_secs: env::var("ALPHA_RT_REFRESH_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(45),
            static_reload_interval_secs: env::var("ALPHA_STATIC_POLL_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(600),
            static_diff_tolerance: env::var("ALPHA_STATIC_DIFF_TOLERANCE")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_STATIC_DIFF_TOLERANCE),
            default_max_transfers: env::var("ALPHA_MAX_TRANSFERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(4),
            dvni_knn_candidates: env::var("ALPHA_DVNI_KNN")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_DVNI_KNN_CANDIDATES)
                .clamp(1, 16),
            dvni_max_walk_radius_meters: env::var("ALPHA_DVNI_MAX_WALK_RADIUS_M")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_DVNI_MAX_WALK_RADIUS_METERS)
                .clamp(50.0, 5_000.0),
            hpf_max_distance_meters: env::var("ALPHA_HPF_MAX_DISTANCE_M")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_HPF_MAX_DISTANCE_METERS)
                .clamp(250.0, 20_000.0),
            hpf_snap_tolerance_meters: env::var("ALPHA_HPF_SNAP_TOLERANCE_M")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_HPF_SNAP_TOLERANCE_METERS)
                .clamp(25.0, 1_000.0),
            hpf_snap_quadratic_kappa_meters: env::var("ALPHA_HPF_SNAP_QUADRATIC_KAPPA_M")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_HPF_SNAP_QUADRATIC_KAPPA_METERS)
                .clamp(5.0, 5_000.0),
            hpf_search_window: env::var("ALPHA_HPF_SEARCH_WINDOW")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_HPF_SEARCH_WINDOW)
                .clamp(64, 16_384),
            static_inputs_metadata: StaticCacheMetadata {
                schema_version: STATIC_CACHE_SCHEMA_VERSION,
                manifest_path: None,
                manifest_modified_unix_secs: None,
                feed_sources: Vec::new(),
            },
        };
        config.static_inputs_metadata =
            capture_static_inputs_metadata(config.manifest_path.as_ref(), &config.feeds)?;
        Ok(config)
    }

    pub fn static_sources_display(&self) -> String {
        self.feeds
            .iter()
            .map(|feed| {
                if let Some(url) = &feed.static_gtfs_remote_url {
                    format!(
                        "{}={} -> {}",
                        feed.id,
                        url,
                        feed.static_gtfs_path.display()
                    )
                } else {
                    format!("{}={}", feed.id, feed.static_gtfs_path.display())
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn trip_updates_display(&self) -> String {
        self.feeds
            .iter()
            .filter_map(|feed| {
                feed.trip_updates_url
                    .as_ref()
                    .map(|url| format!("{}={url}", feed.id))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn vehicle_positions_display(&self) -> String {
        self.feeds
            .iter()
            .filter_map(|feed| {
                feed.vehicle_positions_url
                    .as_ref()
                    .map(|url| format!("{}={url}", feed.id))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Engine {
    pub fn load(config: EngineConfig) -> Result<Self> {
        Self::load_internal(config, None)
    }

    pub fn reload_from_previous(previous: &Engine, config: EngineConfig) -> Result<Self> {
        Self::load_internal(config, Some(previous))
    }

    fn load_internal(config: EngineConfig, previous: Option<&Engine>) -> Result<Self> {
        let started = Instant::now();
        let static_core_result = load_or_build_static_core(&config, previous)?;
        let cold_store_started = Instant::now();
        let cold_generation = static_metadata_generation_token(&config.static_inputs_metadata);
        let cold_store = Arc::new(ColdStore::load_or_build(
            &cold_store_paths(&static_cache_path(&config), cold_generation),
            cold_generation,
            &static_core_result.core.stops,
            &static_core_result.core.routes,
            &static_core_result.core.trips,
            &static_core_result.core.shapes,
        )?);
        let cold_store_ms = cold_store_started.elapsed().as_millis();
        let StaticCore {
            feeds,
            stops,
            active_stop_indices,
            stop_lookup,
            stop_lookup_by_global_id,
            routes,
            active_route_indices,
            trips,
            active_trip_indices,
            trip_lookup_by_feed,
            service_to_trip_indices,
            lines,
            stop_to_lines,
            service_by_date,
            shapes: _,
        } = static_core_result.core;

        let walker_started = Instant::now();
        let walker_build = match (
            previous,
            static_core_result.diff_summary.differential_applied,
        ) {
            (Some(previous_engine), true) => match rebuild_differential_walker_transfers(
                &config,
                &previous_engine.static_data,
                &stops,
                &active_stop_indices,
            ) {
                Ok(result) => result,
                Err(error) => {
                    warn!(
                        %error,
                        "differential walker refresh failed, rebuilding full walker matrix"
                    );
                    build_full_walker_matrix(&config, &stops)
                }
            },
            _ => build_full_walker_matrix(&config, &stops),
        };
        let walker_millis = walker_started.elapsed().as_millis();
        let transfers = walker_build.transfers;
        let total_transfers = transfers.iter().map(Vec::len).sum();
        let mut service_days: Vec<_> = service_by_date.keys().copied().collect();
        service_days.sort_unstable();

        let timings = BuildTimings {
            cold_store_ms,
            walker_ms: walker_millis,
            ..static_core_result.timings.clone()
        };

        let hpf_started = Instant::now();
        let hpf_build = build_or_load_hpf(
            &config.osm_pbf_path,
            &runtime_cache_dir(&config.workspace_root, "osm"),
            &stops,
            config.hpf_max_distance_meters,
            config.walk_speed_mps,
            config.hpf_snap_tolerance_meters,
            config.hpf_snap_quadratic_kappa_meters,
            config.hpf_search_window,
        );
        let hpf_millis = hpf_started.elapsed().as_millis();
        let (hpf, hpf_strategy, hpf_cache_hit, hpf_covered_nodes, hpf_anchored_stops) =
            match hpf_build {
                Ok(result) => (
                    Some(result.forest),
                    result.strategy,
                    result.cache_hit,
                    result.covered_nodes,
                    result.anchored_stops,
                ),
                Err(error) => {
                    warn!(%error, "failed to build HPF, falling back to stop-level coordinate connectors");
                    (None, "hpf-unavailable-stop-knn-fallback", false, 0, 0)
                }
            };

        let stop_cells = build_stop_cells(&stops, &active_stop_indices);
        let coordinate_query_strategy = if hpf.is_some() {
            "dvni+hpf-local-forest"
        } else {
            "dvni+stop-knn-fallback"
        };

        let timings = BuildTimings {
            hpf_ms: hpf_millis,
            ..timings
        };

        let build_stats = BuildStats {
            manifest_path: config
                .manifest_path
                .as_ref()
                .map(|path| path.display().to_string()),
            static_gtfs_path: config.static_sources_display(),
            feed_count: config.feeds.len(),
            feed_ids: config.feeds.iter().map(|feed| feed.id.clone()).collect(),
            static_reload_interval_secs: config.static_reload_interval_secs,
            static_reload_strategy: "atomic-pointer-swap-generational",
            static_update_strategy: static_core_result.update_strategy,
            static_diff_tolerance: config.static_diff_tolerance,
            static_divergence_ratio: static_core_result.diff_summary.divergence_ratio,
            static_mutated_entities: static_core_result.diff_summary.mutated_entities,
            static_total_entities: static_core_result.diff_summary.total_entities,
            cold_store_strategy: "hydra-slab-direct-index+mmap-data",
            osm_pbf_path: config.osm_pbf_path.display().to_string(),
            static_gtfs_bytes: config
                .feeds
                .iter()
                .map(|feed| file_size(&feed.static_gtfs_path))
                .sum(),
            osm_pbf_bytes: file_size(&config.osm_pbf_path),
            stops: active_stop_indices.len(),
            routes: active_route_indices.len(),
            trips: active_trip_indices.len(),
            lines: lines.len(),
            transfers: total_transfers,
            service_days: service_by_date.len(),
            service_date_start: service_days.first().map(ToString::to_string),
            service_date_end: service_days.last().map(ToString::to_string),
            walk_radius_meters: config.walk_radius_meters,
            walk_speed_mps: config.walk_speed_mps,
            walk_strategy: walker_build.strategy,
            coordinate_query_strategy,
            dvni_knn_candidates: config.dvni_knn_candidates,
            dvni_max_walk_radius_meters: config.dvni_max_walk_radius_meters,
            hpf_strategy,
            hpf_cache_hit,
            hpf_covered_nodes,
            hpf_anchored_stops,
            hpf_max_distance_meters: config.hpf_max_distance_meters,
            hpf_snap_tolerance_meters: config.hpf_snap_tolerance_meters,
            hpf_snap_quadratic_kappa_meters: config.hpf_snap_quadratic_kappa_meters,
            static_cache_hit: static_core_result.cache_hit,
            static_cache_bytes: static_core_result.cache_bytes,
            walk_cache_hit: walker_build.cache_hit,
            walk_graph_nodes: walker_build.graph_nodes,
            walk_graph_edges: walker_build.graph_edges,
            walk_anchored_stops: walker_build.anchored_stops,
            timings,
            build_millis: started.elapsed().as_millis(),
        };

        let static_data = Arc::new(StaticData {
            feeds,
            stops,
            active_stop_indices,
            stop_lookup,
            stop_lookup_by_global_id,
            routes,
            active_route_indices,
            trips,
            active_trip_indices,
            trip_lookup_by_feed,
            service_to_trip_indices,
            lines,
            stop_to_lines,
            transfers,
            stop_cells,
            service_by_date,
            cold_store,
            build_stats,
        });

        Ok(Self {
            config: Arc::new(config),
            static_data,
            realtime: RealtimeStore::new(),
            profile_cache: ProfileCache::new(),
            hpf,
        })
    }

    pub async fn refresh_realtime(&self) -> Result<RealtimeDebugSnapshot> {
        let refresh = self.realtime.refresh(&self.static_data, &self.config).await?;
        let invalidation = self
            .profile_cache
            .invalidate_trips(&refresh.changed_trip_indices);
        if invalidation.invalidated_points > 0 || invalidation.invalidated_keys > 0 {
            info!(
                changed_trip_count = refresh.changed_trip_indices.len(),
                invalidated_profile_points = invalidation.invalidated_points,
                invalidated_profile_keys = invalidation.invalidated_keys,
                bloom_checks = invalidation.bloom_checks,
                "invalidated memoized destination profiles after realtime refresh"
            );
        }

        if let Some(error) = refresh.terminal_error {
            return Err(anyhow!(error));
        }

        Ok(refresh.snapshot)
    }

    pub fn stats(&self) -> EngineStats {
        EngineStats {
            build: self.static_data.build_stats.clone(),
            realtime: self.realtime.snapshot(&self.static_data, 16),
            memoization: self.profile_cache.snapshot(),
        }
    }

    pub fn realtime_snapshot(&self, limit: usize) -> RealtimeDebugSnapshot {
        self.realtime.snapshot(&self.static_data, limit)
    }

    pub fn search_stops(&self, query: &str, limit: usize) -> Vec<StopSearchResult> {
        let needle = query.trim().to_lowercase();
        let mut ranked = Vec::<(usize, usize)>::new();
        for &index in &self.static_data.active_stop_indices {
            let stop = &self.static_data.stops[index];
            if needle.is_empty() {
                ranked.push((0, index));
                continue;
            }
            let mut score = None;
            if stop.id.eq_ignore_ascii_case(query) {
                score = Some(0);
            } else if stop
                .code
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(query))
            {
                score = Some(1);
            } else if stop.search_blob.starts_with(&needle) {
                score = Some(2);
            } else if stop.search_blob.contains(&needle) {
                score = Some(3);
            }

            if let Some(score) = score {
                ranked.push((score, index));
            }
        }

        ranked.sort_by(|left, right| {
            left.0.cmp(&right.0).then_with(|| {
                self.static_data.stops[left.1]
                    .id
                    .cmp(&self.static_data.stops[right.1].id)
            })
        });

        ranked
            .into_iter()
            .take(limit)
            .map(|(_, index)| self.stop_result(index))
            .collect()
    }

    async fn resolve_query_plan(&self, request: &QueryRequest) -> Result<(QueryPlan, QueryPlanMetrics)> {
        let from_input = self.resolve_query_input(
            "origin",
            request.from.as_deref(),
            request.from_gid,
            request.from_lat,
            request.from_lon,
        )?;
        let to_input = self.resolve_query_input(
            "destination",
            request.to.as_deref(),
            request.to_gid,
            request.to_lat,
            request.to_lon,
        )?;

        let static_stop_count = self.static_data.stops.len();
        let mut next_virtual_index = static_stop_count;
        let mut overlay = QueryOverlay::default();
        let mut metrics = QueryPlanMetrics::default();

        let (from_index, display_from, source_access_edges) = match from_input {
            QueryEndpointInput::Stop { stop_index, display } => (stop_index, display, Vec::new()),
            QueryEndpointInput::Coordinate(point) => {
                metrics.source_virtualized = true;
                let virtual_index = next_virtual_index;
                next_virtual_index += 1;
                let display = virtual_stop_result("Origine GPS", point.latitude, point.longitude);
                overlay.virtual_stops.insert(virtual_index, display.clone());
                let edges = self
                    .resolve_virtual_walks(point, virtual_index, true, &mut metrics)
                    .await?;
                for edge in &edges {
                    overlay
                        .virtual_walks
                        .insert((edge.from_stop, edge.to_stop), edge.clone());
                }
                metrics.source_seed_count = edges.len();
                (virtual_index, display, edges)
            }
        };

        let (to_index, display_to, exact_destination_stop, destination_cells, destination_cell_for_insert, destination_egress_edges) =
            match to_input {
                QueryEndpointInput::Stop { stop_index, display } => (
                    stop_index,
                    display,
                    Some(stop_index),
                    self.stop_destination_neighborhood(stop_index),
                    self.stop_destination_cell(stop_index),
                    Vec::new(),
                ),
                QueryEndpointInput::Coordinate(point) => {
                    metrics.destination_virtualized = true;
                    let virtual_index = next_virtual_index;
                    let display = virtual_stop_result(
                        "Destinazione GPS",
                        point.latitude,
                        point.longitude,
                    );
                    overlay.virtual_stops.insert(virtual_index, display.clone());
                    let edges = self
                        .resolve_virtual_walks(point.clone(), virtual_index, false, &mut metrics)
                        .await?;
                    for edge in &edges {
                        overlay
                            .virtual_walks
                            .insert((edge.from_stop, edge.to_stop), edge.clone());
                    }
                    metrics.destination_seed_count = edges.len();
                    (
                        virtual_index,
                        display,
                        None,
                        geo_destination_cell_neighborhood(Some(point.latitude), Some(point.longitude)),
                        geo_destination_cell(Some(point.latitude), Some(point.longitude)),
                        edges,
                    )
                }
            };

        metrics.connector_strategy = if metrics.source_virtualized || metrics.destination_virtualized {
            if self.hpf.is_some() {
                "hpf-local-forest".to_owned()
            } else {
                "stop-knn-haversine-fallback".to_owned()
            }
        } else {
            "discrete-stop-query".to_owned()
        };

        Ok((
            QueryPlan {
                from_index,
                to_index,
                display_from,
                display_to,
                exact_destination_stop,
                destination_cells,
                destination_cell_for_insert,
                source_access_edges,
                destination_egress_edges,
                overlay,
            },
            metrics,
        ))
    }

    fn resolve_query_input(
        &self,
        label: &str,
        stop_id: Option<&str>,
        stop_gid: Option<u64>,
        latitude: Option<f64>,
        longitude: Option<f64>,
    ) -> Result<QueryEndpointInput> {
        let has_stop = stop_id.is_some() || stop_gid.is_some();
        let has_coordinates = latitude.is_some() || longitude.is_some();

        if has_stop && has_coordinates {
            bail!("{label} must be specified either as a stop or as coordinates, not both");
        }

        if has_coordinates {
            let latitude = latitude.ok_or_else(|| anyhow!("{label} latitude is missing"))?;
            let longitude = longitude.ok_or_else(|| anyhow!("{label} longitude is missing"))?;
            if !(-90.0..=90.0).contains(&latitude) {
                bail!("{label} latitude must be between -90 and 90");
            }
            if !(-180.0..=180.0).contains(&longitude) {
                bail!("{label} longitude must be between -180 and 180");
            }
            return Ok(QueryEndpointInput::Coordinate(CoordinatePoint {
                latitude,
                longitude,
            }));
        }

        let stop_index = self.resolve_stop_query(stop_id, stop_gid).with_context(|| {
            match (stop_id, stop_gid) {
                (Some(stop_id), _) => format!("unknown {label} stop {stop_id}"),
                (_, Some(stop_gid)) => format!("unknown {label} stop gid:{stop_gid}"),
                _ => format!("missing {label} stop"),
            }
        })?;
        Ok(QueryEndpointInput::Stop {
            stop_index,
            display: self.stop_result(stop_index),
        })
    }

    async fn resolve_virtual_walks(
        &self,
        point: CoordinatePoint,
        virtual_index: usize,
        is_source: bool,
        metrics: &mut QueryPlanMetrics,
    ) -> Result<Vec<QueryVirtualWalk>> {
        let lookup_started = Instant::now();
        if let Some(hpf) = &self.hpf {
            let base_limit = self.config.dvni_knn_candidates;
            let adaptive_limit = (base_limit.saturating_mul(4)).max(base_limit.saturating_add(4));
            let mut connectors = hpf.query_connectors(
                point.latitude,
                point.longitude,
                base_limit,
                &self.static_data.stops,
            );

            if !connectors.is_empty()
                && adaptive_limit > base_limit
                && connectors
                    .iter()
                    .all(|connector| connector.used_asymptotic_penalty)
            {
                connectors = hpf.query_connectors(
                    point.latitude,
                    point.longitude,
                    adaptive_limit,
                    &self.static_data.stops,
                );
            }

            let walks = connectors
                .into_iter()
                .map(|connector| {
                    let mut polyline = connector.polyline;
                    if !is_source {
                        polyline.reverse();
                    }
                    if connector.used_asymptotic_penalty {
                        if is_source {
                            metrics.source_asymptotic_connectors += 1;
                        } else {
                            metrics.destination_asymptotic_connectors += 1;
                        }
                    }
                    if is_source {
                        QueryVirtualWalk {
                            from_stop: virtual_index,
                            to_stop: connector.stop_index,
                            duration_secs: connector.duration_secs.max(1),
                            distance_meters: connector.distance_meters,
                            polyline,
                        }
                    } else {
                        QueryVirtualWalk {
                            from_stop: connector.stop_index,
                            to_stop: virtual_index,
                            duration_secs: connector.duration_secs.max(1),
                            distance_meters: connector.distance_meters,
                            polyline,
                        }
                    }
                })
                .collect::<Vec<_>>();
            metrics.pedestrian_lookup_ms += lookup_started.elapsed().as_millis();
            if !walks.is_empty() {
                return Ok(walks);
            }
        }

        let projection_started = Instant::now();
        let projected = self.project_coordinate_candidates(
            point.latitude,
            point.longitude,
            self.config.dvni_knn_candidates,
            self.config.dvni_max_walk_radius_meters,
        );
        metrics.coordinate_projection_ms += projection_started.elapsed().as_millis();

        if projected.is_empty() {
            bail!(
                "no GTFS stops found within {:.0} meters of the coordinate",
                self.config.dvni_max_walk_radius_meters
            );
        }

        let walks = self.resolve_projected_walks(point, virtual_index, is_source, &projected);
        metrics.pedestrian_lookup_ms += lookup_started.elapsed().as_millis();

        if walks.is_empty() {
            bail!("coordinate projection did not yield any usable walking connectors");
        }

        Ok(walks)
    }

    fn resolve_projected_walks(
        &self,
        point: CoordinatePoint,
        virtual_index: usize,
        is_source: bool,
        projected: &[ProjectedStopCandidate],
    ) -> Vec<QueryVirtualWalk> {
        let mut walks = projected
            .iter()
            .cloned()
            .map(|candidate| self.virtual_walk_fallback(point.clone(), virtual_index, candidate, is_source))
            .collect::<Vec<_>>();

        walks.sort_by(|left, right| {
            left.duration_secs
                .cmp(&right.duration_secs)
                .then_with(|| left.distance_meters.total_cmp(&right.distance_meters))
                .then_with(|| left.to_stop.cmp(&right.to_stop))
        });
        walks.truncate(self.config.dvni_knn_candidates);
        walks
    }

    fn project_coordinate_candidates(
        &self,
        latitude: f64,
        longitude: f64,
        limit: usize,
        max_radius_meters: f64,
    ) -> Vec<ProjectedStopCandidate> {
        let mut seen = HashSet::<usize>::new();
        let mut candidates = Vec::<usize>::new();

        if let Some(center_cell) = geo_destination_cell(Some(latitude), Some(longitude)) {
            for radius in 0..=2 {
                for cell in geo_destination_cell_window(center_cell, radius) {
                    let Some(stops) = self.static_data.stop_cells.get(&cell) else {
                        continue;
                    };
                    for &stop_index in stops {
                        if seen.insert(stop_index) {
                            candidates.push(stop_index);
                        }
                    }
                }
                if candidates.len() >= limit {
                    break;
                }
            }
        }

        if candidates.len() < limit {
            for &stop_index in &self.static_data.active_stop_indices {
                if seen.insert(stop_index) {
                    candidates.push(stop_index);
                }
            }
        }

        let mut ranked = candidates
            .into_iter()
            .filter_map(|stop_index| {
                let stop = &self.static_data.stops[stop_index];
                let (Some(stop_lat), Some(stop_lon)) = (stop.latitude, stop.longitude) else {
                    return None;
                };
                let distance_meters = haversine_meters(latitude, longitude, stop_lat, stop_lon);
                if distance_meters > max_radius_meters {
                    return None;
                }
                Some(ProjectedStopCandidate {
                    stop_index,
                    distance_meters,
                })
            })
            .collect::<Vec<_>>();

        ranked.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.stop_index.cmp(&right.stop_index))
        });
        ranked.truncate(limit);
        ranked
    }

    fn virtual_walk_fallback(
        &self,
        point: CoordinatePoint,
        virtual_index: usize,
        candidate: ProjectedStopCandidate,
        is_source: bool,
    ) -> QueryVirtualWalk {
        let duration_secs = (candidate.distance_meters / self.config.walk_speed_mps)
            .ceil()
            .max(1.0) as i32;
        let polyline = self.virtual_walk_polyline(point, candidate.stop_index, is_source);

        if is_source {
            QueryVirtualWalk {
                from_stop: virtual_index,
                to_stop: candidate.stop_index,
                duration_secs,
                distance_meters: candidate.distance_meters,
                polyline,
            }
        } else {
            QueryVirtualWalk {
                from_stop: candidate.stop_index,
                to_stop: virtual_index,
                duration_secs,
                distance_meters: candidate.distance_meters,
                polyline,
            }
        }
    }

    fn virtual_walk_polyline(
        &self,
        point: CoordinatePoint,
        stop_index: usize,
        is_source: bool,
    ) -> Vec<PolylinePoint> {
        let stop = &self.static_data.stops[stop_index];
        let (Some(stop_lat), Some(stop_lon)) = (stop.latitude, stop.longitude) else {
            return Vec::new();
        };

        if is_source {
            vec![
                PolylinePoint {
                    lat: point.latitude,
                    lon: point.longitude,
                },
                PolylinePoint {
                    lat: stop_lat,
                    lon: stop_lon,
                },
            ]
        } else {
            vec![
                PolylinePoint {
                    lat: stop_lat,
                    lon: stop_lon,
                },
                PolylinePoint {
                    lat: point.latitude,
                    lon: point.longitude,
                },
            ]
        }
    }

    pub async fn run_query(&self, request: QueryRequest) -> Result<QueryResponse> {
        let query_started = Instant::now();
        let request_setup_started = Instant::now();
        let service_date = NaiveDate::parse_from_str(&request.date, "%Y-%m-%d")
            .with_context(|| format!("invalid date {}", request.date))?;
        let departure_time = parse_clock_time(&request.time)
            .with_context(|| format!("invalid departure time {}", request.time))?;
        let departure_secs = departure_time.num_seconds_from_midnight() as i32;
        let max_transfers = request
            .max_transfers
            .unwrap_or(self.config.default_max_transfers)
            .clamp(0, 8);
        let (plan, plan_metrics) = self.resolve_query_plan(&request).await?;
        let from_index = plan.from_index;
        let to_index = plan.to_index;
        let static_stop_count = self.static_data.stops.len();

        let active_services = self
            .static_data
            .service_by_date
            .get(&service_date)
            .ok_or_else(|| anyhow!("no active services for {}", service_date))?;
        if active_services.is_empty() {
            bail!("service calendar for {} is empty", service_date);
        }

        let request_setup_ms = request_setup_started.elapsed().as_millis();
        let destination_cell = plan.destination_cell_for_insert;
        let destination_cells = plan.destination_cells.clone();

        let trip_mask_started = Instant::now();
        let trip_is_available = self.build_trip_availability(active_services);
        let trip_max_positive_delay_secs = self
            .realtime
            .trip_max_positive_departure_delay_secs(self.static_data.trips.len());
        let mut line_max_positive_delay_secs = vec![0i32; self.static_data.lines.len()];
        for (line_index, line) in self.static_data.lines.iter().enumerate() {
            line_max_positive_delay_secs[line_index] = line
                .trip_indices
                .iter()
                .map(|trip_index| trip_max_positive_delay_secs[*trip_index])
                .max()
                .unwrap_or(0);
        }
        let trip_mask_ms = trip_mask_started.elapsed().as_millis();

        let flat_spatial_mask_started = Instant::now();
        let spatial_flat_mask = self.profile_cache.materialize_spatial_query_surface(
            service_date,
            &destination_cells,
            static_stop_count,
        );
        let flat_spatial_mask_ms = flat_spatial_mask_started.elapsed().as_millis();

        let state_init_started = Instant::now();
        let stop_count = static_stop_count + plan.overlay.virtual_stops.len();
        let line_count = self.static_data.lines.len();
        let rounds = max_transfers + 1;
        let mut global_best = vec![INF_TIME; stop_count];
        let mut improved_round = vec![None::<usize>; stop_count];
        let mut round_arrivals = vec![vec![INF_TIME; stop_count]; rounds + 1];
        let mut parents = vec![vec![None::<ParentStep>; stop_count]; rounds + 1];
        let mut marked_stops = Vec::<usize>::new();
        let mut marked_flags = vec![false; stop_count];
        let mut next_marked = Vec::<usize>::new();
        let mut next_marked_flags = vec![false; stop_count];
        let mut best_before_round = vec![INF_TIME; stop_count];
        let mut queued_line_positions = vec![usize::MAX; line_count];
        let state_init_ms = state_init_started.elapsed().as_millis();

        global_best[from_index] = departure_secs;
        round_arrivals[0][from_index] = departure_secs;
        parents[0][from_index] = Some(ParentStep::Origin);
        improved_round[from_index] = Some(0);
        if from_index < static_stop_count {
            marked_stops.push(from_index);
            marked_flags[from_index] = true;
        }

        let initial_walk_started = Instant::now();
        if plan.source_access_edges.is_empty() {
            for transfer in &self.static_data.transfers[from_index] {
                let candidate = departure_secs + transfer.duration_secs;
                if record_stop_improvement(
                    transfer.to_stop,
                    candidate,
                    &mut global_best,
                    &mut round_arrivals[0],
                    &mut parents[0],
                    ParentStep::Walk {
                        from_stop: from_index,
                        duration_secs: transfer.duration_secs,
                        distance_meters: transfer.distance_meters,
                    },
                    &mut marked_stops,
                    &mut marked_flags,
                ) {
                    improved_round[transfer.to_stop] = Some(0);
                }
            }
        } else {
            for edge in &plan.source_access_edges {
                let candidate = departure_secs + edge.duration_secs;
                if record_stop_improvement(
                    edge.to_stop,
                    candidate,
                    &mut global_best,
                    &mut round_arrivals[0],
                    &mut parents[0],
                    ParentStep::Walk {
                        from_stop: edge.from_stop,
                        duration_secs: edge.duration_secs,
                        distance_meters: edge.distance_meters,
                    },
                    &mut marked_stops,
                    &mut marked_flags,
                ) {
                    improved_round[edge.to_stop] = Some(0);
                }
            }

            let seeded_frontier_len = marked_stops.len();
            for seeded_index in 0..seeded_frontier_len {
                let stop_index = marked_stops[seeded_index];
                let stop_arrival = round_arrivals[0][stop_index];
                if stop_arrival >= INF_TIME || stop_arrival >= global_best[to_index] {
                    continue;
                }
                for transfer in &self.static_data.transfers[stop_index] {
                    let candidate = stop_arrival + transfer.duration_secs;
                    if record_stop_improvement(
                        transfer.to_stop,
                        candidate,
                        &mut global_best,
                        &mut round_arrivals[0],
                        &mut parents[0],
                        ParentStep::Walk {
                            from_stop: stop_index,
                            duration_secs: transfer.duration_secs,
                            distance_meters: transfer.distance_meters,
                        },
                        &mut marked_stops,
                        &mut marked_flags,
                    ) {
                        improved_round[transfer.to_stop] = Some(0);
                    }
                }
            }
        }
        let initial_walk_ms = initial_walk_started.elapsed().as_millis();
        let mut profile_lookup_ms = 0u128;

        let mut trace_rounds = Vec::new();
        let mut round_timing_totals_us = RaptorRoundTimingBreakdownUs::default();
        let mut destination_round = if from_index == to_index { Some(0) } else { None };
        let mut counters = QueryPerformanceCounters::default();
        let mut profile_lookups = 0usize;
        let mut profile_hits = 0usize;
        let mut profile_bound_improvements = 0usize;
        let mut flat_spatial_mask_checks = 0usize;
        let mut flat_spatial_mask_hits = 0usize;
        let mut local_subquery_cache = HashMap::<LocalSubqueryKey, Option<LocalSubqueryResult>>::new();

        if !plan.destination_egress_edges.is_empty() {
            self.apply_virtual_destination_egress(
                to_index,
                0,
                &marked_stops,
                &plan.destination_egress_edges,
                &mut global_best,
                &mut round_arrivals,
                &mut parents,
                &mut destination_round,
            );
        }

        let initial_profile_lookup_started = Instant::now();
        self.apply_profile_lookahead(
            service_date,
            to_index,
            plan.exact_destination_stop,
            &spatial_flat_mask,
            &plan.destination_egress_edges,
            0,
            rounds,
            &marked_stops,
            &trip_is_available,
            &line_max_positive_delay_secs,
            &mut global_best,
            &mut round_arrivals,
            &mut parents,
            &mut destination_round,
            &mut profile_lookups,
            &mut profile_hits,
            &mut profile_bound_improvements,
            &mut counters.destination_bound_prunes,
            &mut flat_spatial_mask_checks,
            &mut flat_spatial_mask_hits,
            &mut local_subquery_cache,
        );
        profile_lookup_ms += initial_profile_lookup_started.elapsed().as_millis();

        let rounds_started = Instant::now();

        for round in 1..=rounds {
            if marked_stops.is_empty() {
                break;
            }

            let round_started = Instant::now();

            let marked_count = marked_stops.len();

            let best_before_clone_started = Instant::now();
            best_before_round.copy_from_slice(&global_best);
            let best_before_clone_ms = best_before_clone_started.elapsed().as_millis();
            let best_before_clone_us = best_before_clone_started.elapsed().as_micros();

            let queue_build_started = Instant::now();
            queued_line_positions.fill(usize::MAX);
            for &stop_index in &marked_stops {
                if best_before_round[stop_index] >= global_best[to_index] {
                    counters.destination_bound_prunes += 1;
                    continue;
                }
                if stop_index >= static_stop_count {
                    continue;
                }
                for line_ref in &self.static_data.stop_to_lines[stop_index] {
                    let queued_pos = &mut queued_line_positions[line_ref.line_index];
                    if line_ref.stop_pos < *queued_pos {
                        *queued_pos = line_ref.stop_pos;
                    }
                }
            }

            next_marked.clear();
            next_marked_flags.fill(false);

            let mut round_metrics = RoundMetrics {
                queued_lines: queued_line_positions
                    .iter()
                    .filter(|&&position| position != usize::MAX)
                    .count(),
                ..RoundMetrics::default()
            };
            let queue_build_ms = queue_build_started.elapsed().as_millis();
            round_metrics.timings_us.best_before_clone_us = best_before_clone_us;
            round_metrics.timings_us.queue_build_us = queue_build_started.elapsed().as_micros();

            let line_scan_started = Instant::now();
            for (line_index, start_pos) in queued_line_positions.iter().copied().enumerate() {
                if start_pos == usize::MAX {
                    continue;
                }
                round_metrics.lines_scanned += 1;
                self.scan_line(
                    line_index,
                    start_pos,
                    to_index,
                    &trip_is_available,
                    line_max_positive_delay_secs[line_index],
                    &best_before_round,
                    &mut global_best,
                    &mut round_arrivals[round],
                    &mut parents[round],
                    &mut next_marked,
                    &mut next_marked_flags,
                    &mut round_metrics,
                );
            }
            let line_scan_ms = line_scan_started.elapsed().as_millis();
            round_metrics.timings_us.line_scan_us = line_scan_started.elapsed().as_micros();

            let transit_frontier_len = next_marked.len();
            if !plan.destination_egress_edges.is_empty() && transit_frontier_len > 0 {
                let destination_egress_started = Instant::now();
                self.apply_virtual_destination_egress(
                    to_index,
                    round,
                    &next_marked[..transit_frontier_len],
                    &plan.destination_egress_edges,
                    &mut global_best,
                    &mut round_arrivals,
                    &mut parents,
                    &mut destination_round,
                );
                round_metrics.timings_us.destination_egress_pre_transfer_us +=
                    destination_egress_started.elapsed().as_micros();
            }
            if transit_frontier_len > 0 {
                let profile_lookup_started = Instant::now();
                let profile_lookups_before = profile_lookups;
                let profile_hits_before = profile_hits;
                let profile_bound_improvements_before = profile_bound_improvements;
                let destination_bound_prunes_before = counters.destination_bound_prunes;
                let flat_spatial_mask_checks_before = flat_spatial_mask_checks;
                let flat_spatial_mask_hits_before = flat_spatial_mask_hits;
                self.apply_profile_lookahead(
                    service_date,
                    to_index,
                    plan.exact_destination_stop,
                    &spatial_flat_mask,
                    &plan.destination_egress_edges,
                    round,
                    rounds,
                    &next_marked,
                    &trip_is_available,
                    &line_max_positive_delay_secs,
                    &mut global_best,
                    &mut round_arrivals,
                    &mut parents,
                    &mut destination_round,
                    &mut profile_lookups,
                    &mut profile_hits,
                    &mut profile_bound_improvements,
                    &mut counters.destination_bound_prunes,
                    &mut flat_spatial_mask_checks,
                    &mut flat_spatial_mask_hits,
                    &mut local_subquery_cache,
                );
                profile_lookup_ms += profile_lookup_started.elapsed().as_millis();
                round_metrics.timings_us.profile_lookup_pre_transfer_us +=
                    profile_lookup_started.elapsed().as_micros();
                round_metrics.profile_lookups += profile_lookups - profile_lookups_before;
                round_metrics.profile_hits += profile_hits - profile_hits_before;
                round_metrics.profile_bound_improvements +=
                    profile_bound_improvements - profile_bound_improvements_before;
                round_metrics.destination_bound_prunes +=
                    counters.destination_bound_prunes - destination_bound_prunes_before;
                round_metrics.flat_spatial_mask_checks +=
                    flat_spatial_mask_checks - flat_spatial_mask_checks_before;
                round_metrics.flat_spatial_mask_hits +=
                    flat_spatial_mask_hits - flat_spatial_mask_hits_before;
            }

            let transfer_relax_started = Instant::now();
            for improved_index in 0..transit_frontier_len {
                let stop_index = next_marked[improved_index];
                let stop_arrival = round_arrivals[round][stop_index];
                if stop_arrival >= INF_TIME || stop_arrival >= global_best[to_index] {
                    round_metrics.destination_bound_prunes += 1;
                    continue;
                }
                for transfer in &self.static_data.transfers[stop_index] {
                    round_metrics.transfer_relaxations += 1;
                    let candidate = stop_arrival + transfer.duration_secs;
                    if record_stop_improvement(
                        transfer.to_stop,
                        candidate,
                        &mut global_best,
                        &mut round_arrivals[round],
                        &mut parents[round],
                        ParentStep::Walk {
                            from_stop: stop_index,
                            duration_secs: transfer.duration_secs,
                            distance_meters: transfer.distance_meters,
                        },
                        &mut next_marked,
                        &mut next_marked_flags,
                    ) {
                        round_metrics.transfer_improvements += 1;
                    }
                }
            }
            let transfer_relax_ms = transfer_relax_started.elapsed().as_millis();
            round_metrics.timings_us.transfer_relax_us = transfer_relax_started.elapsed().as_micros();

            if next_marked.len() > transit_frontier_len {
                if !plan.destination_egress_edges.is_empty() {
                    let destination_egress_started = Instant::now();
                    self.apply_virtual_destination_egress(
                        to_index,
                        round,
                        &next_marked[transit_frontier_len..],
                        &plan.destination_egress_edges,
                        &mut global_best,
                        &mut round_arrivals,
                        &mut parents,
                        &mut destination_round,
                    );
                    round_metrics.timings_us.destination_egress_post_transfer_us +=
                        destination_egress_started.elapsed().as_micros();
                }
                let profile_lookup_started = Instant::now();
                let profile_lookups_before = profile_lookups;
                let profile_hits_before = profile_hits;
                let profile_bound_improvements_before = profile_bound_improvements;
                let destination_bound_prunes_before = counters.destination_bound_prunes;
                let flat_spatial_mask_checks_before = flat_spatial_mask_checks;
                let flat_spatial_mask_hits_before = flat_spatial_mask_hits;
                self.apply_profile_lookahead(
                    service_date,
                    to_index,
                    plan.exact_destination_stop,
                    &spatial_flat_mask,
                    &plan.destination_egress_edges,
                    round,
                    rounds,
                    &next_marked[transit_frontier_len..],
                    &trip_is_available,
                    &line_max_positive_delay_secs,
                    &mut global_best,
                    &mut round_arrivals,
                    &mut parents,
                    &mut destination_round,
                    &mut profile_lookups,
                    &mut profile_hits,
                    &mut profile_bound_improvements,
                    &mut counters.destination_bound_prunes,
                    &mut flat_spatial_mask_checks,
                    &mut flat_spatial_mask_hits,
                    &mut local_subquery_cache,
                );
                profile_lookup_ms += profile_lookup_started.elapsed().as_millis();
                round_metrics.timings_us.profile_lookup_post_transfer_us +=
                    profile_lookup_started.elapsed().as_micros();
                round_metrics.profile_lookups += profile_lookups - profile_lookups_before;
                round_metrics.profile_hits += profile_hits - profile_hits_before;
                round_metrics.profile_bound_improvements +=
                    profile_bound_improvements - profile_bound_improvements_before;
                round_metrics.destination_bound_prunes +=
                    counters.destination_bound_prunes - destination_bound_prunes_before;
                round_metrics.flat_spatial_mask_checks +=
                    flat_spatial_mask_checks - flat_spatial_mask_checks_before;
                round_metrics.flat_spatial_mask_hits +=
                    flat_spatial_mask_hits - flat_spatial_mask_hits_before;
            }

            for stop_index in &next_marked {
                improved_round[*stop_index] = Some(round);
            }

            if to_index < next_marked_flags.len() && next_marked_flags[to_index] {
                destination_round = Some(round);
            }

            counters.rounds_executed += 1;
            counters.queued_lines_total += round_metrics.queued_lines;
            counters.chronos_bucket_lookups += round_metrics.chronos_bucket_lookups;
            counters.chronos_indirection_lookups += round_metrics.chronos_indirection_lookups;
            counters.chronos_bucket_fallback_searches += round_metrics.chronos_bucket_fallback_searches;
            counters.chronos_bucket_fallback_end_of_service +=
                round_metrics.chronos_bucket_fallback_end_of_service;
            counters.chronos_bucket_fallback_non_monotonic +=
                round_metrics.chronos_bucket_fallback_non_monotonic;
            counters.chronos_bucket_lookback_secs_total +=
                round_metrics.chronos_bucket_lookback_secs_total;
            counters.chronos_bucket_lookback_secs_max = counters
                .chronos_bucket_lookback_secs_max
                .max(round_metrics.chronos_bucket_lookback_secs_max);
            counters.flat_spatial_mask_checks += round_metrics.flat_spatial_mask_checks;
            counters.flat_spatial_mask_hits += round_metrics.flat_spatial_mask_hits;
            counters.stop_positions_scanned += round_metrics.stop_positions_scanned;
            counters.onboard_arrival_evaluations += round_metrics.onboard_arrival_evaluations;
            counters.actual_arrival_calls += round_metrics.actual_arrival_calls;
            counters.actual_departure_calls += round_metrics.actual_departure_calls;
            counters.skipped_stop_checks += round_metrics.skipped_stop_checks;
            counters.trip_searches += round_metrics.trip_searches;
            counters.binary_trip_searches += round_metrics.binary_trip_searches;
            counters.trip_departure_checks += round_metrics.trip_departure_checks;
            counters.transfer_relaxations += round_metrics.transfer_relaxations;
            counters.transfer_improvements += round_metrics.transfer_improvements;
            counters.destination_bound_prunes += round_metrics.destination_bound_prunes;

            round_metrics.timings_us.line_scan_other_us = round_metrics
                .timings_us
                .line_scan_us
                .saturating_sub(
                    round_metrics.timings_us.line_scan_onboard_us
                        + round_metrics.timings_us.line_scan_trip_search_us
                        + round_metrics.timings_us.line_scan_candidate_compare_us,
                );
            round_metrics.timings_us.round_total_us = round_started.elapsed().as_micros();
            round_metrics.timings_us.round_other_us = round_metrics
                .timings_us
                .round_total_us
                .saturating_sub(
                    round_metrics.timings_us.best_before_clone_us
                        + round_metrics.timings_us.queue_build_us
                        + round_metrics.timings_us.line_scan_us
                        + round_metrics.timings_us.destination_egress_pre_transfer_us
                        + round_metrics.timings_us.profile_lookup_pre_transfer_us
                        + round_metrics.timings_us.transfer_relax_us
                        + round_metrics.timings_us.destination_egress_post_transfer_us
                        + round_metrics.timings_us.profile_lookup_post_transfer_us,
                );
            round_timing_totals_us.accumulate(&round_metrics.timings_us);

            trace_rounds.push(QueryRoundTrace {
                round,
                marked_stops: marked_count,
                queued_lines: round_metrics.queued_lines,
                lines_scanned: round_metrics.lines_scanned,
                improvements: next_marked.len(),
                best_before_clone_ms,
                queue_build_ms,
                line_scan_ms,
                transfer_relax_ms,
                timings_us: round_metrics.timings_us,
                stop_positions_scanned: round_metrics.stop_positions_scanned,
                onboard_arrival_evaluations: round_metrics.onboard_arrival_evaluations,
                actual_arrival_calls: round_metrics.actual_arrival_calls,
                actual_departure_calls: round_metrics.actual_departure_calls,
                skipped_stop_checks: round_metrics.skipped_stop_checks,
                profile_lookups: round_metrics.profile_lookups,
                profile_hits: round_metrics.profile_hits,
                profile_bound_improvements: round_metrics.profile_bound_improvements,
                trip_searches: round_metrics.trip_searches,
                binary_trip_searches: round_metrics.binary_trip_searches,
                trip_departure_checks: round_metrics.trip_departure_checks,
                transfer_relaxations: round_metrics.transfer_relaxations,
                transfer_improvements: round_metrics.transfer_improvements,
                destination_time: destination_round.map(|destination_round| {
                    format_service_time(service_date, round_arrivals[destination_round][to_index])
                }),
            });

            std::mem::swap(&mut marked_stops, &mut next_marked);
        }
        let rounds_ms = rounds_started.elapsed().as_millis();

        let destination_round = destination_round.ok_or_else(|| {
            anyhow!(
                "no itinerary found from {} to {} after {} transfers",
                plan.display_from.name,
                plan.display_to.name,
                max_transfers
            )
        })?;
        let arrival_secs = round_arrivals[destination_round][to_index];

        let reconstruct_started = Instant::now();
        let raw_legs = self.reconstruct_path(
            from_index,
            to_index,
            destination_round,
            &parents,
            &round_arrivals,
        )?;
        let reconstruct_ms = reconstruct_started.elapsed().as_millis();
        let cacheable_raw_legs = self.cacheable_raw_legs(&raw_legs, static_stop_count);
        let profile_insertions = if let Some(destination_stop) = plan.exact_destination_stop {
            self.build_profile_insertions(destination_stop, &cacheable_raw_legs)
        } else {
            Vec::new()
        };
        let spatial_profile_insertions = self.build_spatial_profile_insertions(
            destination_cell,
            &cacheable_raw_legs,
        );
        let transit_legs = raw_legs
            .iter()
            .filter(|leg| matches!(leg, RawLeg::Transit { .. }))
            .count();

        let hydrate_started = Instant::now();
        let legs = raw_legs
            .into_iter()
            .map(|leg| self.hydrate_leg(service_date, leg, &plan.overlay))
            .collect::<Result<Vec<_>>>()?;
        let deferred_hydration = self.build_deferred_hydration(&legs)?;
        let hydrate_ms = hydrate_started.elapsed().as_millis();

        if !profile_insertions.is_empty() || !spatial_profile_insertions.is_empty() {
            let cache = self.profile_cache.clone();
            let destination_cell = destination_cell;
            let exact_destination_stop = plan.exact_destination_stop;
            rayon::spawn(move || {
                if let Some(exact_destination_stop) = exact_destination_stop {
                    if !profile_insertions.is_empty() {
                        cache.insert_batch(service_date, exact_destination_stop, profile_insertions);
                    }
                }
                if let Some(destination_cell) = destination_cell {
                    if !spatial_profile_insertions.is_empty() {
                        cache.insert_spatial_batch(
                            service_date,
                            destination_cell,
                            spatial_profile_insertions,
                        );
                    }
                }
            });
        }

        let query_runtime_ms = query_started.elapsed().as_millis();
        counters.flat_spatial_mask_populated_sources = spatial_flat_mask.populated_sources;
        counters.flat_spatial_mask_materialized_matches = spatial_flat_mask.materialized_matches;
        counters.profile_lookups = profile_lookups;
        counters.profile_hits = profile_hits;
        counters.profile_bound_improvements = profile_bound_improvements;

        Ok(QueryResponse {
            from: plan.display_from,
            to: plan.display_to,
            departure_time: format_service_time(service_date, departure_secs),
            arrival_time: format_service_time(service_date, arrival_secs),
            duration_seconds: arrival_secs - departure_secs,
            transfers: transit_legs.saturating_sub(1),
            legs,
            deferred_hydration,
            trace: QueryTrace {
                service_date: service_date.to_string(),
                query_runtime_ms,
                active_services: active_services.len(),
                shadow_delta_count: self.realtime.shadow_delta_count(),
                canceled_trip_count: self.realtime.canceled_trip_count(),
                timings: QueryTimingBreakdown {
                    request_setup_ms,
                    coordinate_projection_ms: plan_metrics.coordinate_projection_ms,
                    pedestrian_lookup_ms: plan_metrics.pedestrian_lookup_ms,
                    trip_mask_ms,
                    flat_spatial_mask_ms,
                    state_init_ms,
                    initial_walk_ms,
                    profile_lookup_ms,
                    rounds_ms,
                    reconstruct_ms,
                    hydrate_ms,
                },
                round_timing_totals_us,
                coordinate_routing: (plan_metrics.source_virtualized
                    || plan_metrics.destination_virtualized)
                    .then_some(CoordinateRoutingTrace {
                        source_virtualized: plan_metrics.source_virtualized,
                        destination_virtualized: plan_metrics.destination_virtualized,
                        source_seed_count: plan_metrics.source_seed_count,
                        destination_seed_count: plan_metrics.destination_seed_count,
                        connector_strategy: plan_metrics.connector_strategy,
                        source_asymptotic_connectors: plan_metrics.source_asymptotic_connectors,
                        destination_asymptotic_connectors: plan_metrics
                            .destination_asymptotic_connectors,
                    }),
                counters,
                rounds: trace_rounds,
            },
        })
    }

    fn scan_line(
        &self,
        line_index: usize,
        start_pos: usize,
        destination_stop: usize,
        trip_is_available: &[bool],
        line_max_positive_delay_secs: i32,
        best_before_round: &[i32],
        global_best: &mut [i32],
        round_arrivals: &mut [i32],
        parents: &mut [Option<ParentStep>],
        improved_stops: &mut Vec<usize>,
        improved_flags: &mut [bool],
        metrics: &mut RoundMetrics,
    ) {
        let line = &self.static_data.lines[line_index];
        let mut current_trip = None::<usize>;
        let mut boarded_at = 0usize;
        metrics.stop_positions_scanned += line.stop_indices.len().saturating_sub(start_pos);

        for stop_pos in start_pos..line.stop_indices.len() {
            let stop_index = line.stop_indices[stop_pos];
            let destination_bound = global_best[destination_stop];

            if let Some(trip_index) = current_trip {
                let onboard_started = Instant::now();
                metrics.onboard_arrival_evaluations += 1;
                metrics.skipped_stop_checks += 1;
                if !self.realtime.is_stop_skipped(trip_index, stop_pos) {
                    metrics.actual_arrival_calls += 1;
                    let arrival =
                        self.realtime
                            .actual_arrival(&self.static_data.trips, trip_index, stop_pos);
                    if arrival < destination_bound {
                        record_stop_improvement(
                            stop_index,
                            arrival,
                            global_best,
                            round_arrivals,
                            parents,
                            ParentStep::Transit {
                                trip_index,
                                board_stop: line.stop_indices[boarded_at],
                                board_pos: boarded_at,
                                alight_stop: stop_index,
                                alight_pos: stop_pos,
                            },
                            improved_stops,
                            improved_flags,
                        );
                    } else {
                        metrics.destination_bound_prunes += 1;
                    }
                }
                metrics.timings_us.line_scan_onboard_us += onboard_started.elapsed().as_micros();
            }

            let ready_at = best_before_round[stop_index];
            if ready_at >= INF_TIME || ready_at >= destination_bound {
                metrics.destination_bound_prunes += 1;
                continue;
            }

            let trip_search_started = Instant::now();
            let candidate_trip = self.find_earliest_trip(
                line,
                stop_pos,
                ready_at,
                trip_is_available,
                line_max_positive_delay_secs,
                metrics,
            );
            metrics.timings_us.line_scan_trip_search_us += trip_search_started.elapsed().as_micros();

            if let Some(candidate_trip) = candidate_trip {
                let candidate_compare_started = Instant::now();
                metrics.actual_departure_calls += 1;
                let candidate_departure = self.realtime.actual_departure(
                    &self.static_data.trips,
                    candidate_trip,
                    stop_pos,
                );

                let replace_current = match current_trip {
                    Some(active_trip) => {
                        metrics.actual_departure_calls += 1;
                        let active_departure = self.realtime.actual_departure(
                            &self.static_data.trips,
                            active_trip,
                            stop_pos,
                        );
                        candidate_departure < active_departure
                    }
                    None => true,
                };

                if replace_current {
                    current_trip = Some(candidate_trip);
                    boarded_at = stop_pos;
                }
                metrics.timings_us.line_scan_candidate_compare_us +=
                    candidate_compare_started.elapsed().as_micros();
            }
        }
    }

    fn find_earliest_trip(
        &self,
        line: &LineRecord,
        stop_pos: usize,
        ready_at: i32,
        trip_is_available: &[bool],
        line_max_positive_delay_secs: i32,
        metrics: &mut RoundMetrics,
    ) -> Option<usize> {
        metrics.trip_searches += 1;

        metrics.chronos_bucket_lookups += 1;
        metrics.chronos_bucket_lookback_secs_total += line_max_positive_delay_secs as u64;
        metrics.chronos_bucket_lookback_secs_max = metrics
            .chronos_bucket_lookback_secs_max
            .max(line_max_positive_delay_secs);

        let uses_indirection = line
            .trip_order_indirection_by_stop
            .get(stop_pos)
            .is_some_and(|order| !order.is_empty());
        if uses_indirection {
            metrics.chronos_indirection_lookups += 1;
        }

        let Some(start_index) = chronos_bucket_start_index(
            line,
            stop_pos,
            ready_at,
            line_max_positive_delay_secs,
        ) else {
            metrics.chronos_bucket_fallback_searches += 1;
            metrics.chronos_bucket_fallback_non_monotonic += 1;
            return self.find_earliest_trip_linear_fallback(
                line,
                stop_pos,
                ready_at,
                trip_is_available,
                metrics,
            );
        };

        let search_len = line_temporal_search_len(line, stop_pos);
        if start_index >= search_len {
            metrics.chronos_bucket_fallback_end_of_service += 1;
            return None;
        }

        metrics.binary_trip_searches += 1;
        let binary_scan_started = Instant::now();

        if line_max_positive_delay_secs == 0 {
            let mut found_trip = None;
            for search_index in start_index..search_len {
                let Some(trip_index) =
                    line_trip_index_at_temporal_position(line, stop_pos, search_index)
                else {
                    break;
                };
                metrics.trip_departure_checks += 1;
                if !trip_is_available[trip_index] {
                    continue;
                }
                metrics.skipped_stop_checks += 1;
                if self.realtime.is_stop_skipped(trip_index, stop_pos) {
                    continue;
                }
                if line.scheduled_departures_by_stop[stop_pos][search_index] >= ready_at {
                    found_trip = Some(trip_index);
                    break;
                }
            }
            if found_trip.is_none() {
                metrics.chronos_bucket_fallback_end_of_service += 1;
            }
            metrics.timings_us.line_scan_trip_search_binary_scan_us +=
                binary_scan_started.elapsed().as_micros();
            return found_trip;
        }

        let mut best_trip = None;
        let mut best_departure = INF_TIME;
        for search_index in start_index..search_len {
            let Some(trip_index) = line_trip_index_at_temporal_position(line, stop_pos, search_index)
            else {
                break;
            };
            metrics.trip_departure_checks += 1;
            if !trip_is_available[trip_index] {
                continue;
            }
            metrics.skipped_stop_checks += 1;
            if self.realtime.is_stop_skipped(trip_index, stop_pos) {
                continue;
            }
            metrics.actual_departure_calls += 1;
            let departure = self
                .realtime
                .actual_departure(&self.static_data.trips, trip_index, stop_pos);
            if departure >= ready_at && departure < best_departure {
                best_departure = departure;
                best_trip = Some(trip_index);
            }
        }
        if best_trip.is_none() {
            metrics.chronos_bucket_fallback_end_of_service += 1;
        }
        metrics.timings_us.line_scan_trip_search_binary_scan_us +=
            binary_scan_started.elapsed().as_micros();
        best_trip
    }

    fn find_earliest_trip_linear_fallback(
        &self,
        line: &LineRecord,
        stop_pos: usize,
        ready_at: i32,
        trip_is_available: &[bool],
        metrics: &mut RoundMetrics,
    ) -> Option<usize> {
        let mut best_trip = None;
        let mut best_departure = INF_TIME;
        let linear_scan_started = Instant::now();
        for trip_index in &line.trip_indices {
            metrics.trip_departure_checks += 1;
            if !trip_is_available[*trip_index] {
                continue;
            }
            metrics.skipped_stop_checks += 1;
            if self.realtime.is_stop_skipped(*trip_index, stop_pos) {
                continue;
            }
            metrics.actual_departure_calls += 1;
            let departure =
                self.realtime
                    .actual_departure(&self.static_data.trips, *trip_index, stop_pos);
            if departure >= ready_at && departure < best_departure {
                best_departure = departure;
                best_trip = Some(*trip_index);
            }
        }
        metrics.timings_us.line_scan_trip_search_linear_scan_us +=
            linear_scan_started.elapsed().as_micros();
        best_trip
    }

    fn build_trip_availability(&self, active_services: &HashSet<String>) -> Vec<bool> {
        let mut trip_is_available = vec![false; self.static_data.trips.len()];
        for service_id in active_services {
            if let Some(trip_indices) = self.static_data.service_to_trip_indices.get(service_id) {
                for trip_index in trip_indices {
                    trip_is_available[*trip_index] = true;
                }
            }
        }

        for (trip_index, canceled) in self
            .realtime
            .canceled_trip_mask(self.static_data.trips.len())
            .into_iter()
            .enumerate()
        {
            if canceled {
                trip_is_available[trip_index] = false;
            }
        }

        trip_is_available
    }

    fn apply_profile_lookahead(
        &self,
        service_date: NaiveDate,
        destination_stop: usize,
        exact_destination_stop: Option<usize>,
        spatial_flat_mask: &PreparedSpatialLookup,
        destination_egress_edges: &[QueryVirtualWalk],
        current_round: usize,
        max_rounds: usize,
        candidate_stops: &[usize],
        trip_is_available: &[bool],
        line_max_positive_delay_secs: &[i32],
        global_best: &mut [i32],
        round_arrivals: &mut [Vec<i32>],
        parents: &mut [Vec<Option<ParentStep>>],
        destination_round: &mut Option<usize>,
        profile_lookups: &mut usize,
        profile_hits: &mut usize,
        profile_bound_improvements: &mut usize,
        destination_bound_prunes: &mut usize,
        flat_spatial_mask_checks: &mut usize,
        flat_spatial_mask_hits: &mut usize,
        local_subquery_cache: &mut HashMap<LocalSubqueryKey, Option<LocalSubqueryResult>>,
    ) {
        let remaining_transit_legs = max_rounds.saturating_sub(current_round);

        for &stop_index in candidate_stops {
            let ready_at = round_arrivals[current_round][stop_index];
            if ready_at >= INF_TIME {
                continue;
            }

            if let Some(exact_destination_stop) = exact_destination_stop {
                *profile_lookups += 1;
                match self.profile_cache.lookup_bounded(
                    service_date,
                    exact_destination_stop,
                    stop_index,
                    ready_at,
                    remaining_transit_legs,
                    global_best[destination_stop],
                ) {
                    ProfileLookupDecision::SummaryPruned => {
                        *destination_bound_prunes += 1;
                        continue;
                    }
                    ProfileLookupDecision::Match(profile_match) => {
                        let lower_bound_arrival =
                            ready_at.saturating_add(profile_match.absolute_min_duration_secs);
                        if profile_match.absolute_min_transfers > remaining_transit_legs
                            || lower_bound_arrival >= global_best[destination_stop]
                        {
                            *destination_bound_prunes += 1;
                            continue;
                        }

                        *profile_hits += 1;

                        let target_round = current_round + profile_match.transit_legs;
                        if target_round <= max_rounds
                            && record_memoized_destination_improvement(
                                destination_stop,
                                stop_index,
                                current_round,
                                target_round,
                                profile_match.arrival_secs,
                                profile_match.suffix_legs,
                                global_best,
                                round_arrivals,
                                parents,
                                destination_round,
                            )
                        {
                            *profile_bound_improvements += 1;
                            self.profile_cache.note_bound_improvement();
                        }
                    }
                    ProfileLookupDecision::Miss => {}
                }
            }

            if spatial_flat_mask.enabled_source_stops.is_empty() {
                continue;
            }

            *profile_lookups += 1;
            *flat_spatial_mask_checks += 1;
            let Some(true) = spatial_flat_mask.enabled_source_stops.get(stop_index).copied() else {
                continue;
            };

            if spatial_flat_mask
                .absolute_min_transfers_by_source_stop
                .get(stop_index)
                .copied()
                .unwrap_or(usize::MAX)
                > remaining_transit_legs
            {
                continue;
            }

            let source_stop_lower_bound = ready_at.saturating_add(
                spatial_flat_mask
                    .absolute_min_duration_by_source_stop
                    .get(stop_index)
                    .copied()
                    .unwrap_or(i32::MAX),
            );
            if source_stop_lower_bound >= global_best[destination_stop] {
                *destination_bound_prunes += 1;
                continue;
            }

            let spatial_matches = &spatial_flat_mask.matches_by_source_stop[stop_index];
            let mut counted_spatial_hit = false;

            for spatial_match in spatial_matches {
                if ready_at > spatial_match.latest_ready_secs
                    || spatial_match.absolute_min_transfers > remaining_transit_legs
                {
                    continue;
                }

                let lower_bound_arrival =
                    ready_at.saturating_add(spatial_match.absolute_min_duration_secs);
                if lower_bound_arrival >= global_best[destination_stop] {
                    *destination_bound_prunes += 1;
                    continue;
                }

                if !counted_spatial_hit {
                    *flat_spatial_mask_hits += 1;
                    *profile_hits += 1;
                    counted_spatial_hit = true;
                }

                if spatial_match.boundary_arrival_secs >= global_best[destination_stop] {
                    continue;
                }

                if destination_egress_edges.is_empty() {
                    let Some(exact_destination_stop) = exact_destination_stop else {
                        continue;
                    };
                    let remaining_after_trunk =
                        remaining_transit_legs.saturating_sub(spatial_match.transit_legs);
                    let Some(local_result) = self.lookup_or_compute_local_subquery(
                        spatial_match.boundary_stop,
                        exact_destination_stop,
                        spatial_match.boundary_arrival_secs,
                        remaining_after_trunk,
                        trip_is_available,
                        line_max_positive_delay_secs,
                        local_subquery_cache,
                    ) else {
                        continue;
                    };

                    let total_transit_legs = spatial_match.transit_legs + local_result.transit_legs;
                    let target_round = current_round + total_transit_legs;
                    if target_round > max_rounds
                        || local_result.arrival_secs >= global_best[destination_stop]
                    {
                        continue;
                    }

                    let combined_legs = combine_cached_leg_sequences(
                        spatial_match.trunk_legs.as_ref(),
                        &local_result.legs,
                    );

                    if record_memoized_destination_improvement(
                        destination_stop,
                        stop_index,
                        current_round,
                        target_round,
                        local_result.arrival_secs,
                        Arc::new(combined_legs),
                        global_best,
                        round_arrivals,
                        parents,
                        destination_round,
                    ) {
                        *profile_bound_improvements += 1;
                        self.profile_cache.note_bound_improvement();
                        break;
                    }
                    continue;
                }

                let Some((candidate_arrival, total_transit_legs, suffix_legs)) = self
                    .best_virtual_destination_completion(
                        destination_stop,
                        spatial_match,
                        remaining_transit_legs,
                        destination_egress_edges,
                        trip_is_available,
                        line_max_positive_delay_secs,
                        local_subquery_cache,
                    )
                else {
                    continue;
                };

                let target_round = current_round + total_transit_legs;
                if target_round > max_rounds || candidate_arrival >= global_best[destination_stop] {
                    continue;
                }

                if record_memoized_destination_improvement(
                    destination_stop,
                    stop_index,
                    current_round,
                    target_round,
                    candidate_arrival,
                    Arc::new(suffix_legs),
                    global_best,
                    round_arrivals,
                    parents,
                    destination_round,
                ) {
                    *profile_bound_improvements += 1;
                    self.profile_cache.note_bound_improvement();
                    break;
                }
            }
        }
    }

    fn apply_virtual_destination_egress(
        &self,
        destination_stop: usize,
        current_round: usize,
        candidate_stops: &[usize],
        destination_egress_edges: &[QueryVirtualWalk],
        global_best: &mut [i32],
        round_arrivals: &mut [Vec<i32>],
        parents: &mut [Vec<Option<ParentStep>>],
        destination_round: &mut Option<usize>,
    ) {
        for &stop_index in candidate_stops {
            let Some(edge) = destination_egress_edges
                .iter()
                .find(|edge| edge.from_stop == stop_index)
            else {
                continue;
            };
            let ready_at = round_arrivals[current_round][stop_index];
            if ready_at >= INF_TIME {
                continue;
            }

            let arrival_secs = ready_at + edge.duration_secs;
            let suffix = Arc::new(vec![CachedLeg::Walk {
                from_stop: edge.from_stop,
                to_stop: edge.to_stop,
                departure_secs: ready_at,
                arrival_secs,
                duration_secs: edge.duration_secs,
                distance_meters: edge.distance_meters,
            }]);
            record_memoized_destination_improvement(
                destination_stop,
                stop_index,
                current_round,
                current_round,
                arrival_secs,
                suffix,
                global_best,
                round_arrivals,
                parents,
                destination_round,
            );
        }
    }

    fn best_virtual_destination_completion(
        &self,
        destination_stop: usize,
        spatial_match: &crate::profile_cache::SpatialProfileMatch,
        remaining_transit_legs: usize,
        destination_egress_edges: &[QueryVirtualWalk],
        trip_is_available: &[bool],
        line_max_positive_delay_secs: &[i32],
        local_subquery_cache: &mut HashMap<LocalSubqueryKey, Option<LocalSubqueryResult>>,
    ) -> Option<(i32, usize, Vec<CachedLeg>)> {
        let remaining_after_trunk = remaining_transit_legs.saturating_sub(spatial_match.transit_legs);
        let mut best_match = None::<(i32, usize, Vec<CachedLeg>)>;

        for edge in destination_egress_edges {
            let Some(local_result) = self.lookup_or_compute_local_subquery(
                spatial_match.boundary_stop,
                edge.from_stop,
                spatial_match.boundary_arrival_secs,
                remaining_after_trunk,
                trip_is_available,
                line_max_positive_delay_secs,
                local_subquery_cache,
            ) else {
                continue;
            };

            let final_arrival = local_result.arrival_secs + edge.duration_secs;
            let total_transit_legs = spatial_match.transit_legs + local_result.transit_legs;
            let mut suffix_legs = combine_cached_leg_sequences(
                spatial_match.trunk_legs.as_ref(),
                &local_result.legs,
            );
            suffix_legs.push(CachedLeg::Walk {
                from_stop: edge.from_stop,
                to_stop: destination_stop,
                departure_secs: local_result.arrival_secs,
                arrival_secs: final_arrival,
                duration_secs: edge.duration_secs,
                distance_meters: edge.distance_meters,
            });

            let should_replace = match &best_match {
                Some((best_arrival, best_transit_legs, _)) => {
                    final_arrival < *best_arrival
                        || (final_arrival == *best_arrival
                            && total_transit_legs < *best_transit_legs)
                }
                None => true,
            };

            if should_replace {
                best_match = Some((final_arrival, total_transit_legs, suffix_legs));
            }
        }

        best_match
    }

    fn lookup_or_compute_local_subquery(
        &self,
        from_stop: usize,
        to_stop: usize,
        departure_secs: i32,
        remaining_transit_legs: usize,
        trip_is_available: &[bool],
        line_max_positive_delay_secs: &[i32],
        cache: &mut HashMap<LocalSubqueryKey, Option<LocalSubqueryResult>>,
    ) -> Option<LocalSubqueryResult> {
        let key = LocalSubqueryKey {
            from_stop,
            to_stop,
            departure_secs,
            remaining_transit_legs,
        };

        if let Some(cached) = cache.get(&key) {
            return cached.clone();
        }

        let computed = self.compute_local_subquery(
            from_stop,
            to_stop,
            departure_secs,
            remaining_transit_legs,
            trip_is_available,
            line_max_positive_delay_secs,
        );
        cache.insert(key, computed.clone());
        computed
    }

    fn compute_local_subquery(
        &self,
        from_stop: usize,
        to_stop: usize,
        departure_secs: i32,
        remaining_transit_legs: usize,
        trip_is_available: &[bool],
        line_max_positive_delay_secs: &[i32],
    ) -> Option<LocalSubqueryResult> {
        if from_stop == to_stop {
            return Some(LocalSubqueryResult {
                arrival_secs: departure_secs,
                transit_legs: 0,
                legs: Vec::new(),
            });
        }

        let stop_count = self.static_data.stops.len();
        let line_count = self.static_data.lines.len();
        let rounds = remaining_transit_legs;

        let mut global_best = vec![INF_TIME; stop_count];
        let mut round_arrivals = vec![vec![INF_TIME; stop_count]; rounds + 1];
        let mut parents = vec![vec![None::<ParentStep>; stop_count]; rounds + 1];
        let mut marked_stops = vec![from_stop];
        let mut marked_flags = vec![false; stop_count];
        let mut next_marked = Vec::<usize>::new();
        let mut next_marked_flags = vec![false; stop_count];
        let mut best_before_round = vec![INF_TIME; stop_count];
        let mut queued_line_positions = vec![usize::MAX; line_count];

        global_best[from_stop] = departure_secs;
        round_arrivals[0][from_stop] = departure_secs;
        parents[0][from_stop] = Some(ParentStep::Origin);
        marked_flags[from_stop] = true;

        let mut destination_round = None::<usize>;

        for transfer in &self.static_data.transfers[from_stop] {
            let candidate = departure_secs + transfer.duration_secs;
            if record_stop_improvement(
                transfer.to_stop,
                candidate,
                &mut global_best,
                &mut round_arrivals[0],
                &mut parents[0],
                ParentStep::Walk {
                    from_stop,
                    duration_secs: transfer.duration_secs,
                    distance_meters: transfer.distance_meters,
                },
                &mut marked_stops,
                &mut marked_flags,
            ) && transfer.to_stop == to_stop
            {
                destination_round = Some(0);
            }
        }

        for round in 1..=rounds {
            if marked_stops.is_empty() {
                break;
            }

            best_before_round.copy_from_slice(&global_best);
            queued_line_positions.fill(usize::MAX);
            for &stop_index in &marked_stops {
                if best_before_round[stop_index] >= global_best[to_stop] {
                    continue;
                }
                for line_ref in &self.static_data.stop_to_lines[stop_index] {
                    let queued_pos = &mut queued_line_positions[line_ref.line_index];
                    if line_ref.stop_pos < *queued_pos {
                        *queued_pos = line_ref.stop_pos;
                    }
                }
            }

            next_marked.clear();
            next_marked_flags.fill(false);
            let mut round_metrics = RoundMetrics::default();

            for (line_index, start_pos) in queued_line_positions.iter().copied().enumerate() {
                if start_pos == usize::MAX {
                    continue;
                }
                self.scan_line(
                    line_index,
                    start_pos,
                    to_stop,
                    trip_is_available,
                    line_max_positive_delay_secs[line_index],
                    &best_before_round,
                    &mut global_best,
                    &mut round_arrivals[round],
                    &mut parents[round],
                    &mut next_marked,
                    &mut next_marked_flags,
                    &mut round_metrics,
                );
            }

            let transit_frontier_len = next_marked.len();
            for improved_index in 0..transit_frontier_len {
                let stop_index = next_marked[improved_index];
                let stop_arrival = round_arrivals[round][stop_index];
                if stop_arrival >= INF_TIME || stop_arrival >= global_best[to_stop] {
                    continue;
                }

                for transfer in &self.static_data.transfers[stop_index] {
                    let candidate = stop_arrival + transfer.duration_secs;
                    if record_stop_improvement(
                        transfer.to_stop,
                        candidate,
                        &mut global_best,
                        &mut round_arrivals[round],
                        &mut parents[round],
                        ParentStep::Walk {
                            from_stop: stop_index,
                            duration_secs: transfer.duration_secs,
                            distance_meters: transfer.distance_meters,
                        },
                        &mut next_marked,
                        &mut next_marked_flags,
                    ) && transfer.to_stop == to_stop
                    {
                        destination_round = Some(round);
                    }
                }
            }

            if next_marked_flags[to_stop] {
                destination_round = Some(round);
            }

            std::mem::swap(&mut marked_stops, &mut next_marked);
        }

        let destination_round = destination_round?;
        let arrival_secs = round_arrivals[destination_round][to_stop];
        let raw_legs = self
            .reconstruct_path(from_stop, to_stop, destination_round, &parents, &round_arrivals)
            .ok()?;
        let transit_legs = raw_legs
            .iter()
            .filter(|leg| matches!(leg, RawLeg::Transit { .. }))
            .count();

        Some(LocalSubqueryResult {
            arrival_secs,
            transit_legs,
            legs: raw_legs.iter().map(cached_leg_from_raw_leg).collect(),
        })
    }

    fn reconstruct_path(
        &self,
        from_index: usize,
        to_index: usize,
        destination_round: usize,
        parents: &[Vec<Option<ParentStep>>],
        round_arrivals: &[Vec<i32>],
    ) -> Result<Vec<RawLeg>> {
        let mut legs = Vec::<RawLeg>::new();
        let mut current_stop = to_index;
        let mut current_round = destination_round;

        loop {
            if current_stop == from_index && current_round == 0 {
                break;
            }

            let parent = parents[current_round][current_stop]
                .clone()
                .ok_or_else(|| anyhow!("path reconstruction failed for stop {}", current_stop))?;

            match parent {
                ParentStep::Origin => break,
                ParentStep::Walk {
                    from_stop,
                    duration_secs,
                    distance_meters,
                } => {
                    let arrival_secs = round_arrivals[current_round][current_stop];
                    let departure_secs = arrival_secs - duration_secs;
                    legs.push(RawLeg::Walk {
                        from_stop,
                        to_stop: current_stop,
                        departure_secs,
                        arrival_secs,
                        duration_secs,
                        distance_meters,
                    });
                    current_stop = from_stop;
                }
                ParentStep::Transit {
                    trip_index,
                    board_stop,
                    board_pos,
                    alight_stop,
                    alight_pos,
                } => {
                    let departure_secs = self.realtime.actual_departure(
                        &self.static_data.trips,
                        trip_index,
                        board_pos,
                    );
                    let arrival_secs = round_arrivals[current_round][alight_stop];
                    legs.push(RawLeg::Transit {
                        trip_index,
                        board_stop,
                        board_pos,
                        alight_stop,
                        alight_pos,
                        departure_secs,
                        arrival_secs,
                    });
                    current_stop = board_stop;
                    current_round = current_round.saturating_sub(1);
                }
                ParentStep::Memoized {
                    from_stop,
                    source_round,
                    legs: cached_legs,
                } => {
                    for leg in cached_legs.iter().rev() {
                        legs.push(cached_leg_to_raw_leg(leg));
                    }
                    current_stop = from_stop;
                    current_round = source_round;
                }
            }
        }

        legs.reverse();
        Ok(legs)
    }

    fn hydrate_leg(
        &self,
        service_date: NaiveDate,
        leg: RawLeg,
        overlay: &QueryOverlay,
    ) -> Result<LegResponse> {
        match leg {
            RawLeg::Walk {
                from_stop,
                to_stop,
                departure_secs,
                arrival_secs,
                duration_secs,
                distance_meters,
            } => Ok(LegResponse {
                kind: "walk",
                departure_time: format_service_time(service_date, departure_secs),
                arrival_time: format_service_time(service_date, arrival_secs),
                duration_seconds: duration_secs,
                from_stop: self.query_stop_result(from_stop, overlay),
                to_stop: self.query_stop_result(to_stop, overlay),
                trip_gid: None,
                route_gid: None,
                trip_id: None,
                route_id: None,
                route_label: None,
                route_type: None,
                route_color: None,
                route_text_color: None,
                headsign: None,
                walk_distance_meters: Some(distance_meters),
                delay_applied_seconds: None,
                polyline: self.query_walk_polyline(from_stop, to_stop, overlay),
            }),
            RawLeg::Transit {
                trip_index,
                board_stop,
                board_pos,
                alight_stop,
                alight_pos,
                departure_secs,
                arrival_secs,
            } => {
                let trip = &self.static_data.trips[trip_index];
                let route = &self.static_data.routes[trip.route_index];
                let cold_trip = self.cold_trip_record(trip_index);
                let cold_route = self.cold_route_record(trip.route_index);
                let scheduled_departure = trip.stop_times[board_pos].departure_secs;
                let delay_applied_seconds = departure_secs - scheduled_departure;
                Ok(LegResponse {
                    kind: "transit",
                    departure_time: format_service_time(service_date, departure_secs),
                    arrival_time: format_service_time(service_date, arrival_secs),
                    duration_seconds: arrival_secs - departure_secs,
                    from_stop: self.query_stop_result(board_stop, overlay),
                    to_stop: self.query_stop_result(alight_stop, overlay),
                    trip_gid: Some(trip.global_id),
                    route_gid: Some(route.global_id),
                    trip_id: Some(cold_trip.id.clone()),
                    route_id: Some(cold_route.id.clone()),
                    route_label: Some(route_display_name_cold(&cold_route)),
                    route_type: Some(cold_route.route_type.clone()),
                    route_color: cold_route.color.clone(),
                    route_text_color: cold_route.text_color.clone(),
                    headsign: cold_trip.headsign.clone(),
                    walk_distance_meters: None,
                    delay_applied_seconds: Some(delay_applied_seconds),
                    polyline: self.trip_polyline(trip, board_pos, alight_pos),
                })
            }
        }
    }

    fn trip_polyline(
        &self,
        trip: &TripRecord,
        board_pos: usize,
        alight_pos: usize,
    ) -> Vec<PolylinePoint> {
        if let Some(shape_id) = &trip.shape_id {
            if let Ok(Some(shape)) = self.static_data.cold_store.shape_points(shape_id) {
                let start_dist = trip.stop_times[board_pos].shape_dist_traveled;
                let end_dist = trip.stop_times[alight_pos].shape_dist_traveled;
                if let (Some(start_dist), Some(end_dist)) = (start_dist, end_dist) {
                    let points: Vec<_> = shape
                        .iter()
                        .filter(|point| {
                            point.dist_traveled.is_some_and(|distance| {
                                distance >= start_dist && distance <= end_dist
                            })
                        })
                        .map(|point| PolylinePoint {
                            lat: point.lat,
                            lon: point.lon,
                        })
                        .collect();
                    if points.len() >= 2 {
                        return points;
                    }
                }
            }
        }

        straight_polyline(
            &self.static_data.stops[trip.stop_times[board_pos].stop_index],
            &self.static_data.stops[trip.stop_times[alight_pos].stop_index],
        )
    }

    fn stop_result(&self, stop_index: usize) -> StopSearchResult {
        let stop = &self.static_data.stops[stop_index];
        if let Ok(cold_stop) = self.static_data.cold_store.stop(stop_index) {
            return StopSearchResult {
                global_id: cold_stop.global_id,
                feed_id: cold_stop.feed_id,
                local_id: cold_stop.local_id,
                id: cold_stop.id,
                code: cold_stop.code,
                name: cold_stop.name,
                latitude: cold_stop.latitude,
                longitude: cold_stop.longitude,
                is_virtual: false,
            };
        }

        StopSearchResult {
            global_id: stop.global_id,
            feed_id: stop.feed_id.clone(),
            local_id: stop.local_id.clone(),
            id: stop.id.clone(),
            code: stop.code.clone(),
            name: stop.name.clone(),
            latitude: stop.latitude,
            longitude: stop.longitude,
            is_virtual: false,
        }
    }

    fn query_stop_result(&self, stop_index: usize, overlay: &QueryOverlay) -> StopSearchResult {
        overlay
            .virtual_stops
            .get(&stop_index)
            .cloned()
            .unwrap_or_else(|| self.stop_result(stop_index))
    }

    fn query_walk_polyline(
        &self,
        from_stop: usize,
        to_stop: usize,
        overlay: &QueryOverlay,
    ) -> Vec<PolylinePoint> {
        if let Some(edge) = overlay.virtual_walks.get(&(from_stop, to_stop)) {
            return edge.polyline.clone();
        }
        if from_stop < self.static_data.stops.len() && to_stop < self.static_data.stops.len() {
            return self.walk_transfer_polyline(from_stop, to_stop);
        }

        let Some((from_lat, from_lon)) = self.query_stop_coordinates(from_stop, overlay) else {
            return Vec::new();
        };
        let Some((to_lat, to_lon)) = self.query_stop_coordinates(to_stop, overlay) else {
            return Vec::new();
        };

        vec![
            PolylinePoint {
                lat: from_lat,
                lon: from_lon,
            },
            PolylinePoint {
                lat: to_lat,
                lon: to_lon,
            },
        ]
    }

    fn query_stop_coordinates(
        &self,
        stop_index: usize,
        overlay: &QueryOverlay,
    ) -> Option<(f64, f64)> {
        if let Some(stop) = overlay.virtual_stops.get(&stop_index) {
            return Some((stop.latitude?, stop.longitude?));
        }
        let stop = self.static_data.stops.get(stop_index)?;
        Some((stop.latitude?, stop.longitude?))
    }

    fn resolve_stop_query(&self, stop_id: Option<&str>, stop_gid: Option<u64>) -> Result<usize> {
        if let Some(stop_gid) = stop_gid {
            if let Some(stop_index) = self.static_data.stop_lookup_by_global_id.get(&stop_gid) {
                return Ok(*stop_index);
            }
        }

        if let Some(stop_id) = stop_id {
            if let Some(stop_index) = self.static_data.stop_lookup.get(stop_id) {
                return Ok(*stop_index);
            }

            if self.static_data.feeds.len() == 1 {
                if let Some(stop_index) = self
                    .static_data
                    .active_stop_indices
                    .iter()
                    .copied()
                    .find(|stop_index| self.static_data.stops[*stop_index].local_id == stop_id)
                {
                    return Ok(stop_index);
                }
            }
        }

        bail!("stop not found")
    }

    fn walk_transfer_polyline(&self, from_stop: usize, to_stop: usize) -> Vec<PolylinePoint> {
        self.static_data.transfers[from_stop]
            .iter()
            .find(|transfer| transfer.to_stop == to_stop)
            .map(|transfer| transfer.polyline.clone())
            .filter(|polyline| polyline.len() >= 2)
            .unwrap_or_else(|| {
                straight_polyline(
                    &self.static_data.stops[from_stop],
                    &self.static_data.stops[to_stop],
                )
            })
    }

    fn cold_route_record(&self, route_index: usize) -> ColdRouteRecord {
        self.static_data
            .cold_store
            .route(route_index)
            .unwrap_or_else(|_| {
                let route = &self.static_data.routes[route_index];
                ColdRouteRecord {
                    global_id: route.global_id,
                    id: route.id.clone(),
                    short_name: route.short_name.clone(),
                    long_name: route.long_name.clone(),
                    route_type: route.route_type.clone(),
                    color: route.color.clone(),
                    text_color: route.text_color.clone(),
                }
            })
    }

    fn cold_trip_record(&self, trip_index: usize) -> ColdTripRecord {
        self.static_data
            .cold_store
            .trip(trip_index)
            .unwrap_or_else(|_| {
                let trip = &self.static_data.trips[trip_index];
                ColdTripRecord {
                    global_id: trip.global_id,
                    id: trip.id.clone(),
                    headsign: trip.headsign.clone(),
                    shape_id: trip.shape_id.clone(),
                }
            })
    }

    fn build_deferred_hydration(
        &self,
        legs: &[LegResponse],
    ) -> Result<DeferredHydrationResponse> {
        let mut entities = HydrationEntityDictionary::default();
        let mut stop_seen = HashSet::<u64>::new();
        let mut route_seen = HashSet::<u64>::new();
        let mut trip_seen = HashSet::<u64>::new();
        let mut polyline_lookup = HashMap::<u64, usize>::new();
        let mut deferred_legs = Vec::with_capacity(legs.len());

        for leg in legs {
            if stop_seen.insert(leg.from_stop.global_id) {
                entities.stops.push(leg.from_stop.clone());
            }
            if stop_seen.insert(leg.to_stop.global_id) {
                entities.stops.push(leg.to_stop.clone());
            }

            if let (Some(route_gid), Some(route_id), Some(route_label), Some(route_type)) = (
                leg.route_gid,
                leg.route_id.as_ref(),
                leg.route_label.as_ref(),
                leg.route_type.as_ref(),
            ) {
                if route_seen.insert(route_gid) {
                    entities.routes.push(RouteHydration {
                        global_id: route_gid,
                        id: route_id.clone(),
                        label: route_label.clone(),
                        route_type: route_type.clone(),
                        color: leg.route_color.clone(),
                        text_color: leg.route_text_color.clone(),
                    });
                }
            }

            if let (Some(trip_gid), Some(trip_id)) = (leg.trip_gid, leg.trip_id.as_ref()) {
                if trip_seen.insert(trip_gid) {
                    entities.trips.push(TripHydration {
                        global_id: trip_gid,
                        id: trip_id.clone(),
                        headsign: leg.headsign.clone(),
                    });
                }
            }

            let polyline_key = polyline_fingerprint(&leg.polyline);
            let polyline_index = if let Some(index) = polyline_lookup.get(&polyline_key).copied() {
                index
            } else {
                let index = entities.polylines.len();
                entities.polylines.push(leg.polyline.clone());
                polyline_lookup.insert(polyline_key, index);
                index
            };

            deferred_legs.push(DeferredLegRef {
                kind: leg.kind,
                departure_time: leg.departure_time.clone(),
                arrival_time: leg.arrival_time.clone(),
                duration_seconds: leg.duration_seconds,
                from_stop_gid: leg.from_stop.global_id,
                to_stop_gid: leg.to_stop.global_id,
                trip_gid: leg.trip_gid,
                route_gid: leg.route_gid,
                headsign: leg.headsign.clone(),
                walk_distance_meters: leg.walk_distance_meters,
                delay_applied_seconds: leg.delay_applied_seconds,
                polyline_index,
            });
        }

        Ok(DeferredHydrationResponse {
            legs: deferred_legs,
            entities,
        })
    }

    fn cacheable_raw_legs(&self, raw_legs: &[RawLeg], static_stop_count: usize) -> Vec<RawLeg> {
        raw_legs
            .iter()
            .filter(|leg| match leg {
                RawLeg::Walk {
                    from_stop, to_stop, ..
                } => *from_stop < static_stop_count && *to_stop < static_stop_count,
                RawLeg::Transit {
                    board_stop,
                    alight_stop,
                    ..
                } => *board_stop < static_stop_count && *alight_stop < static_stop_count,
            })
            .cloned()
            .collect()
    }

    fn build_profile_insertions(
        &self,
        _destination_stop: usize,
        raw_legs: &[RawLeg],
    ) -> Vec<ProfileInsertionPoint> {
        if raw_legs.is_empty() {
            return Vec::new();
        }

        let final_arrival_secs = raw_leg_arrival_secs(
            raw_legs
                .last()
                .expect("raw path must contain at least one leg"),
        );
        let trip_indices = raw_legs
            .iter()
            .filter_map(raw_leg_trip_index)
            .collect::<Vec<_>>();

        let mut trip_offset = 0usize;
        let mut points = Vec::with_capacity(raw_legs.len());
        for (start_index, leg) in raw_legs.iter().enumerate() {
            points.push(ProfileInsertionPoint {
                source_stop: raw_leg_source_stop(leg),
                latest_ready_secs: raw_leg_departure_secs(leg),
                arrival_secs: final_arrival_secs,
                transit_legs: trip_indices.len().saturating_sub(trip_offset),
                trip_indices: trip_indices[trip_offset..].to_vec(),
                suffix_legs: raw_legs[start_index..]
                    .iter()
                    .map(cached_leg_from_raw_leg)
                    .collect(),
            });

            if matches!(leg, RawLeg::Transit { .. }) {
                trip_offset += 1;
            }
        }

        points
    }

    fn build_spatial_profile_insertions(
        &self,
        destination_cell: Option<u64>,
        raw_legs: &[RawLeg],
    ) -> Vec<SpatialProfileInsertionPoint> {
        let Some(destination_cell) = destination_cell else {
            return Vec::new();
        };
        if raw_legs.is_empty() {
            return Vec::new();
        }

        let mut points = Vec::with_capacity(raw_legs.len());
        for (start_index, leg) in raw_legs.iter().enumerate() {
            let source_stop = raw_leg_source_stop(leg);
            if self.stop_destination_cell(source_stop) == Some(destination_cell) {
                continue;
            }

            let latest_ready_secs = raw_leg_departure_secs(leg);
            let mut trip_indices = Vec::new();
            let mut trunk_legs = Vec::<CachedLeg>::new();
            let mut boundary_stop = None::<usize>;
            let mut boundary_arrival_secs = INF_TIME;

            for suffix_leg in &raw_legs[start_index..] {
                match suffix_leg {
                    RawLeg::Walk {
                        to_stop,
                        arrival_secs,
                        ..
                    } => {
                        trunk_legs.push(cached_leg_from_raw_leg(suffix_leg));
                        if self.stop_destination_cell(*to_stop) == Some(destination_cell) {
                            boundary_stop = Some(*to_stop);
                            boundary_arrival_secs = *arrival_secs;
                            break;
                        }
                    }
                    RawLeg::Transit {
                        trip_index,
                        board_stop,
                        board_pos,
                        departure_secs,
                        ..
                    } => {
                        trip_indices.push(*trip_index);
                        if let Some((cell_boundary_stop, cell_boundary_pos, cell_boundary_arrival)) =
                            self.first_transit_stop_in_cell(
                                *trip_index,
                                *board_pos,
                                destination_cell,
                            )
                        {
                            trunk_legs.push(CachedLeg::Transit {
                                trip_index: *trip_index,
                                board_stop: *board_stop,
                                board_pos: *board_pos,
                                alight_stop: cell_boundary_stop,
                                alight_pos: cell_boundary_pos,
                                departure_secs: *departure_secs,
                                arrival_secs: cell_boundary_arrival,
                            });
                            boundary_stop = Some(cell_boundary_stop);
                            boundary_arrival_secs = cell_boundary_arrival;
                            break;
                        }

                        trunk_legs.push(cached_leg_from_raw_leg(suffix_leg));
                    }
                }
            }

            let Some(boundary_stop) = boundary_stop else {
                continue;
            };
            if boundary_stop == source_stop {
                continue;
            }

            let base_point = SpatialProfileInsertionPoint {
                source_stop,
                latest_ready_secs,
                boundary_arrival_secs,
                transit_legs: trip_indices.len(),
                boundary_stop,
                trip_indices,
                trunk_legs,
            };
            points.push(base_point.clone());
            points.extend(self.expand_spatial_anchor_points(destination_cell, &base_point));
        }

        points
    }

    fn first_transit_stop_in_cell(
        &self,
        trip_index: usize,
        board_pos: usize,
        destination_cell: u64,
    ) -> Option<(usize, usize, i32)> {
        let trip = &self.static_data.trips[trip_index];
        for stop_pos in board_pos..trip.stop_times.len() {
            let stop_index = trip.stop_times[stop_pos].stop_index;
            if self.stop_destination_cell(stop_index) == Some(destination_cell) {
                return Some((
                    stop_index,
                    stop_pos,
                    self.realtime
                        .actual_arrival(&self.static_data.trips, trip_index, stop_pos),
                ));
            }
        }
        None
    }

    fn stop_destination_cell(&self, stop_index: usize) -> Option<u64> {
        let stop = &self.static_data.stops[stop_index];
        geo_destination_cell(stop.latitude, stop.longitude)
    }

    fn stop_destination_neighborhood(&self, stop_index: usize) -> Vec<u64> {
        let stop = &self.static_data.stops[stop_index];
        geo_destination_cell_neighborhood(stop.latitude, stop.longitude)
    }

    fn expand_spatial_anchor_points(
        &self,
        destination_cell: u64,
        point: &SpatialProfileInsertionPoint,
    ) -> Vec<SpatialProfileInsertionPoint> {
        if point.transit_legs == 0 {
            return Vec::new();
        }

        let mut points = Vec::new();
        let mut transit_legs_before = 0usize;

        for (leg_index, leg) in point.trunk_legs.iter().enumerate() {
            let CachedLeg::Transit {
                trip_index,
                board_pos,
                alight_stop,
                alight_pos,
                arrival_secs,
                ..
            } = leg
            else {
                continue;
            };

            let trip = &self.static_data.trips[*trip_index];
            let remaining_transit_legs = point.transit_legs.saturating_sub(transit_legs_before);
            let remaining_trip_indices = point.trip_indices[transit_legs_before..].to_vec();

            for anchor_pos in (*board_pos + 1)..*alight_pos {
                let anchor_stop = trip.stop_times[anchor_pos].stop_index;
                if self.stop_destination_cell(anchor_stop) == Some(destination_cell)
                    || anchor_stop == point.source_stop
                    || !self.is_strong_anchor_stop(anchor_stop)
                {
                    continue;
                }

                let departure_secs = self.realtime.actual_departure(
                    &self.static_data.trips,
                    *trip_index,
                    anchor_pos,
                );
                let mut trunk_legs = Vec::with_capacity(point.trunk_legs.len() - leg_index);
                trunk_legs.push(CachedLeg::Transit {
                    trip_index: *trip_index,
                    board_stop: anchor_stop,
                    board_pos: anchor_pos,
                    alight_stop: *alight_stop,
                    alight_pos: *alight_pos,
                    departure_secs,
                    arrival_secs: *arrival_secs,
                });
                trunk_legs.extend(point.trunk_legs[(leg_index + 1)..].iter().cloned());

                points.push(SpatialProfileInsertionPoint {
                    source_stop: anchor_stop,
                    latest_ready_secs: departure_secs,
                    boundary_arrival_secs: point.boundary_arrival_secs,
                    transit_legs: remaining_transit_legs,
                    boundary_stop: point.boundary_stop,
                    trip_indices: remaining_trip_indices.clone(),
                    trunk_legs,
                });
            }

            transit_legs_before += 1;
        }

        points
    }

    fn is_strong_anchor_stop(&self, stop_index: usize) -> bool {
        let line_degree = self.static_data.stop_to_lines[stop_index].len();
        let transfer_degree = self.static_data.transfers[stop_index].len();
        if line_degree >= 3 || transfer_degree >= 4 {
            return true;
        }

        self.static_data.stop_to_lines[stop_index]
            .iter()
            .any(|line_ref| self.line_is_rapid_transit(line_ref.line_index))
    }

    fn line_is_rapid_transit(&self, line_index: usize) -> bool {
        let Some(&trip_index) = self.static_data.lines[line_index].trip_indices.first() else {
            return false;
        };
        let route = &self.static_data.routes[self.static_data.trips[trip_index].route_index];
        route.route_type.contains("Subway")
            || route.route_type.contains("Rail")
            || route.route_type.contains("Metro")
    }
}

fn resolve_path(
    workspace_root: &PathBuf,
    override_path: Option<String>,
    default_name: &str,
) -> PathBuf {
    match override_path {
        Some(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                workspace_root.join(path)
            }
        }
        None => workspace_root.join(default_name),
    }
}

fn resolve_path_from(base_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn runtime_root(workspace_root: &Path) -> PathBuf {
    workspace_root.join(".alpha-raptor")
}

fn runtime_cache_dir(workspace_root: &Path, category: &str) -> PathBuf {
    runtime_root(workspace_root).join("cache").join(category)
}

fn default_data_dir(workspace_root: &Path) -> PathBuf {
    workspace_root.join("data")
}

fn default_osm_pbf_path(workspace_root: &Path) -> PathBuf {
    default_data_dir(workspace_root)
        .join("osm")
        .join("lazio-latest.osm.pbf")
}

struct PreparedStaticGtfsSource {
    local_path: PathBuf,
    source: String,
    remote_url: Option<String>,
    allow_invalid_tls: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StaticGtfsRefreshMode {
    Bootstrap,
    Poll,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RemoteStaticGtfsVersionMetadata {
    url: String,
    last_modified: Option<String>,
    etag: Option<String>,
}

fn prepare_static_gtfs_source(
    workspace_root: &Path,
    base_dir: &Path,
    feed_id: &str,
    source_value: &str,
    allow_invalid_tls: bool,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<PreparedStaticGtfsSource> {
    if is_remote_source(source_value) {
        let cache_path = remote_static_gtfs_cache_path(workspace_root, feed_id);
        sync_remote_static_gtfs(
            feed_id,
            source_value,
            allow_invalid_tls,
            &cache_path,
            refresh_mode,
        )?;
        return Ok(PreparedStaticGtfsSource {
            local_path: cache_path,
            source: source_value.to_owned(),
            remote_url: Some(source_value.to_owned()),
            allow_invalid_tls,
        });
    }

    Ok(PreparedStaticGtfsSource {
        local_path: resolve_path_from(base_dir, source_value),
        source: source_value.to_owned(),
        remote_url: None,
        allow_invalid_tls,
    })
}

fn prepare_static_gtfs_source_legacy(
    workspace_root: &Path,
    feed_id: &str,
    source_value: Option<String>,
    default_name: &str,
    allow_invalid_tls: bool,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<PreparedStaticGtfsSource> {
    match source_value {
        Some(source_value) => prepare_static_gtfs_source(
            workspace_root,
            workspace_root,
            feed_id,
            &source_value,
            allow_invalid_tls,
            refresh_mode,
        ),
        None => Ok(PreparedStaticGtfsSource {
            local_path: workspace_root.join(default_name),
            source: default_name.to_owned(),
            remote_url: None,
            allow_invalid_tls,
        }),
    }
}

fn is_remote_source(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized.starts_with("http://") || normalized.starts_with("https://")
}

fn remote_static_gtfs_cache_path(workspace_root: &Path, feed_id: &str) -> PathBuf {
    runtime_root(workspace_root)
        .join("static-feeds")
        .join(format!("{feed_id}.static.gtfs.zip"))
}

fn remote_static_gtfs_version_path(cache_path: &Path) -> PathBuf {
    cache_path.with_extension("version.json")
}

fn load_remote_static_gtfs_version_metadata(
    version_path: &Path,
) -> Result<Option<RemoteStaticGtfsVersionMetadata>> {
    if !version_path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(version_path).with_context(|| {
        format!(
            "failed to read remote static GTFS version metadata {}",
            version_path.display()
        )
    })?;
    let metadata = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "failed to parse remote static GTFS version metadata {}",
            version_path.display()
        )
    })?;
    Ok(Some(metadata))
}

fn store_remote_static_gtfs_version_metadata(
    version_path: &Path,
    metadata: &RemoteStaticGtfsVersionMetadata,
) -> Result<()> {
    if let Some(parent) = version_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create remote static GTFS metadata directory {}",
                parent.display()
            )
        })?;
    }

    let bytes = serde_json::to_vec_pretty(metadata)
        .context("failed to serialize remote static GTFS version metadata")?;
    fs::write(version_path, bytes).with_context(|| {
        format!(
            "failed to write remote static GTFS version metadata {}",
            version_path.display()
        )
    })
}

fn remote_static_gtfs_version_from_headers(
    url: &str,
    headers: &reqwest::header::HeaderMap,
) -> RemoteStaticGtfsVersionMetadata {
    RemoteStaticGtfsVersionMetadata {
        url: url.to_owned(),
        last_modified: headers
            .get(LAST_MODIFIED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
        etag: headers
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    }
}

fn probe_remote_static_gtfs_version(
    client: &BlockingHttpClient,
    feed_id: &str,
    url: &str,
) -> Result<RemoteStaticGtfsVersionMetadata> {
    let response = client
        .head(url)
        .send()
        .and_then(|response| response.error_for_status())
        .with_context(|| {
            format!(
                "failed to probe static GTFS feed {feed_id} metadata from {url}"
            )
        })?;

    Ok(remote_static_gtfs_version_from_headers(url, response.headers()))
}

fn remote_static_gtfs_version_changed(
    cached: Option<&RemoteStaticGtfsVersionMetadata>,
    remote: &RemoteStaticGtfsVersionMetadata,
) -> bool {
    let Some(cached) = cached else {
        return true;
    };

    if cached.url != remote.url {
        return true;
    }
    if cached.etag.is_some() || remote.etag.is_some() {
        return cached.etag != remote.etag;
    }
    if cached.last_modified.is_some() || remote.last_modified.is_some() {
        return cached.last_modified != remote.last_modified;
    }

    false
}

fn merged_remote_static_gtfs_version_metadata(
    url: &str,
    preferred: RemoteStaticGtfsVersionMetadata,
    fallback: Option<&RemoteStaticGtfsVersionMetadata>,
) -> RemoteStaticGtfsVersionMetadata {
    let mut metadata = preferred;
    metadata.url = url.to_owned();
    if metadata.last_modified.is_none() {
        metadata.last_modified = fallback.and_then(|value| value.last_modified.clone());
    }
    if metadata.etag.is_none() {
        metadata.etag = fallback.and_then(|value| value.etag.clone());
    }
    metadata
}

fn sync_remote_static_gtfs(
    feed_id: &str,
    url: &str,
    allow_invalid_tls: bool,
    cache_path: &Path,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<()> {
    let client = BlockingHttpClient::builder()
        .user_agent("alpha-raptor-engine/0.1")
        .danger_accept_invalid_certs(allow_invalid_tls)
        .build()
        .with_context(|| format!("failed to build HTTP client for static GTFS feed {feed_id}"))?;

    let version_path = remote_static_gtfs_version_path(cache_path);
    let cached_version = match load_remote_static_gtfs_version_metadata(&version_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            warn!(
                %error,
                feed_id,
                metadata = %version_path.display(),
                "failed to load cached remote static GTFS version metadata"
            );
            None
        }
    };

    let mut probed_version = None;
    if cache_path.exists() {
        match refresh_mode {
            StaticGtfsRefreshMode::Bootstrap => {
                match probe_remote_static_gtfs_version(&client, feed_id, url) {
                    Ok(remote_version) => {
                        if remote_static_gtfs_version_changed(cached_version.as_ref(), &remote_version)
                        {
                            info!(
                                feed_id,
                                url,
                                cache = %cache_path.display(),
                                remote_last_modified = remote_version
                                    .last_modified
                                    .as_deref()
                                    .unwrap_or("<missing>"),
                                remote_etag = remote_version.etag.as_deref().unwrap_or("<missing>"),
                                "remote static GTFS differs upstream; bootstrapping from cached zip and deferring sync to background poll"
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            %error,
                            feed_id,
                            url,
                            cache = %cache_path.display(),
                            "failed to probe remote static GTFS metadata at bootstrap; using cached zip"
                        );
                    }
                }
                return Ok(());
            }
            StaticGtfsRefreshMode::Poll => match probe_remote_static_gtfs_version(&client, feed_id, url)
            {
                Ok(remote_version) => {
                    if !remote_static_gtfs_version_changed(cached_version.as_ref(), &remote_version)
                    {
                        return Ok(());
                    }
                    info!(
                        feed_id,
                        url,
                        cache = %cache_path.display(),
                        remote_last_modified = remote_version
                            .last_modified
                            .as_deref()
                            .unwrap_or("<missing>"),
                        remote_etag = remote_version.etag.as_deref().unwrap_or("<missing>"),
                        "detected upstream static GTFS version change"
                    );
                    probed_version = Some(remote_version);
                }
                Err(error) => {
                    warn!(
                        %error,
                        feed_id,
                        url,
                        cache = %cache_path.display(),
                        "failed to probe remote static GTFS metadata during poll; keeping cached zip"
                    );
                    return Ok(());
                }
            },
        }
    }

    let response = client
        .get(url)
        .send()
        .and_then(|response| response.error_for_status())
        .with_context(|| format!("failed to download static GTFS feed {feed_id} from {url}"));

    let (bytes, version_metadata) = match response {
        Ok(response) => {
            let response_version = merged_remote_static_gtfs_version_metadata(
                url,
                remote_static_gtfs_version_from_headers(url, response.headers()),
                probed_version.as_ref(),
            );
            let bytes = response.bytes().with_context(|| {
                format!("failed to read static GTFS response body for feed {feed_id}")
            })?;
            (bytes, response_version)
        }
        Err(error) => {
            if cache_path.exists() {
                warn!(
                    %error,
                    feed_id,
                    url,
                    cache = %cache_path.display(),
                    "remote static GTFS refresh failed, reusing cached zip"
                );
                return Ok(());
            }
            return Err(error);
        }
    };

    if let Ok(existing_bytes) = fs::read(cache_path) {
        if existing_bytes.as_slice() == bytes.as_ref() {
            if let Err(error) =
                store_remote_static_gtfs_version_metadata(&version_path, &version_metadata)
            {
                warn!(
                    %error,
                    feed_id,
                    metadata = %version_path.display(),
                    "failed to persist remote static GTFS version metadata"
                );
            }
            return Ok(());
        }
    }

    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create cache directory for static GTFS feed {} at {}",
                feed_id,
                parent.display()
            )
        })?;
    }

    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let tmp_path = cache_path.with_extension(format!("download-{unique_suffix}.tmp"));
    fs::write(&tmp_path, bytes.as_ref()).with_context(|| {
        format!(
            "failed to write downloaded static GTFS feed {} to temporary file {}",
            feed_id,
            tmp_path.display()
        )
    })?;
    if cache_path.exists() {
        fs::remove_file(cache_path).with_context(|| {
            format!(
                "failed to replace cached static GTFS feed {} at {}",
                feed_id,
                cache_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, cache_path).with_context(|| {
        format!(
            "failed to move downloaded static GTFS feed {} into cache path {}",
            feed_id,
            cache_path.display()
        )
    })?;

    if let Err(error) = store_remote_static_gtfs_version_metadata(&version_path, &version_metadata) {
        warn!(
            %error,
            feed_id,
            metadata = %version_path.display(),
            "failed to persist remote static GTFS version metadata"
        );
    }

    info!(
        feed_id,
        url,
        cache = %cache_path.display(),
        allow_invalid_tls,
        remote_last_modified = version_metadata.last_modified.as_deref().unwrap_or("<missing>"),
        remote_etag = version_metadata.etag.as_deref().unwrap_or("<missing>"),
        "synced remote static GTFS feed"
    );

    Ok(())
}

fn validate_feed_id(feed_id: &str) -> Result<()> {
    let feed_id = feed_id.trim();
    if feed_id.is_empty() {
        bail!("feed id cannot be empty");
    }
    if feed_id.contains(':') {
        bail!("feed id {feed_id} cannot contain ':' because it is reserved for namespaced IDs");
    }
    Ok(())
}

fn validate_feed_dependencies(feeds: &[FeedConfig]) -> Result<()> {
    let feed_positions = feeds
        .iter()
        .enumerate()
        .map(|(position, feed)| (feed.id.clone(), position))
        .collect::<HashMap<_, _>>();

    for feed in feeds {
        for dependency in &feed.depends_on {
            if !feed_positions.contains_key(dependency) {
                bail!("feed {} depends on unknown feed {}", feed.id, dependency);
            }
        }
    }

    let mut visit_state = vec![0u8; feeds.len()];
    for index in 0..feeds.len() {
        visit_feed(index, feeds, &feed_positions, &mut visit_state)?;
    }
    Ok(())
}

fn visit_feed(
    index: usize,
    feeds: &[FeedConfig],
    feed_positions: &HashMap<String, usize>,
    visit_state: &mut [u8],
) -> Result<()> {
    match visit_state[index] {
        1 => bail!(
            "cycle detected in feed dependency graph at {}",
            feeds[index].id
        ),
        2 => return Ok(()),
        _ => {}
    }

    visit_state[index] = 1;
    for dependency in &feeds[index].depends_on {
        if let Some(&dependency_index) = feed_positions.get(dependency) {
            visit_feed(dependency_index, feeds, feed_positions, visit_state)?;
        }
    }
    visit_state[index] = 2;
    Ok(())
}

fn namespaced_id(feed_id: &str, local_id: &str) -> String {
    format!("{feed_id}:{local_id}")
}

fn pack_global_id(feed_index: u16, kind: EntityKind, local_ordinal: u64) -> Result<u64> {
    if local_ordinal > ENTITY_ORDINAL_MASK {
        bail!("entity ordinal {local_ordinal} exceeds the 44-bit local namespace budget");
    }

    let local_index = ((kind as u64) << ENTITY_KIND_SHIFT) | (local_ordinal & ENTITY_ORDINAL_MASK);
    Ok(((u64::from(feed_index)) << 48) | (local_index & GLOBAL_ID_LOCAL_MASK))
}

fn capture_static_inputs_metadata(
    manifest_path: Option<&PathBuf>,
    feeds: &[FeedConfig],
) -> Result<StaticCacheMetadata> {
    let manifest_modified_unix_secs = manifest_path
        .and_then(|path| std::fs::metadata(path).ok())
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());

    let mut feed_sources = Vec::with_capacity(feeds.len());
    for feed in feeds {
        let metadata = std::fs::metadata(&feed.static_gtfs_path).with_context(|| {
            format!(
                "unable to stat GTFS zip for feed {} at {}",
                feed.id,
                feed.static_gtfs_path.display()
            )
        })?;
        feed_sources.push(StaticFeedSourceMetadata {
            feed_id: feed.id.clone(),
            static_gtfs_source: feed.static_gtfs_source.clone(),
            static_gtfs_path: feed.static_gtfs_path.display().to_string(),
            static_gtfs_allow_invalid_tls: feed.static_gtfs_allow_invalid_tls,
            static_gtfs_bytes: metadata.len(),
            static_gtfs_modified_unix_secs: metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs()),
        });
    }

    Ok(StaticCacheMetadata {
        schema_version: STATIC_CACHE_SCHEMA_VERSION,
        manifest_path: manifest_path.map(|path| path.display().to_string()),
        manifest_modified_unix_secs,
        feed_sources,
    })
}

fn parse_clock_time(value: &str) -> Result<NaiveTime> {
    NaiveTime::parse_from_str(value, "%H:%M")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M:%S"))
        .context("expected HH:MM or HH:MM:SS")
}

fn file_size(path: &PathBuf) -> u64 {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn build_stop_cells(stops: &[StopRecord], active_stop_indices: &[usize]) -> HashMap<u64, Vec<usize>> {
    let mut stop_cells = HashMap::<u64, Vec<usize>>::new();
    for &stop_index in active_stop_indices {
        let stop = &stops[stop_index];
        if let Some(cell) = geo_destination_cell(stop.latitude, stop.longitude) {
            stop_cells.entry(cell).or_default().push(stop_index);
        }
    }
    stop_cells
}

fn virtual_stop_result(label: &str, latitude: f64, longitude: f64) -> StopSearchResult {
    let local_id = format!("{latitude:.6},{longitude:.6}");
    StopSearchResult {
        global_id: virtual_stop_global_id(label, latitude, longitude),
        feed_id: "virtual".to_owned(),
        local_id: local_id.clone(),
        id: format!("virtual:{local_id}"),
        code: None,
        name: format!("{label} ({latitude:.5}, {longitude:.5})"),
        latitude: Some(latitude),
        longitude: Some(longitude),
        is_virtual: true,
    }
}

fn virtual_stop_global_id(label: &str, latitude: f64, longitude: f64) -> u64 {
    let mut hasher = DefaultHasher::new();
    label.hash(&mut hasher);
    latitude.to_bits().hash(&mut hasher);
    longitude.to_bits().hash(&mut hasher);
    0xFFFF_0000_0000_0000 | (hasher.finish() & 0x0000_FFFF_FFFF_FFFF)
}

fn load_or_build_static_core(
    config: &EngineConfig,
    previous: Option<&Engine>,
) -> Result<StaticCoreLoadResult> {
    let mut timings = BuildTimings::default();
    let metadata = build_static_cache_metadata(config)?;
    let cache_path = static_cache_path(config);

    let cache_read_started = Instant::now();
    if let Some(cache) = load_static_cache(&cache_path, &metadata)? {
        timings.static_cache_read_ms = cache_read_started.elapsed().as_millis();
        info!(
            cache = %cache_path.display(),
            static_cache_read_ms = timings.static_cache_read_ms,
            cache_bytes = file_size(&cache_path),
            "loaded static transit core from cache"
        );
        return Ok(StaticCoreLoadResult {
            core: cache.core,
            cache_hit: true,
            cache_bytes: file_size(&cache_path),
            timings,
            update_strategy: "cache-hit",
            diff_summary: StaticDiffSummary::default(),
        });
    }
    timings.static_cache_read_ms = cache_read_started.elapsed().as_millis();

    let gtfs_parse_started = Instant::now();
    let mut parsed_feeds = Vec::<(FeedConfig, Gtfs)>::with_capacity(config.feeds.len());
    for feed in &config.feeds {
        let gtfs = Gtfs::from_path(&feed.static_gtfs_path).with_context(|| {
            format!(
                "unable to read GTFS for feed {} from {}",
                feed.id,
                feed.static_gtfs_path.display()
            )
        })?;
        parsed_feeds.push((feed.clone(), gtfs));
    }
    timings.gtfs_parse_ms = gtfs_parse_started.elapsed().as_millis();
    info!(
        gtfs_parse_ms = timings.gtfs_parse_ms,
        feed_count = parsed_feeds.len(),
        "parsed GTFS zip into memory"
    );

    let transit_model_started = Instant::now();
    let built_core = build_static_core(&parsed_feeds)?;
    let (core, update_strategy, diff_summary) = if let Some(previous_engine) = previous {
        let diff_summary = summarize_static_divergence(&previous_engine.static_data, &built_core);
        if diff_summary.divergence_ratio <= config.static_diff_tolerance {
            (
                merge_append_only_core(&previous_engine.static_data, built_core)?,
                "differential-injection-append-only",
                diff_summary,
            )
        } else {
            (built_core, "full-rebuild", diff_summary)
        }
    } else {
        (
            built_core,
            "cold-start-full-build",
            StaticDiffSummary::default(),
        )
    };
    timings.transit_model_ms = transit_model_started.elapsed().as_millis();
    info!(
        transit_model_ms = timings.transit_model_ms,
        feed_count = core.feeds.len(),
        stops = core.active_stop_indices.len(),
        routes = core.active_route_indices.len(),
        trips = core.active_trip_indices.len(),
        lines = core.lines.len(),
        update_strategy,
        divergence_ratio = diff_summary.divergence_ratio,
        "built transit core from GTFS"
    );

    let cache_write_started = Instant::now();
    let cache = StaticCache { metadata, core };
    store_static_cache(&cache_path, &cache)?;
    timings.static_cache_write_ms = cache_write_started.elapsed().as_millis();
    info!(
        cache = %cache_path.display(),
        static_cache_write_ms = timings.static_cache_write_ms,
        cache_bytes = file_size(&cache_path),
        "stored static transit core cache"
    );

    Ok(StaticCoreLoadResult {
        core: cache.core,
        cache_hit: false,
        cache_bytes: file_size(&cache_path),
        timings,
        update_strategy,
        diff_summary,
    })
}

fn summarize_static_divergence(previous: &StaticData, next: &StaticCore) -> StaticDiffSummary {
    let changed_stops = collect_changed_stop_ids(previous, next);
    let changed_routes = collect_changed_route_ids(previous, next);
    let changed_trips = collect_changed_trip_ids(previous, next);
    let mutated_entities = changed_stops.len() + changed_routes.len() + changed_trips.len();
    let total_entities = previous.active_stop_indices.len().max(next.active_stop_indices.len())
        + previous.active_route_indices.len().max(next.active_route_indices.len())
        + previous.active_trip_indices.len().max(next.active_trip_indices.len());
    let divergence_ratio = if total_entities == 0 {
        0.0
    } else {
        mutated_entities as f64 / total_entities as f64
    };

    StaticDiffSummary {
        differential_applied: mutated_entities > 0,
        divergence_ratio,
        mutated_entities,
        total_entities,
    }
}

fn merge_append_only_core(previous: &StaticData, next: StaticCore) -> Result<StaticCore> {
    let StaticCore {
        feeds,
        stops: next_stops,
        active_stop_indices: next_active_stop_indices,
        stop_lookup: _,
        stop_lookup_by_global_id: _,
        routes: next_routes,
        active_route_indices: next_active_route_indices,
        trips: next_trips,
        active_trip_indices: next_active_trip_indices,
        trip_lookup_by_feed: _,
        service_to_trip_indices: next_service_to_trip_indices,
        lines: _,
        stop_to_lines: _,
        service_by_date,
        shapes,
    } = next;

    let mut stops = previous.stops.clone();
    let mut routes = previous.routes.clone();
    let mut trips = previous.trips.clone();

    let mut stop_global_ids = stops.iter().map(|stop| stop.global_id).collect::<HashSet<_>>();
    let mut route_global_ids = routes
        .iter()
        .map(|route| route.global_id)
        .collect::<HashSet<_>>();
    let mut trip_global_ids = trips.iter().map(|trip| trip.global_id).collect::<HashSet<_>>();

    let previous_stop_lookup = previous
        .active_stop_indices
        .iter()
        .map(|index| (previous.stops[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();
    let previous_route_lookup = previous
        .active_route_indices
        .iter()
        .map(|index| (previous.routes[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();
    let previous_trip_lookup = previous
        .active_trip_indices
        .iter()
        .map(|index| (previous.trips[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();

    let mut active_stop_indices = Vec::with_capacity(next_active_stop_indices.len());
    let mut stop_lookup = HashMap::with_capacity(next_active_stop_indices.len());
    let mut stop_lookup_by_global_id = HashMap::with_capacity(next_active_stop_indices.len());
    let mut full_to_merged_stop = HashMap::<usize, usize>::with_capacity(next_stops.len());

    for &next_index in &next_active_stop_indices {
        let next_stop = &next_stops[next_index];
        let merged_index = if let Some(previous_index) = previous_stop_lookup.get(&next_stop.id) {
            if stop_records_equivalent(&previous.stops[*previous_index], next_stop) {
                *previous_index
            } else {
                append_stop_record(
                    &mut stops,
                    next_stop,
                    &mut stop_global_ids,
                )?
            }
        } else {
            append_stop_record(&mut stops, next_stop, &mut stop_global_ids)?
        };

        active_stop_indices.push(merged_index);
        full_to_merged_stop.insert(next_index, merged_index);
        stop_lookup.insert(stops[merged_index].id.clone(), merged_index);
        stop_lookup_by_global_id.insert(stops[merged_index].global_id, merged_index);
    }

    let mut active_route_indices = Vec::with_capacity(next_active_route_indices.len());
    let mut full_to_merged_route = HashMap::<usize, usize>::with_capacity(next_routes.len());
    for &next_index in &next_active_route_indices {
        let next_route = &next_routes[next_index];
        let merged_index = if let Some(previous_index) = previous_route_lookup.get(&next_route.id) {
            if route_records_equivalent(&previous.routes[*previous_index], next_route) {
                *previous_index
            } else {
                append_route_record(
                    &mut routes,
                    next_route,
                    &mut route_global_ids,
                )?
            }
        } else {
            append_route_record(&mut routes, next_route, &mut route_global_ids)?
        };

        active_route_indices.push(merged_index);
        full_to_merged_route.insert(next_index, merged_index);
    }

    let mut active_trip_indices = Vec::with_capacity(next_active_trip_indices.len());
    let mut full_to_merged_trip = HashMap::<usize, usize>::with_capacity(next_trips.len());
    for &next_index in &next_active_trip_indices {
        let next_trip = &next_trips[next_index];
        let merged_index = if let Some(previous_index) = previous_trip_lookup.get(&next_trip.id) {
            if trip_records_equivalent(
                &previous.trips[*previous_index],
                &previous.routes,
                &previous.stops,
                next_trip,
                &next_routes,
                &next_stops,
            ) {
                *previous_index
            } else {
                append_trip_record(
                    &mut trips,
                    next_trip,
                    &full_to_merged_stop,
                    &full_to_merged_route,
                    &mut trip_global_ids,
                )?
            }
        } else {
            append_trip_record(
                &mut trips,
                next_trip,
                &full_to_merged_stop,
                &full_to_merged_route,
                &mut trip_global_ids,
            )?
        };

        active_trip_indices.push(merged_index);
        full_to_merged_trip.insert(next_index, merged_index);
    }

    let mut trip_lookup_by_feed = vec![HashMap::<String, usize>::new(); feeds.len()];
    for &trip_index in &active_trip_indices {
        let trip = &trips[trip_index];
        trip_lookup_by_feed[usize::from(trip.feed_index)].insert(trip.local_id.clone(), trip_index);
    }

    let mut service_to_trip_indices = HashMap::<String, Vec<usize>>::new();
    for (service_id, trip_indices) in next_service_to_trip_indices {
        let remapped = trip_indices
            .into_iter()
            .filter_map(|trip_index| full_to_merged_trip.get(&trip_index).copied())
            .collect::<Vec<_>>();
        if !remapped.is_empty() {
            service_to_trip_indices.insert(service_id, remapped);
        }
    }

    let (lines, stop_to_lines) = rebuild_lines_and_stop_to_lines(&trips, &active_trip_indices, stops.len());

    Ok(StaticCore {
        feeds,
        stops,
        active_stop_indices,
        stop_lookup,
        stop_lookup_by_global_id,
        routes,
        active_route_indices,
        trips,
        active_trip_indices,
        trip_lookup_by_feed,
        service_to_trip_indices,
        lines,
        stop_to_lines,
        service_by_date,
        shapes,
    })
}

fn build_static_core(parsed_feeds: &[(FeedConfig, Gtfs)]) -> Result<StaticCore> {
    let total_stops = parsed_feeds.iter().map(|(_, gtfs)| gtfs.stops.len()).sum();
    let total_routes = parsed_feeds.iter().map(|(_, gtfs)| gtfs.routes.len()).sum();
    let total_trips = parsed_feeds.iter().map(|(_, gtfs)| gtfs.trips.len()).sum();

    let mut feeds = Vec::with_capacity(parsed_feeds.len());
    let mut stops = Vec::with_capacity(total_stops);
    let mut active_stop_indices = Vec::with_capacity(total_stops);
    let mut stop_lookup = HashMap::with_capacity(total_stops);
    let mut stop_lookup_by_global_id = HashMap::with_capacity(total_stops);
    let mut routes = Vec::with_capacity(total_routes);
    let mut active_route_indices = Vec::with_capacity(total_routes);
    let mut trips = Vec::with_capacity(total_trips);
    let mut active_trip_indices = Vec::with_capacity(total_trips);
    let mut trip_lookup_by_feed = vec![HashMap::<String, usize>::new(); parsed_feeds.len()];
    let mut service_to_trip_indices = HashMap::<String, Vec<usize>>::new();
    let mut service_by_date = HashMap::<NaiveDate, HashSet<String>>::new();
    let mut shapes = HashMap::new();

    for (feed, gtfs) in parsed_feeds {
        feeds.push(FeedRecord {
            feed_index: feed.feed_index,
            id: feed.id.clone(),
            static_gtfs_path: feed.static_gtfs_path.display().to_string(),
            depends_on: feed.depends_on.clone(),
        });

        let mut local_stop_lookup = HashMap::with_capacity(gtfs.stops.len());
        let mut ordered_stops: Vec<_> = gtfs.stops.values().collect();
        ordered_stops.sort_by(|left, right| left.id.cmp(&right.id));
        for (local_index, stop) in ordered_stops.into_iter().enumerate() {
            let name = stop
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(stop.id.as_str())
                .to_owned();
            let global_id = pack_global_id(feed.feed_index, EntityKind::Stop, local_index as u64)?;
            let namespaced_id = namespaced_id(&feed.id, &stop.id);
            let search_blob = format!(
                "{} {} {} {} {}",
                namespaced_id,
                stop.id,
                stop.code.as_deref().unwrap_or_default(),
                feed.id,
                name
            )
            .to_lowercase();

            let index = stops.len();
            stops.push(StopRecord {
                global_id,
                feed_index: feed.feed_index,
                feed_id: feed.id.clone(),
                local_id: stop.id.clone(),
                id: namespaced_id.clone(),
                code: stop.code.clone(),
                name,
                latitude: stop.latitude,
                longitude: stop.longitude,
                search_blob,
            });
            active_stop_indices.push(index);
            stop_lookup.insert(namespaced_id, index);
            stop_lookup_by_global_id.insert(global_id, index);
            local_stop_lookup.insert(stop.id.clone(), index);
        }

        let mut route_lookup = HashMap::with_capacity(gtfs.routes.len());
        let mut ordered_routes: Vec<_> = gtfs.routes.values().collect();
        ordered_routes.sort_by(|left, right| left.id.cmp(&right.id));
        for (local_index, route) in ordered_routes.into_iter().enumerate() {
            let color = route
                .color
                .map(|value| format!("#{:02X}{:02X}{:02X}", value.r, value.g, value.b));
            let text_color = route
                .text_color
                .map(|value| format!("#{:02X}{:02X}{:02X}", value.r, value.g, value.b));
            let index = routes.len();
            routes.push(RouteRecord {
                global_id: pack_global_id(feed.feed_index, EntityKind::Route, local_index as u64)?,
                feed_index: feed.feed_index,
                feed_id: feed.id.clone(),
                local_id: route.id.clone(),
                id: namespaced_id(&feed.id, &route.id),
                short_name: route.short_name.clone(),
                long_name: route.long_name.clone(),
                route_type: format!("{:?}", route.route_type),
                color,
                text_color,
            });
            active_route_indices.push(index);
            route_lookup.insert(route.id.clone(), index);
        }

        for (shape_id, points) in &gtfs.shapes {
            let mut shape_points = Vec::with_capacity(points.len());
            for point in points {
                shape_points.push(ShapePoint {
                    lat: point.latitude,
                    lon: point.longitude,
                    dist_traveled: point.dist_traveled,
                });
            }
            shapes.insert(namespaced_id(&feed.id, shape_id), shape_points);
        }

        let mut ordered_trips: Vec<_> = gtfs.trips.values().collect();
        ordered_trips.sort_by(|left, right| left.id.cmp(&right.id));
        for (local_index, trip) in ordered_trips.into_iter().enumerate() {
            let route_index = *route_lookup
                .get(&trip.route_id)
                .with_context(|| format!("missing route {} for trip {}", trip.route_id, trip.id))?;
            let service_id = namespaced_id(&feed.id, &trip.service_id);
            let mut stop_times = Vec::with_capacity(trip.stop_times.len());
            for stop_time in &trip.stop_times {
                let stop_index = *local_stop_lookup.get(&stop_time.stop.id).with_context(|| {
                    format!(
                        "missing stop {} for trip {} in feed {}",
                        stop_time.stop.id, trip.id, feed.id
                    )
                })?;
                let arrival_secs = stop_time
                    .arrival_time
                    .or(stop_time.departure_time)
                    .map(|value| value as i32)
                    .ok_or_else(|| anyhow!("trip {} has stop_times without times", trip.id))?;
                let departure_secs = stop_time
                    .departure_time
                    .or(stop_time.arrival_time)
                    .map(|value| value as i32)
                    .ok_or_else(|| anyhow!("trip {} has stop_times without times", trip.id))?;
                stop_times.push(TripStopRecord {
                    stop_index,
                    arrival_secs,
                    departure_secs,
                    stop_sequence: stop_time.stop_sequence,
                    shape_dist_traveled: stop_time.shape_dist_traveled,
                });
            }

            if stop_times.len() < 2 {
                continue;
            }

            let namespaced_trip_id = namespaced_id(&feed.id, &trip.id);
            let index = trips.len();
            trips.push(TripRecord {
                global_id: pack_global_id(feed.feed_index, EntityKind::Trip, local_index as u64)?,
                feed_index: feed.feed_index,
                feed_id: feed.id.clone(),
                local_id: trip.id.clone(),
                id: namespaced_trip_id.clone(),
                route_index,
                shape_id: trip
                    .shape_id
                    .as_ref()
                    .map(|shape_id| namespaced_id(&feed.id, shape_id)),
                headsign: trip.trip_headsign.clone(),
                stop_times,
            });
            active_trip_indices.push(index);
            trip_lookup_by_feed[usize::from(feed.feed_index)].insert(trip.id.clone(), index);
            service_to_trip_indices
                .entry(service_id)
                .or_default()
                .push(index);
        }

        merge_service_calendar(&mut service_by_date, &feed.id, gtfs);
    }

    let mut lines = Vec::<LineRecord>::new();
    let mut line_lookup = HashMap::<LineKey, usize>::new();
    for &trip_index in &active_trip_indices {
        let trip = &trips[trip_index];
        let key = LineKey {
            route_index: trip.route_index,
            stop_indices: trip.stop_times.iter().map(|stop| stop.stop_index).collect(),
        };
        let line_index = if let Some(index) = line_lookup.get(&key) {
            *index
        } else {
            let index = lines.len();
            lines.push(LineRecord {
                stop_indices: key.stop_indices.clone(),
                trip_indices: Vec::new(),
                scheduled_departures_by_stop: Vec::new(),
                chronos_bucket_start_indices_by_stop: Vec::new(),
                binary_searchable_by_stop: Vec::new(),
                trip_order_indirection_by_stop: Vec::new(),
            });
            line_lookup.insert(key, index);
            index
        };
        lines[line_index].trip_indices.push(trip_index);
    }

    finalize_line_temporal_indices(&mut lines, &trips);

    let mut stop_to_lines = vec![Vec::<StopLineRef>::new(); stops.len()];
    for (line_index, line) in lines.iter().enumerate() {
        for (stop_pos, stop_index) in line.stop_indices.iter().copied().enumerate() {
            stop_to_lines[stop_index].push(StopLineRef {
                line_index,
                stop_pos,
            });
        }
    }

    Ok(StaticCore {
        feeds,
        stops,
        active_stop_indices,
        stop_lookup,
        stop_lookup_by_global_id,
        routes,
        active_route_indices,
        trips,
        active_trip_indices,
        trip_lookup_by_feed,
        service_to_trip_indices,
        lines,
        stop_to_lines,
        service_by_date,
        shapes,
    })
}

fn collect_changed_stop_ids(previous: &StaticData, next: &StaticCore) -> HashSet<String> {
    collect_changed_stop_ids_from_slices(
        &previous.stops,
        &previous.active_stop_indices,
        &next.stops,
        &next.active_stop_indices,
    )
}

fn collect_changed_route_ids(previous: &StaticData, next: &StaticCore) -> HashSet<String> {
    let previous_lookup = previous
        .active_route_indices
        .iter()
        .map(|index| (previous.routes[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();
    let next_lookup = next
        .active_route_indices
        .iter()
        .map(|index| (next.routes[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();

    let mut changed = HashSet::new();
    for (route_id, next_index) in &next_lookup {
        match previous_lookup.get(route_id) {
            Some(previous_index) => {
                if !route_records_equivalent(&previous.routes[*previous_index], &next.routes[*next_index]) {
                    changed.insert(route_id.clone());
                }
            }
            None => {
                changed.insert(route_id.clone());
            }
        }
    }
    for route_id in previous_lookup.keys() {
        if !next_lookup.contains_key(route_id) {
            changed.insert(route_id.clone());
        }
    }
    changed
}

fn collect_changed_trip_ids(previous: &StaticData, next: &StaticCore) -> HashSet<String> {
    let previous_lookup = previous
        .active_trip_indices
        .iter()
        .map(|index| (previous.trips[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();
    let next_lookup = next
        .active_trip_indices
        .iter()
        .map(|index| (next.trips[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();

    let mut changed = HashSet::new();
    for (trip_id, next_index) in &next_lookup {
        match previous_lookup.get(trip_id) {
            Some(previous_index) => {
                if !trip_records_equivalent(
                    &previous.trips[*previous_index],
                    &previous.routes,
                    &previous.stops,
                    &next.trips[*next_index],
                    &next.routes,
                    &next.stops,
                ) {
                    changed.insert(trip_id.clone());
                }
            }
            None => {
                changed.insert(trip_id.clone());
            }
        }
    }
    for trip_id in previous_lookup.keys() {
        if !next_lookup.contains_key(trip_id) {
            changed.insert(trip_id.clone());
        }
    }
    changed
}

fn collect_changed_stop_ids_from_slices(
    previous_stops: &[StopRecord],
    previous_active_indices: &[usize],
    next_stops: &[StopRecord],
    next_active_indices: &[usize],
) -> HashSet<String> {
    let previous_lookup = previous_active_indices
        .iter()
        .map(|index| (previous_stops[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();
    let next_lookup = next_active_indices
        .iter()
        .map(|index| (next_stops[*index].id.clone(), *index))
        .collect::<HashMap<_, _>>();

    let mut changed = HashSet::new();
    for (stop_id, next_index) in &next_lookup {
        match previous_lookup.get(stop_id) {
            Some(previous_index) => {
                if !stop_records_equivalent(&previous_stops[*previous_index], &next_stops[*next_index]) {
                    changed.insert(stop_id.clone());
                }
            }
            None => {
                changed.insert(stop_id.clone());
            }
        }
    }
    for stop_id in previous_lookup.keys() {
        if !next_lookup.contains_key(stop_id) {
            changed.insert(stop_id.clone());
        }
    }
    changed
}

fn stop_records_equivalent(left: &StopRecord, right: &StopRecord) -> bool {
    left.id == right.id
        && left.code == right.code
        && left.name == right.name
        && left.latitude == right.latitude
        && left.longitude == right.longitude
}

fn route_records_equivalent(left: &RouteRecord, right: &RouteRecord) -> bool {
    left.id == right.id
        && left.short_name == right.short_name
        && left.long_name == right.long_name
        && left.route_type == right.route_type
        && left.color == right.color
        && left.text_color == right.text_color
}

fn trip_records_equivalent(
    left: &TripRecord,
    left_routes: &[RouteRecord],
    left_stops: &[StopRecord],
    right: &TripRecord,
    right_routes: &[RouteRecord],
    right_stops: &[StopRecord],
) -> bool {
    if !route_records_equivalent(&left_routes[left.route_index], &right_routes[right.route_index]) {
        return false;
    }
    if left.shape_id != right.shape_id || left.headsign != right.headsign {
        return false;
    }
    if left.stop_times.len() != right.stop_times.len() {
        return false;
    }

    left.stop_times.iter().zip(&right.stop_times).all(|(left_stop, right_stop)| {
        left_stop.arrival_secs == right_stop.arrival_secs
            && left_stop.departure_secs == right_stop.departure_secs
            && left_stop.stop_sequence == right_stop.stop_sequence
            && left_stops[left_stop.stop_index].id == right_stops[right_stop.stop_index].id
    })
}

fn append_stop_record(
    stops: &mut Vec<StopRecord>,
    next_stop: &StopRecord,
    global_ids: &mut HashSet<u64>,
) -> Result<usize> {
    let mut record = next_stop.clone();
    record.global_id = allocate_unique_global_id(
        record.global_id,
        record.feed_index,
        EntityKind::Stop,
        &record.local_id,
        global_ids,
    )?;
    let index = stops.len();
    stops.push(record);
    Ok(index)
}

fn append_route_record(
    routes: &mut Vec<RouteRecord>,
    next_route: &RouteRecord,
    global_ids: &mut HashSet<u64>,
) -> Result<usize> {
    let mut record = next_route.clone();
    record.global_id = allocate_unique_global_id(
        record.global_id,
        record.feed_index,
        EntityKind::Route,
        &record.local_id,
        global_ids,
    )?;
    let index = routes.len();
    routes.push(record);
    Ok(index)
}

fn append_trip_record(
    trips: &mut Vec<TripRecord>,
    next_trip: &TripRecord,
    stop_index_remap: &HashMap<usize, usize>,
    route_index_remap: &HashMap<usize, usize>,
    global_ids: &mut HashSet<u64>,
) -> Result<usize> {
    let mut record = next_trip.clone();
    record.global_id = allocate_unique_global_id(
        record.global_id,
        record.feed_index,
        EntityKind::Trip,
        &record.local_id,
        global_ids,
    )?;
    record.route_index = *route_index_remap
        .get(&next_trip.route_index)
        .ok_or_else(|| anyhow!("missing remapped route for trip {}", next_trip.id))?;
    for stop_time in &mut record.stop_times {
        stop_time.stop_index = *stop_index_remap
            .get(&stop_time.stop_index)
            .ok_or_else(|| anyhow!("missing remapped stop for trip {}", next_trip.id))?;
    }
    let index = trips.len();
    trips.push(record);
    Ok(index)
}

fn allocate_unique_global_id(
    proposed: u64,
    feed_index: u16,
    kind: EntityKind,
    local_id: &str,
    global_ids: &mut HashSet<u64>,
) -> Result<u64> {
    if global_ids.insert(proposed) {
        return Ok(proposed);
    }

    let mut salt = 1u64;
    loop {
        let local_ordinal = hash_local_ordinal(local_id, salt);
        let candidate = pack_global_id(feed_index, kind, local_ordinal)?;
        if global_ids.insert(candidate) {
            return Ok(candidate);
        }
        salt += 1;
    }
}

fn hash_local_ordinal(local_id: &str, salt: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    local_id.hash(&mut hasher);
    salt.hash(&mut hasher);
    hasher.finish() & ENTITY_ORDINAL_MASK
}

fn rebuild_lines_and_stop_to_lines(
    trips: &[TripRecord],
    active_trip_indices: &[usize],
    stop_count: usize,
) -> (Vec<LineRecord>, Vec<Vec<StopLineRef>>) {
    let mut lines = Vec::<LineRecord>::new();
    let mut line_lookup = HashMap::<LineKey, usize>::new();

    for &trip_index in active_trip_indices {
        let trip = &trips[trip_index];
        let key = LineKey {
            route_index: trip.route_index,
            stop_indices: trip.stop_times.iter().map(|stop| stop.stop_index).collect(),
        };
        let line_index = if let Some(index) = line_lookup.get(&key) {
            *index
        } else {
            let index = lines.len();
            lines.push(LineRecord {
                stop_indices: key.stop_indices.clone(),
                trip_indices: Vec::new(),
                scheduled_departures_by_stop: Vec::new(),
                chronos_bucket_start_indices_by_stop: Vec::new(),
                binary_searchable_by_stop: Vec::new(),
                trip_order_indirection_by_stop: Vec::new(),
            });
            line_lookup.insert(key, index);
            index
        };
        lines[line_index].trip_indices.push(trip_index);
    }

    finalize_line_temporal_indices(&mut lines, &trips);

    let mut stop_to_lines = vec![Vec::<StopLineRef>::new(); stop_count];
    for (line_index, line) in lines.iter().enumerate() {
        for (stop_pos, stop_index) in line.stop_indices.iter().copied().enumerate() {
            stop_to_lines[stop_index].push(StopLineRef {
                line_index,
                stop_pos,
            });
        }
    }

    (lines, stop_to_lines)
}

fn finalize_line_temporal_indices(lines: &mut [LineRecord], trips: &[TripRecord]) {
    for line in lines {
        line.trip_indices
            .sort_by_key(|trip_index| trips[*trip_index].stop_times[0].departure_secs);

        let mut scheduled_departures_by_stop =
            vec![Vec::with_capacity(line.trip_indices.len()); line.stop_indices.len()];
        let mut previous_departures = vec![i32::MIN; line.stop_indices.len()];
        let mut binary_searchable_by_stop = vec![true; line.stop_indices.len()];

        for trip_index in &line.trip_indices {
            let trip = &trips[*trip_index];
            for (stop_pos, stop_time) in trip.stop_times.iter().enumerate() {
                let departure_secs = stop_time.departure_secs;
                if departure_secs < previous_departures[stop_pos] {
                    binary_searchable_by_stop[stop_pos] = false;
                }
                previous_departures[stop_pos] = departure_secs;
                scheduled_departures_by_stop[stop_pos].push(departure_secs);
            }
        }

        let mut chronos_bucket_start_indices_by_stop =
            Vec::with_capacity(line.stop_indices.len());
        let mut trip_order_indirection_by_stop = Vec::with_capacity(line.stop_indices.len());

        for (stop_pos, departures) in scheduled_departures_by_stop.iter_mut().enumerate() {
            if binary_searchable_by_stop[stop_pos] {
                chronos_bucket_start_indices_by_stop
                    .push(build_chronos_bucket_start_indices(departures));
                trip_order_indirection_by_stop.push(Vec::new());
                continue;
            }

            let mut virtual_positions = (0..departures.len())
                .map(|trip_position| trip_position as u32)
                .collect::<Vec<_>>();
            virtual_positions.sort_by_key(|trip_position| departures[*trip_position as usize]);

            let virtual_departures = virtual_positions
                .iter()
                .map(|trip_position| departures[*trip_position as usize])
                .collect::<Vec<_>>();
            *departures = virtual_departures;
            chronos_bucket_start_indices_by_stop
                .push(build_chronos_bucket_start_indices(departures));
            trip_order_indirection_by_stop.push(virtual_positions);
        }

        line.scheduled_departures_by_stop = scheduled_departures_by_stop;
        line.chronos_bucket_start_indices_by_stop = chronos_bucket_start_indices_by_stop;
        line.binary_searchable_by_stop = binary_searchable_by_stop;
        line.trip_order_indirection_by_stop = trip_order_indirection_by_stop;
    }
}

fn build_chronos_bucket_start_indices(departures: &[i32]) -> Vec<u32> {
    if departures.is_empty() {
        return Vec::new();
    }

    let max_departure_secs = departures.last().copied().unwrap_or_default().max(0);
    let bucket_count = ((max_departure_secs / CHRONOS_BUCKET_SECS) + 2) as usize;
    let mut indices = Vec::with_capacity(bucket_count);
    let mut cursor = 0usize;

    for bucket_index in 0..bucket_count {
        let bucket_start_secs = (bucket_index as i32) * CHRONOS_BUCKET_SECS;
        while cursor < departures.len() && departures[cursor] < bucket_start_secs {
            cursor += 1;
        }
        indices.push(cursor as u32);
    }

    indices
}

fn chronos_bucket_start_index(
    line: &LineRecord,
    stop_pos: usize,
    ready_at: i32,
    safety_lookback_secs: i32,
) -> Option<usize> {
    let buckets = line.chronos_bucket_start_indices_by_stop.get(stop_pos)?;
    if buckets.is_empty() {
        return None;
    }

    let safe_ready_at = ready_at.saturating_sub(safety_lookback_secs).max(0);
    let bucket_index = (safe_ready_at / CHRONOS_BUCKET_SECS) as usize;
    let clamped_bucket_index = bucket_index.min(buckets.len().saturating_sub(1));
    Some(buckets[clamped_bucket_index] as usize)
}

fn line_temporal_search_len(line: &LineRecord, stop_pos: usize) -> usize {
    line.trip_order_indirection_by_stop
        .get(stop_pos)
        .filter(|order| !order.is_empty())
        .map(|order| order.len())
        .unwrap_or(line.trip_indices.len())
}

fn line_trip_index_at_temporal_position(
    line: &LineRecord,
    stop_pos: usize,
    temporal_position: usize,
) -> Option<usize> {
    let physical_position = line
        .trip_order_indirection_by_stop
        .get(stop_pos)
        .filter(|order| !order.is_empty())
        .and_then(|order| order.get(temporal_position).copied().map(|value| value as usize))
        .unwrap_or(temporal_position);
    line.trip_indices.get(physical_position).copied()
}

fn build_full_walker_matrix(config: &EngineConfig, stops: &[StopRecord]) -> WalkerBuildResult {
    match build_or_load_walker_transfers(
        &config.osm_pbf_path,
        &runtime_cache_dir(&config.workspace_root, "osm"),
        stops,
        config.walk_radius_meters,
        config.walk_speed_mps,
        config.max_transfer_candidates,
    ) {
        Ok(result) => result,
        Err(error) => {
            warn!(
                %error,
                "failed to build OSM walker matrix, falling back to radius-haversine transfers"
            );
            WalkerBuildResult::fallback(build_haversine_transfers(
                stops,
                config.walk_radius_meters,
                config.walk_speed_mps,
                config.max_transfer_candidates,
            ))
        }
    }
}

fn rebuild_differential_walker_transfers(
    config: &EngineConfig,
    previous: &StaticData,
    next_stops: &[StopRecord],
    next_active_stop_indices: &[usize],
) -> Result<WalkerBuildResult> {
    let changed_stop_ids = collect_changed_stop_ids_from_slices(
        &previous.stops,
        &previous.active_stop_indices,
        next_stops,
        next_active_stop_indices,
    );
    let affected_indices = expand_affected_stop_indices(
        &previous.stops,
        &previous.active_stop_indices,
        next_stops,
        next_active_stop_indices,
        &changed_stop_ids,
        config.walk_radius_meters,
    );

    let subset = rebuild_walker_transfers_subset(
        &config.osm_pbf_path,
        next_stops,
        &affected_indices,
        config.walk_radius_meters,
        config.walk_speed_mps,
        config.max_transfer_candidates,
    )?;

    let mut transfers = previous.transfers.clone();
    if transfers.len() < next_stops.len() {
        transfers.resize(next_stops.len(), Vec::new());
    }
    for (source_index, updated_transfers) in subset.updated_transfers {
        transfers[source_index] = updated_transfers;
    }

    Ok(WalkerBuildResult {
        transfers,
        strategy: "osm-pbf-differential-local-rebuild",
        cache_hit: false,
        graph_nodes: subset.graph_nodes,
        graph_edges: subset.graph_edges,
        anchored_stops: subset.anchored_stops,
    })
}

fn expand_affected_stop_indices(
    previous_stops: &[StopRecord],
    previous_active_stop_indices: &[usize],
    next_stops: &[StopRecord],
    next_active_stop_indices: &[usize],
    changed_stop_ids: &HashSet<String>,
    walk_radius_meters: f64,
) -> Vec<usize> {
    let next_stop_index = RTree::bulk_load(
        next_active_stop_indices
            .iter()
            .filter_map(|index| {
                let stop = &next_stops[*index];
                Some(IndexedStopPoint {
                    index: *index,
                    point: [stop.longitude?, stop.latitude?],
                })
            })
            .collect(),
    );

    let mut affected = HashSet::<usize>::new();
    for &index in next_active_stop_indices {
        if changed_stop_ids.contains(&next_stops[index].id) {
            affected.insert(index);
        }
    }

    for source in previous_active_stop_indices
        .iter()
        .map(|index| &previous_stops[*index])
        .chain(next_active_stop_indices.iter().map(|index| &next_stops[*index]))
    {
        if !changed_stop_ids.contains(&source.id) {
            continue;
        }
        let (Some(latitude), Some(longitude)) = (source.latitude, source.longitude) else {
            continue;
        };

        let lat_delta = walk_radius_meters / 111_320.0;
        let lon_delta = walk_radius_meters
            / (111_320.0 * latitude.to_radians().cos().abs().max(0.25));
        let envelope = AABB::from_corners(
            [longitude - lon_delta, latitude - lat_delta],
            [longitude + lon_delta, latitude + lat_delta],
        );

        for candidate in next_stop_index.locate_in_envelope_intersecting(&envelope) {
            let stop = &next_stops[candidate.index];
            let (Some(candidate_lat), Some(candidate_lon)) = (stop.latitude, stop.longitude) else {
                continue;
            };
            if haversine_meters(latitude, longitude, candidate_lat, candidate_lon)
                <= walk_radius_meters
            {
                affected.insert(candidate.index);
            }
        }
    }

    let mut affected = affected.into_iter().collect::<Vec<_>>();
    affected.sort_unstable();
    affected
}

fn static_metadata_generation_token(metadata: &StaticCacheMetadata) -> u64 {
    let mut hasher = DefaultHasher::new();
    metadata.schema_version.hash(&mut hasher);
    metadata.manifest_path.hash(&mut hasher);
    metadata.manifest_modified_unix_secs.hash(&mut hasher);
    for source in &metadata.feed_sources {
        source.feed_id.hash(&mut hasher);
        source.static_gtfs_source.hash(&mut hasher);
        source.static_gtfs_path.hash(&mut hasher);
        source.static_gtfs_allow_invalid_tls.hash(&mut hasher);
        source.static_gtfs_bytes.hash(&mut hasher);
        source.static_gtfs_modified_unix_secs.hash(&mut hasher);
    }
    hasher.finish()
}

fn polyline_fingerprint(polyline: &[PolylinePoint]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for point in polyline {
        point.lat.to_bits().hash(&mut hasher);
        point.lon.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

fn route_display_name_cold(route: &ColdRouteRecord) -> String {
    if let Some(short_name) = route.short_name.as_deref().filter(|value| !value.is_empty()) {
        return short_name.to_owned();
    }
    if let Some(long_name) = route.long_name.as_deref().filter(|value| !value.is_empty()) {
        return long_name.to_owned();
    }
    route.id.clone()
}

fn load_static_cache(
    cache_path: &PathBuf,
    metadata: &StaticCacheMetadata,
) -> Result<Option<StaticCache>> {
    if !cache_path.exists() {
        return Ok(None);
    }

    let file = File::open(cache_path)
        .with_context(|| format!("unable to open static cache {}", cache_path.display()))?;
    let reader = BufReader::new(file);
    let cache: StaticCache = match bincode::deserialize_from(reader) {
        Ok(cache) => cache,
        Err(error) => {
            warn!(%error, cache = %cache_path.display(), "invalid static cache, rebuilding");
            return Ok(None);
        }
    };

    if cache.metadata == *metadata {
        Ok(Some(cache))
    } else {
        info!(cache = %cache_path.display(), "static cache metadata mismatch, rebuilding");
        Ok(None)
    }
}

fn store_static_cache(cache_path: &PathBuf, cache: &StaticCache) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("unable to create static cache directory {}", parent.display())
        })?;
    }
    let file = File::create(cache_path)
        .with_context(|| format!("unable to create static cache {}", cache_path.display()))?;
    let writer = BufWriter::new(file);
    bincode::serialize_into(writer, cache).context("failed to serialize static cache")
}

fn build_static_cache_metadata(config: &EngineConfig) -> Result<StaticCacheMetadata> {
    Ok(config.static_inputs_metadata.clone())
}

fn static_cache_path(config: &EngineConfig) -> PathBuf {
    let cache_dir = runtime_cache_dir(&config.workspace_root, "static");
    if let Some(manifest_path) = &config.manifest_path {
        let stem = manifest_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("alpha-raptor");
        return cache_dir.join(format!("{stem}.static.v{STATIC_CACHE_SCHEMA_VERSION}.bin"));
    }

    let static_gtfs_path = &config.feeds[0].static_gtfs_path;
    let stem = static_gtfs_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor-static");
    cache_dir.join(format!("{stem}.static.v{STATIC_CACHE_SCHEMA_VERSION}.bin"))
}

fn merge_service_calendar(
    calendar: &mut HashMap<NaiveDate, HashSet<String>>,
    feed_id: &str,
    gtfs: &Gtfs,
) {
    for (service_id, service) in &gtfs.calendar {
        let mut cursor = service.start_date;
        while cursor <= service.end_date {
            if service.valid_weekday(cursor) {
                calendar
                    .entry(cursor)
                    .or_default()
                    .insert(namespaced_id(feed_id, service_id));
            }
            cursor += Duration::days(1);
        }
    }

    for (service_id, dates) in &gtfs.calendar_dates {
        for entry in dates {
            match entry.exception_type {
                Exception::Added => {
                    calendar
                        .entry(entry.date)
                        .or_default()
                        .insert(namespaced_id(feed_id, service_id));
                }
                Exception::Deleted => {
                    if let Some(services) = calendar.get_mut(&entry.date) {
                        services.remove(&namespaced_id(feed_id, service_id));
                    }
                }
            }
        }
    }
}

fn build_haversine_transfers(
    stops: &[StopRecord],
    walk_radius_meters: f64,
    walk_speed_mps: f64,
    max_transfer_candidates: usize,
) -> Vec<Vec<WalkTransfer>> {
    let mut transfers = vec![Vec::<WalkTransfer>::new(); stops.len()];
    let points: Vec<_> = stops
        .iter()
        .enumerate()
        .filter_map(|(index, stop)| {
            Some(IndexedStopPoint {
                index,
                point: [stop.longitude?, stop.latitude?],
            })
        })
        .collect();
    let spatial_index = RTree::bulk_load(points.clone());

    for point in &points {
        let origin = &stops[point.index];
        let latitude = match origin.latitude {
            Some(value) => value,
            None => continue,
        };
        let longitude = match origin.longitude {
            Some(value) => value,
            None => continue,
        };
        let lat_delta = walk_radius_meters / 111_320.0;
        let lon_denominator = 111_320.0 * latitude.to_radians().cos().abs().max(0.25);
        let lon_delta = walk_radius_meters / lon_denominator;
        let envelope = AABB::from_corners(
            [longitude - lon_delta, latitude - lat_delta],
            [longitude + lon_delta, latitude + lat_delta],
        );
        let mut neighbours = Vec::<WalkTransfer>::new();
        for candidate in spatial_index.locate_in_envelope_intersecting(&envelope) {
            if candidate.index == point.index {
                continue;
            }
            let candidate_stop = &stops[candidate.index];
            let distance = haversine_meters(
                latitude,
                longitude,
                candidate_stop.latitude.unwrap_or(latitude),
                candidate_stop.longitude.unwrap_or(longitude),
            );
            if distance <= walk_radius_meters {
                neighbours.push(WalkTransfer {
                    to_stop: candidate.index,
                    duration_secs: (distance / walk_speed_mps).ceil() as i32,
                    distance_meters: distance,
                    polyline: straight_polyline(origin, candidate_stop),
                });
            }
        }
        neighbours.sort_by_key(|transfer| transfer.duration_secs);
        neighbours.truncate(max_transfer_candidates);
        transfers[point.index] = neighbours;
    }

    transfers
}

fn record_stop_improvement(
    stop_index: usize,
    candidate_arrival: i32,
    global_best: &mut [i32],
    round_arrivals: &mut [i32],
    parents: &mut [Option<ParentStep>],
    parent: ParentStep,
    improved_stops: &mut Vec<usize>,
    improved_flags: &mut [bool],
) -> bool {
    if candidate_arrival >= global_best[stop_index] {
        return false;
    }

    global_best[stop_index] = candidate_arrival;
    round_arrivals[stop_index] = candidate_arrival;
    parents[stop_index] = Some(parent);
    if !improved_flags[stop_index] {
        improved_flags[stop_index] = true;
        improved_stops.push(stop_index);
    }
    true
}

fn record_memoized_destination_improvement(
    destination_stop: usize,
    source_stop: usize,
    source_round: usize,
    target_round: usize,
    candidate_arrival: i32,
    suffix_legs: Arc<Vec<CachedLeg>>,
    global_best: &mut [i32],
    round_arrivals: &mut [Vec<i32>],
    parents: &mut [Vec<Option<ParentStep>>],
    destination_round: &mut Option<usize>,
) -> bool {
    let should_replace = if candidate_arrival < global_best[destination_stop] {
        true
    } else if candidate_arrival > global_best[destination_stop] {
        false
    } else {
        match destination_round {
            Some(current_round) => target_round < *current_round,
            None => true,
        }
    };

    if !should_replace {
        return false;
    }

    global_best[destination_stop] = candidate_arrival;
    round_arrivals[target_round][destination_stop] = candidate_arrival;
    parents[target_round][destination_stop] = Some(ParentStep::Memoized {
        from_stop: source_stop,
        source_round,
        legs: suffix_legs,
    });
    *destination_round = Some(target_round);
    true
}

fn combine_cached_leg_sequences(left: &[CachedLeg], right: &[CachedLeg]) -> Vec<CachedLeg> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend(left.iter().cloned());
    combined.extend(right.iter().cloned());
    combined
}

fn cached_leg_from_raw_leg(leg: &RawLeg) -> CachedLeg {
    match leg {
        RawLeg::Walk {
            from_stop,
            to_stop,
            departure_secs,
            arrival_secs,
            duration_secs,
            distance_meters,
        } => CachedLeg::Walk {
            from_stop: *from_stop,
            to_stop: *to_stop,
            departure_secs: *departure_secs,
            arrival_secs: *arrival_secs,
            duration_secs: *duration_secs,
            distance_meters: *distance_meters,
        },
        RawLeg::Transit {
            trip_index,
            board_stop,
            board_pos,
            alight_stop,
            alight_pos,
            departure_secs,
            arrival_secs,
        } => CachedLeg::Transit {
            trip_index: *trip_index,
            board_stop: *board_stop,
            board_pos: *board_pos,
            alight_stop: *alight_stop,
            alight_pos: *alight_pos,
            departure_secs: *departure_secs,
            arrival_secs: *arrival_secs,
        },
    }
}

fn cached_leg_to_raw_leg(leg: &CachedLeg) -> RawLeg {
    match leg {
        CachedLeg::Walk {
            from_stop,
            to_stop,
            departure_secs,
            arrival_secs,
            duration_secs,
            distance_meters,
        } => RawLeg::Walk {
            from_stop: *from_stop,
            to_stop: *to_stop,
            departure_secs: *departure_secs,
            arrival_secs: *arrival_secs,
            duration_secs: *duration_secs,
            distance_meters: *distance_meters,
        },
        CachedLeg::Transit {
            trip_index,
            board_stop,
            board_pos,
            alight_stop,
            alight_pos,
            departure_secs,
            arrival_secs,
        } => RawLeg::Transit {
            trip_index: *trip_index,
            board_stop: *board_stop,
            board_pos: *board_pos,
            alight_stop: *alight_stop,
            alight_pos: *alight_pos,
            departure_secs: *departure_secs,
            arrival_secs: *arrival_secs,
        },
    }
}

fn raw_leg_source_stop(leg: &RawLeg) -> usize {
    match leg {
        RawLeg::Walk { from_stop, .. } => *from_stop,
        RawLeg::Transit { board_stop, .. } => *board_stop,
    }
}

fn raw_leg_departure_secs(leg: &RawLeg) -> i32 {
    match leg {
        RawLeg::Walk { departure_secs, .. } => *departure_secs,
        RawLeg::Transit { departure_secs, .. } => *departure_secs,
    }
}

fn raw_leg_arrival_secs(leg: &RawLeg) -> i32 {
    match leg {
        RawLeg::Walk { arrival_secs, .. } => *arrival_secs,
        RawLeg::Transit { arrival_secs, .. } => *arrival_secs,
    }
}

fn raw_leg_trip_index(leg: &RawLeg) -> Option<usize> {
    match leg {
        RawLeg::Walk { .. } => None,
        RawLeg::Transit { trip_index, .. } => Some(*trip_index),
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

pub fn format_service_time(service_date: NaiveDate, seconds: i32) -> String {
    let day_offset = seconds.div_euclid(86_400) as i64;
    let seconds_of_day = seconds.rem_euclid(86_400) as u32;
    let date = service_date + Duration::days(day_offset);
    let time = NaiveTime::from_num_seconds_from_midnight_opt(seconds_of_day, 0)
        .unwrap_or_else(|| NaiveTime::from_hms_opt(0, 0, 0).expect("midnight is valid"));
    format!("{} {}", date.format("%Y-%m-%d"), time.format("%H:%M:%S"))
}

pub fn route_display_name(route: &RouteRecord) -> String {
    route
        .short_name
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            route
                .long_name
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| route.id.clone())
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

#[cfg(test)]
mod tests {
    use super::{
        CHRONOS_BUCKET_SECS, LineRecord, RemoteStaticGtfsVersionMetadata, TripRecord,
        TripStopRecord,
        build_chronos_bucket_start_indices, finalize_line_temporal_indices,
        remote_static_gtfs_version_changed,
    };

    #[test]
    fn chronos_bucket_indices_are_dense_across_schedule_gaps() {
        let departures = vec![10 * 3600 + 5 * 60, 10 * 3600 + 45 * 60];
        let indices = build_chronos_bucket_start_indices(&departures);

        let gap_bucket = ((10 * 3600) + (15 * 60)) / CHRONOS_BUCKET_SECS;
        assert_eq!(indices[gap_bucket as usize], 1);
    }

    #[test]
    fn finalize_line_temporal_indices_tracks_searchability_per_stop() {
        let trips = vec![
            TripRecord {
                global_id: 1,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "t1".to_owned(),
                id: "feed:t1".to_owned(),
                route_index: 0,
                shape_id: None,
                headsign: None,
                stop_times: vec![
                    TripStopRecord {
                        stop_index: 0,
                        arrival_secs: 8 * 3600,
                        departure_secs: 8 * 3600,
                        stop_sequence: 1,
                        shape_dist_traveled: None,
                    },
                    TripStopRecord {
                        stop_index: 1,
                        arrival_secs: 8 * 3600 + 10 * 60,
                        departure_secs: 8 * 3600 + 10 * 60,
                        stop_sequence: 2,
                        shape_dist_traveled: None,
                    },
                ],
            },
            TripRecord {
                global_id: 2,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "t2".to_owned(),
                id: "feed:t2".to_owned(),
                route_index: 0,
                shape_id: None,
                headsign: None,
                stop_times: vec![
                    TripStopRecord {
                        stop_index: 0,
                        arrival_secs: 8 * 3600 + 5 * 60,
                        departure_secs: 8 * 3600 + 5 * 60,
                        stop_sequence: 1,
                        shape_dist_traveled: None,
                    },
                    TripStopRecord {
                        stop_index: 1,
                        arrival_secs: 8 * 3600 + 9 * 60,
                        departure_secs: 8 * 3600 + 9 * 60,
                        stop_sequence: 2,
                        shape_dist_traveled: None,
                    },
                ],
            },
        ];

        let mut lines = vec![LineRecord {
            stop_indices: vec![0, 1],
            trip_indices: vec![0, 1],
            scheduled_departures_by_stop: Vec::new(),
            chronos_bucket_start_indices_by_stop: Vec::new(),
            binary_searchable_by_stop: Vec::new(),
            trip_order_indirection_by_stop: Vec::new(),
        }];

        finalize_line_temporal_indices(&mut lines, &trips);

        assert_eq!(lines[0].binary_searchable_by_stop, vec![true, false]);
        assert!(!lines[0].chronos_bucket_start_indices_by_stop[0].is_empty());
        assert!(!lines[0].chronos_bucket_start_indices_by_stop[1].is_empty());
        assert_eq!(lines[0].trip_order_indirection_by_stop[0], Vec::<u32>::new());
        assert_eq!(lines[0].trip_order_indirection_by_stop[1], vec![1, 0]);
    }

    #[test]
    fn remote_static_gtfs_version_prefers_etag_when_present() {
        let cached = RemoteStaticGtfsVersionMetadata {
            url: "https://example.com/feed.zip".to_owned(),
            last_modified: Some("Sun, 06 Apr 2026 09:00:00 GMT".to_owned()),
            etag: Some("etag-v1".to_owned()),
        };
        let remote = RemoteStaticGtfsVersionMetadata {
            url: "https://example.com/feed.zip".to_owned(),
            last_modified: Some("Sun, 06 Apr 2026 10:00:00 GMT".to_owned()),
            etag: Some("etag-v1".to_owned()),
        };

        assert!(!remote_static_gtfs_version_changed(Some(&cached), &remote));
    }

    #[test]
    fn remote_static_gtfs_version_falls_back_to_last_modified() {
        let cached = RemoteStaticGtfsVersionMetadata {
            url: "https://example.com/feed.zip".to_owned(),
            last_modified: Some("Sun, 06 Apr 2026 09:00:00 GMT".to_owned()),
            etag: None,
        };
        let remote = RemoteStaticGtfsVersionMetadata {
            url: "https://example.com/feed.zip".to_owned(),
            last_modified: Some("Sun, 06 Apr 2026 10:00:00 GMT".to_owned()),
            etag: None,
        };

        assert!(remote_static_gtfs_version_changed(Some(&cached), &remote));
    }
}
