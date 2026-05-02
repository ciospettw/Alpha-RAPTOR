use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{Duration, NaiveDate, NaiveTime, Timelike};
use gtfs_structures::{Exception, Gtfs};
use gtfs_rt::{trip_descriptor, vehicle_position};
use reqwest::{
    blocking::Client as BlockingHttpClient,
    header::{ETAG, LAST_MODIFIED},
};
use rstar::{AABB, PointDistance, RTree, RTreeObject};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::control::{descriptor_url_from_env, maybe_add_internal_token_blocking};
use crate::cold_storage::{ColdRouteRecord, ColdStore, ColdTripRecord, cold_store_paths};
use crate::geo::{
    destination_cell as geo_destination_cell,
    destination_cell_neighborhood as geo_destination_cell_neighborhood,
    destination_cell_window as geo_destination_cell_window,
};
use crate::hpf::{
    HolographicPedestrianForest, HpfDiffConfig, HpfOverlaySnapshot, build_or_load_hpf,
};
use crate::profile_cache::{
    CachedLeg, PreparedSpatialLookup, ProfileCache, ProfileCacheStats, ProfileInsertionPoint,
    ProfileLookupDecision, SpatialProfileInsertionPoint,
};
use crate::progress::{progress_bar, progress_bar_u64, progress_percent, progress_percent_u64};
use crate::realtime::{RealtimeDebugSnapshot, RealtimeStore, TripRealtimeMetrics};
use crate::street::{
    StreetMode, StreetRouter, StreetRouterOverlaySnapshot, build_or_load_street_router,
};
use crate::walker::{
    WalkerBuildResult, build_or_load_walker_transfers, rebuild_walker_transfers_subset,
};

const INF_TIME: i32 = i32::MAX / 8;
const STATIC_CACHE_SCHEMA_VERSION: u32 = 8;
const CHRONOS_BUCKET_SECS: i32 = 15 * 60;
const DEFAULT_MANIFEST_NAME: &str = "alpha-raptor.toml";
const DEFAULT_STATIC_DIFF_TOLERANCE: f64 = 0.05;
const DEFAULT_DVNI_KNN_CANDIDATES: usize = 5;
const DEFAULT_DVNI_MAX_WALK_RADIUS_METERS: f64 = 1_500.0;
const DEFAULT_HPF_MAX_DISTANCE_METERS: f64 = 4_000.0;
const DEFAULT_HPF_SNAP_TOLERANCE_METERS: f64 = 140.0;
const DEFAULT_HPF_SNAP_QUADRATIC_KAPPA_METERS: f64 = 40.0;
const DEFAULT_HPF_SEARCH_WINDOW: usize = 512;
const DEFAULT_OSM_DIFF_POLL_INTERVAL_SECS: u64 = 30 * 60;
const DEFAULT_QUERY_ITINERARY_COUNT: usize = 5;
const MAX_QUERY_ITINERARY_COUNT: usize = 6;
const SVRT_WIDTH: usize = 8;
const BTT_MIN_HUB_CELL_METERS: f64 = 120.0;
const BTT_MAX_HUB_CELL_METERS: f64 = 220.0;
const EMPTY_TRANSFER_SLOT: u16 = u16::MAX;
const GLOBAL_ID_LOCAL_MASK: u64 = (1u64 << 48) - 1;
const ENTITY_KIND_SHIFT: u64 = 44;
const ENTITY_ORDINAL_MASK: u64 = (1u64 << ENTITY_KIND_SHIFT) - 1;
const WALK_TURN_STRAIGHT_ANGLE_DEGREES: f64 = 20.0;
const WALK_TURN_MIN_SPACING_METERS: f64 = 15.0;

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

#[derive(Debug, Serialize, Deserialize)]
struct RawEngineManifest {
    osm_pbf: Option<String>,
    osm_pbf_allow_invalid_tls: Option<bool>,
    walk_radius_meters: Option<f64>,
    walk_speed_mps: Option<f64>,
    max_transfer_candidates: Option<usize>,
    static_reload_interval_secs: Option<u64>,
    static_diff_tolerance: Option<f64>,
    default_max_transfers: Option<usize>,
    dvni: Option<RawDvniConfig>,
    hpf: Option<RawHpfConfig>,
    osm_diff: Option<RawOsmDiffConfig>,
    feeds: Vec<RawFeedConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawDvniConfig {
    knn_candidates: Option<usize>,
    max_walk_radius_meters: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawHpfConfig {
    max_distance_meters: Option<f64>,
    snap_tolerance_meters: Option<f64>,
    snap_quadratic_kappa_meters: Option<f64>,
    search_window: Option<usize>,
    defer_rebuild_on_bootstrap: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RawOsmDiffConfig {
    state_url: String,
    diff_base_url: Option<String>,
    poll_interval_secs: Option<u64>,
    allow_invalid_tls: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutingDescriptor {
    static_feeds: Vec<RoutingStaticFeedDescriptor>,
    realtime_feeds: Vec<RoutingRealtimeFeedDescriptor>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutingStaticFeedDescriptor {
    id: Option<String>,
    namespace: Option<String>,
    feed_id: Option<String>,
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RoutingRealtimeFeedDescriptor {
    namespace: Option<String>,
    feed_id: Option<String>,
    trip_updates_url: Option<String>,
    vehicle_positions_url: Option<String>,
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
    walk_way_names: Arc<HashMap<i64, String>>,
    hpf: Option<HolographicPedestrianForest>,
    street_router: Option<Arc<StreetRouter>>,
}

#[derive(Clone)]
pub struct EngineConfig {
    pub workspace_root: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub source_manifest_path: Option<PathBuf>,
    pub feeds: Vec<FeedConfig>,
    pub osm_pbf_path: PathBuf,
    pub osm_pbf_source: String,
    pub osm_pbf_remote_url: Option<String>,
    pub osm_pbf_allow_invalid_tls: bool,
    pub walk_radius_meters: f64,
    pub walk_speed_mps: f64,
    pub max_transfer_candidates: usize,
    pub static_reload_interval_secs: u64,
    pub static_diff_tolerance: f64,
    pub default_max_transfers: usize,
    pub dvni_knn_candidates: usize,
    pub dvni_max_walk_radius_meters: f64,
    pub hpf_max_distance_meters: f64,
    pub hpf_snap_tolerance_meters: f64,
    pub hpf_snap_quadratic_kappa_meters: f64,
    pub hpf_search_window: usize,
    pub hpf_defer_rebuild_on_bootstrap: bool,
    pub osm_diff: Option<HpfDiffConfig>,
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
    transfer_index: TransferRelaxIndex,
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
    osm_source: StaticOsmSourceMetadata,
    feed_sources: Vec<StaticFeedSourceMetadata>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StaticOsmSourceMetadata {
    osm_pbf_source: String,
    osm_pbf_path: String,
    osm_pbf_remote_url: Option<String>,
    osm_pbf_allow_invalid_tls: bool,
    osm_pbf_bytes: u64,
    osm_pbf_modified_unix_secs: Option<u64>,
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
    pub shape_stop_point_indices: Option<Vec<u32>>,
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
    #[serde(default)]
    pub segment_way_ids: Vec<Option<i64>>,
}

#[derive(Clone)]
struct TransferRelaxIndex {
    stop_to_hub: Vec<usize>,
    stop_to_hub_offset: Vec<usize>,
    hubs: Vec<TransferHub>,
}

#[derive(Clone)]
struct TransferHub {
    stop_indices: Vec<usize>,
    outgoing_tiles: Vec<TransferTile>,
}

#[derive(Clone)]
struct TransferTile {
    target_hub: usize,
    target_stop_indices: Vec<usize>,
    transfer_slots: Vec<u16>,
    durations: Vec<i32>,
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
    pub num_itineraries: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct StreetRouteRequest {
    pub mode: Option<String>,
    pub from_lat: Option<f64>,
    pub from_lon: Option<f64>,
    pub to_lat: Option<f64>,
    pub to_lon: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct EngineStats {
    pub build: BuildStats,
    pub realtime: RealtimeDebugSnapshot,
    pub memoization: ProfileCacheStats,
    pub hpf_overlay: Option<HpfOverlaySnapshot>,
    pub street_overlay: Option<StreetRouterOverlaySnapshot>,
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
    pub osm_pbf_source: String,
    pub osm_pbf_remote_url: Option<String>,
    pub osm_pbf_allow_invalid_tls: bool,
    pub osm_diff_state_url: Option<String>,
    pub osm_diff_poll_interval_secs: Option<u64>,
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
    pub itineraries: Vec<QueryItineraryResponse>,
    pub deferred_hydration: DeferredHydrationResponse,
    pub trace: QueryTrace,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryItineraryResponse {
    pub id: String,
    pub label: String,
    pub badges: Vec<String>,
    pub is_recommended: bool,
    pub is_fastest: bool,
    pub is_fewest_transfers: bool,
    pub is_best_realtime: bool,
    pub is_least_crowded: bool,
    pub has_canceled_legs: bool,
    pub departure_time: String,
    pub arrival_time: String,
    pub duration_seconds: i32,
    pub transfers: usize,
    pub realtime_score: usize,
    pub transit_leg_count: usize,
    pub transit_legs_with_gtfs_rt: usize,
    pub crowding_score: Option<u16>,
    pub crowding_level: &'static str,
    pub occupancy_covered_transit_legs: usize,
    pub canceled_transit_legs: usize,
    pub legs: Vec<LegResponse>,
    pub deferred_hydration: DeferredHydrationResponse,
}

#[derive(Debug, Serialize)]
pub struct StreetRouteResponse {
    pub mode: &'static str,
    pub from: StreetCoordinatePoint,
    pub to: StreetCoordinatePoint,
    pub duration_seconds: i32,
    pub distance_meters: f64,
    pub polyline: Vec<PolylinePoint>,
    pub directions: Vec<WalkDirection>,
    pub trace: StreetRouteTrace,
}

#[derive(Debug, Serialize)]
pub struct StreetCoordinatePoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Serialize)]
pub struct StreetRouteTrace {
    pub strategy: &'static str,
    pub query_runtime_ms: u128,
    pub source_snap_distance_meters: f64,
    pub destination_snap_distance_meters: f64,
    pub explored_forward_nodes: usize,
    pub explored_backward_nodes: usize,
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
    pub has_gtfs_rt: bool,
    pub has_trip_update: bool,
    pub has_vehicle_position: bool,
    pub schedule_relationship: Option<String>,
    pub occupancy_status: Option<String>,
    pub occupancy_percentage: Option<u32>,
    pub occupancy_score: Option<u16>,
    pub intermediate_stops: Vec<StopSearchResult>,
    pub polyline: Vec<PolylinePoint>,
    pub walk_directions: Vec<WalkDirection>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeferredHydrationResponse {
    pub legs: Vec<DeferredLegRef>,
    pub entities: HydrationEntityDictionary,
}

#[derive(Debug, Clone, Serialize)]
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
    pub has_gtfs_rt: bool,
    pub has_trip_update: bool,
    pub has_vehicle_position: bool,
    pub schedule_relationship: Option<String>,
    pub occupancy_status: Option<String>,
    pub occupancy_percentage: Option<u32>,
    pub occupancy_score: Option<u16>,
    pub intermediate_stop_gids: Vec<u64>,
    pub polyline_index: usize,
    pub walk_directions: Vec<WalkDirection>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct HydrationEntityDictionary {
    pub stops: Vec<StopSearchResult>,
    pub routes: Vec<RouteHydration>,
    pub trips: Vec<TripHydration>,
    pub polylines: Vec<Vec<PolylinePoint>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteHydration {
    pub global_id: u64,
    pub id: String,
    pub label: String,
    pub route_type: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
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

#[derive(Clone, Debug, Serialize)]
pub struct WalkDirection {
    pub maneuver: &'static str,
    pub instruction: String,
    pub street_name: Option<String>,
    pub distance_meters: f64,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Clone, Debug, Default)]
struct WalkGeometry {
    polyline: Vec<PolylinePoint>,
    segment_way_ids: Vec<Option<i64>>,
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
    segment_way_ids: Vec<Option<i64>>,
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
struct QueryItineraryCandidate {
    round: usize,
    arrival_secs: i32,
    transit_legs: usize,
    raw_legs: Vec<RawLeg>,
}

struct ItineraryPassiveMetrics {
    transit_leg_count: usize,
    transit_legs_with_gtfs_rt: usize,
    occupancy_covered_transit_legs: usize,
    crowding_score: Option<u16>,
    crowding_level: &'static str,
    canceled_transit_legs: usize,
    has_canceled_legs: bool,
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
            return Self::from_manifest_source(
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
            return Self::from_manifest_source(
                &workspace_root,
                default_manifest,
                StaticGtfsRefreshMode::Bootstrap,
            );
        }

        Self::from_legacy_env(workspace_root, StaticGtfsRefreshMode::Bootstrap)
    }

    pub fn reload_from_source(&self) -> Result<Self> {
        if let Some(source_manifest_path) = &self.source_manifest_path {
            return Self::from_manifest_source(
                &self.workspace_root,
                source_manifest_path.clone(),
                StaticGtfsRefreshMode::Poll,
            );
        }

        Self::from_legacy_env(self.workspace_root.clone(), StaticGtfsRefreshMode::Poll)
    }

    pub fn static_inputs_changed(&self, other: &Self) -> bool {
        self.static_inputs_metadata != other.static_inputs_metadata
    }

    fn from_manifest_source(
        workspace_root: &PathBuf,
        source_manifest_path: PathBuf,
        refresh_mode: StaticGtfsRefreshMode,
    ) -> Result<Self> {
        let runtime_manifest_path = materialize_runtime_manifest(workspace_root, &source_manifest_path)?;
        Self::from_manifest(
            workspace_root,
            source_manifest_path,
            runtime_manifest_path,
            refresh_mode,
        )
    }

    fn from_manifest(
        workspace_root: &PathBuf,
        source_manifest_path: PathBuf,
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
        let default_osm_pbf = default_osm_pbf_path(workspace_root).display().to_string();
        let prepared_osm_pbf = prepare_osm_pbf_source(
            workspace_root,
            &manifest_dir,
            manifest
                .osm_pbf
                .as_deref()
                .unwrap_or(default_osm_pbf.as_str()),
            manifest.osm_pbf_allow_invalid_tls.unwrap_or(false),
            refresh_mode,
        )?;
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
            source_manifest_path: Some(source_manifest_path),
            feeds,
            osm_pbf_path: prepared_osm_pbf.local_path,
            osm_pbf_source: prepared_osm_pbf.source,
            osm_pbf_remote_url: prepared_osm_pbf.remote_url,
            osm_pbf_allow_invalid_tls: prepared_osm_pbf.allow_invalid_tls,
            walk_radius_meters: manifest.walk_radius_meters.unwrap_or(450.0),
            walk_speed_mps: manifest.walk_speed_mps.unwrap_or(1.35),
            max_transfer_candidates: manifest.max_transfer_candidates.unwrap_or(12),
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
            hpf_defer_rebuild_on_bootstrap: manifest
                .hpf
                .as_ref()
                .and_then(|hpf| hpf.defer_rebuild_on_bootstrap)
                .unwrap_or(false),
            osm_diff: manifest.osm_diff.as_ref().map(|diff| HpfDiffConfig {
                state_url: diff.state_url.clone(),
                diff_base_url: diff.diff_base_url.clone(),
                poll_interval_secs: diff
                    .poll_interval_secs
                    .unwrap_or(DEFAULT_OSM_DIFF_POLL_INTERVAL_SECS)
                    .max(60),
                allow_invalid_tls: diff
                    .allow_invalid_tls
                    .unwrap_or(prepared_osm_pbf.allow_invalid_tls),
            }),
            static_inputs_metadata: StaticCacheMetadata {
                schema_version: STATIC_CACHE_SCHEMA_VERSION,
                manifest_path: None,
                manifest_modified_unix_secs: None,
                osm_source: StaticOsmSourceMetadata {
                    osm_pbf_source: String::new(),
                    osm_pbf_path: String::new(),
                    osm_pbf_remote_url: None,
                    osm_pbf_allow_invalid_tls: false,
                    osm_pbf_bytes: 0,
                    osm_pbf_modified_unix_secs: None,
                },
                feed_sources: Vec::new(),
            },
        };
        config.static_inputs_metadata = capture_static_inputs_metadata(
            config
                .source_manifest_path
                .as_ref()
                .or(config.manifest_path.as_ref()),
            &config,
        )?;
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
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let prepared_static_gtfs = prepare_static_gtfs_source_legacy(
            &workspace_root,
            &feed_id,
            static_gtfs_value,
            "data/gtfs/rome_static_gtfs.zip",
            static_gtfs_allow_invalid_tls,
            refresh_mode,
        )?;
        let osm_pbf_allow_invalid_tls = env::var("ALPHA_OSM_PBF_ALLOW_INVALID_TLS")
            .ok()
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
        let default_osm_pbf = default_osm_pbf_path(&workspace_root).display().to_string();
        let prepared_osm_pbf = prepare_osm_pbf_source_legacy(
            &workspace_root,
            env::var("ALPHA_OSM_PBF").ok(),
            default_osm_pbf.as_str(),
            osm_pbf_allow_invalid_tls,
            refresh_mode,
        )?;

        let mut config = Self {
            workspace_root: workspace_root.clone(),
            manifest_path: None,
            source_manifest_path: None,
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
            osm_pbf_path: prepared_osm_pbf.local_path,
            osm_pbf_source: prepared_osm_pbf.source,
            osm_pbf_remote_url: prepared_osm_pbf.remote_url,
            osm_pbf_allow_invalid_tls: prepared_osm_pbf.allow_invalid_tls,
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
            hpf_defer_rebuild_on_bootstrap: matches!(
                env::var("ALPHA_HPF_DEFER_REBUILD_ON_BOOTSTRAP")
                    .ok()
                    .as_deref()
                    .map(str::trim)
                    .map(str::to_ascii_lowercase)
                    .as_deref(),
                Some("1" | "true" | "yes" | "on")
            ),
            osm_diff: env::var("ALPHA_OSM_DIFF_STATE_URL")
                .ok()
                .map(|state_url| HpfDiffConfig {
                    diff_base_url: env::var("ALPHA_OSM_DIFF_BASE_URL").ok(),
                    poll_interval_secs: env::var("ALPHA_OSM_DIFF_POLL_SECS")
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(DEFAULT_OSM_DIFF_POLL_INTERVAL_SECS)
                        .max(60),
                    allow_invalid_tls: env::var("ALPHA_OSM_DIFF_ALLOW_INVALID_TLS")
                        .ok()
                        .map(|value| {
                            matches!(
                                value.trim().to_ascii_lowercase().as_str(),
                                "1" | "true" | "yes" | "on"
                            )
                        })
                        .unwrap_or(prepared_osm_pbf.allow_invalid_tls),
                    state_url,
                }),
            static_inputs_metadata: StaticCacheMetadata {
                schema_version: STATIC_CACHE_SCHEMA_VERSION,
                manifest_path: None,
                manifest_modified_unix_secs: None,
                osm_source: StaticOsmSourceMetadata {
                    osm_pbf_source: String::new(),
                    osm_pbf_path: String::new(),
                    osm_pbf_remote_url: None,
                    osm_pbf_allow_invalid_tls: false,
                    osm_pbf_bytes: 0,
                    osm_pbf_modified_unix_secs: None,
                },
                feed_sources: Vec::new(),
            },
        };
        config.static_inputs_metadata = capture_static_inputs_metadata(
            config
                .source_manifest_path
                .as_ref()
                .or(config.manifest_path.as_ref()),
            &config,
        )?;
        Ok(config)
    }

    pub fn static_sources_display(&self) -> String {
        self.feeds
            .iter()
            .map(|feed| {
                if let Some(url) = &feed.static_gtfs_remote_url {
                    format!("{}={} -> {}", feed.id, url, feed.static_gtfs_path.display())
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

fn materialize_runtime_manifest(
    workspace_root: &Path,
    source_manifest_path: &Path,
) -> Result<PathBuf> {
    if descriptor_url_from_env().is_none() {
        return Ok(source_manifest_path.to_path_buf());
    }

    let generated_manifest_path = runtime_root(workspace_root)
        .join("generated")
        .join("busone-descriptor.toml");

    match write_descriptor_manifest(source_manifest_path, &generated_manifest_path) {
        Ok(()) => Ok(generated_manifest_path),
        Err(error) if generated_manifest_path.exists() => {
            warn!(
                %error,
                manifest = %generated_manifest_path.display(),
                "descriptor refresh failed; reusing cached generated manifest"
            );
            Ok(generated_manifest_path)
        }
        Err(error) => Err(error),
    }
}

fn write_descriptor_manifest(
    source_manifest_path: &Path,
    generated_manifest_path: &Path,
) -> Result<()> {
    let descriptor_url = descriptor_url_from_env()
        .context("ALPHA_DESCRIPTOR_URL is required when descriptor bootstrap is enabled")?;
    let source_manifest_body = fs::read_to_string(source_manifest_path).with_context(|| {
        format!(
            "unable to read descriptor template manifest {}",
            source_manifest_path.display()
        )
    })?;
    let mut manifest: RawEngineManifest = toml::from_str(&source_manifest_body).with_context(|| {
        format!(
            "invalid descriptor template manifest TOML at {}",
            source_manifest_path.display()
        )
    })?;

    manifest.feeds = fetch_descriptor_feeds(&descriptor_url, &manifest)?;

    let rendered = toml::to_string_pretty(&manifest)
        .context("failed to render generated descriptor manifest")?;
    if let Ok(existing) = fs::read_to_string(generated_manifest_path) {
        if existing == rendered {
            return Ok(());
        }
    }

    if let Some(parent) = generated_manifest_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create generated manifest directory {}",
                parent.display()
            )
        })?;
    }

    fs::write(generated_manifest_path, rendered).with_context(|| {
        format!(
            "failed to write generated descriptor manifest {}",
            generated_manifest_path.display()
        )
    })
}

fn fetch_descriptor_feeds(
    descriptor_url: &str,
    base_manifest: &RawEngineManifest,
) -> Result<Vec<RawFeedConfig>> {
    let client = BlockingHttpClient::builder()
        .user_agent("alpha-raptor-engine/0.1")
        .build()
        .context("failed to build HTTP client for descriptor bootstrap")?;
    let response = maybe_add_internal_token_blocking(client.get(descriptor_url), descriptor_url)?
        .send()
        .and_then(|response| response.error_for_status())
        .with_context(|| format!("failed to fetch routing descriptor from {descriptor_url}"))?;
    let descriptor: RoutingDescriptor = response
        .json()
        .with_context(|| format!("failed to parse routing descriptor from {descriptor_url}"))?;

    let mut depends_on_by_id = HashMap::<String, Vec<String>>::new();
    for feed in &base_manifest.feeds {
        depends_on_by_id.insert(feed.id.clone(), feed.depends_on.clone());
    }

    let mut realtime_by_key = HashMap::<String, RoutingRealtimeFeedDescriptor>::new();
    for realtime in descriptor.realtime_feeds {
        if let Some(key) = descriptor_feed_key(realtime.feed_id.as_ref(), realtime.namespace.as_ref()) {
            realtime_by_key.insert(key, realtime);
        }
    }

    let mut feeds = Vec::with_capacity(descriptor.static_feeds.len());
    for static_feed in descriptor.static_feeds {
        let feed_id = descriptor_feed_key(
            static_feed.feed_id.as_ref(),
            static_feed.namespace.as_ref().or(static_feed.id.as_ref()),
        )
        .context("routing descriptor static feed is missing feedId/namespace/id")?;
        let realtime = realtime_by_key
            .get(&feed_id)
            .or_else(|| {
                static_feed
                    .namespace
                    .as_ref()
                    .and_then(|namespace| realtime_by_key.get(namespace))
            });

        let depends_on = depends_on_by_id
            .get(&feed_id)
            .cloned()
            .or_else(|| {
                static_feed
                    .namespace
                    .as_ref()
                    .and_then(|namespace| depends_on_by_id.get(namespace).cloned())
            })
            .unwrap_or_default();

        feeds.push(RawFeedConfig {
            id: feed_id,
            static_gtfs: static_feed.url,
            static_gtfs_allow_invalid_tls: false,
            trip_updates_url: realtime.and_then(|value| value.trip_updates_url.clone()),
            vehicle_positions_url: realtime.and_then(|value| value.vehicle_positions_url.clone()),
            depends_on,
        });
    }

    if feeds.is_empty() {
        bail!("routing descriptor at {descriptor_url} does not define any static feeds");
    }

    Ok(feeds)
}

fn descriptor_feed_key(primary: Option<&String>, fallback: Option<&String>) -> Option<String> {
    primary
        .map(String::as_str)
        .or(fallback.map(String::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
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
        info!(
            phase = "engine-bootstrap",
            step = 1,
            total_steps = 5,
            progress = %progress_bar(1, 5),
            percent = progress_percent(1, 5),
            "bootstrap progress"
        );
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
        info!(
            phase = "engine-bootstrap",
            step = 2,
            total_steps = 5,
            progress = %progress_bar(2, 5),
            percent = progress_percent(2, 5),
            cold_store_ms,
            "bootstrap progress"
        );
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
        let mut walker_build = match (
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
        if walker_build.way_names.is_empty() {
            if let Some(previous_engine) = previous {
                walker_build.way_names = previous_engine.walk_way_names.as_ref().clone();
            }
        }
        let walker_millis = walker_started.elapsed().as_millis();
        info!(
            phase = "engine-bootstrap",
            step = 3,
            total_steps = 5,
            progress = %progress_bar(3, 5),
            percent = progress_percent(3, 5),
            walker_ms = walker_millis,
            "bootstrap progress"
        );
        let transfers = walker_build.transfers;
        let walk_way_names = Arc::new(walker_build.way_names.clone());
        let transfer_hub_cell_meters = (config.walk_radius_meters / 3.0)
            .clamp(BTT_MIN_HUB_CELL_METERS, BTT_MAX_HUB_CELL_METERS);
        let transfer_index =
            build_transfer_relax_index(&stops, &transfers, transfer_hub_cell_meters);
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
            previous.is_none() && config.hpf_defer_rebuild_on_bootstrap,
            config.osm_diff.clone(),
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
        info!(
            phase = "engine-bootstrap",
            step = 4,
            total_steps = 5,
            progress = %progress_bar(4, 5),
            percent = progress_percent(4, 5),
            hpf_ms = hpf_millis,
            "bootstrap progress"
        );

        let street_router = match build_or_load_street_router(
            &config.osm_pbf_path,
            &runtime_cache_dir(&config.workspace_root, "osm"),
            config.osm_diff.clone(),
        ) {
            Ok(router) => Some(Arc::new(router)),
            Err(error) => {
                warn!(%error, "failed to build street router; /api/street will be unavailable");
                None
            }
        };
        info!(
            phase = "engine-bootstrap",
            step = 5,
            total_steps = 5,
            progress = %progress_bar(5, 5),
            percent = progress_percent(5, 5),
            "bootstrap progress"
        );

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
            osm_pbf_source: config.osm_pbf_source.clone(),
            osm_pbf_remote_url: config.osm_pbf_remote_url.clone(),
            osm_pbf_allow_invalid_tls: config.osm_pbf_allow_invalid_tls,
            osm_diff_state_url: config
                .osm_diff
                .as_ref()
                .map(|value| value.state_url.clone()),
            osm_diff_poll_interval_secs: config
                .osm_diff
                .as_ref()
                .map(|value| value.poll_interval_secs),
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
            transfer_index,
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
            walk_way_names,
            hpf,
            street_router,
        })
    }

    pub async fn refresh_realtime(&self) -> Result<RealtimeDebugSnapshot> {
        let refresh = self
            .realtime
            .refresh(&self.static_data, &self.config)
            .await?;
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
            hpf_overlay: self.hpf.as_ref().map(|hpf| hpf.overlay_snapshot()),
            street_overlay: self.street_router.as_ref().map(|router| router.overlay_snapshot()),
        }
    }

    pub async fn refresh_hpf_overlay(&self) -> Result<Option<HpfOverlaySnapshot>> {
        let Some(hpf) = self.hpf.clone() else {
            return Ok(None);
        };

        tokio::task::spawn_blocking(move || hpf.poll_remote_updates())
            .await
            .context("HPF overlay refresh task panicked")?
            .map(Some)
    }

    pub async fn refresh_street_overlay(&self) -> Result<Option<StreetRouterOverlaySnapshot>> {
        let Some(router) = self.street_router.clone() else {
            return Ok(None);
        };

        tokio::task::spawn_blocking(move || router.poll_remote_updates())
            .await
            .context("street overlay refresh task panicked")?
            .map(Some)
    }

    pub fn realtime_snapshot(&self, limit: usize) -> RealtimeDebugSnapshot {
        self.realtime.snapshot(&self.static_data, limit)
    }

    pub async fn run_street_route(
        &self,
        request: StreetRouteRequest,
    ) -> Result<StreetRouteResponse> {
        let mode = StreetMode::parse(request.mode.as_deref())?;
        let from_lat = validate_query_latitude("from_lat", request.from_lat)?;
        let from_lon = validate_query_longitude("from_lon", request.from_lon)?;
        let to_lat = validate_query_latitude("to_lat", request.to_lat)?;
        let to_lon = validate_query_longitude("to_lon", request.to_lon)?;
        let router = self
            .street_router
            .clone()
            .ok_or_else(|| anyhow!("street router unavailable for the current OSM extract"))?;

        let route_started = Instant::now();
        let path = tokio::task::spawn_blocking(move || {
            router.route(mode, (from_lat, from_lon), (to_lat, to_lon))
        })
        .await
        .context("street routing task panicked")??;

        let geometry = WalkGeometry {
            polyline: path.polyline.clone(),
            segment_way_ids: path.segment_way_ids.clone(),
        };
        let directions = build_road_directions(&geometry, path.way_names.as_ref(), "");

        Ok(StreetRouteResponse {
            mode: mode.as_str(),
            from: StreetCoordinatePoint {
                lat: from_lat,
                lon: from_lon,
            },
            to: StreetCoordinatePoint {
                lat: to_lat,
                lon: to_lon,
            },
            duration_seconds: i32::try_from(path.duration_seconds)
                .unwrap_or(i32::MAX.saturating_sub(1)),
            distance_meters: path.distance_meters,
            polyline: path.polyline,
            directions,
            trace: StreetRouteTrace {
                strategy: path.strategy,
                query_runtime_ms: route_started.elapsed().as_millis(),
                source_snap_distance_meters: path.source_snap_distance_meters,
                destination_snap_distance_meters: path.destination_snap_distance_meters,
                explored_forward_nodes: path.explored_forward_nodes,
                explored_backward_nodes: path.explored_backward_nodes,
            },
        })
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

    async fn resolve_query_plan(
        &self,
        request: &QueryRequest,
    ) -> Result<(QueryPlan, QueryPlanMetrics)> {
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
            QueryEndpointInput::Stop {
                stop_index,
                display,
            } => (stop_index, display, Vec::new()),
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

        let (
            to_index,
            display_to,
            exact_destination_stop,
            destination_cells,
            destination_cell_for_insert,
            destination_egress_edges,
        ) = match to_input {
            QueryEndpointInput::Stop {
                stop_index,
                display,
            } => (
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
                let display =
                    virtual_stop_result("Destinazione GPS", point.latitude, point.longitude);
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

        metrics.connector_strategy =
            if metrics.source_virtualized || metrics.destination_virtualized {
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

        let stop_index = self
            .resolve_stop_query(stop_id, stop_gid)
            .with_context(|| match (stop_id, stop_gid) {
                (Some(stop_id), _) => format!("unknown {label} stop {stop_id}"),
                (_, Some(stop_gid)) => format!("unknown {label} stop gid:{stop_gid}"),
                _ => format!("missing {label} stop"),
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
                    let mut segment_way_ids = connector.segment_way_ids;
                    if !is_source {
                        polyline.reverse();
                        segment_way_ids.reverse();
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
                            segment_way_ids,
                        }
                    } else {
                        QueryVirtualWalk {
                            from_stop: connector.stop_index,
                            to_stop: virtual_index,
                            duration_secs: connector.duration_secs.max(1),
                            distance_meters: connector.distance_meters,
                            polyline,
                            segment_way_ids,
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
            .map(|candidate| {
                self.virtual_walk_fallback(point.clone(), virtual_index, candidate, is_source)
            })
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
        let segment_way_ids = empty_segment_way_ids(&polyline);

        if is_source {
            QueryVirtualWalk {
                from_stop: virtual_index,
                to_stop: candidate.stop_index,
                duration_secs,
                distance_meters: candidate.distance_meters,
                polyline,
                segment_way_ids,
            }
        } else {
            QueryVirtualWalk {
                from_stop: candidate.stop_index,
                to_stop: virtual_index,
                duration_secs,
                distance_meters: candidate.distance_meters,
                polyline,
                segment_way_ids,
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
        let requested_itinerary_count = request
            .num_itineraries
            .unwrap_or(DEFAULT_QUERY_ITINERARY_COUNT)
            .clamp(1, MAX_QUERY_ITINERARY_COUNT);
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
        let trip_has_realtime_update = self
            .realtime
            .updated_trip_mask(self.static_data.trips.len());
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
        let mut destination_round = if from_index == to_index {
            Some(0)
        } else {
            None
        };
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
                    if transfer.to_stop == to_index {
                        destination_round = Some(0);
                    }
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
                    if edge.to_stop == to_index {
                        destination_round = Some(0);
                    }
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
                        if transfer.to_stop == to_index {
                            destination_round = Some(0);
                        }
                    }
                }
            }
        }
        let initial_walk_ms = initial_walk_started.elapsed().as_millis();
        let mut profile_lookup_ms = 0u128;

        let mut trace_rounds = Vec::new();
        let mut round_timing_totals_us = RaptorRoundTimingBreakdownUs::default();
        let mut counters = QueryPerformanceCounters::default();
        let mut profile_lookups = 0usize;
        let mut profile_hits = 0usize;
        let mut profile_bound_improvements = 0usize;
        let mut flat_spatial_mask_checks = 0usize;
        let mut flat_spatial_mask_hits = 0usize;
        let mut local_subquery_cache =
            HashMap::<LocalSubqueryKey, Option<LocalSubqueryResult>>::new();

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
            &trip_has_realtime_update,
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
                    &trip_has_realtime_update,
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
                    &trip_has_realtime_update,
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
            let transfer_frontier = next_marked[..transit_frontier_len].to_vec();
            self.relax_transfers_tiled(
                to_index,
                &transfer_frontier,
                &mut global_best,
                &mut round_arrivals[round],
                &mut parents[round],
                &mut next_marked,
                &mut next_marked_flags,
                &mut round_metrics,
            );
            let transfer_relax_ms = transfer_relax_started.elapsed().as_millis();
            round_metrics.timings_us.transfer_relax_us =
                transfer_relax_started.elapsed().as_micros();

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
                    &trip_has_realtime_update,
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
            counters.chronos_bucket_fallback_searches +=
                round_metrics.chronos_bucket_fallback_searches;
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

            round_metrics.timings_us.line_scan_other_us =
                round_metrics.timings_us.line_scan_us.saturating_sub(
                    round_metrics.timings_us.line_scan_onboard_us
                        + round_metrics.timings_us.line_scan_trip_search_us
                        + round_metrics.timings_us.line_scan_candidate_compare_us,
                );
            round_metrics.timings_us.round_total_us = round_started.elapsed().as_micros();
            round_metrics.timings_us.round_other_us =
                round_metrics.timings_us.round_total_us.saturating_sub(
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

        if destination_round.is_none() {
            bail!(
                "no itinerary found from {} to {} after {} transfers",
                plan.display_from.name,
                plan.display_to.name,
                max_transfers
            );
        }
        let itinerary_limit = requested_itinerary_count.min(rounds + 1);

        let reconstruct_started = Instant::now();
        let itinerary_candidates = self.collect_query_itinerary_candidates(
            from_index,
            to_index,
            itinerary_limit,
            &parents,
            &round_arrivals,
        )?;
        let reconstruct_ms = reconstruct_started.elapsed().as_millis();
        let primary_candidate = itinerary_candidates
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("no itinerary candidates reconstructed"))?;
        let cacheable_raw_legs = self.cacheable_raw_legs(&primary_candidate.raw_legs, static_stop_count);
        let profile_insertions = if let Some(destination_stop) = plan.exact_destination_stop {
            self.build_profile_insertions(destination_stop, &cacheable_raw_legs)
        } else {
            Vec::new()
        };
        let spatial_profile_insertions =
            self.build_spatial_profile_insertions(destination_cell, &cacheable_raw_legs);
        let transit_legs = primary_candidate.transit_legs;

        let hydrate_started = Instant::now();
        let itineraries = self.hydrate_query_itinerary_candidates(
            service_date,
            departure_secs,
            &itinerary_candidates,
            &plan.overlay,
        )?;
        let primary_itinerary = itineraries
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("no hydrated itinerary candidates"))?;
        let hydrate_ms = hydrate_started.elapsed().as_millis();

        if !profile_insertions.is_empty() || !spatial_profile_insertions.is_empty() {
            let cache = self.profile_cache.clone();
            let destination_cell = destination_cell;
            let exact_destination_stop = plan.exact_destination_stop;
            rayon::spawn(move || {
                if let Some(exact_destination_stop) = exact_destination_stop {
                    if !profile_insertions.is_empty() {
                        cache.insert_batch(
                            service_date,
                            exact_destination_stop,
                            profile_insertions,
                        );
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
            departure_time: primary_itinerary.departure_time.clone(),
            arrival_time: primary_itinerary.arrival_time.clone(),
            duration_seconds: primary_itinerary.duration_seconds,
            transfers: transit_legs.saturating_sub(1),
            legs: primary_itinerary.legs.clone(),
            itineraries,
            deferred_hydration: primary_itinerary.deferred_hydration.clone(),
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

    fn collect_query_itinerary_candidates(
        &self,
        from_index: usize,
        to_index: usize,
        limit: usize,
        parents: &[Vec<Option<ParentStep>>],
        round_arrivals: &[Vec<i32>],
    ) -> Result<Vec<QueryItineraryCandidate>> {
        let mut candidates = Vec::<QueryItineraryCandidate>::new();
        let mut seen_signatures = HashSet::<String>::new();

        for round in 0..round_arrivals.len() {
            let arrival_secs = round_arrivals[round][to_index];
            if arrival_secs >= INF_TIME || parents[round][to_index].is_none() {
                continue;
            }

            let raw_legs = self.reconstruct_path(from_index, to_index, round, parents, round_arrivals)?;
            let signature = raw_leg_signature(&raw_legs);
            if !seen_signatures.insert(signature) {
                continue;
            }

            let transit_legs = raw_legs
                .iter()
                .filter(|leg| matches!(leg, RawLeg::Transit { .. }))
                .count();
            candidates.push(QueryItineraryCandidate {
                round,
                arrival_secs,
                transit_legs,
                raw_legs,
            });
        }

        candidates.sort_by(|left, right| {
            left.arrival_secs
                .cmp(&right.arrival_secs)
                .then_with(|| left.transit_legs.cmp(&right.transit_legs))
                .then_with(|| left.round.cmp(&right.round))
        });

        let fastest_index = candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| (candidate.arrival_secs, candidate.transit_legs, candidate.round))
            .map(|(index, _)| index);
        let fewest_transfers_index = candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| {
                (
                    candidate.transit_legs.saturating_sub(1),
                    candidate.arrival_secs,
                    candidate.round,
                )
            })
            .map(|(index, _)| index);

        let mut ordered = Vec::<QueryItineraryCandidate>::with_capacity(candidates.len());
        let mut used_indices = HashSet::<usize>::new();

        if let Some(index) = fastest_index {
            used_indices.insert(index);
            ordered.push(candidates[index].clone());
        }
        if let Some(index) = fewest_transfers_index {
            if used_indices.insert(index) {
                ordered.push(candidates[index].clone());
            }
        }
        for (index, candidate) in candidates.into_iter().enumerate() {
            if used_indices.insert(index) {
                ordered.push(candidate);
            }
        }

        ordered.truncate(limit.max(1));
        Ok(ordered)
    }

    fn hydrate_query_itinerary_candidates(
        &self,
        service_date: NaiveDate,
        departure_secs: i32,
        candidates: &[QueryItineraryCandidate],
        overlay: &QueryOverlay,
    ) -> Result<Vec<QueryItineraryResponse>> {
        let mut itineraries = Vec::with_capacity(candidates.len());

        for candidate in candidates {
            let transfers = candidate.transit_legs.saturating_sub(1);
            let legs = self.hydrate_legs(service_date, &candidate.raw_legs, overlay)?;
            let passive_metrics = score_itinerary_passive_metrics(&legs);
            let deferred_hydration = self.build_deferred_hydration(&legs)?;

            itineraries.push(QueryItineraryResponse {
                id: format!(
                    "round-{}-{}-{}",
                    candidate.round,
                    candidate.arrival_secs,
                    transfers
                ),
                label: String::new(),
                badges: Vec::new(),
                is_recommended: false,
                is_fastest: false,
                is_fewest_transfers: false,
                is_best_realtime: false,
                is_least_crowded: false,
                has_canceled_legs: passive_metrics.has_canceled_legs,
                departure_time: format_service_time(service_date, departure_secs),
                arrival_time: format_service_time(service_date, candidate.arrival_secs),
                duration_seconds: candidate.arrival_secs - departure_secs,
                transfers,
                realtime_score: passive_metrics.transit_legs_with_gtfs_rt,
                transit_leg_count: passive_metrics.transit_leg_count,
                transit_legs_with_gtfs_rt: passive_metrics.transit_legs_with_gtfs_rt,
                crowding_score: passive_metrics.crowding_score,
                crowding_level: passive_metrics.crowding_level,
                occupancy_covered_transit_legs: passive_metrics.occupancy_covered_transit_legs,
                canceled_transit_legs: passive_metrics.canceled_transit_legs,
                legs,
                deferred_hydration,
            });
        }

        let fastest_duration = itineraries
            .iter()
            .map(|itinerary| itinerary.duration_seconds)
            .min()
            .unwrap_or(0);
        let fewest_transfers = itineraries
            .iter()
            .map(|itinerary| itinerary.transfers)
            .min()
            .unwrap_or(0);
        let best_realtime_score = itineraries
            .iter()
            .map(|itinerary| itinerary.transit_legs_with_gtfs_rt)
            .max()
            .unwrap_or(0);
        let best_crowding_score = itineraries
            .iter()
            .filter(|itinerary| itinerary.occupancy_covered_transit_legs > 0)
            .filter_map(|itinerary| itinerary.crowding_score)
            .min();

        itineraries.sort_by(|left, right| {
            left.has_canceled_legs
                .cmp(&right.has_canceled_legs)
                .then_with(|| left.duration_seconds.cmp(&right.duration_seconds))
                .then_with(|| left.transfers.cmp(&right.transfers))
                .then_with(|| right.transit_legs_with_gtfs_rt.cmp(&left.transit_legs_with_gtfs_rt))
                .then_with(|| compare_optional_crowding(left.crowding_score, right.crowding_score))
                .then_with(|| {
                    right
                        .occupancy_covered_transit_legs
                        .cmp(&left.occupancy_covered_transit_legs)
                })
                .then_with(|| left.id.cmp(&right.id))
        });

        for (index, itinerary) in itineraries.iter_mut().enumerate() {
            let is_recommended = index == 0;
            let is_fastest = itinerary.duration_seconds == fastest_duration;
            let is_fewest_transfers = itinerary.transfers == fewest_transfers;
            let is_best_realtime = best_realtime_score > 0
                && itinerary.transit_legs_with_gtfs_rt == best_realtime_score;
            let is_least_crowded = best_crowding_score.is_some()
                && itinerary.occupancy_covered_transit_legs > 0
                && itinerary.crowding_score == best_crowding_score;

            itinerary.is_recommended = is_recommended;
            itinerary.is_fastest = is_fastest;
            itinerary.is_fewest_transfers = is_fewest_transfers;
            itinerary.is_best_realtime = is_best_realtime;
            itinerary.is_least_crowded = is_least_crowded;
            itinerary.label = build_itinerary_label(
                index,
                is_recommended,
                is_fastest,
                is_fewest_transfers,
                is_best_realtime,
                is_least_crowded,
            );
            itinerary.badges = build_itinerary_badges(
                index,
                is_recommended,
                is_fastest,
                is_fewest_transfers,
                is_best_realtime,
                is_least_crowded,
                itinerary.has_canceled_legs,
            );
        }

        Ok(itineraries)
    }

    fn scan_line(
        &self,
        line_index: usize,
        start_pos: usize,
        destination_stop: usize,
        trip_is_available: &[bool],
        trip_has_realtime_update: &[bool],
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

        let mut stop_pos = start_pos;
        while stop_pos < line.stop_indices.len() {
            if let Some(trip_index) = current_trip {
                let remaining = line.stop_indices.len() - stop_pos;
                let trip_has_update = trip_has_realtime_update
                    .get(trip_index)
                    .copied()
                    .unwrap_or(true);
                if !trip_has_update && remaining >= SVRT_WIDTH {
                    let departure_chunk =
                        scheduled_departure_chunk(&self.static_data.trips[trip_index], stop_pos);
                    if !svrt_chunk_has_catchup_candidate(
                        &line.stop_indices[stop_pos..stop_pos + SVRT_WIDTH],
                        best_before_round,
                        &departure_chunk,
                    ) {
                        for chunk_pos in stop_pos..stop_pos + SVRT_WIDTH {
                            self.evaluate_onboard_arrival(
                                line,
                                chunk_pos,
                                destination_stop,
                                trip_has_realtime_update,
                                global_best,
                                round_arrivals,
                                parents,
                                improved_stops,
                                improved_flags,
                                trip_index,
                                boarded_at,
                                metrics,
                            );
                        }
                        stop_pos += SVRT_WIDTH;
                        continue;
                    }
                }
            }

            self.scan_line_scalar_step(
                line,
                stop_pos,
                destination_stop,
                trip_is_available,
                trip_has_realtime_update,
                line_max_positive_delay_secs,
                best_before_round,
                global_best,
                round_arrivals,
                parents,
                improved_stops,
                improved_flags,
                &mut current_trip,
                &mut boarded_at,
                metrics,
            );
            stop_pos += 1;
        }
    }

    fn scan_line_scalar_step(
        &self,
        line: &LineRecord,
        stop_pos: usize,
        destination_stop: usize,
        trip_is_available: &[bool],
        trip_has_realtime_update: &[bool],
        line_max_positive_delay_secs: i32,
        best_before_round: &[i32],
        global_best: &mut [i32],
        round_arrivals: &mut [i32],
        parents: &mut [Option<ParentStep>],
        improved_stops: &mut Vec<usize>,
        improved_flags: &mut [bool],
        current_trip: &mut Option<usize>,
        boarded_at: &mut usize,
        metrics: &mut RoundMetrics,
    ) {
        let stop_index = line.stop_indices[stop_pos];

        if let Some(trip_index) = *current_trip {
            self.evaluate_onboard_arrival(
                line,
                stop_pos,
                destination_stop,
                trip_has_realtime_update,
                global_best,
                round_arrivals,
                parents,
                improved_stops,
                improved_flags,
                trip_index,
                *boarded_at,
                metrics,
            );
        }

        let destination_bound = global_best[destination_stop];
        let ready_at = best_before_round[stop_index];
        if ready_at >= INF_TIME || ready_at >= destination_bound {
            metrics.destination_bound_prunes += 1;
            return;
        }

        let trip_search_started = Instant::now();
        let candidate_trip = self.find_earliest_trip(
            line,
            stop_pos,
            ready_at,
            trip_is_available,
            trip_has_realtime_update,
            line_max_positive_delay_secs,
            metrics,
        );
        metrics.timings_us.line_scan_trip_search_us += trip_search_started.elapsed().as_micros();

        if let Some(candidate_trip) = candidate_trip {
            let candidate_compare_started = Instant::now();
            let candidate_departure = self.scan_departure_for_trip(
                candidate_trip,
                stop_pos,
                trip_has_realtime_update,
                metrics,
            );

            let replace_current = match *current_trip {
                Some(active_trip) => {
                    let active_departure = self.scan_departure_for_trip(
                        active_trip,
                        stop_pos,
                        trip_has_realtime_update,
                        metrics,
                    );
                    candidate_departure < active_departure
                }
                None => true,
            };

            if replace_current {
                *current_trip = Some(candidate_trip);
                *boarded_at = stop_pos;
            }
            metrics.timings_us.line_scan_candidate_compare_us +=
                candidate_compare_started.elapsed().as_micros();
        }
    }

    fn evaluate_onboard_arrival(
        &self,
        line: &LineRecord,
        stop_pos: usize,
        destination_stop: usize,
        trip_has_realtime_update: &[bool],
        global_best: &mut [i32],
        round_arrivals: &mut [i32],
        parents: &mut [Option<ParentStep>],
        improved_stops: &mut Vec<usize>,
        improved_flags: &mut [bool],
        trip_index: usize,
        boarded_at: usize,
        metrics: &mut RoundMetrics,
    ) {
        let stop_index = line.stop_indices[stop_pos];
        let destination_bound = global_best[destination_stop];
        let onboard_started = Instant::now();
        metrics.onboard_arrival_evaluations += 1;

        let trip_has_update = trip_has_realtime_update
            .get(trip_index)
            .copied()
            .unwrap_or(true);
        if trip_has_update {
            metrics.skipped_stop_checks += 1;
            if self.realtime.is_stop_skipped(trip_index, stop_pos) {
                metrics.timings_us.line_scan_onboard_us += onboard_started.elapsed().as_micros();
                return;
            }
            metrics.actual_arrival_calls += 1;
        }

        let arrival = if trip_has_update {
            self.realtime
                .actual_arrival(&self.static_data.trips, trip_index, stop_pos)
        } else {
            self.static_data.trips[trip_index].stop_times[stop_pos].arrival_secs
        };

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

        metrics.timings_us.line_scan_onboard_us += onboard_started.elapsed().as_micros();
    }

    fn scan_departure_for_trip(
        &self,
        trip_index: usize,
        stop_pos: usize,
        trip_has_realtime_update: &[bool],
        metrics: &mut RoundMetrics,
    ) -> i32 {
        if trip_has_realtime_update
            .get(trip_index)
            .copied()
            .unwrap_or(true)
        {
            metrics.actual_departure_calls += 1;
            self.realtime
                .actual_departure(&self.static_data.trips, trip_index, stop_pos)
        } else {
            self.static_data.trips[trip_index].stop_times[stop_pos].departure_secs
        }
    }

    fn relax_transfers_tiled(
        &self,
        destination_stop: usize,
        frontier: &[usize],
        global_best: &mut [i32],
        round_arrivals: &mut [i32],
        parents: &mut [Option<ParentStep>],
        improved_stops: &mut Vec<usize>,
        improved_flags: &mut [bool],
        metrics: &mut RoundMetrics,
    ) {
        let transfer_index = &self.static_data.transfer_index;
        if frontier.is_empty() || transfer_index.hubs.is_empty() {
            return;
        }

        let destination_bound = global_best[destination_stop];
        let mut touched_hubs = Vec::<usize>::new();
        let mut frontier_by_hub = HashMap::<usize, Vec<(usize, usize, i32)>>::new();

        for &source_stop in frontier {
            if source_stop >= transfer_index.stop_to_hub.len() {
                continue;
            }

            let stop_arrival = round_arrivals[source_stop];
            if stop_arrival >= INF_TIME || stop_arrival >= destination_bound {
                metrics.destination_bound_prunes += 1;
                continue;
            }

            let hub_index = transfer_index.stop_to_hub[source_stop];
            let source_offset = transfer_index.stop_to_hub_offset[source_stop];
            if let Some(active_sources) = frontier_by_hub.get_mut(&hub_index) {
                active_sources.push((source_stop, source_offset, stop_arrival));
            } else {
                touched_hubs.push(hub_index);
                frontier_by_hub.insert(hub_index, vec![(source_stop, source_offset, stop_arrival)]);
            }
        }

        for hub_index in touched_hubs {
            let Some(active_sources) = frontier_by_hub.get(&hub_index) else {
                continue;
            };
            for tile in &transfer_index.hubs[hub_index].outgoing_tiles {
                let target_count = tile.target_stop_indices.len();
                for (target_offset, &target_stop) in tile.target_stop_indices.iter().enumerate() {
                    let mut best_candidate = global_best[target_stop];
                    let mut best_edge = None::<(usize, usize)>;

                    for &(source_stop, source_offset, source_arrival) in active_sources {
                        let cell_index = source_offset * target_count + target_offset;
                        let transfer_slot = tile.transfer_slots[cell_index];
                        if transfer_slot == EMPTY_TRANSFER_SLOT {
                            continue;
                        }

                        metrics.transfer_relaxations += 1;
                        let candidate = source_arrival + tile.durations[cell_index];
                        if candidate < best_candidate {
                            best_candidate = candidate;
                            best_edge = Some((source_stop, transfer_slot as usize));
                        }
                    }

                    if let Some((source_stop, transfer_slot)) = best_edge {
                        let transfer = &self.static_data.transfers[source_stop][transfer_slot];
                        if record_stop_improvement(
                            transfer.to_stop,
                            best_candidate,
                            global_best,
                            round_arrivals,
                            parents,
                            ParentStep::Walk {
                                from_stop: source_stop,
                                duration_secs: transfer.duration_secs,
                                distance_meters: transfer.distance_meters,
                            },
                            improved_stops,
                            improved_flags,
                        ) {
                            metrics.transfer_improvements += 1;
                        }
                    }
                }
            }
        }
    }

    fn find_earliest_trip(
        &self,
        line: &LineRecord,
        stop_pos: usize,
        ready_at: i32,
        trip_is_available: &[bool],
        trip_has_realtime_update: &[bool],
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
        let Some(start_index) =
            chronos_bucket_start_index(line, stop_pos, ready_at, line_max_positive_delay_secs)
        else {
            metrics.chronos_bucket_fallback_searches += 1;
            metrics.chronos_bucket_fallback_non_monotonic += 1;
            return self.find_earliest_trip_linear_fallback(
                line,
                stop_pos,
                ready_at,
                trip_is_available,
                trip_has_realtime_update,
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
            let Some(trip_index) =
                line_trip_index_at_temporal_position(line, stop_pos, search_index)
            else {
                break;
            };
            metrics.trip_departure_checks += 1;
            if !trip_is_available[trip_index] {
                continue;
            }
            if trip_has_realtime_update
                .get(trip_index)
                .copied()
                .unwrap_or(true)
            {
                metrics.skipped_stop_checks += 1;
                if self.realtime.is_stop_skipped(trip_index, stop_pos) {
                    continue;
                }
            }
            let departure = self.scan_departure_for_trip(
                trip_index,
                stop_pos,
                trip_has_realtime_update,
                metrics,
            );
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
        trip_has_realtime_update: &[bool],
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
            if trip_has_realtime_update
                .get(*trip_index)
                .copied()
                .unwrap_or(true)
            {
                metrics.skipped_stop_checks += 1;
                if self.realtime.is_stop_skipped(*trip_index, stop_pos) {
                    continue;
                }
            }
            let departure = self.scan_departure_for_trip(
                *trip_index,
                stop_pos,
                trip_has_realtime_update,
                metrics,
            );
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
        trip_has_realtime_update: &[bool],
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
            let Some(true) = spatial_flat_mask
                .enabled_source_stops
                .get(stop_index)
                .copied()
            else {
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
                        trip_has_realtime_update,
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
                        trip_has_realtime_update,
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
        trip_has_realtime_update: &[bool],
        line_max_positive_delay_secs: &[i32],
        local_subquery_cache: &mut HashMap<LocalSubqueryKey, Option<LocalSubqueryResult>>,
    ) -> Option<(i32, usize, Vec<CachedLeg>)> {
        let remaining_after_trunk =
            remaining_transit_legs.saturating_sub(spatial_match.transit_legs);
        let mut best_match = None::<(i32, usize, Vec<CachedLeg>)>;

        for edge in destination_egress_edges {
            let Some(local_result) = self.lookup_or_compute_local_subquery(
                spatial_match.boundary_stop,
                edge.from_stop,
                spatial_match.boundary_arrival_secs,
                remaining_after_trunk,
                trip_is_available,
                trip_has_realtime_update,
                line_max_positive_delay_secs,
                local_subquery_cache,
            ) else {
                continue;
            };

            let final_arrival = local_result.arrival_secs + edge.duration_secs;
            let total_transit_legs = spatial_match.transit_legs + local_result.transit_legs;
            let mut suffix_legs =
                combine_cached_leg_sequences(spatial_match.trunk_legs.as_ref(), &local_result.legs);
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
        trip_has_realtime_update: &[bool],
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
            trip_has_realtime_update,
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
        trip_has_realtime_update: &[bool],
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
                    trip_has_realtime_update,
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
            let transfer_frontier = next_marked[..transit_frontier_len].to_vec();
            self.relax_transfers_tiled(
                to_stop,
                &transfer_frontier,
                &mut global_best,
                &mut round_arrivals[round],
                &mut parents[round],
                &mut next_marked,
                &mut next_marked_flags,
                &mut round_metrics,
            );

            if next_marked_flags[to_stop] {
                destination_round = Some(round);
            }

            std::mem::swap(&mut marked_stops, &mut next_marked);
        }

        let destination_round = destination_round?;
        let arrival_secs = round_arrivals[destination_round][to_stop];
        let raw_legs = self
            .reconstruct_path(
                from_stop,
                to_stop,
                destination_round,
                &parents,
                &round_arrivals,
            )
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

    fn hydrate_legs(
        &self,
        service_date: NaiveDate,
        raw_legs: &[RawLeg],
        overlay: &QueryOverlay,
    ) -> Result<Vec<LegResponse>> {
        let mut legs = Vec::with_capacity(raw_legs.len());
        let mut index = 0usize;

        while index < raw_legs.len() {
            match &raw_legs[index] {
                RawLeg::Walk { .. } => {
                    let run_start = index;
                    while index < raw_legs.len() && matches!(raw_legs[index], RawLeg::Walk { .. }) {
                        index += 1;
                    }
                    legs.push(self.hydrate_walk_run(
                        service_date,
                        &raw_legs[run_start..index],
                        overlay,
                    )?);
                }
                _ => {
                    legs.push(self.hydrate_leg(service_date, raw_legs[index].clone(), overlay)?);
                    index += 1;
                }
            }
        }

        Ok(legs)
    }

    fn hydrate_walk_run(
        &self,
        service_date: NaiveDate,
        walk_legs: &[RawLeg],
        overlay: &QueryOverlay,
    ) -> Result<LegResponse> {
        let Some(RawLeg::Walk {
            from_stop,
            departure_secs,
            ..
        }) = walk_legs.first()
        else {
            bail!("walk hydration requires at least one walk leg");
        };
        let Some(RawLeg::Walk {
            to_stop,
            arrival_secs,
            ..
        }) = walk_legs.last()
        else {
            bail!("walk hydration requires at least one walk leg");
        };

        let distance_meters = walk_legs
            .iter()
            .map(|leg| match leg {
                RawLeg::Walk {
                    distance_meters, ..
                } => *distance_meters,
                RawLeg::Transit { .. } => 0.0,
            })
            .sum();

        let geometries = walk_legs
            .iter()
            .map(|leg| match leg {
                RawLeg::Walk {
                    from_stop, to_stop, ..
                } => self.query_walk_geometry(*from_stop, *to_stop, overlay),
                RawLeg::Transit { .. } => WalkGeometry::default(),
            })
            .collect::<Vec<_>>();
        let geometry = stitch_walk_geometries(&geometries);

        let from_result = self.query_stop_result(*from_stop, overlay);
        let to_result = self.query_stop_result(*to_stop, overlay);
        let walk_directions = build_walk_directions(
            &geometry,
            self.walk_way_names.as_ref(),
            to_result.name.as_str(),
        );

        Ok(LegResponse {
            kind: "walk",
            departure_time: format_service_time(service_date, *departure_secs),
            arrival_time: format_service_time(service_date, *arrival_secs),
            duration_seconds: *arrival_secs - *departure_secs,
            from_stop: from_result,
            to_stop: to_result,
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
            has_gtfs_rt: false,
            has_trip_update: false,
            has_vehicle_position: false,
            schedule_relationship: None,
            occupancy_status: None,
            occupancy_percentage: None,
            occupancy_score: None,
            intermediate_stops: Vec::new(),
            polyline: geometry.polyline,
            walk_directions,
        })
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
            } => {
                let geometry = self.query_walk_geometry(from_stop, to_stop, overlay);
                let from_result = self.query_stop_result(from_stop, overlay);
                let to_result = self.query_stop_result(to_stop, overlay);
                Ok(LegResponse {
                    kind: "walk",
                    departure_time: format_service_time(service_date, departure_secs),
                    arrival_time: format_service_time(service_date, arrival_secs),
                    duration_seconds: duration_secs,
                    from_stop: from_result,
                    to_stop: to_result.clone(),
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
                    has_gtfs_rt: false,
                    has_trip_update: false,
                    has_vehicle_position: false,
                    schedule_relationship: None,
                    occupancy_status: None,
                    occupancy_percentage: None,
                    occupancy_score: None,
                    intermediate_stops: Vec::new(),
                    polyline: geometry.polyline.clone(),
                    walk_directions: build_walk_directions(
                        &geometry,
                        self.walk_way_names.as_ref(),
                        to_result.name.as_str(),
                    ),
                })
            }
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
                let passive_realtime = self.passive_realtime_fields(trip_index, board_pos);
                let scheduled_departure = trip.stop_times[board_pos].departure_secs;
                let delay_applied_seconds = departure_secs - scheduled_departure;
                let intermediate_stops = self.trip_intermediate_stops(trip, board_pos, alight_pos);
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
                    has_gtfs_rt: passive_realtime.has_gtfs_rt(),
                    has_trip_update: passive_realtime.has_trip_update,
                    has_vehicle_position: passive_realtime.has_vehicle_position,
                    schedule_relationship: schedule_relationship_string(
                        passive_realtime.schedule_relationship,
                    ),
                    occupancy_status: occupancy_status_string(
                        passive_realtime
                            .stop_departure_occupancy_status
                            .or(passive_realtime.vehicle_occupancy_status),
                    ),
                    occupancy_percentage: passive_realtime.vehicle_occupancy_percentage,
                    occupancy_score: passive_occupancy_score(
                        passive_realtime
                            .stop_departure_occupancy_status
                            .or(passive_realtime.vehicle_occupancy_status),
                        passive_realtime.vehicle_occupancy_percentage,
                    ),
                    intermediate_stops,
                    polyline: self.trip_polyline(trip, board_pos, alight_pos),
                    walk_directions: Vec::new(),
                })
            }
        }
    }

    fn trip_intermediate_stops(
        &self,
        trip: &TripRecord,
        board_pos: usize,
        alight_pos: usize,
    ) -> Vec<StopSearchResult> {
        intermediate_trip_stop_times(trip, board_pos, alight_pos)
            .iter()
            .map(|stop_time| self.stop_result(stop_time.stop_index))
            .collect()
    }

    fn trip_polyline(
        &self,
        trip: &TripRecord,
        board_pos: usize,
        alight_pos: usize,
    ) -> Vec<PolylinePoint> {
        if let Some(shape_id) = &trip.shape_id {
            if let Ok(Some(shape)) = self.static_data.cold_store.shape_points(shape_id) {
                if let Some(points) = shape_polyline_segment(
                    &shape,
                    &self.static_data.stops,
                    trip,
                    board_pos,
                    alight_pos,
                ) {
                    return points;
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

    fn query_walk_geometry(
        &self,
        from_stop: usize,
        to_stop: usize,
        overlay: &QueryOverlay,
    ) -> WalkGeometry {
        if let Some(edge) = overlay.virtual_walks.get(&(from_stop, to_stop)) {
            return WalkGeometry {
                polyline: edge.polyline.clone(),
                segment_way_ids: edge.segment_way_ids.clone(),
            };
        }
        if from_stop < self.static_data.stops.len() && to_stop < self.static_data.stops.len() {
            return self.walk_transfer_geometry(from_stop, to_stop);
        }

        let Some((from_lat, from_lon)) = self.query_stop_coordinates(from_stop, overlay) else {
            return WalkGeometry::default();
        };
        let Some((to_lat, to_lon)) = self.query_stop_coordinates(to_stop, overlay) else {
            return WalkGeometry::default();
        };

        let polyline = vec![
            PolylinePoint {
                lat: from_lat,
                lon: from_lon,
            },
            PolylinePoint {
                lat: to_lat,
                lon: to_lon,
            },
        ];
        WalkGeometry {
            segment_way_ids: empty_segment_way_ids(&polyline),
            polyline,
        }
    }

    fn passive_realtime_fields(&self, trip_index: usize, board_pos: usize) -> TripRealtimeMetrics {
        self.realtime.trip_realtime_metrics(trip_index, board_pos)
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

    fn walk_transfer_geometry(&self, from_stop: usize, to_stop: usize) -> WalkGeometry {
        self.static_data.transfers[from_stop]
            .iter()
            .find(|transfer| transfer.to_stop == to_stop)
            .map(|transfer| WalkGeometry {
                polyline: transfer.polyline.clone(),
                segment_way_ids: transfer.segment_way_ids.clone(),
            })
            .filter(|geometry| geometry.polyline.len() >= 2)
            .unwrap_or_else(|| {
                let polyline = straight_polyline(
                    &self.static_data.stops[from_stop],
                    &self.static_data.stops[to_stop],
                );
                WalkGeometry {
                    segment_way_ids: empty_segment_way_ids(&polyline),
                    polyline,
                }
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

    fn build_deferred_hydration(&self, legs: &[LegResponse]) -> Result<DeferredHydrationResponse> {
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
            for stop in &leg.intermediate_stops {
                if stop_seen.insert(stop.global_id) {
                    entities.stops.push(stop.clone());
                }
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

            let intermediate_stop_gids = leg
                .intermediate_stops
                .iter()
                .map(|stop| stop.global_id)
                .collect();

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
                has_gtfs_rt: leg.has_gtfs_rt,
                has_trip_update: leg.has_trip_update,
                has_vehicle_position: leg.has_vehicle_position,
                schedule_relationship: leg.schedule_relationship.clone(),
                occupancy_status: leg.occupancy_status.clone(),
                occupancy_percentage: leg.occupancy_percentage,
                occupancy_score: leg.occupancy_score,
                intermediate_stop_gids,
                polyline_index,
                walk_directions: leg.walk_directions.clone(),
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
                        if let Some((
                            cell_boundary_stop,
                            cell_boundary_pos,
                            cell_boundary_arrival,
                        )) = self.first_transit_stop_in_cell(
                            *trip_index,
                            *board_pos,
                            destination_cell,
                        ) {
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

struct PreparedOsmPbfSource {
    local_path: PathBuf,
    source: String,
    remote_url: Option<String>,
    allow_invalid_tls: bool,
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

fn prepare_osm_pbf_source(
    workspace_root: &Path,
    base_dir: &Path,
    source_value: &str,
    allow_invalid_tls: bool,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<PreparedOsmPbfSource> {
    if is_remote_source(source_value) {
        let cache_path = remote_osm_pbf_cache_path(workspace_root, source_value);
        sync_remote_osm_pbf(source_value, allow_invalid_tls, &cache_path, refresh_mode)?;
        return Ok(PreparedOsmPbfSource {
            local_path: cache_path,
            source: source_value.to_owned(),
            remote_url: Some(source_value.to_owned()),
            allow_invalid_tls,
        });
    }

    Ok(PreparedOsmPbfSource {
        local_path: resolve_path_from(base_dir, source_value),
        source: source_value.to_owned(),
        remote_url: None,
        allow_invalid_tls,
    })
}

fn prepare_osm_pbf_source_legacy(
    workspace_root: &Path,
    source_value: Option<String>,
    default_name: &str,
    allow_invalid_tls: bool,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<PreparedOsmPbfSource> {
    match source_value {
        Some(source_value) => prepare_osm_pbf_source(
            workspace_root,
            workspace_root,
            &source_value,
            allow_invalid_tls,
            refresh_mode,
        ),
        None => Ok(PreparedOsmPbfSource {
            local_path: workspace_root.join(default_name),
            source: default_name.to_owned(),
            remote_url: None,
            allow_invalid_tls,
        }),
    }
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

fn remote_osm_pbf_cache_path(workspace_root: &Path, source_value: &str) -> PathBuf {
    let file_name = source_value
        .rsplit('/')
        .next()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("osm-latest.osm.pbf");
    runtime_root(workspace_root).join("osm").join(file_name)
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
    let response = maybe_add_internal_token_blocking(client.head(url), url)?
        .send()
        .and_then(|response| response.error_for_status())
        .with_context(|| {
            format!("failed to probe static GTFS feed {feed_id} metadata from {url}")
        })?;

    Ok(remote_static_gtfs_version_from_headers(
        url,
        response.headers(),
    ))
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
                        if remote_static_gtfs_version_changed(
                            cached_version.as_ref(),
                            &remote_version,
                        ) {
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
            StaticGtfsRefreshMode::Poll => {
                match probe_remote_static_gtfs_version(&client, feed_id, url) {
                    Ok(remote_version) => {
                        if !remote_static_gtfs_version_changed(
                            cached_version.as_ref(),
                            &remote_version,
                        ) {
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
                }
            }
        }
    }

    let response = maybe_add_internal_token_blocking(client.get(url), url)?
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

    if let Err(error) = store_remote_static_gtfs_version_metadata(&version_path, &version_metadata)
    {
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

fn sync_remote_osm_pbf(
    url: &str,
    allow_invalid_tls: bool,
    cache_path: &Path,
    refresh_mode: StaticGtfsRefreshMode,
) -> Result<()> {
    let client = BlockingHttpClient::builder()
        .user_agent("alpha-raptor-engine/0.1")
        .danger_accept_invalid_certs(allow_invalid_tls)
        .build()
        .context("failed to build HTTP client for OSM PBF source")?;

    let version_path = remote_static_gtfs_version_path(cache_path);
    let cached_version = match load_remote_static_gtfs_version_metadata(&version_path) {
        Ok(metadata) => metadata,
        Err(error) => {
            warn!(
                %error,
                metadata = %version_path.display(),
                "failed to load cached remote OSM PBF version metadata"
            );
            None
        }
    };

    let mut probed_version = None;
    if cache_path.exists() {
        match refresh_mode {
            StaticGtfsRefreshMode::Bootstrap => {
                match probe_remote_static_gtfs_version(&client, "osm-pbf", url) {
                    Ok(remote_version) => {
                        if remote_static_gtfs_version_changed(
                            cached_version.as_ref(),
                            &remote_version,
                        ) {
                            info!(
                                url,
                                cache = %cache_path.display(),
                                remote_last_modified = remote_version
                                    .last_modified
                                    .as_deref()
                                    .unwrap_or("<missing>"),
                                remote_etag = remote_version.etag.as_deref().unwrap_or("<missing>"),
                                "remote OSM PBF differs upstream; bootstrapping from cached file and deferring sync to background poll"
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            %error,
                            url,
                            cache = %cache_path.display(),
                            "failed to probe remote OSM PBF metadata during bootstrap; reusing cached file"
                        );
                    }
                }
                return Ok(());
            }
            StaticGtfsRefreshMode::Poll => {
                match probe_remote_static_gtfs_version(&client, "osm-pbf", url) {
                    Ok(remote_version) => {
                        if !remote_static_gtfs_version_changed(
                            cached_version.as_ref(),
                            &remote_version,
                        ) {
                            return Ok(());
                        }
                        info!(
                            url,
                            cache = %cache_path.display(),
                            remote_last_modified = remote_version
                                .last_modified
                                .as_deref()
                                .unwrap_or("<missing>"),
                            remote_etag = remote_version.etag.as_deref().unwrap_or("<missing>"),
                            "detected upstream OSM PBF version change"
                        );
                        probed_version = Some(remote_version);
                    }
                    Err(error) => {
                        warn!(
                            %error,
                            url,
                            cache = %cache_path.display(),
                            "failed to probe remote OSM PBF metadata during poll; keeping cached file"
                        );
                        return Ok(());
                    }
                }
            }
        }
    }

    let response = maybe_add_internal_token_blocking(client.get(url), url)?
        .send()
        .and_then(|response| response.error_for_status())
        .with_context(|| format!("failed to download OSM PBF from {url}"));

    if let Some(parent) = cache_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create cache directory for OSM PBF at {}",
                parent.display()
            )
        })?;
    }

    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let tmp_path = cache_path.with_extension(format!("download-{unique_suffix}.tmp"));

    let (downloaded_bytes, version_metadata) = match response {
        Ok(response) => {
            let response_version = merged_remote_static_gtfs_version_metadata(
                url,
                remote_static_gtfs_version_from_headers(url, response.headers()),
                probed_version.as_ref(),
            );
            match stream_blocking_response_to_file(response, &tmp_path, "OSM PBF") {
                Ok(downloaded_bytes) => (downloaded_bytes, response_version),
                Err(error) => {
                    let _ = fs::remove_file(&tmp_path);
                    if cache_path.exists() {
                        warn!(
                            %error,
                            url,
                            cache = %cache_path.display(),
                            "remote OSM PBF refresh failed during body stream, reusing cached file"
                        );
                        return Ok(());
                    }
                    return Err(error);
                }
            }
        }
        Err(error) => {
            if cache_path.exists() {
                warn!(
                    %error,
                    url,
                    cache = %cache_path.display(),
                    "remote OSM PBF refresh failed, reusing cached file"
                );
                return Ok(());
            }
            return Err(error);
        }
    };
    if cache_path.exists() {
        fs::remove_file(cache_path).with_context(|| {
            format!(
                "failed to replace cached OSM PBF at {}",
                cache_path.display()
            )
        })?;
    }
    fs::rename(&tmp_path, cache_path).with_context(|| {
        format!(
            "failed to move downloaded OSM PBF into cache path {}",
            cache_path.display()
        )
    })?;

    if let Err(error) = store_remote_static_gtfs_version_metadata(&version_path, &version_metadata)
    {
        warn!(
            %error,
            metadata = %version_path.display(),
            "failed to persist remote OSM PBF version metadata"
        );
    }

    info!(
        url,
        cache = %cache_path.display(),
        allow_invalid_tls,
        downloaded_bytes,
        remote_last_modified = version_metadata.last_modified.as_deref().unwrap_or("<missing>"),
        remote_etag = version_metadata.etag.as_deref().unwrap_or("<missing>"),
        "synced remote OSM PBF"
    );

    Ok(())
}

fn stream_blocking_response_to_file(
    mut response: reqwest::blocking::Response,
    destination_path: &Path,
    artifact_name: &str,
) -> Result<u64> {
    let file = File::create(destination_path).with_context(|| {
        format!(
            "failed to create temporary {} file {}",
            artifact_name,
            destination_path.display()
        )
    })?;
    let mut writer = BufWriter::new(file);
    let total_bytes = response.content_length();
    let mut downloaded_bytes = 0u64;
    let mut next_progress_log = 64_u64 * 1024 * 1024;
    let mut buffer = vec![0u8; 1024 * 1024];

    info!(
        artifact = artifact_name,
        destination = %destination_path.display(),
        total_bytes = total_bytes.unwrap_or(0),
        "starting streamed remote download"
    );

    loop {
        let read = response
            .read(buffer.as_mut_slice())
            .with_context(|| format!("failed to read {artifact_name} response body"))?;
        if read == 0 {
            break;
        }

        writer.write_all(&buffer[..read]).with_context(|| {
            format!(
                "failed to write streamed {} chunk into {}",
                artifact_name,
                destination_path.display()
            )
        })?;
        downloaded_bytes += read as u64;

        if downloaded_bytes >= next_progress_log {
            if let Some(total_bytes) = total_bytes {
                info!(
                    artifact = artifact_name,
                    destination = %destination_path.display(),
                    downloaded_bytes,
                    total_bytes,
                    progress = %progress_bar_u64(downloaded_bytes, total_bytes),
                    percent = progress_percent_u64(downloaded_bytes, total_bytes),
                    "streamed remote download progress"
                );
            } else {
                info!(
                    artifact = artifact_name,
                    destination = %destination_path.display(),
                    downloaded_bytes,
                    total_bytes = 0,
                    "streamed remote download progress"
                );
            }
            next_progress_log += 64_u64 * 1024 * 1024;
        }
    }

    writer.flush().with_context(|| {
        format!(
            "failed to flush streamed {} into {}",
            artifact_name,
            destination_path.display()
        )
    })?;

    Ok(downloaded_bytes)
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
    config: &EngineConfig,
) -> Result<StaticCacheMetadata> {
    let manifest_modified_unix_secs = manifest_path
        .and_then(|path| std::fs::metadata(path).ok())
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs());

    let osm_metadata = std::fs::metadata(&config.osm_pbf_path).with_context(|| {
        format!(
            "unable to stat OSM PBF at {}",
            config.osm_pbf_path.display()
        )
    })?;

    let mut feed_sources = Vec::with_capacity(config.feeds.len());
    for feed in &config.feeds {
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
        osm_source: StaticOsmSourceMetadata {
            osm_pbf_source: config.osm_pbf_source.clone(),
            osm_pbf_path: config.osm_pbf_path.display().to_string(),
            osm_pbf_remote_url: config.osm_pbf_remote_url.clone(),
            osm_pbf_allow_invalid_tls: config.osm_pbf_allow_invalid_tls,
            osm_pbf_bytes: osm_metadata.len(),
            osm_pbf_modified_unix_secs: osm_metadata
                .modified()
                .ok()
                .and_then(|value| value.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs()),
        },
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

fn build_stop_cells(
    stops: &[StopRecord],
    active_stop_indices: &[usize],
) -> HashMap<u64, Vec<usize>> {
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
    let feed_total = config.feeds.len().max(1);
    for (feed_index, feed) in config.feeds.iter().enumerate() {
        let gtfs = Gtfs::from_path(&feed.static_gtfs_path).with_context(|| {
            format!(
                "unable to read GTFS for feed {} from {}",
                feed.id,
                feed.static_gtfs_path.display()
            )
        })?;
        parsed_feeds.push((feed.clone(), gtfs));
        info!(
            phase = "gtfs-parse",
            progress = %progress_bar(feed_index + 1, feed_total),
            percent = progress_percent(feed_index + 1, feed_total),
            completed = feed_index + 1,
            total = feed_total,
            feed_id = %feed.id,
            "GTFS parse progress"
        );
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
    let total_entities = previous
        .active_stop_indices
        .len()
        .max(next.active_stop_indices.len())
        + previous
            .active_route_indices
            .len()
            .max(next.active_route_indices.len())
        + previous
            .active_trip_indices
            .len()
            .max(next.active_trip_indices.len());
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

    let mut stop_global_ids = stops
        .iter()
        .map(|stop| stop.global_id)
        .collect::<HashSet<_>>();
    let mut route_global_ids = routes
        .iter()
        .map(|route| route.global_id)
        .collect::<HashSet<_>>();
    let mut trip_global_ids = trips
        .iter()
        .map(|trip| trip.global_id)
        .collect::<HashSet<_>>();

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
                append_stop_record(&mut stops, next_stop, &mut stop_global_ids)?
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
                append_route_record(&mut routes, next_route, &mut route_global_ids)?
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

    let (lines, stop_to_lines) =
        rebuild_lines_and_stop_to_lines(&trips, &active_trip_indices, stops.len());

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

    let feed_total = parsed_feeds.len().max(1);
    for (feed_index, (feed, gtfs)) in parsed_feeds.iter().enumerate() {
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
            let shape_id = trip
                .shape_id
                .as_ref()
                .map(|shape_id| namespaced_id(&feed.id, shape_id));
            let shape_stop_point_indices = shape_id.as_ref().and_then(|shape_id| {
                let shape = shapes.get(shape_id)?;
                let needs_projection = stop_times
                    .iter()
                    .any(|stop_time| stop_time.shape_dist_traveled.is_none())
                    || shape.iter().any(|point| point.dist_traveled.is_none());
                if !needs_projection {
                    return None;
                }
                build_shape_stop_point_indices(shape, &stops, &stop_times)
            });
            let index = trips.len();
            trips.push(TripRecord {
                global_id: pack_global_id(feed.feed_index, EntityKind::Trip, local_index as u64)?,
                feed_index: feed.feed_index,
                feed_id: feed.id.clone(),
                local_id: trip.id.clone(),
                id: namespaced_trip_id.clone(),
                route_index,
                shape_id,
                shape_stop_point_indices,
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
        info!(
            phase = "transit-core",
            progress = %progress_bar(feed_index + 1, feed_total),
            percent = progress_percent(feed_index + 1, feed_total),
            completed = feed_index + 1,
            total = feed_total,
            feed_id = %feed.id,
            stops = gtfs.stops.len(),
            routes = gtfs.routes.len(),
            trips = gtfs.trips.len(),
            "transit core feed normalization progress"
        );
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
                if !route_records_equivalent(
                    &previous.routes[*previous_index],
                    &next.routes[*next_index],
                ) {
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
                if !stop_records_equivalent(
                    &previous_stops[*previous_index],
                    &next_stops[*next_index],
                ) {
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
    if !route_records_equivalent(
        &left_routes[left.route_index],
        &right_routes[right.route_index],
    ) {
        return false;
    }
    if left.shape_id != right.shape_id
        || left.shape_stop_point_indices != right.shape_stop_point_indices
        || left.headsign != right.headsign
    {
        return false;
    }
    if left.stop_times.len() != right.stop_times.len() {
        return false;
    }

    left.stop_times
        .iter()
        .zip(&right.stop_times)
        .all(|(left_stop, right_stop)| {
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

        let mut chronos_bucket_start_indices_by_stop = Vec::with_capacity(line.stop_indices.len());
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
        .and_then(|order| {
            order
                .get(temporal_position)
                .copied()
                .map(|value| value as usize)
        })
        .unwrap_or(temporal_position);
    line.trip_indices.get(physical_position).copied()
}

fn empty_segment_way_ids(polyline: &[PolylinePoint]) -> Vec<Option<i64>> {
    vec![None; polyline.len().saturating_sub(1)]
}

fn normalize_segment_way_ids(
    polyline: &[PolylinePoint],
    segment_way_ids: &[Option<i64>],
) -> Vec<Option<i64>> {
    let target_len = polyline.len().saturating_sub(1);
    let mut normalized = segment_way_ids
        .iter()
        .copied()
        .take(target_len)
        .collect::<Vec<_>>();
    if normalized.len() < target_len {
        normalized.resize(target_len, None);
    }
    normalized
}

fn stitch_walk_geometries(geometries: &[WalkGeometry]) -> WalkGeometry {
    let mut stitched = WalkGeometry::default();

    for (index, geometry) in geometries.iter().enumerate() {
        let trimmed = trim_walk_geometry_for_stitch(
            geometry,
            index > 0,
            index + 1 < geometries.len(),
        );
        append_walk_geometry(&mut stitched, &trimmed);
    }

    stitched.segment_way_ids =
        normalize_segment_way_ids(&stitched.polyline, &stitched.segment_way_ids);
    stitched
}

fn trim_walk_geometry_for_stitch(
    geometry: &WalkGeometry,
    trim_head: bool,
    trim_tail: bool,
) -> WalkGeometry {
    let mut polyline = geometry.polyline.clone();
    let mut segment_way_ids = normalize_segment_way_ids(&polyline, &geometry.segment_way_ids);

    if trim_head && polyline.len() > 1 {
        polyline.remove(0);
        if !segment_way_ids.is_empty() {
            segment_way_ids.remove(0);
        }
    }
    if trim_tail && polyline.len() > 1 {
        polyline.pop();
        segment_way_ids.pop();
    }

    WalkGeometry {
        polyline,
        segment_way_ids,
    }
}

fn append_walk_geometry(target: &mut WalkGeometry, next: &WalkGeometry) {
    if next.polyline.is_empty() {
        return;
    }

    let next_segment_way_ids = normalize_segment_way_ids(&next.polyline, &next.segment_way_ids);
    if target.polyline.is_empty() {
        target.polyline = next.polyline.clone();
        target.segment_way_ids = next_segment_way_ids;
        return;
    }

    if points_equal(
        target.polyline.last().expect("target polyline must be non-empty"),
        &next.polyline[0],
    ) {
        target.polyline.extend(next.polyline.iter().skip(1).cloned());
        target.segment_way_ids.extend(next_segment_way_ids);
        return;
    }

    target.segment_way_ids.push(None);
    target.polyline.extend(next.polyline.iter().cloned());
    target.segment_way_ids.extend(next_segment_way_ids);
}

fn build_walk_directions(
    geometry: &WalkGeometry,
    way_names: &HashMap<i64, String>,
    destination_name: &str,
) -> Vec<WalkDirection> {
    build_turn_directions(
        geometry,
        way_names,
        destination_name,
        TurnDirectionNarrative::Walk,
    )
}

fn build_road_directions(
    geometry: &WalkGeometry,
    way_names: &HashMap<i64, String>,
    destination_name: &str,
) -> Vec<WalkDirection> {
    build_turn_directions(
        geometry,
        way_names,
        destination_name,
        TurnDirectionNarrative::Drive,
    )
}

#[derive(Clone, Copy)]
enum TurnDirectionNarrative {
    Walk,
    Drive,
}

fn build_turn_directions(
    geometry: &WalkGeometry,
    way_names: &HashMap<i64, String>,
    destination_name: &str,
    narrative: TurnDirectionNarrative,
) -> Vec<WalkDirection> {
    if geometry.polyline.len() < 2 {
        return Vec::new();
    }

    let segment_way_ids = normalize_segment_way_ids(&geometry.polyline, &geometry.segment_way_ids);
    let cumulative_distances = cumulative_polyline_distances(&geometry.polyline);
    let start_street_name = segment_way_ids
        .iter()
        .find_map(|way_id| road_label_for_way(*way_id, way_names))
        .map(str::to_owned);

    let mut directions = vec![WalkDirection {
        maneuver: "depart",
        instruction: departure_instruction(narrative, start_street_name.as_deref()),
        street_name: start_street_name,
        distance_meters: 0.0,
        lat: geometry.polyline[0].lat,
        lon: geometry.polyline[0].lon,
    }];
    let mut last_instruction_distance = 0.0;

    for pivot in 1..geometry.polyline.len().saturating_sub(1) {
        let previous_way_id = segment_way_ids[pivot - 1];
        let next_way_id = segment_way_ids[pivot];
        if previous_way_id.is_none()
            || next_way_id.is_none()
            || same_road_identity(previous_way_id, next_way_id, way_names)
        {
            continue;
        }

        let maneuver = classify_walk_turn(
            &geometry.polyline[pivot - 1],
            &geometry.polyline[pivot],
            &geometry.polyline[pivot + 1],
        );
        let street_name = road_label_for_way(next_way_id, way_names).map(str::to_owned);
        let instruction_distance = cumulative_distances[pivot];
        let distance_since_last_instruction = instruction_distance - last_instruction_distance;
        if maneuver == "continue" && street_name.is_none() {
            continue;
        }
        if street_name.is_none() && distance_since_last_instruction < WALK_TURN_MIN_SPACING_METERS {
            continue;
        }
        let instruction = match maneuver {
            "turn-left" => match street_name.as_ref() {
                Some(street_name) => format!("Svolta a sinistra su {street_name}"),
                None => "Svolta a sinistra".to_owned(),
            },
            "turn-right" => match street_name.as_ref() {
                Some(street_name) => format!("Svolta a destra su {street_name}"),
                None => "Svolta a destra".to_owned(),
            },
            _ => match street_name.as_ref() {
                Some(street_name) => format!("Continua dritto su {street_name}"),
                None => "Continua dritto".to_owned(),
            },
        };

        directions.push(WalkDirection {
            maneuver,
            instruction,
            street_name,
            distance_meters: instruction_distance,
            lat: geometry.polyline[pivot].lat,
            lon: geometry.polyline[pivot].lon,
        });
        last_instruction_distance = instruction_distance;
    }

    let destination_label = destination_name.trim();
    directions.push(WalkDirection {
        maneuver: "arrive",
        instruction: if destination_label.is_empty() {
            "Arrivo a destinazione".to_owned()
        } else {
            format!("Arrivo a {destination_label}")
        },
        street_name: None,
        distance_meters: *cumulative_distances.last().unwrap_or(&0.0),
        lat: geometry.polyline.last().map(|point| point.lat).unwrap_or(0.0),
        lon: geometry.polyline.last().map(|point| point.lon).unwrap_or(0.0),
    });

    directions
}

fn departure_instruction(
    narrative: TurnDirectionNarrative,
    street_name: Option<&str>,
) -> String {
    match (narrative, street_name) {
        (TurnDirectionNarrative::Walk, Some(street_name)) => {
            format!("Parti a piedi su {street_name}")
        }
        (TurnDirectionNarrative::Walk, None) => "Parti a piedi".to_owned(),
        (TurnDirectionNarrative::Drive, Some(street_name)) => {
            format!("Parti in auto su {street_name}")
        }
        (TurnDirectionNarrative::Drive, None) => "Parti in auto".to_owned(),
    }
}

fn cumulative_polyline_distances(polyline: &[PolylinePoint]) -> Vec<f64> {
    let mut cumulative = Vec::with_capacity(polyline.len());
    let mut total = 0.0;
    cumulative.push(0.0);

    for window in polyline.windows(2) {
        total += haversine_meters(window[0].lat, window[0].lon, window[1].lat, window[1].lon);
        cumulative.push(total);
    }

    cumulative
}

fn classify_walk_turn(
    from: &PolylinePoint,
    pivot: &PolylinePoint,
    to: &PolylinePoint,
) -> &'static str {
    let longitude_scale = pivot.lat.to_radians().cos();
    let v1x = (pivot.lon - from.lon) * longitude_scale;
    let v1y = pivot.lat - from.lat;
    let v2x = (to.lon - pivot.lon) * longitude_scale;
    let v2y = to.lat - pivot.lat;

    if (v1x.abs() + v1y.abs()) <= f64::EPSILON || (v2x.abs() + v2y.abs()) <= f64::EPSILON {
        return "continue";
    }

    let cross = (v1x * v2y) - (v1y * v2x);
    let dot = (v1x * v2x) + (v1y * v2y);
    let angle_degrees = cross.abs().atan2(dot).to_degrees();

    if angle_degrees < WALK_TURN_STRAIGHT_ANGLE_DEGREES {
        "continue"
    } else if cross > 0.0 {
        "turn-left"
    } else {
        "turn-right"
    }
}

fn road_label_for_way<'a>(
    way_id: Option<i64>,
    way_names: &'a HashMap<i64, String>,
) -> Option<&'a str> {
    way_id
        .and_then(|way_id| way_names.get(&way_id))
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn same_road_identity(
    previous_way_id: Option<i64>,
    next_way_id: Option<i64>,
    way_names: &HashMap<i64, String>,
) -> bool {
    match (previous_way_id, next_way_id) {
        (Some(previous), Some(next)) if previous == next => true,
        (Some(previous), Some(next)) => match (way_names.get(&previous), way_names.get(&next)) {
            (Some(previous_name), Some(next_name)) => {
                previous_name.trim().eq_ignore_ascii_case(next_name.trim())
            }
            _ => false,
        },
        (None, None) => true,
        _ => false,
    }
}

fn points_equal(left: &PolylinePoint, right: &PolylinePoint) -> bool {
    (left.lat - right.lat).abs() <= 1e-7 && (left.lon - right.lon).abs() <= 1e-7
}

fn validate_query_latitude(label: &str, value: Option<f64>) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow!("missing {label}"))?;
    if !value.is_finite() || !(-90.0..=90.0).contains(&value) {
        bail!("invalid {label}");
    }
    Ok(value)
}

fn validate_query_longitude(label: &str, value: Option<f64>) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow!("missing {label}"))?;
    if !value.is_finite() || !(-180.0..=180.0).contains(&value) {
        bail!("invalid {label}");
    }
    Ok(value)
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

#[derive(Clone, Copy)]
struct TransferTileBuildCell {
    source_offset: usize,
    target_stop: usize,
    transfer_slot: u16,
    duration_secs: i32,
}

fn build_transfer_relax_index(
    stops: &[StopRecord],
    transfers: &[Vec<WalkTransfer>],
    hub_cell_meters: f64,
) -> TransferRelaxIndex {
    if stops.is_empty() {
        return TransferRelaxIndex {
            stop_to_hub: Vec::new(),
            stop_to_hub_offset: Vec::new(),
            hubs: Vec::new(),
        };
    }

    let hub_cell_meters = hub_cell_meters.max(1.0);
    let mut hub_lookup = HashMap::<(u16, i32, i32, usize), usize>::new();
    let mut stop_to_hub = vec![0usize; stops.len()];
    let mut stop_to_hub_offset = vec![0usize; stops.len()];
    let mut hubs = Vec::<TransferHub>::new();

    for (stop_index, stop) in stops.iter().enumerate() {
        let hub_key = transfer_hub_key(stop, stop_index, hub_cell_meters);
        let hub_index = if let Some(hub_index) = hub_lookup.get(&hub_key).copied() {
            hub_index
        } else {
            let hub_index = hubs.len();
            hubs.push(TransferHub {
                stop_indices: Vec::new(),
                outgoing_tiles: Vec::new(),
            });
            hub_lookup.insert(hub_key, hub_index);
            hub_index
        };

        stop_to_hub[stop_index] = hub_index;
        stop_to_hub_offset[stop_index] = hubs[hub_index].stop_indices.len();
        hubs[hub_index].stop_indices.push(stop_index);
    }

    let mut tile_builds = HashMap::<(usize, usize), Vec<TransferTileBuildCell>>::new();
    for (source_stop, outgoing) in transfers.iter().enumerate() {
        if source_stop >= stop_to_hub.len() {
            continue;
        }

        let source_hub = stop_to_hub[source_stop];
        let source_offset = stop_to_hub_offset[source_stop];
        for (transfer_slot, transfer) in outgoing.iter().enumerate() {
            let Ok(transfer_slot) = u16::try_from(transfer_slot) else {
                continue;
            };
            let target_hub = stop_to_hub[transfer.to_stop];
            tile_builds
                .entry((source_hub, target_hub))
                .or_default()
                .push(TransferTileBuildCell {
                    source_offset,
                    target_stop: transfer.to_stop,
                    transfer_slot,
                    duration_secs: transfer.duration_secs,
                });
        }
    }

    for ((source_hub, target_hub), cells) in tile_builds {
        let source_count = hubs[source_hub].stop_indices.len();
        if source_count == 0 {
            continue;
        }

        let mut target_stop_indices = cells
            .iter()
            .map(|cell| cell.target_stop)
            .collect::<Vec<_>>();
        target_stop_indices
            .sort_unstable_by_key(|stop_index| (stop_to_hub_offset[*stop_index], *stop_index));
        target_stop_indices.dedup();
        if target_stop_indices.is_empty() {
            continue;
        }

        let target_count = target_stop_indices.len();
        let mut target_offsets = HashMap::<usize, usize>::with_capacity(target_count);
        for (target_offset, target_stop) in target_stop_indices.iter().copied().enumerate() {
            target_offsets.insert(target_stop, target_offset);
        }

        let mut transfer_slots = vec![EMPTY_TRANSFER_SLOT; source_count * target_count];
        let mut durations = vec![INF_TIME; source_count * target_count];
        for cell in cells {
            let Some(target_offset) = target_offsets.get(&cell.target_stop).copied() else {
                continue;
            };
            let matrix_index = cell.source_offset * target_count + target_offset;
            if cell.duration_secs < durations[matrix_index] {
                durations[matrix_index] = cell.duration_secs;
                transfer_slots[matrix_index] = cell.transfer_slot;
            }
        }

        hubs[source_hub].outgoing_tiles.push(TransferTile {
            target_hub,
            target_stop_indices,
            transfer_slots,
            durations,
        });
    }

    for hub in &mut hubs {
        hub.outgoing_tiles.sort_by_key(|tile| {
            (
                tile.target_hub,
                tile.target_stop_indices
                    .first()
                    .copied()
                    .unwrap_or(usize::MAX),
            )
        });
    }

    TransferRelaxIndex {
        stop_to_hub,
        stop_to_hub_offset,
        hubs,
    }
}

fn transfer_hub_key(
    stop: &StopRecord,
    stop_index: usize,
    hub_cell_meters: f64,
) -> (u16, i32, i32, usize) {
    match (stop.latitude, stop.longitude) {
        (Some(lat), Some(lon)) => {
            let lat_bucket = ((lat * 111_320.0) / hub_cell_meters).floor() as i32;
            let lon_scale = lat.to_radians().cos().abs().max(0.25);
            let lon_bucket = ((lon * 111_320.0 * lon_scale) / hub_cell_meters).floor() as i32;
            (stop.feed_index, lat_bucket, lon_bucket, usize::MAX)
        }
        _ => (stop.feed_index, 0, 0, stop_index),
    }
}

fn scheduled_departure_chunk(trip: &TripRecord, start_pos: usize) -> [i32; SVRT_WIDTH] {
    let mut departures = [INF_TIME; SVRT_WIDTH];
    for (lane, stop_time) in trip.stop_times[start_pos..start_pos + SVRT_WIDTH]
        .iter()
        .enumerate()
    {
        departures[lane] = stop_time.departure_secs;
    }
    departures
}

fn svrt_chunk_has_catchup_candidate(
    stop_indices: &[usize],
    best_before_round: &[i32],
    departure_secs: &[i32],
) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if stop_indices.len() == SVRT_WIDTH
            && departure_secs.len() == SVRT_WIDTH
            && std::is_x86_feature_detected!("avx2")
        {
            let mut lane_indices = [0i32; SVRT_WIDTH];
            for (lane, stop_index) in stop_indices.iter().copied().enumerate() {
                lane_indices[lane] = stop_index as i32;
            }

            let mut lane_departures = [0i32; SVRT_WIDTH];
            lane_departures.copy_from_slice(&departure_secs[..SVRT_WIDTH]);

            return unsafe {
                svrt_chunk_has_catchup_candidate_avx2(
                    &lane_indices,
                    best_before_round.as_ptr(),
                    &lane_departures,
                )
            };
        }
    }

    svrt_chunk_has_catchup_candidate_scalar(stop_indices, best_before_round, departure_secs)
}

fn svrt_chunk_has_catchup_candidate_scalar(
    stop_indices: &[usize],
    best_before_round: &[i32],
    departure_secs: &[i32],
) -> bool {
    stop_indices
        .iter()
        .zip(departure_secs.iter().copied())
        .any(|(stop_index, departure)| {
            best_before_round
                .get(*stop_index)
                .copied()
                .unwrap_or(INF_TIME)
                < departure
        })
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn svrt_chunk_has_catchup_candidate_avx2(
    lane_indices: &[i32; SVRT_WIDTH],
    best_before_ptr: *const i32,
    lane_departures: &[i32; SVRT_WIDTH],
) -> bool {
    use std::arch::x86_64::{
        __m256i, _mm256_castsi256_ps, _mm256_cmpgt_epi32, _mm256_i32gather_epi32,
        _mm256_loadu_si256, _mm256_movemask_ps,
    };

    let indices = unsafe { _mm256_loadu_si256(lane_indices.as_ptr() as *const __m256i) };
    let ready = unsafe { _mm256_i32gather_epi32(best_before_ptr, indices, 4) };
    let departures = unsafe { _mm256_loadu_si256(lane_departures.as_ptr() as *const __m256i) };
    let cmp = _mm256_cmpgt_epi32(departures, ready);
    _mm256_movemask_ps(_mm256_castsi256_ps(cmp)) != 0
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
        way_names: HashMap::new(),
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
        .chain(
            next_active_stop_indices
                .iter()
                .map(|index| &next_stops[*index]),
        )
    {
        if !changed_stop_ids.contains(&source.id) {
            continue;
        }
        let (Some(latitude), Some(longitude)) = (source.latitude, source.longitude) else {
            continue;
        };

        let lat_delta = walk_radius_meters / 111_320.0;
        let lon_delta =
            walk_radius_meters / (111_320.0 * latitude.to_radians().cos().abs().max(0.25));
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
    if let Some(short_name) = route
        .short_name
        .as_deref()
        .filter(|value| !value.is_empty())
    {
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
            format!(
                "unable to create static cache directory {}",
                parent.display()
            )
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
                let polyline = straight_polyline(origin, candidate_stop);
                neighbours.push(WalkTransfer {
                    to_stop: candidate.index,
                    duration_secs: (distance / walk_speed_mps).ceil() as i32,
                    distance_meters: distance,
                    segment_way_ids: empty_segment_way_ids(&polyline),
                    polyline,
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

    fn raw_leg_signature(raw_legs: &[RawLeg]) -> String {
        raw_legs
            .iter()
            .map(|leg| match leg {
                RawLeg::Walk {
                    from_stop,
                    to_stop,
                    departure_secs,
                    arrival_secs,
                    ..
                } => format!(
                    "walk:{from_stop}:{to_stop}:{departure_secs}:{arrival_secs}"
                ),
                RawLeg::Transit {
                    trip_index,
                    board_stop,
                    alight_stop,
                    departure_secs,
                    arrival_secs,
                    ..
                } => format!(
                    "transit:{trip_index}:{board_stop}:{alight_stop}:{departure_secs}:{arrival_secs}"
                ),
            })
            .collect::<Vec<_>>()
            .join("|")
    }

    fn build_itinerary_label(
        index: usize,
        is_recommended: bool,
        is_fastest: bool,
        is_fewest_transfers: bool,
        is_best_realtime: bool,
        is_least_crowded: bool,
    ) -> String {
        if is_fastest {
            "Piu veloce".to_owned()
        } else if is_best_realtime {
            "Migliori dati RT".to_owned()
        } else if is_least_crowded {
            "Meno affollato".to_owned()
        } else if is_fewest_transfers {
            "Meno cambi".to_owned()
        } else if is_recommended {
            "Consigliato".to_owned()
        } else {
            format!("Alternativa {}", index + 1)
        }
    }

    fn build_itinerary_badges(
        index: usize,
        is_recommended: bool,
        is_fastest: bool,
        is_fewest_transfers: bool,
        is_best_realtime: bool,
        is_least_crowded: bool,
        has_canceled_legs: bool,
    ) -> Vec<String> {
        let mut badges = Vec::new();
        if is_recommended {
            badges.push("Consigliato".to_owned());
        }
        if is_fastest {
            badges.push("Piu veloce".to_owned());
        }
        if is_fewest_transfers {
            badges.push("Meno cambi".to_owned());
        }
        if is_best_realtime {
            badges.push("Migliori dati RT".to_owned());
        }
        if is_least_crowded {
            badges.push("Meno affollato".to_owned());
        }
        if has_canceled_legs {
            badges.push("Contiene CANCELED".to_owned());
        }
        if badges.is_empty() {
            badges.push(format!("Alternativa {}", index + 1));
        }
        badges
    }

fn compare_optional_crowding(left: Option<u16>, right: Option<u16>) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn score_itinerary_passive_metrics(legs: &[LegResponse]) -> ItineraryPassiveMetrics {
    let mut transit_leg_count = 0usize;
    let mut transit_legs_with_gtfs_rt = 0usize;
    let mut occupancy_covered_transit_legs = 0usize;
    let mut crowding_score_total = 0u16;
    let mut canceled_transit_legs = 0usize;

    for leg in legs {
        if leg.kind != "transit" {
            continue;
        }

        transit_leg_count += 1;
        if leg.has_gtfs_rt {
            transit_legs_with_gtfs_rt += 1;
        }
        if let Some(score) = leg.occupancy_score {
            occupancy_covered_transit_legs += 1;
            crowding_score_total = crowding_score_total.saturating_add(score);
        }
        if leg.schedule_relationship.as_deref() == Some("CANCELED") {
            canceled_transit_legs += 1;
        }
    }

    let crowding_score = (occupancy_covered_transit_legs > 0)
        .then(|| crowding_score_total / occupancy_covered_transit_legs as u16);
    let crowding_level = passive_occupancy_level(crowding_score, occupancy_covered_transit_legs);

    ItineraryPassiveMetrics {
        transit_leg_count,
        transit_legs_with_gtfs_rt,
        occupancy_covered_transit_legs,
        crowding_score,
        crowding_level,
        canceled_transit_legs,
        has_canceled_legs: canceled_transit_legs > 0,
    }
}

fn passive_occupancy_level(score: Option<u16>, covered_legs: usize) -> &'static str {
    let Some(score) = score else {
        return "unknown";
    };
    if covered_legs == 0 {
        return "unknown";
    }

    if score <= 35 {
        "low"
    } else if score <= 70 {
        "medium"
    } else {
        "high"
    }
}

fn passive_occupancy_score(
    occupancy_status: Option<i32>,
    occupancy_percentage: Option<u32>,
) -> Option<u16> {
    if let Some(occupancy_percentage) = occupancy_percentage {
        return Some(occupancy_percentage.min(150) as u16);
    }

    occupancy_status.and_then(|value| match vehicle_position::OccupancyStatus::from_i32(value) {
        Some(vehicle_position::OccupancyStatus::Empty) => Some(0),
        Some(vehicle_position::OccupancyStatus::ManySeatsAvailable) => Some(20),
        Some(vehicle_position::OccupancyStatus::FewSeatsAvailable) => Some(45),
        Some(vehicle_position::OccupancyStatus::StandingRoomOnly) => Some(75),
        Some(vehicle_position::OccupancyStatus::CrushedStandingRoomOnly) => Some(90),
        Some(vehicle_position::OccupancyStatus::Full) => Some(100),
        Some(vehicle_position::OccupancyStatus::NotAcceptingPassengers) => Some(110),
        Some(vehicle_position::OccupancyStatus::NotBoardable) => Some(120),
        Some(vehicle_position::OccupancyStatus::NoDataAvailable) => None,
        None => None,
    })
}

fn schedule_relationship_string(value: Option<i32>) -> Option<String> {
    value.and_then(|value| {
        trip_descriptor::ScheduleRelationship::from_i32(value)
            .map(|relationship| relationship.as_str_name().to_owned())
    })
}

fn occupancy_status_string(value: Option<i32>) -> Option<String> {
    value.and_then(|value| {
        vehicle_position::OccupancyStatus::from_i32(value)
            .map(|status| status.as_str_name().to_owned())
    })
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

fn intermediate_trip_stop_times(
    trip: &TripRecord,
    board_pos: usize,
    alight_pos: usize,
) -> &[TripStopRecord] {
    let start_pos = board_pos.saturating_add(1).min(trip.stop_times.len());
    let end_pos = alight_pos.min(trip.stop_times.len());
    if start_pos >= end_pos {
        return &trip.stop_times[0..0];
    }

    &trip.stop_times[start_pos..end_pos]
}

fn shape_polyline_segment(
    shape: &[ShapePoint],
    stops: &[StopRecord],
    trip: &TripRecord,
    board_pos: usize,
    alight_pos: usize,
) -> Option<Vec<PolylinePoint>> {
    shape_polyline_by_distance(
        shape,
        trip.stop_times.get(board_pos)?.shape_dist_traveled,
        trip.stop_times.get(alight_pos)?.shape_dist_traveled,
    )
    .or_else(|| shape_polyline_by_stop_projection(shape, stops, trip, board_pos, alight_pos))
}

fn shape_polyline_by_distance(
    shape: &[ShapePoint],
    start_dist: Option<f32>,
    end_dist: Option<f32>,
) -> Option<Vec<PolylinePoint>> {
    let (Some(start_dist), Some(end_dist)) = (start_dist, end_dist) else {
        return None;
    };
    if end_dist < start_dist {
        return None;
    }

    let points: Vec<_> = shape
        .iter()
        .filter(|point| {
            point
                .dist_traveled
                .is_some_and(|distance| distance >= start_dist && distance <= end_dist)
        })
        .map(|point| PolylinePoint {
            lat: point.lat,
            lon: point.lon,
        })
        .collect();
    (points.len() >= 2).then_some(points)
}

fn shape_polyline_by_stop_projection(
    shape: &[ShapePoint],
    stops: &[StopRecord],
    trip: &TripRecord,
    board_pos: usize,
    alight_pos: usize,
) -> Option<Vec<PolylinePoint>> {
    if shape.len() < 2 || board_pos > alight_pos {
        return None;
    }

    let projected_indices = trip.shape_stop_point_indices.as_ref()?;
    let start_index = usize::try_from(*projected_indices.get(board_pos)?).ok()?;
    let end_index = usize::try_from(*projected_indices.get(alight_pos)?).ok()?;
    if end_index <= start_index {
        return None;
    }

    let mut polyline = Vec::with_capacity(end_index - start_index + 3);
    push_stop_polyline_point(
        &mut polyline,
        stops.get(trip.stop_times.get(board_pos)?.stop_index)?,
    );
    for point in &shape[start_index..=end_index] {
        push_polyline_point(&mut polyline, point.lat, point.lon);
    }
    push_stop_polyline_point(
        &mut polyline,
        stops.get(trip.stop_times.get(alight_pos)?.stop_index)?,
    );

    (polyline.len() >= 2).then_some(polyline)
}

fn build_shape_stop_point_indices(
    shape: &[ShapePoint],
    stops: &[StopRecord],
    stop_times: &[TripStopRecord],
) -> Option<Vec<u32>> {
    if shape.is_empty() {
        return None;
    }

    let mut projected = Vec::with_capacity(stop_times.len());
    let mut search_start = 0usize;
    for stop_time in stop_times {
        let stop = stops.get(stop_time.stop_index)?;
        let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) else {
            return None;
        };
        let index = nearest_shape_point_index(shape, lat, lon, search_start)?;
        projected.push(u32::try_from(index).ok()?);
        search_start = index;
    }

    Some(projected)
}

fn nearest_shape_point_index(
    shape: &[ShapePoint],
    latitude: f64,
    longitude: f64,
    start_index: usize,
) -> Option<usize> {
    let first_point = shape.get(start_index)?;
    let mut best_index = start_index;
    let mut best_distance = haversine_meters(latitude, longitude, first_point.lat, first_point.lon);

    for (offset, point) in shape[start_index + 1..].iter().enumerate() {
        let index = start_index + offset + 1;
        let distance = haversine_meters(latitude, longitude, point.lat, point.lon);
        if distance < best_distance {
            best_distance = distance;
            best_index = index;
        }
    }

    Some(best_index)
}

fn push_stop_polyline_point(polyline: &mut Vec<PolylinePoint>, stop: &StopRecord) {
    if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
        push_polyline_point(polyline, lat, lon);
    }
}

fn push_polyline_point(polyline: &mut Vec<PolylinePoint>, lat: f64, lon: f64) {
    let should_push = polyline.last().is_none_or(|point| {
        point.lat.to_bits() != lat.to_bits() || point.lon.to_bits() != lon.to_bits()
    });
    if should_push {
        polyline.push(PolylinePoint { lat, lon });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        CHRONOS_BUCKET_SECS, LineRecord, PolylinePoint, RemoteStaticGtfsVersionMetadata,
        ShapePoint, StopRecord, TripRecord, TripStopRecord, WalkGeometry, WalkTransfer,
        build_chronos_bucket_start_indices, build_shape_stop_point_indices,
        build_transfer_relax_index, build_walk_directions, finalize_line_temporal_indices,
        intermediate_trip_stop_times, remote_static_gtfs_version_changed,
        shape_polyline_segment, stitch_walk_geometries,
        svrt_chunk_has_catchup_candidate_scalar,
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
                shape_stop_point_indices: None,
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
                shape_stop_point_indices: None,
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
        assert_eq!(
            lines[0].trip_order_indirection_by_stop[0],
            Vec::<u32>::new()
        );
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

    #[test]
    fn transfer_relax_index_tiles_edges_by_hub_pair() {
        let stops = vec![
            StopRecord {
                global_id: 1,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "a".to_owned(),
                id: "feed:a".to_owned(),
                code: None,
                name: "A".to_owned(),
                latitude: Some(41.9000),
                longitude: Some(12.5000),
                search_blob: String::new(),
            },
            StopRecord {
                global_id: 2,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "b".to_owned(),
                id: "feed:b".to_owned(),
                code: None,
                name: "B".to_owned(),
                latitude: Some(41.9001),
                longitude: Some(12.5001),
                search_blob: String::new(),
            },
            StopRecord {
                global_id: 3,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "c".to_owned(),
                id: "feed:c".to_owned(),
                code: None,
                name: "C".to_owned(),
                latitude: Some(41.9020),
                longitude: Some(12.5020),
                search_blob: String::new(),
            },
            StopRecord {
                global_id: 4,
                feed_index: 0,
                feed_id: "feed".to_owned(),
                local_id: "d".to_owned(),
                id: "feed:d".to_owned(),
                code: None,
                name: "D".to_owned(),
                latitude: Some(41.9021),
                longitude: Some(12.5021),
                search_blob: String::new(),
            },
        ];
        let transfers = vec![
            vec![WalkTransfer {
                to_stop: 2,
                duration_secs: 90,
                distance_meters: 120.0,
                polyline: Vec::new(),
                segment_way_ids: Vec::new(),
            }],
            vec![WalkTransfer {
                to_stop: 3,
                duration_secs: 60,
                distance_meters: 80.0,
                polyline: Vec::new(),
                segment_way_ids: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
        ];

        let index = build_transfer_relax_index(&stops, &transfers, 180.0);

        assert_eq!(index.hubs.len(), 2);
        assert_eq!(index.stop_to_hub[0], index.stop_to_hub[1]);
        assert_eq!(index.stop_to_hub[2], index.stop_to_hub[3]);

        let source_hub = index.stop_to_hub[0];
        let tile = &index.hubs[source_hub].outgoing_tiles[0];
        assert_eq!(tile.target_stop_indices, vec![2, 3]);
        assert_eq!(tile.transfer_slots.len(), 4);
        assert_eq!(tile.transfer_slots[0], 0);
        assert_eq!(tile.transfer_slots[3], 0);
    }

    #[test]
    fn svrt_scalar_mask_detects_when_catchup_is_possible() {
        let stop_indices = vec![1usize, 3, 5, 7];
        let mut best_before_round = vec![i32::MAX; 8];
        best_before_round[1] = 320;
        best_before_round[3] = 410;
        best_before_round[5] = 600;
        best_before_round[7] = 720;

        assert!(svrt_chunk_has_catchup_candidate_scalar(
            &stop_indices,
            &best_before_round,
            &[300, 420, 590, 700],
        ));
        assert!(!svrt_chunk_has_catchup_candidate_scalar(
            &stop_indices,
            &best_before_round,
            &[300, 400, 590, 700],
        ));
    }

    #[test]
    fn shape_polyline_segment_prefers_shape_distances_when_present() {
        let stops = vec![test_stop(0, "a", 41.0, 12.0), test_stop(1, "b", 41.0, 12.2)];
        let trip = TripRecord {
            global_id: 1,
            feed_index: 0,
            feed_id: "feed".to_owned(),
            local_id: "trip-1".to_owned(),
            id: "feed:trip-1".to_owned(),
            route_index: 0,
            shape_id: Some("feed:shape-1".to_owned()),
            shape_stop_point_indices: None,
            headsign: None,
            stop_times: vec![
                TripStopRecord {
                    stop_index: 0,
                    arrival_secs: 0,
                    departure_secs: 0,
                    stop_sequence: 1,
                    shape_dist_traveled: Some(10.0),
                },
                TripStopRecord {
                    stop_index: 1,
                    arrival_secs: 60,
                    departure_secs: 60,
                    stop_sequence: 2,
                    shape_dist_traveled: Some(20.0),
                },
            ],
        };
        let shape = vec![
            ShapePoint {
                lat: 40.0,
                lon: 10.0,
                dist_traveled: Some(0.0),
            },
            ShapePoint {
                lat: 40.1,
                lon: 10.1,
                dist_traveled: Some(10.0),
            },
            ShapePoint {
                lat: 40.2,
                lon: 10.2,
                dist_traveled: Some(20.0),
            },
            ShapePoint {
                lat: 40.3,
                lon: 10.3,
                dist_traveled: Some(30.0),
            },
        ];

        let polyline = shape_polyline_segment(&shape, &stops, &trip, 0, 1).expect("shape polyline");

        assert_eq!(polyline.len(), 2);
        assert_eq!(polyline[0].lat, 40.1);
        assert_eq!(polyline[1].lat, 40.2);
    }

    #[test]
    fn build_shape_stop_point_indices_projects_stops_in_order() {
        let stops = vec![
            test_stop(0, "a", 41.0000, 12.0000),
            test_stop(1, "b", 41.0000, 12.0101),
            test_stop(2, "c", 41.0101, 12.0201),
        ];
        let stop_times = vec![
            TripStopRecord {
                stop_index: 0,
                arrival_secs: 0,
                departure_secs: 0,
                stop_sequence: 1,
                shape_dist_traveled: None,
            },
            TripStopRecord {
                stop_index: 1,
                arrival_secs: 60,
                departure_secs: 60,
                stop_sequence: 2,
                shape_dist_traveled: None,
            },
            TripStopRecord {
                stop_index: 2,
                arrival_secs: 120,
                departure_secs: 120,
                stop_sequence: 3,
                shape_dist_traveled: None,
            },
        ];
        let shape = vec![
            ShapePoint {
                lat: 41.0000,
                lon: 12.0000,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0000,
                lon: 12.0050,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0000,
                lon: 12.0100,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0050,
                lon: 12.0150,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0100,
                lon: 12.0200,
                dist_traveled: None,
            },
        ];

        let indices =
            build_shape_stop_point_indices(&shape, &stops, &stop_times).expect("shape indices");

        assert_eq!(indices, vec![0, 2, 4]);
    }

    #[test]
    fn shape_polyline_segment_projects_stops_when_shape_distances_are_missing() {
        let stops = vec![
            test_stop(0, "a", 41.0000, 12.0000),
            test_stop(1, "b", 41.0000, 12.0101),
            test_stop(2, "c", 41.0101, 12.0201),
        ];
        let trip = TripRecord {
            global_id: 1,
            feed_index: 0,
            feed_id: "feed".to_owned(),
            local_id: "trip-2".to_owned(),
            id: "feed:trip-2".to_owned(),
            route_index: 0,
            shape_id: Some("feed:shape-2".to_owned()),
            shape_stop_point_indices: Some(vec![0, 2, 4]),
            headsign: None,
            stop_times: vec![
                TripStopRecord {
                    stop_index: 0,
                    arrival_secs: 0,
                    departure_secs: 0,
                    stop_sequence: 1,
                    shape_dist_traveled: None,
                },
                TripStopRecord {
                    stop_index: 1,
                    arrival_secs: 60,
                    departure_secs: 60,
                    stop_sequence: 2,
                    shape_dist_traveled: None,
                },
                TripStopRecord {
                    stop_index: 2,
                    arrival_secs: 120,
                    departure_secs: 120,
                    stop_sequence: 3,
                    shape_dist_traveled: None,
                },
            ],
        };
        let shape = vec![
            ShapePoint {
                lat: 41.0000,
                lon: 12.0000,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0000,
                lon: 12.0050,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0000,
                lon: 12.0100,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0050,
                lon: 12.0150,
                dist_traveled: None,
            },
            ShapePoint {
                lat: 41.0100,
                lon: 12.0200,
                dist_traveled: None,
            },
        ];

        let polyline = shape_polyline_segment(&shape, &stops, &trip, 0, 2).expect("shape polyline");

        assert!(polyline.len() >= 5);
        assert_eq!(polyline.first().unwrap().lat, 41.0000);
        assert!(polyline.iter().any(|point| point.lon == 12.0050));
        assert!(polyline.iter().any(|point| point.lon == 12.0150));
        assert_eq!(polyline.last().unwrap().lat, 41.0101);
    }

    #[test]
    fn intermediate_trip_stop_times_return_only_stops_between_board_and_alight() {
        let trip = TripRecord {
            global_id: 1,
            feed_index: 0,
            feed_id: "feed".to_owned(),
            local_id: "trip-3".to_owned(),
            id: "feed:trip-3".to_owned(),
            route_index: 0,
            shape_id: None,
            shape_stop_point_indices: None,
            headsign: None,
            stop_times: vec![
                TripStopRecord {
                    stop_index: 10,
                    arrival_secs: 0,
                    departure_secs: 0,
                    stop_sequence: 1,
                    shape_dist_traveled: None,
                },
                TripStopRecord {
                    stop_index: 11,
                    arrival_secs: 60,
                    departure_secs: 60,
                    stop_sequence: 2,
                    shape_dist_traveled: None,
                },
                TripStopRecord {
                    stop_index: 12,
                    arrival_secs: 120,
                    departure_secs: 120,
                    stop_sequence: 3,
                    shape_dist_traveled: None,
                },
                TripStopRecord {
                    stop_index: 13,
                    arrival_secs: 180,
                    departure_secs: 180,
                    stop_sequence: 4,
                    shape_dist_traveled: None,
                },
            ],
        };

        let intermediate = intermediate_trip_stop_times(&trip, 0, 3)
            .iter()
            .map(|stop_time| stop_time.stop_index)
            .collect::<Vec<_>>();

        assert_eq!(intermediate, vec![11, 12]);
        assert!(intermediate_trip_stop_times(&trip, 1, 2).is_empty());
    }

    #[test]
    fn stitch_walk_geometries_removes_intermediate_stop_spike() {
        let stitched = stitch_walk_geometries(&[
            WalkGeometry {
                polyline: vec![
                    test_point(41.9000, 12.5000),
                    test_point(41.9000, 12.5005),
                    test_point(41.9000, 12.5010),
                ],
                segment_way_ids: vec![Some(10), None],
            },
            WalkGeometry {
                polyline: vec![
                    test_point(41.9000, 12.5010),
                    test_point(41.9000, 12.5005),
                    test_point(41.9005, 12.5005),
                ],
                segment_way_ids: vec![None, Some(20)],
            },
        ]);

        assert_eq!(stitched.polyline.len(), 3);
        assert_eq!(stitched.polyline[0].lon, 12.5000);
        assert_eq!(stitched.polyline[1].lon, 12.5005);
        assert_eq!(stitched.polyline[2].lat, 41.9005);
        assert_eq!(stitched.segment_way_ids, vec![Some(10), Some(20)]);
    }

    #[test]
    fn build_walk_directions_emits_left_turn_for_named_road_change() {
        let mut way_names = HashMap::new();
        way_names.insert(10, "Via Lambrate".to_owned());
        way_names.insert(20, "Via D'Annunzio".to_owned());

        let directions = build_walk_directions(
            &WalkGeometry {
                polyline: vec![
                    test_point(41.9000, 12.5000),
                    test_point(41.9000, 12.5010),
                    test_point(41.9010, 12.5010),
                ],
                segment_way_ids: vec![Some(10), Some(20)],
            },
            &way_names,
            "Destinazione",
        );

        assert_eq!(directions.len(), 3);
        assert_eq!(directions[1].maneuver, "turn-left");
        assert!(directions[1].instruction.contains("Via D'Annunzio"));
    }

    #[test]
    fn build_walk_directions_ignores_same_name_way_splits() {
        let mut way_names = HashMap::new();
        way_names.insert(10, "Via Lambrate".to_owned());
        way_names.insert(11, "Via Lambrate".to_owned());

        let directions = build_walk_directions(
            &WalkGeometry {
                polyline: vec![
                    test_point(41.9000, 12.5000),
                    test_point(41.9000, 12.5010),
                    test_point(41.9010, 12.5010),
                ],
                segment_way_ids: vec![Some(10), Some(11)],
            },
            &way_names,
            "Destinazione",
        );

        assert_eq!(directions.len(), 2);
        assert_eq!(directions[0].maneuver, "depart");
        assert_eq!(directions[1].maneuver, "arrive");
    }

    #[test]
    fn build_walk_directions_suppresses_tight_unnamed_micro_turns() {
        let directions = build_walk_directions(
            &WalkGeometry {
                polyline: vec![
                    test_point(41.9395, 12.5283),
                    test_point(41.9391, 12.5281),
                    test_point(41.93887, 12.52788),
                    test_point(41.93885, 12.52791),
                    test_point(41.93884, 12.52792),
                    test_point(41.93900, 12.52805),
                ],
                segment_way_ids: vec![Some(10), Some(11), Some(12), Some(13), Some(14)],
            },
            &HashMap::new(),
            "Destinazione",
        );

        assert_eq!(directions.len(), 3);
        assert_eq!(directions[0].maneuver, "depart");
        assert_eq!(directions[1].maneuver, "turn-left");
        assert_eq!(directions[2].maneuver, "arrive");
    }

    #[test]
    fn build_road_directions_uses_driving_departure_copy() {
        let mut way_names = HashMap::new();
        way_names.insert(10, "Tangenziale Est".to_owned());

        let directions = super::build_road_directions(
            &WalkGeometry {
                polyline: vec![
                    test_point(41.9000, 12.5000),
                    test_point(41.9000, 12.5010),
                ],
                segment_way_ids: vec![Some(10)],
            },
            &way_names,
            "",
        );

        assert_eq!(directions[0].maneuver, "depart");
        assert!(directions[0].instruction.contains("Parti in auto"));
    }

    fn test_stop(global_id: u64, local_id: &str, latitude: f64, longitude: f64) -> StopRecord {
        StopRecord {
            global_id,
            feed_index: 0,
            feed_id: "feed".to_owned(),
            local_id: local_id.to_owned(),
            id: format!("feed:{local_id}"),
            code: None,
            name: local_id.to_owned(),
            latitude: Some(latitude),
            longitude: Some(longitude),
            search_blob: String::new(),
        }
    }

    fn test_point(lat: f64, lon: f64) -> PolylinePoint {
        PolylinePoint { lat, lon }
    }
}
