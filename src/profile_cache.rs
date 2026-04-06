use std::{
    collections::HashSet,
    hash::{DefaultHasher, Hash, Hasher},
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

use chrono::NaiveDate;
use dashmap::{DashMap, mapref::entry::Entry};
use serde::Serialize;

const PROFILE_BLOOM_BITS: usize = 256;
const PROFILE_BLOOM_WORDS: usize = PROFILE_BLOOM_BITS / 64;
const PROFILE_BLOOM_HASHES: u64 = 5;

#[derive(Clone)]
pub struct ProfileCache {
    gate: Arc<RwLock<()>>,
    exact_entries: Arc<DashMap<ProfileKey, Vec<ProfilePoint>>>,
    exact_summaries: Arc<DashMap<ProfileKey, ExactProfileSummary>>,
    spatial_entries: Arc<DashMap<SpatialProfileKey, Vec<SpatialProfilePoint>>>,
    exact_lookups: Arc<AtomicU64>,
    exact_hits: Arc<AtomicU64>,
    spatial_lookups: Arc<AtomicU64>,
    spatial_hits: Arc<AtomicU64>,
    bound_improvements: Arc<AtomicU64>,
    inserted_points: Arc<AtomicU64>,
    inserted_spatial_points: Arc<AtomicU64>,
    invalidation_passes: Arc<AtomicU64>,
    invalidated_points: Arc<AtomicU64>,
    invalidated_keys: Arc<AtomicU64>,
    bloom_checks: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ProfileCacheStats {
    pub keys: usize,
    pub points: usize,
    pub lookups: u64,
    pub hits: u64,
    pub exact_keys: usize,
    pub exact_points: usize,
    pub exact_lookups: u64,
    pub exact_hits: u64,
    pub spatial_keys: usize,
    pub spatial_points: usize,
    pub spatial_lookups: u64,
    pub spatial_hits: u64,
    pub bound_improvements: u64,
    pub inserted_points: u64,
    pub inserted_spatial_points: u64,
    pub invalidation_passes: u64,
    pub invalidated_points: u64,
    pub invalidated_keys: u64,
    pub bloom_checks: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ProfileInvalidationSummary {
    pub invalidated_points: u64,
    pub invalidated_keys: u64,
    pub bloom_checks: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CachedLeg {
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

#[derive(Clone, Debug)]
pub struct ProfileInsertionPoint {
    pub source_stop: usize,
    pub latest_ready_secs: i32,
    pub arrival_secs: i32,
    pub transit_legs: usize,
    pub trip_indices: Vec<usize>,
    pub suffix_legs: Vec<CachedLeg>,
}

#[derive(Clone, Debug)]
pub struct ProfileMatch {
    pub arrival_secs: i32,
    pub transit_legs: usize,
    pub absolute_min_duration_secs: i32,
    pub absolute_min_transfers: usize,
    pub suffix_legs: Arc<Vec<CachedLeg>>,
}

#[derive(Clone, Debug)]
pub enum ProfileLookupDecision {
    Miss,
    SummaryPruned,
    Match(ProfileMatch),
}

#[derive(Clone, Debug)]
pub struct SpatialProfileInsertionPoint {
    pub source_stop: usize,
    pub latest_ready_secs: i32,
    pub boundary_arrival_secs: i32,
    pub transit_legs: usize,
    pub boundary_stop: usize,
    pub trip_indices: Vec<usize>,
    pub trunk_legs: Vec<CachedLeg>,
}

#[derive(Clone, Debug)]
pub struct SpatialProfileMatch {
    pub latest_ready_secs: i32,
    pub boundary_arrival_secs: i32,
    pub transit_legs: usize,
    pub boundary_stop: usize,
    pub absolute_min_duration_secs: i32,
    pub absolute_min_transfers: usize,
    pub trunk_legs: Arc<Vec<CachedLeg>>,
}

#[derive(Clone, Debug, Default)]
pub struct PreparedSpatialLookup {
    pub enabled_source_stops: Vec<bool>,
    pub matches_by_source_stop: Vec<Vec<SpatialProfileMatch>>,
    pub absolute_min_duration_by_source_stop: Vec<i32>,
    pub absolute_min_transfers_by_source_stop: Vec<usize>,
    pub populated_sources: usize,
    pub materialized_matches: usize,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ProfileKey {
    service_date: NaiveDate,
    destination_stop: usize,
    source_stop: usize,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SpatialProfileKey {
    service_date: NaiveDate,
    destination_cell: u64,
    source_stop: usize,
}

#[derive(Clone, Debug)]
struct ProfilePoint {
    latest_ready_secs: i32,
    arrival_secs: i32,
    transit_legs: usize,
    absolute_min_duration_secs: i32,
    absolute_min_transfers: usize,
    bloom: TripBloomFilter,
    suffix_legs: Arc<Vec<CachedLeg>>,
}

#[derive(Clone, Debug)]
struct SpatialProfilePoint {
    latest_ready_secs: i32,
    boundary_arrival_secs: i32,
    transit_legs: usize,
    boundary_stop: usize,
    absolute_min_duration_secs: i32,
    absolute_min_transfers: usize,
    bloom: TripBloomFilter,
    trunk_legs: Arc<Vec<CachedLeg>>,
}

#[derive(Clone, Copy, Debug)]
struct ExactProfileSummary {
    absolute_min_duration_secs: i32,
    absolute_min_transfers: usize,
}

#[derive(Clone, Debug, Default)]
struct TripBloomFilter {
    words: [u64; PROFILE_BLOOM_WORDS],
}

impl ProfileCache {
    pub fn new() -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            exact_entries: Arc::new(DashMap::new()),
            exact_summaries: Arc::new(DashMap::new()),
            spatial_entries: Arc::new(DashMap::new()),
            exact_lookups: Arc::new(AtomicU64::new(0)),
            exact_hits: Arc::new(AtomicU64::new(0)),
            spatial_lookups: Arc::new(AtomicU64::new(0)),
            spatial_hits: Arc::new(AtomicU64::new(0)),
            bound_improvements: Arc::new(AtomicU64::new(0)),
            inserted_points: Arc::new(AtomicU64::new(0)),
            inserted_spatial_points: Arc::new(AtomicU64::new(0)),
            invalidation_passes: Arc::new(AtomicU64::new(0)),
            invalidated_points: Arc::new(AtomicU64::new(0)),
            invalidated_keys: Arc::new(AtomicU64::new(0)),
            bloom_checks: Arc::new(AtomicU64::new(0)),
        }
    }

    #[allow(dead_code)]
    pub fn lookup(
        &self,
        service_date: NaiveDate,
        destination_stop: usize,
        source_stop: usize,
        ready_at: i32,
        remaining_transit_legs: usize,
    ) -> Option<ProfileMatch> {
        match self.lookup_bounded(
            service_date,
            destination_stop,
            source_stop,
            ready_at,
            remaining_transit_legs,
            i32::MAX,
        ) {
            ProfileLookupDecision::Match(profile_match) => Some(profile_match),
            ProfileLookupDecision::Miss | ProfileLookupDecision::SummaryPruned => None,
        }
    }

    pub fn lookup_bounded(
        &self,
        service_date: NaiveDate,
        destination_stop: usize,
        source_stop: usize,
        ready_at: i32,
        remaining_transit_legs: usize,
        best_arrival_upper_bound: i32,
    ) -> ProfileLookupDecision {
        let _guard = self.gate.read().expect("profile cache gate poisoned");
        self.exact_lookups.fetch_add(1, Ordering::Relaxed);

        let key = ProfileKey {
            service_date,
            destination_stop,
            source_stop,
        };
        let Some(summary) = self.exact_summaries.get(&key) else {
            return ProfileLookupDecision::Miss;
        };
        if summary.absolute_min_transfers > remaining_transit_legs
            || ready_at.saturating_add(summary.absolute_min_duration_secs)
                >= best_arrival_upper_bound
        {
            return ProfileLookupDecision::SummaryPruned;
        }

        let Some(entry) = self.exact_entries.get(&key) else {
            return ProfileLookupDecision::Miss;
        };

        let mut best_match = None::<ProfileMatch>;
        for point in entry.iter() {
            if ready_at > point.latest_ready_secs || point.transit_legs > remaining_transit_legs {
                continue;
            }

            let should_replace = match &best_match {
                Some(current) => {
                    point.arrival_secs < current.arrival_secs
                        || (point.arrival_secs == current.arrival_secs
                            && point.transit_legs < current.transit_legs)
                }
                None => true,
            };

            if should_replace {
                best_match = Some(ProfileMatch {
                    arrival_secs: point.arrival_secs,
                    transit_legs: point.transit_legs,
                    absolute_min_duration_secs: point.absolute_min_duration_secs,
                    absolute_min_transfers: point.absolute_min_transfers,
                    suffix_legs: point.suffix_legs.clone(),
                });
            }
        }

        if best_match.is_some() {
            self.exact_hits.fetch_add(1, Ordering::Relaxed);
        }
        best_match
            .map(ProfileLookupDecision::Match)
            .unwrap_or(ProfileLookupDecision::Miss)
    }

    #[allow(dead_code)]
    pub fn lookup_spatial_cells(
        &self,
        service_date: NaiveDate,
        destination_cells: &[u64],
        source_stop: usize,
        ready_at: i32,
        remaining_transit_legs: usize,
    ) -> Vec<SpatialProfileMatch> {
        let _guard = self.gate.read().expect("profile cache gate poisoned");
        self.spatial_lookups.fetch_add(1, Ordering::Relaxed);

        let mut matches = Vec::<SpatialProfileMatch>::new();
        for &destination_cell in destination_cells {
            let key = SpatialProfileKey {
                service_date,
                destination_cell,
                source_stop,
            };
            let Some(entry) = self.spatial_entries.get(&key) else {
                continue;
            };

            matches.extend(entry.iter().filter_map(|point| {
                if ready_at > point.latest_ready_secs || point.transit_legs > remaining_transit_legs
                {
                    return None;
                }

                Some(SpatialProfileMatch {
                    latest_ready_secs: point.latest_ready_secs,
                    boundary_arrival_secs: point.boundary_arrival_secs,
                    transit_legs: point.transit_legs,
                    boundary_stop: point.boundary_stop,
                    absolute_min_duration_secs: point.absolute_min_duration_secs,
                    absolute_min_transfers: point.absolute_min_transfers,
                    trunk_legs: point.trunk_legs.clone(),
                })
            }));
        }

        matches.sort_by(|left, right| {
            left.boundary_arrival_secs
                .cmp(&right.boundary_arrival_secs)
                .then_with(|| left.transit_legs.cmp(&right.transit_legs))
                .then_with(|| left.boundary_stop.cmp(&right.boundary_stop))
        });
        matches.truncate(6);

        if !matches.is_empty() {
            self.spatial_hits.fetch_add(1, Ordering::Relaxed);
        }
        matches
    }

    pub fn materialize_spatial_query_surface(
        &self,
        service_date: NaiveDate,
        destination_cells: &[u64],
        stop_count: usize,
    ) -> PreparedSpatialLookup {
        if destination_cells.is_empty() || stop_count == 0 {
            return PreparedSpatialLookup::default();
        }

        let _guard = self.gate.read().expect("profile cache gate poisoned");
        let destination_set = destination_cells.iter().copied().collect::<HashSet<_>>();
        let mut matches_by_source_stop = vec![Vec::<SpatialProfileMatch>::new(); stop_count];
        let mut absolute_min_duration_by_source_stop = vec![i32::MAX; stop_count];
        let mut absolute_min_transfers_by_source_stop = vec![usize::MAX; stop_count];

        for entry in self.spatial_entries.iter() {
            let key = entry.key();
            if key.service_date != service_date
                || key.source_stop >= stop_count
                || !destination_set.contains(&key.destination_cell)
            {
                continue;
            }

            let bucket = &mut matches_by_source_stop[key.source_stop];
            bucket.extend(entry.value().iter().map(|point| SpatialProfileMatch {
                latest_ready_secs: point.latest_ready_secs,
                boundary_arrival_secs: point.boundary_arrival_secs,
                transit_legs: point.transit_legs,
                boundary_stop: point.boundary_stop,
                absolute_min_duration_secs: point.absolute_min_duration_secs,
                absolute_min_transfers: point.absolute_min_transfers,
                trunk_legs: point.trunk_legs.clone(),
            }));
        }

        let mut enabled_source_stops = vec![false; stop_count];
        let mut populated_sources = 0usize;
        let mut materialized_matches = 0usize;

        for (source_stop, bucket) in matches_by_source_stop.iter_mut().enumerate() {
            if bucket.is_empty() {
                continue;
            }

            bucket.sort_by(|left, right| {
                left.boundary_arrival_secs
                    .cmp(&right.boundary_arrival_secs)
                    .then_with(|| left.transit_legs.cmp(&right.transit_legs))
                    .then_with(|| left.boundary_stop.cmp(&right.boundary_stop))
            });
            bucket.truncate(6);

            enabled_source_stops[source_stop] = true;
            absolute_min_duration_by_source_stop[source_stop] = bucket
                .iter()
                .map(|point| point.absolute_min_duration_secs)
                .min()
                .unwrap_or(i32::MAX);
            absolute_min_transfers_by_source_stop[source_stop] = bucket
                .iter()
                .map(|point| point.absolute_min_transfers)
                .min()
                .unwrap_or(usize::MAX);
            populated_sources += 1;
            materialized_matches += bucket.len();
        }

        PreparedSpatialLookup {
            enabled_source_stops,
            matches_by_source_stop,
            absolute_min_duration_by_source_stop,
            absolute_min_transfers_by_source_stop,
            populated_sources,
            materialized_matches,
        }
    }

    pub fn note_bound_improvement(&self) {
        self.bound_improvements.fetch_add(1, Ordering::Relaxed);
    }

    pub fn insert_batch(
        &self,
        service_date: NaiveDate,
        destination_stop: usize,
        points: Vec<ProfileInsertionPoint>,
    ) {
        if points.is_empty() {
            return;
        }

        let _guard = self.gate.read().expect("profile cache gate poisoned");
        let mut inserted_points = 0u64;

        for point in points {
            let key = ProfileKey {
                service_date,
                destination_stop,
                source_stop: point.source_stop,
            };
            let summary_key = key.clone();
            let mut bloom = TripBloomFilter::default();
            for trip_index in &point.trip_indices {
                bloom.insert_trip(*trip_index);
            }
            let next_point = ProfilePoint {
                latest_ready_secs: point.latest_ready_secs,
                arrival_secs: point.arrival_secs,
                transit_legs: point.transit_legs,
                absolute_min_duration_secs: point
                    .arrival_secs
                    .saturating_sub(point.latest_ready_secs),
                absolute_min_transfers: point.transit_legs,
                bloom,
                suffix_legs: Arc::new(point.suffix_legs),
            };

            match self.exact_entries.entry(key) {
                Entry::Occupied(mut occupied) => {
                    let bucket = occupied.get_mut();
                    if merge_profile_point(bucket, next_point) {
                        inserted_points += 1;
                    }
                    self.exact_summaries
                        .insert(summary_key, summarize_profile_bucket(bucket));
                }
                Entry::Vacant(vacant) => {
                    let bucket = vec![next_point];
                    self.exact_summaries
                        .insert(summary_key, summarize_profile_bucket(&bucket));
                    vacant.insert(bucket);
                    inserted_points += 1;
                }
            }
        }

        self.inserted_points
            .fetch_add(inserted_points, Ordering::Relaxed);
    }

    pub fn insert_spatial_batch(
        &self,
        service_date: NaiveDate,
        destination_cell: u64,
        points: Vec<SpatialProfileInsertionPoint>,
    ) {
        if points.is_empty() {
            return;
        }

        let _guard = self.gate.read().expect("profile cache gate poisoned");
        let mut inserted_points = 0u64;

        for point in points {
            let key = SpatialProfileKey {
                service_date,
                destination_cell,
                source_stop: point.source_stop,
            };
            let mut bloom = TripBloomFilter::default();
            for trip_index in &point.trip_indices {
                bloom.insert_trip(*trip_index);
            }
            let next_point = SpatialProfilePoint {
                latest_ready_secs: point.latest_ready_secs,
                boundary_arrival_secs: point.boundary_arrival_secs,
                transit_legs: point.transit_legs,
                boundary_stop: point.boundary_stop,
                absolute_min_duration_secs: point
                    .boundary_arrival_secs
                    .saturating_sub(point.latest_ready_secs),
                absolute_min_transfers: point.transit_legs,
                bloom,
                trunk_legs: Arc::new(point.trunk_legs),
            };

            match self.spatial_entries.entry(key) {
                Entry::Occupied(mut occupied) => {
                    if merge_spatial_profile_point(occupied.get_mut(), next_point) {
                        inserted_points += 1;
                    }
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(vec![next_point]);
                    inserted_points += 1;
                }
            }
        }

        self.inserted_spatial_points
            .fetch_add(inserted_points, Ordering::Relaxed);
    }

    pub fn invalidate_trips(&self, changed_trips: &[usize]) -> ProfileInvalidationSummary {
        if changed_trips.is_empty() {
            return ProfileInvalidationSummary::default();
        }

        let _guard = self.gate.write().expect("profile cache gate poisoned");
        self.invalidation_passes.fetch_add(1, Ordering::Relaxed);

        let keys = self
            .exact_entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();

        let mut summary = ProfileInvalidationSummary::default();
        for key in keys {
            let mut remove_bucket = false;
            let mut refreshed_summary = None::<ExactProfileSummary>;
            if let Some(mut bucket) = self.exact_entries.get_mut(&key) {
                let before = bucket.len();
                summary.bloom_checks += before as u64 * changed_trips.len() as u64;
                bucket.retain(|point| !point.bloom.might_intersect_any(changed_trips));
                let removed = before.saturating_sub(bucket.len());
                summary.invalidated_points += removed as u64;
                remove_bucket = bucket.is_empty();
                if !remove_bucket {
                    refreshed_summary = Some(summarize_profile_bucket(bucket.as_slice()));
                }
            }

            if remove_bucket {
                self.exact_entries.remove(&key);
                self.exact_summaries.remove(&key);
                summary.invalidated_keys += 1;
            } else if let Some(refreshed_summary) = refreshed_summary {
                self.exact_summaries.insert(key.clone(), refreshed_summary);
            }
        }

        let spatial_keys = self
            .spatial_entries
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for key in spatial_keys {
            let mut remove_bucket = false;
            if let Some(mut bucket) = self.spatial_entries.get_mut(&key) {
                let before = bucket.len();
                summary.bloom_checks += before as u64 * changed_trips.len() as u64;
                bucket.retain(|point| !point.bloom.might_intersect_any(changed_trips));
                let removed = before.saturating_sub(bucket.len());
                summary.invalidated_points += removed as u64;
                remove_bucket = bucket.is_empty();
            }

            if remove_bucket {
                self.spatial_entries.remove(&key);
                summary.invalidated_keys += 1;
            }
        }

        self.invalidated_points
            .fetch_add(summary.invalidated_points, Ordering::Relaxed);
        self.invalidated_keys
            .fetch_add(summary.invalidated_keys, Ordering::Relaxed);
        self.bloom_checks
            .fetch_add(summary.bloom_checks, Ordering::Relaxed);
        summary
    }

    pub fn snapshot(&self) -> ProfileCacheStats {
        let _guard = self.gate.read().expect("profile cache gate poisoned");
        let exact_keys = self.exact_entries.len();
        let exact_points = self.exact_entries.iter().map(|entry| entry.value().len()).sum();
        let spatial_keys = self.spatial_entries.len();
        let spatial_points = self
            .spatial_entries
            .iter()
            .map(|entry| entry.value().len())
            .sum();
        let exact_lookups = self.exact_lookups.load(Ordering::Relaxed);
        let exact_hits = self.exact_hits.load(Ordering::Relaxed);
        let spatial_lookups = self.spatial_lookups.load(Ordering::Relaxed);
        let spatial_hits = self.spatial_hits.load(Ordering::Relaxed);

        ProfileCacheStats {
            keys: exact_keys + spatial_keys,
            points: exact_points + spatial_points,
            lookups: exact_lookups + spatial_lookups,
            hits: exact_hits + spatial_hits,
            exact_keys,
            exact_points,
            exact_lookups,
            exact_hits,
            spatial_keys,
            spatial_points,
            spatial_lookups,
            spatial_hits,
            bound_improvements: self.bound_improvements.load(Ordering::Relaxed),
            inserted_points: self.inserted_points.load(Ordering::Relaxed),
            inserted_spatial_points: self.inserted_spatial_points.load(Ordering::Relaxed),
            invalidation_passes: self.invalidation_passes.load(Ordering::Relaxed),
            invalidated_points: self.invalidated_points.load(Ordering::Relaxed),
            invalidated_keys: self.invalidated_keys.load(Ordering::Relaxed),
            bloom_checks: self.bloom_checks.load(Ordering::Relaxed),
        }
    }
}

impl TripBloomFilter {
    fn insert_trip(&mut self, trip_index: usize) {
        for seed in 0..PROFILE_BLOOM_HASHES {
            let bit = bloom_bit(trip_index, seed);
            let word = bit / 64;
            let bit_offset = bit % 64;
            self.words[word] |= 1u64 << bit_offset;
        }
    }

    fn might_intersect_any(&self, changed_trips: &[usize]) -> bool {
        changed_trips
            .iter()
            .copied()
            .any(|trip_index| self.might_contain_trip(trip_index))
    }

    fn might_contain_trip(&self, trip_index: usize) -> bool {
        (0..PROFILE_BLOOM_HASHES).all(|seed| {
            let bit = bloom_bit(trip_index, seed);
            let word = bit / 64;
            let bit_offset = bit % 64;
            (self.words[word] & (1u64 << bit_offset)) != 0
        })
    }
}

fn bloom_bit(trip_index: usize, seed: u64) -> usize {
    let mut hasher = DefaultHasher::new();
    trip_index.hash(&mut hasher);
    seed.hash(&mut hasher);
    (hasher.finish() as usize) % PROFILE_BLOOM_BITS
}

fn merge_profile_point(bucket: &mut Vec<ProfilePoint>, next_point: ProfilePoint) -> bool {
    if let Some(existing) = bucket.iter_mut().find(|point| {
        point.latest_ready_secs == next_point.latest_ready_secs
            && point.arrival_secs == next_point.arrival_secs
            && point.transit_legs == next_point.transit_legs
    }) {
        if next_point.suffix_legs.len() < existing.suffix_legs.len() {
            *existing = next_point;
            return true;
        }
        return false;
    }

    bucket.push(next_point);
    compact_bucket(bucket);
    true
}

fn compact_bucket(bucket: &mut Vec<ProfilePoint>) {
    bucket.sort_by(|left, right| {
        left.transit_legs
            .cmp(&right.transit_legs)
            .then_with(|| right.latest_ready_secs.cmp(&left.latest_ready_secs))
            .then_with(|| left.arrival_secs.cmp(&right.arrival_secs))
    });

    let mut compacted = Vec::with_capacity(bucket.len());
    let mut current_transit_legs = None::<usize>;
    let mut best_arrival = i32::MAX;

    for point in bucket.drain(..) {
        if current_transit_legs != Some(point.transit_legs) {
            current_transit_legs = Some(point.transit_legs);
            best_arrival = i32::MAX;
        }

        if point.arrival_secs < best_arrival {
            best_arrival = point.arrival_secs;
            compacted.push(point);
        }
    }

    *bucket = compacted;
}

fn summarize_profile_bucket(bucket: &[ProfilePoint]) -> ExactProfileSummary {
    ExactProfileSummary {
        absolute_min_duration_secs: bucket
            .iter()
            .map(|point| point.absolute_min_duration_secs)
            .min()
            .unwrap_or(i32::MAX),
        absolute_min_transfers: bucket
            .iter()
            .map(|point| point.absolute_min_transfers)
            .min()
            .unwrap_or(usize::MAX),
    }
}

fn merge_spatial_profile_point(
    bucket: &mut Vec<SpatialProfilePoint>,
    next_point: SpatialProfilePoint,
) -> bool {
    if let Some(existing) = bucket.iter_mut().find(|point| {
        point.boundary_stop == next_point.boundary_stop
            && point.latest_ready_secs == next_point.latest_ready_secs
            && point.boundary_arrival_secs == next_point.boundary_arrival_secs
            && point.transit_legs == next_point.transit_legs
    }) {
        if next_point.trunk_legs.len() < existing.trunk_legs.len() {
            *existing = next_point;
            return true;
        }
        return false;
    }

    bucket.push(next_point);
    compact_spatial_bucket(bucket);
    true
}

fn compact_spatial_bucket(bucket: &mut Vec<SpatialProfilePoint>) {
    bucket.sort_by(|left, right| {
        left.boundary_stop
            .cmp(&right.boundary_stop)
            .then_with(|| left.transit_legs.cmp(&right.transit_legs))
            .then_with(|| right.latest_ready_secs.cmp(&left.latest_ready_secs))
            .then_with(|| left.boundary_arrival_secs.cmp(&right.boundary_arrival_secs))
    });

    let mut compacted = Vec::with_capacity(bucket.len());
    let mut current_boundary = None::<usize>;
    let mut current_transit_legs = None::<usize>;
    let mut best_arrival = i32::MAX;

    for point in bucket.drain(..) {
        if current_boundary != Some(point.boundary_stop)
            || current_transit_legs != Some(point.transit_legs)
        {
            current_boundary = Some(point.boundary_stop);
            current_transit_legs = Some(point.transit_legs);
            best_arrival = i32::MAX;
        }

        if point.boundary_arrival_secs < best_arrival {
            best_arrival = point.boundary_arrival_secs;
            compacted.push(point);
        }
    }

    *bucket = compacted;
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::{
        CachedLeg, ProfileCache, ProfileInsertionPoint, ProfileLookupDecision,
        SpatialProfileInsertionPoint,
    };

    #[test]
    fn prefers_best_feasible_profile_point() {
        let cache = ProfileCache::new();
        let service_date = NaiveDate::from_ymd_opt(2026, 4, 5).expect("valid test date");

        cache.insert_batch(
            service_date,
            42,
            vec![
                ProfileInsertionPoint {
                    source_stop: 7,
                    latest_ready_secs: 8 * 3600,
                    arrival_secs: 9 * 3600,
                    transit_legs: 1,
                    trip_indices: vec![11],
                    suffix_legs: vec![CachedLeg::Transit {
                        trip_index: 11,
                        board_stop: 7,
                        board_pos: 0,
                        alight_stop: 42,
                        alight_pos: 1,
                        departure_secs: 8 * 3600,
                        arrival_secs: 9 * 3600,
                    }],
                },
                ProfileInsertionPoint {
                    source_stop: 7,
                    latest_ready_secs: 7 * 3600 + 30 * 60,
                    arrival_secs: 8 * 3600 + 20 * 60,
                    transit_legs: 1,
                    trip_indices: vec![12],
                    suffix_legs: vec![CachedLeg::Transit {
                        trip_index: 12,
                        board_stop: 7,
                        board_pos: 0,
                        alight_stop: 42,
                        alight_pos: 1,
                        departure_secs: 7 * 3600 + 30 * 60,
                        arrival_secs: 8 * 3600 + 20 * 60,
                    }],
                },
            ],
        );

        let earlier = cache
            .lookup(service_date, 42, 7, 7 * 3600, 1)
            .expect("earlier ready time should hit");
        assert_eq!(earlier.arrival_secs, 8 * 3600 + 20 * 60);
        assert_eq!(earlier.absolute_min_duration_secs, 50 * 60);
        assert_eq!(earlier.absolute_min_transfers, 1);

        let later = cache
            .lookup(service_date, 42, 7, 7 * 3600 + 45 * 60, 1)
            .expect("later ready time should still hit");
        assert_eq!(later.arrival_secs, 9 * 3600);
        assert_eq!(later.absolute_min_duration_secs, 60 * 60);
        assert_eq!(later.absolute_min_transfers, 1);

        assert!(matches!(
            cache.lookup_bounded(service_date, 42, 7, 7 * 3600, 1, 7 * 3600 + 40 * 60),
            ProfileLookupDecision::SummaryPruned
        ));
    }

    #[test]
    fn materialized_spatial_surface_preserves_bounds_and_readiness() {
        let cache = ProfileCache::new();
        let service_date = NaiveDate::from_ymd_opt(2026, 4, 5).expect("valid test date");

        cache.insert_spatial_batch(
            service_date,
            99,
            vec![SpatialProfileInsertionPoint {
                source_stop: 7,
                latest_ready_secs: 8 * 3600,
                boundary_arrival_secs: 8 * 3600 + 20 * 60,
                transit_legs: 1,
                boundary_stop: 11,
                trip_indices: vec![12],
                trunk_legs: vec![CachedLeg::Transit {
                    trip_index: 12,
                    board_stop: 7,
                    board_pos: 0,
                    alight_stop: 11,
                    alight_pos: 1,
                    departure_secs: 8 * 3600,
                    arrival_secs: 8 * 3600 + 20 * 60,
                }],
            }],
        );

        let prepared = cache.materialize_spatial_query_surface(service_date, &[99], 16);
        assert!(prepared.enabled_source_stops[7]);
        assert_eq!(prepared.absolute_min_duration_by_source_stop[7], 20 * 60);
        assert_eq!(prepared.absolute_min_transfers_by_source_stop[7], 1);

        let point = &prepared.matches_by_source_stop[7][0];
        assert_eq!(point.latest_ready_secs, 8 * 3600);
        assert_eq!(point.absolute_min_duration_secs, 20 * 60);
        assert_eq!(point.absolute_min_transfers, 1);
    }

    #[test]
    fn invalidates_points_that_touch_changed_trips() {
        let cache = ProfileCache::new();
        let service_date = NaiveDate::from_ymd_opt(2026, 4, 5).expect("valid test date");

        cache.insert_batch(
            service_date,
            42,
            vec![
                ProfileInsertionPoint {
                    source_stop: 7,
                    latest_ready_secs: 8 * 3600,
                    arrival_secs: 9 * 3600,
                    transit_legs: 1,
                    trip_indices: vec![11],
                    suffix_legs: vec![CachedLeg::Transit {
                        trip_index: 11,
                        board_stop: 7,
                        board_pos: 0,
                        alight_stop: 42,
                        alight_pos: 1,
                        departure_secs: 8 * 3600,
                        arrival_secs: 9 * 3600,
                    }],
                },
                ProfileInsertionPoint {
                    source_stop: 8,
                    latest_ready_secs: 8 * 3600,
                    arrival_secs: 8 * 3600 + 15 * 60,
                    transit_legs: 0,
                    trip_indices: vec![],
                    suffix_legs: vec![CachedLeg::Walk {
                        from_stop: 8,
                        to_stop: 42,
                        departure_secs: 8 * 3600,
                        arrival_secs: 8 * 3600 + 15 * 60,
                        duration_secs: 15 * 60,
                        distance_meters: 1200.0,
                    }],
                },
            ],
        );

        let summary = cache.invalidate_trips(&[11]);
        assert_eq!(summary.invalidated_points, 1);

        assert!(cache.lookup(service_date, 42, 7, 7 * 3600, 1).is_none());
        assert!(cache.lookup(service_date, 42, 8, 7 * 3600, 0).is_some());
    }

    #[test]
    fn spatial_profiles_hit_for_same_cell() {
        let cache = ProfileCache::new();
        let service_date = NaiveDate::from_ymd_opt(2026, 4, 5).expect("valid test date");

        cache.insert_spatial_batch(
            service_date,
            9001,
            vec![SpatialProfileInsertionPoint {
                source_stop: 7,
                latest_ready_secs: 8 * 3600,
                boundary_arrival_secs: 8 * 3600 + 20 * 60,
                transit_legs: 1,
                boundary_stop: 42,
                trip_indices: vec![11],
                trunk_legs: vec![CachedLeg::Transit {
                    trip_index: 11,
                    board_stop: 7,
                    board_pos: 0,
                    alight_stop: 42,
                    alight_pos: 1,
                    departure_secs: 8 * 3600,
                    arrival_secs: 8 * 3600 + 20 * 60,
                }],
            }],
        );

        let matches = cache.lookup_spatial_cells(service_date, &[9001], 7, 7 * 3600 + 30 * 60, 1);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].boundary_stop, 42);
    }

    #[test]
    fn spatial_profiles_hit_across_neighbor_cells() {
        let cache = ProfileCache::new();
        let service_date = NaiveDate::from_ymd_opt(2026, 4, 5).expect("valid test date");

        cache.insert_spatial_batch(
            service_date,
            9001,
            vec![SpatialProfileInsertionPoint {
                source_stop: 7,
                latest_ready_secs: 8 * 3600,
                boundary_arrival_secs: 8 * 3600 + 20 * 60,
                transit_legs: 1,
                boundary_stop: 42,
                trip_indices: vec![11],
                trunk_legs: vec![CachedLeg::Transit {
                    trip_index: 11,
                    board_stop: 7,
                    board_pos: 0,
                    alight_stop: 42,
                    alight_pos: 1,
                    departure_secs: 8 * 3600,
                    arrival_secs: 8 * 3600 + 20 * 60,
                }],
            }],
        );

        let matches = cache.lookup_spatial_cells(
            service_date,
            &[9000, 9001, 9002],
            7,
            7 * 3600 + 30 * 60,
            1,
        );
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].boundary_stop, 42);
    }
}