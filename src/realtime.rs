use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use gtfs_rt::{FeedMessage, trip_descriptor, trip_update, vehicle_position};
use prost::Message;
use reqwest::Client;
use serde::Serialize;

use crate::control::maybe_add_internal_token_async;
use crate::engine::{EngineConfig, FeedConfig, StaticData, TripRecord, route_display_name};

const MAX_REASONABLE_DELAY_SECS: i32 = 6 * 60 * 60;

#[derive(Clone)]
pub struct RealtimeStore {
    client: Client,
    stop_deltas: Arc<DashMap<(usize, usize), StopDelta>>,
    trip_metrics: Arc<DashMap<usize, TripRealtimeMeta>>,
    canceled_trips: Arc<DashMap<usize, ()>>,
    state: Arc<Mutex<RealtimeState>>,
}

#[derive(Clone, Default)]
struct RealtimeState {
    trip_entities: usize,
    vehicle_entities: usize,
    trip_feed_timestamp: Option<u64>,
    vehicle_feed_timestamp: Option<u64>,
    last_trip_refresh: Option<DateTime<Utc>>,
    last_vehicle_refresh: Option<DateTime<Utc>>,
    feed_statuses: Vec<FeedRealtimeStatus>,
    vehicles: Vec<VehicleSnapshot>,
    last_error: Option<String>,
}

#[derive(Clone, Debug)]
struct StopDelta {
    arrival_delay_secs: Option<i32>,
    departure_delay_secs: Option<i32>,
    skipped: bool,
    departure_occupancy_status: Option<i32>,
}

impl StopDelta {
    fn affects_schedule(&self) -> bool {
        self.arrival_delay_secs.is_some() || self.departure_delay_secs.is_some() || self.skipped
    }
}

#[derive(Clone, Debug, Default)]
struct TripRealtimeMeta {
    has_trip_update: bool,
    has_vehicle_position: bool,
    schedule_relationship: Option<i32>,
    vehicle_occupancy_status: Option<i32>,
    vehicle_occupancy_percentage: Option<u32>,
    vehicle_timestamp: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TripRealtimeMetrics {
    pub has_trip_update: bool,
    pub has_vehicle_position: bool,
    pub is_canceled: bool,
    pub schedule_relationship: Option<i32>,
    pub stop_departure_occupancy_status: Option<i32>,
    pub vehicle_occupancy_status: Option<i32>,
    pub vehicle_occupancy_percentage: Option<u32>,
}

impl TripRealtimeMetrics {
    pub fn has_gtfs_rt(self) -> bool {
        self.has_trip_update || self.has_vehicle_position || self.is_canceled
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DelaySample {
    pub feed_id: String,
    pub trip_id: String,
    pub route_label: String,
    pub stop_id: String,
    pub stop_name: String,
    pub stop_sequence: u32,
    pub arrival_delay_secs: Option<i32>,
    pub departure_delay_secs: Option<i32>,
    pub skipped: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct VehicleSnapshot {
    pub feed_id: String,
    pub entity_id: String,
    pub trip_id: Option<String>,
    pub vehicle_id: Option<String>,
    pub vehicle_label: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub bearing: Option<f32>,
    pub speed: Option<f32>,
    pub current_stop_sequence: Option<u32>,
    pub stop_id: Option<String>,
    pub current_status_code: Option<i32>,
    pub schedule_relationship: Option<String>,
    pub occupancy_status: Option<String>,
    pub occupancy_percentage: Option<u32>,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct FeedRealtimeStatus {
    pub feed_id: String,
    pub trip_update_url: Option<String>,
    pub vehicle_positions_url: Option<String>,
    pub trip_update_entities: usize,
    pub vehicle_count: usize,
    pub trip_feed_timestamp: Option<u64>,
    pub vehicle_feed_timestamp: Option<u64>,
    pub last_trip_refresh: Option<String>,
    pub last_vehicle_refresh: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RealtimeDebugSnapshot {
    pub trip_update_url: String,
    pub vehicle_positions_url: String,
    pub shadow_delta_count: usize,
    pub canceled_trip_count: usize,
    pub trip_update_entities: usize,
    pub vehicle_count: usize,
    pub trip_feed_timestamp: Option<u64>,
    pub vehicle_feed_timestamp: Option<u64>,
    pub last_trip_refresh: Option<String>,
    pub last_vehicle_refresh: Option<String>,
    pub last_error: Option<String>,
    pub feeds: Vec<FeedRealtimeStatus>,
    pub delay_samples: Vec<DelaySample>,
    pub vehicles: Vec<VehicleSnapshot>,
}

struct TripRefreshOutcome {
    trip_entities: usize,
    trip_feed_timestamp: Option<u64>,
}

struct VehicleRefreshOutcome {
    vehicle_entities: usize,
    vehicle_feed_timestamp: Option<u64>,
    vehicles: Vec<VehicleSnapshot>,
}

pub struct RealtimeRefreshResult {
    pub snapshot: RealtimeDebugSnapshot,
    pub changed_trip_indices: Vec<usize>,
    pub terminal_error: Option<String>,
}

impl RealtimeStore {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("alpha-raptor-engine/0.1")
                .build()
                .expect("reqwest client initialization should not fail"),
            stop_deltas: Arc::new(DashMap::new()),
            trip_metrics: Arc::new(DashMap::new()),
            canceled_trips: Arc::new(DashMap::new()),
            state: Arc::new(Mutex::new(RealtimeState::default())),
        }
    }

    pub async fn refresh(
        &self,
        static_data: &StaticData,
        config: &EngineConfig,
    ) -> Result<RealtimeRefreshResult> {
        let mut changed_trip_indices = self.updated_trip_indices();
        self.stop_deltas.clear();
        self.trip_metrics.clear();
        self.canceled_trips.clear();

        let mut trip_entities_total = 0usize;
        let mut vehicle_entities_total = 0usize;
        let mut max_trip_feed_timestamp = None::<u64>;
        let mut max_vehicle_feed_timestamp = None::<u64>;
        let mut vehicles = Vec::<VehicleSnapshot>::new();
        let mut feed_statuses = Vec::<FeedRealtimeStatus>::with_capacity(config.feeds.len());
        let mut error_messages = Vec::<String>::new();
        let refresh_timestamp = Utc::now();

        for feed in &config.feeds {
            let mut feed_status = FeedRealtimeStatus {
                feed_id: feed.id.clone(),
                trip_update_url: feed.trip_updates_url.clone(),
                vehicle_positions_url: feed.vehicle_positions_url.clone(),
                ..FeedRealtimeStatus::default()
            };

            if feed.trip_updates_url.is_some() {
                match self.refresh_trip_updates(static_data, feed).await {
                    Ok(outcome) => {
                        trip_entities_total += outcome.trip_entities;
                        feed_status.trip_update_entities = outcome.trip_entities;
                        feed_status.trip_feed_timestamp = outcome.trip_feed_timestamp;
                        feed_status.last_trip_refresh = Some(refresh_timestamp.to_rfc3339());
                        max_trip_feed_timestamp =
                            max_option_u64(max_trip_feed_timestamp, outcome.trip_feed_timestamp);
                    }
                    Err(error) => {
                        let message =
                            format!("feed {} trip updates refresh failed: {error}", feed.id);
                        feed_status.last_error = Some(message.clone());
                        error_messages.push(message);
                    }
                }
            }

            if feed.vehicle_positions_url.is_some() {
                match self.refresh_vehicle_positions(static_data, feed).await {
                    Ok(outcome) => {
                        vehicle_entities_total += outcome.vehicle_entities;
                        feed_status.vehicle_count = outcome.vehicle_entities;
                        feed_status.vehicle_feed_timestamp = outcome.vehicle_feed_timestamp;
                        feed_status.last_vehicle_refresh = Some(refresh_timestamp.to_rfc3339());
                        max_vehicle_feed_timestamp = max_option_u64(
                            max_vehicle_feed_timestamp,
                            outcome.vehicle_feed_timestamp,
                        );
                        vehicles.extend(outcome.vehicles);
                    }
                    Err(error) => {
                        let message =
                            format!("feed {} vehicle positions refresh failed: {error}", feed.id);
                        match &mut feed_status.last_error {
                            Some(existing) => {
                                existing.push_str("; ");
                                existing.push_str(&message);
                            }
                            None => feed_status.last_error = Some(message.clone()),
                        }
                        error_messages.push(message);
                    }
                }
            }

            feed_statuses.push(feed_status);
        }

        let mut state = self.state.lock().expect("realtime state poisoned");
        state.trip_entities = trip_entities_total;
        state.vehicle_entities = vehicle_entities_total;
        state.trip_feed_timestamp = max_trip_feed_timestamp;
        state.vehicle_feed_timestamp = max_vehicle_feed_timestamp;
        state.last_trip_refresh = Some(refresh_timestamp);
        state.last_vehicle_refresh = Some(refresh_timestamp);
        state.feed_statuses = feed_statuses;
        state.vehicles = vehicles;
        state.last_error = if error_messages.is_empty() {
            None
        } else {
            Some(error_messages.join("; "))
        };
        drop(state);

        let snapshot = self.snapshot(static_data, 24);
        changed_trip_indices.extend(self.updated_trip_indices());
        changed_trip_indices.sort_unstable();
        changed_trip_indices.dedup();

        let terminal_error = if snapshot.trip_update_entities == 0
            && snapshot.vehicle_count == 0
            && snapshot.last_error.is_some()
        {
            Some(snapshot.last_error.clone().unwrap_or_default())
        } else {
            None
        };

        Ok(RealtimeRefreshResult {
            snapshot,
            changed_trip_indices,
            terminal_error,
        })
    }

    pub fn snapshot(&self, static_data: &StaticData, limit: usize) -> RealtimeDebugSnapshot {
        let state = self.state.lock().expect("realtime state poisoned").clone();
        let mut delay_samples = Vec::new();
        for entry in self.stop_deltas.iter().take(limit) {
            let (trip_index, stop_pos) = *entry.key();
            let delta = entry.value();
            let trip = &static_data.trips[trip_index];
            let stop_time = &trip.stop_times[stop_pos];
            let stop = &static_data.stops[stop_time.stop_index];
            let route = &static_data.routes[trip.route_index];
            delay_samples.push(DelaySample {
                feed_id: trip.feed_id.clone(),
                trip_id: trip.id.clone(),
                route_label: route_display_name(route),
                stop_id: stop.id.clone(),
                stop_name: stop.name.clone(),
                stop_sequence: stop_time.stop_sequence,
                arrival_delay_secs: delta.arrival_delay_secs,
                departure_delay_secs: delta.departure_delay_secs,
                skipped: delta.skipped,
            });
        }

        RealtimeDebugSnapshot {
            trip_update_url: state
                .feed_statuses
                .iter()
                .filter_map(|feed| {
                    feed.trip_update_url
                        .as_ref()
                        .map(|url| format!("{}={url}", feed.feed_id))
                })
                .collect::<Vec<_>>()
                .join(", "),
            vehicle_positions_url: state
                .feed_statuses
                .iter()
                .filter_map(|feed| {
                    feed.vehicle_positions_url
                        .as_ref()
                        .map(|url| format!("{}={url}", feed.feed_id))
                })
                .collect::<Vec<_>>()
                .join(", "),
            shadow_delta_count: self.stop_deltas.len(),
            canceled_trip_count: self.canceled_trips.len(),
            trip_update_entities: state.trip_entities,
            vehicle_count: state.vehicle_entities,
            trip_feed_timestamp: state.trip_feed_timestamp,
            vehicle_feed_timestamp: state.vehicle_feed_timestamp,
            last_trip_refresh: state.last_trip_refresh.map(|value| value.to_rfc3339()),
            last_vehicle_refresh: state.last_vehicle_refresh.map(|value| value.to_rfc3339()),
            last_error: state.last_error,
            feeds: state.feed_statuses,
            delay_samples,
            vehicles: state.vehicles.into_iter().take(limit).collect(),
        }
    }

    pub fn shadow_delta_count(&self) -> usize {
        self.stop_deltas.len()
    }

    pub fn canceled_trip_count(&self) -> usize {
        self.canceled_trips.len()
    }

    pub fn updated_trip_indices(&self) -> Vec<usize> {
        let mut updated = self
            .stop_deltas
            .iter()
            .filter(|entry| entry.value().affects_schedule())
            .map(|entry| entry.key().0)
            .collect::<Vec<_>>();
        updated.extend(self.canceled_trips.iter().map(|entry| *entry.key()));
        updated.sort_unstable();
        updated.dedup();
        updated
    }

    pub fn canceled_trip_mask(&self, trip_count: usize) -> Vec<bool> {
        let mut canceled = vec![false; trip_count];
        for trip_index in self.canceled_trips.iter().map(|entry| *entry.key()) {
            if trip_index < canceled.len() {
                canceled[trip_index] = true;
            }
        }
        canceled
    }

    #[allow(dead_code)]
    pub fn updated_trip_mask(&self, trip_count: usize) -> Vec<bool> {
        let mut updated = vec![false; trip_count];
        for entry in self.stop_deltas.iter() {
            if !entry.value().affects_schedule() {
                continue;
            }
            let (trip_index, _) = *entry.key();
            if trip_index < updated.len() {
                updated[trip_index] = true;
            }
        }
        for trip_index in self.canceled_trips.iter().map(|entry| *entry.key()) {
            if trip_index < updated.len() {
                updated[trip_index] = true;
            }
        }
        updated
    }

    pub fn trip_max_positive_departure_delay_secs(&self, trip_count: usize) -> Vec<i32> {
        let mut max_delay_secs = vec![0; trip_count];
        for entry in self.stop_deltas.iter() {
            let (trip_index, _) = *entry.key();
            if trip_index >= max_delay_secs.len() {
                continue;
            }

            let delay_secs = entry
                .value()
                .departure_delay_secs
                .or(entry.value().arrival_delay_secs)
                .unwrap_or(0)
                .clamp(0, MAX_REASONABLE_DELAY_SECS);
            if delay_secs > max_delay_secs[trip_index] {
                max_delay_secs[trip_index] = delay_secs;
            }
        }
        max_delay_secs
    }

    pub fn trip_realtime_metrics(&self, trip_index: usize, stop_pos: usize) -> TripRealtimeMetrics {
        let trip_meta = self.trip_metrics.get(&trip_index);
        let stop_delta = self.stop_deltas.get(&(trip_index, stop_pos));
        let is_canceled = self.canceled_trips.contains_key(&trip_index);

        TripRealtimeMetrics {
            has_trip_update: trip_meta.as_ref().map(|value| value.has_trip_update).unwrap_or(false)
                || is_canceled,
            has_vehicle_position: trip_meta
                .as_ref()
                .map(|value| value.has_vehicle_position)
                .unwrap_or(false),
            is_canceled,
            schedule_relationship: trip_meta
                .as_ref()
                .and_then(|value| value.schedule_relationship)
                .or_else(|| {
                    is_canceled.then_some(trip_descriptor::ScheduleRelationship::Canceled as i32)
                }),
            stop_departure_occupancy_status: stop_delta
                .as_ref()
                .and_then(|value| value.departure_occupancy_status),
            vehicle_occupancy_status: trip_meta
                .as_ref()
                .and_then(|value| value.vehicle_occupancy_status),
            vehicle_occupancy_percentage: trip_meta
                .as_ref()
                .and_then(|value| value.vehicle_occupancy_percentage),
        }
    }

    pub fn is_stop_skipped(&self, trip_index: usize, stop_pos: usize) -> bool {
        self.stop_deltas
            .get(&(trip_index, stop_pos))
            .map(|value| value.skipped)
            .unwrap_or(false)
    }

    pub fn actual_arrival(&self, trips: &[TripRecord], trip_index: usize, stop_pos: usize) -> i32 {
        let trip = &trips[trip_index];
        let scheduled = trip.stop_times[stop_pos].arrival_secs;
        self.stop_deltas
            .get(&(trip_index, stop_pos))
            .and_then(|value| value.arrival_delay_secs.map(|delay| scheduled + delay))
            .unwrap_or(scheduled)
    }

    pub fn actual_departure(
        &self,
        trips: &[TripRecord],
        trip_index: usize,
        stop_pos: usize,
    ) -> i32 {
        let trip = &trips[trip_index];
        let scheduled = trip.stop_times[stop_pos].departure_secs;
        self.stop_deltas
            .get(&(trip_index, stop_pos))
            .and_then(|value| {
                value
                    .departure_delay_secs
                    .or(value.arrival_delay_secs)
                    .map(|delay| scheduled + delay)
            })
            .unwrap_or(scheduled)
    }

    async fn refresh_trip_updates(
        &self,
        static_data: &StaticData,
        feed: &FeedConfig,
    ) -> Result<TripRefreshOutcome> {
        let url = feed
            .trip_updates_url
            .as_ref()
            .ok_or_else(|| anyhow!("trip updates URL not configured"))?;
        let request = maybe_add_internal_token_async(self.client.get(url), url)?;
        let bytes = self
            .client
            .execute(request.build().context("failed to build trip updates request")?)
            .await
            .context("trip updates request failed")?
            .error_for_status()
            .context("trip updates endpoint returned an error status")?
            .bytes()
            .await
            .context("trip updates body download failed")?;

        let feed_message =
            FeedMessage::decode(bytes.as_ref()).context("trip updates protobuf decode failed")?;

        let trip_feed_timestamp = feed_message.header.timestamp;
        let mut trip_entities = 0usize;

        for entity in feed_message.entity {
            let Some(trip_update) = entity.trip_update else {
                continue;
            };
            trip_entities += 1;
            let trip_descriptor = &trip_update.trip;
            let Some(local_trip_id) = trip_descriptor.trip_id.as_ref() else {
                continue;
            };
            let Some(trip_index) = static_data
                .trip_lookup_by_feed
                .get(usize::from(feed.feed_index))
                .and_then(|lookup| lookup.get(local_trip_id))
                .copied()
            else {
                continue;
            };

            self.note_trip_update(trip_index, trip_descriptor.schedule_relationship);

            if trip_descriptor.schedule_relationship
                == Some(trip_descriptor::ScheduleRelationship::Canceled as i32)
            {
                self.canceled_trips.insert(trip_index, ());
                continue;
            }

            let trip = &static_data.trips[trip_index];
            let mut stop_update_positions = Vec::<(usize, trip_update::StopTimeUpdate)>::new();
            for stop_update in trip_update.stop_time_update {
                if let Some(position) =
                    resolve_stop_update_position(static_data, trip, &stop_update)
                {
                    stop_update_positions.push((position, stop_update));
                }
            }
            stop_update_positions.sort_by_key(|(position, _)| *position);

            let mut arrival_delay = sanitize_delay(trip_update.delay);
            let mut departure_delay = sanitize_delay(trip_update.delay);
            let mut next_update = 0usize;

            for stop_pos in 0..trip.stop_times.len() {
                if let Some((position, update)) = stop_update_positions.get(next_update) {
                    if *position == stop_pos {
                        if let Some(delay) = stop_event_delay(
                            update.arrival.as_ref(),
                            trip.stop_times[stop_pos].arrival_secs,
                        ) {
                            arrival_delay = Some(delay);
                        }
                        if let Some(delay) = stop_event_delay(
                            update.departure.as_ref(),
                            trip.stop_times[stop_pos].departure_secs,
                        ) {
                            departure_delay = Some(delay);
                        }
                        let skipped = update.schedule_relationship
                            == Some(trip_update::stop_time_update::ScheduleRelationship::Skipped as i32);
                        self.stop_deltas.insert(
                            (trip_index, stop_pos),
                            StopDelta {
                                arrival_delay_secs: arrival_delay,
                                departure_delay_secs: departure_delay,
                                skipped,
                                departure_occupancy_status: update.departure_occupancy_status,
                            },
                        );
                        next_update += 1;
                        continue;
                    }
                }

                if arrival_delay.is_some() || departure_delay.is_some() {
                    self.stop_deltas.insert(
                        (trip_index, stop_pos),
                        StopDelta {
                            arrival_delay_secs: arrival_delay,
                            departure_delay_secs: departure_delay,
                            skipped: false,
                            departure_occupancy_status: None,
                        },
                    );
                }
            }
        }

        Ok(TripRefreshOutcome {
            trip_entities,
            trip_feed_timestamp,
        })
    }

    async fn refresh_vehicle_positions(
        &self,
        static_data: &StaticData,
        feed: &FeedConfig,
    ) -> Result<VehicleRefreshOutcome> {
        let url = feed
            .vehicle_positions_url
            .as_ref()
            .ok_or_else(|| anyhow!("vehicle positions URL not configured"))?;
        let request = maybe_add_internal_token_async(self.client.get(url), url)?;
        let bytes = self
            .client
            .execute(request.build().context("failed to build vehicle positions request")?)
            .await
            .context("vehicle positions request failed")?
            .error_for_status()
            .context("vehicle positions endpoint returned an error status")?
            .bytes()
            .await
            .context("vehicle positions body download failed")?;

        let feed_message = FeedMessage::decode(bytes.as_ref())
            .context("vehicle positions protobuf decode failed")?;
        let vehicle_feed_timestamp = feed_message.header.timestamp;

        let mut vehicles = Vec::<VehicleSnapshot>::new();
        let mut vehicle_entities = 0usize;
        for entity in feed_message.entity {
            let Some(vehicle) = entity.vehicle else {
                continue;
            };
            vehicle_entities += 1;
            let schedule_relationship = vehicle
                .trip
                .as_ref()
                .and_then(|trip| trip.schedule_relationship);
            let (occupancy_status, occupancy_percentage) = resolve_vehicle_occupancy(&vehicle);
            if let Some(local_trip_id) = vehicle
                .trip
                .as_ref()
                .and_then(|trip| trip.trip_id.as_ref())
            {
                if let Some(trip_index) = static_data
                    .trip_lookup_by_feed
                    .get(usize::from(feed.feed_index))
                    .and_then(|lookup| lookup.get(local_trip_id))
                    .copied()
                {
                    self.note_vehicle_position(
                        trip_index,
                        schedule_relationship,
                        occupancy_status,
                        occupancy_percentage,
                        vehicle.timestamp,
                    );
                }
            }
            vehicles.push(VehicleSnapshot {
                feed_id: feed.id.clone(),
                entity_id: entity.id,
                trip_id: vehicle
                    .trip
                    .as_ref()
                    .and_then(|trip| trip.trip_id.clone())
                    .map(|trip_id| scoped_id(&feed.id, &trip_id)),
                vehicle_id: vehicle.vehicle.as_ref().and_then(|meta| meta.id.clone()),
                vehicle_label: vehicle.vehicle.as_ref().and_then(|meta| meta.label.clone()),
                latitude: vehicle
                    .position
                    .as_ref()
                    .map(|position| position.latitude as f64),
                longitude: vehicle
                    .position
                    .as_ref()
                    .map(|position| position.longitude as f64),
                bearing: vehicle
                    .position
                    .as_ref()
                    .and_then(|position| position.bearing),
                speed: vehicle
                    .position
                    .as_ref()
                    .and_then(|position| position.speed),
                current_stop_sequence: vehicle.current_stop_sequence,
                stop_id: vehicle.stop_id.map(|stop_id| scoped_id(&feed.id, &stop_id)),
                current_status_code: vehicle.current_status,
                schedule_relationship: schedule_relationship
                    .and_then(schedule_relationship_name)
                    .map(str::to_owned),
                occupancy_status: occupancy_status
                    .and_then(occupancy_status_name)
                    .map(str::to_owned),
                occupancy_percentage,
                timestamp: vehicle.timestamp,
            });
        }

        Ok(VehicleRefreshOutcome {
            vehicle_entities,
            vehicle_feed_timestamp,
            vehicles,
        })
    }

    fn note_trip_update(&self, trip_index: usize, schedule_relationship: Option<i32>) {
        let mut entry = self.trip_metrics.entry(trip_index).or_default();
        entry.has_trip_update = true;
        merge_schedule_relationship(&mut entry.schedule_relationship, schedule_relationship);
    }

    fn note_vehicle_position(
        &self,
        trip_index: usize,
        schedule_relationship: Option<i32>,
        occupancy_status: Option<i32>,
        occupancy_percentage: Option<u32>,
        timestamp: Option<u64>,
    ) {
        let mut entry = self.trip_metrics.entry(trip_index).or_default();
        entry.has_vehicle_position = true;
        merge_schedule_relationship(&mut entry.schedule_relationship, schedule_relationship);

        if should_replace_vehicle_snapshot(entry.vehicle_timestamp, timestamp) {
            if occupancy_status.is_some() {
                entry.vehicle_occupancy_status = occupancy_status;
            }
            if occupancy_percentage.is_some() {
                entry.vehicle_occupancy_percentage = occupancy_percentage;
            }
            entry.vehicle_timestamp = timestamp.or(entry.vehicle_timestamp);
        } else {
            if entry.vehicle_occupancy_status.is_none() {
                entry.vehicle_occupancy_status = occupancy_status;
            }
            if entry.vehicle_occupancy_percentage.is_none() {
                entry.vehicle_occupancy_percentage = occupancy_percentage;
            }
        }
    }
}

fn resolve_stop_update_position(
    static_data: &StaticData,
    trip: &TripRecord,
    update: &trip_update::StopTimeUpdate,
) -> Option<usize> {
    if let Some(stop_sequence) = update.stop_sequence {
        if let Some((position, _)) = trip
            .stop_times
            .iter()
            .enumerate()
            .find(|(_, stop_time)| stop_time.stop_sequence == stop_sequence)
        {
            return Some(position);
        }
    }

    update.stop_id.as_ref().and_then(|stop_id| {
        trip.stop_times.iter().position(|stop_time| {
            let stop = &static_data.stops[stop_time.stop_index];
            stop.local_id == *stop_id || stop.id == *stop_id
        })
    })
}

fn resolve_vehicle_occupancy(vehicle: &gtfs_rt::VehiclePosition) -> (Option<i32>, Option<u32>) {
    let occupancy_percentage = vehicle
        .occupancy_percentage
        .or_else(|| average_carriage_occupancy_percentage(&vehicle.multi_carriage_details));
    let occupancy_status = vehicle
        .occupancy_status
        .or_else(|| worst_carriage_occupancy_status(&vehicle.multi_carriage_details));
    (occupancy_status, occupancy_percentage)
}

fn average_carriage_occupancy_percentage(
    carriages: &[vehicle_position::CarriageDetails],
) -> Option<u32> {
    let percentages = carriages
        .iter()
        .filter_map(|carriage| carriage.occupancy_percentage)
        .filter(|percentage| *percentage >= 0)
        .map(|percentage| percentage as u32)
        .collect::<Vec<_>>();
    if percentages.is_empty() {
        return None;
    }

    let total = percentages.iter().copied().sum::<u32>();
    Some(total / percentages.len() as u32)
}

fn worst_carriage_occupancy_status(
    carriages: &[vehicle_position::CarriageDetails],
) -> Option<i32> {
    carriages
        .iter()
        .filter_map(|carriage| carriage.occupancy_status)
        .max_by_key(|status| occupancy_status_severity(*status))
}

fn occupancy_status_severity(status: i32) -> u8 {
    match vehicle_position::OccupancyStatus::from_i32(status) {
        Some(vehicle_position::OccupancyStatus::Empty) => 0,
        Some(vehicle_position::OccupancyStatus::ManySeatsAvailable) => 1,
        Some(vehicle_position::OccupancyStatus::FewSeatsAvailable) => 2,
        Some(vehicle_position::OccupancyStatus::StandingRoomOnly) => 3,
        Some(vehicle_position::OccupancyStatus::CrushedStandingRoomOnly) => 4,
        Some(vehicle_position::OccupancyStatus::Full) => 5,
        Some(vehicle_position::OccupancyStatus::NotAcceptingPassengers) => 6,
        Some(vehicle_position::OccupancyStatus::NotBoardable) => 7,
        Some(vehicle_position::OccupancyStatus::NoDataAvailable) => 8,
        None => 255,
    }
}

fn merge_schedule_relationship(target: &mut Option<i32>, incoming: Option<i32>) {
    let Some(incoming) = incoming else {
        return;
    };
    let scheduled = trip_descriptor::ScheduleRelationship::Scheduled as i32;
    let canceled = trip_descriptor::ScheduleRelationship::Canceled as i32;
    match target {
        None => *target = Some(incoming),
        Some(current) if *current == canceled => {}
        Some(current) if incoming == canceled || *current == scheduled => {
            *target = Some(incoming);
        }
        Some(_) => {}
    }
}

fn should_replace_vehicle_snapshot(current: Option<u64>, next: Option<u64>) -> bool {
    match (current, next) {
        (None, Some(_)) => true,
        (Some(current), Some(next)) => next >= current,
        (None, None) => true,
        (Some(_), None) => false,
    }
}

fn schedule_relationship_name(value: i32) -> Option<&'static str> {
    trip_descriptor::ScheduleRelationship::from_i32(value)
        .map(|relationship| relationship.as_str_name())
}

fn occupancy_status_name(value: i32) -> Option<&'static str> {
    vehicle_position::OccupancyStatus::from_i32(value)
        .map(|status| status.as_str_name())
}

fn stop_event_delay(
    event: Option<&gtfs_rt::trip_update::StopTimeEvent>,
    _scheduled_secs: i32,
) -> Option<i32> {
    event.and_then(|event| sanitize_delay(event.delay))
}

fn sanitize_delay(delay: Option<i32>) -> Option<i32> {
    delay.filter(|value| value.abs() <= MAX_REASONABLE_DELAY_SECS)
}

fn scoped_id(feed_id: &str, local_id: &str) -> String {
    format!("{feed_id}:{local_id}")
}

fn max_option_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}
