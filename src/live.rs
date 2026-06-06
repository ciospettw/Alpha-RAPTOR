use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use dashmap::DashMap;
use futures::{sink::SinkExt, stream::StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::AppState;
use crate::engine::{QueryRequest, Engine, LegResponse, PolylinePoint, WalkDirection};

static NEXT_SOCKET_ID: AtomicUsize = AtomicUsize::new(1);

pub type SocketId = usize;
pub type ActiveDependencyMatrix = Arc<DashMap<usize, Vec<SocketId>>>; // Trip Index -> Vec<SocketId>

#[derive(Clone, Debug)]
pub struct UserSession {
    pub sender: mpsc::Sender<serde_json::Value>,
    pub original_query: Option<QueryRequest>,
    pub original_legs: Option<Vec<LegResponse>>,
    pub user_lat: Option<f64>,
    pub user_lon: Option<f64>,
    pub current_leg_index: Option<usize>,
    pub trip_indices: Option<Vec<usize>>,
}
pub type ClientRegistry = Arc<DashMap<SocketId, UserSession>>;

#[derive(Deserialize, Debug, Clone)]
pub struct SubscribeMessage {
    pub trip_indices: Option<Vec<usize>>,
    pub original_query: Option<QueryRequest>,
    pub original_legs: Option<Vec<LegResponse>>,
    pub user_lat: Option<f64>,
    pub user_lon: Option<f64>,
    pub current_leg_index: Option<usize>,
}

#[derive(Serialize)]
pub struct HealingMessage {
    pub r#type: String,
    pub reason: String,
    pub events: Vec<UxEvent>,
    pub new_itinerary: serde_json::Value,
    pub affected_leg_index: Option<usize>,
}

#[derive(Serialize, Clone, Debug)]
pub struct UxEvent {
    pub r#type: String,
    pub message: String,
    pub leg_index: Option<usize>,
    pub stops_remaining: Option<usize>,
    pub alighting_stop_name: Option<String>,
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371000.0; // meters
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let delta_phi = (lat2 - lat1).to_radians();
    let delta_lambda = (lon2 - lon1).to_radians();
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

async fn detect_boarding_and_stops(
    state: &AppState,
    socket_id: SocketId,
    user_lat: f64,
    user_lon: f64,
    current_leg_index: usize,
) {
    let (original_legs, sender) = match state.client_registry.get(&socket_id) {
        Some(session) => (session.original_legs.clone(), session.sender.clone()),
        None => return,
    };

    let legs = match original_legs {
        Some(l) => l,
        None => return,
    };

    // 1. Boarding Detection
    let mut next_transit_idx = None;
    for (i, leg) in legs.iter().enumerate() {
        if i >= current_leg_index && leg.kind == "transit" {
            next_transit_idx = Some(i);
            break;
        }
    }

    if let Some(transit_idx) = next_transit_idx {
        let leg = &legs[transit_idx];
        let mut boarded = false;

        if let Some(trip_id) = &leg.trip_id {
            let vehicles = state.engine.current().realtime.vehicles();
            let vehicle = vehicles.iter().find(|v| {
                v.trip_id.as_deref() == Some(trip_id.as_str()) ||
                v.trip_id.as_deref().map(|id| id.contains(trip_id)).unwrap_or(false)
            });

            if let Some(veh) = vehicle {
                if let (Some(v_lat), Some(v_lon)) = (veh.latitude, veh.longitude) {
                    let dist = haversine_distance(user_lat, user_lon, v_lat, v_lon);
                    // Boarding detected if user is within 80 meters
                    if dist < 80.0 {
                        boarded = true;
                    }
                }
            }
        }

        // Simulate boarding for scheduled legs that don't have GTFS-RT
        if !boarded {
            if let Ok(dep_time) = chrono::NaiveDateTime::parse_from_str(&leg.departure_time, "%Y-%m-%d %H:%M:%S") {
                if chrono::Local::now().naive_local() >= dep_time {
                    if !leg.has_realtime_update {
                        boarded = true;
                    }
                }
            }
        }

        if boarded {
            info!(socket_id, transit_idx, "Transit vehicle boarding detected/simulated");
            let msg = serde_json::json!({
                "type": "BOARDED",
                "leg_index": transit_idx,
                "trip_id": leg.trip_id,
            });
            let _ = sender.send(msg).await;

            // Update current_leg_index in registry to avoid repeatedly sending BOARDED
            if let Some(mut session) = state.client_registry.get_mut(&socket_id) {
                session.current_leg_index = Some(transit_idx);
            }
            return;
        }
    }

    // 2. Current Stop Update
    if current_leg_index < legs.len() && legs[current_leg_index].kind == "transit" {
        let leg = &legs[current_leg_index];
        let mut stops_seq = Vec::new();
        
        if let (Some(lat), Some(lon)) = (leg.from_stop.latitude, leg.from_stop.longitude) {
            stops_seq.push((&leg.from_stop.name, lat, lon));
        }
        for stop in &leg.intermediate_stops {
            if let (Some(lat), Some(lon)) = (stop.latitude, stop.longitude) {
                stops_seq.push((&stop.name, lat, lon));
            }
        }
        if let (Some(lat), Some(lon)) = (leg.to_stop.latitude, leg.to_stop.longitude) {
            stops_seq.push((&leg.to_stop.name, lat, lon));
        }

        if !stops_seq.is_empty() {
            let mut closest_stop_idx = 0;
            let mut min_dist = f64::MAX;

            for (idx, &(_, s_lat, s_lon)) in stops_seq.iter().enumerate() {
                let dist = haversine_distance(user_lat, user_lon, s_lat, s_lon);
                if dist < min_dist {
                    min_dist = dist;
                    closest_stop_idx = idx;
                }
            }

            let current_stop_name = stops_seq[closest_stop_idx].0;
            let stops_remaining = stops_seq.len().saturating_sub(closest_stop_idx + 1);

            let msg = serde_json::json!({
                "type": "STOP_UPDATE",
                "current_stop_name": current_stop_name,
                "stops_remaining": stops_remaining,
            });
            let _ = sender.send(msg).await;
        }
    }
}

pub async fn live_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let socket_id = NEXT_SOCKET_ID.fetch_add(1, Ordering::Relaxed);
    info!(socket_id, "WebSocket client connected");

    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<serde_json::Value>(32);

    // Add client to registry
    state.client_registry.insert(
        socket_id,
        UserSession {
            sender: tx,
            original_query: None,
            original_legs: None,
            user_lat: None,
            user_lon: None,
            current_leg_index: None,
            trip_indices: None,
        },
    );

    // Task to send messages from tx to the WebSocket
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Ok(json_str) = serde_json::to_string(&msg) {
                if sender.send(Message::Text(json_str.into())).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task to receive messages from the WebSocket
    let local_state = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            if let Ok(sub) = serde_json::from_str::<SubscribeMessage>(&text) {
                if let Some(trip_indices) = &sub.trip_indices {
                    info!(socket_id, trips = ?trip_indices, "client subscribed to trips");
                    
                    // Update original query and initial state in registry
                    if let Some(mut session) = local_state.client_registry.get_mut(&socket_id) {
                        session.original_query = sub.original_query.clone();
                        session.original_legs = sub.original_legs.clone();
                        session.trip_indices = Some(trip_indices.clone());
                        session.user_lat = sub.user_lat;
                        session.user_lon = sub.user_lon;
                        session.current_leg_index = sub.current_leg_index;
                    }

                    // Register in ADM
                    for &trip_idx in trip_indices {
                        local_state
                            .adm
                            .entry(trip_idx)
                            .or_default()
                            .push(socket_id);
                    }
                } else if sub.user_lat.is_some() && sub.user_lon.is_some() {
                    let lat = sub.user_lat.unwrap();
                    let lon = sub.user_lon.unwrap();
                    let leg_idx = sub.current_leg_index.unwrap_or(0);

                    if let Some(mut session) = local_state.client_registry.get_mut(&socket_id) {
                        session.user_lat = Some(lat);
                        session.user_lon = Some(lon);
                        session.current_leg_index = Some(leg_idx);
                    }

                    detect_boarding_and_stops(&local_state, socket_id, lat, lon, leg_idx).await;
                }
            }
        }
    });

    // If any task completes, abort the other.
    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    info!(socket_id, "WebSocket client disconnected");
    if let Some((_, session)) = state.client_registry.remove(&socket_id) {
        if let Some(trip_indices) = session.trip_indices {
            for trip_idx in trip_indices {
                if let Some(mut sockets) = state.adm.get_mut(&trip_idx) {
                    sockets.retain(|&id| id != socket_id);
                }
            }
        }
    }
}

pub fn spawn_healing_worker(state: AppState) {
    let mut rx = state.engine.trip_events.subscribe();
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(changed_trip_indices) => {
                    for trip_idx in changed_trip_indices {
                        if let Some(sockets) = state.adm.get(&trip_idx) {
                            let mut dead_sockets = vec![];
                            for &socket_id in sockets.iter() {
                                if let Some(session) = state.client_registry.get(&socket_id) {
                                    info!(socket_id, trip_idx, "triggering AIH (Asynchronous In-Flight Healing)");
                                    
                                    // 1. Locazione Cinematica Implicita
                                    // Simplification: In a full implementation, we'd interpolate the exact stop.
                                    // For now, we rerun the original query with updated tau_now (System time)
                                    // or a modified start node if they are mid-trip.
                                    
                                    if let Some(original_query) = &session.original_query {
                                        let engine = state.engine.current();
                                        
                                        // Execute ricalcolo asincrono on thread pool
                                        let mut new_query = original_query.clone();
                                        let now = chrono::Local::now();
                                        new_query.time = now.format("%H:%M:%S").to_string();
                                        new_query.date = now.format("%Y-%m-%d").to_string();

                                        let tx = session.sender.clone();
                                        let reason = format!("Trip_{} altered", trip_idx);
                                        let original_legs = session.original_legs.clone();

                                        tokio::spawn(async move {
                                            if let Ok(result) = engine.run_query(new_query).await {
                                                let mut events = Vec::new();
                                                if let Some(old_legs) = original_legs {
                                                    events = diff_itineraries(&old_legs, &result.legs);
                                                }
                                                
                                                let healing_msg = HealingMessage {
                                                    r#type: "HEALING".into(),
                                                    reason,
                                                    events,
                                                    new_itinerary: serde_json::to_value(&result).unwrap_or_default(),
                                                    affected_leg_index: None, // Could be determined if we knew exactly which leg changed
                                                };
                                                if let Ok(json) = serde_json::to_value(healing_msg) {
                                                    let _ = tx.send(json).await;
                                                }
                                            }
                                        });
                                    }
                                } else {
                                    dead_sockets.push(socket_id);
                                }
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "healing worker lagged behind broadcast events");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });
}

fn diff_itineraries(old_legs: &[LegResponse], new_legs: &[LegResponse]) -> Vec<UxEvent> {
    let mut events = Vec::new();

    let old_transit: Vec<(usize, &LegResponse)> = old_legs.iter().enumerate().filter(|(_, l)| l.kind == "transit").collect();
    let new_transit: Vec<(usize, &LegResponse)> = new_legs.iter().enumerate().filter(|(_, l)| l.kind == "transit").collect();

    // Check for canceled or missing legs
    for &(old_idx, old_leg) in &old_transit {
        let mut found = false;
        for &(new_idx, new_leg) in &new_transit {
            if old_leg.trip_id == new_leg.trip_id {
                found = true;
                
                // Compare stops for EARLY_ALIGHT
                if old_leg.to_stop.id != new_leg.to_stop.id {
                    let stops_remaining = new_leg.intermediate_stops.len();
                    events.push(UxEvent {
                        r#type: "EARLY_ALIGHT".into(),
                        message: format!(
                            "Attenzione: scendi a {} anziché a {} per non perdere le coincidenze.",
                            new_leg.to_stop.name, old_leg.to_stop.name
                        ),
                        leg_index: Some(new_idx),
                        stops_remaining: Some(stops_remaining),
                        alighting_stop_name: Some(new_leg.to_stop.name.clone()),
                    });
                }

                // Check for delays
                let old_arr = old_leg.arrival_time.clone();
                let new_arr = new_leg.arrival_time.clone();
                if new_arr > old_arr {
                    events.push(UxEvent {
                        r#type: "DELAY_WARNING".into(),
                        message: format!(
                            "Il bus {} arriverà a {} alle {} anziché alle {}.",
                            new_leg.route_label.as_deref().unwrap_or(""),
                            new_leg.to_stop.name,
                            new_arr, old_arr
                        ),
                        leg_index: Some(new_idx),
                        stops_remaining: None,
                        alighting_stop_name: None,
                    });
                }
                break;
            }
        }
        
        if !found {
            // trip not found in new itinerary
            events.push(UxEvent {
                r#type: "TRIP_CANCELED".into(),
                message: format!(
                    "La tua corsa {} è stata soppressa o non è più valida.",
                    old_leg.route_label.as_deref().unwrap_or(""),
                ),
                leg_index: Some(old_idx),
                stops_remaining: None,
                alighting_stop_name: None,
            });
        }
    }

    // Check for newly introduced alternatives
    for &(new_idx, new_leg) in &new_transit {
        let mut is_new = true;
        for &(_, old_leg) in &old_transit {
            if old_leg.trip_id == new_leg.trip_id {
                is_new = false;
                break;
            }
        }
        if is_new {
            events.push(UxEvent {
                r#type: "ROUTE_ALTERNATIVE".into(),
                message: format!(
                    "Prendi il {} in direzione {}.",
                    new_leg.route_label.as_deref().unwrap_or("bus"),
                    new_leg.headsign.as_deref().unwrap_or("sconosciuta")
                ),
                leg_index: Some(new_idx),
                stops_remaining: None,
                alighting_stop_name: None,
            });
        }
    }

    events
}
