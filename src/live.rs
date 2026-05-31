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
}
pub type ClientRegistry = Arc<DashMap<SocketId, UserSession>>;

#[derive(Deserialize, Debug, Clone)]
pub struct SubscribeMessage {
    pub trip_indices: Vec<usize>,
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
                info!(socket_id, trips = ?sub.trip_indices, "client subscribed to trips");
                
                // Update original query in registry
                if let Some(mut session) = local_state.client_registry.get_mut(&socket_id) {
                    session.original_query = sub.original_query.clone();
                    session.original_legs = sub.original_legs.clone();
                }

                // Register in ADM
                for trip_idx in sub.trip_indices {
                    local_state
                        .adm
                        .entry(trip_idx)
                        .or_default()
                        .push(socket_id);
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
    state.client_registry.remove(&socket_id);
    
    // We do not eagerly clean up ADM right now for simplicity (it's lock-free and fast enough),
    // but in a production environment, we might remove socket_id from the vectors.
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
