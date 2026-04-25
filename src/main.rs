mod cold_storage;
mod engine;
mod geo;
mod hpf;
mod profile_cache;
mod realtime;
mod walker;

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tower_http::{services::ServeDir, trace::TraceLayer};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::engine::{Engine, EngineConfig, QueryRequest};

#[derive(Clone)]
struct AppState {
    engine: SharedEngine,
}

#[derive(Clone)]
struct SharedEngine {
    current: Arc<ArcSwap<Engine>>,
}

impl SharedEngine {
    fn new(engine: Engine) -> Self {
        Self {
            current: Arc::new(ArcSwap::from_pointee(engine)),
        }
    }

    fn current(&self) -> Arc<Engine> {
        self.current.load_full()
    }

    fn swap(&self, engine: Engine) {
        self.current.store(Arc::new(engine));
    }
}

#[derive(Debug, Deserialize)]
struct StopSearchParams {
    q: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RealtimeQueryParams {
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let startup_started = std::time::Instant::now();

    let workspace_root = env::current_dir().context("unable to resolve workspace root")?;
    let bind = env::var("ALPHA_BIND")
        .unwrap_or_else(|_| "0.0.0.0:7878".to_owned())
        .parse::<SocketAddr>()
        .context("ALPHA_BIND must be a valid socket address")?;
    let engine_load_started = std::time::Instant::now();
    let engine = load_engine_from_workspace(workspace_root.clone())
        .await
        .context("failed to initialize Alpha-RAPTOR engine")?;
    let engine_load_ms = engine_load_started.elapsed().as_millis();
    let build_snapshot = engine.stats().build;
    info!(
        engine_load_ms,
        build_millis = build_snapshot.build_millis,
        static_cache_hit = build_snapshot.static_cache_hit,
        walk_cache_hit = build_snapshot.walk_cache_hit,
        static_cache_read_ms = build_snapshot.timings.static_cache_read_ms,
        gtfs_parse_ms = build_snapshot.timings.gtfs_parse_ms,
        transit_model_ms = build_snapshot.timings.transit_model_ms,
        static_cache_write_ms = build_snapshot.timings.static_cache_write_ms,
        walker_ms = build_snapshot.timings.walker_ms,
        "engine load completed"
    );

    let realtime_refresh_started = std::time::Instant::now();
    match engine.refresh_realtime().await {
        Ok(snapshot) => info!(
            shadow_delta_count = snapshot.shadow_delta_count,
            vehicle_count = snapshot.vehicle_count,
            realtime_refresh_ms = realtime_refresh_started.elapsed().as_millis(),
            "initial realtime refresh completed"
        ),
        Err(error) => warn!(%error, "initial realtime refresh failed"),
    }

    let shared_engine = SharedEngine::new(engine.clone());
    spawn_realtime_refresh(shared_engine.clone());
    spawn_static_reload(shared_engine.clone());
    spawn_hpf_overlay_refresh(shared_engine.clone());

    let public_dir = workspace_root.join("public");
    let app = Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/stats", get(stats))
        .route("/api/stops", get(search_stops))
        .route("/api/query", get(run_query))
        .route("/api/realtime", get(realtime_snapshot))
        .route("/api/realtime/refresh", post(refresh_realtime))
        .nest_service("/assets", ServeDir::new(public_dir))
        .layer(TraceLayer::new_for_http())
        .with_state(AppState {
            engine: shared_engine.clone(),
        });

    info!(address = %bind, "Alpha-RAPTOR debug server listening");
    info!(
        ui = %format!("http://{bind}"),
        manifest = ?engine.config.manifest_path.as_ref().map(|path| path.display().to_string()),
        feed_count = engine.config.feeds.len(),
        static_poll_secs = engine.config.static_reload_interval_secs,
        osm_pbf = %engine.config.osm_pbf_path.display(),
        osm_pbf_source = %engine.config.osm_pbf_source,
        osm_diff_state_url = ?engine.config.osm_diff.as_ref().map(|value| value.state_url.clone()),
        osm_diff_poll_secs = ?engine.config.osm_diff.as_ref().map(|value| value.poll_interval_secs),
        static_gtfs = %engine.config.static_sources_display(),
        trip_updates = %engine.config.trip_updates_display(),
        vehicle_positions = %engine.config.vehicle_positions_display(),
        startup_ms = startup_started.elapsed().as_millis(),
        "runtime configuration"
    );

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .context("failed to bind TCP listener")?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server exited with an error")?;

    Ok(())
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "alpha_raptor_engine=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn load_engine_from_workspace(workspace_root: PathBuf) -> Result<Engine> {
    tokio::task::spawn_blocking(move || {
        let config = EngineConfig::from_env(workspace_root)?;
        Engine::load(config)
    })
    .await
    .context("engine bootstrap task panicked")?
}

fn spawn_realtime_refresh(engine: SharedEngine) {
    tokio::spawn(async move {
        loop {
            let refresh_every = Duration::from_secs(engine.current().config.refresh_interval_secs);
            tokio::time::sleep(refresh_every).await;
            let current_engine = engine.current();
            if let Err(error) = current_engine.refresh_realtime().await {
                warn!(%error, "background realtime refresh failed");
            }
        }
    });
}

fn spawn_static_reload(engine: SharedEngine) {
    tokio::spawn(async move {
        let mut first_cycle = true;
        loop {
            if first_cycle {
                first_cycle = false;
            } else {
                let poll_every =
                    Duration::from_secs(engine.current().config.static_reload_interval_secs);
                tokio::time::sleep(poll_every).await;
            }

            let current_engine = engine.current();
            let current_config = current_engine.config.clone();
            let next_config = match tokio::task::spawn_blocking(move || {
                current_config.reload_from_source()
            })
            .await
            {
                Ok(Ok(config)) => config,
                Ok(Err(error)) => {
                    warn!(%error, "failed to reload engine configuration for static polling");
                    continue;
                }
                Err(error) => {
                    warn!(%error, "static polling configuration task panicked");
                    continue;
                }
            };

            if !current_engine.config.static_inputs_changed(&next_config) {
                continue;
            }

            info!(
                sources = %next_config.static_sources_display(),
                "detected static GTFS change, rebuilding engine in background"
            );
            let rebuild_started = std::time::Instant::now();
            let previous_engine = current_engine.clone();
            match tokio::task::spawn_blocking(move || {
                Engine::reload_from_previous(previous_engine.as_ref(), next_config)
            })
            .await
            {
                Ok(Ok(next_engine)) => {
                    if let Err(error) = next_engine.refresh_realtime().await {
                        warn!(%error, "reloaded engine initialized but realtime prewarm failed");
                    }
                    let next_stats = next_engine.stats().build;
                    engine.swap(next_engine);
                    info!(
                        reload_ms = rebuild_started.elapsed().as_millis(),
                        static_cache_hit = next_stats.static_cache_hit,
                        walk_cache_hit = next_stats.walk_cache_hit,
                        build_millis = next_stats.build_millis,
                        "static engine swap completed"
                    );
                }
                Ok(Err(error)) => {
                    warn!(%error, "static GTFS change detected but rebuild failed; keeping current engine");
                }
                Err(error) => {
                    warn!(%error, "static GTFS rebuild task panicked; keeping current engine");
                }
            }
        }
    });
}

fn spawn_hpf_overlay_refresh(engine: SharedEngine) {
    tokio::spawn(async move {
        let mut first_cycle = true;
        loop {
            let poll_every = match engine.current().config.osm_diff.as_ref() {
                Some(config) => Duration::from_secs(config.poll_interval_secs),
                None => Duration::from_secs(300),
            };

            if first_cycle {
                first_cycle = false;
            } else {
                tokio::time::sleep(poll_every).await;
            }

            let current_engine = engine.current();
            if current_engine.config.osm_diff.is_none() {
                continue;
            }

            match current_engine.refresh_hpf_overlay().await {
                Ok(Some(snapshot)) => {
                    info!(
                        applied_sequence = ?snapshot.applied_sequence,
                        overlay_cells = snapshot.overlay_cells,
                        blocked_cells = snapshot.blocked_cells,
                        synthetic_cells = snapshot.synthetic_cells,
                        "background HPF overlay refresh completed"
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(%error, "background HPF overlay refresh failed");
                }
            }
        }
    });
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        error!(%error, "failed to install Ctrl+C handler");
    }
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../public/index.html"))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn stats(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.engine.current().stats())
}

async fn search_stops(
    State(state): State<AppState>,
    Query(params): Query<StopSearchParams>,
) -> impl IntoResponse {
    let query = params.q.unwrap_or_default();
    let limit = params.limit.unwrap_or(12).clamp(1, 50);
    Json(state.engine.current().search_stops(&query, limit))
}

async fn run_query(
    State(state): State<AppState>,
    Query(params): Query<QueryRequest>,
) -> impl IntoResponse {
    match state.engine.current().run_query(params).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(error) => json_error(StatusCode::BAD_REQUEST, error.to_string()),
    }
}

async fn realtime_snapshot(
    State(state): State<AppState>,
    Query(params): Query<RealtimeQueryParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(24).clamp(1, 200);
    Json(state.engine.current().realtime_snapshot(limit))
}

async fn refresh_realtime(State(state): State<AppState>) -> impl IntoResponse {
    match state.engine.current().refresh_realtime().await {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(error) => json_error(StatusCode::BAD_GATEWAY, error.to_string()),
    }
}

fn json_error(status: StatusCode, message: String) -> axum::response::Response {
    (status, Json(ErrorBody { error: message })).into_response()
}
