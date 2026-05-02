# α-RAPTOR

[![Rust](https://img.shields.io/badge/Rust-2024%20edition-black?logo=rust)](https://www.rust-lang.org/)
[![HTTP API](https://img.shields.io/badge/API-Axum-0f766e)](https://github.com/tokio-rs/axum)
[![Config](https://img.shields.io/badge/config-TOML-blue)](./alpha-raptor.toml)
[![OSM](https://img.shields.io/badge/OSM-GeoFabrik%20diffs%20supported-3b82f6)](https://download.geofabrik.de/)
[![GTFS](https://img.shields.io/badge/feeds-GTFS%20%2B%20GTFS--RT-orange)](https://gtfs.org/)
[![License](https://img.shields.io/badge/license-CC%20BY--NC%204.0-lightgrey)](./LICENSE)

α-RAPTOR is a local multimodal routing engine for GTFS, GTFS-Realtime, and OSM-based pedestrian access. It exposes an HTTP API, a small built-in web UI, and a manifest-driven runtime that can hot-reload static GTFS sources and refresh realtime feeds in background.

The repository also contains the architecture notes for the core subsystems under [docs/btt.md](docs/btt.md), [docs/hpf.md](docs/hpf.md), [docs/hydra-slab.md](docs/hydra-slab.md), [docs/svrt.md](docs/svrt.md), and [docs/temporal-indirection.md](docs/temporal-indirection.md).

## Features

- Multi-feed static GTFS loading from local files or remote ZIP URLs
- Background GTFS static polling and atomic hot swap with no process restart
- GTFS-Realtime trip updates and vehicle positions refresh in background
- Coordinate-to-coordinate queries with local OSM-backed walking connectors
- Built-in HPF cache for first/last mile routing without an external walking service
- Street-only walk and drive queries over cached local OSM graphs
- OSM differential overlay support for periodic pedestrian-network updates
- Built-in HTTP server with JSON endpoints and static UI assets

## Requirements

- Rust toolchain with Cargo
- A static GTFS ZIP for each feed you want to load
- An OSM `.osm.pbf` extract for the pedestrian graph
- Network access only if you use remote GTFS or remote OSM sources

Notes:

- The preferred configuration mode is the manifest file [alpha-raptor.toml](alpha-raptor.toml).
- If [alpha-raptor.toml](alpha-raptor.toml) exists in the workspace root, α-RAPTOR uses it automatically.
- You can override the manifest path with the environment variable `ALPHA_CONFIG`.
- If no manifest exists, the runtime falls back to legacy environment variables.

## Quick Start

1. Put your GTFS ZIP files under [data/gtfs](data/gtfs) and your OSM extract under [data/osm](data/osm), or point the manifest to remote URLs.
2. Review or replace the sample config in [alpha-raptor.toml](alpha-raptor.toml).
3. Start the engine:

```powershell
cargo run --release
```

4. Open the UI:

```text
http://127.0.0.1:7878
```

5. Try the health endpoint:

```text
http://127.0.0.1:7878/api/health
```

## Install And Run

### Windows PowerShell

```powershell
git clone <your-fork-or-repo-url>
cd Alpha-RAPTOR
cargo run --release
```

### Linux / macOS

```bash
git clone <your-fork-or-repo-url>
cd Alpha-RAPTOR
cargo run --release
```

The server binds to `127.0.0.1:7878` by default. Override it with:

```powershell
$env:ALPHA_BIND = "0.0.0.0:7878"
cargo run --release
```

Useful runtime directories created automatically:

- `.alpha-raptor/target`: Cargo target directory
- `.alpha-raptor/cache/static`: static cache artifacts
- `.alpha-raptor/cache/osm`: OSM/HPF cache artifacts
- `.alpha-raptor/static-feeds`: cached remote GTFS ZIP files
- `.alpha-raptor/osm`: cached remote OSM extracts

## Minimal Manifest Example

Example [alpha-raptor.toml](alpha-raptor.toml):

```toml
osm_pbf = "data/osm/lazio-latest.osm.pbf"
walk_radius_meters = 450.0
walk_speed_mps = 1.35
max_transfer_candidates = 12
refresh_interval_secs = 45
static_reload_interval_secs = 600
static_diff_tolerance = 0.05
default_max_transfers = 4

[dvni]
knn_candidates = 5
max_walk_radius_meters = 1500.0

[hpf]
max_distance_meters = 4000.0
snap_tolerance_meters = 140.0
snap_quadratic_kappa_meters = 40.0
search_window = 512

[osm_diff]
state_url = "https://download.geofabrik.de/europe/italy/centro-updates/state.txt"
poll_interval_secs = 1800

[[feeds]]
id = "roma"
static_gtfs = "data/gtfs/rome_static_gtfs.zip"
trip_updates_url = "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb"
vehicle_positions_url = "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb"
depends_on = []

[[feeds]]
id = "cotral"
static_gtfs = "https://travel.mob.cotralspa.it:4443/GTFS/GTFS_COTRAL.zip"
static_gtfs_allow_invalid_tls = true
depends_on = []
```

## Configuration Reference

### Root TOML Keys

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `osm_pbf` | string | `data/osm/lazio-latest.osm.pbf` | Local path or remote URL of the base OSM PBF extract. |
| `osm_pbf_allow_invalid_tls` | bool | `false` | Allows invalid TLS certificates when downloading a remote OSM PBF. |
| `walk_radius_meters` | float | `450.0` | Maximum stop-to-stop walking radius used by the transfer builder. |
| `walk_speed_mps` | float | `1.35` | Walking speed used to derive walking durations. |
| `max_transfer_candidates` | integer | `12` | Maximum number of walking transfer candidates retained per stop. |
| `refresh_interval_secs` | integer | `45` | Background GTFS-Realtime refresh interval. |
| `static_reload_interval_secs` | integer | `600` | Background polling interval for static GTFS changes and manifest changes. |
| `static_diff_tolerance` | float | `0.05` | Tolerance used by static diff logic before choosing a fuller rebuild path. |
| `default_max_transfers` | integer | `4` | Default max transfers used when `/api/query` omits `max_transfers`. |

### `[dvni]`

| Key | Type | Default | Range | Description |
| --- | --- | --- | --- | --- |
| `knn_candidates` | integer | `5` | clamped to `1..16` | Number of candidate stop connectors used for coordinate queries. |
| `max_walk_radius_meters` | float | `1500.0` | clamped to `50..5000` | Fallback maximum walking radius for coordinate projection. |

### `[hpf]`

| Key | Type | Default | Range | Description |
| --- | --- | --- | --- | --- |
| `max_distance_meters` | float | `4000.0` | clamped to `250..20000` | Build-time expansion radius for the HPF forest. |
| `snap_tolerance_meters` | float | `140.0` | clamped to `25..1000` | Snap threshold used in connector scoring and trace metrics. |
| `snap_quadratic_kappa_meters` | float | `40.0` | clamped to `5..5000` | Quadratic snap penalty constant. |
| `search_window` | integer | `512` | clamped to `64..16384` | Initial Morton search window for HPF runtime lookup. |

### `[osm_diff]`

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `state_url` | string | none | URL of the OSM replication `state.txt`. Required to enable periodic OSM diff polling. |
| `diff_base_url` | string | derived from `state_url` | Optional override for the `.osc.gz` replication base URL. |
| `poll_interval_secs` | integer | `1800` | Diff polling interval, with runtime minimum `60` seconds. |
| `allow_invalid_tls` | bool | inherits `osm_pbf_allow_invalid_tls` | Allows invalid TLS certificates for OSM diff downloads. |

### `[[feeds]]`

Each feed entry defines one static GTFS source and optional realtime endpoints.

| Key | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `id` | string | yes | none | Feed identifier. Must be unique inside the manifest. |
| `static_gtfs` | string | yes | none | Local path or remote URL for the static GTFS ZIP. |
| `static_gtfs_allow_invalid_tls` | bool | no | `false` | Allows invalid TLS certificates for remote GTFS downloads. |
| `trip_updates_url` | string | no | none | GTFS-Realtime Trip Updates protobuf endpoint. |
| `vehicle_positions_url` | string | no | none | GTFS-Realtime Vehicle Positions protobuf endpoint. |
| `depends_on` | string array | no | `[]` | Feed dependency ordering for multi-feed setups. |

Operational notes:

- Feed ids must be unique and must not contain `:` because the runtime uses `feed_id:local_id` namespacing.
- Relative paths in the manifest are resolved from the manifest directory.
- Remote GTFS ZIPs are cached locally and probed with `HEAD` during polling.
- If upstream metadata changes, Alpha-RAPTOR downloads the new ZIP and hot-swaps the rebuilt engine in background.

## Environment Variables

### Runtime And Manifest Selection

| Variable | Description |
| --- | --- |
| `ALPHA_CONFIG` | Overrides the manifest path instead of the default [alpha-raptor.toml](alpha-raptor.toml). |
| `ALPHA_BIND` | Bind address for the HTTP server, for example `127.0.0.1:7878` or `0.0.0.0:7878`. |
| `RUST_LOG` | Standard tracing filter used by the runtime logger. |

### Legacy Environment-Only Configuration

These are still supported when no manifest is present.

| Variable | Maps to |
| --- | --- |
| `ALPHA_DEFAULT_FEED_ID` | default feed id in legacy single-feed mode |
| `ALPHA_STATIC_GTFS` | static GTFS path or URL |
| `ALPHA_STATIC_GTFS_ALLOW_INVALID_TLS` | static GTFS TLS override |
| `ALPHA_TRIP_UPDATES_URL` | Trip Updates endpoint |
| `ALPHA_VEHICLE_POSITIONS_URL` | Vehicle Positions endpoint |
| `ALPHA_OSM_PBF` | OSM PBF path or URL |
| `ALPHA_OSM_PBF_ALLOW_INVALID_TLS` | OSM PBF TLS override |
| `ALPHA_WALK_RADIUS_M` | `walk_radius_meters` |
| `ALPHA_WALK_SPEED_MPS` | `walk_speed_mps` |
| `ALPHA_MAX_WALK_NEIGHBORS` | `max_transfer_candidates` |
| `ALPHA_RT_REFRESH_SECS` | `refresh_interval_secs` |
| `ALPHA_STATIC_POLL_SECS` | `static_reload_interval_secs` |
| `ALPHA_STATIC_DIFF_TOLERANCE` | `static_diff_tolerance` |
| `ALPHA_MAX_TRANSFERS` | `default_max_transfers` |
| `ALPHA_DVNI_KNN` | `dvni.knn_candidates` |
| `ALPHA_DVNI_MAX_WALK_RADIUS_M` | `dvni.max_walk_radius_meters` |
| `ALPHA_HPF_MAX_DISTANCE_M` | `hpf.max_distance_meters` |
| `ALPHA_HPF_SNAP_TOLERANCE_M` | `hpf.snap_tolerance_meters` |
| `ALPHA_HPF_SNAP_QUADRATIC_KAPPA_M` | `hpf.snap_quadratic_kappa_meters` |
| `ALPHA_HPF_SEARCH_WINDOW` | `hpf.search_window` |
| `ALPHA_OSM_DIFF_STATE_URL` | `osm_diff.state_url` |
| `ALPHA_OSM_DIFF_BASE_URL` | `osm_diff.diff_base_url` |
| `ALPHA_OSM_DIFF_POLL_SECS` | `osm_diff.poll_interval_secs` |
| `ALPHA_OSM_DIFF_ALLOW_INVALID_TLS` | `osm_diff.allow_invalid_tls` |

Boolean environment variables accept values such as `1`, `true`, `yes`, and `on`.

## OSM Inputs And Periodic Updates

α-RAPTOR accepts two different OSM workflows:

1. Any regular local `.osm.pbf` file can be used as the base OSM input.
2. Periodic automatic OSM updates require a replication source compatible with the `[osm_diff]` mechanism.

Important limitation:

- The periodic OSM update flow is intended for GeoFabrik replication endpoints, for example `https://download.geofabrik.de/...-updates/state.txt`.
- If you upload or point the engine to a normal standalone OSM extract that is not backed by GeoFabrik replication diffs, α-RAPTOR can still build and answer queries, but it will not have automatic periodic OSM updates.

In practice:

- Local or arbitrary OSM extract: supported as a base file, no automatic diff polling unless you also configure a valid replication source.
- GeoFabrik extract plus matching GeoFabrik `state.txt`: supported for periodic HPF overlay updates.

## API Overview

The server exposes these endpoints:

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/api/health` | `GET` | Liveness check |
| `/api/stats` | `GET` | Build, realtime, memoization, and overlay metrics |
| `/api/stops` | `GET` | Stop search |
| `/api/query` | `GET` | Journey planning query |
| `/api/street` | `GET` | Street-only walk or drive routing |
| `/api/realtime` | `GET` | Current realtime snapshot |
| `/api/realtime/refresh` | `POST` | Force an immediate realtime refresh |

### Stop Search Example

```text
GET /api/stops?q=conca&limit=10
```

### Stop-To-Stop Query Example

```text
GET /api/query?from=roma:70378&to=roma:71404&date=2026-04-19&time=09:05&max_transfers=3
```

You can also use packed global stop ids:

```text
GET /api/query?from_gid=123456789&to_gid=987654321&date=2026-04-19&time=09:05
```

### Coordinate-To-Coordinate Query Example

```text
GET /api/query?from_lat=41.94048&from_lon=12.52909&to_lat=41.82647&to_lon=12.48104&date=2026-04-19&time=09:05
```

### Street-Only Routing Example

```text
GET /api/street?mode=drive&from_lat=41.90085&from_lon=12.48354&to_lat=41.89025&to_lon=12.49223
GET /api/street?mode=walk&from_lat=41.90212&from_lon=12.49611&to_lat=41.89802&to_lon=12.48391
```

### cURL Examples

```bash
curl "http://127.0.0.1:7878/api/health"
curl "http://127.0.0.1:7878/api/stops?q=termini&limit=8"
curl "http://127.0.0.1:7878/api/query?from=roma:70378&to=roma:71404&date=2026-04-19&time=09:05&max_transfers=3"
curl "http://127.0.0.1:7878/api/street?mode=drive&from_lat=41.90085&from_lon=12.48354&to_lat=41.89025&to_lon=12.49223"
curl -X POST "http://127.0.0.1:7878/api/realtime/refresh"
```

### Query Parameters

`/api/query` accepts these parameters:

| Parameter | Required | Description |
| --- | --- | --- |
| `from` | conditional | Source stop id, usually namespaced as `feed_id:stop_id`. |
| `to` | conditional | Destination stop id. |
| `from_gid` | conditional | Source global stop id. |
| `to_gid` | conditional | Destination global stop id. |
| `from_lat` | conditional | Source latitude for coordinate routing. |
| `from_lon` | conditional | Source longitude for coordinate routing. |
| `to_lat` | conditional | Destination latitude for coordinate routing. |
| `to_lon` | conditional | Destination longitude for coordinate routing. |
| `date` | yes | Service date in `YYYY-MM-DD` format. |
| `time` | yes | Departure time in `HH:MM` or compatible clock format. |
| `max_transfers` | no | Overrides the runtime default transfer limit. |

Use either stop identifiers or coordinates. Coordinate queries inject virtual source/destination walking connectors into the routing search.

`/api/street` accepts `from_lat`, `from_lon`, `to_lat`, `to_lon`, and an optional `mode` parameter (`drive` by default, `walk` also supported). The response contains distance, duration, polyline geometry, turn-by-turn directions, and snap/query trace metrics.

## Repository Layout

| Path | Purpose |
| --- | --- |
| [src](src) | Rust engine, API server, realtime logic, HPF, and routing core |
| [public](public) | Static web UI assets |
| [data](data) | Suggested location for local GTFS and OSM inputs |
| [docs](docs) | Detailed architecture notes for the main subsystems |
| [paper-figures](paper-figures) | Benchmark and architecture figure sources |

## Operational Notes

- Static GTFS and manifest changes are polled periodically and can trigger a background engine rebuild.
- Realtime refresh runs independently from static reload.
- A comment-only change in the manifest can invalidate the static cache and force a rebuild on next startup.
- Remote GTFS and remote OSM inputs are cached locally under `.alpha-raptor`.
- The built-in UI is served from [public/index.html](public/index.html) and assets are mounted under `/assets`.

## Documentation

Subsystem notes live in [docs](docs):

- [docs/btt.md](docs/btt.md): Bipartite Transfer Tiling
- [docs/hpf.md](docs/hpf.md): Holographic Pedestrian Forest and OSM-backed coordinate routing
- [docs/hydra-slab.md](docs/hydra-slab.md): cold metadata storage strategy
- [docs/svrt.md](docs/svrt.md): SIMD-Vectorized Route Traversal
- [docs/temporal-indirection.md](docs/temporal-indirection.md): temporal indirection and Chronos behavior

## License

This repository is licensed under CC BY-NC 4.0. See [LICENSE](LICENSE).