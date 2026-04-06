# Data Layout

Runtime inputs live under this directory so large binaries stay out of the repository root.

- `gtfs/`: local GTFS zip inputs such as `rome_static_gtfs.zip`
- `osm/`: OSM extracts such as `lazio-latest.osm.pbf`

These binary files are ignored by Git via the repository `.gitignore`.
