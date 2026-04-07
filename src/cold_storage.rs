use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::RwLock,
};

use anyhow::{Context, Result, anyhow, bail};
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};

use crate::engine::{RouteRecord, ShapePoint, StopRecord, TripRecord};

const COLD_STORE_SCHEMA_VERSION: u32 = 2;
const HYDRA_INDEX_MAGIC: &[u8; 8] = b"HYDRASLB";
const INDEX_ENTRY_BYTES: usize = 12;
const INDEX_HEADER_BYTES: usize = 88;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdStopRecord {
    pub global_id: u64,
    pub feed_id: String,
    pub local_id: String,
    pub id: String,
    pub code: Option<String>,
    pub name: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdRouteRecord {
    pub global_id: u64,
    pub id: String,
    pub short_name: Option<String>,
    pub long_name: Option<String>,
    pub route_type: String,
    pub color: Option<String>,
    pub text_color: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColdTripRecord {
    pub global_id: u64,
    pub id: String,
    pub headsign: Option<String>,
    pub shape_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ColdStorePaths {
    pub data_path: PathBuf,
    pub index_path: PathBuf,
}

pub struct ColdStore {
    data: Mmap,
    index: Mmap,
    header: HydraIndexHeader,
    shape_directory: HashMap<String, BlobIndex>,
    overlay: ColdStoreOverlay,
}

#[derive(Default)]
struct ColdStoreOverlay {
    stops: RwLock<HashMap<usize, ColdStopRecord>>,
    routes: RwLock<HashMap<usize, ColdRouteRecord>>,
    trips: RwLock<HashMap<usize, ColdTripRecord>>,
    shape_points: RwLock<HashMap<String, Vec<ShapePoint>>>,
}

#[derive(Debug, Clone, Copy)]
struct HydraIndexHeader {
    schema_version: u32,
    generation_token: u64,
    stop_count: u64,
    route_count: u64,
    trip_count: u64,
    stop_index_offset: u64,
    route_index_offset: u64,
    trip_index_offset: u64,
    shape_directory_offset: u64,
    shape_directory_len: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct BlobIndex {
    offset: u64,
    len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShapeDirectoryEntry {
    shape_id: String,
    blob: BlobIndex,
}

impl ColdStore {
    pub fn load_or_build(
        paths: &ColdStorePaths,
        generation_token: u64,
        stops: &[StopRecord],
        routes: &[RouteRecord],
        trips: &[TripRecord],
        shapes: &HashMap<String, Vec<ShapePoint>>,
    ) -> Result<Self> {
        if Self::is_current(paths, generation_token)? {
            return Self::open(paths);
        }

        build_store(paths, generation_token, stops, routes, trips, shapes)?;
        Self::open(paths)
    }

    pub fn stop(&self, stop_index: usize) -> Result<ColdStopRecord> {
        if let Some(record) = self
            .overlay
            .stops
            .read()
            .expect("Hydra-Slab stop overlay poisoned")
            .get(&stop_index)
            .cloned()
        {
            return Ok(record);
        }

        self.read_direct_slot(
            self.header.stop_index_offset,
            self.header.stop_count,
            stop_index,
            "stop",
        )
    }

    pub fn route(&self, route_index: usize) -> Result<ColdRouteRecord> {
        if let Some(record) = self
            .overlay
            .routes
            .read()
            .expect("Hydra-Slab route overlay poisoned")
            .get(&route_index)
            .cloned()
        {
            return Ok(record);
        }

        self.read_direct_slot(
            self.header.route_index_offset,
            self.header.route_count,
            route_index,
            "route",
        )
    }

    pub fn trip(&self, trip_index: usize) -> Result<ColdTripRecord> {
        if let Some(record) = self
            .overlay
            .trips
            .read()
            .expect("Hydra-Slab trip overlay poisoned")
            .get(&trip_index)
            .cloned()
        {
            return Ok(record);
        }

        self.read_direct_slot(
            self.header.trip_index_offset,
            self.header.trip_count,
            trip_index,
            "trip",
        )
    }

    pub fn shape_points(&self, shape_id: &str) -> Result<Option<Vec<ShapePoint>>> {
        if let Some(points) = self
            .overlay
            .shape_points
            .read()
            .expect("Hydra-Slab shape overlay poisoned")
            .get(shape_id)
            .cloned()
        {
            return Ok(Some(points));
        }

        let Some(blob) = self.shape_directory.get(shape_id).copied() else {
            return Ok(None);
        };
        Ok(Some(self.read_payload(blob)?))
    }

    #[allow(dead_code)]
    pub fn overlay_stop(&self, stop_index: usize, record: ColdStopRecord) {
        self.overlay
            .stops
            .write()
            .expect("Hydra-Slab stop overlay poisoned")
            .insert(stop_index, record);
    }

    #[allow(dead_code)]
    pub fn overlay_route(&self, route_index: usize, record: ColdRouteRecord) {
        self.overlay
            .routes
            .write()
            .expect("Hydra-Slab route overlay poisoned")
            .insert(route_index, record);
    }

    #[allow(dead_code)]
    pub fn overlay_trip(&self, trip_index: usize, record: ColdTripRecord) {
        self.overlay
            .trips
            .write()
            .expect("Hydra-Slab trip overlay poisoned")
            .insert(trip_index, record);
    }

    #[allow(dead_code)]
    pub fn overlay_shape_points(&self, shape_id: impl Into<String>, points: Vec<ShapePoint>) {
        self.overlay
            .shape_points
            .write()
            .expect("Hydra-Slab shape overlay poisoned")
            .insert(shape_id.into(), points);
    }

    #[allow(dead_code)]
    pub fn clear_overlays(&self) {
        self.overlay
            .stops
            .write()
            .expect("Hydra-Slab stop overlay poisoned")
            .clear();
        self.overlay
            .routes
            .write()
            .expect("Hydra-Slab route overlay poisoned")
            .clear();
        self.overlay
            .trips
            .write()
            .expect("Hydra-Slab trip overlay poisoned")
            .clear();
        self.overlay
            .shape_points
            .write()
            .expect("Hydra-Slab shape overlay poisoned")
            .clear();
    }

    fn is_current(paths: &ColdStorePaths, generation_token: u64) -> Result<bool> {
        if !paths.data_path.exists() || !paths.index_path.exists() {
            return Ok(false);
        }

        let mut file = match File::open(&paths.index_path) {
            Ok(file) => file,
            Err(_) => return Ok(false),
        };

        let mut header_bytes = [0u8; INDEX_HEADER_BYTES];
        if file.read_exact(&mut header_bytes).is_err() {
            return Ok(false);
        }

        let header = match HydraIndexHeader::decode(&header_bytes) {
            Ok(header) => header,
            Err(_) => return Ok(false),
        };

        Ok(
            header.schema_version == COLD_STORE_SCHEMA_VERSION
                && header.generation_token == generation_token,
        )
    }

    fn open(paths: &ColdStorePaths) -> Result<Self> {
        let data_file = File::open(&paths.data_path).with_context(|| {
            format!(
                "unable to open Hydra-Slab data file {}",
                paths.data_path.display()
            )
        })?;
        let data = unsafe { MmapOptions::new().map(&data_file) }.with_context(|| {
            format!("unable to mmap Hydra-Slab data {}", paths.data_path.display())
        })
        ?;

        let index_file = File::open(&paths.index_path).with_context(|| {
            format!(
                "unable to open Hydra-Slab index file {}",
                paths.index_path.display()
            )
        })?;
        let index = unsafe { MmapOptions::new().map(&index_file) }.with_context(|| {
            format!("unable to mmap Hydra-Slab index {}", paths.index_path.display())
        })?;

        if index.len() < INDEX_HEADER_BYTES {
            bail!(
                "Hydra-Slab index {} is shorter than the {}-byte header",
                paths.index_path.display(),
                INDEX_HEADER_BYTES
            );
        }

        let header = HydraIndexHeader::decode(&index[..INDEX_HEADER_BYTES])?;
        header.validate(index.len())?;

        let shape_directory_range = byte_range(
            header.shape_directory_offset,
            header.shape_directory_len,
            index.len(),
            "shape directory",
        )?;
        let shape_entries: Vec<ShapeDirectoryEntry> = bincode::deserialize(
            &index[shape_directory_range],
        )
        .context("failed to deserialize Hydra-Slab shape directory")?;
        let shape_directory = shape_entries
            .into_iter()
            .map(|entry| (entry.shape_id, entry.blob))
            .collect();

        Ok(Self {
            data,
            index,
            header,
            shape_directory,
            overlay: ColdStoreOverlay::default(),
        })
    }

    fn read_direct_slot<T>(
        &self,
        section_offset: u64,
        section_len: u64,
        slot: usize,
        kind: &str,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let slot = u64::try_from(slot).context("slot index exceeds u64 range")?;
        if slot >= section_len {
            bail!(
                "Hydra-Slab {kind} slot {slot} exceeds section length {section_len}"
            );
        }

        let relative = slot
            .checked_mul(INDEX_ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow!("Hydra-Slab {kind} slot byte offset overflow"))?;
        let entry_offset = section_offset
            .checked_add(relative)
            .ok_or_else(|| anyhow!("Hydra-Slab {kind} section overflow"))?;
        let entry_range = byte_range(
            entry_offset,
            INDEX_ENTRY_BYTES as u64,
            self.index.len(),
            kind,
        )?;
        let blob = BlobIndex::decode(&self.index[entry_range])?;
        self.read_payload(blob)
    }

    fn read_payload<T>(&self, blob: BlobIndex) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let payload_range = byte_range(
            blob.offset,
            u64::from(blob.len),
            self.data.len(),
            "payload",
        )?;
        bincode::deserialize(&self.data[payload_range])
            .context("failed to deserialize Hydra-Slab payload")
    }
}

pub fn cold_store_paths(base_path: &Path, generation_token: u64) -> ColdStorePaths {
    let file_stem = base_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("alpha-raptor");
    let parent = base_path.parent().unwrap_or_else(|| Path::new("."));
    let generation = format!("{generation_token:016x}");

    ColdStorePaths {
        data_path: parent.join(format!("{file_stem}.{generation}.hydra.data.bin")),
        index_path: parent.join(format!("{file_stem}.{generation}.hydra.index.bin")),
    }
}

fn build_store(
    paths: &ColdStorePaths,
    generation_token: u64,
    stops: &[StopRecord],
    routes: &[RouteRecord],
    trips: &[TripRecord],
    shapes: &HashMap<String, Vec<ShapePoint>>,
) -> Result<()> {
    if let Some(parent) = paths.data_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "unable to create Hydra-Slab directory {}",
                parent.display()
            )
        })?;
    }

    if paths.data_path.exists() {
        fs::remove_file(&paths.data_path).with_context(|| {
            format!(
                "unable to remove stale Hydra-Slab data {}",
                paths.data_path.display()
            )
        })?;
    }
    if paths.index_path.exists() {
        fs::remove_file(&paths.index_path).with_context(|| {
            format!(
                "unable to remove stale Hydra-Slab index {}",
                paths.index_path.display()
            )
        })?;
    }

    let data_file = File::create(&paths.data_path).with_context(|| {
        format!(
            "unable to create Hydra-Slab data file {}",
            paths.data_path.display()
        )
    })?;
    let mut writer = BufWriter::new(data_file);

    let mut offset = 0u64;
    let mut index_bytes = vec![0u8; INDEX_HEADER_BYTES];

    let stop_index_offset = index_bytes.len() as u64;

    for stop in stops {
        let cold_stop = ColdStopRecord {
            global_id: stop.global_id,
            feed_id: stop.feed_id.clone(),
            local_id: stop.local_id.clone(),
            id: stop.id.clone(),
            code: stop.code.clone(),
            name: stop.name.clone(),
            latitude: stop.latitude,
            longitude: stop.longitude,
        };
        let blob = write_blob(&mut writer, &cold_stop, &mut offset)?;
        blob.encode_into(&mut index_bytes);
    }

    let route_index_offset = index_bytes.len() as u64;

    for route in routes {
        let cold_route = ColdRouteRecord {
            global_id: route.global_id,
            id: route.id.clone(),
            short_name: route.short_name.clone(),
            long_name: route.long_name.clone(),
            route_type: route.route_type.clone(),
            color: route.color.clone(),
            text_color: route.text_color.clone(),
        };
        let blob = write_blob(&mut writer, &cold_route, &mut offset)?;
        blob.encode_into(&mut index_bytes);
    }

    let trip_index_offset = index_bytes.len() as u64;

    for trip in trips {
        let cold_trip = ColdTripRecord {
            global_id: trip.global_id,
            id: trip.id.clone(),
            headsign: trip.headsign.clone(),
            shape_id: trip.shape_id.clone(),
        };
        let blob = write_blob(&mut writer, &cold_trip, &mut offset)?;
        blob.encode_into(&mut index_bytes);
    }

    let mut ordered_shapes: Vec<_> = shapes.iter().collect();
    ordered_shapes.sort_by(|left, right| left.0.cmp(right.0));
    let mut shape_directory = Vec::with_capacity(ordered_shapes.len());
    for (shape_id, points) in ordered_shapes {
        let blob = write_blob(&mut writer, points, &mut offset)?;
        shape_directory.push(ShapeDirectoryEntry {
            shape_id: shape_id.clone(),
            blob,
        });
    }

    let shape_directory_offset = index_bytes.len() as u64;
    let shape_directory_bytes =
        bincode::serialize(&shape_directory).context("failed to serialize Hydra-Slab shape directory")?;
    index_bytes.extend_from_slice(&shape_directory_bytes);

    let header = HydraIndexHeader {
        schema_version: COLD_STORE_SCHEMA_VERSION,
        generation_token,
        stop_count: stops.len() as u64,
        route_count: routes.len() as u64,
        trip_count: trips.len() as u64,
        stop_index_offset,
        route_index_offset,
        trip_index_offset,
        shape_directory_offset,
        shape_directory_len: shape_directory_bytes.len() as u64,
    };
    let header_bytes = header.encode();
    index_bytes[..INDEX_HEADER_BYTES].copy_from_slice(&header_bytes);

    writer.flush().context("failed to flush Hydra-Slab data")?;
    fs::write(&paths.index_path, &index_bytes).with_context(|| {
        format!(
            "failed to write Hydra-Slab index {}",
            paths.index_path.display()
        )
    })?;
    Ok(())
}

fn write_blob<T>(
    writer: &mut BufWriter<File>,
    value: &T,
    offset: &mut u64,
) -> Result<BlobIndex>
where
    T: Serialize,
{
    let payload = bincode::serialize(value).context("failed to serialize Hydra-Slab payload")?;
    let len = u32::try_from(payload.len()).context("Hydra-Slab payload exceeds u32 length")?;
    writer
        .write_all(&payload)
        .context("failed to write Hydra-Slab payload")?;
    let blob = BlobIndex {
        offset: *offset,
        len,
    };
    *offset += payload.len() as u64;
    Ok(blob)
}

impl HydraIndexHeader {
    fn encode(self) -> [u8; INDEX_HEADER_BYTES] {
        let mut buffer = [0u8; INDEX_HEADER_BYTES];
        buffer[..8].copy_from_slice(HYDRA_INDEX_MAGIC);
        write_u32(&mut buffer[8..12], self.schema_version);
        write_u32(&mut buffer[12..16], INDEX_HEADER_BYTES as u32);
        write_u64(&mut buffer[16..24], self.generation_token);
        write_u64(&mut buffer[24..32], self.stop_count);
        write_u64(&mut buffer[32..40], self.route_count);
        write_u64(&mut buffer[40..48], self.trip_count);
        write_u64(&mut buffer[48..56], self.stop_index_offset);
        write_u64(&mut buffer[56..64], self.route_index_offset);
        write_u64(&mut buffer[64..72], self.trip_index_offset);
        write_u64(&mut buffer[72..80], self.shape_directory_offset);
        write_u64(&mut buffer[80..88], self.shape_directory_len);
        buffer
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < INDEX_HEADER_BYTES {
            bail!(
                "Hydra-Slab index header requires {} bytes, got {}",
                INDEX_HEADER_BYTES,
                bytes.len()
            );
        }
        if &bytes[..8] != HYDRA_INDEX_MAGIC {
            bail!("unexpected Hydra-Slab index magic");
        }

        let header_bytes = read_u32(&bytes[12..16]);
        if header_bytes as usize != INDEX_HEADER_BYTES {
            bail!("unsupported Hydra-Slab index header size {header_bytes}");
        }

        Ok(Self {
            schema_version: read_u32(&bytes[8..12]),
            generation_token: read_u64(&bytes[16..24]),
            stop_count: read_u64(&bytes[24..32]),
            route_count: read_u64(&bytes[32..40]),
            trip_count: read_u64(&bytes[40..48]),
            stop_index_offset: read_u64(&bytes[48..56]),
            route_index_offset: read_u64(&bytes[56..64]),
            trip_index_offset: read_u64(&bytes[64..72]),
            shape_directory_offset: read_u64(&bytes[72..80]),
            shape_directory_len: read_u64(&bytes[80..88]),
        })
    }

    fn validate(&self, index_len: usize) -> Result<()> {
        if self.schema_version != COLD_STORE_SCHEMA_VERSION {
            bail!(
                "Hydra-Slab schema version {} does not match expected {}",
                self.schema_version,
                COLD_STORE_SCHEMA_VERSION
            );
        }

        let index_len = index_len as u64;
        let stop_bytes = self
            .stop_count
            .checked_mul(INDEX_ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow!("Hydra-Slab stop section size overflow"))?;
        let route_bytes = self
            .route_count
            .checked_mul(INDEX_ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow!("Hydra-Slab route section size overflow"))?;
        let trip_bytes = self
            .trip_count
            .checked_mul(INDEX_ENTRY_BYTES as u64)
            .ok_or_else(|| anyhow!("Hydra-Slab trip section size overflow"))?;

        let stop_end = self
            .stop_index_offset
            .checked_add(stop_bytes)
            .ok_or_else(|| anyhow!("Hydra-Slab stop section overflow"))?;
        let route_end = self
            .route_index_offset
            .checked_add(route_bytes)
            .ok_or_else(|| anyhow!("Hydra-Slab route section overflow"))?;
        let trip_end = self
            .trip_index_offset
            .checked_add(trip_bytes)
            .ok_or_else(|| anyhow!("Hydra-Slab trip section overflow"))?;
        let shape_end = self
            .shape_directory_offset
            .checked_add(self.shape_directory_len)
            .ok_or_else(|| anyhow!("Hydra-Slab shape directory overflow"))?;

        if self.stop_index_offset < INDEX_HEADER_BYTES as u64
            || self.route_index_offset < self.stop_index_offset
            || self.trip_index_offset < self.route_index_offset
            || self.shape_directory_offset < self.trip_index_offset
        {
            bail!("Hydra-Slab index sections are not monotonic");
        }

        if stop_end > index_len
            || route_end > index_len
            || trip_end > index_len
            || shape_end > index_len
        {
            bail!(
                "Hydra-Slab index references bytes beyond mapped index length {}",
                index_len
            );
        }

        Ok(())
    }
}

impl BlobIndex {
    fn encode_into(self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&self.offset.to_le_bytes());
        buffer.extend_from_slice(&self.len.to_le_bytes());
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != INDEX_ENTRY_BYTES {
            bail!(
                "Hydra-Slab index entry requires {} bytes, got {}",
                INDEX_ENTRY_BYTES,
                bytes.len()
            );
        }

        Ok(Self {
            offset: read_u64(&bytes[..8]),
            len: read_u32(&bytes[8..12]),
        })
    }
}

fn byte_range(offset: u64, len: u64, total_len: usize, label: &str) -> Result<std::ops::Range<usize>> {
    let start = usize::try_from(offset).context("Hydra-Slab offset exceeds usize")?;
    let len = usize::try_from(len).context("Hydra-Slab length exceeds usize")?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| anyhow!("Hydra-Slab {label} range overflow"))?;
    if end > total_len {
        bail!(
            "Hydra-Slab {label} slice {}..{} exceeds mapped length {}",
            start,
            end,
            total_len
        );
    }
    Ok(start..end)
}

fn write_u32(bytes: &mut [u8], value: u32) {
    bytes.copy_from_slice(&value.to_le_bytes());
}

fn write_u64(bytes: &mut [u8], value: u64) {
    bytes.copy_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut array = [0u8; 4];
    array.copy_from_slice(bytes);
    u32::from_le_bytes(array)
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    array.copy_from_slice(bytes);
    u64::from_le_bytes(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{process, time::{SystemTime, UNIX_EPOCH}};

    use crate::engine::{RouteRecord, ShapePoint, StopRecord, TripRecord, TripStopRecord};

    #[test]
    fn hydra_slab_round_trips_direct_and_shape_records() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "alpha-raptor-hydra-slab-{}-{}",
            process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::create_dir_all(&root)?;

        let base_path = root.join("alpha-raptor.static.v3.bin");
        let paths = cold_store_paths(&base_path, 42);

        let stops = vec![
            StopRecord {
                global_id: 11,
                feed_index: 0,
                feed_id: "roma".to_owned(),
                local_id: "s0".to_owned(),
                id: "roma:s0".to_owned(),
                code: Some("001".to_owned()),
                name: "Alpha".to_owned(),
                latitude: Some(41.0),
                longitude: Some(12.0),
                search_blob: "roma:s0 001 alpha".to_owned(),
            },
            StopRecord {
                global_id: 12,
                feed_index: 0,
                feed_id: "roma".to_owned(),
                local_id: "s1".to_owned(),
                id: "roma:s1".to_owned(),
                code: Some("002".to_owned()),
                name: "Beta".to_owned(),
                latitude: Some(41.1),
                longitude: Some(12.1),
                search_blob: "roma:s1 002 beta".to_owned(),
            },
        ];

        let routes = vec![RouteRecord {
            global_id: 21,
            feed_index: 0,
            feed_id: "roma".to_owned(),
            local_id: "r0".to_owned(),
            id: "roma:r0".to_owned(),
            short_name: Some("M1".to_owned()),
            long_name: Some("Metro 1".to_owned()),
            route_type: "Subway".to_owned(),
            color: Some("#FF0000".to_owned()),
            text_color: Some("#FFFFFF".to_owned()),
        }];

        let trips = vec![TripRecord {
            global_id: 31,
            feed_index: 0,
            feed_id: "roma".to_owned(),
            local_id: "t0".to_owned(),
            id: "roma:t0".to_owned(),
            route_index: 0,
            shape_id: Some("roma:shape-0".to_owned()),
            shape_stop_point_indices: None,
            headsign: Some("Laurentina".to_owned()),
            stop_times: vec![
                TripStopRecord {
                    stop_index: 0,
                    arrival_secs: 0,
                    departure_secs: 0,
                    stop_sequence: 1,
                    shape_dist_traveled: Some(0.0),
                },
                TripStopRecord {
                    stop_index: 1,
                    arrival_secs: 120,
                    departure_secs: 120,
                    stop_sequence: 2,
                    shape_dist_traveled: Some(100.0),
                },
            ],
        }];

        let shapes = HashMap::from([(
            "roma:shape-0".to_owned(),
            vec![
                ShapePoint {
                    lat: 41.0,
                    lon: 12.0,
                    dist_traveled: Some(0.0),
                },
                ShapePoint {
                    lat: 41.1,
                    lon: 12.1,
                    dist_traveled: Some(100.0),
                },
            ],
        )]);

        let store = ColdStore::load_or_build(&paths, 42, &stops, &routes, &trips, &shapes)?;

        assert_eq!(store.stop(0)?.name, "Alpha");
        assert_eq!(store.route(0)?.route_type, "Subway");
        assert_eq!(store.trip(0)?.id, "roma:t0");
        assert_eq!(store.shape_points("roma:shape-0")?.unwrap().len(), 2);
        assert!(store.shape_points("roma:missing")?.is_none());

        store.overlay_stop(
            0,
            ColdStopRecord {
                global_id: 11,
                feed_id: "roma".to_owned(),
                local_id: "s0".to_owned(),
                id: "roma:s0".to_owned(),
                code: Some("001".to_owned()),
                name: "Alpha overlay".to_owned(),
                latitude: Some(41.0),
                longitude: Some(12.0),
            },
        );
        assert_eq!(store.stop(0)?.name, "Alpha overlay");

        drop(store);
        fs::remove_file(&paths.data_path).ok();
        fs::remove_file(&paths.index_path).ok();
        fs::remove_dir_all(&root).ok();
        Ok(())
    }
}
