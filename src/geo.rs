pub const DESTINATION_CELL_SHIFT_BITS: u8 = 36;
const DESTINATION_CELL_AXIS_BITS: u8 = (64 - DESTINATION_CELL_SHIFT_BITS) / 2;

pub fn destination_cell(latitude: Option<f64>, longitude: Option<f64>) -> Option<u64> {
    let (Some(latitude), Some(longitude)) = (latitude, longitude) else {
        return None;
    };
    Some(morton_code(latitude, longitude) >> DESTINATION_CELL_SHIFT_BITS)
}

pub fn destination_cell_neighborhood(latitude: Option<f64>, longitude: Option<f64>) -> Vec<u64> {
    let Some(cell) = destination_cell(latitude, longitude) else {
        return Vec::new();
    };
    moore_neighborhood(cell)
}

pub fn destination_cell_window(cell: u64, radius_cells: u32) -> Vec<u64> {
    moore_neighborhood_with_radius(cell, radius_cells)
}

pub fn morton_code(latitude: f64, longitude: f64) -> u64 {
    let lat = quantize_latitude(latitude);
    let lon = quantize_longitude(longitude);
    spread_bits(lon) | (spread_bits(lat) << 1)
}

pub fn decode_morton_code(code: u64) -> (f64, f64) {
    let lon = compact_bits(code);
    let lat = compact_bits(code >> 1);
    (
        dequantize_range(lat, -90.0, 90.0),
        dequantize_range(lon, -180.0, 180.0),
    )
}

fn quantize_latitude(latitude: f64) -> u32 {
    quantize_range(latitude, -90.0, 90.0)
}

fn quantize_longitude(longitude: f64) -> u32 {
    quantize_range(longitude, -180.0, 180.0)
}

fn quantize_range(value: f64, minimum: f64, maximum: f64) -> u32 {
    let normalized = ((value.clamp(minimum, maximum) - minimum) / (maximum - minimum))
        .clamp(0.0, 1.0);
    (normalized * u32::MAX as f64).round() as u32
}

fn dequantize_range(value: u32, minimum: f64, maximum: f64) -> f64 {
    let normalized = value as f64 / u32::MAX as f64;
    minimum + (normalized * (maximum - minimum))
}

fn spread_bits(value: u32) -> u64 {
    let mut value = value as u64;
    value = (value | (value << 16)) & 0x0000_FFFF_0000_FFFF;
    value = (value | (value << 8)) & 0x00FF_00FF_00FF_00FF;
    value = (value | (value << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    value = (value | (value << 2)) & 0x3333_3333_3333_3333;
    value = (value | (value << 1)) & 0x5555_5555_5555_5555;
    value
}

fn compact_bits(value: u64) -> u32 {
    let mut value = value & 0x5555_5555_5555_5555;
    value = (value | (value >> 1)) & 0x3333_3333_3333_3333;
    value = (value | (value >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    value = (value | (value >> 4)) & 0x00FF_00FF_00FF_00FF;
    value = (value | (value >> 8)) & 0x0000_FFFF_0000_FFFF;
    value = (value | (value >> 16)) & 0x0000_0000_FFFF_FFFF;
    value as u32
}

fn moore_neighborhood(cell: u64) -> Vec<u64> {
    moore_neighborhood_with_radius(cell, 1)
}

fn moore_neighborhood_with_radius(cell: u64, radius_cells: u32) -> Vec<u64> {
    let (lon_component, lat_component) = decode_destination_cell(cell);
    let component_limit = destination_cell_component_limit();
    let side = (radius_cells * 2 + 1) as usize;
    let mut cells = Vec::with_capacity(side * side);

    let radius = radius_cells as i32;
    for lat_delta in -radius..=radius {
        for lon_delta in -radius..=radius {
            let next_lon = lon_component as i32 + lon_delta;
            let next_lat = lat_component as i32 + lat_delta;
            if next_lon < 0
                || next_lat < 0
                || next_lon > component_limit as i32
                || next_lat > component_limit as i32
            {
                continue;
            }
            cells.push(encode_destination_cell(next_lon as u32, next_lat as u32));
        }
    }

    cells
}

fn decode_destination_cell(cell: u64) -> (u32, u32) {
    (compact_bits(cell), compact_bits(cell >> 1))
}

fn encode_destination_cell(lon_component: u32, lat_component: u32) -> u64 {
    spread_bits(lon_component) | (spread_bits(lat_component) << 1)
}

fn destination_cell_component_limit() -> u32 {
    if DESTINATION_CELL_AXIS_BITS >= 32 {
        u32::MAX
    } else {
        (1u32 << DESTINATION_CELL_AXIS_BITS) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DESTINATION_CELL_SHIFT_BITS, destination_cell, destination_cell_neighborhood,
        encode_destination_cell, morton_code,
    };

    #[test]
    fn morton_code_is_stable_for_identical_points() {
        let code = morton_code(41.9028, 12.4964);
        assert_eq!(code, morton_code(41.9028, 12.4964));
    }

    #[test]
    fn nearby_points_share_the_same_destination_cell() {
        let left = destination_cell(Some(41.902800), Some(12.496400));
        let right = destination_cell(Some(41.902820), Some(12.496420));
        assert_eq!(left, right);
    }

    #[test]
    fn distant_points_split_into_different_cells() {
        let north = destination_cell(Some(41.9650), Some(12.4550));
        let south = destination_cell(Some(41.8000), Some(12.5000));
        assert_ne!(north, south);
    }

    #[test]
    fn shift_constant_matches_two_kilometer_scale() {
        assert_eq!(DESTINATION_CELL_SHIFT_BITS, 36);
    }

    #[test]
    fn moore_neighborhood_includes_adjacent_cells() {
        let center = encode_destination_cell(100, 200);
        let east = encode_destination_cell(101, 200);
        let north_west = encode_destination_cell(99, 201);
        let neighborhood = destination_cell_neighborhood(Some(41.9028), Some(12.4964));
        assert!(!neighborhood.is_empty());

        let synthetic_neighborhood = super::moore_neighborhood(center);
        assert!(synthetic_neighborhood.contains(&center));
        assert!(synthetic_neighborhood.contains(&east));
        assert!(synthetic_neighborhood.contains(&north_west));
    }
}