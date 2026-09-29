use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment::footer::parse_segment_footer;
use crate::storage::segment::meta::{read_embedded_footer, read_latest_sidecar_footer};
use crate::storage::segment::{
    is_supported_segment_version, ZoneMap, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC,
};
use crate::types::batch::Schema;
#[allow(unused_imports)]
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct SegmentInspectInfo {
    pub path: PathBuf,
    pub header_magic: u32,
    pub header_version: u32,
    pub header_created_at: u64,
    pub header_flags: u64,
    pub version: u32,
    pub generation: u64,
    pub row_count: u64,
    pub header_row_count: u64,
    pub footer_row_count: u64,
    pub row_group_count: u32,
    pub avg_row_group_size: u64,
    pub file_size: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub created_at: u64,
    pub modified_at: u64,
    pub file_created: Option<SystemTime>,
    pub file_modified: Option<SystemTime>,
    pub schema: Schema,
    pub row_groups: Vec<RowGroupInfo>,
    pub partition_key: String,
    pub sequence: u32,
    pub deleted_row_count: u64,
    pub delete_bitmap_len: u32,
}

#[derive(Debug, Clone)]
pub struct RowGroupInfo {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub compressed_sizes: Vec<u64>,
    pub uncompressed_sizes: Vec<u64>,
    pub zone_maps: Vec<ZoneMap>,
}

pub fn inspect_segment(path: &Path) -> Result<SegmentInspectInfo> {
    let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let (partition_key, sequence) = parse_partition_filename(filename);

    let file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();
    let file_created = metadata.created().ok();
    let file_modified = metadata.modified().ok();

    if file_size < SEGMENT_HEADER_SIZE as u64 {
        return Err(TaibaiError::Storage(format!(
            "segment file too small: {} bytes (need >= {})",
            file_size, SEGMENT_HEADER_SIZE
        )));
    }

    let mut tmp_file = file;
    tmp_file.seek(SeekFrom::Start(0))?;
    let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
    tmp_file.read_exact(&mut header_buf)?;
    let header_magic = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
    let header_version = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
    let header_created_at = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());
    let header_flags = u64::from_le_bytes(header_buf[16..24].try_into().unwrap());
    if header_magic != SEGMENT_MAGIC {
        return Err(TaibaiError::Storage(format!(
            "invalid segment header magic: {:08X} (expected {:08X})",
            header_magic, SEGMENT_MAGIC
        )));
    }
    if !is_supported_segment_version(header_version) {
        return Err(TaibaiError::Storage(format!(
            "unsupported segment header version: {} (supported: {:?}) - \r
             run the migrate tool after upgrading",
            header_version,
            crate::storage::segment::SUPPORTED_SEGMENT_VERSIONS
        )));
    }

    let footer_source = match read_latest_sidecar_footer(path)? {
        Some(sidecar) if sidecar.data_end <= file_size => sidecar,
        _ => read_embedded_footer(path)?,
    };
    let footer_data = footer_source.footer;

    let parsed = parse_segment_footer(footer_data.as_slice())?;
    let version = parsed.version;
    let generation = parsed.generation;
    let header_row_count = parsed.row_count;
    let row_group_count = parsed.row_groups.len() as u32;
    let created_at = parsed.created_at;
    let modified_at = parsed.modified_at;

    let mut footer_row_count: u64 = 0;
    let mut compressed_size: u64 = 0;
    let mut uncompressed_size: u64 = 0;
    let mut row_groups = Vec::with_capacity(parsed.row_groups.len());
    for rg in &parsed.row_groups {
        footer_row_count += rg.row_count;
        compressed_size += rg.compressed_sizes.iter().sum::<u64>();
        uncompressed_size += rg.uncompressed_sizes.iter().sum::<u64>();
        row_groups.push(RowGroupInfo {
            offset: rg.offset,
            length: rg.length,
            row_count: rg.row_count,
            compressed_sizes: rg.compressed_sizes.clone(),
            uncompressed_sizes: rg.uncompressed_sizes.clone(),
            zone_maps: rg.zone_maps.clone(),
        });
    }

    let row_count = if footer_row_count > 0 {
        footer_row_count
    } else {
        header_row_count
    };

    let avg_row_group_size = if row_group_count > 0 {
        row_count / row_group_count as u64
    } else {
        0
    };

    Ok(SegmentInspectInfo {
        path: path.to_path_buf(),
        header_magic,
        header_version,
        header_created_at,
        header_flags,
        version,
        generation,
        row_count,
        header_row_count,
        footer_row_count,
        row_group_count,
        avg_row_group_size,
        file_size,
        compressed_size,
        uncompressed_size,
        created_at,
        modified_at,
        file_created,
        file_modified,
        schema: parsed.schema.clone(),
        row_groups,
        partition_key,
        sequence,
        deleted_row_count: parsed.deleted_row_count,
        delete_bitmap_len: parsed.delete_bitmap.len() as u32,
    })
}

pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
    if !table_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in fs::read_dir(table_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if (name.starts_with("seg_") || is_partition_filename(name))
                && name.ends_with(".dat")
                && !name.starts_with("wal_")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn is_partition_filename(name: &str) -> bool {
    if !name.starts_with('p') && !name.starts_with('P') {
        return false;
    }
    name.contains('_')
}

pub fn parse_partition_filename(filename: &str) -> (String, u32) {
    let base = filename.trim_end_matches(".dat");

    if let Some(seq_str) = base.strip_prefix("seg_") {
        if let Ok(seq) = seq_str.parse::<u32>() {
            return (String::new(), seq);
        }
        return (String::new(), 0);
    }

    let first_char = base.chars().next().unwrap_or(' ');
    if (first_char == 'p' || first_char == 'P') && base.contains('_') {
        let rest = &base[1..];
        if let Some(pos) = rest.find('_') {
            let key = &rest[..pos];
            let seq_str = &rest[pos + 1..];
            if let Ok(seq) = seq_str.parse::<u32>() {
                return (key.to_string(), seq);
            }
        }
    }

    (String::new(), 0)
}

pub fn format_size(size: u64) -> String {
    const UNIT: u64 = 1024;
    if size < UNIT {
        return format!("{} B", size);
    }
    let mut div = UNIT;
    let mut exp = 0;
    let mut n = size / UNIT;
    while n >= UNIT {
        div *= UNIT;
        exp += 1;
        n /= UNIT;
    }
    format!(
        "{:.2} {}B",
        size as f64 / div as f64,
        b"KMGTPE"[exp] as char
    )
}

pub fn format_timestamp(ts: u64) -> String {
    if ts == 0 {
        return "N/A".to_string();
    }
    format_datetime_from_timestamp(ts)
}

fn format_datetime_from_timestamp(secs: u64) -> String {
    const SECONDS_PER_DAY: u64 = 86400;
    const SECONDS_PER_HOUR: u64 = 3600;
    const SECONDS_PER_MINUTE: u64 = 60;

    let days_since_epoch = secs / SECONDS_PER_DAY;
    let remaining_secs = secs % SECONDS_PER_DAY;

    let hour = remaining_secs / SECONDS_PER_HOUR;
    let minute = (remaining_secs % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE;
    let second = remaining_secs % SECONDS_PER_MINUTE;

    let mut year = 1970u64;
    let mut days_left = days_since_epoch;

    loop {
        let days_in_year = if is_leap_year(year) { 366u64 } else { 365u64 };
        if days_left >= days_in_year {
            days_left -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    let month_days = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1u64;
    let mut day = days_left + 1;
    for &days_in_month in &month_days {
        if day <= days_in_month as u64 {
            break;
        }
        day -= days_in_month as u64;
        month += 1;
    }

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, minute, second
    )
}

pub fn format_system_time(time: Option<SystemTime>) -> String {
    let time = match time {
        Some(t) => t,
        None => return "N/A".to_string(),
    };

    let duration = match time.duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(_) => return "N/A".to_string(),
    };

    format_datetime_from_timestamp(duration.as_secs())
}

fn is_leap_year(year: u64) -> bool {
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

#[cfg(test)]
#[path = "segment_test.rs"]
mod tests;
