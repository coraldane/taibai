use std::sync::OnceLock;

pub(super) const SEGMENT_BUFWRITER_CAP: usize = 512 * 1024;
pub(super) const FLUSH_CHECK_INTERVAL_SECS: u64 = 10;
pub(super) const FLUSH_IDLE_THRESHOLD_SECS: u64 = 5;
pub(super) const FLUSH_META_AFTER_ROW_GROUP: bool = false;
pub(super) const SIDECAR_COMPACT_MIN_RECORDS: usize = 256;
pub(super) const SIDECAR_COMPACT_MIN_BYTES: u64 = 4 * 1024 * 1024;

const DEFAULT_MIN_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;
const DEFAULT_READER_CACHE_CAPACITY: usize = 1024;

pub struct SegmentConfig {
    pub min_segment_size: u64,
    pub max_segment_size: u64,
    pub row_group_target_rows: usize,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            min_segment_size: DEFAULT_MIN_SEGMENT_SIZE,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
            row_group_target_rows: 131_072,
        }
    }
}

pub(super) fn bulk_compression_level() -> i32 {
    static LEVEL: OnceLock<i32> = OnceLock::new();
    *LEVEL.get_or_init(|| {
        std::env::var("TAIBAI_BULK_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1)
    })
}

pub(super) fn reader_cache_capacity() -> usize {
    static CAPACITY: OnceLock<usize> = OnceLock::new();
    *CAPACITY.get_or_init(|| {
        std::env::var("TAIBAI_READER_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(DEFAULT_READER_CACHE_CAPACITY)
    })
}
