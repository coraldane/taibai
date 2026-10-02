pub(super) const SEGMENT_BUFWRITER_CAP: usize = 512 * 1024;
pub(super) const FLUSH_CHECK_INTERVAL_SECS: u64 = 10;
pub(super) const FLUSH_IDLE_THRESHOLD_SECS: u64 = 5;
pub(super) const FLUSH_META_AFTER_ROW_GROUP: bool = false;
pub(super) const SIDECAR_COMPACT_MIN_RECORDS: usize = 256;
pub(super) const SIDECAR_COMPACT_MIN_BYTES: u64 = 4 * 1024 * 1024;

const DEFAULT_MIN_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

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
            row_group_target_rows: 8_192,
        }
    }
}

/// Bulk-write zstd level, installed from `compression.bulk_zstd_level` in the
/// config file during server startup.
pub(crate) fn bulk_compression_level() -> i32 {
    crate::common::config::bulk_zstd_level()
}

/// Effective zstd level for a table: the per-table `COMPRESSION_LEVEL` DDL
/// override when set, otherwise the global configured level.
pub(crate) fn resolve_compression_level(override_level: Option<i32>) -> i32 {
    override_level.unwrap_or_else(bulk_compression_level)
}

/// Reader/meta cache capacity, installed from `storage.reader_cache_capacity`
/// in the config file during server startup. The reader cache converts this
/// entry count into a byte budget of `capacity * 256 KiB` (256 MiB by
/// default) and evicts by LRU bytes; see `reader_cache::ReaderCache`.
pub(super) fn reader_cache_capacity() -> usize {
    crate::common::config::reader_cache_capacity()
}
