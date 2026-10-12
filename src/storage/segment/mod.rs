use crate::storage::index::bloomfilter::BloomFilter;

pub mod constants;
pub mod delete_bitmap;
pub mod footer;
pub mod footer_ext;
pub mod meta;
pub mod migrate;
pub mod reader;
pub mod snapshot;
pub mod store;
pub mod writer;

#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub column_compressed_sizes: Vec<u64>,
    pub compressed_sizes: Vec<u64>,
    pub uncompressed_sizes: Vec<u64>,
    pub zone_maps: Vec<crate::storage::partition::pruning::ZoneMap>,
    pub bloom_filters: Vec<Option<BloomFilter>>,
    pub value_sets: Vec<Option<Vec<crate::types::scalar::ScalarValue>>>,
    /// Per-column pre-aggregated sums (wrapping i64, non-null values only)
    /// persisted in the footer's column-sum extension. One entry per column
    /// when the extension is present; empty for segments written before it
    /// existed. Entries for columns outside [`column_sum_supported`] carry 0
    /// and are never consulted.
    pub col_sums: Vec<i64>,
    /// CRC32 of each column's stored (compressed) block, persisted in the
    /// footer's column-CRC extension. Empty for segments written before the
    /// extension existed; readers verify block reads whenever present.
    pub column_crcs: Vec<u32>,
}

/// Column types whose values have an exact i64 view, so a footer-persisted
/// running sum (and run-level folding) is bit-identical to `col_sum_i64`
/// over the decoded column. UInt64 is excluded: its u64 domain can wrap the
/// i64 view (and `col_sum_i64` saturates it instead).
pub fn column_sum_supported(dt: crate::protocol::types::DataType) -> bool {
    use crate::protocol::types::DataType;
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Decimal
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Interval
    )
}

#[allow(unused_imports)]
pub use constants::{
    is_supported_segment_version, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION,
    SUPPORTED_SEGMENT_VERSIONS,
};
pub use reader::SegmentReader;
pub use writer::{encode_segment_footer, encode_segment_header, SegmentWriter};

#[allow(unused_imports)]
pub use crate::storage::codec::scalar_tag::decode_scalar_with_tag;
pub use crate::storage::partition::pruning::{
    RangeClause, RangeOp, RowGroupPredicate, ScanStats, ZoneMap,
};

#[cfg(test)]
#[path = "statement_journal_test.rs"]
mod statement_journal_tests;
