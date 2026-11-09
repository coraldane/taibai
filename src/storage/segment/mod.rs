use crate::storage::index::bloomfilter::BloomFilter;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

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

impl RowGroupMeta {
    /// Empty row group, used as the decode scratch of
    /// [`crate::storage::segment::footer::SegmentFooterIndex`]. Callers reuse
    /// one instance across a full-footer walk: the vectors keep their
    /// capacity, so the walk does not allocate per row group.
    pub fn empty() -> Self {
        Self {
            offset: 0,
            length: 0,
            row_count: 0,
            column_compressed_sizes: Vec::new(),
            compressed_sizes: Vec::new(),
            uncompressed_sizes: Vec::new(),
            zone_maps: Vec::new(),
            bloom_filters: Vec::new(),
            value_sets: Vec::new(),
            col_sums: Vec::new(),
            column_crcs: Vec::new(),
        }
    }
}

/// Heap bytes owned by one parsed row group, beyond its struct header.
///
/// The vector capacities matter more than the lengths: a footer parsed from a
/// 2 GiB segment carries ~28k row groups at the default 8192-row granule, and
/// each one owns eight `Vec`s plus a `ZoneMap` per column. Measured on a real
/// `md_trade` segment: 23.5 MiB of footer bytes expand to ~68 MiB of heap,
/// i.e. ~2.4 KiB per row group. Charging caches by ENTRY COUNT instead of by
/// this number is what let a "1024-segment" cache become a 25 GiB resident
/// set.
pub fn row_group_heap_bytes(row_group: &RowGroupMeta) -> u64 {
    let mut total = std::mem::size_of::<RowGroupMeta>() as u64;
    total += vec_header_bytes::<u64>(row_group.column_compressed_sizes.capacity());
    total += vec_header_bytes::<u64>(row_group.compressed_sizes.capacity());
    total += vec_header_bytes::<u64>(row_group.uncompressed_sizes.capacity());
    total += vec_header_bytes::<ZoneMap>(row_group.zone_maps.capacity());
    total += vec_header_bytes::<Option<BloomFilter>>(row_group.bloom_filters.capacity());
    total += vec_header_bytes::<Option<Vec<ScalarValue>>>(row_group.value_sets.capacity());
    total += vec_header_bytes::<i64>(row_group.col_sums.capacity());
    total += vec_header_bytes::<u32>(row_group.column_crcs.capacity());
    total += row_group
        .zone_maps
        .iter()
        .map(zone_map_heap_bytes)
        .sum::<u64>();
    total += row_group
        .bloom_filters
        .iter()
        .flatten()
        .map(|filter| filter.heap_bytes() as u64)
        .sum::<u64>();
    total += row_group
        .value_sets
        .iter()
        .flatten()
        .map(|values| {
            vec_header_bytes::<ScalarValue>(values.capacity())
                + values
                    .iter()
                    .map(|value| value.heap_bytes() as u64)
                    .sum::<u64>()
        })
        .sum::<u64>();
    total
}

/// Heap bytes owned by a parsed footer's row-group vector.
///
/// Charged from `len` rather than `capacity`: a slice lost the allocation
/// size on the way in, and a footer's row-group vector is built by
/// `with_capacity` + push, so the two are equal in practice.
pub fn row_groups_heap_bytes(row_groups: &[RowGroupMeta]) -> u64 {
    vec_header_bytes::<RowGroupMeta>(row_groups.len())
        + row_groups.iter().map(row_group_heap_bytes).sum::<u64>()
}

/// Heap bytes owned by a schema, beyond its struct header.
pub fn schema_heap_bytes(schema: &Schema) -> u64 {
    let mut total = 0u64;
    for column in &schema.columns {
        total += column.name.capacity() as u64;
    }
    total
}

/// Heap bytes owned by a segment-level zone-map vector.
pub fn zone_maps_heap_bytes(zone_maps: &[ZoneMap]) -> u64 {
    vec_header_bytes::<ZoneMap>(zone_maps.len())
        + zone_maps.iter().map(zone_map_heap_bytes).sum::<u64>()
}

fn vec_header_bytes<T>(capacity: usize) -> u64 {
    (capacity * std::mem::size_of::<T>()) as u64
}

fn zone_map_heap_bytes(map: &ZoneMap) -> u64 {
    (map.min_value.heap_bytes() + map.max_value.heap_bytes()) as u64
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

#[cfg(test)]
#[path = "atomic_publish_test.rs"]
mod atomic_publish_tests;
