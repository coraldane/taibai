#![allow(dead_code)]
use crate::storage::index::bloomfilter::BloomFilter;

pub mod constants;
pub mod footer;
pub mod reader;
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
}

#[allow(unused_imports)]
pub use constants::{SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
pub use reader::SegmentReader;
pub use writer::{encode_segment_footer, encode_segment_header, SegmentWriter};

#[allow(unused_imports)]
pub use crate::storage::codec::scalar_tag::decode_scalar_with_tag;
pub use crate::storage::partition::pruning::{
    RangeClause, RangeOp, RowGroupPredicate, ScanStats, ZoneMap,
};