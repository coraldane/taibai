use std::io::{Cursor, Read};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::codec::io_util::{read_len_prefixed_str, read_u32_le, read_u64_le, read_u8};
use crate::storage::codec::scalar_tag::decode_scalar_with_tag;
use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::partition::pruning::ZoneMap;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

use super::constants::SEGMENT_MAGIC;
use super::footer_ext::{
    read_col_sum_extension, read_column_crc_extension, read_footer_tail, read_value_set_extension,
    verify_footer_crc, FooterTailEntry,
};
use super::RowGroupMeta;

pub struct ParsedSegmentFooter {
    pub version: u32,
    pub generation: u64,
    pub created_at: u64,
    pub modified_at: u64,
    pub row_count: u64,
    pub row_groups: Vec<RowGroupMeta>,
    pub schema: Schema,
    pub deleted_row_count: u64,
    pub delete_bitmap: Vec<u8>,
    pub segment_zone_maps: Vec<ZoneMap>,
    /// Entries of the footer tail TLV block (empty for v1 footers and v2
    /// footers without entries). The delete-footer rewrite re-emits them so
    /// extensions survive tombstone updates.
    pub tail: Vec<FooterTailEntry>,
}

/// Compact, resident index of a segment footer.
///
/// Holds the segment-level metadata (schema, segment zone maps, delete
/// bitmap, tail) plus a 4-byte footer offset and an 8-byte row count per row
/// group. The row-group records themselves — per-column sizes, zone maps,
/// CRCs and pre-aggregated sums — stay in the footer byte buffer and are
/// decoded on demand by [`Self::decode_row_group`].
///
/// Why not keep the decoded graph: on a real 2 GiB `md_trade` segment
/// (210,193,861 rows, 28,315 row groups) the footer is 23.5 MiB on disk and
/// parses into ~68 MiB of heap, because every row group owns eight parallel
/// vectors plus one two-`ScalarValue` zone map per column. Nothing needs all
/// of it at once: pruning walks the zone maps once and discards them, and
/// decoding reads one row group's layout at a time. Keeping only the index
/// costs ~340 KiB for that same segment (~200x less), and the footer bytes
/// can be served straight out of the segment's mmap — reclaimable page
/// cache rather than anonymous RSS.
///
/// This is the layout Parquet and ClickHouse converge on: a resident
/// footer/index of bounded size, with the bulky per-group metadata addressed
/// by offset instead of being materialised.
#[derive(Debug, Clone)]
pub struct SegmentFooterIndex {
    pub version: u32,
    pub generation: u64,
    pub created_at: u64,
    pub modified_at: u64,
    pub row_count: u64,
    pub deleted_row_count: u64,
    pub delete_bitmap: Vec<u8>,
    pub segment_zone_maps: Vec<ZoneMap>,
    pub schema: Schema,
    pub tail: Vec<FooterTailEntry>,
    /// Byte offset of each row-group record inside the footer buffer.
    pub row_group_offsets: Vec<u32>,
    /// Row count of each row group, parallel to `row_group_offsets`. Kept
    /// resident because row-number arithmetic (delete-bitmap filtering,
    /// `row_group_start_row`) runs per scan for every row group.
    pub row_group_row_counts: Vec<u64>,
    /// Column count in footer space; a row-group record is self-describing
    /// only up to this width, so decoding needs it.
    pub col_count: usize,
}

/// Layout prefix of a row-group record: where its column blocks live and how
/// many rows they hold.
///
/// These fields all precede the zone maps in the on-disk record, so decoding
/// them skips the variable-length tail entirely. Column reads need nothing
/// else, which keeps the decode cost off the per-column hot path.
#[derive(Debug, Clone, Default)]
pub struct RowGroupLayout {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub column_compressed_sizes: Vec<u64>,
}

impl SegmentFooterIndex {
    fn row_group_cursor<'a>(&self, buf: &'a [u8], index: usize) -> Result<Cursor<&'a [u8]>> {
        let offset = *self.row_group_offsets.get(index).ok_or_else(|| {
            TaibaiError::Storage(format!(
                "row group index {index} out of range ({} row groups)",
                self.row_group_offsets.len()
            ))
        })? as usize;
        if offset >= buf.len() {
            return Err(TaibaiError::Storage(format!(
                "row group {index} offset {offset} is outside the footer ({} bytes)",
                buf.len()
            )));
        }
        let mut cursor = Cursor::new(buf);
        cursor.set_position(offset as u64);
        Ok(cursor)
    }

    /// Decode only the layout prefix of a row group: file offset, length, row
    /// count and per-column compressed sizes.
    pub fn decode_row_group_layout(&self, buf: &[u8], index: usize) -> Result<RowGroupLayout> {
        let mut cursor = self.row_group_cursor(buf, index)?;
        let offset = read_u64_le(&mut cursor)?;
        let length = read_u64_le(&mut cursor)?;
        let row_count = read_u64_le(&mut cursor)?;
        let mut column_compressed_sizes = Vec::with_capacity(self.col_count);
        for _ in 0..self.col_count {
            column_compressed_sizes.push(read_u64_le(&mut cursor)?);
        }
        Ok(RowGroupLayout {
            offset,
            length,
            row_count,
            column_compressed_sizes,
        })
    }

    pub fn num_row_groups(&self) -> usize {
        self.row_group_offsets.len()
    }

    pub fn row_group_row_count(&self, index: usize) -> u64 {
        self.row_group_row_counts.get(index).copied().unwrap_or(0)
    }

    /// Decode one row group's metadata out of the footer buffer.
    ///
    /// Cost is O(that row group's record) — a few hundred bytes of sizes and
    /// zone maps at the default 8192-row granule, against the 550 KiB of
    /// column payload the caller is about to read. Callers that walk every
    /// row group should reuse one `RowGroupMeta` via
    /// [`Self::decode_row_group_into`] to keep the walk allocation-free.
    pub fn decode_row_group(&self, buf: &[u8], index: usize) -> Result<RowGroupMeta> {
        let mut row_group = RowGroupMeta::empty();
        self.decode_row_group_into(buf, index, &mut row_group)?;
        Ok(row_group)
    }

    /// [`Self::decode_row_group`] into a caller-owned buffer, clearing the
    /// previous contents in place so a full-footer walk performs no
    /// allocation per row group.
    pub fn decode_row_group_into(
        &self,
        buf: &[u8],
        index: usize,
        row_group: &mut RowGroupMeta,
    ) -> Result<()> {
        let offset = *self.row_group_offsets.get(index).ok_or_else(|| {
            TaibaiError::Storage(format!(
                "row group index {index} out of range ({} row groups)",
                self.row_group_offsets.len()
            ))
        })? as usize;
        if offset >= buf.len() {
            return Err(TaibaiError::Storage(format!(
                "row group {index} offset {offset} is outside the footer ({} bytes)",
                buf.len()
            )));
        }
        let mut cursor = Cursor::new(buf);
        cursor.set_position(offset as u64);
        decode_row_group_record(buf, &mut cursor, self.col_count, row_group)
    }

    /// Resident heap footprint of the index, in bytes.
    pub fn heap_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
            + crate::storage::segment::schema_heap_bytes(&self.schema)
            + crate::storage::segment::zone_maps_heap_bytes(&self.segment_zone_maps)
            + self.delete_bitmap.capacity() as u64
            + (self.row_group_offsets.capacity() as u64) * 4
            + (self.row_group_row_counts.capacity() as u64) * 8
    }
}

pub fn parse_segment_footer(buf: &[u8]) -> Result<ParsedSegmentFooter> {
    let mut cursor = Cursor::new(buf);

    let magic = read_u32_le(&mut cursor)?;
    if magic != SEGMENT_MAGIC {
        return Err(TaibaiError::Storage(format!(
            "invalid segment magic: {:08X}, expected {:08X}",
            magic, SEGMENT_MAGIC
        )));
    }

    let version = read_u32_le(&mut cursor)?;
    let generation = read_u64_le(&mut cursor)?;
    let row_count = read_u64_le(&mut cursor)?;
    let row_group_count = read_u32_le(&mut cursor)?;
    let created_at = read_u64_le(&mut cursor)?;
    let modified_at = read_u64_le(&mut cursor)?;

    let _table_name = read_len_prefixed_str(&mut cursor)?;
    let col_count = read_u32_le(&mut cursor)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let name = read_len_prefixed_str(&mut cursor)?;
        let dtype_byte = read_u8(&mut cursor)?;
        let nullable = read_u8(&mut cursor)? != 0;
        let scale = read_u8(&mut cursor)? as i32;
        let precision = read_u8(&mut cursor)? as i32;
        let column_id = read_u32_le(&mut cursor)?;
        columns.push(ColumnDef {
            name,
            data_type: DataType::from_u8(dtype_byte)?,
            nullable,
            scale,
            precision,
            auto_increment: false,
            primary_key: false,
            default_value: None,
            column_id,
            previous_names: Vec::new(),
            added_at_micros: None,
        });
    }
    let schema = Schema::new(columns);

    let mut segment_zone_maps = Vec::new();
    let has_seg_zm = read_u8(&mut cursor).unwrap_or(0);
    if has_seg_zm != 0 {
        let seg_zm_count = read_u32_le(&mut cursor).unwrap_or(0) as usize;
        for _ in 0..seg_zm_count {
            let min_v = decode_scalar_with_tag(&mut cursor).unwrap_or(ScalarValue::Int64(None));
            let max_v = decode_scalar_with_tag(&mut cursor).unwrap_or(ScalarValue::Int64(None));
            let null_count = read_u64_le(&mut cursor).unwrap_or(0);
            let zm_row_count = read_u64_le(&mut cursor).unwrap_or(0);
            segment_zone_maps.push(ZoneMap {
                min_value: min_v,
                max_value: max_v,
                null_count,
                row_count: zm_row_count,
            });
        }
    }

    // Each row group consumes at least (3 + 3*col_count) u64s of footer
    // space; a corrupted count must not turn into a multi-GB allocation
    // before the read loop errors out.
    let row_group_alloc_bound = buf.len() / 8 / (3 + col_count.max(1) * 3);
    let mut row_groups = Vec::with_capacity((row_group_count as usize).min(row_group_alloc_bound));
    for _ in 0..row_group_count {
        let mut row_group = RowGroupMeta::empty();
        decode_row_group_record(buf, &mut cursor, col_count, &mut row_group)?;
        row_groups.push(row_group);
    }

    let deleted_row_count = read_u64_le(&mut cursor).unwrap_or(0);
    let bitmap_len = read_u32_le(&mut cursor).unwrap_or(0) as usize;
    // Bound by remaining footer bytes: a corrupted length must error out
    // instead of allocating up to 4GB.
    if bitmap_len > buf.len().saturating_sub(cursor.position() as usize) {
        return Err(TaibaiError::Storage(format!(
            "delete bitmap length {} exceeds remaining footer bytes",
            bitmap_len
        )));
    }
    let mut delete_bitmap = vec![0u8; bitmap_len];
    if bitmap_len > 0 {
        cursor.read_exact(&mut delete_bitmap)?;
    }

    // Footer tail TLV entries (optional; v2+). Written before the checksum
    // so FCR3 keeps covering the whole footer.
    let tail = read_footer_tail(buf, &mut cursor)?;

    // Whole-footer checksum extension (optional; absent in old segments).
    verify_footer_crc(buf, &mut cursor)?;

    if cursor.position() as usize != buf.len() {
        return Err(TaibaiError::Storage(format!(
            "trailing bytes after segment footer: parsed {}, footer size {}",
            cursor.position(),
            buf.len()
        )));
    }

    Ok(ParsedSegmentFooter {
        version,
        generation,
        created_at,
        modified_at,
        row_count,
        row_groups,
        schema,
        deleted_row_count,
        delete_bitmap,
        segment_zone_maps,
        tail,
    })
}

/// Decode one row-group record into `out`, clearing `out` first.
///
/// Single decoder for both the eager [`parse_segment_footer`] and the lazy
/// [`SegmentFooterIndex::decode_row_group_into`], so the eager and lazy paths
/// cannot drift apart in their reading of the on-disk layout.
fn decode_row_group_record(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
    out: &mut RowGroupMeta,
) -> Result<()> {
    out.offset = read_u64_le(cursor)?;
    out.length = read_u64_le(cursor)?;
    out.row_count = read_u64_le(cursor)?;

    out.column_compressed_sizes.clear();
    out.column_compressed_sizes.reserve(col_count);
    for _ in 0..col_count {
        out.column_compressed_sizes.push(read_u64_le(cursor)?);
    }
    out.compressed_sizes.clear();
    out.compressed_sizes.reserve(col_count);
    for _ in 0..col_count {
        out.compressed_sizes.push(read_u64_le(cursor)?);
    }
    out.uncompressed_sizes.clear();
    out.uncompressed_sizes.reserve(col_count);
    for _ in 0..col_count {
        out.uncompressed_sizes.push(read_u64_le(cursor)?);
    }

    out.zone_maps.clear();
    out.zone_maps.reserve(col_count);
    for _ in 0..col_count {
        let min_value = decode_scalar_with_tag(cursor)?;
        let max_value = decode_scalar_with_tag(cursor)?;
        let null_count = read_u64_le(cursor)?;
        let row_count = read_u64_le(cursor)?;
        out.zone_maps.push(ZoneMap {
            min_value,
            max_value,
            null_count,
            row_count,
        });
    }

    out.bloom_filters.clear();
    out.bloom_filters.reserve(col_count);
    for _ in 0..col_count {
        let has_bloom = read_u8(cursor)?;
        if has_bloom != 0 {
            let bloom_len = read_u32_le(cursor)? as usize;
            let bloom_start = cursor.position() as usize;
            let bloom_end = bloom_start
                .checked_add(bloom_len)
                .ok_or_else(|| TaibaiError::Storage("invalid bloom filter length".to_string()))?;
            if bloom_end > buf.len() {
                return Err(TaibaiError::Storage(format!(
                    "invalid bloom filter length: {} at footer offset {} (footer size {})",
                    bloom_len,
                    bloom_start,
                    buf.len()
                )));
            }
            out.bloom_filters
                .push(BloomFilter::deserialize(&buf[bloom_start..bloom_end]));
            cursor.set_position(bloom_end as u64);
        } else {
            out.bloom_filters.push(None);
        }
    }

    out.value_sets = read_value_set_extension(buf, cursor, col_count)?;
    out.col_sums = read_col_sum_extension(buf, cursor, col_count)?;
    out.column_crcs = read_column_crc_extension(buf, cursor, col_count)?;
    Ok(())
}

/// Parse a footer into a resident [`SegmentFooterIndex`], dropping the
/// row-group bodies.
///
/// Walks the same bytes as [`parse_segment_footer`] (so a malformed footer
/// fails here too, not later on some unlucky scan) but retains only one
/// `u32` offset and one `u64` row count per row group. Peak heap is one
/// scratch [`RowGroupMeta`] regardless of how many row groups the segment
/// holds.
pub fn parse_segment_footer_index(buf: &[u8]) -> Result<SegmentFooterIndex> {
    let mut cursor = Cursor::new(buf);

    let magic = read_u32_le(&mut cursor)?;
    if magic != SEGMENT_MAGIC {
        return Err(TaibaiError::Storage(format!(
            "invalid segment magic: {:08X}, expected {:08X}",
            magic, SEGMENT_MAGIC
        )));
    }

    let version = read_u32_le(&mut cursor)?;
    let generation = read_u64_le(&mut cursor)?;
    let row_count = read_u64_le(&mut cursor)?;
    let row_group_count = read_u32_le(&mut cursor)?;
    let created_at = read_u64_le(&mut cursor)?;
    let modified_at = read_u64_le(&mut cursor)?;

    let _table_name = read_len_prefixed_str(&mut cursor)?;
    let col_count = read_u32_le(&mut cursor)? as usize;
    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let name = read_len_prefixed_str(&mut cursor)?;
        let dtype_byte = read_u8(&mut cursor)?;
        let nullable = read_u8(&mut cursor)? != 0;
        let scale = read_u8(&mut cursor)? as i32;
        let precision = read_u8(&mut cursor)? as i32;
        let column_id = read_u32_le(&mut cursor)?;
        columns.push(ColumnDef {
            name,
            data_type: DataType::from_u8(dtype_byte)?,
            nullable,
            scale,
            precision,
            auto_increment: false,
            primary_key: false,
            default_value: None,
            column_id,
            previous_names: Vec::new(),
            added_at_micros: None,
        });
    }
    let schema = Schema::new(columns);

    let mut segment_zone_maps = Vec::new();
    let has_seg_zm = read_u8(&mut cursor).unwrap_or(0);
    if has_seg_zm != 0 {
        let seg_zm_count = read_u32_le(&mut cursor).unwrap_or(0) as usize;
        for _ in 0..seg_zm_count {
            let min_value = decode_scalar_with_tag(&mut cursor).unwrap_or(ScalarValue::Int64(None));
            let max_value = decode_scalar_with_tag(&mut cursor).unwrap_or(ScalarValue::Int64(None));
            let null_count = read_u64_le(&mut cursor).unwrap_or(0);
            let zm_row_count = read_u64_le(&mut cursor).unwrap_or(0);
            segment_zone_maps.push(ZoneMap {
                min_value,
                max_value,
                null_count,
                row_count: zm_row_count,
            });
        }
    }

    // Same corruption bound as the eager parse: a bad count must not turn
    // into a multi-GB reservation.
    let row_group_alloc_bound = buf.len() / 8 / (3 + col_count.max(1) * 3);
    let planned = (row_group_count as usize).min(row_group_alloc_bound);
    let mut row_group_offsets = Vec::with_capacity(planned);
    let mut row_group_row_counts = Vec::with_capacity(planned);
    let mut scratch = RowGroupMeta::empty();
    for _ in 0..row_group_count {
        let offset = cursor.position();
        let offset = u32::try_from(offset).map_err(|_| {
            TaibaiError::Storage(format!(
                "footer too large for a 32-bit row-group offset: {} bytes",
                offset
            ))
        })?;
        decode_row_group_record(buf, &mut cursor, col_count, &mut scratch)?;
        row_group_offsets.push(offset);
        row_group_row_counts.push(scratch.row_count);
    }

    let deleted_row_count = read_u64_le(&mut cursor).unwrap_or(0);
    let bitmap_len = read_u32_le(&mut cursor).unwrap_or(0) as usize;
    if bitmap_len > buf.len().saturating_sub(cursor.position() as usize) {
        return Err(TaibaiError::Storage(format!(
            "delete bitmap length {} exceeds remaining footer bytes",
            bitmap_len
        )));
    }
    let mut delete_bitmap = vec![0u8; bitmap_len];
    if bitmap_len > 0 {
        cursor.read_exact(&mut delete_bitmap)?;
    }

    let tail = read_footer_tail(buf, &mut cursor)?;
    verify_footer_crc(buf, &mut cursor)?;

    if cursor.position() as usize != buf.len() {
        return Err(TaibaiError::Storage(format!(
            "trailing bytes after segment footer: parsed {}, footer size {}",
            cursor.position(),
            buf.len()
        )));
    }

    Ok(SegmentFooterIndex {
        version,
        generation,
        created_at,
        modified_at,
        row_count,
        deleted_row_count,
        delete_bitmap,
        segment_zone_maps,
        schema,
        tail,
        row_group_offsets,
        row_group_row_counts,
        col_count,
    })
}

pub fn reserved_section_offset(buf: &[u8]) -> Result<usize> {
    let mut cursor = Cursor::new(buf);
    let _magic = read_u32_le(&mut cursor)?;
    let _version = read_u32_le(&mut cursor)?;
    let _generation = read_u64_le(&mut cursor)?;
    let _row_count = read_u64_le(&mut cursor)?;
    let row_group_count = read_u32_le(&mut cursor)? as usize;
    let _created_at = read_u64_le(&mut cursor)?;
    let _modified_at = read_u64_le(&mut cursor)?;
    let _table_name = read_len_prefixed_str(&mut cursor)?;
    let col_count = read_u32_le(&mut cursor)? as usize;
    for _ in 0..col_count {
        let _name = read_len_prefixed_str(&mut cursor)?;
        let _dt = read_u8(&mut cursor)?;
        let _nullable = read_u8(&mut cursor)?;
        let _scale = read_u8(&mut cursor)?;
        let _precision = read_u8(&mut cursor)?;
        let _column_id = read_u32_le(&mut cursor)?;
    }

    let has_seg_zm = read_u8(&mut cursor).unwrap_or(0);
    if has_seg_zm != 0 {
        let seg_zm_count = read_u32_le(&mut cursor).unwrap_or(0) as usize;
        for _ in 0..seg_zm_count {
            let _ = decode_scalar_with_tag(&mut cursor)?;
            let _ = decode_scalar_with_tag(&mut cursor)?;
            let _ = read_u64_le(&mut cursor)?;
            let _ = read_u64_le(&mut cursor)?;
        }
    }

    for _ in 0..row_group_count {
        let _offset = read_u64_le(&mut cursor)?;
        let _length = read_u64_le(&mut cursor)?;
        let _rc = read_u64_le(&mut cursor)?;
        for _ in 0..col_count {
            let _ = read_u64_le(&mut cursor)?;
        }
        for _ in 0..col_count {
            let _ = read_u64_le(&mut cursor)?;
        }
        for _ in 0..col_count {
            let _ = read_u64_le(&mut cursor)?;
        }
        for _ in 0..col_count {
            let _ = decode_scalar_with_tag(&mut cursor)?;
            let _ = decode_scalar_with_tag(&mut cursor)?;
            let _ = read_u64_le(&mut cursor)?;
            let _ = read_u64_le(&mut cursor)?;
        }
        for _ in 0..col_count {
            let has_bf = read_u8(&mut cursor)?;
            if has_bf != 0 {
                let bf_len = read_u32_le(&mut cursor)? as usize;
                let start = cursor.position() as usize;
                let end = start.checked_add(bf_len).ok_or_else(|| {
                    TaibaiError::Storage("invalid bloom filter length".to_string())
                })?;
                if end > buf.len() {
                    return Err(TaibaiError::Storage(format!(
                        "invalid bloom filter length: {} at footer offset {} (footer size {})",
                        bf_len,
                        start,
                        buf.len()
                    )));
                }
                cursor.set_position(end as u64);
            }
        }
        super::footer_ext::skip_row_group_extensions(buf, &mut cursor, col_count)?;
    }
    // The reserved section ends before the delete bitmap; tail TLV
    // entries and the checksum live after it and are not part of the
    // preserved prefix.
    Ok(cursor.position() as usize)
}

#[cfg(test)]
#[path = "footer_test.rs"]
mod tests;
