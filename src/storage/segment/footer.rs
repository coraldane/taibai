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
use super::writer::{
    COL_SUM_EXTENSION_MAGIC, COLUMN_CRC_EXTENSION_MAGIC, FOOTER_CRC_EXTENSION_MAGIC,
    VALUE_SET_EXTENSION_MAGIC,
};
use super::RowGroupMeta;

pub struct ParsedSegmentFooter {
    pub version: u32,
    pub row_count: u64,
    pub row_groups: Vec<RowGroupMeta>,
    pub schema: Schema,
    pub deleted_row_count: u64,
    pub delete_bitmap: Vec<u8>,
    pub segment_zone_maps: Vec<ZoneMap>,
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
    let _generation = read_u64_le(&mut cursor)?;
    let row_count = read_u64_le(&mut cursor)?;
    let row_group_count = read_u32_le(&mut cursor)?;
    let _created_at = read_u64_le(&mut cursor)?;
    let _modified_at = read_u64_le(&mut cursor)?;

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
        let offset = read_u64_le(&mut cursor)?;
        let length = read_u64_le(&mut cursor)?;
        let rg_row_count = read_u64_le(&mut cursor)?;

        let mut _column_sizes = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            _column_sizes.push(read_u64_le(&mut cursor)?);
        }
        let mut comp_sizes = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            comp_sizes.push(read_u64_le(&mut cursor)?);
        }
        let mut uncompressed_sizes = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            uncompressed_sizes.push(read_u64_le(&mut cursor)?);
        }

        let mut zone_maps = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let min_v = decode_scalar_with_tag(&mut cursor)?;
            let max_v = decode_scalar_with_tag(&mut cursor)?;
            let null_count = read_u64_le(&mut cursor)?;
            let zm_row_count = read_u64_le(&mut cursor)?;
            zone_maps.push(ZoneMap {
                min_value: min_v,
                max_value: max_v,
                null_count,
                row_count: zm_row_count,
            });
        }

        let mut bloom_filters = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let has_bf = read_u8(&mut cursor)?;
            if has_bf != 0 {
                let bf_len = read_u32_le(&mut cursor)? as usize;
                let bf_data_start = cursor.position() as usize;
                let bf_data_end = bf_data_start.checked_add(bf_len).ok_or_else(|| {
                    TaibaiError::Storage("invalid bloom filter length".to_string())
                })?;
                if bf_data_end > buf.len() {
                    return Err(TaibaiError::Storage(format!(
                        "invalid bloom filter length: {} at footer offset {} (footer size {})",
                        bf_len,
                        bf_data_start,
                        buf.len()
                    )));
                }
                let bf_data = &buf[bf_data_start..bf_data_start + bf_len];
                let bf = BloomFilter::deserialize(bf_data);
                cursor.set_position(bf_data_end as u64);
                bloom_filters.push(bf);
            } else {
                bloom_filters.push(None);
            }
        }

        let value_sets = if peek_u32(buf, &cursor) == Some(VALUE_SET_EXTENSION_MAGIC) {
            let _magic = read_u32_le(&mut cursor)?;
            let mut value_sets = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let has_set = read_u8(&mut cursor)?;
                if has_set != 0 {
                    let value_count = read_u32_le(&mut cursor)? as usize;
                    let mut values = Vec::with_capacity(value_count);
                    for _ in 0..value_count {
                        values.push(decode_scalar_with_tag(&mut cursor)?);
                    }
                    value_sets.push(Some(values));
                } else {
                    value_sets.push(None);
                }
            }
            value_sets
        } else {
            vec![None; col_count]
        };

        let col_sums = parse_col_sum_extension(buf, &mut cursor, col_count)?;
        let column_crcs = parse_column_crc_extension(buf, &mut cursor, col_count)?;

        row_groups.push(RowGroupMeta {
            offset,
            length,
            row_count: rg_row_count,
            column_compressed_sizes: _column_sizes,
            compressed_sizes: comp_sizes,
            uncompressed_sizes,
            zone_maps,
            bloom_filters,
            value_sets,
            col_sums,
            column_crcs,
        });
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

    // Whole-footer checksum extension (optional; absent in old segments).
    if peek_u32(buf, &cursor) == Some(FOOTER_CRC_EXTENSION_MAGIC) {
        let _magic = read_u32_le(&mut cursor)?;
        let mut crc_bytes = [0u8; 4];
        cursor.read_exact(&mut crc_bytes)?;
        let stored = u32::from_le_bytes(crc_bytes);
        let computed = crc32fast::hash(&buf[..cursor.position() as usize - 8]);
        if stored != computed {
            return Err(TaibaiError::Storage(
                "segment footer checksum mismatch".to_string(),
            ));
        }
    }

    if cursor.position() as usize != buf.len() {
        return Err(TaibaiError::Storage(format!(
            "trailing bytes after segment footer: parsed {}, footer size {}",
            cursor.position(),
            buf.len()
        )));
    }

    Ok(ParsedSegmentFooter {
        version,
        row_count,
        row_groups,
        schema,
        deleted_row_count,
        delete_bitmap,
        segment_zone_maps,
    })
}

/// Parse the per-row-group column-CRC extension when the magic is next.
/// Absent magic (segments written before checksums) yields an empty vec;
/// readers then skip block verification.
fn parse_column_crc_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<Vec<u32>> {
    if peek_u32(buf, cursor) != Some(COLUMN_CRC_EXTENSION_MAGIC) {
        return Ok(Vec::new());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    if stored_count != col_count {
        return Err(TaibaiError::Storage(format!(
            "column-crc extension count mismatch: {} columns stored, {} in schema",
            stored_count, col_count
        )));
    }
    let mut crcs = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let mut bytes = [0u8; 4];
        cursor.read_exact(&mut bytes)?;
        crcs.push(u32::from_le_bytes(bytes));
    }
    Ok(crcs)
}

fn peek_u32(buf: &[u8], cursor: &Cursor<&[u8]>) -> Option<u32> {
    let pos = cursor.position() as usize;
    if pos + 4 > buf.len() {
        return None;
    }
    Some(u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?))
}

/// Parse the per-row-group column-sum extension when the magic is next.
/// Absent magic (segments written before the extension) yields an empty
/// vec, which every consumer treats as "sums unavailable". The stored
/// column count must match the schema — a mismatch means the magic bytes
/// were random data (e.g. a row-group offset colliding with the magic), so
/// fail loudly instead of misfolding sums.
fn parse_col_sum_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<Vec<i64>> {
    if peek_u32(buf, cursor) != Some(COL_SUM_EXTENSION_MAGIC) {
        return Ok(Vec::new());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    if stored_count != col_count {
        return Err(TaibaiError::Storage(format!(
            "column-sum extension count mismatch: {} columns stored, {} in schema",
            stored_count, col_count
        )));
    }
    let mut col_sums = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let mut bytes = [0u8; 8];
        cursor.read_exact(&mut bytes)?;
        col_sums.push(i64::from_le_bytes(bytes));
    }
    Ok(col_sums)
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
        skip_value_set_extension(buf, &mut cursor, col_count)?;
        skip_col_sum_extension(buf, &mut cursor, col_count)?;
        skip_column_crc_extension(buf, &mut cursor, col_count)?;
    }
    Ok(cursor.position() as usize)
}

fn skip_column_crc_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<()> {
    if peek_u32(buf, cursor) != Some(COLUMN_CRC_EXTENSION_MAGIC) {
        return Ok(());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    // Same validation as the full parser: a mismatched count must error,
    // not silently desynchronize the reserved-section offset (the rewrite
    // path would then truncate the footer at a wrong boundary).
    if stored_count != col_count {
        return Err(TaibaiError::Storage(format!(
            "column-crc extension count mismatch: {} columns stored, {} in schema",
            stored_count, col_count
        )));
    }
    for _ in 0..col_count {
        let mut bytes = [0u8; 4];
        cursor.read_exact(&mut bytes)?;
    }
    Ok(())
}

fn skip_value_set_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<()> {
    if peek_u32(buf, cursor) != Some(VALUE_SET_EXTENSION_MAGIC) {
        return Ok(());
    }

    let _magic = read_u32_le(cursor)?;
    for _ in 0..col_count {
        let has_set = read_u8(cursor)?;
        if has_set != 0 {
            let value_count = read_u32_le(cursor)? as usize;
            for _ in 0..value_count {
                let _ = decode_scalar_with_tag(cursor)?;
            }
        }
    }
    Ok(())
}

fn skip_col_sum_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<()> {
    if peek_u32(buf, cursor) != Some(COL_SUM_EXTENSION_MAGIC) {
        return Ok(());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    if stored_count != col_count {
        return Err(TaibaiError::Storage(
            "column-sum extension count mismatch while scanning footer".to_string(),
        ));
    }
    cursor.set_position(cursor.position() + stored_count as u64 * 8);
    Ok(())
}

#[cfg(test)]
#[path = "footer_test.rs"]
mod tests;
