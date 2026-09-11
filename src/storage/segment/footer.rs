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
use super::writer::VALUE_SET_EXTENSION_MAGIC;
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

    let mut row_groups = Vec::with_capacity(row_group_count as usize);
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
        });
    }

    let deleted_row_count = read_u64_le(&mut cursor).unwrap_or(0);
    let bitmap_len = read_u32_le(&mut cursor).unwrap_or(0) as usize;
    let mut delete_bitmap = vec![0u8; bitmap_len];
    if bitmap_len > 0 {
        cursor.read_exact(&mut delete_bitmap)?;
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

fn peek_u32(buf: &[u8], cursor: &Cursor<&[u8]>) -> Option<u32> {
    let pos = cursor.position() as usize;
    if pos + 4 > buf.len() {
        return None;
    }
    Some(u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?))
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
    }
    Ok(cursor.position() as usize)
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

#[cfg(test)]
#[path = "footer_test.rs"]
mod tests;
