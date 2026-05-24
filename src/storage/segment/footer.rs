use std::io::{Cursor, Read};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::bloomfilter::BloomFilter;
use crate::storage::encoding::{
    decode_scalar_with_tag, read_len_prefixed_str, read_u32_le, read_u64_le, read_u8,
};
use crate::storage::pruning::ZoneMap;
use crate::types::batch::{ColumnDef, Schema};

use super::constants::SEGMENT_MAGIC;
use super::RowGroupMeta;

pub struct ParsedSegmentFooter {
    pub version: u32,
    pub row_count: u64,
    pub row_groups: Vec<RowGroupMeta>,
    pub schema: Schema,
    pub deleted_row_count: u64,
    pub delete_bitmap: Vec<u8>,
}

pub fn parse_segment_footer(buf: &[u8]) -> Result<ParsedSegmentFooter> {
    let mut cursor = Cursor::new(buf);

    let magic = read_u32_le(&mut cursor)?;
    if magic != SEGMENT_MAGIC {
        return Err(MonaxError::Storage(format!(
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
        let scale = read_u32_le(&mut cursor)? as i32;
        let precision = read_u32_le(&mut cursor)? as i32;
        columns.push(ColumnDef {
            name,
            data_type: DataType::from_u8(dtype_byte)?,
            nullable,
            scale,
            precision,
            auto_increment: false,
        });
    }
    let schema = Schema::new(columns);

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
                    MonaxError::Storage("invalid bloom filter length".to_string())
                })?;
                if bf_data_end > buf.len() {
                    return Err(MonaxError::Storage(format!(
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

        row_groups.push(RowGroupMeta {
            offset,
            length,
            row_count: rg_row_count,
            column_compressed_sizes: _column_sizes,
            compressed_sizes: comp_sizes,
            uncompressed_sizes,
            zone_maps,
            bloom_filters,
        });
    }

    let deleted_row_count = read_u64_le(&mut cursor).unwrap_or(0);
    let bitmap_len = read_u32_le(&mut cursor).unwrap_or(0) as usize;
    let mut delete_bitmap = vec![0u8; bitmap_len];
    if bitmap_len > 0 {
        cursor.read_exact(&mut delete_bitmap)?;
    }

    Ok(ParsedSegmentFooter {
        version,
        row_count,
        row_groups,
        schema,
        deleted_row_count,
        delete_bitmap,
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
        let _scale = read_u32_le(&mut cursor)?;
        let _precision = read_u32_le(&mut cursor)?;
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
                    MonaxError::Storage("invalid bloom filter length".to_string())
                })?;
                if end > buf.len() {
                    return Err(MonaxError::Storage(format!(
                        "invalid bloom filter length: {} at footer offset {} (footer size {})",
                        bf_len,
                        start,
                        buf.len()
                    )));
                }
                cursor.set_position(end as u64);
            }
        }
    }
    Ok(cursor.position() as usize)
}
