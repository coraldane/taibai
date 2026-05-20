use std::path::{Path, PathBuf};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::schema::TableMeta;
use crate::storage::wal::WalWriter;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::Engine;

const WAL_BATCH_MAGIC: u32 = 0x4D4F4E58;
const FLAG_COMPRESSED: u8 = 0x01;

pub fn write_wal(engine: &Engine, table_dir: &Path, batch: &RecordBatch) -> Result<()> {
    if engine.is_bulk_mode() {
        return Ok(());
    }

    let wal_dir = table_dir.join("wal");
    let key = table_dir.to_string_lossy().to_string();

    let wal_data = serialize_batch_for_wal(batch);

    let txn_id = engine.alloc_txn_id();

    let mut wal_writers = engine.wal_writers().lock().unwrap();
    if !wal_writers.contains_key(&key) {
        let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
        wal_writers.insert(key.clone(), writer);
    }
    let wal_writer = wal_writers.get_mut(&key).unwrap();

    wal_writer.write_compact(txn_id, &wal_data, false)?;

    Ok(())
}

pub fn determine_write_dir(
    _engine: &Engine,
    table_meta: &TableMeta,
    batch: &RecordBatch,
    default_dir: &Path,
) -> Result<(PathBuf, String)> {
    if table_meta.partition_key.is_empty() {
        return Ok((default_dir.to_path_buf(), String::new()));
    }

    let part_col_idx = batch.schema.columns.iter()
        .position(|c| c.name == table_meta.partition_key);

    let Some(col_idx) = part_col_idx else {
        return Ok((default_dir.to_path_buf(), String::new()));
    };

    let strategy = match table_meta.partition_type {
        crate::storage::schema::PartitionType::Function => {
            crate::storage::partition::PartitionStrategy::Day
        }
        crate::storage::schema::PartitionType::Value => crate::storage::partition::PartitionStrategy::Day,
        crate::storage::schema::PartitionType::None => return Ok((default_dir.to_path_buf(), String::new())),
    };

    let first_val = batch.scalar(0, col_idx)
        .unwrap_or(ScalarValue::Int64(None));

    let partition_key = crate::storage::partition::compute_partition_key(&first_val, strategy)
        .unwrap_or_else(|_| "default".to_string());

    Ok((default_dir.to_path_buf(), partition_key))
}

pub fn serialize_batch_for_wal(batch: &RecordBatch) -> Vec<u8> {
    let num_cols = batch.num_columns();
    let num_rows = batch.num_rows();

    let mut raw = Vec::with_capacity(64 + num_cols * 8 + num_rows * num_cols * 8);
    raw.extend_from_slice(&WAL_BATCH_MAGIC.to_le_bytes());
    raw.push(0u8);
    raw.extend_from_slice(&(num_cols as u32).to_le_bytes());
    raw.extend_from_slice(&(num_rows as u32).to_le_bytes());

    for col_idx in 0..num_cols {
        let col = batch.column(col_idx).unwrap();
        let dt = col.data_type();
        raw.push(dt as u8);

        let bitmap = col.bitmap();
        let has_nulls = !bitmap.is_all_valid();
        if has_nulls {
            let bm_bytes = bitmap.to_bytes();
            raw.extend_from_slice(&(bm_bytes.len() as u32).to_le_bytes());
            raw.extend_from_slice(&bm_bytes);
        } else {
            raw.extend_from_slice(&0u32.to_le_bytes());
        }

        let col_data = encode_column_raw(col);
        raw.extend_from_slice(&(col_data.len() as u32).to_le_bytes());
        raw.extend_from_slice(&col_data);
    }

    match zstd::bulk::compress(&raw, 3) {
        Ok(compressed) if compressed.len() < raw.len() => {
            let mut out = Vec::with_capacity(13 + compressed.len());
            out.extend_from_slice(&WAL_BATCH_MAGIC.to_le_bytes());
            out.push(FLAG_COMPRESSED);
            out.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
            out.extend_from_slice(&compressed);
            out
        }
        _ => raw,
    }
}

fn encode_column_raw(col: &crate::types::array::ColumnArray) -> Vec<u8> {
    use crate::types::array::ColumnArray;
    match col {
        ColumnArray::Bool { data, .. } => data.clone(),
        ColumnArray::Int8 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len())
            };
            bytes.to_vec()
        }
        ColumnArray::Int16 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2)
            };
            bytes.to_vec()
        }
        ColumnArray::Int32 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
            };
            bytes.to_vec()
        }
        ColumnArray::Int64 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::UInt8 { data, .. } => data.clone(),
        ColumnArray::UInt16 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2)
            };
            bytes.to_vec()
        }
        ColumnArray::UInt32 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
            };
            bytes.to_vec()
        }
        ColumnArray::UInt64 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::Float32 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
            };
            bytes.to_vec()
        }
        ColumnArray::Float64 { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::Decimal { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::Date { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
            };
            bytes.to_vec()
        }
        ColumnArray::Time { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::Timestamp { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::Interval { data, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
            };
            bytes.to_vec()
        }
        ColumnArray::VarChar { data, .. } => {
            let mut buf = Vec::with_capacity(data.iter().map(|s| s.len() + 4).sum::<usize>());
            for s in data {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            buf
        }
        ColumnArray::Blob { data, .. } => {
            let mut buf = Vec::with_capacity(data.iter().map(|b| b.len() + 4).sum::<usize>());
            for b in data {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            buf
        }
        ColumnArray::Vector { data, .. } => {
            let mut buf = Vec::new();
            for v in data {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
            buf
        }
    }
}

pub fn deserialize_batch_from_wal(data: &[u8], schema: &Schema) -> Result<RecordBatch> {
    if data.len() < 13 {
        return Err(MonaxError::Internal("WAL data too short".into()));
    }

    let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if magic != WAL_BATCH_MAGIC {
        return Err(MonaxError::Internal("WAL data: invalid magic".into()));
    }

    let flags = data[4];

    let payload = if flags & FLAG_COMPRESSED != 0 {
        if data.len() < 13 {
            return Err(MonaxError::Internal("WAL compressed data too short".into()));
        }
        let uncompressed_len = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;
        let compressed_len = u32::from_le_bytes([data[9], data[10], data[11], data[12]]) as usize;
        if data.len() < 13 + compressed_len {
            return Err(MonaxError::Internal("WAL compressed data truncated".into()));
        }
        zstd::bulk::decompress(&data[13..13 + compressed_len], uncompressed_len)
            .map_err(|e| MonaxError::Internal(format!("WAL decompression failed: {}", e)))?
    } else {
        data.to_vec()
    };

    if payload.len() < 13 {
        return Err(MonaxError::Internal("WAL payload too short".into()));
    }

    let p_magic = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    if p_magic != WAL_BATCH_MAGIC {
        return Err(MonaxError::Internal("WAL payload: invalid magic".into()));
    }

    let num_cols = u32::from_le_bytes([payload[5], payload[6], payload[7], payload[8]]) as usize;
    let num_rows = u32::from_le_bytes([payload[9], payload[10], payload[11], payload[12]]) as usize;

    if num_cols != schema.columns.len() {
        return Err(MonaxError::Internal(
            format!("WAL column count mismatch: {} vs {}", num_cols, schema.columns.len())
        ));
    }

    let mut batch = RecordBatch::new(schema.clone());
    let mut offset = 13;

    for col_idx in 0..num_cols {
        if offset >= payload.len() {
            return Err(MonaxError::Internal("WAL data truncated at column header".into()));
        }

        let _dtype_byte = payload[offset];
        offset += 1;

        if offset + 4 > payload.len() {
            return Err(MonaxError::Internal("WAL data truncated at bitmap len".into()));
        }
        let bitmap_len = u32::from_le_bytes([
            payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]
        ]) as usize;
        offset += 4;

        let null_bitmap = if bitmap_len > 0 {
            if offset + bitmap_len > payload.len() {
                return Err(MonaxError::Internal("WAL data truncated at bitmap data".into()));
            }
            Some(crate::types::bitmap::NullBitmap::from_bytes(&payload[offset..offset + bitmap_len], num_rows))
        } else {
            None
        };
        offset += bitmap_len;

        if offset + 4 > payload.len() {
            return Err(MonaxError::Internal("WAL data truncated at data len".into()));
        }
        let data_len = u32::from_le_bytes([
            payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3]
        ]) as usize;
        offset += 4;

        if offset + data_len > payload.len() {
            return Err(MonaxError::Internal("WAL data truncated at column data".into()));
        }
        let col_data = &payload[offset..offset + data_len];
        offset += data_len;

        let dt = schema.columns[col_idx].data_type;
        decode_column_raw(&mut batch, col_idx, dt, col_data, num_rows, null_bitmap.as_ref())?;
    }

    Ok(batch)
}

fn decode_column_raw(
    batch: &mut RecordBatch,
    col_idx: usize,
    dt: DataType,
    data: &[u8],
    num_rows: usize,
    null_bitmap: Option<&crate::types::bitmap::NullBitmap>,
) -> Result<()> {
    let col = batch.column_mut(col_idx).unwrap();
    let mut pos = 0;

    for row in 0..num_rows {
        let is_null = null_bitmap.map_or(false, |bm| bm.is_null(row));
        if is_null {
            col.push(&ScalarValue::null_for_type(dt))?;
        } else {
            let val = decode_fixed_value(data, &mut pos, dt)?;
            col.push(&val)?;
        }
    }

    Ok(())
}

fn decode_fixed_value(data: &[u8], pos: &mut usize, dt: DataType) -> Result<ScalarValue> {
    match dt {
        DataType::Bool => {
            if *pos >= data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Bool".into()));
            }
            let v = data[*pos] != 0;
            *pos += 1;
            Ok(ScalarValue::Bool(Some(v)))
        }
        DataType::Int8 => {
            if *pos >= data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int8".into()));
            }
            let v = data[*pos] as i8;
            *pos += 1;
            Ok(ScalarValue::Int8(Some(v)))
        }
        DataType::Int16 => {
            if *pos + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int16".into()));
            }
            let v = i16::from_le_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
            Ok(ScalarValue::Int16(Some(v)))
        }
        DataType::Int32 => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int32".into()));
            }
            let v = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(ScalarValue::Int32(Some(v)))
        }
        DataType::Int64 => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int64".into()));
            }
            let v = i64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Int64(Some(v)))
        }
        DataType::UInt8 => {
            if *pos >= data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt8".into()));
            }
            let v = data[*pos];
            *pos += 1;
            Ok(ScalarValue::UInt8(Some(v)))
        }
        DataType::UInt16 => {
            if *pos + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt16".into()));
            }
            let v = u16::from_le_bytes([data[*pos], data[*pos + 1]]);
            *pos += 2;
            Ok(ScalarValue::UInt16(Some(v)))
        }
        DataType::UInt32 => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt32".into()));
            }
            let v = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(ScalarValue::UInt32(Some(v)))
        }
        DataType::UInt64 => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt64".into()));
            }
            let v = u64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::UInt64(Some(v)))
        }
        DataType::Float32 => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float32".into()));
            }
            let v = f32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(ScalarValue::Float32(Some(v)))
        }
        DataType::Float64 => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float64".into()));
            }
            let v = f64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Float64(Some(v)))
        }
        DataType::Decimal => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Decimal".into()));
            }
            let v = i64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Decimal(Some(v)))
        }
        DataType::Date => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Date".into()));
            }
            let v = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            Ok(ScalarValue::Date(Some(v)))
        }
        DataType::Time => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Time".into()));
            }
            let v = i64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Time(Some(v)))
        }
        DataType::Timestamp => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Timestamp".into()));
            }
            let v = i64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Timestamp(Some(v)))
        }
        DataType::Interval => {
            if *pos + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Interval".into()));
            }
            let v = i64::from_le_bytes([
                data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3],
                data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7],
            ]);
            *pos += 8;
            Ok(ScalarValue::Interval(Some(v)))
        }
        DataType::VarChar => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar".into()));
            }
            let len = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            if *pos + len > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar payload".into()));
            }
            let s = std::str::from_utf8(&data[*pos..*pos + len])
                .map_err(|_| MonaxError::Internal("invalid UTF-8 in WAL".into()))?;
            *pos += len;
            Ok(ScalarValue::VarChar(Some(s.to_string())))
        }
        DataType::Blob => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Blob".into()));
            }
            let len = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            if *pos + len > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Blob payload".into()));
            }
            let v = data[*pos..*pos + len].to_vec();
            *pos += len;
            Ok(ScalarValue::Blob(Some(v)))
        }
        DataType::Vector => {
            if *pos + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Vector".into()));
            }
            let len = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]) as usize;
            *pos += 4;
            let total_bytes = len * 4;
            if *pos + total_bytes > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Vector payload".into()));
            }
            let mut floats = Vec::with_capacity(len);
            for i in 0..len {
                let start = *pos + i * 4;
                floats.push(f32::from_le_bytes([
                    data[start], data[start + 1], data[start + 2], data[start + 3],
                ]));
            }
            *pos += total_bytes;
            Ok(ScalarValue::Vector(Some(floats)))
        }
    }
}
