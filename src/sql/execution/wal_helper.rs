use std::path::{Path, PathBuf};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::schema::TableMeta;
use crate::storage::wal::{WalWriter, RecordType};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::Engine;

pub fn write_wal(engine: &Engine, table_dir: &Path, batch: &RecordBatch) -> Result<()> {
    let wal_dir = table_dir.join("wal");
    let key = table_dir.to_string_lossy().to_string();

    let wal_data = serialize_batch_for_wal(batch);

    let txn_id = {
        let mut txn = engine.next_txn_id().lock().unwrap();
        *txn += 1;
        *txn - 1
    };

    let mut wal_writers = engine.wal_writers().lock().unwrap();
    if !wal_writers.contains_key(&key) {
        let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
        wal_writers.insert(key.clone(), writer);
    }
    let wal_writer = wal_writers.get_mut(&key).unwrap();
    wal_writer.write_batch(txn_id, &[(RecordType::Insert, wal_data.as_slice())])?;

    Ok(())
}

pub fn determine_write_dir(
    engine: &Engine,
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
    let mut buf = Vec::new();
    let num_cols = batch.num_columns() as u32;
    let num_rows = batch.num_rows() as u32;

    buf.extend_from_slice(&num_cols.to_le_bytes());
    buf.extend_from_slice(&num_rows.to_le_bytes());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        buf.push(col.data_type() as u8);

        let mut data = Vec::new();
        for i in 0..batch.num_rows() {
            let val = col.scalar(i);
            if val.is_null() {
                data.push(0u8);
            } else {
                data.push(1u8);
                serialize_scalar(&mut data, &val);
            }
        }
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
    }

    buf
}

fn serialize_scalar(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Bool(Some(v)) => buf.push(if *v { 1 } else { 0 }),
        ScalarValue::Int8(Some(v)) => buf.push(*v as u8),
        ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => buf.push(*v),
        ScalarValue::UInt16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Timestamp(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        _ => {}
    }
}

pub fn deserialize_batch_from_wal(data: &[u8], schema: &Schema) -> Result<RecordBatch> {
    if data.len() < 8 {
        return Err(MonaxError::Internal("WAL data too short".into()));
    }

    let num_cols = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let num_rows = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    if num_cols != schema.columns.len() {
        return Err(MonaxError::Internal(
            format!("WAL column count mismatch: {} vs {}", num_cols, schema.columns.len())
        ));
    }

    let mut batch = RecordBatch::new(schema.clone());
    let mut offset = 8;

    for col_idx in 0..num_cols {
        if offset >= data.len() {
            return Err(MonaxError::Internal("WAL data truncated".into()));
        }

        let _dtype_byte = data[offset];
        offset += 1;

        if offset + 4 > data.len() {
            return Err(MonaxError::Internal("WAL data truncated".into()));
        }
        let data_len = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
        ]) as usize;
        offset += 4;

        let col_data = &data[offset..offset + data_len];
        offset += data_len;

        let mut val_offset = 0;
        for _row_idx in 0..num_rows {
            if val_offset >= col_data.len() {
                break;
            }
            let null_flag = col_data[val_offset];
            val_offset += 1;

            if null_flag == 0 {
                batch.column_mut(col_idx).unwrap().push(&ScalarValue::null_for_type(schema.columns[col_idx].data_type))?;
            } else {
                let val = deserialize_scalar(col_data, &mut val_offset, schema.columns[col_idx].data_type)?;
                batch.column_mut(col_idx).unwrap().push(&val)?;
            }
        }
    }

    Ok(batch)
}

fn deserialize_scalar(data: &[u8], offset: &mut usize, dtype: DataType) -> Result<ScalarValue> {
    match dtype {
        DataType::Bool => {
            let v = data[*offset];
            *offset += 1;
            Ok(ScalarValue::Bool(Some(v != 0)))
        }
        DataType::Int8 => {
            let v = data[*offset] as i8;
            *offset += 1;
            Ok(ScalarValue::Int8(Some(v)))
        }
        DataType::Int16 => {
            if *offset + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int16".into()));
            }
            let v = i16::from_le_bytes([data[*offset], data[*offset + 1]]);
            *offset += 2;
            Ok(ScalarValue::Int16(Some(v)))
        }
        DataType::Int32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int32".into()));
            }
            let v = i32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
            *offset += 4;
            Ok(ScalarValue::Int32(Some(v)))
        }
        DataType::Int64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int64".into()));
            }
            let v = i64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Int64(Some(v)))
        }
        DataType::UInt8 => {
            let v = data[*offset];
            *offset += 1;
            Ok(ScalarValue::UInt8(Some(v)))
        }
        DataType::UInt16 => {
            if *offset + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt16".into()));
            }
            let v = u16::from_le_bytes([data[*offset], data[*offset + 1]]);
            *offset += 2;
            Ok(ScalarValue::UInt16(Some(v)))
        }
        DataType::UInt32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt32".into()));
            }
            let v = u32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
            *offset += 4;
            Ok(ScalarValue::UInt32(Some(v)))
        }
        DataType::UInt64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt64".into()));
            }
            let v = u64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::UInt64(Some(v)))
        }
        DataType::Float32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float32".into()));
            }
            let v = f32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
            *offset += 4;
            Ok(ScalarValue::Float32(Some(v)))
        }
        DataType::Float64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float64".into()));
            }
            let v = f64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Float64(Some(v)))
        }
        DataType::VarChar => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar".into()));
            }
            let len = u32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]) as usize;
            *offset += 4;
            if *offset + len > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar payload".into()));
            }
            let s = std::str::from_utf8(&data[*offset..*offset + len])
                .map_err(|_| MonaxError::Internal("invalid UTF-8 in WAL".into()))?;
            *offset += len;
            Ok(ScalarValue::VarChar(Some(s.to_string())))
        }
        DataType::Timestamp => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Timestamp".into()));
            }
            let v = i64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Timestamp(Some(v)))
        }
        DataType::Date => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Date".into()));
            }
            let v = i32::from_le_bytes([data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]]);
            *offset += 4;
            Ok(ScalarValue::Date(Some(v)))
        }
        _ => Ok(ScalarValue::null_for_type(dtype)),
    }
}
