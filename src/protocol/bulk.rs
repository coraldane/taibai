use std::io::Cursor;

use rayon::prelude::*;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

pub struct BulkInsertRequest {
    pub table_name: String,
    pub batch: RecordBatch,
}

#[inline]
pub fn decode_bulk_insert(payload: &[u8]) -> Result<BulkInsertRequest> {
    if payload.len() < 2 {
        return Err(MonaxError::Protocol("bulk payload too short".into()));
    }

    let mut cursor = Cursor::new(payload);

    let table_name_len = read_u16_le(&mut cursor)? as usize;
    if 2 + table_name_len > payload.len() {
        return Err(MonaxError::Protocol("invalid table_name_len".into()));
    }
    let table_name = fast_string(&payload[2..2 + table_name_len]);
    cursor.set_position((2 + table_name_len) as u64);

    if cursor.position() as usize + 8 > payload.len() {
        return Err(MonaxError::Protocol("payload too short for row/column count".into()));
    }
    let row_count = read_u32_le(&mut cursor)? as usize;
    let column_count = read_u32_le(&mut cursor)? as usize;

    let mut column_defs = Vec::with_capacity(column_count);
    let mut column_data_slices: Vec<(DataType, &[u8], &[u8], usize)> = Vec::with_capacity(column_count);

    let mut pos = cursor.position() as usize;

    for col_idx in 0..column_count {
        if pos + 2 > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: payload too short for name_len", col_idx)));
        }
        let name_len = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;

        if pos + name_len > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: payload too short for name", col_idx)));
        }
        let name = fast_string(&payload[pos..pos + name_len]);
        pos += name_len;

        if pos + 1 > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: payload too short for dtype", col_idx)));
        }
        let dtype_byte = payload[pos];
        pos += 1;
        let data_type = DataType::from_u8(dtype_byte)?;

        if pos + 8 > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: payload too short for scale/precision", col_idx)));
        }
        let scale = i32::from_le_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;
        let precision = i32::from_le_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]);
        pos += 4;

        if pos + 8 > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: payload too short for data_len/nulls_size", col_idx)));
        }
        let elem_count = u32::from_le_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]) as usize;
        pos += 4;
        let nulls_size = u32::from_le_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]) as usize;
        pos += 4;

        column_defs.push(ColumnDef {
            name,
            data_type,
            nullable: true,
            scale,
            precision,
        });

        let data_bytes_len = calculate_data_bytes(data_type, elem_count, &payload[pos..]);

        if pos + data_bytes_len > payload.len() {
            return Err(MonaxError::Protocol(format!(
                "column {}: data_bytes_len {} exceeds payload (pos={}, len={})",
                col_idx, data_bytes_len, pos, payload.len()
            )));
        }
        let data_slice = &payload[pos..pos + data_bytes_len];
        pos += data_bytes_len;

        if pos + nulls_size > payload.len() {
            return Err(MonaxError::Protocol(format!("column {}: nulls_size exceeds payload", col_idx)));
        }
        let nulls_slice = if nulls_size > 0 {
            &payload[pos..pos + nulls_size]
        } else {
            &[]
        };
        pos += nulls_size;

        column_data_slices.push((data_type, data_slice, nulls_slice, row_count));
    }

    let schema = Schema::new(column_defs);
    let columns: Vec<ColumnArray> = column_data_slices
        .into_par_iter()
        .map(|(dtype, data, nulls, row_count)| {
            decode_column_fast(dtype, data, nulls, row_count).unwrap()
        })
        .collect();

    let batch = RecordBatch::try_new(schema, columns)?;

    Ok(BulkInsertRequest { table_name, batch })
}

fn calculate_data_bytes(data_type: DataType, elem_count: usize, data: &[u8]) -> usize {
    match data_type.fixed_size() {
        Some(fixed) => elem_count * fixed,
        None => {
            let mut offset = 0usize;
            for _ in 0..elem_count {
                if offset + 4 > data.len() {
                    break;
                }
                let len = u32::from_le_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]) as usize;
                offset += 4 + len;
            }
            offset
        }
    }
}

#[inline]
fn decode_column_fast(data_type: DataType, data: &[u8], nulls: &[u8], row_count: usize) -> Result<ColumnArray> {
    let bitmap = if nulls.is_empty() {
        NullBitmap::new_valid(row_count)
    } else {
        NullBitmap::from_bytes(nulls, row_count)
    };

    Ok(match data_type {
        DataType::Int64 => {
            let vals = decode_fixed::<i64>(data, row_count);
            ColumnArray::Int64 { data: vals, bitmap }
        }
        DataType::Int32 => {
            let vals = decode_fixed::<i32>(data, row_count);
            ColumnArray::Int32 { data: vals, bitmap }
        }
        DataType::Int16 => {
            let vals = decode_fixed::<i16>(data, row_count);
            ColumnArray::Int16 { data: vals, bitmap }
        }
        DataType::Int8 => {
            let vals = data[0..row_count.min(data.len())].iter().map(|&b| b as i8).collect();
            ColumnArray::Int8 { data: vals, bitmap }
        }
        DataType::UInt64 => {
            let vals = decode_fixed::<u64>(data, row_count);
            ColumnArray::UInt64 { data: vals, bitmap }
        }
        DataType::UInt32 => {
            let vals = decode_fixed::<u32>(data, row_count);
            ColumnArray::UInt32 { data: vals, bitmap }
        }
        DataType::UInt16 => {
            let vals = decode_fixed::<u16>(data, row_count);
            ColumnArray::UInt16 { data: vals, bitmap }
        }
        DataType::UInt8 => {
            let vals = data[0..row_count.min(data.len())].to_vec();
            ColumnArray::UInt8 { data: vals, bitmap }
        }
        DataType::Float64 => {
            let vals = decode_fixed::<f64>(data, row_count);
            ColumnArray::Float64 { data: vals, bitmap }
        }
        DataType::Float32 => {
            let vals = decode_fixed::<f32>(data, row_count);
            ColumnArray::Float32 { data: vals, bitmap }
        }
        DataType::Bool => {
            let vals = data[0..row_count.min(data.len())].to_vec();
            ColumnArray::Bool { data: vals, bitmap }
        }
        DataType::Timestamp => {
            let vals = decode_fixed::<i64>(data, row_count);
            ColumnArray::Timestamp { data: vals, bitmap }
        }
        DataType::Date => {
            let vals = decode_fixed::<i32>(data, row_count);
            ColumnArray::Date { data: vals, bitmap }
        }
        DataType::Time => {
            let vals = decode_fixed::<i64>(data, row_count);
            ColumnArray::Time { data: vals, bitmap }
        }
        DataType::Decimal => {
            let vals = decode_fixed::<i64>(data, row_count);
            ColumnArray::Decimal { data: vals, bitmap }
        }
        DataType::VarChar => {
            let vals = decode_varchar(data, row_count);
            ColumnArray::VarChar { data: vals, bitmap }
        }
        DataType::Blob => {
            let vals = decode_blob(data, row_count);
            ColumnArray::Blob { data: vals, bitmap }
        }
        DataType::Vector => {
            let vals = decode_vector(data, row_count);
            ColumnArray::Vector { data: vals, bitmap }
        }
        DataType::Interval => {
            let vals = decode_fixed::<i64>(data, row_count);
            ColumnArray::Interval { data: vals, bitmap }
        }
    })
}

#[inline]
fn decode_fixed<T: Copy + Default>(data: &[u8], count: usize) -> Vec<T> {
    let size = std::mem::size_of::<T>();
    let available = data.len() / size;
    let actual_count = count.min(available);
    let mut vals = vec![T::default(); actual_count];
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            vals.as_mut_ptr() as *mut u8,
            actual_count * size,
        );
    }
    vals
}

#[inline]
fn decode_varchar(data: &[u8], count: usize) -> Vec<String> {
    let mut vals = Vec::with_capacity(count);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 4 > data.len() {
            vals.push(String::new());
            continue;
        }
        let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + len > data.len() {
            vals.push(String::new());
            continue;
        }
        vals.push(fast_string(&data[pos..pos + len]));
        pos += len;
    }
    vals
}

#[inline]
fn decode_blob(data: &[u8], count: usize) -> Vec<Vec<u8>> {
    let mut vals = Vec::with_capacity(count);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 4 > data.len() {
            vals.push(Vec::new());
            continue;
        }
        let len = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + len > data.len() {
            vals.push(Vec::new());
            continue;
        }
        vals.push(data[pos..pos + len].to_vec());
        pos += len;
    }
    vals
}

#[inline]
fn decode_vector(data: &[u8], count: usize) -> Vec<Vec<f32>> {
    let mut vals = Vec::with_capacity(count);
    let mut pos = 0;
    for _ in 0..count {
        if pos + 4 > data.len() {
            vals.push(Vec::new());
            continue;
        }
        let dim = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            if pos + 4 > data.len() {
                break;
            }
            v.push(f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]));
            pos += 4;
        }
        vals.push(v);
    }
    vals
}

#[inline]
fn fast_string(bytes: &[u8]) -> String {
    unsafe { String::from_utf8_unchecked(bytes.to_vec()) }
}

#[inline]
fn read_u16_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u16, std::io::Error> {
    let mut buf = [0u8; 2];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

#[inline]
fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u32, std::io::Error> {
    let mut buf = [0u8; 4];
    std::io::Read::read_exact(cursor, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub struct BulkEncoder {
    buffer: Vec<u8>,
}

impl BulkEncoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(2 * 1024 * 1024),
        }
    }

    pub fn encode(&mut self, table_name: &str, batch: &RecordBatch) -> &[u8] {
        self.buffer.clear();

        self.buffer.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
        self.buffer.extend_from_slice(table_name.as_bytes());

        let row_count = batch.num_rows();
        let col_count = batch.num_columns();

        self.buffer.extend_from_slice(&(row_count as u32).to_le_bytes());
        self.buffer.extend_from_slice(&(col_count as u32).to_le_bytes());

        for col_idx in 0..col_count {
            let col = batch.column(col_idx).unwrap();
            let col_def = &batch.schema.columns[col_idx];

            self.buffer.extend_from_slice(&(col_def.name.len() as u16).to_le_bytes());
            self.buffer.extend_from_slice(col_def.name.as_bytes());
            self.buffer.push(col_def.data_type as u8);
            self.buffer.extend_from_slice(&(col_def.scale as u32).to_le_bytes());
            self.buffer.extend_from_slice(&(col_def.precision as u32).to_le_bytes());

            let start = self.buffer.len();
            self.buffer.extend_from_slice(&0u32.to_le_bytes());

            let data_start = self.buffer.len();
            encode_column(&mut self.buffer, col);
            let data_end = self.buffer.len();

            let null_count = col.bitmap().null_count();
            if null_count > 0 {
                let nulls = col.bitmap().to_bytes();
                self.buffer.extend_from_slice(&nulls);
            }

            let data_len = (data_end - data_start) as u32;
            self.buffer[start..start + 4].copy_from_slice(&data_len.to_le_bytes());
            let nulls_size = if null_count > 0 { col.bitmap().to_bytes().len() } else { 0 };
            self.buffer.extend_from_slice(&(nulls_size as u32).to_le_bytes());
        }

        &self.buffer
    }

    pub fn encode_owned(&mut self, table_name: &str, batch: &RecordBatch) -> Vec<u8> {
        self.encode(table_name, batch);
        self.buffer.clone()
    }
}

impl Default for BulkEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn encode_column(buf: &mut Vec<u8>, col: &ColumnArray) {
    match col {
        ColumnArray::Int64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int16 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int8 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt16 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt8 { data, .. } => {
            buf.extend_from_slice(data);
        }
        ColumnArray::Float64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Float32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Bool { data, .. } => {
            buf.extend_from_slice(data);
        }
        ColumnArray::Timestamp { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Date { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Time { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Decimal { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Interval { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::VarChar { data, .. } => {
            for s in data {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
        ColumnArray::Blob { data, .. } => {
            for b in data {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }
        ColumnArray::Vector { data, .. } => {
            for v in data {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    buf.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::scalar::ScalarValue;

    fn build_bulk_payload(table: &str, columns: &[(&str, DataType, u32, u32, usize, &[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(table.len() as u16).to_le_bytes());
        buf.extend_from_slice(table.as_bytes());

        let row_count = if columns.is_empty() { 0u32 } else { columns[0].4 as u32 };
        buf.extend_from_slice(&row_count.to_le_bytes());
        buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());

        for &(name, dtype, scale, precision, data_len, data, nulls) in columns {
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.push(dtype as u8);
            buf.extend_from_slice(&scale.to_le_bytes());
            buf.extend_from_slice(&precision.to_le_bytes());
            buf.extend_from_slice(&(data_len as u32).to_le_bytes());
            buf.extend_from_slice(&(nulls.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
            buf.extend_from_slice(nulls);
        }

        buf
    }

    #[test]
    fn test_decode_simple_int64_column() {
        let mut data = Vec::new();
        for v in [10i64, 20, 30] {
            data.extend_from_slice(&v.to_le_bytes());
        }

        let payload = build_bulk_payload("test_table", &[
            ("id", DataType::Int64, 0, 0, 3, &data, &[]),
        ]);

        let req = decode_bulk_insert(&payload).unwrap();
        assert_eq!(req.table_name, "test_table");
        assert_eq!(req.batch.num_rows(), 3);
        assert_eq!(req.batch.num_columns(), 1);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(10)));
        assert_eq!(req.batch.scalar(1, 0).unwrap(), ScalarValue::Int64(Some(20)));
        assert_eq!(req.batch.scalar(2, 0).unwrap(), ScalarValue::Int64(Some(30)));
    }

    #[test]
    fn test_decode_mixed_columns() {
        let mut id_data = Vec::new();
        id_data.extend_from_slice(&1i64.to_le_bytes());
        id_data.extend_from_slice(&2i64.to_le_bytes());

        let mut name_data = Vec::new();
        name_data.extend_from_slice(&(5u32).to_le_bytes());
        name_data.extend_from_slice(b"Alice");
        name_data.extend_from_slice(&(3u32).to_le_bytes());
        name_data.extend_from_slice(b"Bob");

        let payload = build_bulk_payload("users", &[
            ("id", DataType::Int64, 0, 0, 2, &id_data, &[]),
            ("name", DataType::VarChar, 0, 0, 2, &name_data, &[]),
        ]);

        let req = decode_bulk_insert(&payload).unwrap();
        assert_eq!(req.table_name, "users");
        assert_eq!(req.batch.num_rows(), 2);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(1)));
        assert_eq!(req.batch.scalar(0, 1).unwrap(), ScalarValue::VarChar(Some("Alice".into())));
        assert_eq!(req.batch.scalar(1, 0).unwrap(), ScalarValue::Int64(Some(2)));
        assert_eq!(req.batch.scalar(1, 1).unwrap(), ScalarValue::VarChar(Some("Bob".into())));
    }

    #[test]
    fn test_decode_with_nulls() {
        let mut data = Vec::new();
        data.extend_from_slice(&1i64.to_le_bytes());
        data.extend_from_slice(&0i64.to_le_bytes());
        data.extend_from_slice(&3i64.to_le_bytes());

        let null_bitmap = (1u64 << 1).to_le_bytes();
        let nulls = &null_bitmap[..];

        let payload = build_bulk_payload("t", &[
            ("val", DataType::Int64, 0, 0, 3, &data, nulls),
        ]);

        let req = decode_bulk_insert(&payload).unwrap();
        assert_eq!(req.batch.num_rows(), 3);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(1)));
        assert!(req.batch.scalar(1, 0).unwrap().is_null());
        assert_eq!(req.batch.scalar(2, 0).unwrap(), ScalarValue::Int64(Some(3)));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
            ColumnDef::new("score", DataType::Float64),
        ]);
        let mut batch = RecordBatch::new(schema);
        for i in 0..100 {
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i as i64))).unwrap();
            batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some(format!("user_{}", i)))).unwrap();
            batch.column_mut(2).unwrap().push(&ScalarValue::Float64(Some(i as f64 * 1.5))).unwrap();
        }

        let mut encoder = BulkEncoder::new();
        let encoded = encoder.encode_owned("test_roundtrip", &batch);

        let req = decode_bulk_insert(&encoded).unwrap();
        assert_eq!(req.table_name, "test_roundtrip");
        assert_eq!(req.batch.num_rows(), 100);
        assert_eq!(req.batch.num_columns(), 3);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(0)));
        assert_eq!(req.batch.scalar(50, 1).unwrap(), ScalarValue::VarChar(Some("user_50".into())));
        assert_eq!(req.batch.scalar(99, 2).unwrap(), ScalarValue::Float64(Some(148.5)));
    }
}
