use std::io::Cursor;

use rayon::prelude::*;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

pub struct FastBulkRequest {
    pub table_name: String,
    pub batch: RecordBatch,
}

#[inline]
pub fn decode_bulk_fast(payload: &[u8]) -> Result<FastBulkRequest> {
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

    Ok(FastBulkRequest { table_name, batch })
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

pub struct BulkEncoderV2 {
    buffer: Vec<u8>,
}

impl BulkEncoderV2 {
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
            encode_column_v2(&mut self.buffer, col);
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

impl Default for BulkEncoderV2 {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn encode_column_v2(buf: &mut Vec<u8>, col: &ColumnArray) {
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