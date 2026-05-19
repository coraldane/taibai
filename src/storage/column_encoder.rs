use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use rayon::prelude::*;

use crate::common::error::{MonaxError, Result};
use crate::types::array::ColumnArray;

pub const DEFAULT_ROW_GROUP_SIZE: usize = 50000;

lazy_static::lazy_static! {
    static ref BUFFER_POOL: Arc<ArrayQueue<Vec<u8>>> = Arc::new(ArrayQueue::new(256));
}

pub fn get_buffer(min_capacity: usize) -> Vec<u8> {
    if let Some(mut buf) = BUFFER_POOL.pop() {
        if buf.capacity() >= min_capacity {
            buf.clear();
            return buf;
        }
    }
    Vec::with_capacity(min_capacity.max(64 * 1024))
}

pub fn return_buffer(buf: Vec<u8>) {
    if buf.capacity() <= 4 * 1024 * 1024 {
        let _ = BUFFER_POOL.push(buf);
    }
}

#[derive(Clone)]
pub struct EncodedColumn {
    pub data: Vec<u8>,
    pub uncompressed_size: usize,
    pub compressed_size: usize,
}

pub struct ColumnEncoder {
    pub row_group_size: usize,
}

impl ColumnEncoder {
    pub fn new(row_group_size: usize) -> Self {
        Self { row_group_size }
    }

    pub fn encode_batch_parallel(&self, batch: &crate::types::batch::RecordBatch) -> Result<Vec<EncodedColumn>> {
        let num_cols = batch.num_columns();
        
        (0..num_cols).into_par_iter()
            .map(|col_idx| {
                batch.column(col_idx)
                    .ok_or_else(|| MonaxError::Storage(format!("Column {} not found", col_idx)))
                    .and_then(|col| encode_column_fast(col))
            })
            .collect()
    }

    pub fn compress_parallel(columns: Vec<EncodedColumn>, compression_level: i32) -> Result<Vec<EncodedColumn>> {
        let results: Vec<_> = columns.into_par_iter()
            .map(|mut col| {
                let compressed = zstd::bulk::compress(&col.data, compression_level as i32)
                    .unwrap_or_else(|_| col.data.clone());
                col.compressed_size = compressed.len();
                let _ = std::mem::replace(&mut col.data, compressed);
                col
            })
            .collect();
        Ok(results)
    }
}

#[inline]
pub fn encode_column_fast(col: &ColumnArray) -> Result<EncodedColumn> {
    let row_count = col.len();
    let mut buf = get_buffer(estimate_column_size(col));

    let null_count = col.bitmap().null_count();
    let has_nulls = null_count > 0;

    buf.extend_from_slice(&(row_count as u32).to_le_bytes());
    buf.extend_from_slice(&(null_count as u32).to_le_bytes());

    if has_nulls {
        let bitmap_bytes = col.bitmap().to_bytes();
        buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bitmap_bytes);
    } else {
        buf.extend_from_slice(&0u32.to_le_bytes());
    }

    match col {
        ColumnArray::Bool { data, .. } => {
            buf.extend_from_slice(data);
        }
        ColumnArray::Int8 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int16 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Int64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt8 { data, .. } => {
            buf.extend_from_slice(data);
        }
        ColumnArray::UInt16 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 2) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::UInt64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Float32 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Float64 { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Decimal { data, .. } => {
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
        ColumnArray::Timestamp { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::Interval { data, .. } => {
            let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
            buf.extend_from_slice(slice);
        }
        ColumnArray::VarChar { data, .. } => {
            let total_len: usize = data.iter().map(|s| s.len()).sum();
            buf.reserve(total_len + data.len() * 4);
            for s in data {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
        ColumnArray::Blob { data, .. } => {
            let total_len: usize = data.iter().map(|b| b.len()).sum();
            buf.reserve(total_len + data.len() * 4);
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

    let uncompressed_size = buf.len();
    Ok(EncodedColumn {
        data: buf,
        uncompressed_size,
        compressed_size: 0,
    })
}

#[inline]
fn estimate_column_size(col: &ColumnArray) -> usize {
    let row_count = col.len();
    match col {
        ColumnArray::Bool { .. } | ColumnArray::Int8 { .. } | ColumnArray::UInt8 { .. } => {
            row_count + 8 + (row_count / 8 + 8)
        }
        ColumnArray::Int16 { .. } | ColumnArray::UInt16 { .. } => {
            row_count * 2 + 8 + (row_count / 8 + 8)
        }
        ColumnArray::Int32 { .. } | ColumnArray::UInt32 { .. } | ColumnArray::Float32 { .. } | ColumnArray::Date { .. } => {
            row_count * 4 + 8 + (row_count / 8 + 8)
        }
        ColumnArray::Int64 { .. } | ColumnArray::UInt64 { .. } | ColumnArray::Float64 { .. }
        | ColumnArray::Decimal { .. } | ColumnArray::Time { .. } | ColumnArray::Timestamp { .. }
        | ColumnArray::Interval { .. } => {
            row_count * 8 + 8 + (row_count / 8 + 8)
        }
        ColumnArray::VarChar { data, .. } => {
            let str_len: usize = data.iter().map(|s| s.len()).sum();
            str_len + row_count * 4 + 8 + (row_count / 8 + 8)
        }
        ColumnArray::Blob { data, .. } => {
            let blob_len: usize = data.iter().map(|b| b.len()).sum();
            blob_len + row_count * 4 + 8 + (row_count / 8 + 8)
        }
        ColumnArray::Vector { data, .. } => {
            let vec_len: usize = data.iter().map(|v| v.len() * 4).sum();
            vec_len + row_count * 4 + 8 + (row_count / 8 + 8)
        }
    }
}

pub struct BulkEncoder {
    buffer: Vec<u8>,
}

impl BulkEncoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024 * 1024),
        }
    }

    pub fn encode_batch(&mut self, table_name: &str, batch: &crate::types::batch::RecordBatch) -> Result<Vec<u8>> {
        self.buffer.clear();

        self.buffer.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
        self.buffer.extend_from_slice(table_name.as_bytes());

        let row_count = batch.num_rows();
        let col_count = batch.num_columns();

        self.buffer.extend_from_slice(&(row_count as u32).to_le_bytes());
        self.buffer.extend_from_slice(&(col_count as u32).to_le_bytes());

        for col_idx in 0..col_count {
            let col = batch.column(col_idx).ok_or_else(|| MonaxError::Storage(format!("Column {} not found", col_idx)))?;
            let col_def = batch.schema.columns[col_idx].clone();

            self.buffer.extend_from_slice(&(col_def.name.len() as u16).to_le_bytes());
            self.buffer.extend_from_slice(col_def.name.as_bytes());
            self.buffer.push(col_def.data_type as u8);
            self.buffer.extend_from_slice(&(col_def.scale as u32).to_le_bytes());
            self.buffer.extend_from_slice(&(col_def.precision as u32).to_le_bytes());

            let encoded = encode_column_fast(col)?;
            self.buffer.extend_from_slice(&(encoded.data.len() as u32).to_le_bytes());
            self.buffer.extend_from_slice(&encoded.data);
        }

        Ok(self.buffer.clone())
    }

    pub fn encode_batch_zero_copy(&mut self, table_name: &str, batch: &crate::types::batch::RecordBatch) -> Result<()> {
        self.buffer.clear();

        self.buffer.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
        self.buffer.extend_from_slice(table_name.as_bytes());

        let row_count = batch.num_rows();
        let col_count = batch.num_columns();

        self.buffer.extend_from_slice(&(row_count as u32).to_le_bytes());
        self.buffer.extend_from_slice(&(col_count as u32).to_le_bytes());

        for col_idx in 0..col_count {
            let col = batch.column(col_idx).ok_or_else(|| MonaxError::Storage(format!("Column {} not found", col_idx)))?;
            let col_def = &batch.schema.columns[col_idx];

            self.buffer.extend_from_slice(&(col_def.name.len() as u16).to_le_bytes());
            self.buffer.extend_from_slice(col_def.name.as_bytes());
            self.buffer.push(col_def.data_type as u8);
            self.buffer.extend_from_slice(&(col_def.scale as u32).to_le_bytes());
            self.buffer.extend_from_slice(&(col_def.precision as u32).to_le_bytes());

            let encoded = encode_column_fast(col)?;
            self.buffer.extend_from_slice(&(encoded.data.len() as u32).to_le_bytes());
            self.buffer.extend_from_slice(&encoded.data);
        }

        Ok(())
    }

    pub fn take_buffer(self) -> Vec<u8> {
        self.buffer
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buffer
    }
}

impl Default for BulkEncoder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BatchBuilder {
    columns: Vec<ColumnBuilder>,
    schema: crate::types::batch::Schema,
}

struct ColumnBuilder {
    data: ColumnArray,
}

impl BatchBuilder {
    pub fn new(schema: crate::types::batch::Schema) -> Self {
        let columns = schema.columns.iter()
            .map(|def| ColumnBuilder {
                data: ColumnArray::empty(def.data_type),
            })
            .collect();
        Self { columns, schema }
    }

    pub fn append_row(&mut self, values: &[crate::types::scalar::ScalarValue]) -> Result<()> {
        for (i, val) in values.iter().enumerate() {
            self.columns[i].data.push(val)?;
        }
        Ok(())
    }

    pub fn build(self) -> Result<crate::types::batch::RecordBatch> {
        let columns: Vec<ColumnArray> = self.columns.into_iter().map(|c| c.data).collect();
        crate::types::batch::RecordBatch::try_new(self.schema, columns)
    }

    pub fn len(&self) -> usize {
        self.columns.first().map(|c| c.data.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}