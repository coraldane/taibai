use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::common::error::Result;
use crate::types::array::ColumnArray;

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
