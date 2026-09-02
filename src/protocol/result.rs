use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};

/// Binary query result encoder for MsgTypeBinaryQueryResult (0x24).
/// Uses a zero-copy friendly format:
///
/// Wire format:
/// ```text
/// [u32 num_cols]
/// per column:
///   [u32 name_len] [name bytes] [u8 dtype] [u8 scale] [u8 precision]
/// [u32 num_rows]
/// per column:
///   [u32 null_bitmap_len] [null_bitmap bytes]
///   [u32 data_len] [data bytes]
/// ```
pub fn encode_binary_result(batch: &RecordBatch) -> Vec<u8> {
    encode_binary_result_batches(&batch.schema, std::slice::from_ref(batch))
}

pub fn encode_binary_result_batches(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    let num_cols = schema.num_columns();
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Number of columns
    buf.extend_from_slice(&(num_cols as u32).to_le_bytes());

    // Column headers
    for col_def in &schema.columns {
        let name_bytes = col_def.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(col_def.data_type as u8);
        buf.push(col_def.scale as u8);
        buf.push(col_def.precision as u8);
    }

    // Number of rows
    buf.extend_from_slice(&(num_rows as u32).to_le_bytes());

    // Column data
    for col_idx in 0..num_cols {
        let mut null_bitmap = Vec::with_capacity(num_rows);
        for batch in batches {
            match batch.column(col_idx) {
                Some(col) => {
                    null_bitmap.extend_from_slice(&col.bitmap().null_flag_bytes());
                }
                None => {
                    null_bitmap.extend(std::iter::repeat_n(0u8, batch.num_rows()));
                }
            }
        }

        buf.extend_from_slice(&(null_bitmap.len() as u32).to_le_bytes());
        buf.extend_from_slice(&null_bitmap);

        // Data payload
        let data = encode_column_data_batches(col_idx, batches);
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
    }

    buf
}

/// Encode column data as fixed-width little-endian values.
fn encode_column_data_batches(col_idx: usize, batches: &[RecordBatch]) -> Vec<u8> {
    let mut buf = Vec::new();
    for batch in batches {
        let Some(col) = batch.column(col_idx) else {
            continue;
        };
        append_column_data(col, &mut buf);
    }
    buf
}

fn append_column_data(col: &ColumnArray, buf: &mut Vec<u8>) {
    macro_rules! append_fixed {
        ($data:expr) => {{
            if $data.is_empty() {
                return;
            }
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    $data.as_ptr() as *const u8,
                    $data.len() * std::mem::size_of_val(&$data[0]),
                )
            };
            buf.extend_from_slice(bytes);
        }};
    }

    match col {
        ColumnArray::Bool { data, .. } => buf.extend_from_slice(data),
        ColumnArray::Int8 { data, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len()) };
            buf.extend_from_slice(bytes);
        }
        ColumnArray::Int16 { data, .. } => append_fixed!(data),
        ColumnArray::Int32 { data, .. } => append_fixed!(data),
        ColumnArray::Int64 { data, .. } => append_fixed!(data),
        ColumnArray::UInt8 { data, .. } => buf.extend_from_slice(data),
        ColumnArray::UInt16 { data, .. } => append_fixed!(data),
        ColumnArray::UInt32 { data, .. } => append_fixed!(data),
        ColumnArray::UInt64 { data, .. } => append_fixed!(data),
        ColumnArray::Float32 { data, .. } => append_fixed!(data),
        ColumnArray::Float64 { data, .. } => append_fixed!(data),
        ColumnArray::Decimal { data, .. } => append_fixed!(data),
        ColumnArray::Date { data, .. } => append_fixed!(data),
        ColumnArray::Time { data, .. } => append_fixed!(data),
        ColumnArray::Timestamp { data, .. } => append_fixed!(data),
        ColumnArray::Interval { data, .. } => append_fixed!(data),
        ColumnArray::VarChar { data, .. } => {
            for v in data {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
        }
        ColumnArray::Blob { data, .. } => {
            for v in data {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
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
#[path = "result_test.rs"]
mod tests;
