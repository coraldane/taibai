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
    let num_cols = schema.num_columns();
    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // Rough size estimate so the payload builds with at most one realloc
    // instead of doubling through a multi-MB result.
    let mut estimated = 8 + num_rows * (num_cols + 4 * num_cols + 8 * num_cols);
    for col_def in &schema.columns {
        estimated += 4 + col_def.name.len() + 3;
    }
    let mut buf = Vec::with_capacity(estimated);

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

    // Column data. Sections are appended in place and their u32 length is
    // patched afterwards, so column bytes are copied once instead of being
    // materialized in a temporary Vec and copied again.
    for col_idx in 0..num_cols {
        let bitmap_len_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);
        let bitmap_start = buf.len();
        for batch in batches {
            match batch.column(col_idx) {
                Some(col) => {
                    buf.extend_from_slice(&col.bitmap().null_flag_bytes());
                }
                None => {
                    buf.extend(std::iter::repeat_n(0u8, batch.num_rows()));
                }
            }
        }
        let bitmap_len = (buf.len() - bitmap_start) as u32;
        buf[bitmap_len_pos..bitmap_len_pos + 4].copy_from_slice(&bitmap_len.to_le_bytes());

        let data_len_pos = buf.len();
        buf.extend_from_slice(&[0u8; 4]);
        let data_start = buf.len();
        for batch in batches {
            if let Some(col) = batch.column(col_idx) {
                append_column_data(col, &mut buf);
            }
        }
        let data_len = (buf.len() - data_start) as u32;
        buf[data_len_pos..data_len_pos + 4].copy_from_slice(&data_len.to_le_bytes());
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
        ColumnArray::Selected { .. } => append_column_data(col.flattened(), buf),
        ColumnArray::Dictionary { .. } => append_column_data(&col.materialize_varchar(), buf),
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
