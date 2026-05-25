use crate::protocol::types::DataType;
use crate::types::batch::RecordBatch;

/// Binary query result encoder for MsgTypeBinaryQueryResult (0x24).
/// Uses a zero-copy friendly format:
///
/// Wire format:
/// ```text
/// [u32 num_cols]
/// per column:
///   [u32 name_len] [name bytes] [u8 dtype] [u32 scale]
/// [u32 num_rows]
/// per column:
///   [u32 null_bitmap_len] [null_bitmap bytes]
///   [u32 data_len] [data bytes]
/// ```
pub fn encode_binary_result(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let num_cols = batch.num_columns();
    let num_rows = batch.num_rows();

    // Number of columns
    buf.extend_from_slice(&(num_cols as u32).to_le_bytes());

    // Column headers
    for col_def in &batch.schema.columns {
        let name_bytes = col_def.name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.push(dtype_to_u8(col_def.data_type));
        buf.extend_from_slice(&(col_def.scale as u32).to_le_bytes());
    }

    // Number of rows
    buf.extend_from_slice(&(num_rows as u32).to_le_bytes());

    // Column data
    for col_idx in 0..num_cols {
        let col = batch.column(col_idx).unwrap();

        // Null bitmap (1 byte per row for simplicity; bit=1 means NULL)
        let null_bitmap: Vec<u8> = (0..num_rows)
            .map(|i| {
                let val = col.scalar(i);
                if val.is_null() {
                    1u8
                } else {
                    0u8
                }
            })
            .collect();

        buf.extend_from_slice(&(null_bitmap.len() as u32).to_le_bytes());
        buf.extend_from_slice(&null_bitmap);

        // Data payload
        let data = encode_column_data(col_idx, batch);
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
    }

    buf
}

/// Encode column data as fixed-width little-endian values.
fn encode_column_data(col_idx: usize, batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let num_rows = batch.num_rows();
    let col = batch.column(col_idx).unwrap();
    let dt = col.data_type();

    for i in 0..num_rows {
        let val = col.scalar(i);
        if val.is_null() {
            // Write zero placeholder
            match dt {
                DataType::Int64 | DataType::Timestamp => buf.extend_from_slice(&0i64.to_le_bytes()),
                DataType::Int32 | DataType::Date => buf.extend_from_slice(&0i32.to_le_bytes()),
                DataType::Int16 => buf.extend_from_slice(&0i16.to_le_bytes()),
                DataType::Int8 => buf.push(0),
                DataType::Float64 => buf.extend_from_slice(&0.0f64.to_le_bytes()),
                DataType::Float32 => buf.extend_from_slice(&0.0f32.to_le_bytes()),
                DataType::Bool => buf.push(0),
                DataType::VarChar => buf.extend_from_slice(&0u32.to_le_bytes()),
                _ => buf.extend_from_slice(&0u32.to_le_bytes()),
            }
            continue;
        }
        match val {
            ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Int8(Some(v)) => buf.push(v as u8),
            ScalarValue::UInt64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::UInt32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Bool(Some(v)) => buf.push(if v { 1 } else { 0 }),
            ScalarValue::VarChar(Some(v)) => {
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v.as_bytes());
            }
            ScalarValue::Timestamp(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            ScalarValue::Date(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            _ => {}
        }
    }
    buf
}

fn dtype_to_u8(dt: DataType) -> u8 {
    match dt {
        DataType::Int8 => 1,
        DataType::Int16 => 2,
        DataType::Int32 => 3,
        DataType::Int64 => 4,
        DataType::UInt32 => 5,
        DataType::UInt64 => 6,
        DataType::Float32 => 7,
        DataType::Float64 => 8,
        DataType::Bool => 9,
        DataType::VarChar => 10,
        DataType::Date => 11,
        DataType::Timestamp => 12,
        _ => 0,
    }
}

use crate::types::scalar::ScalarValue;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::batch::{ColumnDef, Schema};

    #[test]
    fn test_encode_empty_result() {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let batch = RecordBatch::new(schema);
        let encoded = encode_binary_result(&batch);
        assert!(!encoded.is_empty());

        // First 4 bytes = num_cols = 1
        let num_cols = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
        assert_eq!(num_cols, 1);
    }

    #[test]
    fn test_encode_with_data() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let mut batch = RecordBatch::new(schema);
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(42)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("hello".into())))
            .unwrap();

        let encoded = encode_binary_result(&batch);
        assert!(encoded.len() > 10);

        // Verify num_cols = 2
        let num_cols = u32::from_le_bytes(encoded[0..4].try_into().unwrap());
        assert_eq!(num_cols, 2);
    }

    #[test]
    fn test_encode_multiple_rows() {
        let schema = Schema::new(vec![ColumnDef::new("val", DataType::Float64)]);
        let mut batch = RecordBatch::new(schema);
        for i in 0..5i64 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Float64(Some(i as f64)))
                .unwrap();
        }

        let encoded = encode_binary_result(&batch);
        // Should contain 5 * 8 bytes of float data + overhead
        assert!(encoded.len() > 40);
    }
}
