use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

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
