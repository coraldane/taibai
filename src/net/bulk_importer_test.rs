use super::*;
use crate::protocol::types::DataType;
use crate::storage::bulk_payload::parse_direct_bulk_payload;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

#[test]
fn encoded_bulk_payload_uses_row_count_for_each_column() {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int32),
        ColumnDef::new("name", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int32(Some(1)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("alice".into())))
        .unwrap();
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int32(Some(2)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("bob".into())))
        .unwrap();

    let payload = encode_columnar_bulk_insert("users", &batch).unwrap();
    let parsed = parse_direct_bulk_payload(&payload).unwrap();

    assert_eq!(parsed.table_name, "users");
    assert_eq!(parsed.row_count, 2);
    assert_eq!(parsed.columns.len(), 2);
    assert_eq!(parsed.columns[0].data.len(), 8);
    assert_eq!(parsed.columns[1].data.len(), 16);
    assert_eq!(
        parsed.columns[1].variable_ranges.as_deref(),
        Some(&[(4, 5), (13, 3)][..])
    );
}

#[test]
fn encoded_bulk_payload_preserves_null_bitmap_and_dense_value_slots() {
    let schema = Schema::new(vec![
        ColumnDef::new("score", DataType::Int32),
        ColumnDef::new("note", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema);
    for (score, note) in [
        (ScalarValue::Int32(Some(10)), ScalarValue::VarChar(None)),
        (
            ScalarValue::Int32(None),
            ScalarValue::VarChar(Some("ok".into())),
        ),
    ] {
        batch.column_mut(0).unwrap().push(&score).unwrap();
        batch.column_mut(1).unwrap().push(&note).unwrap();
    }

    let payload = encode_columnar_bulk_insert("metrics", &batch).unwrap();
    let parsed = parse_direct_bulk_payload(&payload).unwrap();

    assert_eq!(parsed.columns[0].nulls.as_ref(), 2u64.to_le_bytes().as_slice());
    assert_eq!(parsed.columns[1].nulls.as_ref(), 1u64.to_le_bytes().as_slice());
    assert_eq!(parsed.columns[0].data.len(), 8);
    assert_eq!(
        parsed.columns[1].variable_ranges.as_deref(),
        Some(&[(4, 0), (8, 2)][..])
    );
}
