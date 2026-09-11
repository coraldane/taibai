use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, Schema};

fn make_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("score", DataType::Float64),
    ])
}

fn make_sorted_batch() -> RecordBatch {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    let data: Vec<(i64, &str, f64)> = vec![
        (1, "alice", 85.0),
        (3, "charlie", 72.5),
        (5, "eve", 91.0),
        (7, "grace", 88.0),
        (9, "irene", 79.5),
    ];
    for (id, name, score) in data {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name.to_string())))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(score)))
            .unwrap();
    }
    batch
}

#[test]
fn test_is_sorted_true() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    assert!(inserter.is_sorted(&batch));
}

#[test]
fn test_is_sorted_false() {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(5)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("a".into())))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Float64(Some(1.0)))
        .unwrap();
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(3)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("b".into())))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Float64(Some(2.0)))
        .unwrap();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    assert!(!inserter.is_sorted(&batch));
}

#[test]
fn test_descending_sort() {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    for id in [9i64, 7, 5, 3, 1] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!("u{}", id))))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(id as f64)))
            .unwrap();
    }
    let inserter = SortKeyInserter::new(vec![0], vec![true]);
    assert!(inserter.is_sorted(&batch));
    assert!(!SortKeyInserter::new(vec![0], vec![false]).is_sorted(&batch));
}

#[test]
fn test_compare_scalars_for_sort() {
    assert_eq!(
        compare_scalars_for_sort(&ScalarValue::Int64(Some(1)), &ScalarValue::Int64(Some(2))),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_scalars_for_sort(&ScalarValue::Int64(None), &ScalarValue::Int64(Some(2))),
        std::cmp::Ordering::Greater
    );
    assert!(scalar_lt(
        &ScalarValue::Int64(Some(1)),
        &ScalarValue::Int64(Some(2))
    ));
    assert!(!scalar_lt(
        &ScalarValue::Int64(None),
        &ScalarValue::Int64(Some(2))
    ));
}
