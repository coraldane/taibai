use super::*;
use crate::protocol::types::DataType;

fn make_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("age", DataType::Int32),
    ]);
    let mut batch = RecordBatch::new(schema);
    for i in 0..5i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!("user_{}", i))))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int32(Some((20 + i as i32) * 2)))
            .unwrap();
    }
    batch
}

#[test]
fn test_scan_exec() {
    let batch = make_test_batch();
    let mut exec = ScanExec::new(vec![batch], None);
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_rows(), 5);
    assert!(exec.next_batch().unwrap().is_none());
}

#[test]
fn test_filter_exec() {
    let batch = make_test_batch();
    let schema = batch.schema.clone();
    // Filter: age > 44 (ages are 40, 42, 44, 46, 48)
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(2)),
        op: BinOp::Gt,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int32(Some(44)))),
    };
    let mut exec = FilterExec {
        predicate,
        input: Box::new(ScanExec::new(vec![batch], None)),
        schema,
    };
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_rows(), 2); // ages 46 and 48
}

#[test]
fn test_sort_exec() {
    let batch = make_test_batch();
    let schema = batch.schema.clone();
    let sort_keys = vec![SortKey {
        expr: LogicalExpr::Column(0),
        desc: true,
    }];
    let mut exec = SortExec::new(
        sort_keys,
        Box::new(ScanExec::new(vec![batch], None)),
        schema,
    );
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_rows(), 5);
    // First row should be id=4 (sorted desc)
    assert_eq!(result.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(4)));
    assert_eq!(result.scalar(4, 0).unwrap(), ScalarValue::Int64(Some(0)));
}

#[test]
fn test_limit_exec() {
    let batch = make_test_batch();
    let schema = batch.schema.clone();
    let mut exec = LimitExec::new(2, 1, Box::new(ScanExec::new(vec![batch], None)), schema);
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_rows(), 2);
    // Should skip id=0, return id=1 and id=2
    assert_eq!(result.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(1)));
    assert_eq!(result.scalar(1, 0).unwrap(), ScalarValue::Int64(Some(2)));
}

#[test]
fn test_hash_aggregate_count() {
    let batch = make_test_batch();
    let input_schema = batch.schema.clone();
    let agg = AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    };
    let mut exec = HashAggregateExec::new(
        vec![],
        vec![agg],
        Box::new(ScanExec::new(vec![batch], None)),
        input_schema,
    );
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(5)));
}

#[test]
fn test_project_exec() {
    let batch = make_test_batch();
    let input_schema = batch.schema.clone();
    let mut exec = ProjectExec {
        expressions: vec![LogicalExpr::Column(0), LogicalExpr::Column(1)],
        aliases: vec![Some("id".into()), Some("name".into())],
        input: Box::new(ScanExec::new(vec![batch], None)),
        input_schema,
    };
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.num_columns(), 2);
    assert_eq!(result.num_rows(), 5);
}
