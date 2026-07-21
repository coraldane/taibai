use super::*;
use crate::protocol::types::DataType;

fn make_test_batch() -> RecordBatch {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("age", DataType::Int32),
    ]);
    let mut batch = RecordBatch::new(schema);
    for i in 0..10i64 {
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
            .push(&ScalarValue::Int32(Some((20 + i as i32) * 3)))
            .unwrap();
    }
    batch
}

#[test]
fn test_push_filter() {
    let batch = make_test_batch();
    let schema = batch.schema.clone();
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Gt,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(5)))),
    };
    let mut op = PushFilter::new(predicate, schema);
    op.push(batch).unwrap();
    let result = Box::new(op).finalize().unwrap().unwrap();
    assert_eq!(result.num_rows(), 4); // id 6,7,8,9
}

#[test]
fn test_push_project() {
    let batch = make_test_batch();
    let mut op = PushProject::new(
        vec![LogicalExpr::Column(0), LogicalExpr::Column(1)],
        vec![Some("id".into()), Some("name".into())],
    );
    op.push(batch).unwrap();
    let result = Box::new(op).finalize().unwrap().unwrap();
    assert_eq!(result.num_columns(), 2);
    assert_eq!(result.num_rows(), 10);
}

#[test]
fn test_push_limit() {
    let batch = make_test_batch();
    let mut op = PushLimit::new(3, 2);
    op.push(batch).unwrap();
    let result = Box::new(op).finalize().unwrap().unwrap();
    assert_eq!(result.num_rows(), 3);
    // Should skip id=0,1 and return id=2,3,4
    assert_eq!(result.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(2)));
}

#[test]
fn test_push_aggregate() {
    let batch = make_test_batch();
    let input_schema = batch.schema.clone();
    let agg = AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    };
    let mut op = PushHashAggregate::new(vec![], vec![agg], input_schema);
    op.push(batch).unwrap();
    let result = Box::new(op).finalize().unwrap().unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(10)));
}

#[test]
fn test_mono_pipeline_filter_project() {
    let batch = make_test_batch();
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Lt,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(3)))),
    };

    // Forward-flow: Filter receives data, processes, pushes to Project, then Sink
    let mut pipeline = MonoPipeline::Filter {
        predicate,
        child: Box::new(MonoPipeline::Project {
            expressions: vec![LogicalExpr::Column(0), LogicalExpr::Column(1)],
            aliases: vec![Some("id".into()), Some("name".into())],
            child: Box::new(MonoPipeline::Sink { buffer: Vec::new() }),
        }),
    };

    pipeline.push(batch).unwrap();
    let result = pipeline.finalize().unwrap().unwrap();
    assert_eq!(result.num_rows(), 3); // id 0,1,2
    assert_eq!(result.num_columns(), 2);
}

#[test]
fn test_merge_batches() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut b1 = RecordBatch::new(schema.clone());
    b1.column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(1)))
        .unwrap();
    b1.column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(2)))
        .unwrap();

    let mut b2 = RecordBatch::new(schema);
    b2.column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(3)))
        .unwrap();

    let merged = merge_batches(&[b1, b2]);
    assert_eq!(merged.num_rows(), 3);
    assert_eq!(merged.scalar(2, 0).unwrap(), ScalarValue::Int64(Some(3)));
}
