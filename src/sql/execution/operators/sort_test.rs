use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::SortKey;
use crate::types::array::ColumnArray;
use crate::types::batch::ColumnDef;

struct OneBatchExec {
    batch: Option<RecordBatch>,
}

struct ManyBatchExec {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl ExecutionOp for ManyBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

impl ExecutionOp for OneBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

#[test]
fn sorts_by_expression_not_first_column() {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("x", DataType::Float64),
        ColumnDef::new("y", DataType::Float64),
    ]);
    let mut id = ColumnArray::empty(DataType::Int64);
    let mut x = ColumnArray::empty(DataType::Float64);
    let mut y = ColumnArray::empty(DataType::Float64);
    for (i, c, p) in [(3, 11.0, 10.0), (1, 20.0, 10.0), (2, 9.0, 10.0)] {
        id.push(&ScalarValue::Int64(Some(i))).unwrap();
        x.push(&ScalarValue::Float64(Some(c))).unwrap();
        y.push(&ScalarValue::Float64(Some(p))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![id, x, y]).unwrap();
    let expr = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(1)),
            op: BinOp::Sub,
            right: Box::new(LogicalExpr::Column(2)),
        }),
        op: BinOp::Div,
        right: Box::new(LogicalExpr::Column(2)),
    };
    let mut exec = SortExec::new(
        vec![SortKey { expr, desc: true }],
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    )
    .with_limit(2);
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
    assert_eq!(result.scalar(1, 0), Some(ScalarValue::Int64(Some(3))));
}

#[test]
fn external_sort_spills_and_streams_sorted_batches() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut id = ColumnArray::empty(DataType::Int64);
    for value in [9, 1, 7, 3, 5] {
        id.push(&ScalarValue::Int64(Some(value))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![id]).unwrap();
    let mut exec = SortExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    )
    .with_memory_limit_bytes(1);

    let mut values = Vec::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            values.push(batch.scalar(row, 0).unwrap());
        }
    }
    assert_eq!(
        values,
        [1, 3, 5, 7, 9]
            .into_iter()
            .map(|value| ScalarValue::Int64(Some(value)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn external_sort_heap_merges_multiple_runs_descending() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batches = [[2, 8, 4], [9, 1, 7], [6, 3, 5]]
        .into_iter()
        .map(|values| {
            let mut column = ColumnArray::empty(DataType::Int64);
            for value in values {
                column.push(&ScalarValue::Int64(Some(value))).unwrap();
            }
            RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
        })
        .collect::<Vec<_>>();
    let mut exec = SortExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: true,
        }],
        Box::new(ManyBatchExec {
            batches: batches.into_iter(),
        }),
        schema,
    )
    .with_memory_limit_bytes(1);

    let mut values = Vec::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            values.push(batch.scalar(row, 0).unwrap());
        }
    }
    assert_eq!(
        values,
        (1..=9)
            .rev()
            .map(|value| ScalarValue::Int64(Some(value)))
            .collect::<Vec<_>>()
    );
}
