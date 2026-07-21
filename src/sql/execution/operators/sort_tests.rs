use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::SortKey;
use crate::types::array::ColumnArray;
use crate::types::batch::ColumnDef;

struct OneBatchExec {
    batch: Option<RecordBatch>,
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
