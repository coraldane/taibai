use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct LogicalExpression {
    pub left: Box<dyn ColumnExpression>,
    pub op: BinOp,
    pub right: Box<dyn ColumnExpression>,
}

pub struct NotExpression {
    pub child: Box<dyn ColumnExpression>,
}

impl ColumnExpression for LogicalExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let left_col = self.left.evaluate(batch)?;
        let right_col = self.right.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let mut result = ColumnArray::empty(DataType::Bool);
        for i in 0..num_rows {
            let l = left_col.scalar(i);
            let r = right_col.scalar(i);
            let val = match self.op {
                BinOp::And => and_bools(&l, &r),
                BinOp::Or => or_bools(&l, &r),
                _ => ScalarValue::Bool(None),
            };
            result.push(&val)?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType { DataType::Bool }
}

impl ColumnExpression for NotExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let child_col = self.child.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let mut result = ColumnArray::empty(DataType::Bool);
        for i in 0..num_rows {
            let v = child_col.scalar(i);
            let val = match v {
                ScalarValue::Bool(Some(b)) => ScalarValue::Bool(Some(!b)),
                _ => ScalarValue::Bool(None),
            };
            result.push(&val)?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType { DataType::Bool }
}

fn and_bools(l: &ScalarValue, r: &ScalarValue) -> ScalarValue {
    match (l, r) {
        (ScalarValue::Bool(Some(a)), ScalarValue::Bool(Some(b))) => ScalarValue::Bool(Some(*a && *b)),
        _ => ScalarValue::Bool(None),
    }
}

fn or_bools(l: &ScalarValue, r: &ScalarValue) -> ScalarValue {
    match (l, r) {
        (ScalarValue::Bool(Some(a)), ScalarValue::Bool(Some(b))) => ScalarValue::Bool(Some(*a || *b)),
        _ => ScalarValue::Bool(None),
    }
}
