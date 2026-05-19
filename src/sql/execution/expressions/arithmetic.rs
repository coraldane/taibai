use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct ArithmeticExpression {
    pub left: Box<dyn ColumnExpression>,
    pub op: BinOp,
    pub right: Box<dyn ColumnExpression>,
    pub result_type: DataType,
}

impl ArithmeticExpression {
    pub fn new(left: Box<dyn ColumnExpression>, op: BinOp, right: Box<dyn ColumnExpression>, result_type: DataType) -> Self {
        Self { left, op, right, result_type }
    }
}

impl ColumnExpression for ArithmeticExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let left_col = self.left.evaluate(batch)?;
        let right_col = self.right.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let mut result = ColumnArray::empty(self.result_type);
        for i in 0..num_rows {
            let l = left_col.scalar(i);
            let r = right_col.scalar(i);
            let val = compute(&l, &self.op, &r, &self.result_type);
            result.push(&val)?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType {
        self.result_type
    }
}

fn compute(l: &ScalarValue, op: &BinOp, r: &ScalarValue, dt: &DataType) -> ScalarValue {
    if l.is_null() || r.is_null() {
        return ScalarValue::null_for_type(*dt);
    }
    match (l, r) {
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => {
            let v = match op {
                BinOp::Add => *a + *b, BinOp::Sub => *a - *b,
                BinOp::Mul => *a * *b, BinOp::Div => *a / *b,
                _ => return ScalarValue::null_for_type(*dt),
            };
            ScalarValue::Int64(Some(v))
        }
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
            let v = match op {
                BinOp::Add => *a + *b, BinOp::Sub => *a - *b,
                BinOp::Mul => *a * *b, BinOp::Div => *a / *b,
                _ => return ScalarValue::null_for_type(*dt),
            };
            ScalarValue::Float64(Some(v))
        }
        _ => ScalarValue::null_for_type(*dt),
    }
}
