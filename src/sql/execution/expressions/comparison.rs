use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct ComparisonExpression {
    pub left: Box<dyn ColumnExpression>,
    pub op: BinOp,
    pub right: Box<dyn ColumnExpression>,
}

impl ComparisonExpression {
    pub fn new(
        left: Box<dyn ColumnExpression>,
        op: BinOp,
        right: Box<dyn ColumnExpression>,
    ) -> Self {
        Self { left, op, right }
    }
}

impl ColumnExpression for ComparisonExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let left_col = self.left.evaluate(batch)?;
        let right_col = self.right.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let mut result = ColumnArray::empty(DataType::Bool);
        for i in 0..num_rows {
            let l = left_col.scalar(i);
            let r = right_col.scalar(i);
            let cmp = compare(&l, &self.op, &r);
            result.push(&ScalarValue::Bool(Some(cmp)))?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType {
        DataType::Bool
    }
}

fn compare(l: &ScalarValue, op: &BinOp, r: &ScalarValue) -> bool {
    if l.is_null() || r.is_null() {
        return false;
    }
    match (l, r) {
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => num_cmp(*a, op, *b),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => num_cmp(*a, op, *b),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => num_cmp(*a, op, *b),
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => str_cmp(a, op, b),
        _ => false,
    }
}

fn num_cmp<T: PartialOrd>(a: T, op: &BinOp, b: T) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Neq => a != b,
        BinOp::Lt => a < b,
        BinOp::Gt => a > b,
        BinOp::Lte => a <= b,
        BinOp::Gte => a >= b,
        _ => false,
    }
}

fn str_cmp(a: &str, op: &BinOp, b: &str) -> bool {
    match op {
        BinOp::Eq => a == b,
        BinOp::Neq => a != b,
        BinOp::Lt => a < b,
        BinOp::Gt => a > b,
        BinOp::Lte => a <= b,
        BinOp::Gte => a >= b,
        _ => false,
    }
}
