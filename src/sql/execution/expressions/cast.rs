use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct CastExpression {
    pub child: Box<dyn ColumnExpression>,
    pub target_type: DataType,
}

impl CastExpression {
    pub fn new(child: Box<dyn ColumnExpression>, target_type: DataType) -> Self {
        Self { child, target_type }
    }
}

impl ColumnExpression for CastExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let col = self.child.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let mut result = ColumnArray::empty(self.target_type);
        for i in 0..num_rows {
            let v = col.scalar(i);
            let casted = cast_scalar(&v, &self.target_type);
            result.push(&casted)?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType {
        self.target_type
    }
}

fn cast_scalar(v: &ScalarValue, target: &DataType) -> ScalarValue {
    if v.is_null() {
        return ScalarValue::null_for_type(*target);
    }
    match (v, target) {
        (ScalarValue::Int64(Some(n)), DataType::Float64) => ScalarValue::Float64(Some(*n as f64)),
        (ScalarValue::Int32(Some(n)), DataType::Float64) => ScalarValue::Float64(Some(*n as f64)),
        (ScalarValue::Int32(Some(n)), DataType::Int64) => ScalarValue::Int64(Some(*n as i64)),
        (ScalarValue::Float64(Some(n)), DataType::Int64) => ScalarValue::Int64(Some(*n as i64)),
        (ScalarValue::VarChar(Some(s)), DataType::Int64) => {
            match s.parse::<i64>() {
                Ok(n) => ScalarValue::Int64(Some(n)),
                Err(_) => ScalarValue::Int64(None),
            }
        }
        (ScalarValue::VarChar(Some(s)), DataType::Float64) => {
            match s.parse::<f64>() {
                Ok(n) => ScalarValue::Float64(Some(n)),
                Err(_) => ScalarValue::Float64(None),
            }
        }
        _ => v.clone(),
    }
}
