use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::functions::date::parse_timestamp_smart;
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
            let casted = cast_scalar(&v, &self.target_type, 0);
            result.push(&casted)?;
        }
        Ok(result)
    }

    fn data_type(&self) -> DataType {
        self.target_type
    }
}

pub fn cast_scalar_with_timezone(
    v: &ScalarValue,
    target: &DataType,
    timezone_offset_hours: i32,
) -> ScalarValue {
    if v.is_null() {
        return ScalarValue::null_for_type(*target);
    }
    match (v, target) {
        (ScalarValue::Int64(Some(n)), DataType::Float64) => ScalarValue::Float64(Some(*n as f64)),
        (ScalarValue::Int32(Some(n)), DataType::Float64) => ScalarValue::Float64(Some(*n as f64)),
        (ScalarValue::Int32(Some(n)), DataType::Int64) => ScalarValue::Int64(Some(*n as i64)),
        (ScalarValue::Float64(Some(n)), DataType::Int64) => ScalarValue::Int64(Some(*n as i64)),
        (ScalarValue::VarChar(Some(s)), DataType::Int64) => match s.parse::<i64>() {
            Ok(n) => ScalarValue::Int64(Some(n)),
            Err(_) => ScalarValue::Int64(None),
        },
        (ScalarValue::VarChar(Some(s)), DataType::Float64) => match s.parse::<f64>() {
            Ok(n) => ScalarValue::Float64(Some(n)),
            Err(_) => ScalarValue::Float64(None),
        },
        (ScalarValue::VarChar(Some(s)), DataType::Timestamp) => {
            match parse_timestamp_smart(s, timezone_offset_hours) {
                Some(ts) => ScalarValue::Timestamp(Some(ts)),
                None => ScalarValue::Timestamp(None),
            }
        }
        (ScalarValue::Int64(Some(n)), DataType::Timestamp) => {
            use crate::sql::execution::functions::date::parse_timestamp_from_int;
            ScalarValue::Timestamp(Some(parse_timestamp_from_int(*n)))
        }
        _ => v.clone(),
    }
}

fn cast_scalar(v: &ScalarValue, target: &DataType, timezone_offset_hours: i32) -> ScalarValue {
    cast_scalar_with_timezone(v, target, timezone_offset_hours)
}
