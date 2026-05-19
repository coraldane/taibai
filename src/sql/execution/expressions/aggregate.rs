use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::planner::AggFunc;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct AggregateExpression {
    pub func: AggFunc,
    pub child: Box<dyn ColumnExpression>,
    pub distinct: bool,
}

impl AggregateExpression {
    pub fn new(func: AggFunc, child: Box<dyn ColumnExpression>, distinct: bool) -> Self {
        Self { func, child, distinct }
    }
}

impl ColumnExpression for AggregateExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let col = self.child.evaluate(batch)?;
        let num_rows = batch.num_rows();
        let result_type = match self.func {
            AggFunc::Count => DataType::Int64,
            AggFunc::Sum | AggFunc::Avg => DataType::Float64,
            _ => self.child.data_type(),
        };
        let mut result = ColumnArray::empty(result_type);
        let val = match self.func {
            AggFunc::Count => {
                let non_null = (0..num_rows)
                    .filter(|&i| !col.scalar(i).is_null())
                    .count();
                ScalarValue::Int64(Some(non_null as i64))
            }
            AggFunc::Sum => {
                let sum: f64 = (0..num_rows)
                    .filter_map(|i| scalar_to_f64(&col.scalar(i)))
                    .sum();
                ScalarValue::Float64(Some(sum))
            }
            AggFunc::Avg => {
                let vals: Vec<f64> = (0..num_rows)
                    .filter_map(|i| scalar_to_f64(&col.scalar(i)))
                    .collect();
                if vals.is_empty() { ScalarValue::Float64(None) }
                else { ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64)) }
            }
            AggFunc::Min => find_min(&col, num_rows),
            AggFunc::Max => find_max(&col, num_rows),
        };
        result.push(&val)?;
        Ok(result)
    }

    fn data_type(&self) -> DataType {
        match self.func {
            AggFunc::Count => DataType::Int64,
            AggFunc::Sum | AggFunc::Avg => DataType::Float64,
            _ => self.child.data_type(),
        }
    }
}

fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

fn find_min(col: &ColumnArray, num_rows: usize) -> ScalarValue {
    (0..num_rows)
        .map(|i| col.scalar(i))
        .filter(|v| !v.is_null())
        .min_by(|a, b| compare_scalars(a, b))
        .unwrap_or(ScalarValue::Int64(None))
}

fn find_max(col: &ColumnArray, num_rows: usize) -> ScalarValue {
    (0..num_rows)
        .map(|i| col.scalar(i))
        .filter(|v| !v.is_null())
        .max_by(|a, b| compare_scalars(a, b))
        .unwrap_or(ScalarValue::Int64(None))
}

fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}
