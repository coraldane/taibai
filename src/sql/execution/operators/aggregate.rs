use std::collections::HashMap;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::sql::execution::operators::filter::eval_expr_column;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct HashAggregateExec {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
    done: bool,
}

impl HashAggregateExec {
    pub fn new(group_by: Vec<LogicalExpr>, aggregates: Vec<AggregateExpr>, input: Box<dyn ExecutionOp>, input_schema: Schema) -> Self {
        Self { group_by, aggregates, input, input_schema, done: false }
    }
}

impl ExecutionOp for HashAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut all_rows: Vec<Vec<ScalarValue>> = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            for row in 0..batch.num_rows() {
                let vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                all_rows.push(vals);
            }
        }

        let mut groups: HashMap<Vec<String>, Vec<Vec<ScalarValue>>> = HashMap::new();
        for row in &all_rows {
            let key: Vec<String> = self.group_by.iter()
                .map(|expr| {
                    match expr {
                        LogicalExpr::Column(idx) => row.get(*idx).map(|v| v.to_string()).unwrap_or_default(),
                        _ => String::new(),
                    }
                })
                .collect();
            groups.entry(key).or_default().push(row.clone());
        }

        let mut col_defs = Vec::new();
        for expr in &self.group_by {
            if let LogicalExpr::Column(idx) = expr {
                if let Some(cd) = self.input_schema.columns.get(*idx) {
                    col_defs.push(ColumnDef::new(&cd.name, cd.data_type));
                }
            }
        }
        for agg in &self.aggregates {
            let name = format!("{:?}({})", agg.func, match &*agg.arg {
                LogicalExpr::Wildcard => "*".into(),
                LogicalExpr::Column(idx) => self.input_schema.columns.get(*idx).map(|c| c.name.clone()).unwrap_or_default(),
                _ => "expr".into(),
            });
            let dt = match agg.func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Sum | AggFunc::Avg => DataType::Float64,
                AggFunc::Min | AggFunc::Max => {
                    match &*agg.arg {
                        LogicalExpr::Column(idx) => self.input_schema.columns.get(*idx).map(|c| c.data_type).unwrap_or(DataType::Int64),
                        _ => DataType::Int64,
                    }
                }
            };
            col_defs.push(ColumnDef::new(&name, dt));
        }
        let schema = Schema::new(col_defs);

        let mut batch = RecordBatch::new(schema);
        for (_key, rows) in &groups {
            let mut row_vals = Vec::new();
            for expr in &self.group_by {
                if let LogicalExpr::Column(idx) = expr {
                    row_vals.push(rows.first().and_then(|r| r.get(*idx)).cloned().unwrap_or(ScalarValue::Int64(None)));
                }
            }
            for agg in &self.aggregates {
                row_vals.push(compute_aggregate(agg, rows, &self.input_schema));
            }
            for (col_idx, val) in row_vals.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }

        Ok(Some(batch))
    }
}

pub fn compute_aggregate(agg: &AggregateExpr, rows: &[Vec<ScalarValue>], _schema: &Schema) -> ScalarValue {
    let col_idx = match &*agg.arg {
        LogicalExpr::Column(idx) => Some(*idx),
        LogicalExpr::Wildcard => None,
        _ => None,
    };

    match agg.func {
        AggFunc::Count => {
            if let Some(idx) = col_idx {
                let non_null = rows.iter().filter(|r| !r.get(idx).map(|v| v.is_null()).unwrap_or(true)).count();
                ScalarValue::Int64(Some(non_null as i64))
            } else {
                ScalarValue::Int64(Some(rows.len() as i64))
            }
        }
        AggFunc::Sum => {
            if let Some(idx) = col_idx {
                let sum: f64 = rows.iter()
                    .filter_map(|r| r.get(idx).and_then(|v| scalar_to_f64(v)))
                    .sum();
                ScalarValue::Float64(Some(sum))
            } else {
                ScalarValue::Float64(None)
            }
        }
        AggFunc::Avg => {
            if let Some(idx) = col_idx {
                let vals: Vec<f64> = rows.iter()
                    .filter_map(|r| r.get(idx).and_then(|v| scalar_to_f64(v)))
                    .collect();
                if vals.is_empty() { ScalarValue::Float64(None) }
                else { ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64)) }
            } else {
                ScalarValue::Float64(None)
            }
        }
        AggFunc::Min => {
            if let Some(idx) = col_idx {
                rows.iter()
                    .filter_map(|r| r.get(idx).cloned())
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| compare_scalars(a, b))
                    .unwrap_or(ScalarValue::Int64(None))
            } else { ScalarValue::Int64(None) }
        }
        AggFunc::Max => {
            if let Some(idx) = col_idx {
                rows.iter()
                    .filter_map(|r| r.get(idx).cloned())
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| compare_scalars(a, b))
                    .unwrap_or(ScalarValue::Int64(None))
            } else { ScalarValue::Int64(None) }
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

fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() { return std::cmp::Ordering::Equal; }
    if a.is_null() { return std::cmp::Ordering::Less; }
    if b.is_null() { return std::cmp::Ordering::Greater; }
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}
