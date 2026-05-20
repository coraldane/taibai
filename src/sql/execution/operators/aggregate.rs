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
    /// Optional HAVING filter predicate (applied after aggregation)
    pub having: Option<LogicalExpr>,
    /// Whether to apply DISTINCT on the output
    pub distinct: bool,
    done: bool,
}

impl HashAggregateExec {
    pub fn new(group_by: Vec<LogicalExpr>, aggregates: Vec<AggregateExpr>, input: Box<dyn ExecutionOp>, input_schema: Schema) -> Self {
        Self { group_by, aggregates, input, input_schema, having: None, distinct: false, done: false }
    }

    pub fn with_having(mut self, having: LogicalExpr) -> Self {
        self.having = Some(having);
        self
    }

    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }
}

impl ExecutionOp for HashAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Collect all input rows
        let mut all_rows: Vec<Vec<ScalarValue>> = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            for row in 0..batch.num_rows() {
                let vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                all_rows.push(vals);
            }
        }

        // Group by key
        let mut groups: HashMap<Vec<String>, Vec<Vec<ScalarValue>>> = HashMap::new();
        let mut group_order: Vec<Vec<String>> = Vec::new();
        for row in &all_rows {
            let key: Vec<String> = self.group_by.iter()
                .map(|expr| eval_group_key(expr, row))
                .collect();
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
            }
            groups.entry(key).or_default().push(row.clone());
        }

        // Build output schema
        let mut col_defs = Vec::new();
        for expr in &self.group_by {
            if let LogicalExpr::Column(idx) = expr {
                if let Some(cd) = self.input_schema.columns.get(*idx) {
                    col_defs.push(ColumnDef::new(&cd.name, cd.data_type));
                }
            }
        }
        for agg in &self.aggregates {
            let name = format_aggregate_name(agg, &self.input_schema);
            let dt = aggregate_result_type(agg, &self.input_schema);
            col_defs.push(ColumnDef::new(&name, dt));
        }
        let schema = Schema::new(col_defs.clone());

        // Compute aggregations
        let mut result_batch = RecordBatch::new(schema);
        for key in &group_order {
            let rows = groups.get(key).unwrap();
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
                result_batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }

        // Apply DISTINCT if needed
        if self.distinct {
            result_batch = apply_distinct(result_batch);
        }

        // Apply HAVING filter if present
        if let Some(ref having_pred) = self.having {
            result_batch = apply_having(result_batch, having_pred)?;
        }

        Ok(Some(result_batch))
    }
}

fn eval_group_key(expr: &LogicalExpr, row: &[ScalarValue]) -> String {
    match expr {
        LogicalExpr::Column(idx) => row.get(*idx).map(|v| v.to_string()).unwrap_or_default(),
        _ => String::new(),
    }
}

pub fn format_aggregate_name(agg: &AggregateExpr, schema: &Schema) -> String {
    let arg_name = match &*agg.arg {
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::Column(idx) => schema.columns.get(*idx).map(|c| c.name.clone()).unwrap_or_default(),
        _ => "expr".to_string(),
    };
    let distinct_str = if agg.distinct { "DISTINCT " } else { "" };
    format!("{:?}({}{})", agg.func, distinct_str, arg_name)
}

fn aggregate_result_type(agg: &AggregateExpr, schema: &Schema) -> DataType {
    match agg.func {
        AggFunc::Count => DataType::Int64,
        AggFunc::Sum | AggFunc::Avg => DataType::Float64,
        AggFunc::Min | AggFunc::Max => {
            match &*agg.arg {
                LogicalExpr::Column(idx) => schema.columns.get(*idx).map(|c| c.data_type).unwrap_or(DataType::Int64),
                _ => DataType::Int64,
            }
        }
    }
}

pub fn compute_aggregate(agg: &AggregateExpr, rows: &[Vec<ScalarValue>], _schema: &Schema) -> ScalarValue {
    let col_idx = match &*agg.arg {
        LogicalExpr::Column(idx) => Some(*idx),
        LogicalExpr::Wildcard => None,
        _ => None,
    };

    // Handle DISTINCT
    let values: Vec<&ScalarValue> = if agg.distinct {
        if let Some(idx) = col_idx {
            let mut seen: Vec<String> = Vec::new();
            rows.iter()
                .filter_map(|r| r.get(idx))
                .filter(|v| {
                    let s = v.to_string();
                    if seen.contains(&s) { false } else { seen.push(s); true }
                })
                .collect()
        } else {
            rows.iter().map(|r| r.first().unwrap_or(&ScalarValue::Int64(None))).collect()
        }
    } else {
        if let Some(idx) = col_idx {
            rows.iter().filter_map(|r| r.get(idx)).collect()
        } else {
            rows.iter().map(|r| r.first().unwrap_or(&ScalarValue::Int64(None))).collect()
        }
    };

    match agg.func {
        AggFunc::Count => {
            if agg.distinct {
                ScalarValue::Int64(Some(values.len() as i64))
            } else if col_idx.is_some() {
                let non_null = values.iter().filter(|v| !v.is_null()).count();
                ScalarValue::Int64(Some(non_null as i64))
            } else {
                ScalarValue::Int64(Some(rows.len() as i64))
            }
        }
        AggFunc::Sum => {
            let sum: f64 = values.iter()
                .filter_map(|v| scalar_to_f64(v))
                .sum();
            if sum == 0.0 && values.iter().all(|v| v.is_null()) {
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(sum))
            }
        }
        AggFunc::Avg => {
            let vals: Vec<f64> = values.iter()
                .filter_map(|v| scalar_to_f64(v))
                .collect();
            if vals.is_empty() { ScalarValue::Float64(None) }
            else { ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64)) }
        }
        AggFunc::Min => {
            match values.iter()
                .filter(|v| !v.is_null())
                .min_by(|a, b| compare_scalars(a, b)) {
                Some(v) => (*v).clone(),
                None => ScalarValue::Int64(None),
            }
        }
        AggFunc::Max => {
            match values.iter()
                .filter(|v| !v.is_null())
                .max_by(|a, b| compare_scalars(a, b)) {
                Some(v) => (*v).clone(),
                None => ScalarValue::Int64(None),
            }
        }
    }
}

pub fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Decimal(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() { return std::cmp::Ordering::Equal; }
    if a.is_null() { return std::cmp::Ordering::Less; }
    if b.is_null() { return std::cmp::Ordering::Greater; }
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Apply DISTINCT to a RecordBatch, removing duplicate rows.
fn apply_distinct(batch: RecordBatch) -> RecordBatch {
    let mut seen: Vec<String> = Vec::new();
    let mut mask = Vec::new();

    for row in 0..batch.num_rows() {
        let key: String = (0..batch.num_columns())
            .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)).to_string())
            .collect::<Vec<_>>()
            .join("|");
        if !seen.contains(&key) {
            seen.push(key);
            mask.push(true);
        } else {
            mask.push(false);
        }
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&col.scalar(i)).unwrap();
            }
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

/// Apply HAVING filter to aggregated results.
fn apply_having(batch: RecordBatch, _predicate: &LogicalExpr) -> Result<RecordBatch> {
    // HAVING is evaluated using the same filter logic as WHERE
    // but operates on the aggregated result schema
    let mask = crate::sql::execution::operators::filter::eval_filter(_predicate, &batch)?;
    Ok(super::apply_filter_mask(&batch, &mask))
}
