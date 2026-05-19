use crate::common::error::Result;
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct SortExec {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    done: bool,
}

impl SortExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self { sort_keys, input, schema, done: false }
    }
}

impl ExecutionOp for SortExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut all_batches = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            all_batches.push(batch);
        }
        if all_batches.is_empty() {
            return Ok(None);
        }

        let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
        for batch in &all_batches {
            for row in 0..batch.num_rows() {
                let row_vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                rows.push(row_vals);
            }
        }

        if rows.is_empty() {
            return Ok(Some(RecordBatch::new(self.schema.clone())));
        }

        let sort_key_indices: Vec<(usize, bool)> = self.sort_keys.iter()
            .map(|sk| {
                match &sk.expr {
                    LogicalExpr::Column(idx) => (*idx, sk.desc),
                    _ => (0, sk.desc),
                }
            })
            .collect();

        rows.sort_by(|a, b| {
            for &(idx, desc) in &sort_key_indices {
                let cmp = compare_scalars(&a[idx], &b[idx]);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &rows {
            for (col_idx, val) in row.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }
        Ok(Some(batch))
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
