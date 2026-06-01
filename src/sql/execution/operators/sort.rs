use crate::common::error::Result;
use crate::sql::execution::operators::filter::remap_expr_for_batch;
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct SortExec {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    /// Optional limit for TopN optimization.
    pub limit: Option<usize>,
    done: bool,
}

impl SortExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self {
            sort_keys,
            input,
            schema,
            limit: None,
            done: false,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
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
        let output_schema = all_batches[0].schema.clone();

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(Some(RecordBatch::new(output_schema)));
        }

        // Materialize rows for sorting
        let mut rows: Vec<Vec<ScalarValue>> = Vec::with_capacity(total_rows);
        for batch in &all_batches {
            for row in 0..batch.num_rows() {
                let row_vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                rows.push(row_vals);
            }
        }

        let sort_key_indices: Vec<(usize, bool)> = self
            .sort_keys
            .iter()
            .map(|sk| {
                let expr = remap_expr_for_batch(&sk.expr, &self.schema, &output_schema);
                match &expr {
                    LogicalExpr::Column(idx) => (*idx, sk.desc),
                    _ => (0, sk.desc),
                }
            })
            .collect();

        if let Some(limit) = self.limit {
            // TopN optimization: use partial sort instead of full sort
            topn_sort(&mut rows, &sort_key_indices, limit);
            rows.truncate(limit);
        } else {
            rows.sort_by(|a, b| compare_rows(a, b, &sort_key_indices));
        }

        let mut batch = RecordBatch::new(output_schema);
        for row in &rows {
            for (col_idx, val) in row.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }
        Ok(Some(batch))
    }
}

/// TopN sort using partial_sort approach.
/// Only keeps the top N rows, avoiding full sort of large datasets.
fn topn_sort(rows: &mut [Vec<ScalarValue>], sort_key_indices: &[(usize, bool)], limit: usize) {
    if rows.len() <= limit {
        rows.sort_by(|a, b| compare_rows(a, b, sort_key_indices));
        return;
    }

    // Use select_nth_unstable for partial sort (standard library)
    rows.select_nth_unstable_by(limit, |a, b| compare_rows(a, b, sort_key_indices));
    // Sort only the first `limit` elements
    rows[..limit].sort_by(|a, b| compare_rows(a, b, sort_key_indices));
}

fn compare_rows(
    a: &[ScalarValue],
    b: &[ScalarValue],
    keys: &[(usize, bool)],
) -> std::cmp::Ordering {
    for &(idx, desc) in keys {
        let cmp = compare_scalars(&a[idx], &b[idx]);
        let cmp = if desc { cmp.reverse() } else { cmp };
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Less;
    }
    if b.is_null() {
        return std::cmp::Ordering::Greater;
    }
    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        // Cross-type comparison: promote to i64
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(&(*r as i64)),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => (*l as i64).cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Int64(Some(r))) => l
            .partial_cmp(&(*r as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Float64(Some(r))) => (*l as f64)
            .partial_cmp(r)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}
