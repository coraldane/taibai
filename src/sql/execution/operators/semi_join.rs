use crate::common::error::Result;
use crate::sql::planner::LogicalExpr;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

/// Semi / anti join.
///
/// The join condition is split into cross-side equi conjuncts
/// (`left.col = right.col`, possibly several, AND-combined) and a residual
/// predicate that is evaluated per candidate pair. Right rows are staged in
/// a hash multi-map keyed by the equi tuple; left rows probe it and keep a
/// match when at least one candidate also satisfies the residual.
///
/// NULL semantics: a NULL in any equi-key component can never match (SQL
/// join semantics), and residual evaluation follows the ordinary two-valued
/// comparison rules of `eval_filter`.
pub struct SemiJoinExec {
    pub left: Box<dyn ExecutionOp>,
    pub right: Box<dyn ExecutionOp>,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub is_anti: bool,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    residual: Option<LogicalExpr>,
    left_batches: Vec<RecordBatch>,
    /// Right rows keyed by the equi tuple; empty map when the condition has
    /// no equi part (then `right_all_rows` is used instead).
    right_hash_map: std::collections::HashMap<Vec<ScalarValue>, Vec<Vec<ScalarValue>>>,
    /// All right rows, used when the condition has no equi conjuncts.
    right_all_rows: Vec<Vec<ScalarValue>>,
    right_collected: bool,
    left_batch_idx: usize,
    left_row_idx: usize,
    matched_left_rows: Vec<bool>,
    /// Reusable 1-row batch layout for residual evaluation.
    residual_schema: Schema,
}

impl SemiJoinExec {
    pub fn new(
        left: Box<dyn ExecutionOp>,
        right: Box<dyn ExecutionOp>,
        condition: LogicalExpr,
        left_schema: Schema,
        right_schema: Schema,
        is_anti: bool,
    ) -> Self {
        let left_ncols = left_schema.columns.len();
        let (left_key_cols, right_key_cols, residual) =
            extract_semi_join_keys(&condition, left_ncols);
        let mut residual_columns = left_schema.columns.clone();
        residual_columns.extend(right_schema.columns.clone());
        Self {
            left,
            right,
            condition,
            left_schema: left_schema.clone(),
            right_schema,
            is_anti,
            left_key_cols,
            right_key_cols,
            residual,
            left_batches: Vec::new(),
            right_hash_map: std::collections::HashMap::new(),
            right_all_rows: Vec::new(),
            right_collected: false,
            left_batch_idx: 0,
            left_row_idx: 0,
            matched_left_rows: Vec::new(),
            residual_schema: Schema::new(residual_columns),
        }
    }

    fn collect_right(&mut self) -> Result<()> {
        let has_keys = !self.right_key_cols.is_empty();
        while let Some(batch) = self.right.next_batch()? {
            crate::common::query_cancel::check()?;
            for row in 0..batch.num_rows() {
                let values: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                if has_keys {
                    let key: Vec<ScalarValue> = self
                        .right_key_cols
                        .iter()
                        .map(|&idx| values[idx].clone())
                        .collect();
                    // A NULL in the equi key can never match anything.
                    if key.iter().any(|v| v.is_null()) {
                        continue;
                    }
                    self.right_hash_map.entry(key).or_default().push(values);
                } else {
                    self.right_all_rows.push(values);
                }
            }
        }
        self.right_collected = true;
        Ok(())
    }

    fn collect_left(&mut self) -> Result<()> {
        let has_keys = !self.left_key_cols.is_empty();
        while let Some(batch) = self.left.next_batch()? {
            crate::common::query_cancel::check()?;
            let batch_start = self.matched_left_rows.len();
            self.matched_left_rows.extend(vec![false; batch.num_rows()]);
            for row in 0..batch.num_rows() {
                let left_values: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                let candidates: Option<&Vec<Vec<ScalarValue>>> = if has_keys {
                    let key: Vec<ScalarValue> = self
                        .left_key_cols
                        .iter()
                        .map(|&idx| left_values[idx].clone())
                        .collect();
                    if key.iter().any(|v| v.is_null()) {
                        None
                    } else {
                        self.right_hash_map.get(&key)
                    }
                } else {
                    None // placeholder; handled below via right_all_rows
                };

                let matched = if has_keys {
                    match candidates {
                        Some(rows) => rows
                            .iter()
                            .any(|right_values| self.residual_matches(&left_values, right_values)),
                        None => false,
                    }
                } else {
                    self.right_all_rows
                        .iter()
                        .any(|right_values| self.residual_matches(&left_values, right_values))
                };
                self.matched_left_rows[batch_start + row] = matched;
            }
            self.left_batches.push(batch);
        }
        Ok(())
    }

    fn residual_matches(&self, left_values: &[ScalarValue], right_values: &[ScalarValue]) -> bool {
        let Some(residual) = &self.residual else {
            return true;
        };
        let mut columns = Vec::with_capacity(self.residual_schema.columns.len());
        for def in &self.residual_schema.columns {
            columns.push(crate::types::array::ColumnArray::empty(def.data_type));
        }
        for (i, value) in left_values.iter().enumerate() {
            if columns[i].push(value).is_err() {
                return false;
            }
        }
        for (i, value) in right_values.iter().enumerate() {
            if columns[self.left_schema.columns.len() + i]
                .push(value)
                .is_err()
            {
                return false;
            }
        }
        let Ok(batch) = RecordBatch::try_new(self.residual_schema.clone(), columns) else {
            return false;
        };
        matches!(
            super::filter::eval_filter_with(residual, &batch, None),
            Ok(mask) if mask.first() == Some(&true)
        )
    }
}

impl ExecutionOp for SemiJoinExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.right_collected {
            self.collect_right()?;
            self.collect_left()?;
        }

        const BATCH_SIZE: usize = 4096;
        let mut output_rows = Vec::new();

        while output_rows.len() < BATCH_SIZE {
            if self.left_batch_idx >= self.left_batches.len() {
                break;
            }
            let batch = &self.left_batches[self.left_batch_idx];
            while self.left_row_idx < batch.num_rows() && output_rows.len() < BATCH_SIZE {
                let global_idx = self.left_batches[..self.left_batch_idx]
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>()
                    + self.left_row_idx;
                let should_emit = if self.is_anti {
                    !self.matched_left_rows[global_idx]
                } else {
                    self.matched_left_rows[global_idx]
                };
                if should_emit {
                    let row_vals: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| {
                            batch
                                .scalar(self.left_row_idx, c)
                                .unwrap_or(ScalarValue::Int64(None))
                        })
                        .collect();
                    output_rows.push(row_vals);
                }
                self.left_row_idx += 1;
            }
            if self.left_row_idx >= batch.num_rows() {
                self.left_batch_idx += 1;
                self.left_row_idx = 0;
            }
        }

        if output_rows.is_empty() {
            return Ok(None);
        }

        let mut columns = Vec::with_capacity(self.left_schema.columns.len());
        for col_def in &self.left_schema.columns {
            columns.push(crate::types::array::ColumnArray::empty(col_def.data_type));
        }

        for row in &output_rows {
            for (i, val) in row.iter().enumerate() {
                columns[i].push(val)?;
            }
        }

        Ok(Some(RecordBatch::try_new(
            self.left_schema.clone(),
            columns,
        )?))
    }
}

/// Split the condition into cross-side equi key pairs and a residual
/// predicate. Only top-level AND conjuncts are considered; equi conjuncts
/// must compare two bare columns from opposite sides.
fn extract_semi_join_keys(
    condition: &LogicalExpr,
    left_ncols: usize,
) -> (Vec<usize>, Vec<usize>, Option<LogicalExpr>) {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    let mut residual = Vec::new();
    for conjunct in split_and_conjuncts(condition) {
        if let Some((l, r)) = cross_equi_pair(&conjunct, left_ncols) {
            left_keys.push(l);
            right_keys.push(r);
        } else {
            residual.push(conjunct);
        }
    }
    let residual = combine_conjuncts(residual);
    (left_keys, right_keys, residual)
}

fn split_and_conjuncts(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: crate::sql::parser::ast::BinOp::And,
            right,
        } => {
            let mut conjuncts = split_and_conjuncts(left);
            conjuncts.extend(split_and_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

fn combine_conjuncts(conjuncts: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut combined: Option<LogicalExpr> = None;
    for conjunct in conjuncts {
        combined = Some(match combined {
            Some(prev) => LogicalExpr::BinaryOp {
                left: Box::new(prev),
                op: crate::sql::parser::ast::BinOp::And,
                right: Box::new(conjunct),
            },
            None => conjunct,
        });
    }
    combined
}

fn cross_equi_pair(expr: &LogicalExpr, left_ncols: usize) -> Option<(usize, usize)> {
    let LogicalExpr::BinaryOp {
        left,
        op: crate::sql::parser::ast::BinOp::Eq,
        right,
    } = expr
    else {
        return None;
    };
    let (LogicalExpr::Column(l), LogicalExpr::Column(r)) = (left.as_ref(), right.as_ref()) else {
        return None;
    };
    if *l < left_ncols && *r >= left_ncols {
        Some((*l, r - left_ncols))
    } else if *r < left_ncols && *l >= left_ncols {
        Some((*r, l - left_ncols))
    } else {
        None
    }
}
