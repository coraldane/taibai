use crate::common::error::Result;
use crate::sql::planner::LogicalExpr;
use crate::types::batch::{RecordBatch, Schema};

use super::ExecutionOp;

pub struct SemiJoinExec {
    pub left: Box<dyn ExecutionOp>,
    pub right: Box<dyn ExecutionOp>,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub is_anti: bool,
    left_key_cols: Vec<usize>,
    right_key_cols: Vec<usize>,
    left_batches: Vec<RecordBatch>,
    right_hash_set: std::collections::HashSet<Vec<crate::types::scalar::ScalarValue>>,
    right_collected: bool,
    left_batch_idx: usize,
    left_row_idx: usize,
    matched_left_rows: Vec<bool>,
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
        let (left_key_cols, right_key_cols) = extract_semi_join_keys(&condition, left_ncols);
        Self {
            left,
            right,
            condition,
            left_schema,
            right_schema,
            is_anti,
            left_key_cols,
            right_key_cols,
            left_batches: Vec::new(),
            right_hash_set: std::collections::HashSet::new(),
            right_collected: false,
            left_batch_idx: 0,
            left_row_idx: 0,
            matched_left_rows: Vec::new(),
        }
    }

    fn collect_right(&mut self) -> Result<()> {
        while let Some(batch) = self.right.next_batch()? {
            for row in 0..batch.num_rows() {
                let key: Vec<crate::types::scalar::ScalarValue> = self
                    .right_key_cols
                    .iter()
                    .map(|&idx| {
                        batch
                            .scalar(row, idx)
                            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None))
                    })
                    .collect();
                self.right_hash_set.insert(key);
            }
        }
        self.right_collected = true;
        Ok(())
    }

    fn collect_left(&mut self) -> Result<()> {
        while let Some(batch) = self.left.next_batch()? {
            let batch_start = self.matched_left_rows.len();
            self.matched_left_rows.extend(vec![false; batch.num_rows()]);
            for row in 0..batch.num_rows() {
                let key: Vec<crate::types::scalar::ScalarValue> = self
                    .left_key_cols
                    .iter()
                    .map(|&idx| {
                        batch
                            .scalar(row, idx)
                            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None))
                    })
                    .collect();
                if self.right_hash_set.contains(&key) {
                    self.matched_left_rows[batch_start + row] = true;
                }
            }
            self.left_batches.push(batch);
        }
        Ok(())
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
                    let row_vals: Vec<crate::types::scalar::ScalarValue> = (0..batch.num_columns())
                        .map(|c| {
                            batch
                                .scalar(self.left_row_idx, c)
                                .unwrap_or(crate::types::scalar::ScalarValue::Int64(None))
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

fn extract_semi_join_keys(condition: &LogicalExpr, left_ncols: usize) -> (Vec<usize>, Vec<usize>) {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    collect_keys(condition, left_ncols, &mut left_keys, &mut right_keys);
    (left_keys, right_keys)
}

fn collect_keys(
    expr: &LogicalExpr,
    left_ncols: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) {
    match expr {
        LogicalExpr::BinaryOp { left, op: _, right } => {
            if let LogicalExpr::Column(l) = **left {
                if let LogicalExpr::Column(r) = **right {
                    if l < left_ncols && r >= left_ncols {
                        left_keys.push(l);
                        right_keys.push(r - left_ncols);
                    } else if r < left_ncols && l >= left_ncols {
                        left_keys.push(r);
                        right_keys.push(l - left_ncols);
                    }
                }
            }
        }
        LogicalExpr::Literal(_) => {}
        _ => {}
    }
}
