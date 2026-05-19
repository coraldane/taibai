use crate::common::error::Result;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};

use super::ExecutionOp;

pub struct LimitExec {
    pub limit: usize,
    pub offset: usize,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    rows_returned: usize,
    skip_remaining: usize,
    done: bool,
}

impl LimitExec {
    pub fn new(limit: usize, offset: usize, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self { limit, offset, input, schema, rows_returned: 0, skip_remaining: offset, done: false }
    }
}

impl ExecutionOp for LimitExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done || self.rows_returned >= self.limit {
            return Ok(None);
        }

        match self.input.next_batch()? {
            Some(batch) => {
                if self.skip_remaining > 0 {
                    if batch.num_rows() <= self.skip_remaining {
                        self.skip_remaining -= batch.num_rows();
                        return self.next_batch();
                    }
                    let start = self.skip_remaining;
                    self.skip_remaining = 0;
                    let remaining = self.limit - self.rows_returned;
                    let end = (start + remaining).min(batch.num_rows());
                    let trimmed = trim_batch(&batch, start, end);
                    self.rows_returned += trimmed.num_rows();
                    if self.rows_returned >= self.limit {
                        self.done = true;
                    }
                    return Ok(Some(trimmed));
                }

                let remaining = self.limit - self.rows_returned;
                if batch.num_rows() <= remaining {
                    self.rows_returned += batch.num_rows();
                    Ok(Some(batch))
                } else {
                    let trimmed = trim_batch(&batch, 0, remaining);
                    self.rows_returned = self.limit;
                    self.done = true;
                    Ok(Some(trimmed))
                }
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

pub fn trim_batch(batch: &RecordBatch, start: usize, end: usize) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for i in start..end {
            new_col.push(&col.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}
