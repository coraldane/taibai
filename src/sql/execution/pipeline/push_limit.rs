use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::PushOp;

pub struct PushLimit {
    limit: usize,
    offset: usize,
    rows_passed: usize,
    skipped: usize,
    buffer: Vec<RecordBatch>,
    done: bool,
}

impl PushLimit {
    pub fn new(limit: usize, offset: usize) -> Self {
        Self {
            limit,
            offset,
            rows_passed: 0,
            skipped: 0,
            buffer: Vec::new(),
            done: false,
        }
    }
}

impl PushOp for PushLimit {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.done || batch.num_rows() == 0 {
            return Ok(());
        }

        let mut start = 0;
        let num_rows = batch.num_rows();

        if self.skipped < self.offset {
            let to_skip = (self.offset - self.skipped).min(num_rows);
            self.skipped += to_skip;
            start = to_skip;
            if start >= num_rows {
                return Ok(());
            }
        }

        let remaining = self.limit - self.rows_passed;
        let available = num_rows - start;
        let take = available.min(remaining);

        if take > 0 {
            let trimmed = trim_batch_rows(&batch, start, start + take);
            self.rows_passed += take;
            self.buffer.push(trimmed);
        }

        if self.rows_passed >= self.limit {
            self.done = true;
        }

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        if self.buffer.len() == 1 {
            return Ok(Some(self.buffer.into_iter().next().unwrap()));
        }
        let schema = self.buffer[0].schema.clone();
        let num_cols = schema.columns.len();
        let mut columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let dt = schema.columns[col_idx].data_type;
            let mut col = crate::types::array::ColumnArray::empty(dt);
            for batch in &self.buffer {
                if let Some(src) = batch.column(col_idx) {
                    for i in 0..batch.num_rows() {
                        col.push(&src.scalar(i)).unwrap();
                    }
                }
            }
            columns.push(col);
        }
        Ok(Some(RecordBatch::try_new(schema, columns).unwrap()))
    }
}

fn trim_batch_rows(batch: &RecordBatch, start: usize, end: usize) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = crate::types::array::ColumnArray::empty(col.data_type());
        for i in start..end {
            new_col.push(&col.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}
