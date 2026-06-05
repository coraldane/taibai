use crate::common::error::Result;
use crate::sql::execution::operators::filter::eval_filter;
use crate::sql::planner::LogicalExpr;
use crate::types::batch::{RecordBatch, Schema};

use super::PushOp;

pub struct PushFilter {
    predicate: LogicalExpr,
    output_schema: Schema,
    buffer: Vec<RecordBatch>,
}

impl PushFilter {
    pub fn new(predicate: LogicalExpr, schema: Schema) -> Self {
        Self {
            predicate,
            output_schema: schema,
            buffer: Vec::new(),
        }
    }
}

impl PushOp for PushFilter {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mask = eval_filter(&self.predicate, &batch)?;
        let filtered = crate::sql::execution::operators::apply_filter_mask(&batch, &mask);
        if filtered.num_rows() > 0 {
            self.buffer.push(filtered);
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(Some(RecordBatch::new(self.output_schema)));
        }
        Ok(Some(merge_batches(&self.buffer)))
    }
}

fn merge_batches(batches: &[RecordBatch]) -> RecordBatch {
    if batches.is_empty() {
        return RecordBatch::new(crate::types::batch::Schema::empty());
    }
    if batches.len() == 1 {
        return batches[0].clone();
    }
    let schema = batches[0].schema.clone();
    let num_cols = schema.columns.len();
    let mut columns = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let dt = schema.columns[col_idx].data_type;
        let mut col = crate::types::array::ColumnArray::empty(dt);
        for batch in batches {
            if let Some(src) = batch.column(col_idx) {
                for i in 0..batch.num_rows() {
                    col.push(&src.scalar(i)).unwrap();
                }
            }
        }
        columns.push(col);
    }
    RecordBatch::try_new(schema, columns).unwrap()
}
