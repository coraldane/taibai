use crate::common::error::Result;
use crate::types::batch::{RecordBatch, Schema};

use super::ExecutionOp;

pub struct ScanExec {
    batches: Vec<RecordBatch>,
    pos: usize,
    projection: Option<Vec<usize>>,
}

impl ScanExec {
    pub fn new(batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> Self {
        Self {
            batches,
            pos: 0,
            projection,
        }
    }
}

impl ExecutionOp for ScanExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pos >= self.batches.len() {
            return Ok(None);
        }
        let batch = &self.batches[self.pos];
        self.pos += 1;

        match &self.projection {
            Some(indices) => {
                if is_already_projected(batch, indices) {
                    return Ok(Some(batch.clone()));
                }
                let schema_cols: Vec<&crate::types::batch::ColumnDef> =
                    indices.iter().map(|&i| &batch.schema.columns[i]).collect();
                let new_schema = Schema::new(schema_cols.iter().map(|c| (*c).clone()).collect());
                let mut cols = Vec::with_capacity(indices.len());
                for &idx in indices {
                    cols.push(batch.column(idx).unwrap().clone());
                }
                Ok(Some(RecordBatch::try_new(new_schema, cols)?))
            }
            None => Ok(Some(batch.clone())),
        }
    }
}

fn is_already_projected(batch: &RecordBatch, indices: &[usize]) -> bool {
    batch.schema.columns.len() == indices.len()
        && (indices
            .iter()
            .enumerate()
            .all(|(idx, &source_idx)| idx == source_idx)
            || indices
                .iter()
                .any(|&source_idx| source_idx >= batch.schema.columns.len()))
}
