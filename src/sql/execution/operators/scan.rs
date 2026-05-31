use crate::common::error::Result;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct ScanExec {
    batches: Vec<RecordBatch>,
    pos: usize,
    projection: Option<Vec<usize>>,
    table_schema: Option<Schema>,
}

impl ScanExec {
    pub fn new(batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> Self {
        Self {
            batches,
            pos: 0,
            projection,
            table_schema: None,
        }
    }

    pub fn with_table_schema(mut self, schema: Schema) -> Self {
        self.table_schema = Some(schema);
        self
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

                let batch_col_count = batch.schema.columns.len();
                let needs_null_fill = indices.iter().any(|&i| i >= batch_col_count);

                if needs_null_fill {
                    let table_schema = self.table_schema.as_ref().unwrap_or(&batch.schema);
                    let row_count = batch.num_rows();
                    let mut cols = Vec::with_capacity(indices.len());
                    let mut col_defs = Vec::with_capacity(indices.len());

                    for &idx in indices {
                        if idx < batch_col_count {
                            cols.push(batch.column(idx).unwrap().clone());
                            col_defs.push(batch.schema.columns[idx].clone());
                        } else if idx < table_schema.columns.len() {
                            let col_def = &table_schema.columns[idx];
                            let mut null_col = ColumnArray::empty(col_def.data_type);
                            for _ in 0..row_count {
                                null_col.push(&ScalarValue::null_for_type(col_def.data_type))?;
                            }
                            cols.push(null_col);
                            col_defs.push(col_def.clone());
                        }
                    }

                    let new_schema = Schema::new(col_defs);
                    Ok(Some(RecordBatch::try_new(new_schema, cols)?))
                } else {
                    let schema_cols: Vec<&ColumnDef> =
                        indices.iter().map(|&i| &batch.schema.columns[i]).collect();
                    let new_schema =
                        Schema::new(schema_cols.iter().map(|c| (*c).clone()).collect());
                    let mut cols = Vec::with_capacity(indices.len());
                    for &idx in indices {
                        cols.push(batch.column(idx).unwrap().clone());
                    }
                    Ok(Some(RecordBatch::try_new(new_schema, cols)?))
                }
            }
            None => Ok(Some(batch.clone())),
        }
    }
}

fn is_already_projected(batch: &RecordBatch, indices: &[usize]) -> bool {
    batch.schema.columns.len() == indices.len()
        && indices
            .iter()
            .enumerate()
            .all(|(idx, &source_idx)| idx == source_idx)
}
