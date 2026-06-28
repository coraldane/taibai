use crate::common::error::Result;
use std::collections::VecDeque;

use crate::sql::execution::scan::cursor::ScanCursor;
use crate::sql::execution::scan::runtime::ScanRuntime;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::storage::catalog::schema::TableMeta;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct ScanExec {
    source: ScanSource,
    projection: Option<Vec<usize>>,
    table_schema: Option<Schema>,
}

enum ScanSource {
    Batches(VecDeque<RecordBatch>),
    Cursor(ScanCursor),
    Deferred {
        spec: ScanSpec,
        runtime: ScanRuntime,
        schema: Schema,
        meta: TableMeta,
        db: String,
        batches: Option<VecDeque<RecordBatch>>,
    },
}

impl ScanExec {
    pub fn new(batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> Self {
        Self {
            source: ScanSource::Batches(VecDeque::from(batches)),
            projection,
            table_schema: None,
        }
    }

    pub fn deferred(
        spec: ScanSpec,
        runtime: ScanRuntime,
        schema: Schema,
        meta: TableMeta,
        db: String,
    ) -> Self {
        if let Ok(Some(cursor)) = ScanCursor::try_new(
            spec.clone(),
            runtime.clone(),
            schema.clone(),
            meta.clone(),
            db.clone(),
        ) {
            return Self {
                source: ScanSource::Cursor(cursor),
                projection: None,
                table_schema: Some(schema),
            };
        }

        Self {
            source: ScanSource::Deferred {
                spec,
                runtime,
                schema: schema.clone(),
                meta,
                db,
                batches: None,
            },
            projection: None,
            table_schema: Some(schema),
        }
    }

    pub fn with_table_schema(mut self, schema: Schema) -> Self {
        self.table_schema = Some(schema);
        self
    }

    fn batches(&mut self) -> Result<&mut VecDeque<RecordBatch>> {
        match &mut self.source {
            ScanSource::Batches(batches) => Ok(batches),
            ScanSource::Cursor(_) => unreachable!(),
            ScanSource::Deferred {
                spec,
                runtime,
                schema,
                meta,
                db,
                batches,
            } => {
                if batches.is_none() {
                    let mut executor = crate::sql::execution::scan::ScanExecutor::from_runtime(
                        spec,
                        runtime.clone(),
                        schema.clone(),
                        meta.clone(),
                        db,
                    );
                    *batches = Some(VecDeque::from(executor.execute()?));
                }
                Ok(batches.as_mut().unwrap())
            }
        }
    }
}

impl ExecutionOp for ScanExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let ScanSource::Cursor(cursor) = &mut self.source {
            return cursor.next_batch();
        }

        let batch = {
            let batches = self.batches()?;
            let Some(batch) = batches.pop_front() else {
                return Ok(None);
            };
            batch
        };

        match &self.projection {
            Some(indices) => project_batch(batch, indices, self.table_schema.as_ref()),
            None => Ok(Some(batch)),
        }
    }
}

fn project_batch(
    batch: RecordBatch,
    indices: &[usize],
    table_schema: Option<&Schema>,
) -> Result<Option<RecordBatch>> {
    if is_already_projected(&batch, indices, table_schema) {
        return Ok(Some(batch));
    }

    let batch_col_count = batch.schema.columns.len();
    let needs_null_fill = indices.iter().any(|&i| i >= batch_col_count);

    if needs_null_fill {
        let table_schema = table_schema.unwrap_or(&batch.schema);
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
        let new_schema = Schema::new(schema_cols.iter().map(|c| (*c).clone()).collect());
        let mut cols = Vec::with_capacity(indices.len());
        for &idx in indices {
            cols.push(batch.column(idx).unwrap().clone());
        }
        Ok(Some(RecordBatch::try_new(new_schema, cols)?))
    }
}

fn is_already_projected(
    batch: &RecordBatch,
    indices: &[usize],
    table_schema: Option<&Schema>,
) -> bool {
    if batch.schema.columns.len() != indices.len() {
        return false;
    }

    if indices
        .iter()
        .enumerate()
        .all(|(idx, &source_idx)| idx == source_idx)
    {
        return true;
    }

    let Some(table_schema) = table_schema else {
        return false;
    };

    indices
        .iter()
        .enumerate()
        .all(|(projected_idx, &source_idx)| {
            batch.schema.columns.get(projected_idx) == table_schema.columns.get(source_idx)
        })
}
