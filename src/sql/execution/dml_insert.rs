use crate::common::error::{MonaxError, Result};
use crate::storage::segment::SegmentWriter;
use crate::storage::sort_key::SortKeyInserter;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{discover_segments, sort_batch_by_key, eval_expr_to_scalar};
use crate::sql::parser::ast::Expr;

pub fn exec_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs.get(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog.get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    let schema = Schema::new(table_meta.columns.clone());

    let col_indices: Vec<usize> = if columns.is_empty() {
        (0..schema.columns.len()).collect()
    } else {
        columns.iter().map(|c| {
            schema.index_of(c)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", c)))
        }).collect::<Result<Vec<_>>>()?
    };

    if col_indices.len() != values[0].len() {
        return Err(MonaxError::Sql("column count doesn't match value count".into()));
    }

    let mut batch = RecordBatch::new(schema.clone());
    for row in values {
        for (val_idx, &col_idx) in col_indices.iter().enumerate() {
            let scalar = eval_expr_to_scalar(&row[val_idx], &schema.columns[col_idx].data_type)?;
            batch.column_mut(col_idx).unwrap().push(&scalar)?;
        }
    }

    drop(catalogs);

    let table_dir = engine.data_dir().join(db).join(table);
    let (write_dir, partition_key) = super::wal_helper::determine_write_dir(
        engine, &table_meta, &batch, &table_dir,
    )?;
    std::fs::create_dir_all(&write_dir)?;

    super::wal_helper::write_wal(engine, &table_dir, &batch)?;

    let has_sort_key = !table_meta.cluster_key.is_empty();
    if has_sort_key {
        let key_indices: Vec<usize> = table_meta.cluster_key.iter()
            .filter_map(|k| schema.index_of(k))
            .collect();
        if !key_indices.is_empty() {
            let inserter = SortKeyInserter::new(key_indices, vec![false]);
            if !inserter.is_sorted(&batch) {
                batch = sort_batch_by_key(&batch, &inserter)?;
            }
        }
    }

    let seg_path = if partition_key.is_empty() {
        engine.next_segment_path(&write_dir, "")?
    } else {
        engine.next_partition_segment_path(&write_dir, &partition_key)?
    };
    let mut writer = SegmentWriter::create(&seg_path, schema, 8192)?;
    writer.write_batch(&batch)?;
    writer.seal()?;

    {
        let mut wal_writers = engine.wal_writers().lock().unwrap();
        let key = table_dir.to_string_lossy().to_string();
        if let Some(wal_writer) = wal_writers.get_mut(&key) {
            wal_writer.clear()?;
        }
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(meta) = catalog.get_table_mut(table) {
            meta.row_count += values.len() as i64;
            meta.segment_count += 1;
        }
        let meta_dir = engine.data_dir().join("meta");
        catalog.save(&meta_dir)?;
    }

    Ok(ExecResult::Ok { rows_affected: values.len() as u64 })
}
