use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::schema::{DatabaseCatalog, PartitionType, TableMeta};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{ColumnDefAst, PartitionDef, PartitionStrategy};

pub fn exec_create_table(
    engine: &Engine,
    db: &str,
    name: &str,
    columns: &[ColumnDefAst],
    if_not_exists: bool,
    sort_key: &Option<Vec<String>>,
    partition_by: &Option<PartitionDef>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_or_create_catalog(&mut catalogs, engine.data_dir(), db)?;

    if catalog.get_table(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(MonaxError::Sql(format!("table '{}.{}' already exists", db, name)));
    }

    let col_defs: Vec<ColumnDef> = columns.iter().map(|c| {
        ColumnDef::new(&c.name, c.data_type)
            .with_nullable(c.nullable)
            .with_scale(c.scale)
            .with_precision(c.precision)
    }).collect();

    catalog.create_table(name, col_defs)?;

    if let Some(meta) = catalog.get_table_mut(name) {
        if let Some(ref keys) = sort_key {
            meta.cluster_key = keys.clone();
        }
        if let Some(ref pdef) = partition_by {
            meta.partition_key = pdef.column.clone();
            meta.partition_type = match pdef.strategy {
                PartitionStrategy::Day => PartitionType::Function,
                PartitionStrategy::Year => PartitionType::Function,
                PartitionStrategy::Value => PartitionType::Value,
            };
        }
    }

    let table_dir = engine.data_dir().join(db).join(name);
    std::fs::create_dir_all(&table_dir)?;

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_table(
    engine: &Engine,
    db: &str,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_or_create_catalog(&mut catalogs, engine.data_dir(), db)?;

    if catalog.get_table(name).is_none() {
        if if_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(MonaxError::Sql(format!("table '{}.{}' does not exist", db, name)));
    }

    catalog.drop_table(name)?;

    let table_dir = engine.data_dir().join(db).join(name);
    {
        let key = table_dir.to_string_lossy().to_string();
        let mut wal_writers = engine.wal_writers().lock().unwrap();
        wal_writers.remove(&key);
    }

    if table_dir.exists() {
        let _ = std::fs::remove_dir_all(&table_dir);
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_show_tables(engine: &Engine, db: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("table_name", DataType::VarChar)]);

    let mut batch = RecordBatch::new(schema.clone());
    if let Some(catalog) = catalogs.get(db) {
        for name in catalog.table_names() {
            batch.column_mut(0).unwrap().push(&crate::types::scalar::ScalarValue::VarChar(Some(name.to_string())))?;
        }
    }

    Ok(ExecResult::Query { schema, batches: vec![batch] })
}

pub fn exec_show_databases(engine: &Engine) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("database", DataType::VarChar)]);

    let mut batch = RecordBatch::new(schema.clone());
    for name in catalogs.keys() {
        batch.column_mut(0).unwrap().push(&crate::types::scalar::ScalarValue::VarChar(Some(name.clone())))?;
    }

    Ok(ExecResult::Query { schema, batches: vec![batch] })
}

fn get_or_create_catalog<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    data_dir: &std::path::Path,
    db: &str,
) -> Result<&'a mut DatabaseCatalog> {
    if !catalogs.contains_key(db) {
        let meta_dir = data_dir.join("meta");
        let catalog = DatabaseCatalog::load_or_create(&meta_dir, db)?;
        catalogs.insert(db.to_string(), catalog);
    }
    Ok(catalogs.get_mut(db).unwrap())
}
