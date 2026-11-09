use super::*;

pub fn exec_show_tables(engine: &Engine, db: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let schema = Schema::new(vec![ColumnDef::new("table_name", DataType::VarChar)]);

    let mut batch = RecordBatch::new(schema.clone());
    for name in catalog.table_names() {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name.to_string())))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_databases(engine: &Engine) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("database", DataType::VarChar)]);

    let mut batch = RecordBatch::new(schema.clone());
    for name in catalogs.keys().filter(|name| !name.starts_with('.')) {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name.clone())))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_columns(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

    let schema = Schema::new(vec![
        ColumnDef::new("column_name", DataType::VarChar),
        ColumnDef::new("data_type", DataType::VarChar),
        ColumnDef::new("nullable", DataType::Bool),
    ]);

    let mut batch = RecordBatch::new(schema.clone());
    for col in &table_meta.columns {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(col.name.clone())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(column_type_display(col))))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Bool(Some(col.nullable)))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

fn column_type_display(col: &ColumnDef) -> String {
    if col.data_type == DataType::Decimal && col.precision > 0 {
        if col.scale > 0 {
            format!("DECIMAL({},{})", col.precision, col.scale)
        } else {
            format!("DECIMAL({})", col.precision)
        }
    } else {
        col.data_type.to_string()
    }
}

pub fn exec_show_indexes(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

    let schema = Schema::new(vec![
        ColumnDef::new("Table", DataType::VarChar),
        ColumnDef::new("Non_unique", DataType::Int64),
        ColumnDef::new("Key_name", DataType::VarChar),
        ColumnDef::new("Seq_in_index", DataType::Int64),
        ColumnDef::new("Column_name", DataType::VarChar),
        ColumnDef::new("Index_type", DataType::VarChar),
    ]);

    let mut batch = RecordBatch::new(schema.clone());
    for (idx, col_name) in table_meta.cluster_key.iter().enumerate() {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(table.to_string())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(0)))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("cluster_key".to_string())))?;
        batch
            .column_mut(3)
            .unwrap()
            .push(&ScalarValue::Int64(Some((idx + 1) as i64)))?;
        batch
            .column_mut(4)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(col_name.clone())))?;
        batch
            .column_mut(5)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("CLUSTER_BY".to_string())))?;
    }

    for unique_index in &table_meta.unique_indexes {
        for (idx, col_name) in unique_index.columns.iter().enumerate() {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(table.to_string())))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::Int64(Some(0)))?;
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(unique_index.name.clone())))?;
            batch
                .column_mut(3)
                .unwrap()
                .push(&ScalarValue::Int64(Some((idx + 1) as i64)))?;
            batch
                .column_mut(4)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(col_name.clone())))?;
            batch
                .column_mut(5)
                .unwrap()
                .push(&ScalarValue::VarChar(Some("UNIQUE".to_string())))?;
        }
    }

    for secondary_index in &table_meta.secondary_indexes {
        for (idx, col_name) in secondary_index.columns.iter().enumerate() {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(table.to_string())))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::Int64(Some(1)))?;
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(secondary_index.name.clone())))?;
            batch
                .column_mut(3)
                .unwrap()
                .push(&ScalarValue::Int64(Some((idx + 1) as i64)))?;
            batch
                .column_mut(4)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(col_name.clone())))?;
            batch
                .column_mut(5)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(
                    secondary_index_type_display(secondary_index.index_type).to_string(),
                )))?;
        }
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

fn secondary_index_type_display(index_type: SecondaryIndexType) -> &'static str {
    match index_type {
        SecondaryIndexType::Bloom => "BLOOM",
        SecondaryIndexType::MinMax => "MINMAX",
        SecondaryIndexType::Set => "SET",
    }
}

pub fn exec_show_create_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

    let mut create_sql = format!("CREATE TABLE {} (\n", table);
    for (i, col) in table_meta.columns.iter().enumerate() {
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        let auto_inc = if col.auto_increment {
            " AUTO_INCREMENT"
        } else {
            ""
        };
        let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
        let def_str = match &col.default_value {
            Some(ColumnDefaultValue::CurrentTimestamp) => " DEFAULT CURRENT_TIMESTAMP".to_string(),
            Some(ColumnDefaultValue::Literal(s)) => match s {
                ScalarValue::VarChar(Some(v)) => format!(" DEFAULT '{}'", v),
                ScalarValue::Int64(Some(v)) => format!(" DEFAULT {}", v),
                ScalarValue::Float64(Some(v)) => format!(" DEFAULT {}", v),
                ScalarValue::Int64(None) => " DEFAULT NULL".to_string(),
                _ => "".to_string(),
            },
            None => "".to_string(),
        };
        if i < table_meta.columns.len() - 1 {
            create_sql.push_str(&format!(
                "  {} {}{}{}{}{},\n",
                col.name,
                column_type_display(col),
                nullable,
                auto_inc,
                pk,
                def_str
            ));
        } else {
            create_sql.push_str(&format!(
                "  {} {}{}{}{}{}\n",
                col.name,
                column_type_display(col),
                nullable,
                auto_inc,
                pk,
                def_str
            ));
        }
    }
    for unique_index in &table_meta.unique_indexes {
        let cols = unique_index.columns.join(", ");
        create_sql.push_str(&format!(",\n  UNIQUE KEY {} ({})", unique_index.name, cols));
    }
    create_sql.push(')');

    if !table_meta.cluster_key.is_empty() {
        create_sql.push_str(&format!(
            " CLUSTER BY ({})",
            table_meta.cluster_key.join(", ")
        ));
    }

    if let Some(time_col) = table_meta.hot_time_column.as_ref() {
        let mut hot_str = format!(" HOT BY ({})", time_col);
        if let Some(idx_col) = &table_meta.hot_index_column {
            hot_str.push_str(&format!(" INDEX ({})", idx_col));
        }
        if let Some(close_time) = &table_meta.hot_close_time {
            hot_str.push_str(&format!(" CLOSE AT '{}'", close_time));
        }
        create_sql.push_str(&hot_str);
    }

    if table_meta.is_partitioned() {
        let partition_str = match table_meta.partition_type {
            PartitionType::Day => format!(" PARTITION BY DAY({})", table_meta.partition_key),
            PartitionType::Year => format!(" PARTITION BY YEAR({})", table_meta.partition_key),
            PartitionType::Month => format!(" PARTITION BY MONTH({})", table_meta.partition_key),
            PartitionType::Value => format!(" PARTITION BY {}", table_meta.partition_key),
            PartitionType::Hash => format!(
                " PARTITION BY HASH({}, {})",
                table_meta.partition_key, table_meta.partition_param
            ),
            PartitionType::Mod => format!(
                " PARTITION BY MOD({}, {})",
                table_meta.partition_key, table_meta.partition_param
            ),
            PartitionType::None => String::new(),
        };
        create_sql.push_str(&partition_str);
    }

    if let Some(ttl) = &table_meta.ttl {
        create_sql.push_str(&format!(" TTL ({}, {})", ttl.column, ttl.duration_secs));
    }

    if table_meta.durability == TableDurability::Relaxed {
        create_sql.push_str(" DURABILITY = RELAXED");
    }

    if let Some(level) = table_meta.compression_level {
        create_sql.push_str(&format!(" COMPRESSION_LEVEL = {}", level));
    }

    for secondary_index in &table_meta.secondary_indexes {
        let cols = secondary_index.columns.join(", ");
        create_sql.push_str(&format!(
            ";\nCREATE INDEX {} ON {} ({}) TYPE {}",
            secondary_index.name,
            table,
            cols,
            secondary_index_type_display(secondary_index.index_type).to_ascii_lowercase()
        ));
    }

    let schema = Schema::new(vec![
        ColumnDef::new("Table", DataType::VarChar),
        ColumnDef::new("Create Table", DataType::VarChar),
    ]);

    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some(table.to_string())))?;
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some(create_sql)))?;

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub(super) fn get_existing_catalog<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    db: &str,
) -> Result<&'a mut DatabaseCatalog> {
    catalogs
        .get_mut(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))
}

use crate::sql::parser::ast::AlterUserOperation;

pub fn exec_alter_user(
    engine: &Engine,
    name: &str,
    operations: &[AlterUserOperation],
) -> Result<ExecResult> {
    for op in operations {
        match op {
            AlterUserOperation::SetPassword { password } => {
                engine.auth_manager().alter_user_password(name, password)?;
            }
            AlterUserOperation::Lock => {
                engine.auth_manager().lock_user(name)?;
            }
            AlterUserOperation::Unlock => {
                engine.auth_manager().unlock_user(name)?;
            }
        }
    }
    tracing::info!(
        user = name,
        operations_count = operations.len(),
        "ALTER USER executed"
    );
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_show_users(engine: &Engine) -> Result<ExecResult> {
    let users = engine.auth_manager().list_users();
    let schema = Schema::new(vec![ColumnDef::new("User", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema.clone());
    for user in users {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(user)))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_roles(engine: &Engine) -> Result<ExecResult> {
    let roles = engine.auth_manager().list_roles();
    let schema = Schema::new(vec![ColumnDef::new("Role", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema.clone());
    for role in roles {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(role)))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_grants(engine: &Engine, user: &Option<String>) -> Result<ExecResult> {
    let user_name = user.clone().unwrap_or_else(|| "root".to_string());
    let grants = engine.auth_manager().show_grants(&user_name);
    let schema = Schema::new(vec![ColumnDef::new(
        &format!("Grants for {}", user_name),
        DataType::VarChar,
    )]);
    let mut batch = RecordBatch::new(schema.clone());
    for grant in grants {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(grant)))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_partitions(
    engine: &Engine,
    db: &str,
    table: &Option<String>,
) -> Result<ExecResult> {
    // Only table names are collected under the catalog read guard; the
    // per-partition scan touches the segment footer of every sealed file, and
    // holding the global catalog lock across that would stall writers on a
    // large table for the whole SHOW.
    let tables: Vec<String> = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
        match table {
            Some(table_name) => {
                if catalog
                    .get_table(table_name)
                    .is_some_and(|meta| meta.is_partitioned())
                {
                    vec![table_name.clone()]
                } else {
                    Vec::new()
                }
            }
            None => catalog
                .table_names()
                .into_iter()
                .filter(|name| {
                    catalog
                        .get_table(name)
                        .is_some_and(|meta| meta.is_partitioned())
                })
                .map(|name| name.to_string())
                .collect(),
        }
    };

    let schema = Schema::new(vec![
        ColumnDef::new("Table", DataType::VarChar),
        ColumnDef::new("Partition", DataType::VarChar),
        ColumnDef::new("Rows", DataType::Int64),
        ColumnDef::new("Segments", DataType::Int64),
    ]);

    let mut batch = RecordBatch::new(schema.clone());

    for table_name in tables {
        // The partition list is derived from disk plus the active writers —
        // the same view OPTIMIZE TABLE matches against — so the names shown
        // are exactly the names ON PARTITION accepts.
        let mut partitions: std::collections::BTreeMap<String, (i64, i64)> = Default::default();
        let table_dir = engine.data_dir().join(db).join(&table_name);
        for path in engine
            .segment_mgr()
            .discover_inactive_segments(db, &table_name, &table_dir)?
        {
            let Some(key) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(crate::storage::partition::partition::parse_partition_key)
            else {
                continue;
            };
            let entry = partitions.entry(key).or_insert((0, 0));
            entry.1 += 1;
            match crate::storage::segment::meta::read_embedded_segment_meta(&path) {
                Ok(meta) => {
                    entry.0 = entry.0.saturating_add(
                        meta.row_count.saturating_sub(meta.deleted_row_count) as i64,
                    );
                }
                Err(error) => tracing::warn!(
                    db,
                    table = %table_name,
                    file = %path.display(),
                    error = %error,
                    "SHOW PARTITIONS cannot read segment footer"
                ),
            }
        }
        for (key, rows) in engine
            .segment_mgr()
            .active_partition_live_rows(db, &table_name)
        {
            // Empty key is the unpartitioned bucket (NULL / unparseable
            // partition values); it has no name OPTIMIZE could target.
            if key.is_empty() {
                continue;
            }
            let entry = partitions.entry(key).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(rows as i64);
            entry.1 += 1;
        }
        // Rows still in memtables land in those partitions on the next flush,
        // so they count towards the partition they will belong to.
        for (key, rows) in engine
            .batch_writer()
            .memtable_manager()
            .flushable_partition_rows(db, &table_name)
        {
            if key.is_empty() {
                continue;
            }
            let entry = partitions.entry(key).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(rows as i64);
        }

        for (key, (rows, segments)) in partitions {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(table_name.clone())))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(key)))?;
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::Int64(Some(rows)))?;
            batch
                .column_mut(3)
                .unwrap()
                .push(&ScalarValue::Int64(Some(segments)))?;
        }
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_stats(engine: &Engine, db: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let schema = Schema::new(vec![
        ColumnDef::new("Table", DataType::VarChar),
        ColumnDef::new("Rows", DataType::Int64),
        ColumnDef::new("Size", DataType::Int64),
        ColumnDef::new("LastUpdated", DataType::Timestamp),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    if let Some(catalog) = catalogs.get(db) {
        for table_name in catalog.table_names() {
            if let Some(meta) = catalog.get_table(table_name) {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(table_name.to_string())))?;
                batch
                    .column_mut(1)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(meta.row_count)))?;
                batch
                    .column_mut(2)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(0)))?;
                batch
                    .column_mut(3)
                    .unwrap()
                    .push(&ScalarValue::Timestamp(Some(0)))?;
            }
        }
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_engines(_engine: &Engine) -> Result<ExecResult> {
    let schema = Schema::new(vec![
        ColumnDef::new("Engine", DataType::VarChar),
        ColumnDef::new("Support", DataType::VarChar),
        ColumnDef::new("Comment", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("COLUMNAR".to_string())))?;
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("DEFAULT".to_string())))?;
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::VarChar(Some(
            "Columnar storage engine".to_string(),
        )))?;
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

/// `SHOW MEMORY`: resident-memory accounting of the engine's long-lived
/// structures. Every row is a point-in-time counter; none of these were
/// observable before, which made server-side RSS growth impossible to
/// attribute from outside the process.
pub fn exec_show_memory(engine: &Engine) -> Result<ExecResult> {
    let mut rows: Vec<(&'static str, String, u64)> = Vec::new();

    let hot_mgr = engine.hot_store_manager();
    let snapshot = hot_mgr.memory_snapshot();
    rows.push(("hot_store", "stores".into(), snapshot.len() as u64));
    rows.push((
        "hot_store",
        "stale_tables".into(),
        hot_mgr.stale_table_count() as u64,
    ));
    for ((db, table), bytes) in snapshot {
        rows.push(("hot_store", format!("{db}.{table}"), bytes));
    }

    match engine.rep_log() {
        Some(rep_log) => {
            let (ring_events, ring_bytes) = rep_log.ring_stats();
            rows.push(("replication", "enabled".into(), 1));
            rows.push(("replication", "ring_events".into(), ring_events as u64));
            rows.push(("replication", "ring_bytes_est".into(), ring_bytes));
            rows.push((
                "replication",
                "ring_max_bytes".into(),
                rep_log.ring_byte_limit(),
            ));
            rows.push((
                "replication",
                "sessions".into(),
                rep_log.session_count() as u64,
            ));
        }
        None => {
            rows.push(("replication", "enabled".into(), 0));
            // The ceiling is config-sourced, so it stays visible even where
            // no RepLog instance exists (standby / replication off).
            rows.push((
                "replication",
                "ring_max_bytes".into(),
                crate::common::config::replication_settings().ring_max_bytes,
            ));
        }
    }

    let (cached_entries, cached_bytes, budget_bytes) = engine.segment_mgr().reader_cache_stats();
    rows.push((
        "segment_readers",
        "cached_entries".into(),
        cached_entries as u64,
    ));
    rows.push(("segment_readers", "cached_heap_bytes".into(), cached_bytes));
    rows.push(("segment_readers", "cache_budget_bytes".into(), budget_bytes));
    let (mutable_entries, mutable_bytes) = engine.segment_mgr().mutable_reader_cache_stats();
    rows.push((
        "segment_readers",
        "mutable_entries".into(),
        mutable_entries as u64,
    ));
    rows.push((
        "segment_readers",
        "mutable_heap_bytes".into(),
        mutable_bytes,
    ));
    rows.push((
        "segment_readers",
        "mutable_budget_bytes".into(),
        crate::common::config::mutable_reader_cache_budget_bytes(),
    ));

    // The segment-metadata cache. Entries are the compact footer index
    // (~12 B per row group) plus the footer bytes, not a parsed object graph.
    // It was once the only metadata cache missing from this report, which is
    // exactly how it grew to 25 GiB unnoticed.
    let (meta_entries, meta_bytes, meta_budget) = engine.segment_mgr().segment_meta_cache_stats();
    rows.push((
        "segment_meta",
        "cached_entries".into(),
        meta_entries as u64,
    ));
    rows.push(("segment_meta", "cached_heap_bytes".into(), meta_bytes));
    rows.push(("segment_meta", "cache_budget_bytes".into(), meta_budget));
    rows.push((
        "caches",
        "charged_heap_bytes_total".into(),
        cached_bytes + mutable_bytes + meta_bytes,
    ));

    rows.push((
        "memtable",
        "estimated_bytes".into(),
        engine.memtable_manager().total_estimated_memory_bytes(),
    ));

    let schema = Schema::new(vec![
        ColumnDef::new("Component", DataType::VarChar),
        ColumnDef::new("Name", DataType::VarChar),
        ColumnDef::new("Value", DataType::Int64),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    for (component, name, value) in rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(component.to_string())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name)))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value as i64)))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

/// `SHOW VERSION` — one row carrying the full build identity (version +
/// git commit + build time). Exists so a deployed server's exact source
/// revision is always queryable; before this, pinning a running binary to a
/// source state required filesystem access that remote hosts often deny.
pub fn exec_show_version() -> Result<ExecResult> {
    let schema = Schema::new(vec![ColumnDef::new("Version", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some(
            crate::version::get_full_version(),
        )))?;
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_processlist(engine: &Engine, full: bool) -> Result<ExecResult> {
    let entries = engine.query_registry().list();
    let schema = Schema::new(vec![
        ColumnDef::new("Id", DataType::Int64),
        ColumnDef::new("User", DataType::VarChar),
        ColumnDef::new("Database", DataType::VarChar),
        ColumnDef::new("Command", DataType::VarChar),
        ColumnDef::new("Time", DataType::Int64),
        ColumnDef::new("State", DataType::VarChar),
        ColumnDef::new("Info", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    for entry in entries {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(entry.query_id as i64)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(entry.username.clone())))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(entry.database.clone())))?;
        batch
            .column_mut(3)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("Query".to_string())))?;
        batch
            .column_mut(4)
            .unwrap()
            .push(&ScalarValue::Int64(Some(entry.elapsed_secs as i64)))?;
        let state = if entry.cancelled {
            "Killed".to_string()
        } else {
            match entry.phase {
                crate::common::query_cancel::QueryPhase::Planning => "Planning".to_string(),
                crate::common::query_cancel::QueryPhase::Executing => "Executing".to_string(),
                crate::common::query_cancel::QueryPhase::Committing => "Committing".to_string(),
            }
        };
        batch
            .column_mut(5)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(state)))?;
        batch
            .column_mut(6)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(processlist_info(
                &entry.sql, full,
            ))))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

/// The `Info` column: the retained statement snippet in full, or truncated
/// to 100 characters like MySQL's default processlist.
fn processlist_info(sql: &str, full: bool) -> String {
    if full || sql.len() <= 100 {
        return sql.to_string();
    }
    let mut truncated: String = sql.chars().take(100).collect();
    truncated.push_str("...");
    truncated
}

pub fn exec_kill_query(engine: &Engine, killer: &str, query_id: u64) -> Result<ExecResult> {
    let killed = engine.query_registry().kill(
        query_id,
        format!("query {} was killed by user '{}'", query_id, killer),
    );
    if !killed {
        return Err(TaibaiError::Sql(format!(
            "query {} is not running",
            query_id
        )));
    }
    tracing::info!(query_id, killer, "KILL QUERY dispatched");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_flush_table(engine: &Engine, db: &str, table: &Option<String>) -> Result<ExecResult> {
    if let Some(table_name) = table {
        engine.flush_table_buffers(db, table_name)?;
        tracing::info!(db, table = table_name, "FLUSH TABLE executed");
    } else {
        engine.flush_all_buffers()?;
        tracing::info!("FLUSH TABLES executed");
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_optimize_table(
    engine: &Engine,
    db: &str,
    table: &str,
    partition: Option<&str>,
) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    if let Some(partition) = partition {
        for candidate in crate::storage::core::compactor::partition_key_candidates(partition) {
            engine.flush_partition_buffers(db, table, &candidate)?;
        }
    } else {
        engine.flush_table_buffers(db, table)?;
    }

    let compactor = crate::storage::core::compactor::Compactor::new(
        engine.segment_mgr().clone(),
        engine.data_dir().to_path_buf(),
    );

    let result = if let Some(partition) = partition {
        compactor.optimize_partition(db, table, &table_meta, partition)
    } else {
        compactor.optimize_table(db, table, &table_meta)
    };
    let stats = compactor.committed_stats();

    // Compaction frees segment path numbers the next writer may reuse; the
    // sum-cache key cannot tell incarnations apart (see drop table).
    crate::sql::execution::scan::aggregate_meta::invalidate_segment_sum_cache_under(
        &engine.data_dir().join(db).join(table),
    );

    // TTL-expired rows physically removed by the rewrite were never
    // subtracted from the catalog by a DELETE; correct row_count first.
    if stats.rows_dropped_ttl > 0 {
        let mut catalogs = engine.catalogs().write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = (meta.row_count - stats.rows_dropped_ttl as i64).max(0);
                let meta_dir = engine.data_dir().join("meta");
                let _ = catalog.save(&meta_dir);
            }
        }
    }

    // Full-table maintenance preserves eager index rebuilding. A partition
    // operation refreshes on demand so its finishing phase does not take the
    // table gate for a scan of every historical partition.
    if (stats.segments_written > 0 || stats.rows_dropped > 0) && partition.is_some() {
        // Partition maintenance must not rebuild every index over all historical
        // partitions under the table gate. Epoch validation already rejects old
        // locations; invalidate the key cache and refresh snapshots on demand.
        engine
            .index_manager()
            .invalidate_primary_key_cache(db, table);
        let names = table_meta
            .unique_indexes
            .iter()
            .map(|i| i.name.clone())
            .collect::<Vec<_>>();
        engine
            .index_manager()
            .invalidate_table_unique_index_snapshots(db, table, &names);
    } else if stats.segments_written > 0 || stats.rows_dropped > 0 {
        let schema = Schema::new(table_meta.columns.clone());
        let rebuilt = crate::sql::execution::dml_insert::rebuild_table_location_indexes_two_phase(
            engine.index_manager(),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.data_dir(),
            db,
            table,
            &table_meta,
            &schema,
        );
        if let Err(error) = rebuilt {
            tracing::warn!(
                db,
                table,
                error = %error,
                "OPTIMIZE TABLE index auto-rebuild failed; falling back to snapshot invalidation"
            );
            let unique_index_names = table_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            engine
                .index_manager()
                .invalidate_table_unique_index_snapshots(db, table, &unique_index_names);
        }
    }

    result?;
    tracing::info!(
        db,
        table,
        table_full_name = %format!("{}.{}", db, table),
        partition_key = partition.unwrap_or("<all>"),
        rows_written = stats.rows_written,
        rows_dropped = stats.rows_dropped,
        "OPTIMIZE TABLE executed"
    );
    Ok(ExecResult::Ok {
        rows_affected: stats.rows_written,
    })
}

pub fn exec_analyze_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    engine.flush_table_buffers(db, table)?;
    let table_meta = {
        let catalogs = engine.catalogs().read().unwrap();
        catalogs
            .get(db)
            .and_then(|catalog| catalog.get_table(table))
            .cloned()
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
    };
    let table_dir = engine.data_dir().join(db).join(table);
    let composite_groups = analyze_composite_groups(&table_meta);
    // Statistics are advisory and the scan below is read-only over
    // immutable segments, so it does NOT take the wide mutation gate:
    // holding that gate for a full-table scan would freeze every
    // partition's INSERTs for the scan duration. The flush-visibility
    // read lock only keeps the scan from observing a memtable→segment
    // transition mid-flip.
    // Snapshot the segment list under the flush read lock (consistent with
    // the flush that just ran), then decode outside it: ANALYZE holds the
    // lease for the whole scan, so compaction defers physical deletion of
    // the listed files, while a long statistics pass cannot stall the flush
    // writer the way the previous lock-spanning-decode did.
    let snapshot = engine.segment_mgr().with_table_flush_read_lock(db, table, || {
        if !table_dir.exists() {
            return Ok(None);
        }
        crate::storage::segment::snapshot::snapshot_segments(&table_dir).map(Some)
    })?;
    let _lease = snapshot.as_ref();
    let paths = snapshot
        .as_ref()
        .map(|snapshot| snapshot.paths.clone())
        .unwrap_or_default();
    let (row_count, columns, composite_columns) = {
        let mut collectors = table_meta
            .columns
            .iter()
            .map(|_| AnalyzeColumnCollector::new())
            .collect::<Vec<_>>();
        let mut composite_collectors = composite_groups
            .iter()
            .map(|group| AnalyzeCompositeCollector::new(group.clone()))
            .collect::<Vec<_>>();
        let mut row_count = 0u64;
        for path in paths {
            let reader = engine.segment_mgr().open_reader_cached(&path)?;
            for row_group in 0..reader.num_row_groups() {
                let mut batch = reader.read_row_group(row_group)?;
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        batch,
                        reader.row_group_start_row(row_group),
                        reader.delete_bitmap(),
                    )?;
                }
                row_count = row_count.saturating_add(batch.num_rows() as u64);
                for (column, collector) in collectors.iter_mut().enumerate() {
                    if let Some(values) = batch.column(column) {
                        collector.update(values);
                    }
                }
                for collector in &mut composite_collectors {
                    collector.update(&batch);
                }
            }
        }
        let columns = table_meta
            .columns
            .iter()
            .zip(collectors)
            .map(|(column, collector)| (column.name.clone(), collector.finish()))
            .collect::<HashMap<_, _>>();
        let composite_columns = composite_collectors
            .into_iter()
            .map(AnalyzeCompositeCollector::finish)
            .collect();
        (row_count, columns, composite_columns)
    };

    let collected_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    {
        let mut catalogs = engine.catalogs().write().unwrap();
        let catalog = catalogs
            .get_mut(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
        let current = catalog
            .get_table_mut(table)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
        current.statistics = Some(crate::storage::catalog::schema::TableStatisticsMeta {
            row_count: row_count.min(i64::MAX as u64) as i64,
            schema_version: table_meta.schema_version,
            collected_at,
            columns,
            composite_columns,
        });
        catalog.save(&engine.data_dir().join("meta"))?;
    }
    tracing::info!(db, table, row_count, "ANALYZE TABLE completed");
    Ok(ExecResult::Ok {
        rows_affected: row_count,
    })
}

const ANALYZE_HLL_PRECISION: u32 = 12;
const ANALYZE_HLL_REGISTERS: usize = 1 << ANALYZE_HLL_PRECISION;
pub(crate) const ANALYZE_HISTOGRAM_SAMPLE_SIZE: usize = 2048;
pub(crate) const ANALYZE_HISTOGRAM_BUCKETS: usize = 32;
const ANALYZE_HISTOGRAM_MAX_VALUE_BYTES: usize = 1024;
const ANALYZE_COMPOSITE_GROUP_LIMIT: usize = 32;

#[derive(Clone)]
struct AnalyzeCompositeGroup {
    columns: Vec<String>,
    indices: Vec<usize>,
    unique: bool,
}

fn analyze_composite_groups(table_meta: &TableMeta) -> Vec<AnalyzeCompositeGroup> {
    let mut candidates = Vec::new();
    let primary_key = table_meta
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if primary_key.len() > 1 {
        candidates.push((primary_key, true));
    }
    for index in &table_meta.unique_indexes {
        if index.columns.len() > 1 {
            candidates.push((index.columns.clone(), true));
        }
    }
    if table_meta.cluster_key.len() > 1 {
        candidates.push((table_meta.cluster_key.clone(), false));
    }

    let mut seen = HashSet::new();
    candidates
        .into_iter()
        .filter_map(|(columns, unique)| {
            let key = columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("\0");
            if !seen.insert(key) {
                return None;
            }
            let indices = columns
                .iter()
                .map(|column| {
                    table_meta
                        .columns
                        .iter()
                        .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(AnalyzeCompositeGroup {
                columns,
                indices,
                unique,
            })
        })
        .take(ANALYZE_COMPOSITE_GROUP_LIMIT)
        .collect()
}

struct AnalyzeCompositeCollector {
    group: AnalyzeCompositeGroup,
    null_count: u64,
    registers: Vec<u8>,
}

impl AnalyzeCompositeCollector {
    pub(crate) fn new(group: AnalyzeCompositeGroup) -> Self {
        Self {
            group,
            null_count: 0,
            registers: vec![0; ANALYZE_HLL_REGISTERS],
        }
    }

    fn update(&mut self, batch: &RecordBatch) {
        for row in 0..batch.num_rows() {
            let values = self
                .group
                .indices
                .iter()
                .filter_map(|column| batch.scalar(row, *column))
                .collect::<Vec<_>>();
            if values.len() != self.group.indices.len() || values.iter().any(ScalarValue::is_null) {
                self.null_count = self.null_count.saturating_add(1);
                continue;
            }
            let mut hasher = std::hash::DefaultHasher::new();
            for value in &values {
                std::hash::Hash::hash(value, &mut hasher);
            }
            update_hll_registers(&mut self.registers, std::hash::Hasher::finish(&hasher));
        }
    }

    pub(crate) fn finish(self) -> crate::storage::catalog::schema::CompositeStatisticsMeta {
        crate::storage::catalog::schema::CompositeStatisticsMeta {
            columns: self.group.columns,
            distinct_count: estimate_hll(&self.registers).min(i64::MAX as u64) as i64,
            null_count: self.null_count.min(i64::MAX as u64) as i64,
            unique: self.group.unique,
        }
    }
}

pub(crate) struct AnalyzeSample {
    priority: u64,
    value: ScalarValue,
}

impl PartialEq for AnalyzeSample {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for AnalyzeSample {}

impl PartialOrd for AnalyzeSample {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AnalyzeSample {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

pub(crate) struct AnalyzeColumnCollector {
    pub(crate) null_count: u64,
    pub(crate) non_null_count: u64,
    pub(crate) min_value: Option<ScalarValue>,
    pub(crate) max_value: Option<ScalarValue>,
    registers: Vec<u8>,
    pub(crate) samples: std::collections::BinaryHeap<AnalyzeSample>,
}

impl AnalyzeColumnCollector {
    pub(crate) fn new() -> Self {
        Self {
            null_count: 0,
            non_null_count: 0,
            min_value: None,
            max_value: None,
            registers: vec![0; ANALYZE_HLL_REGISTERS],
            samples: std::collections::BinaryHeap::new(),
        }
    }

    fn update(&mut self, column: &crate::types::array::ColumnArray) {
        for row in 0..column.len() {
            let value = column.scalar(row);
            if value.is_null() {
                self.null_count = self.null_count.saturating_add(1);
                continue;
            }
            self.non_null_count = self.non_null_count.saturating_add(1);
            if self.min_value.as_ref().is_none_or(|current| {
                crate::sql::execution::operators::sort::compare_scalars_null_last(&value, current)
                    .is_lt()
            }) {
                self.min_value = Some(value.clone());
            }
            if self.max_value.as_ref().is_none_or(|current| {
                crate::sql::execution::operators::sort::compare_scalars_null_last(&value, current)
                    .is_gt()
            }) {
                self.max_value = Some(value.clone());
            }
            let mut hasher = std::hash::DefaultHasher::new();
            std::hash::Hash::hash(&value, &mut hasher);
            let hash = std::hash::Hasher::finish(&hasher);
            update_hll_registers(&mut self.registers, hash);
            self.sample_value(value);
        }
    }

    pub(crate) fn sample_value(&mut self, value: ScalarValue) {
        if histogram_value_size(&value) > ANALYZE_HISTOGRAM_MAX_VALUE_BYTES {
            return;
        }
        let mut hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&value, &mut hasher);
        std::hash::Hash::hash(&self.non_null_count, &mut hasher);
        let sample = AnalyzeSample {
            priority: std::hash::Hasher::finish(&hasher),
            value,
        };
        if self.samples.len() < ANALYZE_HISTOGRAM_SAMPLE_SIZE {
            self.samples.push(sample);
        } else if self
            .samples
            .peek()
            .is_some_and(|largest| sample.priority < largest.priority)
        {
            self.samples.pop();
            self.samples.push(sample);
        }
    }

    pub(crate) fn finish(self) -> crate::storage::catalog::schema::ColumnStatisticsMeta {
        crate::storage::catalog::schema::ColumnStatisticsMeta {
            distinct_count: estimate_hll(&self.registers).min(i64::MAX as u64) as i64,
            null_count: self.null_count.min(i64::MAX as u64) as i64,
            min_value: self.min_value,
            max_value: self.max_value,
            histogram: build_histogram(self.samples),
        }
    }
}

fn update_hll_registers(registers: &mut [u8], hash: u64) {
    let register = (hash as usize) & (ANALYZE_HLL_REGISTERS - 1);
    let remaining = hash >> ANALYZE_HLL_PRECISION;
    let rank = remaining
        .leading_zeros()
        .saturating_sub(ANALYZE_HLL_PRECISION)
        .saturating_add(1) as u8;
    registers[register] = registers[register].max(rank);
}

fn histogram_value_size(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::VarChar(Some(value)) => value.len(),
        ScalarValue::Blob(Some(value)) => value.len(),
        ScalarValue::Vector(Some(value)) => value.len().saturating_mul(std::mem::size_of::<f32>()),
        _ => std::mem::size_of::<ScalarValue>(),
    }
}

fn build_histogram(
    samples: std::collections::BinaryHeap<AnalyzeSample>,
) -> Vec<crate::storage::catalog::schema::HistogramBucketMeta> {
    let mut values = samples
        .into_iter()
        .map(|sample| sample.value)
        .collect::<Vec<_>>();
    values.sort_by(crate::sql::execution::operators::sort::compare_scalars_null_last);
    if values.is_empty() {
        return Vec::new();
    }
    let target = values.len().div_ceil(ANALYZE_HISTOGRAM_BUCKETS).max(1);
    let mut buckets = Vec::new();
    let mut start = 0usize;
    while start < values.len() {
        let mut end = (start + target).min(values.len());
        while end < values.len()
            && crate::sql::execution::operators::sort::compare_scalars_null_last(
                &values[end - 1],
                &values[end],
            )
            .is_eq()
        {
            end += 1;
        }
        buckets.push(crate::storage::catalog::schema::HistogramBucketMeta {
            lower_bound: values[start].clone(),
            upper_bound: values[end - 1].clone(),
            sample_count: (end - start).min(u32::MAX as usize) as u32,
        });
        start = end;
    }
    buckets
}

fn estimate_hll(registers: &[u8]) -> u64 {
    let m = registers.len() as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let harmonic = registers
        .iter()
        .map(|register| 2f64.powi(-(*register as i32)))
        .sum::<f64>();
    let raw = alpha * m * m / harmonic.max(f64::MIN_POSITIVE);
    let zero_registers = registers.iter().filter(|register| **register == 0).count();
    let corrected = if raw <= 2.5 * m && zero_registers > 0 {
        m * (m / zero_registers as f64).ln()
    } else {
        raw
    };
    corrected.round().max(0.0) as u64
}

/// `SHOW WORKLOAD`: the admission, canary and fast-path telemetry that made
/// the 2026-11-05 incident invisible until clients timed out — queue
/// pressure is meant to be watched here, not scraped from logs after the
/// fact. Canary status codes: 0 ok, 1 failed, 2 timed out, 3 never ran.
pub fn exec_show_workload(engine: &Engine) -> Result<ExecResult> {
    let mut rows: Vec<(&'static str, String, u64)> = Vec::new();

    for (pool, capacity, acquired, rejected) in crate::net::server::admission::snapshot() {
        rows.push(("admission", format!("{pool}.capacity"), capacity));
        rows.push(("admission", format!("{pool}.acquired_total"), acquired));
        rows.push(("admission", format!("{pool}.rejected_overload_total"), rejected));
    }

    rows.push((
        "queries",
        "running".into(),
        engine.query_registry().list().len() as u64,
    ));

    let (latency_ms, status, failures) = crate::common::watchdog::canary_snapshot();
    if latency_ms != u64::MAX {
        rows.push(("canary", "last_latency_ms".into(), latency_ms));
    }
    rows.push(("canary", "last_status".into(), status as u64));
    rows.push((
        "canary",
        "consecutive_failures".into(),
        failures as u64,
    ));

    rows.push((
        "pools",
        "rayon_global_threads".into(),
        rayon::current_num_threads() as u64,
    ));

    rows.push((
        "fast_path",
        "metadata_answers_total".into(),
        crate::sql::execution::scan::fast_path::metadata_answers_total(),
    ));

    let schema = Schema::new(vec![
        ColumnDef::new("Component", DataType::VarChar),
        ColumnDef::new("Name", DataType::VarChar),
        ColumnDef::new("Value", DataType::Int64),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    for (component, name, value) in rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(component.to_string())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name)))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value as i64)))?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}
