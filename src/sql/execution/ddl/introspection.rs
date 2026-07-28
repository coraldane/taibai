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
    for name in catalogs.keys() {
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

    if table_meta.hot_time_column.is_some() {
        let time_col = table_meta.hot_time_column.as_ref().unwrap();
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

fn get_existing_catalog<'a>(
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
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let schema = Schema::new(vec![
        ColumnDef::new("Table", DataType::VarChar),
        ColumnDef::new("Partition", DataType::VarChar),
        ColumnDef::new("Rows", DataType::Int64),
        ColumnDef::new("Segments", DataType::Int64),
    ]);

    let mut batch = RecordBatch::new(schema.clone());

    if let Some(table_name) = table {
        let table_name_owned: String = table_name.clone();
        if let Some(meta) = catalog.get_table(&table_name_owned) {
            for partition in &meta.partitions {
                let col0 = batch.column_mut(0).unwrap();
                col0.push(&ScalarValue::VarChar(Some(table_name_owned.clone())))?;
                let col1 = batch.column_mut(1).unwrap();
                col1.push(&ScalarValue::VarChar(Some(partition.key.clone())))?;
                let col2 = batch.column_mut(2).unwrap();
                col2.push(&ScalarValue::Int64(Some(partition.row_count)))?;
                let col3 = batch.column_mut(3).unwrap();
                col3.push(&ScalarValue::Int64(Some(partition.segment_count as i64)))?;
            }
        }
    } else {
        for table_name in catalog.table_names() {
            let table_name_owned: String = table_name.to_string();
            if let Some(meta) = catalog.get_table(&table_name_owned) {
                for partition in &meta.partitions {
                    let col0 = batch.column_mut(0).unwrap();
                    col0.push(&ScalarValue::VarChar(Some(table_name_owned.clone())))?;
                    let col1 = batch.column_mut(1).unwrap();
                    col1.push(&ScalarValue::VarChar(Some(partition.key.clone())))?;
                    let col2 = batch.column_mut(2).unwrap();
                    col2.push(&ScalarValue::Int64(Some(partition.row_count)))?;
                    let col3 = batch.column_mut(3).unwrap();
                    col3.push(&ScalarValue::Int64(Some(partition.segment_count as i64)))?;
                }
            }
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

pub fn exec_show_processlist(_engine: &Engine, _full: bool) -> Result<ExecResult> {
    let schema = Schema::new(vec![
        ColumnDef::new("Id", DataType::Int64),
        ColumnDef::new("User", DataType::VarChar),
        ColumnDef::new("Database", DataType::VarChar),
        ColumnDef::new("Command", DataType::VarChar),
        ColumnDef::new("Time", DataType::Int64),
        ColumnDef::new("State", DataType::VarChar),
        ColumnDef::new("Info", DataType::VarChar),
    ]);
    let batch = RecordBatch::new(schema.clone());
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
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

    engine.flush_all_buffers()?;

    let compactor = crate::storage::core::compactor::Compactor::new(
        engine.segment_mgr().clone(),
        engine.data_dir().to_path_buf(),
    );

    let stats = if let Some(partition) = partition {
        compactor.optimize_partition(db, table, &table_meta, partition)?
    } else {
        compactor.optimize_table(db, table, &table_meta)?
    };

    engine.index_manager().mark_index_stale(db, table);

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
    let (row_count, columns, composite_columns) =
        engine
            .segment_mgr()
            .with_table_mutation_lock(db, table, || {
                let paths =
                    crate::storage::segment_manager::SegmentManager::discover_segments(&table_dir)?;
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
                                &batch,
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
                Ok((row_count, columns, composite_columns))
            })?;

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
const ANALYZE_HISTOGRAM_SAMPLE_SIZE: usize = 2048;
const ANALYZE_HISTOGRAM_BUCKETS: usize = 32;
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
    fn new(group: AnalyzeCompositeGroup) -> Self {
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

    fn finish(self) -> crate::storage::catalog::schema::CompositeStatisticsMeta {
        crate::storage::catalog::schema::CompositeStatisticsMeta {
            columns: self.group.columns,
            distinct_count: estimate_hll(&self.registers).min(i64::MAX as u64) as i64,
            null_count: self.null_count.min(i64::MAX as u64) as i64,
            unique: self.group.unique,
        }
    }
}

struct AnalyzeSample {
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

struct AnalyzeColumnCollector {
    null_count: u64,
    non_null_count: u64,
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
    registers: Vec<u8>,
    samples: std::collections::BinaryHeap<AnalyzeSample>,
}

impl AnalyzeColumnCollector {
    fn new() -> Self {
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
                crate::sql::execution::operators::sort::compare_scalars(&value, current).is_lt()
            }) {
                self.min_value = Some(value.clone());
            }
            if self.max_value.as_ref().is_none_or(|current| {
                crate::sql::execution::operators::sort::compare_scalars(&value, current).is_gt()
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

    fn sample_value(&mut self, value: ScalarValue) {
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

    fn finish(self) -> crate::storage::catalog::schema::ColumnStatisticsMeta {
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
    values.sort_by(crate::sql::execution::operators::sort::compare_scalars);
    if values.is_empty() {
        return Vec::new();
    }
    let target = values.len().div_ceil(ANALYZE_HISTOGRAM_BUCKETS).max(1);
    let mut buckets = Vec::new();
    let mut start = 0usize;
    while start < values.len() {
        let mut end = (start + target).min(values.len());
        while end < values.len()
            && crate::sql::execution::operators::sort::compare_scalars(
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

#[cfg(test)]
mod analyze_histogram_tests {
    use super::{AnalyzeColumnCollector, ANALYZE_HISTOGRAM_BUCKETS, ANALYZE_HISTOGRAM_SAMPLE_SIZE};
    use crate::types::scalar::ScalarValue;

    #[test]
    fn histogram_sampling_and_bucket_count_are_bounded() {
        let mut collector = AnalyzeColumnCollector::new();
        for value in 0..10_000i64 {
            collector.non_null_count += 1;
            collector.sample_value(ScalarValue::Int64(Some(value)));
        }
        assert_eq!(collector.samples.len(), ANALYZE_HISTOGRAM_SAMPLE_SIZE);

        let statistics = collector.finish();
        assert!(statistics.histogram.len() <= ANALYZE_HISTOGRAM_BUCKETS);
        assert_eq!(
            statistics
                .histogram
                .iter()
                .map(|bucket| bucket.sample_count as usize)
                .sum::<usize>(),
            ANALYZE_HISTOGRAM_SAMPLE_SIZE
        );
    }
}
