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
