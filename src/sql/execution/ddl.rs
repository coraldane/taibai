use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::catalog::auth::{
    AuthManager, Permission, PermissionObjectType, PermissionType,
};
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::{DatabaseCatalog, PartitionType};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{
    AlterOperation, ColumnDefAst, GrantObject, PartitionDef, PartitionStrategy, Privilege,
};

pub fn exec_create_table(
    engine: &Engine,
    db: &str,
    name: &str,
    columns: &[ColumnDefAst],
    if_not_exists: bool,
    cluster_by: &Option<Vec<String>>,
    partition_by: &Option<PartitionDef>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_or_create_catalog(&mut catalogs, engine.data_dir(), db)?;

    if catalog.get_table(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(MonaxError::Sql(format!(
            "table '{}.{}' already exists",
            db, name
        )));
    }

    let col_defs: Vec<ColumnDef> = columns
        .iter()
        .map(|c| {
            ColumnDef::new(&c.name, c.data_type)
                .with_nullable(c.nullable)
                .with_scale(c.scale)
                .with_precision(c.precision)
        })
        .collect();

    catalog.create_table(name, col_defs)?;

    if let Some(meta) = catalog.get_table_mut(name) {
        if let Some(ref keys) = cluster_by {
            meta.cluster_key = keys.clone();
        }
        if let Some(ref pdef) = partition_by {
            meta.partition_key = pdef.column.clone();
            meta.partition_type = match pdef.strategy {
                PartitionStrategy::Day => PartitionType::Day,
                PartitionStrategy::Year => PartitionType::Year,
                PartitionStrategy::Month => PartitionType::Month,
                PartitionStrategy::Value => PartitionType::Value,
                PartitionStrategy::Hash(buckets) => {
                    meta.partition_param = buckets;
                    PartitionType::Hash
                }
            };
            meta.update_partition_key_idx();
        }
    }

    let auto_inc_col = columns.iter().find(|c| c.auto_increment);
    if let Some(ai_col) = auto_inc_col {
        engine
            .auto_increment_manager()
            .register(db, name, &ai_col.name, 1)?;
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
        return Err(MonaxError::Sql(format!(
            "table '{}.{}' does not exist",
            db, name
        )));
    }

    catalog.drop_table(name)?;
    engine.auto_increment_manager().unregister(db, name);

    let table_dir = engine.data_dir().join(db).join(name);
    engine.segment_mgr().remove_wal(&table_dir);

    if table_dir.exists() {
        let _ = std::fs::remove_dir_all(&table_dir);
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_alter_table(
    engine: &Engine,
    db: &str,
    name: &str,
    operations: &[AlterOperation],
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = catalogs
        .get_mut(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    if catalog.get_table(name).is_none() {
        return Err(MonaxError::Sql(format!(
            "table '{}.{}' does not exist",
            db, name
        )));
    }

    for op in operations {
        match op {
            AlterOperation::AddColumn { column } => {
                let col_def = ColumnDef::new(&column.name, column.data_type)
                    .with_nullable(column.nullable)
                    .with_scale(column.scale)
                    .with_precision(column.precision);
                catalog.add_column(name, col_def)?;
            }
            AlterOperation::DropColumn { name: col_name } => {
                catalog.drop_column(name, col_name)?;
            }
            AlterOperation::RenameColumn { old_name, new_name } => {
                catalog.rename_column(name, old_name, new_name)?;
            }
            AlterOperation::RenameTable { new_name } => {
                let old_table_dir = engine.data_dir().join(db).join(name);
                let new_table_dir = engine.data_dir().join(db).join(new_name);
                catalog.rename_table(name, new_name)?;
                if old_table_dir.exists() && !new_table_dir.exists() {
                    std::fs::rename(&old_table_dir, &new_table_dir)?;
                }
            }
            AlterOperation::DropPartition { partition_key } => {
                let table_dir = engine.data_dir().join(db).join(name);
                if table_dir.exists() {
                    for entry in std::fs::read_dir(&table_dir)? {
                        if let Ok(e) = entry {
                            let file_name = e.file_name().to_string_lossy().to_string();
                            if file_name.starts_with(&format!("p{}", partition_key))
                                && file_name.ends_with(".seg")
                            {
                                let _ = std::fs::remove_file(e.path());
                            }
                        }
                    }
                }
                if let Some(meta) = catalog.get_table_mut(name) {
                    meta.partitions.retain(|p| p.key != *partition_key);
                }
            }
            AlterOperation::TruncatePartition { partition_key } => {
                let table_dir = engine.data_dir().join(db).join(name);
                if table_dir.exists() {
                    for entry in std::fs::read_dir(&table_dir)? {
                        if let Ok(e) = entry {
                            let file_name = e.file_name().to_string_lossy().to_string();
                            if file_name.starts_with(&format!("p{}", partition_key))
                                && file_name.ends_with(".seg")
                            {
                                let _ = std::fs::remove_file(e.path());
                            }
                        }
                    }
                }
                if let Some(meta) = catalog.get_table_mut(name) {
                    if let Some(part) = meta.partitions.iter_mut().find(|p| p.key == *partition_key)
                    {
                        part.row_count = 0;
                        part.segment_count = 0;
                    }
                }
            }
        }
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_truncate_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = catalogs
        .get_mut(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    if catalog.get_table(table).is_none() {
        return Err(MonaxError::Sql(format!(
            "table '{}.{}' does not exist",
            db, table
        )));
    }

    let table_dir = engine.data_dir().join(db).join(table);
    engine.segment_mgr().remove_wal(&table_dir);

    if table_dir.exists() {
        for entry in std::fs::read_dir(&table_dir)? {
            if let Ok(e) = entry {
                let path = e.path();
                if path.extension().map(|ext| ext == "seg").unwrap_or(false) {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }

    engine.auto_increment_manager().reset(db, table, 1)?;

    if let Some(meta) = catalog.get_table_mut(table) {
        meta.row_count = 0;
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_create_database(
    engine: &Engine,
    name: &str,
    if_not_exists: bool,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();

    if catalogs.contains_key(name) {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(MonaxError::Sql(format!(
            "database '{}' already exists",
            name
        )));
    }

    let db_dir = engine.data_dir().join(name);
    std::fs::create_dir_all(&db_dir)?;

    let meta_dir = engine.data_dir().join("meta");
    let catalog = DatabaseCatalog::load_or_create(&meta_dir, name)?;
    catalogs.insert(name.to_string(), catalog);

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_database(engine: &Engine, name: &str, if_exists: bool) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();

    if !catalogs.contains_key(name) {
        if if_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(MonaxError::Sql(format!(
            "database '{}' does not exist",
            name
        )));
    }

    if name == "default" {
        return Err(MonaxError::Sql("cannot drop default database".into()));
    }

    catalogs.remove(name);

    let db_dir = engine.data_dir().join(name);
    if db_dir.exists() {
        let _ = std::fs::remove_dir_all(&db_dir);
    }

    let meta_dir = engine.data_dir().join("meta");
    let catalog_file = meta_dir.join(format!("{}.json", name));
    if catalog_file.exists() {
        let _ = std::fs::remove_file(&catalog_file);
    }

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_create_user(engine: &Engine, name: &str, pwd: &str) -> Result<ExecResult> {
    engine.auth_manager().create_user(name, pwd)?;
    tracing::info!(user = name, "CREATE USER executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_create_role(engine: &Engine, name: &str) -> Result<ExecResult> {
    engine.auth_manager().create_role(name)?;
    tracing::info!(role = name, "CREATE ROLE executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_user(engine: &Engine, name: &str, if_exists: bool) -> Result<ExecResult> {
    engine.auth_manager().drop_user(name, if_exists)?;
    tracing::info!(user = name, if_exists, "DROP USER executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_role(engine: &Engine, name: &str, if_exists: bool) -> Result<ExecResult> {
    engine.auth_manager().drop_role(name, if_exists)?;
    tracing::info!(role = name, if_exists, "DROP ROLE executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_grant(
    engine: &Engine,
    privileges: &[Privilege],
    on: &Option<GrantObject>,
    to: &str,
) -> Result<ExecResult> {
    let perms: Vec<Permission> = privileges
        .iter()
        .map(|p| {
            let perm_type = match p {
                Privilege::All => PermissionType::All,
                Privilege::Select => PermissionType::Select,
                Privilege::Insert => PermissionType::Insert,
                Privilege::Update => PermissionType::Update,
                Privilege::Delete => PermissionType::Delete,
                Privilege::Create => PermissionType::Create,
                Privilege::Drop => PermissionType::Drop,
                Privilege::Alter => PermissionType::Alter,
                Privilege::Truncate => PermissionType::Truncate,
            };
            let (obj_type, obj_name) = match on {
                Some(GrantObject::Table(t)) => (PermissionObjectType::Table, t.clone()),
                Some(GrantObject::Database(d)) => (PermissionObjectType::Database, d.clone()),
                Some(GrantObject::AllTables) => (PermissionObjectType::Table, "*".to_string()),
                Some(GrantObject::AllDatabases) => {
                    (PermissionObjectType::Database, "*".to_string())
                }
                None => (PermissionObjectType::Global, "*".to_string()),
            };
            Permission {
                permission_type: perm_type,
                object_type: obj_type,
                object_name: obj_name,
            }
        })
        .collect();
    engine.auth_manager().grant(to, perms)?;
    tracing::info!(privileges_count = privileges.len(), to, "GRANT executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_revoke(
    engine: &Engine,
    privileges: &[Privilege],
    on: &Option<GrantObject>,
    from: &str,
) -> Result<ExecResult> {
    let perm_types: Vec<PermissionType> = privileges
        .iter()
        .map(|p| match p {
            Privilege::All => PermissionType::All,
            Privilege::Select => PermissionType::Select,
            Privilege::Insert => PermissionType::Insert,
            Privilege::Update => PermissionType::Update,
            Privilege::Delete => PermissionType::Delete,
            Privilege::Create => PermissionType::Create,
            Privilege::Drop => PermissionType::Drop,
            Privilege::Alter => PermissionType::Alter,
            Privilege::Truncate => PermissionType::Truncate,
        })
        .collect();
    let obj_name = match on {
        Some(GrantObject::Table(t)) => Some(t.as_str()),
        Some(GrantObject::Database(d)) => Some(d.as_str()),
        Some(GrantObject::AllTables) => Some("*"),
        Some(GrantObject::AllDatabases) => Some("*"),
        None => None,
    };
    engine.auth_manager().revoke(from, &perm_types, obj_name)?;
    tracing::info!(privileges_count = privileges.len(), from, "REVOKE executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_show_tables(engine: &Engine, db: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("table_name", DataType::VarChar)]);

    let mut batch = RecordBatch::new(schema.clone());
    if let Some(catalog) = catalogs.get(db) {
        for name in catalog.table_names() {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(name.to_string())))?;
        }
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
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

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
            .push(&ScalarValue::VarChar(Some(col.data_type.to_string())))?;
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

pub fn exec_show_indexes(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

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
            .push(&ScalarValue::VarChar(Some("BTREE".to_string())))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_show_create_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?;

    let mut create_sql = format!("CREATE TABLE {} (\n", table);
    for (i, col) in table_meta.columns.iter().enumerate() {
        let nullable = if col.nullable { "" } else { " NOT NULL" };
        if i < table_meta.columns.len() - 1 {
            create_sql.push_str(&format!("  {} {}{},\n", col.name, col.data_type, nullable));
        } else {
            create_sql.push_str(&format!("  {} {}{}\n", col.name, col.data_type, nullable));
        }
    }
    create_sql.push_str(")");

    if !table_meta.cluster_key.is_empty() {
        create_sql.push_str(&format!(
            " CLUSTER BY ({})",
            table_meta.cluster_key.join(", ")
        ));
    }

    if table_meta.is_partitioned() {
        let partition_str = match table_meta.partition_type {
            PartitionType::Day => format!(" PARTITION BY DAY({})", table_meta.partition_key),
            PartitionType::Year => format!(" PARTITION BY YEAR({})", table_meta.partition_key),
            PartitionType::Month => format!(" PARTITION BY MONTH({})", table_meta.partition_key),
            PartitionType::Value => format!(" PARTITION BY {}", table_meta.partition_key),
            PartitionType::Hash => format!(
                " PARTITION BY HASH({})({})",
                table_meta.partition_key, table_meta.partition_param
            ),
            PartitionType::None => String::new(),
        };
        create_sql.push_str(&partition_str);
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

use crate::sql::parser::ast::{AlterUserOperation, OrderByItem, Statement};

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
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

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
            if let Some(meta) = catalog.get_table(&table_name) {
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

pub fn exec_show_engines(engine: &Engine) -> Result<ExecResult> {
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

pub fn exec_show_processlist(engine: &Engine, full: bool) -> Result<ExecResult> {
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
        let table_dir = engine.data_dir().join(db).join(table_name);
        engine.segment_mgr().flush_all_active()?;
        tracing::info!(db, table = table_name, "FLUSH TABLE executed");
    } else {
        engine.flush_all_buffers()?;
        tracing::info!("FLUSH TABLES executed");
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_optimize_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    engine.flush_all_buffers()?;

    let compactor = crate::storage::core::compactor::Compactor::new(
        engine.segment_mgr().clone(),
        engine.data_dir().to_path_buf(),
    );

    compactor.optimize_table(db, table, &table_meta)?;

    tracing::info!(db, table, "OPTIMIZE TABLE executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}
