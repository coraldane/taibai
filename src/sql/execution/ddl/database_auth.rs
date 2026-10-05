use super::*;

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
        return Err(TaibaiError::Sql(format!(
            "database '{}' already exists",
            name
        )));
    }

    let db_dir = engine.data_dir().join(name);
    std::fs::create_dir_all(&db_dir)?;

    let meta_dir = engine.data_dir().join("meta");
    let catalog = DatabaseCatalog::new(name);
    catalog.save(&meta_dir)?;
    catalogs.insert(name.to_string(), catalog);

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_database(engine: &Engine, name: &str, if_exists: bool) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();

    if !catalogs.contains_key(name) {
        let tombstone = engine
            .data_dir()
            .join(DATABASE_DROP_TOMBSTONE_DIR)
            .join(name);
        if tombstone.exists() {
            remove_dir_all_with_retry(&tombstone).map_err(|error| {
                TaibaiError::Storage(format!(
                    "database '{}' is absent from the catalog, but pending physical cleanup for '{}' failed: {}",
                    name,
                    tombstone.display(),
                    error
                ))
            })?;
            if let Some(root) = tombstone.parent() {
                let _ = std::fs::remove_dir(root);
            }
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        if if_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "database '{}' does not exist",
            name
        )));
    }

    if name == "default" {
        return Err(TaibaiError::Sql("cannot drop default database".into()));
    }

    let mut lock_attempts = 0u32;
    let (tombstone, table_indexes) = loop {
        if !catalogs.contains_key(name) {
            return Err(TaibaiError::Sql(format!(
                "database '{}' does not exist",
                name
            )));
        }

        let table_indexes = catalogs
            .get(name)
            .map(|catalog| {
                catalog
                    .tables
                    .iter()
                    .map(|(table_name, table_meta)| {
                        (
                            table_name.clone(),
                            table_meta
                                .unique_indexes
                                .iter()
                                .map(|index| index.name.clone())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let table_names = table_indexes
            .iter()
            .map(|(table_name, _)| table_name.clone())
            .collect::<Vec<_>>();
        let database_dir = engine.data_dir().join(name);
        let meta_dir = engine.data_dir().join("meta");
        let catalog_file = DatabaseCatalog::catalog_path(&meta_dir, name);
        let mut op = || {
            for table_name in &table_names {
                engine.batch_writer().drain_table_tail(name, table_name)?;
                engine
                    .segment_mgr()
                    .close_table_state_handles(name, table_name);
            }
            engine.segment_mgr().invalidate_readers_under(&database_dir);
            let tombstone = stage_database_directory(engine.data_dir(), name).map_err(|error| {
                TaibaiError::Storage(format!(
                    "failed to stage database '{}' for DROP: {}",
                    name, error
                ))
            })?;

            if let Err(error) = remove_file_with_retry(&catalog_file) {
                if let Some(tombstone) = tombstone.as_deref() {
                    restore_staged_database_directory(tombstone, &database_dir).map_err(
                        |restore_error| {
                            TaibaiError::Storage(format!(
                                "failed to remove catalog for database '{}': {}; additionally failed to restore '{}': {}",
                                name,
                                error,
                                tombstone.display(),
                                restore_error
                            ))
                        },
                    )?;
                }
                return Err(error.into());
            }

            catalogs.remove(name);
            engine.segment_mgr().remove_database_state(name);
            for (table_name, _) in &table_indexes {
                engine.batch_writer().clear_memtables(name, table_name);
            }
            Ok(tombstone)
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_database_mutation_locks(name, &table_names, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_database_mutation_locks(name, &table_names, &mut op)?
        };
        match attempt {
            Some(tombstone) => break (tombstone, table_indexes),
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db = name,
                        lock_attempts,
                        "drop database: mutation locks busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    };

    for (table_name, unique_index_names) in &table_indexes {
        engine.index_manager().remove_table_unique_index_snapshots(
            name,
            table_name,
            unique_index_names,
        );
    }
    engine.auto_increment_manager().unregister_database(name);
    drop(catalogs);

    if let Some(tombstone) = tombstone {
        if let Err(error) = remove_committed_operation_directory(&tombstone) {
            return Err(TaibaiError::Storage(format!(
                "database '{}' was removed from the catalog, but physical data cleanup failed for '{}': {}; cleanup will be retried when the engine restarts",
                name,
                tombstone.display(),
                error
            )));
        }
        if let Some(root) = tombstone.parent() {
            let _ = std::fs::remove_dir(root);
        }
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
    let mut perms = Vec::with_capacity(privileges.len());
    for p in privileges {
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
            Privilege::Subscribe => PermissionType::Subscribe,
            Privilege::Replication => PermissionType::Replication,
        };
        // REPLICATION authorizes reading the whole replication stream; the
        // only check site (REPL_HELLO) asks for the Global scope, so scoped
        // grants are rejected instead of being stored where they can never
        // match.
        let (obj_type, obj_name) = if *p == Privilege::Replication {
            if !is_global_grant_object(on) {
                return Err(TaibaiError::Sql(
                    "REPLICATION is a global privilege: grant it without an ON object \
                     (e.g. GRANT REPLICATION TO user)"
                        .into(),
                ));
            }
            (PermissionObjectType::Global, "*".to_string())
        } else {
            grant_object_scope(on)
        };
        perms.push(Permission {
            permission_type: perm_type,
            object_type: obj_type,
            object_name: obj_name,
        });
    }
    engine.auth_manager().grant(to, perms)?;
    tracing::info!(privileges_count = privileges.len(), to, "GRANT executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn grant_object_scope(on: &Option<GrantObject>) -> (PermissionObjectType, String) {
    match on {
        Some(GrantObject::Table(t)) => (PermissionObjectType::Table, t.clone()),
        Some(GrantObject::Database(d)) => (PermissionObjectType::Database, d.clone()),
        Some(GrantObject::AllTables) => (PermissionObjectType::Table, "*".to_string()),
        Some(GrantObject::AllDatabases) => (PermissionObjectType::Database, "*".to_string()),
        None => (PermissionObjectType::Global, "*".to_string()),
    }
}

fn is_global_grant_object(on: &Option<GrantObject>) -> bool {
    match on {
        None | Some(GrantObject::AllTables) | Some(GrantObject::AllDatabases) => true,
        Some(GrantObject::Table(t)) => t == "*",
        Some(GrantObject::Database(_)) => false,
    }
}

pub fn exec_revoke(
    engine: &Engine,
    privileges: &[Privilege],
    on: &Option<GrantObject>,
    from: &str,
) -> Result<ExecResult> {
    let mut perm_types = Vec::with_capacity(privileges.len());
    let mut global_only = false;
    for p in privileges {
        if *p == Privilege::Replication {
            if !is_global_grant_object(on) {
                return Err(TaibaiError::Sql(
                    "REPLICATION is a global privilege: revoke it without an ON object \
                     (e.g. REVOKE REPLICATION FROM user)"
                        .into(),
                ));
            }
            global_only = true;
        }
        perm_types.push(match p {
            Privilege::All => PermissionType::All,
            Privilege::Select => PermissionType::Select,
            Privilege::Insert => PermissionType::Insert,
            Privilege::Update => PermissionType::Update,
            Privilege::Delete => PermissionType::Delete,
            Privilege::Create => PermissionType::Create,
            Privilege::Drop => PermissionType::Drop,
            Privilege::Alter => PermissionType::Alter,
            Privilege::Truncate => PermissionType::Truncate,
            Privilege::Subscribe => PermissionType::Subscribe,
            Privilege::Replication => PermissionType::Replication,
        });
    }
    let obj_name = if global_only {
        Some("*")
    } else {
        match on {
            Some(GrantObject::Table(t)) => Some(t.as_str()),
            Some(GrantObject::Database(d)) => Some(d.as_str()),
            Some(GrantObject::AllTables) => Some("*"),
            Some(GrantObject::AllDatabases) => Some("*"),
            None => None,
        }
    };
    engine.auth_manager().revoke(from, &perm_types, obj_name)?;
    tracing::info!(privileges_count = privileges.len(), from, "REVOKE executed");
    Ok(ExecResult::Ok { rows_affected: 0 })
}
