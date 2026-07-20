use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::auth::{Permission, PermissionObjectType, PermissionType};
use crate::storage::catalog::schema::{
    DatabaseCatalog, PartitionType, SecondaryIndexType, TableMeta,
};
use crate::types::batch::{ColumnDef, ColumnDefaultValue, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{
    AlterOperation, ColumnDefAst, DefaultValue, GrantObject, HotDef, PartitionDef,
    PartitionStrategy, Privilege, TtlDef, UniqueKeyDef,
};

const FILE_DELETE_MAX_ATTEMPTS: usize = 5;
const FILE_DELETE_INITIAL_DELAY: Duration = Duration::from_millis(10);
const DROP_TOMBSTONE_DIR: &str = ".taibai-drop-tombstones";
const DATABASE_DROP_TOMBSTONE_DIR: &str = ".taibai-drop-databases";
const TRUNCATE_TOMBSTONE_DIR: &str = ".taibai-truncate-tombstones";
const PARTITION_TOMBSTONE_DIR: &str = ".taibai-partition-tombstones";
const RENAME_TOMBSTONE_DIR: &str = ".taibai-rename-tombstones";
const BEFORE_META_FILE: &str = "before.json";
const COMMITTED_FILE: &str = "committed";
const RENAME_TARGET_FILE: &str = "new-name";
const STAGED_DATA_DIR: &str = "data";
const FRESH_DATA_DIR: &str = "fresh";

#[cfg(test)]
thread_local! {
    static FORCE_DROP_CLEANUP_FAILURE: std::cell::Cell<bool> = std::cell::Cell::new(false);
    static FORCE_DDL_CLEANUP_FAILURE: std::cell::Cell<bool> = std::cell::Cell::new(false);
}

fn is_retryable_delete_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock | io::ErrorKind::PermissionDenied
    ) || matches!(error.raw_os_error(), Some(32 | 33))
}

fn retry_delete<T, F>(operation: &str, path: &Path, mut delete: F) -> io::Result<()>
where
    F: FnMut(&Path) -> std::result::Result<(), T>,
    T: Into<io::Error>,
{
    let mut delay = FILE_DELETE_INITIAL_DELAY;
    for attempt in 1..=FILE_DELETE_MAX_ATTEMPTS {
        match delete(path).map_err(Into::into) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error)
                if attempt < FILE_DELETE_MAX_ATTEMPTS && is_retryable_delete_error(&error) =>
            {
                tracing::warn!(
                    operation,
                    path = %path.display(),
                    attempt,
                    max_attempts = FILE_DELETE_MAX_ATTEMPTS,
                    retry_delay_ms = delay.as_millis(),
                    error = %error,
                    "file deletion failed; retrying"
                );
                thread::sleep(delay);
                delay *= 2;
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "{} '{}' failed after {} attempt(s): {}",
                        operation,
                        path.display(),
                        attempt,
                        error
                    ),
                ));
            }
        }
    }
    unreachable!("delete retry loop always returns")
}

fn remove_file_with_retry(path: &Path) -> io::Result<()> {
    retry_delete("remove file", path, |path| std::fs::remove_file(path))
}

fn remove_dir_all_with_retry(path: &Path) -> io::Result<()> {
    retry_delete("remove directory", path, |path| {
        std::fs::remove_dir_all(path)
    })
}

fn remove_staged_table_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FORCE_DROP_CLEANUP_FAILURE.with(|force| force.replace(false)) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "forced staged-directory cleanup failure",
        ));
    }
    remove_dir_all_with_retry(path)
}

fn remove_committed_operation_directory(path: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FORCE_DDL_CLEANUP_FAILURE.with(|force| force.replace(false)) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "forced committed-operation cleanup failure",
        ));
    }
    remove_dir_all_with_retry(path)
}

fn stage_table_directory(path: &Path) -> io::Result<Option<std::path::PathBuf>> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("table path '{}' is not a directory", path.display()),
        ));
    }

    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("table path '{}' has no parent directory", path.display()),
        )
    })?;
    let table_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("table path '{}' has no table name", path.display()),
        )
    })?;
    let tombstone_root = parent.join(DROP_TOMBSTONE_DIR);
    std::fs::create_dir_all(&tombstone_root)?;
    let tombstone = tombstone_root.join(table_name);
    if tombstone.exists() {
        remove_dir_all_with_retry(&tombstone)?;
    }
    std::fs::rename(path, &tombstone)?;
    Ok(Some(tombstone))
}

fn restore_staged_table_directory(tombstone: &Path, table_dir: &Path) -> io::Result<()> {
    std::fs::rename(tombstone, table_dir)?;
    if let Some(root) = tombstone.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(())
}

fn cleanup_pending_table_drop(data_dir: &Path, db: &str, table: &str) -> Result<bool> {
    let tombstone = data_dir.join(db).join(DROP_TOMBSTONE_DIR).join(table);
    if !tombstone.exists() {
        return Ok(false);
    }
    remove_staged_table_directory(&tombstone).map_err(|error| {
        TaibaiError::Storage(format!(
            "table '{}.{}' is absent from the catalog, but pending physical cleanup for '{}' failed: {}",
            db,
            table,
            tombstone.display(),
            error
        ))
    })?;
    if let Some(root) = tombstone.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(true)
}

fn stage_database_directory(data_dir: &Path, name: &str) -> io::Result<Option<std::path::PathBuf>> {
    let database_dir = data_dir.join(name);
    let metadata = match std::fs::metadata(&database_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "database path '{}' is not a directory",
                database_dir.display()
            ),
        ));
    }
    let root = data_dir.join(DATABASE_DROP_TOMBSTONE_DIR);
    std::fs::create_dir_all(&root)?;
    let tombstone = root.join(name);
    if tombstone.exists() {
        remove_dir_all_with_retry(&tombstone)?;
    }
    std::fs::rename(&database_dir, &tombstone)?;
    Ok(Some(tombstone))
}

fn restore_staged_database_directory(tombstone: &Path, database_dir: &Path) -> io::Result<()> {
    std::fs::rename(tombstone, database_dir)?;
    if let Some(root) = tombstone.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(())
}

fn write_before_meta(operation_dir: &Path, table_meta: &TableMeta) -> Result<()> {
    std::fs::create_dir_all(operation_dir)?;
    let data = serde_json::to_vec_pretty(table_meta).map_err(|error| {
        TaibaiError::Storage(format!("failed to serialize DDL journal: {error}"))
    })?;
    std::fs::write(operation_dir.join(BEFORE_META_FILE), data)?;
    Ok(())
}

fn read_before_meta(operation_dir: &Path) -> Result<TableMeta> {
    let data = std::fs::read(operation_dir.join(BEFORE_META_FILE))?;
    serde_json::from_slice(&data)
        .map_err(|error| TaibaiError::Storage(format!("failed to parse DDL journal: {error}")))
}

fn table_meta_matches(left: &TableMeta, right: &TableMeta) -> Result<bool> {
    let left = serde_json::to_vec(left).map_err(|error| {
        TaibaiError::Storage(format!("failed to compare table metadata: {error}"))
    })?;
    let right = serde_json::to_vec(right).map_err(|error| {
        TaibaiError::Storage(format!("failed to compare table metadata: {error}"))
    })?;
    Ok(left == right)
}

fn mark_operation_committed(operation_dir: &Path) -> Result<()> {
    std::fs::write(operation_dir.join(COMMITTED_FILE), b"committed")?;
    Ok(())
}

fn rollback_staged_table_data(operation_dir: &Path, table_dir: &Path) -> Result<()> {
    let staged_data = operation_dir.join(STAGED_DATA_DIR);
    if staged_data.exists() {
        if table_dir.exists() {
            return Err(TaibaiError::Storage(format!(
                "cannot restore staged table data '{}' because '{}' already exists",
                staged_data.display(),
                table_dir.display()
            )));
        }
        std::fs::rename(&staged_data, table_dir)?;
    }
    let fresh_data = operation_dir.join(FRESH_DATA_DIR);
    if fresh_data.exists() {
        remove_dir_all_with_retry(&fresh_data)?;
    }
    let _ = std::fs::remove_file(operation_dir.join(BEFORE_META_FILE));
    let _ = std::fs::remove_dir(operation_dir);
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(())
}

fn restore_partition_files(operation_dir: &Path, table_dir: &Path) -> Result<()> {
    let staged_data = operation_dir.join(STAGED_DATA_DIR);
    if staged_data.exists() {
        std::fs::create_dir_all(table_dir)?;
        for entry in std::fs::read_dir(&staged_data)? {
            let entry = entry?;
            let destination = table_dir.join(entry.file_name());
            if destination.exists() {
                return Err(TaibaiError::Storage(format!(
                    "cannot restore staged partition file '{}' because '{}' already exists",
                    entry.path().display(),
                    destination.display()
                )));
            }
            std::fs::rename(entry.path(), destination)?;
        }
        let _ = std::fs::remove_dir(&staged_data);
    }
    let _ = std::fs::remove_file(operation_dir.join(BEFORE_META_FILE));
    let _ = std::fs::remove_dir(operation_dir);
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(())
}

fn stage_partition_files(
    operation_dir: &Path,
    table_dir: &Path,
    partition_key: &str,
) -> Result<()> {
    let staged_data = operation_dir.join(STAGED_DATA_DIR);
    std::fs::create_dir_all(&staged_data)?;
    let mut paths = Vec::new();
    let mut seen = HashSet::new();
    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = entry.file_name().to_string_lossy().to_string();
            if !file_name.ends_with(".dat")
                || crate::storage::partition::parse_partition_key(&file_name).as_deref()
                    != Some(partition_key)
            {
                continue;
            }
            for candidate in [
                path.clone(),
                crate::storage::segment::meta::sidecar_meta_path(&path),
                path.with_extension("meta"),
            ] {
                if candidate.exists() && seen.insert(candidate.clone()) {
                    paths.push(candidate);
                }
            }
        }
    }

    for path in paths {
        let destination = staged_data.join(path.file_name().ok_or_else(|| {
            TaibaiError::Storage(format!(
                "partition file '{}' has no file name",
                path.display()
            ))
        })?);
        if let Err(error) = std::fs::rename(&path, &destination) {
            restore_partition_files(operation_dir, table_dir)?;
            return Err(error.into());
        }
    }
    Ok(())
}

fn resolve_partition_key(
    table_meta: &TableMeta,
    table_dir: &Path,
    requested_key: &str,
    resident_partition_keys: &[String],
) -> Result<Option<String>> {
    let mut existing_keys = table_meta
        .partitions
        .iter()
        .map(|partition| partition.key.clone())
        .collect::<HashSet<_>>();
    existing_keys.extend(resident_partition_keys.iter().cloned());

    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            if let Some(key) = crate::storage::partition::parse_partition_key(&file_name) {
                existing_keys.insert(key);
            }
        }
    }

    if existing_keys.contains(requested_key) {
        return Ok(Some(requested_key.to_string()));
    }

    let unprefixed_key = requested_key
        .strip_prefix('p')
        .or_else(|| requested_key.strip_prefix('P'));
    if let Some(key) = unprefixed_key.filter(|key| !key.is_empty()) {
        if existing_keys.contains(key) {
            return Ok(Some(key.to_string()));
        }
    }

    Ok(None)
}

pub(super) fn recover_table_data_tombstones(
    data_dir: &Path,
    catalogs: &HashMap<String, DatabaseCatalog>,
) -> Result<()> {
    for (database_name, catalog) in catalogs {
        let database_dir = data_dir.join(database_name);
        for root_name in [TRUNCATE_TOMBSTONE_DIR, PARTITION_TOMBSTONE_DIR] {
            let root = database_dir.join(root_name);
            let entries = match std::fs::read_dir(&root) {
                Ok(entries) => entries,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            for entry in entries {
                let entry = entry?;
                let operation_dir = entry.path();
                let table_name = entry.file_name().to_string_lossy().to_string();
                let Some(current_meta) = catalog.get_table(&table_name) else {
                    if let Err(error) = remove_dir_all_with_retry(&operation_dir) {
                        tracing::warn!(path = %operation_dir.display(), error = %error, "failed to clean orphaned DDL tombstone");
                    }
                    continue;
                };
                let table_dir = database_dir.join(&table_name);
                let before_meta_path = operation_dir.join(BEFORE_META_FILE);
                if !before_meta_path.exists() {
                    // The journal is written before any data is staged and is
                    // only removed during rollback or committed cleanup. A
                    // surviving operation directory without it is therefore a
                    // partially cleaned committed operation.
                    if root_name == TRUNCATE_TOMBSTONE_DIR && !table_dir.exists() {
                        let fresh = operation_dir.join(FRESH_DATA_DIR);
                        if fresh.exists() {
                            std::fs::rename(&fresh, &table_dir)?;
                        } else {
                            std::fs::create_dir_all(&table_dir)?;
                        }
                    }
                    if let Err(error) = remove_dir_all_with_retry(&operation_dir) {
                        tracing::warn!(path = %operation_dir.display(), error = %error, "failed to finish partial committed DDL cleanup");
                    }
                    continue;
                }
                let before_meta = read_before_meta(&operation_dir)?;
                let committed = operation_dir.join(COMMITTED_FILE).exists()
                    || !table_meta_matches(current_meta, &before_meta)?;
                if !committed {
                    if root_name == TRUNCATE_TOMBSTONE_DIR {
                        rollback_staged_table_data(&operation_dir, &table_dir)?;
                    } else {
                        restore_partition_files(&operation_dir, &table_dir)?;
                    }
                    tracing::info!(database = %database_name, table = %table_name, "rolled back interrupted DDL operation");
                } else {
                    if root_name == TRUNCATE_TOMBSTONE_DIR && !table_dir.exists() {
                        let fresh = operation_dir.join(FRESH_DATA_DIR);
                        if fresh.exists() {
                            std::fs::rename(&fresh, &table_dir)?;
                        } else {
                            std::fs::create_dir_all(&table_dir)?;
                        }
                    }
                    if let Err(error) = remove_dir_all_with_retry(&operation_dir) {
                        tracing::warn!(path = %operation_dir.display(), error = %error, "failed to clean committed DDL tombstone");
                    }
                }
            }
            let _ = std::fs::remove_dir(&root);
        }
    }
    Ok(())
}

pub(super) fn recover_rename_tombstones(
    data_dir: &Path,
    catalogs: &HashMap<String, DatabaseCatalog>,
) -> Result<()> {
    for (database_name, catalog) in catalogs {
        let database_dir = data_dir.join(database_name);
        let root = database_dir.join(RENAME_TOMBSTONE_DIR);
        let entries = match std::fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let operation_dir = entry.path();
            let old_name = entry.file_name().to_string_lossy().to_string();
            let new_name = std::fs::read_to_string(operation_dir.join(RENAME_TARGET_FILE))?;
            let old_dir = database_dir.join(&old_name);
            let new_dir = database_dir.join(&new_name);
            if catalog.get_table(&old_name).is_some() && catalog.get_table(&new_name).is_none() {
                if !old_dir.exists() && new_dir.exists() {
                    std::fs::rename(&new_dir, &old_dir).map_err(|error| {
                        TaibaiError::Storage(format!(
                            "failed to roll back interrupted rename '{}.{}' to '{}': {}",
                            database_name, old_name, new_name, error
                        ))
                    })?;
                }
                if !old_dir.exists() {
                    return Err(TaibaiError::Storage(format!(
                        "cannot recover interrupted rename '{}.{}': neither '{}' nor '{}' exists",
                        database_name,
                        old_name,
                        old_dir.display(),
                        new_dir.display()
                    )));
                }
            } else if catalog.get_table(&new_name).is_some() && !new_dir.exists() {
                if old_dir.exists() {
                    std::fs::rename(&old_dir, &new_dir)?;
                } else {
                    return Err(TaibaiError::Storage(format!(
                        "cannot finish committed rename '{}.{}': data directory '{}' is missing",
                        database_name,
                        new_name,
                        new_dir.display()
                    )));
                }
            }
            remove_dir_all_with_retry(&operation_dir)?;
        }
        let _ = std::fs::remove_dir(&root);
    }
    Ok(())
}

pub(super) fn recover_database_drop_tombstones(data_dir: &Path) -> Result<()> {
    let root = data_dir.join(DATABASE_DROP_TOMBSTONE_DIR);
    let entries = match std::fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let meta_dir = data_dir.join("meta");
    for entry in entries {
        let entry = entry?;
        let tombstone = entry.path();
        let database_name = entry.file_name().to_string_lossy().to_string();
        let database_dir = data_dir.join(&database_name);
        let catalog_file = DatabaseCatalog::catalog_path(&meta_dir, &database_name);
        if catalog_file.exists() && !database_dir.exists() {
            restore_staged_database_directory(&tombstone, &database_dir).map_err(|error| {
                TaibaiError::Storage(format!(
                    "failed to restore database '{}' from interrupted DROP at '{}': {}",
                    database_name,
                    tombstone.display(),
                    error
                ))
            })?;
            tracing::info!(database = %database_name, "restored database directory from interrupted drop");
        } else if let Err(error) = remove_dir_all_with_retry(&tombstone) {
            tracing::warn!(
                database = %database_name,
                path = %tombstone.display(),
                error = %error,
                "failed to clean committed database drop tombstone"
            );
        }
    }
    let _ = std::fs::remove_dir(&root);
    Ok(())
}

/// Recover a DROP interrupted on either side of the durable catalog update.
/// A table still present in the catalog is restored; a committed DROP is cleaned.
pub(super) fn recover_drop_tombstones(
    data_dir: &Path,
    catalogs: &HashMap<String, DatabaseCatalog>,
) -> Result<()> {
    let Ok(databases) = std::fs::read_dir(data_dir) else {
        return Ok(());
    };
    for database in databases.flatten() {
        if !database
            .file_type()
            .map(|file_type| file_type.is_dir())
            .unwrap_or(false)
        {
            continue;
        }
        let database_name = database.file_name();
        if database_name == "meta" || database_name == "wal" {
            continue;
        }
        let database_name = database_name.to_string_lossy();
        let database_dir = database.path();
        let tombstone_root = database_dir.join(DROP_TOMBSTONE_DIR);
        let Some(catalog) = catalogs.get(database_name.as_ref()) else {
            tracing::warn!(
                database = %database_name,
                path = %tombstone_root.display(),
                "skipping drop recovery because the database catalog is unavailable"
            );
            continue;
        };
        let Ok(entries) = std::fs::read_dir(&tombstone_root) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let table_name = entry.file_name().to_string_lossy().to_string();
            let table_is_cataloged = catalog.get_table(&table_name).is_some();
            let table_dir = database_dir.join(&table_name);
            if table_is_cataloged {
                if table_dir.exists() && !table_dir.is_dir() {
                    return Err(TaibaiError::Storage(format!(
                        "cannot recover table '{}.{}': cataloged table path '{}' is not a directory",
                        database_name,
                        table_name,
                        table_dir.display()
                    )));
                }
                if !table_dir.exists() {
                    match restore_staged_table_directory(&path, &table_dir) {
                        Ok(()) => tracing::info!(
                            database = %database_name,
                            table = %table_name,
                            path = %table_dir.display(),
                            "restored table directory from interrupted drop"
                        ),
                        Err(error) => {
                            return Err(TaibaiError::Storage(format!(
                                "failed to restore table '{}.{}' from interrupted DROP at '{}': {}",
                                database_name,
                                table_name,
                                path.display(),
                                error
                            )))
                        }
                    }
                } else if let Err(error) = remove_dir_all_with_retry(&path) {
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "failed to remove stale drop tombstone"
                    );
                }
            } else {
                match remove_dir_all_with_retry(&path) {
                    Ok(()) => {
                        tracing::info!(path = %path.display(), "removed stale drop tombstone")
                    }
                    Err(error) => tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "failed to remove stale drop tombstone"
                    ),
                }
            }
        }
        let _ = std::fs::remove_dir(&tombstone_root);
    }
    Ok(())
}

fn validate_hot_def(col_defs: &[ColumnDef], table: &str, hot_def: &HotDef) -> Result<()> {
    let time_col = col_defs
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&hot_def.time_column))
        .ok_or_else(|| {
            TaibaiError::Sql(format!(
                "HOT BY column '{}' not found in table '{}'",
                hot_def.time_column, table
            ))
        })?;
    if !matches!(time_col.data_type, DataType::Timestamp) {
        return Err(TaibaiError::Sql(format!(
            "HOT BY column '{}' must be TIMESTAMP, got {:?}",
            hot_def.time_column, time_col.data_type
        )));
    }
    if hot_def.index_column.is_none() {
        return Err(TaibaiError::Sql(
            "HOT INDEX column is required; use HOT BY (...) INDEX (...)".into(),
        ));
    }
    if let Some(idx_col) = &hot_def.index_column {
        if !col_defs
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(idx_col))
        {
            return Err(TaibaiError::Sql(format!(
                "HOT INDEX column '{}' not found in table '{}'",
                idx_col, table
            )));
        }
    }
    if let Some(close_time) = &hot_def.close_time {
        if chrono::NaiveTime::parse_from_str(close_time.trim(), "%H:%M").is_err() {
            return Err(TaibaiError::Sql(format!(
                "HOT CLOSE AT time '{}' must use HH:MM format",
                close_time
            )));
        }
    }
    Ok(())
}

pub fn exec_create_table(
    engine: &Engine,
    db: &str,
    name: &str,
    columns: &[ColumnDefAst],
    if_not_exists: bool,
    primary_key: &[String],
    unique_keys: &[UniqueKeyDef],
    cluster_by: &Option<Vec<String>>,
    partition_by: &Option<PartitionDef>,
    ttl: &Option<TtlDef>,
    hot: &Option<HotDef>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;

    if catalog.get_table(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' already exists",
            db, name
        )));
    }

    let mut col_defs: Vec<ColumnDef> = columns
        .iter()
        .map(|c| {
            let default_value = c.default_value.as_ref().map(|dv| match dv {
                DefaultValue::CurrentTimestamp => ColumnDefaultValue::CurrentTimestamp,
                DefaultValue::Literal(s) => ColumnDefaultValue::Literal(s.clone()),
            });
            ColumnDef::new(&c.name, c.data_type)
                .with_nullable(c.nullable)
                .with_scale(c.scale)
                .with_precision(c.precision)
                .with_auto_increment(c.auto_increment)
                .with_primary_key(c.primary_key)
                .with_default_value(default_value)
        })
        .collect();

    apply_table_primary_key(&mut col_defs, primary_key)?;
    validate_column_constraints(&mut col_defs)?;
    let table_unique_indexes = build_table_unique_indexes(columns, &col_defs, unique_keys)?;
    if let Some(ttl_def) = ttl {
        validate_ttl_columns(&col_defs, ttl_def)?;
    }

    if let Some(hot_def) = hot {
        validate_hot_def(&col_defs, name, hot_def)?;
    }

    catalog.create_table(name, col_defs)?;

    if let Some(meta) = catalog.get_table_mut(name) {
        for (index_name, columns) in table_unique_indexes {
            meta.add_unique_index(&index_name, columns)?;
        }
        if let Some(ref keys) = cluster_by {
            meta.cluster_key = keys.clone();
        }
        if let Some(hot_def) = hot {
            meta.hot_time_column = Some(hot_def.time_column.clone());
            meta.hot_close_time = hot_def.close_time.clone();
            meta.hot_index_column = hot_def.index_column.clone();
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
                PartitionStrategy::Mod(divisor) => {
                    meta.partition_param = divisor;
                    PartitionType::Mod
                }
            };
            meta.update_partition_key_idx();
        }
        if let Some(ttl_def) = ttl {
            meta.set_ttl(&ttl_def.column, ttl_def.duration_secs);
        }
    }

    let auto_inc_col = columns.iter().find(|c| c.auto_increment);
    if let Some(ai_col) = auto_inc_col {
        if let Some(meta) = catalog.get_table_mut(name) {
            meta.auto_increment_next_value = 1;
        }
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

pub fn exec_drop_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    let has_unique_index = table_meta
        .unique_indexes
        .iter()
        .any(|idx| idx.name.eq_ignore_ascii_case(index_name));
    let has_secondary_index = table_meta
        .secondary_indexes
        .iter()
        .any(|idx| idx.name.eq_ignore_ascii_case(index_name));
    if !has_unique_index && !has_secondary_index {
        if if_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "index '{}' does not exist",
            index_name
        )));
    }
    let table_meta = catalog
        .get_table_mut(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    if has_unique_index {
        table_meta.drop_unique_index(index_name)?;
        engine
            .index_manager()
            .remove_unique_index_snapshot(db, table, index_name);
        engine.index_manager().mark_index_stale(db, table);
    } else {
        table_meta.drop_secondary_index(index_name)?;
        engine.index_manager().mark_index_stale(db, table);
    }
    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_create_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: &str,
    columns: &[String],
    unique: bool,
    if_not_exists: bool,
    index_type: Option<&str>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    let schema = Schema::new(table_meta.columns.clone());
    let indices = resolve_unique_index_columns(&schema, columns)?;
    let table_meta = catalog
        .get_table_mut(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    if table_meta.has_index(index_name) {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "index '{}' already exists",
            index_name
        )));
    }
    if unique {
        super::dml_insert::validate_unique_key_uniqueness(
            engine, db, table, &schema, &indices, index_name, None,
        )?;
        table_meta.add_unique_index(index_name, columns.to_vec())?;
    } else {
        let index_type = parse_secondary_index_type(index_type)?;
        table_meta.add_secondary_index(index_name, columns.to_vec(), index_type)?;
    }
    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn parse_secondary_index_type(index_type: Option<&str>) -> Result<SecondaryIndexType> {
    match index_type.unwrap_or("bloom").to_ascii_lowercase().as_str() {
        "bloom" => Ok(SecondaryIndexType::Bloom),
        "minmax" | "min_max" => Ok(SecondaryIndexType::MinMax),
        "set" => Ok(SecondaryIndexType::Set),
        other => Err(TaibaiError::Sql(format!(
            "unsupported index type '{}'; supported types: bloom, minmax, set",
            other
        ))),
    }
}

fn resolve_unique_index_columns(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(TaibaiError::Sql(
            "index must contain at least one column".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = schema
            .index_of(column)
            .ok_or_else(|| TaibaiError::Sql(format!("index column '{}' not found", column)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "duplicate index column '{}'",
                column
            )));
        }
        indices.push(idx);
    }
    Ok(indices)
}

pub fn exec_show_unique_index_status(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    let schema = Schema::new(vec![
        ColumnDef::new("table", DataType::VarChar),
        ColumnDef::new("index_name", DataType::VarChar),
        ColumnDef::new("columns", DataType::VarChar),
        ColumnDef::new("status", DataType::VarChar),
        ColumnDef::new("message", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    let table_schema = Schema::new(table_meta.columns.clone());

    for index in &table_meta.unique_indexes {
        let indices = resolve_unique_index_columns(&table_schema, &index.columns);
        let status = indices.and_then(|indices| {
            super::dml_insert::validate_unique_key_uniqueness(
                engine,
                db,
                table,
                &table_schema,
                &indices,
                &index.name,
                None,
            )?;
            engine
                .index_manager()
                .load_unique_index_snapshot(db, table, &index.name)
        });
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(table.to_string())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(index.name.clone())))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(index.columns.join(","))))?;
        match status {
            Ok(snapshot) => {
                let message = snapshot
                    .map(|snapshot| format!("persisted keys={}", snapshot.key_count))
                    .unwrap_or_else(|| "not persisted; run REBUILD UNIQUE INDEX".to_string());
                batch
                    .column_mut(3)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some("READY".to_string())))?;
                batch
                    .column_mut(4)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(message)))?;
            }
            Err(err) => {
                batch
                    .column_mut(3)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some("INVALID".to_string())))?;
                batch
                    .column_mut(4)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(err.to_string())))?;
            }
        }
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_rebuild_unique_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: Option<&str>,
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

    let table_schema = Schema::new(table_meta.columns.clone());
    engine.flush_all_buffers()?;
    let mut rebuilt = 0u64;
    engine
        .segment_mgr()
        .with_table_mutation_lock(db, table, || {
            for index in &table_meta.unique_indexes {
                if index_name
                    .map(|name| !index.name.eq_ignore_ascii_case(name))
                    .unwrap_or(false)
                {
                    continue;
                }
                let indices = resolve_unique_index_columns(&table_schema, &index.columns)?;
                let entries = super::dml_insert::collect_unique_index_entries(
                    engine,
                    db,
                    table,
                    &indices,
                    &index.name,
                )?;
                engine.index_manager().persist_unique_index_entries(
                    db,
                    table,
                    &index.name,
                    &index.columns,
                    entries,
                )?;
                rebuilt += 1;
            }
            Ok(())
        })?;

    if rebuilt == 0 && index_name.is_some() {
        return Err(TaibaiError::Sql(format!(
            "unique index '{}' does not exist",
            index_name.unwrap()
        )));
    }
    engine.index_manager().mark_index_stale(db, table);
    Ok(ExecResult::Ok {
        rows_affected: rebuilt,
    })
}

pub fn exec_drop_table(
    engine: &Engine,
    db: &str,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let total_start = Instant::now();
    tracing::info!(db, table = name, "drop table: start");
    let table_dir = engine.data_dir().join(db).join(name);
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let table_meta = match catalog.get_table(name) {
        Some(table_meta) => table_meta.clone(),
        None if cleanup_pending_table_drop(engine.data_dir(), db, name)? => {
            tracing::info!(
                db,
                table = name,
                "drop table: completed pending physical cleanup"
            );
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        None if if_exists => {
            tracing::info!(
                db,
                table = name,
                total_elapsed_ms = total_start.elapsed().as_millis(),
                "drop table: table missing, if exists ignored"
            );
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        None => {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' does not exist",
                db, name
            )))
        }
    };

    tracing::info!(db, table = name, path = %table_dir.display(), "drop table: waiting for table mutation lock");
    let stage_start = Instant::now();
    let tombstone = engine
        .segment_mgr()
        .with_table_mutation_lock(db, name, || {
            tracing::info!(db, table = name, elapsed_ms = stage_start.elapsed().as_millis(), "drop table: acquired table mutation lock");

            let step_start = Instant::now();
            engine.segment_mgr().remove_wal(&table_dir);
            tracing::info!(db, table = name, elapsed_ms = step_start.elapsed().as_millis(), "drop table: removed wal handle");

            let step_start = Instant::now();
            engine.segment_mgr().remove_table_state(db, name);
            tracing::info!(db, table = name, elapsed_ms = step_start.elapsed().as_millis(), "drop table: removed table state");

            let step_start = Instant::now();
            engine.segment_mgr().invalidate_readers_under(&table_dir);
            tracing::info!(db, table = name, elapsed_ms = step_start.elapsed().as_millis(), "drop table: invalidated readers");

            let step_start = Instant::now();
            tracing::info!(db, table = name, path = %table_dir.display(), "drop table: staging table directory");
            let tombstone = stage_table_directory(&table_dir).map_err(|error| {
                tracing::error!(db, table = name, path = %table_dir.display(), error = %error, elapsed_ms = step_start.elapsed().as_millis(), "drop table: failed to stage table directory");
                error
            })?;
            tracing::info!(db, table = name, path = %table_dir.display(), elapsed_ms = step_start.elapsed().as_millis(), "drop table: staged table directory");

            catalog.drop_table(name)?;
            let meta_dir = engine.data_dir().join("meta");
            if let Err(save_error) = catalog.save(&meta_dir) {
                catalog.tables.insert(name.to_string(), table_meta.clone());
                if let Some(tombstone) = tombstone.as_deref() {
                    if let Err(restore_error) =
                        restore_staged_table_directory(tombstone, &table_dir)
                    {
                        return Err(TaibaiError::Storage(format!(
                            "failed to save catalog after staging table '{}.{}': {}; additionally failed to restore '{}' to '{}': {}",
                            db,
                            name,
                            save_error,
                            tombstone.display(),
                            table_dir.display(),
                            restore_error
                        )));
                    }
                }
                return Err(save_error);
            }

            let step_start = Instant::now();
            engine.hot_store_manager().remove(db, name);
            tracing::info!(db, table = name, elapsed_ms = step_start.elapsed().as_millis(), "drop table: removed resident hot store");

            let step_start = Instant::now();
            let cleared_rows = engine.batch_writer().clear_memtables(db, name);
            tracing::info!(db, table = name, cleared_rows, elapsed_ms = step_start.elapsed().as_millis(), "drop table: cleared memtables");

            Ok(tombstone)
        })?;
    drop(catalogs);

    engine.auto_increment_manager().unregister(db, name);
    engine.index_manager().remove_index(db, name);
    let unique_index_names = table_meta
        .unique_indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, name, &unique_index_names);

    let mut cleanup_error = None;
    if let Some(tombstone) = tombstone {
        let step_start = Instant::now();
        tracing::info!(db, table = name, path = %tombstone.display(), "drop table: removing staged table directory");
        if let Err(error) = remove_staged_table_directory(&tombstone) {
            tracing::warn!(
                db,
                table = name,
                path = %tombstone.display(),
                error = %error,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: logical drop committed; staged directory cleanup deferred"
            );
            cleanup_error = Some(TaibaiError::Storage(format!(
                "table '{}.{}' was removed from the catalog, but physical data cleanup failed for '{}': {}; cleanup will be retried when the engine restarts",
                db,
                name,
                tombstone.display(),
                error
            )));
        } else {
            tracing::info!(db, table = name, path = %tombstone.display(), elapsed_ms = step_start.elapsed().as_millis(), "drop table: removed staged table directory");
            if let Some(root) = tombstone.parent() {
                let _ = std::fs::remove_dir(root);
            }
        }
    }

    tracing::info!(
        db,
        table = name,
        total_elapsed_ms = total_start.elapsed().as_millis(),
        "drop table: saved catalog and removed metadata managers"
    );

    if let Some(error) = cleanup_error {
        return Err(error);
    }

    tracing::info!(
        db,
        table = name,
        total_elapsed_ms = total_start.elapsed().as_millis(),
        "drop table: finished"
    );

    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn exec_rename_table(engine: &Engine, db: &str, name: &str, new_name: &str) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    if catalog.get_table(name).is_none() {
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' does not exist",
            db, name
        )));
    }
    if catalog.get_table(new_name).is_some() {
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' already exists",
            db, new_name
        )));
    }

    let catalog_before = catalog.clone();
    let old_table_dir = engine.data_dir().join(db).join(name);
    let new_table_dir = engine.data_dir().join(db).join(new_name);
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(RENAME_TOMBSTONE_DIR)
        .join(name);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending rename recovery at '{}'",
            db,
            name,
            operation_dir.display()
        )));
    }
    engine
        .segment_mgr()
        .with_table_mutation_lock(db, name, || {
            if new_table_dir.exists() {
                return Err(TaibaiError::Storage(format!(
                    "cannot rename table '{}.{}': destination directory '{}' already exists",
                    db,
                    name,
                    new_table_dir.display()
                )));
            }
            engine.segment_mgr().flush_table_active(db, name)?;
            engine.segment_mgr().remove_wal(&old_table_dir);
            engine.segment_mgr().remove_table_state(db, name);
            engine.segment_mgr().invalidate_readers_under(&old_table_dir);

            std::fs::create_dir_all(&operation_dir)?;
            std::fs::write(operation_dir.join(RENAME_TARGET_FILE), new_name.as_bytes())?;

            let renamed = if old_table_dir.exists() {
                std::fs::rename(&old_table_dir, &new_table_dir)?;
                true
            } else {
                false
            };
            catalog.rename_table(name, new_name)?;
            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                *catalog = catalog_before.clone();
                if renamed {
                    std::fs::rename(&new_table_dir, &old_table_dir).map_err(|restore_error| {
                        TaibaiError::Storage(format!(
                            "failed to persist rename of table '{}.{}': {}; additionally failed to restore directory '{}': {}",
                            db,
                            name,
                            error,
                            old_table_dir.display(),
                            restore_error
                        ))
                    })?;
                }
                let _ = remove_dir_all_with_retry(&operation_dir);
                return Err(error);
            }
            let renamed_rows = engine.batch_writer().rename_memtables(db, name, new_name);
            tracing::info!(
                db,
                old_table = name,
                new_table = new_name,
                renamed_rows,
                "renamed resident memtables"
            );
            Ok(())
        })?;

    engine
        .auto_increment_manager()
        .rename_table(db, name, new_name);
    engine.index_manager().rename_index(db, name, new_name);
    let unique_index_names = catalog_before
        .get_table(name)
        .map(|meta| {
            meta.unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, name, &unique_index_names);
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, new_name, &unique_index_names);
    engine.hot_store_manager().remove(db, name);
    if let Err(error) = remove_dir_all_with_retry(&operation_dir) {
        tracing::warn!(
            database = db,
            table = name,
            path = %operation_dir.display(),
            error = %error,
            "rename committed but recovery journal cleanup was deferred"
        );
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn exec_partition_change(
    engine: &Engine,
    db: &str,
    name: &str,
    partition_key: &str,
    drop_partition: bool,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let before_meta = catalog
        .get_table(name)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name)))?
        .clone();
    let table_dir = engine.data_dir().join(db).join(name);
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(PARTITION_TOMBSTONE_DIR)
        .join(name);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending partition recovery at '{}'",
            db,
            name,
            operation_dir.display()
        )));
    }

    let operation_dir = engine
        .segment_mgr()
        .with_table_mutation_lock(db, name, || {
            let resident_partition_keys = engine
                .batch_writer()
                .memtable_manager()
                .partition_keys(db, name);
            let partition_key = resolve_partition_key(
                &before_meta,
                &table_dir,
                partition_key,
                &resident_partition_keys,
            )?
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "partition '{}' does not exist in table '{}.{}'",
                    partition_key, db, name
                ))
            })?;

            // Active writers cover all partitions, so persist their buffers and metadata before
            // retiring the table state. Inserts append under this same table mutation lock and
            // cannot race this flush or the partition file staging below.
            engine.segment_mgr().flush_table_active(db, name)?;
            engine.segment_mgr().remove_wal(&table_dir);
            engine.segment_mgr().remove_table_state(db, name);
            engine.segment_mgr().invalidate_readers_under(&table_dir);
            write_before_meta(&operation_dir, &before_meta)?;
            stage_partition_files(&operation_dir, &table_dir, &partition_key)?;

            if let Some(meta) = catalog.get_table_mut(name) {
                if drop_partition {
                    if let Some(partition) = meta
                        .partitions
                        .iter()
                        .find(|part| part.key == partition_key)
                    {
                        meta.row_count -= partition.row_count;
                        meta.segment_count -= partition.segment_count;
                    }
                    meta.partitions.retain(|part| part.key != partition_key);
                } else if let Some(partition) = meta
                    .partitions
                    .iter_mut()
                    .find(|part| part.key == partition_key)
                {
                    meta.row_count -= partition.row_count;
                    meta.segment_count -= partition.segment_count;
                    partition.row_count = 0;
                    partition.segment_count = 0;
                }
            }

            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                catalog.tables.insert(name.to_string(), before_meta.clone());
                restore_partition_files(&operation_dir, &table_dir)?;
                return Err(error);
            }
            mark_operation_committed(&operation_dir)?;
            // Clear resident rows only after all fallible staging/catalog work has committed.
            // A flush worker that already owns a target memtable observes the cancelled state
            // under this same mutation lock before it is allowed to write its detached snapshot.
            let cleared_rows =
                engine
                    .batch_writer()
                    .clear_partition_memtables(db, name, &partition_key);
            tracing::info!(
                db,
                table = name,
                partition_key,
                cleared_rows,
                "partition change cleared resident memtables and flushed active segments"
            );
            engine.hot_store_manager().remove(db, name);
            engine.index_manager().mark_index_stale(db, name);
            let unique_index_names = before_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            engine.index_manager().remove_table_unique_index_snapshots(
                db,
                name,
                &unique_index_names,
            );
            Ok(operation_dir.clone())
        })?;
    drop(catalogs);

    if let Err(error) = remove_committed_operation_directory(&operation_dir) {
        let operation = if drop_partition {
            "DROP PARTITION"
        } else {
            "TRUNCATE PARTITION"
        };
        return Err(TaibaiError::Storage(format!(
            "{} for '{}.{}' committed, but cleanup of staged partition data at '{}' failed: {}; cleanup will be retried when the engine restarts",
            operation,
            db,
            name,
            operation_dir.display(),
            error
        )));
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_alter_table(
    engine: &Engine,
    db: &str,
    name: &str,
    operations: &[AlterOperation],
) -> Result<ExecResult> {
    if operations.len() != 1
        && operations.iter().any(|operation| {
            matches!(
                operation,
                AlterOperation::RenameTable { .. }
                    | AlterOperation::DropPartition { .. }
                    | AlterOperation::TruncatePartition { .. }
            )
        })
    {
        return Err(TaibaiError::Sql(
            "RENAME TABLE, DROP PARTITION, and TRUNCATE PARTITION must be executed as a single ALTER operation"
                .into(),
        ));
    }
    if let [operation] = operations {
        match operation {
            AlterOperation::RenameTable { new_name } => {
                return exec_rename_table(engine, db, name, new_name)
            }
            AlterOperation::DropPartition { partition_key } => {
                return exec_partition_change(engine, db, name, partition_key, true)
            }
            AlterOperation::TruncatePartition { partition_key } => {
                return exec_partition_change(engine, db, name, partition_key, false)
            }
            _ => {}
        }
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = catalogs
        .get_mut(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    if catalog.get_table(name).is_none() {
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' does not exist",
            db, name
        )));
    }

    for op in operations {
        match op {
            AlterOperation::AddColumn { column } => {
                let default_value = column.default_value.as_ref().map(|dv| match dv {
                    DefaultValue::CurrentTimestamp => ColumnDefaultValue::CurrentTimestamp,
                    DefaultValue::Literal(s) => ColumnDefaultValue::Literal(s.clone()),
                });
                let mut col_def = ColumnDef::new(&column.name, column.data_type)
                    .with_nullable(column.nullable)
                    .with_scale(column.scale)
                    .with_precision(column.precision)
                    .with_auto_increment(column.auto_increment)
                    .with_primary_key(column.primary_key)
                    .with_default_value(default_value);
                validate_column_constraints(std::slice::from_mut(&mut col_def))?;
                if !col_def.nullable && col_def.default_value.is_none() {
                    let existing_rows = catalog
                        .get_table(name)
                        .map(|meta| meta.row_count)
                        .unwrap_or(0);
                    if existing_rows > 0 {
                        return Err(TaibaiError::Sql(format!(
                            "cannot add NOT NULL column '{}' without default to non-empty table",
                            col_def.name
                        )));
                    }
                }
                let added_auto_increment = col_def.auto_increment;
                let added_column_name = col_def.name.clone();
                catalog.add_column(name, col_def)?;
                if added_auto_increment {
                    if let Some(meta) = catalog.get_table_mut(name) {
                        meta.auto_increment_next_value = 1;
                    }
                    engine
                        .auto_increment_manager()
                        .register(db, name, &added_column_name, 1)?;
                }
            }
            AlterOperation::DropColumn { name: col_name } => {
                let dropped_auto_increment = catalog
                    .get_table(name)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|col| col.name.eq_ignore_ascii_case(col_name))
                    })
                    .is_some_and(|col| col.auto_increment);
                catalog.drop_column(name, col_name)?;
                if dropped_auto_increment {
                    engine.auto_increment_manager().unregister(db, name);
                }
                if let Some(meta) = catalog.get_table_mut(name) {
                    meta.unique_indexes.retain(|idx| {
                        !idx.columns
                            .iter()
                            .any(|col| col.eq_ignore_ascii_case(col_name))
                    });
                    meta.secondary_indexes.retain(|idx| {
                        !idx.columns
                            .iter()
                            .any(|col| col.eq_ignore_ascii_case(col_name))
                    });
                    meta.update_partition_key_idx();
                    meta.update_ttl_column_idx();
                }
            }
            AlterOperation::AddUniqueKey { key } => {
                let table_meta = catalog
                    .get_table(name)
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                    })?
                    .clone();
                let schema = Schema::new(table_meta.columns.clone());
                let index_name = key
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("uk_{}", table_meta.unique_indexes.len() + 1));
                let indices = resolve_unique_index_columns(&schema, &key.columns)?;
                super::dml_insert::validate_unique_key_uniqueness(
                    engine,
                    db,
                    name,
                    &schema,
                    &indices,
                    &index_name,
                    None,
                )?;
                catalog
                    .get_table_mut(name)
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                    })?
                    .add_unique_index(&index_name, key.columns.clone())?;
            }
            AlterOperation::DropIndex { name: index_name } => {
                let table_meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                if table_meta
                    .unique_indexes
                    .iter()
                    .any(|idx| idx.name.eq_ignore_ascii_case(index_name))
                {
                    table_meta.drop_unique_index(index_name)?;
                    engine
                        .index_manager()
                        .remove_unique_index_snapshot(db, name, index_name);
                    engine.index_manager().mark_index_stale(db, name);
                } else {
                    table_meta.drop_secondary_index(index_name)?;
                    engine.index_manager().mark_index_stale(db, name);
                }
            }
            AlterOperation::RenameColumn { old_name, new_name } => {
                catalog.rename_column(name, old_name, new_name)?;
                if let Some(meta) = catalog.get_table_mut(name) {
                    for index in &mut meta.unique_indexes {
                        for column in &mut index.columns {
                            if column.eq_ignore_ascii_case(old_name) {
                                *column = new_name.clone();
                            }
                        }
                    }
                    for index in &mut meta.secondary_indexes {
                        for column in &mut index.columns {
                            if column.eq_ignore_ascii_case(old_name) {
                                *column = new_name.clone();
                            }
                        }
                    }
                }
            }
            AlterOperation::ModifyColumn { column } => {
                let existing = catalog
                    .get_table(name)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|col| col.name.eq_ignore_ascii_case(&column.name))
                    })
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!(
                            "column '{}' does not exist in table '{}'",
                            column.name, name
                        ))
                    })?;
                if existing.data_type != column.data_type
                    || existing.scale != column.scale
                    || existing.precision != column.precision
                {
                    return Err(TaibaiError::Sql(format!(
                        "MODIFY COLUMN type changes require column rewrite and are not supported yet for '{}'",
                        column.name
                    )));
                }
                catalog.modify_column(
                    name,
                    &column.name,
                    column.data_type,
                    column.scale,
                    column.precision,
                )?;
                engine.persist_auto_increment_next_value(db, name, false)?;
            }
            AlterOperation::RenameTable { new_name: _ } => {
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::DropPartition { partition_key } => {
                let _ = partition_key;
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::TruncatePartition { partition_key } => {
                let _ = partition_key;
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::SetTtl { ttl } => {
                let meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                validate_ttl_definition(meta, ttl)?;
                meta.set_ttl(&ttl.column, ttl.duration_secs);
            }
            AlterOperation::DropTtl => {
                let meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                meta.clear_ttl();
            }
        }
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn build_table_unique_indexes(
    ast_columns: &[ColumnDefAst],
    columns: &[ColumnDef],
    unique_keys: &[UniqueKeyDef],
) -> Result<Vec<(String, Vec<String>)>> {
    let mut indexes = Vec::new();
    for col in ast_columns
        .iter()
        .filter(|col| col.unique && !col.primary_key)
    {
        indexes.push((format!("uk_{}", col.name), vec![col.name.clone()]));
    }
    for (idx, key) in unique_keys.iter().enumerate() {
        let name = key
            .name
            .clone()
            .unwrap_or_else(|| format!("uk_{}", idx + 1));
        if key.columns.is_empty() {
            return Err(TaibaiError::Sql(
                "unique key must contain at least one column".into(),
            ));
        }
        for key_col in &key.columns {
            if !columns
                .iter()
                .any(|col| col.name.eq_ignore_ascii_case(key_col))
            {
                return Err(TaibaiError::Sql(format!(
                    "unique key column '{}' not found",
                    key_col
                )));
            }
        }
        indexes.push((name, key.columns.clone()));
    }
    let mut seen = std::collections::HashSet::new();
    for (name, _) in &indexes {
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(TaibaiError::Sql(format!("duplicate unique key '{}'", name)));
        }
    }
    Ok(indexes)
}

fn validate_ttl_definition(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    ttl: &TtlDef,
) -> Result<()> {
    validate_ttl_columns(&table_meta.columns, ttl)
}

fn validate_ttl_columns(columns: &[ColumnDef], ttl: &TtlDef) -> Result<()> {
    if ttl.duration_secs <= 0 {
        return Err(TaibaiError::Sql(
            "TTL duration must be a positive number of seconds".into(),
        ));
    }

    let column = columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&ttl.column))
        .ok_or_else(|| TaibaiError::Sql(format!("TTL column '{}' not found", ttl.column)))?;

    if column.data_type != DataType::Timestamp {
        return Err(TaibaiError::Sql(format!(
            "TTL column '{}' must be TIMESTAMP",
            ttl.column
        )));
    }

    Ok(())
}

fn apply_table_primary_key(columns: &mut [ColumnDef], primary_key: &[String]) -> Result<()> {
    for pk_col in primary_key {
        let column = columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(pk_col))
            .ok_or_else(|| {
                TaibaiError::Sql(format!("primary key column '{}' not found", pk_col))
            })?;
        column.primary_key = true;
    }
    Ok(())
}

fn validate_column_constraints(columns: &mut [ColumnDef]) -> Result<()> {
    let mut seen_names = std::collections::HashSet::new();
    let mut seen_pk = std::collections::HashSet::new();

    for col in columns {
        let name_key = col.name.to_ascii_lowercase();
        if !seen_names.insert(name_key.clone()) {
            return Err(TaibaiError::Sql(format!("duplicate column '{}'", col.name)));
        }
        if col.primary_key {
            if !seen_pk.insert(name_key) {
                return Err(TaibaiError::Sql(format!(
                    "duplicate primary key column '{}'",
                    col.name
                )));
            }
            col.nullable = false;
        }
        if let Some(default_value) = col.default_value.take() {
            col.default_value = Some(normalize_default_value(
                default_value,
                col.data_type,
                col.scale,
                &col.name,
            )?);
        }
    }
    Ok(())
}

fn normalize_default_value(
    default_value: ColumnDefaultValue,
    data_type: DataType,
    scale: i32,
    column_name: &str,
) -> Result<ColumnDefaultValue> {
    match default_value {
        ColumnDefaultValue::CurrentTimestamp => {
            if data_type != DataType::Timestamp {
                return Err(TaibaiError::Sql(format!(
                    "CURRENT_TIMESTAMP default requires TIMESTAMP column '{}'",
                    column_name
                )));
            }
            Ok(ColumnDefaultValue::CurrentTimestamp)
        }
        ColumnDefaultValue::Literal(value) => {
            let expr = crate::sql::parser::ast::Expr::Literal(value);
            let scalar = super::helpers::eval_expr_to_scalar_with_scale_timezone(
                &expr, &data_type, scale, 0,
            )?;
            if !scalar.is_null() && scalar.data_type() != data_type {
                return Err(TaibaiError::Sql(format!(
                    "default value for column '{}' has type {}, expected {}",
                    column_name,
                    scalar.data_type().display_name(),
                    data_type.display_name()
                )));
            }
            Ok(ColumnDefaultValue::Literal(scalar))
        }
    }
}

pub fn exec_truncate_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = catalogs
        .get_mut(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let before_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    let table_dir = engine.data_dir().join(db).join(table);
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(TRUNCATE_TOMBSTONE_DIR)
        .join(table);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending TRUNCATE recovery at '{}'",
            db,
            table,
            operation_dir.display()
        )));
    }

    let operation_dir = engine
        .segment_mgr()
        .with_table_mutation_lock(db, table, || {
            engine.segment_mgr().remove_wal(&table_dir);
            engine.segment_mgr().remove_table_state(db, table);
            engine.segment_mgr().invalidate_readers_under(&table_dir);

            write_before_meta(&operation_dir, &before_meta)?;
            let fresh_data = operation_dir.join(FRESH_DATA_DIR);
            std::fs::create_dir_all(&fresh_data)?;
            if table_dir.exists() {
                std::fs::rename(&table_dir, operation_dir.join(STAGED_DATA_DIR))?;
            }

            if let Some(meta) = catalog.get_table_mut(table) {
                meta.reset_auto_increment_next_value();
                meta.row_count = 0;
                meta.segment_count = 0;
                for part in &mut meta.partitions {
                    part.row_count = 0;
                    part.segment_count = 0;
                }
            }
            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                catalog
                    .tables
                    .insert(table.to_string(), before_meta.clone());
                rollback_staged_table_data(&operation_dir, &table_dir)?;
                return Err(error);
            }
            mark_operation_committed(&operation_dir)?;

            if let Err(error) = std::fs::rename(&fresh_data, &table_dir) {
                return Err(TaibaiError::Storage(format!(
                    "TRUNCATE TABLE '{}.{}' committed, but failed to activate the empty table directory '{}': {}; restart will retry recovery",
                    db,
                    table,
                    table_dir.display(),
                    error
                )));
            }

            engine.batch_writer().clear_memtables(db, table);
            engine.hot_store_manager().remove(db, table);
            engine.index_manager().clear_index(db, table);
            let unique_index_names = before_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            engine
                .index_manager()
                .remove_table_unique_index_snapshots(db, table, &unique_index_names);
            Ok(operation_dir.clone())
        })?;

    if engine.auto_increment_manager().get(db, table).is_some() {
        engine.auto_increment_manager().reset(db, table, 1)?;
    }
    drop(catalogs);

    if let Err(error) = remove_committed_operation_directory(&operation_dir) {
        return Err(TaibaiError::Storage(format!(
            "TRUNCATE TABLE '{}.{}' committed, but cleanup of previous data at '{}' failed: {}; cleanup will be retried when the engine restarts",
            db,
            table,
            operation_dir.display(),
            error
        )));
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }

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
    let tombstone = engine.segment_mgr().with_database_mutation_locks(
        name,
        &table_names,
        || {
            engine.segment_mgr().invalidate_readers_under(&database_dir);
            engine.segment_mgr().remove_database_state(name);
            let tombstone =
                stage_database_directory(engine.data_dir(), name).map_err(|error| {
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
            for (table_name, _) in &table_indexes {
                engine.batch_writer().clear_memtables(name, table_name);
            }
            Ok(tombstone)
        },
    )?;

    for (table_name, unique_index_names) in &table_indexes {
        engine.index_manager().remove_table_unique_index_snapshots(
            name,
            table_name,
            unique_index_names,
        );
    }
    engine.auto_increment_manager().unregister_database(name);
    engine.index_manager().remove_database_indexes(name);
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
                Privilege::Subscribe => PermissionType::Subscribe,
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
            Privilege::Subscribe => PermissionType::Subscribe,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::session::Session;
    use std::cell::Cell;
    use std::sync::{Arc, Barrier};
    use tempfile::TempDir;

    fn scalar_count(engine: &Engine, session: &mut Session, sql: &str) -> i64 {
        match engine.execute(session, sql).expect("execute count query") {
            ExecResult::Query { batches, .. } => match batches[0].scalar(0, 0) {
                Some(ScalarValue::Int64(Some(value))) => value,
                other => panic!("unexpected count value: {other:?}"),
            },
            other => panic!("unexpected count result: {other:?}"),
        }
    }

    fn partition_segment_exists(table_dir: &Path, partition_key: &str) -> bool {
        std::fs::read_dir(table_dir)
            .expect("read table directory")
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(crate::storage::partition::parse_partition_key)
                    .as_deref()
                    == Some(partition_key)
            })
    }

    #[test]
    fn delete_retries_transient_errors_until_success() {
        let attempts = Cell::new(0);
        retry_delete("test delete", Path::new("test-path"), |_| {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            if attempt < 3 {
                Err(io::Error::new(io::ErrorKind::Interrupted, "busy"))
            } else {
                Ok(())
            }
        })
        .unwrap();
        assert_eq!(attempts.get(), 3);
    }

    #[test]
    fn delete_returns_error_after_retries_are_exhausted() {
        let attempts = Cell::new(0);
        let error = retry_delete("test delete", Path::new("test-path"), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "busy"))
        })
        .unwrap_err();
        assert_eq!(attempts.get(), FILE_DELETE_MAX_ATTEMPTS);
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("failed after 5 attempt(s)"));
    }

    #[test]
    fn delete_treats_not_found_as_success() {
        let attempts = Cell::new(0);
        retry_delete("test delete", Path::new("test-path"), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::new(io::ErrorKind::NotFound, "missing"))
        })
        .unwrap();
        assert_eq!(attempts.get(), 1);
    }

    #[test]
    fn delete_does_not_retry_permanent_errors() {
        let attempts = Cell::new(0);
        let error = retry_delete("test delete", Path::new("test-path"), |_| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid"))
        })
        .unwrap_err();
        assert_eq!(attempts.get(), 1);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn drop_table_staging_failure_keeps_catalog_entry() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(&mut session, "CREATE TABLE guarded_drop (id INT64)")
            .expect("create table");
        let table_dir = temp_dir.path().join("main").join("guarded_drop");
        std::fs::remove_dir(&table_dir).expect("remove empty table directory");
        std::fs::write(&table_dir, b"not a directory").expect("replace table path with a file");

        let error = engine
            .execute(&mut session, "DROP TABLE guarded_drop")
            .expect_err("drop must fail before changing the catalog");
        assert!(
            error.to_string().contains("is not a directory"),
            "unexpected drop error: {error}"
        );
        assert!(engine.get_table_meta("main", "guarded_drop").is_some());

        std::fs::remove_file(&table_dir).expect("remove invalid table path");
        engine
            .execute(&mut session, "DROP TABLE guarded_drop")
            .expect("retry drop");
        assert!(engine.get_table_meta("main", "guarded_drop").is_none());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn drop_table_removes_two_hundred_partition_files() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE partitioned_drop (id INT64) PARTITION BY id",
            )
            .expect("create partitioned table");
        let database_dir = temp_dir.path().join("main");
        let table_dir = database_dir.join("partitioned_drop");
        for partition in 0..200 {
            let path = table_dir.join(format!("p{partition}_0001.dat"));
            std::fs::write(path, partition.to_string()).expect("write partition fixture");
        }

        engine
            .execute(&mut session, "DROP TABLE partitioned_drop")
            .expect("drop partitioned table");
        assert!(!table_dir.exists());
        assert!(engine.get_table_meta("main", "partitioned_drop").is_none());
        assert!(
            !database_dir.join(DROP_TOMBSTONE_DIR).exists(),
            "drop tombstone should be removed after a successful cleanup"
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_removes_interrupted_drop_tombstones() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let tombstone = temp_dir
            .path()
            .join("main")
            .join(DROP_TOMBSTONE_DIR)
            .join("interrupted");
        std::fs::create_dir_all(&tombstone).expect("create tombstone");
        std::fs::write(tombstone.join("p1_0001.dat"), b"stale").expect("write stale data");

        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        assert!(!tombstone.exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_restores_uncommitted_drop_tombstone() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let table_dir = temp_dir.path().join("main").join("recover_me");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE TABLE recover_me (id INT64)")
                .expect("create table");
            engine.segment_mgr().shutdown();
        }

        let tombstone = temp_dir
            .path()
            .join("main")
            .join(DROP_TOMBSTONE_DIR)
            .join("recover_me");
        std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create tombstone root");
        std::fs::rename(&table_dir, &tombstone).expect("simulate crash after staging");

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        assert!(table_dir.exists());
        assert!(!tombstone.exists());
        assert!(engine.get_table_meta("main", "recover_me").is_some());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn drop_table_reports_committed_physical_cleanup_failure_to_client() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE cleanup_error (id INT64)")
            .expect("create table");

        FORCE_DROP_CLEANUP_FAILURE.with(|force| force.set(true));
        let error = engine
            .execute(&mut session, "DROP TABLE cleanup_error")
            .expect_err("physical cleanup failure must reach the client");
        let message = error.to_string();
        assert!(
            message.contains("was removed from the catalog"),
            "{message}"
        );
        assert!(
            message.contains("physical data cleanup failed"),
            "{message}"
        );
        assert!(
            message.contains("retried when the engine restarts"),
            "{message}"
        );
        assert!(engine.get_table_meta("main", "cleanup_error").is_none());
        assert!(temp_dir
            .path()
            .join("main")
            .join(DROP_TOMBSTONE_DIR)
            .join("cleanup_error")
            .exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn drop_database_reports_cleanup_failure_and_retry_finishes_cleanup() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE DATABASE doomed")
            .expect("create database");
        engine
            .execute(&mut session, "CREATE TABLE doomed.t (id INT64)")
            .expect("create table");

        FORCE_DDL_CLEANUP_FAILURE.with(|force| force.set(true));
        let error = engine
            .execute(&mut session, "DROP DATABASE doomed")
            .expect_err("cleanup failure must reach client");
        let message = error.to_string();
        assert!(message.contains("removed from the catalog"), "{message}");
        assert!(
            message.contains("physical data cleanup failed"),
            "{message}"
        );
        assert!(!engine.database_exists("doomed"));

        engine
            .execute(&mut session, "DROP DATABASE doomed")
            .expect("retry pending cleanup");
        assert!(!temp_dir.path().join(DATABASE_DROP_TOMBSTONE_DIR).exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn drop_database_cancels_detached_memtable_flushes() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE DATABASE drop_in_flight")
            .expect("create database");
        engine
            .execute(&mut session, "CREATE TABLE drop_in_flight.t (id INT64)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO drop_in_flight.t VALUES (1)")
            .expect("insert pending row");

        let memtable = engine
            .batch_writer()
            .memtable_manager()
            .get_by_table("drop_in_flight", "t")
            .into_iter()
            .next()
            .expect("pending memtable");
        memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
        let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

        engine
            .execute(&mut session, "DROP DATABASE drop_in_flight")
            .expect("drop database during detached flush");

        let wrote = engine
            .segment_mgr()
            .flush_memtable_batch_if(
                "drop_in_flight",
                "t",
                "",
                &detached_batch,
                0,
                || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
                || {},
            )
            .expect("cancel detached database flush");
        assert!(!wrote);
        assert!(!temp_dir.path().join("drop_in_flight").exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn truncate_cleanup_failure_keeps_empty_table_and_is_recovered_on_restart() {
        let temp_dir = TempDir::new().expect("create temp dir");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE TABLE truncate_me (id INT64)")
                .expect("create table");
            engine
                .execute(&mut session, "INSERT INTO truncate_me VALUES (1), (2)")
                .expect("insert rows");

            FORCE_DDL_CLEANUP_FAILURE.with(|force| force.set(true));
            let error = engine
                .execute(&mut session, "TRUNCATE TABLE truncate_me")
                .expect_err("cleanup failure must reach client");
            assert!(error.to_string().contains("TRUNCATE TABLE"));
            assert_eq!(
                scalar_count(&engine, &mut session, "SELECT COUNT(*) FROM truncate_me"),
                0
            );
            assert!(temp_dir
                .path()
                .join("main")
                .join(TRUNCATE_TOMBSTONE_DIR)
                .join("truncate_me")
                .exists());
            engine.segment_mgr().shutdown();
        }

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        assert_eq!(
            scalar_count(&engine, &mut session, "SELECT COUNT(*) FROM truncate_me"),
            0
        );
        assert!(!temp_dir
            .path()
            .join("main")
            .join(TRUNCATE_TOMBSTONE_DIR)
            .exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn drop_partition_matches_exact_partition_key() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE partition_keys (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO partition_keys VALUES (1), (10)")
            .expect("insert partitions");
        engine
            .execute(&mut session, "FLUSH TABLE partition_keys")
            .expect("flush table");

        exec_partition_change(&engine, "main", "partition_keys", "1", true)
            .expect("drop exact partition");
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM partition_keys WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM partition_keys WHERE id = 10"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn alter_table_drop_partition_accepts_storage_name_and_removes_files() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE drop_partition_sql (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(
                &mut session,
                "INSERT INTO drop_partition_sql VALUES (1), (10)",
            )
            .expect("insert partitions");
        engine
            .execute(&mut session, "FLUSH TABLE drop_partition_sql")
            .expect("flush table");

        let table_dir = temp_dir.path().join("main").join("drop_partition_sql");
        assert!(partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));

        engine
            .execute(
                &mut session,
                "ALTER TABLE drop_partition_sql DROP PARTITION p1",
            )
            .expect("drop p-prefixed partition");

        assert!(!partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM drop_partition_sql WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM drop_partition_sql WHERE id = 10"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn alter_table_truncate_partition_accepts_storage_name_and_removes_files() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE truncate_partition_sql (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(
                &mut session,
                "INSERT INTO truncate_partition_sql VALUES (1), (10)",
            )
            .expect("insert partitions");
        engine
            .execute(&mut session, "FLUSH TABLE truncate_partition_sql")
            .expect("flush table");

        let table_dir = temp_dir.path().join("main").join("truncate_partition_sql");
        assert!(partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));

        engine
            .execute(
                &mut session,
                "ALTER TABLE truncate_partition_sql TRUNCATE PARTITION p1",
            )
            .expect("truncate p-prefixed partition");

        assert!(!partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_sql WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_sql WHERE id = 10"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn truncate_partition_clears_only_target_partition_memtables() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE truncate_partition_memtables (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(
                &mut session,
                "INSERT INTO truncate_partition_memtables VALUES (1), (10)",
            )
            .expect("insert pending partitions");

        engine
            .execute(
                &mut session,
                "ALTER TABLE truncate_partition_memtables TRUNCATE PARTITION p1",
            )
            .expect("truncate pending partition");

        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 10"
            ),
            1
        );

        engine
            .execute(&mut session, "FLUSH TABLE truncate_partition_memtables")
            .expect("flush surviving partition");
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 10"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn truncate_partition_cancels_detached_in_flight_memtable_flush() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE truncate_partition_in_flight (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(
                &mut session,
                "INSERT INTO truncate_partition_in_flight VALUES (1)",
            )
            .expect("insert pending partition");

        let memtable = engine
            .batch_writer()
            .memtable_manager()
            .get_by_table("main", "truncate_partition_in_flight")
            .into_iter()
            .next()
            .expect("pending memtable");
        memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
        let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

        engine
            .execute(
                &mut session,
                "ALTER TABLE truncate_partition_in_flight TRUNCATE PARTITION p1",
            )
            .expect("truncate while flush is detached");

        let wrote = engine
            .segment_mgr()
            .flush_memtable_batch_if(
                "main",
                "truncate_partition_in_flight",
                "1",
                &detached_batch,
                0,
                || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
                || {},
            )
            .expect("detached flush cancellation");
        assert!(!wrote);
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_in_flight WHERE id = 1"
            ),
            0
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn truncate_partition_flushes_bulk_import_state_for_surviving_partitions() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE truncate_partition_bulk (id INT64) PARTITION BY id",
            )
            .expect("create table");

        engine.begin_bulk_import();
        session.bulk_import_mode = true;
        engine
            .execute(
                &mut session,
                "INSERT INTO truncate_partition_bulk VALUES (1)",
            )
            .expect("bulk insert target partition");
        engine
            .execute(
                &mut session,
                "INSERT INTO truncate_partition_bulk VALUES (10)",
            )
            .expect("bulk insert surviving partition");

        let table_dir = temp_dir.path().join("main").join("truncate_partition_bulk");
        assert!(partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));

        engine
            .execute(
                &mut session,
                "ALTER TABLE truncate_partition_bulk TRUNCATE PARTITION p1",
            )
            .expect("truncate partition during bulk import");

        assert!(!partition_segment_exists(&table_dir, "1"));
        assert!(partition_segment_exists(&table_dir, "10"));
        let surviving_segments =
            crate::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
                .expect("discover surviving segments");
        let surviving_rows = surviving_segments
            .iter()
            .map(|path| {
                engine
                    .segment_mgr()
                    .open_reader_cached(path)
                    .expect("open surviving segment")
                    .row_count()
            })
            .sum::<u64>();
        assert_eq!(surviving_rows, 1);

        session.bulk_import_mode = false;
        engine.end_bulk_import();
        engine
            .flush_all_buffers()
            .expect("finish bulk import flush");
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_bulk WHERE id = 1"
            ),
            0
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_partition_bulk WHERE id = 10"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn alter_table_partition_change_rejects_unknown_partition() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE unknown_partition (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO unknown_partition VALUES (1)")
            .expect("insert partition");

        let error = engine
            .execute(
                &mut session,
                "ALTER TABLE unknown_partition DROP PARTITION p2",
            )
            .expect_err("unknown partition must fail");
        assert!(error.to_string().contains("partition 'p2' does not exist"));
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM unknown_partition"
            ),
            1
        );
        assert!(!temp_dir
            .path()
            .join("main")
            .join(PARTITION_TOMBSTONE_DIR)
            .exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn rename_destination_failure_keeps_old_catalog_and_directory() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE old_name (id INT64)")
            .expect("create table");
        let database_dir = temp_dir.path().join("main");
        let destination = database_dir.join("new_name");
        std::fs::create_dir_all(&destination).expect("create conflicting destination");

        engine
            .execute(&mut session, "ALTER TABLE old_name RENAME TO new_name")
            .expect_err("rename must fail before catalog mutation");
        assert!(engine.get_table_meta("main", "old_name").is_some());
        assert!(engine.get_table_meta("main", "new_name").is_none());
        assert!(database_dir.join("old_name").exists());

        std::fs::remove_dir(&destination).expect("remove destination conflict");
        engine
            .execute(&mut session, "ALTER TABLE old_name RENAME TO new_name")
            .expect("rename table");
        assert!(engine.get_table_meta("main", "old_name").is_none());
        assert!(engine.get_table_meta("main", "new_name").is_some());
        assert!(destination.exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_rolls_back_uncommitted_rename() {
        let temp_dir = TempDir::new().expect("create temp dir");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE TABLE rename_old (id INT64)")
                .expect("create table");
            engine.segment_mgr().shutdown();
        }
        let database_dir = temp_dir.path().join("main");
        let old_dir = database_dir.join("rename_old");
        let new_dir = database_dir.join("rename_new");
        let operation_dir = database_dir.join(RENAME_TOMBSTONE_DIR).join("rename_old");
        std::fs::create_dir_all(&operation_dir).expect("create rename journal");
        std::fs::write(operation_dir.join(RENAME_TARGET_FILE), b"rename_new")
            .expect("write rename target");
        std::fs::rename(&old_dir, &new_dir).expect("simulate directory rename");

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        assert!(old_dir.exists());
        assert!(!new_dir.exists());
        assert!(engine.get_table_meta("main", "rename_old").is_some());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_restores_uncommitted_database_drop() {
        let temp_dir = TempDir::new().expect("create temp dir");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE DATABASE recover_db")
                .expect("create database");
            engine.segment_mgr().shutdown();
        }
        let database_dir = temp_dir.path().join("recover_db");
        let tombstone = temp_dir
            .path()
            .join(DATABASE_DROP_TOMBSTONE_DIR)
            .join("recover_db");
        std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create database tombstones");
        std::fs::rename(&database_dir, &tombstone).expect("simulate staged database drop");

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        assert!(database_dir.exists());
        assert!(engine.database_exists("recover_db"));
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_rolls_back_uncommitted_truncate() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let table_dir = temp_dir.path().join("main").join("truncate_recovery");
        let operation_dir = temp_dir
            .path()
            .join("main")
            .join(TRUNCATE_TOMBSTONE_DIR)
            .join("truncate_recovery");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE TABLE truncate_recovery (id INT64)")
                .expect("create table");
            engine
                .execute(&mut session, "INSERT INTO truncate_recovery VALUES (7)")
                .expect("insert row");
            engine
                .execute(&mut session, "FLUSH TABLE truncate_recovery")
                .expect("flush table");
            let before_meta = engine
                .get_table_meta("main", "truncate_recovery")
                .expect("table metadata");
            engine.segment_mgr().remove_wal(&table_dir);
            engine
                .segment_mgr()
                .remove_table_state("main", "truncate_recovery");
            engine.segment_mgr().invalidate_readers_under(&table_dir);
            write_before_meta(&operation_dir, &before_meta).expect("write truncate journal");
            std::fs::create_dir_all(operation_dir.join(FRESH_DATA_DIR)).expect("create fresh data");
            std::fs::rename(&table_dir, operation_dir.join(STAGED_DATA_DIR))
                .expect("stage old table data");
            engine.segment_mgr().shutdown();
        }

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM truncate_recovery"
            ),
            1
        );
        assert!(!operation_dir.exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_rolls_back_uncommitted_partition_drop() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let before_meta;
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(
                    &mut session,
                    "CREATE TABLE partition_recovery (id INT64) PARTITION BY id",
                )
                .expect("create table");
            engine
                .execute(
                    &mut session,
                    "INSERT INTO partition_recovery VALUES (1), (2)",
                )
                .expect("insert partitions");
            engine
                .execute(&mut session, "FLUSH TABLE partition_recovery")
                .expect("flush table");
            before_meta = engine
                .get_table_meta("main", "partition_recovery")
                .expect("table metadata");
            engine.segment_mgr().shutdown();
        }

        let table_dir = temp_dir.path().join("main").join("partition_recovery");
        let operation_dir = temp_dir
            .path()
            .join("main")
            .join(PARTITION_TOMBSTONE_DIR)
            .join("partition_recovery");
        write_before_meta(&operation_dir, &before_meta).expect("write partition journal");
        stage_partition_files(&operation_dir, &table_dir, "1").expect("stage partition files");

        let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM partition_recovery WHERE id = 1"
            ),
            1
        );
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM partition_recovery WHERE id = 2"
            ),
            1
        );
        assert!(!operation_dir.exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn engine_startup_fails_when_cataloged_table_cannot_be_restored() {
        let temp_dir = TempDir::new().expect("create temp dir");
        {
            let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
            let mut session = Session::new("root".to_string(), "main".to_string());
            engine
                .execute(&mut session, "CREATE TABLE blocked_restore (id INT64)")
                .expect("create table");
            engine.segment_mgr().shutdown();
        }
        let table_dir = temp_dir.path().join("main").join("blocked_restore");
        let tombstone = temp_dir
            .path()
            .join("main")
            .join(DROP_TOMBSTONE_DIR)
            .join("blocked_restore");
        std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create tombstone root");
        std::fs::rename(&table_dir, &tombstone).expect("stage table directory");
        std::fs::write(&table_dir, b"blocks directory restore").expect("create conflicting file");

        let error = match Engine::new(temp_dir.path(), "main") {
            Ok(_) => panic!("engine startup should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("is not a directory"));
        assert!(tombstone.exists());
    }

    #[test]
    fn rename_table_preserves_pending_memtables_under_new_name() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE rename_pending (id INT64)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO rename_pending VALUES (1)")
            .expect("insert pending row");

        engine
            .execute(
                &mut session,
                "ALTER TABLE rename_pending RENAME TO rename_pending_new",
            )
            .expect("rename table with pending row");

        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM rename_pending_new"
            ),
            1
        );
        engine
            .execute(&mut session, "FLUSH TABLE rename_pending_new")
            .expect("flush renamed memtable");
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM rename_pending_new"
            ),
            1
        );
        assert!(!temp_dir.path().join("main").join("rename_pending").exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn rename_table_cancels_detached_flush_for_old_name() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE rename_in_flight (id INT64)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO rename_in_flight VALUES (1)")
            .expect("insert pending row");

        let memtable = engine
            .batch_writer()
            .memtable_manager()
            .get_by_table("main", "rename_in_flight")
            .into_iter()
            .next()
            .expect("pending memtable");
        memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
        let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

        engine
            .execute(
                &mut session,
                "ALTER TABLE rename_in_flight RENAME TO rename_in_flight_new",
            )
            .expect("rename during detached flush");

        let wrote = engine
            .segment_mgr()
            .flush_memtable_batch_if(
                "main",
                "rename_in_flight",
                "",
                &detached_batch,
                0,
                || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
                || {},
            )
            .expect("cancel detached old-name flush");
        assert!(!wrote);
        assert!(!temp_dir
            .path()
            .join("main")
            .join("rename_in_flight")
            .exists());
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM rename_in_flight_new"
            ),
            1
        );
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn rename_table_flushes_bulk_import_active_state() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE rename_bulk (id INT64)")
            .expect("create table");

        engine.begin_bulk_import();
        session.bulk_import_mode = true;
        engine
            .execute(&mut session, "INSERT INTO rename_bulk VALUES (1)")
            .expect("bulk insert row");
        engine
            .execute(
                &mut session,
                "ALTER TABLE rename_bulk RENAME TO rename_bulk_new",
            )
            .expect("rename table during bulk import");

        session.bulk_import_mode = false;
        engine.end_bulk_import();
        engine
            .flush_all_buffers()
            .expect("finish bulk import flush");
        assert_eq!(
            scalar_count(
                &engine,
                &mut session,
                "SELECT COUNT(*) FROM rename_bulk_new"
            ),
            1
        );
        assert!(!temp_dir.path().join("main").join("rename_bulk").exists());
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn rename_and_truncate_invalidate_unique_index_snapshots() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE unique_ddl (id INT64 UNIQUE)")
            .expect("create unique table");
        engine
            .execute(&mut session, "INSERT INTO unique_ddl VALUES (1)")
            .expect("insert unique row");
        engine
            .execute(&mut session, "FLUSH TABLE unique_ddl")
            .expect("flush table");
        engine
            .execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON unique_ddl")
            .expect("rebuild unique index");

        engine
            .execute(
                &mut session,
                "ALTER TABLE unique_ddl RENAME TO unique_ddl_new",
            )
            .expect("rename unique table");
        engine
            .execute(&mut session, "INSERT INTO unique_ddl_new VALUES (1)")
            .expect_err("duplicate must still be rejected after rename");

        engine
            .execute(&mut session, "TRUNCATE TABLE unique_ddl_new")
            .expect("truncate unique table");
        engine
            .execute(&mut session, "INSERT INTO unique_ddl_new VALUES (1)")
            .expect("truncated unique key can be reused");
        engine.segment_mgr().shutdown();
    }

    #[test]
    fn concurrent_insert_cannot_recreate_dropped_table_directory() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
        let mut setup = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut setup, "CREATE TABLE concurrent_drop (id INT64)")
            .expect("create table");

        let barrier = Arc::new(Barrier::new(2));
        let writer_engine = Arc::clone(&engine);
        let writer_barrier = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let mut session = Session::new("root".to_string(), "main".to_string());
            writer_barrier.wait();
            for value in 0..500 {
                if writer_engine
                    .execute(
                        &mut session,
                        &format!("INSERT INTO concurrent_drop VALUES ({value})"),
                    )
                    .is_err()
                {
                    break;
                }
            }
        });

        barrier.wait();
        engine
            .execute(&mut setup, "DROP TABLE concurrent_drop")
            .expect("drop table during inserts");
        writer.join().expect("join writer");

        assert!(engine.get_table_meta("main", "concurrent_drop").is_none());
        assert!(!temp_dir
            .path()
            .join("main")
            .join("concurrent_drop")
            .exists());
        engine.segment_mgr().shutdown();
    }
}
