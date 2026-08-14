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
const DDL_TABLE_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS: u32 = 1200;
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

fn rename_directory_with_retry(source: &Path, destination: &Path) -> io::Result<()> {
    let mut delay = FILE_DELETE_INITIAL_DELAY;
    for attempt in 1..=FILE_DELETE_MAX_ATTEMPTS {
        match std::fs::rename(source, destination) {
            Ok(()) => return Ok(()),
            Err(error)
                if attempt < FILE_DELETE_MAX_ATTEMPTS && is_retryable_delete_error(&error) =>
            {
                tracing::warn!(
                    source = %source.display(),
                    destination = %destination.display(),
                    attempt,
                    max_attempts = FILE_DELETE_MAX_ATTEMPTS,
                    retry_delay_ms = delay.as_millis(),
                    error = %error,
                    "directory rename failed; retrying"
                );
                thread::sleep(delay);
                delay *= 2;
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "rename directory '{}' to '{}' failed after {} attempt(s): {}",
                        source.display(),
                        destination.display(),
                        attempt,
                        error
                    ),
                ));
            }
        }
    }
    unreachable!("rename retry loop always returns")
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
    rename_directory_with_retry(path, &tombstone)?;
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
            ] {
                if candidate.exists() && seen.insert(candidate.clone()) {
                    paths.push(candidate);
                }
            }
            if let Some(candidate) =
                crate::storage::segment::meta::legacy_partition_sidecar_meta_path(&path)
            {
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

include!("ddl/table.rs");
include!("ddl/database_auth.rs");
include!("ddl/introspection.rs");

#[cfg(test)]
#[path = "ddl_tests.rs"]
mod tests;
