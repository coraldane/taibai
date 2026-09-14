//! Partition-level TTL purge (roadmap step 3, P3.4): for temporal partitions
//! whose TTL column is the partition column, fully-expired partitions are
//! dropped physically — segment files removed and catalog entries retired —
//! instead of lingering behind the read-time row filter forever.
//!
//! The purge is an idempotent reconciliation: each run deletes expired
//! partition files and retires catalog entries whose files are gone. A crash
//! between the two steps converges on the next run. Rows remain invisible
//! throughout because the read-time TTL filter is never disabled.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::common::error::{Result, TaibaiError};
use crate::storage::catalog::schema::{PartitionType, TableMeta};
use crate::storage::partition::partition::parse_partition_key;
use crate::storage::partition::ttl::partition_fully_expired;

use super::Engine;

/// Purge fully-expired partitions across every candidate table. Returns the
/// number of rows reclaimed (per catalog accounting).
pub fn purge_expired_partitions(engine: &Engine) -> Result<u64> {
    let candidates: Vec<(String, String)> = {
        let catalogs = engine.catalogs().read().unwrap();
        let mut out = Vec::new();
        for (db, catalog) in catalogs.iter() {
            for (table, meta) in catalog.tables.iter() {
                if purge_applies(meta) {
                    out.push((db.clone(), table.clone()));
                }
            }
        }
        out
    };

    let mut rows_reclaimed = 0u64;
    for (db, table) in candidates {
        rows_reclaimed += purge_table(engine, &db, &table)?;
    }
    Ok(rows_reclaimed)
}

/// Whole-partition expiry requires a temporal partition scheme whose
/// partition column carries the TTL.
fn purge_applies(meta: &TableMeta) -> bool {
    if !matches!(
        meta.partition_type,
        PartitionType::Day | PartitionType::Month | PartitionType::Year
    ) {
        return false;
    }
    let Some(ttl) = meta.ttl.as_ref() else {
        return false;
    };
    let Some(partition_col) = meta.get_partition_column_index() else {
        return false;
    };
    partition_col == ttl.column_idx as usize
}

fn purge_table(engine: &Engine, db: &str, table: &str) -> Result<u64> {
    let Some(meta) = engine.get_table_meta(db, table) else {
        return Ok(0);
    };
    if !purge_applies(&meta) {
        return Ok(0);
    }
    let Some(ttl) = meta.ttl.clone() else {
        return Ok(0);
    };

    let table_dir = engine.data_dir().join(db).join(table);
    if !table_dir.exists() {
        return Ok(0);
    }

    let mut files_by_key: HashMap<String, Vec<PathBuf>> = HashMap::new();
    for entry in std::fs::read_dir(&table_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.starts_with('p') || !name.ends_with(".dat") {
            continue;
        }
        if let Some(key) = parse_partition_key(name) {
            files_by_key.entry(key).or_default().push(path);
        }
    }

    let now = now_micros();
    let expired: Vec<String> = files_by_key
        .keys()
        .filter(|key| partition_fully_expired(&ttl, &meta, key, now))
        .cloned()
        .collect();
    // Catalog entries whose files are gone (crash window of a previous run).
    let stale: Vec<String> = meta
        .partitions
        .iter()
        .map(|info| info.key.clone())
        .filter(|key| {
            !files_by_key.contains_key(key) && partition_fully_expired(&ttl, &meta, key, now)
        })
        .collect();
    if expired.is_empty() && stale.is_empty() {
        return Ok(0);
    }

    // Memtable-resident rows of the expired partitions must land on disk
    // (into the files we are about to remove); otherwise a later flush would
    // recreate the partition. The read-time TTL filter keeps them invisible
    // either way, but flushing avoids resurrecting the files.
    engine.segment_mgr().flush_table_active(db, table)?;

    let mgr = engine.segment_mgr();
    // Phase A — delete the expired segment files under the mutation lock so
    // no concurrent flush recreates them mid-delete. The catalog is NOT
    // touched here: DDL takes catalogs().write() first and then waits for
    // this lock, so acquiring the catalog lock while holding the mutation
    // lock would deadlock against it (the order dml_insert/lock_order_test
    // pins).
    let mut file_rows: i64 = 0;
    mgr.with_table_mutation_lock(db, table, || -> Result<()> {
        for key in &expired {
            for path in &files_by_key[key] {
                mgr.invalidate_readers_under(path);
                // Count live rows from the segment itself: the catalog's
                // partition list is not always maintained by the write
                // path, so the file is the source of truth here.
                if let Ok(reader) = mgr.open_reader(path) {
                    file_rows += reader
                        .row_count()
                        .saturating_sub(reader.deleted_row_count())
                        as i64;
                }
                let sidecar = crate::storage::segment::meta::sidecar_meta_path(path);
                if let Err(error) = std::fs::remove_file(path) {
                    tracing::warn!(path = %path.display(), error = %error, "ttl purge: failed to remove segment file");
                }
                if sidecar.exists() {
                    let _ = std::fs::remove_file(&sidecar);
                }
            }
        }
        Ok(())
    })?;

    // Phase B — retire catalog entries while holding only catalogs().write().
    // Re-check the directory first: a concurrent writer may have recreated
    // files for an expired key after phase A released the lock (e.g. a
    // backdated insert). Keeping such an entry lets the next round converge
    // instead of hiding live files behind a pruned partition.
    let mut catalogs = engine.catalogs().write().unwrap();
    let Some(catalog) = catalogs.get_mut(db) else {
        return Ok(0);
    };
    let Some(table_meta) = catalog.get_table_mut(table) else {
        return Ok(0);
    };
    let previous_meta = table_meta.clone();
    let mut retired_partitions = 0usize;
    table_meta.partitions.retain(|info| {
        let gone = (expired.contains(&info.key) || stale.contains(&info.key))
            && !partition_files_exist(&table_dir, &info.key);
        if gone {
            retired_partitions += 1;
        }
        !gone
    });
    table_meta.row_count = table_meta.row_count.saturating_sub(file_rows);
    let meta_dir = engine.data_dir().join("meta");
    if let Err(save_error) = catalog.save(&meta_dir) {
        if let Some(table_meta) = catalog.get_table_mut(table) {
            *table_meta = previous_meta;
        }
        return Err(TaibaiError::Storage(format!(
            "ttl purge: failed to save catalog for '{db}.{table}': {save_error}"
        )));
    }
    drop(catalogs);

    tracing::info!(
        db,
        table,
        partitions = retired_partitions,
        rows = file_rows,
        "ttl purge: dropped expired partitions"
    );
    Ok(file_rows.max(0) as u64)
}

/// True when at least one segment file for `key` currently exists under
/// `table_dir`.
fn partition_files_exist(table_dir: &std::path::Path, key: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .and_then(parse_partition_key)
            .is_some_and(|k| k == key)
    })
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
