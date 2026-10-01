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
        // A live transaction with deferred DML pins this table's segments;
        // removing whole partition directories would strand its plans.
        if engine.txn_mutations().table_has_pins(&db, &table) {
            continue;
        }
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

    let mgr = engine.segment_mgr();
    // Memtable-resident rows of the expired partitions must land on disk
    // (into the files we are about to remove); otherwise a later flush would
    // recreate the partition. The read-time TTL filter keeps them invisible
    // either way, but flushing avoids resurrecting the files. Everything runs
    // inside the partition locks below so a concurrent writer cannot append
    // to an expired partition between the flush and the deletion.
    // Phase A — drain memtables, then delete the partition's segment files
    // under those partition locks so no concurrent flush recreates them
    // mid-delete. The file list is re-scanned AFTER the flush: the flush may
    // materialize fresh files for the partition, and those must go too. The
    // catalog is NOT touched here: DDL takes catalogs().write() first and
    // then waits for these locks, so acquiring the catalog lock while holding
    // the partition locks would deadlock against it (the order
    // dml_insert/lock_order_test pins).
    let expired_keys = expired.iter().map(String::as_str).collect::<Vec<_>>();
    let mut file_rows: i64 = 0;
    mgr.with_partition_set_mutation_lock(db, table, &expired_keys, || -> Result<()> {
        for key in &expired {
            engine
                .batch_writer()
                .flush_partition_under_partition_lock(db, table, key)?;
            mgr.flush_partition_active(db, table, key)?;
            file_rows += remove_partition_files(&table_dir, key, mgr)?;
        }
        Ok(())
    })?;

    // Purged rows may still be mirrored in the hot store, and the purged
    // partitions may be cached as resident: drop the residency marks and the
    // cached partition→segment knowledge so scans fall back to the (now
    // smaller) segment set. The read-time TTL filter stays as the safety net.
    engine
        .hot_store_manager()
        .clear_resident_coverage(db, table);
    engine.clear_hot_partition_segments(&table_dir);

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

    // Physically purged segments still appear in unique-index location
    // snapshots (and their keys in key sets): without invalidation, a probe
    // for a value the purge just deleted reports "exists" and re-inserting
    // it fails with a phantom duplicate key until the next rebuild.
    engine
        .index_manager()
        .invalidate_primary_key_index(db, table);
    let unique_index_names = previous_meta
        .unique_indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    engine
        .index_manager()
        .invalidate_table_unique_index_snapshots(db, table, &unique_index_names);

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
    std::fs::read_dir(table_dir)
        .map(|entries| {
            entries.flatten().any(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(parse_partition_key)
                    .is_some_and(|k| k == key)
            })
        })
        .unwrap_or(false)
}

/// Remove every segment file (and sidecar) of `key` under `table_dir`,
/// invalidating cached readers first. Returns the number of live rows the
/// removed files carried. A failed removal only logs: the purge is an
/// idempotent reconciliation and the next run converges.
fn remove_partition_files(
    table_dir: &std::path::Path,
    key: &str,
    mgr: &crate::storage::segment_manager::SegmentManager,
) -> Result<i64> {
    let mut file_rows: i64 = 0;
    let mut removed: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(table_dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if parse_partition_key(name).as_deref() != Some(key) {
            continue;
        }
        mgr.invalidate_readers_under(&path);
        // Purged segment paths are recyclable; drop their cached SUMs.
        crate::sql::execution::scan::aggregate_meta::invalidate_segment_sum_cache_under(&path);
        // Count live rows from the segment itself: the catalog's partition
        // list is not always maintained by the write path, so the file is
        // the source of truth here.
        let live_rows = mgr
            .open_reader(&path)
            .ok()
            .map(|reader| {
                reader
                    .row_count()
                    .saturating_sub(reader.deleted_row_count()) as i64
            })
            .unwrap_or(0);
        let sidecar = crate::storage::segment::meta::sidecar_meta_path(&path);
        // Windows sharing violations clear within a few hundred
        // milliseconds (in-flight readers); retry like the DDL paths
        // (`ddl::remove_file_with_retry`) instead of skipping on the first
        // failure.
        if let Err(error) = remove_file_with_retry(&path) {
            // A file that survived the purge still holds its rows: counting
            // them into file_rows would permanently drift row_count.
            tracing::warn!(path = %path.display(), error = %error, "ttl purge: failed to remove segment file");
            continue;
        }
        removed.push(name.to_string());
        file_rows += live_rows;
        if sidecar.exists() {
            let _ = std::fs::remove_file(&sidecar);
        }
    }
    mgr.emit_segments_removed(table_dir, &removed);
    Ok(file_rows)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Bounded retry for segment file removal: transient Windows sharing
/// violations from in-flight readers clear within milliseconds.
fn remove_file_with_retry(path: &std::path::Path) -> std::io::Result<()> {
    const MAX_ATTEMPTS: usize = 5;
    let mut delay = std::time::Duration::from_millis(50);
    let mut last_err = None;
    for attempt in 0..MAX_ATTEMPTS {
        match std::fs::remove_file(path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let retryable = error.kind() == std::io::ErrorKind::PermissionDenied
                    || format!("{error}").contains("being used by another process");
                last_err = Some(error);
                if !retryable || attempt + 1 == MAX_ATTEMPTS {
                    break;
                }
                std::thread::sleep(delay);
                delay *= 2;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| std::io::Error::other("remove failed")))
}
