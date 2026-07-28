#![allow(dead_code)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::net::session::Session;
use crate::storage::catalog::auth::{AuthManager, PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::DatabaseCatalog;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::storage::core::compactor::Compactor;
use crate::storage::core::memtable::MemTableManager;
use crate::storage::index::index_manager::IndexManager;
use crate::storage::partition::{parse_partition_key, PartitionStrategy};
use crate::storage::segment_manager::{SegmentConfig, SegmentManager};

use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

use crate::types::batch::{RecordBatch, Schema};

#[derive(Debug)]
pub enum ExecResult {
    Ok {
        rows_affected: u64,
    },
    Query {
        schema: Schema,
        batches: Vec<RecordBatch>,
    },
}

pub struct Engine {
    data_dir: PathBuf,
    catalogs: Arc<RwLock<HashMap<String, DatabaseCatalog>>>,
    catalog_save_state: Mutex<CatalogSaveState>,
    segment_mgr: Arc<SegmentManager>,
    batch_writer: BatchWriter,
    bulk_mode_sessions: AtomicUsize,
    in_flight_operations: AtomicUsize,
    auth_manager: Arc<AuthManager>,
    auto_increment_mgr: Arc<AutoIncrementManager>,
    index_manager: Arc<IndexManager>,
    hot_store_mgr: Arc<crate::storage::core::hot_store::HotStoreManager>,
    performance_baselines: Arc<crate::common::performance_baseline::PerformanceBaselineStore>,
}

enum HotResidentMark {
    None,
    FullTable,
    Partitions(Vec<String>),
}

struct CatalogSaveState {
    pending_insert_rows: u64,
    last_save_at: Instant,
}

const CATALOG_SAVE_INSERT_ROWS: u64 = 500_000;
const CATALOG_SAVE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct AutoCompactConfig {
    pub enabled: bool,
    pub min_deleted_ratio: f64,
    pub min_deleted_rows: u64,
    pub interval: Duration,
    pub max_tables_per_run: usize,
}

impl Default for AutoCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_deleted_ratio: 0.30,
            min_deleted_rows: 8192,
            interval: Duration::from_secs(60),
            max_tables_per_run: 8,
        }
    }
}

fn start_delete_bitmap_cleanup_thread(
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    catalogs: Arc<RwLock<HashMap<String, DatabaseCatalog>>>,
    config: AutoCompactConfig,
) {
    if !config.enabled {
        return;
    }
    std::thread::spawn(move || loop {
        std::thread::sleep(config.interval);
        let compactor = Compactor::new(Arc::clone(&segment_mgr), data_dir.clone());
        let table_metas = {
            let catalogs = catalogs.read().unwrap();
            catalogs
                .iter()
                .flat_map(|(db, catalog)| {
                    catalog
                        .tables
                        .iter()
                        .map(|(table, meta)| (db.clone(), table.clone(), meta.clone()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let mut compacted = 0usize;
        for (db, table, table_meta) in table_metas {
            if compacted >= config.max_tables_per_run {
                break;
            }
            let table_dir = data_dir.join(&db).join(&table);
            if !table_dir.exists() {
                continue;
            }
            match table_needs_delete_cleanup(
                &segment_mgr,
                &db,
                &table,
                &table_dir,
                config.min_deleted_ratio,
                config.min_deleted_rows,
            ) {
                Ok(true) => {
                    match compactor.compact_table_for_delete_cleanup(&db, &table, &table_meta) {
                        Ok(stats) => {
                            if stats.rows_dropped > 0 || stats.segments_written > 0 {
                                compacted += 1;
                                tracing::info!(
                                    db,
                                    table,
                                    rows_dropped = stats.rows_dropped,
                                    rows_written = stats.rows_written,
                                    segments_read = stats.segments_read,
                                    segments_written = stats.segments_written,
                                    "auto compact cleaned delete bitmap rows"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(db, table, error = %err, "auto compact failed");
                        }
                    }
                }
                Ok(false) => {
                    match segment_mgr.cleanup_fully_deleted_segments(&db, &table, &table_dir) {
                        Ok(removed) if removed > 0 => {
                            compacted += 1;
                            tracing::info!(
                                db,
                                table,
                                segments_removed = removed,
                                "delete bitmap cleanup removed fully deleted segments"
                            );
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(db, table, error = %err, "delete bitmap cleanup failed");
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(db, table, error = %err, "auto compact scan failed");
                }
            }
        }
    });
}

fn table_needs_delete_cleanup(
    segment_mgr: &SegmentManager,
    db: &str,
    table: &str,
    table_dir: &Path,
    min_deleted_ratio: f64,
    min_deleted_rows: u64,
) -> Result<bool> {
    let mut rows = 0u64;
    let mut deleted = 0u64;
    for path in segment_mgr.discover_inactive_segments(db, table, table_dir)? {
        let reader = segment_mgr.open_reader_cached(&path)?;
        rows = rows.saturating_add(reader.row_count());
        deleted = deleted.saturating_add(reader.deleted_row_count());
    }
    if deleted == 0 || deleted < min_deleted_rows {
        return Ok(false);
    }
    if rows == 0 {
        return Ok(false);
    }
    Ok((deleted as f64 / rows as f64) >= min_deleted_ratio)
}

include!("engine/core.rs");
include!("engine/runtime.rs");
include!("engine/bulk.rs");

impl Drop for Engine {
    fn drop(&mut self) {
        // Best-effort graceful shutdown so a process exit does not lose the
        // resident write buffer (design 鎼?.3). Idempotent with explicit shutdown().
        let _ = self.shutdown();
    }
}

fn parse_hh_mm(s: &str) -> Option<(u32, u32)> {
    use chrono::{NaiveTime, Timelike};
    let t = NaiveTime::parse_from_str(s.trim(), "%H:%M").ok()?;
    Some((t.hour(), t.minute()))
}

/// Start the background scheduler that flushes HOT tables once per day when
/// each table's `HOT ... CLOSE AT` time is reached. Tables without an explicit
/// close time default to 15:30 local time.
pub fn start_close_of_day_scheduler(engine: Arc<Engine>) -> std::thread::JoinHandle<()> {
    use chrono::{Local, Timelike};
    use std::collections::HashSet;
    std::thread::Builder::new()
        .name("taibai-close-of-day".into())
        .spawn(move || {
            let mut last_fired: HashSet<(String, String, chrono::NaiveDate)> = HashSet::new();
            loop {
                std::thread::sleep(Duration::from_secs(30));
                let now = Local::now();
                let today = now.date_naive();
                let today_str = today.to_string();
                last_fired.retain(|(_, _, d)| *d == today);
                let hot_tables = {
                    let catalogs = engine.catalogs.read().unwrap();
                    let mut tables = Vec::new();
                    for (db, catalog) in catalogs.iter() {
                        for (table, meta) in catalog.tables.iter() {
                            if meta.hot_time_column.is_some() {
                                tables.push((
                                    db.clone(),
                                    table.clone(),
                                    meta.hot_close_time.as_deref().unwrap_or("15:30").to_string(),
                                    meta.hot_last_close_date.clone(),
                                ));
                            }
                        }
                    }
                    tables
                };
                for (db, table, close_time, last_close_date) in hot_tables {
                    let Some((target_h, target_m)) = parse_hh_mm(&close_time).or(Some((15, 30))) else {
                        continue;
                    };
                    let key = (db.clone(), table.clone(), today);
                    if last_fired.contains(&key) || last_close_date.as_deref() == Some(today_str.as_str()) {
                        continue;
                    }
                    let reached = now.hour() > target_h
                        || (now.hour() == target_h && now.minute() >= target_m);
                    if reached {
                        tracing::info!(db, table, close_time, "close-of-day scheduler: firing table archival flush");
                        if let Err(e) = engine.close_of_day_flush_table(&db, &table) {
                            tracing::warn!(db, table, error = %e, "close-of-day scheduler: table flush failed");
                        }
                        last_fired.insert(key);
                    }
                }
            }
        })
        .expect("failed to spawn close-of-day scheduler")
}

struct EngineOperationGuard<'a> {
    engine: &'a Engine,
}

impl Drop for EngineOperationGuard<'_> {
    fn drop(&mut self) {
        self.engine
            .in_flight_operations
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn bulk_payload_table_name(payload: &[u8]) -> Result<&str> {
    if payload.len() < 2 {
        return Err(TaibaiError::Protocol("bulk payload too short".into()));
    }
    let table_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let start = 2;
    let end = start + table_len;
    if end > payload.len() {
        return Err(TaibaiError::Protocol("bulk payload truncated".into()));
    }
    std::str::from_utf8(&payload[start..end])
        .map_err(|e| TaibaiError::Protocol(format!("invalid utf8: {}", e)))
}

#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
