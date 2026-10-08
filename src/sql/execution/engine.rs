use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::common::thread_supervisor::{
    sleep_interruptibly, spawn_supervised_with_stop, SupervisorHandle,
};
use crate::net::session::Session;
use crate::storage::catalog::auth::{AuthManager, PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::DatabaseCatalog;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::storage::core::compactor::Compactor;
use crate::storage::core::memtable::MemTableManager;
use crate::storage::index::index_manager::IndexManager;
use crate::storage::partition::parse_partition_key;
use crate::storage::segment_manager::{SegmentConfig, SegmentManager};

use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

use crate::types::batch::{RecordBatch, Schema};

fn recover_durable_memtable_tails(
    data_dir: &Path,
    catalogs: &HashMap<String, DatabaseCatalog>,
    batch_writer: &BatchWriter,
) -> Result<std::collections::HashSet<(String, String)>> {
    let mut recovered_tables = std::collections::HashSet::new();
    for (db, catalog) in catalogs {
        for (table, table_meta) in &catalog.tables {
            let table_dir = data_dir.join(db).join(table);
            let schema = Schema::new(table_meta.columns.clone());
            // Lazy recovery: tails are only indexed here (no decode), and each
            // memtable materializes from its tail file on first access. This
            // keeps restart time and post-restart memory independent of the
            // amount of unflushed data.
            for tail in crate::storage::core::durable_tail::recover_index(&table_dir, &schema)? {
                if tail.rows == 0 {
                    continue;
                }
                let rows = tail.rows;
                batch_writer.memtable_manager().restore_spilled_tail(
                    tail.id,
                    db,
                    table,
                    &tail.partition_key,
                    schema.clone(),
                    rows,
                    tail.path,
                );
                recovered_tables.insert((db.clone(), table.clone()));
                tracing::info!(
                    db,
                    table,
                    rows,
                    "recovered durable low-rate memtable tail (spilled)"
                );
            }
        }
    }
    Ok(recovered_tables)
}

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
    pubsub: Arc<crate::pubsub::PubSubHub>,
    pubsub_manager: Arc<crate::storage::core::pubsub::PubSubManager>,
    bulk_mode_sessions: AtomicUsize,
    bulk_dirty_tables: Mutex<HashSet<(String, String)>>,
    in_flight_operations: AtomicUsize,
    auth_manager: Arc<AuthManager>,
    auto_increment_mgr: Arc<AutoIncrementManager>,
    index_manager: Arc<IndexManager>,
    hot_store_mgr: Arc<crate::storage::core::hot_store::HotStoreManager>,
    performance_baselines: Arc<crate::common::performance_baseline::PerformanceBaselineStore>,
    /// Per-table directories: partitions already known to have at least one
    /// segment. Feeds the hot-store resident decision so a busy partitioned
    /// INSERT path does not re-run `read_dir` per statement. Positive-only
    /// entries (never invalidated by time) keep the filter conservative.
    hot_partition_segments: Mutex<HashMap<std::path::PathBuf, HashSet<String>>>,
    /// Running queries as seen by `SHOW PROCESSLIST` / `KILL QUERY`.
    query_registry: Arc<crate::sql::execution::query_registry::QueryRegistry>,
    /// Deferred DELETE/UPDATE mutations of explicit transactions. Shared
    /// with the segment manager (scan overlay) and memtable manager
    /// (flush pin); statements and commit/rollback reach it via this handle.
    txn_mutations: Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>,
    /// Replication log when this engine runs as a replication primary.
    rep_log: Option<Arc<crate::storage::replication::RepLog>>,
    /// Standby engines reject every write (the applier is the only writer).
    read_only: AtomicBool,
    /// Supervised auto-compact background thread (panic-tolerant; audit
    /// P1-5). Taken and stopped by [`Engine::shutdown`].
    auto_compact_supervisor: Mutex<Option<SupervisorHandle>>,
    /// Supervised close-of-day scheduler. Registered after construction by
    /// [`start_close_of_day_scheduler`] (the engine is `Arc`-wrapped by
    /// then) and stopped by [`Engine::shutdown`].
    close_of_day_supervisor: Mutex<Option<SupervisorHandle>>,
}

impl Engine {
    pub fn txn_mutations(&self) -> &Arc<crate::storage::core::txn_mutations::TxnMutationRegistry> {
        &self.txn_mutations
    }

    /// Replication log when running as a replication primary.
    pub fn rep_log(&self) -> Option<&Arc<crate::storage::replication::RepLog>> {
        self.rep_log.as_ref()
    }

    /// Mark the engine read-only (standby role). Writes fail with a clear
    /// error; reads are unaffected.
    pub fn set_read_only(&self) {
        use std::sync::atomic::Ordering;
        self.read_only.store(true, Ordering::Release);
    }

    pub fn is_read_only(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.read_only.load(Ordering::Acquire)
    }

    /// Store the supervised close-of-day scheduler handle so
    /// [`Engine::shutdown`] can stop and join the thread. Called by
    /// [`start_close_of_day_scheduler`] right after spawning (the engine is
    /// already `Arc`-wrapped there, so construction-time wiring is not
    /// possible).
    fn set_close_of_day_scheduler(&self, handle: SupervisorHandle) {
        *self
            .close_of_day_supervisor
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(handle);
    }
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
    pub fragmentation_enabled: bool,
    pub min_fragmented_row_groups: usize,
    pub max_row_group_overhead_ratio: f64,
    pub min_small_row_group_ratio: f64,
    pub fragmentation_cooldown: Duration,
    pub fragmentation_scan_interval: Duration,
    pub low_rate_max_rows: u64,
    pub low_rate_max_age: Duration,
    pub interval: Duration,
    pub max_tables_per_run: usize,
}

impl Default for AutoCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_deleted_ratio: 0.30,
            min_deleted_rows: 8192,
            fragmentation_enabled: true,
            min_fragmented_row_groups: 32,
            max_row_group_overhead_ratio: 2.0,
            min_small_row_group_ratio: 0.50,
            fragmentation_cooldown: Duration::from_secs(3600),
            fragmentation_scan_interval: Duration::from_secs(300),
            low_rate_max_rows: 1024,
            low_rate_max_age: Duration::from_secs(3600),
            interval: Duration::from_secs(60),
            max_tables_per_run: 8,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct TablePhysicalHealth {
    live_rows: u64,
    deleted_rows: u64,
    row_groups: usize,
    small_row_groups: usize,
}

impl TablePhysicalHealth {
    fn needs_delete_cleanup(&self, config: &AutoCompactConfig) -> bool {
        config.enabled
            && self.deleted_rows >= config.min_deleted_rows
            && self.live_rows.saturating_add(self.deleted_rows) > 0
            && (self.deleted_rows as f64 / self.live_rows.saturating_add(self.deleted_rows) as f64)
                >= config.min_deleted_ratio
    }

    fn needs_fragmentation_cleanup(
        &self,
        target_row_group_rows: u64,
        config: &AutoCompactConfig,
    ) -> bool {
        if !config.fragmentation_enabled
            || self.row_groups < config.min_fragmented_row_groups.max(2)
        {
            return false;
        }
        let target = target_row_group_rows.max(1);
        let expected = self.live_rows.max(1).div_ceil(target).max(1) as f64;
        let overhead = self.row_groups as f64 / expected;
        let small_ratio = self.small_row_groups as f64 / self.row_groups as f64;
        overhead >= config.max_row_group_overhead_ratio.max(1.0)
            && small_ratio >= config.min_small_row_group_ratio.clamp(0.0, 1.0)
    }
}

fn table_physical_health(
    segment_mgr: &SegmentManager,
    _db: &str,
    _table: &str,
    table_dir: &Path,
    target_row_group_rows: u64,
    collect_fragmentation: bool,
) -> Result<TablePhysicalHealth> {
    let mut health = TablePhysicalHealth::default();
    let small_limit = target_row_group_rows.max(1).div_ceil(4);
    for path in SegmentManager::discover_segments(table_dir)? {
        let reader = segment_mgr.open_reader_cached(&path)?;
        health.deleted_rows = health
            .deleted_rows
            .saturating_add(reader.deleted_row_count());
        health.live_rows = health.live_rows.saturating_add(
            reader
                .row_count()
                .saturating_sub(reader.deleted_row_count()),
        );
        if collect_fragmentation {
            health.row_groups = health.row_groups.saturating_add(reader.num_row_groups());
            for row_group_idx in 0..reader.num_row_groups() {
                if reader.row_group_rows(row_group_idx) <= small_limit {
                    health.small_row_groups = health.small_row_groups.saturating_add(1);
                }
            }
        }
    }
    Ok(health)
}

fn rebuild_compacted_location_indexes(
    index_manager: &IndexManager,
    segment_mgr: &SegmentManager,
    memtable_manager: &MemTableManager,
    data_dir: &Path,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
) {
    let schema = Schema::new(table_meta.columns.clone());
    let memtables = memtable_manager.get_by_table(db, table);
    // Compaction frees segment path numbers the next writer may reuse; the
    // (path, column, rows, deleted) sum-cache key cannot tell incarnations
    // apart, so clear the table's entries up front.
    crate::sql::execution::scan::aggregate_meta::invalidate_segment_sum_cache_under(
        &data_dir.join(db).join(table),
    );
    let result = segment_mgr.with_table_mutation_lock(db, table, || {
        super::dml_insert::rebuild_table_location_indexes_from(
            index_manager,
            segment_mgr,
            &memtables,
            data_dir,
            db,
            table,
            table_meta,
            &schema,
        )
    });
    match result {
        Ok(()) => {}
        Err(error) => {
            tracing::warn!(
                db,
                table,
                error = %error,
                "auto-compact index auto-rebuild failed; falling back to snapshot invalidation"
            );
            let index_names = table_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            index_manager.invalidate_table_unique_index_snapshots(db, table, &index_names);
        }
    }
}

fn adjust_row_count_for_ttl_drops(
    catalogs: &RwLock<HashMap<String, DatabaseCatalog>>,
    data_dir: &Path,
    db: &str,
    table: &str,
    rows_dropped_ttl: u64,
) {
    if rows_dropped_ttl == 0 {
        return;
    }
    // TTL-expired rows physically removed by compaction were never
    // subtracted by a DELETE; without this the catalog row_count drifts
    // upward forever.
    let mut catalogs = catalogs.write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(meta) = catalog.get_table_mut(table) {
            meta.row_count = (meta.row_count - rows_dropped_ttl as i64).max(0);
            let meta_dir = data_dir.join("meta");
            let _ = catalog.save(&meta_dir);
        }
    }
}

fn start_auto_compaction_thread(
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    catalogs: Arc<RwLock<HashMap<String, DatabaseCatalog>>>,
    index_manager: Arc<IndexManager>,
    memtable_manager: Arc<MemTableManager>,
    config: AutoCompactConfig,
) -> Option<SupervisorHandle> {
    if !config.enabled && !config.fragmentation_enabled {
        return None;
    }
    // Tick state that used to live in the thread's loop scope. It sits
    // behind `Mutex` (the supervised round body is `Fn`), and a panic mid
    // round must not poison it into a permanent panic loop, hence
    // `into_inner` recovery.
    let fragmentation_last_run = Mutex::new(HashMap::<(String, String), Instant>::new());
    let last_fragmentation_scan = Mutex::new(None::<Instant>);
    let stop = Arc::new(AtomicBool::new(false));
    let body_stop = Arc::clone(&stop);
    let body = move || {
        // One compaction tick = one round of the original infinite loop.
        // The supervisor (audit P1-5) catches a panicking tick — e.g. a
        // poisoned-lock `unwrap` — logs it and retries with backoff, so
        // auto-compaction degrades its cadence instead of silently dying
        // until process restart. Cadence sleep first, as before.
        sleep_interruptibly(&body_stop, config.interval);
        if body_stop.load(Ordering::Acquire) {
            return;
        }
        let collect_fragmentation = config.fragmentation_enabled
            && last_fragmentation_scan
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none_or(|last| last.elapsed() >= config.fragmentation_scan_interval);
        let compactor = Compactor::new(Arc::clone(&segment_mgr), data_dir.clone());
        // A bulk import holds the unique-index compaction pause: segment
        // rewrites here would rebuild location-index base snapshots
        // mid-import (the delta-compaction dispatcher is paused, but this
        // pass reaches the same files from a different subsystem). Defer
        // the whole pass to the next tick instead.
        if index_manager.unique_index_compaction_paused() {
            return;
        }
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
            let target_row_group_rows = table_meta.row_group_size.max(1) as u64;
            match table_physical_health(
                &segment_mgr,
                &db,
                &table,
                &table_dir,
                target_row_group_rows,
                collect_fragmentation,
            ) {
                Ok(health) if health.needs_delete_cleanup(&config) => {
                    // With a resident active segment, the no-seal cleanup
                    // could not see its tombstones, so the table would
                    // stay past the dirtiness gate forever: route through
                    // the fragmentation compaction, which seals the
                    // active segment first. The sealed segment only joins
                    // the rewrite when it is itself dirty or under-full
                    // (the compactor's dirty-segment input selection);
                    // clean full-size segments are never rewritten.
                    let compact_result =
                        if segment_mgr.active_partition_keys(&db, &table).is_empty() {
                            compactor.compact_table_for_delete_cleanup(&db, &table, &table_meta)
                        } else {
                            compactor.compact_table_for_fragmentation(&db, &table, &table_meta)
                        };
                    match compact_result {
                        Ok(stats) => {
                            adjust_row_count_for_ttl_drops(
                                &catalogs,
                                &data_dir,
                                &db,
                                &table,
                                stats.rows_dropped_ttl,
                            );
                            if stats.rows_dropped > 0 || stats.segments_written > 0 {
                                rebuild_compacted_location_indexes(
                                    &index_manager,
                                    &segment_mgr,
                                    &memtable_manager,
                                    &data_dir,
                                    &db,
                                    &table,
                                    &table_meta,
                                );
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
                Ok(health)
                    if health.needs_fragmentation_cleanup(target_row_group_rows, &config)
                        && fragmentation_last_run
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .get(&(db.clone(), table.clone()))
                            .is_none_or(|last| last.elapsed() >= config.fragmentation_cooldown) =>
                {
                    match compactor.compact_table_for_fragmentation(&db, &table, &table_meta) {
                        Ok(stats) => {
                            adjust_row_count_for_ttl_drops(
                                &catalogs,
                                &data_dir,
                                &db,
                                &table,
                                stats.rows_dropped_ttl,
                            );
                            if stats.segments_written > 0 {
                                rebuild_compacted_location_indexes(
                                    &index_manager,
                                    &segment_mgr,
                                    &memtable_manager,
                                    &data_dir,
                                    &db,
                                    &table,
                                    &table_meta,
                                );
                                compacted += 1;
                                fragmentation_last_run
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .insert((db.clone(), table.clone()), Instant::now());
                                tracing::info!(
                                    db,
                                    table,
                                    live_rows = health.live_rows,
                                    row_groups_before = health.row_groups,
                                    small_row_groups = health.small_row_groups,
                                    segments_read = stats.segments_read,
                                    segments_written = stats.segments_written,
                                    rows_written = stats.rows_written,
                                    "auto compact repaired row-group fragmentation"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(db, table, error = %err, "fragmentation auto compact failed");
                        }
                    }
                }
                Ok(_) => {
                    match segment_mgr.cleanup_fully_deleted_segments(&db, &table, &table_dir) {
                        Ok(removed) if removed > 0 => {
                            rebuild_compacted_location_indexes(
                                &index_manager,
                                &segment_mgr,
                                &memtable_manager,
                                &data_dir,
                                &db,
                                &table,
                                &table_meta,
                            );
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
        if collect_fragmentation {
            *last_fragmentation_scan
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Instant::now());
        }
    };
    Some(spawn_supervised_with_stop(
        "taibai-auto-compact",
        stop,
        body,
    ))
}

mod bulk;
mod core;
mod runtime;

pub use runtime::HotFeedGuard;

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
/// each table's explicit `HOT ... CLOSE AT` time is reached.
///
/// The loop runs under the panic-tolerant supervisor (audit P1-5): one
/// scheduler round is one supervised tick, so a panicking round is logged
/// and retried with backoff instead of silently killing archival flushes
/// until process restart. The handle is stored on the engine and stopped by
/// [`Engine::shutdown`].
pub fn start_close_of_day_scheduler(engine: Arc<Engine>) {
    use chrono::{Local, Timelike};
    use std::collections::HashSet;
    let stop = Arc::new(AtomicBool::new(false));
    // Round state that used to live in the thread's loop scope. Behind
    // `Mutex` because the supervised round body is `Fn`; `into_inner`
    // recovery keeps a mid-round panic from poisoning it permanently.
    let last_fired = Mutex::new(HashSet::<(String, String, chrono::NaiveDate)>::new());
    let last_ttl_purge = Mutex::new(std::time::Instant::now());
    let body_engine = Arc::clone(&engine);
    let body_stop = Arc::clone(&stop);
    let handle = spawn_supervised_with_stop("taibai-close-of-day", stop, move || {
        // One scheduler round = one iteration of the original loop.
        sleep_interruptibly(&body_stop, Duration::from_secs(30));
        if body_stop.load(Ordering::Acquire) {
            return;
        }
        let now = Local::now();
        let today = now.date_naive();
        let today_str = today.to_string();
        {
            let mut last_fired = last_fired.lock().unwrap_or_else(|p| p.into_inner());
            last_fired.retain(|(_, _, d)| *d == today);
        }
        let hot_tables = {
            let catalogs = body_engine.catalogs.read().unwrap();
            let mut tables = Vec::new();
            for (db, catalog) in catalogs.iter() {
                for (table, meta) in catalog.tables.iter() {
                    if let (Some(_), Some(close_time)) =
                        (&meta.hot_time_column, &meta.hot_close_time)
                    {
                        tables.push((
                            db.clone(),
                            table.clone(),
                            close_time.clone(),
                            meta.hot_last_close_date.clone(),
                        ));
                    }
                }
            }
            tables
        };
        // Partition-level TTL purge, at most once per minute: drop
        // fully-expired temporal partitions instead of leaving them
        // to the read-time row filter forever.
        {
            let mut last_ttl_purge = last_ttl_purge.lock().unwrap_or_else(|p| p.into_inner());
            if last_ttl_purge.elapsed() >= Duration::from_secs(60) {
                *last_ttl_purge = std::time::Instant::now();
                drop(last_ttl_purge);
                if let Err(error) = body_engine.run_ttl_partition_purge() {
                    tracing::warn!(error = %error, "ttl purge: run failed");
                }
            }
        }

        for (db, table, close_time, last_close_date) in hot_tables {
            // CLOSE AT is validated at DDL time; an unparseable value
            // here means corrupt or legacy catalog state — skip the
            // table rather than guessing a default firing time.
            let Some((target_h, target_m)) = parse_hh_mm(&close_time) else {
                tracing::warn!(
                    db,
                    table,
                    close_time,
                    "close-of-day scheduler: unparseable HOT CLOSE AT, skipping table"
                );
                continue;
            };
            let key = (db.clone(), table.clone(), today);
            let already_fired_today = last_fired
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains(&key);
            if already_fired_today || last_close_date.as_deref() == Some(today_str.as_str()) {
                continue;
            }
            let reached =
                now.hour() > target_h || (now.hour() == target_h && now.minute() >= target_m);
            if reached {
                tracing::info!(
                    db,
                    table,
                    close_time,
                    "close-of-day scheduler: firing table archival flush"
                );
                if let Err(e) = body_engine.close_of_day_flush_table(&db, &table) {
                    tracing::warn!(db, table, error = %e, "close-of-day scheduler: table flush failed");
                }
                last_fired
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(key);
            }
        }
    });
    engine.set_close_of_day_scheduler(handle);
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
#[path = "engine_test/mod.rs"]
mod tests;
