use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::common::config::AutoCompactSettings;
use crate::common::error::Result;
use crate::sql::execution::Engine;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::core::compactor::{CompactStats, Compactor};

pub struct AutoCompactorHandle {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl AutoCompactorHandle {
    pub fn start(engine: Arc<Engine>, config: AutoCompactSettings) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("taibai-auto-compact".into())
            .spawn(move || run_loop(engine, config, thread_stop))
            .expect("failed to spawn auto compact thread");

        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AutoCompactorHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug, Clone)]
struct AutoCompactCandidate {
    db: String,
    table: String,
    table_meta: TableMeta,
    segment_count: usize,
    deleted_rows: u64,
}

pub fn run_auto_compact_once(engine: &Engine, config: &AutoCompactSettings) -> Result<usize> {
    if !engine.auto_compact_enabled() {
        tracing::debug!("auto compact skipped because runtime flag is disabled");
        return Ok(0);
    }
    if engine.is_bulk_mode() {
        tracing::debug!("auto compact skipped while bulk import mode is active");
        return Ok(0);
    }
    if engine.has_active_segments() {
        tracing::debug!("auto compact skipped because active segments are still being written");
        return Ok(0);
    }
    if !engine.is_idle_for(Duration::from_secs(config.idle_threshold_secs)) {
        tracing::debug!("auto compact skipped because server is not idle");
        return Ok(0);
    }

    let mut candidates = collect_candidates(engine, config)?;
    candidates.sort_by(|left, right| {
        right
            .segment_count
            .cmp(&left.segment_count)
            .then_with(|| right.deleted_rows.cmp(&left.deleted_rows))
    });

    let max_tables = config.max_tables_per_run.max(1);
    let compactor = Compactor::new(
        engine.segment_mgr().clone(),
        engine.data_dir().to_path_buf(),
    );
    let mut compacted = 0usize;

    for candidate in candidates.into_iter().take(max_tables) {
        if engine.is_bulk_mode()
            || engine.has_active_segments()
            || !engine.is_idle_for(Duration::from_secs(config.idle_threshold_secs))
        {
            break;
        }
        if !engine
            .memtable_manager()
            .get_by_table(&candidate.db, &candidate.table)
            .is_empty()
        {
            tracing::debug!(
                db = %candidate.db,
                table = %candidate.table,
                "auto compact candidate skipped because pending memtables appeared"
            );
            continue;
        }

        match compactor.optimize_one_partition(
            &candidate.db,
            &candidate.table,
            &candidate.table_meta,
        ) {
            Ok(stats) => {
                if compact_succeeded(stats) {
                    engine
                        .index_manager()
                        .mark_index_stale(&candidate.db, &candidate.table);
                    compacted += 1;
                    tracing::info!(
                        db = %candidate.db,
                        table = %candidate.table,
                        table_full_name = %format!("{}.{}", candidate.db, candidate.table),
                        segments = candidate.segment_count,
                        deleted_rows = candidate.deleted_rows,
                        rows_written = stats.rows_written,
                        rows_dropped = stats.rows_dropped,
                        "auto compact completed"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(
                    db = %candidate.db,
                    table = %candidate.table,
                    table_full_name = %format!("{}.{}", candidate.db, candidate.table),
                    error = %err,
                    "auto compact failed"
                );
            }
        }
    }

    Ok(compacted)
}

fn run_loop(engine: Arc<Engine>, config: AutoCompactSettings, stop: Arc<AtomicBool>) {
    tracing::info!(
        enabled = config.enabled,
        check_interval_secs = config.check_interval_secs,
        idle_threshold_secs = config.idle_threshold_secs,
        min_segments = config.min_segments,
        min_deleted_rows = config.min_deleted_rows,
        max_tables_per_run = config.max_tables_per_run,
        "auto compact started"
    );

    let check_interval = Duration::from_secs(config.check_interval_secs.max(1));
    while sleep_until_next_check(&stop, check_interval) {
        if let Err(err) = run_auto_compact_once(&engine, &config) {
            tracing::warn!(error = %err, "auto compact check failed");
        }
    }

    tracing::info!("auto compact stopped");
}

fn sleep_until_next_check(stop: &AtomicBool, interval: Duration) -> bool {
    let mut slept = Duration::ZERO;
    while slept < interval {
        if stop.load(Ordering::Acquire) {
            return false;
        }
        let step = Duration::from_secs(1).min(interval - slept);
        std::thread::sleep(step);
        slept += step;
    }
    !stop.load(Ordering::Acquire)
}

fn collect_candidates(
    engine: &Engine,
    config: &AutoCompactSettings,
) -> Result<Vec<AutoCompactCandidate>> {
    let tables = {
        let catalogs = engine.catalogs().read().unwrap();
        catalogs
            .iter()
            .flat_map(|(db, catalog)| {
                catalog
                    .tables
                    .values()
                    .map(move |meta| (db.clone(), meta.name.clone(), meta.clone()))
            })
            .collect::<Vec<_>>()
    };

    let mut candidates = Vec::new();
    for (db, table, table_meta) in tables {
        if !engine
            .memtable_manager()
            .get_by_table(&db, &table)
            .is_empty()
        {
            continue;
        }

        let table_dir = engine.data_dir().join(&db).join(&table);
        if !table_dir.exists() {
            continue;
        }

        let segments = engine
            .segment_mgr()
            .discover_inactive_segments(&db, &table, &table_dir)?;
        if segments.is_empty() {
            continue;
        }

        let deleted_rows = if config.min_deleted_rows > 0 {
            deleted_rows_in_segments(engine, &segments)?
        } else {
            0
        };
        let enough_segments = segments.len() >= config.min_segments.max(2);
        let enough_deleted_rows =
            config.min_deleted_rows > 0 && deleted_rows >= config.min_deleted_rows;
        if !enough_segments && !enough_deleted_rows {
            continue;
        }

        candidates.push(AutoCompactCandidate {
            db,
            table,
            table_meta,
            segment_count: segments.len(),
            deleted_rows,
        });
    }

    Ok(candidates)
}

fn deleted_rows_in_segments(engine: &Engine, segments: &[PathBuf]) -> Result<u64> {
    let mut deleted_rows = 0u64;
    for segment in segments {
        let reader = engine.segment_mgr().open_reader(segment)?;
        deleted_rows = deleted_rows.saturating_add(reader.deleted_row_count());
    }
    Ok(deleted_rows)
}

fn compact_succeeded(stats: CompactStats) -> bool {
    stats.segments_read > 0 && (stats.segments_written > 0 || stats.rows_dropped > 0)
}
