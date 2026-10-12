//! Compaction orchestration, input identity checks and commit validation.
//! `rewrite` selects inputs and streams unsorted rewrites; `clustered`
//! prepares sorted partitions/runs; `merge` owns bounded merging and gathering.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::partition::parse_partition_key;
use crate::storage::segment::{store, SegmentWriter};
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

mod clustered;
mod merge;
mod rewrite;
mod runs;
mod stream_merge;
use clustered::*;
use merge::*;
use stream_merge::ensure_compaction_memory;

#[derive(Debug, Clone, Copy)]
struct RowRef {
    batch_idx: usize,
    row_idx: usize,
}

#[derive(Debug, Clone)]
struct SourceRowGroup {
    segment_path: PathBuf,
    row_group_idx: usize,
}

/// One row-group read source of the clustered path: its reader plus the
/// row-id bases needed to address rows both segment-locally (delete
/// bitmaps) and partition-globally (the sorted index space).
struct ClusterReadSource {
    reader: Arc<crate::storage::segment::SegmentReader>,
    row_group_idx: usize,
    segment_row_base: u64,
    global_row_base: usize,
    row_count: usize,
}

/// Outcome of the streaming unsorted rewrite: the counters the
/// prepared-compaction builder needs from the heavy phase.
struct UnsortedRewriteStats {
    rows_written: usize,
    deleted_rows: u64,
    ttl_dropped: u64,
}

/// Decoded in-memory partition state for the clustered path: every source
/// column concatenated into one contiguous batch plus the cluster-key
/// sorted index of the surviving rows.
struct ClusteredPartition {
    batch: RecordBatch,
    sorted_indices: Vec<u32>,
    deleted_rows: u64,
    ttl_dropped: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactStats {
    pub segments_read: usize,
    pub segments_written: usize,
    pub rows_written: u64,
    pub rows_dropped: u64,
    /// Rows physically removed by the TTL filter during the rewrite. These
    /// were never subtracted from the catalog `row_count` (unlike
    /// `rows_dropped`, which is dominated by DELETE-tombstoned rows the
    /// DELETE statement already subtracted), so the caller adjusts
    /// `row_count` by exactly this value.
    pub rows_dropped_ttl: u64,
}

/// Identity of a compaction input file at the moment the unlocked heavy
/// phase starts: (file length, row count, deleted rows). The commit phase
/// re-reads it under the partition lock; any drift means another mutator
/// touched the file while the rewrite ran without the lock, and the
/// prepared output must be discarded.
type SegmentFileIdentity = (u64, u64, u64);

/// Memory budget for the clustered-compaction in-memory fast path. Beyond
/// it the rewrite switches to an external merge sort (bounded-size sorted
/// runs spilled to temp segment files, then a k-way merge into the output),
/// so peak memory no longer scales with partition size.
const CLUSTERED_MERGE_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Test hook: when non-zero, overrides the in-memory budget so the external
/// merge path can be exercised on small data sets.
#[cfg(test)]
pub(crate) static CLUSTERED_MERGE_BUDGET_OVERRIDE: TestBudgetOverride = TestBudgetOverride;

#[cfg(test)]
thread_local! { static TEST_BUDGET: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
#[cfg(test)]
pub(crate) struct TestBudgetOverride;
#[cfg(test)]
impl TestBudgetOverride {
    pub(crate) fn store(&self, value: usize, _: AtomicOrdering) {
        TEST_BUDGET.with(|b| b.set(value));
    }
}

#[cfg(test)]
fn clustered_merge_budget_bytes() -> usize {
    let override_value = TEST_BUDGET.with(|b| b.get());
    if override_value > 0 {
        override_value
    } else {
        CLUSTERED_MERGE_BUDGET_BYTES
    }
}

#[cfg(not(test))]
fn clustered_merge_budget_bytes() -> usize {
    CLUSTERED_MERGE_BUDGET_BYTES
}
/// Maximum number of sorted runs merged at once; larger run counts are
/// merged in waves.
const CLUSTERED_MERGE_MAX_RUNS: usize = 64;

/// Memory one `RowRef` occupies in the unsorted rewrite's collection buffer
/// (two `usize`s = 16 bytes). It converts the shared compaction byte budget
/// into that buffer's row cap, so the unsorted path stays under the same
/// budget the clustered path already enforces.
const ROW_REF_BYTES: usize = std::mem::size_of::<RowRef>();

/// Outcome of the unlocked heavy phase (decode/sort/tmp write) of one
/// partition compaction. The commit phase validates and installs it under
/// the partition mutation lock.
enum PreparedCompaction {
    /// Nothing to rewrite this round (empty input or clean single segment).
    Skipped,
    /// Every input row is deleted or expired; commit removes the inputs.
    AllDropped {
        segment_files: Vec<PathBuf>,
        identities: Vec<SegmentFileIdentity>,
        segments_read: usize,
        rows_dropped: u64,
        rows_dropped_ttl: u64,
    },
    /// Replacement segment written to a non-discoverable tmp path; commit
    /// re-validates the inputs and atomically swaps it in.
    Replacement {
        _cleanup: runs::TempRuns,
        segment_files: Vec<PathBuf>,
        identities: Vec<SegmentFileIdentity>,
        tmp_path: PathBuf,
        final_path: PathBuf,
        segments_read: usize,
        rows_to_write: usize,
        rows_dropped: u64,
        rows_dropped_ttl: u64,
    },
}

/// Test seam: stalls the unlocked heavy phase so tests can prove that
/// INSERTs into the compacted partition proceed while the decode/sort/write
/// runs. Production value stays 0.
static COMPACTION_HEAVY_DELAY_MS: AtomicU64 = AtomicU64::new(0);

fn compaction_input_identity(
    segment_mgr: &SegmentManager,
    path: &Path,
) -> Option<SegmentFileIdentity> {
    let len = std::fs::metadata(path).ok()?.len();
    let reader = segment_mgr.open_reader_cached(path).ok()?;
    Some((len, reader.row_count(), reader.deleted_row_count()))
}

fn compaction_inputs_unchanged(
    segment_mgr: &SegmentManager,
    files: &[PathBuf],
    identities: &[SegmentFileIdentity],
) -> bool {
    if let Some(parent) = files.first().and_then(|p| p.parent()) {
        let Ok(live) = store::discover_segments(parent) else {
            return false;
        };
        let live: std::collections::HashSet<_> = live.into_iter().collect();
        if files.iter().any(|p| !live.contains(p)) {
            return false;
        }
    }
    files
        .iter()
        .zip(identities.iter())
        .all(|(path, expected)| compaction_input_identity(segment_mgr, path) == Some(*expected))
}

/// True when a row is still visible under the table's TTL: the read-time
/// filter hides rows whose TTL timestamp satisfies `ts + duration <= now`
/// (and rows whose TTL column is NULL or not a timestamp), so every
/// rewrite strategy must physically drop exactly the same rows to keep
/// compaction output in sync with query visibility.
#[inline]
fn ttl_row_visible(
    value: &crate::types::scalar::ScalarValue,
    duration_us: i64,
    now_us: i64,
) -> bool {
    matches!(
        value,
        crate::types::scalar::ScalarValue::Timestamp(Some(ts))
            if ts.saturating_add(duration_us) > now_us
    )
}

/// Cheap footer-only proof that EVERY row of a segment is TTL-expired: the
/// TTL column's zone-map max plus the duration is already past `now_us`.
/// Used to pull otherwise clean segments into a rewrite — the row-level TTL
/// purge only runs inside compaction rewrites, so without this proof-based
/// exception a clean segment's expired rows could never be physically
/// removed. Anything the zone maps cannot prove returns false so the
/// segment conservatively stays untouched: an uncomputed or empty map, a
/// NULL max, NULL TTL values (`null_count` — those rows stay visible to
/// the read filter), or a non-timestamp TTL column (never hidden either).
fn segment_ttl_fully_expired(
    reader: &crate::storage::segment::SegmentReader,
    ttl_col_idx: usize,
    duration_us: i64,
    now_us: i64,
) -> bool {
    (0..reader.num_row_groups()).all(|rg_idx| {
        reader
            .zone_maps(rg_idx)
            .get(ttl_col_idx)
            .is_some_and(|zone| {
                zone.row_count > 0
                    && zone.null_count == 0
                    && matches!(
                        &zone.max_value,
                        crate::types::scalar::ScalarValue::Timestamp(Some(ts))
                            if ts.saturating_add(duration_us) <= now_us
                    )
            })
    })
}

pub struct Compactor {
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    committed: std::sync::Mutex<CompactStats>,
}

impl Compactor {
    pub fn new(segment_mgr: Arc<SegmentManager>, data_dir: PathBuf) -> Self {
        Self {
            segment_mgr,
            data_dir,
            committed: std::sync::Mutex::new(CompactStats::default()),
        }
    }

    /// Includes successful partitions even when a later partition fails.
    pub fn committed_stats(&self) -> CompactStats {
        *self.committed.lock().unwrap()
    }

    /// See `COMPACTION_HEAVY_DELAY_MS`.
    pub fn set_heavy_phase_delay_for_tests(ms: u64) {
        COMPACTION_HEAVY_DELAY_MS.store(ms, AtomicOrdering::Release);
    }

    fn sleep_heavy_phase_delay() {
        let ms = COMPACTION_HEAVY_DELAY_MS.load(AtomicOrdering::Acquire);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }

    pub fn optimize_table(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Result<CompactStats> {
        self.compact_table(db, table, table_meta, true, true)
    }

    /// Rewrite segments to purge delete bitmaps without paying the cost of a
    /// CLUSTER BY reorder. This is reserved for the background delete-cleanup
    /// worker; user-triggered OPTIMIZE TABLE must call `optimize_table`.
    pub fn compact_table_for_delete_cleanup(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Result<CompactStats> {
        self.compact_table(db, table, table_meta, false, false)
    }

    /// Repair row-group/segment fragmentation, including a fragmented active
    /// segment. Pending memtables stay resident; only this table's active
    /// writer is sealed before the immutable rewrite.
    pub fn compact_table_for_fragmentation(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Result<CompactStats> {
        self.compact_table(db, table, table_meta, false, true)
    }

    fn compact_table(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        sort_by_cluster_key: bool,
        seal_active: bool,
    ) -> Result<CompactStats> {
        *self.committed.lock().unwrap() = CompactStats::default();
        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);

        if !table_dir.exists() {
            tracing::info!(db, table, "table directory does not exist, skipping");
            return Ok(CompactStats::default());
        }

        // A live transaction holding deferred DELETE/UPDATE tombstones needs
        // its pinned segments' row ids stable until COMMIT; rewriting them
        // would strand the plans. The next run after the transaction ends
        // picks the table up again.
        if self
            .segment_mgr
            .txn_mutations()
            .is_some_and(|registry| registry.table_has_pins(db, table))
        {
            tracing::debug!(
                db,
                table,
                "table pinned by an open transaction, skipping compaction"
            );
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let ttl = table_meta.ttl.as_ref().and_then(|info| {
            let idx = table_meta.get_ttl_column_index_cached()?;
            Some((idx, info.duration_secs))
        });
        let compression_level = crate::storage::segment_manager::resolve_compression_level(
            table_meta.compression_level,
        );
        let sort_key_indices = if sort_by_cluster_key {
            cluster_key_indices.as_slice()
        } else {
            &[]
        };
        let output_row_group_rows = self.segment_mgr.row_group_target_rows();
        // C11: seal BEFORE taking the table-wide mutation lock. The seal's
        // footer fsync is IO; running it inside the wide gate froze every
        // queued DDL/DML latch on the table for the duration of the flush.
        // This mirrors the checkpoint path (`seal_table_active`): writers are
        // extracted under the short active-segments lock and sealed outside
        // any mutation lock. Writes racing between the seal and the gate can
        // create a fresh active segment — it is simply left for the next
        // compaction round (the old in-lock seal raced identically against
        // writers already queued on the gate).
        if seal_active {
            self.segment_mgr.seal_table_active(db, table)?;
        }
        let _input_snapshot = crate::storage::segment::snapshot::pin_directory(&table_dir);
        let partition_groups = self.segment_mgr.with_table_mutation_lock(db, table, || {
            let segment_files = self
                .segment_mgr
                .discover_inactive_segments(db, table, &table_dir)?;
            if segment_files.is_empty() {
                return Ok(Vec::new());
            }

            tracing::info!(
                db,
                table,
                cluster_key = ?table_meta.cluster_key,
                sort_by_cluster_key,
                segments = segment_files.len(),
                "starting table optimization"
            );

            Ok(group_segments_by_partition(segment_files)
                .into_iter()
                .collect::<Vec<_>>())
        })?;
        if partition_groups.is_empty() {
            return Ok(CompactStats::default());
        }

        // `segments_read` counts segments actually consumed by the rewrite
        // (dirty inputs only on the non-clustered path), not every discovered
        // segment: clean segments are left untouched, so they are not read.
        let mut stats = CompactStats::default();

        let partition_count = partition_groups.len();
        let partition_stats = partition_groups
            .into_iter()
            .map(|(partition_key, files)| -> Result<CompactStats> {
                let lock_key = partition_key.as_deref().unwrap_or("");
                // Permits are acquired BEFORE the partition lock, in the same
                // order as query scans (cpu then io). Sleeping on the global
                // work limiter while holding the partition lock would freeze
                // that partition's INSERTs for the whole queue delay.
                let prepared = {
                    let _memory = compaction_memory_gate();
                    let _cpu_permit = crate::common::work_limiter::acquire_cpu();
                    let _io_permit = crate::common::work_limiter::acquire_io();
                    self.prepare_compact_segment_group(
                        db,
                        table,
                        &table_dir,
                        partition_key.as_deref(),
                        files,
                        &schema,
                        sort_key_indices,
                        &cluster_key_indices,
                        output_row_group_rows,
                        ttl,
                        compression_level,
                    )?
                };
                self.segment_mgr
                    .with_partition_mutation_lock(db, table, lock_key, || {
                        self.commit_prepared_compaction(
                            db,
                            table,
                            &table_dir,
                            partition_key.as_deref(),
                            prepared,
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        for part_stats in partition_stats {
            stats.segments_read += part_stats.segments_read;
            stats.segments_written += part_stats.segments_written;
            stats.rows_written += part_stats.rows_written;
            stats.rows_dropped += part_stats.rows_dropped;
            stats.rows_dropped_ttl += part_stats.rows_dropped_ttl;
        }

        tracing::info!(
            db,
            table,
            partitions = partition_count,
            "table optimization partition workers completed"
        );

        tracing::info!(
            db,
            table,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            segments_read = stats.segments_read,
            segments_written = stats.segments_written,
            sort_by_cluster_key,
            elapsed_ms = start_time.elapsed().as_millis(),
            "optimization completed"
        );

        Ok(stats)
    }

    pub fn optimize_partition(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        partition_key: &str,
    ) -> Result<CompactStats> {
        *self.committed.lock().unwrap() = CompactStats::default();
        if !table_meta.is_partitioned() {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' is not partitioned",
                db, table
            )));
        }

        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);
        let candidates = partition_key_candidates(partition_key);

        if !table_dir.exists() {
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key,
                "table directory does not exist, skipping partition optimization"
            );
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let ttl = table_meta.ttl.as_ref().and_then(|info| {
            let idx = table_meta.get_ttl_column_index_cached()?;
            Some((idx, info.duration_secs))
        });
        let compression_level = crate::storage::segment_manager::resolve_compression_level(
            table_meta.compression_level,
        );
        let output_row_group_rows = self.segment_mgr.row_group_target_rows();
        for candidate in &candidates {
            self.segment_mgr
                .seal_partition_active(db, table, candidate)?;
        }
        let _input_snapshot = crate::storage::segment::snapshot::pin_directory(&table_dir);
        let candidate_refs = candidates.iter().map(String::as_str).collect::<Vec<_>>();
        let matched = self.segment_mgr.with_partition_set_mutation_lock(
            db,
            table,
            &candidate_refs,
            || {
                let segment_files = self
                    .segment_mgr
                    .discover_inactive_segments(db, table, &table_dir)?;
                let groups = group_segments_by_partition(segment_files);
                let matched_key = candidates
                    .iter()
                    .find(|candidate| groups.contains_key(&Some((*candidate).clone())))
                    .cloned();
                let Some(matched_key) = matched_key else {
                    let available = groups
                        .keys()
                        .filter_map(|key| key.as_deref())
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Err(TaibaiError::Sql(format!(
                        "partition '{}' not found for table '{}.{}'{}",
                        partition_key,
                        db,
                        table,
                        if available.is_empty() {
                            String::new()
                        } else {
                            format!("; available partitions: {}", available)
                        }
                    )));
                };

                let files = groups
                    .get(&Some(matched_key.clone()))
                    .cloned()
                    .unwrap_or_default();
                Ok(Some((matched_key, files)))
            },
        )?;
        let Some((matched_key, files)) = matched else {
            return Ok(CompactStats::default());
        };

        // Permits before the partition lock, heavy decode/sort/write outside
        // it — same split as `compact_table`'s partition workers.
        let stats = {
            let prepared = {
                let _memory = compaction_memory_gate();
                let _cpu_permit = crate::common::work_limiter::acquire_cpu();
                let _io_permit = crate::common::work_limiter::acquire_io();
                self.prepare_compact_segment_group(
                    db,
                    table,
                    &table_dir,
                    Some(matched_key.as_str()),
                    files,
                    &schema,
                    &cluster_key_indices,
                    &cluster_key_indices,
                    output_row_group_rows,
                    ttl,
                    compression_level,
                )?
            };
            self.segment_mgr
                .with_partition_mutation_lock(db, table, &matched_key, || {
                    self.commit_prepared_compaction(
                        db,
                        table,
                        &table_dir,
                        Some(matched_key.as_str()),
                        prepared,
                    )
                })?
        };

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            requested_partition = partition_key,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            segments_read = stats.segments_read,
            segments_written = stats.segments_written,
            elapsed_ms = start_time.elapsed().as_millis(),
            "requested partition optimization completed"
        );

        Ok(stats)
    }

    /// Commit phase, run under the target partition's mutation lock:
    /// re-validate that no other mutator touched the input files while the
    /// heavy phase ran unlocked, then either remove the (fully deleted)
    /// inputs or atomically swap the replacement segment in. Both steps are
    /// metadata-only, so the lock is held for milliseconds, not for the
    /// length of the rewrite I/O.
    /// Commit-time guards shared by both commit arms: the rewrite's input
    /// identity must still hold, and no transaction may have pinned the table
    /// after the heavy phase (a pin that never touched segment files is
    /// invisible to the identity check, and committing under one would strand
    /// the transaction's later COMMIT on a vanished path). Both checks run
    /// under the same partition mutation lock the pin registration uses, so
    /// they cannot race. Returns true when the arm must abort: the caller
    /// runs its own cleanup and reports a partial stats value instead of
    /// committing.
    fn commit_guards_block(
        &self,
        db: &str,
        table: &str,
        partition_label: &str,
        segment_files: &[PathBuf],
        identities: &[SegmentFileIdentity],
        abort_label: &str,
    ) -> bool {
        if !compaction_inputs_unchanged(&self.segment_mgr, segment_files, identities) {
            tracing::info!(
                db,
                table,
                partition_key = partition_label,
                "compact inputs changed while rewriting; {abort_label}"
            );
            return true;
        }
        if self
            .segment_mgr
            .txn_mutations()
            .is_some_and(|registry| registry.table_has_pins(db, table))
        {
            tracing::info!(
                db,
                table,
                partition_key = partition_label,
                "transaction pinned the table during compaction; {abort_label}"
            );
            return true;
        }
        false
    }

    fn commit_prepared_compaction(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        partition_key: Option<&str>,
        prepared: PreparedCompaction,
    ) -> Result<CompactStats> {
        let stats =
            self.commit_prepared_compaction_inner(db, table, table_dir, partition_key, prepared)?;
        let mut committed = self.committed.lock().unwrap();
        committed.segments_read += stats.segments_read;
        committed.segments_written += stats.segments_written;
        committed.rows_written += stats.rows_written;
        committed.rows_dropped += stats.rows_dropped;
        committed.rows_dropped_ttl += stats.rows_dropped_ttl;
        Ok(stats)
    }

    fn commit_prepared_compaction_inner(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        partition_key: Option<&str>,
        prepared: PreparedCompaction,
    ) -> Result<CompactStats> {
        let partition_label = partition_key.unwrap_or("<unpartitioned>");
        match prepared {
            PreparedCompaction::Skipped => Ok(CompactStats::default()),
            PreparedCompaction::AllDropped {
                segment_files,
                identities,
                segments_read,
                rows_dropped,
                rows_dropped_ttl,
            } => {
                if self.commit_guards_block(
                    db,
                    table,
                    partition_label,
                    &segment_files,
                    &identities,
                    "deferring empty-input cleanup",
                ) {
                    return Ok(CompactStats {
                        segments_read,
                        ..CompactStats::default()
                    });
                }
                self.segment_mgr
                    .retire_segments(table_dir, &segment_files)?;
                // Persisted unique-index snapshots still point at the removed
                // segments. Bump the content epoch so every probe refreshes
                // (or falls back) instead of trusting locations that no longer
                // exist — including after a crash before the caller's rebuild.
                // (Removal-only: no replace primitive runs here, so the bump
                // cannot be sunk into one.)
                self.segment_mgr.note_segments_changed(db, table);
                tracing::info!(
                    db,
                    table,
                    table_full_name = %format!("{}.{}", db, table),
                    partition_key = partition_label,
                    segments_read,
                    rows_dropped,
                    "compact partition removed empty/deleted segments"
                );
                Ok(CompactStats {
                    segments_read,
                    rows_dropped,
                    rows_dropped_ttl,
                    ..CompactStats::default()
                })
            }
            PreparedCompaction::Replacement {
                _cleanup,
                segment_files,
                identities,
                tmp_path,
                final_path,
                segments_read,
                rows_to_write,
                rows_dropped,
                rows_dropped_ttl,
            } => {
                if self.commit_guards_block(
                    db,
                    table,
                    partition_label,
                    &segment_files,
                    &identities,
                    "discarding prepared replacement",
                ) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                    return Ok(CompactStats {
                        segments_read,
                        ..CompactStats::default()
                    });
                }
                // A concurrent compaction round of the same partition may
                // have committed its own replacement between our discovery
                // and this commit (both heavy phases ran unlocked); our
                // inputs survived validation only when the sets were
                // disjoint, so re-target a free final name instead of
                // failing the rename.
                let mut final_path = final_path;
                if final_path.exists() {
                    final_path = self
                        .segment_mgr
                        .next_replacement_segment_path(table_dir, partition_key)?;
                }
                self.segment_mgr.replace_segments_with_tmp(
                    &tmp_path,
                    &final_path,
                    &segment_files,
                )?;
                // The replacement no longer contains the rows the persisted
                // unique-index snapshots point at, and their recorded
                // watermarks are meaningless against the merged segment.
                // `replace_segments_with_tmp` bumps the content epoch itself,
                // so every probe refreshes until the caller rebuilds.

                tracing::info!(
                    db,
                    table,
                    table_full_name = %format!("{}.{}", db, table),
                    partition_key = partition_label,
                    final_path = %final_path.display(),
                    segments_read,
                    rows_written = rows_to_write,
                    rows_dropped,
                    "compact partition completed"
                );

                Ok(CompactStats {
                    segments_read,
                    segments_written: 1,
                    rows_written: rows_to_write as u64,
                    rows_dropped,
                    rows_dropped_ttl,
                })
            }
        }
    }

    fn get_cluster_key_indices(
        &self,
        table_meta: &TableMeta,
        schema: &Schema,
    ) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        for key_col in &table_meta.cluster_key {
            let idx = schema.index_of(key_col).ok_or_else(|| {
                TaibaiError::Sql(format!("cluster key column '{}' not found", key_col))
            })?;
            indices.push(idx);
        }
        Ok(indices)
    }
}

/// Heavy rewrites share one process-wide memory allowance. Acquire on the
/// calling thread, before CPU/IO permits and outside mutation locks; columns
/// still decode/compress in parallel without blocking Rayon workers on permits.
fn compaction_memory_gate() -> std::sync::MutexGuard<'static, ()> {
    static GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    GATE.lock().unwrap_or_else(|e| e.into_inner())
}

fn group_segments_by_partition(
    segment_files: Vec<PathBuf>,
) -> BTreeMap<Option<String>, Vec<PathBuf>> {
    let mut groups = BTreeMap::new();
    for path in segment_files {
        let partition_key = path
            .file_name()
            .and_then(|name| parse_partition_key(&name.to_string_lossy()));
        groups
            .entry(partition_key)
            .or_insert_with(Vec::new)
            .push(path);
    }
    groups
}

pub(crate) fn partition_key_candidates(raw: &str) -> Vec<String> {
    let key = raw.trim();
    let mut candidates = vec![key.to_string()];
    if key.len() > 1
        && key.as_bytes()[0].eq_ignore_ascii_case(&b'p')
        && key[1..].chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        candidates.push(key[1..].to_string());
    }
    candidates
}

#[cfg(test)]
#[path = "compactor_test.rs"]
mod tests;
