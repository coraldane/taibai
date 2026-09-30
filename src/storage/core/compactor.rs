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

/// Surviving rows of an unsorted partition as (row group, row) references,
/// with deleted and TTL-expired rows already purged.
struct UnsortedRowRefs {
    row_refs: Vec<RowRef>,
    source_row_groups: Vec<SourceRowGroup>,
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
pub(crate) static CLUSTERED_MERGE_BUDGET_OVERRIDE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
fn clustered_merge_budget_bytes() -> usize {
    let override_value = CLUSTERED_MERGE_BUDGET_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
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

pub struct Compactor {
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
}

impl Compactor {
    pub fn new(segment_mgr: Arc<SegmentManager>, data_dir: PathBuf) -> Self {
        Self {
            segment_mgr,
            data_dir,
        }
    }

    /// See [`COMPACTION_HEAVY_DELAY_MS`].
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
        let partition_groups = self.segment_mgr.with_table_mutation_lock(db, table, || {
            if seal_active {
                self.segment_mgr
                    .seal_table_active_under_table_lock(db, table)?;
            }
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

        let mut stats = CompactStats {
            segments_read: partition_groups.iter().map(|(_, files)| files.len()).sum(),
            ..CompactStats::default()
        };

        let partition_count = partition_groups.len();
        let partition_stats = partition_groups
            .into_par_iter()
            .map(|(partition_key, files)| -> Result<CompactStats> {
                let lock_key = partition_key.as_deref().unwrap_or("");
                // Permits are acquired BEFORE the partition lock, in the same
                // order as query scans (cpu then io). Sleeping on the global
                // work limiter while holding the partition lock would freeze
                // that partition's INSERTs for the whole queue delay.
                let prepared = {
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
        let matched = self.segment_mgr.with_table_mutation_lock(db, table, || {
            for candidate in &candidates {
                self.segment_mgr
                    .seal_partition_active_under_table_lock(db, table, candidate)?;
            }
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
        })?;
        let Some((matched_key, files)) = matched else {
            return Ok(CompactStats::default());
        };

        // Permits before the partition lock, heavy decode/sort/write outside
        // it — same split as `compact_table`'s partition workers.
        let stats = {
            let prepared = {
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

    /// Collect the surviving rows of an unsorted partition as (row group,
    /// row) references, purging deleted and TTL-expired rows.
    fn collect_unsorted_row_refs(
        &self,
        segment_files: &[PathBuf],
        ttl: Option<(usize, i64)>,
        now_us: i64,
    ) -> Result<UnsortedRowRefs> {
        let mut row_refs = Vec::new();
        let mut source_row_groups = Vec::new();
        let mut deleted_rows = 0u64;
        let mut ttl_dropped = 0u64;
        let duration = ttl.map(|(_, secs)| secs.saturating_mul(1_000_000));
        for seg_path in segment_files {
            let reader = self.segment_mgr.open_reader(seg_path)?;
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                let batch_idx = source_row_groups.len();
                let row_count = reader.row_group_rows(rg_idx) as usize;
                // TTL purge: read just the TTL column and drop rows the
                // read-time filter would hide.
                let ttl_col = ttl.and_then(|(idx, _)| reader.read_column(rg_idx, idx).ok());
                for row_idx in 0..row_count {
                    let row_id = segment_row_base + row_idx as u64;
                    if reader.is_row_deleted(row_id) {
                        deleted_rows += 1;
                        continue;
                    }
                    if let (Some(col), Some(duration)) = (&ttl_col, duration) {
                        if !ttl_row_visible(&col.scalar(row_idx), duration, now_us) {
                            deleted_rows += 1;
                            ttl_dropped += 1;
                            continue;
                        }
                    }
                    row_refs.push(RowRef { batch_idx, row_idx });
                }
                segment_row_base += row_count as u64;
                source_row_groups.push(SourceRowGroup {
                    segment_path: seg_path.clone(),
                    row_group_idx: rg_idx,
                });
            }
        }
        Ok(UnsortedRowRefs {
            row_refs,
            source_row_groups,
            deleted_rows,
            ttl_dropped,
        })
    }

    /// Build the per-row-group read sources of the clustered path and the
    /// global row count they cover (the partition's row space for the sort).
    fn collect_clustered_sources(
        &self,
        segment_files: &[PathBuf],
    ) -> Result<(Vec<ClusterReadSource>, usize)> {
        let mut sources = Vec::new();
        let mut global_row_base = 0usize;
        for seg_path in segment_files {
            let reader = Arc::new(self.segment_mgr.open_reader(seg_path)?);
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                let row_count = reader.row_group_rows(rg_idx) as usize;
                sources.push(ClusterReadSource {
                    reader: Arc::clone(&reader),
                    row_group_idx: rg_idx,
                    segment_row_base,
                    global_row_base,
                    row_count,
                });
                segment_row_base += row_count as u64;
                global_row_base = global_row_base.checked_add(row_count).ok_or_else(|| {
                    TaibaiError::Storage("row count overflow during optimization".to_string())
                })?;
            }
        }
        Ok((sources, global_row_base))
    }

    /// Decode every source column into one contiguous partition batch,
    /// purge deleted and TTL-expired rows from the global index, and sort it
    /// by the cluster key. In-memory only: the caller has already checked
    /// the memory budget and the u32 row-count bound.
    fn decode_clustered_partition(
        &self,
        db: &str,
        table: &str,
        partition_label: &str,
        schema: &Schema,
        sources: &[ClusterReadSource],
        global_row_base: usize,
        sort_key_indices: &[usize],
        ttl: Option<(usize, i64)>,
        decode_started: Instant,
    ) -> Result<ClusteredPartition> {
        let mut sorted_indices = Vec::with_capacity(global_row_base);
        let mut deleted_rows = 0u64;
        let mut ttl_dropped = 0u64;
        for source in sources {
            for local_row_idx in 0..source.row_count {
                let segment_row_id = source.segment_row_base + local_row_idx as u64;
                if source.reader.is_row_deleted(segment_row_id) {
                    deleted_rows += 1;
                } else {
                    sorted_indices.push((source.global_row_base + local_row_idx) as u32);
                }
            }
        }

        // Parallelize by column, like the Go ReadColumnsDirect path. Each
        // worker builds one contiguous destination column and moves decoded
        // variable-width buffers into it without cloning strings.
        let columns = schema
            .columns
            .par_iter()
            .enumerate()
            .map(|(column_idx, column_def)| -> Result<ColumnArray> {
                let mut destination =
                    ColumnArray::with_capacity(column_def.data_type, global_row_base);
                for source in sources {
                    let decoded = source
                        .reader
                        .read_column(source.row_group_idx, column_idx)?;
                    destination.append_owned(decoded)?;
                }
                Ok(destination)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(schema.clone(), columns)?;

        // TTL purge: physically drop rows the read-time TTL filter would
        // hide anyway (ts + duration <= now, or a non-timestamp TTL value),
        // keeping compaction output and query visibility in sync.
        if let Some((ttl_col_idx, duration_secs)) = ttl {
            if let Some(ttl_col) = batch.column(ttl_col_idx) {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as i64;
                let duration = duration_secs.saturating_mul(1_000_000);
                let before = sorted_indices.len();
                sorted_indices.retain(|&global_row| {
                    ttl_row_visible(&ttl_col.scalar(global_row as usize), duration, now)
                });
                let ttl_removed = (before - sorted_indices.len()) as u64;
                deleted_rows += ttl_removed;
                ttl_dropped += ttl_removed;
            }
        }

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            source_row_groups = sources.len(),
            rows_to_sort = sorted_indices.len(),
            elapsed_ms = decode_started.elapsed().as_millis(),
            "compact partition decoded contiguous columns"
        );
        let sort_started = Instant::now();
        sort_clustered_row_indices(&mut sorted_indices, &batch, sort_key_indices);
        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            rows_sorted = sorted_indices.len(),
            elapsed_ms = sort_started.elapsed().as_millis(),
            "compact partition sorted cluster keys"
        );

        Ok(ClusteredPartition {
            batch,
            sorted_indices,
            deleted_rows,
            ttl_dropped,
        })
    }

    /// Heavy phase of one partition compaction, run WITHOUT the partition
    /// mutation lock: decode every source row group, purge TTL/deleted rows,
    /// optionally cluster-sort, and write the replacement segment to a tmp
    /// path. Input identities are recorded before any read so the commit
    /// phase can detect concurrent mutations of the inputs.
    #[allow(clippy::too_many_arguments)]
    fn prepare_compact_segment_group(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        partition_key: Option<&str>,
        segment_files: Vec<PathBuf>,
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        output_row_group_rows: usize,
        ttl: Option<(usize, i64)>,
        compression_level: i32,
    ) -> Result<PreparedCompaction> {
        let segment_files: Vec<PathBuf> = segment_files
            .into_iter()
            .filter(|path| path.exists())
            .collect();
        if segment_files.is_empty() {
            return Ok(PreparedCompaction::Skipped);
        }
        let identities = segment_files
            .iter()
            .map(|path| compaction_input_identity(&self.segment_mgr, path))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                TaibaiError::Storage(format!(
                    "compaction input unreadable before rewrite on {}.{} partition {}",
                    db,
                    table,
                    partition_key.unwrap_or("<none>")
                ))
            })?;
        let segments_read = segment_files.len();
        let partition_label = partition_key.unwrap_or("<unpartitioned>");

        // A clean single segment can only be skipped when this operation has
        // no physical ordering requirement and its row groups are already
        // compact. A segment can contain thousands of tiny row groups after
        // many small flushes; skipping it makes OPTIMIZE unable to repair
        // the main source of read amplification.
        if sort_key_indices.is_empty() && segment_files.len() == 1 {
            let reader = self.segment_mgr.open_reader_cached(&segment_files[0])?;
            let compact_row_groups =
                reader.row_count().div_ceil(output_row_group_rows as u64) as usize;
            let needs_row_group_compaction = reader.num_row_groups() > compact_row_groups.max(1);
            if reader.deleted_row_count() == 0 && !needs_row_group_compaction {
                tracing::info!(
                    db,
                    table,
                    partition_key = partition_key.unwrap_or("<none>"),
                    segment = %segment_files[0].display(),
                    "skipping clean single-segment optimization"
                );
                return Ok(PreparedCompaction::Skipped);
            }
        }

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            segments = segment_files.len(),
            "compact partition started"
        );

        Self::sleep_heavy_phase_delay();

        // The clustered path follows the original Go implementation's
        // index-only sort design: concatenate each decoded column into one
        // global column, sort compact u32 row indices, then gather directly
        // from those global arrays. This avoids carrying a 16-byte
        // (row-group,row) reference and rebuilding a source map for every
        // output chunk.
        let mut preloaded_batch = None;
        let mut source_row_groups = Vec::new();
        let mut row_refs = Vec::new();
        let mut sorted_indices = Vec::new();
        let source_row_group_count;
        // Both collection strategies assign these before any use below.
        let deleted_rows: u64;
        let ttl_dropped: u64;

        if sort_key_indices.is_empty() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            let unsorted = self.collect_unsorted_row_refs(&segment_files, ttl, now)?;
            row_refs = unsorted.row_refs;
            source_row_groups = unsorted.source_row_groups;
            deleted_rows = unsorted.deleted_rows;
            ttl_dropped = unsorted.ttl_dropped;
            source_row_group_count = source_row_groups.len();
        } else {
            let decode_started = Instant::now();
            let (sources, global_row_base) = self.collect_clustered_sources(&segment_files)?;
            source_row_group_count = sources.len();

            let estimated_width = estimated_row_width_bytes(schema);
            let in_memory_budget_rows =
                (clustered_merge_budget_bytes() / estimated_width).max(output_row_group_rows * 4);
            if global_row_base > in_memory_budget_rows {
                // Partition exceeds the in-memory budget: spill sorted runs
                // to temp files and k-way merge them into the output, keeping
                // peak memory proportional to the budget instead of the
                // partition.
                let final_path = self
                    .segment_mgr
                    .next_replacement_segment_path(table_dir, partition_key)?;
                let tmp_path = store::replacement_tmp_path(&final_path, "compact_tmp");
                let (rows_written, dropped, dropped_ttl) = self.prepare_clustered_external(
                    db,
                    table,
                    partition_label,
                    &tmp_path,
                    &final_path,
                    schema,
                    sort_key_indices,
                    bloom_column_indices,
                    &segment_files,
                    ttl,
                    output_row_group_rows,
                    compression_level,
                )?;
                if rows_written == 0 {
                    let _ = std::fs::remove_file(&tmp_path);
                    return Ok(PreparedCompaction::AllDropped {
                        segment_files,
                        identities,
                        segments_read,
                        rows_dropped: dropped,
                        rows_dropped_ttl: dropped_ttl,
                    });
                }
                tracing::info!(
                    db,
                    table,
                    table_full_name = %format!("{}.{}", db, table),
                    partition_key = partition_label,
                    rows = rows_written,
                    rows_dropped = dropped,
                    elapsed_ms = decode_started.elapsed().as_millis(),
                    "clustered optimization finished via external merge sort"
                );
                return Ok(PreparedCompaction::Replacement {
                    segment_files,
                    identities,
                    tmp_path,
                    final_path,
                    segments_read,
                    rows_to_write: rows_written,
                    rows_dropped: dropped,
                    rows_dropped_ttl: dropped_ttl,
                });
            }
            if global_row_base > u32::MAX as usize {
                return Err(TaibaiError::Storage(format!(
                    "clustered optimization supports at most {} rows per partition",
                    u32::MAX
                )));
            }

            let clustered = self.decode_clustered_partition(
                db,
                table,
                partition_label,
                schema,
                &sources,
                global_row_base,
                sort_key_indices,
                ttl,
                decode_started,
            )?;
            preloaded_batch = Some(clustered.batch);
            sorted_indices = clustered.sorted_indices;
            deleted_rows = clustered.deleted_rows;
            ttl_dropped = clustered.ttl_dropped;
        }

        let rows_to_write = if sort_key_indices.is_empty() {
            row_refs.len()
        } else {
            sorted_indices.len()
        };
        if rows_to_write == 0 {
            return Ok(PreparedCompaction::AllDropped {
                segment_files,
                identities,
                segments_read,
                rows_dropped: deleted_rows,
                rows_dropped_ttl: ttl_dropped,
            });
        }

        let final_path = self
            .segment_mgr
            .next_replacement_segment_path(table_dir, partition_key)?;
        let tmp_path = store::replacement_tmp_path(&final_path, "compact_tmp");

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            source_row_groups = source_row_group_count,
            rows_to_write,
            tmp_path = %tmp_path.display(),
            final_path = %final_path.display(),
            "compact partition writing replacement segment"
        );

        let write_result = self.write_compacted_segment(
            db,
            table,
            partition_label,
            &tmp_path,
            schema,
            sort_key_indices,
            bloom_column_indices,
            preloaded_batch.as_ref(),
            &source_row_groups,
            &row_refs,
            &sorted_indices,
            output_row_group_rows,
            compression_level,
        );
        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
            tracing::warn!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                tmp_path = %tmp_path.display(),
                error = %err,
                "compact partition write failed"
            );
            return Err(err);
        }

        Ok(PreparedCompaction::Replacement {
            segment_files,
            identities,
            tmp_path,
            final_path,
            segments_read,
            rows_to_write,
            rows_dropped: deleted_rows,
            rows_dropped_ttl: ttl_dropped,
        })
    }

    /// Commit phase, run under the target partition's mutation lock:
    /// re-validate that no other mutator touched the input files while the
    /// heavy phase ran unlocked, then either remove the (fully deleted)
    /// inputs or atomically swap the replacement segment in. Both steps are
    /// metadata-only, so the lock is held for milliseconds, not for the
    /// length of the rewrite I/O.
    fn commit_prepared_compaction(
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
                if !compaction_inputs_unchanged(&self.segment_mgr, &segment_files, &identities) {
                    tracing::info!(
                        db,
                        table,
                        partition_key = partition_label,
                        "compact inputs changed while rewriting; deferring empty-input cleanup"
                    );
                    return Ok(CompactStats {
                        segments_read,
                        ..CompactStats::default()
                    });
                }
                // Re-check transaction pins at commit time. A BEGIN...DELETE/
                // UPDATE that started after the heavy phase registered pins
                // without touching segment files, so the identity validation
                // above cannot see it. Committing under a pin makes the
                // transaction's later COMMIT fail on a vanished path and the
                // retry fail forever on the same stale plan; this check runs
                // under the same partition mutation lock the registration
                // uses, so it cannot race.
                if self
                    .segment_mgr
                    .txn_mutations()
                    .is_some_and(|registry| registry.table_has_pins(db, table))
                {
                    tracing::info!(
                        db,
                        table,
                        partition_key = partition_label,
                        "transaction pinned the table during compaction; deferring empty-input cleanup"
                    );
                    return Ok(CompactStats {
                        segments_read,
                        ..CompactStats::default()
                    });
                }
                for old_seg in &segment_files {
                    self.segment_mgr.remove_segment_file(old_seg)?;
                }
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
                segment_files,
                identities,
                tmp_path,
                final_path,
                segments_read,
                rows_to_write,
                rows_dropped,
                rows_dropped_ttl,
            } => {
                if !compaction_inputs_unchanged(&self.segment_mgr, &segment_files, &identities) {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                    tracing::info!(
                        db,
                        table,
                        partition_key = partition_label,
                        "compact inputs changed while rewriting; discarding prepared replacement"
                    );
                    return Ok(CompactStats {
                        segments_read,
                        ..CompactStats::default()
                    });
                }
                // Same commit-time pin re-check as the AllDropped arm: a pin
                // registered after the heavy phase references the segments
                // this replacement is about to remove, and committing would
                // deadlock the transaction's COMMIT on a missing file.
                if self
                    .segment_mgr
                    .txn_mutations()
                    .is_some_and(|registry| registry.table_has_pins(db, table))
                {
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                    tracing::info!(
                        db,
                        table,
                        partition_key = partition_label,
                        "transaction pinned the table during compaction; discarding prepared replacement"
                    );
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
                let mut tmp_path = tmp_path;
                let mut final_path = final_path;
                if final_path.exists() {
                    final_path = self
                        .segment_mgr
                        .next_replacement_segment_path(table_dir, partition_key)?;
                    let retargeted_tmp = store::replacement_tmp_path(&final_path, "compact_tmp");
                    std::fs::rename(&tmp_path, &retargeted_tmp)?;
                    tmp_path = retargeted_tmp;
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

    #[allow(clippy::too_many_arguments)]
    fn write_compacted_segment(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        path: &Path,
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        preloaded_batch: Option<&RecordBatch>,
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
        sorted_indices: &[u32],
        output_row_group_rows: usize,
        compression_level: i32,
    ) -> Result<()> {
        let mut writer = SegmentWriter::create_with_bloom_columns(
            path,
            schema.clone(),
            output_row_group_rows,
            bloom_column_indices,
            compression_level,
        )?;
        let write_gather_chunk_rows = output_row_group_rows.max(1);
        let write_progress_rows = write_gather_chunk_rows.saturating_mul(16).max(1);
        let mut rows_written = 0usize;
        let projection = (0..schema.num_columns()).collect::<Vec<_>>();
        // Non-sorting cleanup preserves source order, so a small cache remains
        // effective there. Clustered OPTIMIZE uses the source batches decoded
        // during its sort pass and performs no I/O in the write/gather phase.
        let mut row_group_cache = RowGroupCache::new(32, projection);

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key,
            source_row_groups = source_row_groups.len(),
            preloaded_contiguous_columns = preloaded_batch.is_some(),
            fallback_cache_capacity = row_group_cache.capacity(),
            chunk_rows = write_gather_chunk_rows,
            output_row_group_rows,
            compression_level,
            "compact write using index-only gather"
        );

        let total_rows = if sort_key_indices.is_empty() {
            for chunk in row_refs.chunks(write_gather_chunk_rows) {
                let batch = self.gather_cached_chunk(
                    schema,
                    source_row_groups,
                    chunk,
                    &mut row_group_cache,
                )?;
                rows_written += batch.num_rows();
                writer.write_batch_parallel(&batch)?;
            }
            row_refs.len()
        } else {
            let source = preloaded_batch.ok_or_else(|| {
                TaibaiError::Storage(
                    "missing contiguous source columns for clustered optimization".to_string(),
                )
            })?;
            for chunk in sorted_indices.chunks(write_gather_chunk_rows) {
                let batch = gather_global_chunk(schema, source, chunk)?;
                rows_written += batch.num_rows();
                writer.write_batch_parallel(&batch)?;
                if rows_written.is_multiple_of(write_progress_rows) {
                    let progress_pct = rows_written as f64 * 100.0 / sorted_indices.len() as f64;
                    tracing::info!(
                        db,
                        table,
                        table_full_name = %format!("{}.{}", db, table),
                        partition_key,
                        rows_written,
                        total_rows = sorted_indices.len(),
                        progress_pct = format_args!("{:.2}", progress_pct),
                        "compact write progress"
                    );
                }
            }
            sorted_indices.len()
        };

        if sort_key_indices.is_empty() && total_rows > 0 && rows_written != total_rows {
            let progress_pct = rows_written as f64 * 100.0 / total_rows as f64;
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key,
                rows_written,
                total_rows,
                progress_pct = format_args!("{:.2}", progress_pct),
                "compact write progress"
            );
        }

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key,
            rows_written,
            fallback_cache_hits = row_group_cache.hits(),
            fallback_cache_misses = row_group_cache.misses(),
            fallback_cached_row_groups = row_group_cache.len(),
            "compact write gathered rows"
        );

        writer.seal()
    }

    /// External-merge clustered rewrite: build bounded sorted runs, merge
    /// them (in waves when necessary) into the output tmp segment. Returns
    /// (rows written to the output, rows dropped as deleted/expired).
    #[allow(clippy::too_many_arguments)]
    fn prepare_clustered_external(
        &self,
        db: &str,
        table: &str,
        partition_label: &str,
        tmp_path: &Path,
        final_path: &Path,
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        segment_files: &[PathBuf],
        ttl: Option<(usize, i64)>,
        output_row_group_rows: usize,
        compression_level: i32,
    ) -> Result<(usize, u64, u64)> {
        let estimated_width = estimated_row_width_bytes(schema);
        // Bound the run to the budget (no row-group-multiplier floor: wide
        // rows must not lift the run past it). A small floor keeps the run
        // count sane for very narrow rows.
        let run_rows = (clustered_merge_budget_bytes() / estimated_width)
            .max(4096)
            .max(1);

        // ---- Pass A: bounded chunks, each sorted and spilled to a run ----
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let duration = ttl.map(|(_, secs)| secs.saturating_mul(1_000_000));

        let mut source_row_groups: Vec<SourceRowGroup> = Vec::new();
        let mut rows_per_source: Vec<usize> = Vec::new();
        // Segment-local start row of each source row group, computed during
        // the same discovery pass (recomputing it per source below made the
        // row-ref loop quadratic in a segment's row-group count).
        let mut row_base_per_source: Vec<u64> = Vec::new();
        for seg_path in segment_files {
            let reader = self.segment_mgr.open_reader(seg_path)?;
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                source_row_groups.push(SourceRowGroup {
                    segment_path: seg_path.clone(),
                    row_group_idx: rg_idx,
                });
                rows_per_source.push(reader.row_group_rows(rg_idx) as usize);
                row_base_per_source.push(segment_row_base);
                segment_row_base += reader.row_group_rows(rg_idx) as u64;
            }
        }

        let projection = (0..schema.num_columns()).collect::<Vec<_>>();
        let mut cache = RowGroupCache::new(16, projection);
        let mut refs: Vec<RowRef> = Vec::with_capacity(run_rows);
        let mut run_paths: Vec<PathBuf> = Vec::new();
        let mut run_seq = 0usize;
        let mut dropped = 0u64;
        let mut dropped_ttl = 0u64;

        let run_cleanup = |paths: &[PathBuf]| {
            for path in paths {
                let _ = std::fs::remove_file(path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(path);
            }
        };
        let flush_run = |cache: &mut RowGroupCache,
                         run_paths: &mut Vec<PathBuf>,
                         run_seq: &mut usize,
                         refs: &mut Vec<RowRef>|
         -> Result<()> {
            if refs.is_empty() {
                return Ok(());
            }
            let batch = self.gather_cached_chunk(schema, &source_row_groups, refs, cache)?;
            let mut indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
            sort_clustered_row_indices(&mut indices, &batch, sort_key_indices);
            let sorted = gather_global_chunk(schema, &batch, &indices)?;
            let run_path = merge_run_path(final_path, *run_seq);
            *run_seq += 1;
            let written = (|| -> Result<()> {
                let mut writer = SegmentWriter::create(&run_path, schema.clone(), run_rows, 0)?;
                writer.write_batch_parallel(&sorted)?;
                writer.seal()?;
                Ok(())
            })();
            if written.is_err() {
                let _ = std::fs::remove_file(&run_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&run_path);
                return written;
            }
            run_paths.push(run_path);
            refs.clear();
            Ok(())
        };

        for (source_idx, source) in source_row_groups.iter().enumerate() {
            let row_count = rows_per_source[source_idx];
            let reader = self.segment_mgr.open_reader(&source.segment_path)?;
            let segment_row_base = row_base_per_source[source_idx];
            let ttl_col =
                ttl.and_then(|(idx, _)| reader.read_column(source.row_group_idx, idx).ok());
            for row_idx in 0..row_count {
                let row_id = segment_row_base + row_idx as u64;
                if reader.is_row_deleted(row_id) {
                    dropped += 1;
                    continue;
                }
                if let (Some(col), Some(duration)) = (&ttl_col, duration) {
                    if !ttl_row_visible(&col.scalar(row_idx), duration, now) {
                        dropped += 1;
                        dropped_ttl += 1;
                        continue;
                    }
                }
                refs.push(RowRef {
                    batch_idx: source_idx,
                    row_idx,
                });
                if refs.len() >= run_rows {
                    flush_run(&mut cache, &mut run_paths, &mut run_seq, &mut refs)?;
                }
            }
        }
        if let Err(error) = flush_run(&mut cache, &mut run_paths, &mut run_seq, &mut refs) {
            run_cleanup(&run_paths);
            return Err(error);
        }

        if run_paths.is_empty() {
            return Ok((0, dropped, dropped_ttl));
        }

        // ---- Pass B: k-way merge (waves when runs exceed the fan-in) ----
        let cleanup_runs = |paths: &[PathBuf]| {
            for path in paths {
                let _ = std::fs::remove_file(path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(path);
            }
        };
        let mut wave = 0usize;
        let mut current_runs = run_paths;
        let mut created_in_round: Vec<PathBuf> = Vec::new();
        let result = (|| -> Result<usize> {
            while current_runs.len() > CLUSTERED_MERGE_MAX_RUNS {
                wave += 1;
                let mut next_runs = Vec::new();
                let mut next_seq = 0usize;
                for group in current_runs.chunks(CLUSTERED_MERGE_MAX_RUNS) {
                    let merged_path = merge_wave_path(final_path, wave, next_seq);
                    next_seq += 1;
                    let mut writer = SegmentWriter::create(
                        &merged_path,
                        schema.clone(),
                        output_row_group_rows,
                        0,
                    )?;
                    self.merge_runs_into_writer(
                        group,
                        schema,
                        sort_key_indices,
                        output_row_group_rows,
                        &mut writer,
                    )?;
                    writer.seal()?;
                    created_in_round.push(merged_path.clone());
                    next_runs.push(merged_path);
                }
                cleanup_runs(&current_runs);
                current_runs = next_runs;
            }

            let mut writer = SegmentWriter::create_with_bloom_columns(
                tmp_path,
                schema.clone(),
                output_row_group_rows,
                bloom_column_indices,
                compression_level,
            )?;
            let written = self.merge_runs_into_writer(
                &current_runs,
                schema,
                sort_key_indices,
                output_row_group_rows,
                &mut writer,
            )?;
            writer.seal()?;
            Ok(written)
        })();
        cleanup_runs(&current_runs);
        if result.is_err() {
            run_cleanup(&created_in_round);
        }
        let written = result?;
        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            runs = current_runs.len(),
            rows_written = written,
            "clustered external merge completed"
        );
        Ok((written, dropped, dropped_ttl))
    }

    /// K-way merge of sorted run files into `writer`, gathering rows through
    /// a bounded row-group cache.
    fn merge_runs_into_writer(
        &self,
        run_paths: &[PathBuf],
        schema: &Schema,
        sort_key_indices: &[usize],
        output_row_group_rows: usize,
        writer: &mut SegmentWriter,
    ) -> Result<usize> {
        // Streams are files; each file's row groups occupy contiguous slots
        // in source_row_groups, so (source_idx, row_idx) ordering matches the
        // original global row order for stable tie-breaks.
        let mut source_row_groups: Vec<SourceRowGroup> = Vec::new();
        let mut rows_per_source: Vec<usize> = Vec::new();
        let mut file_bounds: Vec<(usize, usize)> = Vec::new(); // [start, end) source slots
        for path in run_paths {
            let reader = self.segment_mgr.open_reader(path)?;
            let start = source_row_groups.len();
            for rg_idx in 0..reader.num_row_groups() {
                source_row_groups.push(SourceRowGroup {
                    segment_path: path.clone(),
                    row_group_idx: rg_idx,
                });
                rows_per_source.push(reader.row_group_rows(rg_idx) as usize);
            }
            file_bounds.push((start, source_row_groups.len()));
        }

        let projection = (0..schema.num_columns()).collect::<Vec<_>>();
        let mut cache = RowGroupCache::new(32, projection);

        struct MergeHead<'k> {
            keys: Vec<crate::types::scalar::ScalarValue>,
            source_idx: usize,
            row_idx: usize,
            sort_keys: &'k [usize],
        }
        impl PartialEq for MergeHead<'_> {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == std::cmp::Ordering::Equal
            }
        }
        impl Eq for MergeHead<'_> {}
        impl PartialOrd for MergeHead<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for MergeHead<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                for (col, _key_idx) in self.sort_keys.iter().enumerate() {
                    let Some(left) = self.keys.get(col) else {
                        continue;
                    };
                    let Some(right) = other.keys.get(col) else {
                        continue;
                    };
                    let ord =
                        crate::storage::index::sort_key::compare_scalars_for_sort(left, right);
                    if ord != std::cmp::Ordering::Equal {
                        return ord
                            // Stable, deterministic tie-break: original row
                            // order.
                            .then(self.source_idx.cmp(&other.source_idx))
                            .then(self.row_idx.cmp(&other.row_idx));
                    }
                }
                self.source_idx
                    .cmp(&other.source_idx)
                    .then(self.row_idx.cmp(&other.row_idx))
            }
        }

        // BinaryHeap is a max-heap; reverse for smallest-first.
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap: BinaryHeap<Reverse<MergeHead>> = BinaryHeap::new();

        let materialize_keys = |cache: &mut RowGroupCache,
                                source_idx: usize,
                                row_idx: usize|
         -> Result<Vec<crate::types::scalar::ScalarValue>> {
            let source = &source_row_groups[source_idx];
            let batch = cache.get(source, &self.segment_mgr)?;
            Ok(sort_key_indices
                .iter()
                .map(|&col| {
                    batch
                        .column(col)
                        .map(|column| column.scalar(row_idx))
                        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None))
                })
                .collect())
        };

        for file_idx in 0..file_bounds.len() {
            let (start, end) = file_bounds[file_idx];
            // First NON-EMPTY row group of the file (a leading empty row
            // group must not hide the rest of the file).
            let mut first_source = start;
            while first_source < end && rows_per_source[first_source] == 0 {
                first_source += 1;
            }
            if first_source >= end {
                continue;
            }
            let keys = materialize_keys(&mut cache, first_source, 0)?;
            heap.push(Reverse(MergeHead {
                keys,
                source_idx: first_source,
                row_idx: 0,
                sort_keys: sort_key_indices,
            }));
        }

        let mut chunk: Vec<RowRef> = Vec::with_capacity(output_row_group_rows);
        let mut rows_written = 0usize;
        while let Some(Reverse(head)) = heap.pop() {
            chunk.push(RowRef {
                batch_idx: head.source_idx,
                row_idx: head.row_idx,
            });
            rows_written += 1;
            // Advance this stream: next row in the same row group, else the
            // next NON-EMPTY row group of the same file (empty row groups
            // must be skipped forward, not abandon the file).
            let mut next_source = head.source_idx;
            let mut next_row = head.row_idx + 1;
            if next_row >= rows_per_source[next_source] {
                next_row = 0;
                let file = file_bounds
                    .iter()
                    .find(|&&(start, end)| head.source_idx >= start && head.source_idx < end);
                let end = match file {
                    Some(&(_start, end)) => end,
                    None => source_row_groups.len(),
                };
                next_source += 1;
                while next_source < end && rows_per_source[next_source] == 0 {
                    next_source += 1;
                }
                if next_source >= end {
                    continue; // file exhausted
                }
            }
            let keys = materialize_keys(&mut cache, next_source, next_row)?;
            heap.push(Reverse(MergeHead {
                keys,
                source_idx: next_source,
                row_idx: next_row,
                sort_keys: sort_key_indices,
            }));

            if chunk.len() >= output_row_group_rows {
                let batch =
                    self.gather_cached_chunk(schema, &source_row_groups, &chunk, &mut cache)?;
                writer.write_batch_parallel(&batch)?;
                chunk.clear();
            }
        }
        if !chunk.is_empty() {
            let batch = self.gather_cached_chunk(schema, &source_row_groups, &chunk, &mut cache)?;
            writer.write_batch_parallel(&batch)?;
        }
        Ok(rows_written)
    }

    fn gather_cached_chunk(
        &self,
        schema: &Schema,
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
        row_group_cache: &mut RowGroupCache,
    ) -> Result<RecordBatch> {
        let mut positions_by_source = (0..source_row_groups.len())
            .map(|_| Vec::new())
            .collect::<Vec<Vec<(usize, usize)>>>();
        for (output_pos, row_ref) in row_refs.iter().enumerate() {
            let positions = positions_by_source
                .get_mut(row_ref.batch_idx)
                .ok_or_else(|| {
                    TaibaiError::Storage(format!(
                        "missing source row group {} during gather",
                        row_ref.batch_idx
                    ))
                })?;
            positions.push((output_pos, row_ref.row_idx));
        }

        let mut columns = schema
            .columns
            .iter()
            .map(|col| empty_column_for_gather(col.data_type, row_refs.len()))
            .collect::<Vec<_>>();

        for (source_idx, positions) in positions_by_source.into_iter().enumerate() {
            if positions.is_empty() {
                continue;
            }
            let source = source_row_groups.get(source_idx).ok_or_else(|| {
                TaibaiError::Storage(format!("missing source row group {}", source_idx))
            })?;
            let src_batch = row_group_cache.get(source, &self.segment_mgr)?;

            for col_idx in 0..schema.columns.len() {
                let src_col = src_batch.column(col_idx).ok_or_else(|| {
                    TaibaiError::Storage(format!(
                        "missing source column {} in row group {}",
                        col_idx, source_idx
                    ))
                })?;
                gather_column_positions(&mut columns[col_idx], src_col, &positions)?;
            }
        }

        RecordBatch::try_new(schema.clone(), columns)
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

struct RowGroupCache {
    capacity: usize,
    projection: Vec<usize>,
    batches: HashMap<(PathBuf, usize), RecordBatch>,
    order: VecDeque<(PathBuf, usize)>,
    hits: usize,
    misses: usize,
}

impl RowGroupCache {
    fn new(capacity: usize, projection: Vec<usize>) -> Self {
        Self {
            capacity: capacity.max(1),
            projection,
            batches: HashMap::new(),
            order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn len(&self) -> usize {
        self.batches.len()
    }

    fn hits(&self) -> usize {
        self.hits
    }

    fn misses(&self) -> usize {
        self.misses
    }

    fn get(
        &mut self,
        source: &SourceRowGroup,
        segment_mgr: &SegmentManager,
    ) -> Result<&RecordBatch> {
        let key = (source.segment_path.clone(), source.row_group_idx);
        if self.batches.contains_key(&key) {
            self.hits += 1;
            self.touch(&key);
            return Ok(self.batches.get(&key).unwrap());
        }

        self.misses += 1;
        // Reuse the parsed footer and mmap for every row group in the same
        // segment. Reopening here is especially expensive for fragmented
        // segments because their footer can be almost as large as the data.
        let reader = segment_mgr.open_reader_cached(&source.segment_path)?;
        let batch =
            reader.read_row_group_with_projection(source.row_group_idx, &self.projection)?;
        self.batches.insert(key.clone(), batch);
        self.order.push_back(key.clone());
        self.evict_if_needed();
        Ok(self.batches.get(&key).unwrap())
    }

    fn touch(&mut self, key: &(PathBuf, usize)) {
        if let Some(pos) = self.order.iter().position(|existing| existing == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
    }

    fn evict_if_needed(&mut self) {
        while self.batches.len() > self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.batches.remove(&old_key);
            } else {
                break;
            }
        }
    }
}

fn compare_row_indices(
    left: u32,
    right: u32,
    source_batch: &RecordBatch,
    sort_key_indices: &[usize],
) -> Ordering {
    for &col_idx in sort_key_indices {
        let Some(column) = source_batch.column(col_idx) else {
            continue;
        };
        let cmp = compare_column_rows(column, left as usize, column, right as usize);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

fn estimated_row_width_bytes(schema: &Schema) -> usize {
    schema
        .columns
        .iter()
        .map(|col| {
            // Variable-width columns (VarChar/Blob/Vector) carry a
            // conservative per-value allowance: underestimating them let
            // external-merge runs overshoot the memory budget.
            let base = col
                .data_type
                .fixed_size()
                .map(|w| w as usize)
                .unwrap_or(0)
                .max(8);
            let variable_extra = if col.data_type.fixed_size().is_none() {
                64
            } else {
                0
            };
            base + variable_extra + 24
        })
        .sum::<usize>()
        .max(32)
}

/// Temp path for one sorted run of the external merge. The name never
/// matches segment discovery patterns, and startup cleanup removes
/// `.merge.tmp.` leftovers after a crash.
fn merge_run_path(final_path: &Path, seq: usize) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.merge.tmp.{}", name, seq))
}

fn merge_wave_path(final_path: &Path, wave: usize, seq: usize) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.merge.tmp.w{}n{}", name, wave, seq))
}

fn sort_clustered_row_indices(
    row_indices: &mut Vec<u32>,
    source_batch: &RecordBatch,
    sort_key_indices: &[usize],
) {
    let Some(&leading_key_idx) = sort_key_indices.first() else {
        return;
    };
    let varchar_leading_key = matches!(
        source_batch.column(leading_key_idx),
        Some(ColumnArray::VarChar { .. })
    );
    if !varchar_leading_key {
        row_indices.par_sort_unstable_by(|left, right| {
            compare_row_indices(*left, *right, source_batch, sort_key_indices)
                .then_with(|| left.cmp(right))
        });
        return;
    }

    // Tick data arrives in time order, while its leading cluster key is the
    // security identifier. Group that string key in one linear pass, sort the
    // few thousand group names once, then sort each group by the remaining
    // numeric/time keys in parallel. Null leading keys stay last.
    let total_rows = row_indices.len();
    let unsorted = std::mem::take(row_indices);
    let mut groups: HashMap<&str, Vec<u32>> = HashMap::new();
    let mut null_group = Vec::new();
    let Some(ColumnArray::VarChar { data, bitmap }) = source_batch.column(leading_key_idx) else {
        return;
    };
    for row_idx in unsorted {
        if bitmap.is_null(row_idx as usize) {
            null_group.push(row_idx);
        } else if let Some(value) = data.get(row_idx as usize) {
            groups.entry(value).or_default().push(row_idx);
        } else {
            // A row index out of range for the leading column means the
            // column and the surviving-row set disagree (some other bug's
            // symptom). The old `continue` silently dropped the row from
            // the compaction output — quieter than a panic and far more
            // dangerous. Keep the row (sorted with the null group) and say
            // so loudly.
            tracing::error!(
                row_idx,
                len = data.len(),
                "cluster sort: leading-key row index out of range; keeping row in null group"
            );
            null_group.push(row_idx);
        }
    }

    let remaining_keys = &sort_key_indices[1..];
    let mut ordered_groups = groups.into_iter().collect::<Vec<_>>();
    ordered_groups.par_iter_mut().for_each(|(_, rows)| {
        rows.sort_unstable_by(|left, right| {
            compare_row_indices(*left, *right, source_batch, remaining_keys)
                .then_with(|| left.cmp(right))
        });
    });
    ordered_groups.sort_unstable_by(|left, right| left.0.cmp(right.0));
    null_group.sort_unstable_by(|left, right| {
        compare_row_indices(*left, *right, source_batch, remaining_keys)
            .then_with(|| left.cmp(right))
    });

    row_indices.reserve(total_rows);
    for (_, mut rows) in ordered_groups {
        row_indices.append(&mut rows);
    }
    row_indices.append(&mut null_group);
}

fn gather_global_chunk(
    schema: &Schema,
    source_batch: &RecordBatch,
    row_indices: &[u32],
) -> Result<RecordBatch> {
    let columns = (0..schema.num_columns())
        .into_par_iter()
        .map(|col_idx| {
            source_batch
                .column(col_idx)
                .map(|column| column.take(row_indices))
                .ok_or_else(|| TaibaiError::Storage(format!("missing source column {}", col_idx)))
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(schema.clone(), columns)
}

fn compare_column_rows(
    left: &ColumnArray,
    left_row: usize,
    right: &ColumnArray,
    right_row: usize,
) -> Ordering {
    let left_null = left.bitmap().is_null(left_row);
    let right_null = right.bitmap().is_null(right_row);
    match (left_null, right_null) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater,
        (false, true) => return Ordering::Less,
        (false, false) => {}
    }

    macro_rules! compare_ord {
        ($left_data:expr, $right_data:expr) => {
            $left_data[left_row].cmp(&$right_data[right_row])
        };
    }
    macro_rules! compare_float {
        ($left_data:expr, $right_data:expr) => {
            $left_data[left_row]
                .partial_cmp(&$right_data[right_row])
                .unwrap_or(Ordering::Equal)
        };
    }

    match (left, right) {
        (ColumnArray::Bool { data: l, .. }, ColumnArray::Bool { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Int8 { data: l, .. }, ColumnArray::Int8 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Int16 { data: l, .. }, ColumnArray::Int16 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Int32 { data: l, .. }, ColumnArray::Int32 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Int64 { data: l, .. }, ColumnArray::Int64 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::UInt8 { data: l, .. }, ColumnArray::UInt8 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::UInt16 { data: l, .. }, ColumnArray::UInt16 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::UInt32 { data: l, .. }, ColumnArray::UInt32 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::UInt64 { data: l, .. }, ColumnArray::UInt64 { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Float32 { data: l, .. }, ColumnArray::Float32 { data: r, .. }) => {
            compare_float!(l, r)
        }
        (ColumnArray::Float64 { data: l, .. }, ColumnArray::Float64 { data: r, .. }) => {
            compare_float!(l, r)
        }
        (ColumnArray::Decimal { data: l, .. }, ColumnArray::Decimal { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Date { data: l, .. }, ColumnArray::Date { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Time { data: l, .. }, ColumnArray::Time { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Timestamp { data: l, .. }, ColumnArray::Timestamp { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Interval { data: l, .. }, ColumnArray::Interval { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::VarChar { data: l, .. }, ColumnArray::VarChar { data: r, .. }) => {
            compare_ord!(l, r)
        }
        (ColumnArray::Blob { data: l, .. }, ColumnArray::Blob { data: r, .. }) => {
            compare_ord!(l, r)
        }
        _ => compare_scalars_for_sort(&left.scalar(left_row), &right.scalar(right_row)),
    }
}

fn empty_column_for_gather(data_type: DataType, len: usize) -> ColumnArray {
    let bitmap = NullBitmap::new_valid(len);
    match data_type {
        DataType::Bool => ColumnArray::Bool {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int8 => ColumnArray::Int8 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int16 => ColumnArray::Int16 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int32 => ColumnArray::Int32 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int64 => ColumnArray::Int64 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt8 => ColumnArray::UInt8 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt16 => ColumnArray::UInt16 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt32 => ColumnArray::UInt32 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt64 => ColumnArray::UInt64 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Float32 => ColumnArray::Float32 {
            data: vec![0.0; len],
            bitmap,
        },
        DataType::Float64 => ColumnArray::Float64 {
            data: vec![0.0; len],
            bitmap,
        },
        DataType::Decimal => ColumnArray::Decimal {
            data: vec![0; len],
            bitmap,
        },
        DataType::Date => ColumnArray::Date {
            data: vec![0; len],
            bitmap,
        },
        DataType::Time => ColumnArray::Time {
            data: vec![0; len],
            bitmap,
        },
        DataType::Timestamp => ColumnArray::Timestamp {
            data: vec![0; len],
            bitmap,
        },
        DataType::Interval => ColumnArray::Interval {
            data: vec![0; len],
            bitmap,
        },
        DataType::VarChar => ColumnArray::VarChar {
            data: vec![String::new(); len].into(),
            bitmap,
        },
        DataType::Blob => ColumnArray::Blob {
            data: vec![Vec::new(); len].into(),
            bitmap,
        },
        DataType::Vector => ColumnArray::Vector {
            data: vec![Vec::new(); len],
            bitmap,
        },
    }
}

fn gather_column_positions(
    dst: &mut ColumnArray,
    src: &ColumnArray,
    positions: &[(usize, usize)],
) -> Result<()> {
    macro_rules! copy_fixed {
        ($dst_data:expr, $dst_bitmap:expr, $src_data:expr, $src_bitmap:expr) => {{
            for &(output_pos, source_row_idx) in positions {
                $dst_data[output_pos] = $src_data[source_row_idx];
                if $src_bitmap.is_null(source_row_idx) {
                    $dst_bitmap.set_null(output_pos);
                }
            }
        }};
    }

    macro_rules! copy_clone {
        ($dst_data:expr, $dst_bitmap:expr, $src_data:expr, $src_bitmap:expr) => {{
            for &(output_pos, source_row_idx) in positions {
                $dst_data[output_pos] = $src_data[source_row_idx].clone();
                if $src_bitmap.is_null(source_row_idx) {
                    $dst_bitmap.set_null(output_pos);
                }
            }
        }};
    }

    match (dst, src) {
        (
            ColumnArray::Bool {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Bool {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int8 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int8 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int16 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int16 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt8 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt8 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt16 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt16 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Float32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Float32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Float64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Float64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Decimal {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Decimal {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Date {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Date {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Time {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Time {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Timestamp {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Timestamp {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Interval {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Interval {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::VarChar {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::VarChar {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => {
            for &(output_pos, source_row_idx) in positions {
                dst_data.set(output_pos, src_data.value(source_row_idx));
                if src_bitmap.is_null(source_row_idx) {
                    dst_bitmap.set_null(output_pos);
                }
            }
        }
        (
            ColumnArray::Blob {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Blob {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => {
            for &(output_pos, source_row_idx) in positions {
                dst_data.set(output_pos, src_data.value(source_row_idx));
                if src_bitmap.is_null(source_row_idx) {
                    dst_bitmap.set_null(output_pos);
                }
            }
        }
        (
            ColumnArray::Vector {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Vector {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_clone!(dst_data, dst_bitmap, src_data, src_bitmap),
        (dst, src) => {
            return Err(TaibaiError::Storage(format!(
                "column type mismatch while compacting: dst={} src={}",
                dst.data_type().display_name(),
                src.data_type().display_name()
            )));
        }
    }
    Ok(())
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
