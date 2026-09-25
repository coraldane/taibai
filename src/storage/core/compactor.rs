use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

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

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactStats {
    pub segments_read: usize,
    pub segments_written: usize,
    pub rows_written: u64,
    pub rows_dropped: u64,
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
                self.segment_mgr
                    .with_partition_mutation_lock(db, table, lock_key, || {
                        // Keep the acquisition order aligned with query scans to
                        // avoid CPU/IO permit inversion. Holding both permits for
                        // the partition bounds process-wide compaction pressure.
                        let _cpu_permit = crate::common::work_limiter::acquire_cpu();
                        let _io_permit = crate::common::work_limiter::acquire_io();

                        let files: Vec<PathBuf> =
                            files.into_iter().filter(|path| path.exists()).collect();
                        if files.is_empty() {
                            return Ok(CompactStats::default());
                        }

                        // A clean single segment can only be skipped when this
                        // operation has no physical ordering requirement and its
                        // row groups are already compact. A segment can contain
                        // thousands of tiny row groups after many small flushes;
                        // skipping it makes OPTIMIZE unable to repair the main
                        // source of read amplification.
                        if sort_key_indices.is_empty() && files.len() == 1 {
                            let reader = self.segment_mgr.open_reader_cached(&files[0])?;
                            let compact_row_groups =
                                reader.row_count().div_ceil(output_row_group_rows as u64) as usize;
                            let needs_row_group_compaction =
                                reader.num_row_groups() > compact_row_groups.max(1);
                            if reader.deleted_row_count() == 0 && !needs_row_group_compaction {
                                tracing::info!(
                                    db,
                                    table,
                                    partition_key = partition_key.as_deref().unwrap_or("<none>"),
                                    segment = %files[0].display(),
                                    "skipping clean single-segment optimization"
                                );
                                return Ok(CompactStats::default());
                            }
                        }
                        self.compact_segment_group(
                            db,
                            table,
                            &table_dir,
                            partition_key.as_deref(),
                            &files,
                            &schema,
                            sort_key_indices,
                            &cluster_key_indices,
                            output_row_group_rows,
                            ttl,
                            compression_level,
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        for part_stats in partition_stats {
            stats.segments_written += part_stats.segments_written;
            stats.rows_written += part_stats.rows_written;
            stats.rows_dropped += part_stats.rows_dropped;
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
        let Some((matched_key, mut files)) = matched else {
            return Ok(CompactStats::default());
        };

        let stats =
            self.segment_mgr
                .with_partition_mutation_lock(db, table, &matched_key, || {
                    files.retain(|path| path.exists());
                    if files.is_empty() {
                        return Ok(CompactStats::default());
                    }

                    tracing::info!(
                        db,
                        table,
                        table_full_name = %format!("{}.{}", db, table),
                        partition_key = %matched_key,
                        requested_partition = partition_key,
                        cluster_key = ?table_meta.cluster_key,
                        segments = files.len(),
                        "starting requested partition optimization"
                    );

                    let _cpu_permit = crate::common::work_limiter::acquire_cpu();
                    let _io_permit = crate::common::work_limiter::acquire_io();
                    self.compact_segment_group(
                        db,
                        table,
                        &table_dir,
                        Some(&matched_key),
                        &files,
                        &schema,
                        &cluster_key_indices,
                        &cluster_key_indices,
                        output_row_group_rows,
                        ttl,
                        compression_level,
                    )
                })?;

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

    #[allow(clippy::too_many_arguments)]
    fn compact_segment_group(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        partition_key: Option<&str>,
        segment_files: &[PathBuf],
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        output_row_group_rows: usize,
        ttl: Option<(usize, i64)>,
        compression_level: i32,
    ) -> Result<CompactStats> {
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
        let mut deleted_rows = 0u64;
        let partition_label = partition_key.unwrap_or("<unpartitioned>");

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            segments = segment_files.len(),
            "compact partition started"
        );

        if sort_key_indices.is_empty() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            for seg_path in segment_files {
                let reader = self.segment_mgr.open_reader(seg_path)?;
                let mut segment_row_base = 0u64;
                for rg_idx in 0..reader.num_row_groups() {
                    let batch_idx = source_row_groups.len();
                    let row_count = reader.row_group_rows(rg_idx) as usize;
                    // TTL purge for the unsorted path: read just the TTL
                    // column and drop rows the read-time filter would hide.
                    let ttl_col = ttl.and_then(|(idx, _)| reader.read_column(rg_idx, idx).ok());
                    let duration = ttl.map(|(_, secs)| secs.saturating_mul(1_000_000));
                    for row_idx in 0..row_count {
                        let row_id = segment_row_base + row_idx as u64;
                        if reader.is_row_deleted(row_id) {
                            deleted_rows += 1;
                            continue;
                        }
                        if let (Some(col), Some(duration)) = (&ttl_col, duration) {
                            let visible = matches!(
                                col.scalar(row_idx),
                                crate::types::scalar::ScalarValue::Timestamp(Some(ts))
                                    if ts.saturating_add(duration) > now
                            );
                            if !visible {
                                deleted_rows += 1;
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
            source_row_group_count = source_row_groups.len();
        } else {
            let decode_started = Instant::now();
            struct ColumnReadSource {
                reader: Arc<crate::storage::segment::SegmentReader>,
                row_group_idx: usize,
                segment_row_base: u64,
                global_row_base: usize,
                row_count: usize,
            }

            let mut sources = Vec::new();
            let mut global_row_base = 0usize;
            for seg_path in segment_files {
                let reader = Arc::new(self.segment_mgr.open_reader(seg_path)?);
                let mut segment_row_base = 0u64;
                for rg_idx in 0..reader.num_row_groups() {
                    let row_count = reader.row_group_rows(rg_idx) as usize;
                    sources.push(ColumnReadSource {
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
            source_row_group_count = sources.len();
            if global_row_base > u32::MAX as usize {
                return Err(TaibaiError::Storage(format!(
                    "clustered optimization supports at most {} rows per partition",
                    u32::MAX
                )));
            }

            sorted_indices.reserve(global_row_base);
            for source in &sources {
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
                    for source in &sources {
                        let decoded = source
                            .reader
                            .read_column(source.row_group_idx, column_idx)?;
                        destination.append_owned(decoded)?;
                    }
                    Ok(destination)
                })
                .collect::<Result<Vec<_>>>()?;
            preloaded_batch = Some(RecordBatch::try_new(schema.clone(), columns)?);

            // TTL purge: physically drop rows the read-time TTL filter would
            // hide anyway (ts + duration <= now, or a non-timestamp TTL
            // value), keeping compaction output and query visibility in sync.
            if let (Some((ttl_col_idx, duration_secs)), Some(batch)) =
                (ttl, preloaded_batch.as_ref())
            {
                if let Some(ttl_col) = batch.column(ttl_col_idx) {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as i64;
                    let duration = duration_secs.saturating_mul(1_000_000);
                    let before = sorted_indices.len();
                    sorted_indices.retain(|&global_row| {
                        matches!(
                            ttl_col.scalar(global_row as usize),
                            crate::types::scalar::ScalarValue::Timestamp(Some(ts))
                                if ts.saturating_add(duration) > now
                        )
                    });
                    deleted_rows += (before - sorted_indices.len()) as u64;
                }
            }

            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                source_row_groups = source_row_group_count,
                rows_to_sort = sorted_indices.len(),
                elapsed_ms = decode_started.elapsed().as_millis(),
                "compact partition decoded contiguous columns"
            );
            let sort_started = Instant::now();
            sort_clustered_row_indices(
                &mut sorted_indices,
                preloaded_batch.as_ref().unwrap(),
                sort_key_indices,
            );
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                rows_sorted = sorted_indices.len(),
                elapsed_ms = sort_started.elapsed().as_millis(),
                "compact partition sorted cluster keys"
            );
        }

        let rows_to_write = if sort_key_indices.is_empty() {
            row_refs.len()
        } else {
            sorted_indices.len()
        };
        if rows_to_write == 0 {
            for old_seg in segment_files {
                self.segment_mgr.remove_segment_file(old_seg)?;
            }
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                segments_read = segment_files.len(),
                rows_dropped = deleted_rows,
                "compact partition removed empty/deleted segments"
            );
            return Ok(CompactStats {
                segments_read: segment_files.len(),
                rows_dropped: deleted_rows,
                ..CompactStats::default()
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

        self.segment_mgr
            .replace_segments_with_tmp(&tmp_path, &final_path, segment_files)?;

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            final_path = %final_path.display(),
            segments_read = segment_files.len(),
            rows_written = rows_to_write,
            rows_dropped = deleted_rows,
            "compact partition completed"
        );

        Ok(CompactStats {
            segments_read: segment_files.len(),
            segments_written: 1,
            rows_written: rows_to_write as u64,
            rows_dropped: deleted_rows,
        })
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
            continue;
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
