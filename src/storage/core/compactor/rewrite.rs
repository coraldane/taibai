//! Streaming rewrites and selection of compaction inputs.

use super::*;
use crate::storage::segment_manager::plan_ttl_fully_expired;

impl Compactor {
    /// Streaming rewrite of an unsorted partition: walk the input segments
    /// in source order, purge deleted and TTL-expired rows, and write the
    /// survivors to the replacement segment.
    ///
    /// The surviving-row reference buffer is bounded by the shared
    /// compaction memory budget: once the 16-byte refs cross it, whole
    /// output row groups are flushed to the writer and the buffer is reused
    /// for the rest of the scan. Before this bound the refs of every live
    /// row were collected up front (~8GB for 500M rows) while the clustered
    /// path already had a budget via its external merge. Flushing only
    /// whole row groups, one chunk-sized gather at a time, and appending
    /// refs in source order keeps the rewritten row set, its order, and the
    /// output row-group layout identical to the old collect-then-chunk
    /// loop.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn rewrite_unsorted_partition(
        &self,
        db: &str,
        table: &str,
        partition_label: &str,
        tmp_path: &Path,
        schema: &Schema,
        bloom_column_indices: &[usize],
        segment_files: &[PathBuf],
        ttl: Option<(usize, i64)>,
        output_row_group_rows: usize,
        compression_level: i32,
    ) -> Result<UnsortedRewriteStats> {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let duration = ttl.map(|(_, secs)| secs.saturating_mul(1_000_000));
        let chunk_rows = output_row_group_rows.max(1);
        let write_progress_rows = chunk_rows.saturating_mul(16).max(1);
        // Soft row cap of the ref buffer, translated from the byte budget
        // via the per-row ref cost. One output chunk is the minimum flush
        // granularity (a partial flush would strand a short row group
        // mid-segment and change the output layout), so budgets smaller
        // than a chunk still buffer a whole chunk.
        let ref_budget_rows = (clustered_merge_budget_bytes() / 4 / ROW_REF_BYTES).max(chunk_rows);

        let mut writer = SegmentWriter::create_with_bloom_columns(
            tmp_path,
            schema.clone(),
            output_row_group_rows,
            bloom_column_indices,
            compression_level,
        )?;
        // The cache lives across budget flushes: after a flush the next rows
        // usually come from the same source row groups the scan is walking.
        let mut row_group_cache = RowGroupCache::new(32, schema.clone());
        let mut source_row_groups: Vec<SourceRowGroup> = Vec::new();
        let mut row_refs: Vec<RowRef> = Vec::new();
        let mut deleted_rows = 0u64;
        let mut ttl_dropped = 0u64;
        let mut rows_written = 0usize;

        // Gathers and writes one chunk of buffered refs. Sources and refs
        // are passed per call: the scan keeps appending to
        // `source_row_groups`, which a capturing closure would pin for the
        // whole loop.
        let mut write_refs = |sources: &[SourceRowGroup], refs: &[RowRef]| -> Result<()> {
            if refs.is_empty() {
                return Ok(());
            }
            let batch = self.gather_cached_chunk(schema, sources, refs, &mut row_group_cache)?;
            rows_written += batch.num_rows();
            writer.write_batch_parallel(&batch)?;
            if rows_written.is_multiple_of(write_progress_rows) {
                tracing::info!(
                    db,
                    table,
                    table_full_name = %format!("{}.{}", db, table),
                    partition_key = partition_label,
                    rows_written,
                    "compact write progress"
                );
            }
            Ok(())
        };

        for seg_path in segment_files {
            let reader = self.segment_mgr.open_reader(seg_path)?;
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                let batch_idx = source_row_groups.len();
                let row_count = reader.row_group_rows(rg_idx) as usize;
                // TTL purge: read just the TTL column and drop rows the
                // read-time filter would hide.
                let ttl_col = ttl
                    .map(|(idx, _)| reader.read_catalog_column(rg_idx, schema, idx))
                    .transpose()?;
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
                if row_refs.len() >= ref_budget_rows {
                    // Budget drain: flush every whole chunk currently
                    // buffered, keeping only the sub-chunk tail for the next
                    // round. Each flushed batch is one chunk large — the
                    // same gather memory the single-pass loop used — so only
                    // the ref buffer, never the write path, changes shape.
                    let whole_chunks = row_refs.len() - row_refs.len() % chunk_rows;
                    let tail = row_refs.split_off(whole_chunks);
                    for chunk in row_refs.chunks(chunk_rows) {
                        write_refs(&source_row_groups, chunk)?;
                    }
                    row_refs.clear();
                    row_refs.extend(tail);
                }
            }
        }
        // Tail flush: the final chunk may be short, exactly like the last
        // chunk of the old collect-then-chunk loop.
        for chunk in row_refs.chunks(chunk_rows) {
            write_refs(&source_row_groups, chunk)?;
        }

        if rows_written == 0 {
            // Nothing survived: skip sealing an empty replacement — the
            // caller's AllDropped path removes the header-only tmp file.
            return Ok(UnsortedRewriteStats {
                rows_written,
                deleted_rows,
                ttl_dropped,
            });
        }
        writer.seal()?;
        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            source_row_groups = source_row_groups.len(),
            rows_written,
            rows_dropped = deleted_rows,
            rows_dropped_ttl = ttl_dropped,
            ref_budget_rows,
            fallback_cache_hits = row_group_cache.hits(),
            fallback_cache_misses = row_group_cache.misses(),
            "unsorted partition rewrite completed"
        );
        Ok(UnsortedRewriteStats {
            rows_written,
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
    pub(super) fn prepare_compact_segment_group(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        partition_key: Option<&str>,
        segment_inputs: Vec<super::PlannedSegment>,
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        output_row_group_rows: usize,
        ttl: Option<(usize, i64)>,
        compression_level: i32,
    ) -> Result<PreparedCompaction> {
        let segment_inputs: Vec<super::PlannedSegment> = segment_inputs
            .into_iter()
            .filter(|segment| segment.path.exists())
            .collect();
        if segment_inputs.is_empty() {
            return Ok(PreparedCompaction::Skipped);
        }
        let partition_label = partition_key.unwrap_or("<unpartitioned>");
        // `bloom_column_indices` is always the table's cluster-key column set
        // (both call sites pass `cluster_key_indices`), so it doubles as the
        // "is this table clustered" signal for the input selection below.
        let table_is_clustered = !bloom_column_indices.is_empty();
        let identities = if sort_key_indices.is_empty() && !table_is_clustered {
            // Non-clustered table: only dirty (or under-full, merge-worthy)
            // segments enter the rewrite; clean full-size segments are never
            // rewritten — rewriting them would be pure write amplification.
            // The one exception is clean segments the zone maps prove fully
            // TTL-expired (see `select_noncluster_compaction_inputs`): those
            // must join the rewrite or their expired rows linger forever.
            let now_us = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            match self.select_noncluster_compaction_inputs(
                db,
                table,
                partition_key,
                &segment_inputs,
                schema,
                output_row_group_rows,
                ttl,
                now_us,
            )? {
                Some(inputs) => inputs,
                None => return Ok(PreparedCompaction::Skipped),
            }
        } else {
            // Clustered tables keep the full-partition merge on every path:
            // global cluster order is an invariant, and leaving clean
            // segments out of the merge to save IO would break it. The
            // unsorted background delete-cleanup still preserves the whole
            // partition's source order across the merged output. The only
            // clean-input optimization kept here is the single-segment skip:
            // with one segment there is no cross-file order to preserve, so a
            // clean compact one is left alone. A segment can contain
            // thousands of tiny row groups after many small flushes; this
            // check keeps OPTIMIZE able to repair the main source of read
            // amplification.
            if sort_key_indices.is_empty() && segment_inputs.len() == 1 {
                let meta = &segment_inputs[0].meta;
                let compact_row_groups =
                    meta.row_count.div_ceil(output_row_group_rows as u64) as usize;
                let needs_row_group_compaction =
                    meta.row_group_rows.len() > compact_row_groups.max(1);
                if meta.deleted_row_count == 0 && !needs_row_group_compaction {
                    tracing::info!(
                        db,
                        table,
                        partition_key = partition_key.unwrap_or("<none>"),
                        segment = %segment_inputs[0].path.display(),
                        "skipping clean single-segment optimization"
                    );
                    return Ok(PreparedCompaction::Skipped);
                }
            }
            segment_inputs
        };
        let segment_files = identities
            .iter()
            .map(|segment| segment.path.clone())
            .collect::<Vec<PathBuf>>();
        let segments_read = segment_files.len();

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
        if sort_key_indices.is_empty() {
            // Unsorted rewrite: refs are streamed to the replacement writer
            // under the compaction memory budget (see
            // `rewrite_unsorted_partition`), so this path owns its tmp path
            // and returns the finished outcome directly — the same early
            // return shape as the clustered external merge below.
            let final_path = self
                .segment_mgr
                .next_replacement_segment_path(table_dir, partition_key)?;
            let tmp_path = store::replacement_tmp_path(&final_path, "compact_tmp");
            let cleanup = super::runs::TempRuns(vec![tmp_path.clone()]);
            let rewrite = self.rewrite_unsorted_partition(
                db,
                table,
                partition_label,
                &tmp_path,
                schema,
                bloom_column_indices,
                &segment_files,
                ttl,
                output_row_group_rows,
                compression_level,
            )?;
            if rewrite.rows_written == 0 {
                // Every input row was deleted or expired; the writer holds
                // only the header, so discard it instead of sealing an empty
                // replacement.
                let _ = std::fs::remove_file(&tmp_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                return Ok(PreparedCompaction::AllDropped {
                    segment_files,
                    identities,
                    segments_read,
                    rows_dropped: rewrite.deleted_rows,
                    rows_dropped_ttl: rewrite.ttl_dropped,
                });
            }
            return Ok(PreparedCompaction::Replacement {
                _cleanup: cleanup,
                segment_files,
                identities,
                tmp_path,
                final_path,
                segments_read,
                rows_to_write: rewrite.rows_written,
                rows_dropped: rewrite.deleted_rows,
                rows_dropped_ttl: rewrite.ttl_dropped,
            });
        }

        let decode_started = Instant::now();
        let (sources, global_row_base) = self.collect_clustered_sources(&segment_files)?;
        let source_row_group_count = sources.len();

        let estimated_width = estimated_row_width_bytes(schema);
        let in_memory_budget_rows = (clustered_merge_budget_bytes() / 4 / estimated_width).max(1);
        let stored_bytes = sources.iter().fold(0usize, |sum, source| {
            sum.saturating_add(
                source
                    .reader
                    .row_group_meta(source.row_group_idx)
                    .map(|row_group| {
                        row_group
                            .uncompressed_sizes
                            .iter()
                            .fold(0usize, |n, &v| n.saturating_add(v as usize))
                    })
                    .unwrap_or(0),
            )
        });
        let prepare_external = || -> Result<PreparedCompaction> {
            // Partition exceeds the in-memory budget: spill sorted runs
            // to temp files and k-way merge them into the output, keeping
            // peak memory proportional to the budget instead of the
            // partition.
            let final_path = self
                .segment_mgr
                .next_replacement_segment_path(table_dir, partition_key)?;
            let tmp_path = store::replacement_tmp_path(&final_path, "compact_tmp");
            let mut output_cleanup = super::runs::TempRuns(vec![tmp_path.clone()]);
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
                    segment_files: segment_files.clone(),
                    identities: identities.clone(),
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
            output_cleanup.0.clear();
            Ok(PreparedCompaction::Replacement {
                _cleanup: super::runs::TempRuns(vec![tmp_path.clone()]),
                segment_files: segment_files.clone(),
                identities: identities.clone(),
                tmp_path,
                final_path,
                segments_read,
                rows_to_write: rows_written,
                rows_dropped: dropped,
                rows_dropped_ttl: dropped_ttl,
            })
        };
        if global_row_base > in_memory_budget_rows
            || stored_bytes > clustered_merge_budget_bytes() / 4
        {
            return prepare_external();
        }
        if global_row_base > u32::MAX as usize {
            return Err(TaibaiError::Storage(format!(
                "clustered optimization supports at most {} rows per partition",
                u32::MAX
            )));
        }

        let clustered = match self.decode_clustered_partition(
            db,
            table,
            partition_label,
            schema,
            &sources,
            global_row_base,
            sort_key_indices,
            ttl,
            decode_started,
        ) {
            Ok(clustered) => clustered,
            Err(TaibaiError::ResourceBusy(_)) => return prepare_external(),
            Err(error) => return Err(error),
        };
        let preloaded_batch = clustered.batch;
        let sorted_indices = clustered.sorted_indices;
        let deleted_rows = clustered.deleted_rows;
        let ttl_dropped = clustered.ttl_dropped;

        let rows_to_write = sorted_indices.len();
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

        let write_result = self.write_clustered_segment(
            db,
            table,
            partition_label,
            &tmp_path,
            schema,
            bloom_column_indices,
            &preloaded_batch,
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
            _cleanup: super::runs::TempRuns(vec![tmp_path.clone()]),
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

    /// Dirty-segment input selection for non-clustered tables: pick the
    /// subset of a partition's discovered segments the rewrite may touch.
    ///
    /// A segment joins the input set when it is dirty (footer tombstones or
    /// row-group fragmentation — rewriting it alone already pays for itself)
    /// or under-full (fewer rows than one target row group, the readable
    /// proxy for "file below the segment seal size": the seal threshold
    /// itself is private to `SegmentManager`). Under-full but otherwise clean
    /// segments merge only in company with other inputs: a lone one would be
    /// rewritten into the same lone small file. Clean segments that already
    /// hold a full row group of compact live data are never rewritten — that
    /// would be pure write amplification — except when the table has a TTL
    /// and the segment's zone maps prove every row already expired: the
    /// row-level TTL purge only runs inside a rewrite, so keeping such a
    /// segment would strand its expired rows on disk (and in the catalog
    /// row_count) forever; whole-partition TTL drops cannot cover it because
    /// they only fire when the TTL column is the time partition column.
    /// Returns `None` when nothing qualifies (the whole partition is
    /// skipped).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_noncluster_compaction_inputs(
        &self,
        db: &str,
        table: &str,
        partition_key: Option<&str>,
        segment_files: &[super::PlannedSegment],
        schema: &Schema,
        output_row_group_rows: usize,
        ttl: Option<(usize, i64)>,
        now_us: i64,
    ) -> Result<Option<Vec<super::PlannedSegment>>> {
        #[derive(Clone, Copy, PartialEq)]
        enum InputClass {
            /// Tombstoned or row-group fragmented: worth rewriting alone.
            Rewrite,
            /// Clean but under-full: only worth rewriting together with
            /// other inputs.
            MergeSmall,
            /// Clean and full: must be kept as-is.
            Keep,
        }

        let mut classes = Vec::with_capacity(segment_files.len());
        let mut rewrite_count = 0usize;
        let mut small_count = 0usize;
        let ttl_plan_column = ttl
            .and_then(|(ttl_col, _)| schema.columns.get(ttl_col))
            .map(|column| column.name.as_str());
        for segment in segment_files {
            let meta = &segment.meta;
            let compact_row_groups = meta.row_count.div_ceil(output_row_group_rows as u64) as usize;
            let fragmented = meta.row_group_rows.len() > compact_row_groups.max(1);
            let tombstoned = meta.deleted_row_count > 0;
            let under_full = meta.row_count < output_row_group_rows as u64;
            let class = if tombstoned || fragmented {
                rewrite_count += 1;
                InputClass::Rewrite
            } else if ttl.is_some_and(|(_, secs)| {
                plan_ttl_fully_expired(
                    meta,
                    ttl_plan_column.unwrap_or_default(),
                    secs.saturating_mul(1_000_000),
                    now_us,
                )
            }) {
                // Clean and full, but provably past the TTL horizon: the
                // only way these rows ever leave the disk is a rewrite.
                rewrite_count += 1;
                InputClass::Rewrite
            } else if under_full {
                small_count += 1;
                InputClass::MergeSmall
            } else {
                InputClass::Keep
            };
            classes.push(class);
        }

        let merge_smalls = rewrite_count > 0 || small_count > 1;
        let selected = segment_files
            .iter()
            .zip(classes.iter())
            .filter(|(_, &class)| match class {
                InputClass::Rewrite => true,
                InputClass::MergeSmall => merge_smalls,
                InputClass::Keep => false,
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        if selected.is_empty() {
            tracing::info!(
                db,
                table,
                partition_key = partition_key.unwrap_or("<none>"),
                segments = segment_files.len(),
                "all segments clean and full; skipping non-clustered compaction"
            );
            return Ok(None);
        }
        tracing::info!(
            db,
            table,
            partition_key = partition_key.unwrap_or("<none>"),
            rewrite_inputs = selected.len(),
            clean_kept = segment_files.len() - selected.len(),
            "non-clustered compaction selected dirty/under-full segments only"
        );
        Ok(Some(selected))
    }
}
