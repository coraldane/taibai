//! Clustered decoding, sorting and preparation of external runs.

use super::*;

impl Compactor {
    /// Build the per-row-group read sources of the clustered path and the
    /// global row count they cover (the partition's row space for the sort).
    pub(super) fn collect_clustered_sources(
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

    #[allow(clippy::too_many_arguments)]
    /// Decode every source column into one contiguous partition batch,
    /// purge deleted and TTL-expired rows from the global index, and sort it
    /// by the cluster key. In-memory only: the caller has already checked
    /// the memory budget and the u32 row-count bound.
    pub(super) fn decode_clustered_partition(
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

        // Account decoded columns incrementally; the first over-budget column
        // aborts this attempt before more columns allocate, and the caller
        // falls back to external runs. Variable-width buffers move into place.
        let mut decoded_bytes = sorted_indices.capacity() * std::mem::size_of::<u32>();
        let columns = schema
            .columns
            .iter()
            .enumerate()
            .map(|(column_idx, column_def)| -> Result<ColumnArray> {
                let mut destination =
                    ColumnArray::with_capacity(column_def.data_type, global_row_base);
                for source in sources {
                    let decoded = source.reader.read_catalog_column(
                        source.row_group_idx,
                        schema,
                        column_idx,
                    )?;
                    ensure_compaction_memory(
                        decoded_bytes
                            .saturating_add(destination.estimated_size_bytes())
                            .saturating_add(decoded.estimated_size_bytes().saturating_mul(2)),
                        clustered_merge_budget_bytes() / 2,
                        "in-memory decode",
                    )?;
                    destination.append_owned(decoded)?;
                }
                decoded_bytes = decoded_bytes.saturating_add(destination.estimated_size_bytes());
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

    /// Write the clustered in-memory rewrite: gather the pre-sorted global
    /// row indices out of the contiguous decoded batch in output-row-group
    /// chunks. The unsorted rewrite streams its own chunks inside
    /// `rewrite_unsorted_partition`; this writer performs no source I/O —
    /// the decode pass already holds every column in memory.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_clustered_segment(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        path: &Path,
        schema: &Schema,
        bloom_column_indices: &[usize],
        source: &RecordBatch,
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

        writer.seal()
    }

    /// External-merge clustered rewrite: build bounded sorted runs, merge
    /// them (in waves when necessary) into the output tmp segment. Returns
    /// (rows written to the output, rows dropped as deleted/expired).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare_clustered_external(
        &self,
        db: &str,
        table: &str,
        partition_label: &str,
        tmp_path: &Path,
        _final_path: &Path,
        schema: &Schema,
        sort_key_indices: &[usize],
        bloom_column_indices: &[usize],
        segment_files: &[PathBuf],
        ttl: Option<(usize, i64)>,
        output_row_group_rows: usize,
        compression_level: i32,
    ) -> Result<(usize, u64, u64)> {
        let mut scratch = super::runs::TempRuns::default();
        let (run_paths, dropped, dropped_ttl) = self.build_clustered_runs(
            db,
            table,
            partition_label,
            tmp_path,
            schema,
            sort_key_indices,
            segment_files,
            ttl,
            output_row_group_rows,
            &mut scratch,
        )?;
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
        let mut fan_in = self.merge_fan_in(&current_runs, schema)?;
        let mut created_in_round: Vec<PathBuf> = Vec::new();
        let result = (|| -> Result<usize> {
            while current_runs.len() > fan_in {
                wave += 1;
                let mut next_runs = Vec::new();
                for (next_seq, group) in current_runs.chunks(fan_in).enumerate() {
                    let merged_path = merge_wave_path(tmp_path, wave, next_seq);
                    scratch.0.push(merged_path.clone());
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
                fan_in = self.merge_fan_in(&current_runs, schema)?;
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
            cleanup_runs(&created_in_round);
        }
        let written = result?;
        #[cfg(test)]
        EXTERNAL_MERGE_WAVES.with(|n| n.set(wave));
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
}

#[cfg(test)]
thread_local! { pub(super) static EXTERNAL_MERGE_WAVES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) fn estimated_row_width_bytes(schema: &Schema) -> usize {
    schema
        .columns
        .iter()
        .map(|col| {
            // Variable-width columns (VarChar/Blob/Vector) carry a
            // conservative per-value allowance: underestimating them let
            // external-merge runs overshoot the memory budget.
            let base = col.data_type.fixed_size().unwrap_or(0).max(8);
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
pub(super) fn merge_run_path(final_path: &Path, seq: usize) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.merge.tmp.{}", name, seq))
}

pub(super) fn merge_wave_path(final_path: &Path, wave: usize, seq: usize) -> PathBuf {
    let name = final_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.merge.tmp.w{}n{}", name, wave, seq))
}

pub(super) fn sort_clustered_row_indices(
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

pub(super) fn gather_global_chunk(
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

pub(super) fn compare_column_rows(
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
