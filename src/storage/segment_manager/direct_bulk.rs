use super::*;

fn log_profile(stage: &str, rows: usize, elapsed: std::time::Duration) {
    if crate::common::config::insert_profile_enabled() {
        println!(
            "[insert-profile] stage=direct_bulk_{} rows={} elapsed_us={}",
            stage,
            rows,
            elapsed.as_micros()
        );
    }
}

/// Bulk appends this slow deserve a permanent WARN (not only behind the
/// insert-profile flag): the usual cause is high table COMPRESSION_LEVEL
/// (zstd 19 is single-digit MB/s per core) or a saturated disk; segment
/// seals run outside the locks, so they no longer stall concurrent writers.
const SLOW_BULK_APPEND_WARN: std::time::Duration = std::time::Duration::from_secs(10);
const SLOW_SEAL_WARN: std::time::Duration = std::time::Duration::from_secs(10);

fn warn_slow_seal(
    db: &str,
    table: &str,
    partition: &str,
    size_bytes: u64,
    elapsed: std::time::Duration,
) {
    if elapsed >= SLOW_SEAL_WARN {
        tracing::warn!(
            db,
            table,
            partition,
            size_bytes,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow segment seal; it runs outside the table locks so writers keep going, but the disk/fsync behind it is slow"
        );
    }
}

/// RAII finish of a seal registration; Drop is the safety net for error and
/// panic paths so a leaked entry can never wedge the wide-gate drain.
pub(super) struct SealingToken {
    state: Arc<TableSegmentState>,
    path: PathBuf,
}

impl Drop for SealingToken {
    fn drop(&mut self) {
        self.state.finish_sealing_segment(&self.path);
    }
}

/// A segment removed from the active map whose seal (footer + fsync) still
/// has to run — always executed after the partition and active-segments
/// locks have been released.
pub(super) struct PendingSeal {
    pub(super) token: SealingToken,
    pub(super) writer: ActiveSegmentWriter,
    pub(super) partition: String,
}

impl PendingSeal {
    pub(super) fn new(
        state: Arc<TableSegmentState>,
        partition: &str,
        writer: ActiveSegmentWriter,
    ) -> Self {
        Self {
            token: SealingToken {
                state,
                path: writer.path.clone(),
            },
            partition: partition.to_string(),
            writer,
        }
    }
}

impl SegmentManager {
    pub fn bulk_insert_payload(&self, db: &str, payload: &[u8]) -> Result<u64> {
        let req = parse_direct_bulk_payload(payload)?;
        if req.row_count == 0 {
            return Ok(0);
        }

        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;

        let schema = {
            let col_defs: Vec<crate::types::batch::ColumnDef> = req
                .columns
                .iter()
                .map(|c| {
                    crate::types::batch::ColumnDef::new(c.name, c.data_type)
                        .with_scale(c.scale)
                        .with_precision(c.precision)
                })
                .collect();
            Schema::new(col_defs)
        };

        let key = table_key(db, req.table_name);
        let state = self.get_or_create_state(&key);

        let compressed_columns =
            self.compress_direct_bulk_columns(&req.columns, bulk_compression_level())?;

        let mut pending_seal = None;
        {
            let partition_lock = partition_mutation_lock_arc(&state, "");
            let _partition_guard =
                acquire_partition_mutation_lock(&state, &partition_lock, &key, "")?;
            ensure_state_not_retired(&state, db, req.table_name)?;

            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &schema,
                "",
                &mut active_guard,
                &state.sealing_paths(),
            )?;

            let seal_full = {
                let seg = active_guard.get_mut("").unwrap();
                seg.append_direct_row_group(
                    &req.columns,
                    &compressed_columns,
                    req.row_count as u64,
                    true,
                    None,
                )?;
                seg.current_size >= self.max_segment_size()
            };

            if seal_full {
                if let Some(seg) = active_guard.remove("") {
                    state.register_sealing_segment("", seg.path.clone());
                    pending_seal = Some(PendingSeal::new(Arc::clone(&state), "", seg));
                }
            } else {
                active_guard
                    .get_mut("")
                    .unwrap()
                    .flush_meta(&|path| self.invalidate_reader(path))?;
            }
        }

        if let Some(mut pending) = pending_seal {
            let seal_start = std::time::Instant::now();
            let result = pending.writer.seal(&|path| self.invalidate_reader(path));
            let size = pending.writer.current_size;
            if let Err(error) = &result {
                tracing::error!(
                    db,
                    table = req.table_name,
                    error = %error,
                    "segment seal failed outside the locks"
                );
            } else {
                warn_slow_seal(
                    db,
                    req.table_name,
                    &pending.partition,
                    size,
                    seal_start.elapsed(),
                );
            }
            drop(pending.token);
            result?;
        }

        self.note_segments_changed(db, req.table_name);
        Ok(req.row_count as u64)
    }

    pub fn bulk_insert_payload_with_catalog(
        &self,
        db: &str,
        payload: &[u8],
        catalogs: &crate::storage::catalog::schema::DatabaseCatalog,
        flush_immediately: bool,
    ) -> Result<u64> {
        let parse_start = std::time::Instant::now();
        let req = parse_direct_bulk_payload(payload)?;
        log_profile("parse", req.row_count, parse_start.elapsed());
        if req.row_count == 0 {
            return Ok(0);
        }

        let (schema, table_meta) = {
            let table_meta = catalogs.get_table(req.table_name).ok_or_else(|| {
                TaibaiError::Sql(format!("table '{}.{}' does not exist", db, req.table_name))
            })?;
            (Schema::new(table_meta.columns.clone()), table_meta.clone())
        };

        self.validate_direct_bulk_request(&req, &schema)?;
        self.append_direct_bulk_request(db, &req, &schema, &table_meta, flush_immediately)
    }

    fn validate_direct_bulk_request(
        &self,
        req: &DirectBulkRequest<'_>,
        schema: &Schema,
    ) -> Result<()> {
        if schema.num_columns() != req.columns.len() {
            return Err(TaibaiError::Protocol(format!(
                "bulk column count mismatch: schema has {} columns but payload has {}",
                schema.num_columns(),
                req.columns.len()
            )));
        }
        for (idx, (col_def, payload_col)) in
            schema.columns.iter().zip(req.columns.iter()).enumerate()
        {
            if col_def.data_type != payload_col.data_type {
                return Err(TaibaiError::Protocol(format!(
                    "bulk column {} type mismatch for '{}': schema={} payload={}",
                    idx,
                    col_def.name,
                    col_def.data_type.display_name(),
                    payload_col.data_type.display_name()
                )));
            }
            if !col_def.name.eq_ignore_ascii_case(payload_col.name) {
                return Err(TaibaiError::Protocol(format!(
                    "bulk column {} name mismatch: schema='{}' payload='{}'",
                    idx, col_def.name, payload_col.name
                )));
            }
            if (col_def.primary_key || !col_def.nullable) && payload_col.has_nulls(req.row_count) {
                return Err(TaibaiError::Protocol(format!(
                    "bulk column '{}' does not allow NULL values",
                    col_def.name
                )));
            }
        }
        Ok(())
    }

    fn append_direct_bulk_request(
        &self,
        db: &str,
        req: &DirectBulkRequest<'_>,
        schema: &Schema,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        flush_immediately: bool,
    ) -> Result<u64> {
        // Timed from the very top: column compression (prepare_direct_bulk_groups)
        // runs before any lock and can dominate the request at high table
        // COMPRESSION_LEVEL — zstd 19 is single-digit MB/s per core.
        let total_start = std::time::Instant::now();
        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;
        self.set_table_compression_level(db, req.table_name, table_meta.compression_level);
        let compression_level = crate::storage::segment_manager::config::resolve_compression_level(
            table_meta.compression_level,
        );
        let partition_start = std::time::Instant::now();
        let partition_groups = self.direct_bulk_partition_groups(req, table_meta)?;
        log_profile("partition", req.row_count, partition_start.elapsed());
        let prepare_start = std::time::Instant::now();
        let prepared_groups = self.prepare_direct_bulk_groups(
            req,
            partition_groups,
            self.config.row_group_target_rows.max(1),
            compression_level,
        )?;
        log_profile("prepare", req.row_count, prepare_start.elapsed());
        let key = table_key(db, req.table_name);
        let state = self.get_or_create_state(&key);
        let build_value_sets = flush_immediately;
        let zone_map_columns = if flush_immediately {
            None
        } else {
            Some(self.direct_bulk_key_column_indices(schema, table_meta))
        };

        let mut partition_keys = prepared_groups
            .iter()
            .map(|group| group.partition_key.clone())
            .collect::<Vec<_>>();
        partition_keys.sort();
        partition_keys.dedup();
        let partition_refs = partition_keys
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        let result =
            self.with_partition_set_mutation_lock(db, req.table_name, &partition_refs, || {
                ensure_state_not_retired(&state, db, req.table_name)?;

                let mut written = 0u64;
                let mut pending_seals: Vec<PendingSeal> = Vec::new();
                let append_start = std::time::Instant::now();
                for group in prepared_groups {
                    let mut active_guard = state.active_segments.lock().unwrap();
                    self.get_or_create_active_segment_for_partition_locked(
                        &table_dir,
                        schema,
                        &group.partition_key,
                        &mut active_guard,
                        &state.sealing_paths(),
                    )?;
                    let seal_full = {
                        let seg = active_guard.get_mut(&group.partition_key).unwrap();
                        seg.append_direct_row_group(
                            &group.columns,
                            &group.compressed_columns,
                            group.row_count as u64,
                            build_value_sets,
                            zone_map_columns.as_deref(),
                        )?;
                        seg.current_size >= self.max_segment_size()
                    };
                    if seal_full {
                        if let Some(seg) = active_guard.remove(&group.partition_key) {
                            state.register_sealing_segment(&group.partition_key, seg.path.clone());
                            pending_seals.push(PendingSeal::new(
                                Arc::clone(&state),
                                &group.partition_key,
                                seg,
                            ));
                        }
                    } else if flush_immediately {
                        active_guard
                            .get_mut(&group.partition_key)
                            .unwrap()
                            .flush_meta(&|path| self.invalidate_reader(path))?;
                    }
                    written += group.row_count as u64;
                }
                if written > 0 {
                    self.note_segments_changed(db, req.table_name);
                }
                log_profile("append", written as usize, append_start.elapsed());
                Ok((written, pending_seals))
            });
        let (written, pending_seals) = result?;
        let mut sealed_any = false;
        let mut first_seal_error: Option<TaibaiError> = None;
        for mut pending in pending_seals {
            let partition = pending.partition.clone();
            let seal_start = std::time::Instant::now();
            let result = pending.writer.seal(&|path| self.invalidate_reader(path));
            let size = pending.writer.current_size;
            drop(pending.token);
            match result {
                Ok(()) => {
                    sealed_any = true;
                    warn_slow_seal(db, req.table_name, &partition, size, seal_start.elapsed());
                }
                Err(error) => {
                    tracing::error!(
                        db,
                        table = req.table_name,
                        partition = %partition,
                        error = %error,
                        "segment seal failed outside the locks"
                    );
                    if first_seal_error.is_none() {
                        first_seal_error = Some(error);
                    }
                }
            }
        }
        if sealed_any {
            self.note_segments_changed(db, req.table_name);
        }
        let elapsed = total_start.elapsed();
        if elapsed >= SLOW_BULK_APPEND_WARN {
            tracing::warn!(
                db,
                table = req.table_name,
                rows = req.row_count,
                compression_level,
                elapsed_ms = elapsed.as_millis() as u64,
                "slow direct bulk append: high table COMPRESSION_LEVEL makes column compression dominate (zstd 19 is single-digit MB/s per core); a saturated disk is the other usual cause (segment seals run outside the locks)"
            );
        }
        if let Some(error) = first_seal_error {
            return Err(error);
        }
        Ok(written)
    }

    fn direct_bulk_key_column_indices(
        &self,
        schema: &Schema,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> Vec<usize> {
        let mut indices = Vec::new();
        if let Some(idx) = table_meta.get_partition_column_index_cached() {
            indices.push(idx);
        }
        for key in &table_meta.cluster_key {
            if let Some(idx) = schema.index_of(key) {
                if !indices.contains(&idx) {
                    indices.push(idx);
                }
            }
        }
        indices
    }

    fn direct_bulk_partition_groups(
        &self,
        req: &DirectBulkRequest<'_>,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> Result<Vec<(String, Vec<usize>)>> {
        if !table_meta.is_partitioned() {
            return Ok(vec![(String::new(), (0..req.row_count).collect())]);
        }
        let pk_idx = table_meta
            .get_partition_column_index_cached()
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "partitioned table '{}' has no cached partition column index",
                    table_meta.name
                ))
            })?;
        let pk_col = req.columns.get(pk_idx).ok_or_else(|| {
            TaibaiError::Protocol(format!(
                "partition column index {} out of bulk payload",
                pk_idx
            ))
        })?;
        let strategy = table_meta.partition_strategy();

        // Uniform fast path: day-ordered imports land every row of a request
        // in one partition. If the partition column's raw slots are all equal
        // (and NULL-free), one key computation replaces the per-row
        // scalar + format! work.
        if let Some(key) = direct_bulk_uniform_partition_key(pk_col, req.row_count, &strategy)? {
            return Ok(vec![(key, (0..req.row_count).collect())]);
        }

        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        let mut order = Vec::new();
        for row_idx in 0..req.row_count {
            let value = helpers::direct_bulk_column_scalar_at(pk_col, row_idx)?;
            let partition_key = compute_partition_key_from_scalar(&strategy, &value);
            if !groups.contains_key(&partition_key) {
                order.push(partition_key.clone());
                groups.insert(partition_key.clone(), Vec::new());
            }
            groups.get_mut(&partition_key).unwrap().push(row_idx);
        }
        Ok(order
            .into_iter()
            .filter_map(|key| groups.remove(&key).map(|rows| (key, rows)))
            .collect())
    }

    fn prepare_direct_bulk_groups<'a>(
        &self,
        req: &'a DirectBulkRequest<'a>,
        partition_groups: Vec<(String, Vec<usize>)>,
        row_group_target_rows: usize,
        compression_level: i32,
    ) -> Result<Vec<PreparedDirectBulkGroup<'a>>> {
        let mut prepared = Vec::with_capacity(partition_groups.len());
        for (partition_key, rows) in partition_groups {
            if rows.is_empty() {
                continue;
            }
            // Row groups are cut from contiguous runs of the request so every
            // group stays a zero-copy sub-slice of the payload; the per-value
            // gather only existed because arbitrary chunks rarely covered the
            // whole request.
            for (run_start, run_len) in contiguous_row_runs(&rows) {
                let mut offset = 0usize;
                while offset < run_len {
                    let piece_len = (run_len - offset).min(row_group_target_rows.max(1));
                    let first_row = run_start + offset;
                    let columns = direct_bulk_zero_copy_columns(req, first_row, piece_len)?;
                    let compress_start = std::time::Instant::now();
                    let compressed_columns =
                        self.compress_direct_bulk_columns(&columns, compression_level)?;
                    log_profile("compress", piece_len, compress_start.elapsed());
                    prepared.push(PreparedDirectBulkGroup {
                        partition_key: partition_key.clone(),
                        row_count: piece_len,
                        columns,
                        compressed_columns,
                    });
                    offset += piece_len;
                }
            }
        }
        Ok(prepared)
    }

    fn compress_direct_bulk_columns(
        &self,
        columns: &[DirectBulkColumn<'_>],
        compression_level: i32,
    ) -> Result<Vec<(Vec<u8>, u64)>> {
        use rayon::prelude::*;
        columns
            .par_iter()
            .map(|col| {
                let encoded = encode_direct_bulk_column(col)?;
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| TaibaiError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()
    }
}

/// Split an ascending row-index list into maximal contiguous
/// `(first_row, len)` runs. Contiguous runs tile the input, so zero-copy
/// row groups can cover arbitrary partition groupings.
/// Partition key of the whole request when every row carries the identical
/// fixed-width partition-column value, else `None`. Raw-slot equality implies
/// key equality, so a memcmp scan is sufficient to detect the uniform case;
/// non-uniform requests fall back to per-row key computation.
fn direct_bulk_uniform_partition_key(
    pk_col: &DirectBulkColumn<'_>,
    row_count: usize,
    strategy: &crate::storage::partition::partition_function::PartitionFunction,
) -> Result<Option<String>> {
    let Some(width) = pk_col.data_type.fixed_size() else {
        return Ok(None);
    };
    if row_count == 0 || !pk_col.nulls.iter().all(|&byte| byte == 0) {
        return Ok(None);
    }
    let n = row_count.min(pk_col.data.len() / width);
    if n == 0 || n != row_count {
        return Ok(None);
    }
    let first = &pk_col.data[..width];
    if !pk_col.data[width..n * width]
        .chunks_exact(width)
        .all(|slot| slot == first)
    {
        return Ok(None);
    }
    let value = helpers::direct_bulk_column_scalar_at(pk_col, 0)?;
    Ok(Some(compute_partition_key_from_scalar(strategy, &value)))
}

fn contiguous_row_runs(rows: &[usize]) -> Vec<(usize, usize)> {
    let mut runs = Vec::new();
    let mut idx = 0usize;
    while idx < rows.len() {
        let start = rows[idx];
        let mut end = idx + 1;
        while end < rows.len() && rows[end] == rows[end - 1] + 1 {
            end += 1;
        }
        runs.push((start, end - idx));
        idx = end;
    }
    runs
}

/// Borrow the row range `[first_row, first_row + len)` of a parsed bulk
/// request as zero-copy column slices. Fixed-width columns sub-slice the raw
/// payload directly; variable columns keep the whole arena and slice the
/// per-row ranges. Returns an empty null bitmap when the range has no NULLs so
/// downstream encoding keeps its null-free fast path.
fn direct_bulk_zero_copy_columns<'a>(
    req: &'a DirectBulkRequest<'a>,
    first_row: usize,
    len: usize,
) -> Result<Vec<DirectBulkColumn<'a>>> {
    let end_row = first_row
        .checked_add(len)
        .ok_or_else(|| TaibaiError::Protocol("bulk row range overflow".into()))?;
    req.columns
        .iter()
        .map(|col| {
            let nulls: Vec<u8> = if col.nulls.is_empty() {
                Vec::new()
            } else {
                slice_null_bitmap(&col.nulls, first_row, len)
            };
            if let Some(width) = col.data_type.fixed_size() {
                let start = first_row
                    .checked_mul(width)
                    .ok_or_else(|| TaibaiError::Protocol("bulk column offset overflow".into()))?;
                let end = start
                    .checked_add(len.checked_mul(width).ok_or_else(|| {
                        TaibaiError::Protocol("bulk column length overflow".into())
                    })?)
                    .ok_or_else(|| TaibaiError::Protocol("bulk column offset overflow".into()))?;
                if end > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' rows {}..{} are truncated",
                        col.name, first_row, end_row
                    )));
                }
                Ok(DirectBulkColumn {
                    name: col.name,
                    data_type: col.data_type,
                    scale: col.scale,
                    precision: col.precision,
                    data: &col.data[start..end],
                    nulls: std::borrow::Cow::Owned(nulls),
                    variable_ranges: None,
                })
            } else {
                let ranges = col.variable_ranges.as_ref().ok_or_else(|| {
                    TaibaiError::Protocol(format!(
                        "bulk column '{}' is missing variable value ranges",
                        col.name
                    ))
                })?;
                if end_row > ranges.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' rows {}..{} are truncated",
                        col.name, first_row, end_row
                    )));
                }
                // The wire arena is a concatenation of per-row
                // [u32 length prefix][value] slots, so a contiguous row range
                // occupies one contiguous arena span. Slice it — the codec's
                // null-free fast path appends `data` verbatim, so passing the
                // whole request arena would replay every piece's rows in every
                // row group — and re-base the ranges onto the slice.
                let first = &ranges[first_row];
                let last = &ranges[end_row - 1];
                let start_prefix = first
                    .0
                    .checked_sub(4)
                    .ok_or_else(|| TaibaiError::Protocol("bulk variable range underflow".into()))?;
                let end = last
                    .0
                    .checked_add(last.1)
                    .ok_or_else(|| TaibaiError::Protocol("bulk variable range overflow".into()))?;
                if end > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' rows {}..{} are truncated",
                        col.name, first_row, end_row
                    )));
                }
                Ok(DirectBulkColumn {
                    name: col.name,
                    data_type: col.data_type,
                    scale: col.scale,
                    precision: col.precision,
                    data: &col.data[start_prefix..end],
                    nulls: std::borrow::Cow::Owned(nulls),
                    variable_ranges: Some(
                        ranges[first_row..end_row]
                            .iter()
                            .map(|(offset, len)| (offset - start_prefix, *len))
                            .collect(),
                    ),
                })
            }
        })
        .collect()
}

/// Extract bits `[start, start + len)` of a row-null bitmap into a fresh
/// bitmap. NullBitmap serializes complete little-endian u64 words with zero
/// trailing bits (`types::bitmap::from_bytes` copies whole words and would
/// drop a byte-granular tail), so the output is word-aligned and trimmed;
/// null-free ranges return an empty bitmap to keep the codec's fast path.
fn slice_null_bitmap(nulls: &[u8], start: usize, len: usize) -> Vec<u8> {
    let mut out = vec![0u8; len.div_ceil(64) * 8];
    let mut any = false;
    if start.is_multiple_of(8) {
        let src = &nulls[start / 8..];
        let byte_len = len.div_ceil(8);
        for (i, byte) in out.iter_mut().enumerate().take(byte_len) {
            *byte = src.get(i).copied().unwrap_or(0);
        }
        let tail_bits = len % 8;
        if tail_bits != 0 {
            out[byte_len - 1] &= (1u8 << tail_bits) - 1;
        }
        any = out.iter().any(|&byte| byte != 0);
    } else {
        for row in 0..len {
            let src_bit = start + row;
            if (nulls[src_bit / 8] >> (src_bit % 8)) & 1 != 0 {
                out[row / 8] |= 1 << (row % 8);
                any = true;
            }
        }
    }
    if any {
        out
    } else {
        Vec::new()
    }
}

/// Convert the wire representation (one physical value slot per row) into the
/// segment codec representation (bitmap followed by non-null values only).
fn encode_direct_bulk_column(col: &DirectBulkColumn<'_>) -> Result<Vec<u8>> {
    let bitmap_len = u32::try_from(col.nulls.len()).map_err(|_| {
        TaibaiError::Protocol(format!(
            "bulk column '{}' null bitmap is too large",
            col.name
        ))
    })?;
    let mut encoded = Vec::with_capacity(4 + col.nulls.len() + col.data.len());
    encoded.extend_from_slice(&bitmap_len.to_le_bytes());
    encoded.extend_from_slice(&col.nulls);

    if col.nulls.is_empty() {
        encoded.extend_from_slice(col.data);
        return Ok(encoded);
    }

    if let Some(width) = col.data_type.fixed_size() {
        let row_count = col.data.len() / width;
        for row_idx in 0..row_count {
            if !col.is_null(row_idx) {
                let offset = row_idx * width;
                encoded.extend_from_slice(&col.data[offset..offset + width]);
            }
        }
        return Ok(encoded);
    }

    let ranges = col.variable_ranges.as_ref().ok_or_else(|| {
        TaibaiError::Protocol(format!(
            "bulk column '{}' is missing variable value ranges",
            col.name
        ))
    })?;
    for (row_idx, &(offset, len)) in ranges.iter().enumerate() {
        if col.is_null(row_idx) {
            continue;
        }
        let end = offset.checked_add(len).ok_or_else(|| {
            TaibaiError::Protocol(format!("bulk column '{}' range overflow", col.name))
        })?;
        if end > col.data.len() {
            return Err(TaibaiError::Protocol(format!(
                "bulk column '{}' variable data truncated",
                col.name
            )));
        }
        let len = u32::try_from(len).map_err(|_| {
            TaibaiError::Protocol(format!("bulk column '{}' value is too large", col.name))
        })?;
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(&col.data[offset..end]);
    }
    Ok(encoded)
}
