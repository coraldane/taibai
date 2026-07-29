impl SegmentManager {
    pub fn bulk_insert_payload(&self, db: &str, payload: &[u8], _disable_wal: bool) -> Result<u64> {
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

        let compressed_columns = self.compress_direct_bulk_columns(&req.columns)?;

        {
            let _mutation_guard = state.mutation_lock.lock().unwrap();
            ensure_state_not_retired(&state, db, req.table_name)?;

            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &schema,
                "",
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut("").unwrap();

            seg.append_direct_row_group(
                &req.columns,
                &compressed_columns,
                req.row_count as u64,
                true,
                None,
            )?;

            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove("");
            } else {
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            }
        }

        Ok(req.row_count as u64)
    }

    pub fn bulk_insert_payload_with_catalog(
        &self,
        db: &str,
        payload: &[u8],
        _disable_wal: bool,
        catalogs: &crate::storage::catalog::schema::DatabaseCatalog,
        flush_immediately: bool,
    ) -> Result<u64> {
        let req = parse_direct_bulk_payload(payload)?;
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
            if (col_def.primary_key || !col_def.nullable)
                && payload_col.has_nulls(req.row_count)
            {
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
        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;
        let partition_groups = self.direct_bulk_partition_groups(req, table_meta)?;
        let prepared_groups = self.prepare_direct_bulk_groups(
            req,
            partition_groups,
            self.config.row_group_target_rows.max(1),
        )?;
        let key = table_key(db, req.table_name);
        let state = self.get_or_create_state(&key);
        let build_value_sets = flush_immediately;
        let zone_map_columns = if flush_immediately {
            None
        } else {
            Some(self.direct_bulk_key_column_indices(schema, table_meta))
        };

        let _mutation_guard = state.mutation_lock.lock().unwrap();
        ensure_state_not_retired(&state, db, req.table_name)?;

        let mut written = 0u64;
        let mut active_guard = state.active_segments.lock().unwrap();
        for group in prepared_groups {
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                schema,
                &group.partition_key,
                &mut active_guard,
            )?;
            let seg = active_guard.get_mut(&group.partition_key).unwrap();
            match &group.columns {
                PreparedDirectColumns::Borrowed(columns) => {
                    seg.append_direct_row_group(
                        columns,
                        &group.compressed_columns,
                        group.row_count as u64,
                        build_value_sets,
                        zone_map_columns.as_deref(),
                    )?;
                }
                PreparedDirectColumns::Owned(storage) => {
                    let columns: Vec<DirectBulkColumn<'_>> = storage
                        .iter()
                        .map(|col| DirectBulkColumn {
                            name: col.name,
                            data_type: col.data_type,
                            scale: col.scale,
                            precision: col.precision,
                            data: &col.data,
                            nulls: &col.nulls,
                            variable_ranges: col.variable_ranges.clone(),
                        })
                        .collect();
                    seg.append_direct_row_group(
                        &columns,
                        &group.compressed_columns,
                        group.row_count as u64,
                        build_value_sets,
                        zone_map_columns.as_deref(),
                    )?;
                }
            }
            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove(&group.partition_key);
            } else if flush_immediately {
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            }
            written += group.row_count as u64;
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
    ) -> Result<Vec<PreparedDirectBulkGroup<'a>>> {
        let mut prepared = Vec::with_capacity(partition_groups.len());
        for (partition_key, rows) in partition_groups {
            if rows.is_empty() {
                continue;
            }
            for chunk in rows.chunks(row_group_target_rows.max(1)) {
            if self.direct_bulk_rows_are_full_range(chunk, req.row_count) {
                let columns: Vec<DirectBulkColumn<'a>> = req
                    .columns
                    .iter()
                    .map(|col| DirectBulkColumn {
                        name: col.name,
                        data_type: col.data_type,
                        scale: col.scale,
                        precision: col.precision,
                        data: col.data,
                        nulls: col.nulls,
                        variable_ranges: col.variable_ranges.clone(),
                    })
                    .collect();
                let compressed_columns = self.compress_direct_bulk_columns(&columns)?;
                prepared.push(PreparedDirectBulkGroup {
                    partition_key: partition_key.clone(),
                    row_count: req.row_count,
                    columns: PreparedDirectColumns::Borrowed(columns),
                    compressed_columns,
                });
            } else {
                let storage = self.direct_bulk_select_columns(req, chunk)?;
                let columns: Vec<DirectBulkColumn<'_>> = storage
                    .iter()
                    .map(|col| DirectBulkColumn {
                        name: col.name,
                        data_type: col.data_type,
                        scale: col.scale,
                        precision: col.precision,
                        data: &col.data,
                        nulls: &col.nulls,
                        variable_ranges: col.variable_ranges.clone(),
                    })
                    .collect();
                let compressed_columns = self.compress_direct_bulk_columns(&columns)?;
                prepared.push(PreparedDirectBulkGroup {
                    partition_key: partition_key.clone(),
                    row_count: chunk.len(),
                    columns: PreparedDirectColumns::Owned(storage),
                    compressed_columns,
                });
            }
            }
        }
        Ok(prepared)
    }

    fn direct_bulk_rows_are_full_range(&self, rows: &[usize], row_count: usize) -> bool {
        rows.len() == row_count
            && rows
                .iter()
                .enumerate()
                .all(|(idx, &row_idx)| idx == row_idx)
    }

    fn direct_bulk_select_columns<'a>(
        &self,
        req: &'a DirectBulkRequest<'a>,
        rows: &[usize],
    ) -> Result<Vec<OwnedDirectBulkColumn<'a>>> {
        req.columns
            .iter()
            .map(|col| self.direct_bulk_select_column(col, rows))
            .collect()
    }

    fn direct_bulk_select_column<'a>(
        &self,
        col: &DirectBulkColumn<'a>,
        rows: &[usize],
    ) -> Result<OwnedDirectBulkColumn<'a>> {
        let mut data = Vec::new();
        let mut variable_ranges = col
            .variable_ranges
            .as_ref()
            .map(|_| Vec::with_capacity(rows.len()));
        let mut nulls = if col.nulls.is_empty() {
            Vec::new()
        } else {
            vec![0u8; rows.len().div_ceil(64) * 8]
        };
        let mut selected_has_nulls = false;
        for (selected_idx, &row_idx) in rows.iter().enumerate() {
            if col.is_null(row_idx) {
                nulls[selected_idx / 8] |= 1u8 << (selected_idx % 8);
                selected_has_nulls = true;
            }
        }
        if !selected_has_nulls {
            nulls.clear();
        }
        if let Some(width) = col.data_type.fixed_size() {
            data.reserve(rows.len() * width);
            for &row_idx in rows {
                let offset = row_idx
                    .checked_mul(width)
                    .ok_or_else(|| TaibaiError::Protocol("bulk column offset overflow".into()))?;
                let end = offset + width;
                if end > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' row {} is truncated",
                        col.name, row_idx
                    )));
                }
                data.extend_from_slice(&col.data[offset..end]);
            }
        } else {
            for &row_idx in rows {
                let (offset, len) = col
                    .variable_ranges
                    .as_ref()
                    .and_then(|ranges| ranges.get(row_idx).copied())
                    .ok_or_else(|| {
                        TaibaiError::Protocol(format!(
                            "bulk column '{}' row {} not found",
                            col.name, row_idx
                        ))
                    })?;
                if offset + len > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' variable data truncated",
                        col.name
                    )));
                }
                data.extend_from_slice(&(len as u32).to_le_bytes());
                let value_offset = data.len();
                data.extend_from_slice(&col.data[offset..offset + len]);
                if let Some(ranges) = variable_ranges.as_mut() {
                    ranges.push((value_offset, len));
                }
            }
        }
        Ok(OwnedDirectBulkColumn {
            name: col.name,
            data_type: col.data_type,
            scale: col.scale,
            precision: col.precision,
            data,
            nulls,
            variable_ranges,
        })
    }

    fn compress_direct_bulk_columns(
        &self,
        columns: &[DirectBulkColumn<'_>],
    ) -> Result<Vec<(Vec<u8>, u64)>> {
        use rayon::prelude::*;
        let compression_level = bulk_compression_level();
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

/// Convert the wire representation (one physical value slot per row) into the
/// segment codec representation (bitmap followed by non-null values only).
fn encode_direct_bulk_column(col: &DirectBulkColumn<'_>) -> Result<Vec<u8>> {
    let bitmap_len = u32::try_from(col.nulls.len()).map_err(|_| {
        TaibaiError::Protocol(format!("bulk column '{}' null bitmap is too large", col.name))
    })?;
    let mut encoded = Vec::with_capacity(4 + col.nulls.len() + col.data.len());
    encoded.extend_from_slice(&bitmap_len.to_le_bytes());
    encoded.extend_from_slice(col.nulls);

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
