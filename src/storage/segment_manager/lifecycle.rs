impl SegmentManager {
    pub fn bulk_insert_batch_partitioned(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, true)
    }

    pub fn bulk_insert_batch_partitioned_deferred(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, false)
    }

    fn bulk_insert_batch_partitioned_inner(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        flush_immediately: bool,
    ) -> Result<u64> {
        if !table_meta.is_partitioned() {
            return self.bulk_insert_batch_with_partition_inner(
                db,
                table,
                "",
                &batch,
                flush_immediately,
            );
        }

        let pk_idx = match table_meta.get_partition_column_index_cached() {
            Some(idx) => idx,
            None => {
                return self.bulk_insert_batch_with_partition_inner(
                    db,
                    table,
                    "",
                    &batch,
                    flush_immediately,
                )
            }
        };
        let partition_strategy = table_meta.partition_strategy();
        let first_val = batch.scalar(0, pk_idx).unwrap_or(ScalarValue::Int64(None));
        let first_key = compute_partition_key_from_scalar(&partition_strategy, &first_val);
        let mut single_partition = true;
        for row_idx in 1..batch.num_rows() {
            let val = batch
                .scalar(row_idx, pk_idx)
                .unwrap_or(ScalarValue::Int64(None));
            let key = compute_partition_key_from_scalar(&partition_strategy, &val);
            if key != first_key {
                single_partition = false;
                break;
            }
        }
        if single_partition {
            return self.bulk_insert_batch_with_partition_inner(
                db,
                table,
                &first_key,
                &batch,
                flush_immediately,
            );
        }

        let mut ranges: Vec<(String, usize, usize)> = Vec::new();
        let mut contiguous = true;
        let mut current_key = first_key.clone();
        let mut current_start = 0usize;
        let mut seen_keys = HashSet::new();
        seen_keys.insert(first_key.clone());
        for row_idx in 1..batch.num_rows() {
            let val = batch
                .scalar(row_idx, pk_idx)
                .unwrap_or(ScalarValue::Int64(None));
            let key = compute_partition_key_from_scalar(&partition_strategy, &val);
            if key != current_key {
                if seen_keys.contains(&key) {
                    contiguous = false;
                    break;
                }
                ranges.push((current_key, current_start, row_idx - current_start));
                seen_keys.insert(key.clone());
                current_key = key;
                current_start = row_idx;
            }
        }
        if contiguous {
            ranges.push((current_key, current_start, batch.num_rows() - current_start));
            let mut written = 0;
            for (partition_key, start, len) in ranges {
                let part_batch = slice_batch_fast(&batch, start, len)?;
                written += self.bulk_insert_batch_with_partition_inner(
                    db,
                    table,
                    &partition_key,
                    &part_batch,
                    flush_immediately,
                )?;
            }
            return Ok(written);
        }

        let mut key_order = Vec::new();
        let mut groups: HashMap<String, Vec<bool>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let val = batch
                .scalar(row_idx, pk_idx)
                .unwrap_or(ScalarValue::Int64(None));
            let key = compute_partition_key_from_scalar(&table_meta.partition_strategy(), &val);
            if !groups.contains_key(&key) {
                key_order.push(key.clone());
                groups.insert(key.clone(), vec![false; batch.num_rows()]);
            }
            if let Some(mask) = groups.get_mut(&key) {
                mask[row_idx] = true;
            }
        }

        let mut written = 0;
        for partition_key in key_order {
            let Some(mask) = groups.remove(&partition_key) else {
                continue;
            };
            let match_count = mask.iter().filter(|keep| **keep).count();
            let part_batch = filter_batch_by_mask(&batch, &mask, match_count)?;
            written += self.bulk_insert_batch_with_partition_inner(
                db,
                table,
                &partition_key,
                &part_batch,
                flush_immediately,
            )?;
        }
        Ok(written)
    }

    pub fn flush_table_active(&self, db: &str, table: &str) -> Result<u64> {
        let table_dir = self.table_dir(db, table);

        let key = table_key(db, table);
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal: Vec<String> = active_guard
                .iter()
                .filter(|(_, seg)| {
                    seg.total_rows > 0 && seg.current_size >= self.min_segment_seal_size()
                })
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_seal {
                if let Some(seg) = active_guard.get_mut(&key) {
                    seg.seal(&|path| self.invalidate_reader(path))?;
                    total += seg.total_rows;
                }
                active_guard.remove(&key);
            }

            for (_, seg) in active_guard.iter_mut() {
                if seg.total_rows > 0 || seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                    seg.flush_meta(&|path| self.invalidate_reader(path))?;
                    total += seg.total_rows;
                }
            }
        }
        drop(tables);
        self.clear_wal(&table_dir)?;
        Ok(total)
    }

    /// Flush and seal every non-empty active segment for one table, regardless
    /// of the normal minimum segment size. Close-of-day optimization needs the
    /// segments to become immutable so they are visible to the compactor.
    pub fn seal_table_active(&self, db: &str, table: &str) -> Result<u64> {
        let table_dir = self.table_dir(db, table);
        let key = table_key(db, table);
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal = active_guard
                .iter()
                .filter(|(_, segment)| segment.total_rows > 0 || segment.buffered_rows > 0)
                .map(|(partition_key, _)| partition_key.clone())
                .collect::<Vec<_>>();

            for partition_key in keys_to_seal {
                if let Some(segment) = active_guard.get_mut(&partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    total += segment.total_rows;
                    tracing::info!(
                        path = %segment.path.display(),
                        rows = segment.total_rows,
                        partition_key,
                        "sealing active segment for close-of-day optimization"
                    );
                }
                active_guard.remove(&partition_key);
            }
        }
        drop(tables);
        self.clear_wal(&table_dir)?;
        Ok(total)
    }

    pub fn flush_partition_active(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<u64> {
        let key = table_key(db, table);
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut active_guard = state.active_segments.lock().unwrap();
            let should_seal = active_guard.get(partition_key).is_some_and(|segment| {
                segment.total_rows > 0 && segment.current_size >= self.min_segment_seal_size()
            });
            if should_seal {
                if let Some(segment) = active_guard.get_mut(partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    total = total.saturating_add(segment.total_rows);
                }
                active_guard.remove(partition_key);
            } else if let Some(segment) = active_guard.get_mut(partition_key) {
                if segment.total_rows > 0 || segment.buffered_rows > 0 {
                    segment.flush_buffer(&|path| self.invalidate_reader(path))?;
                    segment.flush_meta(&|path| self.invalidate_reader(path))?;
                    total = total.saturating_add(segment.total_rows);
                }
            }
        }
        Ok(total)
    }

    /// Seal the target table's active writers while its mutation lock is held.
    /// Unlike a checkpoint flush this deliberately leaves the table WAL alone:
    /// pending memtables may still depend on it.
    pub(crate) fn seal_table_active_under_table_lock(
        &self,
        db: &str,
        table: &str,
    ) -> Result<u64> {
        let key = table_key(db, table);
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal = active_guard
                .iter()
                .filter(|(_, segment)| segment.total_rows > 0 || segment.buffered_rows > 0)
                .map(|(partition_key, _)| partition_key.clone())
                .collect::<Vec<_>>();

            for partition_key in keys_to_seal {
                if let Some(segment) = active_guard.get_mut(&partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    total += segment.total_rows;
                }
                active_guard.remove(&partition_key);
            }
        }
        Ok(total)
    }

    pub(crate) fn seal_partition_active_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<u64> {
        let key = table_key(db, table);
        let tables = self.tables.read().unwrap();
        let Some(state) = tables.get(&key) else {
            return Ok(0);
        };
        let mut active_guard = state.active_segments.lock().unwrap();
        let Some(segment) = active_guard.get_mut(partition_key) else {
            return Ok(0);
        };
        if segment.total_rows == 0 && segment.buffered_rows == 0 {
            return Ok(0);
        }
        segment.seal(&|path| self.invalidate_reader(path))?;
        let rows = segment.total_rows;
        active_guard.remove(partition_key);
        Ok(rows)
    }

    pub fn flush_all_active(&self) -> Result<u64> {
        self.sync_all_wals();

        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal: Vec<String> = active_guard
                .iter()
                .filter(|(_, seg)| {
                    seg.total_rows > 0 && seg.current_size >= self.min_segment_seal_size()
                })
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_seal {
                if let Some(seg) = active_guard.get_mut(&key) {
                    seg.seal(&|path| self.invalidate_reader(path))?;
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
                        partition_key = key,
                        "sealing segment on flush (meets minimum size threshold)"
                    );
                    total += seg.total_rows;
                }
                active_guard.remove(&key);
            }

            for (_, seg) in active_guard.iter_mut() {
                if seg.total_rows > 0 || seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                    seg.flush_meta(&|path| self.invalidate_reader(path))?;
                    total += seg.total_rows;
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
                        buffered = seg.buffered_rows,
                        "active segment flushed but kept open (below minimum seal size)"
                    );
                }
            }
        }
        drop(tables);

        self.clear_all_wals();

        Ok(total)
    }

    fn periodic_flush_check(&self) -> Result<u64> {
        let now = Instant::now();
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut active_guard = state.active_segments.lock().unwrap();
            for (_, seg) in active_guard.iter_mut() {
                if !seg.dirty_meta && seg.buffered_rows == 0 {
                    continue;
                }
                let idle = now.duration_since(seg.last_write_at).as_secs();
                if idle < FLUSH_IDLE_THRESHOLD_SECS {
                    continue;
                }
                if seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                }
                if seg.dirty_meta {
                    seg.flush_meta(&|path| self.invalidate_reader(path))?;
                }
                total += seg.total_rows;
            }
        }
        Ok(total)
    }

    pub fn begin_txn(&self) -> u64 {
        self.alloc_txn_id()
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<()> {
        self.sync_all_wals();
        tracing::debug!(txn_id, "transaction committed in storage layer");
        Ok(())
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<()> {
        tracing::debug!(txn_id, "transaction rolled back in storage layer");
        Ok(())
    }

    pub fn write_txn_wal(&self, table_dir: &Path, txn_id: u64, batch: &RecordBatch) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;

        let wal_data = crate::sql::execution::wal_helper::serialize_batch_for_wal(batch);

        let mut wal_guard = state.wal_writer.lock().unwrap();
        if wal_guard.is_none() {
            let wal_dir = table_dir.join("wal");
            let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
            *wal_guard = Some(writer);
        }
        let wal_writer = wal_guard.as_mut().unwrap();
        wal_writer.write_compact(txn_id, &wal_data, false)?;
        Ok(())
    }

    pub fn clear_txn_wal(&self, table_dir: &Path, txn_id: u64) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear_entries_for_txn(txn_id)?;
        }
        Ok(())
    }

    pub fn sync_table_wal(&self, table_dir: &Path) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.sync()?;
        }
        Ok(())
    }
}
