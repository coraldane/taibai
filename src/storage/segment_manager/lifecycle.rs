impl SegmentManager {
    pub fn bulk_insert_batch_partitioned(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, true, false)
    }

    pub fn bulk_insert_batch_partitioned_deferred(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, false, true)
    }

    fn bulk_insert_batch_partitioned_inner(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
    ) -> Result<u64> {
        if !table_meta.is_partitioned() {
            return self.bulk_insert_batch_with_partition_owned_inner(
                db,
                table,
                "",
                batch,
                flush_immediately,
                pubsub_deferred,
            );
        }

        let pk_idx = match table_meta.get_partition_column_index_cached() {
            Some(idx) => idx,
            None => {
                return self.bulk_insert_batch_with_partition_owned_inner(
                    db,
                    table,
                    "",
                    batch,
                    flush_immediately,
                    pubsub_deferred,
                )
            }
        };
        let partition_strategy = table_meta.partition_strategy();
        let partition_column = batch.column(pk_idx);
        let mut keys = partition_column
            .map(|column| {
                crate::storage::partition::partition_function::PartitionKeysAt::new(
                    crate::storage::partition::partition_function::PartitionKeyKind::Strategy(
                        partition_strategy,
                    ),
                    column,
                )
            })
            .expect("partitioned table must expose its partition column");
        let first_key = keys.key_at(0).to_string();
        let mut single_partition = true;
        for row_idx in 1..batch.num_rows() {
            if keys.key_at(row_idx) != first_key {
                single_partition = false;
                break;
            }
        }
        if single_partition {
            return self.bulk_insert_batch_with_partition_owned_inner(
                db,
                table,
                &first_key,
                batch,
                flush_immediately,
                pubsub_deferred,
            );
        }

        let mut ranges: Vec<(String, usize, usize)> = Vec::new();
        let mut contiguous = true;
        let mut current_key = first_key.clone();
        let mut current_start = 0usize;
        let mut seen_keys = HashSet::new();
        seen_keys.insert(first_key.clone());
        for row_idx in 1..batch.num_rows() {
            let key = keys.key_at(row_idx);
            if key != current_key {
                if seen_keys.contains(key) {
                    contiguous = false;
                    break;
                }
                ranges.push((current_key, current_start, row_idx - current_start));
                seen_keys.insert(key.to_string());
                current_key = key.to_string();
                current_start = row_idx;
            }
        }
        if contiguous {
            ranges.push((current_key, current_start, batch.num_rows() - current_start));
            let mut written = 0;
            for (partition_key, start, len) in ranges {
                let part_batch = slice_batch_fast(&batch, start, len)?;
                written += self.bulk_insert_batch_with_partition_owned_inner(
                    db,
                    table,
                    &partition_key,
                    part_batch,
                    flush_immediately,
                    pubsub_deferred,
                )?;
            }
            return Ok(written);
        }

        // Group by row indices (total O(rows)) instead of one N-wide bool
        // mask per partition, which was O(partitions x rows) of pure memory.
        let mut key_order = Vec::new();
        let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let key = keys.key_at(row_idx);
            if !groups.contains_key(key) {
                key_order.push(key.to_string());
                groups.insert(key.to_string(), Vec::new());
            }
            groups.get_mut(key).unwrap().push(row_idx as u32);
        }

        let mut written = 0;
        for partition_key in key_order {
            let Some(indices) = groups.remove(&partition_key) else {
                continue;
            };
            let part_batch = take_batch_rows(&batch, &indices)?;
            written += self.bulk_insert_batch_with_partition_owned_inner(
                db,
                table,
                &partition_key,
                part_batch,
                flush_immediately,
                pubsub_deferred,
            )?;
        }
        Ok(written)
    }

    pub fn flush_table_active(&self, db: &str, table: &str) -> Result<u64> {
        let key = table_key(db, table);
        let mut total = 0u64;
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
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
                    self.emit_segment_update_final_path(&seg.path);
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
        Ok(total)
    }

    /// Flush and seal every non-empty active segment for one table, regardless
    /// of the normal minimum segment size. Close-of-day optimization needs the
    /// segments to become immutable so they are visible to the compactor.
    pub fn seal_table_active(&self, db: &str, table: &str) -> Result<u64> {
        let key = table_key(db, table);
        let mut total = 0u64;
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal = active_guard
                .iter()
                .filter(|(_, segment)| segment.total_rows > 0 || segment.buffered_rows > 0)
                .map(|(partition_key, _)| partition_key.clone())
                .collect::<Vec<_>>();

            for partition_key in keys_to_seal {
                if let Some(segment) = active_guard.get_mut(&partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    self.emit_segment_update_final_path(&segment.path);
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
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            let mut active_guard = state.active_segments.lock().unwrap();
            let should_seal = active_guard.get(partition_key).is_some_and(|segment| {
                segment.total_rows > 0 && segment.current_size >= self.min_segment_seal_size()
            });
            if should_seal {
                if let Some(segment) = active_guard.get_mut(partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    self.emit_segment_update_final_path(&segment.path);
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
    pub(crate) fn seal_table_active_under_table_lock(&self, db: &str, table: &str) -> Result<u64> {
        let key = table_key(db, table);
        let mut total = 0u64;
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal = active_guard
                .iter()
                .filter(|(_, segment)| segment.total_rows > 0 || segment.buffered_rows > 0)
                .map(|(partition_key, _)| partition_key.clone())
                .collect::<Vec<_>>();

            for partition_key in keys_to_seal {
                if let Some(segment) = active_guard.get_mut(&partition_key) {
                    segment.seal(&|path| self.invalidate_reader(path))?;
                    self.emit_segment_update_final_path(&segment.path);
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
        let state = self.tables.read().unwrap().get(&key).cloned();
        let Some(state) = state else {
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
        self.emit_segment_update_final_path(&segment.path);
        let rows = segment.total_rows;
        active_guard.remove(partition_key);
        Ok(rows)
    }

    pub fn flush_all_active(&self) -> Result<u64> {

        let mut total = 0u64;
        let states = self.table_states_snapshot();
        for state in states {
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
                    self.emit_segment_update_final_path(&seg.path);
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

        Ok(total)
    }

    fn periodic_flush_check(&self) -> Result<u64> {
        let now = Instant::now();
        let mut total = 0u64;
        let states = self.table_states_snapshot();
        for state in states {
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
        tracing::debug!(txn_id, "transaction committed in storage layer");
        Ok(())
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<()> {
        tracing::debug!(txn_id, "transaction rolled back in storage layer");
        Ok(())
    }

}

/// Gather the selected rows of a batch (columnar take, no per-cell boxing).
pub(super) fn take_batch_rows(
    batch: &crate::types::batch::RecordBatch,
    indices: &[u32],
) -> crate::common::error::Result<crate::types::batch::RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|col| col.take(indices))
        .collect::<Vec<_>>();
    crate::types::batch::RecordBatch::try_new(batch.schema.clone(), columns)
}
