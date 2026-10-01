/// Result of [`SegmentManager::flush_memtable_batch_with_intents_under_table_lock`].
pub(crate) struct FlushIntentAppendOutcome {
    pub wrote: bool,
    /// `(segment file name, expected total rows after the append)` — the
    /// watermark the intents were stamped with, read under the append's own
    /// lock.
    pub target: Option<(String, u64)>,
}

impl SegmentManager {
    pub fn bulk_insert_batch(&self, db: &str, table: &str, batch: RecordBatch) -> Result<u64> {
        self.bulk_insert_batch_with_partition_owned_inner(db, table, "", batch, true, false)
    }

    pub fn bulk_insert_batch_ref(&self, db: &str, table: &str, batch: &RecordBatch) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", batch, true, false)
    }

    pub fn bulk_insert_batch_with_partition(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_owned_inner(
            db,
            table,
            partition_key,
            batch,
            true,
            false,
        )
    }

    fn bulk_insert_batch_with_partition_owned_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let partition_lock = partition_mutation_lock_arc(&state, partition_key);
        let _partition_guard =
            acquire_partition_mutation_lock(&state, &partition_lock, &key, partition_key)?;
        ensure_state_not_retired(&state, db, table)?;

        self.append_batch_with_partition_owned_locked(
            &table_dir,
            &state,
            partition_key,
            batch,
            flush_immediately,
            pubsub_deferred,
        )?;
        self.note_segments_changed(db, table);
        Ok(row_count)
    }

    fn bulk_insert_batch_with_partition_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let partition_lock = partition_mutation_lock_arc(&state, partition_key);
        let _partition_guard =
            acquire_partition_mutation_lock(&state, &partition_lock, &key, partition_key)?;
        ensure_state_not_retired(&state, db, table)?;

        self.append_batch_with_partition_locked(
            &table_dir,
            &state,
            partition_key,
            batch,
            flush_immediately,
            pubsub_deferred,
        )?;

        self.note_segments_changed(db, table);
        Ok(row_count)
    }

    /// Test-only helper: flush one batch through the memtable path when
    /// `should_write` returns true. Production writes go through the bulk
    /// insert paths above; DDL tests use this to force deterministic flushes.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn flush_memtable_batch_if(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        txn_id: u64,
        should_write: impl FnOnce() -> bool,
        on_written: impl FnOnce(),
    ) -> Result<bool> {
        if batch.num_rows() == 0 {
            return Ok(false);
        }

        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let partition_lock = partition_mutation_lock_arc(&state, partition_key);
        let _partition_guard =
            acquire_partition_mutation_lock(&state, &partition_lock, &key, partition_key)?;
        ensure_state_not_retired(&state, db, table)?;
        self.flush_memtable_batch_under_table_lock_inner(
            db,
            table,
            partition_key,
            batch,
            txn_id,
            &state,
            should_write,
            on_written,
        )
    }

    /// Return the durability watermark for appending `batch_rows` more rows
    /// to the *existing* active segment of the partition: the segment file
    /// name plus the row count its footer must reach once the append and meta
    /// flush complete. Returns `None` when no appendable active segment
    /// exists yet; the caller must then materialize the rows through the
    /// atomic standalone-segment path so a segment file only becomes
    /// discoverable with a valid footer. This method never creates a file:
    /// doing so before the append would expose a header-only segment without
    /// an embedded footer (and leak one if the append later failed).
    pub(crate) fn durable_active_segment_watermark_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: &Schema,
        batch_rows: u64,
    ) -> Result<Option<(String, u64)>> {
        let table_dir = self.data_dir.join(db).join(table);
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        ensure_state_not_retired(&state, db, table)?;
        let active_guard = state.active_segments.lock().unwrap();
        if let Some(seg) = active_guard.get(partition_key) {
            let file_name = Self::active_segment_file_name(&seg.path)?;
            return Ok(Some((
                file_name,
                seg.total_rows + seg.buffered_rows as u64 + batch_rows,
            )));
        }

        let unsealed = if partition_key.is_empty() {
            Self::find_unsealed_segment(
                &table_dir,
                self.min_segment_seal_size(),
                &state.sealing_paths(),
            )?
        } else {
            Self::find_unsealed_partition_segment(
                &table_dir,
                partition_key,
                self.min_segment_seal_size(),
                &state.sealing_paths(),
            )?
        };
        if let Some((seg_path, existing_schema, existing_rows)) = unsealed {
            if existing_schema.columns.len() == schema.columns.len() {
                let file_name = Self::active_segment_file_name(&seg_path)?;
                return Ok(Some((file_name, existing_rows + batch_rows)));
            }
        }
        Ok(None)
    }

    fn active_segment_file_name(path: &Path) -> Result<String> {
        path.file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| TaibaiError::Storage("active segment path has no file name".into()))
    }

    #[allow(clippy::too_many_arguments)]
    /// Legacy no-intent entry; production flushes route through
    /// [`Self::flush_memtable_batch_with_intents_under_table_lock`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn flush_memtable_batch_if_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        txn_id: u64,
        should_write: impl FnOnce() -> bool,
        on_written: impl FnOnce(),
    ) -> Result<bool> {
        self.flush_memtable_batch_with_intents_under_table_lock(
            db,
            table,
            partition_key,
            batch,
            txn_id,
            &mut |_, _| Ok(()),
            should_write,
            on_written,
        )
        .map(|outcome| outcome.wrote)
    }

    /// [`Self::flush_memtable_batch_if_under_table_lock`] whose durable
    /// flush intents are written by `write_intents` INSIDE the append's
    /// active_segments critical section, stamped with the authoritative
    /// pre-append watermark of the very segment receiving the rows. Returns
    /// the wrote flag plus `(segment_file, expected_total_rows)` for
    /// logging/recovery bookkeeping.
    pub(crate) fn flush_memtable_batch_with_intents_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        _txn_id: u64,
        write_intents: &mut dyn FnMut(&str, u64) -> Result<()>,
        should_write: impl FnOnce() -> bool,
        on_written: impl FnOnce(),
    ) -> Result<FlushIntentAppendOutcome> {
        if batch.num_rows() == 0 {
            return Ok(FlushIntentAppendOutcome {
                wrote: false,
                target: None,
            });
        }
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        ensure_state_not_retired(&state, db, table)?;
        if !should_write() {
            return Ok(FlushIntentAppendOutcome {
                wrote: false,
                target: None,
            });
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let _visibility = state.flush_visibility.write().unwrap();
        let mut target = None;
        let result = self.append_batch_partition_locked_with_hook(
            &table_dir,
            &state,
            partition_key,
            batch,
            true,
            false,
            Some(&mut |file_name: &str, base_rows: u64| {
                target = Some((file_name.to_string(), base_rows + batch.num_rows() as u64));
                write_intents(file_name, base_rows)
            }),
        );
        match result {
            Ok(()) => {
                Self::sleep_flush_transition_delay();
                on_written();
                Ok(FlushIntentAppendOutcome {
                    wrote: true,
                    target,
                })
            }
            Err(error) => Err(error),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(not(test), allow(dead_code))]
    fn flush_memtable_batch_under_table_lock_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        _txn_id: u64,
        state: &Arc<TableSegmentState>,
        should_write: impl FnOnce() -> bool,
        on_written: impl FnOnce(),
    ) -> Result<bool> {
        if !should_write() {
            return Ok(false);
        }

        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let _visibility = state.flush_visibility.write().unwrap();
        self.append_batch_with_partition_locked(
            &table_dir,
            state,
            partition_key,
            batch,
            true,
            false,
        )?;
        Self::sleep_flush_transition_delay();
        on_written();
        Ok(true)
    }

    fn append_batch_with_partition_locked(
        &self,
        table_dir: &Path,
        state: &Arc<TableSegmentState>,
        partition_key: &str,
        batch: &RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
    ) -> Result<()> {
        self.append_batch_partition_locked_with_hook(
            table_dir,
            state,
            partition_key,
            batch,
            flush_immediately,
            pubsub_deferred,
            None,
        )
    }

    /// [`Self::append_batch_with_partition_locked`] plus a hook that runs
    /// under the SAME active_segments lock, after the target segment is
    /// resolved and before the append: the hook receives the segment file
    /// name and its pre-append row count, which is exactly the watermark a
    /// durable flush intent must be stamped with. Running watermark + intent
    /// write + append in one critical section closes the race where a
    /// partition-locked direct writer slipped rows between a separately
    /// read watermark and our append — at recovery those foreign rows could
    /// satisfy our intent and retire a tail whose rows never reached the
    /// segment (silent loss of fsync-acknowledged data).
    fn append_batch_partition_locked_with_hook(
        &self,
        table_dir: &Path,
        state: &Arc<TableSegmentState>,
        partition_key: &str,
        batch: &RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
        mut under_lock_hook: Option<&mut dyn FnMut(&str, u64) -> Result<()>>,
    ) -> Result<()> {
        let mut pending_seal = None;
        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                table_dir,
                &batch.schema,
                partition_key,
                &mut active_guard,
                &state.sealing_paths(),
            )?;

            let seg = active_guard.get_mut(partition_key).unwrap();
            if let Some(hook) = under_lock_hook.as_deref_mut() {
                let file_name = Self::active_segment_file_name(&seg.path)?;
                let base_rows = seg.total_rows + seg.buffered_rows as u64;
                hook(&file_name, base_rows)?;
            }
            seg.append_batch(batch, pubsub_deferred, &|path| self.invalidate_reader(path))?;

            if seg.current_size >= self.max_segment_size() {
                if let Some(seg) = active_guard.remove(partition_key) {
                    state.register_sealing_segment(partition_key, seg.path.clone());
                    pending_seal = Some(PendingSeal::new(Arc::clone(state), partition_key, seg));
                }
            } else if flush_immediately && (seg.buffered_rows > 0 || seg.total_rows > 0) {
                seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            } else if !flush_immediately {
                if seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                }
            } else if seg.buffered_rows > 0 {
                seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                seg.flush_visible_footer(&|path| self.invalidate_reader(path))?;
            } else {
                seg.flush_visible_footer(&|path| self.invalidate_reader(path))?;
            }
        }
        if let Some(mut pending) = pending_seal {
            let result = pending.writer.seal(&|path| self.invalidate_reader(path));
            if result.is_ok() {
                self.emit_segment_update_final_path(&pending.writer.path);
            }
            drop(pending.token);
            result?;
        }
        Ok(())
    }

    fn get_or_create_active_segment_for_partition_locked(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
        active_guard: &mut std::sync::MutexGuard<HashMap<String, ActiveSegmentWriter>>,
        sealing: &HashSet<PathBuf>,
    ) -> Result<()> {
        if active_guard.contains_key(partition_key) {
            return Ok(());
        }

        let unsealed = if partition_key.is_empty() {
            Self::find_unsealed_segment(table_dir, self.min_segment_seal_size(), sealing)?
        } else {
            Self::find_unsealed_partition_segment(
                table_dir,
                partition_key,
                self.min_segment_seal_size(),
                sealing,
            )?
        };

        let active = if let Some((seg_path, existing_schema, existing_rows)) = unsealed {
            if existing_schema.columns.len() == schema.columns.len() {
                tracing::info!(
                    path = %seg_path.display(),
                    partition_key,
                    existing_rows,
                    "resuming unsealed segment for append"
                );
                if let Some(recovered) = self.recover_active_segment(&seg_path, &existing_schema)? {
                    recovered
                } else {
                    self.create_new_active_segment(table_dir, schema, partition_key)?
                }
            } else {
                self.create_new_active_segment(table_dir, schema, partition_key)?
            }
        } else {
            self.create_new_active_segment(table_dir, schema, partition_key)?
        };

        active_guard.insert(partition_key.to_string(), active);
        Ok(())
    }

    fn create_new_active_segment(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
    ) -> Result<ActiveSegmentWriter> {
        let seg_path = if partition_key.is_empty() {
            self.next_segment_path(table_dir, "")?
        } else {
            self.next_partition_segment_path(table_dir, partition_key)?
        };

        remove_sidecar_meta(&seg_path)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&seg_path)?;
        file.write_all(&encode_segment_header())?;

        tracing::info!(
            path = %seg_path.display(),
            partition_key,
            "created new active segment"
        );

        Ok(ActiveSegmentWriter {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path,
            schema: schema.clone(),
            row_groups: Vec::new(),
            current_size: SEGMENT_HEADER_SIZE as u64,
            total_rows: 0,
            delete_bitmap: Vec::new(),
            deleted_row_count: 0,
            buffered_batch: None,
            buffered_rows: 0,
            flushed_buffer_prefix: 0,
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
            pubsub: self.pubsub(),
            pending_publish: Vec::new(),
            pubsub_deferred_ranges: Vec::new(),
            compression_level: self.table_compression_level_for_path(table_dir)?,
        })
    }

    /// Build a standalone sealed segment with the same encoding and row-group
    /// policy used by the normal active writer. The caller chooses a temporary
    /// non-discoverable path and can atomically rename it after `seal` returns.
    pub(crate) fn write_standalone_active_encoded_segment(
        &self,
        path: &Path,
        batch: &RecordBatch,
    ) -> Result<()> {
        remove_sidecar_meta(path)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        file.write_all(&encode_segment_header())?;
        let mut writer = ActiveSegmentWriter {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: path.to_path_buf(),
            schema: batch.schema.clone(),
            row_groups: Vec::new(),
            current_size: SEGMENT_HEADER_SIZE as u64,
            total_rows: 0,
            delete_bitmap: Vec::new(),
            deleted_row_count: 0,
            buffered_batch: None,
            buffered_rows: 0,
            flushed_buffer_prefix: 0,
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            pubsub: None,
            pending_publish: Vec::new(),
            pubsub_deferred_ranges: Vec::new(),
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
            compression_level: self.table_compression_level_for_path(path)?,
        };
        writer.append_batch(batch, false, &|segment_path| {
            self.invalidate_reader(segment_path)
        })?;
        // NOTE: no replication emission here - this writer writes a TMP file
        // that the caller renames; the flush sites emit the SegmentUpdate
        // with the final name after the rename.
        writer.seal(&|segment_path| self.invalidate_reader(segment_path))?;
        Ok(())
    }

    fn append_batch_with_partition_owned_locked(
        &self,
        table_dir: &Path,
        state: &Arc<TableSegmentState>,
        partition_key: &str,
        batch: RecordBatch,
        flush_immediately: bool,
        pubsub_deferred: bool,
    ) -> Result<()> {
        let schema = batch.schema.clone();
        let mut pending_seal = None;
        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                table_dir,
                &schema,
                partition_key,
                &mut active_guard,
                &state.sealing_paths(),
            )?;

            let seg = active_guard.get_mut(partition_key).unwrap();
            seg.append_batch_owned(batch, pubsub_deferred, &|path| self.invalidate_reader(path))?;

            if seg.current_size >= self.max_segment_size() {
                if let Some(seg) = active_guard.remove(partition_key) {
                    state.register_sealing_segment(partition_key, seg.path.clone());
                    pending_seal = Some(PendingSeal::new(Arc::clone(state), partition_key, seg));
                }
            } else if flush_immediately && (seg.buffered_rows > 0 || seg.total_rows > 0) {
                seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            } else if !flush_immediately {
                if seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                }
            } else if seg.buffered_rows > 0 {
                seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                seg.flush_visible_footer(&|path| self.invalidate_reader(path))?;
            } else {
                seg.flush_visible_footer(&|path| self.invalidate_reader(path))?;
            }
        }
        if let Some(mut pending) = pending_seal {
            let result = pending.writer.seal(&|path| self.invalidate_reader(path));
            if result.is_ok() {
                // Same replication emission as the borrowed variant: the
                // sealed segment must reach the change feed with its final
                // name regardless of which append variant triggered the seal.
                self.emit_segment_update_final_path(&pending.writer.path);
            }
            drop(pending.token);
            result?;
        }
        Ok(())
    }

    pub fn find_unsealed_partition_segment(
        table_dir: &Path,
        partition_key: &str,
        min_seal_size: u64,
        exclude: &HashSet<PathBuf>,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let prefix = format!("p{}_", partition_key);
        let segments: Vec<PathBuf> = if let Ok(entries) = std::fs::read_dir(table_dir) {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let file_name = e.file_name();
                    let name = file_name.to_string_lossy();
                    // The `_` separator is load-bearing: without it partition
                    // "1" matches partition "10"'s segment (p1 vs p10) and
                    // recovers a file another writer owns.
                    name.starts_with(&prefix) && name.ends_with(".dat")
                })
                .map(|e| e.path())
                .collect()
        } else {
            Vec::new()
        };

        for seg_path in segments.iter().rev() {
            if exclude.contains(seg_path) {
                continue;
            }
            let file_len = match std::fs::metadata(seg_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            if file_len < SEGMENT_HEADER_SIZE as u64 {
                continue;
            }

            if let Ok(reader) = SegmentReader::open(seg_path) {
                if file_len < min_seal_size && reader.row_count() > 0 {
                    return Ok(Some((
                        seg_path.clone(),
                        reader.schema().clone(),
                        reader.row_count(),
                    )));
                }
            }
        }

        Ok(None)
    }

    fn recover_active_segment(
        &self,
        seg_path: &Path,
        schema: &Schema,
    ) -> Result<Option<ActiveSegmentWriter>> {
        let file_len = std::fs::metadata(seg_path)?.len();

        if file_len < SEGMENT_HEADER_SIZE as u64 {
            return Ok(None);
        }

        self.invalidate_reader(seg_path);

        let reader = SegmentReader::open_owned(seg_path)?;
        let data_end = reader.data_end();

        // Recovery rehydrates the writer's row-group metadata once per
        // recovered segment — the deep clone is fine here (not per row-group
        // read); use the borrowed accessor and clone only what the writer
        // takes ownership of.
        let row_groups = reader.footer_row_groups().to_vec();

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(seg_path)?;

        file.set_len(data_end)?;
        file.seek(SeekFrom::Start(data_end))?;

        tracing::info!(
            path = %seg_path.display(),
            data_end,
            existing_rows = reader.row_count(),
            row_groups = row_groups.len(),
            "recovered active segment from unsealed file"
        );

        Ok(Some(ActiveSegmentWriter {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path.to_path_buf(),
            schema: schema.clone(),
            row_groups,
            current_size: data_end,
            total_rows: reader.row_count(),
            delete_bitmap: reader.delete_bitmap().to_vec(),
            deleted_row_count: reader.deleted_row_count(),
            buffered_batch: None,
            buffered_rows: 0,
            flushed_buffer_prefix: 0,
            dirty_meta: true,
            data_end,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
            pubsub: self.pubsub(),
            pending_publish: Vec::new(),
            pubsub_deferred_ranges: Vec::new(),
            compression_level: self.table_compression_level_for_path(seg_path)?,
        }))
    }
}
