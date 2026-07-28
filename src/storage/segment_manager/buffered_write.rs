impl SegmentManager {
    pub fn bulk_insert_batch(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", &batch, true)
    }

    pub fn bulk_insert_batch_ref(
        &self,
        db: &str,
        table: &str,
        batch: &RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", batch, true)
    }

    pub fn bulk_insert_batch_deferred(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", &batch, false)
    }

    pub fn bulk_insert_batch_with_partition(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, partition_key, &batch, true)
    }

    pub fn bulk_insert_batch_with_partition_ref(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, partition_key, batch, true)
    }

    pub fn bulk_insert_batch_with_partition_deferred(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, partition_key, &batch, false)
    }

    fn bulk_insert_batch_with_partition_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        flush_immediately: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _mutation_guard = state.mutation_lock.lock().unwrap();
        ensure_state_not_retired(&state, db, table)?;

        self.append_batch_with_partition_locked(
            &table_dir,
            &state,
            partition_key,
            batch,
            flush_immediately,
        )?;

        Ok(row_count)
    }

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
        let _mutation_guard = state.mutation_lock.lock().unwrap();
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
        if batch.num_rows() == 0 {
            return Ok(false);
        }
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
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

    fn flush_memtable_batch_under_table_lock_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: &RecordBatch,
        txn_id: u64,
        state: &TableSegmentState,
        should_write: impl FnOnce() -> bool,
        on_written: impl FnOnce(),
    ) -> Result<bool> {
        if !should_write() {
            return Ok(false);
        }

        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        if txn_id > 0 {
            self.write_txn_wal(&table_dir, txn_id, batch)?;
        }
        self.append_batch_with_partition_locked(&table_dir, &state, partition_key, batch, true)?;
        if txn_id > 0 {
            self.sync_table_wal(&table_dir)?;
        }
        on_written();
        Ok(true)
    }

    fn append_batch_with_partition_locked(
        &self,
        table_dir: &Path,
        state: &TableSegmentState,
        partition_key: &str,
        batch: &RecordBatch,
        flush_immediately: bool,
    ) -> Result<()> {
        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &batch.schema,
                partition_key,
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut(partition_key).unwrap();
            seg.append_batch(&batch, &|path| self.invalidate_reader(path))?;

            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove(partition_key);
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
        Ok(())
    }

    fn get_or_create_active_segment_for_partition_locked(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
        active_guard: &mut std::sync::MutexGuard<HashMap<String, ActiveSegmentWriter>>,
    ) -> Result<()> {
        if active_guard.contains_key(partition_key) {
            return Ok(());
        }

        let unsealed = if partition_key.is_empty() {
            Self::find_unsealed_segment(table_dir, self.min_segment_seal_size())?
        } else {
            Self::find_unsealed_partition_segment(
                table_dir,
                partition_key,
                self.min_segment_seal_size(),
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
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
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
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
        };
        writer.append_batch(batch, &|segment_path| self.invalidate_reader(segment_path))?;
        writer.seal(&|segment_path| self.invalidate_reader(segment_path))?;
        Ok(())
    }

    pub fn find_unsealed_partition_segment(
        table_dir: &Path,
        partition_key: &str,
        min_seal_size: u64,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let _pattern = format!("p{}_*", partition_key);
        let segments: Vec<PathBuf> = if let Ok(entries) = std::fs::read_dir(table_dir) {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let file_name = e.file_name();
                    let name = file_name.to_string_lossy();
                    name.starts_with(&format!("p{}", partition_key))
                        && name.ends_with(".dat")
                        && name.contains('_')
                })
                .map(|e| e.path())
                .collect()
        } else {
            Vec::new()
        };

        for seg_path in segments.iter().rev() {
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

        let row_groups = reader.footer_row_groups_info();

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
            dirty_meta: true,
            data_end,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
            row_group_target_rows: self.config.row_group_target_rows.max(1),
        }))
    }
}
