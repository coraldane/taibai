impl SegmentManager {
    pub fn new(data_dir: &Path) -> Result<Self> {
        Self::with_config(data_dir, SegmentConfig::default())
    }

    pub fn with_config(data_dir: &Path, config: SegmentConfig) -> Result<Self> {
        let mut tables = HashMap::new();

        if data_dir.exists() {
            if let Ok(dbs) = std::fs::read_dir(data_dir) {
                for db_entry in dbs.flatten() {
                    if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    let db_name = db_entry.file_name().to_string_lossy().to_string();
                    if db_name.starts_with('.') || db_name == "meta" || db_name == "wal" {
                        continue;
                    }
                    let db_dir = data_dir.join(&db_name);
                    if let Ok(tables_dir) = std::fs::read_dir(&db_dir) {
                        for tbl_entry in tables_dir.flatten() {
                            if !tbl_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                                continue;
                            }
                            let table_name = tbl_entry.file_name().to_string_lossy().to_string();
                            if matches!(
                                table_name.as_str(),
                                ".taibai-drop-tombstones"
                                    | ".taibai-truncate-tombstones"
                                    | ".taibai-partition-tombstones"
                                    | ".taibai-rename-tombstones"
                            ) {
                                continue;
                            }
                            let tbl_dir = tbl_entry.path();
                            cleanup_incomplete_compaction_files(&tbl_dir);
                            let max_seq = discover_max_seq(&tbl_dir);
                            let partition_seq = discover_max_partition_seq(&tbl_dir);
                            let key = table_key(&db_name, &table_name);
                            tables.insert(
                                key,
                                Arc::new(TableSegmentState::with_partition_seq(
                                    max_seq,
                                    partition_seq,
                                )),
                            );
                        }
                    }
                }
            }
        }

        let flush_stop = Arc::new(AtomicBool::new(false));
        let flush_paused = Arc::new(AtomicBool::new(false));

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            tables: RwLock::new(tables),
            reader_cache: Mutex::new(ReaderCache::new(reader_cache_capacity())),
            segment_meta_cache: Mutex::new(SegmentMetaCache::new(reader_cache_capacity())),
            cluster_meta_cache: Mutex::new(ClusterMetaCache::new()),
            flush_stop,
            flush_paused,
            next_txn_id: AtomicU64::new(1),
            config,
        })
    }

    pub fn set_config(&mut self, config: SegmentConfig) {
        self.config = config;
    }

    fn min_segment_seal_size(&self) -> u64 {
        self.config.min_segment_size
    }

    fn max_segment_size(&self) -> u64 {
        self.config.max_segment_size
    }

    pub fn row_group_target_rows(&self) -> usize {
        self.config.row_group_target_rows.max(1)
    }

    pub fn start_flush_thread(mgr: &Arc<Self>) {
        let mgr_clone = Arc::clone(mgr);
        let stop = mgr.flush_stop.clone();
        let paused = mgr.flush_paused.clone();
        std::thread::Builder::new()
            .name("taibai-flush".into())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_secs(FLUSH_CHECK_INTERVAL_SECS));
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    if paused.load(Ordering::Acquire) {
                        tracing::debug!("flush thread paused, skipping periodic check");
                        continue;
                    }
                    if let Err(e) = mgr_clone.periodic_flush_check() {
                        tracing::warn!(error = %e, "periodic flush check failed");
                    }
                }
            })
            .expect("failed to spawn flush thread");
    }

    pub fn shutdown(&self) {
        self.flush_stop.store(true, Ordering::Release);
        if let Err(e) = self.flush_all_active() {
            tracing::warn!(error = %e, "failed to flush all active segments during shutdown");
        }
    }

    pub fn pause_flush(&self) {
        self.flush_paused.store(true, Ordering::Release);
        tracing::info!("flush thread paused");
    }

    pub fn resume_flush(&self) {
        self.flush_paused.store(false, Ordering::Release);
        tracing::info!("flush thread resumed");
    }

    pub fn is_flush_paused(&self) -> bool {
        self.flush_paused.load(Ordering::Acquire)
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn table_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join(db).join(table)
    }

    fn get_or_create_state(&self, key: &str) -> Arc<TableSegmentState> {
        {
            let tables = self.tables.read().unwrap();
            if let Some(state) = tables.get(key) {
                return Arc::clone(state);
            }
        }
        let mut tables = self.tables.write().unwrap();
        tables
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(TableSegmentState::new(0)))
            .clone()
    }

    fn table_state_key_from_dir(&self, table_dir: &Path) -> Result<String> {
        let relative = table_dir.strip_prefix(&self.data_dir).map_err(|_| {
            TaibaiError::Storage(format!(
                "table dir '{}' is outside data dir '{}'",
                table_dir.display(),
                self.data_dir.display()
            ))
        })?;

        let mut components = relative.components();
        let db = components
            .next()
            .ok_or_else(|| TaibaiError::Storage("table dir is missing database name".into()))?;
        let table = components
            .next()
            .ok_or_else(|| TaibaiError::Storage("table dir is missing table name".into()))?;

        Ok(table_key(
            &db.as_os_str().to_string_lossy(),
            &table.as_os_str().to_string_lossy(),
        ))
    }

    fn get_or_create_state_for_table_dir(
        &self,
        table_dir: &Path,
    ) -> Result<Arc<TableSegmentState>> {
        let key = self.table_state_key_from_dir(table_dir)?;
        Ok(self.get_or_create_state(&key))
    }

    pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
        store::discover_segments(table_dir)
    }

    pub fn discover_inactive_segments(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        let segments = Self::discover_segments(table_dir)?;
        let key = table_key(db, table);
        let active_paths = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&key)
                .map(|state| {
                    let active_guard = state.active_segments.lock().unwrap();
                    active_guard
                        .values()
                        .map(|segment| segment.path.clone())
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default()
        };

        if active_paths.is_empty() {
            return Ok(segments);
        }

        Ok(segments
            .into_iter()
            .filter(|path| !active_paths.contains(path))
            .collect())
    }

    pub fn cleanup_fully_deleted_segments(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
    ) -> Result<u64> {
        self.with_table_mutation_lock(db, table, || {
            let mut removed = 0u64;
            for path in self.discover_inactive_segments(db, table, table_dir)? {
                let reader = self.open_reader_cached(&path)?;
                if reader.row_count() > 0 && reader.deleted_row_count() >= reader.row_count() {
                    self.remove_segment_file(&path)?;
                    removed += 1;
                }
            }
            Ok(removed)
        })
    }

    pub fn find_unsealed_segment(
        table_dir: &Path,
        min_seal_size: u64,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let segments = Self::discover_segments(table_dir)?;

        for seg_path in segments.iter().rev() {
            let file_len = match std::fs::metadata(seg_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            if file_len < SEGMENT_HEADER_SIZE as u64 {
                continue;
            }

            if let Ok(reader) = SegmentReader::open(seg_path) {
                let seg_size = file_len;
                if seg_size < min_seal_size && reader.row_count() > 0 {
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

    pub fn next_segment_path(&self, table_dir: &Path, _partition_key: &str) -> Result<PathBuf> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(table_dir.join(store::regular_segment_filename(seq)))
    }

    pub fn next_partition_segment_path(
        &self,
        table_dir: &Path,
        partition_key: &str,
    ) -> Result<PathBuf> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let seq = {
            let mut partition_seq = state.partition_seq.lock().unwrap();
            let entry = partition_seq.entry(partition_key.to_string()).or_insert(0);
            *entry += 1;
            *entry
        };
        Ok(table_dir.join(store::partition_segment_filename(partition_key, seq)))
    }

    pub fn next_replacement_segment_path(
        &self,
        table_dir: &Path,
        partition_key: Option<&str>,
    ) -> Result<PathBuf> {
        store::next_segment_path_by_scan(table_dir, partition_key)
    }

    pub fn open_reader(&self, path: &Path) -> Result<SegmentReader> {
        if self.requires_owned_reader(path) {
            return self.open_mutable_reader_owned(path);
        }

        match SegmentReader::open(path) {
            Ok(reader) => Ok(reader),
            Err(err) if is_transient_footer_error(&err) => self.open_mutable_reader_owned(path),
            Err(err) => Err(err),
        }
    }

    pub fn open_reader_cached(&self, path: &Path) -> Result<Arc<SegmentReader>> {
        // Active writers can change their data and sidecar footer at any time, so
        // readers for them must remain uncached snapshots. A small *inactive*
        // segment may be resumed later, but is stable until that happens; cache an
        // owned snapshot for it and let recovery invalidate the cache before append.
        if self.is_mutable_segment_path(path) {
            self.invalidate_reader(path);
            return Ok(Arc::new(self.open_mutable_reader_owned(path)?));
        }

        if let Some(reader) = self.reader_cache.lock().unwrap().get(path) {
            return Ok(reader);
        }

        let reader = match if self.requires_owned_reader(path) {
            SegmentReader::open_owned(path)
        } else {
            SegmentReader::open(path)
        } {
            Ok(reader) => reader,
            Err(err) if is_transient_footer_error(&err) => self.open_mutable_reader_owned(path)?,
            Err(err) => return Err(err),
        };
        Ok(self
            .reader_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), reader))
    }

    pub fn invalidate_reader(&self, path: &Path) {
        self.reader_cache.lock().unwrap().invalidate(path);
        self.segment_meta_cache.lock().unwrap().invalidate(path);
        self.cluster_meta_cache.lock().unwrap().invalidate(path);
    }

    pub fn invalidate_readers_under(&self, dir: &Path) {
        self.reader_cache.lock().unwrap().invalidate_under(dir);
        self.segment_meta_cache
            .lock()
            .unwrap()
            .invalidate_under(dir);
        self.cluster_meta_cache
            .lock()
            .unwrap()
            .invalidate_under(dir);
    }

    /// Read-side metadata cache for a sealed segment's cluster-key column zone map.
    ///
    /// Returns the cached zone map for `col_idx` in `path`, deriving and caching it
    /// on first access. Returns `None` when the segment has no usable bounds for the
    /// column (empty/null). Mutable (unsealed) segments are never cached because their
    /// footer can still change. The cache shares the reader-cache invalidation
    /// lifecycle, so compaction/seal/delete automatically clears stale entries.
    pub fn cluster_zone_map_cached(
        &self,
        path: &Path,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        if self.is_mutable_segment_path(path) {
            return self.derive_cluster_zone_map(path, col_idx);
        }

        if let Some(cached) = self.cluster_meta_cache.lock().unwrap().get(path, col_idx) {
            return cached;
        }

        let derived = self.derive_cluster_zone_map(path, col_idx);
        self.cluster_meta_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), col_idx, derived.clone());
        derived
    }

    fn derive_cluster_zone_map(
        &self,
        path: &Path,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        if let Some(meta) = self.segment_meta(path).ok().flatten() {
            let zm = meta.segment_zone_maps.get(col_idx)?;
            if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
                return None;
            }
            return Some(zm.clone());
        }
        let reader = self.open_reader_cached(path).ok()?;
        let zm = reader.segment_zone_maps().get(col_idx)?;
        if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
            return None;
        }
        Some(zm.clone())
    }

    pub fn segment_meta(&self, path: &Path) -> Result<Option<Arc<SegmentMetaSnapshot>>> {
        if self.is_mutable_segment_path(path) {
            return Ok(None);
        }
        if let Some(meta) = self.segment_meta_cache.lock().unwrap().get(path) {
            return Ok(Some(meta));
        }
        match read_embedded_segment_meta(path) {
            Ok(meta) => Ok(Some(
                self.segment_meta_cache
                    .lock()
                    .unwrap()
                    .put(path.to_path_buf(), meta),
            )),
            Err(_) => Ok(None),
        }
    }

    pub fn remove_segment_file(&self, path: &Path) -> Result<()> {
        self.invalidate_reader(path);
        std::fs::remove_file(path)?;
        remove_sidecar_meta(path)?;
        Ok(())
    }

    pub fn replace_segments_with_tmp(
        &self,
        tmp_path: &Path,
        final_path: &Path,
        old_segments: &[PathBuf],
    ) -> Result<()> {
        if final_path.exists() {
            return Err(TaibaiError::Storage(format!(
                "segment target already exists: {}",
                final_path.display()
            )));
        }

        let final_meta = sidecar_meta_path(final_path);
        if final_meta.exists() {
            return Err(TaibaiError::Storage(format!(
                "segment meta target already exists: {}",
                final_meta.display()
            )));
        }
        // Compaction output is sealed and self-contained. Validate its embedded
        // footer before replacing any source segment; no sidecar is required.
        read_embedded_segment_meta(tmp_path)?;

        std::fs::rename(tmp_path, final_path)?;
        for old_seg in old_segments {
            if old_seg != final_path {
                self.remove_segment_file(old_seg)?;
            }
        }

        remove_sidecar_meta(tmp_path)?;
        let _ = self.open_reader_cached(final_path);
        Ok(())
    }

    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        let mutable = self.is_mutable_segment_path(path);
        self.invalidate_reader(path);
        SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count)?;
        if mutable {
            let footer = read_embedded_footer(path)?;
            append_sidecar_footer(path, footer.data_end, &footer.footer)?;
        } else {
            remove_sidecar_meta(path)?;
        }
        if let Some(state) = self.state_for_segment_path(path) {
            let mut active_guard = state.active_segments.lock().unwrap();
            for segment in active_guard.values_mut() {
                if same_segment_path(&segment.path, path) {
                    segment.delete_bitmap = bitmap.to_vec();
                    segment.deleted_row_count = deleted_count;
                    segment.dirty_meta = true;
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn with_table_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _guard = state.mutation_lock.lock().unwrap();
        ensure_state_not_retired(&state, db, table)?;
        f()
    }

    pub fn with_database_mutation_locks<T>(
        &self,
        db: &str,
        tables: &[String],
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let mut keys = tables
            .iter()
            .map(|table| table_key(db, table))
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        let states = keys
            .iter()
            .map(|key| self.get_or_create_state(key))
            .collect::<Vec<_>>();
        let _guards = states
            .iter()
            .map(|state| state.mutation_lock.lock().unwrap())
            .collect::<Vec<_>>();
        f()
    }

    pub fn try_with_table_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _guard = match state.mutation_lock.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                panic!("table mutation lock poisoned: {}", poisoned)
            }
        };
        ensure_state_not_retired(&state, db, table)?;
        f().map(Some)
    }

    pub fn try_with_database_mutation_locks<T>(
        &self,
        db: &str,
        tables: &[String],
        f: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let mut keys = tables
            .iter()
            .map(|table| table_key(db, table))
            .collect::<Vec<_>>();
        keys.sort();
        keys.dedup();
        let states = keys
            .iter()
            .map(|key| self.get_or_create_state(key))
            .collect::<Vec<_>>();
        let mut guards = Vec::with_capacity(states.len());
        for state in &states {
            match state.mutation_lock.try_lock() {
                Ok(guard) => guards.push(guard),
                Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    panic!("table mutation lock poisoned: {}", poisoned)
                }
            }
        }
        f().map(Some)
    }

    fn open_mutable_reader_owned(&self, path: &Path) -> Result<SegmentReader> {
        let Some(state) = self.state_for_segment_path(path) else {
            return SegmentReader::open_owned(path);
        };

        let mut active_guard = state.active_segments.lock().unwrap();
        for segment in active_guard.values_mut() {
            if same_segment_path(&segment.path, path) {
                if segment.dirty_meta {
                    segment.flush_meta(&|path| self.invalidate_reader(path))?;
                }
                return SegmentReader::open_owned(path);
            }
        }
        SegmentReader::open_owned(path)
    }

    fn is_mutable_segment_path(&self, path: &Path) -> bool {
        self.is_active_segment_path(path)
    }

    fn requires_owned_reader(&self, path: &Path) -> bool {
        if self.is_mutable_segment_path(path) {
            return true;
        }
        std::fs::metadata(path)
            .map(|meta| meta.len() < self.min_segment_seal_size())
            .unwrap_or(false)
    }

    fn is_active_segment_path(&self, path: &Path) -> bool {
        let Some(state) = self.state_for_segment_path(path) else {
            return false;
        };

        let active_guard = state.active_segments.lock().unwrap();
        active_guard
            .values()
            .any(|segment| same_segment_path(&segment.path, path))
    }

    fn state_for_segment_path(&self, path: &Path) -> Option<Arc<TableSegmentState>> {
        let tables = self.tables.read().unwrap();
        let table_dir = path.parent()?;
        let key = self.table_state_key_from_dir(table_dir).ok()?;
        tables.get(&key).cloned()
    }

    pub fn write_new_segment(
        &self,
        path: &Path,
        schema: Schema,
        row_group_size: usize,
        batch: &RecordBatch,
    ) -> Result<()> {
        let mut writer = SegmentWriter::create(path, schema, row_group_size)?;
        writer.write_batch(batch)?;
        writer.seal()?;
        Ok(())
    }

    pub fn write_new_segment_batches(
        &self,
        path: &Path,
        schema: Schema,
        row_group_size: usize,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let total_rows = batches.iter().map(RecordBatch::num_rows).sum();
        let mut combined = RecordBatch::with_capacity(schema.clone(), total_rows);
        for batch in batches {
            if batch.num_columns() != combined.num_columns() {
                return Err(TaibaiError::Storage(format!(
                    "replacement batch has {} columns, expected {}",
                    batch.num_columns(),
                    combined.num_columns()
                )));
            }
            for column_idx in 0..combined.num_columns() {
                combined
                    .column_mut(column_idx)
                    .expect("validated replacement column")
                    .extend(
                        batch
                            .column(column_idx)
                            .expect("validated replacement source column"),
                    )?;
            }
        }
        let mut writer = SegmentWriter::create(path, schema, row_group_size)?;
        if combined.num_rows() > 0 {
            writer.write_batch(&combined)?;
        }
        writer.seal()?;
        Ok(())
    }

    pub fn write_wal(&self, table_dir: &Path, batch: &RecordBatch) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;

        let wal_data = crate::sql::execution::wal_helper::serialize_batch_for_wal(batch);
        let txn_id = self.alloc_txn_id();

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

    fn table_states_snapshot(&self) -> Vec<Arc<TableSegmentState>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    pub fn sync_all_wals(&self) {
        let states = self.table_states_snapshot();
        for state in states {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            if let Some(ref mut w) = *wal_guard {
                let _ = w.sync();
            }
        }
    }

    pub fn clear_wal(&self, table_dir: &Path) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear()?;
        }
        Ok(())
    }

    pub fn clear_active_segments(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            state.active_segments.lock().unwrap().clear();
        }
    }

    /// Return partition keys that currently have buffered or persisted rows in
    /// an active writer. Callers that need to act on the just-written
    /// partitions must capture this before sealing removes those writers.
    pub fn active_partition_keys(&self, db: &str, table: &str) -> Vec<String> {
        let key = table_key(db, table);
        let state = self.tables.read().unwrap().get(&key).cloned();
        let Some(state) = state else {
            return Vec::new();
        };
        let active_segments = state.active_segments.lock().unwrap();
        active_segments
            .iter()
            .filter(|(_, segment)| segment.total_rows > 0 || segment.buffered_rows > 0)
            .map(|(partition_key, _)| partition_key.clone())
            .collect()
    }

    pub fn remove_wal(&self, table_dir: &Path) {
        let Ok(key) = self.table_state_key_from_dir(table_dir) else {
            return;
        };
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
        }
    }

    pub fn remove_table_state(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let state = self.tables.write().unwrap().remove(&key);
        if let Some(state) = state {
            state.retired.store(true, Ordering::Release);
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
            state.active_segments.lock().unwrap().clear();
        }
    }

    /// Close mutable file handles without changing the lock identity. DDL uses
    /// this while holding `mutation_lock`, then retires the state only after
    /// the directory operation is complete. Removing the map entry too early
    /// would let a waiting writer create a second lock for the same table.
    pub fn close_table_state_handles(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let state = self.tables.read().unwrap().get(&key).cloned();
        if let Some(state) = state {
            *state.wal_writer.lock().unwrap() = None;
            state.active_segments.lock().unwrap().clear();
        }
    }

    pub fn rename_table_state(&self, db: &str, old_name: &str, new_name: &str) {
        let old_key = table_key(db, old_name);
        let new_key = table_key(db, new_name);
        let mut tables = self.tables.write().unwrap();
        if let Some(state) = tables.remove(&old_key) {
            tables.insert(new_key, state);
        }
    }

    pub fn remove_database_state(&self, db: &str) {
        let prefix = format!("{}:", db);
        let states = {
            let mut tables = self.tables.write().unwrap();
            let keys = tables
                .keys()
                .filter(|key| key.starts_with(&prefix))
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| tables.remove(&key))
                .collect::<Vec<_>>()
        };
        for state in states {
            state.retired.store(true, Ordering::Release);
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
            state.active_segments.lock().unwrap().clear();
        }
    }

    pub fn clear_all_wals(&self) {
        let states = self.table_states_snapshot();
        for state in states {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            if let Some(ref mut w) = *wal_guard {
                let _ = w.clear();
            }
        }
    }
}
