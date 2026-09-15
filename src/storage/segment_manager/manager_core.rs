static FLUSH_TRANSITION_DELAY_MS: AtomicU64 = AtomicU64::new(0);

impl SegmentManager {
    pub fn set_flush_transition_delay_for_tests(&self, ms: u64) {
        FLUSH_TRANSITION_DELAY_MS.store(ms, Ordering::Release);
    }

    pub(crate) fn flush_transition_delay() -> u64 {
        FLUSH_TRANSITION_DELAY_MS.load(Ordering::Acquire)
    }

    pub(crate) fn sleep_flush_transition_delay() {
        let ms = Self::flush_transition_delay();
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

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
            mutable_reader_cache: Mutex::new(HashMap::new()),
            flush_epochs: RwLock::new(HashMap::new()),
            flush_stop,
            flush_paused,
            next_txn_id: AtomicU64::new(1),
            config,
            pubsub: std::sync::OnceLock::new(),
        })
    }

    /// Wire the pub/sub hub once at engine startup. Deferred bulk/RELAXED
    /// writes publish their change notifications when the segment footer
    /// makes their rows visible.
    pub fn set_pubsub(&self, hub: std::sync::Arc<crate::pubsub::PubSubHub>) {
        let _ = self.pubsub.set(hub);
    }

    fn pubsub(&self) -> Option<std::sync::Arc<crate::pubsub::PubSubHub>> {
        self.pubsub.get().cloned()
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

    pub fn alloc_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn table_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join(db).join(table)
    }

    fn flush_epoch_file_path(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join("meta").join("flush_epochs").join(format!(
            "{}--{}.epoch",
            sanitize_component(db),
            sanitize_component(table)
        ))
    }

    /// Current memtable-flush epoch for a table. Lazily restored from the
    /// on-disk marker so epochs survive restarts.
    pub fn memtable_flush_epoch(&self, db: &str, table: &str) -> u64 {
        let key = table_key(db, table);
        if let Some(epoch) = self.flush_epochs.read().unwrap().get(&key) {
            return *epoch;
        }
        let epoch = read_flush_epoch_file(&self.flush_epoch_file_path(db, table));
        self.flush_epochs.write().unwrap().insert(key, epoch);
        epoch
    }

    /// Bump the segment-content epoch: rows were appended directly to
    /// segments outside the memtable flush pipeline (bulk/direct writes).
    /// Unique index snapshots stamped with the previous epoch are
    /// potentially incomplete and must be refreshed before probing.
    pub fn note_direct_segment_append(&self, db: &str, table: &str) {
        self.note_memtable_flush(db, table)
    }

    /// Bump the segment-content epoch: rows reached segments for this table
    /// (memtable flush or direct append). Unique index snapshots stamped with
    /// the previous epoch are potentially incomplete and must be refreshed
    /// before probing.
    pub fn note_memtable_flush(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let path = self.flush_epoch_file_path(db, table);
        let mut epochs = self.flush_epochs.write().unwrap();
        let current = epochs
            .get(&key)
            .copied()
            .unwrap_or_else(|| read_flush_epoch_file(&path));
        let next = current.saturating_add(1);
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent)
                .and_then(|_| std::fs::write(&path, next.to_string()))
            {
                tracing::error!(
                    db,
                    table,
                    error = %error,
                    "failed to persist memtable flush epoch; in-memory epoch still advanced"
                );
            }
        }
        epochs.insert(key, next);
    }

    /// Run `f` while holding the table's flush-visibility read lock. Scan and
    /// count paths use this so a concurrent memtable flush cannot expose the
    /// same rows via both a segment and the still-readable memtable.
    pub fn with_table_flush_read_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _guard = state.flush_visibility.read().unwrap();
        f()
    }

    /// Run `f` while holding the table's flush-visibility write lock. The
    /// memtable flush transition (rows become segment-visible, then the
    /// memtable is retired) runs under this so readers observe either the
    /// memtable or the segment, never both.
    pub(crate) fn with_flush_transition<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _guard = state.flush_visibility.write().unwrap();
        f()
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
        // Active writers can change their data and sidecar footer at any time,
        // but between writes the published state is stable. Reopening a large
        // active segment costs a full footer/sidecar parse per query, which
        // dominated read latency on write-heavy tables, so reuse the last
        // opened reader while the writer has nothing pending. Any append
        // falls back to the reopen path, whose `open_mutable_reader_owned`
        // also publishes a dirty footer first (preserving old semantics).
        if self.is_mutable_segment_path(path) {
            let cached = self.mutable_reader_cache.lock().unwrap().get(path).cloned();
            if let Some(reader) = cached {
                if !self.mutable_segment_has_pending_writes(path) {
                    return Ok(reader);
                }
            }
            self.invalidate_reader(path);
            let reader = std::sync::Arc::new(self.open_mutable_reader_owned(path)?);
            let mut cache = self.mutable_reader_cache.lock().unwrap();
            if cache.len() >= reader_cache_capacity().max(1) * 4 {
                cache.clear();
            }
            cache.insert(path.to_path_buf(), reader.clone());
            return Ok(reader);
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

    /// True when the active segment for `path` has appended rows that are not
    /// part of its published footer yet (dirty meta, append buffer, or
    /// row groups written without a footer publish). While this is false, a
    /// cached mutable reader exposes exactly the state a fresh open would.
    fn mutable_segment_has_pending_writes(&self, path: &Path) -> bool {
        let Some(state) = self.state_for_segment_path(path) else {
            return true;
        };
        let active_guard = state.active_segments.lock().unwrap();
        for segment in active_guard.values() {
            if same_segment_path(&segment.path, path) {
                return segment.dirty_meta
                    || segment.buffered_rows > 0
                    || segment.unpublished_row_count() > 0;
            }
        }
        true
    }

    /// Rows written to this table's active segments that a fresh reader
    /// cannot see yet (footer not published / still in the append buffer).
    /// Hot-store reclamation must not count these as persisted.
    pub fn unpublished_active_rows(&self, db: &str, table: &str) -> u64 {
        let state = {
            let tables = self.tables.read().unwrap();
            match tables.get(&table_key(db, table)) {
                Some(state) => state.clone(),
                None => return 0,
            }
        };
        let guard = state.active_segments.lock().unwrap();
        guard.values().map(|seg| seg.unpublished_row_count()).sum()
    }

    pub fn invalidate_reader(&self, path: &Path) {
        self.mutable_reader_cache.lock().unwrap().remove(path);
        self.reader_cache.lock().unwrap().invalidate(path);
        self.segment_meta_cache.lock().unwrap().invalidate(path);
        self.cluster_meta_cache.lock().unwrap().invalidate(path);
    }

    pub fn invalidate_readers_under(&self, dir: &Path) {
        self.mutable_reader_cache
            .lock()
            .unwrap()
            .retain(|path, _| !path.starts_with(dir));
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
        let _guard = acquire_mutation_lock(&state, &key)?;
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
        let mut guards = Vec::with_capacity(states.len());
        for (key, state) in keys.iter().zip(states.iter()) {
            guards.push(acquire_mutation_lock(state, key)?);
        }
        let _guards = guards;
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
        batches: &[std::sync::Arc<RecordBatch>],
    ) -> Result<()> {
        let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
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

    fn table_states_snapshot(&self) -> Vec<Arc<TableSegmentState>> {
        self.tables.read().unwrap().values().cloned().collect()
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

    pub fn remove_table_state(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let state = self.tables.write().unwrap().remove(&key);
        if let Some(state) = state {
            state.retired.store(true, Ordering::Release);
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
            state.active_segments.lock().unwrap().clear();
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
            state.active_segments.lock().unwrap().clear();
        }
    }
}

/// Acquire a table mutation lock with a bounded wait. A stalled lock holder
/// (slow fsync, durable-tail rewrite, compaction) must surface as an explicit
/// timeout error instead of blocking the caller forever, and slow acquisitions
/// are logged so lock queueing is visible in the server log.
fn acquire_mutation_lock<'a>(
    state: &'a TableSegmentState,
    key: &str,
) -> Result<std::sync::MutexGuard<'a, ()>> {
    let timeouts = crate::common::config::storage_timeouts();
    let timeout = Duration::from_millis(timeouts.mutation_lock_timeout_ms.max(1));
    let warn_after = Duration::from_millis(timeouts.mutation_lock_warn_ms.max(1));
    let start = Instant::now();
    let mut backoff = Duration::from_millis(1);
    loop {
        match state.mutation_lock.try_lock() {
            Ok(guard) => {
                let waited = start.elapsed();
                if waited >= warn_after {
                    tracing::warn!(
                        table = key,
                        wait_ms = waited.as_millis() as u64,
                        "table mutation lock acquisition was slow"
                    );
                }
                return Ok(guard);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                if start.elapsed() >= timeout {
                    return Err(TaibaiError::ResourceBusy(format!(
                        "table mutation lock timeout after {}ms waiting on {}",
                        timeout.as_millis(),
                        key
                    )));
                }
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(Duration::from_millis(16));
            }
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                panic!("table mutation lock poisoned: {}", poisoned)
            }
        }
    }
}
