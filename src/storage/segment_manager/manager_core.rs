static FLUSH_TRANSITION_DELAY_MS: AtomicU64 = AtomicU64::new(0);

/// Striped mutexes serializing per-table flush-epoch file persistence outside
/// the global epoch lock. Hashing the table key to one of N stripes keeps
/// same-table writes ordered while unrelated tables usually proceed in
/// parallel; the critical section is one small tmp+rename, so stripe sharing
/// between different tables is immaterial. Instance-local semantics are not
/// needed: the persisted value is re-read from the owning manager's memory
/// inside the stripe lock (see `persist_flush_epoch`).
fn flush_epoch_persist_lock(key: &str) -> &std::sync::Mutex<()> {
    const STRIPES: usize = 16;
    static LOCKS: std::sync::OnceLock<Vec<std::sync::Mutex<()>>> = std::sync::OnceLock::new();
    let locks = LOCKS.get_or_init(|| (0..STRIPES).map(|_| std::sync::Mutex::new(())).collect());
    // FNV-1a: stable, allocation-free stripe selection for short table keys.
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    &locks[(hash as usize) % locks.len()]
}

impl SegmentManager {
    pub fn set_flush_transition_delay_for_tests(&self, ms: u64) {
        FLUSH_TRANSITION_DELAY_MS.store(ms, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn partition_lock_arc_for_tests(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Arc<std::sync::Mutex<()>> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        partition_mutation_lock_arc(&state, partition_key)
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
                            recover_replace_journals(&tbl_dir);
                            // Deferred (V02) statement journals are judged
                            // ONLY here: single writer per data dir means no
                            // statement is in flight at startup.
                            crate::storage::segment::reader::recover_statement_footer_journals(&tbl_dir);
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
            txn_mutations: std::sync::OnceLock::new(),
            rep_log: std::sync::OnceLock::new(),
        })
    }

    /// Wire the replication log once at engine startup (primaries only).
    pub fn set_rep_log(&self, log: std::sync::Arc<crate::storage::replication::RepLog>) {
        let _ = self.rep_log.set(log);
    }

    pub fn rep_log(&self) -> Option<&std::sync::Arc<crate::storage::replication::RepLog>> {
        self.rep_log.get()
    }

    /// Emit a `DeleteFooterRewrite` (derive db/table from the segment path).
    pub(crate) fn emit_delete_footer_rewrite(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) {
        let Some(log) = self.rep_log() else {
            return;
        };
        let Some((db, table)) = db_table_from_segment_path(path) else {
            return;
        };
        let Some(segment) = path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        log.enqueue(
            crate::storage::replication::ReplicationEvent::DeleteFooterRewrite {
                db,
                table,
                segment: segment.to_string(),
                bitmap: bitmap.to_vec(),
                deleted_count,
            },
        );
    }

    /// Emit a `SegmentsReplaced` after a compaction/UPDATE replacement commit
    /// (derive db/table from the final segment path).
    pub(crate) fn emit_segments_replaced(&self, final_path: &Path, old_segments: &[PathBuf]) {
        let Some(log) = self.rep_log() else {
            return;
        };
        let Some((db, table)) = db_table_from_segment_path(final_path) else {
            return;
        };
        let Some(new_name) = final_path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        let retired = old_segments
            .iter()
            .filter(|old| old.as_path() != final_path)
            .filter_map(|old| old.file_name().and_then(|name| name.to_str()))
            .map(str::to_string)
            .collect();
        log.enqueue(
            crate::storage::replication::ReplicationEvent::SegmentsReplaced {
                db,
                table,
                new_segments: vec![new_name.to_string()],
                retired,
            },
        );
    }

    /// Emit a `SegmentsRemoved` for a TTL/DROP purge batch (db/table derived
    /// from the table dir).
    pub(crate) fn emit_segments_removed(&self, table_dir: &Path, segments: &[String]) {
        if segments.is_empty() {
            return;
        }
        let Some(log) = self.rep_log() else {
            return;
        };
        let Some(table) = table_dir.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        let Some(db) = table_dir
            .parent()
            .and_then(|parent| parent.file_name())
            .and_then(|name| name.to_str())
        else {
            return;
        };
        log.enqueue(
            crate::storage::replication::ReplicationEvent::SegmentsRemoved {
                db: db.to_string(),
                table: table.to_string(),
                segments: segments.to_vec(),
            },
        );
    }

    /// Current active segment file name for `(db, table, partition)`.
    pub(crate) fn active_segment_name_for(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Option<String> {
        let key = table_key(db, table);
        let state = self.tables.read().unwrap().get(&key).cloned()?;
        let guard = state.active_segments.lock().unwrap();
        let seg = guard.get(partition_key)?;
        Self::active_segment_file_name(&seg.path).ok()
    }

    /// Enqueue a `SegmentUpdate` for replication (no-op without a log).
    /// `new_segment = Some(name)` marks a freshly written sealed segment
    /// (`Final`); `None` marks the active segment's published prefix.
    pub(crate) fn emit_segment_update(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        memtable_ids: Vec<uuid::Uuid>,
        new_segment: Option<&str>,
    ) {
        let Some(log) = self.rep_log() else {
            return;
        };
        let (segment, kind) = match new_segment {
            Some(name) => (
                name.to_string(),
                crate::storage::replication::SegmentUpdateKind::Final,
            ),
            None => {
                let Some(segment) = self.active_segment_name_for(db, table, partition_key) else {
                    return;
                };
                (
                    segment,
                    crate::storage::replication::SegmentUpdateKind::Prefix,
                )
            }
        };
        log.enqueue(
            crate::storage::replication::ReplicationEvent::SegmentUpdate {
                db: db.to_string(),
                table: table.to_string(),
                segment,
                kind,
                memtable_ids,
            },
        );
    }

    /// Emit a `Final` update for a just-sealed active segment, deriving
    /// db/table from the segment path (used by seal sites that only hold the
    /// writer).
    pub(crate) fn emit_segment_update_final_path(&self, segment_path: &std::path::Path) {
        let Some(log) = self.rep_log() else {
            return;
        };
        let Some(name) = segment_path.file_name().and_then(|name| name.to_str()) else {
            return;
        };
        let Some(table_dir) = segment_path.parent() else {
            return;
        };
        let (Some(table), Some(db)) = (
            table_dir.file_name().and_then(|name| name.to_str()),
            table_dir
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str()),
        ) else {
            return;
        };
        log.enqueue(
            crate::storage::replication::ReplicationEvent::SegmentUpdate {
                db: db.to_string(),
                table: table.to_string(),
                segment: name.to_string(),
                kind: crate::storage::replication::SegmentUpdateKind::Final,
                memtable_ids: Vec::new(),
            },
        );
    }

    /// Wire the pub/sub hub once at engine startup. Deferred bulk/RELAXED
    /// writes publish their change notifications when the segment footer
    /// makes their rows visible.
    pub fn set_pubsub(&self, hub: std::sync::Arc<crate::pubsub::PubSubHub>) {
        let _ = self.pubsub.set(hub);
    }

    /// Wire the deferred-DML registry once at engine startup.
    pub fn set_txn_mutations(
        &self,
        registry: std::sync::Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>,
    ) {
        let _ = self.txn_mutations.set(registry);
    }

    pub fn txn_mutations(
        &self,
    ) -> Option<&std::sync::Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>> {
        self.txn_mutations.get()
    }

    /// Open a reader for a query. Transactions that ran deferred DELETE or
    /// UPDATE read through a fresh instance carrying their composed
    /// tombstone overlay; every other reader takes the shared cache. The
    /// overlay composition is the single choke point that makes all scan
    /// paths (strategy, row filter, count, metadata aggregates) honor the
    /// transaction's uncommitted DML.
    pub fn open_scan_reader(
        &self,
        path: &Path,
        reader_txn_id: u64,
    ) -> Result<std::sync::Arc<SegmentReader>> {
        let cached = self.open_reader_cached(path)?;
        if reader_txn_id == 0 {
            return Ok(cached);
        }
        let Some(registry) = self.txn_mutations() else {
            return Ok(cached);
        };
        let Some(overlay) = registry.segment_overlay(reader_txn_id, path) else {
            return Ok(cached);
        };
        let mut reader = SegmentReader::open(path)?;
        reader.set_txn_delete_overlay(overlay);
        Ok(std::sync::Arc::new(reader))
    }

    /// True when this transaction carries deferred tombstones for the
    /// segment. Metadata fast paths that answer from footer counts or sums
    /// must fall back to the overlay-aware reader for such segments.
    pub fn txn_has_segment_overlay(&self, path: &Path, reader_txn_id: u64) -> bool {
        reader_txn_id != 0
            && self
                .txn_mutations()
                .is_some_and(|registry| registry.segment_overlay(reader_txn_id, path).is_some())
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
        let body_stop = Arc::clone(&stop);
        let body = move || {
            // One supervised round = one iteration of the original polling
            // loop. The supervisor (audit P1-5) catches a panicking
            // `periodic_flush_check` — e.g. a poisoned-lock `unwrap` — logs
            // it and retries with backoff instead of leaving periodic
            // sealing silently dead until process restart.
            crate::common::thread_supervisor::sleep_interruptibly(
                &body_stop,
                Duration::from_secs(FLUSH_CHECK_INTERVAL_SECS),
            );
            if body_stop.load(Ordering::Acquire) {
                return;
            }
            if paused.load(Ordering::Acquire) {
                tracing::debug!("flush thread paused, skipping periodic check");
                return;
            }
            if let Err(e) = mgr_clone.periodic_flush_check() {
                tracing::warn!(error = %e, "periodic flush check failed");
            }
        };
        // The supervisor shares `flush_stop`, so `SegmentManager::shutdown`
        // stops the loop exactly as before supervision; the returned handle
        // detaches (dropped here), matching the previous discarded
        // JoinHandle. Pause/resume semantics are unchanged: the flag is
        // re-read at the top of every round.
        let _ =
            crate::common::thread_supervisor::spawn_supervised_with_stop("taibai-flush", stop, body);
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
        self.data_dir
            .join("meta")
            .join("flush_epochs")
            .join(format!(
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

    /// Bump the segment-content epoch: rows reached segments for this table
    /// outside a primitive that already bumps (`replace_segments_with_tmp`
    /// and `rewrite_delete_footer` bump on their own; direct appends, flush
    /// installs, and removal-only paths do not). Unique index snapshots
    /// stamped with the previous epoch are potentially incomplete and must
    /// be refreshed before probing.
    ///
    /// The global epoch lock only covers the in-memory bump — the file write
    /// runs outside it, serialized per table through a stripe lock — so one
    /// table's `fsync` no longer blocks every other table's epoch reads.
    pub fn note_segments_changed(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let path = self.flush_epoch_file_path(db, table);
        {
            let mut epochs = self.flush_epochs.write().unwrap();
            let current = epochs
                .get(&key)
                .copied()
                .unwrap_or_else(|| read_flush_epoch_file(&path));
            let next = current.saturating_add(1);
            epochs.insert(key.clone(), next);
        }
        self.persist_flush_epoch(&key, &path);
    }

    /// Persist the table's epoch file outside the global epoch lock.
    ///
    /// Same-table persistence is serialized by a stripe mutex, and the value
    /// written is re-read from memory INSIDE that mutex, so a concurrent
    /// newer bump always wins and the file never regresses (a bump's own
    /// value may be superseded before it reaches disk — persisting the newer
    /// one implies it). Losing the very last persistence to a crash is
    /// acceptable by design: restart restores the epoch from the file, the
    /// only lost value is the in-memory maximum, and the sole consequence is
    /// one extra index-snapshot invalidation after the next write.
    fn persist_flush_epoch(&self, key: &str, path: &Path) {
        let _stripe = flush_epoch_persist_lock(key).lock().unwrap_or_else(|p| {
            p.into_inner()
        });
        let value = self
            .flush_epochs
            .read()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or_else(|| read_flush_epoch_file(path));
        if let Some(parent) = path.parent() {
            // tmp+rename: a torn in-place overwrite of the epoch file made
            // it fall back to 0 on read, wrongly marking index snapshots as
            // stale (or fresh) after a crash.
            let tmp = path.with_extension("epoch.tmp");
            if let Err(error) = std::fs::create_dir_all(parent).and_then(|_| {
                std::fs::write(&tmp, value.to_string())?;
                std::fs::rename(&tmp, &path)
            }) {
                let _ = std::fs::remove_file(&tmp);
                tracing::error!(
                    key,
                    error = %error,
                    "failed to persist memtable flush epoch; in-memory epoch still advanced"
                );
            }
        }
    }

    /// Bump the content epoch for the table owning `path`, derived from the
    /// `<data_dir>/<db>/<table>/` layout. Used by the segment-change
    /// primitives so every content change bumps exactly where it lands; a
    /// path outside a table dir (scratch files) logs and skips.
    fn bump_flush_epoch_for_segment(&self, path: &Path) {
        let Some(table_dir) = path.parent() else {
            return;
        };
        let (db, table) = match self.db_table_for_segment_path(table_dir) {
            Ok(names) => names,
            Err(error) => {
                tracing::debug!(
                    path = %path.display(),
                    error = %error,
                    "cannot resolve table for content-epoch bump"
                );
                return;
            }
        };
        self.note_segments_changed(&db, &table);
    }

    fn db_table_for_segment_path(&self, table_dir: &Path) -> Result<(String, String)> {
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
        Ok((
            db.as_os_str().to_string_lossy().into_owned(),
            table.as_os_str().to_string_lossy().into_owned(),
        ))
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

    /// Refresh the per-table zstd level override used by storage-internal
    /// writers (memtable flush, recovery, direct bulk). Called from DDL and
    /// the engine write paths that hold fresh `TableMeta`.
    pub fn set_table_compression_level(&self, db: &str, table: &str, level: Option<i32>) {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        state.set_compression_level_override(level);
    }

    /// Effective level for the table right now: its DDL override, or the
    /// global configured level when unset.
    pub(crate) fn table_compression_level(&self, db: &str, table: &str) -> i32 {
        let key = table_key(db, table);
        let tables = self.tables.read().unwrap();
        let override_level = tables
            .get(&key)
            .and_then(|state| state.compression_level_override());
        crate::storage::segment_manager::config::resolve_compression_level(override_level)
    }

    /// Same resolution, keyed by any path inside the table directory (a
    /// segment file or the dir itself). Used by writers that only carry a
    /// path, e.g. recovery and standalone tail-flush segments.
    fn table_compression_level_for_path(&self, path: &Path) -> Result<i32> {
        let key = self.table_state_key_from_dir(path)?;
        let tables = self.tables.read().unwrap();
        let override_level = tables
            .get(&key)
            .and_then(|state| state.compression_level_override());
        Ok(crate::storage::segment_manager::config::resolve_compression_level(override_level))
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
        let (active_paths, sealing_paths) = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&key)
                .map(|state| {
                    let active_guard = state.active_segments.lock().unwrap();
                    let active = active_guard
                        .values()
                        .map(|segment| segment.path.clone())
                        .collect::<HashSet<_>>();
                    (active, state.sealing_paths())
                })
                .unwrap_or_default()
        };

        if active_paths.is_empty() && sealing_paths.is_empty() {
            return Ok(segments);
        }

        Ok(segments
            .into_iter()
            .filter(|path| !active_paths.contains(path) && !sealing_paths.contains(path))
            .collect())
    }

    pub fn cleanup_fully_deleted_segments(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
    ) -> Result<u64> {
        self.with_table_mutation_lock(db, table, || {
            // Same pin gate as compaction/TTL/DDL: an open transaction's
            // deferred tombstones reference segment paths, and deleting a
            // fully-deleted segment under them left that COMMIT retrying a
            // vanished path forever.
            if self
                .txn_mutations()
                .is_some_and(|registry| registry.table_has_pins(db, table))
            {
                tracing::debug!(
                    db,
                    table,
                    "table pinned by an open transaction, skipping fully-deleted cleanup"
                );
                return Ok(0);
            }
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
        exclude: &HashSet<PathBuf>,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let segments = Self::discover_segments(table_dir)?;

        for seg_path in segments.iter().rev() {
            // This search serves the unpartitioned bucket (empty partition
            // key, regular `seg_*.dat` files) and unpartitioned tables.
            // Partition files must be skipped: appending NULL-key rows to a
            // `p<key>_*.dat` segment silently relocates them into a named
            // partition, where filename-based partition pruning then hides
            // them from every range-bounded scan.
            if let Some(name) = seg_path.file_name().and_then(|n| n.to_str()) {
                if crate::storage::partition::is_partition_segment(name) {
                    continue;
                }
            }
            // A path excluded here is mid-seal outside the locks: its footer
            // is about to be finalized by its own seal, so it must never be
            // resumed for append or reported as an append target.
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
        loop {
            let state = self.get_or_create_state_for_table_dir(table_dir)?;
            let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
            let path = table_dir.join(store::regular_segment_filename(seq));
            if !path.exists() {
                return Ok(path);
            }
            // The in-process counter fell behind the directory — a scan-based
            // allocation (compaction replacement) owns this sequence. Reseed
            // from the on-disk truth and retry; opening with truncate would
            // silently destroy the replacement segment.
            let dir_max = store::max_segment_seq_by_scan(table_dir, None)?;
            tracing::warn!(
                seq,
                dir_max,
                table_dir = %table_dir.display(),
                "segment sequence collision; reseeding counter from directory"
            );
            state.seq.fetch_max(dir_max, Ordering::Relaxed);
        }
    }

    pub fn next_partition_segment_path(
        &self,
        table_dir: &Path,
        partition_key: &str,
    ) -> Result<PathBuf> {
        loop {
            let state = self.get_or_create_state_for_table_dir(table_dir)?;
            let seq = {
                let mut partition_seq = state.partition_seq.lock().unwrap();
                let entry = partition_seq.entry(partition_key.to_string()).or_insert(0);
                *entry += 1;
                *entry
            };
            let path = table_dir.join(store::partition_segment_filename(partition_key, seq));
            if !path.exists() {
                return Ok(path);
            }
            let dir_max = store::max_segment_seq_by_scan(table_dir, Some(partition_key))?;
            tracing::warn!(
                seq,
                dir_max,
                partition_key,
                table_dir = %table_dir.display(),
                "partition segment sequence collision; reseeding counter from directory"
            );
            let mut partition_seq = state.partition_seq.lock().unwrap();
            if let Some(entry) = partition_seq.get_mut(partition_key) {
                *entry = (*entry).max(dir_max);
            }
        }
    }

    pub fn next_replacement_segment_path(
        &self,
        table_dir: &Path,
        partition_key: Option<&str>,
    ) -> Result<PathBuf> {
        let path = store::next_segment_path_by_scan(table_dir, partition_key)?;
        // Fold the scan-based allocation back into the in-process counters so
        // `next_segment_path`/`next_partition_segment_path` can never hand out
        // the same sequence again (their `truncate`-on-create would destroy
        // this replacement segment). Failure to sync is non-fatal: the
        // exists-check loop in the counter allocators still self-heals.
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            let seq = match partition_key {
                Some(key) => store::parse_partition_seq(name, key),
                None => store::parse_regular_seq(name),
            };
            if let Some(seq) = seq {
                if let Ok(state) = self.get_or_create_state_for_table_dir(table_dir) {
                    match partition_key {
                        Some(key) => {
                            let mut partition_seq = state.partition_seq.lock().unwrap();
                            if let Some(entry) = partition_seq.get_mut(key) {
                                *entry = (*entry).max(seq);
                            } else {
                                partition_seq.insert(key.to_string(), seq);
                            }
                        }
                        None => {
                            state.seq.fetch_max(seq, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
        Ok(path)
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
                // Evict ONE entry instead of clearing the whole cache: a full
                // clear made every other active segment's next read pay a
                // full reopen + footer parse at once. The cache field is a
                // plain `HashMap` (defined with the struct, no order kept),
                // so the victim is the first entry in iteration order — any
                // single eviction bounds growth, and cache correctness never
                // depended on the choice: `invalidate_reader` evicts on every
                // visibility change regardless.
                if let Some(victim) = cache.keys().next().cloned() {
                    cache.remove(&victim);
                }
            }
            cache.insert(path.to_path_buf(), reader.clone());
            return Ok(reader);
        }

        if let Some(reader) = self.reader_cache.lock().unwrap().get(path) {
            return Ok(reader);
        }

        // The reader cache is bounded by BYTES, not entry count: owned
        // readers hold a full heap copy of the file, so before a5b116b's
        // smaller replacement segments the count-bounded cache could retain
        // `capacity` copies of up to ~255 MB each. Charge each entry by its
        // real footprint — file length for owned readers, a small fixed
        // constant for mmap readers (whose mapping lives in the page cache).
        let (reader, owned) = if self.requires_owned_reader(path) {
            match SegmentReader::open_owned(path) {
                Ok(reader) => (reader, true),
                Err(err) if is_transient_footer_error(&err) => {
                    (self.open_mutable_reader_owned(path)?, true)
                }
                Err(err) => return Err(err),
            }
        } else {
            match SegmentReader::open(path) {
                Ok(reader) => (reader, false),
                Err(err) if is_transient_footer_error(&err) => {
                    (self.open_mutable_reader_owned(path)?, true)
                }
                Err(err) => return Err(err),
            }
        };
        let cost_bytes = if owned {
            std::fs::metadata(path)
                .map(|meta| meta.len())
                .unwrap_or(reader_cache::AVG_READER_COST_BYTES)
        } else {
            reader_cache::AVG_READER_COST_BYTES
        };
        Ok(self
            .reader_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), reader, cost_bytes))
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
    /// Returns the cached zone map for the catalog column `col_idx` (resolved
    /// against `table_schema`) in `path`, deriving and caching it on first
    /// access. Returns `None` when the segment has no usable bounds for the
    /// column (empty/null) or the footer predates the column. Mutable
    /// (unsealed) segments are never cached because their footer can still
    /// change. The cache shares the reader-cache invalidation lifecycle, so
    /// compaction/seal/delete automatically clears stale entries.
    pub fn cluster_zone_map_cached(
        &self,
        path: &Path,
        table_schema: &crate::types::batch::Schema,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        // Resolve the catalog column to the footer index BY NAME. Positional
        // use of the catalog index reads the wrong physical column once a
        // DROP/RENAME COLUMN shifts catalog indices while sealed footers
        // keep their layout — pruning then skips segments that hold matching
        // rows. The footer index also keys the cache: for a fixed segment
        // file it identifies the physical column, so post-DDL lookups land
        // on a fresh key instead of a stale entry.
        let footer_idx = self.resolve_footer_column_index(path, table_schema, col_idx)?;
        if self.is_mutable_segment_path(path) {
            return self.derive_cluster_zone_map(path, footer_idx);
        }

        if let Some(cached) = self.cluster_meta_cache.lock().unwrap().get(path, footer_idx) {
            return cached;
        }

        let derived = self.derive_cluster_zone_map(path, footer_idx);
        self.cluster_meta_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), footer_idx, derived.clone());
        derived
    }

    /// Translate a catalog column index into the segment's footer index by
    /// name (current name first, then the column's historical names), so
    /// positional metadata access below lands on the right physical column.
    fn resolve_footer_column_index(
        &self,
        path: &Path,
        table_schema: &crate::types::batch::Schema,
        catalog_idx: usize,
    ) -> Option<usize> {
        let column = table_schema.columns.get(catalog_idx)?;
        if let Some(meta) = self.segment_meta(path).ok().flatten() {
            let mut names = Vec::with_capacity(column.previous_names.len() + 1);
            names.push(column.name.as_str());
            names.extend(column.previous_names.iter().map(String::as_str));
            return names
                .iter()
                .find_map(|name| meta.footer_schema().index_of(name));
        }
        let reader = self.open_reader_cached(path).ok()?;
        let mut names = Vec::with_capacity(column.previous_names.len() + 1);
        names.push(column.name.as_str());
        names.extend(column.previous_names.iter().map(String::as_str));
        names
            .iter()
            .find_map(|name| reader.schema().index_of(name))
    }

    /// Positional zone-map access in FOOTER space: `footer_idx` must already
    /// have been translated from the catalog index by name (see
    /// [`Self::resolve_footer_column_index`]).
    fn derive_cluster_zone_map(
        &self,
        path: &Path,
        footer_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        if let Some(meta) = self.segment_meta(path).ok().flatten() {
            let zm = meta.footer_zone_maps().get(footer_idx)?;
            if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
                return None;
            }
            return Some(zm.clone());
        }
        let reader = self.open_reader_cached(path).ok()?;
        let zm = reader.segment_zone_maps().get(footer_idx)?;
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

        // Two-phase commit: the journal records which old segments the
        // replacement must retire. A crash between the rename and the old
        // file removals used to leave the new AND old segments on disk, and
        // every scan then counted those rows twice. Startup recovery
        // (`recover_replace_journals`) finishes the retirement.
        let journal = replace_journal_path(final_path);
        write_replace_journal(&journal, old_segments)?;

        if let Err(error) = std::fs::rename(tmp_path, final_path) {
            let _ = std::fs::remove_file(&journal);
            return Err(error.into());
        }
        // From here on the replacement is in effect: keep removing the old
        // segments even after an individual failure (the journal lets
        // startup recovery finish), and report the first error.
        let mut first_removal_error = None;
        for old_seg in old_segments {
            if old_seg != final_path {
                if let Err(error) = self.remove_segment_file(old_seg) {
                    tracing::warn!(
                        old_segment = %old_seg.display(),
                        error = %error,
                        "old segment removal failed; startup recovery will retry"
                    );
                    first_removal_error = first_removal_error.or(Some(error));
                }
            }
        }
        if first_removal_error.is_none() {
            let _ = std::fs::remove_file(&journal);
        }

        remove_sidecar_meta(tmp_path)?;
        let _ = self.open_reader_cached(final_path);
        // The replacement changed this table's on-disk content: bump the
        // epoch here so no caller can forget (persisted unique-index
        // snapshots pointing at the old segments must refresh).
        self.bump_flush_epoch_for_segment(final_path);
        self.emit_segments_replaced(final_path, old_segments);
        Ok(())
    }

    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        self.rewrite_delete_footer_with_mode(path, bitmap, deleted_count, FooterRewriteMode::Immediate)
    }

    /// Rewrite a segment's delete footer under the chosen journal lifecycle:
    /// Immediate (single-leg statements, journal cleared on landing) or
    /// Deferred (multi-leg statements — the journal also carries the undo
    /// footer and the image watermark and is cleared by the caller once the
    /// other legs are durable).
    pub fn rewrite_delete_footer_with_mode(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
        mode: FooterRewriteMode,
    ) -> Result<()> {
        let result = self.rewrite_delete_footer_inner(path, bitmap, deleted_count, mode);
        if result.is_ok() {
            // Deleted rows changed this segment's visible content: bump the
            // epoch here so no caller can forget (a crash between the
            // rewrite and any later tombstone append left deleted keys
            // "present" in stale unique-index snapshots).
            self.bump_flush_epoch_for_segment(path);
            self.emit_delete_footer_rewrite(path, bitmap, deleted_count);
        }
        result
    }

    fn rewrite_delete_footer_inner(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
        mode: FooterRewriteMode,
    ) -> Result<()> {
        let mutable = self.is_mutable_segment_path(path);
        self.invalidate_reader(path);
        if mutable {
            // Hold the per-table active-segment lock across the whole
            // truncate+rewrite: the background flush thread appends buffered
            // rows and publishes footers under the same lock, so an
            // unsynchronized set_len here could truncate freshly written
            // rows before their footer was published. Flushing the target
            // first also guarantees the embedded footer we rewrite from is
            // current.
            let Some(state) = self.state_for_segment_path(path) else {
                SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count, mode)?;
                remove_sidecar_meta(path)?;
                return Ok(());
            };
            let mut active_guard = state.active_segments.lock().unwrap();
            for segment in active_guard.values_mut() {
                if same_segment_path(&segment.path, path) {
                    if segment.buffered_rows > 0 {
                        segment.flush_buffer(&|p| self.invalidate_reader(p))?;
                    }
                    if segment.dirty_meta {
                        segment.flush_meta(&|p| self.invalidate_reader(p))?;
                    }
                    break;
                }
            }
            SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count, mode.clone())?;
            let footer = read_embedded_footer(path)?;
            append_sidecar_footer(path, footer.data_end, &footer.footer)?;
            for segment in active_guard.values_mut() {
                if same_segment_path(&segment.path, path) {
                    segment.delete_bitmap = bitmap.to_vec();
                    segment.deleted_row_count = deleted_count;
                    segment.dirty_meta = true;
                    break;
                }
            }
            return Ok(());
        }
        SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count, mode)?;
        remove_sidecar_meta(path)?;
        Ok(())
    }

    /// Run `f` while holding the table-wide mutation gate. The gate excludes
    /// every other mutator of the table: other table-wide operations (via
    /// `mutation_lock`) and all partition-scoped writers (via the frozen
    /// `partition_locks` registry plus every partition mutex held for the
    /// duration of `f`). Partition-scoped writers should prefer
    /// `with_partition_mutation_lock` so unrelated partitions stay parallel.
    pub fn with_table_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        self.with_state_table_mutation_lock(&state, &key, || {
            ensure_state_not_retired(&state, db, table)?;
            f()
        })
    }

    /// Acquire the wide mutation gate for an already-resolved table state.
    /// Callers are responsible for the retired-state check.
    ///
    /// The gate freezes partition-scoped writers through `partition_gate`:
    /// the registry write guard is only held for the partition-mutex
    /// snapshot, then released while `f` runs so new partitions can still be
    /// created (their writers will self-freeze on the gate flag).
    fn with_state_table_mutation_lock<T>(
        &self,
        state: &Arc<TableSegmentState>,
        key: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let _mutation_guard = acquire_mutation_lock(state, key)?;
        state.partition_gate.store(true, Ordering::Release);
        let _gate_reset = PartitionGateReset(Arc::clone(state));
        let registry = state.partition_locks.write().unwrap();
        let (partition_keys, partition_locks) = sorted_partition_locks(&registry);
        drop(registry);
        let timeouts = crate::common::config::storage_timeouts();
        let timeout = Duration::from_secs(timeouts.mutation_lock_timeout_secs.max(1));
        let mut partition_guards = Vec::with_capacity(partition_locks.len());
        for (partition, lock) in partition_keys.iter().zip(&partition_locks) {
            partition_guards.push(acquire_partition_lock_for_wide_gate(
                lock, key, partition, timeout,
            )?);
        }
        state.wait_sealing_segments_drained(timeout)?;
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
        let mut mutation_guards = Vec::with_capacity(states.len());
        for (key, state) in keys.iter().zip(states.iter()) {
            mutation_guards.push(acquire_mutation_lock(state, key)?);
        }
        for state in &states {
            state.partition_gate.store(true, Ordering::Release);
        }
        let _gate_reset = DatabaseGatesReset(states.clone());
        let mut registries = Vec::with_capacity(states.len());
        let mut partition_arc_sets = Vec::with_capacity(states.len());
        let mut partition_key_sets = Vec::with_capacity(states.len());
        for state in &states {
            let registry = state.partition_locks.write().unwrap();
            let (partition_keys, arcs) = sorted_partition_locks(&registry);
            partition_arc_sets.push(arcs);
            partition_key_sets.push(partition_keys);
            registries.push(registry);
        }
        drop(registries);
        let timeouts = crate::common::config::storage_timeouts();
        let timeout = Duration::from_secs(timeouts.mutation_lock_timeout_secs.max(1));
        let mut partition_guard_sets = Vec::with_capacity(states.len());
        for (table_idx, key) in keys.iter().enumerate() {
            let mut guards = Vec::new();
            let partitions = &partition_key_sets[table_idx];
            for (partition, lock) in partitions.iter().zip(&partition_arc_sets[table_idx]) {
                match acquire_partition_lock_for_wide_gate(lock, key, partition, timeout) {
                    Ok(guard) => guards.push(guard),
                    Err(error) => return Err(error),
                }
            }
            partition_guard_sets.push(guards);
        }
        for state in &states {
            state.wait_sealing_segments_drained(timeout)?;
        }
        f()
    }

    /// Run `f` while holding only one partition's mutation lock. Mutators of
    /// other partitions of the same table proceed in parallel; table-wide
    /// mutation operations still exclude this writer through the wide gate.
    pub fn with_partition_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let lock = partition_mutation_lock_arc(&state, partition_key);
        let _guard = acquire_partition_mutation_lock(&state, &lock, &key, partition_key)?;
        ensure_state_not_retired(&state, db, table)?;
        f()
    }

    /// Run `f` while holding the mutation locks of an explicit partition
    /// subset. Locks are acquired in sorted order so overlapping subset
    /// operations cannot deadlock; table-wide mutations exclude this caller
    /// through the wide-gate re-check inside each partition acquisition, and
    /// partitions outside the set stay fully parallel.
    pub fn with_partition_set_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        partition_keys: &[&str],
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        if partition_keys.is_empty() {
            return f();
        }
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let mut sorted: Vec<&str> = partition_keys.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let locks = sorted
            .iter()
            .map(|partition| partition_mutation_lock_arc(&state, partition))
            .collect::<Vec<_>>();
        let mut guards = Vec::with_capacity(locks.len());
        for (partition, lock) in sorted.iter().zip(&locks) {
            guards.push(acquire_partition_mutation_lock(
                &state, lock, &key, partition,
            )?);
        }
        ensure_state_not_retired(&state, db, table)?;
        f()
    }

    pub fn try_with_partition_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let lock = {
            let registry = match state.partition_locks.try_read() {
                Ok(registry) => registry,
                Err(_) => return Ok(None),
            };
            match registry.get(partition_key) {
                Some(lock) => Some(Arc::clone(lock)),
                None => None,
            }
        };
        let lock = match lock {
            Some(lock) => lock,
            None => partition_mutation_lock_arc(&state, partition_key),
        };
        let _guard = match try_lock_or_none(&lock, "partition mutation lock") {
            Some(guard) if !state.partition_gate.load(Ordering::Acquire) => guard,
            Some(_) => return Ok(None),
            None => return Ok(None),
        };
        ensure_state_not_retired(&state, db, table)?;
        f().map(Some)
    }

    /// Lock-free counterpart of [`Self::with_table_mutation_lock`]. The
    /// freeze protocol differs from the blocking variant on purpose: the
    /// wide gate flag is not set; instead the registry write guard stays
    /// held through `f`, which blocks new partition creation for its
    /// duration.
    pub fn try_with_table_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<Option<T>> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _mutation_guard = match try_lock_or_none(&state.mutation_lock, "table mutation lock") {
            Some(guard) => guard,
            None => return Ok(None),
        };
        let registry = match state.partition_locks.try_write() {
            Ok(registry) => registry,
            Err(_) => return Ok(None),
        };
        let (_partition_keys, partition_locks) = sorted_partition_locks(&registry);
        let mut partition_guards = Vec::with_capacity(partition_locks.len());
        for lock in &partition_locks {
            match try_lock_or_none(lock, "partition mutation lock") {
                Some(guard) => partition_guards.push(guard),
                None => return Ok(None),
            }
        }
        if state.has_sealing_segments() {
            return Ok(None);
        }
        ensure_state_not_retired(&state, db, table)?;
        let result = f().map(Some);
        drop(partition_guards);
        drop(partition_locks);
        drop(registry);
        result
    }

    /// Lock-free counterpart of [`Self::with_database_mutation_locks`],
    /// with the same registry-held freeze protocol as
    /// [`Self::try_with_table_mutation_lock`].
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
        let mut mutation_guards = Vec::with_capacity(states.len());
        let mut registries = Vec::with_capacity(states.len());
        let mut partition_arc_sets = Vec::with_capacity(states.len());
        for (_key, state) in keys.iter().zip(states.iter()) {
            let mutation_guard = match try_lock_or_none(&state.mutation_lock, "table mutation lock")
            {
                Some(guard) => guard,
                None => return Ok(None),
            };
            let registry = match state.partition_locks.try_write() {
                Ok(registry) => registry,
                Err(_) => return Ok(None),
            };
            let (_partition_keys, arcs) = sorted_partition_locks(&registry);
            mutation_guards.push(mutation_guard);
            partition_arc_sets.push(arcs);
            registries.push(registry);
        }
        let mut partition_guard_sets = Vec::with_capacity(states.len());
        for partition_locks in &partition_arc_sets {
            let mut guards = Vec::new();
            for lock in partition_locks {
                match try_lock_or_none(lock, "partition mutation lock") {
                    Some(guard) => guards.push(guard),
                    None => return Ok(None),
                }
            }
            partition_guard_sets.push(guards);
        }
        if states.iter().any(|state| state.has_sealing_segments()) {
            return Ok(None);
        }
        f().map(Some)
    }

    fn open_mutable_reader_owned(&self, path: &Path) -> Result<SegmentReader> {
        let Some(state) = self.state_for_segment_path(path) else {
            return SegmentReader::open_owned(path);
        };

        // A segment removed from the active map is sealed outside every lock:
        // its footer is published by the seal, so a reader that lands in that
        // window waits for the seal instead of failing on a torn footer. The
        // old in-lock seal blocked here on the active-segments mutex; the
        // bounded wait preserves that serialization without stalling writers.
        if state.sealing_paths().contains(path) {
            let timeout = Duration::from_secs(
                crate::common::config::storage_timeouts()
                    .mutation_lock_timeout_secs
                    .max(1),
            );
            state.wait_sealing_segments_drained(timeout)?;
        }

        let mut active_guard = state.active_segments.lock().unwrap();
        for segment in active_guard.values_mut() {
            if same_segment_path(&segment.path, path) {
                // Flush buffered rows as well, not just dirty metadata:
                // rows sitting in the writer's buffer are invisible to a
                // reader, which made DELETE skip them.
                if segment.buffered_rows > 0 {
                    segment.flush_buffer(&|path| self.invalidate_reader(path))?;
                }
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
        compression_level: i32,
    ) -> Result<()> {
        let mut writer = SegmentWriter::create(path, schema, row_group_size, compression_level)?;
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
        compression_level: i32,
    ) -> Result<()> {
        // Validate up front so a malformed batch cannot leave a half-written
        // segment behind (the merge-based version validated before creating
        // the file).
        for batch in batches {
            if batch.num_columns() != schema.num_columns() {
                return Err(TaibaiError::Storage(format!(
                    "replacement batch has {} columns, expected {}",
                    batch.num_columns(),
                    schema.num_columns()
                )));
            }
        }
        // Feed the batches to the writer one by one: `write_batch` buffers a
        // partially-filled row group across calls, so the row-group layout is
        // identical to the old merge-everything-first approach, without the
        // full-payload copy into a combined batch.
        let mut writer = SegmentWriter::create(path, schema, row_group_size, compression_level)?;
        for batch in batches {
            writer.write_batch(batch)?;
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
struct PartitionGateReset(Arc<TableSegmentState>);

impl Drop for PartitionGateReset {
    fn drop(&mut self) {
        self.0.partition_gate.store(false, Ordering::Release);
    }
}

struct DatabaseGatesReset(Vec<Arc<TableSegmentState>>);

impl Drop for DatabaseGatesReset {
    fn drop(&mut self) {
        for state in &self.0 {
            state.partition_gate.store(false, Ordering::Release);
        }
    }
}

/// Snapshot a state's partition locks as (sorted key, arc) pairs under a
/// registry write guard the caller already holds. How long that guard is
/// kept is the caller's freeze strategy: the blocking wide gate drops it
/// before running `f` (the gate flag freezes new partitions instead),
/// while the try variants keep it held through `f`.
fn sorted_partition_locks(
    registry: &std::sync::RwLockWriteGuard<
        '_,
        std::collections::HashMap<String, Arc<std::sync::Mutex<()>>>,
    >,
) -> (Vec<String>, Vec<Arc<std::sync::Mutex<()>>>) {
    let mut partition_keys: Vec<String> = registry.keys().cloned().collect();
    partition_keys.sort();
    let partition_locks = partition_keys
        .iter()
        .map(|partition| Arc::clone(&registry[partition]))
        .collect::<Vec<_>>();
    (partition_keys, partition_locks)
}

/// Try-lock for the lock-free mutation attempts: `None` means busy (the
/// caller reports "not now" and retries later); a poisoned lock still
/// panics so the failure stays loud instead of being silently skipped.
fn try_lock_or_none<'a, T>(
    lock: &'a std::sync::Mutex<T>,
    what: &str,
) -> Option<std::sync::MutexGuard<'a, T>> {
    match lock.try_lock() {
        Ok(guard) => Some(guard),
        Err(std::sync::TryLockError::WouldBlock) => None,
        Err(std::sync::TryLockError::Poisoned(poisoned)) => {
            panic!("{} poisoned: {}", what, poisoned)
        }
    }
}

fn acquire_mutation_lock<'a>(
    state: &'a TableSegmentState,
    key: &str,
) -> Result<std::sync::MutexGuard<'a, ()>> {
    let timeouts = crate::common::config::storage_timeouts();
    let timeout = Duration::from_secs(timeouts.mutation_lock_timeout_secs.max(1));
    let warn_after = Duration::from_secs(timeouts.mutation_lock_warn_secs.max(1));
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

/// Bounded acquisition of one partition mutex by a wide-gate holder. The
/// caller has already set `partition_gate`, so the gate re-check loop of
/// `acquire_partition_mutation_lock` must not be reused here (it would retry
/// against our own gate until its timeout); this variant bounds only the
/// wait. A long-held partition lock (slow partition-scoped mutation) must
/// surface as an explicit timeout instead of freezing every partition
/// writer behind the raised gate for the same duration.
fn acquire_partition_lock_for_wide_gate<'a>(
    lock: &'a std::sync::Mutex<()>,
    key: &str,
    partition_key: &str,
    timeout: Duration,
) -> Result<std::sync::MutexGuard<'a, ()>> {
    let start = Instant::now();
    let mut backoff = Duration::from_millis(1);
    loop {
        match lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                panic!(
                    "partition mutation lock poisoned on {} partition {}: {}",
                    key, partition_key, poisoned
                )
            }
        }
        if start.elapsed() >= timeout {
            return Err(TaibaiError::ResourceBusy(format!(
                "table-wide mutation gate timed out after {}ms waiting on {} partition {}; a long partition mutation is in flight",
                timeout.as_millis(),
                key,
                partition_key
            )));
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_millis(16));
    }
}

/// Clone (or lazily create) the partition's mutation lock. The Arc is owned
/// by the caller's scope so the subsequent `MutexGuard` borrows a stable
/// local instead of registry internals.
fn partition_mutation_lock_arc(
    state: &TableSegmentState,
    partition_key: &str,
) -> Arc<std::sync::Mutex<()>> {
    if let Some(lock) = state.partition_locks.read().unwrap().get(partition_key) {
        return Arc::clone(lock);
    }
    let mut registry = state.partition_locks.write().unwrap();
    Arc::clone(
        registry
            .entry(partition_key.to_string())
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(()))),
    )
}

/// Acquire a partition mutation lock with the same bounded wait, slow-lock
/// warning, and timeout error as the table-wide mutation lock, so partition
/// lock queueing is equally visible in the server log. After acquiring the
/// mutex the writer re-checks the table's wide-gate flag: a table-wide
/// mutation operation that has already snapshotted this partition holds the
/// mutex, so the writer releases and retries until the gate clears.
fn acquire_partition_mutation_lock<'a>(
    state: &TableSegmentState,
    lock: &'a std::sync::Mutex<()>,
    key: &str,
    partition_key: &str,
) -> Result<std::sync::MutexGuard<'a, ()>> {
    let timeouts = crate::common::config::storage_timeouts();
    let timeout = Duration::from_secs(timeouts.mutation_lock_timeout_secs.max(1));
    let warn_after = Duration::from_secs(timeouts.mutation_lock_warn_secs.max(1));
    let start = Instant::now();
    let mut backoff = Duration::from_millis(1);
    loop {
        match lock.try_lock() {
            Ok(guard) => {
                if !state.partition_gate.load(Ordering::Acquire) {
                    let waited = start.elapsed();
                    if waited >= warn_after {
                        tracing::warn!(
                            table = key,
                            partition = partition_key,
                            wait_ms = waited.as_millis() as u64,
                            "partition mutation lock acquisition was slow"
                        );
                    }
                    return Ok(guard);
                }
                drop(guard);
            }
            Err(std::sync::TryLockError::WouldBlock) => {}
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                panic!("partition mutation lock poisoned: {}", poisoned)
            }
        }
        if start.elapsed() >= timeout {
            return Err(TaibaiError::ResourceBusy(format!(
                "partition mutation lock timeout after {}ms waiting on {} partition {}",
                timeout.as_millis(),
                key,
                partition_key
            )));
        }
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_millis(16));
    }
}

// ---------------------------------------------------------------------------
// Segment replacement journal (`<final>.replace`)
//
// Records the old segments a `replace_segments_with_tmp` call must retire, so
// a crash between the rename and the removals cannot leave the new and old
// segments visible at the same time (double-counted rows).
// Layout: magic(8) | old_count u32 | per old: len u32 + utf8 path | crc32 u32
// ---------------------------------------------------------------------------

const REPLACE_JOURNAL_MAGIC: &[u8; 8] = b"TBREPJ01";

pub(crate) fn replace_journal_path(final_path: &Path) -> PathBuf {
    let mut name = final_path.as_os_str().to_os_string();
    name.push(".replace");
    PathBuf::from(name)
}

pub(crate) fn write_replace_journal(journal: &Path, old_segments: &[PathBuf]) -> Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(old_segments.len() as u32).to_le_bytes());
    for old in old_segments {
        let text = old.to_string_lossy();
        body.extend_from_slice(&(text.len() as u32).to_le_bytes());
        body.extend_from_slice(text.as_bytes());
    }
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&body);
    let crc = hasher.finalize();

    let tmp = {
        let mut name = journal.as_os_str().to_os_string();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    use std::io::Write as _;
    file.write_all(REPLACE_JOURNAL_MAGIC)?;
    file.write_all(&body)?;
    file.write_all(&crc.to_le_bytes())?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, journal)?;
    Ok(())
}

/// Finish or discard interrupted segment replacements at startup. With the
/// final segment present the rename had committed, so the retirement of the
/// listed old segments is completed; without it the rename never happened.
pub(crate) fn recover_replace_journals(table_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some(final_name) = name.strip_suffix(".replace") else {
            continue;
        };
        let final_path = table_dir.join(final_name);
        let Some(olds) = read_replace_journal(&path) else {
            if final_path.exists() {
                // The journal is written BEFORE the rename, so a journal whose
                // final segment exists means the replacement committed — but an
                // unreadable journal (torn write / CRC mismatch) leaves the old
                // segments it listed unretirable. Deleting the journal here
                // would turn a crash-window double-count into a permanent,
                // silent one: preserve it for inspection and keep alarming at
                // every startup instead.
                let corrupt = path.with_extension("replace.corrupt");
                let preserved = std::fs::rename(&path, &corrupt).is_ok();
                if !preserved {
                    let _ = std::fs::remove_file(&path);
                }
                tracing::error!(
                    journal = %path.display(),
                    preserved_as = %corrupt.display(),
                    preserved,
                    final_segment = %final_path.display(),
                    "replace journal is unreadable but the replacement had committed; the old \
                     segments it listed cannot be retired automatically and scans may double-count \
                     their rows until they are removed manually"
                );
            } else {
                // No final segment: the rename never committed, so discarding
                // the journal loses nothing.
                tracing::debug!(
                    journal = %path.display(),
                    "discarding journal of a replacement whose rename never committed"
                );
                let _ = std::fs::remove_file(&path);
            }
            continue;
        };
        if final_path.exists() {
            for old in &olds {
                if old != &final_path && old.exists() {
                    tracing::warn!(
                        old_segment = %old.display(),
                        "startup recovery: retiring old segment left by interrupted replacement"
                    );
                    let _ = std::fs::remove_file(old);
                    let _ = remove_sidecar_meta(old);
                }
            }
        }
        let _ = std::fs::remove_file(&path);
    }
}

fn read_replace_journal(journal: &Path) -> Option<Vec<PathBuf>> {
    let bytes = std::fs::read(journal).ok()?;
    if bytes.len() < 8 + 4 + 4 || !bytes.starts_with(REPLACE_JOURNAL_MAGIC) {
        return None;
    }
    let body = &bytes[8..bytes.len() - 4];
    let crc = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .expect("crc slice has fixed length"),
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    if hasher.finalize() != crc {
        return None;
    }
    let count = u32::from_le_bytes(body[0..4].try_into().expect("u32 slice")) as usize;
    let mut olds = Vec::with_capacity(count.min(4096));
    let mut pos = 4usize;
    for _ in 0..count {
        if pos + 4 > body.len() {
            return None;
        }
        let len = u32::from_le_bytes(body[pos..pos + 4].try_into().expect("u32 slice")) as usize;
        pos += 4;
        if pos + len > body.len() {
            return None;
        }
        olds.push(PathBuf::from(
            String::from_utf8_lossy(&body[pos..pos + len]).into_owned(),
        ));
        pos += len;
    }
    Some(olds)
}

/// Test-only re-exports of the replacement journal internals.
#[cfg(test)]
pub mod manager_core_test_support {
    pub(crate) use super::{
        recover_replace_journals as recover_replace_journals_for_test,
        replace_journal_path as replace_journal_path_for_test,
        write_replace_journal as write_replace_journal_for_test,
    };
}

/// `<data>/<db>/<table>/<segment>` path -> `(db, table)`.
fn db_table_from_segment_path(path: &Path) -> Option<(String, String)> {
    let table = path.parent()?.file_name()?.to_string_lossy().to_string();
    let db = path
        .parent()?
        .parent()?
        .file_name()?
        .to_string_lossy()
        .to_string();
    Some((db, table))
}use crate::storage::segment::reader::FooterRewriteMode;

