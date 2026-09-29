# -*- coding: utf-8 -*-
"""R2.2: primary emission points + engine reload helpers."""

# ---- 1. auth.rs reload ----
p = 'src/storage/catalog/auth.rs'
data = open(p, 'rb').read()

def rep(old, new, count=1):
    global data
    old = old.encode('utf-8').replace(b"\n", b"\r\n")
    new = new.encode('utf-8').replace(b"\n", b"\r\n")
    assert data.count(old) == count, (old[:70], data.count(old), data.count(old))
    data = data.replace(old, new)

rep(
    """    pub fn create_user(&self, name: &str, password: &str) -> Result<()> {""",
    """    /// Standby replication apply: re-read the auth store from a snapshot
    /// that the applier already wrote to `path`.
    pub fn reload(&self) -> Result<()> {
        let store = AuthStore::load_or_create(&self.path)?;
        *self.store.write().unwrap() = store;
        Ok(())
    }

    pub fn create_user(&self, name: &str, password: &str) -> Result<()> {""",
)
open(p, 'wb').write(data)
print("auth reload added")

# ---- 2. engine/runtime.rs: reload_catalog / remove_catalog ----
p = 'src/sql/execution/engine/runtime.rs'
data = open(p, 'rb').read()
rep(
    """    pub fn commit_session_txn(&self, session: &mut Session) -> Result<()> {""",
    """    /// Standby replication apply: replace an in-memory catalog from its
    /// on-disk JSON (the snapshot was written by the applier).
    pub fn reload_catalog(&self, db: &str) -> Result<()> {
        let meta_dir = self.data_dir.join("meta");
        let catalog = crate::storage::catalog::schema::DatabaseCatalog::load_or_create(
            &meta_dir, db,
        )?;
        self.catalogs.write().unwrap().insert(db.to_string(), catalog);
        Ok(())
    }

    /// Standby replication apply: DROP DATABASE.
    pub fn remove_catalog(&self, db: &str) {
        self.catalogs.write().unwrap().remove(db);
    }

    pub fn commit_session_txn(&self, session: &mut Session) -> Result<()> {""",
)
open(p, 'wb').write(data)
print("engine reload helpers added")

# ---- 3. manager_core: rewrite_delete_footer emission + replace emission + db_table helper (LF) ----
p = 'src/storage/segment_manager/manager_core.rs'
data = open(p, 'rb').read()

def rep_lf(old, new, count=1):
    global data
    old = old.encode('utf-8'); new = new.encode('utf-8')
    assert data.count(old) == count, (old[:70], data.count(old), data.count(old))
    data = data.replace(old, new)

# wrap rewrite_delete_footer with emission
rep_lf(
    """    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        let mutable = self.is_mutable_segment_path(path);""",
    """    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        let result = self.rewrite_delete_footer_inner(path, bitmap, deleted_count);
        if result.is_ok() {
            self.emit_delete_footer_rewrite(path, bitmap, deleted_count);
        }
        result
    }

    fn rewrite_delete_footer_inner(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        let mutable = self.is_mutable_segment_path(path);""")

# replace_segments_with_tmp emission before final Ok
rep_lf(
    """        remove_sidecar_meta(tmp_path)?;
        let _ = self.open_reader_cached(final_path);
        Ok(())
    }""",
    """        remove_sidecar_meta(tmp_path)?;
        let _ = self.open_reader_cached(final_path);
        self.emit_segments_replaced(final_path, old_segments);
        Ok(())
    }""")

# helpers after active_segment_name_for
rep_lf(
    """    /// Current active segment file name for `(db, table, partition)`.""",
    """    /// Emit a `DeleteFooterRewrite` (derive db/table from the segment path).
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
        log.enqueue(crate::storage::replication::ReplicationEvent::DeleteFooterRewrite {
            db,
            table,
            segment: segment.to_string(),
            bitmap: bitmap.to_vec(),
            deleted_count,
        });
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
        log.enqueue(crate::storage::replication::ReplicationEvent::SegmentsReplaced {
            db,
            table,
            new_segments: vec![new_name.to_string()],
            retired,
        });
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
        log.enqueue(crate::storage::replication::ReplicationEvent::SegmentsRemoved {
            db: db.to_string(),
            table: table.to_string(),
            segments: segments.to_vec(),
        });
    }

    /// Current active segment file name for `(db, table, partition)`.""")
open(p, 'wb').write(data)
print("manager_core emissions added")

# db_table_from_segment_path helper + TTL emit (ttl_purge calls mgr helper)
data = open(p, 'rb').read()
old = b"/// `<data>/<db>/<table>` tail dir -> `(db, table)`."
if data.count(old) == 0:
    old = old.replace(b"\r\n", b"\n")
if data.count(old) != 1:
    # helper lives in durable_tail; add local one in manager_core
    anchor = b"    /// Current active segment file name for `(db, table, partition)`."
    new = b"""    /// Current active segment file name for `(db, table, partition)`."""
    # add free function at end of file scope is fine via include!; append before final newline
    helper = b"""
/// `<data>/<db>/<table>/<segment>` path -> `(db, table)`.
fn db_table_from_segment_path(path: &Path) -> Option<(String, String)> {
    let table = path.parent()?.file_name()?.to_string_lossy().to_string();
    let db = path
        .parent()?
        .parent()?
        .file_name()?
        .to_string_lossy()
        .to_string();
    Some((db, table))}
"""
    data = data + helper
    open(p, 'wb').write(data)
    print("db_table helper appended")
else:
    print("helper anchor elsewhere (durable_tail) - local appended anyway")
    data = data + b"""
/// `<data>/<db>/<table>/<segment>` path -> `(db, table)`.
fn db_table_from_segment_path(path: &Path) -> Option<(String, String)> {
    let table = path.parent()?.file_name()?.to_string_lossy().to_string();
    let db = path
        .parent()?
        .parent()?
        .file_name()?
        .to_string_lossy()
        .to_string();
    Some((db, table))}
"""
    open(p, 'wb').write(data)
