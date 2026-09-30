use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Instant;
use uuid::Uuid;

use crate::common::error::Result;
use crate::types::batch::{RecordBatch, Schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemTableState {
    Pending,
    Flushing,
    Flushed,
    /// Persisted to a segment but kept resident for same-day queries. Unlike
    /// `Flushed`, an `Archived` memtable is NOT removed by `remove_flushed()`;
    /// it stays readable until an explicit close-of-day clears it. This realises
    /// the design's "checkpoint flush copies (does not move) and keeps the data
    /// resident" semantics for tables that are NOT accelerated by a hot store.
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnVisibility {
    Uncommitted(u64),
    Committed,
    Aborted,
}

/// Where a pending memtable's rows live. Committed rows are always durable in
/// the memtable's tail file, so the resident batch is only a query
/// acceleration copy: spilling drops it and re-points the memtable at its
/// tail, from which the rows are lazily re-materialized on first access.
enum MemTableBacking {
    Resident,
    Spilled { tail_path: PathBuf, rows: u64 },
}

pub struct MemTable {
    id: Uuid,
    db: String,
    table: String,
    partition_key: String,
    schema: Schema,
    batch: RwLock<Option<Arc<RecordBatch>>>,
    backing: RwLock<MemTableBacking>,
    /// Tail file backing a spilled memtable (set by `MemTable::spilled`).
    /// Kept outside `backing` so a resident memtable can be demoted back to
    /// spilled when replication appends more frames to its tail.
    tail_path: Mutex<Option<PathBuf>>,
    state: AtomicU8,
    row_count: AtomicU64,
    created_at: Instant,
    txn_id: AtomicU64,
    visibility: RwLock<TxnVisibility>,
    /// Bumped whenever the resident batch is wholesale replaced (REPLACE
    /// conflict filtering, rollback restores). Appends only extend the batch
    /// tail, so REPLACE conflict key indexes can refresh incrementally on a
    /// stable version and rebuild on a bump.
    content_version: AtomicU64,
    /// Set when this memtable's durable-tail commit failed: the tail file
    /// (if any was created) may be torn, so the next flush must take the
    /// segment protocol instead of trusting the tail's presence on disk.
    needs_segment_flush: AtomicBool,
}

impl MemTable {
    pub fn new(db: String, table: String, partition_key: String, schema: Schema) -> Self {
        Self {
            id: Uuid::new_v4(),
            db,
            table,
            partition_key,
            batch: RwLock::new(Some(Arc::new(RecordBatch::new(schema.clone())))),
            schema,
            backing: RwLock::new(MemTableBacking::Resident),
            tail_path: Mutex::new(None),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(0),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(0),
            visibility: RwLock::new(TxnVisibility::Committed),
            content_version: AtomicU64::new(0),
            needs_segment_flush: AtomicBool::new(false),
        }
    }

    pub fn with_txn(
        db: String,
        table: String,
        partition_key: String,
        schema: Schema,
        txn_id: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            db,
            table,
            partition_key,
            batch: RwLock::new(Some(Arc::new(RecordBatch::new(schema.clone())))),
            schema,
            backing: RwLock::new(MemTableBacking::Resident),
            tail_path: Mutex::new(None),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(0),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(txn_id),
            visibility: RwLock::new(TxnVisibility::Uncommitted(txn_id)),
            content_version: AtomicU64::new(0),
            needs_segment_flush: AtomicBool::new(false),
        }
    }

    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn state(&self) -> MemTableState {
        match self.state.load(Ordering::Acquire) {
            0 => MemTableState::Pending,
            1 => MemTableState::Flushing,
            2 => MemTableState::Flushed,
            3 => MemTableState::Archived,
            _ => MemTableState::Pending,
        }
    }

    pub fn set_state(&self, new_state: MemTableState) {
        self.state.store(new_state as u8, Ordering::Release);
    }

    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Acquire)
    }

    /// Standby replication apply: the tail now holds `total_rows` rows.
    /// A spilled memtable just updates its count; a RESIDENT one (already
    /// materialized before the append) is demoted back to spilled so the
    /// next access re-materializes the whole tail - otherwise the newer
    /// frames would stay invisible.
    pub fn set_tail_rows(&self, total_rows: u64) {
        let mut guard = self.backing.write().unwrap();
        match &mut *guard {
            MemTableBacking::Spilled { rows, .. } => {
                *rows = total_rows;
                drop(guard);
                self.row_count.store(total_rows, Ordering::Release);
            }
            MemTableBacking::Resident => {
                let tail_path = self.tail_path.lock().unwrap().clone();
                drop(guard);
                if let Some(tail_path) = tail_path {
                    let mut guard = self.backing.write().unwrap();
                    *guard = MemTableBacking::Spilled {
                        tail_path,
                        rows: total_rows,
                    };
                    self.batch.write().unwrap().take();
                    self.row_count.store(total_rows, Ordering::Release);
                }
            }
        }
    }

    /// Whether this memtable's tail commit failed and its next flush must
    /// take the segment protocol (see `needs_segment_flush`).
    pub fn needs_segment_flush(&self) -> bool {
        self.needs_segment_flush.load(Ordering::Acquire)
    }

    pub fn set_needs_segment_flush(&self, value: bool) {
        self.needs_segment_flush.store(value, Ordering::Release);
    }

    pub fn txn_id(&self) -> u64 {
        self.txn_id.load(Ordering::Acquire)
    }

    pub fn is_transactional(&self) -> bool {
        self.txn_id() != 0
    }

    pub fn visibility(&self) -> TxnVisibility {
        let guard = self.visibility.read().unwrap();
        *guard
    }

    pub fn mark_committed(&self) {
        let mut guard = self.visibility.write().unwrap();
        *guard = TxnVisibility::Committed;
        self.txn_id.store(0, Ordering::Release);
    }

    pub fn mark_aborted(&self) {
        let mut guard = self.visibility.write().unwrap();
        *guard = TxnVisibility::Aborted;
    }

    pub fn is_visible_to(&self, reader_txn_id: u64) -> bool {
        match self.visibility() {
            TxnVisibility::Committed => true,
            TxnVisibility::Aborted => false,
            TxnVisibility::Uncommitted(writer_txn_id) => writer_txn_id == reader_txn_id,
        }
    }

    pub fn append(&self, batch: &RecordBatch) -> Result<u64> {
        let added = batch.num_rows() as u64;
        if added == 0 {
            return Ok(0);
        }
        self.ensure_resident()?;
        let mut guard = self.batch.write().unwrap();
        match guard.as_mut() {
            Some(existing) => {
                let existing = Arc::make_mut(existing);
                for col_idx in 0..batch.num_columns() {
                    if let Some(src) = batch.column(col_idx) {
                        if let Some(dst) = existing.column_mut(col_idx) {
                            dst.extend(src)?;
                        }
                    }
                }
            }
            None => {
                *guard = Some(Arc::new(batch.clone()));
            }
        }
        self.row_count.fetch_add(added, Ordering::Release);
        Ok(added)
    }

    pub fn append_owned(&self, batch: RecordBatch) -> Result<u64> {
        let added = batch.num_rows() as u64;
        if added == 0 {
            return Ok(0);
        }
        self.ensure_resident()?;
        let mut guard = self.batch.write().unwrap();
        match guard.as_mut() {
            Some(existing) if existing.num_rows() > 0 => {
                let existing = Arc::make_mut(existing);
                if existing.num_columns() != batch.num_columns() {
                    return Err(crate::common::error::TaibaiError::Internal(format!(
                        "memtable append column count mismatch: {} != {}",
                        existing.num_columns(),
                        batch.num_columns()
                    )));
                }
                if existing
                    .columns
                    .iter()
                    .zip(&batch.columns)
                    .any(|(dst, src)| dst.data_type() != src.data_type())
                {
                    return Err(crate::common::error::TaibaiError::Internal(
                        "memtable append column type mismatch".into(),
                    ));
                }
                for (dst, src) in existing.columns.iter_mut().zip(batch.columns) {
                    // Consume the incoming buffers. Variable-width payloads use
                    // Vec::append instead of cloning one value at a time; fixed
                    // width columns remain one contiguous bulk copy.
                    dst.append_owned(src)?;
                }
            }
            _ => {
                *guard = Some(Arc::new(batch));
            }
        }
        self.row_count.fetch_add(added, Ordering::Release);
        Ok(added)
    }

    pub fn read(&self) -> Option<RecordBatch> {
        if self.ensure_resident().is_err() {
            return None;
        }
        let guard = self.batch.read().unwrap();
        guard.as_ref().map(|batch| batch.as_ref().clone())
    }

    pub fn read_with<R>(&self, f: impl FnOnce(&RecordBatch) -> R) -> Option<R> {
        if self.ensure_resident().is_err() {
            return None;
        }
        let guard = self.batch.read().unwrap();
        guard.as_ref().map(|batch| f(batch.as_ref()))
    }

    pub fn take_batch(&self) -> Option<RecordBatch> {
        if self.ensure_resident().is_err() {
            return None;
        }
        let mut guard = self.batch.write().unwrap();
        guard
            .take()
            .map(|batch| Arc::try_unwrap(batch).unwrap_or_else(|shared| shared.as_ref().clone()))
    }

    /// Empty the memtable without materializing the batch. Unlike
    /// `take_batch()`, this drops the shared `Arc` instead of deep-copying
    /// the data when another holder (e.g. an in-flight pubsub event) still
    /// references it. Use at every discard-only call site.
    pub fn clear_batch(&self) {
        let mut guard = self.batch.write().unwrap();
        guard.take();
    }

    /// Copy the current batch without moving it out. Used by the checkpoint
    /// flush path so the data stays resident (then the memtable is marked
    /// `Archived`), in contrast to `take_batch()` which empties the memtable.
    pub fn read_snapshot(&self) -> Option<RecordBatch> {
        if self.ensure_resident().is_err() {
            return None;
        }
        let guard = self.batch.read().unwrap();
        guard.as_ref().map(|batch| batch.as_ref().clone())
    }

    /// Share the current batch without copying it. The pubsub commit hook
    /// uses this so change notifications cost an `Arc` clone instead of a
    /// full column copy on the transaction-commit path.
    pub fn read_snapshot_arc(&self) -> Option<Arc<RecordBatch>> {
        if self.ensure_resident().is_err() {
            return None;
        }
        self.batch.read().unwrap().clone()
    }

    pub fn is_spilled(&self) -> bool {
        matches!(
            *self.backing.read().unwrap(),
            MemTableBacking::Spilled { .. }
        )
    }

    /// Resident bytes owned by this memtable; a spilled memtable keeps only
    /// metadata in memory.
    pub fn estimated_memory_bytes(&self) -> u64 {
        if self.is_spilled() {
            return 0;
        }
        self.batch
            .read()
            .unwrap()
            .as_ref()
            .map(|batch| batch.estimated_size_bytes() as u64)
            .unwrap_or(0)
    }

    /// Drop the resident batch and re-point this memtable at its durable
    /// tail. The caller must have verified (under the table mutation lock,
    /// after a tail barrier) that `tail_path` holds exactly `row_count()`
    /// committed rows. Returns `false` without changing anything when the
    /// memtable is no longer pending or has no resident rows.
    pub fn spill_to_tail(&self, tail_path: PathBuf) -> bool {
        if self.state() != MemTableState::Pending {
            return false;
        }
        let mut batch_guard = self.batch.write().unwrap();
        let mut backing_guard = self.backing.write().unwrap();
        if self.state() != MemTableState::Pending {
            return false;
        }
        if matches!(*backing_guard, MemTableBacking::Spilled { .. }) {
            return false;
        }
        let Some(batch) = batch_guard.as_ref() else {
            return false;
        };
        let rows = batch.num_rows() as u64;
        if rows == 0 {
            return false;
        }
        *batch_guard = None;
        *backing_guard = MemTableBacking::Spilled { tail_path, rows };
        self.row_count.store(rows, Ordering::Release);
        true
    }

    /// Materialize a spilled memtable from its durable tail. Lock order is
    /// always `batch` then `backing`; the decode happens outside both locks.
    fn ensure_resident(&self) -> Result<()> {
        let tail_path = {
            let guard = self.backing.read().unwrap();
            match &*guard {
                MemTableBacking::Resident => return Ok(()),
                MemTableBacking::Spilled { tail_path, .. } => tail_path.clone(),
            }
        };
        let decoded =
            crate::storage::core::durable_tail::decode_tail_file(&tail_path, &self.schema);
        let mut batch_guard = self.batch.write().unwrap();
        let mut backing_guard = self.backing.write().unwrap();
        let expected_rows = match &*backing_guard {
            MemTableBacking::Resident => return Ok(()),
            MemTableBacking::Spilled { rows, .. } => *rows,
        };
        let (_, batch) = decoded.map_err(|error| {
            tracing::error!(
                error = %error,
                path = %tail_path.display(),
                db = %self.db,
                table = %self.table,
                "failed to materialize spilled memtable tail"
            );
            error
        })?;
        let decoded_rows = batch.num_rows() as u64;
        if decoded_rows != expected_rows {
            tracing::error!(
                expected_rows,
                decoded_rows,
                path = %tail_path.display(),
                db = %self.db,
                table = %self.table,
                "spilled memtable tail row count mismatch; trusting the tail"
            );
            self.row_count.store(decoded_rows, Ordering::Release);
        }
        *batch_guard = Some(Arc::new(batch));
        *backing_guard = MemTableBacking::Resident;
        Ok(())
    }

    pub fn restore_batch(&self, batch: RecordBatch) {
        let rows = batch.num_rows() as u64;
        let mut guard = self.batch.write().unwrap();
        let mut backing_guard = self.backing.write().unwrap();
        *guard = Some(Arc::new(batch));
        *backing_guard = MemTableBacking::Resident;
        self.row_count.store(rows, Ordering::Release);
        self.content_version.fetch_add(1, Ordering::Release);
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn recovered(
        id: Uuid,
        db: String,
        table: String,
        partition_key: String,
        batch: RecordBatch,
    ) -> Self {
        let rows = batch.num_rows() as u64;
        Self {
            id,
            db,
            table,
            partition_key,
            schema: batch.schema.clone(),
            batch: RwLock::new(Some(Arc::new(batch))),
            backing: RwLock::new(MemTableBacking::Resident),
            tail_path: Mutex::new(None),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(rows),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(0),
            visibility: RwLock::new(TxnVisibility::Committed),
            content_version: AtomicU64::new(0),
            needs_segment_flush: AtomicBool::new(false),
        }
    }

    /// A recovered or spilled memtable whose rows live only in its durable
    /// tail; the batch is materialized lazily on first access.
    pub fn spilled(
        id: Uuid,
        db: String,
        table: String,
        partition_key: String,
        schema: Schema,
        rows: u64,
        tail_path: PathBuf,
    ) -> Self {
        Self {
            id,
            db,
            table,
            partition_key,
            schema,
            batch: RwLock::new(None),
            backing: RwLock::new(MemTableBacking::Spilled {
                tail_path: tail_path.clone(),
                rows,
            }),
            tail_path: Mutex::new(Some(tail_path)),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(rows),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(0),
            visibility: RwLock::new(TxnVisibility::Committed),
            content_version: AtomicU64::new(0),
            needs_segment_flush: AtomicBool::new(false),
        }
    }

    /// Replace a live memtable while the caller holds the table mutation lock.
    /// Returns `false` if a concurrent flush has already moved the memtable out
    /// of the pending state.
    pub fn replace_pending_batch(&self, batch: Arc<RecordBatch>) -> bool {
        if self.state() != MemTableState::Pending {
            return false;
        }
        if self.ensure_resident().is_err() {
            return false;
        }
        let rows = batch.num_rows() as u64;
        let mut guard = self.batch.write().unwrap();
        if self.state() != MemTableState::Pending {
            return false;
        }
        // Store the caller's Arc directly: undo backups and updated batches
        // arrive pre-shared, so the swap costs no deep copy.
        *guard = Some(batch);
        self.row_count.store(rows, Ordering::Release);
        self.content_version.fetch_add(1, Ordering::Release);
        true
    }

    /// Monotonic content version. Stable across tail-only appends; bumped when
    /// the batch is wholesale replaced.
    pub fn content_version(&self) -> u64 {
        self.content_version.load(Ordering::Acquire)
    }

    pub fn schema(&self) -> Option<Schema> {
        Some(self.schema.clone())
    }

    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }
}

/// Lazily built per-memtable key index for REPLACE conflict matching: maps
/// each constraint key to the resident row positions holding it. Refreshed
/// incrementally while the memtable only appends; rebuilt when the batch is
/// wholesale replaced (content version bump); compacted in place when REPLACE
/// conflict filtering removes rows. Probing with this index replaces the
/// per-statement full scan of every committed memtable.
pub struct ReplaceKeyIndex {
    signature: Vec<Vec<usize>>,
    content_version: u64,
    rows_indexed: u64,
    per_constraint:
        Vec<std::collections::HashMap<Vec<crate::types::scalar::ScalarValue>, Vec<u32>>>,
}

fn replace_key_positions_for_rows(
    batch: &RecordBatch,
    constraints: &[Vec<usize>],
    start_row: usize,
    maps: &mut [std::collections::HashMap<Vec<crate::types::scalar::ScalarValue>, Vec<u32>>],
) {
    for row_idx in start_row..batch.num_rows() {
        for (constraint_idx, indices) in constraints.iter().enumerate() {
            let mut key = Vec::with_capacity(indices.len());
            let mut valid = true;
            for &col_idx in indices {
                match batch.scalar(row_idx, col_idx) {
                    Some(value) if !value.is_null() => key.push(value),
                    _ => {
                        valid = false;
                        break;
                    }
                }
            }
            if valid {
                maps[constraint_idx]
                    .entry(key)
                    .or_default()
                    .push(row_idx as u32);
            }
        }
    }
}

type WritableTableKey = (String, String, String, u64);

pub struct MemTableManager {
    memtables: RwLock<Vec<Arc<MemTable>>>,
    writable: RwLock<HashMap<WritableTableKey, Weak<MemTable>>>,
    max_rows_per_memtable: u64,
    max_age_secs: u64,
    low_rate_max_rows: u64,
    low_rate_max_age_secs: u64,
    replace_key_indexes: RwLock<HashMap<Uuid, ReplaceKeyIndex>>,
    /// Rows per `(db, table, partition)` that reached segments (or will)
    /// without being mirrored into the hot store: non-fed auto-commit
    /// appends, committed transaction deltas, bulk/relaxed bypass writes.
    /// Zero/absent means every segment row of the partition is provably
    /// mirrored, which is what lets a resident-partition scan answer from
    /// the hot store alone even after the memtable has been flushed.
    hot_unfed_rows: Mutex<HashMap<(String, String, HotUnfedScope), u64>>,
    /// Deferred-DML registry (see `txn_mutations`). Set once by the engine
    /// after construction; flush selection consults it to skip memtables
    /// pinned by live transactions.
    txn_mutations: RwLock<Option<Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>>>,
}

/// What one hot-unfed accounting entry scopes to. `Table` marks bypasses
/// whose partitions could not be derived (e.g. raw bulk payloads) and
/// disables the resident fast path for the whole table; `Partition`
/// disables it for one partition only. (The previous encoding used a
/// NUL-prefixed partition string as the table marker, which made the file
/// read as binary to tooling.)
#[derive(Clone, PartialEq, Eq, Hash)]
enum HotUnfedScope {
    Table,
    Partition(String),
}

impl MemTableManager {
    pub fn new(
        max_rows_per_memtable: u64,
        max_age_secs: u64,
        low_rate_max_rows: u64,
        low_rate_max_age_secs: u64,
    ) -> Self {
        Self {
            memtables: RwLock::new(Vec::new()),
            writable: RwLock::new(HashMap::new()),
            max_rows_per_memtable,
            max_age_secs,
            low_rate_max_rows,
            low_rate_max_age_secs: low_rate_max_age_secs.max(max_age_secs),
            replace_key_indexes: RwLock::new(HashMap::new()),
            hot_unfed_rows: Mutex::new(HashMap::new()),
            txn_mutations: RwLock::new(None),
        }
    }

    pub fn set_txn_mutations(
        &self,
        registry: Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>,
    ) {
        *self.txn_mutations.write().unwrap() = Some(registry);
    }

    pub fn txn_mutations(
        &self,
    ) -> Option<Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>> {
        self.txn_mutations.read().unwrap().clone()
    }

    /// Standby replication: refresh a tail-backed memtable's total row count
    /// after tail frames were appended (demotes a resident copy if needed).
    pub fn set_tail_rows(&self, id: Uuid, rows: u64) {
        let memtables = self.memtables.read().unwrap();
        if let Some(mt) = memtables.iter().find(|mt| mt.id() == id) {
            mt.set_tail_rows(rows);
        }
    }

    /// Standby replication: the primary's segment now durably holds these
    /// memtables' rows; retire the local spilled copies.
    pub fn retire_replicated_memtables(&self, ids: &[Uuid]) {
        let memtables = self.memtables.write().unwrap();
        for mt in memtables.iter() {
            if ids.contains(&mt.id()) {
                mt.set_state(MemTableState::Flushed);
                mt.clear_batch();
            }
        }
    }

    /// Record rows appended through a path that skipped the hot-store mirror.
    pub fn note_hot_unfed_rows(&self, db: &str, table: &str, partition: &str, rows: u64) {
        if rows == 0 {
            return;
        }
        let mut unfed = self.hot_unfed_rows.lock().unwrap();
        *unfed
            .entry((
                db.to_string(),
                table.to_string(),
                HotUnfedScope::Partition(partition.to_string()),
            ))
            .or_insert(0) += rows;
    }

    /// Zero means every segment row of the partition is provably mirrored in
    /// the hot store, so a resident-partition scan may answer from the store
    /// alone. See [`HotUnfedScope`] for the table-level bypass marker.
    pub fn hot_unfed_rows(&self, db: &str, table: &str, partition: &str) -> u64 {
        let unfed = self.hot_unfed_rows.lock().unwrap();
        unfed
            .get(&(
                db.to_string(),
                table.to_string(),
                HotUnfedScope::Partition(partition.to_string()),
            ))
            .copied()
            .unwrap_or(0)
    }

    /// Table-level bypass marker: partitions cannot be derived for the rows
    /// (e.g. raw bulk payloads), so the whole table loses the resident fast
    /// path. Sticky until the table's hot store is invalidated.
    pub fn note_hot_unfed_table(&self, db: &str, table: &str, rows: u64) {
        if rows == 0 {
            return;
        }
        let mut unfed = self.hot_unfed_rows.lock().unwrap();
        *unfed
            .entry((db.to_string(), table.to_string(), HotUnfedScope::Table))
            .or_insert(0) += rows;
    }

    /// True when a resident-partition scan may answer from the hot store
    /// alone: neither the table-level sentinel nor the partition carries
    /// unmirrored rows.
    pub fn hot_partition_mirrored(&self, db: &str, table: &str, partition: &str) -> bool {
        let unfed = self.hot_unfed_rows.lock().unwrap();
        if unfed
            .get(&(db.to_string(), table.to_string(), HotUnfedScope::Table))
            .copied()
            .unwrap_or(0)
            > 0
        {
            return false;
        }
        unfed
            .get(&(
                db.to_string(),
                table.to_string(),
                HotUnfedScope::Partition(partition.to_string()),
            ))
            .copied()
            .unwrap_or(0)
            == 0
    }

    /// True when no write of the table has ever bypassed the hot-store
    /// mirror, so the hot-store watermark shortcut over live memtables is
    /// exact (every live row is also a hot row above the watermark).
    pub fn hot_table_fully_mirrored(&self, db: &str, table: &str) -> bool {
        let unfed = self.hot_unfed_rows.lock().unwrap();
        !unfed
            .keys()
            .any(|(k_db, k_table, _)| k_db == db && k_table == table)
    }

    /// Flip every accounted partition of the table to unmirrored (hot-store
    /// prefix reclamation: rows below the new front are gone from the store).
    pub fn mark_hot_unfed_all_partitions(&self, db: &str, table: &str) {
        let mut unfed = self.hot_unfed_rows.lock().unwrap();
        for (key, value) in unfed.iter_mut() {
            if key.0 == db && key.1 == table && *value == 0 {
                *value = 1;
            }
        }
    }

    pub fn clear_hot_unfed(&self, db: &str, table: &str) {
        let mut unfed = self.hot_unfed_rows.lock().unwrap();
        unfed.retain(|(k_db, k_table, _), _| k_db != db || k_table != table);
    }

    pub fn restore_committed_tail(
        &self,
        id: Uuid,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Arc<MemTable> {
        let mut memtables = self.memtables.write().unwrap();
        if let Some(existing) = memtables.iter().find(|memtable| memtable.id() == id) {
            return Arc::clone(existing);
        }
        let memtable = Arc::new(MemTable::recovered(
            id,
            db.to_string(),
            table.to_string(),
            partition_key.to_string(),
            batch,
        ));
        memtables.push(Arc::clone(&memtable));
        memtable
    }

    /// Register a recovered tail without decoding it: the memtable starts
    /// spilled and materializes from `tail_path` on first access.
    #[allow(clippy::too_many_arguments)]
    pub fn restore_spilled_tail(
        &self,
        id: Uuid,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: Schema,
        rows: u64,
        tail_path: std::path::PathBuf,
    ) -> Arc<MemTable> {
        let mut memtables = self.memtables.write().unwrap();
        if let Some(existing) = memtables.iter().find(|memtable| memtable.id() == id) {
            return Arc::clone(existing);
        }
        let memtable = Arc::new(MemTable::spilled(
            id,
            db.to_string(),
            table.to_string(),
            partition_key.to_string(),
            schema,
            rows,
            tail_path,
        ));
        memtables.push(Arc::clone(&memtable));
        memtable
    }

    /// Pending, committed, resident memtables old enough to spill. The caller
    /// still verifies the durable tail row count before spilling each one.
    pub fn spill_candidates(&self, min_age_secs: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.state() == MemTableState::Pending
                    && !mt.is_transactional()
                    && mt.visibility() == TxnVisibility::Committed
                    && !mt.is_spilled()
                    && mt.row_count() > 0
                    && mt.age_secs() >= min_age_secs
            })
            .cloned()
            .collect()
    }

    pub fn get_or_create(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: &Schema,
    ) -> Arc<MemTable> {
        self.get_or_create_with_txn(db, table, partition_key, schema, 0)
    }

    pub fn get_or_create_with_txn(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: &Schema,
        txn_id: u64,
    ) -> Arc<MemTable> {
        let key = (
            db.to_string(),
            table.to_string(),
            partition_key.to_string(),
            txn_id,
        );
        if let Some(memtable) = self
            .writable
            .read()
            .unwrap()
            .get(&key)
            .and_then(Weak::upgrade)
        {
            if memtable.state() == MemTableState::Pending
                && memtable.txn_id() == txn_id
                && memtable.row_count() < self.max_rows_per_memtable
            {
                return memtable;
            }
        }

        let mut writable = self.writable.write().unwrap();
        // Dead entries for this key cannot linger: every retirement point
        // (`remove_flushed`, `clear_table`, `clear_partition`,
        // `rename_table`, `rollback_txn`) removes the writable entries of
        // the memtables it retires, and a retired key's entry is only ever
        // re-created by the insert below.
        if let Some(memtable) = writable.get(&key).and_then(Weak::upgrade) {
            if memtable.state() == MemTableState::Pending
                && memtable.txn_id() == txn_id
                && memtable.row_count() < self.max_rows_per_memtable
            {
                return memtable;
            }
        }
        let memtable = if txn_id > 0 {
            Arc::new(MemTable::with_txn(
                db.to_string(),
                table.to_string(),
                partition_key.to_string(),
                schema.clone(),
                txn_id,
            ))
        } else {
            Arc::new(MemTable::new(
                db.to_string(),
                table.to_string(),
                partition_key.to_string(),
                schema.clone(),
            ))
        };
        self.memtables.write().unwrap().push(Arc::clone(&memtable));
        writable.insert(key, Arc::downgrade(&memtable));
        memtable
    }

    pub fn get_by_table(&self, db: &str, table: &str) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db && mt.table() == table && mt.state() == MemTableState::Pending
            })
            .cloned()
            .collect()
    }

    /// Refresh the per-memtable REPLACE key index against `batch` and return
    /// the ascending positions of rows conflicting with any key set. Combined
    /// under one lock so a probe can never observe a partially refreshed
    /// index. The caller holds the table mutation lock and the batch read
    /// lock, so appends cannot interleave.
    pub fn replace_key_conflicts(
        &self,
        memtable: &MemTable,
        key_sets: &[(
            Vec<usize>,
            std::collections::HashSet<Vec<crate::types::scalar::ScalarValue>>,
        )],
        batch: &RecordBatch,
    ) -> Vec<u32> {
        let signature: Vec<Vec<usize>> = key_sets
            .iter()
            .map(|(indices, _)| indices.clone())
            .collect();
        let version = memtable.content_version();
        let rows = batch.num_rows() as u64;
        let mut indexes = self.replace_key_indexes.write().unwrap();
        let entry = indexes
            .entry(memtable.id())
            .or_insert_with(|| ReplaceKeyIndex {
                signature: signature.clone(),
                content_version: version,
                rows_indexed: 0,
                per_constraint: signature.iter().map(|_| HashMap::new()).collect(),
            });
        if entry.signature != signature
            || entry.content_version != version
            || entry.rows_indexed > rows
        {
            entry.signature = signature.clone();
            entry.content_version = version;
            entry.per_constraint = signature.iter().map(|_| HashMap::new()).collect();
            entry.rows_indexed = 0;
        }
        if entry.rows_indexed < rows {
            replace_key_positions_for_rows(
                batch,
                &signature,
                entry.rows_indexed as usize,
                &mut entry.per_constraint,
            );
            entry.rows_indexed = rows;
        }
        let mut matched: Vec<u32> = Vec::new();
        for (indices, keys) in key_sets {
            let Some(constraint_idx) = entry.signature.iter().position(|sig| sig == indices) else {
                continue;
            };
            let Some(map) = entry.per_constraint.get(constraint_idx) else {
                continue;
            };
            for key in keys {
                if let Some(positions) = map.get(key) {
                    matched.extend_from_slice(positions);
                }
            }
        }
        matched.sort_unstable();
        matched.dedup();
        matched
    }

    /// Compact index positions after REPLACE conflict filtering removed
    /// `removed_positions` (ascending) from a batch of `old_row_count` rows.
    /// Verifies the memtable's post-filter state; drops the cached entry on
    /// any mismatch so the next probe rebuilds it.
    pub fn compact_replace_key_index(
        &self,
        memtable: &MemTable,
        removed_positions: &[u32],
        old_row_count: usize,
    ) {
        let mut indexes = self.replace_key_indexes.write().unwrap();
        let Some(entry) = indexes.get_mut(&memtable.id()) else {
            return;
        };
        let expected_rows = (old_row_count - removed_positions.len()) as u64;
        if entry.content_version.wrapping_add(1) != memtable.content_version()
            || memtable.row_count() != expected_rows
        {
            indexes.remove(&memtable.id());
            return;
        }
        let mut new_position = vec![u32::MAX; old_row_count];
        let mut removed_so_far = 0u32;
        let mut removed_iter = removed_positions.iter().copied();
        let mut next_removed = removed_iter.next();
        for (pos, slot) in new_position.iter_mut().enumerate() {
            if next_removed == Some(pos as u32) {
                removed_so_far += 1;
                next_removed = removed_iter.next();
            } else {
                *slot = pos as u32 - removed_so_far;
            }
        }
        for map in &mut entry.per_constraint {
            for positions in map.values_mut() {
                positions.retain_mut(|pos| {
                    let shifted = new_position[*pos as usize];
                    if shifted == u32::MAX {
                        false
                    } else {
                        *pos = shifted;
                        true
                    }
                });
            }
            map.retain(|_, positions| !positions.is_empty());
        }
        entry.content_version += 1;
        entry.rows_indexed = expected_rows;
    }

    /// Drop REPLACE key indexes whose memtables are no longer registered.
    fn prune_replace_key_indexes(&self) {
        let memtables = self.memtables.read().unwrap();
        let mut indexes = self.replace_key_indexes.write().unwrap();
        indexes.retain(|id, _| memtables.iter().any(|mt| mt.id() == *id));
    }

    pub fn get_readable_by_table(&self, db: &str, table: &str) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db
                    && mt.table() == table
                    && matches!(mt.state(), MemTableState::Pending | MemTableState::Flushing)
            })
            .cloned()
            .collect()
    }

    pub fn partition_keys(&self, db: &str, table: &str) -> Vec<String> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.db() == db && mt.table() == table)
            .map(|mt| mt.partition_key().to_string())
            .collect()
    }

    /// Drop memtables that have reached the `Archived` state for a table. Called
    /// at close-of-day after the data is safely archived to history segments.
    pub fn remove_archived(&self, db: &str, table: &str) {
        let mut memtables = self.memtables.write().unwrap();
        memtables.retain(|mt| {
            !(mt.db() == db && mt.table() == table && mt.state() == MemTableState::Archived)
        });
        drop(memtables);
        self.prune_replace_key_indexes();
    }

    pub fn get_visible_to(&self, db: &str, table: &str, reader_txn_id: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db
                    && mt.table() == table
                    && mt.state() == MemTableState::Pending
                    && mt.is_visible_to(reader_txn_id)
            })
            .cloned()
            .collect()
    }

    pub fn get_flushable(&self) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                if mt.state() != MemTableState::Pending || mt.is_transactional() {
                    return false;
                }
                // A transaction with deferred DELETE/UPDATE plans still needs
                // this memtable's batch positions stable until COMMIT.
                if self
                    .txn_mutations
                    .read()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|registry| registry.is_memtable_pinned(mt.id()))
                {
                    return false;
                }
                let rows = mt.row_count();
                // A spilled memtable holds no resident rows, so the extended
                // low-rate age brings no memory benefit — it only keeps tail
                // files on disk. Convert spilled memtables at the regular age.
                let age_limit = if !mt.is_spilled() && rows <= self.low_rate_max_rows {
                    self.low_rate_max_age_secs
                } else {
                    self.max_age_secs
                };
                rows >= self.max_rows_per_memtable || mt.age_secs() >= age_limit
            })
            .cloned()
            .collect()
    }

    pub fn get_all_pending(&self) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                // FLUSH ALL (and shutdown) must never materialize another
                // session's uncommitted rows into segments: transactional
                // memtables only reach segments through COMMIT.
                mt.state() == MemTableState::Pending && !mt.is_transactional()
            })
            .cloned()
            .collect()
    }

    /// Drop the writable entries of retired memtables. Retirement points
    /// call this with the keys of the memtables they removed, so the map
    /// never accumulates dead entries and the insertion path never needs a
    /// full-map sweep.
    fn remove_writable_keys(&self, keys: &[WritableTableKey]) {
        if keys.is_empty() {
            return;
        }
        let mut writable = self.writable.write().unwrap();
        for key in keys {
            // Only drop the entry if it still points at a dead memtable: a
            // live replacement for the same key (re-created after retirement)
            // must survive.
            if writable.get(key).and_then(Weak::upgrade).is_none() {
                writable.remove(key);
            }
        }
    }

    pub fn remove_flushed(&self) {
        let mut memtables = self.memtables.write().unwrap();
        let mut retired_keys: Vec<WritableTableKey> = Vec::new();
        memtables.retain(|mt| {
            if mt.state() != MemTableState::Flushed {
                return true;
            }
            retired_keys.push((
                mt.db().to_string(),
                mt.table().to_string(),
                mt.partition_key().to_string(),
                mt.txn_id(),
            ));
            false
        });
        drop(memtables);
        self.remove_writable_keys(&retired_keys);
        self.prune_replace_key_indexes();
    }

    pub fn clear_table(&self, db: &str, table: &str) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut cleared = 0u64;
        let mut retired_keys: Vec<WritableTableKey> = Vec::new();
        memtables.retain(|mt| {
            if mt.db() == db && mt.table() == table {
                cleared += mt.row_count();
                retired_keys.push((
                    mt.db().to_string(),
                    mt.table().to_string(),
                    mt.partition_key().to_string(),
                    mt.txn_id(),
                ));
                mt.set_state(MemTableState::Flushed);
                mt.clear_batch();
                false
            } else {
                true
            }
        });
        drop(memtables);
        self.remove_writable_keys(&retired_keys);
        self.prune_replace_key_indexes();
        cleared
    }

    pub fn clear_partition(&self, db: &str, table: &str, partition_key: &str) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut cleared = 0u64;
        let mut retired_keys: Vec<WritableTableKey> = Vec::new();
        memtables.retain(|mt| {
            if mt.db() == db && mt.table() == table && mt.partition_key() == partition_key {
                cleared += mt.row_count();
                retired_keys.push((
                    mt.db().to_string(),
                    mt.table().to_string(),
                    mt.partition_key().to_string(),
                    mt.txn_id(),
                ));
                mt.set_state(MemTableState::Flushed);
                mt.clear_batch();
                false
            } else {
                true
            }
        });
        drop(memtables);
        self.remove_writable_keys(&retired_keys);
        self.prune_replace_key_indexes();
        cleared
    }

    pub fn rename_table(&self, db: &str, old_table: &str, new_table: &str) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut retired_keys: Vec<WritableTableKey> = Vec::new();
        let mut replacements = Vec::new();
        let mut renamed_rows = 0u64;
        memtables.retain(|mt| {
            if mt.db() != db || mt.table() != old_table {
                return true;
            }

            retired_keys.push((
                mt.db().to_string(),
                mt.table().to_string(),
                mt.partition_key().to_string(),
                mt.txn_id(),
            ));
            let old_state = mt.state();
            mt.set_state(MemTableState::Flushed);
            let Some(batch) = mt.take_batch() else {
                return false;
            };
            if batch.num_rows() == 0 || mt.visibility() == TxnVisibility::Aborted {
                return false;
            }

            let replacement = match mt.visibility() {
                TxnVisibility::Uncommitted(txn_id) => Arc::new(MemTable::with_txn(
                    db.to_string(),
                    new_table.to_string(),
                    mt.partition_key().to_string(),
                    batch.schema.clone(),
                    txn_id,
                )),
                TxnVisibility::Committed => Arc::new(MemTable::new(
                    db.to_string(),
                    new_table.to_string(),
                    mt.partition_key().to_string(),
                    batch.schema.clone(),
                )),
                TxnVisibility::Aborted => unreachable!("aborted memtable handled above"),
            };
            renamed_rows += batch.num_rows() as u64;
            replacement.restore_batch(batch);
            replacement.set_state(if old_state == MemTableState::Archived {
                MemTableState::Archived
            } else {
                MemTableState::Pending
            });
            replacements.push(replacement);
            false
        });
        memtables.extend(replacements);
        drop(memtables);
        self.remove_writable_keys(&retired_keys);
        self.prune_replace_key_indexes();
        renamed_rows
    }

    pub fn rollback_txn(&self, txn_id: u64) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut rolled_back = 0u64;
        let mut removed_any = false;
        let mut retired_keys: Vec<WritableTableKey> = Vec::new();
        memtables.retain(|mt| {
            if mt.txn_id() == txn_id {
                retired_keys.push((
                    mt.db().to_string(),
                    mt.table().to_string(),
                    mt.partition_key().to_string(),
                    mt.txn_id(),
                ));
                mt.mark_aborted();
                rolled_back += mt.row_count();
                removed_any = true;
                false
            } else {
                true
            }
        });
        drop(memtables);
        if removed_any {
            self.remove_writable_keys(&retired_keys);
            self.prune_replace_key_indexes();
        }
        rolled_back
    }

    pub fn commit_txn(&self, txn_id: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        let committed: Vec<Arc<MemTable>> = memtables
            .iter()
            .filter(|mt| mt.txn_id() == txn_id && mt.state() == MemTableState::Pending)
            .cloned()
            .collect();
        // Transaction deltas never pass through the hot store, so committing
        // them makes the touched partitions unprovable for the resident fast
        // path until the store is rebuilt (invalidate / warmup).
        for mt in &committed {
            self.note_hot_unfed_rows(mt.db(), mt.table(), mt.partition_key(), mt.row_count());
        }
        for mt in &committed {
            mt.mark_committed();
        }
        committed
    }

    /// Memtables that are already visible (`Committed`) and still awaiting
    /// persistence. `commit_txn` flips visibility before the durable-tail
    /// wait and the flip clears each memtable's txn_id, so a retried
    /// `commit_txn(txn_id)` finds nothing; the COMMIT retry path drives
    /// durability from this set instead. Uncommitted transactional
    /// memtables (visibility `Uncommitted`) are never included — flushing
    /// one would publish another session's rows.
    pub fn committed_pending_memtables(&self) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.txn_id() == 0
                    && mt.state() == MemTableState::Pending
                    && mt.visibility() == TxnVisibility::Committed
            })
            .cloned()
            .collect()
    }

    /// Approximate resident bytes owned by pending memtables. Spilled
    /// memtables contribute only metadata (counted as zero), since their rows
    /// live in the durable tail.
    pub fn total_estimated_memory_bytes(&self) -> u64 {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.state() == MemTableState::Pending)
            .map(|mt| mt.estimated_memory_bytes())
            .sum()
    }
}

#[cfg(test)]
#[path = "memtable_test.rs"]
mod tests;
