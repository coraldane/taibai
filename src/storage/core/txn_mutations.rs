//! Per-transaction registry of deferred segment/memtable mutations.
//!
//! DELETE and UPDATE inside an explicit transaction must not touch durable
//! state at statement time (ROLLBACK would be unable to undo them, and
//! uncommitted tombstones would leak to other sessions). Instead the
//! statement plans its hits and records them here; the transaction's own
//! readers consult the registry to hide the doomed rows, other sessions see
//! the pre-statement state, and COMMIT materializes everything under the
//! table mutation locks right before the visibility flip. A crash discards
//! the registry, which is exactly ROLLBACK semantics.
//!
//! The registry also acts as a pin set: segments and memtables referenced by
//! a live entry are off limits to compaction and background flush (row ids
//! and batch positions must stay stable until COMMIT).

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::storage::core::memtable::MemTable;
use crate::types::batch::RecordBatch;

/// Tombstoned rows of one segment, deferred to COMMIT.
pub struct SegmentTombstones {
    pub path: PathBuf,
    /// Absolute row ids this transaction deleted (union across statements).
    pub row_ids: BTreeSet<u64>,
    /// Footer bitmap ∪ `row_ids`, snapshot for scan readers. Short bitmaps
    /// are fine: `is_deleted` treats out-of-range rows as alive, so segments
    /// that grew after composition stay correct.
    pub composed: Arc<Vec<u8>>,
}

/// Rows of one committed memtable hidden from this transaction's readers and
/// physically removed at COMMIT. Batch positions are prefix-stable: appends
/// only extend the batch, so planned indices stay valid across the
/// transaction's lifetime (the memtable is pinned against flush).
pub struct MemtableTombstones {
    pub memtable: Arc<MemTable>,
    pub positions: Arc<BTreeSet<u32>>,
}

/// Deferred mutations of one (db, table).
pub struct TableTxnMutations {
    pub db: String,
    pub table: String,
    /// Partition keys the statements were narrowed to (`None` = table-wide).
    /// Reused as the mutation-lock scope at COMMIT.
    pub partitions: Option<Vec<String>>,
    pub segments: Vec<SegmentTombstones>,
    pub memtables: Vec<MemtableTombstones>,
    /// Rows this transaction deleted or replaced; the catalog `row_count`
    /// decrement is deferred to COMMIT with this value.
    pub deleted_rows: u64,
    /// Change-feed Delete events (pre-delete row images), published at COMMIT.
    pub deferred_delete_batches: Vec<RecordBatch>,
    /// Change-feed Update events (post-update row images), published at COMMIT.
    pub deferred_update_batches: Vec<RecordBatch>,
}

impl TableTxnMutations {
    fn new(db: &str, table: &str, partitions: Option<Vec<String>>) -> Self {
        Self {
            db: db.to_string(),
            table: table.to_string(),
            partitions,
            segments: Vec::new(),
            memtables: Vec::new(),
            deleted_rows: 0,
            deferred_delete_batches: Vec::new(),
            deferred_update_batches: Vec::new(),
        }
    }

    fn segment_entry_mut(&mut self, path: &Path) -> Option<&mut SegmentTombstones> {
        self.segments.iter_mut().find(|entry| entry.path == path)
    }

    fn memtable_entry_mut(&mut self, memtable_id: Uuid) -> Option<&mut MemtableTombstones> {
        self.memtables
            .iter_mut()
            .find(|entry| entry.memtable.id() == memtable_id)
    }

    fn is_pinned(&self) -> bool {
        !self.segments.is_empty() || !self.memtables.is_empty()
    }
}

/// All deferred mutations of one explicit transaction, across tables.
#[derive(Default)]
pub struct TxnMutations {
    tables: Vec<TableTxnMutations>,
    /// Approximate resident size of the deferred mutations (row ids,
    /// tombstone positions, deferred change-event batches). Charged against
    /// [`txn_mutation_budget_bytes`] so a runaway transaction fails at
    /// statement time instead of growing without bound until OOM.
    mutation_bytes: u64,
}

/// Deferred-mutation budget per transaction (row ids, tombstone positions,
/// deferred change-event batches). Overridable for large-import workloads.
pub fn txn_mutation_budget_bytes() -> u64 {
    static BUDGET: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::env::var("TAIBAI_TXN_MUTATION_BUDGET_BYTES")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .filter(|&bytes| bytes > 0)
            .unwrap_or(512 * 1024 * 1024)
    })
}

/// Rough per-entry cost of a BTreeSet row id / tombstone position (key bytes
/// plus tree-node overhead).
const POSITION_CHARGE_BYTES: u64 = 48;

/// Shared registry hanging off the engine, segment manager (scan lookup) and
/// memtable manager (flush pin). All methods take the single lock; entries
/// are created by rare DML statements and probed per scan only by readers of
/// transactions that actually ran DML.
#[derive(Default)]
pub struct TxnMutationRegistry {
    inner: Mutex<HashMap<u64, TxnMutations>>,
}

impl TxnMutationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn table_entry<'a>(
        inner: &'a mut HashMap<u64, TxnMutations>,
        txn_id: u64,
        db: &str,
        table: &str,
        partitions: Option<&[String]>,
    ) -> &'a mut TableTxnMutations {
        let txn = inner.entry(txn_id).or_default();
        if let Some(index) = txn
            .tables
            .iter()
            .position(|entry| entry.db == db && entry.table == table)
        {
            let entry = &mut txn.tables[index];
            // The recorded scope drives the COMMIT mutation-lock breadth, so a
            // later statement must widen it to cover its own tombstones: a
            // table-wide statement (`None`) makes the COMMIT take the whole
            // table lock, and two different narrow partitions union their
            // keys. Keeping the first statement's narrow scope would let
            // COMMIT rewrite another partition's segments while that
            // partition's flush runs concurrently.
            match (&mut entry.partitions, partitions) {
                (_, None) => entry.partitions = None,
                (None, Some(_)) => {}
                (Some(existing), Some(new_parts)) => {
                    for part in new_parts {
                        if !existing.iter().any(|key| key == part) {
                            existing.push(part.clone());
                        }
                    }
                }
            }
            return entry;
        }
        let mut entry = TableTxnMutations::new(db, table, None);
        if let Some(parts) = partitions {
            entry.partitions = Some(parts.to_vec());
        }
        txn.tables.push(entry);
        txn.tables.last_mut().expect("just pushed")
    }

    /// Record newly deleted segment rows for `txn_id`. `footer_bitmap` is the
    /// segment's current footer bitmap; the composed reader bitmap is
    /// recomputed as footer ∪ existing entry ∪ `newly_deleted`.
    pub fn register_segment_tombstones(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        partitions: Option<&[String]>,
        path: &Path,
        footer_bitmap: &[u8],
        newly_deleted: impl IntoIterator<Item = u64>,
    ) -> crate::common::error::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let entry = Self::table_entry(&mut inner, txn_id, db, table, partitions);
        let entry = if let Some(existing) = entry.segment_entry_mut(path) {
            existing
        } else {
            entry.segments.push(SegmentTombstones {
                path: path.to_path_buf(),
                row_ids: BTreeSet::new(),
                composed: Arc::new(Vec::new()),
            });
            entry.segments.last_mut().expect("just pushed")
        };
        let mut new_row_ids: u64 = 0;
        // Incremental composition instead of replaying the whole history:
        // row ids are recorded once and never removed, so folding the current
        // footer once per statement (O(footer bytes)) plus the new ids
        // (O(new ids)) reproduces the replay's footer ∪ all-ids recomputation
        // without its O(all recorded ids) term — the quadratic cost of
        // repeated small DELETEs inside one transaction. Folding the footer
        // EVERY statement (not just at composition start) keeps the result
        // byte-identical to the replay even if the segment's footer gained
        // bits in the already-composed range; bits are never cleared here.
        let composed = Arc::make_mut(&mut entry.composed);
        if composed.len() < footer_bitmap.len() {
            composed.resize(footer_bitmap.len(), 0);
        }
        for (dst, src) in composed.iter_mut().zip(footer_bitmap.iter()) {
            *dst |= src;
        }
        for row_id in newly_deleted {
            if entry.row_ids.insert(row_id) {
                new_row_ids += 1;
                let byte = (row_id / 8) as usize;
                let bit = (row_id % 8) as u8;
                if byte >= composed.len() {
                    composed.resize(byte + 1, 0);
                }
                composed[byte] |= 1u8 << bit;
            }
        }
        // `Arc::make_mut` mutated the existing allocation in place when the
        // Arc was unique (the common case); when a reader still held the old
        // snapshot it cloned once — the same cost the full replay always paid.
        // Charge after the `entry` borrow ends; `charge` re-borrows `inner`.
        let txn = inner.get_mut(&txn_id).expect("entry just created");
        txn.charge(new_row_ids.saturating_mul(POSITION_CHARGE_BYTES))?;
        Ok(())
    }

    /// Record matched rows of a committed memtable for deferred removal.
    pub fn register_memtable_tombstones(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        partitions: Option<&[String]>,
        memtable: Arc<MemTable>,
        positions: impl IntoIterator<Item = u32>,
    ) -> crate::common::error::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let entry = Self::table_entry(&mut inner, txn_id, db, table, partitions);
        let memtable_id = memtable.id();
        let entry = if let Some(existing) = entry.memtable_entry_mut(memtable_id) {
            existing
        } else {
            entry.memtables.push(MemtableTombstones {
                memtable,
                positions: Arc::new(BTreeSet::new()),
            });
            entry.memtables.last_mut().expect("just pushed")
        };
        // Differential merge instead of clone-then-extend: insert each
        // incoming position straight into the set (O(log n) per position,
        // duplicate positions are the no-op they already were), cloning the
        // stored set only when a reader still holds the old `Arc` snapshot.
        let positions_set = Arc::make_mut(&mut entry.positions);
        let mut new_positions = 0u64;
        for position in positions {
            if positions_set.insert(position) {
                new_positions += 1;
            }
        }
        let txn = inner.get_mut(&txn_id).expect("entry just created");
        txn.charge(new_positions.saturating_mul(POSITION_CHARGE_BYTES))?;
        Ok(())
    }

    /// Charge a transactional INSERT's buffered memtable bytes against the
    /// transaction's in-memory budget. Transactional memtables are invisible
    /// to every background relief valve (spill, pressure flush, close-of-day
    /// flush) until COMMIT, so an uncharged transaction could grow memory
    /// without bound; an over-budget statement fails like oversized deferred
    /// DML does, and the entry created here is dropped by the usual
    /// COMMIT/ROLLBACK registry cleanup.
    pub fn charge_txn_insert_bytes(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        bytes: u64,
    ) -> crate::common::error::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let _table = Self::table_entry(&mut inner, txn_id, db, table, None);
        let txn = inner.get_mut(&txn_id).expect("table_entry just ensured it");
        txn.charge_insert(bytes)
    }

    pub fn add_deleted_rows(&self, txn_id: u64, db: &str, table: &str, count: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(txn) = inner.get_mut(&txn_id) {
            if let Some(entry) = txn
                .tables
                .iter_mut()
                .find(|entry| entry.db == db && entry.table == table)
            {
                entry.deleted_rows += count;
            }
        }
    }

    pub fn defer_delete_event(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        batch: RecordBatch,
    ) -> crate::common::error::Result<()> {
        let bytes = batch.estimated_size_bytes() as u64;
        let mut inner = self.inner.lock().unwrap();
        let entry = Self::table_entry(&mut inner, txn_id, db, table, None);
        entry.deferred_delete_batches.push(batch);
        let txn = inner.get_mut(&txn_id).expect("entry just created");
        txn.charge(bytes)
    }

    pub fn defer_update_event(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        batch: RecordBatch,
    ) -> crate::common::error::Result<()> {
        let bytes = batch.estimated_size_bytes() as u64;
        let mut inner = self.inner.lock().unwrap();
        let entry = Self::table_entry(&mut inner, txn_id, db, table, None);
        entry.deferred_update_batches.push(batch);
        let txn = inner.get_mut(&txn_id).expect("entry just created");
        txn.charge(bytes)
    }

    /// Composed delete bitmap for one of the transaction's pinned segments.
    pub fn segment_overlay(&self, txn_id: u64, path: &Path) -> Option<Arc<Vec<u8>>> {
        let inner = self.inner.lock().unwrap();
        let txn = inner.get(&txn_id)?;
        txn.tables
            .iter()
            .flat_map(|entry| entry.segments.iter())
            .find(|segment| segment.path == path)
            .map(|segment| Arc::clone(&segment.composed))
    }

    /// Existing tombstone row ids recorded for a segment by earlier
    /// statements of this transaction, so a later statement can seed its
    /// match bitmap and not double-count rows it already deleted.
    pub fn segment_row_ids(&self, txn_id: u64, path: &Path) -> Vec<u64> {
        let inner = self.inner.lock().unwrap();
        let Some(txn) = inner.get(&txn_id) else {
            return Vec::new();
        };
        txn.tables
            .iter()
            .flat_map(|entry| entry.segments.iter())
            .find(|segment| segment.path == path)
            .map(|segment| segment.row_ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Positions to hide in one of the transaction's pinned memtables.
    pub fn memtable_positions(&self, txn_id: u64, memtable_id: Uuid) -> Option<Arc<BTreeSet<u32>>> {
        let inner = self.inner.lock().unwrap();
        let txn = inner.get(&txn_id)?;
        txn.tables
            .iter()
            .flat_map(|entry| entry.memtables.iter())
            .find(|entry| entry.memtable.id() == memtable_id)
            .map(|entry| Arc::clone(&entry.positions))
    }

    /// The memtable batch as this transaction sees it: earlier statements'
    /// tombstoned rows are filtered out, and the returned position map
    /// translates filtered-batch coordinates back to absolute positions in
    /// the memtable's batch. The committed batch is never mutated between
    /// statements (only tombstone positions are recorded), so every
    /// deferred-DML consumer — DELETE planning, UPDATE planning, change
    /// events — must read through this view or a re-match would
    /// double-count the already-doomed rows.
    pub fn memtable_batch_visible_to_txn(
        &self,
        txn_id: u64,
        memtable: &MemTable,
        original: &Arc<RecordBatch>,
    ) -> crate::common::error::Result<(Arc<RecordBatch>, Option<Vec<u32>>)> {
        match self.memtable_positions(txn_id, memtable.id()) {
            Some(positions) if !positions.is_empty() => {
                let keep: Vec<u32> = (0..original.num_rows() as u32)
                    .filter(|row| !positions.contains(row))
                    .collect();
                let filtered = filter_batch_excluding_positions(original, &positions)?;
                Ok((Arc::new(filtered), Some(keep)))
            }
            _ => Ok((Arc::clone(original), None)),
        }
    }

    /// True when any live transaction holds deferred mutations (pins) on the
    /// table: compaction and TTL must leave its segments alone.
    pub fn table_has_pins(&self, db: &str, table: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.values().any(|txn| {
            txn.tables
                .iter()
                .any(|entry| entry.db == db && entry.table == table && entry.is_pinned())
        })
    }

    /// True when a transaction *other* than `txn_id` pins the table. DML
    /// outside the pinning transaction (auto-commit statements, other open
    /// transactions) must not run while pins exist: an auto-commit DELETE
    /// physically removes memtable rows and shifts positions, so a later
    /// COMMIT of the pinned transaction would delete the wrong rows.
    pub fn table_pinned_by_other(&self, txn_id: u64, db: &str, table: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.iter().any(|(&holder, txn)| {
            holder != txn_id
                && txn
                    .tables
                    .iter()
                    .any(|entry| entry.db == db && entry.table == table && entry.is_pinned())
        })
    }

    /// True when a background flush must skip this memtable: a transaction
    /// still needs its batch positions stable for the deferred removal.
    pub fn is_memtable_pinned(&self, memtable_id: Uuid) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.values().any(|txn| {
            txn.tables.iter().any(|entry| {
                entry
                    .memtables
                    .iter()
                    .any(|pinned| pinned.memtable.id() == memtable_id)
            })
        })
    }

    /// Detach the transaction's mutations for materialization at COMMIT.
    /// On materialization failure the caller puts them back with
    /// [`Self::put_back`] so the transaction can retry or roll back.
    pub fn take(&self, txn_id: u64) -> Option<TxnMutations> {
        let mut inner = self.inner.lock().unwrap();
        inner.remove(&txn_id)
    }

    pub fn put_back(&self, txn_id: u64, mutations: TxnMutations) {
        let mut inner = self.inner.lock().unwrap();
        inner.insert(txn_id, mutations);
    }

    /// Drop the transaction's mutations (ROLLBACK or cleanup). Returns the
    /// deferred deleted-row count for the rollback report.
    pub fn remove(&self, txn_id: u64) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner
            .remove(&txn_id)
            .map(|txn| txn.tables.iter().map(|entry| entry.deleted_rows).sum())
            .unwrap_or(0)
    }
}

impl TxnMutations {
    pub fn tables(&self) -> &[TableTxnMutations] {
        &self.tables
    }

    /// Charge bytes a transactional INSERT buffered into its transactional
    /// memtables. Same budget as deferred DML: both are memory the transaction
    /// holds that no background relief valve may reclaim before COMMIT.
    pub fn charge_insert(&mut self, bytes: u64) -> crate::common::error::Result<()> {
        self.charge(bytes)
    }

    fn charge(&mut self, bytes: u64) -> crate::common::error::Result<()> {
        self.mutation_bytes = self.mutation_bytes.saturating_add(bytes);
        let budget = txn_mutation_budget_bytes();
        if self.mutation_bytes > budget {
            return Err(crate::common::error::TaibaiError::ResourceBusy(format!(
                "transaction in-memory mutation budget exceeded: {} bytes charged against a {} byte limit (deferred DML plans plus buffered transactional inserts); the transaction is too large to keep in memory - commit or roll back and split the work",
                self.mutation_bytes, budget
            )));
        }
        Ok(())
    }
}

/// Remove the given batch positions (deferred memtable tombstones) from a
/// snapshot. Positions are prefix-stable indices recorded at statement time;
/// appends only grow a memtable batch, so they stay valid until COMMIT.
pub fn filter_batch_excluding_positions(
    batch: &RecordBatch,
    positions: &BTreeSet<u32>,
) -> crate::common::error::Result<RecordBatch> {
    if positions.is_empty() {
        return Ok(batch.clone());
    }
    let keep: Vec<u32> = (0..batch.num_rows() as u32)
        .filter(|row| !positions.contains(row))
        .collect();
    if keep.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&keep))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Transactional INSERT memtable bytes count against the same per-
    /// transaction in-memory budget as deferred DML, and the charged entry
    /// disappears on the usual rollback cleanup.
    #[test]
    fn txn_insert_bytes_count_against_mutation_budget() {
        let registry = TxnMutationRegistry::new();
        let budget = txn_mutation_budget_bytes();

        // Exactly at budget is fine; one more byte trips it.
        registry
            .charge_txn_insert_bytes(9, "db", "t", budget)
            .expect("at-budget charge must pass");
        let err = registry
            .charge_txn_insert_bytes(9, "db", "t", 1)
            .expect_err("over-budget charge must fail");
        assert!(err.to_string().contains("budget exceeded"), "{err}");

        // Other transactions are unaffected.
        registry
            .charge_txn_insert_bytes(10, "db", "t", 1024)
            .expect("other txn unaffected");

        // Rollback cleanup drops the charged entry; the next transaction
        // with the same id starts from a fresh budget.
        registry.remove(9);
        registry
            .charge_txn_insert_bytes(9, "db", "t", 1)
            .expect("cleaned entry starts fresh");
    }

    #[test]
    fn segment_tombstones_merge_and_compose() {
        let registry = TxnMutationRegistry::new();
        registry
            .register_segment_tombstones(7, "db", "t", None, Path::new("s1"), &[], [3, 9])
            .unwrap();
        registry
            .register_segment_tombstones(7, "db", "t", None, Path::new("s1"), &[], [3, 20])
            .unwrap();
        let composed = registry.segment_overlay(7, Path::new("s1")).unwrap();
        assert!(composed.len() >= 3);
        let is_set = |row: u64| composed[(row / 8) as usize] & (1u8 << (row % 8)) != 0;
        for row in [3u64, 9, 20] {
            assert!(is_set(row), "row {row} must be set");
        }
        assert!(!is_set(4));
        assert_eq!(registry.segment_row_ids(7, Path::new("s1")), vec![3, 9, 20]);
    }

    #[test]
    fn take_remove_and_pins() {
        let registry = TxnMutationRegistry::new();
        registry
            .register_segment_tombstones(1, "db", "t", None, Path::new("s"), &[], [0])
            .unwrap();
        assert!(registry.table_has_pins("db", "t"));
        assert!(!registry.table_has_pins("db", "other"));
        let taken = registry.take(1).unwrap();
        assert!(!registry.table_has_pins("db", "t"));
        registry.put_back(1, taken);
        assert!(registry.table_has_pins("db", "t"));
        registry
            .register_segment_tombstones(2, "db", "t", None, Path::new("s2"), &[], [1])
            .unwrap();
        assert_eq!(registry.remove(1), 0);
        assert!(registry.table_has_pins("db", "t"), "txn 2 still pins");
        registry.remove(2);
        assert!(!registry.table_has_pins("db", "t"));
    }

    /// A later table-wide statement must widen the COMMIT lock scope: a
    /// narrow-partition DELETE followed by a table-wide DELETE would
    /// otherwise leave the recorded scope narrow while tombstones cover
    /// other partitions, letting COMMIT rewrite those segments against a
    /// concurrent flush.
    #[test]
    fn table_scope_widens_to_none_on_table_wide_statement() {
        let registry = TxnMutationRegistry::new();
        registry
            .register_segment_tombstones(
                1,
                "db",
                "t",
                Some(&["a".to_string()]),
                Path::new("s1"),
                &[],
                [0],
            )
            .unwrap();
        // Table-wide statement (partitions = None).
        registry
            .register_segment_tombstones(1, "db", "t", None, Path::new("s2"), &[], [0])
            .unwrap();
        // Scope is private; verify via take: the merged entry drives the
        // COMMIT lock breadth in materialize_tables.
        let mutations = registry.take(1).unwrap();
        let entry = &mutations.tables[0];
        assert!(
            entry.partitions.is_none(),
            "table-wide statement must widen the recorded scope to None"
        );
    }

    /// Two different narrow partitions union their keys; a repeat of the
    /// same partition does not duplicate the entry.
    #[test]
    fn narrow_scopes_union_across_statements() {
        let registry = TxnMutationRegistry::new();
        registry
            .register_segment_tombstones(
                1,
                "db",
                "t",
                Some(&["a".to_string()]),
                Path::new("s1"),
                &[],
                [0],
            )
            .unwrap();
        registry
            .register_segment_tombstones(
                1,
                "db",
                "t",
                Some(&["b".to_string()]),
                Path::new("s2"),
                &[],
                [0],
            )
            .unwrap();
        registry
            .register_segment_tombstones(
                1,
                "db",
                "t",
                Some(&["a".to_string()]),
                Path::new("s1"),
                &[],
                [1],
            )
            .unwrap();
        let mutations = registry.take(1).unwrap();
        let entry = &mutations.tables[0];
        assert_eq!(
            entry.partitions.as_deref(),
            Some(&["a".to_string(), "b".to_string()][..]),
            "narrow scopes must union without duplicates"
        );
        assert_eq!(entry.segments.len(), 2);
    }

    /// The incremental composition must stay equivalent to the old full
    /// replay: after every statement, the composed bitmap equals
    /// `footer ∪ all recorded row ids` recomputed from scratch. Footers grow
    /// monotonically like a pinned segment's really would (deletes only set
    /// bits, appends only extend the length), which is the invariant the
    /// increment relies on: the pinned segment cannot gain bits
    /// mid-transaction, so folding the first footer equals the replay's
    /// per-statement `current footer ∪ all ids`.
    #[test]
    fn incremental_segment_composition_matches_full_replay() {
        let registry = TxnMutationRegistry::new();
        let path = Path::new("s_incremental");

        // Deterministic xorshift so failures reproduce.
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        let mut reference_ids: BTreeSet<u64> = BTreeSet::new();
        let mut footer: Vec<u8> = Vec::new();
        for _statement in 0..40 {
            // The segment's footer bitmap only ever grows (like a pinned
            // segment's): the length may extend and bits may be set, never
            // cleared.
            footer.resize(footer.len() + (next() as usize) % 3, 0);
            for byte in footer.iter_mut() {
                *byte |= (next() & 0x03) as u8;
            }
            let new_ids: Vec<u64> = (0..(next() as usize) % 12)
                .map(|_| (next() as u64) % 96)
                .collect();

            registry
                .register_segment_tombstones(
                    11,
                    "db",
                    "t",
                    None,
                    path,
                    &footer,
                    new_ids.iter().copied(),
                )
                .unwrap();
            for id in &new_ids {
                reference_ids.insert(*id);
            }

            // Reference: full recomputation the replay used to do.
            let mut expected = footer.clone();
            for &row_id in &reference_ids {
                let byte = (row_id / 8) as usize;
                let bit = (row_id % 8) as u8;
                if byte >= expected.len() {
                    expected.resize(byte + 1, 0);
                }
                expected[byte] |= 1u8 << bit;
            }
            let composed = registry.segment_overlay(11, path).unwrap();
            assert_eq!(
                composed.as_ref(),
                &expected,
                "incremental composition diverged from full replay"
            );
            assert_eq!(
                registry.segment_row_ids(11, path),
                reference_ids.iter().copied().collect::<Vec<_>>()
            );
        }
    }

    /// Memtable tombstone merging is differential now; the visible position
    /// set must still be the exact union across statements, with repeated
    /// positions counted (and charged) only once.
    #[test]
    fn memtable_position_merge_is_the_exact_union() {
        let registry = TxnMutationRegistry::new();
        let schema = crate::types::batch::Schema::new(vec![crate::types::batch::ColumnDef::new(
            "id",
            crate::protocol::types::DataType::Int64,
        )]);
        let memtable = std::sync::Arc::new(MemTable::new(
            "db".into(),
            "t".into(),
            "p".into(),
            schema,
        ));

        registry
            .register_memtable_tombstones(21, "db", "t", None, std::sync::Arc::clone(&memtable), [5, 9])
            .unwrap();
        registry
            .register_memtable_tombstones(21, "db", "t", None, std::sync::Arc::clone(&memtable), [9, 1, 3])
            .unwrap();
        registry
            .register_memtable_tombstones(21, "db", "t", None, std::sync::Arc::clone(&memtable), [1, 5, 9])
            .unwrap();

        let positions = registry
            .memtable_positions(21, memtable.id())
            .expect("positions recorded");
        let merged: Vec<u32> = positions.iter().copied().collect();
        assert_eq!(merged, vec![1, 3, 5, 9]);
    }
}
