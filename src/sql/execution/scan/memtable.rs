use crate::storage::core::hot_store::{HotStore, HotStoreManager};
use crate::storage::core::memtable::MemTableManager;
use crate::types::batch::RecordBatch;

use super::spec::ScanSpec;

fn live_memtables_and_watermark(
    memtable_mgr: &MemTableManager,
    db: &str,
    table_name: &str,
    reader_txn_id: u64,
    store: &HotStore,
) -> (bool, u64) {
    let memtables = memtable_mgr
        .get_readable_by_table(db, table_name)
        .into_iter()
        .filter(|mt| mt.is_visible_to(reader_txn_id))
        .collect::<Vec<_>>();
    let live_rows = memtables.iter().map(|mt| mt.row_count()).sum::<u64>();
    (live_rows > 0, store.row_count().saturating_sub(live_rows))
}

pub fn scan_memtables(
    memtable_mgr: &MemTableManager,
    hot_mgr: &HotStoreManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> Vec<RecordBatch> {
    let memtables = memtable_mgr
        .get_readable_by_table(db, table_name)
        .into_iter()
        .filter(|mt| mt.is_visible_to(spec.reader_txn_id))
        .collect::<Vec<_>>();

    // Hot-store fast path. The store mirrors auto-commit (txn_id == 0) inserts
    // and has no MVCC, so it is only safe for a committed read snapshot and
    // only when there is live memtable data to accelerate. To avoid
    // double-counting rows already persisted to segments, we derive a watermark
    // = (rows ever appended to the store) - (rows still in live memtables); the
    // leading rows below that watermark have been flushed and are served by the
    // segment scan, so the gather here skips them. This prefix invariant holds
    // for the single-active-partition ingestion pattern (e.g. today's ticks).
    let has_live_memtable_data = memtables.iter().any(|mt| mt.row_count() > 0);
    if has_live_memtable_data && spec.reader_txn_id == 0 {
        if let Some(store) = hot_mgr.get(db, table_name) {
            let live_rows = memtables.iter().map(|mt| mt.row_count()).sum::<u64>();
            let watermark = store.row_count().saturating_sub(live_rows);
            if let Some(batch) = try_hot_fast_path(&store, spec, watermark) {
                return vec![batch];
            }
        }
    }

    let mut batches = Vec::new();

    for mt in &memtables {
        if let Some(mt_batch) = mt.read() {
            if mt_batch.num_rows() == 0 {
                continue;
            }

            let mt_batch = if spec.has_predicates() {
                super::row_filter::apply_predicates(
                    &mt_batch,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                )
            } else {
                mt_batch.clone()
            };
            if mt_batch.num_rows() == 0 {
                continue;
            }

            if let Some(proj) = &spec.projection {
                batches.push(apply_projection(&mt_batch, proj));
            } else {
                batches.push(mt_batch);
            }
        }
    }

    batches
}

/// Per-value inverted-index gather fast path. Triggered when the scan carries
/// an equality predicate on the leading cluster-key column. Gathers only the
/// matching rows via the index instead of scanning the whole resident set.
/// `watermark` excludes already-flushed rows (served by the segment scan).
/// Returns `None` when the fast path does not apply (no such predicate),
/// letting the caller fall back to the generic memtable scan.
fn try_hot_fast_path(store: &HotStore, spec: &ScanSpec, watermark: u64) -> Option<RecordBatch> {
    let key_idx = store.key_column_index();
    let key_value = spec
        .eq_predicates
        .iter()
        .find(|(idx, _)| *idx == key_idx)
        .map(|(_, v)| v)?;

    let pure_key_lookup = spec.eq_predicates.len() == 1
        && spec.eq_predicates[0].0 == key_idx
        && spec.range_predicates.is_empty()
        && spec.in_predicates.is_empty()
        && spec.remaining_predicate.is_none();

    let lookup_projection = if pure_key_lookup {
        spec.projection.as_deref()
    } else {
        None
    };

    let batch = match store.lookup(key_value, lookup_projection, watermark) {
        Some(b) => b,
        None => {
            // Value never seen -> empty result, respecting the projection.
            let schema = store.schema().clone();
            let proj = spec
                .projection
                .clone()
                .unwrap_or_else(|| (0..schema.num_columns()).collect());
            return Some(apply_projection(&RecordBatch::new(schema), &proj));
        }
    };

    let filtered = if !pure_key_lookup && spec.has_predicates() {
        super::row_filter::apply_predicates(
            &batch,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        )
    } else {
        batch
    };

    if pure_key_lookup {
        return Some(filtered);
    }

    let projected = match &spec.projection {
        Some(proj) => apply_projection(&filtered, proj),
        None => filtered,
    };
    Some(projected)
}

pub fn count_hot_memtable_rows(
    memtable_mgr: &MemTableManager,
    hot_mgr: &HotStoreManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> Option<u64> {
    if spec.reader_txn_id != 0
        || spec.eq_predicates.len() != 1
        || !spec.range_predicates.is_empty()
        || !spec.in_predicates.is_empty()
        || spec.remaining_predicate.is_some()
    {
        return None;
    }
    let store = hot_mgr.get(db, table_name)?;
    let key_idx = store.key_column_index();
    let (col_idx, key_value) = &spec.eq_predicates[0];
    if *col_idx != key_idx {
        return None;
    }
    let (has_live_memtable_data, watermark) =
        live_memtables_and_watermark(memtable_mgr, db, table_name, spec.reader_txn_id, &store);
    if !has_live_memtable_data {
        return None;
    }
    store.count_by_key_from(key_value, watermark)
}

pub fn count_memtable_rows(
    memtable_mgr: &MemTableManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> u64 {
    let memtables = memtable_mgr
        .get_readable_by_table(db, table_name)
        .into_iter()
        .filter(|mt| mt.is_visible_to(spec.reader_txn_id))
        .collect::<Vec<_>>();
    let mut total: u64 = 0;

    for mt in &memtables {
        if let Some(mt_batch) = mt.read() {
            if mt_batch.num_rows() == 0 {
                continue;
            }

            if spec.eq_predicates.len() == 1
                && spec.range_predicates.is_empty()
                && spec.in_predicates.is_empty()
                && spec.remaining_predicate.is_none()
            {
                let (col_idx, value) = &spec.eq_predicates[0];
                if let Some(col) = mt_batch.column(*col_idx) {
                    let mask = col.eq_vec(value);
                    total += mask.iter().filter(|&&m| m).count() as u64;
                }
            } else if spec.has_predicates() {
                let filtered = super::row_filter::apply_predicates(
                    &mt_batch,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                );
                total += filtered.num_rows() as u64;
            } else {
                total += mt_batch.num_rows() as u64;
            }
        }
    }

    total
}

fn apply_projection(batch: &RecordBatch, projection: &[usize]) -> RecordBatch {
    let mut col_defs = Vec::with_capacity(projection.len());
    let mut columns = Vec::with_capacity(projection.len());
    for &idx in projection {
        if let Some(col) = batch.column(idx) {
            columns.push(col.clone());
            if let Some(col_def) = batch.schema.column(idx) {
                col_defs.push(col_def.clone());
            }
        }
    }
    let schema = crate::types::batch::Schema::new(col_defs);
    RecordBatch::try_new(schema, columns).unwrap_or_else(|_| batch.clone())
}
