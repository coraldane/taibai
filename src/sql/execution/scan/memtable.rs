use std::sync::Arc;

use crate::common::error::Result;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::core::hot_store::{HotStore, HotStoreManager};
use crate::storage::core::memtable::MemTableManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

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

pub fn scan_resident_hot_store(
    hot_mgr: &HotStoreManager,
    memtable_mgr: &MemTableManager,
    db: &str,
    table_meta: &TableMeta,
    spec: &ScanSpec,
) -> Option<RecordBatch> {
    // The hot gather truncates at the pushed LIMIT before TTL filtering
    // runs, so with expired rows present it would return fewer live rows
    // than exist.
    if spec.reader_txn_id != 0 || spec.ttl_active || table_meta.ttl.is_some() {
        return None;
    }
    let store = hot_mgr.get(db, &spec.table_name)?;
    if store.row_count() == 0 {
        return None;
    }
    if !resident_hot_covers(&store, memtable_mgr, db, table_meta, spec) {
        return None;
    }
    scan_hot_store_by_key(&store, spec, 0)
}

/// Partial-residency gate. Returns `(partition_column_index, covers_value)`
/// when the mirror provably holds every row whose partition-column value is
/// strictly greater than `covers_value` (`TableMeta::hot_mirror_covers_from`,
/// established at close-of-day / restart warmup) and the scan carries an
/// equality on that column above the bound. `hot_partition_mirrored` closes
/// the gate after any mirror bypass, prefix reclaim or transaction delta.
fn hybrid_hot_mirror_bound(
    store: &HotStore,
    memtable_mgr: &MemTableManager,
    db: &str,
    table_meta: &TableMeta,
    spec: &ScanSpec,
) -> Option<(usize, ScalarValue)> {
    if spec.reader_txn_id != 0 || table_meta.ttl.is_some() {
        return None;
    }
    if !crate::common::config::query_limits().hybrid_hot_scan_enabled {
        return None;
    }
    if store.row_count() == 0 {
        return None;
    }
    let covers_from = table_meta.hot_mirror_covers_from?;
    let pk_idx = table_meta.get_partition_column_index()?;
    let value = spec.eq_value_for_column(pk_idx)?;
    let value_i64 = match value {
        ScalarValue::Int32(Some(v)) => *v as i64,
        ScalarValue::Int64(Some(v)) => *v,
        _ => return None,
    };
    if value_i64 <= covers_from {
        return None;
    }
    let partition_info = spec.partition_info.as_ref()?;
    let partition_key =
        super::partition::extract_partition_eq_value(spec, partition_info, table_meta)?;
    if !memtable_mgr.hot_partition_mirrored(db, &spec.table_name, &partition_key) {
        return None;
    }
    let column = table_meta.columns.get(pk_idx)?;
    let covers = match column.data_type {
        crate::protocol::types::DataType::Int32 => {
            // A bound below the column's representable range means "covers
            // everything", which i32::MIN expresses exactly; a bound above it
            // cannot be represented and must not be approximated.
            if covers_from > i32::MAX as i64 {
                return None;
            }
            ScalarValue::Int32(Some(
                covers_from.clamp(i32::MIN as i64, i32::MAX as i64) as i32
            ))
        }
        crate::protocol::types::DataType::Int64 => ScalarValue::Int64(Some(covers_from)),
        _ => return None,
    };
    Some((pk_idx, covers))
}

fn spec_with_mirror_bound(
    spec: &ScanSpec,
    pk_idx: usize,
    covers: &ScalarValue,
    op: crate::storage::partition::pruning::RangeOp,
) -> ScanSpec {
    let mut bound_spec = spec.clone();
    bound_spec
        .range_predicates
        .push(crate::storage::partition::pruning::RangeClause {
            column_index: pk_idx,
            op,
            value: covers.clone(),
        });
    bound_spec
}

/// Hybrid partial-residency mirror leg: gather the rows the mirror owns
/// (`partition_column > covers`) through the inverted index. The caller adds
/// the complementary `<= covers` range clause to every non-mirror leg, so the
/// two sides partition the table exactly with no overlap. Returns `None` when
/// the gate declines or the scan has no key equality for the index gather.
pub fn hybrid_hot_mirror_scan(
    hot_mgr: &HotStoreManager,
    memtable_mgr: &MemTableManager,
    db: &str,
    table_meta: &TableMeta,
    spec: &ScanSpec,
) -> Option<(RecordBatch, usize, ScalarValue)> {
    let store = hot_mgr.get(db, &spec.table_name)?;
    let (pk_idx, covers) = hybrid_hot_mirror_bound(&store, memtable_mgr, db, table_meta, spec)?;
    let mirror_spec = spec_with_mirror_bound(
        spec,
        pk_idx,
        &covers,
        crate::storage::partition::pruning::RangeOp::Gt,
    );
    let batch = scan_hot_store_by_key(&store, &mirror_spec, 0)?;
    Some((batch, pk_idx, covers))
}

/// The hot store may answer a partition scan alone only when it holds the
/// partition's full row set: the store must mark the partition resident AND
/// no write may have bypassed the hot-store mirror for it. The second check
/// is what keeps the fast path correct after memtables have been flushed to
/// segments — the flush itself does not invalidate residency because the
/// flushed rows are mirrored, but a transaction delta / relaxed or bulk
/// bypass write does (recorded by the writer as hot-unfed rows).
fn resident_hot_covers(
    store: &HotStore,
    memtable_mgr: &MemTableManager,
    db: &str,
    table_meta: &TableMeta,
    spec: &ScanSpec,
) -> bool {
    let target_partition = if table_meta.is_partitioned() {
        let Some(pi) = spec.partition_info.as_ref() else {
            return false;
        };
        let Some(partition_key) =
            super::partition::extract_partition_eq_value(spec, pi, table_meta)
        else {
            return false;
        };
        Some(partition_key)
    } else {
        None
    };
    if !store.is_resident_for_partition(target_partition.as_deref()) {
        return false;
    }
    memtable_mgr.hot_partition_mirrored(
        db,
        &spec.table_name,
        target_partition.as_deref().unwrap_or(""),
    )
}

/// Whether a resident memtable batch diverges from the catalog schema and
/// needs alignment. Hand-built specs (tests) carry an empty catalog schema
/// and never align.
fn memtable_batch_needs_alignment(batch: &RecordBatch, table_schema: &Schema) -> bool {
    !crate::storage::core::memtable::batch_matches_schema(batch, table_schema)
}

pub fn scan_memtables(
    memtable_mgr: &MemTableManager,
    hot_mgr: &HotStoreManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> crate::common::error::Result<Vec<RecordBatch>> {
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
    // for the single-active-partition ingestion pattern.
    let has_live_memtable_data = memtables.iter().any(|mt| mt.row_count() > 0);
    // The shortcut below replaces the memtable scan with hot-store rows above
    // the watermark, which is only equivalent while every live memtable row is
    // hot-fed. A committed transaction delta (or any other bypass write) lives
    // in a memtable the store never mirrored; with one present, fall through
    // to the plain memtable scan so those rows stay visible.
    if has_live_memtable_data
        && spec.reader_txn_id == 0
        // TTL filtering runs after this gather truncates at the pushed
        // LIMIT — the shortcut is only safe while no rows can expire.
        && !spec.ttl_active
        && memtable_mgr.hot_table_fully_mirrored(db, table_name)
    {
        if let Some(store) = hot_mgr.get(db, table_name) {
            let (_, watermark) = live_memtables_and_watermark(
                memtable_mgr,
                db,
                table_name,
                spec.reader_txn_id,
                &store,
            );
            if let Some(batch) = scan_hot_store_by_key(&store, spec, watermark) {
                return Ok(vec![batch]);
            }
        }
    }

    let mut batches = Vec::new();

    for mt in &memtables {
        // Deferred in-transaction DELETE/UPDATE tombstones for this memtable:
        // the transaction's own readers must not see the doomed rows.
        let txn_positions = if spec.reader_txn_id != 0 {
            memtable_mgr
                .txn_mutations()
                .and_then(|registry| registry.memtable_positions(spec.reader_txn_id, mt.id()))
        } else {
            None
        };
        if let Some(mt_batch) = mt
            .read_with(|snapshot| {
                let source: &RecordBatch = &snapshot;
                if source.num_rows() == 0 {
                    return None;
                }

                // A memtable predating an ALTER keeps its insert-time column
                // layout while catalog indices move immediately: align to
                // the catalog BEFORE any consumer below, so catalog indices
                // resolve to the right columns and added columns fill with
                // their DEFAULT (the segment-side aligned read's semantics).
                // Alignment is row-preserving, so the deferred-DML tombstone
                // positions — row indices, layout-independent — filter
                // equivalently against the aligned batch; filtering before
                // alignment (the old order) evaluated predicates against the
                // stale layout after a DROP/ADD COLUMN.
                if memtable_batch_needs_alignment(source, &spec.table_schema) {
                    let aligned = match crate::storage::core::memtable::align_batch_to_schema(
                        source,
                        &spec.table_schema,
                    ) {
                        Ok(batch) => batch,
                        Err(e) => return Some(Err(e)),
                    };
                    return scan_aligned_memtable_batch(aligned, spec, &txn_positions);
                }
                scan_memtable_snapshot(snapshot, spec, &txn_positions)
            })
            .flatten()
        {
            let mt_batch = mt_batch?;
            if mt_batch.num_rows() == 0 {
                continue;
            }
            batches.push(mt_batch);
        }
    }

    Ok(batches)
}

/// Per-value inverted-index gather fast path. Triggered when the scan carries
/// an equality predicate on the leading cluster-key column. Gathers only the
/// matching rows via the index instead of scanning the whole resident set.
/// `watermark` excludes already-flushed rows (served by the segment scan).
/// Returns `None` when the fast path does not apply (no such predicate),
/// letting the caller fall back to the generic memtable scan.
pub fn scan_hot_store_by_key(
    store: &HotStore,
    spec: &ScanSpec,
    watermark: u64,
) -> Option<RecordBatch> {
    let key_idx = store.key_column_index();
    let key_value = spec
        .eq_predicates
        .iter()
        .find(|(idx, _)| *idx == key_idx)
        .map(|(_, v)| v)?;
    let partition_col_idx = spec
        .partition_info
        .as_ref()
        .and_then(|pi| pi.partition_key_col_idx);
    let partition_value = partition_col_idx.and_then(|idx| spec.eq_value_for_column(idx));

    // Equality/range predicates can be evaluated directly against resident
    // typed columns before gather. IN and arbitrary expressions retain the
    // generic batch path.
    let direct_filter_lookup = spec.in_predicates.is_empty() && spec.remaining_predicate.is_none();

    let lookup_projection = if direct_filter_lookup {
        spec.projection.as_deref()
    } else {
        None
    };

    let ordered_lookup = ordered_cluster_order_for_schema(store.schema(), spec);
    let reverse_lookup = ordered_lookup.as_ref().is_some_and(|order| order.desc);
    // The shared pushdown gate — the hot path must not re-derive limit
    // safety. It used to hand `spec.limit` straight to the lookup, so a
    // `SELECT DISTINCT ... LIMIT n` over a resident store truncated the
    // dedup input and aggregate shapes under-counted.
    let lookup_limit = if direct_filter_lookup {
        super::spec::pushdown_safe_limit(spec, ordered_lookup.is_some())
    } else {
        None
    }
    .map(|limit| limit.get());
    let batch = match store.lookup_filtered(
        key_value,
        partition_value,
        lookup_projection,
        watermark,
        reverse_lookup,
        lookup_limit,
        if direct_filter_lookup {
            &spec.eq_predicates
        } else {
            &[]
        },
        if direct_filter_lookup {
            &spec.range_predicates
        } else {
            &[]
        },
    ) {
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

    let filtered = if !direct_filter_lookup && spec.has_predicates() {
        // On evaluation failure, bail out of the fast path entirely so the
        // caller falls back to the generic memtable scan.
        match super::row_filter::apply_predicates(
            &batch,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        ) {
            Ok(filtered) => filtered,
            Err(e) => {
                tracing::warn!(error = %e, "hot-store predicate evaluation failed");
                return None;
            }
        }
    } else {
        batch
    };

    if direct_filter_lookup {
        return Some(filtered);
    }

    let ordered = apply_order_if_cluster_prefix_matches(filtered, spec);
    let projected = match &spec.projection {
        Some(proj) => apply_projection(&ordered, proj),
        None => ordered,
    };
    Some(projected)
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

/// Shared tail of every memtable scan path: empty → `None`, cluster-prefix
/// order normalization, projection.
fn finish_memtable_batch(batch: RecordBatch, spec: &ScanSpec) -> Option<Result<RecordBatch>> {
    if batch.num_rows() == 0 {
        return None;
    }
    let ordered = apply_order_if_cluster_prefix_matches(batch, spec);
    Some(Ok(if let Some(proj) = &spec.projection {
        apply_projection(&ordered, proj)
    } else {
        ordered
    }))
}

/// A drifted (re-aligned) batch: alignment already materialized once, so
/// the transform pipeline consumes it by value — the untouched case moves
/// it instead of cloning again.
fn scan_aligned_memtable_batch(
    aligned: RecordBatch,
    spec: &ScanSpec,
    txn_positions: &Option<std::sync::Arc<std::collections::BTreeSet<u32>>>,
) -> Option<Result<RecordBatch>> {
    if let Some(positions) = txn_positions {
        match crate::storage::core::txn_mutations::filter_batch_excluding_positions(
            &aligned, positions,
        ) {
            Ok(filtered) => return scan_filtered_memtable_batch(filtered, spec),
            Err(e) => return Some(Err(e)),
        }
    }
    if spec.has_predicates() {
        match super::row_filter::apply_predicates(
            &aligned,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        ) {
            Ok(filtered) => return finish_memtable_batch(filtered, spec),
            // Propagate: a failed predicate used to drop the whole memtable
            // batch from the result.
            Err(e) => return Some(Err(e)),
        }
    }
    finish_memtable_batch(aligned, spec)
}

/// A predicate- or txn-filtered batch: the filter already produced an owned
/// result, finish it.
fn scan_filtered_memtable_batch(
    filtered: RecordBatch,
    spec: &ScanSpec,
) -> Option<Result<RecordBatch>> {
    if !spec.has_predicates() {
        return finish_memtable_batch(filtered, spec);
    }
    match super::row_filter::apply_predicates(
        &filtered,
        &spec.eq_predicates,
        &spec.range_predicates,
        &spec.in_predicates,
        spec.remaining_predicate.as_ref(),
    ) {
        Ok(narrowed) => finish_memtable_batch(narrowed, spec),
        Err(e) => Some(Err(e)),
    }
}

/// The untouched-snapshot fast path. Order normalization here only ever
/// leaves the batch unchanged or reverses it (`apply_order` never sorts),
/// and `reverse_batch` builds from a reference — so no full-width copy is
/// ever needed: a projection copies only the projected columns from the
/// borrowed snapshot, and without one the snapshot's ownership transfers
/// zero-copy when the memtable is the sole holder (one clone only against
/// a concurrent reader).
fn scan_memtable_snapshot(
    snapshot: std::sync::Arc<RecordBatch>,
    spec: &ScanSpec,
    txn_positions: &Option<std::sync::Arc<std::collections::BTreeSet<u32>>>,
) -> Option<Result<RecordBatch>> {
    let source: &RecordBatch = &snapshot;
    if let Some(positions) = txn_positions {
        match crate::storage::core::txn_mutations::filter_batch_excluding_positions(
            source, positions,
        ) {
            Ok(filtered) => return scan_filtered_memtable_batch(filtered, spec),
            Err(e) => return Some(Err(e)),
        }
    }
    if spec.has_predicates() {
        return match super::row_filter::apply_predicates(
            source,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        ) {
            Ok(filtered) => finish_memtable_batch(filtered, spec),
            Err(e) => Some(Err(e)),
        };
    }
    let needs_reverse = ordered_cluster_order_for_batch(source, spec).is_some_and(|order| {
        !is_batch_ordered_by(source, order.column_index, order.desc)
            && is_batch_ordered_by(source, order.column_index, !order.desc)
    });
    if needs_reverse {
        if let Some(reversed) = reverse_batch(source) {
            return finish_memtable_batch(reversed, spec);
        }
        // A type the reversal does not support keeps insert order — the
        // same fallback `apply_order_if_cluster_prefix_matches` makes.
    }
    if let Some(proj) = &spec.projection {
        return Some(Ok(apply_projection(source, proj)));
    }
    Some(Ok(
        Arc::try_unwrap(snapshot).unwrap_or_else(|shared| shared.as_ref().clone())
    ))
}

fn apply_order_if_cluster_prefix_matches(batch: RecordBatch, spec: &ScanSpec) -> RecordBatch {
    let Some(order) = ordered_cluster_order_for_batch(&batch, spec) else {
        return batch;
    };
    if is_batch_ordered_by(&batch, order.column_index, order.desc) {
        return batch;
    }
    if is_batch_ordered_by(&batch, order.column_index, !order.desc) {
        return reverse_batch(&batch).unwrap_or(batch);
    }
    batch
}

struct BatchOrder {
    column_index: usize,
    desc: bool,
}

fn ordered_cluster_order_for_batch(batch: &RecordBatch, spec: &ScanSpec) -> Option<BatchOrder> {
    ordered_cluster_order_for_schema(&batch.schema, spec)
}

fn ordered_cluster_order_for_schema(
    schema: &crate::types::batch::Schema,
    spec: &ScanSpec,
) -> Option<BatchOrder> {
    if !spec.has_order_by
        || spec.has_aggregate
        || spec.has_group_by
        || spec.has_join
        || spec.remaining_predicate.is_some()
        || spec.order_by.len() != 1
    {
        return None;
    }

    let cluster_key = spec.cluster_key.as_ref()?;
    let order = spec.order_by.first()?;
    let order_col_name = schema.columns.get(order.column_index)?.name.as_str();
    let order_key_pos = cluster_key
        .iter()
        .position(|key| key.eq_ignore_ascii_case(order_col_name))?;
    if order_key_pos == 0 {
        return None;
    }

    for key in cluster_key.iter().take(order_key_pos) {
        let key_idx = schema.index_of(key)?;
        spec.eq_predicates.iter().find(|(idx, _)| *idx == key_idx)?;
    }

    Some(BatchOrder {
        column_index: order.column_index,
        desc: order.desc,
    })
}

fn is_batch_ordered_by(batch: &RecordBatch, column_index: usize, desc: bool) -> bool {
    if batch.num_rows() <= 1 {
        return true;
    }
    let Some(column) = batch.column(column_index) else {
        return false;
    };
    for row in 1..batch.num_rows() {
        let cmp = column.compare_at(row - 1, row);
        let violation = if desc {
            cmp == std::cmp::Ordering::Less
        } else {
            cmp == std::cmp::Ordering::Greater
        };
        if violation {
            return false;
        }
    }
    true
}

fn reverse_batch(batch: &RecordBatch) -> Option<RecordBatch> {
    let rows = (0..batch.num_rows() as u32).rev().collect::<Vec<_>>();
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&rows))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns).ok()
}

#[cfg(test)]
#[path = "memtable_test.rs"]
mod audit_tests;
