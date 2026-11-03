//! Metadata-driven folding engine for `Aggregate(single Scan)` queries.
//!
//! The engine answers aggregate queries — with or without GROUP BY — by
//! classifying every fold unit (segment / row group) and folding each one
//! either from metadata (zero decoded rows) or from a targeted decode of just
//! the columns the aggregates and group keys need:
//!
//! * `COUNT(*)`              -> Σ(row_count) − Σ(deleted_row_count)
//! * `COUNT(col)` (no del)   -> Σ(row_count) − Σ(`zone_map[col].null_count`)
//! * `MIN(col)` / `MAX(col)` -> min/max over candidate row groups'
//!   `zone_map[col].min_value` / `max_value`
//! * `SUM` / `AVG`           -> footer per-row-group `col_sums`, the partial
//!   sum cache, or a data fold
//!
//! GROUP BY keys are folded per unit: a unit whose key columns are all
//! single-valued in its zone maps (an invariant the day-partitioned /
//! cluster-sorted layouts guarantee for nearly every unit) collapses into one
//! group with zero decoding; split units decode only the key + argument
//! columns and group row by row. The no-GROUP-BY shape is the degenerate
//! single group and keeps its historical behaviour.
//!
//! All of this sits on top of the existing partition / row-group pruning
//! machinery, so a `WHERE` clause still prunes segments and row groups exactly
//! as a normal scan would.

//! This module owns query/segment orchestration. `row_groups` plans reads;
//! `grouping` assembles groups/results; `columns` resolves drift and run folds;
//! `accumulator` owns aggregate state; `sum_cache` owns cache lifecycle.

use std::path::Path;

use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::{
    aggregate_result_column_def, null_scalar_of_type,
};
use crate::sql::execution::scan::memtable as scan_memtable;
use crate::sql::execution::scan::partition;
use crate::sql::execution::scan::row_filter;
use crate::sql::execution::scan::row_group;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::codec::predicate_scan::{RunFold, RunPredicate};
use crate::storage::partition::pruning::{classify_clause, PredicateMatch, ZoneMap};
use crate::storage::segment::delete_bitmap;
use crate::storage::segment::SegmentReader;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

mod accumulator;
mod columns;
mod grouping;
mod row_groups;
mod sum_cache;
use accumulator::*;
use columns::*;
use grouping::*;
use row_groups::*;
use sum_cache::*;

pub(crate) use sum_cache::invalidate_segment_sum_cache_under;

/// Compute the result of an `Aggregate(single Scan)` query by folding segment
/// metadata (and, where necessary, targeted column reads) per fold unit.
///
/// `group_by` are the (already logical-schema-indexed) GROUP BY expressions;
/// only bare column references are supported here — anything else declines.
/// Query-level shared state for one folded-aggregate evaluation: the
/// post-hot-mirror scan spec plus everything the per-segment fold needs.
/// Built once after the hot legs amend the spec; the per-segment fold and
/// its row-group worker read it immutably (the tiny column-index vectors
/// are cloned into the workers so their iterator patterns stay unchanged).
struct FoldPlan<'a> {
    spec: &'a ScanSpec,
    table_schema: &'a Schema,
    segment_mgr: &'a SegmentManager,
    aggregates: &'a [AggregateExpr],
    key_columns: Vec<usize>,
    key_null_values: Vec<ScalarValue>,
    aggregate_columns: Vec<Option<usize>>,
    grouped: bool,
    has_sum: bool,
    has_avg: bool,
    needs_data_fold: bool,
    run_folds_enabled: bool,
    source_columns: Vec<usize>,
    predicate: crate::storage::partition::pruning::RowGroupPredicate,
    bloom_checks: Vec<(usize, ScalarValue)>,
    in_checks: Vec<&'a crate::sql::execution::scan::spec::InPredicate>,
    set_checks: Vec<(usize, ScalarValue)>,
    set_in_checks: Vec<&'a crate::sql::execution::scan::spec::InPredicate>,
}

/// Fold aggregates straight from segment metadata (zone maps, footer row
/// counts, cached partial sums) instead of decoding row batches; the caller
/// falls back to the regular aggregate pipeline on `Ok(None)`: the
/// no-GROUP-BY shape returns a single-row batch, a grouped shape one row per
/// group in first-seen order. Split into
/// [`fold_one_segment`] (per-segment gates + metadata/cache folds) and
/// [`fold_row_groups`] (row-group work planning and execution).
#[allow(clippy::too_many_arguments)]
pub fn compute_folded_aggregate(
    spec: &ScanSpec,
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
    table_meta: &TableMeta,
    table_schema: &Schema,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    hot_mgr: &crate::storage::core::hot_store::HotStoreManager,
    db: &str,
) -> Result<Option<RecordBatch>> {
    // Read-time TTL filtering is row-dependent and metadata folds cannot see
    // filters the TTL column then aggregates). This gate used to live in the
    // scan executor's routing; it belongs to the engine itself now that the
    // folded operator calls the engine directly.
    if table_meta.ttl.is_some() {
        return Ok(None);
    }
    // Cluster-key point shape (equality on the leading cluster column): the
    // generic pipeline's ClusterKeyRangeRead binary-searches the sorted key
    // and reads a sliver of the data, while this path would fold every
    // zone-map-surviving row group. Decline and let the caller fall back.
    if let Some(leading) = spec.cluster_key.as_ref().and_then(|keys| keys.first()) {
        if let Some(column) = table_schema.index_of(leading) {
            if spec.eq_value_for_column(column).is_some() {
                return Ok(None);
            }
        }
    }
    if !aggregate_args_supported(aggregates, table_schema) {
        return Ok(None);
    }
    // GROUP BY keys must be bare columns: unit-level folding derives each
    // unit's key from zone maps / defaults, and per-row folding extracts it
    // straight from the decoded column. Expression keys decline.
    let mut key_columns: Vec<usize> = Vec::with_capacity(group_by.len());
    for expr in group_by {
        match expr {
            LogicalExpr::Column(index) => {
                if !key_columns.contains(index) {
                    key_columns.push(*index);
                }
            }
            _ => return Ok(None),
        }
    }
    let grouped = !key_columns.is_empty();
    let key_null_values: Vec<ScalarValue> = key_columns
        .iter()
        .map(|&index| {
            ScalarValue::null_for_type(
                table_schema
                    .columns
                    .get(index)
                    .map(|column| column.data_type)
                    .unwrap_or(DataType::VarChar),
            )
        })
        .collect();
    let aggregate_columns: Vec<Option<usize>> = aggregates
        .iter()
        .map(|aggregate| match &*aggregate.arg {
            LogicalExpr::Column(index) => Some(*index),
            LogicalExpr::Wildcard => None,
            _ => None,
        })
        .collect();
    let template = aggregates
        .iter()
        .zip(&aggregate_columns)
        .map(|(aggregate, column)| AggAccum::new(aggregate.func, *column, table_schema))
        .collect::<Vec<_>>();
    let mut groups = GroupFold::new(template, grouped);

    // The spec becomes local: the hybrid mirror leg below amends it with the
    // complementary `<= covers` range clause, like the streaming cursor does.
    let mut spec = spec.clone();

    // Hot-store legs, mirroring the streaming cursor's initialize: a fully
    // resident mirror holds the scan's entire row set (then it is the whole
    // answer — no segments, no memtables), and a hybrid mirror owns the rows
    // above its coverage bound (then every other leg runs bounded to the
    // complementary range so the two sides partition the table exactly).
    let mut hot_spec = spec.clone();
    let mut needed_columns: Vec<usize> = Vec::new();
    for column in key_columns.iter().chain(aggregate_columns.iter().flatten()) {
        if !needed_columns.contains(column) {
            needed_columns.push(*column);
        }
    }
    if !needed_columns.is_empty() {
        hot_spec.projection = Some(needed_columns);
    }
    if let Some(batch) =
        scan_memtable::scan_resident_hot_store(hot_mgr, memtable_mgr, db, table_meta, &hot_spec)
    {
        if fold_named_batch(
            &mut groups,
            &key_columns,
            &aggregate_columns,
            table_schema,
            &batch,
        )?
        .is_none()
        {
            return Ok(None);
        }
        return Ok(Some(finalize_folded(
            groups,
            &key_columns,
            aggregates,
            table_schema,
        )?));
    }
    let mut hybrid_mirror_batch = None;
    if spec.hot_mirror_bound.is_none() {
        if let Some((batch, pk_idx, covers)) =
            scan_memtable::hybrid_hot_mirror_scan(hot_mgr, memtable_mgr, db, table_meta, &hot_spec)
        {
            spec.range_predicates
                .push(crate::storage::partition::pruning::RangeClause {
                    column_index: pk_idx,
                    op: crate::storage::partition::pruning::RangeOp::Le,
                    value: covers.clone(),
                });
            spec.hot_mirror_bound = Some((pk_idx, covers));
            hybrid_mirror_batch = Some(batch);
        }
    }

    let segment_files = if table_dir.as_os_str().is_empty() || !table_dir.exists() {
        Vec::new()
    } else {
        partition::prune_segments_by_partition(
            SegmentManager::discover_segments(table_dir)?,
            &spec,
            Some(table_meta),
        )
    };
    let predicate = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(&spec);
    let in_checks = row_group::build_in_check_list(&spec);
    let set_checks = row_group::build_set_check_list(&spec);
    let set_in_checks = row_group::build_set_in_check_list(&spec);
    let source_columns = aggregate_source_columns(&spec, &aggregate_columns);

    let has_sum = groups
        .template
        .iter()
        .any(|a| a.func == crate::sql::planner::AggFunc::Sum && !a.sum_unsupported);
    // AVG has no zone-map answer either: row groups must fold from data
    // (batch folds, or the run-level fold planned below).
    let has_avg = groups
        .template
        .iter()
        .any(|a| a.func == crate::sql::planner::AggFunc::Avg && !a.avg_unsupported);
    let needs_data_fold = has_sum || has_avg;
    // Run-level folds aggregate a whole row group into one partial state and
    // cannot emit per-run groups, so grouped queries decode+fold instead.
    let run_folds_enabled = !grouped;

    let plan = FoldPlan {
        spec: &spec,
        table_schema,
        segment_mgr,
        aggregates,
        key_columns: key_columns.clone(),
        key_null_values: key_null_values.clone(),
        aggregate_columns: aggregate_columns.clone(),
        grouped,
        has_sum,
        has_avg,
        needs_data_fold,
        run_folds_enabled,
        source_columns: source_columns.clone(),
        predicate: predicate.clone(),
        bloom_checks: bloom_checks.clone(),
        in_checks: in_checks.clone(),
        set_checks: set_checks.clone(),
        set_in_checks: set_in_checks.clone(),
    };
    for segment_path in &segment_files {
        if fold_one_segment(&plan, &mut groups, segment_path)?.is_none() {
            return Ok(None);
        }
    }

    let memtable_batches =
        scan_memtable::scan_memtables(memtable_mgr, hot_mgr, db, &spec.table_name, &spec)?;
    for memtable_batch in &memtable_batches {
        if fold_named_batch(
            &mut groups,
            &key_columns,
            &aggregate_columns,
            table_schema,
            memtable_batch,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }
    if let Some(mirror_batch) = &hybrid_mirror_batch {
        if fold_named_batch(
            &mut groups,
            &key_columns,
            &aggregate_columns,
            table_schema,
            mirror_batch,
        )?
        .is_none()
        {
            return Ok(None);
        }
    }

    Ok(Some(finalize_folded(
        groups,
        &key_columns,
        aggregates,
        table_schema,
    )?))
}

/// Segment-scoped alignment state handed from the per-segment gates to the
/// row-group worker: the opened reader plus the drift-resolved column map,
/// footer-aligned prune checks, and constant defaults.
struct SegmentFoldEnv<'a> {
    reader: &'a SegmentReader,
    aligned_checks: &'a row_filter::FooterPruneChecks,
    column_map: &'a std::collections::HashMap<usize, usize>,
    defaults: &'a std::collections::HashMap<usize, ScalarValue>,
}

/// Fold one segment into `groups` (or answer it entirely from metadata /
/// the partial-sum cache). `Ok(None)` declines the whole query (schema-drift
/// shapes the fast path cannot serve); `Ok(Some(()))` means the segment is
/// done and the caller continues with the next one.
fn fold_one_segment(
    plan: &FoldPlan<'_>,
    groups: &mut GroupFold,
    segment_path: &Path,
) -> Result<Option<()>> {
    let spec = plan.spec;
    let table_schema = plan.table_schema;
    let segment_mgr = plan.segment_mgr;
    let aggregates = plan.aggregates;
    // Column-index collections are tiny (a handful of entries); cloning them
    // keeps the moved body's `&x` iterator patterns byte-identical.
    let key_columns = plan.key_columns.clone();
    let key_null_values = plan.key_null_values.clone();
    let aggregate_columns = plan.aggregate_columns.clone();
    let grouped = plan.grouped;
    let has_sum = plan.has_sum;
    let has_avg = plan.has_avg;
    let needs_data_fold = plan.needs_data_fold;
    let predicate = plan.predicate.clone();
    let bloom_checks = plan.bloom_checks.clone();
    let in_checks = plan.in_checks.clone();
    let set_checks = plan.set_checks.clone();
    let set_in_checks = plan.set_in_checks.clone();

    // Footer sums/counts cannot see this transaction's deferred
    // tombstones; treating the metadata as absent routes the segment
    // through the overlay-aware reader below.
    let meta = if segment_mgr.txn_has_segment_overlay(segment_path, spec.reader_txn_id) {
        None
    } else {
        segment_mgr.segment_meta(segment_path)?
    };
    let meta_deleted = meta
        .as_ref()
        .map(|m| m.deleted_row_count)
        .unwrap_or(u64::MAX);
    let meta_rows = meta.as_ref().map(|m| m.row_count).unwrap_or(0);
    let pruned = match meta.as_ref() {
        Some(m) => {
            // A predicate on a column this segment predates cannot prune:
            // the footer has no zone map to consult (every row logically
            // carries the column's DEFAULT). Keep the segment as a
            // candidate — the reader gate below routes drifted segments
            // through the regular pipeline, which evaluates the predicate
            // on the default-filled batch.
            let clause_columns: Vec<usize> = predicate
                .clauses
                .iter()
                .map(|clause| clause.column_index)
                .collect();
            if row_filter::any_predicate_column_absent(
                table_schema,
                m.footer_schema(),
                &clause_columns,
            ) {
                false
            } else {
                // Sidecar zone maps follow the segment's footer column
                // order; re-index the predicate onto it before comparing.
                let aligned_predicate = row_filter::align_predicate_to_footer(
                    table_schema,
                    m.footer_schema(),
                    &predicate,
                )?;
                !row_group::prune_segment_by_zone_map(&aligned_predicate, m.footer_zone_maps())
            }
        }
        None => false,
    };
    if pruned {
        return Ok(Some(()));
    }

    // The sidecar meta snapshot answers everything up to and including
    // the whole-segment fold below without opening the segment file —
    // the hot shapes (plain COUNT(*), day-partitioned GROUP BY) then
    // never touch a reader at all. The overlay case (no meta) opens the
    // overlay-aware reader here and keeps it for the row-group loop;
    // every other segment opens its reader lazily, right before the
    // row-group work that genuinely needs it.
    let mut overlay_reader: Option<std::sync::Arc<SegmentReader>> = None;
    let (footer_schema, seg_zone_maps, seg_row_count, seg_deleted, footer_sums) = match &meta {
        Some(m) => (
            m.footer_schema().clone(),
            m.footer_zone_maps().to_vec(),
            m.row_count,
            m.deleted_row_count,
            m.row_group_meta(0)
                .map(|row_group| row_group.col_sums.len() == row_group.zone_maps.len())
                .unwrap_or(false),
        ),
        None => {
            let reader = segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?;
            let view = (
                reader.schema().clone(),
                reader.segment_zone_maps().to_vec(),
                reader.row_count(),
                reader.deleted_row_count(),
                reader
                    .row_group_meta(0)
                    .map(|row_group| row_group.col_sums.len() == row_group.zone_maps.len())
                    .unwrap_or(false),
            );
            overlay_reader = Some(reader);
            view
        }
    };
    // Schema drift is resolved per column, by name — not by whole-schema
    // equality. A blanket decline made one ALTER (ADD/DROP/RENAME COLUMN)
    // permanently degrade every SUM/COUNT/MIN/MAX metadata fold to a full
    // scan until compaction rewrote the drifted segment. The fold below
    // is already name-aligned (`align_column_map` and friends resolve by
    // current + previous names), so:
    //
    // * RENAME COLUMN drift resolves through previous_names;
    // * DROP COLUMN drift leaves extra footer columns the query never
    //   mentions — name resolution ignores them;
    // * a column the footer genuinely predates (ADD COLUMN afterwards)
    //   carries its DEFAULT on every row of this segment. A constant
    //   (or absent → NULL) default folds mathematically below;
    //   DEFAULT CURRENT_TIMESTAMP is not constant across post-ALTER
    //   writes, so such segments still decline the fast path.
    //
    // Predicates keep the strict rule: a clause on a column the footer
    // predates would need default-filled evaluation, which this fast
    // path does not implement. Decline; the regular pipeline evaluates
    // the predicate on the materialized default batch.
    if row_filter::any_predicate_column_absent(
        table_schema,
        &footer_schema,
        &spec.all_predicate_columns(),
    ) {
        return Ok(None);
    }
    let mut aggregate_cols: Vec<usize> = Vec::new();
    for column in aggregate_columns.iter().flatten() {
        if !aggregate_cols.contains(column) {
            aggregate_cols.push(*column);
        }
    }
    let mut defaults: std::collections::HashMap<usize, ScalarValue> =
        std::collections::HashMap::new();
    let mut footer_cols: Vec<usize> = Vec::with_capacity(aggregate_cols.len());
    for &column in &aggregate_cols {
        match footer_column_drift(table_schema, &footer_schema, column) {
            FooterColumnDrift::Present => footer_cols.push(column),
            FooterColumnDrift::ConstantDefault(value) => {
                // SUM/AVG fold the default arithmetically; a literal the
                // accumulator cannot consume would be an invariant
                // violation, so decline instead of erroring mid-fold.
                if groups.template.iter().any(|accumulator| {
                    matches!(accumulator.func, AggFunc::Sum | AggFunc::Avg)
                        && accumulator.column == Some(column)
                        && value.integer_i128().is_none()
                        && value.numeric_f64().is_none()
                }) {
                    return Ok(None);
                }
                defaults.insert(column, value);
            }
            FooterColumnDrift::NonConstantDefault => return Ok(None),
        }
    }
    // GROUP BY key columns drift-classify like argument columns. A key
    // column the footer predates with a constant DEFAULT is the strongest
    // case: the whole unit's key is that constant. A non-constant default
    // (CURRENT_TIMESTAMP) has no single key, so the query declines.
    for &key in &key_columns {
        if aggregate_cols.contains(&key) {
            continue;
        }
        match footer_column_drift(table_schema, &footer_schema, key) {
            FooterColumnDrift::Present => footer_cols.push(key),
            FooterColumnDrift::ConstantDefault(value) => {
                defaults.insert(key, value);
            }
            FooterColumnDrift::NonConstantDefault => return Ok(None),
        }
    }
    let column_map = row_filter::align_column_map(table_schema, &footer_schema, &footer_cols)?;
    let aligned_checks = row_filter::align_prune_checks_to_footer(
        table_schema,
        &footer_schema,
        &predicate,
        &bloom_checks,
        &in_checks,
        &set_checks,
        &set_in_checks,
    )?;
    // A SUM/AVG whose argument column this footer predates is answered
    // by the constant DEFAULT (default × rows / row count) — no footer
    // col_sums needed for it; resolved columns need the footer's
    // per-row-group sums plus a sum-supported type.
    let sums_answerable_from_footer = groups
        .template
        .iter()
        .filter(|a| matches!(a.func, AggFunc::Sum | AggFunc::Avg))
        .all(|a| {
            a.column
                .is_some_and(|column| defaults.contains_key(&column))
                || (footer_sums && crate::storage::segment::column_sum_supported(a.col_type))
        });
    // SUM has no zone-map representation: either the per-segment
    // partial-sum cache answers (COUNT/MIN/MAX fold from the zone maps
    // and SUM from the cache — no column reads at all), or the segment
    // streams below with sums folded from data. Folding metadata for a
    // streaming segment would skip SUM and then cache a zero delta, so
    // the two paths are mutually exclusive.
    //
    // AVG is excluded from the cache-hit fold: the cache stores SUM only,
    // and AVG needs both the sum and a non-null row count. Folding an Avg
    // accumulator with empty col_sums would leave avg_count at zero and
    // finalize to NULL, so a SUM+AVG query must stream whenever the
    // footer's per-row-group sums cannot answer both.
    // The partial-sum cache stores one whole-segment SUM per column — a
    // single-group answer. Grouped queries cannot use it (the segment's
    // sum would have to split per key value).
    let sum_cache = if has_sum
        && !has_avg
        && !grouped
        && !sums_answerable_from_footer
        && !spec.has_predicates()
        && meta_deleted == 0
        && meta.as_ref().is_some_and(|m| {
            aggregates_foldable_from_metadata(
                &groups.template,
                &aggregate_columns,
                &column_map,
                &defaults,
                m.footer_zone_maps(),
            )
        }) {
        segment_sum_cache_lookup(
            segment_path.to_string_lossy().as_ref(),
            meta_rows,
            meta_deleted,
            &sum_needed_columns(aggregates, &aggregate_columns, table_schema),
        )
    } else {
        None
    };
    if let Some(cached) = &sum_cache {
        if let Some(m) = &meta {
            fold_metadata(
                groups.single_mut(),
                &column_map,
                &defaults,
                m.footer_zone_maps(),
                m.row_count,
                &[],
            )?;
        }
        let accumulators = groups.single_mut();
        let mut sum_idx = 0usize;
        for accumulator in accumulators.iter_mut() {
            if accumulator.func != crate::sql::planner::AggFunc::Sum {
                continue;
            }
            if let Some(v) = cached.get(sum_idx) {
                accumulator.fold_cached_sum(v)?;
            }
            sum_idx += 1;
        }
        return Ok(Some(()));
    }

    // Cache miss: stream candidate row groups and fold every function
    // from data, so the stored partial sums are real values.
    // Any SUM needs data (zone maps carry no sums), predicates or not —
    // unless the footer's per-row-group sums answer it directly.
    // All-match row groups skip the predicate mask — every row matches —
    // so folding the raw batch there is still exact.
    let streaming_sum = !grouped
        && has_sum
        && !sums_answerable_from_footer
        && sum_cache.is_none()
        && seg_deleted == 0;
    // Whole-segment fold: no predicates, no deletes, every aggregate
    // answerable from the segment zone maps — and, for a grouped query,
    // every key column single-valued across the segment (the day-
    // partitioned layout guarantees this for the partition key, making
    // the whole GROUP BY one footer walk). Runs entirely on the meta
    // view; a hit never opens the segment file.
    if !spec.has_predicates()
        && seg_deleted == 0
        && !needs_data_fold
        && aggregates_foldable_from_metadata(
            &groups.template,
            &aggregate_columns,
            &column_map,
            &defaults,
            &seg_zone_maps,
        )
    {
        if let Some(key) = unit_group_key(
            &key_columns,
            &key_null_values,
            &defaults,
            &column_map,
            &seg_zone_maps,
        ) {
            fold_metadata(
                groups.group(key),
                &column_map,
                &defaults,
                &seg_zone_maps,
                seg_row_count,
                &[],
            )?;
            return Ok(Some(()));
        }
    }
    let reader = match overlay_reader {
        Some(reader) => reader,
        None => segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?,
    };

    let sum_before: Vec<(Option<i64>, Option<f64>)> = if has_sum && !grouped {
        groups.single().iter().map(|a| (a.sum_i, a.sum_f)).collect()
    } else {
        Vec::new()
    };

    let candidate_indices = if let Some(meta) = &meta {
        row_group::prune_row_group_meta(
            meta,
            &aligned_checks.predicate,
            &aligned_checks.bloom_checks,
            &aligned_checks.in_check_refs(),
            &aligned_checks.set_checks,
            &aligned_checks.set_in_check_refs(),
        )
        .candidate_indices
    } else {
        row_group::prune_row_groups(
            &reader,
            &aligned_checks.predicate,
            &aligned_checks.bloom_checks,
            &aligned_checks.in_check_refs(),
            &aligned_checks.set_checks,
            &aligned_checks.set_in_check_refs(),
            reader.segment_zone_maps(),
        )
        .candidate_indices
    };
    let env = SegmentFoldEnv {
        reader: &reader,
        aligned_checks: &aligned_checks,
        column_map: &column_map,
        defaults: &defaults,
    };
    fold_row_groups(
        plan,
        groups,
        &env,
        sums_answerable_from_footer,
        streaming_sum,
        &candidate_indices,
    )?;
    store_segment_sum_delta(
        plan,
        groups,
        sum_before,
        segment_path,
        meta_rows,
        &reader,
        sums_answerable_from_footer,
    )?;
    Ok(Some(()))
}

/// Answer a single-column `SELECT DISTINCT` from row-group zone maps.
///
/// Row groups whose zone map carries one non-null value (`min == max`) and
/// whose segment has no deleted rows contribute that value without reading
/// column data — for append-clustered columns (e.g. `deal_date`) this covers
/// nearly every row group, so the query cost collapses from a full column
/// scan to a metadata walk. Multi-valued row groups and segments carrying
/// deletes stream their column and fold per value, exactly like the simple
/// aggregate path folds MIN/MAX. Live memtable / hot-store rows are folded
/// via `scan_memtables`. Duplicates are harmless (DISTINCT is idempotent),
/// so overlapping sources cannot corrupt the result — only missing sources
/// could, and those match what the regular scan pipeline reads.
///
/// Returns `None` when the fast path declines (schema mismatch, unexpected
/// zone maps), and the caller falls back to the regular scan pipeline.
#[allow(clippy::too_many_arguments)]
pub fn compute_distinct_column(
    spec: &ScanSpec,
    table_meta: &TableMeta,
    table_schema: &Schema,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    hot_mgr: &crate::storage::core::hot_store::HotStoreManager,
    db: &str,
    column_name: &str,
) -> Result<Option<RecordBatch>> {
    // Zone-map shortcuts are only exact without predicates; the planner only
    // sets `distinct_column` for predicate-free scans, but stay defensive.
    if spec.has_predicates() {
        return Ok(None);
    }
    let Some(col_def_idx) = table_schema
        .columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(column_name))
    else {
        return Ok(None);
    };
    let col_type = table_schema.columns[col_def_idx].data_type;
    let null_value = ScalarValue::null_for_type(col_type);

    let mut seen: std::collections::HashSet<ScalarValue> = std::collections::HashSet::new();
    let mut ordered: Vec<ScalarValue> = Vec::new();
    fn insert(
        seen: &mut std::collections::HashSet<ScalarValue>,
        ordered: &mut Vec<ScalarValue>,
        value: ScalarValue,
    ) {
        if seen.insert(value.clone()) {
            ordered.push(value);
        }
    }

    let segment_files = if table_dir.as_os_str().is_empty() || !table_dir.exists() {
        Vec::new()
    } else {
        partition::prune_segments_by_partition(
            SegmentManager::discover_segments(table_dir)?,
            spec,
            Some(table_meta),
        )
    };

    for segment_path in &segment_files {
        let reader = segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?;
        let Some(col_idx) = reader.schema().index_of(column_name) else {
            return Ok(None);
        };
        let segment_has_deletes = reader.deleted_row_count() > 0;
        let bitmap = reader.delete_bitmap();
        let bitmap_is_empty = bitmap.is_empty();

        for rg_idx in 0..reader.num_row_groups() {
            let zone_maps = reader.zone_maps(rg_idx);
            let Some(zone_map) = zone_maps.get(col_idx) else {
                return Ok(None);
            };
            let single_valued = !zone_map.min_value.is_null()
                && !zone_map.max_value.is_null()
                && zone_map.min_value == zone_map.max_value;
            if single_valued && !segment_has_deletes {
                insert(&mut seen, &mut ordered, zone_map.min_value.clone());
                if zone_map.null_count > 0 {
                    insert(&mut seen, &mut ordered, null_value.clone());
                }
                continue;
            }
            let mut batch = reader.read_row_group_with_projection(rg_idx, &[col_idx])?;
            if !bitmap_is_empty {
                let row_start = reader.row_group_start_row(rg_idx);
                batch = delete_bitmap::filter_batch(batch, row_start, bitmap)?;
            }
            let Some(column) = batch.column(0) else {
                return Ok(None);
            };
            for row in 0..batch.num_rows() {
                insert(&mut seen, &mut ordered, column.scalar(row));
            }
        }
    }

    let memtable_batches =
        scan_memtable::scan_memtables(memtable_mgr, hot_mgr, db, &spec.table_name, spec)?;
    for batch in &memtable_batches {
        let Some(idx) = batch.schema.index_of(column_name) else {
            return Ok(None);
        };
        let Some(column) = batch.column(idx) else {
            return Ok(None);
        };
        for row in 0..batch.num_rows() {
            insert(&mut seen, &mut ordered, column.scalar(row));
        }
    }

    let schema = Schema::new(vec![table_schema.columns[col_def_idx].clone()]);
    let mut batch = RecordBatch::new(schema);
    for value in &ordered {
        batch
            .column_mut(0)
            .ok_or_else(|| crate::common::error::TaibaiError::Internal("distinct col".into()))?
            .push(value)?;
    }
    Ok(Some(batch))
}

#[cfg(test)]
#[path = "aggregate_meta_test.rs"]
mod tests;
