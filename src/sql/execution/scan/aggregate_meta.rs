//! Metadata-driven folding engine for `Aggregate(single Scan)` queries.
//!
//! The engine answers aggregate queries — with or without GROUP BY — by
//! classifying every fold unit (segment / row group) and folding each one
//! either from metadata (zero decoded rows) or from a targeted decode of just
//! the columns the aggregates and group keys need:
//!
//! * `COUNT(*)`              -> Σ(row_count) − Σ(deleted_row_count)
//! * `COUNT(col)` (no del)   -> Σ(row_count) − Σ(zone_map[col].null_count)
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

/// Compute the result of an `Aggregate(single Scan)` query by folding segment
/// metadata (and, where necessary, targeted column reads) per fold unit.
///
/// `group_by` are the (already logical-schema-indexed) GROUP BY expressions;
/// only bare column references are supported here — anything else declines.
/// `aggregates` are the aggregate expressions from the SELECT list. The
/// no-GROUP-BY shape returns a single-row batch; a grouped shape returns one
/// row per group in first-seen order. `Ok(None)` declines and the caller
/// falls back to the regular aggregate pipeline.
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
    // it; a table with an active TTL keeps the regular scan pipeline (which
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
    if let Some(batch) = scan_memtable::scan_resident_hot_store(
        hot_mgr,
        memtable_mgr,
        db,
        table_meta,
        &hot_spec,
    ) {
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
        if let Some((batch, pk_idx, covers)) = scan_memtable::hybrid_hot_mirror_scan(
            hot_mgr,
            memtable_mgr,
            db,
            table_meta,
            &hot_spec,
        ) {
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

    for segment_path in &segment_files {
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
                if row_filter::any_predicate_column_absent(table_schema, m.footer_schema(), &clause_columns)
                {
                    false
                } else {
                    // Sidecar zone maps follow the segment's footer column
                    // order; re-index the predicate onto it before comparing.
                    let aligned_predicate =
                        row_filter::align_predicate_to_footer(table_schema, m.footer_schema(), &predicate)?;
                    !row_group::prune_segment_by_zone_map(&aligned_predicate, m.footer_zone_maps())
                }
            }
            None => false,
        };
        if pruned {
            continue;
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
                m.footer_row_groups()
                    .first()
                    .is_some_and(|rg| rg.col_sums.len() == rg.zone_maps.len()),
            ),
            None => {
                let reader = segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?;
                let view = (
                    reader.schema().clone(),
                    reader.segment_zone_maps().to_vec(),
                    reader.row_count(),
                    reader.deleted_row_count(),
                    reader
                        .footer_row_groups()
                        .first()
                        .is_some_and(|rg| rg.col_sums.len() == rg.zone_maps.len()),
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
                a.column.is_some_and(|column| defaults.contains_key(&column))
                    || (footer_sums
                        && crate::storage::segment::column_sum_supported(a.col_type))
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
            continue;
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
                continue;
            }
        }
        let reader = match overlay_reader {
            Some(reader) => reader,
            None => segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?,
        };

        let sum_before: Vec<(Option<i64>, Option<f64>)> = if has_sum && !grouped {
            groups
                .single()
                .iter()
                .map(|a| (a.sum_i, a.sum_f))
                .collect()
        } else {
            Vec::new()
        };

        let candidate_indices = if let Some(meta) = &meta {
            row_group::prune_row_group_meta(
                meta.footer_row_groups(),
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
        let row_groups = reader.footer_row_groups();
        let mut candidates = vec![false; row_groups.len()];
        for index in candidate_indices {
            if let Some(candidate) = candidates.get_mut(index) {
                *candidate = true;
            }
        }
        let mut row_start = 0u64;

        // Build the per-row-group work list first: metadata-only folds run
        // inline; data folds read their batches in parallel chunks (reads
        // dominate the cold path — 600+ row groups), then fold sequentially
        // in candidate order. Count/Sum/Min/Max are all commutative, so the
        // fold order only needs to be deterministic, which the ordered
        // collect below guarantees.
        struct DataFold {
            row_group_index: usize,
            row_start: u64,
            run_fold: Option<RunFoldPlan>,
            /// Columns the batch fold reads for this row group. All-vacuous
            /// row groups drop their predicate columns (the mask would be
            /// all-true); `source_columns` otherwise.
            read_columns: Vec<usize>,
            /// Aggregate argument positions within `read_columns`.
            positions: Vec<Option<usize>>,
            /// GROUP BY key column positions within `read_columns` (empty for
            /// the no-GROUP-BY shape).
            key_positions: Vec<usize>,
            /// Whether the batch fold still evaluates predicates (false when
            /// every clause is zone-map vacuous for this row group).
            apply_mask: bool,
        }
        let mut meta_folds: Vec<(
            Vec<ScalarValue>,
            &[crate::storage::partition::pruning::ZoneMap],
            u64,
            &[i64],
        )> = Vec::new();
        let mut data_folds: Vec<DataFold> = Vec::new();

        // Per-clause zone-map classification is only sound without IN /
        // remaining predicates (they need per-row evaluation and extra
        // columns).
        let clause_vacuity_ok = reader.delete_bitmap().is_empty()
            && spec.in_predicates.is_empty()
            && spec.remaining_predicate.is_none();

        for (row_group_index, row_group_meta) in row_groups.iter().enumerate() {
            if !candidates[row_group_index] {
                row_start += row_group_meta.row_count;
                continue;
            }

            let predicate_match =
                if spec.in_predicates.is_empty() && spec.remaining_predicate.is_none() {
                    // Clauses were re-indexed onto this footer's column
                    // order, so they classify against the right zone maps.
                    aligned_checks
                        .predicate
                        .classify_row_group(&row_group_meta.zone_maps)
                } else {
                    crate::storage::partition::pruning::PredicateMatch::Partial
                };
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::None {
                row_start += row_group_meta.row_count;
                continue;
            }
            // The metadata-only fold cannot produce SUM or AVG — unless the
            // footer's per-row-group sums answer them (this row group's
            // extension present; the argument types were checked above) or
            // the argument column carries a constant DEFAULT this segment
            // predates (folded arithmetically). A streaming_sum segment
            // folds every function from data instead.
            let rg_sums_available = sums_answerable_from_footer
                && groups
                    .template
                    .iter()
                    .filter(|a| matches!(a.func, AggFunc::Sum | AggFunc::Avg))
                    .all(|a| {
                        a.column.is_some_and(|column| defaults.contains_key(&column))
                            || (!row_group_meta.zone_maps.is_empty()
                                && row_group_meta.col_sums.len()
                                    == row_group_meta.zone_maps.len())
                    });
            // A grouped metadata fold additionally needs the unit to collapse
            // into one group: every key column single-valued (or constant
            // default) in this row group's zone maps.
            let rg_unit_key = unit_group_key(
                &key_columns,
                &key_null_values,
                &defaults,
                &column_map,
                &row_group_meta.zone_maps,
            );
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::All
                && reader.delete_bitmap().is_empty()
                && !streaming_sum
                && rg_unit_key.is_some()
                && aggregates_foldable_from_metadata(
                    &groups.template,
                    &aggregate_columns,
                    &column_map,
                    &defaults,
                    &row_group_meta.zone_maps,
                )
                && (!has_sum && !has_avg || rg_sums_available)
            {
                meta_folds.push((
                    rg_unit_key
                        .expect("unit key checked above"),
                    &row_group_meta.zone_maps,
                    row_group_meta.row_count,
                    &row_group_meta.col_sums,
                ));
                row_start += row_group_meta.row_count;
                continue;
            }

            // Classify each clause against this row group's zone maps: All
            // clauses are vacuous (every row satisfies them — no mask work,
            // no predicate column read), Partial clauses must share one
            // column to be run-foldable. Clauses keep their catalog indices
            // for the fold column; the zone-map lookup uses the footer index.
            let mut fold_column: Option<usize> = None;
            let mut run_predicates: Vec<RunPredicate> = Vec::new();
            let mut discriminating = false;
            let mut classifiable = clause_vacuity_ok;
            if clause_vacuity_ok {
                for (clause, aligned_clause) in predicate
                    .clauses
                    .iter()
                    .zip(aligned_checks.predicate.clauses.iter())
                {
                    let Some(zone_map) = row_group_meta.zone_maps.get(aligned_clause.column_index)
                    else {
                        classifiable = false;
                        break;
                    };
                    match classify_clause(clause, zone_map) {
                        PredicateMatch::All => {}
                        PredicateMatch::None => {
                            // classify_row_group above already skipped None
                            // groups; keep the batch fold defensively.
                            classifiable = false;
                            break;
                        }
                        PredicateMatch::Partial => {
                            if fold_column.is_some_and(|column| column != clause.column_index) {
                                classifiable = false;
                                break;
                            }
                            fold_column = Some(clause.column_index);
                            run_predicates.push(RunPredicate::new(clause.op, &clause.value));
                            discriminating = true;
                        }
                    }
                }
            }

            let mut run_fold = None;
            if run_folds_enabled && classifiable && discriminating {
                run_fold = plan_run_fold_on_column(
                    fold_column,
                    run_predicates,
                    &groups.template,
                    table_schema,
                    &reader,
                );
            } else if run_folds_enabled && classifiable && !predicate.clauses.is_empty() {
                // Every clause is vacuous. When the aggregates share one
                // i64-domain argument column, folding its runs answers them
                // without decoding anything (the encoding sniff falls back
                // to the reduced batch read below for non-Rle columns).
                run_fold = plan_vacuous_run_fold(
                    &aggregate_columns,
                    &groups.template,
                    table_schema,
                    &column_map,
                    &row_group_meta.zone_maps,
                    &reader,
                );
            }

            // Batch-fold read set: all-vacuous row groups need only the
            // aggregate argument columns and skip the (all-true) mask.
            // Grouped queries always add their key columns (even constant-
            // default ones — `read_data_fold_batch` splices the constant, so
            // per-row keys come out uniform).
            let all_vacuous = classifiable && !discriminating;
            let (mut read_columns, apply_mask): (Vec<usize>, bool) = if all_vacuous {
                let mut columns: Vec<usize> = Vec::with_capacity(source_columns.len());
                for column in aggregate_columns.iter().flatten() {
                    if !columns.contains(column) {
                        columns.push(*column);
                    }
                }
                (columns, false)
            } else {
                (source_columns.clone(), spec.has_predicates())
            };
            for key in &key_columns {
                if !read_columns.contains(key) {
                    read_columns.push(*key);
                }
            }
            if read_columns.is_empty() {
                // No aggregate reads a column (pure COUNT(*)) and every
                // clause is vacuous: the footer answers it — minus this row
                // group's deleted rows, which the delete bitmap owns even
                // though nothing needs decoding. (This shape used to be
                // served by the dedicated COUNT(*) chain, which subtracted
                // deleted rows up front; unified routing made the subtraction
                // this branch's job.) Grouped queries always carry key
                // columns in the read set, so this is the no-GROUP-BY shape.
                let live_rows = if reader.delete_bitmap().is_empty() {
                    row_group_meta.row_count
                } else {
                    row_group_meta.row_count.saturating_sub(
                        delete_bitmap::row_range_deleted_count(
                            reader.delete_bitmap(),
                            row_start,
                            row_group_meta.row_count,
                        ),
                    )
                };
                meta_folds.push((
                    Vec::new(),
                    &row_group_meta.zone_maps,
                    live_rows,
                    &row_group_meta.col_sums,
                ));
                row_start += row_group_meta.row_count;
                continue;
            }
            let positions = aggregate_columns
                .iter()
                .map(|column| {
                    column.and_then(|col| read_columns.iter().position(|&read| read == col))
                })
                .collect();
            let key_positions = key_columns
                .iter()
                .map(|key| {
                    read_columns
                        .iter()
                        .position(|&read| read == *key)
                        .expect("key columns appended to the read set")
                })
                .collect();

            data_folds.push(DataFold {
                row_group_index,
                row_start,
                run_fold,
                read_columns,
                positions,
                key_positions,
                apply_mask,
            });
            row_start += row_group_meta.row_count;
        }

        for (key, zone_maps, row_count, col_sums) in meta_folds {
            fold_metadata(
                groups.group(key),
                &column_map,
                &defaults,
                zone_maps,
                row_count,
                col_sums,
            )?;
        }

        let delete_bitmap_is_empty = reader.delete_bitmap().is_empty();
        // One outcome per data fold: either the run-level fold succeeded, or
        // the row group went through the regular decode+mask batch fold
        // (including the case where the fold column turned out not to be
        // Rle/DeltaRle after the sniff).
        enum RgOutcome {
            Runs(RunFold),
            Batch(RecordBatch),
        }
        for chunk in data_folds.chunks(32) {
            let outcomes: Vec<Result<RgOutcome>> = chunk
                .par_iter()
                .map(|fold| {
                    if let Some(plan) = &fold.run_fold {
                        let raw =
                            reader.read_column_raw(fold.row_group_index, plan.segment_column)?;
                        if let Some(folded) =
                            crate::storage::codec::predicate_scan::fold_runs_from_raw(
                                &raw,
                                &plan.predicates,
                            )
                        {
                            return folded.map(RgOutcome::Runs);
                        }
                    }
                    // fold.read_columns carries catalog indices: translate
                    // onto this segment's footer layout for the physical
                    // read. The batch keeps the request order, so the
                    // positional mapping in `fold.positions` still holds.
                    // Columns the footer predates splice in as their
                    // constant DEFAULT instead of decoding.
                    let mut batch = read_data_fold_batch(
                        &reader,
                        table_schema,
                        fold.row_group_index,
                        &fold.read_columns,
                        &defaults,
                    )?;
                    if !delete_bitmap_is_empty {
                        batch = delete_bitmap::filter_batch(
                            batch,
                            fold.row_start,
                            reader.delete_bitmap(),
                        )?;
                    }
                    if fold.apply_mask {
                        batch = row_filter::apply_predicates_with_column_map(
                            &batch,
                            &fold.read_columns,
                            &spec.eq_predicates,
                            &spec.range_predicates,
                            &spec.in_predicates,
                            spec.remaining_predicate.as_ref(),
                        )?;
                    }
                    Ok(RgOutcome::Batch(batch))
                })
                .collect();
            for (fold, outcome) in chunk.iter().zip(outcomes) {
                match outcome? {
                    RgOutcome::Runs(out) => {
                        let plan = fold
                            .run_fold
                            .as_ref()
                            .expect("run outcome implies a run fold plan");
                        let col_type = table_schema.columns[plan.fold_column].data_type;
                        fold_run_fold(groups.single_mut(), col_type, &out)?;
                    }
                    RgOutcome::Batch(batch) => {
                        fold_grouped_data_batch(
                            &mut groups,
                            &fold.key_positions,
                            &fold.positions,
                            &batch,
                        )?;
                    }
                }
            }
        }

        if has_sum
            && !grouped
            && !sums_answerable_from_footer
            && !spec.has_predicates()
            && reader.deleted_row_count() == 0
        {
            let needed = sum_needed_columns(aggregates, &aggregate_columns, table_schema);
            // Only SUM accumulators: `needed` (and the cache key order)
            // covers SUM aggregates exclusively, so zipping all accumulators
            // would pair the cache entry with COUNT's empty delta and store
            // a zero sum.
            let deltas: Vec<SumCacheValue> = groups
                .single()
                .iter()
                .zip(sum_before.iter())
                .filter(|(a, _)| a.func == crate::sql::planner::AggFunc::Sum)
                .map(|(a, (i0, f0))| SumCacheValue {
                    i: match (a.sum_i, *i0) {
                        // Delta of two exact i64 partials; the difference can
                        // only exceed i64 when one side already wrapped inside
                        // the documented vectorized col_sum_i64 fold, in which
                        // case the cache entry is garbage either way.
                        (Some(v), Some(base)) => Some(v.wrapping_sub(base)),
                        (v, None) => v,
                        (None, Some(_)) => None,
                    },
                    f: match (a.sum_f, *f0) {
                        (Some(v), Some(base)) => Some(v - base),
                        (v, None) => v,
                        (None, Some(_)) => None,
                    },
                })
                .collect();
            segment_sum_cache_store(
                segment_path.to_string_lossy().as_ref(),
                meta_rows,
                reader.deleted_row_count(),
                &needed,
                &deltas,
            );
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

/// Fold a hot-store / memtable batch (catalog-schema-aligned, columns
/// resolved by name) into the group table. Returns `None` when a GROUP BY
/// key column is missing, so the caller declines the whole query.
fn fold_named_batch(
    groups: &mut GroupFold,
    key_columns: &[usize],
    aggregate_columns: &[Option<usize>],
    table_schema: &Schema,
    batch: &RecordBatch,
) -> Result<Option<()>> {
    let aggregate_positions: Vec<Option<usize>> = aggregate_columns
        .iter()
        .map(|column| {
            column.and_then(|index| {
                table_schema
                    .columns
                    .get(index)
                    .and_then(|column| batch.schema.index_of(&column.name))
            })
        })
        .collect();
    let key_positions = key_columns
        .iter()
        .map(|key| {
            table_schema
                .columns
                .get(*key)
                .and_then(|column| batch.schema.index_of(&column.name))
        })
        .collect::<Option<Vec<_>>>();
    // Batches aligned to the catalog schema always carry the key columns;
    // declining keeps the engine honest if that invariant ever breaks.
    let Some(key_positions) = key_positions else {
        return Ok(None);
    };
    fold_grouped_data_batch(groups, &key_positions, &aggregate_positions, batch)?;
    Ok(Some(()))
}

/// Assemble the final result batch: key columns under their source names,
/// then one column per aggregate — the same layout `grouped_output_schema`
/// gives the HashAggregate pipeline, so the Project above maps both
/// identically.
fn finalize_folded(
    groups: GroupFold,
    key_columns: &[usize],
    aggregates: &[AggregateExpr],
    table_schema: &Schema,
) -> Result<RecordBatch> {
    let mut columns: Vec<crate::types::batch::ColumnDef> =
        Vec::with_capacity(key_columns.len() + aggregates.len());
    for &key in key_columns {
        let def = &table_schema.columns[key];
        columns.push(crate::types::batch::ColumnDef::new(def.name.as_str(), def.data_type));
    }
    for aggregate in aggregates {
        columns.push(aggregate_result_column_def(aggregate, table_schema));
    }
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema);
    for (key, accumulators) in groups.into_rows() {
        for (index, value) in key.into_iter().enumerate() {
            batch
                .column_mut(index)
                .ok_or_else(|| TaibaiError::Internal("group key col".into()))?
                .push(&value)?;
        }
        for (offset, accumulator) in accumulators.into_iter().enumerate() {
            let index = key_columns.len() + offset;
            batch
                .column_mut(index)
                .ok_or_else(|| TaibaiError::Internal("agg col".into()))?
                .push(&accumulator.finalize())?;
        }
    }
    Ok(batch)
}

/// Columns the partial-sum cache needs: (column name, is-float) per SUM
/// aggregate, in aggregate order.
fn sum_needed_columns(
    aggregates: &[AggregateExpr],
    aggregate_columns: &[Option<usize>],
    table_schema: &Schema,
) -> Vec<(String, bool)> {
    aggregates
        .iter()
        .zip(aggregate_columns)
        .filter(|(agg, _)| agg.func == crate::sql::planner::AggFunc::Sum)
        .map(|(_agg, col)| {
            let def = col.and_then(|i| table_schema.columns.get(i));
            let name = def.map(|c| c.name.clone()).unwrap_or_default();
            let is_float = def
                .map(|c| matches!(c.data_type, DataType::Float32 | DataType::Float64))
                .unwrap_or(false);
            (name, is_float)
        })
        .collect()
}

fn aggregate_source_columns(spec: &ScanSpec, aggregate_columns: &[Option<usize>]) -> Vec<usize> {
    let mut columns = spec.all_predicate_columns();
    for column in aggregate_columns.iter().flatten() {
        if !columns.contains(column) {
            columns.push(*column);
        }
    }
    columns
}

fn fold_metadata(
    accumulators: &mut [AggAccum],
    column_map: &std::collections::HashMap<usize, usize>,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
    row_count: u64,
    col_sums: &[i64],
) -> Result<()> {
    for accumulator in accumulators {
        let default = accumulator.column.and_then(|column| defaults.get(&column));
        accumulator.fold_metadata(default, zone_maps, row_count, col_sums, column_map)?;
    }
    Ok(())
}

/// One accumulator set per distinct group key, in first-seen order over the
/// deterministic (segment → row group → row) unit traversal — the same order
/// the streaming aggregate pipeline would discover groups in. The no-GROUP-BY
/// shape is a single pre-created group with the empty key, so the folded
/// single-row behaviour falls out of the same machinery.
struct GroupFold {
    keys: Vec<Vec<ScalarValue>>,
    index: std::collections::HashMap<Vec<ScalarValue>, usize>,
    rows: Vec<Vec<AggAccum>>,
    grouped: bool,
    /// Prototype accumulator set cloned for every new group (shape queries
    /// like `has_sum` checks also read it).
    template: Vec<AggAccum>,
}

impl GroupFold {
    fn new(template: Vec<AggAccum>, grouped: bool) -> Self {
        let mut this = Self {
            keys: Vec::new(),
            index: std::collections::HashMap::new(),
            rows: Vec::new(),
            grouped,
            template,
        };
        if !grouped {
            this.keys.push(Vec::new());
            this.rows.push(this.template.clone());
        }
        this
    }

    /// The accumulator set for `key`, creating the group on first sight.
    /// The no-GROUP-BY shape ignores the key (there is exactly one group).
    fn group(&mut self, key: Vec<ScalarValue>) -> &mut Vec<AggAccum> {
        if !self.grouped {
            return &mut self.rows[0];
        }
        let index = match self.index.get(&key) {
            Some(&index) => index,
            None => {
                let index = self.keys.len();
                self.keys.push(key.clone());
                self.rows.push(self.template.clone());
                self.index.insert(key, index);
                index
            }
        };
        &mut self.rows[index]
    }

    /// The single accumulator set (no-GROUP-BY shape).
    fn single_mut(&mut self) -> &mut Vec<AggAccum> {
        debug_assert!(!self.grouped);
        &mut self.rows[0]
    }

    fn single(&self) -> &Vec<AggAccum> {
        debug_assert!(!self.grouped);
        &self.rows[0]
    }

    fn into_rows(self) -> impl Iterator<Item = (Vec<ScalarValue>, Vec<AggAccum>)> {
        self.keys.into_iter().zip(self.rows)
    }
}

/// What a fold unit's zone maps say about one GROUP BY key column.
enum KeyZone {
    /// Every row of the unit carries this exact value (all-NULL included).
    Single(ScalarValue),
    /// Rows split across values, or the statistics are missing: the unit
    /// must fold from data so each row lands in its own group.
    Split,
}

fn key_column_zone_value(
    key: usize,
    null_value: &ScalarValue,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
) -> KeyZone {
    // A key column the footer predates carries one constant DEFAULT on
    // every row: the unit's key is that constant.
    if let Some(default) = defaults.get(&key) {
        return KeyZone::Single(default.clone());
    }
    let Some(zone_map) = zone_maps.get(row_filter::mapped_column(column_map, key)) else {
        return KeyZone::Split;
    };
    if !zone_map.is_computed() {
        return KeyZone::Split;
    }
    if zone_map.min_value.is_null() && zone_map.max_value.is_null() {
        // `min_value` is only ever set from a non-null value, so both ends
        // NULL means the unit holds no non-null rows: one NULL group.
        return KeyZone::Single(null_value.clone());
    }
    if !zone_map.min_value.is_null()
        && !zone_map.max_value.is_null()
        && zone_map.min_value == zone_map.max_value
        && zone_map.null_count == 0
    {
        return KeyZone::Single(zone_map.min_value.clone());
    }
    // Mixed values, or value rows plus NULL rows: only a pure COUNT(*) could
    // split those arithmetically — keep it conservative and fold from data.
    KeyZone::Split
}

/// The single group key a whole fold unit collapses into, or `None` when any
/// key column splits across values (the unit must then fold from data). The
/// no-GROUP-BY shape always returns the empty key.
fn unit_group_key(
    key_columns: &[usize],
    key_null_values: &[ScalarValue],
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(key_columns.len());
    for (position, &column) in key_columns.iter().enumerate() {
        match key_column_zone_value(
            column,
            &key_null_values[position],
            defaults,
            column_map,
            zone_maps,
        ) {
            KeyZone::Single(value) => key.push(value),
            KeyZone::Split => return None,
        }
    }
    Some(key)
}

/// The single group key a decoded batch collapses into when every key column
/// is batch-singular (one value, or all NULL) — the common clustered case,
/// letting the whole batch fold vectorized into one group.
fn batch_group_key(batch: &RecordBatch, key_positions: &[usize]) -> Option<Vec<ScalarValue>> {
    let rows = batch.num_rows();
    if rows == 0 {
        return None;
    }
    let mut key = Vec::with_capacity(key_positions.len());
    for &position in key_positions {
        let values = batch.column(position)?;
        let data_type = batch.schema.columns.get(position)?.data_type;
        let non_null = values.col_count_non_null();
        if non_null == 0 {
            key.push(ScalarValue::null_for_type(data_type));
            continue;
        }
        if non_null != rows as u64 {
            return None;
        }
        let (Some(min), Some(max)) = (values.col_min(), values.col_max()) else {
            return None;
        };
        if min != max {
            return None;
        }
        key.push(min);
    }
    Some(key)
}

/// Fold one decoded fold-unit batch into the group table. Batch-singular key
/// sets fold vectorized into a single group; split keys fall to a per-row
/// loop that extracts each row's key and folds its argument values.
fn fold_grouped_data_batch(
    groups: &mut GroupFold,
    key_positions: &[usize],
    aggregate_positions: &[Option<usize>],
    batch: &RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if !groups.grouped {
        return fold_batch(groups.single_mut(), aggregate_positions, batch);
    }
    if let Some(key) = batch_group_key(batch, key_positions) {
        return fold_batch(groups.group(key), aggregate_positions, batch);
    }
    let rows = batch.num_rows();
    for row in 0..rows {
        let key: Vec<ScalarValue> = key_positions
            .iter()
            .map(|&position| batch.column(position).expect("key position checked").scalar(row))
            .collect();
        let accumulators = groups.group(key);
        for (accumulator, position) in accumulators.iter_mut().zip(aggregate_positions) {
            match position {
                None => accumulator.fold_row(None)?,
                Some(column) => accumulator.fold_row(Some(
                    &batch
                        .column(*column)
                        .expect("aggregate position checked")
                        .scalar(row),
                ))?,
            }
        }
    }
    Ok(())
}

/// How one catalog-space aggregate argument column relates to a segment
/// footer schema, resolved by name (current name, then previous names).
#[derive(Debug, PartialEq)]
enum FooterColumnDrift {
    /// The footer carries the column (possibly under a previous name, or
    /// with extra columns the query never mentions after DROP COLUMN).
    Present,
    /// The footer predates the column: every row of this segment logically
    /// carries this constant DEFAULT (an absent default is a constant NULL).
    ConstantDefault(ScalarValue),
    /// The footer predates the column and its DEFAULT is not a compile-time
    /// constant (CURRENT_TIMESTAMP) — the metadata fold cannot answer it.
    NonConstantDefault,
}

/// Classify one column of `table_schema` against `footer_schema`. Reuses the
/// catalog's `previous_names` exactly like the count fast paths and the
/// regular pipeline's name-based remapping (`align_indices_against_schema`).
fn footer_column_drift(
    table_schema: &Schema,
    footer_schema: &Schema,
    column: usize,
) -> FooterColumnDrift {
    let Some(col_def) = table_schema.columns.get(column) else {
        // Out-of-range catalog index: let the regular alignment surface its
        // own error instead of guessing here.
        return FooterColumnDrift::Present;
    };
    let mut names = Vec::with_capacity(col_def.previous_names.len() + 1);
    names.push(col_def.name.clone());
    names.extend(col_def.previous_names.iter().cloned());
    if names.iter().any(|name| footer_schema.index_of(name).is_some()) {
        return FooterColumnDrift::Present;
    }
    match &col_def.default_value {
        Some(crate::types::batch::ColumnDefaultValue::Literal(value)) => {
            FooterColumnDrift::ConstantDefault(value.clone())
        }
        Some(crate::types::batch::ColumnDefaultValue::CurrentTimestamp) => {
            FooterColumnDrift::NonConstantDefault
        }
        None => FooterColumnDrift::ConstantDefault(ScalarValue::null_for_type(
            col_def.data_type,
        )),
    }
}

/// The data-fold batch for one row group, in `read_columns` (catalog) order:
/// footer-resolvable columns decode from the row group, columns the footer
/// predates splice in as their constant DEFAULT — the same value
/// `read_row_group_columns_aligned` materializes on the regular path, so the
/// two paths can never disagree. Synthesis happens before the delete/mask
/// filters so the default column follows the surviving row count.
fn read_data_fold_batch(
    reader: &SegmentReader,
    table_schema: &Schema,
    row_group_index: usize,
    read_columns: &[usize],
    defaults: &std::collections::HashMap<usize, ScalarValue>,
) -> Result<RecordBatch> {
    let mut present: Vec<usize> = Vec::with_capacity(read_columns.len());
    for &column in read_columns {
        if !defaults.contains_key(&column) {
            present.push(column);
        }
    }
    let projected = if present.is_empty() {
        None
    } else {
        let aligned_read_columns =
            row_filter::align_indices_to_footer(table_schema, reader.schema(), &present)?;
        Some(reader.read_row_group_with_projection(
            row_group_index,
            &aligned_read_columns,
        )?)
    };
    let rows = projected
        .as_ref()
        .map(|batch| batch.num_rows())
        .unwrap_or_else(|| reader.row_group_rows(row_group_index) as usize);
    let mut present_columns = projected
        .map(|batch| batch.columns)
        .unwrap_or_else(Vec::new);
    let mut present_iter = present_columns.drain(..);
    let mut columns = Vec::with_capacity(read_columns.len());
    let mut col_defs = Vec::with_capacity(read_columns.len());
    for &column in read_columns {
        let col_def = table_schema
            .columns
            .get(column)
            .ok_or_else(|| {
                TaibaiError::Internal(format!(
                    "aggregate column index {column} out of range"
                ))
            })?
            .clone();
        if let Some(default) = defaults.get(&column) {
            columns.push(constant_scalar_column(col_def.data_type, default, rows)?);
        } else {
            columns.push(present_iter.next().ok_or_else(|| {
                TaibaiError::Internal(format!(
                    "data-fold column {column} missing from projected read"
                ))
            })?);
        }
        col_defs.push(col_def);
    }
    RecordBatch::try_new(Schema::new(col_defs), columns)
        .map_err(|e| TaibaiError::Internal(e.to_string()))
}

/// A full-length column whose every row holds the same scalar (the constant
/// DEFAULT of a column this segment predates).
fn constant_scalar_column(
    data_type: DataType,
    value: &ScalarValue,
    row_count: usize,
) -> Result<crate::types::array::ColumnArray> {
    let mut column = crate::types::array::ColumnArray::empty(data_type);
    for _ in 0..row_count {
        column.push(value)?;
    }
    Ok(column)
}

/// Whether every aggregate can fold from these zone maps: each argument
/// column's map must carry computed statistics. Bulk direct writes compute
/// zone maps for the partition/cluster key columns only and fill the rest
/// with defaults whose NULL min / zero null_count are indistinguishable
/// from a genuinely all-null row group — folding those from metadata would
/// miscount NULLs for COUNT(col) and return NULL for MIN/MAX (the same
/// defect once fixed for SUM/AVG). Such segments must fold from data.
/// Columns carrying a constant DEFAULT (the footer predates them) fold
/// arithmetically and are always answerable. COUNT(*) has no argument
/// column and is always foldable.
fn aggregates_foldable_from_metadata(
    accumulators: &[AggAccum],
    aggregate_columns: &[Option<usize>],
    column_map: &std::collections::HashMap<usize, usize>,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
) -> bool {
    accumulators
        .iter()
        .zip(aggregate_columns)
        .all(|(_, column)| match column {
            Some(col) => {
                defaults.contains_key(col)
                    || zone_maps
                        .get(row_filter::mapped_column(column_map, *col))
                        .is_some_and(ZoneMap::is_computed)
            }
            None => true,
        })
}

fn fold_batch(
    accumulators: &mut [AggAccum],
    aggregate_positions: &[Option<usize>],
    batch: &RecordBatch,
) -> Result<()> {
    for (accumulator, column) in accumulators.iter_mut().zip(aggregate_positions) {
        accumulator.fold_batch(batch, *column)?;
    }
    Ok(())
}

/// A row group's run-level fold plan: which column's encoded runs answer
/// every aggregate, and which predicates the runs must satisfy.
struct RunFoldPlan {
    /// Table-schema index of the folded column.
    fold_column: usize,
    /// Segment-schema index of the same column (name-mapped).
    segment_column: usize,
    predicates: Vec<RunPredicate>,
}

/// Finalize a discriminating run fold on `fold_column`: the column type
/// must be i64-representable, every aggregate must read it (COUNT(*) aside),
/// and the segment must still carry it (name-mapped).
fn plan_run_fold_on_column(
    fold_column: Option<usize>,
    predicates: Vec<RunPredicate>,
    accumulators: &[AggAccum],
    table_schema: &Schema,
    reader: &SegmentReader,
) -> Option<RunFoldPlan> {
    let fold_column = fold_column?;
    if !run_fold_type_ok(table_schema.columns.get(fold_column)?.data_type) {
        return None;
    }
    if accumulators
        .iter()
        .any(|a| a.column.is_some_and(|column| column != fold_column))
    {
        return None;
    }
    let segment_column = reader
        .schema()
        .index_of(&table_schema.columns.get(fold_column)?.name)?;
    Some(RunFoldPlan {
        fold_column,
        segment_column,
        predicates,
    })
}

/// Plan a run fold for a row group where every predicate is vacuous: the
/// aggregates must share one i64-domain argument column, and COUNT(*)
/// additionally requires that column null-free (run lengths count non-null
/// values only). No predicates — every run folds.
fn plan_vacuous_run_fold(
    aggregate_columns: &[Option<usize>],
    accumulators: &[AggAccum],
    table_schema: &Schema,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
    reader: &SegmentReader,
) -> Option<RunFoldPlan> {
    let mut distinct: Vec<usize> = Vec::new();
    for &column in aggregate_columns.iter().flatten() {
        if !distinct.contains(&column) {
            distinct.push(column);
        }
    }
    if distinct.len() != 1 {
        return None;
    }
    let column = distinct[0];
    if !run_fold_type_ok(table_schema.columns.get(column)?.data_type) {
        return None;
    }
    let has_count_star = accumulators.iter().any(|a| a.column.is_none());
    // A column the footer predates (folded via its constant DEFAULT) has no
    // runs and no zone map. Resolve by name before the zone-map lookup so
    // the identity fallback of `mapped_column` can never index an unrelated
    // footer slot.
    reader.schema().index_of(&table_schema.columns.get(column)?.name)?;
    let footer_column = row_filter::mapped_column(column_map, column);
    if has_count_star && zone_maps.get(footer_column)?.null_count != 0 {
        return None;
    }
    let segment_column = reader
        .schema()
        .index_of(&table_schema.columns.get(column)?.name)?;
    Some(RunFoldPlan {
        fold_column: column,
        segment_column,
        predicates: Vec::new(),
    })
}

/// Run folding compares and sums the raw i64 run values, so the column must
/// be i64-backed with an ordering the i64 view preserves (UInt64 excluded:
/// its u64 domain can wrap the i64 view).
fn run_fold_type_ok(dt: DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Decimal
            | DataType::Date
            | DataType::Time
            | DataType::Timestamp
            | DataType::Interval
    )
}

/// Fold one row group's run-level result into the accumulators. The shape
/// checks in [`plan_run_fold`] guarantee every accumulator reads the fold
/// column (or is COUNT(*)).
fn fold_run_fold(accumulators: &mut [AggAccum], col_type: DataType, out: &RunFold) -> Result<()> {
    for accumulator in accumulators.iter_mut() {
        match accumulator.func {
            AggFunc::Count => accumulator.count += out.matched_rows,
            AggFunc::Min => {
                if out.any {
                    accumulator.merge_min(raw_i64_scalar(col_type, out.min));
                }
            }
            AggFunc::Max => {
                if out.any {
                    accumulator.merge_max(raw_i64_scalar(col_type, out.max));
                }
            }
            AggFunc::Sum => {
                if out.any {
                    accumulator.sum_i = Some(
                        accumulator
                            .sum_i
                            .unwrap_or(0)
                            .checked_add(out.sum_i)
                            .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
                    );
                }
            }
            AggFunc::Avg => {
                if out.any {
                    accumulator.avg_sum_f =
                        Some(accumulator.avg_sum_f.unwrap_or(0.0) + out.sum_i as f64);
                    accumulator.avg_count += out.matched_rows;
                }
            }
        }
    }
    Ok(())
}

/// Reinterpret a raw run value as the column's scalar type. The i64 view is
/// exact for every [`run_fold_type_ok`] type (narrow ints round-trip through
/// the wider stored value by construction).
fn raw_i64_scalar(dt: DataType, value: i64) -> ScalarValue {
    match dt {
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
        DataType::Decimal => ScalarValue::Decimal(Some(value)),
        DataType::Date => ScalarValue::Date(Some(value as i32)),
        DataType::Time => ScalarValue::Time(Some(value)),
        DataType::Timestamp => ScalarValue::Timestamp(Some(value)),
        DataType::Interval => ScalarValue::Interval(Some(value)),
        _ => ScalarValue::Int64(Some(value)),
    }
}

#[derive(Clone)]
struct AggAccum {
    func: AggFunc,
    column: Option<usize>,
    count: u64,
    best_min: Option<ScalarValue>,
    best_max: Option<ScalarValue>,
    col_type: DataType,
    sum_i: Option<i64>,
    sum_f: Option<f64>,
    /// SUM over a column type the metadata fast path cannot answer
    /// (non-numeric argument columns).
    sum_unsupported: bool,
    /// AVG accumulators: per-batch sums accumulate as f64 (mirroring
    /// `Accumulator::Avg`), fed either from decoded batches or from exact
    /// per-row-group i64 run folds.
    avg_sum_f: Option<f64>,
    avg_count: u64,
    avg_unsupported: bool,
    /// DECIMAL scale of the argument column: sums accumulate in the raw
    /// scaled i64 domain, and finalize divides it back out.
    col_scale: i32,
}

impl AggAccum {
    fn new(func: AggFunc, column: Option<usize>, schema: &Schema) -> Self {
        let col_type = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.data_type)
            .unwrap_or(DataType::Int64);
        let col_scale = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.scale)
            .unwrap_or(0);
        let (sum_i, sum_f, sum_unsupported) = if func == AggFunc::Sum {
            match col_type {
                DataType::Float32 | DataType::Float64 => (None, None, false),
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    (None, None, false)
                }
                _ => (None, None, true),
            }
        } else {
            (None, None, false)
        };
        let avg_unsupported = func == AggFunc::Avg
            && !matches!(
                col_type,
                DataType::Int64
                    | DataType::Int32
                    | DataType::Int16
                    | DataType::Int8
                    | DataType::Float64
                    | DataType::Float32
                    | DataType::Decimal
            );
        Self {
            func,
            column,
            count: 0,
            best_min: None,
            best_max: None,
            col_type,
            sum_i,
            sum_f,
            sum_unsupported,
            avg_sum_f: None,
            avg_count: 0,
            avg_unsupported,
            col_scale,
        }
    }

    fn fold_metadata(
        &mut self,
        default: Option<&ScalarValue>,
        zone_maps: &[crate::storage::partition::pruning::ZoneMap],
        row_count: u64,
        col_sums: &[i64],
        column_map: &std::collections::HashMap<usize, usize>,
    ) -> Result<()> {
        let Some(column) = self.column else {
            if self.func == AggFunc::Count {
                self.count += row_count;
            }
            return Ok(());
        };
        // A column this segment's footer predates carries one constant
        // DEFAULT on every row: fold it arithmetically instead of indexing
        // the (absent) zone map / col_sum slot.
        if let Some(default) = default {
            return self.fold_constant_default(default, row_count);
        }
        // zone_maps / col_sums are indexed by the segment's footer column
        // order: resolve the catalog argument column onto it by name.
        let footer_column = row_filter::mapped_column(column_map, column);
        let Some(zone_map) = zone_maps.get(footer_column) else {
            return Ok(());
        };
        // An all-null row group contributes nothing to SUM/AVG — its footer
        // sum is 0, which must not materialize a zero from an empty
        // accumulator. `min_value` is only ever set from a non-null value, so
        // it doubles as a path-independent "has values" signal (zone map
        // `row_count` semantics differ between the column and bulk writers).
        let has_values = !zone_map.min_value.is_null();
        match self.func {
            AggFunc::Count => self.count += row_count.saturating_sub(zone_map.null_count),
            AggFunc::Min if !zone_map.min_value.is_null() => {
                self.merge_min(zone_map.min_value.clone())
            }
            AggFunc::Max if !zone_map.max_value.is_null() => {
                self.merge_max(zone_map.max_value.clone())
            }
            AggFunc::Sum if has_values => {
                if let Some(&sum) = col_sums.get(footer_column) {
                    self.add_i64(sum)?;
                }
            }
            AggFunc::Avg if has_values => {
                if let Some(&sum) = col_sums.get(footer_column) {
                    self.avg_sum_f = Some(self.avg_sum_f.unwrap_or(0.0) + sum as f64);
                    self.avg_count += row_count.saturating_sub(zone_map.null_count);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Fold `row_count` rows that all carry one constant DEFAULT (a column
    /// this segment's footer predates, e.g. SUM(new_col) over pre-ALTER
    /// rows). Semantics mirror the zone-map fold exactly:
    ///
    /// * callers only reach here for delete-free row groups, so `row_count`
    ///   is the live row count (deleted rows never contribute);
    /// * a NULL default is an all-NULL column: it contributes nothing —
    ///   COUNT(col) adds no row, SUM/MIN/MAX stay unmaterialized (the same
    ///   "footer sum 0 must not materialize a zero" rule as `has_values`);
    /// * SUM/AVG accumulate default × rows through the shared overflow
    ///   checked accumulation point (`add_i64`), reporting "SUM overflow"
    ///   instead of wrapping.
    fn fold_constant_default(&mut self, default: &ScalarValue, row_count: u64) -> Result<()> {
        if row_count == 0 || default.is_null() {
            return Ok(());
        }
        match self.func {
            AggFunc::Count => self.count += row_count,
            AggFunc::Min => self.merge_min(default.clone()),
            AggFunc::Max => self.merge_max(default.clone()),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(value) = default.numeric_f64() {
                        self.sum_f = Some(self.sum_f.unwrap_or(0.0) + value * row_count as f64);
                    }
                }
                _ => {
                    let value = default.integer_i128().ok_or_else(|| {
                        TaibaiError::Internal(format!(
                            "constant default {:?} not foldable as {} SUM",
                            default, self.col_type
                        ))
                    })?;
                    let product = value
                        .checked_mul(i128::from(row_count))
                        .and_then(|product| i64::try_from(product).ok())
                        .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?;
                    self.add_i64(product)?;
                }
            },
            AggFunc::Avg => {
                if let Some(value) = default.numeric_f64() {
                    self.avg_sum_f =
                        Some(self.avg_sum_f.unwrap_or(0.0) + value * row_count as f64);
                    self.avg_count += row_count;
                }
            }
        }
        Ok(())
    }

    /// The single integer-SUM accumulation point for this accumulator: every
    /// path (footer col_sums, data folds, cached partials) adds through it so
    /// overflow reports "SUM overflow" instead of silently wrapping — the
    /// same contract as the generic aggregate operator. The row-group
    /// partials fed in here can themselves have wrapped inside the
    /// documented vectorized `col_sum_i64` fold; this boundary catches
    /// cross-group / cross-segment accumulation overflow.
    fn add_i64(&mut self, part: i64) -> Result<()> {
        self.sum_i = Some(
            self.sum_i
                .unwrap_or(0)
                .checked_add(part)
                .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
        );
        Ok(())
    }

    fn fold_batch(&mut self, batch: &RecordBatch, column: Option<usize>) -> Result<()> {
        let Some(column) = column else {
            if self.func == AggFunc::Count && self.column.is_none() {
                self.count += batch.num_rows() as u64;
            }
            return Ok(());
        };
        let Some(values) = batch.column(column) else {
            return Ok(());
        };
        match self.func {
            AggFunc::Count => self.count += values.col_count_non_null(),
            AggFunc::Min => {
                if let Some(value) = values.col_min() {
                    self.merge_min(value);
                }
            }
            AggFunc::Max => {
                if let Some(value) = values.col_max() {
                    self.merge_max(value);
                }
            }
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(part) = values.col_sum_f64() {
                        self.sum_f = Some(self.sum_f.unwrap_or(0.0) + part);
                    }
                }
                _ => {
                    if let Some(part) = values.col_sum_i64() {
                        self.add_i64(part)?;
                    }
                }
            },
            AggFunc::Avg => {
                let n = values.col_count_non_null();
                if n > 0 {
                    let part = values
                        .col_sum_f64()
                        .or_else(|| values.col_sum_i64().map(|x| x as f64))
                        .unwrap_or(0.0);
                    self.avg_sum_f = Some(self.avg_sum_f.unwrap_or(0.0) + part);
                    self.avg_count += n;
                }
            }
        }
        Ok(())
    }

    /// Fold one row's argument value (per-row grouped data folds). `None` is
    /// the COUNT(*) shape — every row counts regardless of any value.
    fn fold_row(&mut self, value: Option<&ScalarValue>) -> Result<()> {
        let count_star = self.func == AggFunc::Count && self.column.is_none();
        let Some(value) = value else {
            if count_star {
                self.count += 1;
            }
            return Ok(());
        };
        if value.is_null() {
            if count_star {
                self.count += 1;
            }
            return Ok(());
        }
        match self.func {
            AggFunc::Count => self.count += 1,
            AggFunc::Min => self.merge_min(value.clone()),
            AggFunc::Max => self.merge_max(value.clone()),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(part) = value.numeric_f64() {
                        self.sum_f = Some(self.sum_f.unwrap_or(0.0) + part);
                    }
                }
                _ => {
                    if let Some(part) = value.integer_i128() {
                        let part = i64::try_from(part)
                            .map_err(|_| TaibaiError::Sql("SUM overflow".to_string()))?;
                        self.add_i64(part)?;
                    }
                }
            },
            AggFunc::Avg => {
                if let Some(part) = value.numeric_f64() {
                    self.avg_sum_f = Some(self.avg_sum_f.unwrap_or(0.0) + part);
                    self.avg_count += 1;
                }
            }
        }
        Ok(())
    }

    /// Fold a cached per-segment partial sum into this accumulator. A `None`
    /// delta means the segment contributed nothing (e.g. all-NULL rows) and
    /// must not materialize a zero from an empty accumulator.
    fn fold_cached_sum(&mut self, cached: &SumCacheValue) -> Result<()> {
        match self.col_type {
            DataType::Float32 | DataType::Float64 => {
                if let Some(v) = cached.f {
                    self.sum_f = Some(self.sum_f.unwrap_or(0.0) + v);
                }
            }
            _ => {
                if let Some(v) = cached.i {
                    self.add_i64(v)?;
                }
            }
        }
        Ok(())
    }

    fn merge_min(&mut self, value: ScalarValue) {
        self.best_min = Some(match self.best_min.take() {
            Some(current)
                if crate::sql::execution::operators::aggregate::compare_scalars(
                    &value, &current,
                ) != std::cmp::Ordering::Less =>
            {
                current
            }
            _ => value,
        });
    }

    fn merge_max(&mut self, value: ScalarValue) {
        self.best_max = Some(match self.best_max.take() {
            Some(current)
                if crate::sql::execution::operators::aggregate::compare_scalars(
                    &value, &current,
                ) != std::cmp::Ordering::Greater =>
            {
                current
            }
            _ => value,
        });
    }

    fn finalize(self) -> ScalarValue {
        match self.func {
            AggFunc::Count => ScalarValue::Int64(Some(self.count as i64)),
            AggFunc::Min => self
                .best_min
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Max => self
                .best_max
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => ScalarValue::Float64(self.sum_f),
                _ => ScalarValue::Int64(self.sum_i),
            },
            AggFunc::Avg => {
                if self.avg_count > 0 {
                    // Decimal sums accumulate raw scaled i64; divide the
                    // scale back out (AVG used to come out 10^scale too
                    // large for DECIMAL columns).
                    let scale_divisor = if self.col_type == DataType::Decimal {
                        10f64.powi(self.col_scale)
                    } else {
                        1.0
                    };
                    ScalarValue::Float64(Some(
                        self.avg_sum_f.unwrap_or(0.0) / self.avg_count as f64 / scale_divisor,
                    ))
                } else {
                    ScalarValue::Float64(None)
                }
            }
        }
    }
}

/// Per-segment partial sums for the metadata aggregate fast path. Keyed by
/// (segment path, column, row count, deleted row count, float?) so appends,
/// compaction, and deletes naturally invalidate entries. In-process only.
/// Drop every cached per-segment SUM entry whose segment lives under
/// `table_dir`. Segment numbering restarts at 1 once a table directory is
/// recreated (DROP/CREATE same name, TRUNCATE) or compaction frees path
/// numbers, so a surviving entry can be hit by a NEW segment with the same
/// (path, column, row_count, deleted) tuple and answer with the previous
/// incarnation's SUM. Key embeds no table generation, so lifecycle events
/// must invalidate explicitly.
pub(crate) fn invalidate_segment_sum_cache_under(table_dir: &Path) {
    let mut cache = segment_sum_cache().lock().unwrap();
    cache.retain(|(path, _, _, _, _), _| !Path::new(path).starts_with(table_dir));
}

fn segment_sum_cache() -> &'static std::sync::Mutex<
    std::collections::HashMap<(String, String, u64, u64, bool), SumCacheValue>,
> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<
        std::sync::Mutex<
            std::collections::HashMap<(String, String, u64, u64, bool), SumCacheValue>,
        >,
    > = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[derive(Clone, Copy, Debug)]
struct SumCacheValue {
    i: Option<i64>,
    f: Option<f64>,
}

fn segment_sum_cache_lookup(
    segment: &str,
    row_count: u64,
    deleted_row_count: u64,
    columns: &[(String, bool)],
) -> Option<Vec<SumCacheValue>> {
    let cache = segment_sum_cache().lock().unwrap();
    columns
        .iter()
        .map(|(name, is_float)| {
            cache
                .get(&(
                    segment.to_string(),
                    name.clone(),
                    row_count,
                    deleted_row_count,
                    *is_float,
                ))
                .copied()
        })
        .collect()
}

fn segment_sum_cache_store(
    segment: &str,
    row_count: u64,
    deleted_row_count: u64,
    columns: &[(String, bool)],
    values: &[SumCacheValue],
) {
    let mut cache = segment_sum_cache().lock().unwrap();
    // Bound the cache: keys embed (path, row_count, deleted) tuples, so
    // appends and compactions keep producing fresh keys and the map would
    // otherwise grow for the process lifetime.
    if cache.len() >= 4096 {
        cache.clear();
    }
    for ((name, is_float), value) in columns.iter().zip(values.iter()) {
        cache.insert(
            (
                segment.to_string(),
                name.clone(),
                row_count,
                deleted_row_count,
                *is_float,
            ),
            *value,
        );
    }
}

/// Aggregate argument columns the metadata fast path can answer: SUM needs a
/// plain numeric argument (the answer is the argument's integer/float
/// representation); AVG additionally allows Decimal because its answer is
/// always sum/count as Float64, summed in the raw i64 domain exactly like
/// the regular pipeline's `col_sum_i64`. Anything else keeps the generic
/// scan.
fn aggregate_args_supported(aggregates: &[AggregateExpr], table_schema: &Schema) -> bool {
    for agg in aggregates {
        if agg.distinct {
            continue;
        }
        let supported = match agg.func {
            crate::sql::planner::AggFunc::Sum => match &*agg.arg {
                crate::sql::planner::LogicalExpr::Column(idx) => table_schema
                    .columns
                    .get(*idx)
                    .map(|c| {
                        matches!(
                            c.data_type,
                            crate::protocol::types::DataType::Int64
                                | crate::protocol::types::DataType::Int32
                                | crate::protocol::types::DataType::Int16
                                | crate::protocol::types::DataType::Int8
                                | crate::protocol::types::DataType::Float64
                                | crate::protocol::types::DataType::Float32
                        )
                    })
                    .unwrap_or(false),
                _ => false,
            },
            crate::sql::planner::AggFunc::Avg => match &*agg.arg {
                crate::sql::planner::LogicalExpr::Column(idx) => table_schema
                    .columns
                    .get(*idx)
                    .map(|c| {
                        matches!(
                            c.data_type,
                            crate::protocol::types::DataType::Int64
                                | crate::protocol::types::DataType::Int32
                                | crate::protocol::types::DataType::Int16
                                | crate::protocol::types::DataType::Int8
                                | crate::protocol::types::DataType::Float64
                                | crate::protocol::types::DataType::Float32
                                | crate::protocol::types::DataType::Decimal
                        )
                    })
                    .unwrap_or(false),
                _ => false,
            },
            _ => continue,
        };
        if !supported {
            return false;
        }
    }
    true
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
    let col_type = table_schema.columns[col_def_idx].data_type.clone();
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
            let Some(zone_map) = reader.zone_maps(rg_idx).get(col_idx) else {
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
mod tests {
    use super::*;
    use crate::types::batch::ColumnDef;

    fn sum_schema() -> Schema {
        Schema::new(vec![ColumnDef::new("v", DataType::Int64)])
    }

    fn batch_of(values: &[i64]) -> RecordBatch {
        let schema = sum_schema();
        let mut batch = RecordBatch::new(schema);
        for value in values {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(*value)))
                .unwrap();
        }
        batch
    }

    /// The data-fold path must report overflow exactly like the footer
    /// col_sums path and the generic aggregate operator: cross-group
    /// accumulation that exceeds i64 is an error, not a wrapped value.
    #[test]
    fn data_fold_sum_overflow_errors_instead_of_wrapping() {
        let schema = sum_schema();
        let mut accumulator = AggAccum::new(AggFunc::Sum, Some(0), &schema);
        accumulator
            .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
            .expect("first partial fits");
        let error = accumulator
            .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
            .expect_err("cross-group overflow must error");
        assert!(error.to_string().contains("SUM overflow"), "got: {error}");
    }

    /// Cached per-segment partials flow through the same accumulation point.
    #[test]
    fn cached_partial_sum_overflow_errors_instead_of_wrapping() {
        let schema = sum_schema();
        let mut accumulator = AggAccum::new(AggFunc::Sum, Some(0), &schema);
        accumulator
            .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
            .expect("first partial fits");
        let error = accumulator
            .fold_cached_sum(&SumCacheValue {
                i: Some(i64::MAX - 1),
                f: None,
            })
            .expect_err("cached-partial overflow must error");
        assert!(error.to_string().contains("SUM overflow"), "got: {error}");
    }

    /// The drift classifier: RENAME (previous_names) and DROP COLUMN (extra
    /// footer columns) resolve as Present; a missing column folds its
    /// constant DEFAULT (absent default = constant NULL); CURRENT_TIMESTAMP
    /// is not constant and must decline.
    #[test]
    fn footer_column_drift_classifies_by_name() {
        let mut renamed = ColumnDef::new("total", DataType::Int64);
        renamed.previous_names.push("v".to_string());
        let table = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            renamed,
            ColumnDef::new("score", DataType::Int64).with_default_value(Some(
                crate::types::batch::ColumnDefaultValue::Literal(ScalarValue::Int64(Some(7))),
            )),
            ColumnDef::new("note", DataType::Int64),
            ColumnDef::new("created", DataType::Timestamp).with_default_value(Some(
                crate::types::batch::ColumnDefaultValue::CurrentTimestamp,
            )),
        ]);
        // Segment written before the RENAME and before every ADD: footer
        // keeps the old names and none of the new columns. Extra footer
        // columns (a later DROP COLUMN) would be ignored the same way.
        let footer = Schema::new(vec![
            ColumnDef::new("dropped_extra", DataType::Int64),
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("v", DataType::Int64),
        ]);

        assert_eq!(
            footer_column_drift(&table, &footer, 0),
            FooterColumnDrift::Present
        );
        assert_eq!(
            footer_column_drift(&table, &footer, 1),
            FooterColumnDrift::Present,
            "RENAME drift resolves through previous_names"
        );
        assert_eq!(
            footer_column_drift(&table, &footer, 2),
            FooterColumnDrift::ConstantDefault(ScalarValue::Int64(Some(7)))
        );
        assert_eq!(
            footer_column_drift(&table, &footer, 3),
            FooterColumnDrift::ConstantDefault(ScalarValue::Int64(None)),
            "absent default folds as a constant NULL"
        );
        assert_eq!(
            footer_column_drift(&table, &footer, 4),
            FooterColumnDrift::NonConstantDefault
        );
    }

    /// The constant-DEFAULT fold must mirror the zone-map fold semantics:
    /// a NULL default (or zero rows) contributes nothing, a non-null
    /// default contributes to every aggregate, and SUM reports overflow
    /// instead of wrapping.
    #[test]
    fn fold_constant_default_matches_zone_map_semantics() {
        let schema = sum_schema();
        let seven = ScalarValue::Int64(Some(7));

        let mut sum = AggAccum::new(AggFunc::Sum, Some(0), &schema);
        sum.fold_constant_default(&seven, 50).unwrap();
        assert_eq!(sum.finalize(), ScalarValue::Int64(Some(350)));

        // NULL default: all-NULL column — SUM stays NULL, not 0.
        let mut sum_null = AggAccum::new(AggFunc::Sum, Some(0), &schema);
        sum_null
            .fold_constant_default(&ScalarValue::Int64(None), 50)
            .unwrap();
        assert_eq!(sum_null.finalize(), ScalarValue::Int64(None));

        // Zero rows contribute nothing (MIN stays NULL).
        let mut min = AggAccum::new(AggFunc::Min, Some(0), &schema);
        min.fold_constant_default(&seven, 0).unwrap();
        assert_eq!(min.finalize(), ScalarValue::Int64(None));
        let mut min = AggAccum::new(AggFunc::Min, Some(0), &schema);
        min.fold_constant_default(&seven, 10).unwrap();
        assert_eq!(min.finalize(), ScalarValue::Int64(Some(7)));

        let mut max = AggAccum::new(AggFunc::Max, Some(0), &schema);
        max.fold_constant_default(&seven, 10).unwrap();
        assert_eq!(max.finalize(), ScalarValue::Int64(Some(7)));

        let mut count = AggAccum::new(AggFunc::Count, Some(0), &schema);
        count.fold_constant_default(&seven, 10).unwrap();
        count
            .fold_constant_default(&ScalarValue::Int64(None), 5)
            .unwrap();
        assert_eq!(count.finalize(), ScalarValue::Int64(Some(10)));

        let mut avg = AggAccum::new(AggFunc::Avg, Some(0), &schema);
        avg.fold_constant_default(&seven, 21).unwrap();
        match avg.finalize() {
            ScalarValue::Float64(Some(value)) => assert_eq!(value, 7.0),
            other => panic!("expected 7.0, got {other:?}"),
        }

        let mut sum_overflow = AggAccum::new(AggFunc::Sum, Some(0), &schema);
        sum_overflow
            .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
            .unwrap();
        let error = sum_overflow
            .fold_constant_default(&seven, u64::MAX)
            .expect_err("default x rows overflow must error");
        assert!(error.to_string().contains("SUM overflow"), "got: {error}");
    }

    /// Engine-level: after ALTER ADD COLUMN with a constant default, the
    /// pre-ALTER segment's footer drifts from the catalog. The metadata
    /// fast path must NOT decline (the old whole-schema gate returned
    /// None): SUM(new_col) folds default x rows, COUNT folds the row count,
    /// and a RENAME-drifted SUM resolves by previous name and folds the
    /// footer's per-row-group col_sums.
    #[test]
    fn compute_simple_aggregate_keeps_fast_path_across_add_and_rename_drift() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main")
            .expect("engine");
        let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
            .expect("create");
        let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
        for i in 0..50i64 {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({}, {})", i + 1, i + 1));
        }
        engine.execute(&mut session, &sql).expect("insert");
        engine.flush_table_buffers("main", "t").expect("flush");
        engine
            .execute(&mut session, "ALTER TABLE t ADD COLUMN score INT64 DEFAULT 7")
            .expect("add column");
        engine
            .execute(&mut session, "ALTER TABLE t RENAME COLUMN v TO value")
            .expect("rename");

        let table_meta = engine.get_table_meta("main", "t").expect("table meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("t");
        spec.table_schema = table_schema.clone();
        let score_idx = table_schema.index_of("score").expect("score column");
        let value_idx = table_schema.index_of("value").expect("value column");
        let aggregates = vec![
            AggregateExpr {
                func: AggFunc::Sum,
                arg: Box::new(LogicalExpr::Column(score_idx)),
                distinct: false,
            },
            AggregateExpr {
                func: AggFunc::Count,
                arg: Box::new(LogicalExpr::Column(score_idx)),
                distinct: false,
            },
            AggregateExpr {
                func: AggFunc::Sum,
                arg: Box::new(LogicalExpr::Column(value_idx)),
                distinct: false,
            },
        ];
        let batch = compute_folded_aggregate(
            &spec,
            &[],
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("t"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("compute")
        .expect("drifted footer (ADD COLUMN default + RENAME) must keep the fast path");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(
            batch.scalar(0, 0),
            Some(ScalarValue::Int64(Some(350))),
            "SUM(score) = default 7 x 50 drifted rows"
        );
        assert_eq!(
            batch.scalar(0, 1),
            Some(ScalarValue::Int64(Some(50))),
            "COUNT(score) counts the default rows"
        );
        assert_eq!(
            batch.scalar(0, 2),
            Some(ScalarValue::Int64(Some(1275))),
            "SUM(value) folds the renamed column's footer sums"
        );
        engine.segment_mgr().shutdown();
    }

    /// Engine-level boundary: DEFAULT CURRENT_TIMESTAMP is not a constant,
    /// so a segment predating the column still declines the fast path.
    #[test]
    fn compute_simple_aggregate_declines_current_timestamp_default() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main")
            .expect("engine");
        let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
            .expect("create");
        engine
            .execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
            .expect("insert");
        engine.flush_table_buffers("main", "t").expect("flush");
        engine
            .execute(
                &mut session,
                "ALTER TABLE t ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            )
            .expect("add column");

        let table_meta = engine.get_table_meta("main", "t").expect("table meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("t");
        spec.table_schema = table_schema.clone();
        let created_idx = table_schema.index_of("created").expect("created column");
        let aggregates = vec![AggregateExpr {
            func: AggFunc::Min,
            arg: Box::new(LogicalExpr::Column(created_idx)),
            distinct: false,
        }];
        let batch = compute_folded_aggregate(
            &spec,
            &[],
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("t"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("compute");
        assert!(
            batch.is_none(),
            "CURRENT_TIMESTAMP default must decline the metadata fast path"
        );
        engine.segment_mgr().shutdown();
    }

    /// Engine-level: a day-partitioned table's GROUP BY partition key folds
    /// entirely from segment metadata (returns `Some`), one row per day in
    /// first-seen order — no row data is decoded.
    #[test]
    fn compute_folded_aggregate_answers_group_by_partition_key_from_metadata() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main")
            .expect("engine");
        let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE t (id INT64, deal_date INT64) PARTITION BY DAY(deal_date)",
            )
            .expect("create");
        let mut sql = String::from("INSERT INTO t (id, deal_date) VALUES ");
        let mut id = 0i64;
        let mut first = true;
        for day in [20240101i64, 20240102, 20240103] {
            for _ in 0..10 {
                if !first {
                    sql.push_str(", ");
                }
                first = false;
                id += 1;
                sql.push_str(&format!("({}, {})", id, day));
            }
        }
        engine.execute(&mut session, &sql).expect("insert");
        engine.flush_table_buffers("main", "t").expect("flush");

        let table_meta = engine.get_table_meta("main", "t").expect("table meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("t");
        spec.table_schema = table_schema.clone();
        let date_idx = table_schema.index_of("deal_date").expect("date column");
        let group_by = vec![LogicalExpr::Column(date_idx)];
        let aggregates = vec![AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Wildcard),
            distinct: false,
        }];
        let batch = compute_folded_aggregate(
            &spec,
            &group_by,
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("t"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("compute")
        .expect("day-partitioned GROUP BY must fold from metadata");
        assert_eq!(batch.num_rows(), 3, "one row per day");
        assert_eq!(batch.num_columns(), 2, "group key + count columns");
        for (row, day) in [(0usize, 20240101i64), (1, 20240102), (2, 20240103)] {
            assert_eq!(batch.scalar(row, 0), Some(ScalarValue::Int64(Some(day))));
            assert_eq!(batch.scalar(row, 1), Some(ScalarValue::Int64(Some(10))));
        }
        engine.segment_mgr().shutdown();
    }

    /// Engine-level: an active TTL makes every row's visibility row-
    /// dependent, which metadata folds cannot see — the engine must decline
    /// so the regular scan pipeline filters the TTL column first.
    #[test]
    fn compute_folded_aggregate_declines_active_ttl() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main")
            .expect("engine");
        let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE t (id INT64, ts TIMESTAMP) PARTITION BY DAY(ts) TTL (ts, 3600)",
            )
            .expect("create");
        engine
            .execute(
                &mut session,
                "INSERT INTO t (id, ts) VALUES (1, '2024-01-01 00:00:00')",
            )
            .expect("insert");
        engine.flush_table_buffers("main", "t").expect("flush");

        let table_meta = engine.get_table_meta("main", "t").expect("table meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("t");
        spec.table_schema = table_schema.clone();
        let ts_idx = table_schema.index_of("ts").expect("ts column");
        let group_by = vec![LogicalExpr::Column(ts_idx)];
        let aggregates = vec![AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Wildcard),
            distinct: false,
        }];
        let batch = compute_folded_aggregate(
            &spec,
            &group_by,
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("t"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("compute");
        assert!(batch.is_none(), "active TTL must decline the folding engine");
        engine.segment_mgr().shutdown();
    }

    /// Engine-level: expression GROUP BY keys decline (`None`), leaving the
    /// regular pipeline in charge.
    #[test]
    fn compute_folded_aggregate_declines_expression_group_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main")
            .expect("engine");
        let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
            .expect("create");
        engine
            .execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
            .expect("insert");
        engine.flush_table_buffers("main", "t").expect("flush");

        let table_meta = engine.get_table_meta("main", "t").expect("table meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("t");
        spec.table_schema = table_schema.clone();
        let v_idx = table_schema.index_of("v").expect("v column");
        let group_by = vec![LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(v_idx)),
            op: crate::sql::parser::ast::BinOp::Add,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
        }];
        let aggregates = vec![AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Wildcard),
            distinct: false,
        }];
        let batch = compute_folded_aggregate(
            &spec,
            &group_by,
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("t"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("compute");
        assert!(batch.is_none(), "expression group keys must decline");
        engine.segment_mgr().shutdown();
    }
}
