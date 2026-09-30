//! Metadata-driven fast path for "simple" aggregate queries.
//!
//! A simple aggregate query is one with no GROUP BY, no HAVING, no DISTINCT,
//! and whose select list consists entirely of `COUNT(*)`, `COUNT(col)`,
//! `MIN(col)` or `MAX(col)`. For such queries the answer can frequently be
//! derived from row-group zone maps / footers without reading column data:
//!
//! * `COUNT(*)`              -> Σ(row_count) − Σ(deleted_row_count)
//! * `COUNT(col)` (no del)   -> Σ(row_count) − Σ(zone_map[col].null_count)
//! * `MIN(col)` / `MAX(col)` -> min/max over candidate row groups'
//!   `zone_map[col].min_value` / `max_value`
//!
//! When the candidate segments contain deletes we fall back to reading the
//! argument column for MIN/MAX (still only the relevant row groups) so the
//! result remains correct.
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

/// Compute the result of a simple metadata-only aggregate query.
///
/// `aggregates` are the (already logical-schema-indexed) aggregate expressions
/// from the SELECT list. Returns a single-row result batch.
#[allow(clippy::too_many_arguments)]
pub fn compute_simple_aggregate(
    spec: &ScanSpec,
    aggregates: &[AggregateExpr],
    table_meta: &TableMeta,
    table_schema: &Schema,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    hot_mgr: &crate::storage::core::hot_store::HotStoreManager,
    db: &str,
) -> Result<Option<RecordBatch>> {
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
    let aggregate_columns: Vec<Option<usize>> = aggregates
        .iter()
        .map(|aggregate| match &*aggregate.arg {
            LogicalExpr::Column(index) => Some(*index),
            LogicalExpr::Wildcard => None,
            _ => None,
        })
        .collect();
    let mut accumulators = aggregates
        .iter()
        .zip(&aggregate_columns)
        .map(|(aggregate, column)| AggAccum::new(aggregate.func, *column, table_schema))
        .collect::<Vec<_>>();

    let segment_files = if table_dir.as_os_str().is_empty() || !table_dir.exists() {
        Vec::new()
    } else {
        partition::prune_segments_by_partition(
            SegmentManager::discover_segments(table_dir)?,
            spec,
            Some(table_meta),
        )
    };
    let predicate = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(spec);
    let in_checks = row_group::build_in_check_list(spec);
    let set_checks = row_group::build_set_check_list(spec);
    let set_in_checks = row_group::build_set_in_check_list(spec);
    let source_columns = aggregate_source_columns(spec, &aggregate_columns);

    let has_sum = accumulators
        .iter()
        .any(|a| a.func == crate::sql::planner::AggFunc::Sum && !a.sum_unsupported);
    // AVG has no zone-map answer either: row groups must fold from data
    // (batch folds, or the run-level fold planned below).
    let has_avg = accumulators
        .iter()
        .any(|a| a.func == crate::sql::planner::AggFunc::Avg && !a.avg_unsupported);
    let needs_data_fold = has_sum || has_avg;

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
        let pruned = meta.as_ref().is_some_and(|m| {
            !row_group::prune_segment_by_zone_map(&predicate, &m.segment_zone_maps)
        });
        if pruned {
            continue;
        }

        // Footer-persisted per-row-group sums (new-format segments) answer
        // SUM/AVG without reading any data — when present for every row
        // group and every Sum/Avg argument has an exact i64 view. Segments
        // without them keep the partial-sum cache / streaming fold below.
        // The sidecar meta decides without touching the segment file; when
        // it has not been written yet (first query after restart), the
        // cached reader's already-parsed footer answers the same question.
        let reader = segment_mgr.open_scan_reader(segment_path, spec.reader_txn_id)?;
        // Everything below folds zone maps positionally with table-schema
        // column indices. A segment written before an ALTER ADD/DROP COLUMN
        // has a different column layout, and a mis-indexed zone map would
        // silently skip the segment (COUNT/SUM under-count, MIN/MAX miss the
        // pinned default) or read the wrong column after a DROP. Decline the
        // fast path; the regular pipeline remaps columns by name.
        if super::strategy::reader_schema_differs(reader.schema(), table_schema) {
            return Ok(None);
        }
        let footer_sums = if let Some(first) = meta.as_ref().and_then(|m| m.row_groups.first()) {
            first.col_sums.len() == first.zone_maps.len()
        } else {
            reader
                .footer_row_groups()
                .first()
                .is_some_and(|rg| rg.col_sums.len() == rg.zone_maps.len())
        };
        let sums_answerable_from_footer = footer_sums
            && accumulators
                .iter()
                .filter(|a| matches!(a.func, AggFunc::Sum | AggFunc::Avg))
                .all(|a| crate::storage::segment::column_sum_supported(a.col_type));
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
        let sum_cache = if has_sum
            && !has_avg
            && !sums_answerable_from_footer
            && !spec.has_predicates()
            && meta_deleted == 0
            && meta.as_ref().is_some_and(|m| {
                aggregates_foldable_from_metadata(
                    &accumulators,
                    &aggregate_columns,
                    &m.segment_zone_maps,
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
                fold_metadata(&mut accumulators, &m.segment_zone_maps, m.row_count, &[])?;
            }
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
        let streaming_sum = has_sum
            && !sums_answerable_from_footer
            && sum_cache.is_none()
            && reader.deleted_row_count() == 0;
        if !spec.has_predicates()
            && reader.deleted_row_count() == 0
            && !needs_data_fold
            && aggregates_foldable_from_metadata(
                &accumulators,
                &aggregate_columns,
                reader.segment_zone_maps(),
            )
        {
            fold_metadata(
                &mut accumulators,
                reader.segment_zone_maps(),
                reader.row_count(),
                &[],
            )?;
            continue;
        }

        let sum_before: Vec<(Option<i64>, Option<f64>)> = if has_sum {
            accumulators.iter().map(|a| (a.sum_i, a.sum_f)).collect()
        } else {
            Vec::new()
        };

        let candidate_indices = if let Some(meta) = &meta {
            row_group::prune_row_group_meta(
                &meta.row_groups,
                &predicate,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
            )
            .candidate_indices
        } else {
            row_group::prune_row_groups(
                &reader,
                &predicate,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
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
            /// Whether the batch fold still evaluates predicates (false when
            /// every clause is zone-map vacuous for this row group).
            apply_mask: bool,
        }
        let mut meta_folds: Vec<(&[crate::storage::partition::pruning::ZoneMap], u64, &[i64])> =
            Vec::new();
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
                    predicate.classify_row_group(&row_group_meta.zone_maps)
                } else {
                    crate::storage::partition::pruning::PredicateMatch::Partial
                };
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::None {
                row_start += row_group_meta.row_count;
                continue;
            }
            // The metadata-only fold cannot produce SUM or AVG — unless the
            // footer's per-row-group sums answer them (this row group's
            // extension present; the argument types were checked above). A
            // streaming_sum segment folds every function from data instead.
            let rg_sums_available = sums_answerable_from_footer
                && !row_group_meta.zone_maps.is_empty()
                && row_group_meta.col_sums.len() == row_group_meta.zone_maps.len();
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::All
                && reader.delete_bitmap().is_empty()
                && !streaming_sum
                && aggregates_foldable_from_metadata(
                    &accumulators,
                    &aggregate_columns,
                    &row_group_meta.zone_maps,
                )
                && (!has_sum && !has_avg || rg_sums_available)
            {
                meta_folds.push((
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
            // column to be run-foldable.
            let mut fold_column: Option<usize> = None;
            let mut run_predicates: Vec<RunPredicate> = Vec::new();
            let mut discriminating = false;
            let mut classifiable = clause_vacuity_ok;
            if clause_vacuity_ok {
                for clause in &predicate.clauses {
                    let Some(zone_map) = row_group_meta.zone_maps.get(clause.column_index) else {
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
            if classifiable && discriminating {
                run_fold = plan_run_fold_on_column(
                    fold_column,
                    run_predicates,
                    &accumulators,
                    table_schema,
                    &reader,
                );
            } else if classifiable && !predicate.clauses.is_empty() {
                // Every clause is vacuous. When the aggregates share one
                // i64-domain argument column, folding its runs answers them
                // without decoding anything (the encoding sniff falls back
                // to the reduced batch read below for non-Rle columns).
                run_fold = plan_vacuous_run_fold(
                    &aggregate_columns,
                    &accumulators,
                    table_schema,
                    &row_group_meta.zone_maps,
                    &reader,
                );
            }

            // Batch-fold read set: all-vacuous row groups need only the
            // aggregate argument columns and skip the (all-true) mask.
            let all_vacuous = classifiable && !discriminating;
            let (read_columns, apply_mask): (Vec<usize>, bool) = if all_vacuous {
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
            if read_columns.is_empty() {
                // No aggregate reads a column (pure COUNT(*)) and every
                // clause is vacuous: the footer answers it.
                meta_folds.push((
                    &row_group_meta.zone_maps,
                    row_group_meta.row_count,
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

            data_folds.push(DataFold {
                row_group_index,
                row_start,
                run_fold,
                read_columns,
                positions,
                apply_mask,
            });
            row_start += row_group_meta.row_count;
        }

        for (zone_maps, row_count, col_sums) in meta_folds {
            fold_metadata(&mut accumulators, zone_maps, row_count, col_sums)?;
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
                    let mut batch = reader
                        .read_row_group_with_projection(fold.row_group_index, &fold.read_columns)?;
                    if !delete_bitmap_is_empty {
                        batch = delete_bitmap::filter_batch(
                            &batch,
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
                        fold_run_fold(&mut accumulators, col_type, &out)?;
                    }
                    RgOutcome::Batch(batch) => {
                        fold_batch(&mut accumulators, &fold.positions, &batch)?;
                    }
                }
            }
        }

        if has_sum
            && !sums_answerable_from_footer
            && !spec.has_predicates()
            && reader.deleted_row_count() == 0
        {
            let needed = sum_needed_columns(aggregates, &aggregate_columns, table_schema);
            // Only SUM accumulators: `needed` (and the cache key order)
            // covers SUM aggregates exclusively, so zipping all accumulators
            // would pair the cache entry with COUNT's empty delta and store
            // a zero sum.
            let deltas: Vec<SumCacheValue> = accumulators
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
        scan_memtable::scan_memtables(memtable_mgr, hot_mgr, db, &spec.table_name, spec)?;
    for batch in &memtable_batches {
        for (accumulator, column) in accumulators.iter_mut().zip(&aggregate_columns) {
            let batch_column = column.and_then(|index| {
                table_schema
                    .columns
                    .get(index)
                    .and_then(|column| batch.schema.index_of(&column.name))
            });
            accumulator.fold_batch(batch, batch_column)?;
        }
    }

    let results = accumulators
        .into_iter()
        .map(AggAccum::finalize)
        .collect::<Vec<_>>();
    let columns = aggregates
        .iter()
        .map(|aggregate| aggregate_result_column_def(aggregate, table_schema))
        .collect();
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema);
    for (index, value) in results.into_iter().enumerate() {
        batch
            .column_mut(index)
            .ok_or_else(|| crate::common::error::TaibaiError::Internal("agg col".into()))?
            .push(&value)?;
    }
    Ok(Some(batch))
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
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
    row_count: u64,
    col_sums: &[i64],
) -> Result<()> {
    for accumulator in accumulators {
        accumulator.fold_metadata(zone_maps, row_count, col_sums)?;
    }
    Ok(())
}

/// Whether every aggregate can fold from these zone maps: each argument
/// column's map must carry computed statistics. Bulk direct writes compute
/// zone maps for the partition/cluster key columns only and fill the rest
/// with defaults whose NULL min / zero null_count are indistinguishable
/// from a genuinely all-null row group — folding those from metadata would
/// miscount NULLs for COUNT(col) and return NULL for MIN/MAX (the same
/// defect once fixed for SUM/AVG). Such segments must fold from data.
/// COUNT(*) has no argument column and is always foldable.
fn aggregates_foldable_from_metadata(
    accumulators: &[AggAccum],
    aggregate_columns: &[Option<usize>],
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
) -> bool {
    accumulators
        .iter()
        .zip(aggregate_columns)
        .all(|(_, column)| match column {
            Some(col) => zone_maps.get(*col).is_some_and(ZoneMap::is_computed),
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
    if has_count_star && zone_maps.get(column)?.null_count != 0 {
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
        zone_maps: &[crate::storage::partition::pruning::ZoneMap],
        row_count: u64,
        col_sums: &[i64],
    ) -> Result<()> {
        let Some(column) = self.column else {
            if self.func == AggFunc::Count {
                self.count += row_count;
            }
            return Ok(());
        };
        let Some(zone_map) = zone_maps.get(column) else {
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
                if let Some(&sum) = col_sums.get(column) {
                    self.add_i64(sum)?;
                }
            }
            AggFunc::Avg if has_values => {
                if let Some(&sum) = col_sums.get(column) {
                    self.avg_sum_f = Some(self.avg_sum_f.unwrap_or(0.0) + sum as f64);
                    self.avg_count += row_count.saturating_sub(zone_map.null_count);
                }
            }
            _ => {}
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
                batch = delete_bitmap::filter_batch(&batch, row_start, bitmap)?;
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
}
