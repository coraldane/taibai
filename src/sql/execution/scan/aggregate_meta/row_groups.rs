//! Row-group work planning and parallel reads.

use super::*;
use crate::storage::segment::RowGroupMeta;

/// Plan and run the per-row-group work for one segment: metadata-only folds
/// collected first, data folds (run-level or decode+mask batches) read in
/// parallel chunks of 32 and folded sequentially in candidate order. The
pub(super) fn fold_row_groups(
    plan: &FoldPlan<'_>,
    groups: &mut GroupFold,
    env: &SegmentFoldEnv<'_>,
    sums_answerable_from_footer: bool,
    streaming_sum: bool,
    candidate_indices: &[usize],
) -> Result<()> {
    let reader = env.reader;
    let aligned_checks = env.aligned_checks;
    let column_map = env.column_map;
    let defaults = env.defaults;
    let spec = plan.spec;
    let table_schema = plan.table_schema;
    let key_columns = plan.key_columns.clone();
    let key_null_values = plan.key_null_values.clone();
    let aggregate_columns = plan.aggregate_columns.clone();
    let run_folds_enabled = plan.run_folds_enabled;
    let source_columns = plan.source_columns.clone();
    let predicate = plan.predicate.clone();
    let has_sum = plan.has_sum;
    let has_avg = plan.has_avg;

    let row_group_count = reader.num_row_groups();
    let mut candidates = vec![false; row_group_count];
    for &index in candidate_indices {
        if let Some(candidate) = candidates.get_mut(index) {
            *candidate = true;
        }
    }
    let mut row_start = 0u64;
    // One reusable decode buffer for the whole walk: row-group records are
    // decoded on demand and dropped before the next one, so peak memory is a
    // single record instead of the segment's whole metadata graph.
    let mut scratch = RowGroupMeta::empty();

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
    // Per fold unit: (key values, row-group index, live row count).
    // Per metadata-only fold unit: (key values, row-group index, live row
    // count). Only the index is kept — re-decoding that one record when the
    // fold runs costs a few hundred bytes, whereas holding its zone maps here
    // would re-materialise the graph on a full-scan fold where every group
    // qualifies.
    let mut meta_folds: Vec<(Vec<ScalarValue>, usize, u64)> = Vec::new();
    let mut data_folds: Vec<DataFold> = Vec::new();

    // Per-clause zone-map classification is only sound without IN /
    // remaining predicates (they need per-row evaluation and extra
    // columns).
    let clause_vacuity_ok = reader.delete_bitmap().is_empty()
        && spec.in_predicates.is_empty()
        && spec.remaining_predicate.is_none();

    for row_group_index in 0..row_group_count {
        if !candidates[row_group_index] {
            row_start += reader.row_group_rows(row_group_index);
            continue;
        }
        reader.row_group_meta_into(row_group_index, &mut scratch)?;
        let row_group_meta = &scratch;

        let predicate_match = if spec.in_predicates.is_empty() && spec.remaining_predicate.is_none()
        {
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
                    a.column
                        .is_some_and(|column| defaults.contains_key(&column))
                        || (!row_group_meta.zone_maps.is_empty()
                            && row_group_meta.col_sums.len() == row_group_meta.zone_maps.len())
                });
        // A grouped metadata fold additionally needs the unit to collapse
        // into one group: every key column single-valued (or constant
        // default) in this row group's zone maps.
        let rg_unit_key = unit_group_key(
            &key_columns,
            &key_null_values,
            defaults,
            column_map,
            &row_group_meta.zone_maps,
        );
        if predicate_match == crate::storage::partition::pruning::PredicateMatch::All
            && reader.delete_bitmap().is_empty()
            && !streaming_sum
            && aggregates_foldable_from_metadata(
                &groups.template,
                &aggregate_columns,
                column_map,
                defaults,
                &row_group_meta.zone_maps,
            )
            && (!has_sum && !has_avg || rg_sums_available)
        {
            if let Some(unit_key) = rg_unit_key {
                meta_folds.push((unit_key, row_group_index, row_group_meta.row_count));
                row_start += row_group_meta.row_count;
                continue;
            }
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
                reader,
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
                column_map,
                &row_group_meta.zone_maps,
                reader,
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
                row_group_meta
                    .row_count
                    .saturating_sub(delete_bitmap::row_range_deleted_count(
                        reader.delete_bitmap(),
                        row_start,
                        row_group_meta.row_count,
                    ))
            };
            meta_folds.push((Vec::new(), row_group_index, live_rows));
            row_start += row_group_meta.row_count;
            continue;
        }
        let positions = aggregate_columns
            .iter()
            .map(|column| column.and_then(|col| read_columns.iter().position(|&read| read == col)))
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

    for (key, row_group_index, row_count) in meta_folds {
        reader.row_group_meta_into(row_group_index, &mut scratch)?;
        fold_metadata(
            groups.group(key),
            column_map,
            defaults,
            &scratch.zone_maps,
            row_count,
            &scratch.col_sums,
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
                    let raw = reader.read_column_raw(fold.row_group_index, plan.segment_column)?;
                    if let Some(folded) = crate::storage::codec::predicate_scan::fold_runs_from_raw(
                        &raw,
                        &plan.predicates,
                    ) {
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
                    reader,
                    table_schema,
                    fold.row_group_index,
                    &fold.read_columns,
                    defaults,
                )?;
                if !delete_bitmap_is_empty {
                    batch =
                        delete_bitmap::filter_batch(batch, fold.row_start, reader.delete_bitmap())?;
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
                    fold_grouped_data_batch(groups, &fold.key_positions, &fold.positions, &batch)?;
                }
            }
        }
    }
    Ok(())
}

/// After a data fold, store the segment's SUM deltas into the partial-sum
/// cache (whole-segment shape only: no predicates, no deletes, no GROUP BY).
pub(super) fn store_segment_sum_delta(
    plan: &FoldPlan<'_>,
    groups: &mut GroupFold,
    sum_before: Vec<(Option<i64>, Option<f64>)>,
    segment_path: &Path,
    meta_rows: u64,
    reader: &SegmentReader,
    sums_answerable_from_footer: bool,
) -> Result<()> {
    let spec = plan.spec;
    let aggregates = plan.aggregates;
    let aggregate_columns = plan.aggregate_columns.clone();
    let table_schema = plan.table_schema;
    let grouped = plan.grouped;
    let has_sum = plan.has_sum;

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
    Ok(())
}
