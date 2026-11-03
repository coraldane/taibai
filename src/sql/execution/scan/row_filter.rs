use crate::storage::codec::fastscan::apply_filter_mask_fast;
use crate::storage::codec::predicate_scan::selected_eq_rows_from_raw;
use crate::storage::partition::pruning::RangeOp;
use crate::types::array::ColumnArray;

/// Expand dictionary columns into plain varchars before typed predicate
/// evaluation. Recursion terminates: the materialized batch has no
/// dictionary columns.
/// Column indices referenced by the scalar predicates (batch-local). The
/// remaining predicate is excluded: its evaluation kernels flatten dictionary
/// columns on demand.
fn predicate_dictionary_targets(
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
) -> std::collections::HashSet<usize> {
    let mut set = std::collections::HashSet::new();
    for (idx, _) in eq_predicates {
        set.insert(*idx);
    }
    for clause in range_predicates {
        set.insert(clause.column_index);
    }
    for pred in in_predicates {
        set.insert(pred.column_index);
    }
    set
}

/// Materialize only the dictionary columns the predicates actually touch;
/// untouched dictionary columns stay encoded (read kernels flatten lazily).
fn materialize_predicate_dictionaries(
    batch: &RecordBatch,
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
) -> crate::common::error::Result<RecordBatch> {
    // Materialize EVERY dictionary column, not just the predicate targets:
    // apply_predicates re-invokes itself when any dictionary column remains,
    // and a non-target dictionary column used to survive the "materialize
    // targets" pass, recursing forever (stack overflow) on batches whose
    // non-predicate columns were dictionary encoded.
    let _ = predicate_dictionary_targets(eq_predicates, range_predicates, in_predicates);
    let columns: crate::common::error::Result<Vec<ColumnArray>> = batch
        .columns
        .iter()
        .map(|c| {
            if matches!(c, ColumnArray::Dictionary { .. }) {
                Ok(c.materialize_varchar())
            } else {
                Ok(c.clone())
            }
        })
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns?)
}

use crate::types::batch::{RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

use super::spec::InPredicate;

/// Cluster-key search bounds for one scan: a leading equality prefix plus
/// range bounds on the next cluster column. The binary-search key tuple is
/// the prefix values followed by the range bound value; because the segment
/// is sorted by the full cluster key lexicographically, tuple search on any
/// leading subset of the key is always valid.
#[derive(Debug, Clone)]
pub struct ClusterKeyBounds {
    /// `(column_index, value)` equality pairs in cluster-key order.
    pub prefix_eq: Vec<(usize, ScalarValue)>,
    /// The cluster column the range bounds apply to.
    pub range_column_index: usize,
    pub lower: Option<(ScalarValue, bool)>,
    pub upper: Option<(ScalarValue, bool)>,
}

impl ClusterKeyBounds {
    /// Single-column bounds on the first cluster column (no prefix).
    pub fn single(
        column_index: usize,
        lower: Option<(ScalarValue, bool)>,
        upper: Option<(ScalarValue, bool)>,
    ) -> Self {
        Self {
            prefix_eq: Vec::new(),
            range_column_index: column_index,
            lower,
            upper,
        }
    }

    /// The columns forming the search key tuple, in order.
    pub fn key_column_indices(&self) -> Vec<usize> {
        let mut cols: Vec<usize> = self.prefix_eq.iter().map(|(idx, _)| *idx).collect();
        if !cols.contains(&self.range_column_index) {
            cols.push(self.range_column_index);
        }
        cols
    }
}

pub fn apply_predicates(
    batch: &RecordBatch,
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
        return apply_predicates(
            &materialize_predicate_dictionaries(
                batch,
                eq_predicates,
                range_predicates,
                in_predicates,
            )?,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        );
    }

    let mut mask: Vec<bool> = vec![true; batch.num_rows()];

    for (col_idx, value) in eq_predicates {
        apply_eq_mask(batch, *col_idx, value, &mut mask);
        if mask.iter().all(|m| !m) {
            return Ok(empty_like(batch));
        }
    }

    for clause in range_predicates {
        apply_range_mask(batch, clause, &mut mask);
        if mask.iter().all(|m| !m) {
            return Ok(empty_like(batch));
        }
    }

    for pred in in_predicates {
        apply_in_mask(batch, pred, &mut mask);
        if mask.iter().all(|m| !m) {
            return Ok(empty_like(batch));
        }
    }

    if let Some(expr) = remaining_predicate {
        apply_remaining_mask(batch, expr, &mut mask)?;
    }

    let match_count = mask.iter().filter(|&&m| m).count();
    if match_count == batch.num_rows() {
        return Ok(batch.clone());
    }
    if match_count == 0 {
        return Ok(empty_like(batch));
    }

    Ok(apply_filter_mask_fast(batch, &mask))
}

pub fn apply_predicates_with_column_map(
    batch: &RecordBatch,
    source_columns: &[usize],
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
        return apply_predicates_with_column_map(
            &materialize_predicate_dictionaries(
                batch,
                eq_predicates,
                range_predicates,
                in_predicates,
            )?,
            source_columns,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        );
    }
    let col_pos_by_original: std::collections::HashMap<usize, usize> = source_columns
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();
    let remapped_eq = remap_eq_predicates(eq_predicates, &col_pos_by_original)?;
    let remapped_range = remap_range_predicates(range_predicates, &col_pos_by_original)?;
    let remapped_in = remap_in_predicates(in_predicates, &col_pos_by_original)?;
    let remapped_remaining = remaining_predicate
        .map(|expr| remap_expr(expr, &col_pos_by_original))
        .transpose()?;
    apply_predicates(
        batch,
        &remapped_eq,
        &remapped_range,
        &remapped_in,
        remapped_remaining.as_ref(),
    )
}

pub fn apply_eq_mask(batch: &RecordBatch, col_idx: usize, value: &ScalarValue, mask: &mut [bool]) {
    if let Some(col) = batch.column(col_idx) {
        let col_mask = col.eq_vec(value);
        for (i, m) in col_mask.iter().enumerate() {
            mask[i] = mask[i] && *m;
        }
    } else {
        for m in mask.iter_mut() {
            *m = false;
        }
    }
}

fn apply_range_mask(
    batch: &RecordBatch,
    clause: &crate::storage::partition::pruning::RangeClause,
    mask: &mut [bool],
) {
    if let Some(col) = batch.column(clause.column_index) {
        let col_mask = match clause.op {
            RangeOp::Eq => col.eq_vec(&clause.value),
            RangeOp::Lt => col.lt_vec(&clause.value),
            RangeOp::Le => col.le_vec(&clause.value),
            RangeOp::Gt => col.gt_vec(&clause.value),
            RangeOp::Ge => col.ge_vec(&clause.value),
        };
        for (i, m) in col_mask.iter().enumerate() {
            mask[i] = mask[i] && *m;
        }
    }
}

fn apply_in_mask(batch: &RecordBatch, pred: &InPredicate, mask: &mut [bool]) {
    let Some(col) = batch.column(pred.column_index) else {
        for m in mask.iter_mut() {
            *m = false;
        }
        return;
    };
    // The hashed probe needs a plain typed column: `from_column` ignores the
    // null bitmap and would flatten Selected/Rle/Dictionary per row, and
    // Blob/Vector values hash by content since the ScalarValueHashable extension (kept excluded here only because the literal coercion path predates it; linear comparison stays correct). Tiny lists also keep the
    // linear probe (no set construction). Anything bigger — a hash join
    // runtime filter injects up to 1024 values — probes a hash set built
    // once per batch: O(rows + values) instead of O(rows × values) scalar
    // comparisons.
    let hashable = pred.values.len() > 4
        && !matches!(
            col,
            ColumnArray::Selected { .. }
                | ColumnArray::Rle { .. }
                | ColumnArray::Dictionary { .. }
                | ColumnArray::Blob { .. }
                | ColumnArray::Vector { .. }
        );
    if !hashable {
        for (i, m) in mask.iter_mut().enumerate() {
            if !*m {
                continue;
            }
            let found = pred.values.iter().any(|value| {
                col.compare_scalar_at(i, value)
                    .is_some_and(|ordering| ordering.is_eq())
            });
            *m = found;
        }
        return;
    }
    let values: std::collections::HashSet<
        crate::sql::execution::join_hashtable::ScalarValueHashable,
    > = pred
        .values
        .iter()
        .filter(|value| !value.is_null())
        .map(crate::sql::execution::join_hashtable::ScalarValueHashable::from_scalar)
        .collect();
    for (i, m) in mask.iter_mut().enumerate() {
        // A NULL row never satisfies IN, and `from_column` would read the
        // data slot regardless of the bitmap.
        *m = *m
            && !col.bitmap().is_null(i)
            && values.contains(
                &crate::sql::execution::join_hashtable::ScalarValueHashable::from_column(col, i),
            );
    }
}

fn apply_remaining_mask(
    batch: &RecordBatch,
    expr: &crate::sql::planner::LogicalExpr,
    mask: &mut [bool],
) -> crate::common::error::Result<()> {
    let row_mask = crate::sql::execution::operators::filter::eval_filter(expr, batch)?;
    for (i, m) in row_mask.iter().enumerate() {
        mask[i] = mask[i] && *m;
    }
    Ok(())
}

/// Whether a zone map proves an eq/range clause holds for every row of the
/// group (`classify_clause`'s `All` already accounts for NULLs never
/// satisfying a comparison).
fn zone_map_proves_clause(
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
    column: usize,
    op: crate::storage::partition::pruning::RangeOp,
    value: &ScalarValue,
) -> bool {
    zone_maps.get(column).is_some_and(|zone_map| {
        crate::storage::partition::pruning::classify_clause(
            &crate::storage::partition::pruning::RangeClause {
                column_index: column,
                op,
                value: value.clone(),
            },
            zone_map,
        ) == crate::storage::partition::pruning::PredicateMatch::All
    })
}

#[allow(clippy::too_many_arguments)]
pub fn predicate_first_read(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    rg_idx: usize,
    predicate_cols: &[usize],
    projection_cols: &[usize],
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    // Zone-map vacuity: when only eq/range predicates are present, a clause
    // the whole row group satisfies contributes an all-true mask — drop it
    // from both the predicate-column read and the evaluation. A partition
    // column pinned by equality (min == max across the group) vanishes for
    // free. IN / remaining predicates may reference the same columns, so
    // they disable the elimination.
    let can_eliminate = in_predicates.is_empty() && remaining_predicate.is_none();
    // Zone maps, physical column blocks, and footer defs all live in the
    // segment's footer layout, while predicate indices and the output column
    // order stay in catalog space. Translate every touched column once (by
    // name, so DROP/RENAME-drifted footers resolve correctly) and map back
    // to catalog indices wherever the output contract needs them.
    let mut touch_cols: Vec<usize> = Vec::new();
    for (column, _) in eq_predicates {
        if !touch_cols.contains(column) {
            touch_cols.push(*column);
        }
    }
    for clause in range_predicates {
        if !touch_cols.contains(&clause.column_index) {
            touch_cols.push(clause.column_index);
        }
    }
    for column in predicate_cols.iter().chain(projection_cols.iter()) {
        if !touch_cols.contains(column) {
            touch_cols.push(*column);
        }
    }
    let aligned_touch = reader.align_indices(table_schema, &touch_cols)?;
    let footer_lookup: std::collections::HashMap<usize, usize> =
        touch_cols.iter().copied().zip(aligned_touch).collect();
    let footer_index =
        |column: usize| -> usize { footer_lookup.get(&column).copied().unwrap_or(column) };
    let zone_maps = reader.zone_maps(rg_idx);
    let eq_predicates: Vec<&(usize, ScalarValue)> = eq_predicates
        .iter()
        .filter(|(column, value)| {
            !can_eliminate
                || !zone_map_proves_clause(
                    &zone_maps,
                    footer_index(*column),
                    crate::storage::partition::pruning::RangeOp::Eq,
                    value,
                )
        })
        .collect();
    let range_predicates: Vec<&crate::storage::partition::pruning::RangeClause> = range_predicates
        .iter()
        .filter(|clause| {
            !can_eliminate
                || !zone_map_proves_clause(
                    &zone_maps,
                    footer_index(clause.column_index),
                    clause.op,
                    &clause.value,
                )
        })
        .collect();

    // Columns predicate evaluation still needs, in a stable order.
    let mut eval_cols: Vec<usize> = Vec::with_capacity(predicate_cols.len());
    for (column, _) in &eq_predicates {
        if !eval_cols.contains(column) {
            eval_cols.push(*column);
        }
    }
    for clause in &range_predicates {
        if !eval_cols.contains(&clause.column_index) {
            eval_cols.push(clause.column_index);
        }
    }
    for &column in predicate_cols {
        if !eval_cols.contains(&column) {
            eval_cols.push(column);
        }
    }

    let row_count = reader.row_group_rows(rg_idx) as usize;
    let aligned_projection: Vec<usize> = projection_cols.iter().map(|&c| footer_index(c)).collect();
    if eval_cols.is_empty() {
        // Every predicate is vacuous: the whole row group matches modulo the
        // delete bitmap. No predicate column needs decoding at all.
        if reader.delete_bitmap().is_empty() {
            return reader.read_columns_fast_allow_dictionary(rg_idx, &aligned_projection, true);
        }
        let rg_start_row = reader.row_group_start_row(rg_idx);
        let selected_rows: Vec<usize> = (0..row_count)
            .filter(|row| !reader.is_row_deleted(rg_start_row + *row as u64))
            .collect();
        if selected_rows.is_empty() {
            return Ok(empty_with_projection(table_schema, projection_cols));
        }
        let mut columns_by_idx: std::collections::HashMap<usize, ColumnArray> =
            std::collections::HashMap::with_capacity(projection_cols.len());
        // read_columns_filtered keys its pairs by the footer indices it was
        // given: route each column back to its catalog index by position.
        let mut read_by_footer: std::collections::HashMap<usize, ColumnArray> = reader
            .read_columns_filtered(rg_idx, &aligned_projection, &selected_rows)?
            .into_iter()
            .collect();
        for (&catalog_idx, &footer_idx) in projection_cols.iter().zip(&aligned_projection) {
            if let Some(col) = read_by_footer.remove(&footer_idx) {
                columns_by_idx.insert(catalog_idx, col);
            }
        }
        let mut columns = Vec::with_capacity(projection_cols.len());
        for &column in projection_cols {
            let col = columns_by_idx.remove(&column).ok_or_else(|| {
                crate::common::error::TaibaiError::Internal(format!(
                    "projected column {column} was not read"
                ))
            })?;
            columns.push(col);
        }
        let proj_schema = crate::types::batch::Schema::new(
            projection_cols
                .iter()
                .map(|&idx| table_schema.columns[idx].clone())
                .collect(),
        );
        return crate::types::batch::RecordBatch::try_new(proj_schema, columns)
            .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()));
    }

    let aligned_eval_cols: Vec<usize> = eval_cols.iter().map(|&c| footer_index(c)).collect();
    let pred_batch = reader.read_columns_fast(rg_idx, &aligned_eval_cols)?;
    if pred_batch.num_rows() == 0 {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let mut mask: Vec<bool> = if reader.delete_bitmap().is_empty() {
        vec![true; pred_batch.num_rows()]
    } else {
        let rg_start_row = reader.row_group_start_row(rg_idx);
        (0..pred_batch.num_rows() as u64)
            .map(|i| !reader.is_row_deleted(rg_start_row + i))
            .collect()
    };
    if mask.iter().all(|m| !m) {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let pred_to_batch_pos =
        |column: usize| -> Option<usize> { eval_cols.iter().position(|&c| c == column) };

    for (col_idx, value) in eq_predicates.iter().map(|(column, value)| (*column, value)) {
        if let Some(pos) = pred_to_batch_pos(col_idx) {
            if let Some(col) = pred_batch.column(pos) {
                let col_mask = col.eq_vec(value);
                for (i, m) in col_mask.iter().enumerate() {
                    mask[i] = mask[i] && *m;
                }
            }
        }
        if mask.iter().all(|m| !m) {
            return Ok(empty_with_projection(table_schema, projection_cols));
        }
    }

    for clause in range_predicates.iter() {
        if let Some(pos) = pred_to_batch_pos(clause.column_index) {
            if let Some(col) = pred_batch.column(pos) {
                let col_mask = match clause.op {
                    RangeOp::Eq => col.eq_vec(&clause.value),
                    RangeOp::Lt => col.lt_vec(&clause.value),
                    RangeOp::Le => col.le_vec(&clause.value),
                    RangeOp::Gt => col.gt_vec(&clause.value),
                    RangeOp::Ge => col.ge_vec(&clause.value),
                };
                for (i, m) in col_mask.iter().enumerate() {
                    mask[i] = mask[i] && *m;
                }
            }
        }
        if mask.iter().all(|m| !m) {
            return Ok(empty_with_projection(table_schema, projection_cols));
        }
    }

    for pred in in_predicates {
        if let Some(pos) = pred_to_batch_pos(pred.column_index) {
            if let Some(col) = pred_batch.column(pos) {
                for (i, m) in mask.iter_mut().enumerate() {
                    if !*m {
                        continue;
                    }
                    let found = pred.values.iter().any(|value| {
                        col.compare_scalar_at(i, value)
                            .is_some_and(|ordering| ordering.is_eq())
                    });
                    *m = found;
                }
            }
        }
        if mask.iter().all(|m| !m) {
            return Ok(empty_with_projection(table_schema, projection_cols));
        }
    }

    if let Some(expr) = remaining_predicate {
        let col_pos_by_original: std::collections::HashMap<usize, usize> = eval_cols
            .iter()
            .enumerate()
            .map(|(pos, &column)| (column, pos))
            .collect();
        let remapped_expr = remap_expr(expr, &col_pos_by_original)?;
        apply_remaining_mask(&pred_batch, &remapped_expr, &mut mask)?;
    }

    let match_count = mask.iter().filter(|&&m| m).count();
    if match_count == 0 {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }
    if match_count == pred_batch.num_rows() {
        // Whole row group matched: keep dictionary-encoded projections as
        // (ids, dict) so downstream grouped aggregation consumes u32 ids.
        return reader.read_columns_fast_allow_dictionary(rg_idx, &aligned_projection, true);
    }

    let selected_rows: Vec<usize> = mask
        .iter()
        .enumerate()
        .filter_map(|(idx, keep)| (*keep).then_some(idx))
        .collect();

    let mut columns_by_idx: std::collections::HashMap<usize, ColumnArray> =
        std::collections::HashMap::with_capacity(projection_cols.len());
    let mut cols_to_read = Vec::with_capacity(projection_cols.len());

    for &col_idx in projection_cols {
        if columns_by_idx.contains_key(&col_idx) || cols_to_read.contains(&col_idx) {
            continue;
        }
        if let Some(pos) = pred_to_batch_pos(col_idx) {
            if let Some(col) = pred_batch.column(pos) {
                columns_by_idx.insert(col_idx, col.filter_by_mask_fast(&mask, match_count));
                continue;
            }
        }
        cols_to_read.push(col_idx);
    }

    let aligned_cols_to_read: Vec<usize> = cols_to_read.iter().map(|&c| footer_index(c)).collect();
    let mut read_by_footer: std::collections::HashMap<usize, ColumnArray> = reader
        .read_columns_filtered(rg_idx, &aligned_cols_to_read, &selected_rows)?
        .into_iter()
        .collect();
    for (&catalog_idx, &footer_idx) in cols_to_read.iter().zip(&aligned_cols_to_read) {
        if let Some(col) = read_by_footer.remove(&footer_idx) {
            columns_by_idx.insert(catalog_idx, col);
        }
    }

    let mut columns = Vec::with_capacity(projection_cols.len());
    for &col_idx in projection_cols {
        let col = columns_by_idx.get(&col_idx).cloned().ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(format!(
                "projected column {} was not read",
                col_idx
            ))
        })?;
        columns.push(col);
    }
    let proj_schema = crate::types::batch::Schema::new(
        projection_cols
            .iter()
            .map(|&idx| table_schema.columns[idx].clone())
            .collect(),
    );
    crate::types::batch::RecordBatch::try_new(proj_schema, columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub fn cluster_key_range_read(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    rg_idx: usize,
    bounds: &ClusterKeyBounds,
    projection_cols: &[usize],
    predicate_cols: &[usize],
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    let key_col_idx = bounds.range_column_index;
    if bounds.prefix_eq.is_empty() {
        if let Some(eq_value) = simple_cluster_eq_value(
            key_col_idx,
            bounds,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        ) {
            return cluster_key_eq_read(
                reader,
                table_schema,
                rg_idx,
                key_col_idx,
                eq_value,
                projection_cols,
            );
        }
    }

    // The binary search reads the key columns in `key_cols_idx` (catalog)
    // order; translate them onto the segment's footer layout by name so the
    // read hits the right physical blocks when the orders have drifted.
    let key_cols_idx = bounds.key_column_indices();
    let aligned_key_cols = reader.align_indices(table_schema, &key_cols_idx)?;
    let key_batch = reader.read_columns_fast(rg_idx, &aligned_key_cols)?;
    if key_batch.num_rows() == 0 {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let key_cols: Vec<&ColumnArray> = key_cols_idx
        .iter()
        .enumerate()
        .map(|(pos, _)| key_batch.column(pos))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(
                "cluster key columns missing from key batch".into(),
            )
        })?;
    if !key_tuple_is_sorted(&key_cols) {
        return predicate_first_read(
            reader,
            table_schema,
            rg_idx,
            predicate_cols,
            projection_cols,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        );
    }

    let start = match &bounds.lower {
        Some((value, inclusive)) => {
            let tuple = search_tuple(&bounds.prefix_eq, Some(value));
            if *inclusive {
                lower_bound_tuple(&key_cols, &tuple)
            } else {
                upper_bound_tuple(&key_cols, &tuple)
            }
        }
        None => 0,
    };
    let end = match &bounds.upper {
        Some((value, inclusive)) => {
            let tuple = search_tuple(&bounds.prefix_eq, Some(value));
            if *inclusive {
                upper_bound_tuple(&key_cols, &tuple)
            } else {
                lower_bound_tuple(&key_cols, &tuple)
            }
        }
        None => key_batch.num_rows(),
    };
    if start >= end {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let selected_rows = selected_live_rows(reader, rg_idx, start, end);
    if selected_rows.is_empty() {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let union_cols = union_columns(projection_cols, predicate_cols);
    let mut columns_by_idx: std::collections::HashMap<usize, ColumnArray> =
        std::collections::HashMap::with_capacity(union_cols.len());
    let mut cols_to_read = Vec::with_capacity(union_cols.len());

    for (pos, &col_idx) in key_cols_idx.iter().enumerate() {
        if !union_cols.contains(&col_idx) || cols_to_read.contains(&col_idx) {
            continue;
        }
        let key_col = key_cols[pos];
        let key_values = if reader.delete_bitmap().is_empty() {
            key_col.slice_fast(start, end)
        } else {
            let key_mask = selected_rows_mask(key_batch.num_rows(), &selected_rows);
            key_col.filter_by_mask_fast(&key_mask, selected_rows.len())
        };
        columns_by_idx.insert(col_idx, key_values);
    }

    for &col_idx in &union_cols {
        if !key_cols_idx.contains(&col_idx) && !cols_to_read.contains(&col_idx) {
            cols_to_read.push(col_idx);
        }
    }

    let aligned_cols_to_read = reader.align_indices(table_schema, &cols_to_read)?;
    let mut read_by_footer: std::collections::HashMap<usize, ColumnArray> = reader
        .read_columns_filtered(rg_idx, &aligned_cols_to_read, &selected_rows)?
        .into_iter()
        .collect();
    for (&catalog_idx, &footer_idx) in cols_to_read.iter().zip(&aligned_cols_to_read) {
        if let Some(col) = read_by_footer.remove(&footer_idx) {
            columns_by_idx.insert(catalog_idx, col);
        }
    }

    let mut union_columns_data = Vec::with_capacity(union_cols.len());
    for &col_idx in &union_cols {
        let col = columns_by_idx.get(&col_idx).cloned().ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(format!(
                "candidate column {} was not read",
                col_idx
            ))
        })?;
        union_columns_data.push(col);
    }
    let union_schema = Schema::new(
        union_cols
            .iter()
            .map(|&idx| table_schema.columns[idx].clone())
            .collect(),
    );
    let union_batch = RecordBatch::try_new(union_schema, union_columns_data)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))?;

    let col_pos_by_original: std::collections::HashMap<usize, usize> = union_cols
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();
    let remapped_eq = remap_eq_predicates(eq_predicates, &col_pos_by_original)?;
    let remapped_range = remap_range_predicates(range_predicates, &col_pos_by_original)?;
    let remapped_in = remap_in_predicates(in_predicates, &col_pos_by_original)?;
    let remapped_remaining = remaining_predicate
        .map(|expr| remap_expr(expr, &col_pos_by_original))
        .transpose()?;
    let filtered = apply_predicates(
        &union_batch,
        &remapped_eq,
        &remapped_range,
        &remapped_in,
        remapped_remaining.as_ref(),
    )?;

    project_filtered_union(&filtered, &union_cols, projection_cols)
}

fn simple_cluster_eq_value<'a>(
    key_col_idx: usize,
    bounds: &'a ClusterKeyBounds,
    eq_predicates: &'a [(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> Option<&'a ScalarValue> {
    if !in_predicates.is_empty() || remaining_predicate.is_some() {
        return None;
    }
    let (Some((lower, true)), Some((upper, true))) = (&bounds.lower, &bounds.upper) else {
        return None;
    };
    if crate::storage::index::sort_key::compare_scalars_for_sort(lower, upper)
        != std::cmp::Ordering::Equal
    {
        return None;
    }
    if eq_predicates.iter().any(|(col_idx, value)| {
        *col_idx != key_col_idx
            || crate::storage::index::sort_key::compare_scalars_for_sort(value, lower)
                != std::cmp::Ordering::Equal
    }) {
        return None;
    }
    if range_predicates.iter().any(|clause| {
        clause.column_index != key_col_idx
            || clause.op != crate::storage::partition::pruning::RangeOp::Eq
            || crate::storage::index::sort_key::compare_scalars_for_sort(&clause.value, lower)
                != std::cmp::Ordering::Equal
    }) {
        return None;
    }
    Some(lower)
}

fn cluster_key_eq_read(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    rg_idx: usize,
    key_col_idx: usize,
    value: &ScalarValue,
    projection_cols: &[usize],
) -> crate::common::error::Result<RecordBatch> {
    // The raw block and its decoding def live in footer space; the pinned
    // value and the output projection stay in catalog space.
    let aligned_key = reader.align_indices(table_schema, &[key_col_idx])?;
    let footer_key = aligned_key[0];
    let raw = reader.read_column_raw(rg_idx, footer_key)?;
    let col_def = &reader.schema().columns[footer_key];
    let mut selected_rows =
        selected_eq_rows_from_raw(&raw, reader.row_group_rows(rg_idx) as usize, col_def, value)?;
    if !reader.delete_bitmap().is_empty() {
        let rg_start_row = reader.row_group_start_row(rg_idx);
        selected_rows.retain(|row| !reader.is_row_deleted(rg_start_row + *row as u64));
    }
    if selected_rows.is_empty() {
        return Ok(empty_with_projection(table_schema, projection_cols));
    }

    let cols_to_read: Vec<usize> = projection_cols
        .iter()
        .copied()
        .filter(|idx| *idx != key_col_idx)
        .collect();
    let mut columns_by_idx: std::collections::HashMap<usize, ColumnArray> =
        std::collections::HashMap::with_capacity(projection_cols.len());

    if projection_cols.contains(&key_col_idx) {
        columns_by_idx.insert(
            key_col_idx,
            repeated_scalar_column(value, selected_rows.len())?,
        );
    }

    let aligned_cols_to_read = reader.align_indices(table_schema, &cols_to_read)?;
    let mut read_by_footer: std::collections::HashMap<usize, ColumnArray> = reader
        .read_columns_filtered(rg_idx, &aligned_cols_to_read, &selected_rows)?
        .into_iter()
        .collect();
    for (&catalog_idx, &footer_idx) in cols_to_read.iter().zip(&aligned_cols_to_read) {
        if let Some(col) = read_by_footer.remove(&footer_idx) {
            columns_by_idx.insert(catalog_idx, col);
        }
    }

    let mut columns = Vec::with_capacity(projection_cols.len());
    for &col_idx in projection_cols {
        let col = columns_by_idx.remove(&col_idx).ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(format!(
                "projected column {} was not read",
                col_idx
            ))
        })?;
        columns.push(col);
    }

    let schema = Schema::new(
        projection_cols
            .iter()
            .map(|&idx| table_schema.columns[idx].clone())
            .collect(),
    );
    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

fn repeated_scalar_column(
    value: &ScalarValue,
    len: usize,
) -> crate::common::error::Result<ColumnArray> {
    let bitmap = NullBitmap::new_valid(len);
    let col = match value {
        ScalarValue::Bool(Some(v)) => ColumnArray::Bool {
            data: vec![u8::from(*v); len],
            bitmap,
        },
        ScalarValue::Int8(Some(v)) => ColumnArray::Int8 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Int16(Some(v)) => ColumnArray::Int16 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Int32(Some(v)) => ColumnArray::Int32 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Int64(Some(v)) => ColumnArray::Int64 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::UInt8(Some(v)) => ColumnArray::UInt8 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::UInt16(Some(v)) => ColumnArray::UInt16 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::UInt32(Some(v)) => ColumnArray::UInt32 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::UInt64(Some(v)) => ColumnArray::UInt64 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Float32(Some(v)) => ColumnArray::Float32 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Float64(Some(v)) => ColumnArray::Float64 {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Decimal(Some(v)) => ColumnArray::Decimal {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Date(Some(v)) => ColumnArray::Date {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Time(Some(v)) => ColumnArray::Time {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Timestamp(Some(v)) => ColumnArray::Timestamp {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::Interval(Some(v)) => ColumnArray::Interval {
            data: vec![*v; len],
            bitmap,
        },
        ScalarValue::VarChar(Some(v)) => ColumnArray::VarChar {
            data: vec![v.clone(); len].into(),
            bitmap,
        },
        _ => {
            return Err(crate::common::error::TaibaiError::Internal(
                "cluster key equality value cannot be NULL".into(),
            ))
        }
    };
    Ok(col)
}

/// Compare one row against the search tuple lexicographically over the key
/// columns. A NULL row value sorts after any non-NULL bound (mirroring the
/// `compare_at` convention); the surviving slice is re-filtered by the
/// regular predicates, so a slightly wide slice stays correct.
fn compare_row_to_tuple(
    cols: &[&ColumnArray],
    tuple: &[ScalarValue],
    row: usize,
) -> std::cmp::Ordering {
    for (col, value) in cols.iter().zip(tuple.iter()) {
        match col.compare_scalar_at(row, value) {
            Some(std::cmp::Ordering::Equal) => {}
            Some(ord) => return ord,
            None => return std::cmp::Ordering::Greater,
        }
    }
    std::cmp::Ordering::Equal
}

/// Prefix equality values plus an optional range bound value.
fn search_tuple(
    prefix_eq: &[(usize, ScalarValue)],
    bound: Option<&ScalarValue>,
) -> Vec<ScalarValue> {
    let mut tuple: Vec<ScalarValue> = prefix_eq.iter().map(|(_, value)| value.clone()).collect();
    if let Some(value) = bound {
        tuple.push(value.clone());
    }
    tuple
}

fn lower_bound_tuple(cols: &[&ColumnArray], tuple: &[ScalarValue]) -> usize {
    let mut lo = 0usize;
    let mut hi = cols.first().map_or(0, |col| col.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if compare_row_to_tuple(cols, tuple, mid) == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound_tuple(cols: &[&ColumnArray], tuple: &[ScalarValue]) -> usize {
    let mut lo = 0usize;
    let mut hi = cols.first().map_or(0, |col| col.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if compare_row_to_tuple(cols, tuple, mid) == std::cmp::Ordering::Greater {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// The leading cluster-key subset is non-decreasing. Lexicographic sort over
/// the full cluster key implies this for every prefix of it.
fn key_tuple_is_sorted(cols: &[&ColumnArray]) -> bool {
    let len = cols.first().map_or(0, |col| col.len());
    for row_idx in 1..len {
        let mut ord = std::cmp::Ordering::Equal;
        for col in cols {
            ord = col.compare_at(row_idx, row_idx - 1);
            if ord != std::cmp::Ordering::Equal {
                break;
            }
        }
        if ord == std::cmp::Ordering::Less {
            return false;
        }
    }
    true
}

fn selected_live_rows(
    reader: &crate::storage::segment::SegmentReader,
    rg_idx: usize,
    start: usize,
    end: usize,
) -> Vec<usize> {
    if reader.delete_bitmap().is_empty() {
        return (start..end).collect();
    }

    // O(1) prefix sum instead of cloning the whole footer per row group.
    let rg_start_row: u64 = reader.row_group_start_row(rg_idx);
    (start..end)
        .filter(|row| !reader.is_row_deleted(rg_start_row + *row as u64))
        .collect()
}

fn selected_rows_mask(num_rows: usize, selected_rows: &[usize]) -> Vec<bool> {
    let mut mask = vec![false; num_rows];
    for &row in selected_rows {
        mask[row] = true;
    }
    mask
}

fn union_columns(projection_cols: &[usize], predicate_cols: &[usize]) -> Vec<usize> {
    let mut cols = Vec::with_capacity(projection_cols.len() + predicate_cols.len());
    for &col in projection_cols.iter().chain(predicate_cols.iter()) {
        if !cols.contains(&col) {
            cols.push(col);
        }
    }
    cols
}

fn remap_eq_predicates(
    predicates: &[(usize, ScalarValue)],
    col_pos_by_original: &std::collections::HashMap<usize, usize>,
) -> crate::common::error::Result<Vec<(usize, ScalarValue)>> {
    predicates
        .iter()
        .map(|(idx, value)| {
            Ok((
                *col_pos_by_original.get(idx).ok_or_else(|| {
                    crate::common::error::TaibaiError::Internal(format!(
                        "predicate column {} was not read",
                        idx
                    ))
                })?,
                value.clone(),
            ))
        })
        .collect()
}

fn remap_range_predicates(
    predicates: &[crate::storage::partition::pruning::RangeClause],
    col_pos_by_original: &std::collections::HashMap<usize, usize>,
) -> crate::common::error::Result<Vec<crate::storage::partition::pruning::RangeClause>> {
    predicates
        .iter()
        .map(|clause| {
            let column_index = *col_pos_by_original
                .get(&clause.column_index)
                .ok_or_else(|| {
                    crate::common::error::TaibaiError::Internal(format!(
                        "predicate column {} was not read",
                        clause.column_index
                    ))
                })?;
            Ok(crate::storage::partition::pruning::RangeClause {
                column_index,
                op: clause.op,
                value: clause.value.clone(),
            })
        })
        .collect()
}

fn remap_in_predicates(
    predicates: &[InPredicate],
    col_pos_by_original: &std::collections::HashMap<usize, usize>,
) -> crate::common::error::Result<Vec<InPredicate>> {
    predicates
        .iter()
        .map(|pred| {
            let column_index = *col_pos_by_original.get(&pred.column_index).ok_or_else(|| {
                crate::common::error::TaibaiError::Internal(format!(
                    "predicate column {} was not read",
                    pred.column_index
                ))
            })?;
            Ok(InPredicate {
                column_index,
                values: pred.values.clone(),
            })
        })
        .collect()
}

fn remap_expr(
    expr: &crate::sql::planner::LogicalExpr,
    col_pos_by_original: &std::collections::HashMap<usize, usize>,
) -> crate::common::error::Result<crate::sql::planner::LogicalExpr> {
    use crate::sql::planner::LogicalExpr;
    match expr {
        LogicalExpr::Column(idx) => {
            let pos = *col_pos_by_original.get(idx).ok_or_else(|| {
                crate::common::error::TaibaiError::Internal(format!(
                    "predicate column {} was not read",
                    idx
                ))
            })?;
            Ok(LogicalExpr::Column(pos))
        }
        LogicalExpr::Literal(value) => Ok(LogicalExpr::Literal(value.clone())),
        LogicalExpr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryOp {
            left: Box::new(remap_expr(left, col_pos_by_original)?),
            op: *op,
            right: Box::new(remap_expr(right, col_pos_by_original)?),
        }),
        LogicalExpr::Not(inner) => Ok(LogicalExpr::Not(Box::new(remap_expr(
            inner,
            col_pos_by_original,
        )?))),
        LogicalExpr::IsNull(inner) => Ok(LogicalExpr::IsNull(Box::new(remap_expr(
            inner,
            col_pos_by_original,
        )?))),
        LogicalExpr::IsNotNull(inner) => Ok(LogicalExpr::IsNotNull(Box::new(remap_expr(
            inner,
            col_pos_by_original,
        )?))),
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => Ok(LogicalExpr::In {
            expr: Box::new(remap_expr(expr, col_pos_by_original)?),
            values: values.clone(),
            negated: *negated,
        }),
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(LogicalExpr::Between {
            expr: Box::new(remap_expr(expr, col_pos_by_original)?),
            low: Box::new(remap_expr(low, col_pos_by_original)?),
            high: Box::new(remap_expr(high, col_pos_by_original)?),
            negated: *negated,
        }),
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Ok(LogicalExpr::Case {
            operand: operand
                .as_ref()
                .map(|expr| remap_expr(expr, col_pos_by_original).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        remap_expr(when_expr, col_pos_by_original)?,
                        remap_expr(then_expr, col_pos_by_original)?,
                    ))
                })
                .collect::<crate::common::error::Result<_>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|expr| remap_expr(expr, col_pos_by_original).map(Box::new))
                .transpose()?,
        }),
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => Ok(LogicalExpr::Like {
            expr: Box::new(remap_expr(expr, col_pos_by_original)?),
            pattern: pattern.clone(),
            negated: *negated,
        }),
        LogicalExpr::Function { name, args } => Ok(LogicalExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_expr(arg, col_pos_by_original))
                .collect::<crate::common::error::Result<_>>()?,
        }),
        LogicalExpr::Aggregate(agg) => {
            Ok(LogicalExpr::Aggregate(crate::sql::planner::AggregateExpr {
                func: agg.func,
                arg: Box::new(remap_expr(&agg.arg, col_pos_by_original)?),
                distinct: agg.distinct,
            }))
        }
        LogicalExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(LogicalExpr::InSubquery {
            expr: Box::new(remap_expr(expr, col_pos_by_original)?),
            subquery: subquery.clone(),
            negated: *negated,
        }),
        other => Ok(other.clone()),
    }
}

fn project_filtered_union(
    batch: &RecordBatch,
    union_cols: &[usize],
    projection_cols: &[usize],
) -> crate::common::error::Result<RecordBatch> {
    let mut columns = Vec::with_capacity(projection_cols.len());
    let mut col_defs = Vec::with_capacity(projection_cols.len());
    for &col_idx in projection_cols {
        let pos = union_cols
            .iter()
            .position(|idx| *idx == col_idx)
            .ok_or_else(|| {
                crate::common::error::TaibaiError::Internal(format!(
                    "projected column {} was not read",
                    col_idx
                ))
            })?;
        columns.push(batch.column(pos).cloned().ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(format!(
                "projected column {} missing from candidate batch",
                col_idx
            ))
        })?);
        col_defs.push(batch.schema.columns[pos].clone());
    }
    RecordBatch::try_new(Schema::new(col_defs), columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

fn empty_like(batch: &RecordBatch) -> RecordBatch {
    RecordBatch::new(batch.schema.clone())
}

/// Empty batch shaped like `projection_cols`. The column defs come from the
/// catalog schema (request order), not the segment footer: the output of a
/// read is addressed by catalog position downstream, so a drifted footer
/// must not lend it stale names or metadata.
fn empty_with_projection(
    table_schema: &crate::types::batch::Schema,
    projection_cols: &[usize],
) -> RecordBatch {
    let proj_schema = crate::types::batch::Schema::new(
        projection_cols
            .iter()
            .map(|&idx| table_schema.columns[idx].clone())
            .collect(),
    );
    RecordBatch::new(proj_schema)
}

/// Translate catalog column indices into one segment's footer column space
/// by name (current name, then previous names). Hand-built specs (tests)
/// carry an empty catalog schema and keep identity mapping.
pub(super) fn align_indices_to_footer(
    table_schema: &Schema,
    footer_schema: &Schema,
    indices: &[usize],
) -> crate::common::error::Result<Vec<usize>> {
    if table_schema.num_columns() == 0 {
        return Ok(indices.to_vec());
    }
    crate::storage::segment::reader::align_indices_against_schema(
        table_schema,
        footer_schema,
        indices,
    )
}

/// Whether any of the catalog-space `indices` names a column the footer
/// schema predates. COUNT fast paths use this to route such segments to the
/// default-fill evaluation (the SELECT/aggregate semantics) instead of
/// failing the alignment on the missing column.
pub(super) fn any_predicate_column_absent(
    table_schema: &Schema,
    footer_schema: &Schema,
    indices: &[usize],
) -> bool {
    if table_schema.num_columns() == 0 {
        return false;
    }
    crate::storage::segment::reader::any_column_absent_in_schema(
        table_schema,
        footer_schema,
        indices,
    )
}

/// Re-index a row-group predicate from catalog space onto one segment's
/// footer column order, so zone-map pruning indexes the columns the clauses
/// actually reference instead of whatever sits at the same position.
pub(super) fn align_predicate_to_footer(
    table_schema: &Schema,
    footer_schema: &Schema,
    predicate: &crate::storage::partition::pruning::RowGroupPredicate,
) -> crate::common::error::Result<crate::storage::partition::pruning::RowGroupPredicate> {
    if predicate.clauses.is_empty() || table_schema.num_columns() == 0 {
        return Ok(predicate.clone());
    }
    let indices: Vec<usize> = predicate
        .clauses
        .iter()
        .map(|clause| clause.column_index)
        .collect();
    let aligned = align_indices_to_footer(table_schema, footer_schema, &indices)?;
    let clauses = predicate
        .clauses
        .iter()
        .zip(aligned)
        .map(
            |(clause, column_index)| crate::storage::partition::pruning::RangeClause {
                column_index,
                op: clause.op,
                value: clause.value.clone(),
            },
        )
        .collect();
    Ok(crate::storage::partition::pruning::RowGroupPredicate::new(
        clauses,
    ))
}

/// Row-group prune inputs (predicate + bloom/set check lists) re-indexed
/// onto one segment's footer column order. The check lists index footer
/// zone maps, bloom filters, and value sets positionally, exactly like the
/// predicate clauses do.
pub(super) struct FooterPruneChecks {
    pub predicate: crate::storage::partition::pruning::RowGroupPredicate,
    pub bloom_checks: Vec<(usize, ScalarValue)>,
    pub in_checks: Vec<InPredicate>,
    pub set_checks: Vec<(usize, ScalarValue)>,
    pub set_in_checks: Vec<InPredicate>,
}

impl FooterPruneChecks {
    pub fn in_check_refs(&self) -> Vec<&InPredicate> {
        self.in_checks.iter().collect()
    }

    pub fn set_in_check_refs(&self) -> Vec<&InPredicate> {
        self.set_in_checks.iter().collect()
    }
}

/// Translate the full prune input set onto `footer_schema` by name.
pub(super) fn align_prune_checks_to_footer(
    table_schema: &Schema,
    footer_schema: &Schema,
    predicate: &crate::storage::partition::pruning::RowGroupPredicate,
    bloom_checks: &[(usize, ScalarValue)],
    in_checks: &[&InPredicate],
    set_checks: &[(usize, ScalarValue)],
    set_in_checks: &[&InPredicate],
) -> crate::common::error::Result<FooterPruneChecks> {
    let aligned_bloom =
        align_indices_to_footer(table_schema, footer_schema, &check_columns(bloom_checks))?;
    let aligned_in = align_indices_to_footer(
        table_schema,
        footer_schema,
        &in_checks.iter().map(|p| p.column_index).collect::<Vec<_>>(),
    )?;
    let aligned_set =
        align_indices_to_footer(table_schema, footer_schema, &check_columns(set_checks))?;
    let aligned_set_in = align_indices_to_footer(
        table_schema,
        footer_schema,
        &set_in_checks
            .iter()
            .map(|p| p.column_index)
            .collect::<Vec<_>>(),
    )?;
    Ok(FooterPruneChecks {
        predicate: align_predicate_to_footer(table_schema, footer_schema, predicate)?,
        bloom_checks: bloom_checks
            .iter()
            .zip(aligned_bloom)
            .map(|((_, value), column)| (column, value.clone()))
            .collect(),
        in_checks: in_checks
            .iter()
            .zip(aligned_in)
            .map(|(pred, column_index)| InPredicate {
                column_index,
                values: pred.values.clone(),
            })
            .collect(),
        set_checks: set_checks
            .iter()
            .zip(aligned_set)
            .map(|((_, value), column)| (column, value.clone()))
            .collect(),
        set_in_checks: set_in_checks
            .iter()
            .zip(aligned_set_in)
            .map(|(pred, column_index)| InPredicate {
                column_index,
                values: pred.values.clone(),
            })
            .collect(),
    })
}

fn check_columns(checks: &[(usize, ScalarValue)]) -> Vec<usize> {
    checks.iter().map(|(column, _)| *column).collect()
}

/// Catalog→footer index map for `columns` on one segment. Consumers use
/// [`mapped_column`] for the lookup, which falls back to identity so
/// hand-built specs (empty catalog schema) keep working unchanged.
pub(super) fn align_column_map(
    table_schema: &Schema,
    footer_schema: &Schema,
    columns: &[usize],
) -> crate::common::error::Result<std::collections::HashMap<usize, usize>> {
    let aligned = align_indices_to_footer(table_schema, footer_schema, columns)?;
    Ok(columns.iter().copied().zip(aligned).collect())
}

/// Footer-space index for `column` under `map` (identity fallback).
pub(super) fn mapped_column(map: &std::collections::HashMap<usize, usize>, column: usize) -> usize {
    map.get(&column).copied().unwrap_or(column)
}

/// Result of pruning one segment against its sidecar meta snapshot.
pub(super) enum MetaPrune {
    /// The footer could not be aligned onto the catalog schema (or a check
    /// references a column the footer genuinely lacks): NO pruning is
    /// possible here. The caller must fall back to reader-side pruning,
    /// which re-aligns per segment — catalog indices must never be used
    /// positionally against footer-space metadata.
    Unaligned,
    /// Aligned and pruned: the row groups worth decoding. Empty means the
    /// segment-level zone maps pruned the whole segment away.
    Candidates(Vec<usize>),
}

/// The only sanctioned prune against a `SegmentMetaSnapshot`: aligns the
/// catalog-space spec (predicate, bloom / in / set check lists) onto the
/// footer's own column order BEFORE consulting its zone maps and row-group
/// metadata.
///
/// The main scan paths once skipped this alignment and fed catalog indices
/// straight into footer-space metadata. After any DDL that shifts catalog
/// positions while sealed segments keep their old layout, they pruned with
/// the wrong column's zone maps and silently skipped row groups containing
/// matches. This gateway makes that mistake unrepresentable: there is no
/// path from a `ScanSpec` to meta-snapshot pruning that skips the
/// translation (the snapshot's raw fields are module-private, exposed only
/// as `footer_*` accessors whose names carry the space).
pub(super) fn meta_prune_row_groups(
    meta: &crate::storage::segment::meta::SegmentMetaSnapshot,
    spec: &super::spec::ScanSpec,
) -> crate::common::error::Result<MetaPrune> {
    let footer_schema = meta.footer_schema();
    let predicate_columns = spec.all_predicate_columns();
    if !predicate_columns.is_empty()
        && any_predicate_column_absent(&spec.table_schema, footer_schema, &predicate_columns)
    {
        // A predicate on a column this segment predates cannot prune: every
        // row logically carries the column's DEFAULT, so every row group
        // stays a candidate (the reader gate routes drifted segments
        // through default-filled evaluation).
        return Ok(MetaPrune::Candidates(
            (0..meta.row_group_count()).collect(),
        ));
    }
    let aligned = match align_prune_checks_to_footer(
        &spec.table_schema,
        footer_schema,
        &spec.to_row_group_predicate(),
        &super::row_group::build_bloom_check_list(spec),
        &super::row_group::build_in_check_list(spec),
        &super::row_group::build_set_check_list(spec),
        &super::row_group::build_set_in_check_list(spec),
    ) {
        Ok(aligned) => aligned,
        Err(error) => {
            // An unresolvable check column (e.g. a bloom-index column the
            // footer predates) must not fail the query — decline pruning.
            tracing::debug!(
                error = %error,
                "meta-snapshot prune declined: footer alignment failed"
            );
            return Ok(MetaPrune::Unaligned);
        }
    };
    if !super::row_group::prune_segment_by_zone_map(&aligned.predicate, meta.footer_zone_maps()) {
        return Ok(MetaPrune::Candidates(Vec::new()));
    }
    let in_refs = aligned.in_check_refs();
    let set_in_refs = aligned.set_in_check_refs();
    Ok(MetaPrune::Candidates(
        super::row_group::prune_row_group_meta(
            meta,
            &aligned.predicate,
            &aligned.bloom_checks,
            &in_refs,
            &aligned.set_checks,
            &set_in_refs,
        )
        .candidate_indices,
    ))
}

#[cfg(test)]
#[path = "row_filter_test.rs"]
mod tests;
