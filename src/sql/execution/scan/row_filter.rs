use crate::storage::codec::fastscan::apply_filter_mask_fast;
use crate::storage::codec::predicate_scan::selected_eq_rows_from_raw;
use crate::storage::partition::pruning::RangeOp;
use crate::types::array::ColumnArray;

/// Expand dictionary columns into plain varchars before typed predicate
/// evaluation. Recursion terminates: the materialized batch has no
/// dictionary columns.
fn materialize_dictionary_columns(
    batch: &RecordBatch,
) -> crate::common::error::Result<RecordBatch> {
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
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
        return RecordBatch::try_new(batch.schema.clone(), columns?);
    }
    Ok(batch.clone())
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
            &materialize_dictionary_columns(batch)?,
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
            &materialize_dictionary_columns(batch)?,
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
    if let Some(col) = batch.column(pred.column_index) {
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
    } else {
        for m in mask.iter_mut() {
            *m = false;
        }
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

#[allow(clippy::too_many_arguments)]
pub fn predicate_first_read(
    reader: &crate::storage::segment::SegmentReader,
    rg_idx: usize,
    predicate_cols: &[usize],
    projection_cols: &[usize],
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    let pred_batch = reader.read_columns_fast(rg_idx, predicate_cols)?;
    if pred_batch.num_rows() == 0 {
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
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
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
    }

    let pred_to_batch_pos: std::collections::HashMap<usize, usize> = predicate_cols
        .iter()
        .enumerate()
        .map(|(pos, &idx)| (idx, pos))
        .collect();

    for (col_idx, value) in eq_predicates {
        if let Some(&pos) = pred_to_batch_pos.get(col_idx) {
            if let Some(col) = pred_batch.column(pos) {
                let col_mask = col.eq_vec(value);
                for (i, m) in col_mask.iter().enumerate() {
                    mask[i] = mask[i] && *m;
                }
            }
        }
        if mask.iter().all(|m| !m) {
            return Ok(empty_with_projection(reader, rg_idx, projection_cols));
        }
    }

    for clause in range_predicates {
        if let Some(&pos) = pred_to_batch_pos.get(&clause.column_index) {
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
            return Ok(empty_with_projection(reader, rg_idx, projection_cols));
        }
    }

    for pred in in_predicates {
        if let Some(&pos) = pred_to_batch_pos.get(&pred.column_index) {
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
            return Ok(empty_with_projection(reader, rg_idx, projection_cols));
        }
    }

    if let Some(expr) = remaining_predicate {
        let remapped_expr = remap_expr(expr, &pred_to_batch_pos)?;
        apply_remaining_mask(&pred_batch, &remapped_expr, &mut mask)?;
    }

    let match_count = mask.iter().filter(|&&m| m).count();
    if match_count == 0 {
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
    }
    if match_count == pred_batch.num_rows() {
        return reader.read_columns_fast(rg_idx, projection_cols);
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
        if let Some(&pos) = pred_to_batch_pos.get(&col_idx) {
            if let Some(col) = pred_batch.column(pos) {
                columns_by_idx.insert(col_idx, col.filter_by_mask_fast(&mask, match_count));
                continue;
            }
        }
        cols_to_read.push(col_idx);
    }

    for (col_idx, col) in reader.read_columns_filtered(rg_idx, &cols_to_read, &selected_rows)? {
        columns_by_idx.insert(col_idx, col);
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
            .map(|&idx| reader.schema().columns[idx].clone())
            .collect(),
    );
    crate::types::batch::RecordBatch::try_new(proj_schema, columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

#[allow(clippy::too_many_arguments)]
pub fn cluster_key_range_read(
    reader: &crate::storage::segment::SegmentReader,
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
            return cluster_key_eq_read(reader, rg_idx, key_col_idx, eq_value, projection_cols);
        }
    }

    let key_cols_idx = bounds.key_column_indices();
    let key_batch = reader.read_columns_fast(rg_idx, &key_cols_idx)?;
    if key_batch.num_rows() == 0 {
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
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
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
    }

    let selected_rows = selected_live_rows(reader, rg_idx, start, end);
    if selected_rows.is_empty() {
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
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

    for (col_idx, col) in reader.read_columns_filtered(rg_idx, &cols_to_read, &selected_rows)? {
        columns_by_idx.insert(col_idx, col);
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
            .map(|&idx| reader.schema().columns[idx].clone())
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
    rg_idx: usize,
    key_col_idx: usize,
    value: &ScalarValue,
    projection_cols: &[usize],
) -> crate::common::error::Result<RecordBatch> {
    let raw = reader.read_column_raw(rg_idx, key_col_idx)?;
    let col_def = &reader.schema().columns[key_col_idx];
    let mut selected_rows =
        selected_eq_rows_from_raw(&raw, reader.row_group_rows(rg_idx) as usize, col_def, value)?;
    if !reader.delete_bitmap().is_empty() {
        let rg_start_row = reader.row_group_start_row(rg_idx);
        selected_rows.retain(|row| !reader.is_row_deleted(rg_start_row + *row as u64));
    }
    if selected_rows.is_empty() {
        return Ok(empty_with_projection(reader, rg_idx, projection_cols));
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

    for (col_idx, col) in reader.read_columns_filtered(rg_idx, &cols_to_read, &selected_rows)? {
        columns_by_idx.insert(col_idx, col);
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
            .map(|&idx| reader.schema().columns[idx].clone())
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

    let rg_start_row: u64 = reader
        .footer_row_groups_info()
        .iter()
        .take(rg_idx)
        .map(|r| r.row_count)
        .sum();
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

fn empty_with_projection(
    reader: &crate::storage::segment::SegmentReader,
    _rg_idx: usize,
    projection_cols: &[usize],
) -> RecordBatch {
    let proj_schema = crate::types::batch::Schema::new(
        projection_cols
            .iter()
            .map(|&idx| reader.schema().columns[idx].clone())
            .collect(),
    );
    RecordBatch::new(proj_schema)
}

#[cfg(test)]
#[path = "row_filter_test.rs"]
mod tests;
