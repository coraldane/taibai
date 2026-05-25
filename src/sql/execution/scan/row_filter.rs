use crate::storage::codec::fastscan::{apply_filter_mask_fast, FastScanEQ};
use crate::storage::partition::pruning::RangeOp;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::spec::InPredicate;

pub fn apply_predicates(
    batch: &RecordBatch,
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> RecordBatch {
    if batch.num_rows() == 0 {
        return batch.clone();
    }

    let mut mask: Vec<bool> = vec![true; batch.num_rows()];

    for (col_idx, value) in eq_predicates {
        apply_eq_mask(batch, *col_idx, value, &mut mask);
        if mask.iter().all(|m| !m) {
            return empty_like(batch);
        }
    }

    for clause in range_predicates {
        apply_range_mask(batch, clause, &mut mask);
        if mask.iter().all(|m| !m) {
            return empty_like(batch);
        }
    }

    for pred in in_predicates {
        apply_in_mask(batch, pred, &mut mask);
        if mask.iter().all(|m| !m) {
            return empty_like(batch);
        }
    }

    if let Some(expr) = remaining_predicate {
        apply_remaining_mask(batch, expr, &mut mask);
    }

    let match_count = mask.iter().filter(|&&m| m).count();
    if match_count == batch.num_rows() {
        return batch.clone();
    }
    if match_count == 0 {
        return empty_like(batch);
    }

    apply_filter_mask_fast(batch, &mask)
}

pub fn apply_eq_mask(
    batch: &RecordBatch,
    col_idx: usize,
    value: &ScalarValue,
    mask: &mut Vec<bool>,
) {
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
    mask: &mut Vec<bool>,
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

fn apply_in_mask(batch: &RecordBatch, pred: &InPredicate, mask: &mut Vec<bool>) {
    if let Some(col) = batch.column(pred.column_index) {
        for (i, m) in mask.iter_mut().enumerate() {
            if !*m {
                continue;
            }
            let val = col.scalar(i);
            let found = pred.values.iter().any(|v| scalar_eq(&val, v));
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
    mask: &mut Vec<bool>,
) {
    if let Ok(row_mask) = eval_logical_expr_batch(expr, batch) {
        for (i, m) in row_mask.iter().enumerate() {
            mask[i] = mask[i] && *m;
        }
    }
}

fn eval_logical_expr_batch(
    expr: &crate::sql::planner::LogicalExpr,
    batch: &RecordBatch,
) -> crate::common::error::Result<Vec<bool>> {
    use crate::sql::planner::LogicalExpr;
    let num_rows = batch.num_rows();
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            use crate::sql::parser::ast::BinOp;
            match op {
                BinOp::And => {
                    let lm = eval_logical_expr_batch(left, batch)?;
                    let rm = eval_logical_expr_batch(right, batch)?;
                    Ok(lm.iter().zip(rm.iter()).map(|(l, r)| *l && *r).collect())
                }
                BinOp::Or => {
                    let lm = eval_logical_expr_batch(left, batch)?;
                    let rm = eval_logical_expr_batch(right, batch)?;
                    Ok(lm.iter().zip(rm.iter()).map(|(l, r)| *l || *r).collect())
                }
                BinOp::Eq => {
                    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.eq_vec(val));
                        }
                    }
                    if let (LogicalExpr::Literal(val), LogicalExpr::Column(idx)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.eq_vec(val));
                        }
                    }
                    Ok(vec![true; num_rows])
                }
                BinOp::Lt => {
                    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.lt_vec(val));
                        }
                    }
                    Ok(vec![true; num_rows])
                }
                BinOp::Gt => {
                    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.gt_vec(val));
                        }
                    }
                    Ok(vec![true; num_rows])
                }
                BinOp::Lte => {
                    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.le_vec(val));
                        }
                    }
                    Ok(vec![true; num_rows])
                }
                BinOp::Gte => {
                    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) =
                        (&**left, &**right)
                    {
                        if let Some(col) = batch.column(*idx) {
                            return Ok(col.ge_vec(val));
                        }
                    }
                    Ok(vec![true; num_rows])
                }
                _ => Ok(vec![true; num_rows]),
            }
        }
        LogicalExpr::Not(inner) => {
            let inner_mask = eval_logical_expr_batch(inner, batch)?;
            Ok(inner_mask.iter().map(|m| !m).collect())
        }
        LogicalExpr::IsNull(inner) => {
            if let LogicalExpr::Column(idx) = &**inner {
                if let Some(col) = batch.column(*idx) {
                    return Ok(col.is_null_vec());
                }
            }
            Ok(vec![true; num_rows])
        }
        LogicalExpr::IsNotNull(inner) => {
            if let LogicalExpr::Column(idx) = &**inner {
                if let Some(col) = batch.column(*idx) {
                    return Ok(col.is_not_null_vec());
                }
            }
            Ok(vec![true; num_rows])
        }
        _ => Ok(vec![true; num_rows]),
    }
}

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

    let mut mask: Vec<bool> = vec![true; pred_batch.num_rows()];

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
                    let val = col.scalar(i);
                    let found = pred.values.iter().any(|v| scalar_eq(&val, v));
                    *m = found;
                }
            }
        }
        if mask.iter().all(|m| !m) {
            return Ok(empty_with_projection(reader, rg_idx, projection_cols));
        }
    }

    if let Some(expr) = remaining_predicate {
        apply_remaining_mask(&pred_batch, expr, &mut mask);
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
            crate::common::error::MonaxError::Internal(format!(
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
        .map_err(|e| crate::common::error::MonaxError::Internal(e.to_string()))
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

fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    crate::storage::index::sort_key::compare_scalars_for_sort(a, b) == std::cmp::Ordering::Equal
}

pub fn apply_cluster_key_fast_scan(
    batches: &mut Vec<RecordBatch>,
    cluster_key: &[String],
    eq_predicates: &[(usize, ScalarValue)],
    table_schema: &crate::types::batch::Schema,
) {
    if cluster_key.is_empty() || eq_predicates.is_empty() {
        return;
    }
    let key_col = match cluster_key.first() {
        Some(k) => k,
        None => return,
    };
    let col_idx = match table_schema.index_of(key_col) {
        Some(idx) => idx,
        None => return,
    };
    let value = match eq_predicates.iter().find(|(idx, _)| *idx == col_idx) {
        Some((_, v)) => v.clone(),
        None => return,
    };

    let fast_scan = FastScanEQ::new(col_idx, value);
    let mut filtered = Vec::with_capacity(batches.len());
    for batch in batches.drain(..) {
        if let Some(result) = fast_scan.scan_batch_sorted(&batch, &[col_idx]) {
            if result.num_rows() > 0 {
                filtered.push(result);
            }
        }
    }
    *batches = filtered;
}
