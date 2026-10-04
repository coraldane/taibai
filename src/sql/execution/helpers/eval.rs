//! Vectorized and row-level WHERE evaluators. Three tiers share this file:
//! the batch kernel (`eval_where_clause_batch`), the legacy clause evaluator
//! (`eval_where_clause3`), and the per-row fallback (`eval_expr_for_row`).

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr};

use super::scalar_utils::{
    coerce_scalar_for_dtype, compare_scalars, scalar_like_matches,
    scalar_values_equal,
};

fn col_lit_pair<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a str, &'a ScalarValue)> {
    match (left, right) {
        (Expr::Column(col_name), Expr::Literal(val)) => Some((col_name.as_str(), val)),
        _ => None,
    }
}

/// Resolve a `(column, literal)` pair against the batch and run `kernel` on
/// the column. Returns `None` when the shape does not match or the column is
/// absent from this batch.
fn eval_col_lit_mask(
    pair: Option<(&str, &ScalarValue)>,
    batch: &RecordBatch,
    schema: &Schema,
    kernel: fn(&crate::types::array::ColumnArray, &ScalarValue) -> Vec<bool>,
) -> Option<Result<Vec<bool>>> {
    let (col_name, val) = pair?;
    let col_idx = schema.index_of(col_name)?;
    let col = batch.column(col_idx)?;
    let val = match coerce_scalar_for_dtype(val, schema.columns[col_idx].data_type) {
        Ok(v) => v,
        Err(e) => return Some(Err(e)),
    };
    Some(Ok(kernel(col, &val)))
}

/// Negated comparison kernel: `NOT (col <op> lit)` keeps only rows where the
/// comparison is definitely false, which the direct kernels already express
/// (NULL rows return false, i.e. SQL UNKNOWN is excluded).
fn negated_kernel(
    op: BinOp,
) -> Option<fn(&crate::types::array::ColumnArray, &ScalarValue) -> Vec<bool>> {
    match op {
        BinOp::Eq => Some(crate::types::array::ColumnArray::neq_vec),
        BinOp::Neq => Some(crate::types::array::ColumnArray::eq_vec),
        BinOp::Lt => Some(crate::types::array::ColumnArray::ge_vec),
        BinOp::Lte => Some(crate::types::array::ColumnArray::gt_vec),
        BinOp::Gt => Some(crate::types::array::ColumnArray::le_vec),
        BinOp::Gte => Some(crate::types::array::ColumnArray::lt_vec),
        _ => None,
    }
}

pub fn eval_where_clause_batch(
    expr: &Expr,
    batch: &RecordBatch,
    schema: &Schema,
) -> Result<Vec<bool>> {
    let num_rows = batch.num_rows();
    let row_wise = || -> Result<Vec<bool>> {
        (0..num_rows)
            .map(|i| eval_where_clause(expr, batch, i, schema))
            .collect()
    };
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq => {
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right).or_else(|| col_lit_pair(right, left)),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::eq_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::Neq => {
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right).or_else(|| col_lit_pair(right, left)),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::neq_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::Lt => {
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::lt_vec,
                ) {
                    return mask;
                }
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(right, left),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::gt_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::Gt => {
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::gt_vec,
                ) {
                    return mask;
                }
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(right, left),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::lt_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::Lte => {
                // (col <= lit); for the swapped literal-first form the
                // predicate flips: lit <= col <=> col >= lit.
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::le_vec,
                ) {
                    return mask;
                }
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(right, left),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::ge_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::Gte => {
                // (col >= lit); swapped form: lit >= col <=> col <= lit
                // (inclusive on both sides).
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(left, right),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::ge_vec,
                ) {
                    return mask;
                }
                if let Some(mask) = eval_col_lit_mask(
                    col_lit_pair(right, left),
                    batch,
                    schema,
                    crate::types::array::ColumnArray::le_vec,
                ) {
                    return mask;
                }
                row_wise()
            }
            BinOp::And => {
                let left_mask = eval_where_clause_batch(left, batch, schema)?;
                let right_mask = eval_where_clause_batch(right, batch, schema)?;
                // Two-valued AND over TRUE-only masks is exact for filtering:
                // AND(false, UNKNOWN) = false and AND(true, UNKNOWN) = UNKNOWN
                // both exclude the row.
                Ok(left_mask
                    .iter()
                    .zip(right_mask.iter())
                    .map(|(l, r)| *l && *r)
                    .collect())
            }
            BinOp::Or => {
                let left_mask = eval_where_clause_batch(left, batch, schema)?;
                let right_mask = eval_where_clause_batch(right, batch, schema)?;
                Ok(left_mask
                    .iter()
                    .zip(right_mask.iter())
                    .map(|(l, r)| *l || *r)
                    .collect())
            }
            _ => row_wise(),
        },
        Expr::IsNull(inner) => {
            if let Expr::Column(col_name) = inner.as_ref() {
                if let Some(col_idx) = schema.index_of(col_name) {
                    if let Some(col) = batch.column(col_idx) {
                        return Ok(col.is_null_vec());
                    }
                }
            }
            row_wise()
        }
        Expr::IsNotNull(inner) => {
            if let Expr::Column(col_name) = inner.as_ref() {
                if let Some(col_idx) = schema.index_of(col_name) {
                    if let Some(col) = batch.column(col_idx) {
                        return Ok(col.is_not_null_vec());
                    }
                }
            }
            row_wise()
        }
        Expr::UnaryNot(inner) => {
            // Fast path: NOT over a simple column comparison maps to the
            // negated kernel, which keeps SQL UNKNOWN (NULL rows) excluded.
            if let Expr::BinaryOp {
                left,
                op: inner_op,
                right,
            } = inner.as_ref()
            {
                if let Some(kernel) = negated_kernel(*inner_op) {
                    if let Some(mask) = eval_col_lit_mask(
                        col_lit_pair(left, right).or_else(|| col_lit_pair(right, left)),
                        batch,
                        schema,
                        kernel,
                    ) {
                        return mask;
                    }
                }
            }
            // Everything else needs three-valued logic (NOT flips UNKNOWN,
            // which a boolean mask cannot represent).
            row_wise()
        }
        Expr::In {
            expr,
            values,
            negated,
        } => {
            // Hash-set fast path: a column operand over all-literal
            // candidates (the shape subquery resolution produces) probes a
            // set instead of scanning every candidate per row — a DELETE ...
            // WHERE id IN (large subquery) used to pay O(rows x values)
            // scalar comparisons on every rescanned batch.
            if values.len() > 4 {
                if let Some(mask) =
                    eval_in_column_literal_hashed(expr, values, *negated, batch, schema)
                {
                    return mask;
                }
            }
            // Three-valued IN: a NULL operand makes the row UNKNOWN (excluded
            // for both IN and NOT IN); NOT IN over a list containing NULL
            // matches nothing.
            let const_candidates: Vec<Option<&ScalarValue>> = values
                .iter()
                .map(|c| match c {
                    Expr::Literal(v) => Some(v),
                    _ => None,
                })
                .collect();
            let mut mask = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let value = eval_expr_for_row(expr, batch, row_idx, schema)?;
                if value.is_null() {
                    mask.push(false);
                    continue;
                }
                let mut found = false;
                let mut saw_null = false;
                for (idx, candidate) in values.iter().enumerate() {
                    let candidate = match const_candidates[idx] {
                        Some(v) => v.clone(),
                        None => eval_expr_for_row(candidate, batch, row_idx, schema)?,
                    };
                    if candidate.is_null() {
                        saw_null = true;
                        continue;
                    }
                    if scalar_values_equal(&value, &candidate) {
                        found = true;
                        break;
                    }
                }
                let keep = if *negated { !found && !saw_null } else { found };
                mask.push(keep);
            }
            Ok(mask)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            if !*negated {
                if let (Expr::Column(col_name), Expr::Literal(low_val), Expr::Literal(high_val)) =
                    (expr.as_ref(), low.as_ref(), high.as_ref())
                {
                    if let Some(col_idx) = schema.index_of(col_name) {
                        if let Some(col) = batch.column(col_idx) {
                            let dt = schema.columns[col_idx].data_type;
                            let low = coerce_scalar_for_dtype(low_val, dt)?;
                            let high = coerce_scalar_for_dtype(high_val, dt)?;
                            let ge = col.ge_vec(&low);
                            let le = col.le_vec(&high);
                            return Ok(ge.iter().zip(le.iter()).map(|(a, b)| *a && *b).collect());
                        }
                    }
                }
            }
            row_wise()
        }
        Expr::Literal(v) => {
            // MySQL truthiness: nonzero numerics are TRUE, NULL/zero FALSE.
            let keep = truthiness(v)?.unwrap_or(false);
            Ok(vec![keep; num_rows])
        }
        Expr::Like { .. } => row_wise(),
        _ => row_wise(),
    }
}

/// Hash-set fast path for `column IN (all literals)` over a literal-rich
/// candidate list — the shape subquery resolution produces for
/// `WHERE id IN (SELECT ...)`. The row loop probes the set instead of
/// scanning every candidate per row (O(rows x values) scalar comparisons on
/// every rescanned batch used to dominate large DELETE/UPDATE ... IN).
/// Candidates that do not round-trip through the column's type losslessly
/// (coercions the strict `scalar_values_equal` would reject) stay in a small
/// residual list compared per row, so the equality semantics are unchanged.
/// BLOB/VECTOR/INTERVAL columns keep the generic path: their hashable forms
/// collapse or asymmetrize across `from_scalar`/`from_column`. Returns `None`
/// when the shape does not apply (non-column operand, any non-literal
/// candidate) and the caller keeps its generic loop.
fn eval_in_column_literal_hashed(
    expr: &Expr,
    values: &[Expr],
    negated: bool,
    batch: &RecordBatch,
    schema: &Schema,
) -> Option<Result<Vec<bool>>> {
    use crate::sql::execution::join_hashtable::ScalarValueHashable;
    use crate::types::array::ColumnArray;
    use std::collections::HashSet;

    let Expr::Column(col_name) = expr else {
        return None;
    };
    let col_idx = schema.index_of(col_name.as_str())?;
    let col = batch.column(col_idx)?;
    let dtype = schema.columns[col_idx].data_type;
    if matches!(
        dtype,
        DataType::Blob | DataType::Vector | DataType::Interval
    ) {
        return None;
    }

    let mut saw_null = false;
    let mut set = HashSet::with_capacity(values.len());
    let mut residual: Vec<ScalarValue> = Vec::new();
    for value in values {
        let Expr::Literal(v) = value else {
            return None;
        };
        if v.is_null() {
            saw_null = true;
            continue;
        }
        match coerce_scalar_for_dtype(v, dtype) {
            Ok(coerced) if scalar_values_equal(v, &coerced) => {
                set.insert(ScalarValueHashable::from_scalar(&coerced));
            }
            _ => residual.push(v.clone()),
        }
    }

    // from_column flattens view forms per row; flatten once up front.
    let flattened;
    let col = match col {
        ColumnArray::Selected { .. } | ColumnArray::Rle { .. } | ColumnArray::Dictionary { .. } => {
            flattened = col.flattened();
            flattened
        }
        other => other,
    };

    let mut mask = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        if col.bitmap().is_null(row_idx) {
            // NULL operand: UNKNOWN for both IN and NOT IN — excluded.
            mask.push(false);
            continue;
        }
        let found = set.contains(&ScalarValueHashable::from_column(col, row_idx))
            || residual
                .iter()
                .any(|candidate| scalar_values_equal(&col.scalar(row_idx), candidate));
        // IN keeps only hits; NOT IN keeps misses, and nothing at all when
        // the list contained NULL (three-valued UNKNOWN).
        let keep = if negated { !found && !saw_null } else { found };
        mask.push(keep);
    }
    Some(Ok(mask))
}

/// Three-valued (SQL NULL-aware) predicate evaluation used by DML paths.
/// `None` models SQL UNKNOWN; row filters keep only rows whose predicate is
/// definitely TRUE.
pub fn eval_where_clause3(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<Option<bool>> {
    use std::cmp::Ordering;
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let l = eval_where_clause3(left, batch, row_idx, schema)?;
                if l == Some(false) {
                    return Ok(Some(false));
                }
                let r = eval_where_clause3(right, batch, row_idx, schema)?;
                if r == Some(false) {
                    return Ok(Some(false));
                }
                Ok(if l.is_none() || r.is_none() {
                    None
                } else {
                    Some(true)
                })
            }
            BinOp::Or => {
                let l = eval_where_clause3(left, batch, row_idx, schema)?;
                if l == Some(true) {
                    return Ok(Some(true));
                }
                let r = eval_where_clause3(right, batch, row_idx, schema)?;
                if r == Some(true) {
                    return Ok(Some(true));
                }
                Ok(if l.is_none() || r.is_none() {
                    None
                } else {
                    Some(false)
                })
            }
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => {
                let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
                let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
                if left_val.is_null() || right_val.is_null() {
                    return Ok(None);
                }
                match op {
                    BinOp::Eq => Ok(Some(scalar_values_equal(&left_val, &right_val))),
                    BinOp::Neq => Ok(Some(!scalar_values_equal(&left_val, &right_val))),
                    BinOp::Lt => Ok(Some(
                        compare_scalars(&left_val, &right_val) == Some(Ordering::Less),
                    )),
                    BinOp::Gt => Ok(Some(
                        compare_scalars(&left_val, &right_val) == Some(Ordering::Greater),
                    )),
                    // Incomparable (but non-null) operands match nothing,
                    // exactly like Lt/Gt and the SELECT filter path; the
                    // `!= Some(...)` form used to make them match EVERY row.
                    BinOp::Lte => Ok(Some(
                        matches!(compare_scalars(&left_val, &right_val), Some(o)
                            if o != Ordering::Greater),
                    )),
                    BinOp::Gte => Ok(Some(
                        matches!(compare_scalars(&left_val, &right_val), Some(o)
                            if o != Ordering::Less),
                    )),
                    _ => unreachable!(),
                }
            }
            _ => Err(TaibaiError::Sql(format!(
                "operator {:?} cannot be used as a boolean predicate",
                op
            ))),
        },
        Expr::IsNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(Some(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(Some(!val.is_null()))
        }
        Expr::UnaryNot(inner) => Ok(eval_where_clause3(inner, batch, row_idx, schema)?.map(|b| !b)),
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let value = eval_expr_for_row(expr, batch, row_idx, schema)?;
            if value.is_null() {
                return Ok(None);
            }
            let mut found = false;
            let mut saw_null = false;
            for candidate in values {
                let candidate = eval_expr_for_row(candidate, batch, row_idx, schema)?;
                if candidate.is_null() {
                    saw_null = true;
                    continue;
                }
                if scalar_values_equal(&value, &candidate) {
                    found = true;
                    break;
                }
            }
            let inner = if found {
                Some(true)
            } else if saw_null {
                None
            } else {
                Some(false)
            };
            Ok(if *negated { inner.map(|b| !b) } else { inner })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = eval_expr_for_row(expr, batch, row_idx, schema)?;
            let low = eval_expr_for_row(low, batch, row_idx, schema)?;
            let high = eval_expr_for_row(high, batch, row_idx, schema)?;
            if value.is_null() || low.is_null() || high.is_null() {
                return Ok(None);
            }
            let holds = compare_scalars(&value, &low).is_some_and(|o| o != Ordering::Less)
                && compare_scalars(&value, &high).is_some_and(|o| o != Ordering::Greater);
            Ok(Some(if *negated { !holds } else { holds }))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            for (when, then) in when_clauses {
                let matched = if let Some(operand) = operand {
                    let left = eval_expr_for_row(operand, batch, row_idx, schema)?;
                    if left.is_null() {
                        continue;
                    }
                    let right = eval_expr_for_row(when, batch, row_idx, schema)?;
                    if right.is_null() {
                        continue;
                    }
                    scalar_values_equal(&left, &right)
                } else {
                    eval_where_clause3(when, batch, row_idx, schema)? == Some(true)
                };
                if matched {
                    return truthiness(&eval_expr_for_row(then, batch, row_idx, schema)?);
                }
            }
            match else_clause {
                Some(else_expr) => {
                    truthiness(&eval_expr_for_row(else_expr, batch, row_idx, schema)?)
                }
                None => Ok(None),
            }
        }
        Expr::Literal(v) => Ok(truthiness(v)?),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let value = eval_expr_for_row(expr, batch, row_idx, schema)?;
            let pattern_value = eval_expr_for_row(pattern, batch, row_idx, schema)?;
            if value.is_null() || pattern_value.is_null() {
                return Ok(None);
            }
            let Some(pattern_text) = pattern_value.to_text() else {
                return Ok(Some(false));
            };
            let matched = scalar_like_matches(&value, &pattern_text);
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::Column(_) | Expr::QualifiedColumn { .. } => Err(TaibaiError::Sql(
            "column reference cannot be used as a boolean predicate".to_string(),
        )),
        Expr::UnaryMinus(_) | Expr::WindowFunction { .. } | Expr::Placeholder(_) => Err(
            TaibaiError::Sql("unsupported predicate expression in DML statement".to_string()),
        ),
        Expr::Exists(_) | Expr::Subquery(_) => Err(TaibaiError::Sql(
            "subqueries are not supported in this DML statement's WHERE clause".to_string(),
        )),
        Expr::Correlated(_) => Err(TaibaiError::Sql(
            "correlated subqueries are not supported in this context".to_string(),
        )),
        Expr::Function { .. } => {
            // Function predicates resolve to a scalar value here; comparisons
            // above evaluate them per row.
            truthiness(&eval_expr_for_row(expr, batch, row_idx, schema)?)
        }
    }
}

fn truthiness(v: &ScalarValue) -> Result<Option<bool>> {
    match v {
        ScalarValue::Bool(Some(b)) => Ok(Some(*b)),
        value if value.is_null() => Ok(None),
        ScalarValue::Bool(None) => Ok(None),
        // Numeric predicates follow MySQL truthiness: nonzero is TRUE.
        // `DELETE FROM t WHERE 1` used to work before the evaluator
        // rewrite and must keep working.
        value if value.integer_i128().is_some() => Ok(Some(value.integer_i128() != Some(0))),
        value if value.numeric_f64().is_some() => Ok(Some(value.numeric_f64() != Some(0.0))),
        other => Err(TaibaiError::Sql(format!(
            "predicate must be boolean or numeric, got {:?}",
            other.data_type()
        ))),
    }
}

pub fn eval_where_clause(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<bool> {
    Ok(eval_where_clause3(expr, batch, row_idx, schema)? == Some(true))
}

pub fn eval_expr_for_row(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<ScalarValue> {
    match expr {
        Expr::Column(col_name) => {
            let col_idx = schema
                .index_of(col_name)
                .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", col_name)))?;
            Ok(batch
                .scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(
                    schema.columns[col_idx].data_type,
                )))
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
            let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
            // Delegate to the engine's single scalar binary implementation:
            // this arm used to handle only Int64+Int64 Add/Sub and return the
            // LEFT operand for every other shape (Mul/Div/Mod, floats, Int32
            // columns), silently mis-filtering DML WHERE rows.
            Ok(crate::sql::execution::operators::filter::eval_binary_scalar(
                &left_val, op, &right_val,
            ))
        }
        Expr::IsNull(inner) => Ok(ScalarValue::Bool(Some(
            eval_expr_for_row(inner, batch, row_idx, schema)?.is_null(),
        ))),
        Expr::IsNotNull(inner) => Ok(ScalarValue::Bool(Some(
            !eval_expr_for_row(inner, batch, row_idx, schema)?.is_null(),
        ))),
        Expr::UnaryNot(inner) => match eval_expr_for_row(inner, batch, row_idx, schema)? {
            ScalarValue::Bool(Some(b)) => Ok(ScalarValue::Bool(Some(!b))),
            value if value.is_null() => Ok(ScalarValue::Bool(None)),
            other => Err(TaibaiError::Sql(format!(
                "NOT requires a boolean operand, got {:?}",
                other.data_type()
            ))),
        },
        Expr::QualifiedColumn { column, .. } => {
            let col_idx = schema
                .index_of(column)
                .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", column)))?;
            Ok(batch
                .scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(
                    schema.columns[col_idx].data_type,
                )))
        }
        Expr::Function { name, args, .. } => {
            let arg_values = args
                .iter()
                .map(|a| eval_expr_for_row(a, batch, row_idx, schema))
                .collect::<Result<Vec<_>>>()?;
            SHARED_REGISTRY.with(|registry| registry.execute(name, &arg_values))
        }
        Expr::UnaryMinus(inner) => {
            let value = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(match value {
                ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(-v)),
                ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(-v)),
                ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(-v)),
                ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(-v)),
                ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
                ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
                ScalarValue::Decimal(Some(v)) => ScalarValue::Decimal(Some(-v)),
                other => other,
            })
        }
        Expr::In { .. } => Ok(ScalarValue::Bool(None)),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let holds = match eval_where_clause3(
                &Expr::Between {
                    expr: expr.clone(),
                    low: low.clone(),
                    high: high.clone(),
                    negated: false,
                },
                batch,
                row_idx,
                schema,
            )? {
                Some(b) => b,
                None => return Ok(ScalarValue::Bool(None)),
            };
            Ok(ScalarValue::Bool(Some(if *negated {
                !holds
            } else {
                holds
            })))
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            for (when, then) in when_clauses {
                let matched = if let Some(operand) = operand {
                    let left = eval_expr_for_row(operand, batch, row_idx, schema)?;
                    let right = eval_expr_for_row(when, batch, row_idx, schema)?;
                    if left.is_null() || right.is_null() {
                        continue;
                    }
                    scalar_values_equal(&left, &right)
                } else {
                    eval_where_clause3(when, batch, row_idx, schema)? == Some(true)
                };
                if matched {
                    return eval_expr_for_row(then, batch, row_idx, schema);
                }
            }
            match else_clause {
                Some(else_expr) => eval_expr_for_row(else_expr, batch, row_idx, schema),
                None => Ok(ScalarValue::null_for_type(DataType::Bool)),
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let value = eval_expr_for_row(expr, batch, row_idx, schema)?;
            let pattern_value = eval_expr_for_row(pattern, batch, row_idx, schema)?;
            if value.is_null() || pattern_value.is_null() {
                return Ok(ScalarValue::Bool(None));
            }
            let Some(pattern_text) = pattern_value.to_text() else {
                return Ok(ScalarValue::Bool(None));
            };
            let matched = scalar_like_matches(&value, &pattern_text);
            Ok(ScalarValue::Bool(Some(if *negated {
                !matched
            } else {
                matched
            })))
        }
        Expr::Exists(_) => Ok(ScalarValue::Bool(None)),
        Expr::Correlated(_) => Err(TaibaiError::Sql(
            "correlated subqueries are not supported in this context".to_string(),
        )),
        Expr::Subquery(_) => Err(TaibaiError::Sql(
            "subqueries are not supported in this row-level expression context".to_string(),
        )),
        Expr::WindowFunction { .. } => Err(TaibaiError::Sql(
            "window functions are not supported in this context".to_string(),
        )),
        Expr::Placeholder(_) => Err(TaibaiError::Sql(
            "unresolved placeholder in expression".to_string(),
        )),
    }
}

thread_local! {
    /// Shared per-thread function registry for row-level expression
    /// evaluation (same rationale as `operators::filter`'s fallback
    /// registry: rebuilding ~80 entries per call shows up in profiles).
    static SHARED_REGISTRY: crate::sql::execution::functions::registry::FunctionRegistry =
        crate::sql::execution::functions::registry::FunctionRegistry::default();
}
