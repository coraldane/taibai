use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr};

/// (column, literal) operand pair in either order, normalized so the column
/// comes first.
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
    if matches!(dtype, DataType::Blob | DataType::Vector | DataType::Interval) {
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
        ColumnArray::Selected { .. }
        | ColumnArray::Rle { .. }
        | ColumnArray::Dictionary { .. } => {
            flattened = col.flattened();
            &flattened
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
            Ok(super::operators::filter::eval_binary_scalar(
                &left_val,
                op,
                &right_val,
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

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int64(Some(b))) => (*a as i64).partial_cmp(b),
        (ScalarValue::Int64(Some(a)), ScalarValue::Int32(Some(b))) => a.partial_cmp(&(*b as i64)),
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a.partial_cmp(b),
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Timestamp(Some(a)), ScalarValue::Timestamp(Some(b))) => Some(a.cmp(b)),
        _ => None,
    }
}

fn scalar_values_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int32(Some(a)), ScalarValue::Int64(Some(b))) => *a as i64 == *b,
        (ScalarValue::Int64(Some(a)), ScalarValue::Int32(Some(b))) => *a == *b as i64,
        _ => a == b,
    }
}

/// SQL LIKE pattern match with `%` (any run) and `_` (single char) wildcards.
/// A backslash escapes `%`, `_` and itself. Case-sensitive, char-based.
pub fn sql_like_match(value: &str, pattern: &str) -> bool {
    enum Token {
        Char(char),
        Percent,
        Underscore,
    }
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(escaped @ ('%' | '_' | '\\')) => tokens.push(Token::Char(escaped)),
                Some(other) => {
                    tokens.push(Token::Char('\\'));
                    tokens.push(Token::Char(other));
                }
                None => tokens.push(Token::Char('\\')),
            },
            '%' => tokens.push(Token::Percent),
            '_' => tokens.push(Token::Underscore),
            other => tokens.push(Token::Char(other)),
        }
    }
    let value: Vec<char> = value.chars().collect();
    let (mut vi, mut ti) = (0usize, 0usize);
    let (mut star_token, mut star_value) = (usize::MAX, 0usize);
    while vi < value.len() {
        match tokens.get(ti) {
            Some(Token::Underscore) => {
                vi += 1;
                ti += 1;
            }
            Some(Token::Char(c)) if *c == value[vi] => {
                vi += 1;
                ti += 1;
            }
            Some(Token::Percent) => {
                star_token = ti;
                star_value = vi;
                ti += 1;
            }
            _ => {
                if star_token == usize::MAX {
                    return false;
                }
                star_value += 1;
                vi = star_value;
                ti = star_token + 1;
            }
        }
    }
    while matches!(tokens.get(ti), Some(Token::Percent)) {
        ti += 1;
    }
    ti == tokens.len()
}

/// LIKE semantics for one scalar: NULL never matches (also under NOT LIKE).
/// Non-string values are coerced through their text form.
pub fn scalar_like_matches(value: &ScalarValue, pattern: &str) -> bool {
    if value.is_null() {
        return false;
    }
    match value {
        ScalarValue::VarChar(Some(text)) => sql_like_match(text, pattern),
        other => other
            .to_text()
            .is_some_and(|text| sql_like_match(&text, pattern)),
    }
}

pub fn coerce_scalar_for_dtype(value: &ScalarValue, dtype: DataType) -> Result<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::null_for_type(dtype));
    }
    match (dtype, value) {
        (target, value) if target == value.data_type() => Ok(value.clone()),
        (DataType::Bool, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        (DataType::Bool, ScalarValue::Int32(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        (DataType::Bool, ScalarValue::VarChar(Some(v))) => match v.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(ScalarValue::Bool(Some(true))),
            "false" | "0" | "no" => Ok(ScalarValue::Bool(Some(false))),
            _ => Err(TaibaiError::Sql(format!("invalid bool '{}'", v))),
        },
        (
            target @ (DataType::Int64
            | DataType::Int32
            | DataType::Int16
            | DataType::Int8
            | DataType::UInt64
            | DataType::UInt32
            | DataType::UInt16
            | DataType::UInt8),
            value,
        ) => coerce_integer_scalar(value, target),
        (DataType::Float64, value) => value
            .numeric_f64()
            .map(|v| ScalarValue::Float64(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to FLOAT64", value))),
        (DataType::Float32, value) => value
            .numeric_f64()
            .map(|v| ScalarValue::Float32(Some(v as f32)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to FLOAT32", value))),
        (DataType::VarChar, value) => value
            .to_text()
            .map(|v| ScalarValue::VarChar(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to VARCHAR", value))),
        _ => Ok(value.clone()),
    }
}

/// The single integer coercion ladder, shared by the predicate fast path and
/// the constant-expression evaluator (`eval_expr_to_scalar*`): widening is
/// exact via i128, narrowing and negative-UINT rejections carry explicit
/// errors. Both call sites must stay on this ladder so `WHERE flag = 'yes'`
/// and `SET flag = 'yes'` — and every integer conversion — behave
/// identically no matter which path evaluates them.
pub(crate) fn coerce_integer_scalar(value: &ScalarValue, target: DataType) -> Result<ScalarValue> {
    let Some(raw) = value.integer_i128() else {
        return Err(TaibaiError::Sql(format!(
            "cannot cast {:?} to {}",
            value,
            target.display_name()
        )));
    };
    macro_rules! narrowed {
        ($t:ty, $variant:ident, $name:expr) => {
            match <$t>::try_from(raw) {
                Ok(v) => Ok(ScalarValue::$variant(Some(v))),
                Err(_) if raw < 0 => Err(TaibaiError::Sql(format!(
                    "negative value '{}' for {}",
                    raw, $name
                ))),
                Err(_) => Err(TaibaiError::Sql(format!(
                    "value '{}' overflows {}",
                    raw, $name
                ))),
            }
        };
    }
    match target {
        DataType::Int64 => narrowed!(i64, Int64, "INT64"),
        DataType::Int32 => narrowed!(i32, Int32, "INT32"),
        DataType::Int16 => narrowed!(i16, Int16, "INT16"),
        DataType::Int8 => narrowed!(i8, Int8, "INT8"),
        DataType::UInt64 => narrowed!(u64, UInt64, "UINT64"),
        DataType::UInt32 => narrowed!(u32, UInt32, "UINT32"),
        DataType::UInt16 => narrowed!(u16, UInt16, "UINT16"),
        DataType::UInt8 => narrowed!(u8, UInt8, "UINT8"),
        _ => unreachable!("coerce_integer_scalar called with non-integer target"),
    }
}

pub fn eval_constant_expr(expr: &Expr) -> Result<ScalarValue> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::UnaryMinus(inner) => match eval_constant_expr(inner)? {
            ScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(Some(-v))),
            ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(-v))),
            ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(-v))),
            ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(-v))),
            value if value.is_null() => Ok(ScalarValue::Int64(None)),
            value => Err(TaibaiError::Sql(format!(
                "cannot negate non-numeric constant {:?}",
                value
            ))),
        },
        Expr::BinaryOp { left, op, right } => {
            let left = eval_constant_expr(left)?;
            let right = eval_constant_expr(right)?;
            eval_constant_binary(&left, *op, &right)
        }
        Expr::Function { name, args, .. } => {
            let arg_values = args
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            crate::sql::execution::functions::registry::FunctionRegistry::default()
                .execute(name, &arg_values)
        }
        _ => Err(TaibaiError::Sql(
            "INSERT values must be constant expressions".to_string(),
        )),
    }
}

fn eval_constant_binary(left: &ScalarValue, op: BinOp, right: &ScalarValue) -> Result<ScalarValue> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Int64(None));
    }

    if let (Some(left), Some(right)) = (left.integer_i128(), right.integer_i128()) {
        if !matches!(op, BinOp::Div) {
            let value = match op {
                BinOp::Add => left.checked_add(right),
                BinOp::Sub => left.checked_sub(right),
                BinOp::Mul => left.checked_mul(right),
                BinOp::Mod => {
                    if right == 0 {
                        return Ok(ScalarValue::Int64(None));
                    }
                    left.checked_rem(right)
                }
                _ => None,
            }
            .ok_or_else(|| TaibaiError::Sql("integer constant expression overflow".to_string()))?;
            return i64::try_from(value)
                .map(|value| ScalarValue::Int64(Some(value)))
                .map_err(|_| TaibaiError::Sql("integer constant expression overflow".to_string()));
        }
    }

    let left = left.numeric_f64().ok_or_else(|| {
        TaibaiError::Sql("constant arithmetic requires numeric operands".to_string())
    })?;
    let right = right.numeric_f64().ok_or_else(|| {
        TaibaiError::Sql("constant arithmetic requires numeric operands".to_string())
    })?;
    let value = match op {
        BinOp::Add => left + right,
        BinOp::Sub => left - right,
        BinOp::Mul => left * right,
        BinOp::Div => {
            if right == 0.0 {
                return Err(TaibaiError::Sql("division by zero".to_string()));
            }
            left / right
        }
        BinOp::Mod => {
            if right == 0.0 {
                return Ok(ScalarValue::Float64(None));
            }
            left % right
        }
        _ => {
            return Err(TaibaiError::Sql(
                "unsupported operator in constant expression".to_string(),
            ))
        }
    };
    Ok(ScalarValue::Float64(Some(value)))
}

pub fn eval_expr_to_scalar(expr: &Expr, dt: &DataType) -> Result<ScalarValue> {
    eval_expr_to_scalar_with_scale_timezone(expr, dt, 0, 0)
}

pub fn eval_expr_to_scalar_with_scale_timezone(
    expr: &Expr,
    dt: &DataType,
    scale: i32,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    match expr {
        Expr::Function { .. } | Expr::BinaryOp { .. } | Expr::UnaryMinus(_) => {
            let value = eval_constant_expr(expr)?;
            eval_expr_to_scalar_with_scale_timezone(
                &Expr::Literal(value),
                dt,
                scale,
                timezone_offset_hours,
            )
        }
        Expr::Literal(v) if v.is_null() => Ok(ScalarValue::null_for_type(*dt)),
        Expr::Literal(v) => match (dt, v) {
            (target, value) if *target == value.data_type() => Ok(value.clone()),
            // Bool-from-string/int and every integer/float conversion are
            // intentionally NOT re-implemented here: the fallback below
            // delegates to `coerce_scalar_for_dtype`, the single ladder both
            // evaluators share (`WHERE flag = 'yes'` and
            // `SET flag = 'yes'` must accept the same literals).
            (DataType::Decimal, ScalarValue::Float64(Some(v))) => {
                let multiplier = decimal_multiplier(scale)?;
                Ok(ScalarValue::Decimal(Some(
                    (*v * multiplier as f64).round() as i64
                )))
            }
            (DataType::Decimal, ScalarValue::Int64(Some(v))) => {
                let multiplier = decimal_multiplier(scale)?;
                Ok(ScalarValue::Decimal(Some(
                    v.checked_mul(multiplier).ok_or_else(|| {
                        TaibaiError::Sql(format!("decimal value '{}' overflows scale {}", v, scale))
                    })?,
                )))
            }
            (DataType::Timestamp, ScalarValue::VarChar(Some(s))) => {
                use crate::sql::execution::functions::date::parse_timestamp_smart;
                match parse_timestamp_smart(s, timezone_offset_hours) {
                    Some(ts) => Ok(ScalarValue::Timestamp(Some(ts))),
                    None => Ok(ScalarValue::Timestamp(None)),
                }
            }
            (DataType::Timestamp, ScalarValue::Int64(Some(n))) => {
                use crate::sql::execution::functions::date::parse_timestamp_from_int;
                Ok(ScalarValue::Timestamp(Some(parse_timestamp_from_int(*n))))
            }
            (DataType::Date, ScalarValue::VarChar(Some(s))) => {
                // Single string→DATE conversion point (micros → days).
                use crate::sql::execution::functions::date::datetime_string_to_days;
                Ok(ScalarValue::Date(datetime_string_to_days(
                    s,
                    timezone_offset_hours,
                )))
            }
            (DataType::Date, ScalarValue::Int64(Some(n))) => {
                // Interpret as days directly
                Ok(ScalarValue::Date(Some(*n as i32)))
            }
            (DataType::VarChar, value) => match value.to_text() {
                Some(text) => Ok(ScalarValue::VarChar(Some(text))),
                None => Ok(ScalarValue::VarChar(None)),
            },
            (DataType::Decimal, value) => {
                let multiplier = decimal_multiplier(scale)?;
                if let Some(i) = value.integer_i128() {
                    let scaled = i
                        .checked_mul(multiplier as i128)
                        .and_then(|v| i64::try_from(v).ok())
                        .ok_or_else(|| {
                            TaibaiError::Sql(format!(
                                "decimal value '{}' overflows scale {}",
                                i, scale
                            ))
                        })?;
                    Ok(ScalarValue::Decimal(Some(scaled)))
                } else if let Some(f) = value.numeric_f64() {
                    Ok(ScalarValue::Decimal(Some(
                        (f * multiplier as f64).round() as i64
                    )))
                } else {
                    coerce_scalar_for_dtype(v, *dt)
                }
            }
            _ => coerce_scalar_for_dtype(v, *dt),
        },
        // Column references and row-dependent expressions are not constant
        // per statement. Erroring here is fail-safe: the previous silent
        // NULL turned `UPDATE t SET a = b` into "set every a to NULL".
        Expr::Column(col) => Err(TaibaiError::Sql(format!(
            "column reference '{}' is not allowed here; only constant expressions are supported",
            col
        ))),
        _ => Err(TaibaiError::Sql(
            "unsupported expression: only constant expressions are allowed in this context"
                .to_string(),
        )),
    }
}

fn decimal_multiplier(scale: i32) -> Result<i64> {
    if !(0..=18).contains(&scale) {
        return Err(TaibaiError::Sql(format!("invalid decimal scale {}", scale)));
    }
    10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))
}

pub fn sort_batch_by_key(
    batch: &RecordBatch,
    inserter: &crate::storage::index::sort_key::SortKeyInserter,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return Ok(batch.clone());
    }

    // Sort lightweight row indices against the typed column buffers. The old
    // implementation materialized every key as ScalarValue and then rebuilt
    // all cells one by one, which dominated multi-value INSERT workloads.
    let mut rows = (0..num_rows as u32).collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        for (ki, &col_idx) in inserter.key_indices.iter().enumerate() {
            let cmp = batch.columns[col_idx].compare_at(*left as usize, *right as usize);
            let cmp = if inserter.desc.get(ki).copied().unwrap_or(false) {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&rows))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

pub fn build_row_group_predicate(
    where_clause: &Option<Expr>,
    schema: &Schema,
) -> Option<crate::storage::segment::RowGroupPredicate> {
    let expr = where_clause.as_ref()?;
    let mut clauses = Vec::new();
    extract_range_clauses(expr, schema, &mut clauses);
    if clauses.is_empty() {
        None
    } else {
        Some(crate::storage::segment::RowGroupPredicate::new(clauses))
    }
}

pub fn collect_expr_column_indices(expr: &Expr, schema: &Schema) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_expr_column_indices_inner(expr, schema, &mut indices);
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn collect_expr_column_indices_inner(expr: &Expr, schema: &Schema, indices: &mut Vec<usize>) {
    match expr {
        Expr::Column(name) => {
            if let Some(idx) = schema.index_of(name) {
                indices.push(idx);
            }
        }
        Expr::QualifiedColumn { column, .. } => {
            if let Some(idx) = schema.index_of(column) {
                indices.push(idx);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_column_indices_inner(left, schema, indices);
            collect_expr_column_indices_inner(right, schema, indices);
        }
        Expr::UnaryMinus(inner)
        | Expr::UnaryNot(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Exists(inner) => collect_expr_column_indices_inner(inner, schema, indices),
        Expr::Correlated(_) => {}
        Expr::In { expr, values, .. } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            for item in values {
                collect_expr_column_indices_inner(item, schema, indices);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            collect_expr_column_indices_inner(low, schema, indices);
            collect_expr_column_indices_inner(high, schema, indices);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expr_column_indices_inner(operand, schema, indices);
            }
            for (when_expr, then_expr) in when_clauses {
                collect_expr_column_indices_inner(when_expr, schema, indices);
                collect_expr_column_indices_inner(then_expr, schema, indices);
            }
            if let Some(else_clause) = else_clause {
                collect_expr_column_indices_inner(else_clause, schema, indices);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            collect_expr_column_indices_inner(pattern, schema, indices);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_column_indices_inner(arg, schema, indices);
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                collect_expr_column_indices_inner(expr, schema, indices);
            }
            for item in order_by {
                collect_expr_column_indices_inner(&item.expr, schema, indices);
            }
        }
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::Subquery(_) => {}
    }
}

fn extract_range_clauses(
    expr: &Expr,
    schema: &Schema,
    clauses: &mut Vec<crate::storage::segment::RangeClause>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                if let Expr::Column(col_name) = left.as_ref() {
                    if let Expr::Literal(ref val) = right.as_ref() {
                        if let Some(idx) = schema.index_of(col_name) {
                            let op = match op {
                                BinOp::Eq => crate::storage::segment::RangeOp::Eq,
                                BinOp::Lt => crate::storage::segment::RangeOp::Lt,
                                BinOp::Lte => crate::storage::segment::RangeOp::Le,
                                BinOp::Gt => crate::storage::segment::RangeOp::Gt,
                                BinOp::Gte => crate::storage::segment::RangeOp::Ge,
                                _ => unreachable!(),
                            };
                            clauses.push(crate::storage::segment::RangeClause {
                                column_index: idx,
                                op,
                                value: val.clone(),
                            });
                        }
                    }
                }
            }
            BinOp::And => {
                extract_range_clauses(left, schema, clauses);
                extract_range_clauses(right, schema, clauses);
            }
            _ => {}
        },
        Expr::UnaryNot(inner) => {
            if let Expr::IsNull(col_expr) = inner.as_ref() {
                if let Expr::Column(col_name) = col_expr.as_ref() {
                    if let Some(idx) = schema.index_of(col_name) {
                        clauses.push(crate::storage::segment::RangeClause {
                            column_index: idx,
                            op: crate::storage::segment::RangeOp::Eq,
                            value: ScalarValue::null_for_type(schema.columns[idx].data_type),
                        });
                    }
                }
            }
        }
        _ => {}
    }
}

/// Byte budget for buffered change-event row images (UPDATE post-images /
/// DELETE pre-images). These batches are held until every segment rewrite
/// has committed; without a cap, a no-WHERE UPDATE/DELETE on a big table
/// buffered the whole table inside the mutation-lock critical section.
/// Beyond the budget the remaining images are not published — the pubsub
/// channel already has drop-on-overflow semantics and subscribers resync
/// from the change log.
pub const CHANGE_EVENT_BUFFER_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Budget gate for one buffered change-event batch: returns true while
/// `buffered + incoming` fits, otherwise warns once per statement (via
/// `*warned`) and returns false so the caller skips buffering.
pub fn change_event_budget_allows(
    buffered_bytes: usize,
    incoming: usize,
    warned: &mut bool,
    what: &str,
    db: &str,
    table: &str,
) -> bool {
    if buffered_bytes.saturating_add(incoming) <= CHANGE_EVENT_BUFFER_BUDGET_BYTES {
        return true;
    }
    if !*warned {
        *warned = true;
        tracing::warn!(
            db,
            table,
            what,
            budget = CHANGE_EVENT_BUFFER_BUDGET_BYTES,
            "change-event buffer budget exceeded; remaining row images are not published (subscribers re-sync from the change log)"
        );
    }
    false
}

/// Buffered change-event row images for one auto-commit DML statement:
/// materializes selected rows out of source batches via
/// [`RecordBatch::take_rows`], enforces the pubsub buffer budget, and
/// accumulates the batches the statement publishes once its durable work
/// has committed. This is the single collection path for DELETE/UPDATE
/// events; the transactional paths defer instead (charging the transaction
/// budget) and publish through the same pubsub chokepoint at COMMIT.
pub(crate) struct ChangeEventBuffer {
    what: &'static str,
    db: String,
    table: String,
    buffered_bytes: usize,
    warned: bool,
    pub batches: Vec<RecordBatch>,
}

impl ChangeEventBuffer {
    pub(crate) fn new(what: &'static str, db: &str, table: &str) -> Self {
        Self {
            what,
            db: db.to_string(),
            table: table.to_string(),
            buffered_bytes: 0,
            warned: false,
            batches: Vec::new(),
        }
    }

    /// Materialize `positions` out of `source` into the buffer (no-op past
    /// the budget; build failures are logged and skipped — a failed event
    /// batch must not fail the statement that already made the change).
    pub(crate) fn push(&mut self, source: &RecordBatch, positions: &[u32]) {
        if positions.is_empty() {
            return;
        }
        match source.take_rows(positions) {
            Ok(batch) => {
                let bytes = batch.estimated_size_bytes();
                if change_event_budget_allows(
                    self.buffered_bytes,
                    bytes,
                    &mut self.warned,
                    self.what,
                    &self.db,
                    &self.table,
                ) {
                    self.buffered_bytes += bytes;
                    self.batches.push(batch);
                }
            }
            Err(error) => tracing::warn!(
                db = %self.db,
                table = %self.table,
                what = self.what,
                error = %error,
                "failed to build change-event batch"
            ),
        }
    }
}

/// Segment-level zone-map probe: does any row group of this segment possibly
/// match the predicate? Uses the segment-level zone maps when present, else
/// scans per-row-group maps. `None` means no predicate — everything matches.
/// The single copy shared by the DELETE, UPDATE, and transactional DML
/// planners (they used to carry drifting per-module duplicates).
pub(crate) fn segment_may_match(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    predicate: Option<&crate::storage::segment::RowGroupPredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    // The predicate's column indices are catalog-space; zone maps live in the
    // segment's footer order. After a DROP/RENAME COLUMN the two drift, and
    // testing catalog indices against footer zone maps prunes (or keeps) the
    // WRONG segments — re-anchor the predicate onto this segment first.
    //
    // Clauses on columns the footer predates cannot consult a zone map, but
    // under the default-fill semantics every row carries the column's
    // DEFAULT: the clause reduces to one constant comparison, which prunes
    // precisely. The old blanket "absent column cannot match" silently
    // skipped rows a DELETE was required to remove while UPDATE already
    // matched them. Undecidable constants keep the segment; the row-wise
    // evaluation on default-filled batches decides.
    let mut present_clauses = Vec::new();
    for clause in &predicate.clauses {
        if crate::storage::segment::reader::any_column_absent_in_schema(
            table_schema,
            reader.schema(),
            &[clause.column_index],
        ) {
            let Some(column) = table_schema.columns.get(clause.column_index) else {
                return true;
            };
            match default_clause_decision(column, &clause.op, &clause.value) {
                Some(true) => {}
                Some(false) => return false,
                None => {}
            }
        } else {
            present_clauses.push(clause.clone());
        }
    }
    if present_clauses.is_empty() {
        return true;
    }
    let reduced = crate::storage::partition::pruning::RowGroupPredicate::new(present_clauses);
    // Every remaining clause's column exists in the footer, so alignment can
    // only fail on an out-of-range catalog index: keep the segment and let
    // the row-wise evaluation surface the error.
    let Ok(aligned) = reader.align_row_group_predicate(table_schema, &reduced) else {
        return true;
    };
    let segment_zone_maps = reader.segment_zone_maps();
    if !segment_zone_maps.is_empty() {
        return aligned.matches_row_group(segment_zone_maps);
    }
    (0..reader.num_row_groups()).any(|rg_idx| aligned.matches_row_group(reader.zone_maps(rg_idx)))
}

/// Decide one range clause against the constant value a pre-ALTER segment's
/// rows carry for `column`. `None` keeps the segment (undecidable).
fn default_clause_decision(
    column: &crate::types::batch::ColumnDef,
    op: &crate::storage::partition::pruning::RangeOp,
    value: &crate::types::scalar::ScalarValue,
) -> Option<bool> {
    use crate::storage::index::sort_key::compare_scalars_for_sort;
    use crate::storage::partition::pruning::RangeOp;
    let default = crate::storage::segment::reader::column_default_value(column);
    if default.is_null() {
        // NULL compares UNKNOWN against everything: no row matches.
        return Some(false);
    }
    let ordering = compare_scalars_for_sort(&default, value);
    Some(match op {
        RangeOp::Eq => ordering == std::cmp::Ordering::Equal,
        RangeOp::Lt => ordering == std::cmp::Ordering::Less,
        RangeOp::Le => ordering != std::cmp::Ordering::Greater,
        RangeOp::Gt => ordering == std::cmp::Ordering::Greater,
        RangeOp::Ge => ordering != std::cmp::Ordering::Less,
    })
}

#[cfg(test)]
#[path = "helpers_test.rs"]
mod tests;
