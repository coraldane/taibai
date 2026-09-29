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
fn negated_kernel(op: BinOp) -> Option<fn(&crate::types::array::ColumnArray, &ScalarValue) -> Vec<bool>> {
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
                            return Ok(ge
                                .iter()
                                .zip(le.iter())
                                .map(|(a, b)| *a && *b)
                                .collect());
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
                Ok(if l.is_none() || r.is_none() { None } else { Some(true) })
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
                Ok(if l.is_none() || r.is_none() { None } else { Some(false) })
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
                    BinOp::Lt => {
                        Ok(Some(compare_scalars(&left_val, &right_val) == Some(Ordering::Less)))
                    }
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
        Expr::UnaryNot(inner) => {
            Ok(eval_where_clause3(inner, batch, row_idx, schema)?.map(|b| !b))
        }
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
        Expr::UnaryMinus(_) | Expr::WindowFunction { .. } | Expr::Placeholder(_) => {
            Err(TaibaiError::Sql(
                "unsupported predicate expression in DML statement".to_string(),
            ))
        }
        Expr::Exists(_) | Expr::Subquery(_) => Err(TaibaiError::Sql(
            "subqueries are not supported in this DML statement's WHERE clause"
                .to_string(),
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
        value if value.integer_i128().is_some() => {
            Ok(Some(value.integer_i128() != Some(0)))
        }
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
            match op {
                BinOp::Add => match (&left_val, &right_val) {
                    (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => {
                        Ok(ScalarValue::Int64(Some(a + b)))
                    }
                    _ => Ok(left_val),
                },
                BinOp::Sub => match (&left_val, &right_val) {
                    (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => {
                        Ok(ScalarValue::Int64(Some(a - b)))
                    }
                    _ => Ok(left_val),
                },
                _ => Ok(left_val),
            }
        }
        Expr::IsNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::IsNotNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::UnaryNot(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
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
            Ok(ScalarValue::Bool(Some(if *negated { !holds } else { holds })))
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
        (DataType::Int64, value) => value
            .integer_i128()
            .and_then(|v| i64::try_from(v).ok())
            .map(|v| ScalarValue::Int64(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to INT64", value))),
        (DataType::Int32, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int32(Some(
            i32::try_from(*v)
                .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT32", v)))?,
        ))),
        (DataType::Int32, value) => value
            .integer_i128()
            .and_then(|v| i32::try_from(v).ok())
            .map(|v| ScalarValue::Int32(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to INT32", value))),
        (DataType::Int16, value) => value
            .integer_i128()
            .and_then(|v| i16::try_from(v).ok())
            .map(|v| ScalarValue::Int16(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to INT16", value))),
        (DataType::Int8, value) => value
            .integer_i128()
            .and_then(|v| i8::try_from(v).ok())
            .map(|v| ScalarValue::Int8(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to INT8", value))),
        (DataType::UInt64, value) => value
            .integer_i128()
            .and_then(|v| u64::try_from(v).ok())
            .map(|v| ScalarValue::UInt64(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to UINT64", value))),
        (DataType::UInt32, value) => value
            .integer_i128()
            .and_then(|v| u32::try_from(v).ok())
            .map(|v| ScalarValue::UInt32(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to UINT32", value))),
        (DataType::UInt16, value) => value
            .integer_i128()
            .and_then(|v| u16::try_from(v).ok())
            .map(|v| ScalarValue::UInt16(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to UINT16", value))),
        (DataType::UInt8, value) => value
            .integer_i128()
            .and_then(|v| u8::try_from(v).ok())
            .map(|v| ScalarValue::UInt8(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to UINT8", value))),
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
            (DataType::Bool, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
            (DataType::Bool, ScalarValue::VarChar(Some(v))) => {
                match v.to_ascii_lowercase().as_str() {
                    "true" | "1" => Ok(ScalarValue::Bool(Some(true))),
                    "false" | "0" => Ok(ScalarValue::Bool(Some(false))),
                    _ => Err(TaibaiError::Sql(format!("invalid bool '{}'", v))),
                }
            }
            (DataType::Int64, ScalarValue::Int32(Some(v))) => {
                Ok(ScalarValue::Int64(Some(*v as i64)))
            }
            (DataType::Int32, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int32(Some(
                i32::try_from(*v)
                    .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT32", v)))?,
            ))),
            (DataType::Int16, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int16(Some(
                i16::try_from(*v)
                    .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT16", v)))?,
            ))),
            (DataType::Int8, ScalarValue::Int64(Some(v))) => {
                Ok(ScalarValue::Int8(Some(i8::try_from(*v).map_err(|_| {
                    TaibaiError::Sql(format!("value '{}' overflows INT8", v))
                })?)))
            }
            (DataType::UInt64, ScalarValue::Int64(Some(v))) => {
                if *v < 0 {
                    return Err(TaibaiError::Sql(format!(
                        "negative value '{}' for UINT64",
                        v
                    )));
                }
                Ok(ScalarValue::UInt64(Some(*v as u64)))
            }
            (DataType::UInt32, ScalarValue::Int64(Some(v))) => {
                if *v < 0 {
                    return Err(TaibaiError::Sql(format!(
                        "negative value '{}' for UINT32",
                        v
                    )));
                }
                Ok(ScalarValue::UInt32(Some(u32::try_from(*v).map_err(
                    |_| TaibaiError::Sql(format!("value '{}' overflows UINT32", v)),
                )?)))
            }
            (DataType::UInt16, ScalarValue::Int64(Some(v))) => {
                if *v < 0 {
                    return Err(TaibaiError::Sql(format!(
                        "negative value '{}' for UINT16",
                        v
                    )));
                }
                Ok(ScalarValue::UInt16(Some(u16::try_from(*v).map_err(
                    |_| TaibaiError::Sql(format!("value '{}' overflows UINT16", v)),
                )?)))
            }
            (DataType::UInt8, ScalarValue::Int64(Some(v))) => {
                if *v < 0 {
                    return Err(TaibaiError::Sql(format!(
                        "negative value '{}' for UINT8",
                        v
                    )));
                }
                Ok(ScalarValue::UInt8(Some(u8::try_from(*v).map_err(
                    |_| TaibaiError::Sql(format!("value '{}' overflows UINT8", v)),
                )?)))
            }
            (DataType::Float64, ScalarValue::Int64(Some(v))) => {
                Ok(ScalarValue::Float64(Some(*v as f64)))
            }
            (DataType::Float32, ScalarValue::Float64(Some(v))) => {
                Ok(ScalarValue::Float32(Some(*v as f32)))
            }
            (DataType::Float32, ScalarValue::Int64(Some(v))) => {
                Ok(ScalarValue::Float32(Some(*v as f32)))
            }
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
                // Parse date string to days since epoch
                // Format: YYYY-MM-DD
                use crate::sql::execution::functions::date::parse_datetime_string;
                match parse_datetime_string(s, timezone_offset_hours) {
                    Some(ts) => {
                        // Convert timestamp (seconds) to days
                        let days = (ts / 86400) as i32;
                        Ok(ScalarValue::Date(Some(days)))
                    }
                    None => Ok(ScalarValue::Date(None)),
                }
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

#[cfg(test)]
#[path = "helpers_test.rs"]
mod tests;
