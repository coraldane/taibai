use crate::common::error::Result;
use crate::sql::execution::functions::registry::FunctionRegistry;
use crate::sql::execution::helpers::scalar_like_matches;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::{AggregateExpr, LogicalExpr};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::{ExecutionOp, RuntimeInFilter};
use crate::sql::execution::subquery_runtime::SubqueryRegistry;

thread_local! {
    /// Shared per-thread fallback registry. Scalar function execution is only
    /// reached for shapes without typed kernels, but rebuilding the registry
    /// for every batch showed up in profiles.
    static FALLBACK_FUNCTION_REGISTRY: FunctionRegistry = FunctionRegistry::default();
}

pub struct FilterExec {
    pub predicate: LogicalExpr,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    /// Snapshot registry for correlated subqueries, when the query contains
    /// any. `None` makes evaluation of such nodes fail loudly.
    pub subqueries: Option<SubqueryRegistry>,
    /// Remapped predicate cache: `remap_expr_for_batch` is only needed when
    /// the physical batch layout differs from the logical schema, and is
    /// deterministic per batch schema, so cache it instead of re-cloning the
    /// expression tree for every batch.
    remapped: Option<(Schema, LogicalExpr)>,
}

impl FilterExec {
    pub fn new(
        predicate: LogicalExpr,
        input: Box<dyn ExecutionOp>,
        schema: Schema,
        subqueries: Option<SubqueryRegistry>,
    ) -> Self {
        Self {
            predicate,
            input,
            schema,
            subqueries,
            remapped: None,
        }
    }
}

impl ExecutionOp for FilterExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.input.next_batch()? {
            let predicate = if batch.schema == self.schema {
                &self.predicate
            } else {
                if !self
                    .remapped
                    .as_ref()
                    .is_some_and(|(schema, _)| *schema == batch.schema)
                {
                    let expr = remap_expr_for_batch(&self.predicate, &self.schema, &batch.schema);
                    self.remapped = Some((batch.schema.clone(), expr));
                }
                self.remapped
                    .as_ref()
                    .map(|(_, expr)| expr)
                    .unwrap_or(&self.predicate)
            };
            let mask = eval_filter_with(predicate, &batch, self.subqueries.as_ref())?;
            let filtered = super::apply_filter_mask(batch, &mask);
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
        }
        Ok(None)
    }

    fn add_runtime_in_filter(&mut self, filter: RuntimeInFilter) -> bool {
        self.input.add_runtime_in_filter(filter)
    }
}

pub fn remap_expr_for_batch(
    expr: &LogicalExpr,
    logical_schema: &Schema,
    batch_schema: &Schema,
) -> LogicalExpr {
    match expr {
        LogicalExpr::Column(idx) => logical_schema
            .columns
            .get(*idx)
            .and_then(|col| {
                if batch_schema.columns.get(*idx) == Some(col) {
                    Some(*idx)
                } else {
                    batch_schema.index_of(&col.name)
                }
            })
            .map(LogicalExpr::Column)
            .unwrap_or_else(|| expr.clone()),
        LogicalExpr::BinaryOp { left, op, right } => LogicalExpr::BinaryOp {
            left: Box::new(remap_expr_for_batch(left, logical_schema, batch_schema)),
            op: *op,
            right: Box::new(remap_expr_for_batch(right, logical_schema, batch_schema)),
        },
        LogicalExpr::Not(inner) => LogicalExpr::Not(Box::new(remap_expr_for_batch(
            inner,
            logical_schema,
            batch_schema,
        ))),
        LogicalExpr::IsNull(inner) => LogicalExpr::IsNull(Box::new(remap_expr_for_batch(
            inner,
            logical_schema,
            batch_schema,
        ))),
        LogicalExpr::IsNotNull(inner) => LogicalExpr::IsNotNull(Box::new(remap_expr_for_batch(
            inner,
            logical_schema,
            batch_schema,
        ))),
        LogicalExpr::Aggregate(agg) => aggregate_output_name(agg, logical_schema)
            .and_then(|expected| {
                batch_schema
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&expected))
            })
            .map(LogicalExpr::Column)
            .unwrap_or_else(|| {
                LogicalExpr::Aggregate(remap_aggregate_for_batch(agg, logical_schema, batch_schema))
            }),
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => LogicalExpr::In {
            expr: Box::new(remap_expr_for_batch(expr, logical_schema, batch_schema)),
            values: values.clone(),
            negated: *negated,
        },
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => LogicalExpr::Between {
            expr: Box::new(remap_expr_for_batch(expr, logical_schema, batch_schema)),
            low: Box::new(remap_expr_for_batch(low, logical_schema, batch_schema)),
            high: Box::new(remap_expr_for_batch(high, logical_schema, batch_schema)),
            negated: *negated,
        },
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => LogicalExpr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(remap_expr_for_batch(e, logical_schema, batch_schema))),
            when_clauses: when_clauses
                .iter()
                .map(|(when, then)| {
                    (
                        remap_expr_for_batch(when, logical_schema, batch_schema),
                        remap_expr_for_batch(then, logical_schema, batch_schema),
                    )
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(remap_expr_for_batch(e, logical_schema, batch_schema))),
        },
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => LogicalExpr::Like {
            expr: Box::new(remap_expr_for_batch(expr, logical_schema, batch_schema)),
            pattern: pattern.clone(),
            negated: *negated,
        },
        LogicalExpr::Function { name, args } => LogicalExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_expr_for_batch(arg, logical_schema, batch_schema))
                .collect(),
        },
        LogicalExpr::CorrelatedSubquery(corr_box) => {
            let mut corr = (*corr_box).clone();
            // Params point at the enclosing block's logical columns; remap
            // them to the physical batch layout the same way plain columns
            // are remapped.
            corr.params = corr
                .params
                .iter()
                .map(|(marker, logical_idx)| {
                    let batch_idx = logical_schema
                        .columns
                        .get(*logical_idx)
                        .and_then(|col| {
                            if batch_schema.columns.get(*logical_idx) == Some(col) {
                                Some(*logical_idx)
                            } else {
                                batch_schema.index_of(&col.name)
                            }
                        })
                        .unwrap_or(*logical_idx);
                    (*marker, batch_idx)
                })
                .collect();
            if let Some(lhs) = &corr.lhs {
                corr.lhs = Some(Box::new(remap_expr_for_batch(
                    lhs,
                    logical_schema,
                    batch_schema,
                )));
            }
            LogicalExpr::CorrelatedSubquery(corr)
        }
        _ => expr.clone(),
    }
}

/// Equality with the same semantics as `eval_comparison(.., Eq, ..)`: NULL
/// never equals anything.
pub(crate) fn values_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    eval_comparison(left, &BinOp::Eq, right)
}

fn aggregate_output_name(agg: &AggregateExpr, logical_schema: &Schema) -> Option<String> {
    let func = match agg.func {
        crate::sql::planner::AggFunc::Count => "count",
        crate::sql::planner::AggFunc::Sum => "sum",
        crate::sql::planner::AggFunc::Avg => "avg",
        crate::sql::planner::AggFunc::Min => "min",
        crate::sql::planner::AggFunc::Max => "max",
    };
    let arg = match &*agg.arg {
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::Column(idx) => logical_schema.columns.get(*idx)?.name.clone(),
        _ => "expr".to_string(),
    };
    let distinct = if agg.distinct { "DISTINCT " } else { "" };
    Some(format!("{}({}{})", func, distinct, arg))
}

pub fn remap_aggregate_for_batch(
    agg: &AggregateExpr,
    logical_schema: &Schema,
    batch_schema: &Schema,
) -> AggregateExpr {
    AggregateExpr {
        func: agg.func,
        arg: Box::new(remap_expr_for_batch(&agg.arg, logical_schema, batch_schema)),
        distinct: agg.distinct,
    }
}

pub fn eval_filter(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<bool>> {
    eval_filter_with(expr, batch, None)
}

pub fn eval_filter_with(
    expr: &LogicalExpr,
    batch: &RecordBatch,
    subqueries: Option<&SubqueryRegistry>,
) -> Result<Vec<bool>> {
    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let left_mask = eval_filter_with(left, batch, subqueries)?;
                // Short-circuit: when the left arm already excludes every
                // row, the right arm cannot change the result — skipping it
                // avoids paying for an expensive arm (subqueries, functions)
                // behind a provably false conjunct.
                if left_mask.iter().any(|m| *m) {
                    let right_mask = eval_filter_with(right, batch, subqueries)?;
                    for i in 0..num_rows {
                        mask[i] = left_mask[i] && right_mask[i];
                    }
                }
                // else: mask stays all-false.
            }
            BinOp::Or => {
                let left_mask = eval_filter_with(left, batch, subqueries)?;
                if left_mask.iter().any(|m| !m) {
                    let right_mask = eval_filter_with(right, batch, subqueries)?;
                    for i in 0..num_rows {
                        mask[i] = left_mask[i] || right_mask[i];
                    }
                } else {
                    mask.copy_from_slice(&left_mask);
                }
            }
            _ => {
                if let Some(vector_mask) = eval_column_literal_comparison(left, op, right, batch) {
                    return Ok(vector_mask);
                }
                if let Some(typed_mask) =
                    crate::sql::execution::expr_vec::eval_comparison_mask(left, *op, right, batch)?
                {
                    return Ok(typed_mask);
                }
                let left_vals = eval_expr_column_with(left, batch, subqueries)?;
                let right_vals = eval_expr_column_with(right, batch, subqueries)?;
                for i in 0..num_rows {
                    mask[i] = eval_comparison(&left_vals[i], op, &right_vals[i]);
                }
            }
        },
        LogicalExpr::Not(inner) => {
            // Three-valued NOT: a row survives only when the inner predicate
            // is definitely FALSE. The old `!mask[i]` bit-flip turned UNKNOWN
            // rows (inner mask false) into survivors, so `WHERE NOT (a = 5)`
            // wrongly included rows with a NULL.
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            for i in 0..num_rows {
                mask[i] = matches!(vals[i], ScalarValue::Bool(Some(false)));
            }
        }
        LogicalExpr::IsNull(inner) => {
            if let LogicalExpr::Column(idx) = &**inner {
                if let Some(column) = batch.column(*idx) {
                    return Ok(column.is_null_vec());
                }
            }
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            for i in 0..num_rows {
                mask[i] = vals[i].is_null();
            }
        }
        LogicalExpr::IsNotNull(inner) => {
            if let LogicalExpr::Column(idx) = &**inner {
                if let Some(column) = batch.column(*idx) {
                    return Ok(column.is_not_null_vec());
                }
            }
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                *m = !vals[i].is_null();
            }
        }
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            // Three-valued IN: a NULL operand makes the predicate UNKNOWN
            // (row excluded for both IN and NOT IN), and NOT IN over a list
            // containing NULL matches nothing.
            let has_null_candidate = values.iter().any(|candidate| candidate.is_null());
            // Hash-set prefilter for wide lists; the linear scan stays as
            // the fallback for cross-type equality the exact-match set
            // cannot see. The set borrows the (per-statement constant)
            // candidates instead of cloning them on every batch.
            let fast_set = (values.len() >= 8).then(|| {
                values
                    .iter()
                    .collect::<std::collections::HashSet<&ScalarValue>>()
            });
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                if vals[i].is_null() {
                    *m = false;
                    continue;
                }
                let found = if fast_set.as_ref().is_some_and(|set| set.contains(&vals[i])) {
                    true
                } else {
                    values
                        .iter()
                        .any(|candidate| eval_comparison(&vals[i], &BinOp::Eq, candidate))
                };
                *m = if *negated {
                    !found && !has_null_candidate
                } else {
                    found
                };
            }
        }
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                let matched =
                    crate::sql::execution::helpers::scalar_like_matches(&vals[i], pattern);
                *m = if *negated && !vals[i].is_null() {
                    !matched
                } else {
                    matched
                };
            }
        }
        LogicalExpr::Literal(v) => {
            // MySQL truthiness, matching the DML evaluator: booleans pass
            // through, nonzero numerics are TRUE, zero/NULL are FALSE.
            // (`WHERE 0` used to return every row here.)
            let keep = match v {
                ScalarValue::Bool(Some(value)) => *value,
                other if other.is_null() => false,
                other => {
                    other.integer_i128().is_some_and(|i| i != 0)
                        || other.numeric_f64().is_some_and(|f| f != 0.0)
                }
            };
            for m in mask.iter_mut().take(num_rows) {
                *m = keep;
            }
        }
        _ => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                *m = matches!(vals[i], ScalarValue::Bool(Some(true)));
            }
        }
    }

    Ok(mask)
}

fn eval_column_literal_comparison(
    left: &LogicalExpr,
    op: &BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Option<Vec<bool>> {
    let (column_idx, literal, op) = match (left, right) {
        (LogicalExpr::Column(idx), LogicalExpr::Literal(value)) => (*idx, value, *op),
        (LogicalExpr::Literal(value), LogicalExpr::Column(idx)) => {
            let reversed = match op {
                BinOp::Lt => BinOp::Gt,
                BinOp::Lte => BinOp::Gte,
                BinOp::Gt => BinOp::Lt,
                BinOp::Gte => BinOp::Lte,
                other => *other,
            };
            (*idx, value, reversed)
        }
        _ => return None,
    };
    let column = batch.column(column_idx)?;
    crate::sql::execution::expr_vec::column_literal_mask(column, op, literal)
}

fn eval_comparison(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    if let Some(ordering) = left.numeric_cmp(right) {
        return match op {
            BinOp::Eq => ordering == std::cmp::Ordering::Equal,
            BinOp::Neq => ordering != std::cmp::Ordering::Equal,
            BinOp::Lt => ordering == std::cmp::Ordering::Less,
            BinOp::Gt => ordering == std::cmp::Ordering::Greater,
            BinOp::Lte => ordering != std::cmp::Ordering::Greater,
            BinOp::Gte => ordering != std::cmp::Ordering::Less,
            _ => false,
        };
    }
    match (left, right) {
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => match op {
            BinOp::Eq => l == r,
            BinOp::Neq => l != r,
            _ => false,
        },
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l, op, *r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => cmp_u64(*l, op, *r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => {
            cmp_u64(*l as u64, op, *r as u64)
        }
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => {
            cmp_u64(*l as u64, op, *r as u64)
        }
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => {
            cmp_u64(*l as u64, op, *r as u64)
        }
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => cmp_f64(*l, op, *r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            cmp_f64(*l as f64, op, *r as f64)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => match op {
            BinOp::Eq => l == r,
            BinOp::Neq => l != r,
            BinOp::Lt => l < r,
            BinOp::Gt => l > r,
            BinOp::Lte => l <= r,
            BinOp::Gte => l >= r,
            _ => false,
        },
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => cmp_i64(*l, op, *r as i64),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l as i64, op, *r),
        _ => false,
    }
}

fn cmp_i64(l: i64, op: &BinOp, r: i64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}

fn cmp_u64(l: u64, op: &BinOp, r: u64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}

fn cmp_f64(l: f64, op: &BinOp, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}

pub fn eval_expr_column(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<ScalarValue>> {
    eval_expr_column_with(expr, batch, None)
}

pub fn eval_expr_column_with(
    expr: &LogicalExpr,
    batch: &RecordBatch,
    subqueries: Option<&SubqueryRegistry>,
) -> Result<Vec<ScalarValue>> {
    let num_rows = batch.num_rows();
    match expr {
        LogicalExpr::Column(idx) => Ok((0..num_rows)
            .map(|i| batch.scalar(i, *idx).unwrap_or(ScalarValue::Int64(None)))
            .collect()),
        LogicalExpr::Literal(v) => Ok(vec![v.clone(); num_rows]),
        LogicalExpr::BinaryOp { left, op, right } => {
            let lv = eval_expr_column_with(left, batch, subqueries)?;
            let rv = eval_expr_column_with(right, batch, subqueries)?;
            let mut result = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                result.push(eval_binary_scalar(&lv[i], op, &rv[i]));
            }
            Ok(result)
        }
        LogicalExpr::Aggregate(agg) => {
            let agg_type_prefix = match agg.func {
                crate::sql::planner::AggFunc::Count => "COUNT",
                crate::sql::planner::AggFunc::Sum => "SUM",
                crate::sql::planner::AggFunc::Avg => "AVG",
                crate::sql::planner::AggFunc::Min => "MIN",
                crate::sql::planner::AggFunc::Max => "MAX",
            };

            let matches = batch
                .schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, c)| {
                    let name = c.name.to_ascii_uppercase();
                    (name.starts_with(&format!("{}(", agg_type_prefix))
                        || name == format!("{}(*)", agg_type_prefix))
                    .then_some(idx)
                })
                .collect::<Vec<_>>();

            if let [idx] = matches.as_slice() {
                Ok((0..num_rows)
                    .map(|i| batch.scalar(i, *idx).unwrap_or(ScalarValue::Int64(None)))
                    .collect())
            } else if matches.len() > 1 {
                Err(crate::common::error::TaibaiError::Internal(format!(
                    "ambiguous {} aggregate output; projection was not resolved to an exact column",
                    agg_type_prefix
                )))
            } else {
                Ok(vec![ScalarValue::Int64(None); num_rows])
            }
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(v.is_null())))
                .collect())
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(!v.is_null())))
                .collect())
        }
        LogicalExpr::Not(inner) => {
            let vals = eval_expr_column_with(inner, batch, subqueries)?;
            Ok(vals
                .iter()
                .map(|v| match v {
                    ScalarValue::Bool(Some(b)) => ScalarValue::Bool(Some(!b)),
                    _ => ScalarValue::Bool(None),
                })
                .collect())
        }
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            Ok(vals
                .iter()
                .map(|val| {
                    let found = values
                        .iter()
                        .any(|candidate| eval_comparison(val, &BinOp::Eq, candidate));
                    ScalarValue::Bool(Some(if *negated { !found } else { found }))
                })
                .collect())
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand_values = operand
                .as_deref()
                .map(|expr| eval_expr_column_with(expr, batch, subqueries))
                .transpose()?;
            let clause_values = when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        eval_expr_column_with(when_expr, batch, subqueries)?,
                        eval_expr_column_with(then_expr, batch, subqueries)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let else_values = else_clause
                .as_deref()
                .map(|expr| eval_expr_column_with(expr, batch, subqueries))
                .transpose()?;
            let mut result = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let mut value = None;
                for (when_values, then_values) in &clause_values {
                    let matched = match &operand_values {
                        Some(values) => {
                            eval_comparison(&values[row_idx], &BinOp::Eq, &when_values[row_idx])
                        }
                        None => matches!(when_values[row_idx], ScalarValue::Bool(Some(true))),
                    };
                    if matched {
                        value = Some(then_values[row_idx].clone());
                        break;
                    }
                }
                result.push(value.unwrap_or_else(|| {
                    else_values
                        .as_ref()
                        .map(|values| values[row_idx].clone())
                        .unwrap_or(ScalarValue::Int64(None))
                }));
            }
            Ok(result)
        }
        LogicalExpr::Function { name, args } => {
            let arg_columns = args
                .iter()
                .map(|arg| eval_expr_column_with(arg, batch, subqueries))
                .collect::<Result<Vec<_>>>()?;
            let mut result = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let row_args = arg_columns
                    .iter()
                    .map(|values| values[row_idx].clone())
                    .collect::<Vec<_>>();
                result.push(
                    FALLBACK_FUNCTION_REGISTRY
                        .with(|registry| registry.execute(name, &row_args))?,
                );
            }
            Ok(result)
        }
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            let lows = eval_expr_column_with(low, batch, subqueries)?;
            let highs = eval_expr_column_with(high, batch, subqueries)?;
            Ok((0..num_rows)
                .map(|row_idx| {
                    let inside = eval_comparison(&vals[row_idx], &BinOp::Gte, &lows[row_idx])
                        && eval_comparison(&vals[row_idx], &BinOp::Lte, &highs[row_idx]);
                    ScalarValue::Bool(Some(if *negated { !inside } else { inside }))
                })
                .collect())
        }
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let vals = eval_expr_column_with(expr, batch, subqueries)?;
            Ok(vals
                .iter()
                .map(|v| {
                    if v.is_null() {
                        return ScalarValue::Bool(None);
                    }
                    let matched = scalar_like_matches(v, pattern);
                    ScalarValue::Bool(Some(matched != *negated))
                })
                .collect())
        }
        LogicalExpr::CorrelatedSubquery(node) => {
            crate::sql::execution::subquery_runtime::eval_correlated_column(node, batch, subqueries)
        }
        // Unresolved subquery shapes and Wildcard must never reach an
        // evaluator: the previous fallback returned a column of NULLs, so any
        // upstream resolution gap silently turned the missing expression into
        // "all NULL" instead of an error.
        other => Err(crate::common::error::TaibaiError::Internal(format!(
            "unsupported expression shape in filter evaluation: {other:?}"
        ))),
    }
}

/// Single scalar binary-operator implementation shared by the vectorized
/// filter path and the DML row-wise evaluator (helpers::eval_expr_for_row) —
/// a previous private copy in helpers silently returned the left operand for
/// every uncovered arithmetic shape, mis-filtering UPDATE/DELETE rows.
pub(crate) fn eval_binary_scalar(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> ScalarValue {
    use BinOp::*;
    if left.is_null() || right.is_null() {
        return match op {
            Eq | Neq | Lt | Gt | Lte | Gte | And | Or => ScalarValue::Bool(None),
            _ => ScalarValue::Int64(None),
        };
    }
    match op {
        And => {
            let (l, r) = (as_bool(left), as_bool(right));
            if l == Some(false) || r == Some(false) {
                ScalarValue::Bool(Some(false))
            } else if let (Some(l), Some(r)) = (l, r) {
                ScalarValue::Bool(Some(l && r))
            } else {
                ScalarValue::Bool(None)
            }
        }
        Or => {
            let (l, r) = (as_bool(left), as_bool(right));
            if l == Some(true) || r == Some(true) {
                ScalarValue::Bool(Some(true))
            } else if let (Some(l), Some(r)) = (l, r) {
                ScalarValue::Bool(Some(l || r))
            } else {
                ScalarValue::Bool(None)
            }
        }
        Eq | Neq | Lt | Gt | Lte | Gte => {
            let matched = match (left, right) {
                (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => Some(if l == r {
                    std::cmp::Ordering::Equal
                } else if *l {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }),
                _ => left.numeric_cmp(right).or_else(|| match (left, right) {
                    (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => {
                        Some(l.cmp(r))
                    }
                    (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => Some(l.cmp(r)),
                    (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => {
                        Some(l.cmp(r))
                    }
                    _ => None,
                }),
            };
            match matched.map(|ord| match op {
                Eq => ord == std::cmp::Ordering::Equal,
                Neq => ord != std::cmp::Ordering::Equal,
                Lt => ord == std::cmp::Ordering::Less,
                Gt => ord == std::cmp::Ordering::Greater,
                Lte => ord != std::cmp::Ordering::Greater,
                Gte => ord != std::cmp::Ordering::Less,
                _ => unreachable!(),
            }) {
                Some(v) => ScalarValue::Bool(Some(v)),
                None => ScalarValue::Bool(None),
            }
        }
        Add | Sub | Mul | Mod | Div => {
            if matches!(op, Div) {
                let (Some(l), Some(r)) = (left.numeric_f64(), right.numeric_f64()) else {
                    return ScalarValue::Float64(None);
                };
                if r == 0.0 {
                    return ScalarValue::Float64(None);
                }
                return ScalarValue::Float64(Some(l / r));
            }
            if let (Some(l), Some(r)) = (left.integer_i128(), right.integer_i128()) {
                return match op {
                    Div | Mod if r == 0 => ScalarValue::Int64(None),
                    Mod => i64::try_from(l % r)
                        .ok()
                        .map(|v| ScalarValue::Int64(Some(v)))
                        .unwrap_or(ScalarValue::Int64(None)),
                    Add => i64::try_from(l + r)
                        .ok()
                        .map(|v| ScalarValue::Int64(Some(v)))
                        .unwrap_or(ScalarValue::Int64(None)),
                    Sub => i64::try_from(l - r)
                        .ok()
                        .map(|v| ScalarValue::Int64(Some(v)))
                        .unwrap_or(ScalarValue::Int64(None)),
                    Mul => l
                        .checked_mul(r)
                        .and_then(|v| i64::try_from(v).ok())
                        .map(|v| ScalarValue::Int64(Some(v)))
                        .unwrap_or(ScalarValue::Int64(None)),
                    Div => unreachable!("integer division falls through to floating point"),
                    _ => unreachable!("non-arithmetic operator in arithmetic evaluation"),
                };
            }
            if matches!(
                (left, right),
                (ScalarValue::VarChar(Some(_)), ScalarValue::VarChar(Some(_)))
            ) {
                if let (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) =
                    (left, right)
                {
                    return match op {
                        Add => ScalarValue::VarChar(Some(format!("{l}{r}"))),
                        _ => ScalarValue::Bool(None),
                    };
                }
            }
            let (Some(l), Some(r)) = (left.numeric_f64(), right.numeric_f64()) else {
                return ScalarValue::Int64(None);
            };
            let value = match op {
                Add => l + r,
                Sub => l - r,
                Mul => l * r,
                Div => {
                    if r == 0.0 {
                        return ScalarValue::Float64(None);
                    }
                    l / r
                }
                Mod => {
                    if r == 0.0 {
                        return ScalarValue::Float64(None);
                    }
                    l % r
                }
                _ => unreachable!(),
            };
            ScalarValue::Float64(Some(value))
        }
        Like | NotLike | In | NotIn => ScalarValue::Bool(None),
    }
}

fn as_bool(value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Bool(Some(b)) => Some(*b),
        _ => None,
    }
}
