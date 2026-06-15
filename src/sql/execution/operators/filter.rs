use crate::common::error::Result;
use crate::sql::execution::functions::registry::FunctionRegistry;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::{AggregateExpr, LogicalExpr};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct FilterExec {
    pub predicate: LogicalExpr,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
}

impl ExecutionOp for FilterExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.input.next_batch()? {
            let predicate = remap_expr_for_batch(&self.predicate, &self.schema, &batch.schema);
            let mask = eval_filter(&predicate, &batch)?;
            let filtered = super::apply_filter_mask(&batch, &mask);
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
        }
        Ok(None)
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
        LogicalExpr::Aggregate(agg) => {
            LogicalExpr::Aggregate(remap_aggregate_for_batch(agg, logical_schema, batch_schema))
        }
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
        _ => expr.clone(),
    }
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
    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let left_mask = eval_filter(left, batch)?;
                let right_mask = eval_filter(right, batch)?;
                for i in 0..num_rows {
                    mask[i] = left_mask[i] && right_mask[i];
                }
            }
            BinOp::Or => {
                let left_mask = eval_filter(left, batch)?;
                let right_mask = eval_filter(right, batch)?;
                for i in 0..num_rows {
                    mask[i] = left_mask[i] || right_mask[i];
                }
            }
            _ => {
                let left_vals = eval_expr_column(left, batch)?;
                let right_vals = eval_expr_column(right, batch)?;
                for i in 0..num_rows {
                    mask[i] = eval_comparison(&left_vals[i], op, &right_vals[i]);
                }
            }
        },
        LogicalExpr::Not(inner) => {
            let inner_mask = eval_filter(inner, batch)?;
            for i in 0..num_rows {
                mask[i] = !inner_mask[i];
            }
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            for i in 0..num_rows {
                mask[i] = vals[i].is_null();
            }
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                *m = !vals[i].is_null();
            }
        }
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => {
            let vals = eval_expr_column(expr, batch)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                let found = values
                    .iter()
                    .any(|candidate| eval_comparison(&vals[i], &BinOp::Eq, candidate));
                *m = if *negated { !found } else { found };
            }
        }
        LogicalExpr::Literal(v) => {
            for m in mask.iter_mut().take(num_rows) {
                *m = match v {
                    ScalarValue::Bool(Some(value)) => *value,
                    _ => !v.is_null(),
                };
            }
        }
        _ => {
            let vals = eval_expr_column(expr, batch)?;
            for (i, m) in mask.iter_mut().take(num_rows).enumerate() {
                *m = !vals[i].is_null();
            }
        }
    }

    Ok(mask)
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
    let num_rows = batch.num_rows();
    match expr {
        LogicalExpr::Column(idx) => Ok((0..num_rows)
            .map(|i| batch.scalar(i, *idx).unwrap_or(ScalarValue::Int64(None)))
            .collect()),
        LogicalExpr::Literal(v) => Ok(vec![v.clone(); num_rows]),
        LogicalExpr::BinaryOp { left, op, right } => {
            let lv = eval_expr_column(left, batch)?;
            let rv = eval_expr_column(right, batch)?;
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

            let col_idx = batch.schema.columns.iter().position(|c| {
                let name = c.name.to_ascii_uppercase();
                name.starts_with(&format!("{}(", agg_type_prefix))
                    || name == format!("{}(*)", agg_type_prefix)
            });

            if let Some(idx) = col_idx {
                Ok((0..num_rows)
                    .map(|i| batch.scalar(i, idx).unwrap_or(ScalarValue::Int64(None)))
                    .collect())
            } else {
                Ok(vec![ScalarValue::Int64(None); num_rows])
            }
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(v.is_null())))
                .collect())
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(!v.is_null())))
                .collect())
        }
        LogicalExpr::Not(inner) => {
            let vals = eval_expr_column(inner, batch)?;
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
            let vals = eval_expr_column(expr, batch)?;
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
        LogicalExpr::Function { name, args } => {
            let arg_columns = args
                .iter()
                .map(|arg| eval_expr_column(arg, batch))
                .collect::<Result<Vec<_>>>()?;
            let registry = FunctionRegistry::default();
            let mut result = Vec::with_capacity(num_rows);
            for row_idx in 0..num_rows {
                let row_args = arg_columns
                    .iter()
                    .map(|values| values[row_idx].clone())
                    .collect::<Vec<_>>();
                result.push(registry.execute(name, &row_args)?);
            }
            Ok(result)
        }
        _ => Ok(vec![ScalarValue::Int64(None); num_rows]),
    }
}

fn eval_binary_scalar(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> ScalarValue {
    if left.is_null() || right.is_null() {
        return ScalarValue::Int64(None);
    }
    if matches!(op, BinOp::Div) {
        let Some(l) = left.numeric_f64() else {
            return ScalarValue::Float64(None);
        };
        let Some(r) = right.numeric_f64() else {
            return ScalarValue::Float64(None);
        };
        if r == 0.0 {
            return ScalarValue::Float64(None);
        }
        return ScalarValue::Float64(Some(l / r));
    }
    if let (Some(l), Some(r)) = (left.integer_i128(), right.integer_i128()) {
        return match op {
            BinOp::Add => i64::try_from(l + r)
                .ok()
                .map(|v| ScalarValue::Int64(Some(v)))
                .unwrap_or(ScalarValue::Int64(None)),
            BinOp::Sub => i64::try_from(l - r)
                .ok()
                .map(|v| ScalarValue::Int64(Some(v)))
                .unwrap_or(ScalarValue::Int64(None)),
            BinOp::Mul => i64::try_from(l * r)
                .ok()
                .map(|v| ScalarValue::Int64(Some(v)))
                .unwrap_or(ScalarValue::Int64(None)),
            BinOp::Div => unreachable!("division is handled before integer arithmetic"),
            BinOp::Mod if r != 0 => i64::try_from(l % r)
                .ok()
                .map(|v| ScalarValue::Int64(Some(v)))
                .unwrap_or(ScalarValue::Int64(None)),
            BinOp::Eq => ScalarValue::Bool(Some(l == r)),
            BinOp::Neq => ScalarValue::Bool(Some(l != r)),
            BinOp::Lt => ScalarValue::Bool(Some(l < r)),
            BinOp::Gt => ScalarValue::Bool(Some(l > r)),
            BinOp::Lte => ScalarValue::Bool(Some(l <= r)),
            BinOp::Gte => ScalarValue::Bool(Some(l >= r)),
            _ => ScalarValue::Bool(None),
        };
    }
    match (left, right) {
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => match op {
            BinOp::And => ScalarValue::Bool(Some(*l && *r)),
            BinOp::Or => ScalarValue::Bool(Some(*l || *r)),
            BinOp::Eq => ScalarValue::Bool(Some(l == r)),
            BinOp::Neq => ScalarValue::Bool(Some(l != r)),
            _ => ScalarValue::Bool(None),
        },
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => match op {
            BinOp::Add => ScalarValue::Int64(Some(l + r)),
            BinOp::Sub => ScalarValue::Int64(Some(l - r)),
            BinOp::Mul => ScalarValue::Int64(Some(l * r)),
            BinOp::Div => unreachable!("division is handled before integer arithmetic"),
            BinOp::Mod if *r != 0 => ScalarValue::Int64(Some(l % r)),
            BinOp::Mod => ScalarValue::Int64(None),
            BinOp::Eq => ScalarValue::Bool(Some(l == r)),
            BinOp::Neq => ScalarValue::Bool(Some(l != r)),
            BinOp::Lt => ScalarValue::Bool(Some(l < r)),
            BinOp::Gt => ScalarValue::Bool(Some(l > r)),
            BinOp::Lte => ScalarValue::Bool(Some(l <= r)),
            BinOp::Gte => ScalarValue::Bool(Some(l >= r)),
            BinOp::And | BinOp::Or => ScalarValue::Bool(None),
            _ => ScalarValue::Bool(None),
        },
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => match op {
            BinOp::Add => ScalarValue::Float64(Some(l + r)),
            BinOp::Sub => ScalarValue::Float64(Some(l - r)),
            BinOp::Mul => ScalarValue::Float64(Some(l * r)),
            BinOp::Div => ScalarValue::Float64(Some(l / r)),
            BinOp::Mod => ScalarValue::Float64(Some(l % r)),
            _ => ScalarValue::Bool(None),
        },
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => match op {
            BinOp::Add => ScalarValue::Int32(Some(l + r)),
            BinOp::Sub => ScalarValue::Int32(Some(l - r)),
            _ => ScalarValue::Bool(None),
        },
        _ => ScalarValue::Int64(None),
    }
}
