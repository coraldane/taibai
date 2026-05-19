use crate::common::error::Result;
use crate::sql::planner::LogicalExpr;
use crate::sql::parser::ast::BinOp;
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
            let mask = eval_filter(&self.predicate, &batch)?;
            let filtered = super::apply_filter_mask(&batch, &mask);
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
        }
        Ok(None)
    }
}

pub fn eval_filter(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<bool>> {
    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            match op {
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
            }
        }
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
            for i in 0..num_rows {
                mask[i] = !vals[i].is_null();
            }
        }
        LogicalExpr::Literal(v) => {
            for i in 0..num_rows {
                mask[i] = !v.is_null();
            }
        }
        _ => {
            let vals = eval_expr_column(expr, batch)?;
            for i in 0..num_rows {
                mask[i] = !vals[i].is_null();
            }
        }
    }

    Ok(mask)
}

fn eval_comparison(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    match (left, right) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l, op, *r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => cmp_i64(*l as i64, op, *r as i64),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => cmp_i64(*l as i64, op, *r as i64),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => cmp_i64(*l as i64, op, *r as i64),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => cmp_u64(*l, op, *r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => cmp_u64(*l as u64, op, *r as u64),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => cmp_f64(*l, op, *r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => cmp_f64(*l as f64, op, *r as f64),
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => {
            match op {
                BinOp::Eq => l == r, BinOp::Neq => l != r, BinOp::Lt => l < r,
                BinOp::Gt => l > r, BinOp::Lte => l <= r, BinOp::Gte => l >= r,
                _ => false,
            }
        }
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => cmp_i64(*l, op, *r as i64),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l as i64, op, *r),
        _ => false,
    }
}

fn cmp_i64(l: i64, op: &BinOp, r: i64) -> bool {
    match op { BinOp::Eq => l == r, BinOp::Neq => l != r, BinOp::Lt => l < r, BinOp::Gt => l > r, BinOp::Lte => l <= r, BinOp::Gte => l >= r, _ => false }
}

fn cmp_u64(l: u64, op: &BinOp, r: u64) -> bool {
    match op { BinOp::Eq => l == r, BinOp::Neq => l != r, BinOp::Lt => l < r, BinOp::Gt => l > r, BinOp::Lte => l <= r, BinOp::Gte => l >= r, _ => false }
}

fn cmp_f64(l: f64, op: &BinOp, r: f64) -> bool {
    match op { BinOp::Eq => l == r, BinOp::Neq => l != r, BinOp::Lt => l < r, BinOp::Gt => l > r, BinOp::Lte => l <= r, BinOp::Gte => l >= r, _ => false }
}

pub fn eval_expr_column(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<ScalarValue>> {
    let num_rows = batch.num_rows();
    match expr {
        LogicalExpr::Column(idx) => {
            Ok((0..num_rows).map(|i| batch.scalar(i, *idx).unwrap_or(ScalarValue::Int64(None))).collect())
        }
        LogicalExpr::Literal(v) => {
            Ok(vec![v.clone(); num_rows])
        }
        LogicalExpr::BinaryOp { left, op, right } => {
            let lv = eval_expr_column(left, batch)?;
            let rv = eval_expr_column(right, batch)?;
            let mut result = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                result.push(eval_binary_scalar(&lv[i], op, &rv[i]));
            }
            Ok(result)
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals.iter().map(|v| ScalarValue::Bool(Some(v.is_null()))).collect())
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals.iter().map(|v| ScalarValue::Bool(Some(!v.is_null()))).collect())
        }
        LogicalExpr::Not(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals.iter().map(|v| {
                match v {
                    ScalarValue::Bool(Some(b)) => ScalarValue::Bool(Some(!b)),
                    _ => ScalarValue::Bool(None),
                }
            }).collect())
        }
        _ => Ok(vec![ScalarValue::Int64(None); num_rows]),
    }
}

fn eval_binary_scalar(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> ScalarValue {
    if left.is_null() || right.is_null() {
        return ScalarValue::Int64(None);
    }
    match (left, right) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => {
            match op {
                BinOp::Add => ScalarValue::Int64(Some(l + r)),
                BinOp::Sub => ScalarValue::Int64(Some(l - r)),
                BinOp::Mul => ScalarValue::Int64(Some(l * r)),
                BinOp::Div => ScalarValue::Int64(Some(l / r)),
                BinOp::Eq => ScalarValue::Bool(Some(l == r)),
                BinOp::Neq => ScalarValue::Bool(Some(l != r)),
                BinOp::Lt => ScalarValue::Bool(Some(l < r)),
                BinOp::Gt => ScalarValue::Bool(Some(l > r)),
                BinOp::Lte => ScalarValue::Bool(Some(l <= r)),
                BinOp::Gte => ScalarValue::Bool(Some(l >= r)),
                BinOp::And | BinOp::Or => ScalarValue::Bool(None),
            }
        }
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            match op {
                BinOp::Add => ScalarValue::Float64(Some(l + r)),
                BinOp::Sub => ScalarValue::Float64(Some(l - r)),
                BinOp::Mul => ScalarValue::Float64(Some(l * r)),
                BinOp::Div => ScalarValue::Float64(Some(l / r)),
                _ => ScalarValue::Bool(None),
            }
        }
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
            match op {
                BinOp::Add => ScalarValue::Int32(Some(l + r)),
                BinOp::Sub => ScalarValue::Int32(Some(l - r)),
                _ => ScalarValue::Bool(None),
            }
        }
        _ => ScalarValue::Int64(None),
    }
}
