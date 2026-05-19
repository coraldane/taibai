use std::path::{Path, PathBuf};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr, Statement};

pub fn eval_where_clause(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
            let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
            match op {
                BinOp::Eq => Ok(left_val == right_val),
                BinOp::Neq => Ok(left_val != right_val),
                BinOp::Lt => Ok(compare_scalars(&left_val, &right_val) == Some(std::cmp::Ordering::Less)),
                BinOp::Gt => Ok(compare_scalars(&left_val, &right_val) == Some(std::cmp::Ordering::Greater)),
                BinOp::Lte => Ok(compare_scalars(&left_val, &right_val) != Some(std::cmp::Ordering::Greater)),
                BinOp::Gte => Ok(compare_scalars(&left_val, &right_val) != Some(std::cmp::Ordering::Less)),
                BinOp::And => {
                    let left_bool = eval_where_clause(left, batch, row_idx, schema)?;
                    let right_bool = eval_where_clause(right, batch, row_idx, schema)?;
                    Ok(left_bool && right_bool)
                }
                BinOp::Or => {
                    let left_bool = eval_where_clause(left, batch, row_idx, schema)?;
                    let right_bool = eval_where_clause(right, batch, row_idx, schema)?;
                    Ok(left_bool || right_bool)
                }
                _ => Ok(true),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(val.is_null())
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(!val.is_null())
        }
        Expr::UnaryNot(inner) => {
            let inner_bool = eval_where_clause(inner, batch, row_idx, schema)?;
            Ok(!inner_bool)
        }
        Expr::Literal(v) => Ok(!v.is_null()),
        Expr::Column(_) => Ok(true),
        Expr::QualifiedColumn { .. } => Ok(true),
        Expr::Function { .. } => Ok(true),
        Expr::UnaryMinus(_) => Ok(true),
        Expr::In { .. } => Ok(true),
        Expr::Between { .. } => Ok(true),
        Expr::Case { .. } => Ok(true),
        Expr::Like { .. } => Ok(true),
        Expr::Exists(_) => Ok(true),
        Expr::Subquery(_) => Ok(true),
        Expr::WindowFunction { .. } => Ok(true),
    }
}

pub fn eval_expr_for_row(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<ScalarValue> {
    match expr {
        Expr::Column(col_name) => {
            let col_idx = schema.index_of(col_name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", col_name)))?;
            Ok(batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(schema.columns[col_idx].data_type)))
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
            let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
            match op {
                BinOp::Add => {
                    match (&left_val, &right_val) {
                        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Ok(ScalarValue::Int64(Some(a + b))),
                        _ => Ok(left_val),
                    }
                }
                BinOp::Sub => {
                    match (&left_val, &right_val) {
                        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Ok(ScalarValue::Int64(Some(a - b))),
                        _ => Ok(left_val),
                    }
                }
                _ => Ok(left_val),
            }
        }
        Expr::IsNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::IsNotNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::UnaryNot(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::QualifiedColumn { column, .. } => {
            let col_idx = schema.index_of(column)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", column)))?;
            Ok(batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(schema.columns[col_idx].data_type)))
        }
        Expr::Function { .. } => Ok(ScalarValue::Int64(None)),
        Expr::UnaryMinus(_) => Ok(ScalarValue::Int64(None)),
        Expr::In { .. } => Ok(ScalarValue::Bool(None)),
        Expr::Between { .. } => Ok(ScalarValue::Bool(None)),
        Expr::Case { .. } => Ok(ScalarValue::Int64(None)),
        Expr::Like { .. } => Ok(ScalarValue::Bool(None)),
        Expr::Exists(_) => Ok(ScalarValue::Bool(None)),
        Expr::Subquery(_) => Ok(ScalarValue::Int64(None)),
        Expr::WindowFunction { .. } => Ok(ScalarValue::Int64(None)),
    }
}

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a.partial_cmp(b),
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => Some(a.cmp(b)),
        _ => None,
    }
}

pub fn eval_expr_to_scalar(expr: &Expr, dt: &DataType) -> Result<ScalarValue> {
    match expr {
        Expr::Literal(v) => {
            match (dt, v) {
                (DataType::Int32, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int32(Some(*v as i32))),
                (DataType::Int16, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int16(Some(*v as i16))),
                (DataType::Int8, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int8(Some(*v as i8))),
                (DataType::Float32, ScalarValue::Float64(Some(v))) => Ok(ScalarValue::Float32(Some(*v as f32))),
                _ => Ok(v.clone()),
            }
        }
        Expr::Column(_) => Ok(ScalarValue::null_for_type(*dt)),
        _ => Ok(ScalarValue::null_for_type(*dt)),
    }
}

pub fn sort_batch_by_key(
    batch: &RecordBatch,
    inserter: &crate::storage::sort_key::SortKeyInserter,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return Ok(batch.clone());
    }

    let mut rows_with_keys: Vec<(Vec<ScalarValue>, usize)> = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let key_values: Vec<ScalarValue> = inserter.key_indices.iter()
            .map(|&col_idx| batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type)))
            .collect();
        rows_with_keys.push((key_values, row_idx));
    }

    rows_with_keys.sort_by(|a, b| {
        for (ki, _) in inserter.key_indices.iter().enumerate() {
            let cmp = crate::storage::sort_key::compare_scalars_for_sort(&a.0[ki], &b.0[ki]);
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

    let mut sorted_batch = RecordBatch::new(batch.schema.clone());
    for (_, orig_row_idx) in rows_with_keys {
        for col_idx in 0..batch.num_columns() {
            let val = batch.scalar(orig_row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type));
            sorted_batch.column_mut(col_idx).unwrap().push(&val)?;
        }
    }

    Ok(sorted_batch)
}

pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
    if !table_dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name() {
            let name = name.to_string_lossy();
            if (name.starts_with("seg-") || name.starts_with('p')) && name.ends_with(".monax") {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}
