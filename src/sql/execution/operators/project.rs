use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::helpers::coerce_scalar_for_dtype;
use crate::sql::execution::operators::filter::{eval_expr_column_with, remap_expr_for_batch};
use crate::sql::execution::subquery_runtime::SubqueryRegistry;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

use super::ExecutionOp;

pub struct ProjectExec {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
    /// Snapshot registry for correlated subqueries, when the query contains
    /// any.
    pub subqueries: Option<SubqueryRegistry>,
    /// Remapped-expression cache: `remap_expr_for_batch` is deterministic
    /// per batch schema, so cloning the expression tree per batch (and per
    /// expression) is wasted work once the layout is stable.
    pub remapped: Option<(Schema, Vec<LogicalExpr>)>,
}

impl ExecutionOp for ProjectExec {
    fn output_schema(&self) -> Option<Schema> {
        // Derived from the input schema and the projection list, so it holds
        // for an input that yields no batch at all.
        Some(Schema::new(
            self.expressions
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    project_column_def(
                        expr,
                        &self.input_schema,
                        self.aliases.get(i).and_then(|alias| alias.as_deref()),
                    )
                })
                .collect(),
        ))
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.next_batch()? {
            Some(batch) => {
                if let Some((positions, col_defs)) = resolve_plain_column_projection(
                    &batch,
                    &self.expressions,
                    &self.aliases,
                    &self.input_schema,
                ) {
                    let projected = project_plain_columns(batch, positions, col_defs)?;
                    return Ok(Some(projected));
                }

                let mut columns = Vec::with_capacity(self.expressions.len());
                let mut col_defs = Vec::with_capacity(self.expressions.len());

                let exprs = match &self.remapped {
                    Some((cached_schema, exprs)) if *cached_schema == batch.schema => exprs,
                    _ => {
                        let exprs: Vec<LogicalExpr> = self
                            .expressions
                            .iter()
                            .map(|expr| {
                                remap_expr_for_batch(expr, &self.input_schema, &batch.schema)
                            })
                            .collect();
                        self.remapped = Some((batch.schema.clone(), exprs));
                        self.remapped.as_ref().map(|(_, exprs)| exprs).unwrap()
                    }
                };
                for (i, expr) in exprs.iter().enumerate() {
                    let col_def =
                        project_column_def(expr, &batch.schema, self.aliases[i].as_deref());
                    let column = self.project_column(expr, &batch, &col_def)?;
                    columns.push(column);
                    col_defs.push(col_def);
                }

                let schema = Schema::new(col_defs);
                Ok(Some(RecordBatch::try_new(schema, columns)?))
            }
            None => Ok(None),
        }
    }
}

impl ProjectExec {
    /// Evaluate one projection expression. Prefers the typed kernels and only
    /// falls back to the per-value scalar path when the expression shape or
    /// the target coercion is not covered, so both paths stay semantically
    /// identical.
    fn project_column(
        &self,
        expr: &LogicalExpr,
        batch: &RecordBatch,
        col_def: &ColumnDef,
    ) -> Result<ColumnArray> {
        if col_def.data_type == DataType::Decimal {
            if let Some(column) =
                eval_decimal_arithmetic(expr, batch, &batch.schema, self.subqueries.as_ref())?
            {
                return Ok(column);
            }
        }
        if let Some(column) = crate::sql::execution::expr_vec::eval_expr_to_column(expr, batch)? {
            if column.data_type() == col_def.data_type {
                return Ok(column);
            }
        }
        let vals = eval_expr_column_with(expr, batch, self.subqueries.as_ref())?;
        let mut col = ColumnArray::empty(col_def.data_type);
        for v in &vals {
            let coerced = coerce_project_value(v, col_def)?;
            col.push(&coerced)?;
        }
        Ok(col)
    }
}

fn resolve_plain_column_projection(
    batch: &RecordBatch,
    expressions: &[LogicalExpr],
    aliases: &[Option<String>],
    input_schema: &Schema,
) -> Option<(Vec<usize>, Vec<crate::types::batch::ColumnDef>)> {
    if expressions.is_empty() {
        return None;
    }

    let mut positions = Vec::with_capacity(expressions.len());
    let mut col_defs = Vec::with_capacity(expressions.len());

    for (i, expr) in expressions.iter().enumerate() {
        let LogicalExpr::Column(logical_idx) = expr else {
            return None;
        };
        let source_def = input_schema.columns.get(*logical_idx)?;
        let batch_pos = if batch.schema.columns.get(*logical_idx) == Some(source_def) {
            *logical_idx
        } else {
            batch.schema.index_of(&source_def.name)?
        };

        let mut out_def = batch.schema.columns[batch_pos].clone();
        if let Some(alias) = aliases.get(i).and_then(|a| a.as_deref()) {
            out_def.name = alias.to_string();
        }
        positions.push(batch_pos);
        col_defs.push(out_def);
    }

    Some((positions, col_defs))
}

fn project_plain_columns(
    mut batch: RecordBatch,
    positions: Vec<usize>,
    col_defs: Vec<crate::types::batch::ColumnDef>,
) -> Result<RecordBatch> {
    let already_in_order = positions
        .iter()
        .enumerate()
        .all(|(expected, &actual)| expected == actual)
        && positions.len() == batch.num_columns();

    if already_in_order {
        batch.schema = Schema::new(col_defs);
        return Ok(batch);
    }

    let columns = positions
        .iter()
        .map(|&pos| batch.columns[pos].clone())
        .collect();
    RecordBatch::try_new(Schema::new(col_defs), columns)
}

pub fn infer_type(expr: &LogicalExpr, schema: &Schema) -> DataType {
    match expr {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64),
        LogicalExpr::Literal(v) => v.data_type(),
        LogicalExpr::BinaryOp { op, left, right } => match op {
            BinOp::Eq
            | BinOp::Neq
            | BinOp::Lt
            | BinOp::Gt
            | BinOp::Lte
            | BinOp::Gte
            | BinOp::And
            | BinOp::Or => DataType::Bool,
            BinOp::Div => DataType::Float64,
            BinOp::Like | BinOp::NotLike | BinOp::In | BinOp::NotIn => DataType::Bool,
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod => {
                let lt = infer_type(left, schema);
                let rt = infer_type(right, schema);
                crate::sql::binder::type_resolver::common_type(&lt, &rt)
            }
        },
        LogicalExpr::Aggregate(agg) => {
            let agg_prefix = match agg.func {
                crate::sql::planner::AggFunc::Count => "count",
                crate::sql::planner::AggFunc::Sum => "sum",
                crate::sql::planner::AggFunc::Avg => "avg",
                crate::sql::planner::AggFunc::Min => "min",
                crate::sql::planner::AggFunc::Max => "max",
            };

            schema
                .columns
                .iter()
                .find(|c| {
                    let name = c.name.to_ascii_lowercase();
                    name.starts_with(&format!("{}(", agg_prefix))
                        || name == format!("{}(*)", agg_prefix)
                })
                .map(|c| c.data_type)
                .unwrap_or(DataType::Int64)
        }
        LogicalExpr::Function { name, args } => {
            let arg_types = args
                .iter()
                .map(|arg| infer_type(arg, schema))
                .collect::<Vec<_>>();
            crate::sql::planner::expr::infer_scalar_function_type(name, &arg_types)
        }
        LogicalExpr::Case {
            when_clauses,
            else_clause,
            ..
        } => {
            let branch_types = when_clauses
                .iter()
                .map(|(_, then_expr)| infer_type(then_expr, schema))
                .chain(else_clause.as_deref().map(|expr| infer_type(expr, schema)));
            branch_types
                .reduce(|left, right| crate::sql::binder::type_resolver::common_type(&left, &right))
                .unwrap_or(DataType::Int64)
        }
        _ => DataType::Int64,
    }
}

fn coerce_project_value(
    value: &crate::types::scalar::ScalarValue,
    column: &ColumnDef,
) -> Result<crate::types::scalar::ScalarValue> {
    if column.data_type == DataType::Decimal
        && value.data_type() != DataType::Decimal
        && !value.is_null()
    {
        let multiplier = 10_i64.checked_pow(column.scale as u32).ok_or_else(|| {
            crate::common::error::TaibaiError::Sql(format!(
                "invalid decimal scale {}",
                column.scale
            ))
        })?;
        if let Some(integer) = value.integer_i128() {
            let scaled = integer
                .checked_mul(multiplier as i128)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or_else(|| {
                    crate::common::error::TaibaiError::Sql(
                        "decimal projection value overflow".into(),
                    )
                })?;
            return Ok(crate::types::scalar::ScalarValue::Decimal(Some(scaled)));
        }
    }
    coerce_scalar_for_dtype(value, column.data_type)
}

pub fn project_column_def(expr: &LogicalExpr, schema: &Schema, alias: Option<&str>) -> ColumnDef {
    if let LogicalExpr::Column(idx) = expr {
        if let Some(source) = schema.columns.get(*idx) {
            let mut col_def = source.clone();
            if let Some(alias) = alias {
                col_def.name = alias.to_string();
            }
            return col_def;
        }
    }

    if let LogicalExpr::Case {
        when_clauses,
        else_clause,
        ..
    } = expr
    {
        let branch = when_clauses
            .iter()
            .map(|(_, then_expr)| then_expr)
            .chain(else_clause.iter().map(|expr| expr.as_ref()))
            .find_map(|branch| match branch {
                LogicalExpr::Column(index)
                    if schema.columns.get(*index)?.data_type == DataType::Decimal =>
                {
                    schema.columns.get(*index).cloned()
                }
                _ => None,
            });
        if let Some(mut column) = branch {
            column.name = alias.unwrap_or("expr").to_string();
            column.nullable = true;
            return column;
        }
    }

    if let LogicalExpr::Aggregate(agg) = expr {
        let agg_prefix = match agg.func {
            crate::sql::planner::AggFunc::Count => "count",
            crate::sql::planner::AggFunc::Sum => "sum",
            crate::sql::planner::AggFunc::Avg => "avg",
            crate::sql::planner::AggFunc::Min => "min",
            crate::sql::planner::AggFunc::Max => "max",
        };
        if let Some(column) = schema.columns.iter().find(|c| {
            let name = c.name.to_ascii_lowercase();
            name.starts_with(&format!("{}(", agg_prefix)) || name == format!("{}(*)", agg_prefix)
        }) {
            let mut col_def = column.clone();
            if let Some(alias) = alias {
                col_def.name = alias.to_string();
            }
            return col_def;
        }
    }

    let mut col_def = ColumnDef::new(alias.unwrap_or("expr"), infer_type(expr, schema));
    if col_def.data_type == DataType::Decimal {
        if let Some(scale) = decimal_result_scale(expr, schema) {
            col_def = col_def.with_scale(scale).with_precision(18);
        }
    }
    col_def
}

/// Result scale of an arithmetic expression over Decimal operands:
/// Add/Sub take the wider operand scale, Mul sums them (capped at 18,
/// matching the CREATE TABLE scale limit). `None` when no Decimal operand.
fn decimal_result_scale(expr: &LogicalExpr, schema: &Schema) -> Option<i32> {
    match expr {
        LogicalExpr::Column(idx) => {
            let column = schema.columns.get(*idx)?;
            (column.data_type == DataType::Decimal).then_some(column.scale)
        }
        LogicalExpr::BinaryOp { op, left, right } => {
            if !matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod) {
                return None;
            }
            let ls = decimal_result_scale(left, schema);
            let rs = decimal_result_scale(right, schema);
            match (ls, rs) {
                (Some(l), Some(r)) if matches!(op, BinOp::Mul) => Some((l + r).min(18)),
                (Some(l), Some(r)) => Some(l.max(r)),
                (Some(s), None) | (None, Some(s)) => Some(s),
                (None, None) => None,
            }
        }
        LogicalExpr::Literal(v) => {
            use crate::types::scalar::ScalarValue;
            match v {
                ScalarValue::Float32(Some(f)) => Some(float_literal_scale(*f as f64)),
                ScalarValue::Float64(Some(f)) => Some(float_literal_scale(*f)),
                _ => Some(0),
            }
        }
        _ => None,
    }
}

/// Scale-aware Decimal arithmetic for projections. Operand scales are
/// tracked recursively: Add/Sub align both operands to the wider scale,
/// Mul multiplies raws directly (scales add). Returns `None` for shapes it
/// does not cover so the caller falls back to the generic paths.
fn eval_decimal_arithmetic(
    expr: &LogicalExpr,
    batch: &RecordBatch,
    schema: &Schema,
    subqueries: Option<&SubqueryRegistry>,
) -> Result<Option<ColumnArray>> {
    let Some((raws, _scale)) = eval_decimal_operand(expr, batch, schema, subqueries)? else {
        return Ok(None);
    };
    // The result column's scale is set separately by decimal_result_scale.

    let mut data = Vec::with_capacity(raws.len());
    let mut bitmap = NullBitmap::with_capacity(raws.len());
    for raw in raws {
        match i64::try_from(raw) {
            Ok(v) => {
                data.push(v);
                bitmap.push_valid();
            }
            Err(_) => {
                data.push(0);
                bitmap.push_null();
            }
        }
    }
    Ok(Some(ColumnArray::Decimal { data, bitmap }))
}

/// Evaluate `expr` into per-row raw integers plus the scale they carry.
/// Scales follow decimal arithmetic rules: Add/Sub align to the wider
/// operand scale, Mul adds them (capped at 18).
fn eval_decimal_operand(
    expr: &LogicalExpr,
    batch: &RecordBatch,
    schema: &Schema,
    subqueries: Option<&SubqueryRegistry>,
) -> Result<Option<(Vec<i128>, i32)>> {
    match expr {
        LogicalExpr::Column(idx) => {
            let Some(column) = schema.columns.get(*idx) else {
                return Ok(None);
            };
            if column.data_type != DataType::Decimal {
                return Ok(None);
            }
            if batch.column(*idx).is_none() {
                return Ok(None);
            }
            let len = batch.num_rows();
            let mut raws = Vec::with_capacity(len);
            for row in 0..len {
                match batch.scalar(row, *idx) {
                    Some(crate::types::scalar::ScalarValue::Decimal(Some(v))) => {
                        raws.push(v as i128);
                    }
                    _ => raws.push(DECIMAL_NULL_SENTINEL),
                }
            }
            Ok(Some((raws, column.scale)))
        }
        LogicalExpr::BinaryOp { op, left, right } => {
            if !matches!(op, BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod) {
                return Ok(None);
            }
            let Some((lraws, ls)) = eval_decimal_operand(left, batch, schema, subqueries)? else {
                return Ok(None);
            };
            let Some((rraws, rs)) = eval_decimal_operand(right, batch, schema, subqueries)? else {
                return Ok(None);
            };
            let (scale, align_l, align_r) = match op {
                BinOp::Mul => ((ls + rs).min(18), 1, 1),
                _ => {
                    let common = ls.max(rs);
                    (
                        common,
                        10_i128.pow((common - ls).clamp(0, 18) as u32),
                        10_i128.pow((common - rs).clamp(0, 18) as u32),
                    )
                }
            };
            // Multiplication produces raw values at ls + rs decimal places.
            // If the result scale is capped, shrink the raw value as well;
            // changing only metadata would multiply the numeric result.
            let product_divisor = if matches!(op, BinOp::Mul) {
                10_i128.pow((ls + rs - scale) as u32)
            } else {
                1
            };
            let mut out = Vec::with_capacity(lraws.len());
            for (l, r) in lraws.into_iter().zip(rraws) {
                let value = if l == i128::MIN || r == i128::MIN {
                    None
                } else {
                    match op {
                        BinOp::Mul => l.checked_mul(r).map(|raw| raw / product_divisor),
                        BinOp::Add => l
                            .checked_mul(align_l)
                            .and_then(|l| r.checked_mul(align_r).and_then(|r| l.checked_add(r))),
                        BinOp::Sub => l
                            .checked_mul(align_l)
                            .and_then(|l| r.checked_mul(align_r).and_then(|r| l.checked_sub(r))),
                        BinOp::Mod => r.checked_mul(align_r).and_then(|r| {
                            if r == 0 {
                                None
                            } else {
                                l.checked_mul(align_l).and_then(|l| l.checked_rem(r))
                            }
                        }),
                        _ => None,
                    }
                };
                out.push(value.unwrap_or(i128::MIN));
            }
            Ok(Some((out, scale)))
        }
        LogicalExpr::Literal(v) => {
            use crate::types::scalar::ScalarValue;
            if v.is_null() {
                return Ok(None);
            }
            // Float literals keep their fractional digits as scale (1.5 ->
            // scale 1); integer and decimal literals contribute scale 0 /
            // their raw form respectively.
            let scale = match v {
                ScalarValue::Float32(Some(f)) => float_literal_scale(*f as f64),
                ScalarValue::Float64(Some(f)) => float_literal_scale(*f),
                _ => 0,
            };
            let factor = 10_i128.pow(scale as u32);
            let scalar = eval_expr_column_with(expr, batch, subqueries)?;
            let mut raws = Vec::with_capacity(scalar.len());
            for value in &scalar {
                let raw = match value {
                    ScalarValue::Float32(Some(f)) => (*f as f64 * factor as f64).round() as i128,
                    ScalarValue::Float64(Some(f)) => (f * factor as f64).round() as i128,
                    other => other.integer_i128().unwrap_or(DECIMAL_NULL_SENTINEL),
                };
                raws.push(raw);
            }
            Ok(Some((raws, scale)))
        }
        // CASE/functions may return a mixture of integers and already-scaled
        // decimals. Without branch scale metadata, treating their raw values
        // as scale 0 bypasses the projection's required decimal coercion.
        _ => Ok(None),
    }
}

fn float_literal_scale(v: f64) -> i32 {
    match format!("{}", v).split_once('.') {
        Some((_, frac)) => frac.len().min(9) as i32,
        None => 0,
    }
}

const DECIMAL_NULL_SENTINEL: i128 = i128::MIN;

#[cfg(test)]
#[path = "project_test.rs"]
mod tests;
