use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::helpers::coerce_scalar_for_dtype;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::ExecutionOp;

pub struct ProjectExec {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
}

impl ExecutionOp for ProjectExec {
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

                for (i, expr) in self.expressions.iter().enumerate() {
                    let expr = remap_expr_for_batch(expr, &self.input_schema, &batch.schema);
                    let vals = eval_expr_column(&expr, &batch)?;
                    let col_def =
                        project_column_def(&expr, &batch.schema, self.aliases[i].as_deref());
                    let mut col = ColumnArray::empty(col_def.data_type);
                    for v in &vals {
                        let coerced = coerce_project_value(v, &col_def)?;
                        col.push(&coerced)?;
                    }
                    columns.push(col);
                    col_defs.push(col_def);
                }

                let schema = Schema::new(col_defs);
                Ok(Some(RecordBatch::try_new(schema, columns)?))
            }
            None => Ok(None),
        }
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
        LogicalExpr::BinaryOp { op: BinOp::Div, .. } => DataType::Float64,
        LogicalExpr::BinaryOp { .. } => DataType::Int64,
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

    ColumnDef::new(alias.unwrap_or("expr"), infer_type(expr, schema))
}

#[cfg(test)]
#[path = "project_tests.rs"]
mod tests;
