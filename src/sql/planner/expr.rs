use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::types::*;
use super::AliasMap;

pub fn ast_expr_to_logical(
    expr: &Expr,
    schema: &Schema,
    alias_map: &AliasMap,
) -> Result<LogicalExpr> {
    match expr {
        Expr::Literal(v) => Ok(LogicalExpr::Literal(v.clone())),
        Expr::Column(name) => {
            let idx = schema
                .index_of(name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            let idx = resolve_qualified_column_index(table, column, schema, alias_map)?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = ast_expr_to_logical(left, schema, alias_map)?;
            let r = ast_expr_to_logical(right, schema, alias_map)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryNot(inner) => {
            let e = ast_expr_to_logical(inner, schema, alias_map)?;
            Ok(LogicalExpr::Not(Box::new(e)))
        }
        Expr::UnaryMinus(inner) => {
            let e = ast_expr_to_logical(inner, schema, alias_map)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
                op: BinOp::Sub,
                right: Box::new(e),
            })
        }
        Expr::IsNull(inner) => {
            let e = ast_expr_to_logical(inner, schema, alias_map)?;
            Ok(LogicalExpr::IsNull(Box::new(e)))
        }
        Expr::IsNotNull(inner) => {
            let e = ast_expr_to_logical(inner, schema, alias_map)?;
            Ok(LogicalExpr::IsNotNull(Box::new(e)))
        }
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let e = ast_expr_to_logical(expr, schema, alias_map)?;
            let vals: Vec<ScalarValue> = values
                .iter()
                .filter_map(|v| match v {
                    Expr::Literal(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            Ok(LogicalExpr::In {
                expr: Box::new(e),
                values: vals,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = ast_expr_to_logical(expr, schema, alias_map)?;
            let lo = ast_expr_to_logical(low, schema, alias_map)?;
            let hi = ast_expr_to_logical(high, schema, alias_map)?;
            Ok(LogicalExpr::Between {
                expr: Box::new(e),
                low: Box::new(lo),
                high: Box::new(hi),
                negated: *negated,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let e = ast_expr_to_logical(expr, schema, alias_map)?;
            let pat = match &**pattern {
                Expr::Literal(ScalarValue::VarChar(Some(s))) => s.clone(),
                _ => "%".to_string(),
            };
            Ok(LogicalExpr::Like {
                expr: Box::new(e),
                pattern: pat,
                negated: *negated,
            })
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let func = match name.to_uppercase().as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "AVG" => AggFunc::Avg,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                other => return Err(MonaxError::Sql(format!("unknown function '{}'", other))),
            };
            let arg = if args.is_empty()
                || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
            {
                LogicalExpr::Wildcard
            } else {
                ast_expr_to_logical(&args[0], schema, alias_map)?
            };
            Ok(LogicalExpr::Aggregate(AggregateExpr {
                func,
                arg: Box::new(arg),
                distinct: *distinct,
            }))
        }
        _ => Err(MonaxError::Sql(format!("unsupported expression type"))),
    }
}

fn resolve_qualified_column_index(
    table: &str,
    column: &str,
    schema: &Schema,
    _alias_map: &AliasMap,
) -> Result<usize> {
    schema.index_of(column).ok_or_else(|| {
        MonaxError::Sql(format!(
            "column '{}' not found in table '{}'",
            column, table
        ))
    })
}

pub fn ast_expr_to_logical_join(
    expr: &Expr,
    output_schema: &Schema,
    left_schema: &Schema,
    right_schema: &Schema,
    alias_map: &AliasMap,
) -> Result<LogicalExpr> {
    match expr {
        Expr::Literal(v) => Ok(LogicalExpr::Literal(v.clone())),
        Expr::Column(name) => {
            let idx = output_schema
                .index_of(name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            let _resolved_table = alias_map
                .get(&table.to_lowercase())
                .map(|s| s.as_str())
                .unwrap_or(table);

            let left_idx = left_schema.index_of(column);
            let right_idx = right_schema.index_of(column);

            let idx = match (left_idx, right_idx) {
                (Some(l), None) => l,
                (None, Some(r)) => left_schema.columns.len() + r,
                (Some(l), Some(_)) => l,
                (None, None) => {
                    return Err(MonaxError::Sql(format!(
                        "column '{}' not found in table '{}'",
                        column, table
                    )));
                }
            };

            Ok(LogicalExpr::Column(idx))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = ast_expr_to_logical_join(
                left,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            let r = ast_expr_to_logical_join(
                right,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryNot(inner) => {
            let e = ast_expr_to_logical_join(
                inner,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            Ok(LogicalExpr::Not(Box::new(e)))
        }
        Expr::UnaryMinus(inner) => {
            let e = ast_expr_to_logical_join(
                inner,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
                op: BinOp::Sub,
                right: Box::new(e),
            })
        }
        Expr::IsNull(inner) => {
            let e = ast_expr_to_logical_join(
                inner,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            Ok(LogicalExpr::IsNull(Box::new(e)))
        }
        Expr::IsNotNull(inner) => {
            let e = ast_expr_to_logical_join(
                inner,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
            )?;
            Ok(LogicalExpr::IsNotNull(Box::new(e)))
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let func = match name.to_uppercase().as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "AVG" => AggFunc::Avg,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                other => return Err(MonaxError::Sql(format!("unknown function '{}'", other))),
            };
            let arg = if args.is_empty()
                || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
            {
                LogicalExpr::Wildcard
            } else {
                ast_expr_to_logical_join(
                    &args[0],
                    output_schema,
                    left_schema,
                    right_schema,
                    alias_map,
                )?
            };
            Ok(LogicalExpr::Aggregate(AggregateExpr {
                func,
                arg: Box::new(arg),
                distinct: *distinct,
            }))
        }
        _ => Err(MonaxError::Sql(format!(
            "unsupported expression type in join context"
        ))),
    }
}

pub fn extract_aggregates(select_list: &[SelectItem]) -> Vec<(AggFunc, Option<Expr>, bool)> {
    let mut aggs = Vec::new();
    for item in select_list {
        if let SelectItem::Expr(expr, _) = item {
            extract_aggs_from_expr(expr, &mut aggs);
        }
    }
    aggs
}

fn extract_aggs_from_expr(expr: &Expr, aggs: &mut Vec<(AggFunc, Option<Expr>, bool)>) {
    if let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    {
        let func = match name.to_uppercase().as_str() {
            "COUNT" => Some(AggFunc::Count),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            _ => None,
        };
        if let Some(func) = func {
            let arg = if args.is_empty()
                || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
            {
                None
            } else {
                Some(args[0].clone())
            };
            aggs.push((func, arg, *distinct));
        }
    }
}

pub fn resolve_projection_with_alias(
    select_list: &[SelectItem],
    schema: &Schema,
    alias_map: &AliasMap,
) -> (Vec<LogicalExpr>, Vec<Option<String>>) {
    let mut exprs = Vec::new();
    let mut aliases = Vec::new();

    for item in select_list {
        match item {
            SelectItem::Star => {
                for i in 0..schema.columns.len() {
                    exprs.push(LogicalExpr::Column(i));
                    aliases.push(None);
                }
            }
            SelectItem::Expr(expr, alias) => {
                let logical =
                    ast_expr_to_logical(expr, schema, alias_map).unwrap_or(LogicalExpr::Wildcard);
                exprs.push(logical);
                aliases.push(alias.clone());
            }
        }
    }

    (exprs, aliases)
}

pub fn collect_column_refs(exprs: &[LogicalExpr]) -> Vec<usize> {
    let mut cols = Vec::new();
    for expr in exprs {
        collect_cols_from_expr(expr, &mut cols);
    }
    cols.sort();
    cols.dedup();
    cols
}

pub fn collect_cols_from_expr(expr: &LogicalExpr, cols: &mut Vec<usize>) {
    match expr {
        LogicalExpr::Column(idx) => {
            cols.push(*idx);
        }
        LogicalExpr::BinaryOp { left, right, .. } => {
            collect_cols_from_expr(left, cols);
            collect_cols_from_expr(right, cols);
        }
        LogicalExpr::Not(inner) => collect_cols_from_expr(inner, cols),
        LogicalExpr::IsNull(inner) => collect_cols_from_expr(inner, cols),
        LogicalExpr::IsNotNull(inner) => collect_cols_from_expr(inner, cols),
        LogicalExpr::Aggregate(agg) => collect_cols_from_expr(&agg.arg, cols),
        _ => {}
    }
}

pub fn infer_type(expr: &LogicalExpr, schema: &Schema) -> DataType {
    match expr {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64),
        LogicalExpr::Literal(v) => v.data_type(),
        LogicalExpr::BinaryOp { .. } => DataType::Int64,
        LogicalExpr::Aggregate(agg) => match agg.func {
            AggFunc::Count => DataType::Int64,
            _ => DataType::Float64,
        },
        _ => DataType::Int64,
    }
}
