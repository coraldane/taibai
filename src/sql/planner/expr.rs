#![allow(dead_code)]
use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::types::*;
use super::{AliasMap, JoinTableInfo, PlannerContext};

pub fn ast_expr_to_logical_with_context(
    expr: &Expr,
    schema: &Schema,
    alias_map: &AliasMap,
    ctx: &PlannerContext,
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
            let l = ast_expr_to_logical_with_context(left, schema, alias_map, ctx)?;
            let r = ast_expr_to_logical_with_context(right, schema, alias_map, ctx)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryNot(inner) => {
            let e = ast_expr_to_logical_with_context(inner, schema, alias_map, ctx)?;
            Ok(LogicalExpr::Not(Box::new(e)))
        }
        Expr::UnaryMinus(inner) => {
            let e = ast_expr_to_logical_with_context(inner, schema, alias_map, ctx)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
                op: BinOp::Sub,
                right: Box::new(e),
            })
        }
        Expr::IsNull(inner) => {
            let e = ast_expr_to_logical_with_context(inner, schema, alias_map, ctx)?;
            Ok(LogicalExpr::IsNull(Box::new(e)))
        }
        Expr::IsNotNull(inner) => {
            let e = ast_expr_to_logical_with_context(inner, schema, alias_map, ctx)?;
            Ok(LogicalExpr::IsNotNull(Box::new(e)))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let e = ast_expr_to_logical_with_context(expr, schema, alias_map, ctx)?;
            let lo = ast_expr_to_logical_with_context(low, schema, alias_map, ctx)?;
            let hi = ast_expr_to_logical_with_context(high, schema, alias_map, ctx)?;
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
            let e = ast_expr_to_logical_with_context(expr, schema, alias_map, ctx)?;
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
                ast_expr_to_logical_with_context(&args[0], schema, alias_map, ctx)?
            };
            Ok(LogicalExpr::Aggregate(AggregateExpr {
                func,
                arg: Box::new(arg),
                distinct: *distinct,
            }))
        }
        Expr::Subquery(subquery_stmt) => {
            use super::plan_select_with_context;
            let subquery_plan = plan_select_with_context(subquery_stmt, ctx)?;
            Ok(LogicalExpr::ScalarSubquery(Box::new(subquery_plan)))
        }
        Expr::Exists(inner) => {
            match inner.as_ref() {
                Expr::Subquery(subquery_stmt) => {
                    use super::plan_select_with_context;
                    let subquery_plan = plan_select_with_context(subquery_stmt, ctx)?;
                    Ok(LogicalExpr::ExistsSubquery(Box::new(subquery_plan)))
                }
                _ => Err(MonaxError::Sql("EXISTS requires a subquery".into())),
            }
        }
        Expr::In {
            expr: inner_expr,
            values,
            negated,
        } => {
            let e = ast_expr_to_logical_with_context(inner_expr, schema, alias_map, ctx)?;
            let vals: Vec<ScalarValue> = values
                .iter()
                .filter_map(|v| match v {
                    Expr::Literal(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            if vals.len() == values.len() {
                Ok(LogicalExpr::In {
                    expr: Box::new(e),
                    values: vals,
                    negated: *negated,
                })
            } else {
                Err(MonaxError::Sql("IN subquery not yet supported in execution".into()))
            }
        }
        _ => Err(MonaxError::Sql(format!("unsupported expression type"))),
    }
}

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
    alias_map: &AliasMap,
) -> Result<usize> {
    let matching_indices: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.name.eq_ignore_ascii_case(column))
        .map(|(i, _)| i)
        .collect();
    
    if matching_indices.is_empty() {
        return Err(MonaxError::Sql(format!(
            "column '{}' not found in table '{}'",
            column, table
        )));
    }
    
    if matching_indices.len() == 1 {
        return Ok(matching_indices[0]);
    }
    
    let table_lower = table.to_lowercase();
    
    if matching_indices.len() == 2 {
        if table_lower.ends_with("1") || table_lower.starts_with("l") || table_lower.starts_with("a") || table_lower.starts_with("c") {
            return Ok(matching_indices[0]);
        }
        if table_lower.ends_with("2") || table_lower.starts_with("r") || table_lower.starts_with("b") || table_lower.starts_with("p") {
            return Ok(matching_indices[1]);
        }
        
        let resolved = alias_map.get(&table_lower);
        if let Some(real_table) = resolved {
            if real_table.ends_with("1") || real_table.starts_with("l") {
                return Ok(matching_indices[0]);
            }
            if real_table.ends_with("2") || real_table.starts_with("r") {
                return Ok(matching_indices[1]);
            }
        }
        
        let alias_pos = alias_map.iter().position(|(alias, _)| alias.eq_ignore_ascii_case(&table_lower));
        if let Some(pos) = alias_pos {
            if pos < matching_indices.len() {
                return Ok(matching_indices[pos % matching_indices.len()]);
            }
        }
    }
    
    Ok(matching_indices[0])
}

pub fn ast_expr_to_logical_join(
    expr: &Expr,
    output_schema: &Schema,
    left_schema: &Schema,
    right_schema: &Schema,
    alias_map: &AliasMap,
    table_info: &JoinTableInfo,
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
            let alias_lower = table.to_lowercase();
            
            let left_idx = left_schema.index_of(column);
            let right_idx = right_schema.index_of(column);

            let is_left = table_info.left_tables.iter().any(|t| {
                t.eq_ignore_ascii_case(&alias_lower) || 
                alias_map.get(&alias_lower).map(|a| a.eq_ignore_ascii_case(t)).unwrap_or(false)
            });
            let is_right = table_info.right_tables.iter().any(|t| {
                t.eq_ignore_ascii_case(&alias_lower) ||
                alias_map.get(&alias_lower).map(|a| a.eq_ignore_ascii_case(t)).unwrap_or(false)
            });

            let idx = match (left_idx, right_idx, is_left, is_right) {
                (Some(l), None, _, _) => l,
                (None, Some(r), _, _) => left_schema.columns.len() + r,
                (Some(l), Some(_r), true, false) => l,
                (Some(_l), Some(r), false, true) => left_schema.columns.len() + r,
                (Some(l), Some(_r), true, true) => {
                    if alias_lower.ends_with("1") || alias_lower.starts_with("l") {
                        l
                    } else if alias_lower.ends_with("2") || alias_lower.starts_with("r") {
                        left_schema.columns.len() + left_schema.index_of(column).unwrap_or(0)
                    } else if alias_lower.starts_with("a") || alias_lower.starts_with("c") {
                        l
                    } else if alias_lower.starts_with("b") || alias_lower.starts_with("p") {
                        left_schema.columns.len() + left_schema.index_of(column).unwrap_or(0)
                    } else {
                        l
                    }
                }
                (Some(l), Some(_r), _, _) => l,
                (None, None, _, _) => {
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
                table_info,
            )?;
            let r = ast_expr_to_logical_join(
                right,
                output_schema,
                left_schema,
                right_schema,
                alias_map,
                table_info,
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
                table_info,
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
                table_info,
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
                table_info,
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
                table_info,
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
                    table_info,
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
