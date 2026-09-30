use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::functions::registry::FunctionRegistry;
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::types::*;
use super::{AliasMap, PlannerContext};

fn aggregate_func(name: &str) -> Option<AggFunc> {
    match name.to_uppercase().as_str() {
        "COUNT" => Some(AggFunc::Count),
        "SUM" => Some(AggFunc::Sum),
        "AVG" => Some(AggFunc::Avg),
        "MIN" => Some(AggFunc::Min),
        "MAX" => Some(AggFunc::Max),
        _ => None,
    }
}

pub fn scalar_function_exists(name: &str) -> bool {
    FunctionRegistry::default().exists(name)
}

fn common_function_type(arg_types: &[DataType]) -> DataType {
    if arg_types.is_empty() {
        return DataType::Int64;
    }
    if arg_types.contains(&DataType::VarChar) {
        return DataType::VarChar;
    }
    if arg_types
        .iter()
        .any(|dt| matches!(dt, DataType::Float32 | DataType::Float64))
    {
        return DataType::Float64;
    }
    if arg_types.contains(&DataType::Decimal) {
        return DataType::Decimal;
    }
    if arg_types.iter().any(|dt| matches!(dt, DataType::UInt64)) {
        return DataType::UInt64;
    }
    if arg_types.iter().any(|dt| {
        matches!(
            dt,
            DataType::Int64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8
        )
    }) {
        return DataType::Int64;
    }
    arg_types[0]
}

pub fn infer_scalar_function_type(name: &str, arg_types: &[DataType]) -> DataType {
    match name.to_lowercase().as_str() {
        "abs" | "ceil" | "ceiling" | "floor" | "round" | "sqrt" | "pow" | "power" | "log"
        | "ln" | "log10" | "log2" | "exp" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan"
        | "atan2" | "sign" | "mod" | "truncate" | "pi" | "vector_distance" | "vector_norm"
        | "vector_dot" => DataType::Float64,
        "length" | "len" | "instr" | "locate" | "year" | "month" | "day" | "hour" | "minute"
        | "second" | "dayofweek" | "dayofyear" | "week" | "quarter" | "to_int" => DataType::Int64,
        "upper" | "ucase" | "lower" | "lcase" | "trim" | "ltrim" | "rtrim" | "substring"
        | "substr" | "concat" | "concat_ws" | "replace" | "reverse" | "left" | "right" | "lpad"
        | "rpad" | "repeat" | "space" | "date_format" | "to_string" => DataType::VarChar,
        "now" | "current_timestamp" => DataType::Timestamp,
        "current_date" | "last_day" => DataType::Date,
        "current_time" => DataType::Time,
        "to_float" => DataType::Float64,
        // cast(value, NULL::<target>, scale): the target is the second arg.
        "cast" => arg_types.get(1).copied().unwrap_or(DataType::Int64),
        "to_bool" => DataType::Bool,
        "vector_normalize" | "vector_add" | "vector_sub" | "vector_scale" => DataType::Vector,
        "coalesce" | "ifnull" | "isnull" => common_function_type(arg_types),
        "nullif" => arg_types.first().copied().unwrap_or(DataType::Int64),
        _ => arg_types.first().copied().unwrap_or(DataType::Int64),
    }
}

pub fn ast_expr_to_logical_with_context(
    expr: &Expr,
    schema: &Schema,
    alias_map: &AliasMap,
    ctx: &PlannerContext,
) -> Result<LogicalExpr> {
    match expr {
        Expr::Literal(v) => Ok(LogicalExpr::Literal(v.clone())),
        Expr::Column(name) => {
            let idx = resolve_column_index_with_outer(name, schema, ctx)?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            let idx = resolve_qualified_column_index(
                table,
                column,
                schema,
                alias_map,
                ctx.outer.as_ref(),
            )?;
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
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Ok(LogicalExpr::Case {
            operand: operand
                .as_deref()
                .map(|expr| ast_expr_to_logical_with_context(expr, schema, alias_map, ctx))
                .transpose()?
                .map(Box::new),
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        ast_expr_to_logical_with_context(when_expr, schema, alias_map, ctx)?,
                        ast_expr_to_logical_with_context(then_expr, schema, alias_map, ctx)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|expr| ast_expr_to_logical_with_context(expr, schema, alias_map, ctx))
                .transpose()?
                .map(Box::new),
        }),
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
            if let Some(func) = aggregate_func(name) {
                let arg = if args.is_empty()
                    || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
                {
                    LogicalExpr::Wildcard
                } else {
                    ast_expr_to_logical_with_context(&args[0], schema, alias_map, ctx)?
                };
                return Ok(LogicalExpr::Aggregate(AggregateExpr {
                    func,
                    arg: Box::new(arg),
                    distinct: *distinct,
                }));
            }
            if *distinct {
                return Err(TaibaiError::Sql(format!(
                    "DISTINCT is not supported for scalar function '{}'",
                    name
                )));
            }
            if !scalar_function_exists(name) {
                return Err(TaibaiError::Sql(format!("unknown function '{}'", name)));
            }
            let bound_args = args
                .iter()
                .map(|arg| ast_expr_to_logical_with_context(arg, schema, alias_map, ctx))
                .collect::<Result<Vec<_>>>()?;
            Ok(LogicalExpr::Function {
                name: name.clone(),
                args: bound_args,
            })
        }
        Expr::Subquery(subquery_stmt) => {
            use super::plan_select_with_context;
            let mut inner_ctx = ctx.clone();
            inner_ctx.outer = None;
            match plan_select_with_context(subquery_stmt, &inner_ctx) {
                Ok(subquery_plan) => Ok(LogicalExpr::ScalarSubquery(Box::new(subquery_plan))),
                Err(e) if is_correlation_binding_error(&e) => Err(TaibaiError::Sql(format!(
                    "subquery could not be resolved: {e}. Correlated references are only \
                     supported in top-level WHERE conjuncts (EXISTS / NOT EXISTS / IN (SELECT ...)); \
                     outer columns must be qualified with their table alias"
                ))),
                Err(e) => Err(e),
            }
        }
        Expr::Exists(inner) => match inner.as_ref() {
            Expr::Subquery(subquery_stmt) => {
                use super::plan_select_with_context;
                let mut inner_ctx = ctx.clone();
                inner_ctx.outer = None;
                match plan_select_with_context(subquery_stmt, &inner_ctx) {
                    Ok(subquery_plan) => Ok(LogicalExpr::ExistsSubquery(Box::new(subquery_plan))),
                    Err(e) if is_correlation_binding_error(&e) => Err(TaibaiError::Sql(
                        "correlated EXISTS is only supported as a top-level WHERE conjunct \
                         (AND-separated), not inside OR / SELECT list / HAVING"
                            .into(),
                    )),
                    Err(e) => Err(e),
                }
            }
            _ => Err(TaibaiError::Sql("EXISTS requires a subquery".into())),
        },
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
                Err(TaibaiError::Sql(
                    "IN (SELECT ...) is only supported as an uncorrelated subquery or as a \
                     top-level WHERE conjunct (AND-separated)"
                        .into(),
                ))
            }
        }
        Expr::Correlated(corr) => {
            let lhs = corr
                .lhs
                .as_deref()
                .map(|e| ast_expr_to_logical_with_context(e, schema, alias_map, ctx))
                .transpose()?
                .map(Box::new);
            Ok(LogicalExpr::CorrelatedSubquery(Box::new(
                CorrelatedSubqueryPlan {
                    id: corr.id,
                    kind: corr.kind,
                    lhs,
                    stmt: (*corr.stmt).clone(),
                    conjuncts: corr.conjuncts.clone(),
                    params: corr.params.clone(),
                    result_type: corr.result_type,
                },
            )))
        }
        Expr::WindowFunction { .. } => Err(TaibaiError::Sql(
            "window functions are only supported as top-level SELECT expressions".to_string(),
        )),
        _ => Err(TaibaiError::Sql("unsupported expression type".to_string())),
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
                .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            let idx = resolve_qualified_column_index(table, column, schema, alias_map, None)?;
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
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Ok(LogicalExpr::Case {
            operand: operand
                .as_deref()
                .map(|expr| ast_expr_to_logical(expr, schema, alias_map))
                .transpose()?
                .map(Box::new),
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        ast_expr_to_logical(when_expr, schema, alias_map)?,
                        ast_expr_to_logical(then_expr, schema, alias_map)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|expr| ast_expr_to_logical(expr, schema, alias_map))
                .transpose()?
                .map(Box::new),
        }),
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
            if let Some(func) = aggregate_func(name) {
                let arg = if args.is_empty()
                    || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
                {
                    LogicalExpr::Wildcard
                } else {
                    ast_expr_to_logical(&args[0], schema, alias_map)?
                };
                return Ok(LogicalExpr::Aggregate(AggregateExpr {
                    func,
                    arg: Box::new(arg),
                    distinct: *distinct,
                }));
            }
            if *distinct {
                return Err(TaibaiError::Sql(format!(
                    "DISTINCT is not supported for scalar function '{}'",
                    name
                )));
            }
            if !scalar_function_exists(name) {
                return Err(TaibaiError::Sql(format!("unknown function '{}'", name)));
            }
            let bound_args = args
                .iter()
                .map(|arg| ast_expr_to_logical(arg, schema, alias_map))
                .collect::<Result<Vec<_>>>()?;
            Ok(LogicalExpr::Function {
                name: name.clone(),
                args: bound_args,
            })
        }
        Expr::Correlated(corr) => {
            let lhs = corr
                .lhs
                .as_deref()
                .map(|e| ast_expr_to_logical(e, schema, alias_map))
                .transpose()?
                .map(Box::new);
            Ok(LogicalExpr::CorrelatedSubquery(Box::new(
                CorrelatedSubqueryPlan {
                    id: corr.id,
                    kind: corr.kind,
                    lhs,
                    stmt: (*corr.stmt).clone(),
                    conjuncts: corr.conjuncts.clone(),
                    params: corr.params.clone(),
                    result_type: corr.result_type,
                },
            )))
        }
        Expr::WindowFunction { .. } => Err(TaibaiError::Sql(
            "window functions are only supported as top-level SELECT expressions".to_string(),
        )),
        _ => Err(TaibaiError::Sql("unsupported expression type".to_string())),
    }
}

fn resolve_qualified_column_index(
    table: &str,
    column: &str,
    schema: &Schema,
    alias_map: &AliasMap,
    outer: Option<&crate::sql::planner::OuterScope>,
) -> Result<usize> {
    let table_lower = table.to_lowercase();
    if let Some(binding) = alias_map.get(&table_lower) {
        for idx in binding.start..binding.end {
            if schema
                .columns
                .get(idx)
                .map(|c| c.name.eq_ignore_ascii_case(column))
                .unwrap_or(false)
            {
                return Ok(idx);
            }
        }
        return Err(TaibaiError::Sql(format!(
            "column '{}' not found in table '{}'",
            column, table
        )));
    }

    if let Some(outer) = outer {
        if let Some(binding) = outer.alias_map.get(&table_lower) {
            for idx in binding.start..binding.end {
                if schema
                    .columns
                    .get(idx)
                    .map(|c| c.name.eq_ignore_ascii_case(column))
                    .unwrap_or(false)
                {
                    return Ok(idx);
                }
            }
            return Err(TaibaiError::Sql(format!(
                "column '{}' not found in table '{}'",
                column, table
            )));
        }
    }

    Err(TaibaiError::Sql(format!(
        "unknown table or alias '{}' (correlated references are only supported in top-level WHERE conjuncts)",
        table
    )))
}

fn resolve_column_index_with_outer(
    name: &str,
    schema: &Schema,
    ctx: &crate::sql::planner::PlannerContext,
) -> Result<usize> {
    if let Some(outer) = &ctx.outer {
        let outer_len = outer.outer_len;
        if let Some(idx) = schema.columns[outer_len.min(schema.columns.len())..]
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Ok(outer_len + idx);
        }
        if let Some(idx) = schema.columns[..outer_len.min(schema.columns.len())]
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
        {
            return Ok(idx);
        }
        return Err(TaibaiError::Sql(format!("column '{name}' not found")));
    }
    schema
        .index_of(name)
        .ok_or_else(|| TaibaiError::Sql(format!("column '{name}' not found")))
}

pub fn is_correlation_binding_error(err: &TaibaiError) -> bool {
    let msg = err.to_string();
    msg.contains("unknown table or alias")
        || (msg.contains("column '") && msg.contains("not found"))
}

#[allow(clippy::only_used_in_recursion)]
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
                .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            let idx =
                resolve_qualified_column_index(table, column, output_schema, alias_map, None)?;
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
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Ok(LogicalExpr::Case {
            operand: operand
                .as_deref()
                .map(|expr| {
                    ast_expr_to_logical_join(
                        expr,
                        output_schema,
                        left_schema,
                        right_schema,
                        alias_map,
                    )
                })
                .transpose()?
                .map(Box::new),
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        ast_expr_to_logical_join(
                            when_expr,
                            output_schema,
                            left_schema,
                            right_schema,
                            alias_map,
                        )?,
                        ast_expr_to_logical_join(
                            then_expr,
                            output_schema,
                            left_schema,
                            right_schema,
                            alias_map,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_deref()
                .map(|expr| {
                    ast_expr_to_logical_join(
                        expr,
                        output_schema,
                        left_schema,
                        right_schema,
                        alias_map,
                    )
                })
                .transpose()?
                .map(Box::new),
        }),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if let Some(func) = aggregate_func(name) {
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
                return Ok(LogicalExpr::Aggregate(AggregateExpr {
                    func,
                    arg: Box::new(arg),
                    distinct: *distinct,
                }));
            }
            if *distinct {
                return Err(TaibaiError::Sql(format!(
                    "DISTINCT is not supported for scalar function '{}'",
                    name
                )));
            }
            if !scalar_function_exists(name) {
                return Err(TaibaiError::Sql(format!("unknown function '{}'", name)));
            }
            let bound_args = args
                .iter()
                .map(|arg| {
                    ast_expr_to_logical_join(
                        arg,
                        output_schema,
                        left_schema,
                        right_schema,
                        alias_map,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(LogicalExpr::Function {
                name: name.clone(),
                args: bound_args,
            })
        }
        _ => Err(TaibaiError::Sql(
            "unsupported expression type in join context".to_string(),
        )),
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

/// Recursively collect aggregate calls from an arbitrary expression tree
/// (select items, HAVING, ORDER BY). Errors on window functions so they fail
/// loudly instead of silently producing NULL columns.
pub fn collect_aggregate_calls(
    expr: &Expr,
    aggs: &mut Vec<(AggFunc, Option<Expr>, bool)>,
) -> Result<()> {
    match expr {
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if aggregate_func(name).is_some() {
                let arg = if args.is_empty()
                    || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
                {
                    None
                } else {
                    Some(args[0].clone())
                };
                aggs.push((aggregate_func(name).unwrap(), arg, *distinct));
                return Ok(());
            }
            for arg in args {
                collect_aggregate_calls(arg, aggs)?;
            }
        }
        // Window functions are planned separately; their arguments must not
        // register as plain aggregates.
        Expr::WindowFunction { .. } => {}
        Expr::BinaryOp { left, right, .. } => {
            collect_aggregate_calls(left, aggs)?;
            collect_aggregate_calls(right, aggs)?;
        }
        Expr::UnaryNot(inner) | Expr::UnaryMinus(inner) => {
            collect_aggregate_calls(inner, aggs)?;
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_aggregate_calls(inner, aggs)?;
        }
        Expr::In { expr, values, .. } => {
            collect_aggregate_calls(expr, aggs)?;
            for value in values {
                collect_aggregate_calls(value, aggs)?;
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_aggregate_calls(expr, aggs)?;
            collect_aggregate_calls(low, aggs)?;
            collect_aggregate_calls(high, aggs)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_aggregate_calls(operand, aggs)?;
            }
            for (when_expr, then_expr) in when_clauses {
                collect_aggregate_calls(when_expr, aggs)?;
                collect_aggregate_calls(then_expr, aggs)?;
            }
            if let Some(else_expr) = else_clause {
                collect_aggregate_calls(else_expr, aggs)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_aggregate_calls(expr, aggs)?;
            collect_aggregate_calls(pattern, aggs)?;
        }
        Expr::Exists(inner) => collect_aggregate_calls(inner, aggs)?,
        Expr::Correlated(_) => {
            // The inner statement belongs to the subquery's own block; its
            // aggregates must not leak into the outer aggregate set.
        }
        Expr::Subquery(stmt) => {
            if let Statement::Select { select_list, .. } = stmt.as_ref() {
                for item in select_list {
                    if let SelectItem::Expr(inner, _) = item {
                        collect_aggregate_calls(inner, aggs)?;
                    }
                }
            }
        }
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => {}
    }
    Ok(())
}

/// Replace bare column references that match a select-list alias (and do not
/// resolve against the input schema) with the aliased expression. Used for
/// HAVING / ORDER BY / GROUP BY alias references.
pub fn substitute_select_aliases(expr: &Expr, aliases: &[(String, Expr)], schema: &Schema) -> Expr {
    match expr {
        Expr::Column(name) => {
            if schema.index_of(name).is_some() {
                return expr.clone();
            }
            let lower = name.to_lowercase();
            match aliases.iter().find(|(alias, _)| *alias == lower) {
                Some((_, replacement)) => replacement.clone(),
                None => expr.clone(),
            }
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_select_aliases(left, aliases, schema)),
            op: *op,
            right: Box::new(substitute_select_aliases(right, aliases, schema)),
        },
        Expr::UnaryNot(inner) => {
            Expr::UnaryNot(Box::new(substitute_select_aliases(inner, aliases, schema)))
        }
        Expr::UnaryMinus(inner) => {
            Expr::UnaryMinus(Box::new(substitute_select_aliases(inner, aliases, schema)))
        }
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(substitute_select_aliases(inner, aliases, schema)))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(substitute_select_aliases(inner, aliases, schema)))
        }
        Expr::In {
            expr,
            values,
            negated,
        } => Expr::In {
            expr: Box::new(substitute_select_aliases(expr, aliases, schema)),
            values: values
                .iter()
                .map(|v| substitute_select_aliases(v, aliases, schema))
                .collect(),
            negated: *negated,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(substitute_select_aliases(expr, aliases, schema)),
            low: Box::new(substitute_select_aliases(low, aliases, schema)),
            high: Box::new(substitute_select_aliases(high, aliases, schema)),
            negated: *negated,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(substitute_select_aliases(e, aliases, schema))),
            when_clauses: when_clauses
                .iter()
                .map(|(w, t)| {
                    (
                        substitute_select_aliases(w, aliases, schema),
                        substitute_select_aliases(t, aliases, schema),
                    )
                })
                .collect(),
            else_clause: else_clause
                .as_ref()
                .map(|e| Box::new(substitute_select_aliases(e, aliases, schema))),
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(substitute_select_aliases(expr, aliases, schema)),
            pattern: Box::new(substitute_select_aliases(pattern, aliases, schema)),
            negated: *negated,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_select_aliases(a, aliases, schema))
                .collect(),
            distinct: *distinct,
        },
        other => other.clone(),
    }
}

/// After an Aggregate node is planned, references in HAVING / ORDER BY /
/// projection must point at the aggregate OUTPUT layout:
///   [group keys..., aggregate results...]
/// - `Column(i)` matching a GROUP BY key `Column(i)` becomes `Column(group_pos)`.
/// - `Aggregate(a)` matching a planned aggregate becomes `Column(n_groups + k)`.
pub fn rewrite_agg_refs(
    expr: LogicalExpr,
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
) -> LogicalExpr {
    if let Some(position) = group_by.iter().position(|g| g == &expr) {
        return LogicalExpr::Column(position);
    }
    match expr {
        LogicalExpr::Column(_) => {
            if let Some(position) = group_by.iter().position(|g| g == &expr) {
                LogicalExpr::Column(position)
            } else {
                expr
            }
        }
        LogicalExpr::Aggregate(ref agg) => {
            if let Some(k) = aggregates.iter().position(|a| a == agg) {
                LogicalExpr::Column(group_by.len() + k)
            } else {
                expr
            }
        }
        LogicalExpr::BinaryOp { left, op, right } => LogicalExpr::BinaryOp {
            left: Box::new(rewrite_agg_refs(*left, group_by, aggregates)),
            op,
            right: Box::new(rewrite_agg_refs(*right, group_by, aggregates)),
        },
        LogicalExpr::Not(inner) => {
            LogicalExpr::Not(Box::new(rewrite_agg_refs(*inner, group_by, aggregates)))
        }
        LogicalExpr::IsNull(inner) => {
            LogicalExpr::IsNull(Box::new(rewrite_agg_refs(*inner, group_by, aggregates)))
        }
        LogicalExpr::IsNotNull(inner) => {
            LogicalExpr::IsNotNull(Box::new(rewrite_agg_refs(*inner, group_by, aggregates)))
        }
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => LogicalExpr::In {
            expr: Box::new(rewrite_agg_refs(*expr, group_by, aggregates)),
            values,
            negated,
        },
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => LogicalExpr::Between {
            expr: Box::new(rewrite_agg_refs(*expr, group_by, aggregates)),
            low: Box::new(rewrite_agg_refs(*low, group_by, aggregates)),
            high: Box::new(rewrite_agg_refs(*high, group_by, aggregates)),
            negated,
        },
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => LogicalExpr::Case {
            operand: operand.map(|e| Box::new(rewrite_agg_refs(*e, group_by, aggregates))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(w, t)| {
                    (
                        rewrite_agg_refs(w, group_by, aggregates),
                        rewrite_agg_refs(t, group_by, aggregates),
                    )
                })
                .collect(),
            else_clause: else_clause.map(|e| Box::new(rewrite_agg_refs(*e, group_by, aggregates))),
        },
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => LogicalExpr::Like {
            expr: Box::new(rewrite_agg_refs(*expr, group_by, aggregates)),
            pattern,
            negated,
        },
        LogicalExpr::Function { name, args } => LogicalExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|a| rewrite_agg_refs(a, group_by, aggregates))
                .collect(),
        },
        other => other,
    }
}

/// True when the logical expression contains an aggregate call.
pub fn contains_aggregate(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::Aggregate(_) => true,
        LogicalExpr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            contains_aggregate(inner)
        }
        LogicalExpr::In { expr, .. } => contains_aggregate(expr),
        LogicalExpr::Between {
            expr, low, high, ..
        } => contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high),
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(contains_aggregate)
                || when_clauses
                    .iter()
                    .any(|(w, t)| contains_aggregate(w) || contains_aggregate(t))
                || else_clause.as_deref().is_some_and(contains_aggregate)
        }
        LogicalExpr::Like { expr, .. } => contains_aggregate(expr),
        LogicalExpr::Function { args, .. } => args.iter().any(contains_aggregate),
        _ => false,
    }
}

fn extract_aggs_from_expr(expr: &Expr, aggs: &mut Vec<(AggFunc, Option<Expr>, bool)>) {
    if let Expr::Function {
        name,
        args,
        distinct,
    } = expr
    {
        let func = aggregate_func(name);
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

pub fn expr_to_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::Literal(v) => format!("{:?}", v),
        Expr::QualifiedColumn { column, .. } => column.clone(),
        Expr::BinaryOp { left, op, right } => {
            let l = expr_to_name(left);
            let r = expr_to_name(right);
            let op_str = match op {
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                _ => "?",
            };
            format!("{} {} {}", l, op_str, r)
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            let arg_str = if args.is_empty()
                || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
            {
                "*".to_string()
            } else {
                args.iter().map(expr_to_name).collect::<Vec<_>>().join(", ")
            };
            if *distinct {
                format!("{}(distinct {})", name.to_lowercase(), arg_str)
            } else {
                format!("{}({})", name.to_lowercase(), arg_str)
            }
        }
        Expr::UnaryNot(inner) => format!("not {}", expr_to_name(inner)),
        Expr::UnaryMinus(inner) => format!("-{}", expr_to_name(inner)),
        Expr::IsNull(inner) => format!("{} is null", expr_to_name(inner)),
        Expr::IsNotNull(inner) => format!("{} is not null", expr_to_name(inner)),
        _ => "expr".to_string(),
    }
}

pub fn resolve_projection_with_alias(
    select_list: &[SelectItem],
    schema: &Schema,
    alias_map: &AliasMap,
) -> Result<(Vec<LogicalExpr>, Vec<Option<String>>)> {
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
                let logical = ast_expr_to_logical(expr, schema, alias_map)?;
                exprs.push(logical);
                let auto_alias = alias.clone().or_else(|| Some(expr_to_name(expr)));
                aliases.push(auto_alias);
            }
        }
    }

    Ok((exprs, aliases))
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
        LogicalExpr::Function { args, .. } => {
            for arg in args {
                collect_cols_from_expr(arg, cols);
            }
        }
        LogicalExpr::In { expr, .. } => collect_cols_from_expr(expr, cols),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_cols_from_expr(expr, cols);
            collect_cols_from_expr(low, cols);
            collect_cols_from_expr(high, cols);
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_cols_from_expr(operand, cols);
            }
            for (when_expr, then_expr) in when_clauses {
                collect_cols_from_expr(when_expr, cols);
                collect_cols_from_expr(then_expr, cols);
            }
            if let Some(else_expr) = else_clause {
                collect_cols_from_expr(else_expr, cols);
            }
        }
        LogicalExpr::Like { expr, .. } => collect_cols_from_expr(expr, cols),
        LogicalExpr::CorrelatedSubquery(corr) => {
            // Outer columns feeding the subquery must survive column pruning.
            for (_, outer_idx) in &corr.params {
                cols.push(*outer_idx);
            }
            if let Some(lhs) = &corr.lhs {
                collect_cols_from_expr(lhs, cols);
            }
        }
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
        LogicalExpr::BinaryOp { left, op, right } => {
            if matches!(
                op,
                BinOp::Eq
                    | BinOp::Neq
                    | BinOp::Lt
                    | BinOp::Gt
                    | BinOp::Lte
                    | BinOp::Gte
                    | BinOp::And
                    | BinOp::Or
            ) {
                return DataType::Bool;
            }
            let left_type = infer_type(left, schema);
            let right_type = infer_type(right, schema);
            if matches!(op, BinOp::Div) {
                return if matches!(left_type, DataType::Decimal)
                    || matches!(right_type, DataType::Decimal)
                {
                    DataType::Decimal
                } else {
                    DataType::Float64
                };
            }
            crate::sql::binder::type_resolver::common_type(&left_type, &right_type)
        }
        LogicalExpr::Aggregate(agg) => match agg.func {
            AggFunc::Count => DataType::Int64,
            _ => DataType::Float64,
        },
        LogicalExpr::Function { name, args } => {
            let arg_types = args
                .iter()
                .map(|arg| infer_type(arg, schema))
                .collect::<Vec<_>>();
            infer_scalar_function_type(name, &arg_types)
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
        LogicalExpr::CorrelatedSubquery(corr) => corr.result_type,
        _ => DataType::Int64,
    }
}

/// True when the logical expression contains a correlated subquery node.
/// Such predicates must stay with the operator that owns the subquery
/// registry (FilterExec / ProjectExec); they can neither be pushed into
/// scans nor evaluated by join/aggregate condition code.
pub fn contains_correlated_subquery(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::CorrelatedSubquery(_) => true,
        LogicalExpr::BinaryOp { left, right, .. } => {
            contains_correlated_subquery(left) || contains_correlated_subquery(right)
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            contains_correlated_subquery(inner)
        }
        LogicalExpr::In { expr, .. } => contains_correlated_subquery(expr),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            contains_correlated_subquery(expr)
                || contains_correlated_subquery(low)
                || contains_correlated_subquery(high)
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(contains_correlated_subquery)
                || when_clauses.iter().any(|(w, t)| {
                    contains_correlated_subquery(w) || contains_correlated_subquery(t)
                })
                || else_clause
                    .as_deref()
                    .is_some_and(contains_correlated_subquery)
        }
        LogicalExpr::Like { expr, .. } => contains_correlated_subquery(expr),
        LogicalExpr::Function { args, .. } => args.iter().any(contains_correlated_subquery),
        LogicalExpr::Aggregate(agg) => contains_correlated_subquery(&agg.arg),
        _ => false,
    }
}

/// True when the expression tree contains any window function call.
pub fn contains_window_function(expr: &Expr) -> bool {
    match expr {
        Expr::WindowFunction { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        Expr::UnaryNot(inner) | Expr::UnaryMinus(inner) => contains_window_function(inner),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => contains_window_function(inner),
        Expr::In { expr, .. } => contains_window_function(expr),
        Expr::Between { expr, .. } => contains_window_function(expr),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| contains_window_function(e))
                .unwrap_or(false)
                || when_clauses
                    .iter()
                    .any(|(w, t)| contains_window_function(w) || contains_window_function(t))
                || else_clause
                    .as_ref()
                    .map(|e| contains_window_function(e))
                    .unwrap_or(false)
        }
        Expr::Function { args, .. } => args.iter().any(contains_window_function),
        _ => false,
    }
}

/// Convert one parsed window function call into its planned form with a
/// canonical output column name (used for projection references and dedup).
pub fn window_function_to_plan(
    func: &WindowFunc,
    partition_by: &[Expr],
    order_by: &[OrderByItem],
    frame: Option<&WindowFrame>,
    schema: &Schema,
    alias_map: &AliasMap,
    ctx: &PlannerContext,
) -> Result<WindowFuncPlan> {
    let to_logical = |e: &Expr| ast_expr_to_logical_with_context(e, schema, alias_map, ctx);

    let kind = match func {
        WindowFunc::RowNumber => WindowFuncKind::RowNumber,
        WindowFunc::Rank => WindowFuncKind::Rank,
        WindowFunc::DenseRank => WindowFuncKind::DenseRank,
        WindowFunc::Lead {
            arg,
            offset,
            default,
        } => WindowFuncKind::Lead {
            arg: Box::new(to_logical(arg)?),
            offset: *offset,
            default: default
                .as_ref()
                .map(|d| to_logical(d))
                .transpose()?
                .map(Box::new),
        },
        WindowFunc::Lag {
            arg,
            offset,
            default,
        } => WindowFuncKind::Lag {
            arg: Box::new(to_logical(arg)?),
            offset: *offset,
            default: default
                .as_ref()
                .map(|d| to_logical(d))
                .transpose()?
                .map(Box::new),
        },
        WindowFunc::Aggregate {
            func: name,
            args,
            distinct,
        } => {
            let agg_func = aggregate_func(name).ok_or_else(|| {
                TaibaiError::Sql(format!("unsupported window aggregate function '{name}'"))
            })?;
            if args.len() > 1 {
                return Err(TaibaiError::Sql(format!(
                    "window aggregate '{name}' takes at most one argument"
                )));
            }
            let arg = match args.first() {
                // COUNT(*) parses `*` as a bare column name.
                Some(Expr::Column(name)) if name == "*" => None,
                None => None,
                Some(e) => Some(Box::new(to_logical(e)?)),
            };
            WindowFuncKind::Aggregate {
                func: agg_func,
                arg,
                distinct: *distinct,
            }
        }
    };

    let partition_names: Vec<String> = partition_by.iter().map(expr_to_name).collect();
    let order_names: Vec<String> = order_by
        .iter()
        .map(|item| expr_to_name(&item.expr))
        .collect();
    let partition_by = partition_by
        .iter()
        .map(to_logical)
        .collect::<Result<Vec<_>>>()?;
    let order_by = order_by
        .iter()
        .map(|item| {
            Ok(SortKey {
                expr: to_logical(&item.expr)?,
                desc: item.desc,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut output_name = String::new();
    match func {
        WindowFunc::RowNumber => output_name.push_str("ROW_NUMBER()"),
        WindowFunc::Rank => output_name.push_str("RANK()"),
        WindowFunc::DenseRank => output_name.push_str("DENSE_RANK()"),
        WindowFunc::Lead { .. } => output_name.push_str("LEAD()"),
        WindowFunc::Lag { .. } => output_name.push_str("LAG()"),
        WindowFunc::Aggregate {
            func,
            args,
            distinct,
        } => {
            output_name.push_str(&func.to_uppercase());
            output_name.push('(');
            if *distinct {
                output_name.push_str("DISTINCT ");
            }
            output_name.push_str(&args.iter().map(expr_to_name).collect::<Vec<_>>().join(", "));
            output_name.push(')');
        }
    }
    output_name.push_str(" OVER (");
    if !partition_by.is_empty() {
        output_name.push_str("PARTITION BY ");
        output_name.push_str(&partition_names.join(", "));
    }
    if !order_by.is_empty() {
        if !partition_by.is_empty() {
            output_name.push(' ');
        }
        output_name.push_str("ORDER BY ");
        output_name.push_str(&order_names.join(", "));
    }
    output_name.push(')');

    Ok(WindowFuncPlan {
        kind,
        partition_by,
        order_by,
        frame: frame.cloned(),
        output_name,
    })
}
