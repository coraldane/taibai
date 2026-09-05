pub mod expr;
pub mod types;

use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

pub use expr::{collect_cols_from_expr, collect_column_refs, contains_correlated_subquery};
pub use types::*;

#[derive(Debug, Clone)]
pub struct TableBinding {
    pub start: usize,
    pub end: usize,
}

impl TableBinding {
    fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }
}

pub type AliasMap = HashMap<String, TableBinding>;

#[derive(Debug, Clone)]
pub struct PlannerContext {
    pub table_schemas: HashMap<String, Schema>,
    /// Outer scope for correlated subquery binding. When present, unqualified
    /// and unresolvable references in the current (inner) scope fall back to
    /// the outer query's tables, resolved against the concatenated schema
    /// prefix `[0, outer_len)`.
    pub outer: Option<OuterScope>,
}

#[derive(Debug, Clone)]
pub struct OuterScope {
    pub outer_len: usize,
    pub schema: Schema,
    pub alias_map: AliasMap,
}

impl PlannerContext {
    pub fn new(table_schemas: HashMap<String, Schema>) -> Self {
        Self {
            table_schemas,
            outer: None,
        }
    }
}

pub fn plan_select(
    stmt: &Statement,
    table_schemas: &HashMap<String, Schema>,
) -> Result<LogicalPlan> {
    let ctx = PlannerContext::new(table_schemas.clone());
    plan_select_with_context(stmt, &ctx)
}

/// Compute the query block's concatenated schema and table alias bindings
/// for a FROM clause, mirroring exactly how `plan_from_clause` assigns column
/// offsets. Used by the execution layer to build the scope in which
/// subqueries are resolved / correlated references are bound.
pub fn build_scope_bindings(
    from: &FromClause,
    table_schemas: &HashMap<String, Schema>,
) -> (Schema, AliasMap) {
    match from {
        FromClause::Table(table_ref) => match table_ref {
            TableRef::Table { name, alias } => {
                let table_name = name.plan_key();
                let mut alias_map = AliasMap::new();
                if let Some(schema) = table_schemas.get(&table_name) {
                    let binding = TableBinding::new(0, schema.columns.len());
                    if let Some(a) = alias {
                        alias_map.insert(a.to_lowercase(), binding.clone());
                    }
                    alias_map.insert(name.name.to_lowercase(), binding.clone());
                    alias_map.insert(table_name.to_lowercase(), binding);
                    (schema.clone(), alias_map)
                } else {
                    (Schema::empty(), alias_map)
                }
            }
            TableRef::Subquery { .. } => (Schema::empty(), AliasMap::new()),
        },
        FromClause::Join(join_expr) => {
            let (mut schema, mut alias_map) = build_scope_bindings(&join_expr.left, table_schemas);
            for join_clause in &join_expr.joins {
                if let TableRef::Table { name, alias } = &join_clause.right {
                    let right_table = name.plan_key();
                    if let Some(right_schema) = table_schemas.get(&right_table) {
                        let right_start = schema.columns.len();
                        let right_end = right_start + right_schema.columns.len();
                        let binding = TableBinding::new(right_start, right_end);
                        if let Some(a) = alias {
                            alias_map.insert(a.to_lowercase(), binding.clone());
                        }
                        alias_map.insert(name.name.to_lowercase(), binding.clone());
                        alias_map.insert(right_table.to_lowercase(), binding);
                        schema = types::merge_schemas(&schema, right_schema);
                    }
                }
            }
            (schema, alias_map)
        }
    }
}

pub fn plan_select_with_context(stmt: &Statement, ctx: &PlannerContext) -> Result<LogicalPlan> {
    let Statement::Select {
        select_list,
        from,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
        distinct,
    } = stmt
    else {
        return Err(TaibaiError::Internal(
            "plan_select called with non-SELECT".into(),
        ));
    };
    let distinct = *distinct;

    let (base_plan, schema, alias_map) = if let Some(from_clause) = from {
        plan_from_clause(from_clause, &ctx.table_schemas)?
    } else {
        (LogicalPlan::Empty, Schema::empty(), AliasMap::new())
    };

    let mut plan: LogicalPlan = base_plan;

    if let Some(pred) = where_clause {
        let predicate = expr::ast_expr_to_logical_with_context(pred, &schema, &alias_map, ctx)?;
        if expr::contains_aggregate(&predicate) {
            return Err(TaibaiError::Sql(
                "aggregate functions are not allowed in WHERE; use HAVING".to_string(),
            ));
        }
        plan = LogicalPlan::Filter(FilterPlan {
            predicate,
            input: Box::new(plan),
        });
    }

    let alias_entries: Vec<(String, Expr)> = select_list
        .iter()
        .filter_map(|item| match item {
            SelectItem::Expr(e, Some(alias)) => Some((alias.to_lowercase(), e.clone())),
            _ => None,
        })
        .collect();

    let having_ast: Option<Expr> = having
        .as_ref()
        .map(|h| expr::substitute_select_aliases(h, &alias_entries, &schema));

    let group_ast: Vec<Expr> = group_by
        .iter()
        .map(|e| {
            expr::substitute_select_aliases(
                &resolve_ordinal_ref(e, select_list),
                &alias_entries,
                &schema,
            )
        })
        .collect();

    let order_ast: Vec<(Expr, bool)> = order_by
        .iter()
        .map(|item| {
            (
                expr::substitute_select_aliases(
                    &resolve_ordinal_ref(&item.expr, select_list),
                    &alias_entries,
                    &schema,
                ),
                item.desc,
            )
        })
        .collect();

    let mut agg_calls: Vec<(AggFunc, Option<Expr>, bool)> = Vec::new();
    for item in select_list {
        if let SelectItem::Expr(e, _) = item {
            expr::collect_aggregate_calls(e, &mut agg_calls)?;
        }
    }
    if let Some(h) = &having_ast {
        expr::collect_aggregate_calls(h, &mut agg_calls)?;
    }
    for (e, _) in &order_ast {
        expr::collect_aggregate_calls(e, &mut agg_calls)?;
    }

    let has_aggregate = !group_ast.is_empty() || !agg_calls.is_empty() || having_ast.is_some();

    let mut original_group: Vec<LogicalExpr> = Vec::new();
    let mut original_aggs: Vec<AggregateExpr> = Vec::new();

    if has_aggregate {
        let group_exprs: Vec<LogicalExpr> = group_ast
            .iter()
            .map(|e| expr::ast_expr_to_logical_with_context(e, &schema, &alias_map, ctx))
            .collect::<Result<Vec<_>>>()?;

        let mut agg_exprs: Vec<AggregateExpr> = Vec::new();
        for (func, arg_expr, agg_distinct) in &agg_calls {
            let arg = match arg_expr {
                Some(e) => Box::new(expr::ast_expr_to_logical_with_context(
                    e, &schema, &alias_map, ctx,
                )?),
                None => Box::new(LogicalExpr::Wildcard),
            };
            let candidate = AggregateExpr {
                func: *func,
                arg,
                distinct: *agg_distinct,
            };
            if !agg_exprs.contains(&candidate) {
                agg_exprs.push(candidate);
            }
        }

        original_group = group_exprs.clone();
        original_aggs = agg_exprs.clone();

        let (agg_input, group_exprs, agg_exprs) =
            materialize_aggregate_inputs(plan, group_exprs, agg_exprs);

        let having_pred = having_ast
            .as_ref()
            .map(|h| {
                let e = expr::ast_expr_to_logical_with_context(h, &schema, &alias_map, ctx)?;
                Ok::<LogicalExpr, TaibaiError>(expr::rewrite_agg_refs(
                    e,
                    &original_group,
                    &original_aggs,
                ))
            })
            .transpose()?;

        plan = LogicalPlan::Aggregate(AggregatePlan {
            group_by: group_exprs,
            aggregates: agg_exprs,
            input: Box::new(agg_input),
            having: None,
            distinct: false,
        });

        if let Some(pred) = having_pred {
            plan = LogicalPlan::Filter(FilterPlan {
                predicate: pred,
                input: Box::new(plan),
            });
        }
    }

    let (mut proj_exprs, mut proj_aliases) =
        expr::resolve_projection_with_alias(select_list, &schema, &alias_map)?;

    if has_aggregate {
        for e in proj_exprs.iter_mut() {
            *e = expr::rewrite_agg_refs(e.clone(), &original_group, &original_aggs);
        }
    }

    let visible_count = proj_exprs.len();

    let mut logical_keys: Vec<(LogicalExpr, bool)> = Vec::new();
    for (e, desc) in &order_ast {
        let mut key = expr::ast_expr_to_logical_with_context(e, &schema, &alias_map, ctx)?;
        if has_aggregate {
            key = expr::rewrite_agg_refs(key, &original_group, &original_aggs);
        }
        logical_keys.push((key, *desc));
    }

    let force_above = distinct
        || logical_keys
            .iter()
            .any(|(key, _)| !matches!(key, LogicalExpr::Column(_)));

    if force_above {
        let mut above_keys: Vec<SortKey> = Vec::with_capacity(logical_keys.len());
        for (key, desc) in logical_keys {
            let position = proj_exprs
                .iter()
                .position(|e| e == &key)
                .unwrap_or(usize::MAX);
            let idx = if position != usize::MAX {
                position
            } else if distinct {
                return Err(TaibaiError::Sql(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in the select list"
                        .to_string(),
                ));
            } else {
                let idx = proj_exprs.len();
                proj_exprs.push(key);
                proj_aliases.push(Some(format!("__orderby_{idx}")));
                idx
            };
            above_keys.push(SortKey {
                expr: LogicalExpr::Column(idx),
                desc,
            });
        }

        plan = LogicalPlan::Project(ProjectPlan {
            expressions: proj_exprs.clone(),
            aliases: proj_aliases.clone(),
            input: Box::new(plan),
        });

        if distinct {
            plan = LogicalPlan::Distinct(DistinctPlan {
                input: Box::new(plan),
            });
        }

        if !above_keys.is_empty() {
            plan = LogicalPlan::Sort(SortPlan {
                sort_keys: above_keys,
                input: Box::new(plan),
            });
        }

        if let Some(limit_expr) = limit {
            let limit_val = types::eval_const_usize(limit_expr)?;
            let offset_val = offset
                .as_ref()
                .map(types::eval_const_usize)
                .transpose()?
                .unwrap_or(0);
            plan = LogicalPlan::Limit(LimitPlan {
                limit: limit_val,
                offset: offset_val,
                input: Box::new(plan),
            });
        }

        if proj_exprs.len() > visible_count {
            plan = LogicalPlan::Project(ProjectPlan {
                expressions: (0..visible_count).map(LogicalExpr::Column).collect(),
                aliases: proj_aliases[..visible_count].to_vec(),
                input: Box::new(plan),
            });
        }
    } else {
        if !logical_keys.is_empty() {
            plan = LogicalPlan::Sort(SortPlan {
                sort_keys: logical_keys
                    .into_iter()
                    .map(|(key, desc)| SortKey { expr: key, desc })
                    .collect(),
                input: Box::new(plan),
            });
        }

        if let Some(limit_expr) = limit {
            let limit_val = types::eval_const_usize(limit_expr)?;
            let offset_val = offset
                .as_ref()
                .map(types::eval_const_usize)
                .transpose()?
                .unwrap_or(0);
            plan = LogicalPlan::Limit(LimitPlan {
                limit: limit_val,
                offset: offset_val,
                input: Box::new(plan),
            });
        }

        plan = LogicalPlan::Project(ProjectPlan {
            expressions: proj_exprs,
            aliases: proj_aliases,
            input: Box::new(plan),
        });
    }

    Ok(plan)
}

/// Resolve `ORDER BY 2` / `GROUP BY 1` style ordinals against the select list.
/// Anything that is not a positive integer literal is returned unchanged.
fn resolve_ordinal_ref(expr: &Expr, select_list: &[SelectItem]) -> Expr {
    if let Expr::Literal(ScalarValue::Int64(Some(value))) = expr {
        let idx = (*value).max(0) as usize;
        if idx >= 1 {
            if let Some(SelectItem::Expr(inner, _)) = select_list.get(idx - 1) {
                return inner.clone();
            }
        }
    }
    expr.clone()
}

/// GROUP BY keys and aggregate arguments must be plain column references for
/// `HashAggregateExec`. Materialize any expression key/argument through an
/// extra projection below the aggregate and rewrite the references.
fn materialize_aggregate_inputs(
    plan: LogicalPlan,
    group_exprs: Vec<LogicalExpr>,
    agg_exprs: Vec<AggregateExpr>,
) -> (LogicalPlan, Vec<LogicalExpr>, Vec<AggregateExpr>) {
    let group_simple = group_exprs
        .iter()
        .all(|e| matches!(e, LogicalExpr::Column(_)));
    let agg_simple = agg_exprs
        .iter()
        .all(|a| matches!(*a.arg, LogicalExpr::Column(_) | LogicalExpr::Wildcard));
    if group_simple && agg_simple {
        return (plan, group_exprs, agg_exprs);
    }

    // Rebuild every index against one projection layout. Mixing original
    // table indices with projected-expression indices would point simple
    // column refs at the wrong (or out-of-range) projected columns.
    let mut expressions: Vec<LogicalExpr> = Vec::new();
    let mut aliases: Vec<Option<String>> = Vec::new();

    let mut new_group = Vec::with_capacity(group_exprs.len());
    for (i, e) in group_exprs.into_iter().enumerate() {
        new_group.push(LogicalExpr::Column(expressions.len()));
        aliases.push(Some(format!("__group_{i}")));
        expressions.push(e);
    }

    let mut new_aggs = Vec::with_capacity(agg_exprs.len());
    for (i, mut agg) in agg_exprs.into_iter().enumerate() {
        if matches!(*agg.arg, LogicalExpr::Wildcard) {
            new_aggs.push(agg);
            continue;
        }
        let logical_arg = *agg.arg;
        agg.arg = Box::new(LogicalExpr::Column(expressions.len()));
        aliases.push(Some(format!("__agg_arg_{i}")));
        expressions.push(logical_arg);
        new_aggs.push(agg);
    }

    let projected = LogicalPlan::Project(ProjectPlan {
        expressions,
        aliases,
        input: Box::new(plan),
    });
    (projected, new_group, new_aggs)
}

fn plan_from_clause(
    from: &FromClause,
    table_schemas: &HashMap<String, Schema>,
) -> Result<(LogicalPlan, Schema, AliasMap)> {
    match from {
        FromClause::Table(table_ref) => match table_ref {
            TableRef::Table { name, alias } => {
                let table_name = name.plan_key();
                let schema = table_schemas
                    .get(&table_name)
                    .ok_or_else(|| TaibaiError::Sql(format!("table '{}' not found", table_name)))?
                    .clone();
                let plan = LogicalPlan::Scan(ScanPlan::new(table_name.clone(), schema.clone()));

                let mut alias_map = AliasMap::new();
                let binding = TableBinding::new(0, schema.columns.len());
                if let Some(a) = alias {
                    alias_map.insert(a.to_lowercase(), binding.clone());
                }
                alias_map.insert(name.name.to_lowercase(), binding.clone());
                alias_map.insert(table_name.to_lowercase(), binding);

                Ok((plan, schema, alias_map))
            }
            TableRef::Subquery { query: _, alias: _ } => Err(TaibaiError::Sql(
                "subquery in FROM not yet supported in planner".into(),
            )),
        },
        FromClause::Join(join_expr) => {
            let (left_plan, left_schema, mut alias_map) =
                plan_from_clause(&join_expr.left, table_schemas)?;

            let mut current_plan = left_plan;
            let mut current_schema = left_schema;

            for join_clause in &join_expr.joins {
                match &join_clause.right {
                    TableRef::Table { name, alias } => {
                        let right_table = name.plan_key();
                        let right_schema = table_schemas
                            .get(&right_table)
                            .ok_or_else(|| {
                                TaibaiError::Sql(format!("table '{}' not found", right_table))
                            })?
                            .clone();

                        let right_start = current_schema.columns.len();
                        let right_end = right_start + right_schema.columns.len();
                        let binding = TableBinding::new(right_start, right_end);
                        if let Some(a) = alias {
                            alias_map.insert(a.to_lowercase(), binding.clone());
                        }
                        alias_map.insert(name.name.to_lowercase(), binding.clone());
                        alias_map.insert(right_table.to_lowercase(), binding);
                        let right_plan = LogicalPlan::Scan(ScanPlan::new(
                            right_table.clone(),
                            right_schema.clone(),
                        ));

                        let output_schema = types::merge_schemas(&current_schema, &right_schema);

                        let condition = match &join_clause.condition {
                            Some(expr) => expr::ast_expr_to_logical_join(
                                expr,
                                &output_schema,
                                &current_schema,
                                &right_schema,
                                &alias_map,
                            )?,
                            None => LogicalExpr::Literal(ScalarValue::Bool(Some(true))),
                        };

                        current_plan = LogicalPlan::Join(JoinPlan {
                            left: Box::new(current_plan),
                            right: Box::new(right_plan),
                            join_type: join_clause.join_type,
                            condition,
                            left_schema: current_schema.clone(),
                            right_schema: right_schema.clone(),
                            output_schema: output_schema.clone(),
                        });
                        current_schema = output_schema;
                    }
                    TableRef::Subquery { query: _, alias: _ } => {
                        return Err(TaibaiError::Sql(
                            "subquery in JOIN not yet supported in planner".into(),
                        ));
                    }
                }
            }

            Ok((current_plan, current_schema, alias_map))
        }
    }
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
