pub mod expr;
pub mod types;

use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

pub use expr::{collect_cols_from_expr, collect_column_refs};
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
}

impl PlannerContext {
    pub fn new(table_schemas: HashMap<String, Schema>) -> Self {
        Self { table_schemas }
    }
}

pub fn plan_select(
    stmt: &Statement,
    table_schemas: &HashMap<String, Schema>,
) -> Result<LogicalPlan> {
    let ctx = PlannerContext::new(table_schemas.clone());
    plan_select_with_context(stmt, &ctx)
}

pub fn plan_select_with_context(stmt: &Statement, ctx: &PlannerContext) -> Result<LogicalPlan> {
    let (select_list, from, where_clause, group_by, having, order_by, limit, offset, _distinct) =
        match stmt {
            Statement::Select {
                select_list,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
                distinct,
            } => (
                select_list,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
                distinct,
            ),
            _ => {
                return Err(TaibaiError::Internal(
                    "plan_select called with non-SELECT".into(),
                ))
            }
        };

    let (base_plan, schema, alias_map) = if let Some(from_clause) = from {
        plan_from_clause(from_clause, &ctx.table_schemas)?
    } else {
        (LogicalPlan::Empty, Schema::empty(), AliasMap::new())
    };

    let mut plan: LogicalPlan = base_plan;

    if let Some(pred) = where_clause {
        let predicate = expr::ast_expr_to_logical_with_context(pred, &schema, &alias_map, ctx)?;
        plan = LogicalPlan::Filter(FilterPlan {
            predicate,
            input: Box::new(plan),
        });
    }

    let agg_funcs = expr::extract_aggregates(select_list);
    if !group_by.is_empty() || !agg_funcs.is_empty() {
        let group_exprs: Vec<LogicalExpr> = group_by
            .iter()
            .map(|e| expr::ast_expr_to_logical_with_context(e, &schema, &alias_map, ctx))
            .collect::<Result<Vec<_>>>()?;

        let agg_exprs: Vec<AggregateExpr> = agg_funcs
            .into_iter()
            .map(|(func, arg_expr, distinct)| {
                let arg = match &arg_expr {
                    Some(e) => Box::new(
                        expr::ast_expr_to_logical_with_context(e, &schema, &alias_map, ctx)
                            .unwrap_or(LogicalExpr::Wildcard),
                    ),
                    None => Box::new(LogicalExpr::Wildcard),
                };
                AggregateExpr {
                    func,
                    arg,
                    distinct,
                }
            })
            .collect();

        let having_expr = having
            .as_ref()
            .map(|h| expr::ast_expr_to_logical_with_context(h, &schema, &alias_map, ctx))
            .transpose()?;

        plan = LogicalPlan::Aggregate(AggregatePlan {
            group_by: group_exprs,
            aggregates: agg_exprs,
            input: Box::new(plan),
            having: having_expr,
            distinct: false,
        });
    }

    if !order_by.is_empty() {
        let select_aliases: Vec<(String, usize)> = select_list
            .iter()
            .enumerate()
            .filter_map(|(i, item)| match item {
                SelectItem::Expr(_, Some(alias)) => Some((alias.to_lowercase(), i)),
                _ => None,
            })
            .collect();

        let sort_keys: Vec<SortKey> = order_by
            .iter()
            .map(|item| -> Result<SortKey> {
                let e = if let Expr::Column(name) = &item.expr {
                    select_aliases
                        .iter()
                        .find(|(alias, _)| alias == &name.to_lowercase())
                        .map(|(_, idx)| LogicalExpr::Column(*idx))
                        .map_or_else(
                            || {
                                expr::ast_expr_to_logical_with_context(
                                    &item.expr, &schema, &alias_map, ctx,
                                )
                            },
                            Ok,
                        )?
                } else {
                    expr::ast_expr_to_logical_with_context(&item.expr, &schema, &alias_map, ctx)?
                };
                Ok(SortKey {
                    expr: e,
                    desc: item.desc,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        plan = LogicalPlan::Sort(SortPlan {
            sort_keys,
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

    let (proj_exprs, aliases) =
        expr::resolve_projection_with_alias(select_list, &schema, &alias_map)?;
    plan = LogicalPlan::Project(ProjectPlan {
        expressions: proj_exprs,
        aliases,
        input: Box::new(plan),
    });

    Ok(plan)
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
