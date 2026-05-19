pub mod types;
pub mod expr;

use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

pub use types::*;
pub use expr::{ast_expr_to_logical, ast_expr_to_logical_join, collect_column_refs, collect_cols_from_expr, infer_type};

pub fn plan_select(
    stmt: &Statement,
    table_schemas: &HashMap<String, Schema>,
) -> Result<LogicalPlan> {
    let (select_list, from, where_clause, group_by, _having, order_by, limit, offset, _distinct) = match stmt {
        Statement::Select { select_list, from, where_clause, group_by, having, order_by, limit, offset, distinct } => {
            (select_list, from, where_clause, group_by, having, order_by, limit, offset, distinct)
        }
        _ => return Err(MonaxError::Internal("plan_select called with non-SELECT".into())),
    };

    let (base_plan, schema) = if let Some(from_clause) = from {
        plan_from_clause(from_clause, table_schemas)?
    } else {
        (LogicalPlan::Empty, Schema::empty())
    };

    let mut plan: LogicalPlan = base_plan;

    if let Some(pred) = where_clause {
        let predicate = expr::ast_expr_to_logical(pred, &schema)?;
        plan = LogicalPlan::Filter(FilterPlan {
            predicate,
            input: Box::new(plan),
        });
    }

    let agg_funcs = expr::extract_aggregates(select_list);
    if !group_by.is_empty() || !agg_funcs.is_empty() {
        let group_exprs: Vec<LogicalExpr> = group_by.iter()
            .map(|e| expr::ast_expr_to_logical(e, &schema))
            .collect::<Result<Vec<_>>>()?;

        let agg_exprs: Vec<AggregateExpr> = agg_funcs.into_iter().map(|(func, arg_expr, distinct)| {
            let arg = match &arg_expr {
                Some(e) => Box::new(expr::ast_expr_to_logical(e, &schema).unwrap_or(LogicalExpr::Wildcard)),
                None => Box::new(LogicalExpr::Wildcard),
            };
            AggregateExpr { func, arg, distinct }
        }).collect();

        plan = LogicalPlan::Aggregate(AggregatePlan {
            group_by: group_exprs,
            aggregates: agg_exprs,
            input: Box::new(plan),
            having: None,
            distinct: false,
        });
    }

    if !order_by.is_empty() {
        let sort_keys: Vec<SortKey> = order_by.iter()
            .map(|item| -> Result<SortKey> {
                let e = expr::ast_expr_to_logical(&item.expr, &schema)?;
                Ok(SortKey { expr: e, desc: item.desc })
            })
            .collect::<Result<Vec<_>>>()?;

        plan = LogicalPlan::Sort(SortPlan {
            sort_keys,
            input: Box::new(plan),
        });
    }

    if let Some(limit_expr) = limit {
        let limit_val = types::eval_const_usize(limit_expr)?;
        let offset_val = offset.as_ref().map(types::eval_const_usize).transpose()?.unwrap_or(0);
        plan = LogicalPlan::Limit(LimitPlan {
            limit: limit_val,
            offset: offset_val,
            input: Box::new(plan),
        });
    }

    let (proj_exprs, aliases) = expr::resolve_projection(select_list, &schema);
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
) -> Result<(LogicalPlan, Schema)> {
    match from {
        FromClause::Table(table_ref) => {
            let table_name = &table_ref.name;
            let schema = table_schemas.get(table_name)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}' not found", table_name)))?
                .clone();
            let plan = LogicalPlan::Scan(ScanPlan::new(table_name.clone(), schema.clone()));
            Ok((plan, schema))
        }
        FromClause::Join(join_expr) => {
            let (left_plan, left_schema) = plan_from_clause(&join_expr.left, table_schemas)?;

            let mut current_plan = left_plan;
            let mut current_schema = left_schema;

            for join_clause in &join_expr.joins {
                let right_table = &join_clause.right.name;
                let right_schema = table_schemas.get(right_table)
                    .ok_or_else(|| MonaxError::Sql(format!("table '{}' not found", right_table)))?
                    .clone();

                let right_plan = LogicalPlan::Scan(ScanPlan::new(right_table.clone(), right_schema.clone()));

                let output_schema = types::merge_schemas(&current_schema, &right_schema);

                let condition = match &join_clause.condition {
                    Some(expr) => expr::ast_expr_to_logical_join(expr, &output_schema, &current_schema, &right_schema)?,
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

            Ok((current_plan, current_schema))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::parser::Parser;
    use crate::types::batch::ColumnDef;

    fn make_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
            ColumnDef::new("age", DataType::Int32),
        ])
    }

    #[test]
    fn test_plan_select_star() {
        let schema = make_schema();
        let mut schemas = std::collections::HashMap::new();
        schemas.insert("users".into(), schema);

        let stmts = Parser::parse("SELECT * FROM users").unwrap();
        let plan = plan_select(&stmts[0], &schemas).unwrap();

        match &plan {
            LogicalPlan::Project(p) => {
                assert_eq!(p.expressions.len(), 3);
                assert!(matches!(&*p.input, LogicalPlan::Scan(_)));
            }
            _ => panic!("expected Project at root"),
        }
    }

    #[test]
    fn test_plan_select_with_filter() {
        let schema = make_schema();
        let mut schemas = std::collections::HashMap::new();
        schemas.insert("users".into(), schema);

        let stmts = Parser::parse("SELECT id FROM users WHERE age > 30").unwrap();
        let plan = plan_select(&stmts[0], &schemas).unwrap();

        match &plan {
            LogicalPlan::Project(p) => match &*p.input {
                LogicalPlan::Filter(f) => {
                    assert!(matches!(&*f.input, LogicalPlan::Scan(_)));
                }
                _ => panic!("expected Filter"),
            },
            _ => panic!("expected Project"),
        }
    }

    #[test]
    fn test_plan_select_with_limit_order() {
        let schema = make_schema();
        let mut schemas = std::collections::HashMap::new();
        schemas.insert("users".into(), schema);

        let stmts = Parser::parse("SELECT * FROM users ORDER BY age DESC LIMIT 10").unwrap();
        let plan = plan_select(&stmts[0], &schemas).unwrap();

        match &plan {
            LogicalPlan::Project(p) => match &*p.input {
                LogicalPlan::Limit(l) => {
                    assert_eq!(l.limit, 10);
                    match &*l.input {
                        LogicalPlan::Sort(s) => {
                            assert_eq!(s.sort_keys.len(), 1);
                            assert!(s.sort_keys[0].desc);
                        }
                        _ => panic!("expected Sort"),
                    }
                }
                _ => panic!("expected Limit"),
            },
            _ => panic!("expected Project"),
        }
    }

    #[test]
    fn test_plan_aggregate() {
        let schema = make_schema();
        let mut schemas = std::collections::HashMap::new();
        schemas.insert("users".into(), schema);

        let stmts = Parser::parse("SELECT COUNT(*) FROM users").unwrap();
        let plan = plan_select(&stmts[0], &schemas).unwrap();

        fn has_aggregate(plan: &LogicalPlan) -> bool {
            match plan {
                LogicalPlan::Aggregate(_) => true,
                LogicalPlan::Filter(f) => has_aggregate(&f.input),
                LogicalPlan::Project(p) => has_aggregate(&p.input),
                LogicalPlan::Sort(s) => has_aggregate(&s.input),
                LogicalPlan::Limit(l) => has_aggregate(&l.input),
                _ => false,
            }
        }
        assert!(has_aggregate(&plan));
    }
}
