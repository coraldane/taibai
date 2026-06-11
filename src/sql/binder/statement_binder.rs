use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;

use super::expression_binder::ExpressionBinder;
use super::BindContext;

pub struct StatementBinder;

impl StatementBinder {
    pub fn bind_select(
        stmt: &Statement,
        table_schemas: &HashMap<String, Schema>,
    ) -> Result<BindContext> {
        let (select_list, from, where_clause, group_by, _order_by, _limit, _offset, _distinct) =
            match stmt {
                Statement::Select {
                    select_list,
                    from,
                    where_clause,
                    group_by,
                    order_by,
                    limit,
                    offset,
                    distinct,
                    having: _,
                } => (
                    select_list,
                    from,
                    where_clause,
                    group_by,
                    order_by,
                    limit,
                    offset,
                    distinct,
                ),
                _ => {
                    return Err(TaibaiError::Internal(
                        "bind_select called with non-SELECT".into(),
                    ))
                }
            };

        let (mut ctx, schema) = if let Some(from_clause) = from {
            Self::bind_from(from_clause, table_schemas)?
        } else {
            (BindContext::new(String::new()), Schema::empty())
        };

        ctx.output_schema = Some(schema.clone());

        if let Some(pred) = where_clause {
            let binder = ExpressionBinder::new(&ctx);
            let _bound_pred = binder.bind(pred)?;
        }

        for item in select_list {
            if let SelectItem::Expr(expr, _) = item {
                let binder = ExpressionBinder::new(&ctx);
                let _bound = binder.bind(expr)?;
            }
        }

        for expr in group_by {
            let binder = ExpressionBinder::new(&ctx);
            let _bound = binder.bind(expr)?;
        }

        Ok(ctx)
    }

    fn bind_from(
        from: &FromClause,
        table_schemas: &HashMap<String, Schema>,
    ) -> Result<(BindContext, Schema)> {
        match from {
            FromClause::Table(table_ref) => match table_ref {
                TableRef::Table { name, alias } => {
                    let table_name = name.plan_key();
                    let schema = table_schemas
                        .get(&table_name)
                        .ok_or_else(|| {
                            TaibaiError::Sql(format!("table '{}' not found", table_name))
                        })?
                        .clone();
                    let mut ctx = BindContext::with_table(&table_name, schema.clone());
                    if let Some(a) = alias {
                        ctx.register_table_alias(a, &table_name);
                    }
                    Ok((ctx, schema))
                }
                TableRef::Subquery { query: _, alias: _ } => Err(TaibaiError::Sql(
                    "subquery in FROM not yet supported".into(),
                )),
            },
            FromClause::Join(join_expr) => {
                let (mut left_ctx, left_schema) = Self::bind_from(&join_expr.left, table_schemas)?;
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

                            left_ctx
                                .table_schemas
                                .insert(right_table.clone(), right_schema.clone());

                            if let Some(a) = alias {
                                left_ctx.register_table_alias(a, &right_table);
                            }

                            current_schema = Self::merge_schemas(&current_schema, &right_schema);
                            left_ctx.output_schema = Some(current_schema.clone());

                            for (i, col) in right_schema.columns.iter().enumerate() {
                                left_ctx.column_index_map.insert(
                                    col.name.to_lowercase(),
                                    current_schema.columns.len() - right_schema.columns.len() + i,
                                );
                            }
                        }
                        TableRef::Subquery { query: _, alias: _ } => {
                            return Err(TaibaiError::Sql(
                                "subquery in JOIN not yet supported".into(),
                            ));
                        }
                    }
                }

                Ok((left_ctx, current_schema))
            }
        }
    }

    fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        Schema::new(columns)
    }
}
