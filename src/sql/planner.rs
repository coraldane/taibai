use crate::common::error::{MonaxError, Result};
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;
use std::collections::HashMap;

/// A logical plan node in the query plan tree.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a table (optionally projecting specific columns).
    Scan(ScanPlan),
    /// Filter rows by a predicate expression.
    Filter(FilterPlan),
    /// Project specific columns / expressions.
    Project(ProjectPlan),
    /// Aggregate rows by grouping expressions and aggregate functions.
    Aggregate(AggregatePlan),
    /// Sort rows by sort expressions.
    Sort(SortPlan),
    /// Limit the number of rows returned.
    Limit(LimitPlan),
    /// Join two inputs.
    Join(JoinPlan),
    /// Empty result (e.g., DDL returns).
    Empty,
}

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table_name: String,
    pub schema: Schema,
    pub projection: Option<Vec<usize>>, // column indices to project
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub predicate: LogicalExpr,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ProjectPlan {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct AggregatePlan {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct SortPlan {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LimitPlan {
    pub limit: usize,
    pub offset: usize,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: LogicalExpr,
    pub desc: bool,
}

/// Logical expression used in the plan tree.
#[derive(Debug, Clone)]
pub enum LogicalExpr {
    /// A column reference by index in the schema.
    Column(usize),
    /// A literal value.
    Literal(crate::types::scalar::ScalarValue),
    /// A binary operation.
    BinaryOp {
        left: Box<LogicalExpr>,
        op: BinOp,
        right: Box<LogicalExpr>,
    },
    /// Unary NOT.
    Not(Box<LogicalExpr>),
    /// IS NULL check.
    IsNull(Box<LogicalExpr>),
    /// IS NOT NULL check.
    IsNotNull(Box<LogicalExpr>),
    /// An aggregate function call.
    Aggregate(AggregateExpr),
    /// A wildcard (SELECT *).
    Wildcard,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggFunc,
    pub arg: Box<LogicalExpr>,
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Convert an AST Select statement into a LogicalPlan tree.
pub fn plan_select(
    stmt: &Statement,
    table_schemas: &HashMap<String, Schema>,
) -> Result<LogicalPlan> {
    let (select_list, from, where_clause, group_by, order_by, limit, offset, _distinct) = match stmt {
        Statement::Select { select_list, from, where_clause, group_by, order_by, limit, offset, distinct } => {
            (select_list, from, where_clause, group_by, order_by, limit, offset, distinct)
        }
        _ => return Err(MonaxError::Internal("plan_select called with non-SELECT".into())),
    };

    // Step 1: Plan FROM clause (Scan or Join)
    let (base_plan, schema) = if let Some(from_clause) = from {
        plan_from_clause(from_clause, table_schemas)?
    } else {
        (LogicalPlan::Empty, Schema::empty())
    };

    let mut plan: LogicalPlan = base_plan;

    // Step 2: WHERE → Filter
    if let Some(pred) = where_clause {
        let predicate = ast_expr_to_logical(pred, &schema)?;
        plan = LogicalPlan::Filter(FilterPlan {
            predicate,
            input: Box::new(plan),
        });
    }

    // Step 3: GROUP BY → Aggregate
    // Also aggregate if any aggregate functions in select list
    let agg_funcs = extract_aggregates(select_list);
    if !group_by.is_empty() || !agg_funcs.is_empty() {
        let group_exprs: Vec<LogicalExpr> = group_by.iter()
            .map(|e| ast_expr_to_logical(e, &schema))
            .collect::<Result<Vec<_>>>()?;

        let agg_exprs: Vec<AggregateExpr> = agg_funcs.into_iter().map(|(func, arg_expr, distinct)| {
            let arg = match &arg_expr {
                Some(e) => Box::new(ast_expr_to_logical(e, &schema).unwrap_or(LogicalExpr::Wildcard)),
                None => Box::new(LogicalExpr::Wildcard),
            };
            AggregateExpr { func, arg, distinct }
        }).collect();

        plan = LogicalPlan::Aggregate(AggregatePlan {
            group_by: group_exprs,
            aggregates: agg_exprs,
            input: Box::new(plan),
        });
    }

    // Step 4: ORDER BY → Sort
    if !order_by.is_empty() {
        let sort_keys: Vec<SortKey> = order_by.iter()
            .map(|item| -> Result<SortKey> {
                let expr = ast_expr_to_logical(&item.expr, &schema)?;
                Ok(SortKey { expr, desc: item.desc })
            })
            .collect::<Result<Vec<_>>>()?;

        plan = LogicalPlan::Sort(SortPlan {
            sort_keys,
            input: Box::new(plan),
        });
    }

    // Step 5: LIMIT/OFFSET → Limit
    if let Some(limit_expr) = limit {
        let limit_val = eval_const_usize(limit_expr)?;
        let offset_val = offset.as_ref().map(eval_const_usize).transpose()?.unwrap_or(0);
        plan = LogicalPlan::Limit(LimitPlan {
            limit: limit_val,
            offset: offset_val,
            input: Box::new(plan),
        });
    }

    // Step 6: Projection (always applied for select list)
    let (proj_exprs, aliases) = resolve_projection(select_list, &schema);
    plan = LogicalPlan::Project(ProjectPlan {
        expressions: proj_exprs,
        aliases,
        input: Box::new(plan),
    });

    Ok(plan)
}

/// Plan a FROM clause (single table or join).
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
            let plan = LogicalPlan::Scan(ScanPlan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: None,
            });
            Ok((plan, schema))
        }
        FromClause::Join(join_expr) => {
            // Plan left side
            let (left_plan, left_schema) = plan_from_clause(&join_expr.left, table_schemas)?;

            // Process each join in chain
            let mut current_plan = left_plan;
            let mut current_schema = left_schema;

            for join_clause in &join_expr.joins {
                let right_table = &join_clause.right.name;
                let right_schema = table_schemas.get(right_table)
                    .ok_or_else(|| MonaxError::Sql(format!("table '{}' not found", right_table)))?
                    .clone();

                let right_plan = LogicalPlan::Scan(ScanPlan {
                    table_name: right_table.clone(),
                    schema: right_schema.clone(),
                    projection: None,
                });

                // Merge schemas
                let output_schema = merge_schemas(&current_schema, &right_schema);

                // Convert condition to logical expr with merged schema
                let condition = match &join_clause.condition {
                    Some(expr) => ast_expr_to_logical_join(expr, &output_schema, &current_schema, &right_schema)?,
                    None => LogicalExpr::Literal(ScalarValue::Bool(Some(true))), // CROSS JOIN
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

/// Merge two schemas (left + right).
fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    Schema::new(columns)
}

/// Convert an AST expression to a logical expression.
fn ast_expr_to_logical(expr: &Expr, schema: &Schema) -> Result<LogicalExpr> {
    match expr {
        Expr::Literal(v) => Ok(LogicalExpr::Literal(v.clone())),
        Expr::Column(name) => {
            let idx = schema.index_of(name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table, column } => {
            // For simple case, just find column by name
            let idx = schema.index_of(column)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found in table '{}'", column, table)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = ast_expr_to_logical(left, schema)?;
            let r = ast_expr_to_logical(right, schema)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryNot(inner) => {
            let e = ast_expr_to_logical(inner, schema)?;
            Ok(LogicalExpr::Not(Box::new(e)))
        }
        Expr::IsNull(inner) => {
            let e = ast_expr_to_logical(inner, schema)?;
            Ok(LogicalExpr::IsNull(Box::new(e)))
        }
        Expr::IsNotNull(inner) => {
            let e = ast_expr_to_logical(inner, schema)?;
            Ok(LogicalExpr::IsNotNull(Box::new(e)))
        }
        Expr::Function { name, args, distinct } => {
            let func = match name.to_uppercase().as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "AVG" => AggFunc::Avg,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                other => return Err(MonaxError::Sql(format!("unknown function '{}'", other))),
            };
            let arg = if args.is_empty() || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*")) {
                LogicalExpr::Wildcard
            } else {
                ast_expr_to_logical(&args[0], schema)?
            };
            Ok(LogicalExpr::Aggregate(AggregateExpr {
                func,
                arg: Box::new(arg),
                distinct: *distinct,
            }))
        }
    }
}

/// Convert an AST expression to a logical expression for join conditions.
/// Handles QualifiedColumn by resolving column in appropriate schema side.
fn ast_expr_to_logical_join(
    expr: &Expr,
    output_schema: &Schema,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<LogicalExpr> {
    match expr {
        Expr::Literal(v) => Ok(LogicalExpr::Literal(v.clone())),
        Expr::Column(name) => {
            // Try to find column in merged schema
            let idx = output_schema.index_of(name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", name)))?;
            Ok(LogicalExpr::Column(idx))
        }
        Expr::QualifiedColumn { table: _, column } => {
            // Find which side has the column and compute correct index
            let left_idx = left_schema.index_of(column);
            let right_idx = right_schema.index_of(column);
            
            let idx = match (left_idx, right_idx) {
                (Some(l), None) => l,
                (None, Some(r)) => left_schema.columns.len() + r,
                (Some(l), Some(_)) => l, // Prefer left if column exists in both (ambiguous case)
                (None, None) => {
                    return Err(MonaxError::Sql(format!("column '{}' not found", column)));
                }
            };
            Ok(LogicalExpr::Column(idx))
        }
        Expr::BinaryOp { left, op, right } => {
            let l = ast_expr_to_logical_join(left, output_schema, left_schema, right_schema)?;
            let r = ast_expr_to_logical_join(right, output_schema, left_schema, right_schema)?;
            Ok(LogicalExpr::BinaryOp {
                left: Box::new(l),
                op: *op,
                right: Box::new(r),
            })
        }
        Expr::UnaryNot(inner) => {
            let e = ast_expr_to_logical_join(inner, output_schema, left_schema, right_schema)?;
            Ok(LogicalExpr::Not(Box::new(e)))
        }
        Expr::IsNull(inner) => {
            let e = ast_expr_to_logical_join(inner, output_schema, left_schema, right_schema)?;
            Ok(LogicalExpr::IsNull(Box::new(e)))
        }
        Expr::IsNotNull(inner) => {
            let e = ast_expr_to_logical_join(inner, output_schema, left_schema, right_schema)?;
            Ok(LogicalExpr::IsNotNull(Box::new(e)))
        }
        Expr::Function { name, args, distinct } => {
            let func = match name.to_uppercase().as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "AVG" => AggFunc::Avg,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                other => return Err(MonaxError::Sql(format!("unknown function '{}'", other))),
            };
            let arg = if args.is_empty() || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*")) {
                LogicalExpr::Wildcard
            } else {
                ast_expr_to_logical_join(&args[0], output_schema, left_schema, right_schema)?
            };
            Ok(LogicalExpr::Aggregate(AggregateExpr {
                func,
                arg: Box::new(arg),
                distinct: *distinct,
            }))
        }
    }
}

/// Extract aggregate function calls from the select list.
fn extract_aggregates(select_list: &[SelectItem]) -> Vec<(AggFunc, Option<Expr>, bool)> {
    let mut aggs = Vec::new();
    for item in select_list {
        if let SelectItem::Expr(expr, _) = item {
            extract_aggs_from_expr(expr, &mut aggs);
        }
    }
    aggs
}

fn extract_aggs_from_expr(expr: &Expr, aggs: &mut Vec<(AggFunc, Option<Expr>, bool)>) {
    if let Expr::Function { name, args, distinct } = expr {
        let func = match name.to_uppercase().as_str() {
            "COUNT" => Some(AggFunc::Count),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            _ => None,
        };
        if let Some(func) = func {
            let arg = if args.is_empty() || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*")) {
                None
            } else {
                Some(args[0].clone())
            };
            aggs.push((func, arg, *distinct));
        }
    }
}

/// Resolve select list items into projection expressions and aliases.
fn resolve_projection(select_list: &[SelectItem], schema: &Schema) -> (Vec<LogicalExpr>, Vec<Option<String>>) {
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
                let logical = ast_expr_to_logical(expr, schema).unwrap_or(LogicalExpr::Wildcard);
                exprs.push(logical);
                aliases.push(alias.clone());
            }
        }
    }

    (exprs, aliases)
}

/// Evaluate a constant expression to a usize value.
fn eval_const_usize(expr: &Expr) -> Result<usize> {
    match expr {
        Expr::Literal(crate::types::scalar::ScalarValue::Int64(Some(v))) => Ok(*v as usize),
        Expr::Literal(crate::types::scalar::ScalarValue::Int32(Some(v))) => Ok(*v as usize),
        _ => Err(MonaxError::Sql("expected integer literal for LIMIT/OFFSET".into())),
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

        // Plan should be: Project(Scan)
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

        // Plan: Project(Filter(Scan))
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

        // Plan: Project(Limit(Sort(Scan)))
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

        // Should contain an Aggregate node
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
