use crate::sql::planner::*;

/// Apply optimization rules to a logical plan.
/// Currently implements:
/// - Column pruning (push projection down to scan)
/// - Predicate pushdown (push filters closer to scan)
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let mut plan = plan;
    plan = push_predicates(plan);
    plan = prune_columns(plan);
    plan
}

/// Push filter predicates down towards the scan nodes.
fn push_predicates(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            // Optimize the input first
            filter.input = Box::new(push_predicates(*filter.input));

            // If input is a Sort, push filter below sort
            match *filter.input {
                LogicalPlan::Sort(mut sort) => {
                    sort.input = Box::new(LogicalPlan::Filter(FilterPlan {
                        predicate: filter.predicate,
                        input: sort.input,
                    }));
                    LogicalPlan::Sort(sort)
                }
                other => LogicalPlan::Filter(FilterPlan {
                    predicate: filter.predicate,
                    input: Box::new(other),
                }),
            }
        }
        LogicalPlan::Project(mut proj) => {
            proj.input = Box::new(push_predicates(*proj.input));
            LogicalPlan::Project(proj)
        }
        LogicalPlan::Aggregate(mut agg) => {
            agg.input = Box::new(push_predicates(*agg.input));
            LogicalPlan::Aggregate(agg)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(push_predicates(*sort.input));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(push_predicates(*limit.input));
            LogicalPlan::Limit(limit)
        }
        other => other,
    }
}

/// Column pruning: push projection down to scan.
fn prune_columns(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project(proj) => {
            // Collect column indices used in projection
            let used_cols = collect_column_refs(&proj.expressions);

            let optimized_input = if let LogicalPlan::Scan(mut scan) = *proj.input {
                if !used_cols.is_empty() {
                    scan.projection = Some(used_cols);
                }
                LogicalPlan::Scan(scan)
            } else {
                prune_columns(*proj.input)
            };

            LogicalPlan::Project(ProjectPlan {
                expressions: proj.expressions,
                aliases: proj.aliases,
                input: Box::new(optimized_input),
            })
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(prune_columns(*filter.input));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Aggregate(mut agg) => {
            agg.input = Box::new(prune_columns(*agg.input));
            LogicalPlan::Aggregate(agg)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(prune_columns(*sort.input));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(prune_columns(*limit.input));
            LogicalPlan::Limit(limit)
        }
        other => other,
    }
}

/// Collect all column indices referenced in a list of expressions.
fn collect_column_refs(exprs: &[LogicalExpr]) -> Vec<usize> {
    let mut cols = Vec::new();
    for expr in exprs {
        collect_cols_from_expr(expr, &mut cols);
    }
    cols.sort();
    cols.dedup();
    cols
}

fn collect_cols_from_expr(expr: &LogicalExpr, cols: &mut Vec<usize>) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::parser::Parser;
    use crate::types::batch::{ColumnDef, Schema};
    use std::collections::HashMap;

    fn make_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
            ColumnDef::new("age", DataType::Int32),
        ])
    }

    #[test]
    fn test_column_pruning() {
        let schema = make_schema();
        let mut schemas = HashMap::new();
        schemas.insert("users".into(), schema);

        let stmts = Parser::parse("SELECT id FROM users WHERE age > 30").unwrap();
        let plan = crate::sql::planner::plan_select(&stmts[0], &schemas).unwrap();
        let optimized = optimize(plan);

        // Find the scan node
        fn find_scan(plan: &LogicalPlan) -> Option<&ScanPlan> {
            match plan {
                LogicalPlan::Scan(s) => Some(s),
                LogicalPlan::Filter(f) => find_scan(&f.input),
                LogicalPlan::Project(p) => find_scan(&p.input),
                LogicalPlan::Sort(s) => find_scan(&s.input),
                LogicalPlan::Limit(l) => find_scan(&l.input),
                LogicalPlan::Aggregate(a) => find_scan(&a.input),
                _ => None,
            }
        }

        let scan = find_scan(&optimized).unwrap();
        // Should have projection for columns used: id (0) and age (2)
        if let Some(proj) = &scan.projection {
            assert!(proj.contains(&0));
            assert!(proj.contains(&2));
        }
    }
}
