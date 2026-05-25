use crate::sql::planner::*;

use super::Rule;

pub struct PredicatePushdownRule;

impl Rule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        push_predicates(plan)
    }
}

fn push_predicates(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (input, input_changed) = push_predicates(*filter.input);
            filter.input = Box::new(input);

            match *filter.input {
                LogicalPlan::Sort(mut sort) => {
                    let (sort_input, _) = push_predicates(*sort.input);
                    sort.input = Box::new(LogicalPlan::Filter(FilterPlan {
                        predicate: filter.predicate,
                        input: Box::new(sort_input),
                    }));
                    let (result, child_changed) = push_predicates(LogicalPlan::Sort(sort));
                    (result, true || child_changed)
                }
                LogicalPlan::Scan(mut scan) => {
                    if scan.pushed_filter.is_none() {
                        scan.pushed_filter = Some(filter.predicate.clone());
                        filter.input = Box::new(LogicalPlan::Scan(scan));
                        (LogicalPlan::Filter(filter), true)
                    } else {
                        filter.input = Box::new(LogicalPlan::Scan(scan));
                        (LogicalPlan::Filter(filter), input_changed)
                    }
                }
                other => {
                    let applied = input_changed;
                    (
                        LogicalPlan::Filter(FilterPlan {
                            predicate: filter.predicate,
                            input: Box::new(other),
                        }),
                        applied,
                    )
                }
            }
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = push_predicates(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = push_predicates(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = push_predicates(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = push_predicates(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        other => (other, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::parser::Parser;
    use crate::sql::planner::plan_select;
    use crate::types::batch::{ColumnDef, Schema};
    use std::collections::HashMap;

    #[test]
    fn predicate_pushdown_keeps_row_filter() {
        let schema = Schema::new(vec![
            ColumnDef::new("security_id", DataType::VarChar),
            ColumnDef::new("deal_date", DataType::Int32),
        ]);
        let mut schemas = HashMap::new();
        schemas.insert("stock_tick_multi".to_string(), schema);

        let stmt = Parser::parse(
            "SELECT security_id FROM stock_tick_multi \
             WHERE security_id = '688256.SH' AND deal_date = 20251111",
        )
        .unwrap()
        .remove(0);
        let plan = plan_select(&stmt, &schemas).unwrap();
        let (optimized, changed) = PredicatePushdownRule.apply(plan);

        assert!(changed);
        let LogicalPlan::Project(project) = optimized else {
            panic!("expected project root");
        };
        let LogicalPlan::Filter(filter) = *project.input else {
            panic!("predicate pushdown must preserve the row-level filter");
        };
        let LogicalPlan::Scan(scan) = *filter.input else {
            panic!("expected filter input to be scan");
        };
        assert!(scan.pushed_filter.is_some());
    }
}
