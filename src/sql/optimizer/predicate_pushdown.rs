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
                        scan.pushed_filter = Some(filter.predicate);
                        (LogicalPlan::Scan(scan), true)
                    } else {
                        filter.input = Box::new(LogicalPlan::Scan(scan));
                        (LogicalPlan::Filter(filter), input_changed)
                    }
                }
                other => {
                    let applied = input_changed;
                    (LogicalPlan::Filter(FilterPlan {
                        predicate: filter.predicate,
                        input: Box::new(other),
                    }), applied)
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
