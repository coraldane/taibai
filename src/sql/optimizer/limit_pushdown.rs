use crate::sql::planner::*;

use super::Rule;

pub struct LimitPushdownRule;

impl Rule for LimitPushdownRule {
    fn name(&self) -> &str {
        "limit_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_limit_pushdown(plan)
    }
}

fn apply_limit_pushdown(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Limit(mut limit) => {
            let (input, input_changed) = apply_limit_pushdown(*limit.input);

            match &input {
                LogicalPlan::Scan(scan) => {
                    if scan.pushed_filter.is_none() && scan.pushed_limit.is_none() {
                        let mut new_scan = scan.clone();
                        new_scan.pushed_limit = Some(limit.limit.saturating_add(limit.offset));
                        limit.input = Box::new(LogicalPlan::Scan(new_scan));
                        return (LogicalPlan::Limit(limit), true);
                    }
                }
                LogicalPlan::Sort(_) => {}
                _ => {}
            }

            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), input_changed)
        }
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = apply_limit_pushdown(*filter.input);
            filter.input = Box::new(input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = apply_limit_pushdown(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = apply_limit_pushdown(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = apply_limit_pushdown(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        other => (other, false),
    }
}
