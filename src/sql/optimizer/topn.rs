/// TopN Optimizer - combines ORDER BY + LIMIT into a single TopN plan node.
/// Inspired by DuckDB's TopN optimizer.
use crate::sql::planner::*;

use super::Rule;

pub struct TopNRule;

impl Rule for TopNRule {
    fn name(&self) -> &str {
        "topn"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_topn(plan)
    }
}

fn apply_topn(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Limit(mut limit) => {
            match *limit.input {
                LogicalPlan::Sort(sort) => {
                    // Transform Limit(Sort(input)) -> TopN(input)
                    let topn = LogicalPlan::TopN(TopNPlan {
                        sort_keys: sort.sort_keys,
                        limit: limit.limit,
                        offset: limit.offset,
                        input: sort.input,
                    });
                    (topn, true)
                }
                _ => {
                    let (new_input, changed) = apply_topn(*limit.input);
                    limit.input = Box::new(new_input);
                    (LogicalPlan::Limit(limit), changed)
                }
            }
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, changed) = apply_topn(*sort.input);
            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Filter(mut f) => {
            let (new_input, changed) = apply_topn(*f.input);
            f.input = Box::new(new_input);
            (LogicalPlan::Filter(f), changed)
        }
        LogicalPlan::Project(mut p) => {
            let (new_input, changed) = apply_topn(*p.input);
            p.input = Box::new(new_input);
            (LogicalPlan::Project(p), changed)
        }
        LogicalPlan::Aggregate(mut a) => {
            let (new_input, changed) = apply_topn(*a.input);
            a.input = Box::new(new_input);
            (LogicalPlan::Aggregate(a), changed)
        }
        LogicalPlan::Join(mut j) => {
            let (new_left, l_changed) = apply_topn(*j.left);
            let (new_right, r_changed) = apply_topn(*j.right);
            j.left = Box::new(new_left);
            j.right = Box::new(new_right);
            (LogicalPlan::Join(j), l_changed || r_changed)
        }
        _ => (plan, false),
    }
}
