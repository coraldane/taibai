use crate::sql::planner::*;

use super::join_projection::prune_join_under_filters;
use super::Rule;

pub struct ColumnPruningRule;

impl Rule for ColumnPruningRule {
    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        prune_columns(plan)
    }
}

fn prune_columns(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Project(proj) => {
            let used_cols = collect_column_refs(&proj.expressions);

            let mut filter_cols = Vec::new();
            collect_filter_cols(&proj.input, &mut filter_cols);
            let mut all_used = used_cols;
            all_used.extend(filter_cols);
            all_used.sort();
            all_used.dedup();

            let (optimized_input, input_changed) = if let LogicalPlan::Scan(mut scan) = *proj.input
            {
                if !all_used.is_empty() {
                    let changed = scan.projection.as_ref().is_none_or(|p| *p != all_used);
                    scan.projection = Some(all_used);
                    (LogicalPlan::Scan(scan), changed)
                } else {
                    (LogicalPlan::Scan(scan), false)
                }
            } else {
                // Join-bearing inputs: propagate the required columns into
                // each join SIDE's scan (plus the condition's own columns),
                // so both sides decode only what the join and its consumers
                // reference — previously every join query decoded every
                // column of every row group on both sides. Falls back to the
                // generic recursion when no join sits under the filter chain.
                let (joined, join_changed) = prune_join_under_filters(*proj.input, &all_used);
                if join_changed {
                    (joined, true)
                } else {
                    prune_columns(joined)
                }
            };

            (
                LogicalPlan::Project(ProjectPlan {
                    expressions: proj.expressions,
                    aliases: proj.aliases,
                    input: Box::new(optimized_input),
                }),
                input_changed,
            )
        }
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = prune_columns(*filter.input);
            filter.input = Box::new(input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = prune_columns(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = prune_columns(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = prune_columns(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        other => (other, false),
    }
}

fn collect_filter_cols(plan: &LogicalPlan, cols: &mut Vec<usize>) {
    match plan {
        LogicalPlan::Filter(f) => {
            collect_cols_from_expr(&f.predicate, cols);
            collect_filter_cols(&f.input, cols);
        }
        LogicalPlan::Sort(s) => {
            for sk in &s.sort_keys {
                collect_cols_from_expr(&sk.expr, cols);
            }
        }
        LogicalPlan::Aggregate(a) => {
            for expr in &a.group_by {
                collect_cols_from_expr(expr, cols);
            }
            for agg in &a.aggregates {
                collect_cols_from_expr(&agg.arg, cols);
            }
        }
        LogicalPlan::Limit(l) => collect_filter_cols(&l.input, cols),
        LogicalPlan::Project(p) => collect_filter_cols(&p.input, cols),
        _ => {}
    }
}

/// Test-only entry: the full rule pass over a plan.
#[cfg(test)]
pub(crate) fn test_prune(plan: LogicalPlan) -> (LogicalPlan, bool) {
    prune_columns(plan)
}
