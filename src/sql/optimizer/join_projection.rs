//! Join-side projection pruning: the requirement-propagation half of
//! projection pushdown across joins. The physical half is
//! `ExpandToSchemaExec`, which re-pads each narrowed join child to its full
//! planned side schema at execution time — the join itself and everything
//! above keep full-width, full-index semantics, so the two halves cannot
//! desync.

use crate::sql::planner::*;
use crate::types::batch::Schema;

/// Requirement propagation into join sides. `required` are OUTPUT (combined
/// left ++ right) column indices of the subtree; every `Filter` on the way
/// down adds its own predicate columns. At the join, the requirement and the
/// condition's columns split per side and reach each side's scan — through
/// the side's own filters, and recursively through NESTED joins (a side's
/// schema is the inner join's combined output). Only scans without an
/// existing projection are narrowed, never widened, and never to zero.
pub(super) fn prune_join_under_filters(
    plan: LogicalPlan,
    required: &[usize],
) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let mut cols = required.to_vec();
            collect_cols_from_expr(&filter.predicate, &mut cols);
            let (input, changed) = prune_join_under_filters(*filter.input, &cols);
            filter.input = Box::new(input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Join(mut join) => {
            // Only a left-then-right concatenated output layout can be split
            // positionally; anything else (outer-join reorders, qualified
            // name dedup) keeps today's full read.
            let layout_is_concat = {
                let left = &join.left_schema.columns;
                let right = &join.right_schema.columns;
                let output = &join.output_schema.columns;
                output.len() == left.len() + right.len()
                    && output
                        .iter()
                        .zip(left.iter().chain(right.iter()))
                        .all(|(actual, side)| actual.name == side.name)
            };
            if !layout_is_concat {
                return (LogicalPlan::Join(join), false);
            }
            let mut condition_cols = Vec::new();
            collect_cols_from_expr(&join.condition, &mut condition_cols);
            let left_width = join.left_schema.columns.len();
            let mut left_needed: Vec<usize> = required
                .iter()
                .filter(|col| **col < left_width)
                .copied()
                .collect();
            left_needed.extend(condition_cols.iter().filter(|c| **c < left_width).copied());
            let mut right_needed: Vec<usize> = required
                .iter()
                .filter(|col| **col >= left_width)
                .map(|col| col - left_width)
                .collect();
            right_needed.extend(
                condition_cols
                    .iter()
                    .filter(|c| **c >= left_width)
                    .map(|c| *c - left_width),
            );
            let (left, left_changed) =
                narrow_side_scan(*join.left, &left_needed, &join.left_schema);
            let (right, right_changed) =
                narrow_side_scan(*join.right, &right_needed, &join.right_schema);
            join.left = Box::new(left);
            join.right = Box::new(right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        other => (other, false),
    }
}

/// Narrow the (filter-wrapped / nested-join) side subtree to `needed`
/// side-space columns. Stops at any other operator shape.
fn narrow_side_scan(
    plan: LogicalPlan,
    needed: &[usize],
    side_schema: &Schema,
) -> (LogicalPlan, bool) {
    let mut needed = needed.to_vec();
    needed.sort_unstable();
    needed.dedup();
    if needed.is_empty() || needed.len() >= side_schema.columns.len() {
        return (plan, false);
    }
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if scan.projection.is_some() {
                return (LogicalPlan::Scan(scan), false);
            }
            scan.projection = Some(needed);
            (LogicalPlan::Scan(scan), true)
        }
        LogicalPlan::Filter(mut filter) => {
            let mut cols = needed.clone();
            collect_cols_from_expr(&filter.predicate, &mut cols);
            let (input, changed) = narrow_side_scan(*filter.input, &cols, side_schema);
            filter.input = Box::new(input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Join(mut join) => {
            // Nested join: this side's schema IS the inner join's combined
            // output, so the same split applies one level down.
            let inner_left_width = join.left_schema.columns.len();
            let mut condition_cols = Vec::new();
            collect_cols_from_expr(&join.condition, &mut condition_cols);
            let mut left: Vec<usize> = needed
                .iter()
                .filter(|col| **col < inner_left_width)
                .copied()
                .collect();
            left.extend(
                condition_cols
                    .iter()
                    .filter(|c| **c < inner_left_width)
                    .copied(),
            );
            let mut right: Vec<usize> = needed
                .iter()
                .filter(|col| **col >= inner_left_width)
                .map(|col| col - inner_left_width)
                .collect();
            right.extend(
                condition_cols
                    .iter()
                    .filter(|c| **c >= inner_left_width)
                    .map(|c| *c - inner_left_width),
            );
            let (new_left, left_changed) = narrow_side_scan(*join.left, &left, &join.left_schema);
            let (new_right, right_changed) =
                narrow_side_scan(*join.right, &right, &join.right_schema);
            join.left = Box::new(new_left);
            join.right = Box::new(new_right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        other => (other, false),
    }
}

#[cfg(test)]
#[path = "join_projection_test.rs"]
mod tests;
