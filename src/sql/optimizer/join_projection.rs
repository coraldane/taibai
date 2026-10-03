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
fn narrow_side_scan(plan: LogicalPlan, needed: &[usize], side_schema: &Schema) -> (LogicalPlan, bool) {
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
mod tests {
    use super::*;
    use crate::sql::parser::ast::BinOp;
    use crate::sql::planner::{FilterPlan, JoinPlan, ProjectPlan, ScanPlan};
    use crate::sql::parser::ast::JoinType;

    fn schema_of(names: &[&str]) -> Schema {
        Schema::new(
            names
                .iter()
                .map(|name| {
                    crate::types::batch::ColumnDef::new(
                        name,
                        crate::protocol::types::DataType::Int64,
                    )
                })
                .collect(),
        )
    }

    fn join_of(
        left: LogicalPlan,
        left_schema: Schema,
        right: LogicalPlan,
        right_schema: Schema,
        cond: LogicalExpr,
    ) -> LogicalPlan {
        let mut columns = left_schema.columns.clone();
        columns.extend(right_schema.columns.clone());
        LogicalPlan::Join(JoinPlan {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: cond,
            output_schema: Schema::new(columns),
            left_schema,
            right_schema,
        })
    }

    fn scan_projection(plan: &LogicalPlan, table: &str) -> Option<Vec<usize>> {
        match plan {
            LogicalPlan::Scan(s) if s.table_name.eq_ignore_ascii_case(table) => {
                s.projection.clone()
            }
            LogicalPlan::Filter(f) => scan_projection(&f.input, table),
            LogicalPlan::Project(p) => scan_projection(&p.input, table),
            LogicalPlan::Join(j) => {
                scan_projection(&j.left, table).or_else(|| scan_projection(&j.right, table))
            }
            _ => None,
        }
    }

    /// Wide sides (4 columns each); the query references a.k + a.v (join key
    /// plus projected) and b.k (join key) only. Both scans must narrow to
    /// exactly those columns; the unreferenced wide columns are never read.
    #[test]
    fn join_sides_narrow_to_required_columns() {
        let left_schema = schema_of(&["k", "v", "pad1", "pad2"]);
        let right_schema = schema_of(&["k", "pad3", "pad4", "pad5"]);
        let plan = join_of(
            LogicalPlan::Scan(ScanPlan::new("a".into(), left_schema.clone())),
            left_schema,
            LogicalPlan::Scan(ScanPlan::new("b".into(), right_schema.clone())),
            right_schema,
            // ON a.k = b.k — combined space: left col 0 == right col 4.
            LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(0)),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Column(4)),
            },
        );
        let plan = LogicalPlan::Project(ProjectPlan {
            // SELECT a.v
            expressions: vec![LogicalExpr::Column(1)],
            aliases: vec![None],
            input: Box::new(plan),
        });

        let (pruned, changed) = crate::sql::optimizer::column_pruning::test_prune(plan);
        assert!(changed, "the rule must fire for a join under a projection");
        assert_eq!(
            scan_projection(&pruned, "a"),
            Some(vec![0, 1]),
            "left narrows to join key + projected column"
        );
        assert_eq!(
            scan_projection(&pruned, "b"),
            Some(vec![0]),
            "right narrows to the join key alone"
        );
    }

    /// A filter BETWEEN the projection and the join contributes its own
    /// columns to the side requirement.
    #[test]
    fn intermediate_filter_columns_join_the_requirement() {
        let left_schema = schema_of(&["k", "v", "f", "pad"]);
        let right_schema = schema_of(&["k", "pad", "pad"]);
        let plan = join_of(
            LogicalPlan::Scan(ScanPlan::new("a".into(), left_schema.clone())),
            left_schema,
            LogicalPlan::Scan(ScanPlan::new("b".into(), right_schema.clone())),
            right_schema,
            // ON a.k = b.k — combined space: left col 0 == right col 4.
            LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(0)),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Column(4)),
            },
        );
        let plan = LogicalPlan::Filter(FilterPlan {
            // WHERE a.f > 0 — combined space column 2.
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(2)),
                op: BinOp::Gt,
                right: Box::new(LogicalExpr::Literal(
                    crate::types::scalar::ScalarValue::Int64(Some(0)),
                )),
            },
            input: Box::new(plan),
        });
        let plan = LogicalPlan::Project(ProjectPlan {
            expressions: vec![LogicalExpr::Column(1)],
            aliases: vec![None],
            input: Box::new(plan),
        });

        let (pruned, changed) = crate::sql::optimizer::column_pruning::test_prune(plan);
        assert!(changed);
        assert_eq!(
            scan_projection(&pruned, "a"),
            Some(vec![0, 1, 2]),
            "join key + projection + the intermediate filter's column"
        );
        assert_eq!(scan_projection(&pruned, "b"), Some(vec![0]));
    }

    /// An already-projected scan is never re-narrowed (the rule is
    /// strictly additive over scans without a projection).
    #[test]
    fn existing_scan_projection_is_respected() {
        let left_schema = schema_of(&["k", "v", "pad"]);
        let right_schema = schema_of(&["k", "pad"]);
        let mut left_scan = ScanPlan::new("a".into(), left_schema.clone());
        left_scan.projection = Some(vec![0, 1, 2]);
        let plan = join_of(
            LogicalPlan::Scan(left_scan),
            left_schema,
            LogicalPlan::Scan(ScanPlan::new("b".into(), right_schema.clone())),
            right_schema,
            LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(0)),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Column(3)),
            },
        );
        let plan = LogicalPlan::Project(ProjectPlan {
            expressions: vec![LogicalExpr::Column(1)],
            aliases: vec![None],
            input: Box::new(plan),
        });

        let (pruned, changed) = crate::sql::optimizer::column_pruning::test_prune(plan);
        assert_eq!(
            scan_projection(&pruned, "a"),
            Some(vec![0, 1, 2]),
            "an explicit scan projection is never narrowed further"
        );
        assert!(changed, "the untouched right side still narrows");
    }
}
