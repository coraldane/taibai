use super::*;
use crate::sql::parser::ast::BinOp;
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::{FilterPlan, JoinPlan, ProjectPlan, ScanPlan};

fn schema_of(names: &[&str]) -> Schema {
    Schema::new(
        names
            .iter()
            .map(|name| {
                crate::types::batch::ColumnDef::new(name, crate::protocol::types::DataType::Int64)
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
        LogicalPlan::Scan(s) if s.table_name.eq_ignore_ascii_case(table) => s.projection.clone(),
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
