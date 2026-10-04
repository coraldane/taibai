use super::{
    dynamic_programming_join_order, estimate_join_cardinality, greedy_join_order,
    CompositeEstimate, ReorderLeaf,
};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::{LogicalExpr, LogicalPlan, ScanPlan};
use crate::types::batch::{ColumnDef, Schema};
use std::collections::HashMap;

fn equality(left: usize, right: usize) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Column(right)),
    }
}

fn leaf(
    name: &str,
    globals: Vec<usize>,
    rows: u64,
    distinct_counts: Vec<u64>,
    ordinal: usize,
) -> ReorderLeaf {
    let schema = Schema::new(
        globals
            .iter()
            .enumerate()
            .map(|(index, _)| ColumnDef::new(&format!("{name}_{index}"), DataType::Int64))
            .collect(),
    );
    let ndv = globals.iter().copied().zip(distinct_counts).collect();
    ReorderLeaf {
        plan: LogicalPlan::Scan(ScanPlan::new(name.to_string(), schema.clone())),
        schema,
        globals,
        rows,
        ndv,
        composite_ndv: Vec::new(),
        ordinal,
    }
}

#[test]
fn equi_join_cardinality_uses_larger_side_ndv() {
    let predicate = equality(0, 1);
    let left_ndv = HashMap::from([(0, 1_000)]);
    let right_ndv = HashMap::from([(1, 100)]);
    assert_eq!(
        estimate_join_cardinality(
            1_000,
            1_000,
            &[&predicate],
            &left_ndv,
            &right_ndv,
            &[],
            &[],
            &[0],
            &[1],
        ),
        1_000
    );
}

#[test]
fn composite_ndv_avoids_independence_underestimate_and_honors_unique_side() {
    let first = equality(0, 2);
    let second = equality(1, 3);
    let left_ndv = HashMap::from([(0, 1_000), (1, 1_000)]);
    let right_ndv = HashMap::from([(2, 1_000), (3, 1_000)]);
    let left_composite = [CompositeEstimate {
        columns: vec![0, 1],
        ndv: 1_000,
        unique: false,
    }];
    let right_composite = [CompositeEstimate {
        columns: vec![2, 3],
        ndv: 1_000,
        unique: false,
    }];
    assert_eq!(
        estimate_join_cardinality(
            1_000_000,
            1_000_000,
            &[&first, &second],
            &left_ndv,
            &right_ndv,
            &left_composite,
            &right_composite,
            &[0, 1],
            &[2, 3],
        ),
        1_000_000_000
    );

    let high_right_ndv = HashMap::from([(2, 1_000_000), (3, 1_000_000)]);
    assert_eq!(
        estimate_join_cardinality(
            1_000_000,
            1_000_000,
            &[&first, &second],
            &left_ndv,
            &high_right_ndv,
            &left_composite,
            &[],
            &[0, 1],
            &[2, 3],
        ),
        1_000_000
    );

    let unique_right = [CompositeEstimate {
        columns: vec![2, 3],
        ndv: 100,
        unique: true,
    }];
    assert_eq!(
        estimate_join_cardinality(
            1_000_000,
            1_000,
            &[&first, &second],
            &left_ndv,
            &right_ndv,
            &left_composite,
            &unique_right,
            &[0, 1],
            &[2, 3],
        ),
        1_000_000
    );
}

#[test]
fn dynamic_programming_avoids_small_but_unselective_first_join() {
    let leaves = vec![
        leaf("a", vec![0], 1_000, vec![1_000], 0),
        leaf("b", vec![1, 2], 1_000, vec![1_000, 1], 1),
        leaf("c", vec![3], 10, vec![1], 2),
    ];
    let predicates = vec![equality(0, 1), equality(2, 3)];

    let greedy = greedy_join_order(&leaves, &predicates).expect("connected greedy order");
    let optimized =
        dynamic_programming_join_order(&leaves, &predicates).expect("connected DP order");

    assert_eq!(greedy.leaf_order, vec![2, 1, 0]);
    assert_eq!(optimized.leaf_order, vec![0, 1, 2]);
    assert!(optimized.cost < greedy.cost);
    assert_eq!(optimized.rows, 10_000);
}
