use super::{ColumnStats, CostEstimator, TableStats};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::storage::catalog::schema::HistogramBucketMeta;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;
use std::collections::HashMap;

fn estimator() -> CostEstimator {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut columns = HashMap::new();
    columns.insert(
        "id".to_string(),
        ColumnStats {
            distinct_count: 100,
            null_count: 0,
            min_value: Some(ScalarValue::Int64(Some(0))),
            max_value: Some(ScalarValue::Int64(Some(99))),
            histogram: Vec::new(),
        },
    );
    CostEstimator::new(TableStats {
        row_count: 1_000,
        schema,
        column_stats: columns,
    })
}

#[test]
fn equality_uses_ndv_and_schema_mapping() {
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(42)))),
    };
    assert_eq!(estimator().estimate_rows(Some(&predicate)), 10);
}

#[test]
fn impossible_equality_estimates_zero_rows() {
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(200)))),
    };
    assert_eq!(estimator().estimate_rows(Some(&predicate)), 0);
}

#[test]
fn range_selectivity_uses_histogram_for_skewed_values() {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
    let stats = ColumnStats {
        distinct_count: 11,
        null_count: 0,
        min_value: Some(ScalarValue::Int64(Some(0))),
        max_value: Some(ScalarValue::Int64(Some(10))),
        histogram: vec![
            HistogramBucketMeta {
                lower_bound: ScalarValue::Int64(Some(0)),
                upper_bound: ScalarValue::Int64(Some(0)),
                sample_count: 90,
            },
            HistogramBucketMeta {
                lower_bound: ScalarValue::Int64(Some(1)),
                upper_bound: ScalarValue::Int64(Some(10)),
                sample_count: 10,
            },
        ],
    };
    let estimator = CostEstimator::new(TableStats {
        row_count: 100,
        schema,
        column_stats: HashMap::from([("value".to_string(), stats)]),
    });
    let less_than_one = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Lt,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
    };
    let at_least_one = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Gte,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
    };
    let equal_zero = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
    };

    assert_eq!(estimator.estimate_rows(Some(&less_than_one)), 90);
    assert_eq!(estimator.estimate_rows(Some(&at_least_one)), 10);
    assert_eq!(estimator.estimate_rows(Some(&equal_zero)), 90);
}
