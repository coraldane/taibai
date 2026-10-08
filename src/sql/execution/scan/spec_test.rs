use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::types::batch::ColumnDef;

fn int32_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("deal_date", DataType::Int32),
    ])
}

/// Regression: IN literals must be coerced to the column type. Bloom and
/// set index pruning hash/compare the stored type exactly, so Int64
/// literals against an Int32 column used to prune every matching row.
#[test]
fn in_predicate_values_are_coerced_to_column_type() {
    let schema = int32_schema();
    let expr = LogicalExpr::In {
        expr: Box::new(LogicalExpr::Column(1)),
        values: vec![
            ScalarValue::Int64(Some(20260916)),
            ScalarValue::Int64(Some(20260917)),
        ],
        negated: false,
    };
    let mut spec = ScanSpec::new("t");
    extract_predicates(&expr, &schema, &mut spec);

    assert!(spec.remaining_predicate.is_none());
    assert_eq!(spec.in_predicates.len(), 1);
    assert_eq!(
        spec.in_predicates[0].values,
        vec![
            ScalarValue::Int32(Some(20260916)),
            ScalarValue::Int32(Some(20260917))
        ]
    );
}

/// Between bounds feed the same index checks as range clauses; coerce them
/// too so DECIMAL scale and integer widths match the stored column.
#[test]
fn between_bounds_are_coerced_to_column_type() {
    let schema = int32_schema();
    let expr = LogicalExpr::Between {
        expr: Box::new(LogicalExpr::Column(1)),
        low: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(20260901)))),
        high: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(20260930)))),
        negated: false,
    };
    let mut spec = ScanSpec::new("t");
    extract_predicates(&expr, &schema, &mut spec);

    assert!(spec.remaining_predicate.is_none());
    assert_eq!(spec.range_predicates.len(), 2);
    assert!(spec.range_predicates.iter().all(|clause| {
        clause.value == ScalarValue::Int32(Some(20260901))
            || clause.value == ScalarValue::Int32(Some(20260930))
    }));
}

/// A literal that cannot be represented in the column type keeps the
/// predicate out of the pushed-down sets entirely (generic evaluation).
#[test]
fn in_predicate_with_uncoercible_value_stays_generic() {
    let schema = int32_schema();
    let expr = LogicalExpr::In {
        expr: Box::new(LogicalExpr::Column(1)),
        values: vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(i64::from(i32::MAX) + 1)),
        ],
        negated: false,
    };
    let mut spec = ScanSpec::new("t");
    extract_predicates(&expr, &schema, &mut spec);

    assert!(spec.in_predicates.is_empty());
    assert!(spec.remaining_predicate.is_some());
}

/// Sanity: eq literals were already coerced before the IN fix.
#[test]
fn eq_predicate_values_are_coerced_to_column_type() {
    let schema = int32_schema();
    let expr = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(1)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(20260916)))),
    };
    let mut spec = ScanSpec::new("t");
    extract_predicates(&expr, &schema, &mut spec);

    assert_eq!(
        spec.eq_predicates,
        vec![(1usize, ScalarValue::Int32(Some(20260916)))]
    );
}
