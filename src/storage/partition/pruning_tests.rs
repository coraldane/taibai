use super::*;

fn zone(min: i64, max: i64, null_count: u64, row_count: u64) -> ZoneMap {
    ZoneMap {
        min_value: ScalarValue::Int64(Some(min)),
        max_value: ScalarValue::Int64(Some(max)),
        null_count,
        row_count,
    }
}

#[test]
fn classifies_range_predicates_as_none_partial_or_all() {
    let maps = vec![zone(10, 20, 0, 10)];

    let none = RowGroupPredicate::new(vec![RangeClause {
        column_index: 0,
        op: RangeOp::Gt,
        value: ScalarValue::Int64(Some(30)),
    }]);
    let partial = RowGroupPredicate::new(vec![RangeClause {
        column_index: 0,
        op: RangeOp::Ge,
        value: ScalarValue::Int64(Some(15)),
    }]);
    let all = RowGroupPredicate::new(vec![RangeClause {
        column_index: 0,
        op: RangeOp::Ge,
        value: ScalarValue::Int64(Some(10)),
    }]);

    assert_eq!(none.classify_row_group(&maps), PredicateMatch::None);
    assert_eq!(partial.classify_row_group(&maps), PredicateMatch::Partial);
    assert_eq!(all.classify_row_group(&maps), PredicateMatch::All);
}

#[test]
fn nulls_prevent_all_classification() {
    let maps = vec![zone(10, 20, 1, 10)];
    let predicate = RowGroupPredicate::new(vec![RangeClause {
        column_index: 0,
        op: RangeOp::Ge,
        value: ScalarValue::Int64(Some(0)),
    }]);

    assert_eq!(predicate.classify_row_group(&maps), PredicateMatch::Partial);
}

#[test]
fn equality_is_all_only_for_constant_non_null_group() {
    let constant = vec![zone(10, 10, 0, 10)];
    let varied = vec![zone(10, 20, 0, 10)];
    let predicate = RowGroupPredicate::new(vec![RangeClause {
        column_index: 0,
        op: RangeOp::Eq,
        value: ScalarValue::Int64(Some(10)),
    }]);

    assert_eq!(predicate.classify_row_group(&constant), PredicateMatch::All);
    assert_eq!(
        predicate.classify_row_group(&varied),
        PredicateMatch::Partial
    );
}
