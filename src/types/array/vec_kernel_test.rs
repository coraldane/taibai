//! Unit tests for the typed comparison/arithmetic kernels added for the
//! vectorized expression path. Expectations mirror the per-row scalar
//! semantics in `src/sql/execution/operators/filter.rs`.

use super::*;
use crate::types::array::{ArithOp, CompareOp};
use crate::types::scalar::ScalarValue;

fn i64_col(values: Vec<Option<i64>>) -> ColumnArray {
    let mut col = ColumnArray::empty(crate::protocol::types::DataType::Int64);
    for v in values {
        col.push(&ScalarValue::Int64(v)).unwrap();
    }
    col
}

fn f64_col(values: Vec<Option<f64>>) -> ColumnArray {
    let mut col = ColumnArray::empty(crate::protocol::types::DataType::Float64);
    for v in values {
        col.push(&ScalarValue::Float64(v)).unwrap();
    }
    col
}

#[test]
fn compare_column_excludes_null_rows() {
    let l = i64_col(vec![Some(1), None, Some(3), Some(4)]);
    let r = i64_col(vec![Some(1), Some(2), None, Some(9)]);

    let eq = l.compare_column(CompareOp::Eq, &r).unwrap();
    assert_eq!(eq.mask, vec![true, false, false, false]);
    assert_eq!(eq.nulls.null_count(), 2);
    let lt = l.compare_column(CompareOp::Lt, &r).unwrap();
    assert_eq!(lt.mask, vec![false, false, false, true]);
    let neq = l.compare_column(CompareOp::Neq, &r).unwrap();
    assert_eq!(neq.mask, vec![false, false, false, true]);
}

#[test]
fn compare_column_unsupported_shapes_return_none() {
    let l = i64_col(vec![Some(1)]);
    let r = f64_col(vec![Some(1.0)]);
    assert!(l.compare_column(CompareOp::Eq, &r).is_none());
    assert!(l.compare_column(CompareOp::Eq, &i64_col(vec![])).is_none());
}

#[test]
fn compare_column_varchar_uses_string_order() {
    let mut l = ColumnArray::empty(crate::protocol::types::DataType::VarChar);
    let mut r = ColumnArray::empty(crate::protocol::types::DataType::VarChar);
    for (a, b) in [("AAPL", "AAPL"), ("MSFT", "TSLA"), ("GOOG", "HHHH")] {
        l.push(&ScalarValue::VarChar(Some(a.to_string()))).unwrap();
        r.push(&ScalarValue::VarChar(Some(b.to_string()))).unwrap();
    }
    let eq = l.compare_column(CompareOp::Eq, &r).unwrap();
    assert_eq!(eq.mask, vec![true, false, false]);
    let lt = l.compare_column(CompareOp::Lt, &r).unwrap();
    assert_eq!(lt.mask, vec![false, true, true]);
}

#[test]
fn compare_column_float_nan_matches_scalar_semantics() {
    let l = f64_col(vec![Some(f64::NAN), Some(f64::NAN), Some(1.0)]);
    let r = f64_col(vec![Some(f64::NAN), Some(2.0), Some(2.0)]);
    // NaN comparisons are three-valued NULL, matching eval_binary_scalar
    // (numeric_cmp returns None for NaN): the mask is false and the row is
    // flagged NULL. Only the comparable row yields a real value.
    let neq = l.compare_column(CompareOp::Neq, &r).unwrap();
    assert_eq!(neq.mask, vec![false, false, true]);
    assert_eq!(neq.nulls.null_count(), 2);
    let lt = l.compare_column(CompareOp::Lt, &r).unwrap();
    assert_eq!(lt.mask, vec![false, false, true]);
    assert_eq!(lt.nulls.null_count(), 2);
}

#[test]
fn arith_scalar_integer_semantics_match_scalar_path() {
    let col = i64_col(vec![Some(5), None, Some(i64::MAX), Some(7)]);
    // Plain add, cross-variant integer literal widening, NULL propagation.
    let add = col
        .arith_scalar(ArithOp::Add, &ScalarValue::Int32(Some(3)))
        .unwrap();
    match &add {
        ColumnArray::Int64 { data, bitmap } => {
            assert_eq!(data[0], 8);
            assert!(bitmap.is_null(1));
        }
        _ => panic!("expected Int64 result"),
    }
    // Overflow becomes NULL instead of wrapping.
    let overflow = col
        .arith_scalar(ArithOp::Add, &ScalarValue::Int64(Some(1)))
        .unwrap();
    match &overflow {
        ColumnArray::Int64 { bitmap, .. } => assert!(bitmap.is_null(2)),
        _ => panic!("expected Int64 result"),
    }
    // Modulo zero becomes NULL.
    let modulo = col
        .arith_scalar(ArithOp::Mod, &ScalarValue::Int64(Some(0)))
        .unwrap();
    match &modulo {
        ColumnArray::Int64 { bitmap, .. } => assert!(bitmap.is_null(0)),
        _ => panic!("expected Int64 result"),
    }
    // A NULL literal produces an all-NULL Int64 column.
    let null_lit = col
        .arith_scalar(ArithOp::Add, &ScalarValue::Int64(None))
        .unwrap();
    match &null_lit {
        ColumnArray::Int64 { bitmap, .. } => {
            assert_eq!(bitmap.null_count(), col.len());
        }
        _ => panic!("expected Int64 result"),
    }
}

#[test]
fn arith_scalar_division_is_f64_with_null_zero_divisor() {
    let col = i64_col(vec![Some(7), Some(6), None]);
    let div = col
        .arith_scalar(ArithOp::Div, &ScalarValue::Int64(Some(2)))
        .unwrap();
    match &div {
        ColumnArray::Float64 { data, bitmap } => {
            assert_eq!(data[0], 3.5);
            assert_eq!(data[1], 3.0);
            assert!(bitmap.is_null(2));
        }
        _ => panic!("expected Float64 result"),
    }
    let by_zero = col
        .arith_scalar(ArithOp::Div, &ScalarValue::Int64(Some(0)))
        .unwrap();
    match &by_zero {
        ColumnArray::Float64 { bitmap, .. } => {
            assert_eq!(bitmap.null_count(), 3);
        }
        _ => panic!("expected Float64 result"),
    }
}

#[test]
fn arith_scalar_float_and_decimal_classes() {
    // Float column + integer literal → Float64.
    let floats = f64_col(vec![Some(1.5), None]);
    let mixed = floats
        .arith_scalar(ArithOp::Mul, &ScalarValue::Int64(Some(2)))
        .unwrap();
    match &mixed {
        ColumnArray::Float64 { data, .. } => assert_eq!(data[0], 3.0),
        _ => panic!("expected Float64 result"),
    }
    // Decimal column + integer literal stays in the unscaled integer domain,
    // exactly like the scalar path (integer_i128 on both sides).
    let mut decimal = ColumnArray::empty(crate::protocol::types::DataType::Decimal);
    decimal.push(&ScalarValue::Decimal(Some(150))).unwrap();
    let dec_add = decimal
        .arith_scalar(ArithOp::Add, &ScalarValue::Int64(Some(1)))
        .unwrap();
    match &dec_add {
        ColumnArray::Int64 { data, .. } => assert_eq!(data[0], 151),
        _ => panic!("expected Int64 result"),
    }
}

#[test]
fn arith_column_covers_and_rejects_shapes() {
    let l = i64_col(vec![Some(10), Some(20)]);
    let r = i64_col(vec![Some(3), Some(4)]);
    let sub = l.arith_column(ArithOp::Sub, &r).unwrap();
    match &sub {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &[7, 16]),
        _ => panic!("expected Int64 result"),
    }
    let floats = f64_col(vec![Some(1.0), Some(2.0)]);
    // Mixed float x integer (non-division) stays on the scalar path.
    assert!(l.arith_column(ArithOp::Add, &floats).is_none());
    // Division accepts any numeric pair.
    let div = l.arith_column(ArithOp::Div, &r).unwrap();
    match &div {
        ColumnArray::Float64 { data, .. } => assert_eq!(data[0], 10.0 / 3.0),
        _ => panic!("expected Float64 result"),
    }
}

#[test]
fn bool_not_flips_and_preserves_nulls() {
    let mut col = ColumnArray::empty(crate::protocol::types::DataType::Bool);
    col.push(&ScalarValue::Bool(Some(true))).unwrap();
    col.push(&ScalarValue::Bool(None)).unwrap();
    col.push(&ScalarValue::Bool(Some(false))).unwrap();
    let negated = col.bool_not().unwrap();
    match &negated {
        ColumnArray::Bool { data, bitmap } => {
            assert_eq!(*data, vec![0, 0, 1]);
            assert!(!bitmap.is_null(0));
            assert!(bitmap.is_null(1));
            assert!(!bitmap.is_null(2));
        }
        _ => panic!("expected Bool result"),
    }
    assert!(i64_col(vec![Some(1)]).bool_not().is_none());
}

#[test]
fn temporal_literal_comparisons_take_typed_fast_path() {
    // Regression guard: range predicates against Timestamp/Date literals used
    // to fall through to an all-false fallback when the literal variant did
    // not route through numeric_f64.
    let mut ts = ColumnArray::empty(crate::protocol::types::DataType::Timestamp);
    for v in [1_000i64, 2_000, 3_000] {
        ts.push(&ScalarValue::Timestamp(Some(v))).unwrap();
    }
    let gt = ts.gt_vec(&ScalarValue::Timestamp(Some(2_000)));
    assert_eq!(gt, vec![false, false, true]);
    let le = ts.le_vec(&ScalarValue::Timestamp(Some(2_000)));
    assert_eq!(le, vec![true, true, false]);

    let mut dates = ColumnArray::empty(crate::protocol::types::DataType::Date);
    for v in [10i32, 20] {
        dates.push(&ScalarValue::Date(Some(v))).unwrap();
    }
    let lt = dates.lt_vec(&ScalarValue::Date(Some(20)));
    assert_eq!(lt, vec![true, false]);
}

#[test]
fn bool_column_from_mask_round_trips() {
    let col = ColumnArray::bool_column_from_mask(vec![true, false, true]);
    match &col {
        ColumnArray::Bool { data, bitmap } => {
            assert_eq!(*data, vec![1, 0, 1]);
            assert_eq!(bitmap.null_count(), 0);
        }
        _ => panic!("expected Bool result"),
    }
}

#[test]
fn selected_and_dictionary_columns_compare_through_flat_form() {
    use std::sync::Arc;

    // Zero-copy Selected view over rows [5, NULL, 7] picking rows 0 and 2.
    let source = i64_col(vec![Some(5), None, Some(7)]);
    let indices: Arc<[u32]> = vec![0u32, 2].into();
    let parts: SelectedParts = vec![(Arc::new(source), indices)].into();
    let selected = ColumnArray::selected(parts, crate::protocol::types::DataType::Int64, 2);

    assert_eq!(
        selected.compare_scalar_at(0, &ScalarValue::Int64(Some(5))),
        Some(std::cmp::Ordering::Equal)
    );
    assert_eq!(
        selected.compare_scalar_at(1, &ScalarValue::Int64(Some(9))),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(selected.numeric_i64_at(1), Some(7));
    assert_eq!(
        selected.eq_vec(&ScalarValue::Int64(Some(7))),
        vec![false, true]
    );

    // Dictionary varchar compares through its expanded form instead of
    // returning NULL / all-false.
    let mut dict = crate::types::array::Utf8Buffer::new();
    dict.push_str("AAPL");
    dict.push_str("MSFT");
    let dict_col = ColumnArray::Dictionary {
        ids: vec![0, 1, 0],
        dict,
        bitmap: NullBitmap::new_valid(3),
        flat: std::sync::OnceLock::new(),
    };
    assert_eq!(
        dict_col.eq_vec(&ScalarValue::VarChar(Some("AAPL".into()))),
        vec![true, false, true]
    );
    assert_eq!(
        dict_col.compare_scalar_at(1, &ScalarValue::VarChar(Some("ZZZ".into()))),
        Some(std::cmp::Ordering::Less)
    );
}

/// Regression: Int32 column compared against an Int64 literal with lt/le —
/// the literal `20260916` in SQL arrives as Int64 while minute bars store
/// deal_time as Int32; `deal_time <= 1033` must not lose every row.
#[test]
fn int32_column_cross_type_ordering_masks() {
    let mut col = ColumnArray::with_capacity(crate::protocol::types::DataType::Int32, 3);
    for v in [930i32, 1034, 500] {
        col.push(&ScalarValue::Int32(Some(v))).unwrap();
    }
    assert_eq!(
        col.le_vec(&ScalarValue::Int64(Some(1033))),
        vec![true, false, true],
        "le_vec Int32 col vs Int64 literal"
    );
    assert_eq!(
        col.lt_vec(&ScalarValue::Int64(Some(1033))),
        vec![true, false, true],
        "lt_vec Int32 col vs Int64 literal"
    );
    assert_eq!(
        col.ge_vec(&ScalarValue::Int64(Some(930))),
        vec![true, true, false],
        "ge_vec Int32 col vs Int64 literal"
    );
    assert_eq!(
        col.compare_scalar_at(0, &ScalarValue::Int64(Some(1033))),
        Some(std::cmp::Ordering::Less)
    );
}
