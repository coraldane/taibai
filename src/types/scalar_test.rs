use super::*;

#[test]
fn test_null_detection() {
    assert!(ScalarValue::Int32(None).is_null());
    assert!(!ScalarValue::Int32(Some(42)).is_null());
    assert!(ScalarValue::VarChar(None).is_null());
    assert!(!ScalarValue::VarChar(Some("hello".into())).is_null());
}

#[test]
fn test_data_type_roundtrip() {
    assert_eq!(ScalarValue::Int64(Some(1)).data_type(), DataType::Int64);
    assert_eq!(
        ScalarValue::VarChar(Some("x".into())).data_type(),
        DataType::VarChar
    );
    assert_eq!(ScalarValue::Bool(Some(true)).data_type(), DataType::Bool);
}

#[test]
fn test_null_for_type() {
    assert!(ScalarValue::null_for_type(DataType::Int64).is_null());
    assert!(ScalarValue::null_for_type(DataType::VarChar).is_null());
}

use std::cmp::Ordering;

#[test]
fn test_compare_ordered_null_placement() {
    let (a, b) = (ScalarValue::Int64(None), ScalarValue::Int64(Some(1)));
    // nulls last (ascending ORDER BY convention)
    assert_eq!(a.compare_ordered(&b, Ordering::Greater), Ordering::Greater);
    assert_eq!(b.compare_ordered(&a, Ordering::Greater), Ordering::Less);
    // nulls first (MIN/MAX accumulator convention)
    assert_eq!(a.compare_ordered(&b, Ordering::Less), Ordering::Less);
    assert_eq!(b.compare_ordered(&a, Ordering::Less), Ordering::Greater);
    // two nulls always equal
    assert_eq!(
        ScalarValue::Int64(None).compare_ordered(&ScalarValue::Int64(None), Ordering::Less),
        Ordering::Equal
    );
}

#[test]
fn test_compare_ordered_numeric_promotion_and_types() {
    // cross-width integer via i128
    assert_eq!(
        ScalarValue::Int32(Some(5)).compare_ordered(&ScalarValue::Int64(Some(4)), Ordering::Greater),
        Ordering::Greater
    );
    // u64 above i64 range must not wrap through the i128 path
    let big = ScalarValue::UInt64(Some(u64::MAX));
    assert_eq!(
        big.compare_ordered(&ScalarValue::Int64(Some(i64::MAX)), Ordering::Greater),
        Ordering::Greater
    );
    // int vs float promotion
    assert_eq!(
        ScalarValue::Int32(Some(5)).compare_ordered(&ScalarValue::Float64(Some(4.5)), Ordering::Greater),
        Ordering::Greater
    );
    assert_eq!(
        ScalarValue::Decimal(Some(150)).compare_ordered(&ScalarValue::Int64(Some(200)), Ordering::Greater),
        Ordering::Less
    );
    // NaN lands in the Display fallback and sorts after any number
    assert_eq!(
        ScalarValue::Float64(Some(f64::NAN)).compare_ordered(&ScalarValue::Float64(Some(1.0)), Ordering::Greater),
        Ordering::Greater
    );
    assert_eq!(
        ScalarValue::Float64(Some(f64::NAN)).compare_ordered(&ScalarValue::Float64(Some(f64::NAN)), Ordering::Greater),
        Ordering::Equal
    );
}

#[test]
fn test_compare_ordered_non_numeric_types() {
    use ScalarValue::*;
    assert_eq!(
        VarChar(Some("a".into())).compare_ordered(&VarChar(Some("b".into())), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Bool(Some(false)).compare_ordered(&Bool(Some(true)), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Time(Some(100)).compare_ordered(&Time(Some(200)), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Interval(Some(1)).compare_ordered(&Interval(Some(2)), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Blob(Some(vec![1u8])).compare_ordered(&Blob(Some(vec![1u8, 0u8])), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Timestamp(Some(9)).compare_ordered(&Timestamp(Some(10)), Ordering::Greater),
        Ordering::Less
    );
    assert_eq!(
        Date(Some(9)).compare_ordered(&Date(Some(10)), Ordering::Greater),
        Ordering::Less
    );
}
