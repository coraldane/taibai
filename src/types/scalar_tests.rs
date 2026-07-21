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
fn test_parse_from_str() {
    let v = ScalarValue::parse_from_str("42", DataType::Int64).unwrap();
    assert_eq!(v, ScalarValue::Int64(Some(42)));

    let v = ScalarValue::parse_from_str("2.71", DataType::Float64).unwrap();
    assert_eq!(v, ScalarValue::Float64(Some(2.71)));

    let v = ScalarValue::parse_from_str("hello", DataType::VarChar).unwrap();
    assert_eq!(v, ScalarValue::VarChar(Some("hello".into())));

    let v = ScalarValue::parse_from_str("", DataType::Int32).unwrap();
    assert!(v.is_null());
}

#[test]
fn test_null_for_type() {
    assert!(ScalarValue::null_for_type(DataType::Int64).is_null());
    assert!(ScalarValue::null_for_type(DataType::VarChar).is_null());
}
