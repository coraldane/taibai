use super::*;

#[test]
fn test_empty_column() {
    let col = ColumnArray::empty(DataType::Int64);
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
    assert_eq!(col.data_type(), DataType::Int64);
}

#[test]
fn test_push_and_scalar() {
    let mut col = ColumnArray::empty(DataType::Int32);
    col.push(&ScalarValue::Int32(Some(42))).unwrap();
    col.push(&ScalarValue::Int32(None)).unwrap();
    col.push(&ScalarValue::Int32(Some(7))).unwrap();

    assert_eq!(col.len(), 3);
    assert_eq!(col.scalar(0), ScalarValue::Int32(Some(42)));
    assert_eq!(col.scalar(1), ScalarValue::Int32(None));
    assert_eq!(col.scalar(2), ScalarValue::Int32(Some(7)));
}

#[test]
fn test_varchar_column() {
    let mut col = ColumnArray::empty(DataType::VarChar);
    col.push(&ScalarValue::VarChar(Some("hello".into())))
        .unwrap();
    col.push(&ScalarValue::VarChar(None)).unwrap();

    assert_eq!(col.scalar(0), ScalarValue::VarChar(Some("hello".into())));
    assert!(col.scalar(1).is_null());
}

#[test]
fn test_type_mismatch() {
    let mut col = ColumnArray::empty(DataType::Int64);
    let result = col.push(&ScalarValue::VarChar(Some("oops".into())));
    assert!(result.is_err());
}

#[test]
fn test_extend() {
    let mut col1 = ColumnArray::empty(DataType::Int64);
    col1.push(&ScalarValue::Int64(Some(1))).unwrap();
    col1.push(&ScalarValue::Int64(Some(2))).unwrap();

    let mut col2 = ColumnArray::empty(DataType::Int64);
    col2.push(&ScalarValue::Int64(Some(3))).unwrap();

    col1.extend(&col2).unwrap();
    assert_eq!(col1.len(), 3);
    assert_eq!(col1.scalar(2), ScalarValue::Int64(Some(3)));
}
