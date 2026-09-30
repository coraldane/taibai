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
fn varchar_uses_contiguous_arrow_layout() {
    let mut col = ColumnArray::empty(DataType::VarChar);
    col.push(&ScalarValue::VarChar(Some("alpha".into())))
        .unwrap();
    col.push(&ScalarValue::VarChar(None)).unwrap();
    col.push(&ScalarValue::VarChar(Some("beta".into())))
        .unwrap();

    let ColumnArray::VarChar { data, .. } = col else {
        unreachable!()
    };
    assert_eq!(data.offsets(), &[0, 5, 5, 9]);
    assert_eq!(data.values(), b"alphabeta");
    assert_eq!(data.value(2), "beta");
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

#[test]
fn test_extend_from_run_form_source() {
    use crate::types::bitmap::NullBitmap;

    // Rle/DeltaRle run-form sources used to hit an unreachable!() in extend:
    // the data_type gate passes (a view's data_type is its underlying type),
    // so any caller holding a run-form column panicked the session instead of
    // extending. extend must materialize the view and append its values.
    let runs_col = ColumnArray::rle(
        vec![(7, 3), (9, 2)],
        DataType::Int64,
        NullBitmap::new_valid(5),
        5,
    );
    assert_eq!(runs_col.data_type(), DataType::Int64);

    let mut dst = ColumnArray::empty(DataType::Int64);
    dst.push(&ScalarValue::Int64(Some(1))).unwrap();
    dst.extend(&runs_col).unwrap();
    assert_eq!(dst.len(), 6);
    assert_eq!(dst.scalar(0), ScalarValue::Int64(Some(1)));
    assert_eq!(dst.scalar(1), ScalarValue::Int64(Some(7)));
    assert_eq!(dst.scalar(3), ScalarValue::Int64(Some(7)));
    assert_eq!(dst.scalar(4), ScalarValue::Int64(Some(9)));
    assert_eq!(dst.scalar(5), ScalarValue::Int64(Some(9)));

    // append_owned routes variable-width moves separately but falls back to
    // extend for everything else — same contract for run-form sources.
    let runs_col2 = ColumnArray::rle(
        vec![(4, 2)],
        DataType::Int64,
        NullBitmap::new_valid(2),
        2,
    );
    let mut dst2 = ColumnArray::empty(DataType::Int64);
    dst2.append_owned(runs_col2).unwrap();
    assert_eq!(dst2.len(), 2);
    assert_eq!(dst2.scalar(1), ScalarValue::Int64(Some(4)));
}
