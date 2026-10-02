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

#[test]
fn null_rle_flatten_is_dense() {
    use crate::types::bitmap::NullBitmap;
    // rows: [7, NULL, 9]; runs cover non-null rows only per constructor
    // contract. The expansion must be dense — one data slot per row — or
    // positional reads misalign after the first null and panic on tail rows.
    let col = ColumnArray::rle(
        vec![(7, 1), (9, 1)],
        DataType::Int64,
        NullBitmap::from_bools(&[false, true, false]),
        3,
    );
    let flat = col.flattened();
    let ColumnArray::Int64 { data, bitmap } = flat else {
        panic!("not int64");
    };
    assert_eq!(data.len(), 3, "expansion must be dense (len == row count)");
    assert_eq!(bitmap.null_count(), 1);
    assert_eq!(col.scalar(0), ScalarValue::Int64(Some(7)));
    assert_eq!(col.scalar(1), ScalarValue::Int64(None));
    assert_eq!(col.scalar(2), ScalarValue::Int64(Some(9)));
}

/// Representation equivalence (P0): the same logical column must answer
/// identically whether stored flat or in run form. Every consumer kernel
/// that had no `Rle` arm used to silently drop the column from SUM/AVG or
/// panic on positional reads; this pins the contract for the numeric
/// kernels, per-row accessors and scalar reads, with and without nulls.
#[test]
fn run_form_matches_flat_form_across_numeric_kernels() {
    use crate::types::bitmap::NullBitmap;

    let cases: Vec<(Vec<Option<i64>>, &str)> = vec![
        (
            (0..200).map(|i| Some(i % 7)).collect(),
            "low-cardinality, null-free",
        ),
        (
            (0..200)
                .map(|i| if i % 5 == 0 { None } else { Some(1000 + i / 10) })
                .collect(),
            "with nulls",
        ),
        ((0..130).map(|i| Some(i)).collect(), "distinct values"),
    ];

    for (values, label) in cases {
        let len = values.len();
        let null_flags: Vec<bool> = values.iter().map(|v| v.is_none()).collect();
        let bitmap = NullBitmap::from_bools(&null_flags);

        let mut flat = ColumnArray::empty(DataType::Int64);
        for value in &values {
            match value {
                Some(v) => flat.push(&ScalarValue::Int64(Some(*v))).unwrap(),
                None => flat.push(&ScalarValue::Int64(None)).unwrap(),
            }
        }
        // Runs cover the non-null rows in order (the decoder's contract).
        let mut runs: Vec<(i64, u32)> = Vec::new();
        for value in values.iter().flatten() {
            match runs.last_mut() {
                Some((last, count)) if *last == *value => *count += 1,
                _ => runs.push((*value, 1)),
            }
        }
        let rle = ColumnArray::rle(runs, DataType::Int64, bitmap, len);

        // Column-wide sum: the aggregate kernels' entry point.
        assert_eq!(flat.col_sum_i64(), rle.col_sum_i64(), "col_sum_i64 [{label}]");
        assert_eq!(
            flat.col_count_non_null(),
            rle.col_count_non_null(),
            "col_count_non_null [{label}]"
        );
        // Range sums (group-boundary partial aggregation).
        for (offset, count) in [(0, 64), (64, 64), (13, 87)] {
            assert_eq!(
                flat.col_sum_i64_range(offset, count),
                rle.col_sum_i64_range(offset, count),
                "col_sum_i64_range({offset}, {count}) [{label}]"
            );
        }
        // Per-row accessors: the generic GROUP BY and comparison paths.
        for row in 0..len {
            assert_eq!(
                flat.numeric_i64_at(row),
                rle.numeric_i64_at(row),
                "numeric_i64_at({row}) [{label}]"
            );
            assert_eq!(
                flat.numeric_f64_at(row),
                rle.numeric_f64_at(row),
                "numeric_f64_at({row}) [{label}]"
            );
            assert_eq!(flat.scalar(row), rle.scalar(row), "scalar({row}) [{label}]");
        }
    }
}
