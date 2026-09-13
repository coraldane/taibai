use super::*;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

/// The typed kernel must produce exactly the same key as the scalar-based
/// original for every value, function, and column type it handles.
fn assert_matches_scalar(column: &ColumnArray, func: PartitionFunction) {
    for row in 0..column.len() {
        let typed = compute_partition_key_at(&func, column, row);
        let scalar = compute_partition_key_from_scalar(&func, &column.scalar(row));
        assert_eq!(
            typed,
            scalar,
            "key mismatch at row {row} for func {func:?} on {}",
            column.data_type()
        );
    }
}

#[test]
fn partition_key_at_matches_scalar() {
    let funcs = [
        PartitionFunction::Day,
        PartitionFunction::Month,
        PartitionFunction::Year,
        PartitionFunction::Value,
        PartitionFunction::Hash(4),
        PartitionFunction::Mod(7),
    ];

    let ts = vec![0i64, 86_400_000_000, 1_700_000_000_000_000, -5];
    let mut ts_col = ColumnArray::Timestamp {
        data: ts.clone(),
        bitmap: NullBitmap::new_valid(ts.len()),
    };
    ts_col.bitmap_mut().set_null(3);
    let func_list = funcs;
    for func in func_list {
        assert_matches_scalar(&ts_col, func);
    }

    let dates = vec![0i32, 19_766, 19_000];
    let mut date_col = ColumnArray::Date {
        data: dates,
        bitmap: NullBitmap::new_valid(3),
    };
    date_col.bitmap_mut().set_null(2);
    for func in funcs {
        assert_matches_scalar(&date_col, func);
    }

    let ints = vec![0i64, 5, -9, 20_240_105, 1_700_000_000_000];
    let mut int_col = ColumnArray::Int64 {
        data: ints,
        bitmap: NullBitmap::new_valid(5),
    };
    int_col.bitmap_mut().set_null(2);
    for func in funcs {
        assert_matches_scalar(&int_col, func);
    }

    let ints32 = vec![7i32, -3, 20_240_105];
    let int32_col = ColumnArray::Int32 {
        data: ints32,
        bitmap: NullBitmap::new_valid(3),
    };
    for func in funcs {
        assert_matches_scalar(&int32_col, func);
    }

    let u32s = vec![1u32, 9, 4_000_000_000];
    let mut u32_col = ColumnArray::UInt32 {
        data: u32s,
        bitmap: NullBitmap::new_valid(3),
    };
    u32_col.bitmap_mut().set_null(1);
    for func in funcs {
        assert_matches_scalar(&u32_col, func);
    }

    let u64s = vec![1u64, 12, u64::MAX];
    let mut u64_col = ColumnArray::UInt64 {
        data: u64s,
        bitmap: NullBitmap::new_valid(3),
    };
    u64_col.bitmap_mut().set_null(2);
    for func in funcs {
        assert_matches_scalar(&u64_col, func);
    }

    let times = vec![3_600_000_000i64, 7200];
    let mut time_col = ColumnArray::Time {
        data: times,
        bitmap: NullBitmap::new_valid(2),
    };
    time_col.bitmap_mut().set_null(1);
    for func in funcs {
        assert_matches_scalar(&time_col, func);
    }

    let strings = vec!["2024-01-05", "", "abc", "42", "  7  "];
    let mut str_col = ColumnArray::VarChar {
        data: crate::types::array::Utf8Buffer::new(),
        bitmap: NullBitmap::new_valid(strings.len()),
    };
    for s in &strings {
        str_col
            .push(&crate::types::scalar::ScalarValue::VarChar(Some(
                (*s).to_string(),
            )))
            .unwrap();
    }
    str_col.bitmap_mut().set_null(1);
    for func in funcs {
        assert_matches_scalar(&str_col, func);
    }

    // Float/Bool/Decimal columns take the scalar fallback inside the typed
    // kernel; they must still agree with the original.
    let floats = vec![1.5f64, -2.25];
    let float_col = ColumnArray::Float64 {
        data: floats,
        bitmap: NullBitmap::new_valid(2),
    };
    for func in funcs {
        assert_matches_scalar(&float_col, func);
    }

    let bools = vec![1u8, 0];
    let bool_col = ColumnArray::Bool {
        data: bools,
        bitmap: NullBitmap::new_valid(2),
    };
    for func in funcs {
        assert_matches_scalar(&bool_col, func);
    }
}

#[test]
fn value_text_at_matches_to_text() {
    let mut ts_col = ColumnArray::Timestamp {
        data: vec![1i64, -2],
        bitmap: NullBitmap::new_valid(2),
    };
    ts_col.bitmap_mut().set_null(1);
    for row in 0..2 {
        let scalar = ts_col.scalar(row);
        assert_eq!(
            value_text_at(&ts_col, row),
            scalar.to_text().unwrap_or_default()
        );
    }

    let mut str_col = ColumnArray::VarChar {
        data: crate::types::array::Utf8Buffer::new(),
        bitmap: NullBitmap::new_valid(3),
    };
    str_col
        .push(&crate::types::scalar::ScalarValue::VarChar(Some(
            "hello".to_string(),
        )))
        .unwrap();
    str_col
        .push(&crate::types::scalar::ScalarValue::VarChar(Some(
            String::new(),
        )))
        .unwrap();
    str_col.bitmap_mut().set_null(2);
    for row in 0..3 {
        let scalar = str_col.scalar(row);
        assert_eq!(
            value_text_at(&str_col, row),
            scalar.to_text().unwrap_or_default()
        );
    }
}

#[test]
fn partition_keys_at_memo_matches_scalar() {
    let schema = Schema::new(vec![ColumnDef::new("ts", DataType::Timestamp)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![ColumnArray::Timestamp {
            data: vec![0, 1, 1, 2, 2, 2],
            bitmap: NullBitmap::new_valid(6),
        }],
    )
    .unwrap();
    let column = batch.column(0).unwrap();
    let mut computer =
        PartitionKeysAt::new(PartitionKeyKind::Strategy(PartitionFunction::Day), column);
    for row in 0..batch.num_rows() {
        let typed = computer.key_at(row);
        let scalar =
            compute_partition_key_from_scalar(&PartitionFunction::Day, &column.scalar(row));
        assert_eq!(typed, scalar, "memoized key mismatch at row {row}");
    }
}
