use super::*;

fn col_def(data_type: DataType) -> ColumnDef {
    ColumnDef::new("c", data_type)
}

/// Full column block (`[bitmap_len][bitmap][values]`) for the plain,
/// non-adaptive decode paths.
fn raw_block(bitmap: &NullBitmap, values: &[u8]) -> Vec<u8> {
    let bitmap_bytes = bitmap.to_bytes();
    let mut raw = Vec::with_capacity(4 + bitmap_bytes.len() + values.len());
    raw.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    raw.extend_from_slice(&bitmap_bytes);
    raw.extend_from_slice(values);
    raw
}

#[test]
fn count_eq_fixed_truncated_payload_is_error_not_partial_count() {
    // Regression: the fixed-width count kernels used to stop at the
    // truncation ("break") and return a partial count instead of
    // surfacing the corruption like the selected_eq_rows_* family.
    let rows = 4usize;
    let mut payload = Vec::new();
    for v in [7i64, 7, 9, 7] {
        payload.extend_from_slice(&v.to_le_bytes());
    }
    let bitmap = NullBitmap::new_valid(rows);
    let def = col_def(DataType::Int64);
    let target = ScalarValue::Int64(Some(7));

    let counted = count_eq_from_raw(&raw_block(&bitmap, &payload), rows, &def, &target).unwrap();
    assert_eq!(counted, 3);

    // Drop the last row's final bytes: the count must fail, not report 2.
    let truncated = &payload[..payload.len() - 4];
    assert!(count_eq_from_raw(&raw_block(&bitmap, truncated), rows, &def, &target).is_err());
}

#[test]
fn count_eq_varchar_truncated_payload_is_error_not_partial_count() {
    let rows = 3usize;
    let mut payload = Vec::new();
    for s in ["ab", "ab", "cd"] {
        payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
        payload.extend_from_slice(s.as_bytes());
    }
    let bitmap = NullBitmap::new_valid(rows);
    let def = col_def(DataType::VarChar);
    let target = ScalarValue::VarChar(Some("ab".into()));

    let counted = count_eq_from_raw(&raw_block(&bitmap, &payload), rows, &def, &target).unwrap();
    assert_eq!(counted, 2);

    // Cut into the last value: must fail, not return a partial count.
    let truncated = &payload[..payload.len() - 1];
    assert!(count_eq_from_raw(&raw_block(&bitmap, truncated), rows, &def, &target).is_err());
}

#[test]
fn count_eq_varchar_dictionary_truncated_index_is_error_not_mismatch() {
    // Regression: a truncated dictionary index stream used to read as
    // u32::MAX and silently mismatch instead of surfacing the corruption.
    let rows = 2usize;
    let mut values = Vec::new();
    values.extend_from_slice(COLUMN_ENCODING_MAGIC);
    values.push(crate::storage::codec::encoding::EncodingType::Dictionary.to_u8());
    values.extend_from_slice(&1u32.to_le_bytes()); // dict_count
    values.extend_from_slice(&2u32.to_le_bytes()); // "ab"
    values.extend_from_slice(b"ab");
    values.extend_from_slice(&2u32.to_le_bytes()); // index_count
    values.extend_from_slice(&0u32.to_le_bytes()); // row 0's index
                                                   // Row 1's index bytes are missing.

    let bitmap = NullBitmap::new_valid(rows);
    let def = col_def(DataType::VarChar);
    assert!(count_eq_from_raw(
        &raw_block(&bitmap, &values),
        rows,
        &def,
        &ScalarValue::VarChar(Some("ab".into()))
    )
    .is_err());
}
