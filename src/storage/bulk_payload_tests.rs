use super::*;

#[test]
fn parses_u8_decimal_meta_payload() {
    let mut payload = Vec::new();
    write_header(&mut payload, "test_table", 2, 1);
    write_column_header(&mut payload, "amount", DataType::Decimal, 2, 9, 2, 0);
    payload.extend_from_slice(&1234i64.to_le_bytes());
    payload.extend_from_slice(&5678i64.to_le_bytes());

    let req = parse_direct_bulk_payload(&payload).unwrap();

    assert_eq!(req.table_name, "test_table");
    assert_eq!(req.row_count, 2);
    assert_eq!(req.columns.len(), 1);
    assert_eq!(req.columns[0].scale, 2);
    assert_eq!(req.columns[0].precision, 9);
    assert_eq!(req.columns[0].data.len(), 16);
}

#[test]
fn rejects_trailing_bytes() {
    let mut payload = Vec::new();
    write_header(&mut payload, "test_table", 0, 0);
    payload.push(0);

    assert!(parse_direct_bulk_payload(&payload).is_err());
}

#[test]
fn parses_canonical_null_bitmap() {
    let mut payload = Vec::new();
    write_header(&mut payload, "test_table", 3, 1);
    write_column_header(&mut payload, "value", DataType::Int32, 0, 0, 3, 8);
    payload.extend_from_slice(&10i32.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&30i32.to_le_bytes());
    payload.extend_from_slice(&2u64.to_le_bytes());

    let req = parse_direct_bulk_payload(&payload).unwrap();

    assert!(!req.columns[0].is_null(0));
    assert!(req.columns[0].is_null(1));
    assert!(!req.columns[0].is_null(2));
}

#[test]
fn rejects_truncated_null_bitmap() {
    let mut payload = Vec::new();
    write_header(&mut payload, "test_table", 3, 1);
    write_column_header(&mut payload, "value", DataType::Int32, 0, 0, 3, 1);
    payload.extend_from_slice(&10i32.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&30i32.to_le_bytes());
    payload.push(2);

    let err = match parse_direct_bulk_payload(&payload) {
        Ok(_) => panic!("expected truncated null bitmap to fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("null bitmap size mismatch"));
}

#[test]
fn rejects_null_bitmap_bits_beyond_row_count() {
    let mut payload = Vec::new();
    write_header(&mut payload, "test_table", 3, 1);
    write_column_header(&mut payload, "value", DataType::Int32, 0, 0, 3, 8);
    payload.extend_from_slice(&10i32.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&30i32.to_le_bytes());
    payload.extend_from_slice(&(1u64 << 3).to_le_bytes());

    let err = match parse_direct_bulk_payload(&payload) {
        Ok(_) => panic!("expected out-of-range null bit to fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("bits set beyond"));
}

fn write_header(buf: &mut Vec<u8>, table: &str, rows: u32, cols: u32) {
    buf.extend_from_slice(&(table.len() as u16).to_le_bytes());
    buf.extend_from_slice(table.as_bytes());
    buf.extend_from_slice(&rows.to_le_bytes());
    buf.extend_from_slice(&cols.to_le_bytes());
}

fn write_column_header(
    buf: &mut Vec<u8>,
    name: &str,
    data_type: DataType,
    scale: u8,
    precision: u8,
    elem_count: u32,
    nulls_size: u32,
) {
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf.push(data_type as u8);
    buf.push(scale);
    buf.push(precision);
    buf.extend_from_slice(&elem_count.to_le_bytes());
    buf.extend_from_slice(&nulls_size.to_le_bytes());
}
