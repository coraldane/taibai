use super::*;
use crate::protocol::message::write_len_prefixed_string;

#[test]
fn parses_scaled_decimal_without_misaligning_following_varchar() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&2u32.to_le_bytes());
    write_column_meta(&mut payload, "name", DataType::VarChar, 0, 0);
    write_column_meta(&mut payload, "score", DataType::Decimal, 2, 9);
    payload.extend_from_slice(&2u32.to_le_bytes());

    write_varchar_cell(&mut payload, "alice");
    write_decimal_cell(&mut payload, "12.34");
    write_varchar_cell(&mut payload, "dave");
    write_decimal_cell(&mut payload, "0");

    let result = parse_text_result(&payload).unwrap();
    let QueryResult::Rows {
        schema, batches, ..
    } = result
    else {
        panic!("expected row result");
    };

    assert_eq!(schema.columns[1].scale, 2);
    assert_eq!(schema.columns[1].precision, 9);
    assert_eq!(
        batches[0].scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("alice".into()))
    );
    assert_eq!(
        batches[0].scalar(0, 1).unwrap(),
        ScalarValue::Decimal(Some(1234))
    );
    assert_eq!(
        batches[0].scalar(1, 0).unwrap(),
        ScalarValue::VarChar(Some("dave".into()))
    );
    assert_eq!(
        batches[0].scalar(1, 1).unwrap(),
        ScalarValue::Decimal(Some(0))
    );
}

#[test]
fn parses_scaled_decimal_text_values() {
    assert_eq!(parse_scaled_decimal_text("123", 2).unwrap(), 12300);
    assert_eq!(parse_scaled_decimal_text("123.4", 2).unwrap(), 12340);
    assert_eq!(parse_scaled_decimal_text("-0.23", 2).unwrap(), -23);
    assert!(parse_scaled_decimal_text("1.234", 2).is_err());
}

#[test]
fn parses_binary_decimal_result() {
    let schema = Schema::new(vec![
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("amount", DataType::Decimal)
            .with_scale(2)
            .with_precision(9),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("row-a".into())))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::Decimal(Some(12345)))
        .unwrap();

    let payload = crate::protocol::result::encode_binary_result(&batch);
    let QueryResult::Rows {
        schema: parsed_schema,
        batches,
        ..
    } = parse_binary_result(&payload).unwrap()
    else {
        panic!("expected rows");
    };

    assert_eq!(parsed_schema.columns[1].scale, 2);
    assert_eq!(parsed_schema.columns[1].precision, 9);
    assert_eq!(
        batches[0].scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("row-a".into()))
    );
    assert_eq!(
        batches[0].scalar(0, 1).unwrap(),
        ScalarValue::Decimal(Some(12345))
    );
}

fn write_column_meta(
    buf: &mut Vec<u8>,
    name: &str,
    data_type: DataType,
    scale: i32,
    precision: i32,
) {
    write_len_prefixed_string(buf, name);
    buf.push(data_type as u8);
    buf.push(scale as u8);
    buf.push(precision as u8);
}

fn write_varchar_cell(buf: &mut Vec<u8>, value: &str) {
    buf.push(0);
    write_len_prefixed_string(buf, value);
}

fn write_decimal_cell(buf: &mut Vec<u8>, value: &str) {
    buf.push(0);
    write_len_prefixed_string(buf, value);
}
