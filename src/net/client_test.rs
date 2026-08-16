use super::*;
use crate::protocol::message::write_len_prefixed_string;

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
