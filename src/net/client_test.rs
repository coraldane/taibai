use super::*;

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

fn int64_batch(name: &str, values: std::ops::Range<i64>) -> RecordBatch {
    let schema = Schema::new(vec![ColumnDef::new(name, DataType::Int64)]);
    let mut batch = RecordBatch::new(schema);
    for value in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value)))
            .unwrap();
    }
    batch
}

#[test]
fn result_chunks_merge_and_enforce_schema_stability() {
    let mut stream_schema = None;
    let mut batches = Vec::new();

    let first = Message::new(
        MSG_TYPE_RESULT_CHUNK,
        Bytes::from(crate::protocol::result::encode_binary_result(&int64_batch(
            "id",
            0..3,
        ))),
    );
    merge_result_chunk(&first, &mut stream_schema, &mut batches).unwrap();

    let second = Message::new(
        MSG_TYPE_RESULT_CHUNK,
        Bytes::from(crate::protocol::result::encode_binary_result(&int64_batch(
            "id",
            3..5,
        ))),
    );
    merge_result_chunk(&second, &mut stream_schema, &mut batches).unwrap();

    let schema = stream_schema.clone().expect("first chunk sets the schema");
    assert_eq!(schema.columns[0].name, "id");
    let total: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total, 5);

    let renamed = Message::new(
        MSG_TYPE_RESULT_CHUNK,
        Bytes::from(crate::protocol::result::encode_binary_result(&int64_batch(
            "amount",
            5..6,
        ))),
    );
    let error = merge_result_chunk(&renamed, &mut stream_schema, &mut batches)
        .expect_err("a schema change mid-stream must fail");
    assert!(error.to_string().contains("schema changed"), "{error}");

    let garbage = Message::new(MSG_TYPE_RESULT_CHUNK, Bytes::from_static(b"not-a-result"));
    assert!(
        merge_result_chunk(&garbage, &mut stream_schema, &mut batches).is_err(),
        "a non-rows chunk payload must fail instead of silently merging"
    );
}

/// A zero-row SELECT arrives as one schema-only chunk (the server sends no
/// row chunks at all when nothing matched). The merge must keep the column
/// metadata, otherwise the caller sees a result set with zero columns.
#[test]
fn zero_row_chunk_preserves_schema() {
    let mut stream_schema = None;
    let mut batches = Vec::new();
    let empty = RecordBatch::new(Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("label", DataType::VarChar),
    ]));
    let chunk = Message::new(
        MSG_TYPE_RESULT_CHUNK,
        Bytes::from(crate::protocol::result::encode_binary_result(&empty)),
    );

    merge_result_chunk(&chunk, &mut stream_schema, &mut batches).unwrap();

    let schema = stream_schema.expect("a schema-only chunk must still set the schema");
    let names: Vec<&str> = schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "label"]);
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
}
