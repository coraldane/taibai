use super::*;

struct OneBatchExec {
    batch: Option<RecordBatch>,
}

struct ManyBatchExec {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl ExecutionOp for OneBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

impl ExecutionOp for ManyBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

#[test]
fn grouped_aggregate_spills_and_merges_partial_states() {
    let schema = Schema::new(vec![
        ColumnDef::new("group_id", DataType::Int64),
        ColumnDef::new("value", DataType::Int64),
    ]);
    let mut groups = ColumnArray::empty(DataType::Int64);
    let mut values = ColumnArray::empty(DataType::Int64);
    for (group, value) in [(0, 10), (1, 20), (0, 30), (2, 40), (1, 20), (0, 10)] {
        groups.push(&ScalarValue::Int64(Some(group))).unwrap();
        values.push(&ScalarValue::Int64(Some(value))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![groups, values]).unwrap();
    let aggregates = vec![
        AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Wildcard),
            distinct: false,
        },
        AggregateExpr {
            func: AggFunc::Sum,
            arg: Box::new(LogicalExpr::Column(1)),
            distinct: false,
        },
        AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Column(1)),
            distinct: true,
        },
    ];
    let mut exec = HashAggregateExec::new(
        vec![LogicalExpr::Column(0)],
        aggregates,
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    )
    .with_memory_limit_bytes(1);

    let mut results = HashMap::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            results.insert(
                batch.scalar(row, 0).unwrap(),
                (
                    batch.scalar(row, 1).unwrap(),
                    batch.scalar(row, 2).unwrap(),
                    batch.scalar(row, 3).unwrap(),
                ),
            );
        }
    }

    assert_eq!(
        results.get(&ScalarValue::Int64(Some(0))),
        Some(&(
            ScalarValue::Int64(Some(3)),
            ScalarValue::Int64(Some(50)),
            ScalarValue::Int64(Some(2)),
        ))
    );
    assert_eq!(
        results.get(&ScalarValue::Int64(Some(1))),
        Some(&(
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(40)),
            ScalarValue::Int64(Some(1)),
        ))
    );
    assert_eq!(
        results.get(&ScalarValue::Int64(Some(2))),
        Some(&(
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(40)),
            ScalarValue::Int64(Some(1)),
        ))
    );
}

#[test]
fn global_distinct_aggregates_spill_and_deduplicate_across_runs() {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
    let batches = [[1, 2, 2], [2, 3, 1]]
        .into_iter()
        .map(|values| {
            let mut column = ColumnArray::empty(DataType::Int64);
            for value in values {
                column.push(&ScalarValue::Int64(Some(value))).unwrap();
            }
            RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
        })
        .collect::<Vec<_>>();
    let aggregates = [
        AggFunc::Count,
        AggFunc::Sum,
        AggFunc::Avg,
        AggFunc::Min,
        AggFunc::Max,
    ]
    .into_iter()
    .map(|func| AggregateExpr {
        func,
        arg: Box::new(LogicalExpr::Column(0)),
        distinct: true,
    })
    .collect();
    let mut exec = HashAggregateExec::new(
        Vec::new(),
        aggregates,
        Box::new(ManyBatchExec {
            batches: batches.into_iter(),
        }),
        schema,
    )
    .with_memory_limit_bytes(1);

    let output = exec.next_batch().unwrap().unwrap();
    assert_eq!(output.scalar(0, 0), Some(ScalarValue::Int64(Some(3))));
    assert_eq!(output.scalar(0, 1), Some(ScalarValue::Int64(Some(6))));
    assert_eq!(output.scalar(0, 2), Some(ScalarValue::Float64(Some(2.0))));
    assert_eq!(output.scalar(0, 3), Some(ScalarValue::Int64(Some(1))));
    assert_eq!(output.scalar(0, 4), Some(ScalarValue::Int64(Some(3))));
}

#[test]
fn grouped_distinct_aggregates_decode_distinct_values() {
    // Regression (P1.2): the GROUP BY path used to store DISTINCT values as
    // hex-encoded keys while finalize parsed them as JSON, so SUM/AVG/MIN/MAX
    // DISTINCT silently produced zeros/nulls per group. COUNT still worked,
    // which is why only the value-bearing functions were wrong.
    let schema = Schema::new(vec![
        ColumnDef::new("group_id", DataType::Int64),
        ColumnDef::new("value", DataType::Int64),
    ]);
    let mut groups = ColumnArray::empty(DataType::Int64);
    let mut values = ColumnArray::empty(DataType::Int64);
    for (group, value) in [(0, 10), (0, 10), (0, 20), (1, 5), (1, 7)] {
        groups.push(&ScalarValue::Int64(Some(group))).unwrap();
        values.push(&ScalarValue::Int64(Some(value))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![groups, values]).unwrap();
    let aggregates = [
        AggFunc::Sum,
        AggFunc::Avg,
        AggFunc::Min,
        AggFunc::Max,
        AggFunc::Count,
    ]
    .into_iter()
    .map(|func| AggregateExpr {
        func,
        arg: Box::new(LogicalExpr::Column(1)),
        distinct: true,
    })
    .collect();
    let mut exec = HashAggregateExec::new(
        vec![LogicalExpr::Column(0)],
        aggregates,
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    );

    let output = exec.next_batch().unwrap().unwrap();
    assert_eq!(output.num_rows(), 2);
    // Group 0 holds distinct values {10, 20}, group 1 holds {5, 7}.
    let expected = [
        [
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(30)),
            ScalarValue::Float64(Some(15.0)),
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(20)),
            ScalarValue::Int64(Some(2)),
        ],
        [
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(12)),
            ScalarValue::Float64(Some(6.0)),
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(7)),
            ScalarValue::Int64(Some(2)),
        ],
    ];
    for (row, tuple) in expected.iter().enumerate() {
        for (column, want) in tuple.iter().enumerate() {
            assert_eq!(
                output.scalar(row, column).unwrap(),
                want.clone(),
                "row {row} column {column}"
            );
        }
    }
}

#[test]
fn decode_key_scalar_round_trips_encode_key_at() {
    let cases: Vec<(DataType, Vec<ScalarValue>)> = vec![
        (
            DataType::Bool,
            vec![
                ScalarValue::Bool(Some(true)),
                ScalarValue::Bool(Some(false)),
            ],
        ),
        (
            DataType::Int8,
            vec![ScalarValue::Int8(Some(-3)), ScalarValue::Int8(Some(120))],
        ),
        (
            DataType::Int16,
            vec![
                ScalarValue::Int16(Some(-300)),
                ScalarValue::Int16(Some(7000)),
            ],
        ),
        (
            DataType::Int32,
            vec![
                ScalarValue::Int32(Some(-70000)),
                ScalarValue::Int32(Some(5)),
            ],
        ),
        (
            DataType::Int64,
            vec![
                ScalarValue::Int64(Some(-5_000_000_000)),
                ScalarValue::Int64(Some(42)),
            ],
        ),
        (DataType::UInt8, vec![ScalarValue::UInt8(Some(255))]),
        (
            DataType::UInt64,
            vec![ScalarValue::UInt64(Some(u64::MAX / 2))],
        ),
        (
            DataType::Float32,
            vec![
                ScalarValue::Float32(Some(-1.5)),
                ScalarValue::Float32(Some(0.25)),
            ],
        ),
        (
            DataType::Float64,
            vec![
                ScalarValue::Float64(Some(-2.75)),
                ScalarValue::Float64(Some(1e300)),
            ],
        ),
        (
            DataType::Decimal,
            vec![ScalarValue::Decimal(Some(-123456789))],
        ),
        (DataType::Date, vec![ScalarValue::Date(Some(19_500))]),
        (
            DataType::Timestamp,
            vec![ScalarValue::Timestamp(Some(1_700_000_000_123_456))],
        ),
        (
            DataType::VarChar,
            vec![
                ScalarValue::VarChar(Some("AAPL".into())),
                ScalarValue::VarChar(Some("带中文与空格 ".into())),
            ],
        ),
        (
            DataType::Blob,
            vec![ScalarValue::Blob(Some(vec![0u8, 1, 2, 255]))],
        ),
        (
            DataType::Vector,
            vec![ScalarValue::Vector(Some(vec![1.0f32, -2.5, 3.25]))],
        ),
    ];
    for (data_type, values) in cases {
        let mut column = ColumnArray::empty(data_type);
        for value in &values {
            column.push(value).unwrap();
        }
        for (row, value) in values.iter().enumerate() {
            let mut key = Vec::new();
            column.encode_key_at(row, &mut key);
            assert_eq!(
                decode_key_scalar(&key).as_ref(),
                Some(value),
                "round trip failed for {data_type:?}"
            );
        }
    }
}

#[test]
fn decode_key_scalar_round_trips_dictionary_varchar_like_plain() {
    let mut dict = crate::types::array::Utf8Buffer::new();
    dict.push_str("AAPL");
    dict.push_str("MSFT");
    let dict_col = ColumnArray::Dictionary {
        ids: vec![0, 1, 0],
        dict,
        bitmap: crate::types::bitmap::NullBitmap::new_valid(3),
        flat: std::sync::OnceLock::new(),
    };
    let mut plain = ColumnArray::empty(DataType::VarChar);
    for value in ["AAPL", "MSFT", "AAPL"] {
        plain
            .push(&ScalarValue::VarChar(Some(value.to_string())))
            .unwrap();
    }
    for row in 0..3 {
        let mut dict_key = Vec::new();
        dict_col.encode_key_at(row, &mut dict_key);
        let mut plain_key = Vec::new();
        plain.encode_key_at(row, &mut plain_key);
        // Dictionary and plain VarChar must produce byte-identical keys so
        // DISTINCT aggregation and GROUP BY treat both forms as one value.
        assert_eq!(
            dict_key, plain_key,
            "dictionary key differs from plain key at row {row}"
        );
        assert_eq!(
            decode_key_scalar(&dict_key),
            Some(plain.scalar(row)),
            "dictionary round trip failed at row {row}"
        );
    }
}

#[test]
fn grouped_spill_keeps_nan_and_null_group_keys_distinct() -> Result<()> {
    // The spill key used to be serialized through serde_json, which collapses
    // NaN to null: a NULL group and a NaN group merged into one row after a
    // spill round-trip. Typed spill keys keep them distinct.
    let schema = Schema::new(vec![
        ColumnDef::new("ratio", DataType::Float64),
        ColumnDef::new("value", DataType::Int64),
    ]);
    let mut ratios = ColumnArray::empty(DataType::Float64);
    let mut values = ColumnArray::empty(DataType::Int64);
    // NULL, NaN, 1.0 in repeating patterns so both spill runs and the
    // in-memory tail hold every key shape.
    for (ratio, value) in [
        (ScalarValue::Float64(None), 1),
        (ScalarValue::Float64(Some(f64::NAN)), 2),
        (ScalarValue::Float64(Some(1.0)), 3),
        (ScalarValue::Float64(None), 4),
        (ScalarValue::Float64(Some(f64::NAN)), 5),
        (ScalarValue::Float64(Some(1.0)), 6),
    ] {
        ratios.push(&ratio).unwrap();
        values.push(&ScalarValue::Int64(Some(value))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![ratios, values]).unwrap();
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    }];
    let mut exec = HashAggregateExec::new(
        vec![LogicalExpr::Column(0)],
        aggregates,
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    )
    .with_memory_limit_bytes(1);

    let mut counts: HashMap<Vec<u8>, (ScalarValue, u64)> = HashMap::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            let mut key_bytes = Vec::new();
            batch.column(0).unwrap().encode_key_at(row, &mut key_bytes);
            let ScalarValue::Int64(Some(count)) = batch.scalar(row, 1).unwrap() else {
                panic!("count must be an integer");
            };
            let entry = counts
                .entry(key_bytes)
                .or_insert((batch.scalar(row, 0).unwrap(), 0));
            entry.1 += count as u64;
        }
    }

    // Three distinct groups: NULL, NaN, 1.0 — never fewer.
    assert_eq!(counts.len(), 3, "NULL and NaN keys must not merge");
    let mut saw_null = false;
    let mut saw_nan = false;
    for (key, total) in counts.values() {
        match key {
            ScalarValue::Float64(None) => {
                saw_null = true;
                assert_eq!(*total, 2);
            }
            ScalarValue::Float64(Some(v)) if v.is_nan() => {
                saw_nan = true;
                assert_eq!(*total, 2);
            }
            ScalarValue::Float64(Some(v)) if *v == 1.0 => assert_eq!(*total, 2),
            other => panic!("unexpected group key {other:?}"),
        }
    }
    assert!(saw_null && saw_nan, "both special keys must be present");
    Ok(())
}

#[test]
fn grouped_sum_overflow_errors_instead_of_wrapping() {
    // 8192 rows x 2e15 overflow i64 in total while each lane's slice (at
    // most 4096 rows) stays below the limit, so the overflow surfaces
    // either in the per-lane checked adds or — the case this guards — in
    // the slot-table merge, which used to wrap silently instead of erroring
    // like the generic path does.
    let schema = Schema::new(vec![
        ColumnDef::new("g", DataType::VarChar),
        ColumnDef::new("v", DataType::Int64),
    ]);
    let mut groups = ColumnArray::empty(DataType::VarChar);
    let mut values = ColumnArray::empty(DataType::Int64);
    for _ in 0..8192 {
        groups
            .push(&ScalarValue::VarChar(Some("a".to_string())))
            .unwrap();
        values
            .push(&ScalarValue::Int64(Some(2_000_000_000_000_000)))
            .unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![groups, values]).unwrap();
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Sum,
        arg: Box::new(LogicalExpr::Column(1)),
        distinct: false,
    }];
    let mut exec = HashAggregateExec::new(
        vec![LogicalExpr::Column(0)],
        aggregates,
        Box::new(OneBatchExec { batch: Some(batch) }),
        schema,
    );
    let error = exec.next_batch().unwrap_err().to_string();
    assert!(error.contains("SUM overflow"), "got: {error}");
}
