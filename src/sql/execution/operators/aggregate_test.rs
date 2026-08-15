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
