use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;
use crate::types::batch::ColumnDef;

struct ManyBatchExec {
    batches: std::vec::IntoIter<RecordBatch>,
}

impl ExecutionOp for ManyBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.next())
    }
}

fn int_column(values: &[i64]) -> ColumnArray {
    let mut column = ColumnArray::empty(DataType::Int64);
    for value in values {
        column.push(&ScalarValue::Int64(Some(*value))).unwrap();
    }
    column
}

fn int_batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(schema.clone(), vec![int_column(values)]).unwrap()
}

fn collect(exec: &mut dyn ExecutionOp) -> Vec<i64> {
    let mut values = Vec::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            match batch.scalar(row, 0).unwrap() {
                ScalarValue::Int64(Some(value)) => values.push(value),
                other => panic!("unexpected scalar: {other:?}"),
            }
        }
    }
    values
}

#[test]
fn topn_keeps_smallest_k_from_unordered_input() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = int_batch(&schema, &[9, 1, 7, 3, 5, 2, 8, 6, 4, 0]);
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(3);
    assert_eq!(collect(&mut exec), vec![0, 1, 2]);
}

#[test]
fn topn_keeps_largest_k_descending() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = int_batch(&schema, &[9, 1, 7, 3, 5, 2, 8, 6, 4, 0]);
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: true,
        }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(3);
    assert_eq!(collect(&mut exec), vec![9, 8, 7]);
}

#[test]
fn topn_ties_break_by_input_order() {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            int_column(&[10, 11, 12, 13, 14]),
            int_column(&[5, 1, 5, 1, 5]),
        ],
    )
    .unwrap();
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(1),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(2);
    assert_eq!(collect(&mut exec), vec![11, 13]);
}

#[test]
fn topn_worst_case_stream_exercises_compaction() {
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let batches = (0..7)
        .map(|chunk| {
            let start = 224 - chunk * 32;
            let values = (0..32).map(|offset| start - offset).collect::<Vec<_>>();
            int_batch(&schema, &values)
        })
        .collect::<Vec<_>>();
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: batches.into_iter(),
        }),
        schema,
    )
    .with_capacity(10);
    assert_eq!(collect(&mut exec), (1..=10).collect::<Vec<_>>());
}

#[test]
fn topn_matches_full_sort_reference() {
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut state = 88172645463325252u64;
    let mut all = Vec::new();
    for _ in 0..500 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        all.push((state >> 33) as i64 % 1000);
    }
    let batches = all
        .chunks(71)
        .map(|chunk| int_batch(&schema, chunk))
        .collect::<Vec<_>>();
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: batches.into_iter(),
        }),
        schema,
    )
    .with_capacity(7);
    let mut expected = all.clone();
    expected.sort_unstable();
    expected.truncate(7);
    assert_eq!(collect(&mut exec), expected);
}

#[test]
fn topn_expression_sort_key() {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("x", DataType::Float64),
        ColumnDef::new("y", DataType::Float64),
    ]);
    let mut id = ColumnArray::empty(DataType::Int64);
    let mut x = ColumnArray::empty(DataType::Float64);
    let mut y = ColumnArray::empty(DataType::Float64);
    for (i, c, p) in [(3, 11.0, 10.0), (1, 20.0, 10.0), (2, 9.0, 10.0)] {
        id.push(&ScalarValue::Int64(Some(i))).unwrap();
        x.push(&ScalarValue::Float64(Some(c))).unwrap();
        y.push(&ScalarValue::Float64(Some(p))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![id, x, y]).unwrap();
    let expr = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(1)),
            op: BinOp::Sub,
            right: Box::new(LogicalExpr::Column(2)),
        }),
        op: BinOp::Div,
        right: Box::new(LogicalExpr::Column(2)),
    };
    let mut exec = TopNExec::new(
        vec![SortKey { expr, desc: true }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(2);
    let result = exec.next_batch().unwrap().unwrap();
    assert_eq!(result.scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
    assert_eq!(result.scalar(1, 0), Some(ScalarValue::Int64(Some(3))));
}

#[test]
fn topn_empty_input_returns_none() {
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: vec![].into_iter(),
        }),
        schema,
    )
    .with_capacity(4);
    assert!(exec.next_batch().unwrap().is_none());
    assert!(exec.next_batch().unwrap().is_none());
}

#[test]
fn topn_zero_capacity_returns_none() {
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let batch = int_batch(&schema, &[1, 2, 3]);
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(0);
    assert!(exec.next_batch().unwrap().is_none());
}

#[test]
fn topn_nulls_sort_last_ascending() {
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut column = ColumnArray::empty(DataType::Int64);
    for value in [
        ScalarValue::Int64(Some(5)),
        ScalarValue::Int64(None),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Int64(None),
        ScalarValue::Int64(Some(9)),
    ] {
        column.push(&value).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    let mut exec = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        Box::new(ManyBatchExec {
            batches: vec![batch].into_iter(),
        }),
        schema,
    )
    .with_capacity(4);
    let mut values = Vec::new();
    while let Some(batch) = exec.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            match batch.scalar(row, 0).unwrap() {
                ScalarValue::Int64(Some(value)) => values.push(value),
                ScalarValue::Int64(None) => values.push(0),
                other => panic!("unexpected scalar: {other:?}"),
            }
        }
    }
    assert_eq!(values, vec![2, 5, 9, 0]);
}
