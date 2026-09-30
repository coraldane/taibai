use super::*;
use crate::protocol::types::DataType;
use crate::sql::planner::merge_schemas;
use crate::types::batch::ColumnDef;

struct OneBatchExec(Option<RecordBatch>);

impl ExecutionOp for OneBatchExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.0.take())
    }
}

fn int_batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    let mut column = ColumnArray::empty(DataType::Int64);
    for value in values {
        column.push(&ScalarValue::Int64(Some(*value))).unwrap();
    }
    RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
}

#[test]
fn inner_join_can_build_smaller_right_side_without_reordering_output() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_id", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_id", DataType::Int64)]);
    let condition = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Column(1)),
    };
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(int_batch(&left_schema, &[1, 2, 3])))),
        Box::new(OneBatchExec(Some(int_batch(&right_schema, &[2])))),
        JoinType::Inner,
        condition,
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    )
    .with_inner_build_left(false);

    let output = join.next_batch().unwrap().unwrap();
    assert_eq!(output.schema.columns[0].name, "left_id");
    assert_eq!(output.schema.columns[1].name, "right_id");
    assert_eq!(output.scalar(0, 0), Some(ScalarValue::Int64(Some(2))));
    assert_eq!(output.scalar(0, 1), Some(ScalarValue::Int64(Some(2))));
    assert!(join.next_batch().unwrap().is_none());
}

#[test]
fn inner_join_spills_partitions_and_preserves_duplicate_matches() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_id", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_id", DataType::Int64)]);
    let condition = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Column(1)),
    };
    for build_left in [true, false] {
        let mut join = HashJoinExec::new(
            Box::new(OneBatchExec(Some(int_batch(&left_schema, &[1, 2, 2, 3])))),
            Box::new(OneBatchExec(Some(int_batch(&right_schema, &[2, 2, 4])))),
            JoinType::Inner,
            condition.clone(),
            left_schema.clone(),
            right_schema.clone(),
            merge_schemas(&left_schema, &right_schema),
        )
        .with_inner_build_left(build_left)
        .with_memory_limit_bytes(1);

        let mut rows = Vec::new();
        while let Some(batch) = join.next_batch().unwrap() {
            for row in 0..batch.num_rows() {
                rows.push((batch.scalar(row, 0), batch.scalar(row, 1)));
            }
        }
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|(left, right)| {
            *left == Some(ScalarValue::Int64(Some(2)))
                && *right == Some(ScalarValue::Int64(Some(2)))
        }));
    }
}

#[test]
fn inner_join_residual_like_between_in_conditions_match_rows() {
    // Residual non-equi conditions used to fall into eval_join_expr's NULL
    // arm, which join_rows_match_expr treats as "no match" — silently
    // dropping every joined row for ON clauses like
    // `a.id = b.id AND a.name LIKE 'a%' AND a.id BETWEEN 1 AND 5`.
    let left_schema = Schema::new(vec![
        ColumnDef::new("left_id", DataType::Int64),
        ColumnDef::new("left_name", DataType::VarChar),
    ]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_id", DataType::Int64)]);
    let mut ids = ColumnArray::empty(DataType::Int64);
    let mut names = ColumnArray::empty(DataType::VarChar);
    for (id, name) in [(1i64, "apple"), (2, "banana"), (7, "apricot")] {
        ids.push(&ScalarValue::Int64(Some(id))).unwrap();
        names
            .push(&ScalarValue::VarChar(Some(name.to_string())))
            .unwrap();
    }
    let left = RecordBatch::try_new(left_schema.clone(), vec![ids, names]).unwrap();
    let right = int_batch(&right_schema, &[1, 2, 7]);

    let condition = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(2)),
        }),
        op: BinOp::And,
        right: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Like {
                expr: Box::new(LogicalExpr::Column(1)),
                pattern: "a%".to_string(),
                negated: false,
            }),
            op: BinOp::And,
            right: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Between {
                    expr: Box::new(LogicalExpr::Column(0)),
                    low: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
                    high: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(5)))),
                    negated: false,
                }),
                op: BinOp::And,
                right: Box::new(LogicalExpr::In {
                    expr: Box::new(LogicalExpr::Column(0)),
                    values: vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(7))],
                    negated: false,
                }),
            }),
        }),
    };

    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(left))),
        Box::new(OneBatchExec(Some(right))),
        JoinType::Inner,
        condition,
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    )
    .with_inner_build_left(false);

    let output = join.next_batch().unwrap().expect("exactly one match");
    // (1, "apple") passes LIKE + BETWEEN + IN; (2, "banana") fails all three;
    // (7, "apricot") passes LIKE and IN but fails BETWEEN.
    assert_eq!(
        output.num_rows(),
        1,
        "only row id=1 satisfies all residuals"
    );
    assert_eq!(output.scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
    assert!(join.next_batch().unwrap().is_none());
}
