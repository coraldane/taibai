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

fn two_int_col_batch(schema: &Schema, first: &[i64], second: &[i64]) -> RecordBatch {
    let mut col0 = ColumnArray::empty(DataType::Int64);
    let mut col1 = ColumnArray::empty(DataType::Int64);
    for (a, b) in first.iter().zip(second.iter()) {
        col0.push(&ScalarValue::Int64(Some(*a))).unwrap();
        col1.push(&ScalarValue::Int64(Some(*b))).unwrap();
    }
    RecordBatch::try_new(schema.clone(), vec![col0, col1]).unwrap()
}

/// `l.k = r.k AND l.k >= r.k` — trivially true for equal keys, but the
/// residual comparison keeps `condition_is_pure_eq()` false so the join
/// runs the scalar probe path instead of the fast take-based path.
fn eq_with_residual_condition() -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(1)),
        }),
        op: BinOp::And,
        right: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Gte,
            right: Box::new(LogicalExpr::Column(1)),
        }),
    }
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

/// One probe row hitting 10k same-key build rows on the scalar path used
/// to box all 10k matches into `output_buffer` before the first drain;
/// the boxing loop now drains every BATCH_SIZE rows and resumes the rest
/// via the pending-probe state. Row content, count and per-batch sizes
/// must stay exactly what the single-buffer drain produced.
#[test]
fn inner_join_scalar_path_drains_skewed_key_in_chunks() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_k", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_k", DataType::Int64)]);
    for build_left in [true, false] {
        // Keep the skew on the BUILD side whichever side builds: one
        // probe row whose key hits every one of the 10k build rows.
        let (left_rows, right_rows): (&[i64], &[i64]) = if build_left {
            (&vec![7i64; 10_000], &[7])
        } else {
            (&[7], &vec![7i64; 10_000])
        };
        let mut join = HashJoinExec::new(
            Box::new(OneBatchExec(Some(int_batch(&left_schema, left_rows)))),
            Box::new(OneBatchExec(Some(int_batch(&right_schema, right_rows)))),
            JoinType::Inner,
            eq_with_residual_condition(),
            left_schema.clone(),
            right_schema.clone(),
            merge_schemas(&left_schema, &right_schema),
        )
        .with_inner_build_left(build_left);

        let bs = HashJoinExec::BATCH_SIZE;
        let mut batch_sizes = Vec::new();
        let mut total = 0usize;
        while let Some(batch) = join.next_batch().unwrap() {
            for row in 0..batch.num_rows() {
                assert_eq!(batch.scalar(row, 0), Some(ScalarValue::Int64(Some(7))));
                assert_eq!(batch.scalar(row, 1), Some(ScalarValue::Int64(Some(7))));
            }
            total += batch.num_rows();
            batch_sizes.push(batch.num_rows());
        }
        assert_eq!(total, 10_000, "build_left={build_left}");
        assert_eq!(batch_sizes, vec![bs, bs, 10_000 - 2 * bs], "build_left={build_left}");
        assert!(batch_sizes.len() > 1, "skewed key must span multiple batches");
    }
}

/// SEMI emits each build row at most once. Three probe rows share one hot
/// key backed by 9k build rows, so the match list chunk-drains mid-row
/// and the mark-based dedup must survive the pause.
#[test]
fn semi_join_scalar_path_skewed_key_emits_build_rows_once_across_chunks() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_k", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_k", DataType::Int64)]);
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(int_batch(
            &left_schema,
            &vec![5i64; 9_000],
        )))),
        Box::new(OneBatchExec(Some(int_batch(&right_schema, &[5, 5, 99])))),
        JoinType::Semi,
        eq_with_residual_condition(),
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    );

    let bs = HashJoinExec::BATCH_SIZE;
    let mut batch_sizes = Vec::new();
    let mut total = 0usize;
    while let Some(batch) = join.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            assert_eq!(batch.scalar(row, 0), Some(ScalarValue::Int64(Some(5))));
            assert_eq!(batch.scalar(row, 1), Some(ScalarValue::Int64(None)));
        }
        total += batch.num_rows();
        batch_sizes.push(batch.num_rows());
    }
    assert_eq!(total, 9_000, "each of the 9k build rows exactly once");
    assert_eq!(batch_sizes, vec![bs, bs, 9_000 - 2 * bs]);
}

/// LEFT JOIN with a skewed key: one probe row matches 10k build rows
/// (drained in chunks), the never-probed build row null-extends through
/// the finalize stash afterwards.
#[test]
fn left_join_scalar_path_skewed_key_chunks_and_null_extends_leftovers() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_k", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_k", DataType::Int64)]);
    let mut left_values = vec![7i64; 10_000];
    left_values.push(42);
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(int_batch(&left_schema, &left_values)))),
        Box::new(OneBatchExec(Some(int_batch(&right_schema, &[7, 99])))),
        JoinType::Left,
        eq_with_residual_condition(),
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    );

    let bs = HashJoinExec::BATCH_SIZE;
    let mut batch_sizes = Vec::new();
    let mut total = 0usize;
    while let Some(batch) = join.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            let left = batch.scalar(row, 0);
            let right = batch.scalar(row, 1);
            if left == Some(ScalarValue::Int64(Some(42))) {
                // Unmatched build row: NULL right side, emitted by the
                // finalize path after all matched chunks.
                assert_eq!(right, Some(ScalarValue::Int64(None)));
            } else {
                assert_eq!(left, Some(ScalarValue::Int64(Some(7))));
                assert_eq!(right, Some(ScalarValue::Int64(Some(7))));
            }
        }
        total += batch.num_rows();
        batch_sizes.push(batch.num_rows());
    }
    // 10k matched rows + 1 null-extended unmatched build row.
    assert_eq!(total, 10_001);
    assert_eq!(batch_sizes, vec![bs, bs, 10_000 - 2 * bs, 1]);
}

/// RIGHT JOIN builds from the right input: 10k same-key build rows are
/// matched by one probe row and drained in chunks, while the unmatched
/// build row null-extends at finalize.
#[test]
fn right_join_scalar_path_skewed_key_chunks_and_null_extends_build_side() {
    let left_schema = Schema::new(vec![ColumnDef::new("left_k", DataType::Int64)]);
    let right_schema = Schema::new(vec![ColumnDef::new("right_k", DataType::Int64)]);
    let mut right_values = vec![7i64; 10_000];
    right_values.push(42);
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(int_batch(&left_schema, &[7, 99])))),
        Box::new(OneBatchExec(Some(int_batch(&right_schema, &right_values)))),
        JoinType::Right,
        eq_with_residual_condition(),
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    );

    let bs = HashJoinExec::BATCH_SIZE;
    let mut batch_sizes = Vec::new();
    let mut total = 0usize;
    while let Some(batch) = join.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            let left = batch.scalar(row, 0);
            let right = batch.scalar(row, 1);
            if right == Some(ScalarValue::Int64(Some(42))) {
                assert_eq!(left, Some(ScalarValue::Int64(None)));
            } else {
                assert_eq!(left, Some(ScalarValue::Int64(Some(7))));
                assert_eq!(right, Some(ScalarValue::Int64(Some(7))));
            }
        }
        total += batch.num_rows();
        batch_sizes.push(batch.num_rows());
    }
    assert_eq!(total, 10_001);
    assert_eq!(batch_sizes, vec![bs, bs, 10_000 - 2 * bs, 1]);
}

/// FULL JOIN: the probe row's "did anything survive the residual" flag
/// must survive mid-row drains. The probe row (7, 5000) joins the 5k
/// build rows with v < 5000 — a match list that spans two output chunks —
/// and must NOT null-extend afterwards; the build rows it failed to join
/// null-extend from the left at finalize.
#[test]
fn full_join_scalar_path_resume_keeps_matched_any_across_chunks() {
    let left_schema = Schema::new(vec![
        ColumnDef::new("left_k", DataType::Int64),
        ColumnDef::new("left_v", DataType::Int64),
    ]);
    let right_schema = Schema::new(vec![
        ColumnDef::new("right_k", DataType::Int64),
        ColumnDef::new("right_v", DataType::Int64),
    ]);
    // ON l.k = r.k AND l.v > r.v
    let condition = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(2)),
        }),
        op: BinOp::And,
        right: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(1)),
            op: BinOp::Gt,
            right: Box::new(LogicalExpr::Column(3)),
        }),
    };
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(two_int_col_batch(
            &left_schema,
            &[7, 8],
            &[5_000, 1],
        )))),
        Box::new(OneBatchExec(Some(two_int_col_batch(
            &right_schema,
            &vec![7i64; 10_000],
            &(0..10_000).collect::<Vec<_>>(),
        )))),
        JoinType::Full,
        condition,
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    );

    let bs = HashJoinExec::BATCH_SIZE;
    let mut batch_sizes = Vec::new();
    let (mut both, mut null_right, mut null_left) = (0usize, 0usize, 0usize);
    while let Some(batch) = join.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            let lk = batch.scalar(row, 0);
            let lv = batch.scalar(row, 1);
            let rk = batch.scalar(row, 2);
            let rv = batch.scalar(row, 3);
            let left_null = lk == Some(ScalarValue::Int64(None));
            let right_null = rk == Some(ScalarValue::Int64(None));
            match (left_null, right_null) {
                (false, false) => {
                    // Joined: left (7, 5000) with a build row v < 5000.
                    assert_eq!(lk, Some(ScalarValue::Int64(Some(7))));
                    assert_eq!(lv, Some(ScalarValue::Int64(Some(5_000))));
                    assert_eq!(rk, Some(ScalarValue::Int64(Some(7))));
                    let v = match rv {
                        Some(ScalarValue::Int64(Some(v))) => v,
                        other => panic!("joined row right_v must be non-null, got {other:?}"),
                    };
                    assert!((0..5_000).contains(&v));
                    both += 1;
                }
                (false, true) => {
                    // Key-unmatched probe row (8, 1): null-extended right.
                    assert_eq!(lk, Some(ScalarValue::Int64(Some(8))));
                    assert_eq!(lv, Some(ScalarValue::Int64(Some(1))));
                    null_right += 1;
                }
                (true, false) => {
                    // Build rows the residual rejected (v >= 5000).
                    assert_eq!(rk, Some(ScalarValue::Int64(Some(7))));
                    let v = match rv {
                        Some(ScalarValue::Int64(Some(v))) => v,
                        other => panic!("unmatched build right_v must be non-null, got {other:?}"),
                    };
                    assert!((5_000..10_000).contains(&v));
                    null_left += 1;
                }
                (true, true) => panic!("FULL join never emits an all-NULL row"),
            }
        }
        batch_sizes.push(batch.num_rows());
    }
    assert_eq!(both, 5_000);
    assert_eq!(null_right, 1, "probe row (7,5000) matched, must not null-extend");
    assert_eq!(null_left, 5_000);
    // 5000 joined rows in two chunks, the key-unmatched probe row, then
    // the 5000 residual-rejected build rows in two finalize chunks.
    assert_eq!(batch_sizes, vec![bs, 5_000 - bs, 1, bs, 5_000 - bs]);
}

/// FULL JOIN where NO match of a 10k-match skewed key survives the
/// residual: the probe row must null-extend exactly once, after the whole
/// match list was walked, and every build row null-extends from the left
/// through the chunked finalize.
#[test]
fn full_join_scalar_path_skewed_key_null_extends_when_no_match_survives() {
    let left_schema = Schema::new(vec![
        ColumnDef::new("left_k", DataType::Int64),
        ColumnDef::new("left_v", DataType::Int64),
    ]);
    let right_schema = Schema::new(vec![
        ColumnDef::new("right_k", DataType::Int64),
        ColumnDef::new("right_v", DataType::Int64),
    ]);
    // ON l.k = r.k AND l.v > r.v — v = -1 never beats v >= 0.
    let condition = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(2)),
        }),
        op: BinOp::And,
        right: Box::new(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(1)),
            op: BinOp::Gt,
            right: Box::new(LogicalExpr::Column(3)),
        }),
    };
    let mut join = HashJoinExec::new(
        Box::new(OneBatchExec(Some(two_int_col_batch(
            &left_schema,
            &[7],
            &[-1],
        )))),
        Box::new(OneBatchExec(Some(two_int_col_batch(
            &right_schema,
            &vec![7i64; 10_000],
            &(0..10_000).collect::<Vec<_>>(),
        )))),
        JoinType::Full,
        condition,
        left_schema.clone(),
        right_schema.clone(),
        merge_schemas(&left_schema, &right_schema),
    );

    let bs = HashJoinExec::BATCH_SIZE;
    let mut batch_sizes = Vec::new();
    let (mut null_right, mut null_left) = (0usize, 0usize);
    while let Some(batch) = join.next_batch().unwrap() {
        for row in 0..batch.num_rows() {
            let lk = batch.scalar(row, 0);
            let rk = batch.scalar(row, 2);
            if lk != Some(ScalarValue::Int64(None)) {
                assert_eq!(lk, Some(ScalarValue::Int64(Some(7))));
                assert_eq!(batch.scalar(row, 1), Some(ScalarValue::Int64(Some(-1))));
                assert_eq!(rk, Some(ScalarValue::Int64(None)));
                null_right += 1;
            } else {
                assert_eq!(rk, Some(ScalarValue::Int64(Some(7))));
                null_left += 1;
            }
        }
        batch_sizes.push(batch.num_rows());
    }
    assert_eq!(null_right, 1, "the unmatched probe row null-extends exactly once");
    assert_eq!(null_left, 10_000, "every build row null-extends");
    // 1 null-extended probe row, then 10k unmatched build rows chunked.
    assert_eq!(batch_sizes, vec![1, bs, bs, 10_000 - 2 * bs]);
}
