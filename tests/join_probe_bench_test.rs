//! Diagnostic benchmark for the hash join probe path (roadmap step 1,
//! phase P1.3).
//!
//! Run with:
//! ```text
//! cargo test --test join_probe_bench_test -- --ignored --nocapture
//! ```
//!
//! Measures end-to-end drain time of `HashJoinExec` for tick-data-shaped
//! joins: a small build side (dimension-like) against a large probe side
//! (fact-like), keyed by symbol. Numbers are comparable across refactors on
//! the same machine.

use std::collections::VecDeque;
use std::time::Instant;

use taibai::common::error::Result;
use taibai::protocol::types::DataType;
use taibai::sql::execution::operators::join::HashJoinExec;
use taibai::sql::execution::operators::ExecutionOp;
use taibai::sql::parser::ast::{BinOp, JoinType};
use taibai::sql::planner::LogicalExpr;
use taibai::types::array::ColumnArray;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

const BUILD_ROWS: usize = 5_000;
const PROBE_ROWS: usize = 50_000;
const BATCH_ROWS: usize = 8_192;
const ROUNDS: usize = 2;

struct BatchesSource {
    batches: VecDeque<RecordBatch>,
}

impl ExecutionOp for BatchesSource {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }
}

fn build_batches(varchar_key: bool) -> Vec<RecordBatch> {
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("sym_id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
    ]);
    let mut symbol = ColumnArray::empty(DataType::VarChar);
    let mut sym_id = ColumnArray::empty(DataType::Int64);
    let mut name = ColumnArray::empty(DataType::VarChar);
    let mut batches = Vec::new();
    for i in 0..BUILD_ROWS {
        let key = format!("S{:04}", i);
        symbol.push(&ScalarValue::VarChar(Some(key))).unwrap();
        sym_id.push(&ScalarValue::Int64(Some(i as i64))).unwrap();
        name.push(&ScalarValue::VarChar(Some(format!("name-{i}"))))
            .unwrap();
        if (i + 1) % BATCH_ROWS == 0 {
            batches.push(batch_of(&schema, &mut symbol, &mut sym_id, &mut name));
        }
    }
    if !symbol.is_empty() {
        batches.push(batch_of(&schema, &mut symbol, &mut sym_id, &mut name));
    }
    let _ = varchar_key;
    batches
}

fn batch_of(
    schema: &Schema,
    symbol: &mut ColumnArray,
    sym_id: &mut ColumnArray,
    name: &mut ColumnArray,
) -> RecordBatch {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![symbol.clone(), sym_id.clone(), name.clone()],
    )
    .unwrap();
    *symbol = ColumnArray::empty(DataType::VarChar);
    *sym_id = ColumnArray::empty(DataType::Int64);
    *name = ColumnArray::empty(DataType::VarChar);
    batch
}

fn probe_batches(key_column: usize) -> Vec<RecordBatch> {
    // Probe layout mirrors the build side column order so one schema works
    // for both sides; the join key is `symbol` (col 0) or `sym_id` (col 1).
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("sym_id", DataType::Int64),
        ColumnDef::new("qty", DataType::Int64),
    ]);
    let mut symbol = ColumnArray::empty(DataType::VarChar);
    let mut sym_id = ColumnArray::empty(DataType::Int64);
    let mut qty = ColumnArray::empty(DataType::Int64);
    let mut batches = Vec::new();
    for i in 0..PROBE_ROWS {
        let i64v = i as i64;
        symbol
            .push(&ScalarValue::VarChar(Some(format!(
                "S{:04}",
                i64v % BUILD_ROWS as i64
            ))))
            .unwrap();
        sym_id
            .push(&ScalarValue::Int64(Some(i64v % BUILD_ROWS as i64)))
            .unwrap();
        qty.push(&ScalarValue::Int64(Some(i64v % 977))).unwrap();
        if (i + 1) % BATCH_ROWS == 0 {
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![symbol.clone(), sym_id.clone(), qty.clone()],
                )
                .unwrap(),
            );
            symbol = ColumnArray::empty(DataType::VarChar);
            sym_id = ColumnArray::empty(DataType::Int64);
            qty = ColumnArray::empty(DataType::Int64);
        }
    }
    if !symbol.is_empty() {
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![symbol.clone(), sym_id.clone(), qty.clone()],
            )
            .unwrap(),
        );
    }
    let _ = key_column;
    batches
}

fn eq_condition(left_col: usize, right_col: usize) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(left_col)),
        op: BinOp::Eq,
        right: Box::new(LogicalExpr::Column(right_col)),
    }
}

fn drain(mut exec: HashJoinExec) -> usize {
    let mut rows = 0usize;
    while let Some(batch) = exec.next_batch().unwrap() {
        rows += batch.num_rows();
    }
    rows
}

fn run(name: &str, make: &dyn Fn() -> HashJoinExec) {
    // Warm-up.
    let expected = drain(make());
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let drained = drain(make());
        assert_eq!(drained, expected, "unstable output rows across rounds");
    }
    let elapsed = start.elapsed().as_millis() as usize / ROUNDS;
    println!(
        "{:<44} {:>8} ms   {:>10} output rows",
        name, elapsed, expected
    );
}

#[test]
#[ignore = "diagnostic benchmark; run with --ignored --nocapture"]
fn join_probe_benchmark() {
    let build = build_batches(false);
    let probe_int = probe_batches(1);
    let probe_varchar = probe_batches(0);

    let build_source = || BatchesSource {
        batches: build.clone().into(),
    };
    let probe_int_source = || BatchesSource {
        batches: probe_int.clone().into(),
    };
    let probe_varchar_source = || BatchesSource {
        batches: probe_varchar.clone().into(),
    };

    let join_schema = |left: &Schema, right: &Schema| {
        Schema::new(
            left.columns
                .iter()
                .chain(right.columns.iter())
                .cloned()
                .collect(),
        )
    };

    // INNER join on INT64 key: build = left (5k dim rows), probe = right.
    run("INNER int64 key (50k probe x 5k build)", &|| {
        let left_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("qty", DataType::Int64),
        ]);
        HashJoinExec::new(
            Box::new(build_source()),
            Box::new(probe_int_source()),
            JoinType::Inner,
            eq_condition(1, 4),
            left_schema.clone(),
            right_schema.clone(),
            join_schema(&left_schema, &right_schema),
        )
    });

    // INNER join on VARCHAR key (symbol).
    run("INNER varchar key (50k probe x 5k build)", &|| {
        let left_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("qty", DataType::Int64),
        ]);
        HashJoinExec::new(
            Box::new(build_source()),
            Box::new(probe_varchar_source()),
            JoinType::Inner,
            eq_condition(0, 3),
            left_schema.clone(),
            right_schema.clone(),
            join_schema(&left_schema, &right_schema),
        )
    });

    // LEFT join on VARCHAR key: build = left, probe = right, unmatched left
    // rows emitted with NULL right side.
    run("LEFT varchar key (50k probe x 5k build)", &|| {
        let left_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("qty", DataType::Int64),
        ]);
        HashJoinExec::new(
            Box::new(build_source()),
            Box::new(probe_varchar_source()),
            JoinType::Left,
            eq_condition(0, 3),
            left_schema.clone(),
            right_schema.clone(),
            join_schema(&left_schema, &right_schema),
        )
    });

    // SEMI join on VARCHAR key.
    run("SEMI varchar key (50k probe x 5k build)", &|| {
        let left_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            ColumnDef::new("symbol", DataType::VarChar),
            ColumnDef::new("sym_id", DataType::Int64),
            ColumnDef::new("qty", DataType::Int64),
        ]);
        HashJoinExec::new(
            Box::new(build_source()),
            Box::new(probe_varchar_source()),
            JoinType::Semi,
            eq_condition(0, 3),
            left_schema.clone(),
            right_schema.clone(),
            join_schema(&left_schema, &right_schema),
        )
    });
}
