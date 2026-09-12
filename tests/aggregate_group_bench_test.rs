//! Diagnostic benchmark for the GROUP BY aggregation hot path (roadmap
//! step 1, phase P1.2).
//!
//! Run with:
//! ```text
//! cargo test --test aggregate_group_bench_test -- --ignored --nocapture
//! ```
//!
//! Measures end-to-end drain time of `HashAggregateExec` over prebuilt
//! in-process batches for tick-data-shaped groupings (many rows, a few
//! thousand groups, varchar group key). Numbers are comparable across
//! refactors of the aggregation loop on the same machine.

use std::collections::VecDeque;
use std::time::Instant;

use taibai::common::error::Result;
use taibai::protocol::types::DataType;
use taibai::sql::execution::operators::aggregate::HashAggregateExec;
use taibai::sql::execution::operators::ExecutionOp;
use taibai::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use taibai::types::array::ColumnArray;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

const ROWS: usize = 200_000;
const BATCH_ROWS: usize = 8_192;
const SYMBOLS: usize = 5_000;
const ROUNDS: usize = 3;

struct BatchesSource {
    batches: VecDeque<RecordBatch>,
}

impl ExecutionOp for BatchesSource {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.batches.pop_front())
    }
}

fn input_batches() -> Vec<RecordBatch> {
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("price", DataType::Float64),
        ColumnDef::new("volume", DataType::Int64),
    ]);
    let mut batches = Vec::new();
    let mut symbol = ColumnArray::empty(DataType::VarChar);
    let mut price = ColumnArray::empty(DataType::Float64);
    let mut volume = ColumnArray::empty(DataType::Int64);
    for i in 0..ROWS {
        let i64v = i as i64;
        symbol
            .push(&ScalarValue::VarChar(Some(format!(
                "S{:04}",
                i64v % SYMBOLS as i64
            ))))
            .unwrap();
        price
            .push(&ScalarValue::Float64(Some(
                100.0 + (i64v % 997) as f64 / 10.0,
            )))
            .unwrap();
        volume
            .push(&ScalarValue::Int64(Some(i64v % 5_000)))
            .unwrap();
        if (i + 1) % BATCH_ROWS == 0 {
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![symbol.clone(), price.clone(), volume.clone()],
                )
                .unwrap(),
            );
            symbol = ColumnArray::empty(DataType::VarChar);
            price = ColumnArray::empty(DataType::Float64);
            volume = ColumnArray::empty(DataType::Int64);
        }
    }
    batches
}

fn agg(func: AggFunc, arg: LogicalExpr, distinct: bool) -> AggregateExpr {
    AggregateExpr {
        func,
        arg: Box::new(arg),
        distinct,
    }
}

fn drain(mut exec: HashAggregateExec) -> usize {
    let mut rows = 0usize;
    while let Some(batch) = exec.next_batch().unwrap() {
        rows += batch.num_rows();
    }
    rows
}

fn run(
    name: &str,
    group_by: Vec<LogicalExpr>,
    aggregates: Vec<AggregateExpr>,
    source: &dyn Fn() -> BatchesSource,
) {
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("price", DataType::Float64),
        ColumnDef::new("volume", DataType::Int64),
    ]);
    // Warm-up round (page cache, allocator, hasher).
    let rows = drain(HashAggregateExec::new(
        group_by.clone(),
        aggregates.clone(),
        Box::new(source()),
        schema.clone(),
    ));
    let start = Instant::now();
    for _ in 0..ROUNDS {
        let drained = drain(HashAggregateExec::new(
            group_by.clone(),
            aggregates.clone(),
            Box::new(source()),
            schema.clone(),
        ));
        assert_eq!(drained, rows, "unstable group count across rounds");
    }
    let elapsed = start.elapsed().as_millis() as usize / ROUNDS;
    println!(
        "{:<46} {:>8} ms   {:>10} rows   {:>5} groups",
        name, elapsed, ROWS, rows
    );
}

#[test]
#[ignore = "diagnostic benchmark; run with --ignored --nocapture"]
fn aggregate_group_benchmark() {
    let batches = input_batches();
    let source = || BatchesSource {
        batches: batches.clone().into(),
    };
    println!("aggregate group bench: {ROWS} rows, {SYMBOLS} groups, {ROUNDS} rounds");

    run(
        "GROUP BY symbol: COUNT(*), SUM(volume)",
        vec![LogicalExpr::Column(0)],
        vec![
            agg(AggFunc::Count, LogicalExpr::Wildcard, false),
            agg(AggFunc::Sum, LogicalExpr::Column(2), false),
        ],
        &source,
    );
    run(
        "GROUP BY symbol: AVG(price)",
        vec![LogicalExpr::Column(0)],
        vec![agg(AggFunc::Avg, LogicalExpr::Column(1), false)],
        &source,
    );
    run(
        "GROUP BY symbol: MIN/MAX(price)",
        vec![LogicalExpr::Column(0)],
        vec![
            agg(AggFunc::Min, LogicalExpr::Column(1), false),
            agg(AggFunc::Max, LogicalExpr::Column(1), false),
        ],
        &source,
    );
    run(
        "GROUP BY symbol: COUNT(DISTINCT volume)",
        vec![LogicalExpr::Column(0)],
        vec![agg(AggFunc::Count, LogicalExpr::Column(2), true)],
        &source,
    );
    run(
        "no GROUP BY: COUNT(DISTINCT symbol)",
        vec![],
        vec![agg(AggFunc::Count, LogicalExpr::Column(0), true)],
        &source,
    );
}
