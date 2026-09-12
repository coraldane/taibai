//! Diagnostic benchmark for the typed (vectorized) expression evaluation
//! path added by the vectorization roadmap, step 1 phase 1.
//!
//! Run with:
//! ```text
//! cargo test --test vectorized_expr_bench_test -- --ignored --nocapture
//! ```
//!
//! For identical comparison shapes it measures the per-row scalar baseline
//! (materializing both operand sides as `Vec<ScalarValue>`, then a row loop —
//! the cost the pre-vectorization filter paid) against the typed kernels
//! (`expr_vec::eval_comparison_mask`). AND/OR composites are evaluated per
//! branch by the filter operator, so their speedup follows from the
//! per-comparison numbers.

use std::time::Instant;

use taibai::protocol::types::DataType;
use taibai::sql::execution::expr_vec::{eval_comparison_mask, eval_expr_to_column};
use taibai::sql::execution::operators::filter::eval_expr_column_with;
use taibai::sql::parser::ast::BinOp;
use taibai::sql::planner::LogicalExpr;
use taibai::types::array::ColumnArray;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

const ROWS: usize = 100_000;
const ROUNDS: usize = 5;

fn bench_batch() -> RecordBatch {
    let mut price = ColumnArray::empty(DataType::Float64);
    let mut volume = ColumnArray::empty(DataType::Int64);
    let mut ts = ColumnArray::empty(DataType::Timestamp);
    let mut symbol = ColumnArray::empty(DataType::VarChar);
    for i in 0..ROWS {
        let i64v = i as i64;
        price
            .push(&ScalarValue::Float64(Some(
                100.0 + (i64v % 997) as f64 / 10.0,
            )))
            .unwrap();
        if i64v % 10_000 == 0 {
            volume.push(&ScalarValue::Int64(None)).unwrap();
        } else {
            volume
                .push(&ScalarValue::Int64(Some(i64v % 5_000)))
                .unwrap();
        }
        ts.push(&ScalarValue::Timestamp(Some(
            1_700_000_000_000_000 + i64v * 1_000,
        )))
        .unwrap();
        symbol
            .push(&ScalarValue::VarChar(Some(format!("S{:04}", i64v % 5_000))))
            .unwrap();
    }
    RecordBatch::try_new(
        Schema::new(vec![
            ColumnDef::new("price", DataType::Float64),
            ColumnDef::new("volume", DataType::Int64),
            ColumnDef::new("ts", DataType::Timestamp),
            ColumnDef::new("symbol", DataType::VarChar),
        ]),
        vec![price, volume, ts, symbol],
    )
    .unwrap()
}

fn col(idx: usize) -> LogicalExpr {
    LogicalExpr::Column(idx)
}

fn lit(v: ScalarValue) -> LogicalExpr {
    LogicalExpr::Literal(v)
}

struct Scenario {
    name: &'static str,
    left: LogicalExpr,
    op: BinOp,
    right: LogicalExpr,
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "ts >= literal (temporal)",
            left: col(2),
            op: BinOp::Gte,
            right: lit(ScalarValue::Timestamp(Some(1_700_000_000_500_000))),
        },
        Scenario {
            name: "price > 150.0 (float)",
            left: col(0),
            op: BinOp::Gt,
            right: lit(ScalarValue::Float64(Some(150.0))),
        },
        Scenario {
            name: "volume < 2500 (int, has NULLs)",
            left: col(1),
            op: BinOp::Lt,
            right: lit(ScalarValue::Int64(Some(2_500))),
        },
        Scenario {
            name: "symbol = 'S0042' (string)",
            left: col(3),
            op: BinOp::Eq,
            right: lit(ScalarValue::VarChar(Some("S0042".into()))),
        },
        Scenario {
            name: "price*1.5 > 200 (arith lhs)",
            left: LogicalExpr::BinaryOp {
                left: Box::new(col(0)),
                op: BinOp::Mul,
                right: Box::new(lit(ScalarValue::Float64(Some(1.5)))),
            },
            op: BinOp::Gt,
            right: lit(ScalarValue::Float64(Some(200.0))),
        },
        Scenario {
            name: "volume > volume (col vs col)",
            left: col(1),
            op: BinOp::Gt,
            right: col(1),
        },
    ]
}

/// Row-wise comparison used only to complete the scalar baseline; the
/// dominant cost being measured is the operand materialization above it.
fn scalar_compare(left: &ScalarValue, op: BinOp, right: &ScalarValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    let ord = left.numeric_cmp(right).or_else(|| match (left, right) {
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => Some(l.cmp(r)),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => Some(l.cmp(r)),
        _ => None,
    });
    use std::cmp::Ordering::*;
    match (op, ord) {
        (BinOp::Eq, Some(Equal)) => true,
        (BinOp::Neq, Some(o)) => o != Equal,
        (BinOp::Lt, Some(Less)) => true,
        (BinOp::Lte, Some(o)) => o != Greater,
        (BinOp::Gt, Some(Greater)) => true,
        (BinOp::Gte, Some(o)) => o != Less,
        _ => false,
    }
}

fn time_ms<F: FnMut()>(mut f: F) -> usize {
    let start = Instant::now();
    for _ in 0..ROUNDS {
        f();
    }
    start.elapsed().as_millis() as usize
}

#[test]
#[ignore = "diagnostic benchmark; run with --ignored --nocapture"]
fn vectorized_expression_benchmark() {
    let batch = bench_batch();
    println!("vectorized expression bench: {ROWS} rows x {ROUNDS} rounds");
    println!(
        "{:<34} {:>12} {:>12} {:>9}",
        "scenario", "scalar(ms)", "typed(ms)", "speedup"
    );

    for scenario in scenarios() {
        let left = &scenario.left;
        let right = &scenario.right;

        let scalar_ms = time_ms(|| {
            let left_vals = eval_expr_column_with(left, &batch, None).unwrap();
            let right_vals = eval_expr_column_with(right, &batch, None).unwrap();
            let _mask: Vec<bool> = left_vals
                .iter()
                .zip(right_vals.iter())
                .map(|(l, r)| scalar_compare(l, scenario.op, r))
                .collect();
        });

        let typed_ms = time_ms(|| {
            let mask = eval_comparison_mask(left, scenario.op, right, &batch)
                .unwrap()
                .unwrap_or_else(|| panic!("typed path should cover {}", scenario.name));
            assert!(!mask.is_empty());
        });

        let speedup = if typed_ms > 0 {
            scalar_ms as f64 / typed_ms as f64
        } else {
            f64::INFINITY
        };
        println!(
            "{:<34} {:>12} {:>12} {:>8.1}x",
            scenario.name, scalar_ms, typed_ms, speedup
        );
    }

    // Typed column production (project-style) for an arithmetic expression.
    let arith = LogicalExpr::BinaryOp {
        left: Box::new(col(1)),
        op: BinOp::Add,
        right: Box::new(lit(ScalarValue::Int64(Some(7)))),
    };
    let scalar_ms = time_ms(|| {
        let _vals = eval_expr_column_with(&arith, &batch, None).unwrap();
    });
    let typed_ms = time_ms(|| {
        eval_expr_to_column(&arith, &batch)
            .unwrap()
            .expect("typed path should cover arithmetic");
    });
    let speedup = if typed_ms > 0 {
        scalar_ms as f64 / typed_ms as f64
    } else {
        f64::INFINITY
    };
    println!(
        "{:<34} {:>12} {:>12} {:>8.1}x",
        "volume + 7 (project arith)", scalar_ms, typed_ms, speedup
    );
}
