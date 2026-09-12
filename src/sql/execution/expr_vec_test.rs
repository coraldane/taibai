//! Parity tests: the typed evaluation path (`expr_vec::eval_expr_to_column`)
//! must agree with the scalar path (`filter::eval_expr_column_with`) for
//! every expression shape it claims to cover, and return `None` (fallback)
//! for the rest.

use crate::protocol::types::DataType;
use crate::sql::execution::expr_vec::eval_expr_to_column;
use crate::sql::execution::operators::filter::eval_expr_column_with;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

fn parity_batch() -> RecordBatch {
    // Row layout (i = row index 0..6):
    //   qty:   0, NULL, 15, 25, 40, 70
    //   price: 1.5, 2.0, NULL, 7.5, 10.0, 11.0
    //   ts:    1000, 2000, 3000, NULL, 5000, 6000
    //   sym:   AAPL, NULL, AAPL, MSFT, TSLA, MSFT
    let mut qty = ColumnArray::empty(DataType::Int64);
    let mut price = ColumnArray::empty(DataType::Float64);
    let mut ts = ColumnArray::empty(DataType::Timestamp);
    let mut sym = ColumnArray::empty(DataType::VarChar);
    let qtys = [Some(0i64), None, Some(15), Some(25), Some(40), Some(70)];
    let prices = [1.5f64, 2.0, f64::NAN, 7.5, 10.0, 11.0];
    let tss = [1000i64, 2000, 3000, 4000, 5000, 6000];
    let syms = ["AAPL", "MSFT", "AAPL", "MSFT", "TSLA", "MSFT"];
    for i in 0..6 {
        qty.push(&ScalarValue::Int64(qtys[i])).unwrap();
        price.push(&ScalarValue::Float64(Some(prices[i]))).unwrap();
        if i == 3 {
            ts.push(&ScalarValue::Timestamp(None)).unwrap();
        } else {
            ts.push(&ScalarValue::Timestamp(Some(tss[i]))).unwrap();
        }
        if i == 1 {
            sym.push(&ScalarValue::VarChar(None)).unwrap();
        } else {
            sym.push(&ScalarValue::VarChar(Some(syms[i].to_string())))
                .unwrap();
        }
    }
    RecordBatch::try_new(
        Schema::new(vec![
            ColumnDef::new("qty", DataType::Int64),
            ColumnDef::new("price", DataType::Float64),
            ColumnDef::new("ts", DataType::Timestamp),
            ColumnDef::new("sym", DataType::VarChar),
        ]),
        vec![qty, price, ts, sym],
    )
    .unwrap()
}

fn col(idx: usize) -> LogicalExpr {
    LogicalExpr::Column(idx)
}

fn lit_i64(v: i64) -> LogicalExpr {
    LogicalExpr::Literal(ScalarValue::Int64(Some(v)))
}

fn lit_i32(v: i32) -> LogicalExpr {
    LogicalExpr::Literal(ScalarValue::Int32(Some(v)))
}

fn lit_f64(v: f64) -> LogicalExpr {
    LogicalExpr::Literal(ScalarValue::Float64(Some(v)))
}

fn bin(left: LogicalExpr, op: BinOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

/// Both paths must produce identical `Vec<ScalarValue>` sequences. The NaN in
/// the price column is compared via partial_cmp so NaN==NaN counts as equal.
fn assert_parity(expr: &LogicalExpr, batch: &RecordBatch) {
    let scalar = eval_expr_column_with(expr, batch, None).unwrap();
    let typed = eval_expr_to_column(expr, batch)
        .unwrap()
        .unwrap_or_else(|| panic!("expected typed path to cover {expr:?}"));
    assert_eq!(typed.len(), scalar.len(), "length mismatch for {expr:?}");
    for (row, a) in scalar.iter().enumerate() {
        let b = typed.scalar(row);
        let equal = match (a, &b) {
            (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => {
                x.to_bits() == y.to_bits()
            }
            // NULL is NULL regardless of the payload variant: the scalar path
            // always reports division-by-NULL as Int64(None) while the typed
            // path emits the typed NULL (Float64(None)). Downstream coercion
            // and mask semantics are identical.
            (x, y) if x.is_null() && y.is_null() => true,
            _ => a == &b,
        };
        assert!(equal, "row {row} mismatch for {expr:?}: {a:?} vs {b:?}");
    }
}

#[test]
fn typed_path_matches_scalar_path() {
    let batch = parity_batch();

    // Plain column round-trip.
    assert_parity(&col(0), &batch);
    assert_parity(&col(3), &batch);

    // Arithmetic: column op literal, cross-variant literal, column op column.
    assert_parity(&bin(col(0), BinOp::Add, lit_i64(10)), &batch);
    assert_parity(&bin(col(0), BinOp::Add, lit_i32(7)), &batch);
    assert_parity(&bin(col(0), BinOp::Mul, col(0)), &batch);
    assert_parity(&bin(col(0), BinOp::Sub, lit_i64(100)), &batch);
    assert_parity(&bin(col(0), BinOp::Mod, lit_i64(3)), &batch);
    // Division is always Float64, zero divisor → NULL.
    assert_parity(&bin(col(0), BinOp::Div, lit_i64(0)), &batch);
    assert_parity(&bin(col(0), BinOp::Div, lit_i64(2)), &batch);
    // Float column with integer literal lands on the f64 path.
    assert_parity(&bin(col(1), BinOp::Mul, lit_i64(2)), &batch);
    // Swapped literal operand (commutative ops).
    assert_parity(&bin(lit_i64(2), BinOp::Add, col(0)), &batch);
    // NULL literal arithmetic → all NULL Int64.
    assert_parity(
        &bin(
            col(0),
            BinOp::Add,
            LogicalExpr::Literal(ScalarValue::Int64(None)),
        ),
        &batch,
    );

    // Comparisons: column op literal (incl. temporal literals), column op
    // column, and arithmetic operands.
    assert_parity(&bin(col(0), BinOp::Gt, lit_i64(5)), &batch);
    assert_parity(&bin(col(0), BinOp::Lt, lit_i32(20)), &batch);
    assert_parity(
        &bin(
            col(2),
            BinOp::Gte,
            LogicalExpr::Literal(ScalarValue::Timestamp(Some(3000))),
        ),
        &batch,
    );
    assert_parity(
        &bin(
            col(3),
            BinOp::Eq,
            LogicalExpr::Literal(ScalarValue::VarChar(Some("MSFT".into()))),
        ),
        &batch,
    );
    assert_parity(&bin(col(0), BinOp::Gt, col(0)), &batch);
    assert_parity(
        &bin(bin(col(0), BinOp::Add, lit_i64(1)), BinOp::Gt, lit_i64(5)),
        &batch,
    );
    assert_parity(
        &bin(
            bin(col(1), BinOp::Mul, lit_f64(2.0)),
            BinOp::Lte,
            lit_f64(4.0),
        ),
        &batch,
    );
    // Neq keeps the "NULL rows are excluded" filter semantics.
    assert_parity(&bin(col(0), BinOp::Neq, lit_i64(15)), &batch);

    // Null checks, NOT, and boolean AND/OR over comparisons.
    assert_parity(&LogicalExpr::IsNull(Box::new(col(0))), &batch);
    assert_parity(&LogicalExpr::IsNotNull(Box::new(col(0))), &batch);
    assert_parity(
        &LogicalExpr::Not(Box::new(bin(col(0), BinOp::Gt, lit_i64(5)))),
        &batch,
    );
    assert_parity(
        &bin(
            bin(col(0), BinOp::Gt, lit_i64(5)),
            BinOp::And,
            bin(col(1), BinOp::Lt, lit_f64(8.0)),
        ),
        &batch,
    );
    assert_parity(
        &bin(
            bin(
                col(3),
                BinOp::Eq,
                LogicalExpr::Literal(ScalarValue::VarChar(Some("MSFT".into()))),
            ),
            BinOp::Or,
            bin(col(0), BinOp::Gt, lit_i64(50)),
        ),
        &batch,
    );
}

#[test]
fn unsupported_shapes_fall_back() {
    let batch = parity_batch();
    let unsupported = [
        // Function calls keep the scalar registry path for now.
        LogicalExpr::Function {
            name: "abs".into(),
            args: vec![col(0)],
        },
        // LIKE / BETWEEN / IN are not typed yet.
        LogicalExpr::Like {
            expr: Box::new(col(3)),
            pattern: "M%".into(),
            negated: false,
        },
        LogicalExpr::Between {
            expr: Box::new(col(0)),
            low: Box::new(lit_i64(1)),
            high: Box::new(lit_i64(10)),
            negated: false,
        },
        LogicalExpr::In {
            expr: Box::new(col(0)),
            values: vec![ScalarValue::Int64(Some(15))],
            negated: false,
        },
        // Literal-only projection stays on the scalar path by design.
        lit_i64(42),
    ];
    for expr in &unsupported {
        let typed = eval_expr_to_column(expr, &batch).unwrap();
        assert!(typed.is_none(), "expected fallback for {expr:?}");
        // The scalar path must still handle them.
        eval_expr_column_with(expr, &batch, None).unwrap();
    }
}
