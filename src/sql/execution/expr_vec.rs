//! Typed (column-at-a-time) expression evaluation.
//!
//! The general expression path (`operators::filter::eval_expr_column_with`)
//! materializes a `Vec<ScalarValue>` per expression node and loops row by
//! row. The functions here instead evaluate supported shapes directly on the
//! physical column data via the kernels in `types::array::analytics`, and
//! return `Ok(None)` for anything they do not cover so callers can fall back
//! to the scalar path. Semantics must stay identical to the scalar path;
//! `expr_vec_test.rs` pins this contract.
//!
//! Note the two NULL conventions:
//! - Filter masks are two-valued: comparison rows with NULL operands are
//!   simply `false` (SQL drops them from the result), which is what the
//!   literal/comparison kernels produce.
//! - Column producers are three-valued: `SELECT qty > 5` yields `Bool(None)`
//!   on rows where an operand is NULL, matching `eval_binary_scalar`.

use crate::common::error::Result;
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::array::{ArithOp, ColumnArray, CompareOp};
use crate::types::batch::RecordBatch;
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

fn compare_op_from_bin_op(op: BinOp) -> Option<CompareOp> {
    match op {
        BinOp::Eq => Some(CompareOp::Eq),
        BinOp::Neq => Some(CompareOp::Neq),
        BinOp::Lt => Some(CompareOp::Lt),
        BinOp::Lte => Some(CompareOp::Lte),
        BinOp::Gt => Some(CompareOp::Gt),
        BinOp::Gte => Some(CompareOp::Gte),
        _ => None,
    }
}

fn arith_op_from_bin_op(op: BinOp) -> Option<ArithOp> {
    match op {
        BinOp::Add => Some(ArithOp::Add),
        BinOp::Sub => Some(ArithOp::Sub),
        BinOp::Mul => Some(ArithOp::Mul),
        BinOp::Div => Some(ArithOp::Div),
        BinOp::Mod => Some(ArithOp::Mod),
        _ => None,
    }
}

/// One operand of a typed binary expression: a column (borrowed from the
/// batch or freshly computed) or a scalar literal.
enum Operand<'a> {
    Column(&'a ColumnArray),
    Owned(ColumnArray),
    Literal(&'a ScalarValue),
}

impl Operand<'_> {
    fn column(&self) -> Option<&ColumnArray> {
        match self {
            Operand::Column(c) => Some(c),
            Operand::Owned(c) => Some(c),
            Operand::Literal(_) => None,
        }
    }
}

/// Try to evaluate `expr` into a physical `ColumnArray`. `Ok(None)` means
/// "shape not supported by the typed kernels" — callers must fall back to the
/// scalar path rather than error.
pub fn eval_expr_to_column(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Option<ColumnArray>> {
    match expr {
        LogicalExpr::Column(idx) => Ok(batch.column(*idx).cloned()),
        LogicalExpr::Literal(_) => {
            // Constant columns are cheap on the scalar path; typed arithmetic
            // handles literals directly without materializing them.
            Ok(None)
        }
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                Ok(comparison_bool_column(left, *op, right, batch))
            }
            BinOp::And | BinOp::Or => Ok(boolean_combine(left, *op, right, batch)),
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                Ok(arithmetic_column(left, *op, right, batch))
            }
            _ => Ok(None),
        },
        LogicalExpr::IsNull(inner) => Ok(match &**inner {
            LogicalExpr::Column(idx) => batch.column(*idx).map(|c| null_flag_column(c, false)),
            _ => eval_expr_to_column(inner, batch)?.map(|c| null_flag_column(&c, false)),
        }),
        LogicalExpr::IsNotNull(inner) => Ok(match &**inner {
            LogicalExpr::Column(idx) => batch.column(*idx).map(|c| null_flag_column(c, true)),
            _ => eval_expr_to_column(inner, batch)?.map(|c| null_flag_column(&c, true)),
        }),
        LogicalExpr::Not(inner) => {
            Ok(eval_expr_to_column(inner, batch)?.and_then(|c| c.bool_not()))
        }
        _ => Ok(None),
    }
}

/// Try to evaluate a comparison into a two-valued row mask for filter
/// operators (NULL operand rows are `false`). Extends the operator's
/// `col op literal` fast path with `col op col` and comparisons over typed
/// arithmetic operands (e.g. `col + 1 > 10`).
pub fn eval_comparison_mask(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Result<Option<Vec<bool>>> {
    Ok(comparison_mask_inner(left, op, right, batch).map(|(mask, _nulls)| mask))
}

/// Comparison in three-valued form: rows where an operand is NULL yield
/// `Bool(None)`, matching `eval_binary_scalar`.
fn comparison_bool_column(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Option<ColumnArray> {
    let (mask, nulls) = comparison_mask_inner(left, op, right, batch)?;
    let bitmap = nulls?;
    Some(ColumnArray::Bool {
        data: mask.into_iter().map(u8::from).collect(),
        bitmap,
    })
}

/// Shared comparison evaluation. Returns the two-valued mask plus, when the
/// operand columns are known, the combined NULL bitmap (rows where any
/// operand is NULL) for three-valued consumers. `None` = unsupported shape.
#[allow(clippy::type_complexity)]
fn comparison_mask_inner(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Option<(Vec<bool>, Option<NullBitmap>)> {
    let compare_op = compare_op_from_bin_op(op)?;

    let (l, r) = resolve_binary_operands(left, right, batch)?;
    let (lc, rc) = (l.column(), r.column());

    // Compute the two-valued mask, plus the NULL map for three-valued
    // consumers (NaN rows are NULL for float columns via compare_column).
    let mut column_pair_nulls = None;
    let mask = match (&l, &r) {
        (_, Operand::Literal(v)) => {
            let col = lc?;
            if v.is_null() {
                return Some((vec![false; col.len()], Some(all_null_bitmap(col.len()))));
            }
            column_literal_mask(col, op, v)?
        }
        (Operand::Literal(v), _) => {
            let col = rc?;
            if v.is_null() {
                return Some((vec![false; col.len()], Some(all_null_bitmap(col.len()))));
            }
            let reversed = reverse_op(op)?;
            column_literal_mask(col, reversed, v)?
        }
        _ => {
            let (col_l, col_r) = (lc?, rc?);
            let cmp = col_l.compare_column(compare_op, col_r)?;
            // compare_column already folds operand NULLs (and NaN) into its
            // NULL map.
            column_pair_nulls = Some(cmp.nulls);
            cmp.mask
        }
    };

    let nulls = match column_pair_nulls {
        Some(nulls) => Some(nulls),
        None => {
            let l_nulls = lc.map(column_null_bitmap_with_nans);
            let r_nulls = rc.map(column_null_bitmap_with_nans);
            match (l_nulls, r_nulls) {
                (Some(a), Some(b)) => Some(or_null_bitmaps(&a, &b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            }
        }
    };
    Some((mask, nulls))
}

/// Operand column NULL map, extended with NaN rows for float columns: the
/// scalar path treats NaN comparisons as incomparable (Bool(None)).
fn column_null_bitmap_with_nans(column: &ColumnArray) -> NullBitmap {
    let mut out = NullBitmap::with_capacity(column.len());
    macro_rules! float_nulls {
        ($data:expr, $bitmap:expr) => {{
            for (i, value) in $data.iter().enumerate() {
                if $bitmap.is_null(i) || value.is_nan() {
                    out.push_null();
                } else {
                    out.push_valid();
                }
            }
        }};
    }
    match column {
        ColumnArray::Float32 { data, bitmap } => float_nulls!(data, bitmap),
        ColumnArray::Float64 { data, bitmap } => float_nulls!(data, bitmap),
        other => return other.bitmap().clone(),
    }
    out
}

/// Resolve the operands of a binary expression to typed columns or literals;
/// `None` when any operand has an unsupported (non-typed) shape.
fn resolve_binary_operands<'a>(
    left: &'a LogicalExpr,
    right: &'a LogicalExpr,
    batch: &'a RecordBatch,
) -> Option<(Operand<'a>, Operand<'a>)> {
    let resolve = |expr: &'a LogicalExpr| -> Option<Operand<'a>> {
        match expr {
            LogicalExpr::Column(idx) => batch.column(*idx).map(Operand::Column),
            LogicalExpr::Literal(v) => Some(Operand::Literal(v)),
            _ => eval_expr_to_column(expr, batch).ok()?.map(Operand::Owned),
        }
    };

    Some((resolve(left)?, resolve(right)?))
}

/// Typed arithmetic: `col op literal`, `literal op col` (commutative only),
/// and `expr op expr` via the typed kernels.
fn arithmetic_column(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Option<ColumnArray> {
    let arith_op = arith_op_from_bin_op(op)?;
    match (left, right) {
        (LogicalExpr::Column(idx), LogicalExpr::Literal(v)) => {
            batch.column(*idx)?.arith_scalar(arith_op, v)
        }
        (LogicalExpr::Literal(v), LogicalExpr::Column(idx)) => {
            // Only the commutative ops can swap a scalar lhs.
            match arith_op {
                a @ (ArithOp::Add | ArithOp::Mul) => batch.column(*idx)?.arith_scalar(a, v),
                _ => None,
            }
        }
        _ => {
            let lc = eval_expr_to_column(left, batch).ok()??;
            let rc = eval_expr_to_column(right, batch).ok()??;
            lc.arith_column(arith_op, &rc)
        }
    }
}

/// Three-valued AND/OR over Bool columns (Kleene logic, matching
/// `eval_binary_scalar`): FALSE AND NULL = FALSE, TRUE OR NULL = TRUE,
/// NULL AND/OR NULL = NULL.
fn boolean_combine(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
    batch: &RecordBatch,
) -> Option<ColumnArray> {
    let lc = eval_expr_to_column(left, batch).ok()??;
    let rc = eval_expr_to_column(right, batch).ok()??;
    let (
        ColumnArray::Bool {
            data: ld,
            bitmap: lb,
        },
        ColumnArray::Bool {
            data: rd,
            bitmap: rb,
        },
    ) = (&lc, &rc)
    else {
        return None;
    };
    if ld.len() != rd.len() {
        return None;
    }
    let is_and = op == BinOp::And;
    let mut data = Vec::with_capacity(ld.len());
    let mut bitmap = NullBitmap::with_capacity(ld.len());
    for i in 0..ld.len() {
        let (l_null, r_null) = (lb.is_null(i), rb.is_null(i));
        let (l, r) = (ld[i] != 0, rd[i] != 0);
        let value = if is_and {
            if (!l_null && !l) || (!r_null && !r) {
                Some(false)
            } else if l_null || r_null {
                None
            } else {
                Some(true)
            }
        } else if (!l_null && l) || (!r_null && r) {
            Some(true)
        } else if l_null || r_null {
            None
        } else {
            Some(false)
        };
        match value {
            Some(v) => {
                data.push(u8::from(v));
                bitmap.push_valid();
            }
            None => {
                data.push(0);
                bitmap.push_null();
            }
        }
    }
    Some(ColumnArray::Bool { data, bitmap })
}

/// `Column op Literal` mask via the literal kernels. Mirrors the semantics of
/// `eval_column_literal_comparison` in the filter operator, including Neq
/// treating non-matching NULL rows as excluded.
pub(crate) fn column_literal_mask(
    column: &ColumnArray,
    op: BinOp,
    literal: &ScalarValue,
) -> Option<Vec<bool>> {
    match op {
        BinOp::Eq => Some(column.eq_vec(literal)),
        BinOp::Neq => Some(
            column
                .eq_vec(literal)
                .into_iter()
                .enumerate()
                .map(|(idx, matched)| column.bitmap().is_valid(idx) && !matched)
                .collect(),
        ),
        BinOp::Lt => Some(column.lt_vec(literal)),
        BinOp::Lte => Some(column.le_vec(literal)),
        BinOp::Gt => Some(column.gt_vec(literal)),
        BinOp::Gte => Some(column.ge_vec(literal)),
        _ => None,
    }
}

fn reverse_op(op: BinOp) -> Option<BinOp> {
    match op {
        BinOp::Lt => Some(BinOp::Gt),
        BinOp::Lte => Some(BinOp::Gte),
        BinOp::Gt => Some(BinOp::Lt),
        BinOp::Gte => Some(BinOp::Lte),
        BinOp::Eq | BinOp::Neq => Some(op),
        _ => None,
    }
}

fn all_null_bitmap(len: usize) -> NullBitmap {
    NullBitmap::from_bools(&vec![true; len])
}

fn or_null_bitmaps(a: &NullBitmap, b: &NullBitmap) -> NullBitmap {
    let len = a.len();
    let mut out = NullBitmap::with_capacity(len);
    for i in 0..len {
        if a.is_null(i) || b.is_null(i) {
            out.push_null();
        } else {
            out.push_valid();
        }
    }
    out
}

/// IS NULL / IS NOT NULL as a Bool column; `negate` selects IS NOT NULL.
fn null_flag_column(column: &ColumnArray, negate: bool) -> ColumnArray {
    let len = column.len();
    let bitmap = column.bitmap();
    let data = (0..len)
        .map(|i| u8::from(bitmap.is_null(i) ^ negate))
        .collect();
    ColumnArray::Bool {
        data,
        bitmap: NullBitmap::new_valid(len),
    }
}
