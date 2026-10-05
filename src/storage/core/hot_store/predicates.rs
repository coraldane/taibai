//! Compiled predicates for chunk scans.

use super::*;

// ---------------------------------------------------------------------------
// Typed predicate pre-compilation.
//
// `ColumnArray::compare_scalar_at` re-matches the (column, scalar) enum pair on
// every row. The lookup loops run one tight filter pass over each chunk's
// posting list, so each predicate is resolved once per chunk into a typed
// comparator and the per-row work is a direct slice index + `Ord::cmp`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    #[inline]
    pub(super) fn matches(self, ord: std::cmp::Ordering) -> bool {
        match self {
            CmpOp::Eq => ord.is_eq(),
            CmpOp::Lt => ord.is_lt(),
            CmpOp::Le => !ord.is_gt(),
            CmpOp::Gt => ord.is_gt(),
            CmpOp::Ge => !ord.is_lt(),
        }
    }
}

/// A predicate bound to one chunk's typed column storage.
pub(super) enum FastPred<'a> {
    Bool { data: &'a [u8], v: bool },
    I8 { data: &'a [i8], v: i8 },
    I16 { data: &'a [i16], v: i16 },
    I32 { data: &'a [i32], v: i32 },
    I64 { data: &'a [i64], v: i64 },
    U8 { data: &'a [u8], v: u8 },
    U16 { data: &'a [u16], v: u16 },
    U32 { data: &'a [u32], v: u32 },
    U64 { data: &'a [u64], v: u64 },
    F32 { data: &'a [f32], v: f32 },
    F64 { data: &'a [f64], v: f64 },
    Str { data: &'a Utf8Buffer, v: &'a str },
    Bytes { data: &'a BinaryBuffer, v: &'a [u8] },
}

impl FastPred<'_> {
    #[inline]
    pub(super) fn compare(&self, row: usize) -> Option<std::cmp::Ordering> {
        match *self {
            FastPred::Bool { data, v } => Some((data[row] != 0).cmp(&v)),
            FastPred::I8 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I16 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I32 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I64 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U8 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U16 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U32 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U64 { data, v } => Some(data[row].cmp(&v)),
            FastPred::F32 { data, v } => data[row].partial_cmp(&v),
            FastPred::F64 { data, v } => data[row].partial_cmp(&v),
            FastPred::Str { data, v } => Some(data.value(row).cmp(v)),
            FastPred::Bytes { data, v } => Some(data.value(row).cmp(v)),
        }
    }
}

/// A fully-resolved row predicate: either a typed fast comparator or the
/// canonical scalar path (cross-type comparisons and any other pair).
pub(super) enum RowPred<'c, 'v> {
    Fast {
        bitmap: &'c NullBitmap,
        pred: FastPred<'c>,
        op: CmpOp,
    },
    Generic {
        column: &'c ColumnArray,
        value: &'v ScalarValue,
        op: CmpOp,
    },
    AlwaysFalse,
}

impl RowPred<'_, '_> {
    #[inline]
    pub(super) fn matches(&self, row: usize) -> bool {
        match self {
            RowPred::Fast { bitmap, pred, op } => {
                if bitmap.is_null(row) {
                    return false;
                }
                pred.compare(row).is_some_and(|ord| op.matches(ord))
            }
            RowPred::Generic { column, value, op } => column
                .compare_scalar_at(row, value)
                .is_some_and(|ord| op.matches(ord)),
            RowPred::AlwaysFalse => false,
        }
    }
}

pub(super) fn compile_one<'c, 'v>(
    column: Option<&'c ColumnArray>,
    value: &'v ScalarValue,
    op: CmpOp,
) -> RowPred<'c, 'v>
where
    'v: 'c,
{
    use ScalarValue as SV;
    let Some(col) = column else {
        return RowPred::AlwaysFalse;
    };
    macro_rules! typed {
        ($variant:ident, $scalar:ident, $field:ident) => {
            if let (ColumnArray::$variant { data, .. }, SV::$scalar(Some(v))) = (col, value) {
                return RowPred::Fast {
                    bitmap: col.bitmap(),
                    pred: FastPred::$field { data, v: v.clone() },
                    op,
                };
            }
        };
    }
    typed!(Bool, Bool, Bool);
    typed!(Int8, Int8, I8);
    typed!(Int16, Int16, I16);
    typed!(Int32, Int32, I32);
    typed!(Int64, Int64, I64);
    typed!(UInt8, UInt8, U8);
    typed!(UInt16, UInt16, U16);
    typed!(UInt32, UInt32, U32);
    typed!(UInt64, UInt64, U64);
    typed!(Float32, Float32, F32);
    typed!(Float64, Float64, F64);
    typed!(Decimal, Decimal, I64);
    typed!(Date, Date, I32);
    typed!(Time, Time, I64);
    typed!(Timestamp, Timestamp, I64);
    typed!(Interval, Interval, I64);
    if let (ColumnArray::VarChar { data, .. }, SV::VarChar(Some(v))) = (col, value) {
        return RowPred::Fast {
            bitmap: col.bitmap(),
            pred: FastPred::Str {
                data,
                v: v.as_str(),
            },
            op,
        };
    }
    if let (ColumnArray::Blob { data, .. }, SV::Blob(Some(v))) = (col, value) {
        return RowPred::Fast {
            bitmap: col.bitmap(),
            pred: FastPred::Bytes {
                data,
                v: v.as_slice(),
            },
            op,
        };
    }
    RowPred::Generic {
        column: col,
        value,
        op,
    }
}

/// Pre-resolve the pushed-down predicates against one chunk's columns. The
/// index-covered key column (and the partition column when the composite index
/// was used) are skipped, mirroring the guarantee that every candidate row
/// already satisfies them.
pub(super) fn compile_predicates<'c, 'v>(
    columns: &'c [std::sync::Arc<ColumnArray>],
    eq_predicates: &'v [(usize, ScalarValue)],
    range_predicates: &'v [RangeClause],
    covered_key_column: usize,
    covered_partition_column: Option<usize>,
) -> Vec<RowPred<'c, 'v>>
where
    'v: 'c,
{
    let mut preds = Vec::new();
    for (column, value) in eq_predicates {
        if *column == covered_key_column || Some(*column) == covered_partition_column {
            continue;
        }
        preds.push(compile_one(
            columns.get(*column).map(std::sync::Arc::as_ref),
            value,
            CmpOp::Eq,
        ));
    }
    for clause in range_predicates {
        let op = match clause.op {
            RangeOp::Eq => CmpOp::Eq,
            RangeOp::Lt => CmpOp::Lt,
            RangeOp::Le => CmpOp::Le,
            RangeOp::Gt => CmpOp::Gt,
            RangeOp::Ge => CmpOp::Ge,
        };
        preds.push(compile_one(
            columns.get(clause.column_index).map(std::sync::Arc::as_ref),
            &clause.value,
            op,
        ));
    }
    preds
}
