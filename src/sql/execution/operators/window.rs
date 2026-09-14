//! Window function executor (roadmap step 3, P3.5).
//!
//! Materializes the input, then computes one result column per window
//! function spec. Each spec independently sorts row indices by its
//! (partition, order) keys — using `compare_scalars` so NULL semantics match
//! the rest of the engine — and scatters computed values back by original row
//! position, so the operator preserves input row order.

use std::cmp::Ordering;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::{FrameBound, FrameType, WindowFrame};
use crate::sql::planner::{AggFunc, LogicalExpr, WindowFuncKind, WindowFuncPlan};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::filter::eval_expr_column;
use super::ExecutionOp;

pub struct WindowExec {
    input: Box<dyn ExecutionOp>,
    funcs: Vec<WindowFuncPlan>,
    input_schema: Schema,
    done: bool,
}

impl WindowExec {
    pub fn new(
        input: Box<dyn ExecutionOp>,
        funcs: Vec<WindowFuncPlan>,
        input_schema: Schema,
    ) -> Self {
        Self {
            input,
            funcs,
            input_schema,
            done: false,
        }
    }
}

/// Result column type for a window function.
pub fn window_result_type(kind: &WindowFuncKind, input: &Schema) -> DataType {
    let infer =
        |expr: &LogicalExpr| crate::sql::execution::operators::project::infer_type(expr, input);
    match kind {
        WindowFuncKind::RowNumber | WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
            DataType::Int64
        }
        WindowFuncKind::Aggregate { func, arg, .. } => match func {
            AggFunc::Count => DataType::Int64,
            AggFunc::Avg => DataType::Float64,
            AggFunc::Sum => match arg.as_ref().map(|a| infer(a)) {
                Some(DataType::Float32) | Some(DataType::Float64) => DataType::Float64,
                Some(DataType::Decimal) => DataType::Decimal,
                _ => DataType::Int64,
            },
            AggFunc::Min | AggFunc::Max => arg.as_deref().map(&infer).unwrap_or(DataType::Int64),
        },
        WindowFuncKind::Lead { arg, .. } | WindowFuncKind::Lag { arg, .. } => infer(arg),
    }
}

impl ExecutionOp for WindowExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Materialize the input; window functions need their whole partition.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut num_rows = 0usize;
        while let Some(batch) = self.input.next_batch()? {
            num_rows += batch.num_rows();
            batches.push(batch);
        }

        let batch = if batches.is_empty() {
            RecordBatch::new(self.input_schema.clone())
        } else if batches.len() == 1 {
            batches.remove(0)
        } else {
            concat_batches(&batches, num_rows)?
        };

        let mut columns = batch.columns.clone();
        let mut output_schema = batch.schema.clone();
        for plan in &self.funcs {
            let data_type = window_result_type(&plan.kind, &batch.schema);
            if num_rows == 0 {
                columns.push(ColumnArray::empty(data_type));
            } else {
                let values = compute_func(plan, &batch, data_type)?;
                let mut column = ColumnArray::empty(data_type);
                for value in &values {
                    column.push(value)?;
                }
                columns.push(column);
            }
            output_schema
                .columns
                .push(ColumnDef::new(&plan.output_name, data_type));
        }

        Ok(Some(RecordBatch::try_new(output_schema, columns)?))
    }
}

fn concat_batches(batches: &[RecordBatch], num_rows: usize) -> Result<RecordBatch> {
    let ncols = batches[0].num_columns();
    let mut columns: Vec<ColumnArray> = Vec::with_capacity(ncols);
    for col_idx in 0..ncols {
        let mut column =
            ColumnArray::with_capacity(batches[0].schema.columns[col_idx].data_type, num_rows);
        for batch in batches {
            column.append_owned(batch.columns[col_idx].clone())?;
        }
        columns.push(column);
    }
    let schema = batches[0].schema.clone();
    RecordBatch::try_new(schema, columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

fn compute_func(
    plan: &WindowFuncPlan,
    batch: &RecordBatch,
    result_type: DataType,
) -> Result<Vec<ScalarValue>> {
    let null_value = || ScalarValue::null_for_type(result_type);
    let num_rows = batch.num_rows();
    let partition_cols: Vec<Vec<ScalarValue>> = plan
        .partition_by
        .iter()
        .map(|expr| eval_expr_column(expr, batch))
        .collect::<Result<Vec<_>>>()?;
    let order_cols: Vec<(Vec<ScalarValue>, bool)> = plan
        .order_by
        .iter()
        .map(|key| Ok((eval_expr_column(&key.expr, batch)?, key.desc)))
        .collect::<Result<Vec<_>>>()?;

    // Row indices sorted by (partition keys, order keys). The sort is stable
    // so input order breaks ties and results stay deterministic.
    let order: Vec<u32> = {
        let mut order: Vec<u32> = (0..num_rows as u32).collect();
        order.sort_by(|&a, &b| {
            for col in &partition_cols {
                let cmp = compare(&col[a as usize], &col[b as usize]);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            for (col, desc) in &order_cols {
                let mut cmp = compare(&col[a as usize], &col[b as usize]);
                if *desc {
                    cmp = cmp.reverse();
                }
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });
        order
    };
    let mut values = vec![null_value(); num_rows];
    match &plan.kind {
        WindowFuncKind::RowNumber => {
            let mut group_start = 0usize;
            while group_start < order.len() {
                let group_end = partition_end(&order, group_start, &partition_cols);
                for (pos_in_group, &row) in order[group_start..group_end].iter().enumerate() {
                    values[row as usize] = ScalarValue::Int64(Some(pos_in_group as i64 + 1));
                }
                group_start = group_end;
            }
        }
        WindowFuncKind::Rank | WindowFuncKind::DenseRank => {
            let dense = matches!(plan.kind, WindowFuncKind::DenseRank);
            let mut group_start = 0usize;
            while group_start < order.len() {
                let group_end = partition_end(&order, group_start, &partition_cols);
                // Both counters restart from scratch in every partition.
                let mut rank = 0i64;
                let mut dense_rank = 0i64;
                for pos_in_group in 0..(group_end - group_start) {
                    if pos_in_group == 0
                        || !order_key_peer(
                            &order,
                            group_start + pos_in_group - 1,
                            group_start + pos_in_group,
                            &order_cols,
                        )
                    {
                        rank = pos_in_group as i64 + 1;
                        dense_rank += 1;
                    }
                    let value = if dense {
                        ScalarValue::Int64(Some(dense_rank))
                    } else {
                        ScalarValue::Int64(Some(rank))
                    };
                    values[order[group_start + pos_in_group] as usize] = value;
                }
                group_start = group_end;
            }
        }
        WindowFuncKind::Lead { .. } | WindowFuncKind::Lag { .. } => {
            let (arg, offset, default, is_lead) = match &plan.kind {
                WindowFuncKind::Lead {
                    arg,
                    offset,
                    default,
                } => (arg, *offset, default, true),
                WindowFuncKind::Lag {
                    arg,
                    offset,
                    default,
                } => (arg, *offset, default, false),
                _ => unreachable!("window kind matched above"),
            };
            let arg_values = eval_expr_column(arg, batch)?;
            let default_values = match default {
                Some(expr) => Some(eval_expr_column(expr, batch)?),
                None => None,
            };
            let mut group_start = 0usize;
            while group_start < order.len() {
                let group_end = partition_end(&order, group_start, &partition_cols);
                let group_len = group_end - group_start;
                for pos_in_group in 0..group_len {
                    let row = order[group_start + pos_in_group] as usize;
                    let partner_pos = if is_lead {
                        pos_in_group.checked_add(offset)
                    } else {
                        pos_in_group.checked_sub(offset)
                    };
                    let value = match partner_pos {
                        Some(p) if p < group_len => {
                            arg_values[order[group_start + p] as usize].clone()
                        }
                        _ => match &default_values {
                            Some(defaults) => defaults[row].clone(),
                            None => null_value(),
                        },
                    };
                    values[row] = value;
                }
                group_start = group_end;
            }
        }
        WindowFuncKind::Aggregate { func, arg, .. } => {
            let arg_values = match arg {
                Some(arg) => Some(eval_expr_column(arg, batch)?),
                None => None,
            };
            let mut group_start = 0usize;
            while group_start < order.len() {
                let group_end = partition_end(&order, group_start, &partition_cols);
                for pos_in_group in group_start..group_end {
                    let (lo, hi) = frame_bounds(
                        plan,
                        &order,
                        &order_cols,
                        group_start,
                        group_end,
                        pos_in_group,
                    );
                    let frame: Vec<ScalarValue> = match &arg_values {
                        Some(values) => order[lo..hi]
                            .iter()
                            .map(|&row| values[row as usize].clone())
                            .collect(),
                        None => order[lo..hi]
                            .iter()
                            .map(|_| ScalarValue::Int64(Some(1)))
                            .collect(),
                    };
                    let row = order[pos_in_group] as usize;
                    values[row] = aggregate_frame(func, &frame);
                }
                group_start = group_end;
            }
        }
    }

    Ok(values)
}

fn compare(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    crate::sql::execution::operators::aggregate::compare_scalars(a, b)
}

/// End (exclusive) of the partition that `start` belongs to. Compares through
/// `compare` so NULL/NaN partition keys group together like the sort above.
fn partition_end(order: &[u32], start: usize, partition_cols: &[Vec<ScalarValue>]) -> usize {
    let mut end = start + 1;
    while end < order.len() {
        let a = order[end - 1] as usize;
        let b = order[end] as usize;
        let same = partition_cols
            .iter()
            .all(|col| compare(&col[a], &col[b]) == Ordering::Equal);
        if !same {
            break;
        }
        end += 1;
    }
    end
}

/// True when two sorted positions carry the same ORDER BY key (peers). Uses
/// `compare` so NULL/NaN keys are peers of themselves.
fn order_key_peer(
    order: &[u32],
    pos_a: usize,
    pos_b: usize,
    order_cols: &[(Vec<ScalarValue>, bool)],
) -> bool {
    let a = order[pos_a] as usize;
    let b = order[pos_b] as usize;
    order_cols
        .iter()
        .all(|(col, _)| compare(&col[a], &col[b]) == Ordering::Equal)
}

/// Effective frame row span `[lo, hi)` for one row inside its partition.
/// Without an explicit frame: the whole partition when there is no ORDER BY,
/// otherwise `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. RANGE
/// offset bounds work in the order-key *value* domain (SQL semantics); ROWS
/// bounds count positions.
fn frame_bounds(
    plan: &WindowFuncPlan,
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    group_start: usize,
    group_end: usize,
    pos: usize,
) -> (usize, usize) {
    let group_len = group_end - group_start;
    let pos_in_group = pos - group_start;

    let (start_spec, end_spec, frame_type) = match &plan.frame {
        Some(frame) => (&frame.start, &frame.end, frame.frame_type),
        None if !plan.order_by.is_empty() => {
            static DEFAULT: WindowFrame = WindowFrame {
                frame_type: FrameType::Range,
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::CurrentRow,
            };
            (&DEFAULT.start, &DEFAULT.end, FrameType::Range)
        }
        None => return (group_start, group_end),
    };
    let is_range = frame_type == FrameType::Range;

    let lo = match start_spec {
        FrameBound::UnboundedPreceding => group_start,
        FrameBound::Preceding(n) if is_range => range_bound_pos(
            order, order_cols, group_start, group_end, pos, *n, true, true,
        )
        .unwrap_or_else(|| peer_start(order, order_cols, group_start, pos)),
        FrameBound::Preceding(n) => group_start + pos_in_group.saturating_sub(*n),
        FrameBound::CurrentRow if is_range => peer_start(order, order_cols, group_start, pos),
        FrameBound::CurrentRow => pos,
        FrameBound::Following(n) if is_range => range_bound_pos(
            order, order_cols, group_start, group_end, pos, *n, true, false,
        )
        .unwrap_or_else(|| peer_start(order, order_cols, group_start, pos)),
        FrameBound::Following(n) => group_start + (pos_in_group + n).min(group_len),
        FrameBound::UnboundedFollowing => group_end,
    };
    let hi = match end_spec {
        FrameBound::UnboundedFollowing => group_end,
        FrameBound::Following(n) if is_range => range_bound_pos(
            order, order_cols, group_start, group_end, pos, *n, false, false,
        )
        .unwrap_or_else(|| peer_end(order, order_cols, group_start, group_end, pos)),
        FrameBound::Following(n) => group_start + (pos_in_group + n + 1).min(group_len),
        FrameBound::CurrentRow => {
            match &plan.frame {
                // Explicit ROWS frame: CURRENT ROW ends at this row.
                Some(WindowFrame {
                    frame_type: FrameType::Rows,
                    ..
                }) => pos + 1,
                // RANGE (and the default frame): peers share the frame end.
                _ => peer_end(order, order_cols, group_start, group_end, pos),
            }
        }
        FrameBound::Preceding(n) if is_range => range_bound_pos(
            order, order_cols, group_start, group_end, pos, *n, false, true,
        )
        .unwrap_or_else(|| peer_end(order, order_cols, group_start, group_end, pos)),
        FrameBound::Preceding(n) => group_start + (pos_in_group + 1).saturating_sub(*n),
        FrameBound::UnboundedPreceding => group_start,
    };
    (lo, hi.clamp(lo, group_end))
}

/// Numeric value of the first (single) ORDER BY key at sorted position `pos`.
fn range_key_at(
    order_cols: &[(Vec<ScalarValue>, bool)],
    order: &[u32],
    pos: usize,
) -> Option<f64> {
    let (col, _) = order_cols.first()?;
    col.get(*order.get(pos)? as usize)?.numeric_f64()
}

/// First sorted position in `[group_start, group_end)` of a RANGE offset
/// bound. `start_bound` selects the frame's lower edge (returns the first
/// in-frame row) or upper edge (returns the first row *past* the frame);
/// `preceding` selects the direction of `offset` in the value domain. NULL
/// and non-numeric order keys return None: those rows compare only to their
/// own peers, so callers fall back to the peer run.
#[allow(clippy::too_many_arguments)]
fn range_bound_pos(
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    group_start: usize,
    group_end: usize,
    pos: usize,
    offset: usize,
    start_bound: bool,
    preceding: bool,
) -> Option<usize> {
    let (_, desc) = order_cols.first()?;
    let desc = *desc;
    let cur = range_key_at(order_cols, order, pos)?;
    let n = offset as f64;
    let keep: Box<dyn Fn(Option<f64>) -> bool> = match (desc, start_bound, preceding) {
        // ASC, start: frame begins at `cur - n` / `cur + n` (inclusive).
        (false, true, true) => Box::new(move |v| v.is_some_and(|v| v >= cur - n)),
        (false, true, false) => Box::new(move |v| v.is_some_and(|v| v >= cur + n)),
        // DESC, start: earlier sort positions carry larger values.
        (true, true, true) => Box::new(move |v| v.is_some_and(|v| v <= cur + n)),
        (true, true, false) => Box::new(move |v| v.is_some_and(|v| v <= cur - n)),
        // ASC, end: frame ends at `cur + n` (inclusive) / `cur - n` (the
        // PRECEDING end bound is exclusive in SQL).
        (false, false, false) => Box::new(move |v| v.is_some_and(|v| v <= cur + n)),
        (false, false, true) => Box::new(move |v| v.is_some_and(|v| v < cur - n)),
        // DESC, end.
        (true, false, false) => Box::new(move |v| v.is_some_and(|v| v >= cur - n)),
        (true, false, true) => Box::new(move |v| v.is_some_and(|v| v > cur + n)),
    };
    if start_bound {
        (group_start..group_end).find(|&p| keep(range_key_at(order_cols, order, p)))
    } else {
        Some(
            (group_start..group_end)
                .find(|&p| !keep(range_key_at(order_cols, order, p)))
                .unwrap_or(group_end),
        )
    }
}

/// First position of the peer run containing `pos`.
fn peer_start(
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    group_start: usize,
    pos: usize,
) -> usize {
    let mut start = pos;
    while start > group_start && order_key_peer(order, start - 1, start, order_cols) {
        start -= 1;
    }
    start
}

/// Last position (exclusive) of the peer run containing `pos`.
fn peer_end(
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    _group_start: usize,
    group_end: usize,
    pos: usize,
) -> usize {
    let mut end = pos + 1;
    while end < group_end && order_key_peer(order, end - 1, end, order_cols) {
        end += 1;
    }
    end
}

/// Aggregate one frame's values with plain (non-distinct) semantics matching
/// the hash aggregate operator's finalize behavior.
fn aggregate_frame(func: &AggFunc, frame: &[ScalarValue]) -> ScalarValue {
    match func {
        AggFunc::Count => {
            let count = frame.iter().filter(|v| !v.is_null()).count();
            ScalarValue::Int64(Some(count as i64))
        }
        AggFunc::Sum => {
            let mut int_sum: i64 = 0;
            let mut float_sum: f64 = 0.0;
            let mut has_float = false;
            let mut any = false;
            for value in frame {
                if value.is_null() {
                    continue;
                }
                any = true;
                if matches!(value, ScalarValue::Float32(_) | ScalarValue::Float64(_)) {
                    has_float = true;
                    float_sum += value.numeric_f64().unwrap_or(0.0);
                } else if let Some(i) = value.integer_i128() {
                    int_sum = int_sum.wrapping_add(i as i64);
                } else if let Some(f) = value.numeric_f64() {
                    float_sum += f;
                }
            }
            if !any {
                ScalarValue::Int64(None)
            } else if has_float {
                ScalarValue::Float64(Some(float_sum))
            } else {
                ScalarValue::Int64(Some(int_sum))
            }
        }
        AggFunc::Avg => {
            let vals: Vec<f64> = frame
                .iter()
                .filter(|v| !v.is_null())
                .filter_map(|v| v.numeric_f64())
                .collect();
            if vals.is_empty() {
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64))
            }
        }
        AggFunc::Min => extremum(frame, Ordering::Less),
        AggFunc::Max => extremum(frame, Ordering::Greater),
    }
}

fn extremum(frame: &[ScalarValue], wanted: Ordering) -> ScalarValue {
    let mut best: Option<&ScalarValue> = None;
    for value in frame {
        if value.is_null() {
            continue;
        }
        best = Some(match best {
            None => value,
            Some(current) => {
                if compare(value, current) == wanted {
                    value
                } else {
                    current
                }
            }
        });
    }
    best.cloned().unwrap_or(ScalarValue::Int64(None))
}
