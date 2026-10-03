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
            crate::common::query_cancel::check()?;
            num_rows += batch.num_rows();
            batches.push(batch);
        }

        let batch = if batches.is_empty() {
            RecordBatch::new(self.input_schema.clone())
        } else if batches.len() == 1 {
            batches.remove(0)
        } else {
            concat_batches(batches, num_rows)?
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

/// Concatenates the materialized input batches, MOVING each column out of its
/// batch: the previous clone-per-column kept every batch's data alive twice
/// (input vec + concatenated result) at peak.
fn concat_batches(mut batches: Vec<RecordBatch>, num_rows: usize) -> Result<RecordBatch> {
    let ncols = batches[0].num_columns();
    let schema = batches[0].schema.clone();
    let mut columns: Vec<ColumnArray> = Vec::with_capacity(ncols);
    for col_idx in 0..ncols {
        let mut column = ColumnArray::with_capacity(schema.columns[col_idx].data_type, num_rows);
        for batch in batches.iter_mut() {
            let taken = std::mem::replace(
                &mut batch.columns[col_idx],
                ColumnArray::empty(schema.columns[col_idx].data_type),
            );
            column.append_owned(taken)?;
        }
        columns.push(column);
    }
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
            // AVG over DECIMAL accumulates raw scaled i64 and must divide the
            // scale back out at finalize, exactly like HashAggregateExec's
            // Accumulator::Avg. The scale comes from the argument column.
            let decimal_scale = match (func, arg.as_deref()) {
                (AggFunc::Avg, Some(LogicalExpr::Column(idx))) => batch
                    .schema
                    .columns
                    .get(*idx)
                    .filter(|col| col.data_type == DataType::Decimal)
                    .map(|col| col.scale),
                _ => None,
            };
            // Default frame (RANGE UNBOUNDED PRECEDING -> CURRENT ROW, incl.
            // peers) reduces to a running prefix aggregate: O(1) per row
            // instead of re-materializing the whole frame per row.
            if is_running_prefix_frame(plan) {
                return compute_running_aggregate(
                    func,
                    arg_values.as_deref(),
                    &order,
                    &order_cols,
                    &partition_cols,
                    num_rows,
                    result_type,
                    decimal_scale,
                );
            }
            // Result-column type drives the value domain: Decimal sums must
            // stay Decimal, and empty MIN/MAX frames need a typed NULL.
            let arg_type = window_result_type(
                &WindowFuncKind::Aggregate {
                    func: *func,
                    arg: arg.clone(),
                    distinct: false,
                },
                &batch.schema,
            );
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
                    values[row] = aggregate_frame(func, &frame, arg_type, decimal_scale);
                }
                group_start = group_end;
            }
        }
    }

    Ok(values)
}

fn null_of(dt: DataType) -> ScalarValue {
    ScalarValue::null_for_type(dt)
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
            order,
            order_cols,
            group_start,
            group_end,
            pos,
            *n,
            true,
            true,
        )
        .unwrap_or_else(|| peer_start(order, order_cols, group_start, pos)),
        FrameBound::Preceding(n) => group_start + pos_in_group.saturating_sub(*n),
        FrameBound::CurrentRow if is_range => peer_start(order, order_cols, group_start, pos),
        FrameBound::CurrentRow => pos,
        FrameBound::Following(n) if is_range => range_bound_pos(
            order,
            order_cols,
            group_start,
            group_end,
            pos,
            *n,
            true,
            false,
        )
        .unwrap_or_else(|| peer_start(order, order_cols, group_start, pos)),
        FrameBound::Following(n) => group_start + (pos_in_group + n).min(group_len),
        FrameBound::UnboundedFollowing => group_end,
    };
    let hi = match end_spec {
        FrameBound::UnboundedFollowing => group_end,
        FrameBound::Following(n) if is_range => range_bound_pos(
            order,
            order_cols,
            group_start,
            group_end,
            pos,
            *n,
            false,
            false,
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
            order,
            order_cols,
            group_start,
            group_end,
            pos,
            *n,
            false,
            true,
        )
        .unwrap_or_else(|| peer_end(order, order_cols, group_start, group_end, pos)),
        FrameBound::Preceding(n) => group_start + (pos_in_group + 1).saturating_sub(*n),
        FrameBound::UnboundedPreceding => group_start,
    };
    (lo, hi.clamp(lo, group_end))
}

/// Numeric value of the first (single) ORDER BY key at sorted position `pos`.
fn range_key_at(order_cols: &[(Vec<ScalarValue>, bool)], order: &[u32], pos: usize) -> Option<f64> {
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
        // ASC, end: both offset end bounds are INCLUSIVE per the SQL
        // standard (RANGE BETWEEN ... AND n PRECEDING keeps values down to
        // cur - n, mirroring PostgreSQL).
        (false, false, false) => Box::new(move |v| v.is_some_and(|v| v <= cur + n)),
        (false, false, true) => Box::new(move |v| v.is_some_and(|v| v <= cur - n)),
        // DESC, end.
        (true, false, false) => Box::new(move |v| v.is_some_and(|v| v >= cur - n)),
        (true, false, true) => Box::new(move |v| v.is_some_and(|v| v >= cur + n)),
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

/// True for frames equivalent to "unbounded start through the current row"
/// under RANGE semantics (the SQL default when no frame clause is given): a
/// running prefix aggregate that flushes at peer boundaries answers them
/// exactly. An explicit ROWS frame must NOT take this shortcut — ROWS
/// accumulates row by row, so peer-group flushing would hand every row in a
/// peer group the group total instead of its running value.
fn is_running_prefix_frame(plan: &WindowFuncPlan) -> bool {
    match &plan.frame {
        None => true,
        Some(frame) => {
            frame.frame_type == crate::sql::parser::ast::FrameType::Range
                && matches!(
                    frame.start,
                    crate::sql::parser::ast::FrameBound::UnboundedPreceding
                )
                && matches!(frame.end, crate::sql::parser::ast::FrameBound::CurrentRow)
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// Running prefix aggregate over each partition. RANGE semantics include
/// ORDER BY peers, so the prefix flushes at peer-group boundaries; every row
/// inside a peer group reports the value including the whole group.
fn compute_running_aggregate(
    func: &AggFunc,
    arg_values: Option<&[ScalarValue]>,
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    partition_cols: &[Vec<ScalarValue>],
    num_rows: usize,
    result_type: DataType,
    decimal_scale: Option<i32>,
) -> Result<Vec<ScalarValue>> {
    let mut values = vec![ScalarValue::Int64(None); num_rows];
    // Running state mirrors aggregate_frame's domains; initialized in the
    // partition loop (reset at every partition boundary).
    let mut count: i64;
    let mut sum_i: i64;
    let mut sum_decimal: i64;
    let mut sum_f: f64;
    let mut has_float: bool;
    let mut any: bool;
    let mut best: Option<ScalarValue>;

    let mut group_start = 0usize;
    while group_start < order.len() {
        let group_end = partition_end(order, group_start, partition_cols);
        // Running state is per-partition: reset at every boundary.
        count = 0;
        sum_i = 0;
        sum_decimal = 0;
        sum_f = 0.0;
        has_float = false;
        any = false;
        best = None;
        // Peer groups inside the partition: rows with equal ORDER BY keys.
        let mut pos = group_start;
        while pos < group_end {
            let mut peer_end = pos + 1;
            while peer_end < group_end
                && order_key_equal(order_cols, order[pos] as usize, order[peer_end] as usize)
            {
                peer_end += 1;
            }
            for &row in &order[pos..peer_end] {
                let row = row as usize;
                let value = match arg_values {
                    Some(values) => &values[row],
                    None => &COUNT_ONE,
                };
                if value.is_null() {
                    continue;
                }
                any = true;
                match func {
                    AggFunc::Count => count += 1,
                    AggFunc::Sum | AggFunc::Avg => {
                        if result_type == DataType::Decimal
                            && matches!(value, ScalarValue::Decimal(_))
                        {
                            sum_decimal = sum_decimal.saturating_add(
                                value
                                    .integer_i128()
                                    .unwrap_or(0)
                                    .clamp(i64::MIN as i128, i64::MAX as i128)
                                    as i64,
                            );
                        } else if let Some(i) = value.integer_i128() {
                            sum_i = sum_i
                                .saturating_add(i.clamp(i64::MIN as i128, i64::MAX as i128) as i64);
                        } else {
                            has_float = true;
                            sum_f += value.numeric_f64().unwrap_or(0.0);
                        }
                    }
                    AggFunc::Min | AggFunc::Max => {
                        let take = match &best {
                            None => true,
                            Some(current) => {
                                let ord = crate::storage::index::sort_key::compare_scalars_for_sort(
                                    value, current,
                                );
                                if func == &AggFunc::Min {
                                    ord == std::cmp::Ordering::Less
                                } else {
                                    ord == std::cmp::Ordering::Greater
                                }
                            }
                        };
                        if take {
                            best = Some(value.clone());
                        }
                    }
                }
            }
            let current = running_value(
                func,
                any,
                count,
                sum_i,
                sum_decimal,
                sum_f,
                has_float,
                &best,
                result_type,
                decimal_scale,
            );
            for &row in &order[pos..peer_end] {
                values[row as usize] = current.clone();
            }
            pos = peer_end;
        }
        group_start = group_end;
    }
    Ok(values)
}

const COUNT_ONE: ScalarValue = ScalarValue::Int64(Some(1));

#[allow(clippy::too_many_arguments)]
fn running_value(
    func: &AggFunc,
    any: bool,
    count: i64,
    sum_i: i64,
    sum_decimal: i64,
    sum_f: f64,
    has_float: bool,
    best: &Option<ScalarValue>,
    result_type: DataType,
    decimal_scale: Option<i32>,
) -> ScalarValue {
    match func {
        AggFunc::Count => ScalarValue::Int64(Some(count)),
        AggFunc::Sum => {
            if !any {
                ScalarValue::null_for_type(result_type)
            } else if result_type == DataType::Decimal {
                ScalarValue::Decimal(Some(sum_decimal))
            } else if has_float {
                ScalarValue::Float64(Some(sum_f))
            } else {
                ScalarValue::Int64(Some(sum_i))
            }
        }
        AggFunc::Avg => {
            if !any || count == 0 {
                ScalarValue::Float64(None)
            } else {
                let total = if result_type == DataType::Decimal {
                    sum_decimal as f64
                } else if has_float {
                    sum_f
                } else {
                    sum_i as f64
                };
                // Decimal sums accumulate in the raw scaled domain; divide
                // the scale back out (missing step made AVG 10^scale too
                // large, diverging from HashAggregateExec).
                let scale_divisor = decimal_scale.map_or(1.0, |sc| 10f64.powi(sc));
                ScalarValue::Float64(Some(total / count as f64 / scale_divisor))
            }
        }
        AggFunc::Min | AggFunc::Max => best
            .clone()
            .unwrap_or_else(|| ScalarValue::null_for_type(result_type)),
    }
}

fn order_key_equal(
    order_cols: &[(Vec<ScalarValue>, bool)],
    left_row: usize,
    right_row: usize,
) -> bool {
    order_cols.iter().all(|(values, _)| {
        crate::storage::index::sort_key::compare_scalars_for_sort(
            &values[left_row],
            &values[right_row],
        ) == std::cmp::Ordering::Equal
    })
}

/// Aggregate one frame's values with plain (non-distinct) semantics matching
/// the hash aggregate operator's finalize behavior. `decimal_scale` carries
/// the argument column's DECIMAL scale for AVG's finalize division.
fn aggregate_frame(
    func: &AggFunc,
    frame: &[ScalarValue],
    arg_type: DataType,
    decimal_scale: Option<i32>,
) -> ScalarValue {
    match func {
        AggFunc::Count => {
            let count = frame.iter().filter(|v| !v.is_null()).count();
            ScalarValue::Int64(Some(count as i64))
        }
        AggFunc::Sum => {
            let mut int_sum: i64 = 0;
            let mut decimal_sum: i64 = 0;
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
                } else if arg_type == DataType::Decimal && matches!(value, ScalarValue::Decimal(_))
                {
                    // Decimal sums stay in the raw scaled domain so the
                    // result column (also Decimal) accepts them; the old
                    // Int64 coercion failed the strict column push.
                    decimal_sum = decimal_sum.saturating_add(
                        value
                            .integer_i128()
                            .unwrap_or(0)
                            .clamp(i64::MIN as i128, i64::MAX as i128)
                            as i64,
                    );
                } else if let Some(i) = value.integer_i128() {
                    int_sum =
                        int_sum.saturating_add(i.clamp(i64::MIN as i128, i64::MAX as i128) as i64);
                } else if let Some(f) = value.numeric_f64() {
                    float_sum += f;
                }
            }
            if !any {
                null_of(arg_type)
            } else if arg_type == DataType::Decimal {
                ScalarValue::Decimal(Some(decimal_sum))
            } else if has_float {
                ScalarValue::Float64(Some(float_sum))
            } else {
                ScalarValue::Int64(Some(int_sum))
            }
        }
        AggFunc::Avg => {
            if arg_type == DataType::Decimal {
                // Raw scaled i64 domain: divide the scale back out at
                // finalize (same contract as Accumulator::Avg).
                let vals: Vec<f64> = frame
                    .iter()
                    .filter(|v| !v.is_null())
                    .filter_map(|v| v.integer_i128().map(|i| i as f64))
                    .collect();
                if vals.is_empty() {
                    ScalarValue::Float64(None)
                } else {
                    let scale_divisor = decimal_scale.map_or(1.0, |sc| 10f64.powi(sc));
                    ScalarValue::Float64(Some(
                        vals.iter().sum::<f64>() / vals.len() as f64 / scale_divisor,
                    ))
                }
            } else {
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
        }
        AggFunc::Min => extremum(frame, Ordering::Less, arg_type),
        AggFunc::Max => extremum(frame, Ordering::Greater, arg_type),
    }
}

fn extremum(frame: &[ScalarValue], wanted: Ordering, arg_type: DataType) -> ScalarValue {
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
    // Empty (or all-NULL) frames yield NULL of the ARGUMENT's type: the
    // result column inherits the argument type and rejects a foreign null.
    best.cloned().unwrap_or_else(|| null_of(arg_type))
}
