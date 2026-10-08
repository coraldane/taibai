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
    memory_limit_bytes: usize,
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
            memory_limit_bytes: super::super::spill::operator_memory_limit(),
        }
    }
    pub fn with_memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = bytes.max(1);
        self
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
        let mut input_bytes = 0usize;
        let mut memory =
            super::super::spill::OperatorMemory::new("Window", self.memory_limit_bytes);
        while let Some(batch) = self.input.next_batch()? {
            crate::common::query_cancel::check()?;
            num_rows = num_rows
                .checked_add(batch.num_rows())
                .filter(|rows| *rows <= u32::MAX as usize)
                .ok_or_else(|| {
                    crate::common::error::TaibaiError::ResourceBusy(
                        "Window row-position limit exceeded".into(),
                    )
                })?;
            input_bytes = input_bytes.saturating_add(batch.estimated_size_bytes());
            memory.reserve(input_bytes)?;
            batches.push(batch);
        }

        memory.reserve(input_bytes.saturating_mul(2))?;
        let mut batch = if batches.is_empty() {
            RecordBatch::new(self.input_schema.clone())
        } else if batches.len() == 1 {
            batches.remove(0)
        } else {
            concat_batches(batches, num_rows)?
        };

        let mut columns = Vec::new();
        let mut output_bytes = 0usize;
        let mut output_schema = batch.schema.clone();
        for plan in &self.funcs {
            // Account input, scalar keys/arguments, sorted positions, frame
            // state, and the scalar-to-column conversion peak before computing.
            let exprs = plan.partition_by.len() + plan.order_by.len() + 2;
            let working = input_bytes.saturating_mul(exprs + 2).saturating_add(
                num_rows.saturating_mul(
                    exprs
                        .saturating_mul(std::mem::size_of::<ScalarValue>())
                        .saturating_add(160),
                ),
            );
            memory.reserve(working.saturating_add(output_bytes))?;
            let data_type = window_result_type(&plan.kind, &batch.schema);
            if num_rows == 0 {
                columns.push(ColumnArray::empty(data_type));
            } else {
                let values = compute_func(plan, &batch, data_type)?;
                let mut column = ColumnArray::empty(data_type);
                for value in &values {
                    column.push(value)?;
                }
                output_bytes = output_bytes.saturating_add(column.estimated_size_bytes());
                columns.push(column);
            }
            output_schema
                .columns
                .push(ColumnDef::new(&plan.output_name, data_type));
        }

        batch.columns.extend(columns);
        memory.reserve(input_bytes.saturating_add(output_bytes))?;
        Ok(Some(RecordBatch::try_new(output_schema, batch.columns)?))
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
                    if pos_in_group % 1024 == 0 {
                        crate::common::query_cancel::check()?;
                    }
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
                    if pos_in_group % 1024 == 0 {
                        crate::common::query_cancel::check()?;
                    }
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
                    if pos_in_group % 1024 == 0 {
                        crate::common::query_cancel::check()?;
                    }
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
        WindowFuncKind::Aggregate {
            func,
            arg,
            distinct,
        } => {
            let arg_values = arg
                .as_ref()
                .map(|arg| eval_expr_column(arg, batch))
                .transpose()?;
            let arg_type = arg.as_ref().map_or(DataType::Int64, |arg| {
                crate::sql::execution::operators::project::infer_type(arg, &batch.schema)
            });
            let decimal_scale = match arg.as_deref() {
                Some(LogicalExpr::Column(idx)) if arg_type == DataType::Decimal => {
                    Some(batch.schema.columns[*idx].scale)
                }
                _ => None,
            };
            let mut group_start = 0;
            while group_start < order.len() {
                let group_end = partition_end(&order, group_start, &partition_cols);
                let mut accumulator =
                    FrameAccumulator::new(*func, *distinct, arg_type, result_type, decimal_scale);
                let mut active = (group_start, group_start);
                for pos in group_start..group_end {
                    if pos % 1024 == 0 {
                        crate::common::query_cancel::check()?;
                    }
                    let target =
                        frame_bounds(plan, &order, &order_cols, group_start, group_end, pos);
                    accumulator.move_frame(&order, arg_values.as_deref(), &mut active, target)?;
                    values[order[pos] as usize] = accumulator.value();
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
        FrameBound::Following(n) => group_start + pos_in_group.saturating_add(*n).min(group_len),
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
        FrameBound::Following(n) => {
            group_start
                + pos_in_group
                    .saturating_add(*n)
                    .saturating_add(1)
                    .min(group_len)
        }
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
    let boundary = if preceding != desc { cur - n } else { cur + n };
    let mut lo = group_start;
    let mut hi = group_end;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let before = match range_key_at(order_cols, order, mid) {
            Some(value) => {
                if desc {
                    if start_bound {
                        value > boundary
                    } else {
                        value >= boundary
                    }
                } else if start_bound {
                    value < boundary
                } else {
                    value <= boundary
                }
            }
            // NULL sorting is shared with compare; offset frames exclude it.
            None => {
                compare(
                    &order_cols[0].0[order[mid] as usize],
                    &order_cols[0].0[order[pos] as usize],
                ) == if desc {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
        };
        if before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Some(lo)
}

/// First position of the peer run containing `pos`.
fn peer_start(
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    group_start: usize,
    pos: usize,
) -> usize {
    let mut lo = group_start;
    let mut hi = pos;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if order_key_peer(order, mid, pos, order_cols) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Last position (exclusive) of the peer run containing `pos`.
fn peer_end(
    order: &[u32],
    order_cols: &[(Vec<ScalarValue>, bool)],
    _group_start: usize,
    group_end: usize,
    pos: usize,
) -> usize {
    let mut lo = pos + 1;
    let mut hi = group_end;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if order_key_peer(order, pos, mid, order_cols) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// SQL order including NULL/NaN peer semantics, for extrema and DISTINCT.
#[derive(Clone)]
struct FrameKey(ScalarValue);
impl PartialEq for FrameKey {
    fn eq(&self, other: &Self) -> bool {
        compare(&self.0, &other.0) == Ordering::Equal
    }
}
impl Eq for FrameKey {}
impl PartialOrd for FrameKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FrameKey {
    fn cmp(&self, other: &Self) -> Ordering {
        compare(&self.0, &other.0)
    }
}

/// One incremental frame state for all aggregate/frame combinations.
/// Frame borders are looked up in logarithmic time; each entering/leaving
/// value is accumulated once, with no per-output-row frame allocation.
struct FrameAccumulator {
    func: AggFunc,
    distinct: bool,
    result_type: DataType,
    decimal_scale: Option<i32>,
    integer_sum: i128,
    float_sum: f64,
    count: usize,
    nan: usize,
    positive_inf: usize,
    negative_inf: usize,
    members: std::collections::BTreeMap<FrameKey, usize>,
}
const COUNT_ONE: ScalarValue = ScalarValue::Int64(Some(1));
impl FrameAccumulator {
    fn new(
        func: AggFunc,
        distinct: bool,
        _arg_type: DataType,
        result_type: DataType,
        decimal_scale: Option<i32>,
    ) -> Self {
        Self {
            func,
            distinct,
            result_type,
            decimal_scale,
            integer_sum: 0,
            float_sum: 0.0,
            count: 0,
            nan: 0,
            positive_inf: 0,
            negative_inf: 0,
            members: Default::default(),
        }
    }
    fn change(&mut self, value: &ScalarValue, adding: bool) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        if self.distinct || matches!(self.func, AggFunc::Min | AggFunc::Max) {
            let key = FrameKey(value.clone());
            if adding {
                let count = self.members.entry(key).or_default();
                *count += 1;
                if self.distinct && *count != 1 {
                    return Ok(());
                }
            } else {
                let count = self
                    .members
                    .get_mut(&key)
                    .expect("leaving value belongs to frame");
                *count -= 1;
                if *count == 0 {
                    self.members.remove(&key);
                } else if self.distinct {
                    return Ok(());
                }
            }
        }
        if adding {
            self.count += 1;
        } else {
            self.count -= 1;
        }
        if !matches!(self.func, AggFunc::Sum | AggFunc::Avg) {
            return Ok(());
        }
        let sign = if adding { 1 } else { -1 };
        if let Some(value) = value.integer_i128() {
            self.integer_sum = self.integer_sum.checked_add(value * sign).ok_or_else(|| {
                crate::common::error::TaibaiError::Sql("window aggregate integer overflow".into())
            })?;
        } else if let Some(value) = value.numeric_f64() {
            let counter = if value.is_nan() {
                Some(&mut self.nan)
            } else if value == f64::INFINITY {
                Some(&mut self.positive_inf)
            } else if value == f64::NEG_INFINITY {
                Some(&mut self.negative_inf)
            } else {
                None
            };
            if let Some(counter) = counter {
                if adding {
                    *counter += 1;
                } else {
                    *counter -= 1;
                }
            } else {
                self.float_sum += value * sign as f64;
            }
        }
        Ok(())
    }
    fn move_frame(
        &mut self,
        order: &[u32],
        values: Option<&[ScalarValue]>,
        active: &mut (usize, usize),
        target: (usize, usize),
    ) -> Result<()> {
        let at = |pos: usize| values.map_or(&COUNT_ONE, |v| &v[order[pos] as usize]);
        let (lo, hi) = *active;
        let (next_lo, next_hi) = target;
        // Remove the old-only portions, then add the new-only portions.
        for pos in lo..hi.min(next_lo) {
            if pos % 1024 == 0 {
                crate::common::query_cancel::check()?;
            }
            self.change(at(pos), false)?;
        }
        for pos in lo.max(next_hi)..hi {
            if pos % 1024 == 0 {
                crate::common::query_cancel::check()?;
            }
            self.change(at(pos), false)?;
        }
        for pos in next_lo..next_hi.min(lo) {
            if pos % 1024 == 0 {
                crate::common::query_cancel::check()?;
            }
            self.change(at(pos), true)?;
        }
        for pos in next_lo.max(hi)..next_hi {
            if pos % 1024 == 0 {
                crate::common::query_cancel::check()?;
            }
            self.change(at(pos), true)?;
        }
        *active = target;
        Ok(())
    }
    fn value(&self) -> ScalarValue {
        match self.func {
            AggFunc::Count => ScalarValue::Int64(Some(self.count as i64)),
            AggFunc::Min | AggFunc::Max => {
                let member = if self.func == AggFunc::Min {
                    self.members.first_key_value()
                } else {
                    self.members.last_key_value()
                };
                member.map_or_else(
                    || ScalarValue::null_for_type(self.result_type),
                    |(k, _)| k.0.clone(),
                )
            }
            _ if self.count == 0 => ScalarValue::null_for_type(self.result_type),
            AggFunc::Sum if self.result_type == DataType::Decimal => ScalarValue::Decimal(Some(
                self.integer_sum.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            )),
            AggFunc::Sum if self.result_type == DataType::Int64 => ScalarValue::Int64(Some(
                self.integer_sum.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            )),
            AggFunc::Sum | AggFunc::Avg => {
                let total = if self.nan > 0 || (self.positive_inf > 0 && self.negative_inf > 0) {
                    f64::NAN
                } else if self.positive_inf > 0 {
                    f64::INFINITY
                } else if self.negative_inf > 0 {
                    f64::NEG_INFINITY
                } else {
                    self.float_sum + self.integer_sum as f64
                };
                let value = if self.func == AggFunc::Avg {
                    total
                        / self.count as f64
                        / self.decimal_scale.map_or(1.0, |scale| 10f64.powi(scale))
                } else {
                    total
                };
                ScalarValue::Float64(Some(value))
            }
        }
    }
}
