use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::filter::{remap_aggregate_for_batch, remap_expr_for_batch};
use crate::sql::execution::spill::{MemoryManager, SpillFile};
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::storage::segment::SegmentReader;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use self::aggregate_fast::{
    fast_group_supported, fast_group_update, merge_slot_tables, FastGroups,
};
use super::ExecutionOp;

mod aggregate_fast;

pub struct HashAggregateExec {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
    /// Optional HAVING filter predicate (applied after aggregation)
    pub having: Option<LogicalExpr>,
    /// Whether to apply DISTINCT on the output
    pub distinct: bool,
    /// Upper bound on the number of GROUP BY groups before the query errors.
    pub max_groups: usize,
    /// Soft memory ceiling for group state. Sort can spill; aggregation
    /// currently fails predictably at this boundary instead of risking OOM.
    pub memory_limit_bytes: usize,
    done: bool,
    spill_merge: Option<GroupSpillMerge>,
}

impl HashAggregateExec {
    pub fn new(
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
        input: Box<dyn ExecutionOp>,
        input_schema: Schema,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input,
            input_schema,
            having: None,
            distinct: false,
            max_groups: crate::common::config::query_limits().max_aggregate_groups,
            memory_limit_bytes: crate::common::config::query_limits()
                .query_memory_limit_mb
                .saturating_mul(1024 * 1024)
                .min(usize::MAX as u64) as usize,
            done: false,
            spill_merge: None,
        }
    }

    pub fn with_having(mut self, having: LogicalExpr) -> Self {
        self.having = Some(having);
        self
    }

    pub fn with_distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    pub fn with_max_groups(mut self, max_groups: usize) -> Self {
        self.max_groups = max_groups.max(1);
        self
    }

    pub fn with_memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = bytes.max(1);
        self
    }
}

/// Streaming accumulator for a single aggregate function over a single group.
/// Memory usage is O(1) per accumulator (except DISTINCT which needs a value set).
#[derive(Serialize, Deserialize)]
enum Accumulator {
    /// `COUNT(*)` counts all rows; `COUNT(col)` counts non-null of `col`.
    Count {
        count: u64,
        col: CountKind,
    },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    SumI(Option<i64>),
    SumF(Option<f64>),
    /// Average tracks running sum + non-null count. `decimal_scale`
    /// records the argument column's DECIMAL scale: the sum accumulates in
    /// the raw scaled i64 domain, and finalize divides it back out.
    Avg {
        sum: f64,
        count: u64,
        any: bool,
        decimal_scale: Option<i32>,
    },
    /// DISTINCT sets need to remember seen values. Only used for
    /// COUNT/SUM/AVG DISTINCT over a single column. Values are stored as the
    /// self-describing `encode_key_at` bytes (type tag + null flag + typed
    /// payload) so the hot path avoids JSON/string work and the summary can
    /// decode them back for SUM/AVG/MIN/MAX.
    Distinct {
        func: AggFunc,
        seen: std::collections::HashSet<Vec<u8>>,
        is_count_star: bool,
    },
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum CountKind {
    /// COUNT(*) — every row counts.
    Star,
    /// COUNT(col) — only non-null values of this column count.
    Column(usize),
}

impl Accumulator {
    fn new_for(agg: &AggregateExpr, input_col_type: DataType, input_scale: i32) -> Result<Self> {
        let col_idx = match &*agg.arg {
            LogicalExpr::Column(idx) => Some(*idx),
            _ => None,
        };
        if agg.distinct {
            let is_count_star = matches!(agg.func, AggFunc::Count) && col_idx.is_none();
            return Ok(Accumulator::Distinct {
                func: agg.func,
                seen: std::collections::HashSet::new(),
                is_count_star,
            });
        }
        Ok(match agg.func {
            AggFunc::Count => Accumulator::Count {
                count: 0,
                col: match col_idx {
                    Some(i) => CountKind::Column(i),
                    None => CountKind::Star,
                },
            },
            AggFunc::Min => Accumulator::Min(None),
            AggFunc::Max => Accumulator::Max(None),
            AggFunc::Sum => match input_col_type {
                DataType::Float32 | DataType::Float64 => Accumulator::SumF(None),
                _ => Accumulator::SumI(None),
            },
            AggFunc::Avg => Accumulator::Avg {
                sum: 0.0,
                count: 0,
                any: false,
                decimal_scale: (input_col_type == DataType::Decimal).then_some(input_scale),
            },
        })
    }

    /// Update the accumulator from one input batch. `col_idx_opt` is the
    /// argument column index (None for wildcard). Column indices are relative
    /// to `batch` (physical) columns.
    fn update(&mut self, batch: &RecordBatch, col_idx_opt: Option<usize>) -> Result<()> {
        match self {
            Accumulator::Count { count, col } => match *col {
                CountKind::Star => *count += batch.num_rows() as u64,
                CountKind::Column(c) => {
                    // `col_idx_opt` carries the batch-remapped column index;
                    // the stored index is logical and may be stale after
                    // projection pushdown.
                    let column = col_idx_opt.unwrap_or(c);
                    if let Some(col_arr) = batch.column(column) {
                        *count += col_arr.col_count_non_null();
                    }
                }
            },
            Accumulator::Min(best) => {
                if let Some(c) = col_idx_opt {
                    if let Some(col_arr) = batch.column(c) {
                        if let Some(m) = col_arr.col_min() {
                            *best = Some(match best.take() {
                                Some(b) => {
                                    if compare_scalars(&m, &b) == std::cmp::Ordering::Less {
                                        m
                                    } else {
                                        b
                                    }
                                }
                                None => m,
                            });
                        }
                    }
                }
            }
            Accumulator::Max(best) => {
                if let Some(c) = col_idx_opt {
                    if let Some(col_arr) = batch.column(c) {
                        if let Some(m) = col_arr.col_max() {
                            *best = Some(match best.take() {
                                Some(b) => {
                                    if compare_scalars(&m, &b) == std::cmp::Ordering::Greater {
                                        m
                                    } else {
                                        b
                                    }
                                }
                                None => m,
                            });
                        }
                    }
                }
            }
            Accumulator::SumI(acc) => {
                if let Some(c) = col_idx_opt {
                    if let Some(col_arr) = batch.column(c) {
                        if let Some(part) = col_arr.col_sum_i64() {
                            *acc = Some(match acc.take() {
                                Some(prev) => prev
                                    .checked_add(part)
                                    .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
                                None => part,
                            });
                        }
                    }
                }
            }
            Accumulator::SumF(acc) => {
                if let Some(c) = col_idx_opt {
                    if let Some(col_arr) = batch.column(c) {
                        if let Some(part) = col_arr.col_sum_f64() {
                            *acc = Some(match acc.take() {
                                Some(prev) => prev + part,
                                None => part,
                            });
                        } else if let Some(part) = col_arr.col_sum_i64() {
                            *acc = Some(match acc.take() {
                                Some(prev) => prev + part as f64,
                                None => part as f64,
                            });
                        }
                    }
                }
            }
            Accumulator::Avg {
                sum, count, any, ..
            } => {
                if let Some(c) = col_idx_opt {
                    if let Some(col_arr) = batch.column(c) {
                        let n = col_arr.col_count_non_null();
                        if n > 0 {
                            let part = col_arr
                                .col_sum_f64()
                                .or_else(|| col_arr.col_sum_i64().map(|x| x as f64))
                                .unwrap_or(0.0);
                            *sum += part;
                            *count += n;
                            *any = true;
                        }
                    }
                }
            }
            Accumulator::Distinct { seen, .. } => {
                // For DISTINCT we must inspect per-row values; the encoded
                // key bytes double as the decodable value representation.
                let mut key = Vec::new();
                match col_idx_opt {
                    Some(c) => {
                        if let Some(col_arr) = batch.column(c) {
                            for i in 0..col_arr.len() {
                                if col_arr.bitmap().is_null(i) {
                                    continue;
                                }
                                key.clear();
                                col_arr.encode_key_at(i, &mut key);
                                seen.insert(key.clone());
                            }
                        }
                    }
                    None => {
                        // COUNT(DISTINCT *) — count distinct full rows.
                        for row in 0..batch.num_rows() {
                            key.clear();
                            for column in &batch.columns {
                                column.encode_key_at(row, &mut key);
                            }
                            seen.insert(key.clone());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn update_row(
        &mut self,
        batch: &RecordBatch,
        col_idx_opt: Option<usize>,
        row: usize,
    ) -> Result<()> {
        match self {
            Accumulator::Count { count, col } => match *col {
                CountKind::Star => *count += 1,
                CountKind::Column(column) => {
                    let column = col_idx_opt.unwrap_or(column);
                    if batch
                        .column(column)
                        .is_some_and(|col| !col.bitmap().is_null(row))
                    {
                        *count += 1;
                    }
                }
            },
            Accumulator::SumI(value) => {
                if let Some(part) = col_idx_opt
                    .and_then(|column| batch.column(column))
                    .and_then(|col| col.numeric_i64_at(row))
                {
                    *value = Some(
                        value
                            .unwrap_or(0)
                            .checked_add(part)
                            .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
                    );
                }
            }
            Accumulator::SumF(value) => {
                if let Some(part) = col_idx_opt
                    .and_then(|column| batch.column(column))
                    .and_then(|col| col.numeric_f64_at(row))
                {
                    *value = Some(value.unwrap_or(0.0) + part);
                }
            }
            Accumulator::Avg {
                sum, count, any, ..
            } => {
                if let Some(part) = col_idx_opt
                    .and_then(|column| batch.column(column))
                    .and_then(|col| col.numeric_f64_at(row))
                {
                    *sum += part;
                    *count += 1;
                    *any = true;
                }
            }
            Accumulator::Distinct { seen, .. } => {
                let mut key = Vec::new();
                if let Some(column) = col_idx_opt.and_then(|column| batch.column(column)) {
                    if !column.bitmap().is_null(row) {
                        column.encode_key_at(row, &mut key);
                    }
                } else {
                    for column in &batch.columns {
                        column.encode_key_at(row, &mut key);
                    }
                }
                if !key.is_empty() {
                    seen.insert(key);
                }
            }
            // MIN/MAX keep an owned value per group, but avoid constructing a
            // one-row RecordBatch and touching unrelated columns.
            Accumulator::Min(best) => {
                let Some(column) = col_idx_opt.and_then(|column| batch.column(column)) else {
                    return Ok(());
                };
                if column.bitmap().is_null(row) {
                    return Ok(());
                }
                let candidate = column.scalar(row);
                let replace = best
                    .as_ref()
                    .is_none_or(|value| compare_scalars(&candidate, value).is_lt());
                if replace {
                    *best = Some(candidate);
                }
            }
            Accumulator::Max(best) => {
                let Some(column) = col_idx_opt.and_then(|column| batch.column(column)) else {
                    return Ok(());
                };
                if column.bitmap().is_null(row) {
                    return Ok(());
                }
                let candidate = column.scalar(row);
                let replace = best
                    .as_ref()
                    .is_none_or(|value| compare_scalars(&candidate, value).is_gt());
                if replace {
                    *best = Some(candidate);
                }
            }
        }
        Ok(())
    }

    /// Finalize into a result scalar. `col_type` is the argument column type
    /// (used to choose null representation / sum type).
    fn finalize(&self, col_type: DataType) -> ScalarValue {
        match self {
            Accumulator::Count { count, .. } => ScalarValue::Int64(Some(*count as i64)),
            Accumulator::Min(v) => v.clone().unwrap_or_else(|| null_scalar_of_type(col_type)),
            Accumulator::Max(v) => v.clone().unwrap_or_else(|| null_scalar_of_type(col_type)),
            Accumulator::SumI(v) => match col_type {
                DataType::Float32 | DataType::Float64 => ScalarValue::Float64(v.map(|x| x as f64)),
                DataType::Decimal => ScalarValue::Decimal(*v),
                _ => ScalarValue::Int64(*v),
            },
            Accumulator::SumF(v) => ScalarValue::Float64(*v),
            Accumulator::Avg {
                sum,
                count,
                any,
                decimal_scale,
            } => {
                if *any && *count > 0 {
                    // Decimal sums accumulate raw scaled i64; divide the
                    // scale back out so AVG(price) is not 10^scale too large.
                    let scale_divisor = decimal_scale.map_or(1.0, |sc| 10f64.powi(sc));
                    ScalarValue::Float64(Some(*sum / *count as f64 / scale_divisor))
                } else {
                    ScalarValue::Float64(None)
                }
            }
            Accumulator::Distinct {
                func,
                seen,
                is_count_star,
            } => finalize_distinct_values(
                *func,
                seen.iter().map(|bytes| decode_key_scalar(bytes)),
                col_type,
                *is_count_star,
            ),
        }
    }

    fn estimated_size_bytes(&self) -> usize {
        let dynamic = match self {
            Accumulator::Distinct { seen, .. } => seen
                .iter()
                .map(|value| value.capacity() + std::mem::size_of::<Vec<u8>>())
                .sum(),
            Accumulator::Min(Some(value)) | Accumulator::Max(Some(value)) => {
                scalar_dynamic_size(value)
            }
            _ => 0,
        };
        std::mem::size_of::<Self>() + dynamic
    }

    fn merge(&mut self, other: Accumulator) -> Result<()> {
        match (self, other) {
            (Accumulator::Count { count, .. }, Accumulator::Count { count: rhs, .. }) => {
                *count = count.saturating_add(rhs);
            }
            (Accumulator::Min(left), Accumulator::Min(right)) => {
                if let Some(right) = right {
                    let replace = left
                        .as_ref()
                        .is_none_or(|current| compare_scalars(&right, current).is_lt());
                    if replace {
                        *left = Some(right);
                    }
                }
            }
            (Accumulator::Max(left), Accumulator::Max(right)) => {
                if let Some(right) = right {
                    let replace = left
                        .as_ref()
                        .is_none_or(|current| compare_scalars(&right, current).is_gt());
                    if replace {
                        *left = Some(right);
                    }
                }
            }
            (Accumulator::SumI(left), Accumulator::SumI(right)) => {
                if let Some(right) = right {
                    *left = Some(
                        left.unwrap_or(0)
                            .checked_add(right)
                            .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
                    );
                }
            }
            (Accumulator::SumF(left), Accumulator::SumF(right)) => {
                if let Some(right) = right {
                    *left = Some(left.unwrap_or(0.0) + right);
                }
            }
            (
                Accumulator::Avg {
                    sum,
                    count,
                    any,
                    decimal_scale,
                },
                Accumulator::Avg {
                    sum: rhs_sum,
                    count: rhs_count,
                    any: rhs_any,
                    decimal_scale: rhs_scale,
                },
            ) => {
                *sum += rhs_sum;
                *count = count.saturating_add(rhs_count);
                *any |= rhs_any;
                if *decimal_scale != rhs_scale {
                    return Err(TaibaiError::Internal(
                        "avg accumulator scale mismatch in spill merge".into(),
                    ));
                }
            }
            (
                Accumulator::Distinct {
                    func,
                    seen,
                    is_count_star,
                },
                Accumulator::Distinct {
                    func: rhs_func,
                    seen: rhs_seen,
                    is_count_star: rhs_count_star,
                },
            ) if *func == rhs_func && *is_count_star == rhs_count_star => {
                seen.extend(rhs_seen);
            }
            _ => {
                return Err(TaibaiError::Internal(
                    "incompatible aggregate states in spill merge".into(),
                ));
            }
        }
        Ok(())
    }

    fn take_distinct_values(
        &mut self,
    ) -> Option<(AggFunc, bool, std::collections::HashSet<Vec<u8>>)> {
        match self {
            Accumulator::Distinct {
                func,
                seen,
                is_count_star,
            } => Some((*func, *is_count_star, std::mem::take(seen))),
            _ => None,
        }
    }
}

impl ExecutionOp for HashAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(merge) = self.spill_merge.as_mut() {
            let result = merge.next_batch()?;
            if result.is_none() {
                self.spill_merge = None;
            }
            return Ok(result);
        }
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Stream input batches; do NOT materialize all rows.
        let mut physical_schema: Option<Schema> = None;

        // Resolve group-by / aggregate column indices lazily after we see the
        // first batch (physical schema may differ from logical schema due to
        // projection pushdown).
        let mut group_by: Vec<LogicalExpr> = self.group_by.clone();
        let mut aggregates: Vec<AggregateExpr> = self.aggregates.clone();
        let mut remapped = false;

        if self.group_by.is_empty() {
            let mut memory = MemoryManager::reservation();
            // ----- No GROUP BY: a single set of accumulators -----------------
            let mut accums: Vec<Accumulator> = Vec::with_capacity(self.aggregates.len());
            // Pre-create using the logical (input) schema types; col indices are
            // remapped below once the physical schema is known.
            for agg in &self.aggregates {
                let (col_type, col_scale) = match &*agg.arg {
                    LogicalExpr::Column(idx) => self
                        .input_schema
                        .columns
                        .get(*idx)
                        .map(|c| (c.data_type, c.scale))
                        .unwrap_or((DataType::Int64, 0)),
                    _ => (DataType::Int64, 0),
                };
                accums.push(Accumulator::new_for(agg, col_type, col_scale)?);
            }
            let spill_rows = crate::common::config::query_limits()
                .spill_batch_rows
                .max(1);
            let mut distinct_spills: Vec<Option<DistinctSpillAccumulator>> =
                std::iter::repeat_with(|| None).take(accums.len()).collect();

            while let Some(batch) = self.input.next_batch()? {
                crate::common::query_cancel::check()?;
                if physical_schema.is_none() {
                    physical_schema = Some(batch.schema.clone());
                }
                if !remapped {
                    let phys = physical_schema.as_ref().unwrap_or(&self.input_schema);
                    aggregates = self
                        .aggregates
                        .iter()
                        .map(|a| remap_aggregate_for_batch(a, &self.input_schema, phys))
                        .collect();
                    remapped = true;
                }
                for (acc, agg) in accums.iter_mut().zip(aggregates.iter()) {
                    let col_idx = match &*agg.arg {
                        LogicalExpr::Column(idx) => Some(*idx),
                        _ => None,
                    };
                    acc.update(&batch, col_idx)?;
                }
                let estimated_bytes = accums
                    .iter()
                    .map(Accumulator::estimated_size_bytes)
                    .sum::<usize>();
                super::profile::record_current_peak_memory(estimated_bytes);
                if estimated_bytes >= self.memory_limit_bytes || !memory.try_resize(estimated_bytes)
                {
                    spill_distinct_accumulators(&mut accums, &mut distinct_spills, spill_rows)?;
                    memory.release();
                }
            }

            let physical_schema = physical_schema.unwrap_or_else(|| self.input_schema.clone());
            if distinct_spills.iter().any(Option::is_some) {
                spill_distinct_accumulators(&mut accums, &mut distinct_spills, spill_rows)?;
            }
            let mut overrides = vec![None; accums.len()];
            for (index, spill) in distinct_spills.into_iter().enumerate() {
                if let Some(spill) = spill {
                    overrides[index] = Some(
                        spill.finalize(arg_column_type(&aggregates[index], &physical_schema))?,
                    );
                }
            }
            let result = self.finalize_no_group_by(
                &accums,
                &aggregates,
                &physical_schema,
                Some(&overrides),
            )?;
            return Ok(Some(result));
        }

        // ----- GROUP BY: per-group accumulators in a hash map ---------------
        // `groups` maps the stringified key to its accumulators; `group_keys`
        // preserves insertion order alongside the original (typed) group-by
        // scalar values so the output keeps the correct column types.
        let mut groups: HashMap<Vec<u8>, Vec<Accumulator>> = HashMap::new();
        let mut group_keys: Vec<(Vec<u8>, Vec<ScalarValue>)> = Vec::new();
        let mut estimated_group_bytes = 0usize;
        let mut spills = Vec::new();
        let mut memory = MemoryManager::reservation();
        let mut key_buf: Vec<u8> = Vec::new();
        let spill_rows = crate::common::config::query_limits()
            .spill_batch_rows
            .max(1);
        let mut fast_tables: Vec<Option<FastGroups>> = Vec::new();
        let mut fast_unsupported = false;

        while let Some(batch) = self.input.next_batch()? {
            crate::common::query_cancel::check()?;
            if physical_schema.is_none() {
                physical_schema = Some(batch.schema.clone());
            }
            if !remapped {
                let phys = physical_schema.as_ref().unwrap_or(&self.input_schema);
                group_by = self
                    .group_by
                    .iter()
                    .map(|e| remap_expr_for_batch(e, &self.input_schema, phys))
                    .collect();
                aggregates = self
                    .aggregates
                    .iter()
                    .map(|a| remap_aggregate_for_batch(a, &self.input_schema, phys))
                    .collect();
                remapped = true;
            }
            let gb_indices: Vec<usize> = group_by
                .iter()
                .map(|e| match e {
                    LogicalExpr::Column(i) => *i,
                    _ => 0,
                })
                .collect();
            // Aggregate argument column indices are stable once the physical
            // schema is known; resolve them once per batch instead of per row.
            let agg_col_indices: Vec<Option<usize>> = aggregates
                .iter()
                .map(|agg| match &*agg.arg {
                    LogicalExpr::Column(idx) => Some(*idx),
                    _ => None,
                })
                .collect();

            // Single-column GROUP BY fast path: intern the group key into a
            // dense u32 id (FxHash family, no SipHash, no per-row key buffer)
            // and accumulate through pre-resolved typed column views across
            // per-lane tables merged at the end. Falls back to the generic
            // per-row loop for any unsupported shape.
            if !fast_unsupported && fast_tables.is_empty() {
                if !fast_group_supported(
                    &group_by,
                    &gb_indices,
                    &aggregates,
                    &agg_col_indices,
                    &batch,
                ) {
                    fast_unsupported = true;
                }
            }
            if !fast_unsupported {
                let phys = physical_schema.as_ref().unwrap_or(&self.input_schema);
                fast_group_update(
                    &mut fast_tables,
                    &batch,
                    gb_indices[0],
                    &aggregates,
                    &agg_col_indices,
                    phys,
                    self.max_groups,
                    self.memory_limit_bytes,
                    &mut estimated_group_bytes,
                    &mut memory,
                    &mut spills,
                    spill_rows,
                )?;
                continue;
            }

            for row in 0..batch.num_rows() {
                // The key buffer is reused across rows: the previous code
                // allocated a fresh Vec per row, which dominated grouped-query
                // profiles.
                key_buf.clear();
                for &column in &gb_indices {
                    if let Some(array) = batch.column(column) {
                        array.encode_key_at(row, &mut key_buf);
                    }
                }

                // Single hash lookup for the hot (existing group) path; the
                // double-hash contains_key + get_mut pattern is gone.
                let accums = match groups.get_mut(&key_buf) {
                    Some(accums) => accums,
                    None => {
                        let key_scalars: Vec<ScalarValue> = gb_indices
                            .iter()
                            .map(|&column| {
                                batch
                                    .column(column)
                                    .map(|array| array.scalar(row))
                                    .unwrap_or(ScalarValue::Int64(None))
                            })
                            .collect();
                        if groups.len() >= self.max_groups {
                            return Err(TaibaiError::Sql(format!(
                                "aggregate group count exceeds limit {}; \
                                 consider refining GROUP BY to reduce cardinality",
                                self.max_groups
                            )));
                        }
                        let accums: Result<Vec<Accumulator>> = self
                            .aggregates
                            .iter()
                            .map(|agg| {
                                let (col_type, col_scale) = match &*agg.arg {
                                    LogicalExpr::Column(idx) => self
                                        .input_schema
                                        .columns
                                        .get(*idx)
                                        .map(|c| (c.data_type, c.scale))
                                        .unwrap_or((DataType::Int64, 0)),
                                    _ => (DataType::Int64, 0),
                                };
                                Accumulator::new_for(agg, col_type, col_scale)
                            })
                            .collect();
                        let accums = accums?;
                        estimated_group_bytes = estimated_group_bytes.saturating_add(
                            key_buf.len()
                                + accums
                                    .iter()
                                    .map(Accumulator::estimated_size_bytes)
                                    .sum::<usize>()
                                + key_scalars.len() * std::mem::size_of::<ScalarValue>(),
                        );
                        group_keys.push((key_buf.clone(), key_scalars));
                        groups.insert(key_buf.clone(), accums);
                        groups.get_mut(&key_buf).expect("group inserted above")
                    }
                };

                for (acc, col_idx) in accums.iter_mut().zip(agg_col_indices.iter()) {
                    acc.update_row(&batch, *col_idx, row)?;
                }
            }

            // Deep memory refresh once per batch (matching the no-GROUP-BY
            // path): DISTINCT sets and MIN/MAX values grow per row, but
            // re-estimating on every row dominated grouped-query profiles.
            estimated_group_bytes = group_keys
                .iter()
                .map(|(key, scalars)| {
                    key.len() + scalars.len() * std::mem::size_of::<ScalarValue>()
                })
                .sum::<usize>()
                + groups
                    .values()
                    .map(|accums| {
                        accums
                            .iter()
                            .map(Accumulator::estimated_size_bytes)
                            .sum::<usize>()
                    })
                    .sum::<usize>();
            super::profile::record_current_peak_memory(estimated_group_bytes);
            if estimated_group_bytes >= self.memory_limit_bytes
                || !memory.try_resize(estimated_group_bytes)
            {
                spills.push(spill_group_run(&groups, &group_keys, spill_rows)?);
                groups.clear();
                group_keys.clear();
                estimated_group_bytes = 0;
                memory.release();
            }
        }

        let physical_schema = physical_schema.unwrap_or_else(|| self.input_schema.clone());
        let (groups, group_keys) = if !fast_unsupported {
            let (groups, group_keys) = merge_slot_tables(&mut fast_tables)?.into_maps();
            // The per-slot budget only bounds each lane's table; the merged
            // total is what the configured limit means (the generic path
            // errors at the same total).
            if groups.len() > self.max_groups {
                return Err(TaibaiError::Sql(format!(
                    "aggregate group count exceeds limit {}; \
                     consider refining GROUP BY to reduce cardinality",
                    self.max_groups
                )));
            }
            (groups, group_keys)
        } else {
            (groups, group_keys)
        };
        if !spills.is_empty() {
            if !groups.is_empty() {
                spills.push(spill_group_run(&groups, &group_keys, spill_rows)?);
            }
            let output_schema = grouped_output_schema(&group_by, &aggregates, &physical_schema);
            self.spill_merge = Some(GroupSpillMerge::new(
                spills,
                output_schema,
                aggregates,
                physical_schema,
                self.input_schema.clone(),
                self.having.clone(),
                self.max_groups,
                spill_rows,
            )?);
            return self
                .spill_merge
                .as_mut()
                .expect("spill merge initialized")
                .next_batch();
        }
        let result = self.finalize_grouped(
            &group_by,
            &group_keys,
            &groups,
            &aggregates,
            &physical_schema,
        )?;
        Ok(Some(result))
    }
}

impl HashAggregateExec {
    fn finalize_no_group_by(
        &self,
        accums: &[Accumulator],
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
        overrides: Option<&[Option<ScalarValue>]>,
    ) -> Result<RecordBatch> {
        let mut col_defs = Vec::new();
        for agg in aggregates {
            col_defs.push(aggregate_result_column_def(agg, physical_schema));
        }
        let schema = Schema::new(col_defs);
        let mut result_batch = RecordBatch::new(schema);

        for (i, (acc, agg)) in accums.iter().zip(aggregates.iter()).enumerate() {
            let col_type = arg_column_type(agg, physical_schema);
            let val = overrides
                .and_then(|values| values.get(i))
                .and_then(Clone::clone)
                .unwrap_or_else(|| acc.finalize(col_type));
            result_batch
                .column_mut(i)
                .ok_or_else(|| TaibaiError::Internal("aggregate column missing".into()))?
                .push(&val)?;
        }

        if self.distinct {
            result_batch = apply_distinct(result_batch);
        }
        if let Some(ref having_pred) = self.having {
            result_batch = apply_having(result_batch, having_pred, &self.input_schema)?;
        }
        Ok(result_batch)
    }

    fn finalize_grouped(
        &self,
        group_by: &[LogicalExpr],
        group_keys: &[(Vec<u8>, Vec<ScalarValue>)],
        groups: &HashMap<Vec<u8>, Vec<Accumulator>>,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
    ) -> Result<RecordBatch> {
        let mut col_defs = Vec::new();
        let ngb = group_by.len();
        // group-by output columns keep their original physical types
        for (i, _) in group_by.iter().enumerate() {
            // Recover the type from the first representative scalar instead of
            // relying on physical_schema (the remapped logical indices already
            // match physical_schema here, so both are fine).
            let dt = group_by
                .get(i)
                .and_then(|e| match e {
                    LogicalExpr::Column(idx) => physical_schema.columns.get(*idx),
                    _ => None,
                })
                .map(|c| c.data_type)
                .unwrap_or(DataType::VarChar);
            let name = group_by
                .get(i)
                .and_then(|e| match e {
                    LogicalExpr::Column(idx) => physical_schema.columns.get(*idx),
                    _ => None,
                })
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{}", i));
            let mut def = ColumnDef::new(&name, dt);
            if let LogicalExpr::Column(idx) = &group_by[i] {
                if let Some(source) = physical_schema.columns.get(*idx) {
                    def.scale = source.scale;
                    def.precision = source.precision;
                }
            }
            col_defs.push(def);
        }
        for agg in aggregates {
            col_defs.push(aggregate_result_column_def(agg, physical_schema));
        }
        let schema = Schema::new(col_defs);
        let mut result_batch = RecordBatch::new(schema);

        for (key, scalars) in group_keys {
            // group-by columns: use the original typed scalars.
            for (i, s) in scalars.iter().enumerate() {
                result_batch
                    .column_mut(i)
                    .ok_or_else(|| TaibaiError::Internal("aggregate column missing".into()))?
                    .push(s)?;
            }
            if let Some(accums) = groups.get(key) {
                for (i, (acc, agg)) in accums.iter().zip(aggregates.iter()).enumerate() {
                    let col_type = arg_column_type(agg, physical_schema);
                    let val = acc.finalize(col_type);
                    result_batch
                        .column_mut(ngb + i)
                        .ok_or_else(|| TaibaiError::Internal("aggregate column missing".into()))?
                        .push(&val)?;
                }
            }
        }

        if self.distinct {
            result_batch = apply_distinct(result_batch);
        }
        if let Some(ref having_pred) = self.having {
            result_batch = apply_having(result_batch, having_pred, &self.input_schema)?;
        }
        Ok(result_batch)
    }
}

fn spill_distinct_accumulators(
    accumulators: &mut [Accumulator],
    spills: &mut [Option<DistinctSpillAccumulator>],
    spill_rows: usize,
) -> Result<()> {
    for (index, accumulator) in accumulators.iter_mut().enumerate() {
        let Some((func, is_count_star, values)) = accumulator.take_distinct_values() else {
            continue;
        };
        if values.is_empty() {
            continue;
        }
        let spill = spills[index]
            .get_or_insert_with(|| DistinctSpillAccumulator::new(func, is_count_star, spill_rows));
        spill.spill_values(values)?;
    }
    Ok(())
}

fn grouped_output_schema(
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
    physical_schema: &Schema,
) -> Schema {
    let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
    for (position, expr) in group_by.iter().enumerate() {
        let source = match expr {
            LogicalExpr::Column(index) => physical_schema.columns.get(*index),
            _ => None,
        };
        columns.push(ColumnDef::new(
            source
                .map(|column| column.name.as_str())
                .unwrap_or("group_expr"),
            source
                .map(|column| column.data_type)
                .unwrap_or(DataType::VarChar),
        ));
        if source.is_none() {
            columns[position].name = format!("group_expr_{position}");
        }
    }
    for aggregate in aggregates {
        columns.push(aggregate_result_column_def(aggregate, physical_schema));
    }
    Schema::new(columns)
}

fn aggregate_spill_schema(key_types: &[DataType]) -> Schema {
    // Typed key columns: the JSON string encoding used before collapsed NaN
    // to null, so NULL and NaN group keys merged into one group on restore.
    let mut columns: Vec<ColumnDef> = key_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| ColumnDef::new(&format!("group_key_{idx}"), *data_type))
        .collect();
    columns.push(ColumnDef::new("aggregate_state", DataType::VarChar));
    Schema::new(columns)
}

fn spill_group_run(
    groups: &HashMap<Vec<u8>, Vec<Accumulator>>,
    group_keys: &[(Vec<u8>, Vec<ScalarValue>)],
    spill_rows: usize,
) -> Result<SpillFile> {
    // Order by the in-memory byte key: the merge compares exactly these
    // bytes, and unlike the old JSON-string ordering they keep NaN and NULL
    // distinct. Keys spill as typed columns (lossless round-trip for output).
    let mut entries = group_keys
        .iter()
        .filter_map(|(key, scalars)| {
            groups
                .get(key)
                .map(|accumulators| (key, scalars, accumulators))
        })
        .collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0));

    let key_types: Vec<DataType> = group_keys
        .first()
        .map(|(_, scalars)| scalars.iter().map(ScalarValue::data_type).collect())
        .unwrap_or_default();
    let schema = aggregate_spill_schema(&key_types);
    let batch_schema = schema.clone();
    let batches = entries.chunks(spill_rows.max(1)).map(move |chunk| {
        let mut key_columns: Vec<ColumnArray> = key_types
            .iter()
            .map(|data_type| ColumnArray::empty(*data_type))
            .collect();
        let mut state_column = ColumnArray::empty(DataType::VarChar);
        for (_, scalars, accumulators) in chunk {
            let state = serde_json::to_string(accumulators).map_err(|error| {
                TaibaiError::Internal(format!("failed to encode aggregate spill state: {error}"))
            })?;
            for (column, value) in key_columns.iter_mut().zip(scalars.iter()) {
                column.push(value)?;
            }
            state_column.push(&ScalarValue::VarChar(Some(state)))?;
        }
        let mut columns = key_columns;
        columns.push(state_column);
        RecordBatch::try_new(batch_schema.clone(), columns)
    });
    SpillFile::write_batches_from_iter(schema, batches, spill_rows)
}

struct GroupSpillCursor {
    _spill: SpillFile,
    reader: SegmentReader,
    row_group: usize,
    batch: Option<RecordBatch>,
    row: usize,
}

impl GroupSpillCursor {
    fn new(spill: SpillFile) -> Result<Self> {
        let reader = spill.open_reader()?;
        let mut cursor = Self {
            _spill: spill,
            reader,
            row_group: 0,
            batch: None,
            row: 0,
        };
        cursor.load_next_batch()?;
        Ok(cursor)
    }

    fn load_next_batch(&mut self) -> Result<()> {
        while self.row_group < self.reader.num_row_groups() {
            let batch = self.reader.read_row_group(self.row_group)?;
            self.row_group += 1;
            if batch.num_rows() > 0 {
                self.batch = Some(batch);
                self.row = 0;
                return Ok(());
            }
        }
        self.batch = None;
        Ok(())
    }

    /// Byte key (the merge identity, identical to the in-memory group-key
    /// encoding), the scalar values for output reconstruction, and the JSON
    /// accumulator state.
    fn current(&self) -> Result<(Vec<u8>, Vec<ScalarValue>, String)> {
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| TaibaiError::Internal("aggregate spill cursor exhausted".into()))?;
        let key_columns = batch.num_columns() - 1; // trailing state column
        let mut bytes = Vec::new();
        let mut scalars = Vec::with_capacity(key_columns);
        for column_idx in 0..key_columns {
            batch
                .column(column_idx)
                .ok_or_else(|| TaibaiError::Internal("invalid aggregate spill key column".into()))?
                .encode_key_at(self.row, &mut bytes);
            scalars.push(batch.scalar(self.row, column_idx).ok_or_else(|| {
                TaibaiError::Internal("invalid aggregate spill key scalar".into())
            })?);
        }
        let state = match batch.scalar(self.row, key_columns) {
            Some(ScalarValue::VarChar(Some(value))) => value,
            _ => {
                return Err(TaibaiError::Internal(
                    "invalid aggregate spill state".into(),
                ));
            }
        };
        Ok((bytes, scalars, state))
    }

    fn advance(&mut self) -> Result<()> {
        self.row += 1;
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| self.row >= batch.num_rows())
        {
            self.load_next_batch()?;
        }
        Ok(())
    }

    fn has_row(&self) -> bool {
        self.batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.num_rows())
    }
}

#[derive(Eq, PartialEq)]
struct GroupHeapItem {
    key: Vec<u8>,
    cursor: usize,
}

impl Ord for GroupHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.cursor.cmp(&self.cursor))
    }
}

impl PartialOrd for GroupHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct GroupSpillMerge {
    cursors: Vec<GroupSpillCursor>,
    heap: BinaryHeap<GroupHeapItem>,
    output_schema: Schema,
    aggregates: Vec<AggregateExpr>,
    physical_schema: Schema,
    input_schema: Schema,
    having: Option<LogicalExpr>,
    max_groups: usize,
    emitted_groups: usize,
    batch_rows: usize,
}

impl GroupSpillMerge {
    fn new(
        spills: Vec<SpillFile>,
        output_schema: Schema,
        aggregates: Vec<AggregateExpr>,
        physical_schema: Schema,
        input_schema: Schema,
        having: Option<LogicalExpr>,
        max_groups: usize,
        batch_rows: usize,
    ) -> Result<Self> {
        let mut cursors = Vec::with_capacity(spills.len());
        let mut heap = BinaryHeap::new();
        for spill in spills {
            let cursor = GroupSpillCursor::new(spill)?;
            let index = cursors.len();
            if cursor.has_row() {
                heap.push(GroupHeapItem {
                    key: cursor.current()?.0,
                    cursor: index,
                });
            }
            cursors.push(cursor);
        }
        Ok(Self {
            cursors,
            heap,
            output_schema,
            aggregates,
            physical_schema,
            input_schema,
            having,
            max_groups,
            emitted_groups: 0,
            batch_rows: batch_rows.max(1),
        })
    }

    fn pop_state(&mut self) -> Result<(Vec<u8>, Vec<ScalarValue>, Vec<Accumulator>)> {
        let item = self
            .heap
            .pop()
            .ok_or_else(|| TaibaiError::Internal("aggregate spill heap exhausted".into()))?;
        let cursor = &mut self.cursors[item.cursor];
        let (key, key_scalars, encoded_state) = cursor.current()?;
        let state = serde_json::from_str(&encoded_state).map_err(|error| {
            TaibaiError::Internal(format!("failed to decode aggregate spill state: {error}"))
        })?;
        cursor.advance()?;
        if cursor.has_row() {
            self.heap.push(GroupHeapItem {
                key: cursor.current()?.0,
                cursor: item.cursor,
            });
        }
        Ok((key, key_scalars, state))
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            crate::common::query_cancel::check()?;
            if self.heap.is_empty() {
                return Ok(None);
            }
            let mut output =
                RecordBatch::with_capacity(self.output_schema.clone(), self.batch_rows);
            while output.num_rows() < self.batch_rows && !self.heap.is_empty() {
                let (key, key_scalars, mut accumulators) = self.pop_state()?;
                let mut distinct_spills: Vec<Option<DistinctSpillAccumulator>> =
                    std::iter::repeat_with(|| None)
                        .take(accumulators.len())
                        .collect();
                spill_distinct_accumulators(
                    &mut accumulators,
                    &mut distinct_spills,
                    self.batch_rows,
                )?;
                while self.heap.peek().is_some_and(|item| item.key == key) {
                    let (_, _, mut states) = self.pop_state()?;
                    if states.len() != accumulators.len() {
                        return Err(TaibaiError::Internal(
                            "aggregate spill state width mismatch".into(),
                        ));
                    }
                    spill_distinct_accumulators(
                        &mut states,
                        &mut distinct_spills,
                        self.batch_rows,
                    )?;
                    for (left, right) in accumulators.iter_mut().zip(states) {
                        left.merge(right)?;
                    }
                }

                self.emitted_groups = self.emitted_groups.saturating_add(1);
                if self.emitted_groups > self.max_groups {
                    return Err(TaibaiError::Sql(format!(
                        "aggregate group count exceeds limit {}",
                        self.max_groups
                    )));
                }
                for (column, value) in key_scalars.iter().enumerate() {
                    output
                        .column_mut(column)
                        .ok_or_else(|| {
                            TaibaiError::Internal("aggregate group column missing".into())
                        })?
                        .push(value)?;
                }
                let group_columns = key_scalars.len();
                for (index, (accumulator, aggregate)) in
                    accumulators.iter().zip(self.aggregates.iter()).enumerate()
                {
                    let col_type = arg_column_type(aggregate, &self.physical_schema);
                    let value = if let Some(spill) = distinct_spills[index].take() {
                        spill.finalize(col_type)?
                    } else {
                        accumulator.finalize(col_type)
                    };
                    output
                        .column_mut(group_columns + index)
                        .ok_or_else(|| {
                            TaibaiError::Internal("aggregate result column missing".into())
                        })?
                        .push(&value)?;
                }
            }

            if let Some(having) = self.having.as_ref() {
                output = apply_having(output, having, &self.input_schema)?;
            }
            if output.num_rows() > 0 {
                return Ok(Some(output));
            }
        }
    }
}

fn scalar_dynamic_size(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::VarChar(Some(value)) => value.capacity(),
        ScalarValue::Blob(Some(value)) => value.capacity(),
        ScalarValue::Vector(Some(value)) => value.capacity() * std::mem::size_of::<f32>(),
        _ => 0,
    }
}

/// Decode one `encode_key_at`-encoded key back into its scalar value. The
/// format is `[type_tag: u8][null_flag: u8][payload]` with `u32`
/// length/dimension prefixes for variable-width payloads; see
/// `ColumnArray::encode_key_at`. Returns `None` for malformed input (or a
/// NULL key, which never enters a DISTINCT set in the first place).
fn decode_key_scalar(bytes: &[u8]) -> Option<ScalarValue> {
    let (&tag, rest) = bytes.split_first()?;
    let (&flag, payload) = rest.split_first()?;
    if flag == 0 {
        return None;
    }
    fn take(payload: &[u8], width: usize) -> Option<&[u8]> {
        (payload.len() >= width).then(|| &payload[..width])
    }
    match tag {
        tag if tag == DataType::Bool as u8 => {
            Some(ScalarValue::Bool(Some(take(payload, 1)?[0] != 0)))
        }
        tag if tag == DataType::Int8 as u8 => Some(ScalarValue::Int8(Some(i8::from_le_bytes(
            take(payload, 1)?.try_into().ok()?,
        )))),
        tag if tag == DataType::Int16 as u8 => Some(ScalarValue::Int16(Some(i16::from_le_bytes(
            take(payload, 2)?.try_into().ok()?,
        )))),
        tag if tag == DataType::Int32 as u8 => Some(ScalarValue::Int32(Some(i32::from_le_bytes(
            take(payload, 4)?.try_into().ok()?,
        )))),
        tag if tag == DataType::Int64 as u8 => Some(ScalarValue::Int64(Some(i64::from_le_bytes(
            take(payload, 8)?.try_into().ok()?,
        )))),
        tag if tag == DataType::UInt8 as u8 => Some(ScalarValue::UInt8(Some(take(payload, 1)?[0]))),
        tag if tag == DataType::UInt16 as u8 => Some(ScalarValue::UInt16(Some(
            u16::from_le_bytes(take(payload, 2)?.try_into().ok()?),
        ))),
        tag if tag == DataType::UInt32 as u8 => Some(ScalarValue::UInt32(Some(
            u32::from_le_bytes(take(payload, 4)?.try_into().ok()?),
        ))),
        tag if tag == DataType::UInt64 as u8 => Some(ScalarValue::UInt64(Some(
            u64::from_le_bytes(take(payload, 8)?.try_into().ok()?),
        ))),
        tag if tag == DataType::Float32 as u8 => Some(ScalarValue::Float32(Some(
            f32::from_le_bytes(take(payload, 4)?.try_into().ok()?),
        ))),
        tag if tag == DataType::Float64 as u8 => Some(ScalarValue::Float64(Some(
            f64::from_le_bytes(take(payload, 8)?.try_into().ok()?),
        ))),
        tag if tag == DataType::Decimal as u8 => Some(ScalarValue::Decimal(Some(
            i64::from_le_bytes(take(payload, 8)?.try_into().ok()?),
        ))),
        tag if tag == DataType::Date as u8 => Some(ScalarValue::Date(Some(i32::from_le_bytes(
            take(payload, 4)?.try_into().ok()?,
        )))),
        tag if tag == DataType::Time as u8 => Some(ScalarValue::Time(Some(i64::from_le_bytes(
            take(payload, 8)?.try_into().ok()?,
        )))),
        tag if tag == DataType::Timestamp as u8 => Some(ScalarValue::Timestamp(Some(
            i64::from_le_bytes(take(payload, 8)?.try_into().ok()?),
        ))),
        tag if tag == DataType::Interval as u8 => Some(ScalarValue::Interval(Some(
            i64::from_le_bytes(take(payload, 8)?.try_into().ok()?),
        ))),
        tag if tag == DataType::VarChar as u8 => {
            let len = u32::from_le_bytes(take(payload, 4)?.try_into().ok()?) as usize;
            let raw = take(&payload[4..], len)?;
            Some(ScalarValue::VarChar(Some(
                String::from_utf8(raw.to_vec()).ok()?,
            )))
        }
        tag if tag == DataType::Blob as u8 => {
            let len = u32::from_le_bytes(take(payload, 4)?.try_into().ok()?) as usize;
            Some(ScalarValue::Blob(Some(take(&payload[4..], len)?.to_vec())))
        }
        tag if tag == DataType::Vector as u8 => {
            let dim = u32::from_le_bytes(take(payload, 4)?.try_into().ok()?) as usize;
            let raw = take(&payload[4..], dim * 4)?;
            let mut values = Vec::with_capacity(dim);
            for chunk in raw.chunks_exact(4) {
                values.push(f32::from_le_bytes(chunk.try_into().ok()?));
            }
            Some(ScalarValue::Vector(Some(values)))
        }
        _ => None,
    }
}

fn finalize_distinct_values(
    func: AggFunc,
    values: impl Iterator<Item = Option<ScalarValue>>,
    col_type: DataType,
    is_count_star: bool,
) -> ScalarValue {
    let mut summary = DistinctSummary::default();
    for value in values {
        summary.add_value(value, is_count_star);
    }
    summary.finalize(func, col_type)
}

#[derive(Default)]
struct DistinctSummary {
    count: u64,
    integer_sum: i128,
    float_sum: f64,
    min: Option<ScalarValue>,
    max: Option<ScalarValue>,
}

impl DistinctSummary {
    /// `value` is `None` when the encoded key could not be decoded (the
    /// pre-P1.2 behavior silently dropped such values from SUM/AVG/MIN/MAX).
    fn add_value(&mut self, value: Option<ScalarValue>, is_count_star: bool) {
        self.count = self.count.saturating_add(1);
        if is_count_star {
            return;
        }
        let Some(value) = value else {
            return;
        };
        if let Some(value) = value.integer_i128() {
            self.integer_sum = self.integer_sum.wrapping_add(value);
            self.float_sum += value as f64;
        } else if let Some(value) = value.numeric_f64() {
            self.float_sum += value;
        }
        if self
            .min
            .as_ref()
            .is_none_or(|current| compare_scalars(&value, current).is_lt())
        {
            self.min = Some(value.clone());
        }
        if self
            .max
            .as_ref()
            .is_none_or(|current| compare_scalars(&value, current).is_gt())
        {
            self.max = Some(value);
        }
    }

    fn finalize(self, func: AggFunc, col_type: DataType) -> ScalarValue {
        match func {
            AggFunc::Count => ScalarValue::Int64(Some(self.count as i64)),
            AggFunc::Sum if self.count == 0 => null_scalar_of_type(col_type),
            AggFunc::Sum if matches!(col_type, DataType::Float32 | DataType::Float64) => {
                ScalarValue::Float64(Some(self.float_sum))
            }
            AggFunc::Sum if col_type == DataType::Decimal => ScalarValue::Decimal(Some(
                self.integer_sum.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            )),
            AggFunc::Sum => ScalarValue::Int64(Some(
                self.integer_sum.clamp(i64::MIN as i128, i64::MAX as i128) as i64,
            )),
            AggFunc::Avg if self.count == 0 => ScalarValue::Float64(None),
            AggFunc::Avg => ScalarValue::Float64(Some(self.float_sum / self.count as f64)),
            AggFunc::Min => self.min.unwrap_or_else(|| null_scalar_of_type(col_type)),
            AggFunc::Max => self.max.unwrap_or_else(|| null_scalar_of_type(col_type)),
        }
    }
}

struct DistinctSpillAccumulator {
    func: AggFunc,
    is_count_star: bool,
    runs: Vec<SpillFile>,
    batch_rows: usize,
}

impl DistinctSpillAccumulator {
    fn new(func: AggFunc, is_count_star: bool, batch_rows: usize) -> Self {
        Self {
            func,
            is_count_star,
            runs: Vec::new(),
            batch_rows: batch_rows.max(1),
        }
    }

    fn spill_values(&mut self, values: std::collections::HashSet<Vec<u8>>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let mut values = values.into_iter().collect::<Vec<_>>();
        values.sort_unstable();
        let schema = Schema::new(vec![ColumnDef::new("distinct_value", DataType::Blob)]);
        let batch_schema = schema.clone();
        let batches = values.chunks(self.batch_rows).map(move |chunk| {
            let mut column = ColumnArray::empty(DataType::Blob);
            for value in chunk {
                column.push(&ScalarValue::Blob(Some(value.clone())))?;
            }
            RecordBatch::try_new(batch_schema.clone(), vec![column])
        });
        self.runs.push(SpillFile::write_batches_from_iter(
            schema,
            batches,
            self.batch_rows,
        )?);
        Ok(())
    }

    fn finalize(self, col_type: DataType) -> Result<ScalarValue> {
        let mut cursors = Vec::with_capacity(self.runs.len());
        let mut heap = BinaryHeap::new();
        for run in self.runs {
            let cursor = DistinctSpillCursor::new(run)?;
            let index = cursors.len();
            if let Some(value) = cursor.current() {
                heap.push(DistinctHeapItem {
                    value,
                    cursor: index,
                });
            }
            cursors.push(cursor);
        }
        let mut summary = DistinctSummary::default();
        let mut previous = None;
        while let Some(item) = heap.pop() {
            crate::common::query_cancel::check()?;
            if previous.as_ref() != Some(&item.value) {
                summary.add_value(decode_key_scalar(&item.value), self.is_count_star);
                previous = Some(item.value.clone());
            }
            let cursor = &mut cursors[item.cursor];
            cursor.advance()?;
            if let Some(value) = cursor.current() {
                heap.push(DistinctHeapItem {
                    value,
                    cursor: item.cursor,
                });
            }
        }
        Ok(summary.finalize(self.func, col_type))
    }
}

struct DistinctSpillCursor {
    _spill: SpillFile,
    reader: SegmentReader,
    row_group: usize,
    batch: Option<RecordBatch>,
    row: usize,
}

impl DistinctSpillCursor {
    fn new(spill: SpillFile) -> Result<Self> {
        let reader = spill.open_reader()?;
        let mut cursor = Self {
            _spill: spill,
            reader,
            row_group: 0,
            batch: None,
            row: 0,
        };
        cursor.load_next_batch()?;
        Ok(cursor)
    }

    fn load_next_batch(&mut self) -> Result<()> {
        while self.row_group < self.reader.num_row_groups() {
            let batch = self.reader.read_row_group(self.row_group)?;
            self.row_group += 1;
            if batch.num_rows() > 0 {
                self.batch = Some(batch);
                self.row = 0;
                return Ok(());
            }
        }
        self.batch = None;
        Ok(())
    }

    fn current(&self) -> Option<Vec<u8>> {
        match self.batch.as_ref()?.scalar(self.row, 0) {
            Some(ScalarValue::Blob(Some(value))) => Some(value),
            _ => None,
        }
    }

    fn advance(&mut self) -> Result<()> {
        self.row += 1;
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| self.row >= batch.num_rows())
        {
            self.load_next_batch()?;
        }
        Ok(())
    }
}

#[derive(Eq, PartialEq)]
struct DistinctHeapItem {
    value: Vec<u8>,
    cursor: usize,
}

impl Ord for DistinctHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .value
            .cmp(&self.value)
            .then_with(|| other.cursor.cmp(&self.cursor))
    }
}

impl PartialOrd for DistinctHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn arg_column_type(agg: &AggregateExpr, schema: &Schema) -> DataType {
    match &*agg.arg {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64),
        _ => DataType::Int64,
    }
}

pub fn format_aggregate_name(agg: &AggregateExpr, schema: &Schema) -> String {
    let arg_name = match &*agg.arg {
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.name.clone())
            .unwrap_or_default(),
        _ => "expr".to_string(),
    };
    let distinct_str = if agg.distinct { "DISTINCT " } else { "" };
    let func_name = match agg.func {
        AggFunc::Count => "count",
        AggFunc::Sum => "sum",
        AggFunc::Avg => "avg",
        AggFunc::Min => "min",
        AggFunc::Max => "max",
    };
    format!("{}({}{})", func_name, distinct_str, arg_name)
}

pub(crate) fn null_scalar_of_type(dt: DataType) -> ScalarValue {
    match dt {
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::UInt64 => ScalarValue::UInt64(None),
        DataType::UInt32 => ScalarValue::UInt32(None),
        DataType::UInt16 => ScalarValue::UInt16(None),
        DataType::UInt8 => ScalarValue::UInt8(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Float32 => ScalarValue::Float32(None),
        DataType::VarChar => ScalarValue::VarChar(None),
        DataType::Bool => ScalarValue::Bool(None),
        DataType::Date => ScalarValue::Date(None),
        DataType::Timestamp => ScalarValue::Timestamp(None),
        DataType::Decimal => ScalarValue::Decimal(None),
        DataType::Interval => ScalarValue::Interval(None),
        DataType::Blob => ScalarValue::Blob(None),
        DataType::Vector => ScalarValue::Vector(None),
        _ => ScalarValue::Int64(None),
    }
}

/// Output column definition for an aggregate expression. Decimal results
/// inherit the argument column's scale/precision so clients can render the
/// decimal point correctly.
pub fn aggregate_result_column_def(agg: &AggregateExpr, schema: &Schema) -> ColumnDef {
    let dt = aggregate_result_type(agg, schema);
    let mut def = ColumnDef::new(&format_aggregate_name(agg, schema), dt);
    if dt == DataType::Decimal {
        if let LogicalExpr::Column(idx) = &*agg.arg {
            if let Some(source) = schema.columns.get(*idx) {
                def.scale = source.scale;
                def.precision = source.precision;
            }
        }
    }
    def
}

pub fn aggregate_result_type(agg: &AggregateExpr, schema: &Schema) -> DataType {
    match agg.func {
        AggFunc::Count => DataType::Int64,
        AggFunc::Sum => match &*agg.arg {
            LogicalExpr::Column(idx) => schema
                .columns
                .get(*idx)
                .map(|c| match c.data_type {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64 => DataType::Int64,
                    DataType::Float32 | DataType::Float64 => DataType::Float64,
                    other => other,
                })
                .unwrap_or(DataType::Int64),
            _ => DataType::Int64,
        },
        AggFunc::Avg => DataType::Float64,
        AggFunc::Min | AggFunc::Max => match &*agg.arg {
            LogicalExpr::Column(idx) => schema
                .columns
                .get(*idx)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Int64),
            _ => DataType::Int64,
        },
    }
}

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Less;
    }
    if b.is_null() {
        return std::cmp::Ordering::Greater;
    }
    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

/// Apply DISTINCT to a RecordBatch, removing duplicate rows.
fn apply_distinct(batch: RecordBatch) -> RecordBatch {
    // Typed byte keys from column storage (no per-cell ScalarValue or String
    // keys) and a hash set instead of the former O(n^2) linear scan.
    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut key_buf = Vec::new();
    let mut mask = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        key_buf.clear();
        for column in &batch.columns {
            column.encode_key_at(row, &mut key_buf);
        }
        mask.push(seen.insert(key_buf.clone()));
    }

    let kept_rows: Vec<u32> = mask
        .iter()
        .enumerate()
        .filter_map(|(row, &keep)| keep.then_some(row as u32))
        .collect();
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&kept_rows))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
        .expect("apply_distinct: masked columns match the batch schema")
}

/// Apply HAVING filter to aggregated results.
fn apply_having(
    batch: RecordBatch,
    predicate: &LogicalExpr,
    logical_schema: &Schema,
) -> Result<RecordBatch> {
    // HAVING is evaluated using the same filter logic as WHERE
    // but operates on the aggregated result schema
    let remapped = remap_expr_for_batch(predicate, logical_schema, &batch.schema);
    let mask = crate::sql::execution::operators::filter::eval_filter(&remapped, &batch)?;
    Ok(super::apply_filter_mask(&batch, &mask))
}

#[cfg(test)]
#[path = "aggregate_test.rs"]
mod tests;
