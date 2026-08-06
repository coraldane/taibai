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

use super::ExecutionOp;

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
    /// Average tracks running sum + non-null count.
    Avg {
        sum: f64,
        count: u64,
        any: bool,
    },
    /// DISTINCT sets need to remember seen values. Only used for
    /// COUNT/SUM/AVG DISTINCT over a single column.
    Distinct {
        func: AggFunc,
        seen: std::collections::HashSet<String>,
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

fn hex_key(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

impl Accumulator {
    fn new_for(agg: &AggregateExpr, input_col_type: DataType) -> Result<Self> {
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
                    if let Some(col_arr) = batch.column(c) {
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
                                Some(prev) => prev.wrapping_add(part),
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
            Accumulator::Avg { sum, count, any } => {
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
            Accumulator::Distinct {
                func: _,
                seen,
                is_count_star,
            } => {
                // For DISTINCT we must inspect per-row values.
                match col_idx_opt {
                    Some(c) => {
                        if let Some(col_arr) = batch.column(c) {
                            for i in 0..col_arr.len() {
                                if col_arr.bitmap().is_null(i) {
                                    continue;
                                }
                                let s =
                                    serde_json::to_string(&col_arr.scalar(i)).map_err(|error| {
                                        TaibaiError::Internal(format!(
                                            "failed to encode DISTINCT value: {error}"
                                        ))
                                    })?;
                                seen.insert(s);
                            }
                        }
                    }
                    None => {
                        // COUNT(DISTINCT *) — count distinct full rows.
                        for row in 0..batch.num_rows() {
                            let values = (0..batch.num_columns())
                                .map(|col| {
                                    batch.scalar(row, col).unwrap_or(ScalarValue::Int64(None))
                                })
                                .collect::<Vec<_>>();
                            let s = serde_json::to_string(&values).map_err(|error| {
                                TaibaiError::Internal(format!(
                                    "failed to encode DISTINCT row: {error}"
                                ))
                            })?;
                            seen.insert(s);
                        }
                    }
                }
                let _ = is_count_star;
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
                    *value = Some(value.unwrap_or(0).wrapping_add(part));
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
            Accumulator::Avg { sum, count, any } => {
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
                    seen.insert(hex_key(&key));
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
            Accumulator::Avg { sum, count, any } => {
                if *any && *count > 0 {
                    ScalarValue::Float64(Some(*sum / *count as f64))
                } else {
                    ScalarValue::Float64(None)
                }
            }
            Accumulator::Distinct {
                func,
                seen,
                is_count_star,
            } => finalize_distinct_encoded(
                *func,
                seen.iter().map(String::as_str),
                col_type,
                *is_count_star,
            ),
        }
    }

    fn estimated_size_bytes(&self) -> usize {
        let dynamic = match self {
            Accumulator::Distinct { seen, .. } => seen
                .iter()
                .map(|value| value.capacity() + std::mem::size_of::<String>())
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
                    *left = Some(left.unwrap_or(0).wrapping_add(right));
                }
            }
            (Accumulator::SumF(left), Accumulator::SumF(right)) => {
                if let Some(right) = right {
                    *left = Some(left.unwrap_or(0.0) + right);
                }
            }
            (
                Accumulator::Avg { sum, count, any },
                Accumulator::Avg {
                    sum: rhs_sum,
                    count: rhs_count,
                    any: rhs_any,
                },
            ) => {
                *sum += rhs_sum;
                *count = count.saturating_add(rhs_count);
                *any |= rhs_any;
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
    ) -> Option<(AggFunc, bool, std::collections::HashSet<String>)> {
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
                let col_type = match &*agg.arg {
                    LogicalExpr::Column(idx) => self
                        .input_schema
                        .columns
                        .get(*idx)
                        .map(|c| c.data_type)
                        .unwrap_or(DataType::Int64),
                    _ => DataType::Int64,
                };
                accums.push(Accumulator::new_for(agg, col_type)?);
            }
            let spill_rows = crate::common::config::query_limits()
                .spill_batch_rows
                .max(1);
            let mut distinct_spills: Vec<Option<DistinctSpillAccumulator>> =
                std::iter::repeat_with(|| None).take(accums.len()).collect();

            while let Some(batch) = self.input.next_batch()? {
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
        let spill_rows = crate::common::config::query_limits()
            .spill_batch_rows
            .max(1);

        while let Some(batch) = self.input.next_batch()? {
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

            for row in 0..batch.num_rows() {
                let mut key = Vec::with_capacity(gb_indices.len() * 12);
                for &column in &gb_indices {
                    if let Some(array) = batch.column(column) {
                        array.encode_key_at(row, &mut key);
                    }
                }

                if !groups.contains_key(&key) {
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
                            let col_type = match &*agg.arg {
                                LogicalExpr::Column(idx) => self
                                    .input_schema
                                    .columns
                                    .get(*idx)
                                    .map(|c| c.data_type)
                                    .unwrap_or(DataType::Int64),
                                _ => DataType::Int64,
                            };
                            Accumulator::new_for(agg, col_type)
                        })
                        .collect();
                    estimated_group_bytes = estimated_group_bytes.saturating_add(
                        key.len()
                            + accums
                                .as_ref()
                                .map(|values| {
                                    values
                                        .iter()
                                        .map(Accumulator::estimated_size_bytes)
                                        .sum::<usize>()
                                })
                                .unwrap_or(0)
                            + key_scalars.len() * std::mem::size_of::<ScalarValue>(),
                    );
                    group_keys.push((key.clone(), key_scalars));
                    groups.insert(key.clone(), accums?);
                }

                let accums = groups.get_mut(&key).unwrap();
                let before_update = accums
                    .iter()
                    .map(Accumulator::estimated_size_bytes)
                    .sum::<usize>();
                for (acc, agg) in accums.iter_mut().zip(aggregates.iter()) {
                    let col_idx = match &*agg.arg {
                        LogicalExpr::Column(idx) => Some(*idx),
                        _ => None,
                    };
                    acc.update_row(&batch, col_idx, row)?;
                }
                let after_update = accums
                    .iter()
                    .map(Accumulator::estimated_size_bytes)
                    .sum::<usize>();
                estimated_group_bytes = estimated_group_bytes
                    .saturating_sub(before_update)
                    .saturating_add(after_update);
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
        }

        let physical_schema = physical_schema.unwrap_or_else(|| self.input_schema.clone());
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
        let result = self.finalize_grouped(&group_keys, &groups, &aggregates, &physical_schema)?;
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
            let name = format_aggregate_name(agg, physical_schema);
            let dt = aggregate_result_type(agg, physical_schema);
            col_defs.push(ColumnDef::new(&name, dt));
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
            result_batch = apply_having(result_batch, having_pred)?;
        }
        Ok(result_batch)
    }

    fn finalize_grouped(
        &self,
        group_keys: &[(Vec<u8>, Vec<ScalarValue>)],
        groups: &HashMap<Vec<u8>, Vec<Accumulator>>,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
    ) -> Result<RecordBatch> {
        let mut col_defs = Vec::new();
        let ngb = self.group_by.len();
        // group-by output columns keep their original physical types
        for (i, _) in self.group_by.iter().enumerate() {
            // Recover the type from the first representative scalar instead of
            // relying on physical_schema (the remapped logical indices already
            // match physical_schema here, so both are fine).
            let dt = self
                .group_by
                .get(i)
                .and_then(|e| match e {
                    LogicalExpr::Column(idx) => physical_schema.columns.get(*idx),
                    _ => None,
                })
                .map(|c| c.data_type)
                .unwrap_or(DataType::VarChar);
            let name = self
                .group_by
                .get(i)
                .and_then(|e| match e {
                    LogicalExpr::Column(idx) => physical_schema.columns.get(*idx),
                    _ => None,
                })
                .map(|c| c.name.clone())
                .unwrap_or_else(|| format!("col_{}", i));
            col_defs.push(ColumnDef::new(&name, dt));
        }
        for agg in aggregates {
            let name = format_aggregate_name(agg, physical_schema);
            let dt = aggregate_result_type(agg, physical_schema);
            col_defs.push(ColumnDef::new(&name, dt));
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
            result_batch = apply_having(result_batch, having_pred)?;
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
        columns.push(ColumnDef::new(
            &format_aggregate_name(aggregate, physical_schema),
            aggregate_result_type(aggregate, physical_schema),
        ));
    }
    Schema::new(columns)
}

fn aggregate_spill_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("group_key", DataType::VarChar),
        ColumnDef::new("aggregate_state", DataType::VarChar),
    ])
}

fn spill_group_run(
    groups: &HashMap<Vec<u8>, Vec<Accumulator>>,
    group_keys: &[(Vec<u8>, Vec<ScalarValue>)],
    spill_rows: usize,
) -> Result<SpillFile> {
    let mut entries = Vec::with_capacity(groups.len());
    for (key, scalars) in group_keys {
        if let Some(accumulators) = groups.get(key) {
            let encoded_key = serde_json::to_string(scalars).map_err(|error| {
                TaibaiError::Internal(format!("failed to encode aggregate spill key: {error}"))
            })?;
            entries.push((encoded_key, accumulators));
        }
    }
    entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let schema = aggregate_spill_schema();
    let batch_schema = schema.clone();
    let batches = entries.chunks(spill_rows.max(1)).map(move |chunk| {
        let mut key_column = ColumnArray::empty(DataType::VarChar);
        let mut state_column = ColumnArray::empty(DataType::VarChar);
        for (key, accumulators) in chunk {
            let state = serde_json::to_string(accumulators).map_err(|error| {
                TaibaiError::Internal(format!("failed to encode aggregate spill state: {error}"))
            })?;
            key_column.push(&ScalarValue::VarChar(Some(key.clone())))?;
            state_column.push(&ScalarValue::VarChar(Some(state)))?;
        }
        RecordBatch::try_new(batch_schema.clone(), vec![key_column, state_column])
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

    fn current(&self) -> Result<(String, String)> {
        let batch = self
            .batch
            .as_ref()
            .ok_or_else(|| TaibaiError::Internal("aggregate spill cursor exhausted".into()))?;
        let key = match batch.scalar(self.row, 0) {
            Some(ScalarValue::VarChar(Some(value))) => value,
            _ => {
                return Err(TaibaiError::Internal("invalid aggregate spill key".into()));
            }
        };
        let state = match batch.scalar(self.row, 1) {
            Some(ScalarValue::VarChar(Some(value))) => value,
            _ => {
                return Err(TaibaiError::Internal(
                    "invalid aggregate spill state".into(),
                ));
            }
        };
        Ok((key, state))
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
    key: String,
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
            having,
            max_groups,
            emitted_groups: 0,
            batch_rows: batch_rows.max(1),
        })
    }

    fn pop_state(&mut self) -> Result<(String, Vec<Accumulator>)> {
        let item = self
            .heap
            .pop()
            .ok_or_else(|| TaibaiError::Internal("aggregate spill heap exhausted".into()))?;
        let cursor = &mut self.cursors[item.cursor];
        let (key, encoded_state) = cursor.current()?;
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
        Ok((key, state))
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.heap.is_empty() {
                return Ok(None);
            }
            let mut output =
                RecordBatch::with_capacity(self.output_schema.clone(), self.batch_rows);
            while output.num_rows() < self.batch_rows && !self.heap.is_empty() {
                let (key, mut accumulators) = self.pop_state()?;
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
                    let (_, mut states) = self.pop_state()?;
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
                let key_scalars: Vec<ScalarValue> =
                    serde_json::from_str(&key).map_err(|error| {
                        TaibaiError::Internal(format!(
                            "failed to decode aggregate spill key: {error}"
                        ))
                    })?;
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
                output = apply_having(output, having)?;
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

fn finalize_distinct_encoded<'a>(
    func: AggFunc,
    values: impl Iterator<Item = &'a str>,
    col_type: DataType,
    is_count_star: bool,
) -> ScalarValue {
    let mut summary = DistinctSummary::default();
    for value in values {
        summary.add_encoded(value, is_count_star);
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
    fn add_encoded(&mut self, encoded: &str, is_count_star: bool) {
        self.count = self.count.saturating_add(1);
        if is_count_star {
            return;
        }
        let Ok(value) = serde_json::from_str::<ScalarValue>(encoded) else {
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
            AggFunc::Sum if col_type == DataType::Decimal => {
                ScalarValue::Decimal(Some(self.integer_sum as i64))
            }
            AggFunc::Sum => ScalarValue::Int64(Some(self.integer_sum as i64)),
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

    fn spill_values(&mut self, values: std::collections::HashSet<String>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let mut values = values.into_iter().collect::<Vec<_>>();
        values.sort_unstable();
        let schema = Schema::new(vec![ColumnDef::new("distinct_value", DataType::VarChar)]);
        let batch_schema = schema.clone();
        let batches = values.chunks(self.batch_rows).map(move |chunk| {
            let mut column = ColumnArray::empty(DataType::VarChar);
            for value in chunk {
                column.push(&ScalarValue::VarChar(Some(value.clone())))?;
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
            if previous.as_ref() != Some(&item.value) {
                summary.add_encoded(&item.value, self.is_count_star);
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

    fn current(&self) -> Option<String> {
        match self.batch.as_ref()?.scalar(self.row, 0) {
            Some(ScalarValue::VarChar(Some(value))) => Some(value),
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
    value: String,
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

/// Build a single-row RecordBatch containing only `row` from `batch`.
fn slice_batch_single_row(batch: &RecordBatch, row: usize) -> Option<RecordBatch> {
    let ncols = batch.num_columns();
    let mut cols = Vec::with_capacity(ncols);
    for c in 0..ncols {
        let col = batch.column(c)?;
        let sv = col.scalar(row);
        let mut new_col = ColumnArray::empty(col.data_type());
        new_col.push(&sv).ok()?;
        cols.push(new_col);
    }
    Some(RecordBatch::try_new(batch.schema.clone(), cols).ok()?)
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

fn null_scalar_of_type(dt: DataType) -> ScalarValue {
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

pub fn compute_aggregate(
    agg: &AggregateExpr,
    rows: &[Vec<ScalarValue>],
    schema: &Schema,
) -> ScalarValue {
    let col_idx = match &*agg.arg {
        LogicalExpr::Column(idx) => Some(*idx),
        LogicalExpr::Wildcard => None,
        _ => None,
    };

    // Handle DISTINCT
    let values: Vec<&ScalarValue> = if agg.distinct {
        if let Some(idx) = col_idx {
            let mut seen: Vec<String> = Vec::new();
            rows.iter()
                .filter_map(|r| r.get(idx))
                .filter(|v| {
                    let s = v.to_string();
                    if seen.contains(&s) {
                        false
                    } else {
                        seen.push(s);
                        true
                    }
                })
                .collect()
        } else {
            rows.iter()
                .map(|r| r.first().unwrap_or(&ScalarValue::Int64(None)))
                .collect()
        }
    } else {
        if let Some(idx) = col_idx {
            rows.iter().filter_map(|r| r.get(idx)).collect()
        } else {
            rows.iter()
                .map(|r| r.first().unwrap_or(&ScalarValue::Int64(None)))
                .collect()
        }
    };

    match agg.func {
        AggFunc::Count => {
            if agg.distinct {
                ScalarValue::Int64(Some(values.len() as i64))
            } else if col_idx.is_some() {
                let non_null = values.iter().filter(|v| !v.is_null()).count();
                ScalarValue::Int64(Some(non_null as i64))
            } else {
                ScalarValue::Int64(Some(rows.len() as i64))
            }
        }
        AggFunc::Sum => {
            let input_type = col_idx
                .and_then(|idx| schema.columns.get(idx))
                .map(|c| c.data_type)
                .unwrap_or(DataType::Int64);

            let has_non_null = values.iter().any(|v| !v.is_null());
            if !has_non_null {
                match input_type {
                    DataType::Float32 | DataType::Float64 => ScalarValue::Float64(None),
                    DataType::Decimal => ScalarValue::Decimal(None),
                    _ => ScalarValue::Int64(None),
                }
            } else {
                match input_type {
                    DataType::Float32 | DataType::Float64 => {
                        let sum: f64 = values.iter().filter_map(|v| scalar_to_f64(v)).sum();
                        ScalarValue::Float64(Some(sum))
                    }
                    DataType::Decimal => {
                        let sum: i64 = values
                            .iter()
                            .filter_map(|v| match v {
                                ScalarValue::Decimal(Some(n)) => Some(*n),
                                _ => None,
                            })
                            .sum();
                        ScalarValue::Decimal(Some(sum))
                    }
                    _ => {
                        let sum: i64 = values
                            .iter()
                            .filter_map(|v| match v {
                                ScalarValue::Int8(Some(n)) => Some(*n as i64),
                                ScalarValue::Int16(Some(n)) => Some(*n as i64),
                                ScalarValue::Int32(Some(n)) => Some(*n as i64),
                                ScalarValue::Int64(Some(n)) => Some(*n),
                                ScalarValue::UInt8(Some(n)) => Some(*n as i64),
                                ScalarValue::UInt16(Some(n)) => Some(*n as i64),
                                ScalarValue::UInt32(Some(n)) => Some(*n as i64),
                                ScalarValue::UInt64(Some(n)) => i64::try_from(*n).ok(),
                                _ => None,
                            })
                            .sum();
                        ScalarValue::Int64(Some(sum))
                    }
                }
            }
        }
        AggFunc::Avg => {
            let vals: Vec<f64> = values.iter().filter_map(|v| scalar_to_f64(v)).collect();
            if vals.is_empty() {
                ScalarValue::Float64(None)
            } else {
                ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64))
            }
        }
        AggFunc::Min => {
            match values
                .iter()
                .filter(|v| !v.is_null())
                .min_by(|a, b| compare_scalars(a, b))
            {
                Some(v) => (*v).clone(),
                None => {
                    if let LogicalExpr::Column(idx) = &*agg.arg {
                        if let Some(col) = schema.columns.get(*idx) {
                            null_scalar_of_type(col.data_type)
                        } else {
                            ScalarValue::Int64(None)
                        }
                    } else {
                        ScalarValue::Int64(None)
                    }
                }
            }
        }
        AggFunc::Max => {
            match values
                .iter()
                .filter(|v| !v.is_null())
                .max_by(|a, b| compare_scalars(a, b))
            {
                Some(v) => (*v).clone(),
                None => {
                    if let LogicalExpr::Column(idx) = &*agg.arg {
                        if let Some(col) = schema.columns.get(*idx) {
                            null_scalar_of_type(col.data_type)
                        } else {
                            ScalarValue::Int64(None)
                        }
                    } else {
                        ScalarValue::Int64(None)
                    }
                }
            }
        }
    }
}

pub fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    v.numeric_f64()
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
    let mut seen: Vec<String> = Vec::new();
    let mut mask = Vec::new();

    for row in 0..batch.num_rows() {
        let key: String = (0..batch.num_columns())
            .map(|c| {
                batch
                    .scalar(row, c)
                    .unwrap_or(ScalarValue::Int64(None))
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("|");
        if !seen.contains(&key) {
            seen.push(key);
            mask.push(true);
        } else {
            mask.push(false);
        }
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&col.scalar(i)).unwrap();
            }
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

/// Apply HAVING filter to aggregated results.
fn apply_having(batch: RecordBatch, _predicate: &LogicalExpr) -> Result<RecordBatch> {
    // HAVING is evaluated using the same filter logic as WHERE
    // but operates on the aggregated result schema
    let mask = crate::sql::execution::operators::filter::eval_filter(_predicate, &batch)?;
    Ok(super::apply_filter_mask(&batch, &mask))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct OneBatchExec {
        batch: Option<RecordBatch>,
    }

    struct ManyBatchExec {
        batches: std::vec::IntoIter<RecordBatch>,
    }

    impl ExecutionOp for OneBatchExec {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            Ok(self.batch.take())
        }
    }

    impl ExecutionOp for ManyBatchExec {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            Ok(self.batches.next())
        }
    }

    #[test]
    fn grouped_aggregate_spills_and_merges_partial_states() {
        let schema = Schema::new(vec![
            ColumnDef::new("group_id", DataType::Int64),
            ColumnDef::new("value", DataType::Int64),
        ]);
        let mut groups = ColumnArray::empty(DataType::Int64);
        let mut values = ColumnArray::empty(DataType::Int64);
        for (group, value) in [(0, 10), (1, 20), (0, 30), (2, 40), (1, 20), (0, 10)] {
            groups.push(&ScalarValue::Int64(Some(group))).unwrap();
            values.push(&ScalarValue::Int64(Some(value))).unwrap();
        }
        let batch = RecordBatch::try_new(schema.clone(), vec![groups, values]).unwrap();
        let aggregates = vec![
            AggregateExpr {
                func: AggFunc::Count,
                arg: Box::new(LogicalExpr::Wildcard),
                distinct: false,
            },
            AggregateExpr {
                func: AggFunc::Sum,
                arg: Box::new(LogicalExpr::Column(1)),
                distinct: false,
            },
            AggregateExpr {
                func: AggFunc::Count,
                arg: Box::new(LogicalExpr::Column(1)),
                distinct: true,
            },
        ];
        let mut exec = HashAggregateExec::new(
            vec![LogicalExpr::Column(0)],
            aggregates,
            Box::new(OneBatchExec { batch: Some(batch) }),
            schema,
        )
        .with_memory_limit_bytes(1);

        let mut results = HashMap::new();
        while let Some(batch) = exec.next_batch().unwrap() {
            for row in 0..batch.num_rows() {
                results.insert(
                    batch.scalar(row, 0).unwrap(),
                    (
                        batch.scalar(row, 1).unwrap(),
                        batch.scalar(row, 2).unwrap(),
                        batch.scalar(row, 3).unwrap(),
                    ),
                );
            }
        }

        assert_eq!(
            results.get(&ScalarValue::Int64(Some(0))),
            Some(&(
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(50)),
                ScalarValue::Int64(Some(2)),
            ))
        );
        assert_eq!(
            results.get(&ScalarValue::Int64(Some(1))),
            Some(&(
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(40)),
                ScalarValue::Int64(Some(1)),
            ))
        );
        assert_eq!(
            results.get(&ScalarValue::Int64(Some(2))),
            Some(&(
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(40)),
                ScalarValue::Int64(Some(1)),
            ))
        );
    }

    #[test]
    fn global_distinct_aggregates_spill_and_deduplicate_across_runs() {
        let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
        let batches = [[1, 2, 2], [2, 3, 1]]
            .into_iter()
            .map(|values| {
                let mut column = ColumnArray::empty(DataType::Int64);
                for value in values {
                    column.push(&ScalarValue::Int64(Some(value))).unwrap();
                }
                RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
            })
            .collect::<Vec<_>>();
        let aggregates = [
            AggFunc::Count,
            AggFunc::Sum,
            AggFunc::Avg,
            AggFunc::Min,
            AggFunc::Max,
        ]
        .into_iter()
        .map(|func| AggregateExpr {
            func,
            arg: Box::new(LogicalExpr::Column(0)),
            distinct: true,
        })
        .collect();
        let mut exec = HashAggregateExec::new(
            Vec::new(),
            aggregates,
            Box::new(ManyBatchExec {
                batches: batches.into_iter(),
            }),
            schema,
        )
        .with_memory_limit_bytes(1);

        let output = exec.next_batch().unwrap().unwrap();
        assert_eq!(output.scalar(0, 0), Some(ScalarValue::Int64(Some(3))));
        assert_eq!(output.scalar(0, 1), Some(ScalarValue::Int64(Some(6))));
        assert_eq!(output.scalar(0, 2), Some(ScalarValue::Float64(Some(2.0))));
        assert_eq!(output.scalar(0, 3), Some(ScalarValue::Int64(Some(1))));
        assert_eq!(output.scalar(0, 4), Some(ScalarValue::Int64(Some(3))));
    }
}
