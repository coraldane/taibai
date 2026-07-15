use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::filter::{remap_aggregate_for_batch, remap_expr_for_batch};
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
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
    done: bool,
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
            done: false,
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
}

/// Streaming accumulator for a single aggregate function over a single group.
/// Memory usage is O(1) per accumulator (except DISTINCT which needs a value set).
enum Accumulator {
    /// `COUNT(*)` counts all rows; `COUNT(col)` counts non-null of `col`.
    Count { count: u64, col: CountKind },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    SumI(Option<i64>),
    SumF(Option<f64>),
    /// Average tracks running sum + non-null count.
    Avg { sum: f64, count: u64, any: bool },
    /// DISTINCT sets need to remember seen values. Only used for
    /// COUNT/SUM/AVG DISTINCT over a single column.
    Distinct {
        func: AggFunc,
        seen: std::collections::HashSet<String>,
        is_count_star: bool,
    },
}

#[derive(Clone, Copy)]
enum CountKind {
    /// COUNT(*) — every row counts.
    Star,
    /// COUNT(col) — only non-null values of this column count.
    Column(usize),
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
                                let s = col_arr.scalar(i).to_string();
                                seen.insert(s);
                            }
                        }
                    }
                    None => {
                        // COUNT(DISTINCT *) — count distinct full rows.
                        for row in 0..batch.num_rows() {
                            let s = (0..batch.num_columns())
                                .map(|col| {
                                    batch
                                        .scalar(row, col)
                                        .map(|v| v.to_string())
                                        .unwrap_or_default()
                                })
                                .collect::<Vec<_>>()
                                .join("|");
                            seen.insert(s);
                        }
                    }
                }
                let _ = is_count_star;
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
            Accumulator::Distinct { func, seen, .. } => match func {
                AggFunc::Count => ScalarValue::Int64(Some(seen.len() as i64)),
                AggFunc::Sum => {
                    let parsed: Vec<f64> = seen
                        .iter()
                        .filter_map(|s| s.parse::<f64>().ok())
                        .collect();
                    if parsed.is_empty() {
                        ScalarValue::Int64(None)
                    } else {
                        ScalarValue::Int64(Some(parsed.iter().map(|x| *x as i64).sum()))
                    }
                }
                AggFunc::Avg => {
                    let parsed: Vec<f64> = seen
                        .iter()
                        .filter_map(|s| s.parse::<f64>().ok())
                        .collect();
                    if parsed.is_empty() {
                        ScalarValue::Float64(None)
                    } else {
                        ScalarValue::Float64(Some(parsed.iter().sum::<f64>() / parsed.len() as f64))
                    }
                }
                _ => ScalarValue::Int64(Some(seen.len() as i64)),
            },
        }
    }
}

impl ExecutionOp for HashAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
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
            }

            let physical_schema = physical_schema.unwrap_or_else(|| self.input_schema.clone());
            let result = self.finalize_no_group_by(&accums, &aggregates, &physical_schema)?;
            return Ok(Some(result));
        }

        // ----- GROUP BY: per-group accumulators in a hash map ---------------
        // `groups` maps the stringified key to its accumulators; `group_keys`
        // preserves insertion order alongside the original (typed) group-by
        // scalar values so the output keeps the correct column types.
        let mut groups: HashMap<Vec<String>, Vec<Accumulator>> = HashMap::new();
        let mut group_keys: Vec<(Vec<String>, Vec<ScalarValue>)> = Vec::new();

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
                let key_scalars: Vec<ScalarValue> = gb_indices
                    .iter()
                    .map(|&c| {
                        batch
                            .scalar(row, c)
                            .unwrap_or_else(|| ScalarValue::Int64(None))
                    })
                    .collect();
                let key: Vec<String> =
                    key_scalars.iter().map(|v| v.to_string()).collect();

                if !groups.contains_key(&key) {
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
                    group_keys.push((key.clone(), key_scalars));
                    groups.insert(key.clone(), accums?);
                }

                let accums = groups.get_mut(&key).unwrap();
                for (acc, agg) in accums.iter_mut().zip(aggregates.iter()) {
                    let col_idx = match &*agg.arg {
                        LogicalExpr::Column(idx) => Some(*idx),
                        _ => None,
                    };
                    // Build a single-row view so each accumulator sees only this
                    // group's current row. Memory is O(1) per update.
                    if let Some(single) = slice_batch_single_row(&batch, row) {
                        acc.update(&single, col_idx)?;
                    }
                }
            }
        }

        let physical_schema = physical_schema.unwrap_or_else(|| self.input_schema.clone());
        let result = self.finalize_grouped(
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
            let val = acc.finalize(col_type);
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
        group_keys: &[(Vec<String>, Vec<ScalarValue>)],
        groups: &HashMap<Vec<String>, Vec<Accumulator>>,
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
                    _ => ScalarValue::Int64(None),
                }
            } else {
                match input_type {
                    DataType::Float32 | DataType::Float64 => {
                        let sum: f64 = values.iter().filter_map(|v| scalar_to_f64(v)).sum();
                        ScalarValue::Float64(Some(sum))
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
