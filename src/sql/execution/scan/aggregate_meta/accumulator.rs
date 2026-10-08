//! Aggregate state and checked accumulation.

use super::*;

#[derive(Clone)]
pub(super) struct AggAccum {
    pub(super) func: AggFunc,
    pub(super) column: Option<usize>,
    pub(super) count: u64,
    pub(super) best_min: Option<ScalarValue>,
    pub(super) best_max: Option<ScalarValue>,
    pub(super) col_type: DataType,
    pub(super) sum_i: Option<i64>,
    pub(super) sum_f: Option<f64>,
    /// SUM over a column type the metadata fast path cannot answer
    /// (non-numeric argument columns).
    pub(super) sum_unsupported: bool,
    /// AVG accumulators: per-batch sums accumulate as f64 (mirroring
    /// `Accumulator::Avg`), fed either from decoded batches or from exact
    /// per-row-group i64 run folds.
    pub(super) avg_sum_f: Option<f64>,
    pub(super) avg_count: u64,
    pub(super) avg_unsupported: bool,
    /// DECIMAL scale of the argument column: sums accumulate in the raw
    /// scaled i64 domain, and finalize divides it back out.
    pub(super) col_scale: i32,
}

impl AggAccum {
    pub(super) fn new(func: AggFunc, column: Option<usize>, schema: &Schema) -> Self {
        let col_type = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.data_type)
            .unwrap_or(DataType::Int64);
        let col_scale = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.scale)
            .unwrap_or(0);
        let (sum_i, sum_f, sum_unsupported) = if func == AggFunc::Sum {
            match col_type {
                DataType::Float32 | DataType::Float64 => (None, None, false),
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 => {
                    (None, None, false)
                }
                _ => (None, None, true),
            }
        } else {
            (None, None, false)
        };
        let avg_unsupported = func == AggFunc::Avg
            && !matches!(
                col_type,
                DataType::Int64
                    | DataType::Int32
                    | DataType::Int16
                    | DataType::Int8
                    | DataType::Float64
                    | DataType::Float32
                    | DataType::Decimal
            );
        Self {
            func,
            column,
            count: 0,
            best_min: None,
            best_max: None,
            col_type,
            sum_i,
            sum_f,
            sum_unsupported,
            avg_sum_f: None,
            avg_count: 0,
            avg_unsupported,
            col_scale,
        }
    }

    pub(super) fn fold_metadata(
        &mut self,
        default: Option<&ScalarValue>,
        zone_maps: &[crate::storage::partition::pruning::ZoneMap],
        row_count: u64,
        col_sums: &[i64],
        column_map: &std::collections::HashMap<usize, usize>,
    ) -> Result<()> {
        let Some(column) = self.column else {
            if self.func == AggFunc::Count {
                self.count += row_count;
            }
            return Ok(());
        };
        // A column this segment's footer predates carries one constant
        // DEFAULT on every row: fold it arithmetically instead of indexing
        // the (absent) zone map / col_sum slot.
        if let Some(default) = default {
            return self.fold_constant_default(default, row_count);
        }
        // zone_maps / col_sums are indexed by the segment's footer column
        // order: resolve the catalog argument column onto it by name.
        let footer_column = row_filter::mapped_column(column_map, column);
        let Some(zone_map) = zone_maps.get(footer_column) else {
            return Ok(());
        };
        // An all-null row group contributes nothing to SUM/AVG — its footer
        // sum is 0, which must not materialize a zero from an empty
        // accumulator. `min_value` is only ever set from a non-null value, so
        // it doubles as a path-independent "has values" signal (zone map
        // `row_count` semantics differ between the column and bulk writers).
        let has_values = !zone_map.min_value.is_null();
        match self.func {
            AggFunc::Count => self.count += row_count.saturating_sub(zone_map.null_count),
            AggFunc::Min if !zone_map.min_value.is_null() => {
                self.merge_min(zone_map.min_value.clone())
            }
            AggFunc::Max if !zone_map.max_value.is_null() => {
                self.merge_max(zone_map.max_value.clone())
            }
            AggFunc::Sum if has_values => {
                if let Some(&sum) = col_sums.get(footer_column) {
                    self.add_i64(sum)?;
                }
            }
            AggFunc::Avg if has_values => {
                if let Some(&sum) = col_sums.get(footer_column) {
                    self.add_avg(sum as f64, row_count.saturating_sub(zone_map.null_count));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Fold `row_count` rows that all carry one constant DEFAULT (a column
    /// this segment's footer predates, e.g. SUM(new_col) over pre-ALTER
    /// rows). Semantics mirror the zone-map fold exactly:
    ///
    /// * callers only reach here for delete-free row groups, so `row_count`
    ///   is the live row count (deleted rows never contribute);
    /// * a NULL default is an all-NULL column: it contributes nothing —
    ///   COUNT(col) adds no row, SUM/MIN/MAX stay unmaterialized (the same
    ///   "footer sum 0 must not materialize a zero" rule as `has_values`);
    /// * SUM/AVG accumulate default × rows through the shared overflow
    ///   checked accumulation point (`add_i64`), reporting "SUM overflow"
    ///   instead of wrapping.
    pub(super) fn fold_constant_default(
        &mut self,
        default: &ScalarValue,
        row_count: u64,
    ) -> Result<()> {
        if row_count == 0 || default.is_null() {
            return Ok(());
        }
        match self.func {
            AggFunc::Count => self.count += row_count,
            AggFunc::Min => self.merge_min(default.clone()),
            AggFunc::Max => self.merge_max(default.clone()),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(value) = default.numeric_f64() {
                        self.add_f64(value * row_count as f64);
                    }
                }
                _ => {
                    let value = default.integer_i128().ok_or_else(|| {
                        TaibaiError::Internal(format!(
                            "constant default {:?} not foldable as {} SUM",
                            default, self.col_type
                        ))
                    })?;
                    let product = value
                        .checked_mul(i128::from(row_count))
                        .and_then(|product| i64::try_from(product).ok())
                        .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?;
                    self.add_i64(product)?;
                }
            },
            AggFunc::Avg => {
                if let Some(value) = default.numeric_f64() {
                    self.add_avg(value * row_count as f64, row_count);
                }
            }
        }
        Ok(())
    }

    fn add_f64(&mut self, part: f64) {
        self.sum_f = Some(self.sum_f.unwrap_or(0.0) + part);
    }

    pub(super) fn add_avg(&mut self, part: f64, count: u64) {
        self.avg_sum_f = Some(self.avg_sum_f.unwrap_or(0.0) + part);
        self.avg_count += count;
    }

    /// The single integer-SUM accumulation point for this accumulator: every
    /// path (footer col_sums, data folds, cached partials) adds through it so
    /// overflow reports "SUM overflow" instead of silently wrapping — the
    /// same contract as the generic aggregate operator. The row-group
    /// partials fed in here can themselves have wrapped inside the
    /// documented vectorized `col_sum_i64` fold; this boundary catches
    /// cross-group / cross-segment accumulation overflow.
    pub(super) fn add_i64(&mut self, part: i64) -> Result<()> {
        self.sum_i = Some(
            self.sum_i
                .unwrap_or(0)
                .checked_add(part)
                .ok_or_else(|| TaibaiError::Sql("SUM overflow".to_string()))?,
        );
        Ok(())
    }

    pub(super) fn fold_batch(&mut self, batch: &RecordBatch, column: Option<usize>) -> Result<()> {
        let Some(column) = column else {
            if self.func == AggFunc::Count && self.column.is_none() {
                self.count += batch.num_rows() as u64;
            }
            return Ok(());
        };
        let Some(values) = batch.column(column) else {
            return Ok(());
        };
        match self.func {
            AggFunc::Count => self.count += values.col_count_non_null(),
            AggFunc::Min => {
                if let Some(value) = values.col_min() {
                    self.merge_min(value);
                }
            }
            AggFunc::Max => {
                if let Some(value) = values.col_max() {
                    self.merge_max(value);
                }
            }
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(part) = values.col_sum_f64() {
                        self.add_f64(part);
                    }
                }
                _ => {
                    if let Some(part) = values.col_sum_i64() {
                        self.add_i64(part)?;
                    }
                }
            },
            AggFunc::Avg => {
                let n = values.col_count_non_null();
                if n > 0 {
                    let part = values
                        .col_sum_f64()
                        .or_else(|| values.col_sum_i64().map(|x| x as f64))
                        .unwrap_or(0.0);
                    self.add_avg(part, n);
                }
            }
        }
        Ok(())
    }

    /// Fold one row's argument value (per-row grouped data folds). `None` is
    /// the COUNT(*) shape — every row counts regardless of any value.
    pub(super) fn fold_row(&mut self, value: Option<&ScalarValue>) -> Result<()> {
        let count_star = self.func == AggFunc::Count && self.column.is_none();
        let Some(value) = value else {
            if count_star {
                self.count += 1;
            }
            return Ok(());
        };
        if value.is_null() {
            if count_star {
                self.count += 1;
            }
            return Ok(());
        }
        match self.func {
            AggFunc::Count => self.count += 1,
            AggFunc::Min => self.merge_min(value.clone()),
            AggFunc::Max => self.merge_max(value.clone()),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    if let Some(part) = value.numeric_f64() {
                        self.add_f64(part);
                    }
                }
                _ => {
                    if let Some(part) = value.integer_i128() {
                        let part = i64::try_from(part)
                            .map_err(|_| TaibaiError::Sql("SUM overflow".to_string()))?;
                        self.add_i64(part)?;
                    }
                }
            },
            AggFunc::Avg => {
                if let Some(part) = value.numeric_f64() {
                    self.add_avg(part, 1);
                }
            }
        }
        Ok(())
    }

    /// Fold a cached per-segment partial sum into this accumulator. A `None`
    /// delta means the segment contributed nothing (e.g. all-NULL rows) and
    /// must not materialize a zero from an empty accumulator.
    pub(super) fn fold_cached_sum(&mut self, cached: &SumCacheValue) -> Result<()> {
        match self.col_type {
            DataType::Float32 | DataType::Float64 => {
                if let Some(v) = cached.f {
                    self.add_f64(v);
                }
            }
            _ => {
                if let Some(v) = cached.i {
                    self.add_i64(v)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn merge_min(&mut self, value: ScalarValue) {
        self.best_min = Some(match self.best_min.take() {
            Some(current)
                if crate::sql::execution::operators::aggregate::compare_scalars(
                    &value, &current,
                ) != std::cmp::Ordering::Less =>
            {
                current
            }
            _ => value,
        });
    }

    pub(super) fn merge_max(&mut self, value: ScalarValue) {
        self.best_max = Some(match self.best_max.take() {
            Some(current)
                if crate::sql::execution::operators::aggregate::compare_scalars(
                    &value, &current,
                ) != std::cmp::Ordering::Greater =>
            {
                current
            }
            _ => value,
        });
    }

    pub(super) fn finalize(self) -> ScalarValue {
        match self.func {
            AggFunc::Count => ScalarValue::Int64(Some(self.count as i64)),
            AggFunc::Min => self
                .best_min
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Max => self
                .best_max
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => ScalarValue::Float64(self.sum_f),
                _ => ScalarValue::Int64(self.sum_i),
            },
            AggFunc::Avg => {
                if self.avg_count > 0 {
                    // Decimal sums accumulate raw scaled i64; divide the
                    // scale back out (AVG used to come out 10^scale too
                    // large for DECIMAL columns).
                    let scale_divisor = if self.col_type == DataType::Decimal {
                        10f64.powi(self.col_scale)
                    } else {
                        1.0
                    };
                    ScalarValue::Float64(Some(
                        self.avg_sum_f.unwrap_or(0.0) / self.avg_count as f64 / scale_divisor,
                    ))
                } else {
                    ScalarValue::Float64(None)
                }
            }
        }
    }
}
