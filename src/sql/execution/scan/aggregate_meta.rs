//! Metadata-driven fast path for "simple" aggregate queries.
//!
//! A simple aggregate query is one with no GROUP BY, no HAVING, no DISTINCT,
//! and whose select list consists entirely of `COUNT(*)`, `COUNT(col)`,
//! `MIN(col)` or `MAX(col)`. For such queries the answer can frequently be
//! derived from row-group zone maps / footers without reading column data:
//!
//! * `COUNT(*)`              -> Σ(row_count) − Σ(deleted_row_count)
//! * `COUNT(col)` (no del)   -> Σ(row_count) − Σ(zone_map[col].null_count)
//! * `MIN(col)` / `MAX(col)` -> min/max over candidate row groups'
//!   `zone_map[col].min_value` / `max_value`
//!
//! When the candidate segments contain deletes we fall back to reading the
//! argument column for MIN/MAX (still only the relevant row groups) so the
//! result remains correct.
//!
//! All of this sits on top of the existing partition / row-group pruning
//! machinery, so a `WHERE` clause still prunes segments and row groups exactly
//! as a normal scan would.

use std::path::Path;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::aggregate_result_column_def;
use crate::sql::execution::scan::memtable as scan_memtable;
use crate::sql::execution::scan::partition;
use crate::sql::execution::scan::row_filter;
use crate::sql::execution::scan::row_group;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::segment::delete_bitmap;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

/// Compute the result of a simple metadata-only aggregate query.
///
/// `aggregates` are the (already logical-schema-indexed) aggregate expressions
/// from the SELECT list. Returns a single-row result batch.
#[allow(clippy::too_many_arguments)]
pub fn compute_simple_aggregate(
    spec: &ScanSpec,
    aggregates: &[AggregateExpr],
    table_meta: &TableMeta,
    table_schema: &Schema,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    hot_mgr: &crate::storage::core::hot_store::HotStoreManager,
    db: &str,
) -> Result<Option<RecordBatch>> {
    if !sum_aggregates_supported(aggregates, table_schema) {
        return Ok(None);
    }
    let aggregate_columns: Vec<Option<usize>> = aggregates
        .iter()
        .map(|aggregate| match &*aggregate.arg {
            LogicalExpr::Column(index) => Some(*index),
            LogicalExpr::Wildcard => None,
            _ => None,
        })
        .collect();
    let mut accumulators = aggregates
        .iter()
        .zip(&aggregate_columns)
        .map(|(aggregate, column)| AggAccum::new(aggregate.func, *column, table_schema))
        .collect::<Vec<_>>();

    let segment_files = if table_dir.as_os_str().is_empty() || !table_dir.exists() {
        Vec::new()
    } else {
        partition::prune_segments_by_partition(
            SegmentManager::discover_segments(table_dir)?,
            spec,
            Some(table_meta),
        )
    };
    let predicate = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(spec);
    let in_checks = row_group::build_in_check_list(spec);
    let set_checks = row_group::build_set_check_list(spec);
    let set_in_checks = row_group::build_set_in_check_list(spec);
    let source_columns = aggregate_source_columns(spec, &aggregate_columns);
    let aggregate_positions = aggregate_columns
        .iter()
        .map(|column| {
            column.and_then(|index| source_columns.iter().position(|source| *source == index))
        })
        .collect::<Vec<_>>();

    let has_sum = accumulators
        .iter()
        .any(|a| a.func == crate::sql::planner::AggFunc::Sum && !a.sum_unsupported);

    for segment_path in &segment_files {
        let meta = segment_mgr.segment_meta(segment_path)?;
        let meta_deleted = meta
            .as_ref()
            .map(|m| m.deleted_row_count)
            .unwrap_or(u64::MAX);
        let meta_rows = meta.as_ref().map(|m| m.row_count).unwrap_or(0);
        let pruned = meta.as_ref().is_some_and(|m| {
            !row_group::prune_segment_by_zone_map(&predicate, &m.segment_zone_maps)
        });
        if pruned {
            continue;
        }

        // Sum accumulators need the column data (zone maps carry no sums);
        // serve them from the per-segment partial-sum cache when possible.
        if has_sum && !spec.has_predicates() && meta_deleted == 0 {
            // COUNT/MIN/MAX fold from the segment zone maps.
            if let Some(m) = &meta {
                fold_metadata(&mut accumulators, &m.segment_zone_maps, m.row_count)?;
            }
            // SUM folds from the per-segment partial-sum cache; a miss falls
            // through to the reader scan below, which populates the cache.
            let needed: Vec<(String, bool)> = aggregates
                .iter()
                .zip(&aggregate_columns)
                .filter(|(agg, _)| agg.func == crate::sql::planner::AggFunc::Sum)
                .map(|(_agg, col)| {
                    let name = col
                        .and_then(|i| table_schema.columns.get(i))
                        .map(|c| c.name.clone())
                        .unwrap_or_default();
                    let is_float = col
                        .and_then(|i| table_schema.columns.get(i))
                        .map(|c| matches!(c.data_type, DataType::Float32 | DataType::Float64))
                        .unwrap_or(false);
                    (name, is_float)
                })
                .collect();
            let cached = segment_sum_cache_lookup(
                segment_path.to_string_lossy().as_ref(),
                meta_rows,
                &needed,
            );
            if let Some(cached) = &cached {
                let mut sum_idx = 0usize;
                for accumulator in accumulators.iter_mut() {
                    if accumulator.func != crate::sql::planner::AggFunc::Sum {
                        continue;
                    }
                    if let Some(v) = cached.get(sum_idx) {
                        accumulator.fold_cached_sum(v);
                    }
                    sum_idx += 1;
                }
                continue;
            }
        }

        let reader = segment_mgr.open_reader_cached(segment_path)?;
        if !spec.has_predicates() && reader.deleted_row_count() == 0 && !has_sum {
            fold_metadata(
                &mut accumulators,
                reader.segment_zone_maps(),
                reader.row_count(),
            )?;
            continue;
        }

        let sum_before: Vec<(Option<i64>, Option<f64>)> = if has_sum {
            accumulators
                .iter()
                .map(|a| (a.sum_i, a.sum_f))
                .collect()
        } else {
            Vec::new()
        };

        let candidate_indices = if let Some(meta) = &meta {
            row_group::prune_row_group_meta(
                &meta.row_groups,
                &predicate,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
            )
            .candidate_indices
        } else {
            row_group::prune_row_groups(
                &reader,
                &predicate,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
                reader.segment_zone_maps(),
            )
            .candidate_indices
        };
        let row_groups = reader.footer_row_groups_info();
        let mut candidates = vec![false; row_groups.len()];
        for index in candidate_indices {
            if let Some(candidate) = candidates.get_mut(index) {
                *candidate = true;
            }
        }
        let mut row_start = 0u64;

        for (row_group_index, row_group_meta) in row_groups.iter().enumerate() {
            if !candidates[row_group_index] {
                row_start += row_group_meta.row_count;
                continue;
            }

            let predicate_match =
                if spec.in_predicates.is_empty() && spec.remaining_predicate.is_none() {
                    predicate.classify_row_group(&row_group_meta.zone_maps)
                } else {
                    crate::storage::partition::pruning::PredicateMatch::Partial
                };
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::None {
                row_start += row_group_meta.row_count;
                continue;
            }
            if predicate_match == crate::storage::partition::pruning::PredicateMatch::All
                && reader.delete_bitmap().is_empty()
            {
                fold_metadata(
                    &mut accumulators,
                    &row_group_meta.zone_maps,
                    row_group_meta.row_count,
                )?;
                row_start += row_group_meta.row_count;
                continue;
            }

            let mut batch =
                reader.read_row_group_with_projection(row_group_index, &source_columns)?;
            if !reader.delete_bitmap().is_empty() {
                batch = delete_bitmap::filter_batch(&batch, row_start, reader.delete_bitmap())?;
            }
            if spec.has_predicates() {
                batch = row_filter::apply_predicates_with_column_map(
                    &batch,
                    &source_columns,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                )?;
            }
            fold_batch(&mut accumulators, &aggregate_positions, &batch)?;
            row_start += row_group_meta.row_count;
        }

        if has_sum && !spec.has_predicates() && reader.deleted_row_count() == 0 {
            let needed: Vec<(String, bool)> = aggregates
                .iter()
                .zip(&aggregate_columns)
                .filter(|(agg, _)| agg.func == crate::sql::planner::AggFunc::Sum)
                .map(|(_agg, col)| {
                    let name = col
                        .and_then(|i| table_schema.columns.get(i))
                        .map(|c| c.name.clone())
                        .unwrap_or_default();
                    let is_float = col
                        .and_then(|i| table_schema.columns.get(i))
                        .map(|c| matches!(c.data_type, DataType::Float32 | DataType::Float64))
                        .unwrap_or(false);
                    (name, is_float)
                })
                .collect();
            let deltas: Vec<SumCacheValue> = accumulators
                .iter()
                .zip(sum_before.iter())
                .map(|(a, (i0, f0))| SumCacheValue {
                    i: match (a.sum_i, *i0) {
                        (Some(v), Some(base)) => Some(v.wrapping_sub(base)),
                        (v, None) => v,
                        (None, Some(_)) => None,
                    },
                    f: match (a.sum_f, *f0) {
                        (Some(v), Some(base)) => Some(v - base),
                        (v, None) => v,
                        (None, Some(_)) => None,
                    },
                })
                .collect();
            segment_sum_cache_store(
                segment_path.to_string_lossy().as_ref(),
                meta_rows,
                &needed,
                &deltas,
            );
        }
    }

    let memtable_batches =
        scan_memtable::scan_memtables(memtable_mgr, hot_mgr, db, &spec.table_name, spec);
    for batch in &memtable_batches {
        for (accumulator, column) in accumulators.iter_mut().zip(&aggregate_columns) {
            let batch_column = column.and_then(|index| {
                table_schema
                    .columns
                    .get(index)
                    .and_then(|column| batch.schema.index_of(&column.name))
            });
            accumulator.fold_batch(batch, batch_column)?;
        }
    }

    let results = accumulators
        .into_iter()
        .map(AggAccum::finalize)
        .collect::<Vec<_>>();
    let columns = aggregates
        .iter()
        .map(|aggregate| aggregate_result_column_def(aggregate, table_schema))
        .collect();
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema);
    for (index, value) in results.into_iter().enumerate() {
        batch
            .column_mut(index)
            .ok_or_else(|| crate::common::error::TaibaiError::Internal("agg col".into()))?
            .push(&value)?;
    }
    Ok(Some(batch))
}

fn aggregate_source_columns(spec: &ScanSpec, aggregate_columns: &[Option<usize>]) -> Vec<usize> {
    let mut columns = spec.all_predicate_columns();
    for column in aggregate_columns.iter().flatten() {
        if !columns.contains(column) {
            columns.push(*column);
        }
    }
    columns
}

fn fold_metadata(
    accumulators: &mut [AggAccum],
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
    row_count: u64,
) -> Result<()> {
    for accumulator in accumulators {
        accumulator.fold_metadata(zone_maps, row_count)?;
    }
    Ok(())
}

fn fold_batch(
    accumulators: &mut [AggAccum],
    aggregate_positions: &[Option<usize>],
    batch: &RecordBatch,
) -> Result<()> {
    for (accumulator, column) in accumulators.iter_mut().zip(aggregate_positions) {
        accumulator.fold_batch(batch, *column)?;
    }
    Ok(())
}

struct AggAccum {
    func: AggFunc,
    column: Option<usize>,
    count: u64,
    best_min: Option<ScalarValue>,
    best_max: Option<ScalarValue>,
    col_type: DataType,
    sum_i: Option<i64>,
    sum_f: Option<f64>,
    /// SUM over a column type the metadata fast path cannot answer
    /// (non-numeric argument columns).
    sum_unsupported: bool,
}

impl AggAccum {
    fn new(func: AggFunc, column: Option<usize>, schema: &Schema) -> Self {
        let col_type = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.data_type)
            .unwrap_or(DataType::Int64);
        let (sum_i, sum_f, sum_unsupported) = if func == AggFunc::Sum {
            match col_type {
                DataType::Float32 | DataType::Float64 => (None, Some(0.0), false),
                DataType::Int64
                | DataType::Int32
                | DataType::Int16
                | DataType::Int8
                | DataType::Decimal
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp => (Some(0), None, false),
                _ => (None, None, true),
            }
        } else {
            (None, None, false)
        };
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
        }
    }

    fn fold_metadata(
        &mut self,
        zone_maps: &[crate::storage::partition::pruning::ZoneMap],
        row_count: u64,
    ) -> Result<()> {
        let Some(column) = self.column else {
            if self.func == AggFunc::Count {
                self.count += row_count;
            }
            return Ok(());
        };
        let Some(zone_map) = zone_maps.get(column) else {
            return Ok(());
        };
        match self.func {
            AggFunc::Count => self.count += row_count.saturating_sub(zone_map.null_count),
            AggFunc::Min if !zone_map.min_value.is_null() => {
                self.merge_min(zone_map.min_value.clone())
            }
            AggFunc::Max if !zone_map.max_value.is_null() => {
                self.merge_max(zone_map.max_value.clone())
            }
            _ => {}
        }
        Ok(())
    }

    fn fold_batch(&mut self, batch: &RecordBatch, column: Option<usize>) -> Result<()> {
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
            AggFunc::Sum => match self.sum_i {
                Some(_) => {
                    if let Some(part) = values.col_sum_i64() {
                        self.sum_i =
                            Some(self.sum_i.unwrap_or(0).wrapping_add(part));
                    }
                }
                _ => {
                    if let Some(part) = values.col_sum_f64() {
                        self.sum_f = Some(self.sum_f.unwrap_or(0.0) + part);
                    }
                }
            },
            _ => {}
        }
        Ok(())
    }

    /// Fold a cached per-segment partial sum into this accumulator.
    fn fold_cached_sum(&mut self, cached: &SumCacheValue) {
        match self.col_type {
            DataType::Float32 | DataType::Float64 => {
                self.sum_f = Some(self.sum_f.unwrap_or(0.0) + cached.f.unwrap_or(0.0));
            }
            _ => {
                self.sum_i = Some(
                    self.sum_i
                        .unwrap_or(0)
                        .wrapping_add(cached.i.unwrap_or(0)),
                );
            }
        }
    }

    fn merge_min(&mut self, value: ScalarValue) {
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

    fn merge_max(&mut self, value: ScalarValue) {
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

    fn finalize(self) -> ScalarValue {
        match self.func {
            AggFunc::Count => ScalarValue::Int64(Some(self.count as i64)),
            AggFunc::Min => self
                .best_min
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Max => self
                .best_max
                .unwrap_or_else(|| null_scalar_of_type(self.col_type)),
            AggFunc::Sum => match self.col_type {
                DataType::Float32 | DataType::Float64 => {
                    ScalarValue::Float64(self.sum_f)
                }
                _ => ScalarValue::Int64(self.sum_i),
            },
            _ => ScalarValue::Int64(None),
        }
    }
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
        DataType::Decimal => ScalarValue::Decimal(None),
        DataType::Date => ScalarValue::Date(None),
        DataType::Timestamp => ScalarValue::Timestamp(None),
        DataType::VarChar => ScalarValue::VarChar(None),
        DataType::Bool => ScalarValue::Bool(None),
        _ => ScalarValue::Int64(None),
    }
}


/// Per-segment partial sums for the metadata aggregate fast path. Keyed by
/// (segment path, row count, column, float?) so appends, compaction, and
/// deletes naturally invalidate entries. In-process only.
fn segment_sum_cache() -> &'static std::sync::Mutex<
    std::collections::HashMap<(String, String, u64, u64, bool), SumCacheValue>,
> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<std::sync::Mutex<std::collections::HashMap<(String, String, u64, u64, bool), SumCacheValue>>> =
        OnceLock::new();
    CACHE.get_or_init(|| {
            std::sync::Mutex::new(std::collections::HashMap::new())
        })
}

#[derive(Clone, Copy, Debug)]
struct SumCacheValue {
    i: Option<i64>,
    f: Option<f64>,
}

fn segment_sum_cache_lookup(
    segment: &str,
    row_count: u64,
    columns: &[(String, bool)],
) -> Option<Vec<SumCacheValue>> {
    let cache = segment_sum_cache().lock().unwrap();
    columns
        .iter()
        .map(|(name, is_float)| {
            cache
                .get(
                    &(
                        segment.to_string(),
                        name.clone(),
                        row_count,
                        0,
                        *is_float,
                    ),
                )
                .copied()
        })
        .collect()
}

fn segment_sum_cache_store(
    segment: &str,
    row_count: u64,
    columns: &[(String, bool)],
    values: &[SumCacheValue],
) {
    let mut cache = segment_sum_cache().lock().unwrap();
    for ((name, is_float), value) in columns.iter().zip(values.iter()) {
        cache.insert(
            (segment.to_string(), name.clone(), row_count, 0u64, *is_float),
            *value,
        );
    }
}

/// SUM is answerable by the metadata fast path only for numeric argument
/// columns; other types keep the generic scan.
fn sum_aggregates_supported(
    aggregates: &[AggregateExpr],
    table_schema: &Schema,
) -> bool {
    for agg in aggregates {
        if agg.func != crate::sql::planner::AggFunc::Sum || agg.distinct {
            continue;
        }
        let supported = match &*agg.arg {
            crate::sql::planner::LogicalExpr::Column(idx) => table_schema
                .columns
                .get(*idx)
                .map(|c| {
                    matches!(
                        c.data_type,
                        crate::protocol::types::DataType::Int64
                            | crate::protocol::types::DataType::Int32
                            | crate::protocol::types::DataType::Int16
                            | crate::protocol::types::DataType::Int8
                            | crate::protocol::types::DataType::Decimal
                            | crate::protocol::types::DataType::Date
                            | crate::protocol::types::DataType::Time
                            | crate::protocol::types::DataType::Timestamp
                            | crate::protocol::types::DataType::Float64
                            | crate::protocol::types::DataType::Float32
                    )
                })
                .unwrap_or(false),
            _ => false,
        };
        if !supported {
            return false;
        }
    }
    true
}
