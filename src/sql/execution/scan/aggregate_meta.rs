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
use crate::sql::execution::operators::aggregate::{aggregate_result_type, format_aggregate_name};
use crate::sql::execution::scan::memtable as scan_memtable;
use crate::sql::execution::scan::partition;
use crate::sql::execution::scan::row_filter;
use crate::sql::execution::scan::row_group;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::segment::delete_bitmap;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

/// Compute the result of a simple metadata-only aggregate query.
///
/// `aggregates` are the (already logical-schema-indexed) aggregate expressions
/// from the SELECT list. Returns a single-row result batch.
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
) -> Result<RecordBatch> {
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

    for segment_path in &segment_files {
        let meta = segment_mgr.segment_meta(segment_path)?;
        if let Some(meta) = &meta {
            if !row_group::prune_segment_by_zone_map(&predicate, &meta.segment_zone_maps) {
                continue;
            }
            if !spec.has_predicates() && meta.deleted_row_count == 0 {
                fold_metadata(&mut accumulators, &meta.segment_zone_maps, meta.row_count)?;
                continue;
            }
        }

        let reader = segment_mgr.open_reader_cached(segment_path)?;
        if !spec.has_predicates() && reader.deleted_row_count() == 0 {
            fold_metadata(
                &mut accumulators,
                reader.segment_zone_maps(),
                reader.row_count(),
            )?;
            continue;
        }

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
        .map(|aggregate| {
            ColumnDef::new(
                &format_aggregate_name(aggregate, table_schema),
                aggregate_result_type(aggregate, table_schema),
            )
        })
        .collect();
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema);
    for (index, value) in results.into_iter().enumerate() {
        batch
            .column_mut(index)
            .ok_or_else(|| crate::common::error::TaibaiError::Internal("agg col".into()))?
            .push(&value)?;
    }
    Ok(batch)
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
}

impl AggAccum {
    fn new(func: AggFunc, column: Option<usize>, schema: &Schema) -> Self {
        let col_type = column
            .and_then(|index| schema.columns.get(index))
            .map(|column| column.data_type)
            .unwrap_or(DataType::Int64);
        Self {
            func,
            column,
            count: 0,
            best_min: None,
            best_max: None,
            col_type,
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
            _ => {}
        }
        Ok(())
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
