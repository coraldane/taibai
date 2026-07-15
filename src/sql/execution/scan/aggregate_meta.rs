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

use std::collections::HashMap;
use std::path::Path;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::{aggregate_result_type, format_aggregate_name};
use crate::sql::execution::scan::count;
use crate::sql::execution::scan::memtable as scan_memtable;
use crate::sql::execution::scan::partition;
use crate::sql::execution::scan::row_filter;
use crate::sql::execution::scan::row_group;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::sql::planner::{AggregateExpr, AggFunc, LogicalExpr};
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
    // Resolve each aggregate's argument column index (None == wildcard).
    let agg_cols: Vec<Option<usize>> = aggregates
        .iter()
        .map(|agg| match &*agg.arg {
            LogicalExpr::Column(idx) => Some(*idx),
            LogicalExpr::Wildcard => None,
            _ => None,
        })
        .collect();

    let mut results: Vec<ScalarValue> = Vec::with_capacity(aggregates.len());

    for (agg, &col_opt) in aggregates.iter().zip(agg_cols.iter()) {
        let value = compute_single_aggregate(
            spec,
            agg,
            col_opt,
            table_meta,
            table_schema,
            table_dir,
            segment_mgr,
            memtable_mgr,
            hot_mgr,
            db,
        )?;
        results.push(value);
    }

    // Build result schema using the same naming/type rules as the aggregate op.
    let col_defs: Vec<ColumnDef> = aggregates
        .iter()
        .map(|agg| {
            let name = format_aggregate_name(agg, table_schema);
            let dt = aggregate_result_type(agg, table_schema);
            ColumnDef::new(&name, dt)
        })
        .collect();
    let schema = Schema::new(col_defs);
    let mut batch = RecordBatch::new(schema);
    for (i, v) in results.into_iter().enumerate() {
        batch
            .column_mut(i)
            .ok_or_else(|| crate::common::error::TaibaiError::Internal("agg col".into()))?
            .push(&v)?;
    }
    Ok(batch)
}

fn compute_single_aggregate(
    spec: &ScanSpec,
    agg: &AggregateExpr,
    col_opt: Option<usize>,
    table_meta: &TableMeta,
    table_schema: &Schema,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    hot_mgr: &crate::storage::core::hot_store::HotStoreManager,
    db: &str,
) -> Result<ScalarValue> {
    // COUNT(*) — reuse the existing, fully-tested fast path. It already
    // accounts for partition pruning, delete bitmaps and memtable rows.
    if matches!(agg.func, AggFunc::Count) && col_opt.is_none() {
        return count_count_star(spec, table_meta, table_dir, segment_mgr, memtable_mgr, db);
    }

    let col_idx = match col_opt {
        Some(i) => i,
        None => return Ok(ScalarValue::Int64(None)),
    };
    eprintln!("AGGMETA ENTRY func={:?} col_idx={}", agg.func, col_idx);

    // Discover + partition-prune candidate segments.
    let segment_files = partition::prune_segments_by_partition(
        SegmentManager::discover_segments(table_dir)?,
        spec,
        Some(table_meta),
    );
    let pred = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(spec);
    let in_checks = row_group::build_in_check_list(spec);
    let set_checks = row_group::build_set_check_list(spec);
    let set_in_checks = row_group::build_set_in_check_list(spec);

    let mut acc = AggAccum::new(agg.func, col_idx, table_schema);

    for seg_path in &segment_files {
        let meta = segment_mgr.segment_meta(seg_path)?;
        // Segment-level zone map precheck.
        if let Some(m) = &meta {
            if !row_group::prune_segment_by_zone_map(&pred, &m.segment_zone_maps) {
                continue;
            }
        }
        let reader = segment_mgr.open_reader_cached(seg_path)?;

        let candidate_indices = if let Some(m) = &meta {
            row_group::prune_row_group_meta(
                &m.row_groups,
                &pred,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
            )
            .candidate_indices
        } else {
            row_group::prune_row_groups(
                &reader,
                &pred,
                &bloom_checks,
                &in_checks,
                &set_checks,
                &set_in_checks,
                reader.segment_zone_maps(),
            )
            .candidate_indices
        };

        let has_deletes = reader.deleted_row_count() > 0;

        for rg_idx in candidate_indices {
            if has_deletes {
                // Conservative path: read the column, apply delete bitmap, then
                // apply the row-level predicate before folding.
                let mut batch = reader.read_row_group_with_projection(rg_idx, &[col_idx])?;
                let bitmap_active = !reader.delete_bitmap().is_empty();
                if bitmap_active {
                    let rg_start_row: u64 = reader
                        .footer_row_groups_info()
                        .iter()
                        .take(rg_idx)
                        .map(|r| r.row_count)
                        .sum();
                    batch = delete_bitmap::filter_batch(&batch, rg_start_row, reader.delete_bitmap())?;
                }
                // Apply the scan's remaining predicate (if any) on the projected
                // batch so MIN/MAX/COUNT only see matching rows.
                if spec.has_predicates() {
                    let filtered = row_filter::apply_predicates_with_column_map(
                        &batch,
                        &[col_idx],
                        &spec.eq_predicates,
                        &spec.range_predicates,
                        &spec.in_predicates,
                        spec.remaining_predicate.as_ref(),
                    )?;
                    acc.fold_batch(&filtered, col_idx)?;
                } else {
                    acc.fold_batch(&batch, col_idx)?;
                }
            } else {
                // Pure metadata path: trust the row group zone map.
                let zms = reader.zone_maps(rg_idx);
                if let Some(zm) = zms.get(col_idx) {
                    eprintln!(
                        "AGGMETA func={:?} col_idx={} zm.min={:?} zm.max={:?} has_deletes={}",
                        agg.func,
                        col_idx,
                        zm.min_value,
                        zm.max_value,
                        has_deletes
                    );
                    acc.fold_zone_map(zm)?;
                }
            }
        }
    }

    // Fold in memtable rows.
    let memtable_batches =
        scan_memtable::scan_memtables(memtable_mgr, hot_mgr, db, &spec.table_name, spec);
    for b in &memtable_batches {
        // The memtable batch may be a full-row projection; fold the column if
        // present, otherwise count rows.
        acc.fold_batch(b, col_idx)?;
    }

    Ok(acc.finalize())
}

/// COUNT(*) routed through `count::count_star`.
fn count_count_star(
    spec: &ScanSpec,
    table_meta: &TableMeta,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    db: &str,
) -> Result<ScalarValue> {
    let stats: Option<&mut HashMap<String, crate::storage::segment::ScanStats>> = None;
    let n = count::count_star(spec, table_meta, table_dir, segment_mgr, memtable_mgr, db, stats)?;
    Ok(ScalarValue::Int64(Some(n as i64)))
}

/// Running accumulator for one of COUNT(col)/MIN/MAX over metadata or data.
struct AggAccum {
    func: AggFunc,
    count: u64,
    best_min: Option<ScalarValue>,
    best_max: Option<ScalarValue>,
    col_type: DataType,
}

impl AggAccum {
    fn new(func: AggFunc, col_idx: usize, schema: &Schema) -> Self {
        let col_type = schema
            .columns
            .get(col_idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64);
        Self {
            func,
            count: 0,
            best_min: None,
            best_max: None,
            col_type,
        }
    }

    fn fold_zone_map(&mut self, zm: &crate::storage::partition::pruning::ZoneMap) -> Result<()> {
        // row_count - null_count gives non-null count for COUNT(col).
        let non_null = zm.row_count.saturating_sub(zm.null_count);
        match self.func {
            AggFunc::Count => self.count += non_null,
            AggFunc::Min => {
                if !zm.min_value.is_null() {
                    self.merge_min(zm.min_value.clone());
                }
            }
            AggFunc::Max => {
                if !zm.max_value.is_null() {
                    self.merge_max(zm.max_value.clone());
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn fold_batch(&mut self, batch: &RecordBatch, col_idx: usize) -> Result<()> {
        let Some(col) = batch.column(col_idx) else {
            // Column not in this projection; for COUNT(col) treat as all-null,
            // for MIN/MAX there is nothing to fold.
            return Ok(());
        };
        match self.func {
            AggFunc::Count => self.count += col.col_count_non_null(),
            AggFunc::Min => {
                if let Some(m) = col.col_min() {
                    self.merge_min(m);
                }
            }
            AggFunc::Max => {
                if let Some(m) = col.col_max() {
                    self.merge_max(m);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge_min(&mut self, v: ScalarValue) {
        self.best_min = Some(match self.best_min.take() {
            Some(cur) => {
                if crate::sql::execution::operators::aggregate::compare_scalars(&v, &cur)
                    == std::cmp::Ordering::Less
                {
                    v
                } else {
                    cur
                }
            }
            None => v,
        });
    }

    fn merge_max(&mut self, v: ScalarValue) {
        self.best_max = Some(match self.best_max.take() {
            Some(cur) => {
                if crate::sql::execution::operators::aggregate::compare_scalars(&v, &cur)
                    == std::cmp::Ordering::Greater
                {
                    v
                } else {
                    cur
                }
            }
            None => v,
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
