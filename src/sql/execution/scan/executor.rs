use crate::common::error::Result;
use crate::storage::segment::ScanStats;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::count;
use super::memtable;
use super::partition;
use super::row_filter;
use super::runtime::ScanRuntime;
use super::spec::ScanSpec;

pub struct ScanExecutor<'a> {
    spec: &'a ScanSpec,
    runtime: ScanRuntime,
    table_schema: crate::types::batch::Schema,
    table_meta: crate::storage::catalog::schema::TableMeta,
    db: &'a str,
    stats: Option<&'a mut HashMap<String, ScanStats>>,
}

impl<'a> ScanExecutor<'a> {
    pub fn new(
        spec: &'a ScanSpec,
        engine: &'a crate::sql::execution::engine::Engine,
        table_schema: crate::types::batch::Schema,
        table_meta: crate::storage::catalog::schema::TableMeta,
        db: &'a str,
    ) -> Self {
        Self {
            spec,
            runtime: ScanRuntime::from_engine(engine),
            table_schema,
            table_meta,
            db,
            stats: None,
        }
    }

    pub fn from_runtime(
        spec: &'a ScanSpec,
        runtime: ScanRuntime,
        table_schema: crate::types::batch::Schema,
        table_meta: crate::storage::catalog::schema::TableMeta,
        db: &'a str,
    ) -> Self {
        Self {
            spec,
            runtime,
            table_schema,
            table_meta,
            db,
            stats: None,
        }
    }

    pub fn with_stats(mut self, stats: &'a mut HashMap<String, ScanStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        if self.spec.is_count_star {
            return self.execute_count_star();
        }
        self.execute_scan()
    }

    fn execute_count_star(&mut self) -> Result<Vec<RecordBatch>> {
        if let Some(count) = self.try_resident_hot_count()? {
            let result = count::build_count_star_result(count);
            return match result {
                crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
                _ => Ok(Vec::new()),
            };
        }

        if self.table_meta.ttl.is_some() {
            let mut ttl_spec = self.spec.clone();
            ttl_spec.is_count_star = false;
            ttl_spec.projection = self
                .table_meta
                .get_ttl_column_index_cached()
                .map(|idx| vec![idx]);
            let mut executor = ScanExecutor::from_runtime(
                &ttl_spec,
                self.runtime.clone(),
                self.table_schema.clone(),
                self.table_meta.clone(),
                self.db,
            );
            let count = executor
                .execute_scan()?
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum();
            let result = count::build_count_star_result(count);
            return match result {
                crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
                _ => Ok(Vec::new()),
            };
        }

        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        if !table_dir.exists() {
            let count = memtable::count_hot_memtable_rows(
                self.runtime.memtable_manager(),
                self.runtime.hot_store_manager(),
                self.db,
                &self.spec.table_name,
                self.spec,
            )
            .unwrap_or_else(|| {
                memtable::count_memtable_rows(
                    self.runtime.memtable_manager(),
                    self.db,
                    &self.spec.table_name,
                    self.spec,
                )
            });
            let result = count::build_count_star_result(count);
            return match result {
                crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
                _ => Ok(Vec::new()),
            };
        }
        let count = count::count_star(
            self.spec,
            &self.table_meta,
            &table_dir,
            self.runtime.segment_mgr(),
            self.runtime.memtable_manager(),
            self.db,
            self.stats.as_deref_mut(),
        )?;
        let result = count::build_count_star_result(count);
        match result {
            crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
            _ => Ok(Vec::new()),
        }
    }

    fn execute_scan(&mut self) -> Result<Vec<RecordBatch>> {
        let mut output = LimitedBatchSink::new(None);
        self.execute_scan_into(&mut output)?;
        Ok(output.finish())
    }

    fn execute_scan_into<S: BatchSink>(&mut self, output: &mut S) -> Result<()> {
        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);

        let plan =
            super::strategy::ScanPlanner::new(self.spec, &self.table_schema, &self.table_meta)
                .build();
        let mut scan_limit = plan.scan_limit;

        if let Some(batch) = self.try_resident_hot_scan(&plan)? {
            output.set_limit(scan_limit);
            output.push(batch);
            return Ok(());
        }

        let memtables_present = !self
            .runtime
            .memtable_manager()
            .get_by_table(self.db, &self.spec.table_name)
            .is_empty();
        if plan.ordered_limit.is_some() && memtables_present {
            scan_limit = None;
        }

        if table_dir.exists() {
            let all_segments = SegmentManager::discover_segments(&table_dir)?;

            let filtered_segments = partition::prune_segments_by_partition(
                all_segments,
                self.spec,
                Some(&self.table_meta),
            );

            let mgr = self.runtime.segment_mgr().clone();

            let filtered_segments = self
                .prune_segments_by_cluster_key(filtered_segments, plan.cluster_key_bounds.as_ref());
            let ordered_limit = if let Some(order) = plan.ordered_limit {
                if !memtables_present
                    && self.ordered_cluster_segments_are_monotonic(
                        &filtered_segments,
                        order.order_column_index,
                    )
                {
                    Some(order)
                } else {
                    scan_limit = None;
                    None
                }
            } else {
                None
            };
            output.set_limit(scan_limit);

            let mut filtered_segments = filtered_segments;
            if ordered_limit.is_some_and(|order| order.desc) {
                filtered_segments.reverse();
            }

            for seg_path in &filtered_segments {
                let meta = mgr.segment_meta(seg_path).ok().flatten();
                let meta_candidate_indices = meta.as_ref().and_then(|meta| {
                    let pred = self.spec.to_row_group_predicate();
                    if !super::row_group::prune_segment_by_zone_map(&pred, &meta.segment_zone_maps)
                    {
                        return Some(Vec::new());
                    }
                    Some(
                        super::row_group::prune_row_group_meta(
                            &meta.row_groups,
                            &pred,
                            &super::row_group::build_bloom_check_list(self.spec),
                            &super::row_group::build_in_check_list(self.spec),
                            &super::row_group::build_set_check_list(self.spec),
                            &super::row_group::build_set_in_check_list(self.spec),
                        )
                        .candidate_indices,
                    )
                });
                if meta_candidate_indices
                    .as_ref()
                    .is_some_and(|candidates| candidates.is_empty())
                {
                    continue;
                }

                let reader = match mgr.open_reader_cached(seg_path) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if let Some(s) = self.stats.as_deref_mut() {
                    s.entry(self.spec.table_name.clone())
                        .or_default()
                        .segments_opened += 1;
                }

                let candidate_indices = if let Some(candidates) = meta_candidate_indices {
                    candidates
                } else {
                    let Some(prune_result) = super::strategy::prune_segment(self.spec, &reader)
                    else {
                        continue;
                    };
                    prune_result.candidate_indices
                };

                if let Some(s) = self.stats.as_deref_mut() {
                    let stats_entry = s.entry(self.spec.table_name.clone()).or_default();
                    stats_entry.row_groups_total += reader.num_row_groups() as u64;
                    stats_entry.row_groups_pruned +=
                        (reader.num_row_groups() - candidate_indices.len()) as u64;
                    if scan_limit.is_none() && ordered_limit.is_none() {
                        stats_entry.row_groups_read += candidate_indices.len() as u64;
                    }
                }

                let mut candidate_indices = candidate_indices;
                if ordered_limit.is_some_and(|order| order.desc) {
                    candidate_indices.reverse();
                }

                if scan_limit.is_some() || ordered_limit.is_some() {
                    for rg_idx in &candidate_indices {
                        self.record_row_group_read(&reader, *rg_idx, &plan);
                        let mut batch = match super::strategy::read_row_group_with_strategy(
                            self.spec,
                            &plan,
                            &self.table_schema,
                            &reader,
                            *rg_idx,
                        ) {
                            Ok(b) => b,
                            Err(_) => continue,
                        };
                        if ordered_limit.is_some_and(|order| order.desc) {
                            batch = reverse_batch(&batch)?;
                        }
                        self.record_rows_scanned(batch.num_rows() as u64);
                        let batch = super::strategy::apply_ttl_filter_and_projection(
                            batch,
                            &self.table_meta,
                            &plan.projection_cols,
                            plan.strip_ttl_column,
                        )?;
                        if !output.push(batch) {
                            return Ok(());
                        }
                    }
                    continue;
                }

                use rayon::prelude::*;

                let mut rg_batches: Vec<(usize, RecordBatch)> = candidate_indices
                    .par_iter()
                    .enumerate()
                    .filter_map(|(pos, rg_idx)| {
                        super::strategy::read_row_group_with_strategy(
                            self.spec,
                            &plan,
                            &self.table_schema,
                            &reader,
                            *rg_idx,
                        )
                        .ok()
                        .filter(|batch| batch.num_rows() > 0)
                        .map(|batch| (pos, batch))
                    })
                    .collect();
                rg_batches.sort_by_key(|(pos, _)| *pos);

                let rows_scanned = rg_batches
                    .iter()
                    .map(|(_, batch)| batch.num_rows() as u64)
                    .sum::<u64>();
                if let Some(s) = self.stats.as_deref_mut() {
                    s.entry(self.spec.table_name.clone())
                        .or_default()
                        .rows_scanned += rows_scanned;
                }
                for (_, batch) in rg_batches {
                    let batch = super::strategy::apply_ttl_filter_and_projection(
                        batch,
                        &self.table_meta,
                        &plan.projection_cols,
                        plan.strip_ttl_column,
                    )?;
                    if !output.push(batch) {
                        return Ok(());
                    }
                }
            }
        } else {
            output.set_limit(scan_limit);
        }

        let mut memtable_spec = self.spec.clone();
        memtable_spec.projection = Some(plan.projection_cols.clone());

        for batch in memtable::scan_memtables(
            self.runtime.memtable_manager(),
            self.runtime.hot_store_manager(),
            self.db,
            &self.spec.table_name,
            &memtable_spec,
        ) {
            let batch = super::strategy::apply_ttl_filter_and_projection(
                batch,
                &self.table_meta,
                &plan.projection_cols,
                plan.strip_ttl_column,
            )?;
            if !output.push(batch) {
                return Ok(());
            }
        }

        Ok(())
    }

    fn try_resident_hot_scan(
        &self,
        plan: &super::strategy::ScanPlan,
    ) -> Result<Option<RecordBatch>> {
        if self.spec.reader_txn_id != 0 {
            return Ok(None);
        }
        let Some(store) = self
            .runtime
            .hot_store_manager()
            .get(self.db, &self.spec.table_name)
        else {
            return Ok(None);
        };
        if store.row_count() == 0 {
            return Ok(None);
        }

        let target_partition = if self.table_meta.is_partitioned() {
            let Some(pi) = self.spec.partition_info.as_ref() else {
                return Ok(None);
            };
            let Some(partition_key) =
                partition::extract_partition_eq_value(self.spec, pi, &self.table_meta)
            else {
                return Ok(None);
            };
            Some(partition_key)
        } else {
            None
        };

        if !store.is_resident_for_partition(target_partition.as_deref()) {
            return Ok(None);
        }

        let mut hot_spec = self.spec.clone();
        hot_spec.projection = Some(plan.projection_cols.clone());
        let Some(batch) = memtable::scan_hot_store_by_key(&store, &hot_spec, 0) else {
            return Ok(None);
        };
        let batch = super::strategy::apply_ttl_filter_and_projection(
            batch,
            &self.table_meta,
            &plan.projection_cols,
            plan.strip_ttl_column,
        )?;
        Ok(Some(batch))
    }

    fn try_resident_hot_count(&self) -> Result<Option<u64>> {
        if self.spec.reader_txn_id != 0 || self.table_meta.ttl.is_some() {
            return Ok(None);
        }
        let Some(store) = self
            .runtime
            .hot_store_manager()
            .get(self.db, &self.spec.table_name)
        else {
            return Ok(None);
        };
        if store.row_count() == 0 {
            return Ok(None);
        }

        let target_partition = if self.table_meta.is_partitioned() {
            let Some(pi) = self.spec.partition_info.as_ref() else {
                return Ok(None);
            };
            let Some(partition_key) =
                partition::extract_partition_eq_value(self.spec, pi, &self.table_meta)
            else {
                return Ok(None);
            };
            Some(partition_key)
        } else {
            None
        };

        if !store.is_resident_for_partition(target_partition.as_deref()) {
            return Ok(None);
        }

        Ok(memtable::count_hot_store_by_key(&store, self.spec, 0))
    }

    fn ordered_cluster_segments_are_monotonic(&self, segments: &[PathBuf], col_idx: usize) -> bool {
        segments.len() <= 1 || self.cluster_segment_zone_maps_are_monotonic(segments, col_idx)
    }

    fn prune_segments_by_cluster_key(
        &mut self,
        segments: Vec<PathBuf>,
        bounds: Option<&row_filter::ClusterKeyBounds>,
    ) -> Vec<PathBuf> {
        if segments.len() <= 1 {
            return segments;
        }
        let Some(bounds) = bounds else {
            return segments;
        };

        let Some((start, end)) = self.cluster_segment_range(&segments, bounds) else {
            return segments;
        };
        if start > end {
            self.record_cluster_key_segments_pruned(segments.len() as u64);
            return Vec::new();
        }

        let pruned = start + (segments.len() - end - 1);
        if pruned > 0 {
            self.record_cluster_key_segments_pruned(pruned as u64);
        }
        segments[start..=end].to_vec()
    }

    fn cluster_segment_range(
        &self,
        segments: &[PathBuf],
        bounds: &row_filter::ClusterKeyBounds,
    ) -> Option<(usize, usize)> {
        if !self.cluster_segment_zone_maps_are_monotonic(segments, bounds.column_index) {
            return None;
        }

        let start = match &bounds.lower {
            Some((value, inclusive)) => self.first_segment_with_max_at_least(
                segments,
                bounds.column_index,
                value,
                *inclusive,
            )?,
            None => 0,
        };
        let end = match &bounds.upper {
            Some((value, inclusive)) => {
                let end = self.last_segment_with_min_at_most(
                    segments,
                    bounds.column_index,
                    value,
                    *inclusive,
                )?;
                if end < 0 {
                    return Some((1, 0));
                }
                end as usize
            }
            None => segments.len().saturating_sub(1),
        };
        Some((start, end))
    }

    fn cluster_segment_zone_maps_are_monotonic(
        &self,
        segments: &[PathBuf],
        col_idx: usize,
    ) -> bool {
        let mut prev_min: Option<crate::types::scalar::ScalarValue> = None;
        let mut prev_max: Option<crate::types::scalar::ScalarValue> = None;

        for segment in segments {
            let Some(zm) = self.segment_cluster_zone_map(segment, col_idx) else {
                return false;
            };
            if let Some(prev) = &prev_min {
                if crate::storage::index::sort_key::compare_scalars_for_sort(&zm.min_value, prev)
                    == std::cmp::Ordering::Less
                {
                    return false;
                }
            }
            if let Some(prev) = &prev_max {
                if crate::storage::index::sort_key::compare_scalars_for_sort(&zm.max_value, prev)
                    == std::cmp::Ordering::Less
                {
                    return false;
                }
            }
            prev_min = Some(zm.min_value);
            prev_max = Some(zm.max_value);
        }

        true
    }

    fn first_segment_with_max_at_least(
        &self,
        segments: &[PathBuf],
        col_idx: usize,
        value: &crate::types::scalar::ScalarValue,
        inclusive: bool,
    ) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let zm = self.segment_cluster_zone_map(&segments[mid], col_idx)?;
            let cmp =
                crate::storage::index::sort_key::compare_scalars_for_sort(&zm.max_value, value);
            let before = if inclusive {
                cmp == std::cmp::Ordering::Less
            } else {
                cmp != std::cmp::Ordering::Greater
            };
            if before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo)
    }

    fn last_segment_with_min_at_most(
        &self,
        segments: &[PathBuf],
        col_idx: usize,
        value: &crate::types::scalar::ScalarValue,
        inclusive: bool,
    ) -> Option<isize> {
        let mut lo = 0usize;
        let mut hi = segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let zm = self.segment_cluster_zone_map(&segments[mid], col_idx)?;
            let cmp =
                crate::storage::index::sort_key::compare_scalars_for_sort(&zm.min_value, value);
            let at_or_before = if inclusive {
                cmp != std::cmp::Ordering::Greater
            } else {
                cmp == std::cmp::Ordering::Less
            };
            if at_or_before {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo as isize - 1)
    }

    fn segment_cluster_zone_map(
        &self,
        segment: &Path,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        self.runtime
            .segment_mgr()
            .cluster_zone_map_cached(segment, col_idx)
    }

    fn record_row_group_read(
        &mut self,
        reader: &crate::storage::segment::SegmentReader,
        rg_idx: usize,
        plan: &super::strategy::ScanPlan,
    ) {
        if let Some(s) = self.stats.as_deref_mut() {
            let entry = s.entry(self.spec.table_name.clone()).or_default();
            entry.row_groups_read += 1;
            let mut cols = plan.projection_cols.clone();
            for col in &plan.predicate_cols {
                if !cols.contains(col) {
                    cols.push(*col);
                }
            }
            let row_groups = reader.footer_row_groups_info();
            if let Some(rg) = row_groups.get(rg_idx) {
                for col_idx in cols {
                    if let Some(bytes) = rg.column_compressed_sizes.get(col_idx) {
                        entry.bytes_read_compressed += *bytes;
                    }
                }
            }
        }
    }

    fn record_rows_scanned(&mut self, rows: u64) {
        if rows == 0 {
            return;
        }
        if let Some(s) = self.stats.as_deref_mut() {
            s.entry(self.spec.table_name.clone())
                .or_default()
                .rows_scanned += rows;
        }
    }

    fn record_cluster_key_segments_pruned(&mut self, count: u64) {
        if count == 0 {
            return;
        }
        if let Some(s) = self.stats.as_deref_mut() {
            s.entry(self.spec.table_name.clone())
                .or_default()
                .segments_pruned_by_cluster_key += count;
        }
    }
}

trait BatchSink {
    fn push(&mut self, batch: RecordBatch) -> bool;
    fn set_limit(&mut self, _limit: Option<usize>) {}
}

struct LimitedBatchSink {
    batches: Vec<RecordBatch>,
    limit: Option<usize>,
    rows_emitted: usize,
}

impl LimitedBatchSink {
    fn new(limit: Option<usize>) -> Self {
        Self {
            batches: Vec::new(),
            limit,
            rows_emitted: 0,
        }
    }

    fn finish(self) -> Vec<RecordBatch> {
        self.batches
    }
}

impl BatchSink for LimitedBatchSink {
    fn set_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
    }

    fn push(&mut self, batch: RecordBatch) -> bool {
        if batch.num_rows() == 0 {
            return true;
        }

        let Some(limit) = self.limit else {
            self.batches.push(batch);
            return true;
        };

        if self.rows_emitted >= limit {
            return false;
        }

        let remaining = limit - self.rows_emitted;
        if batch.num_rows() > remaining {
            let trimmed = crate::sql::execution::operators::limit::trim_batch(&batch, 0, remaining);
            self.rows_emitted += trimmed.num_rows();
            if trimmed.num_rows() > 0 {
                self.batches.push(trimmed);
            }
            return false;
        }

        self.rows_emitted += batch.num_rows();
        self.batches.push(batch);
        self.rows_emitted < limit
    }
}

fn reverse_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col in &batch.columns {
        columns.push(reverse_column(col)?);
    }
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn reverse_column(col: &ColumnArray) -> Result<ColumnArray> {
    let mut out = ColumnArray::with_capacity(col.data_type(), col.len());
    for row_idx in (0..col.len()).rev() {
        out.push(&col.scalar(row_idx))?;
    }
    Ok(out)
}
