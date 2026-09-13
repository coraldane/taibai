use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::error::Result;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::segment::SegmentReader;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

use super::memtable;
use super::partition;
use super::row_group::RowGroupPruneResult;
use super::runtime::ScanRuntime;
use super::spec::{InPredicate, ScanSpec};
use super::strategy::{self, ScanPlan};

pub struct ScanCursor {
    spec: ScanSpec,
    runtime: ScanRuntime,
    table_schema: Schema,
    table_meta: TableMeta,
    db: String,
    plan: ScanPlan,
    segments: Vec<PathBuf>,
    initialized: bool,
    segment_pos: usize,
    current_reader: Option<Arc<SegmentReader>>,
    current_candidates: Vec<usize>,
    candidate_pos: usize,
    remaining_limit: Option<usize>,
    memtable_batches: Vec<RecordBatch>,
    memtable_pos: usize,
    /// Decoded row groups awaiting emission, in candidate order. Filled a
    /// window at a time by a parallel decode fan-out so cursor-served scans
    /// (streaming reads, aggregates feeding HashAggregateExec) are not bound
    /// to one serial fetch/decode lane.
    prefetch: VecDeque<RecordBatch>,
}

impl ScanCursor {
    pub fn try_new(
        spec: ScanSpec,
        runtime: ScanRuntime,
        table_schema: Schema,
        table_meta: TableMeta,
        db: String,
    ) -> Result<Option<Self>> {
        if !is_streamable(&spec) {
            return Ok(None);
        }

        let plan = strategy::ScanPlanner::new(&spec, &table_schema, &table_meta).build();
        let remaining_limit = plan.scan_limit;

        Ok(Some(Self {
            spec,
            runtime,
            table_schema,
            table_meta,
            db,
            plan,
            segments: Vec::new(),
            initialized: false,
            segment_pos: 0,
            current_reader: None,
            current_candidates: Vec::new(),
            candidate_pos: 0,
            remaining_limit,
            memtable_batches: Vec::new(),
            memtable_pos: 0,
            prefetch: VecDeque::new(),
        }))
    }

    pub fn add_in_filter(
        &mut self,
        column_index: usize,
        values: Vec<crate::types::scalar::ScalarValue>,
    ) -> bool {
        if self.initialized || values.is_empty() {
            return false;
        }
        self.spec.in_predicates.push(InPredicate {
            column_index,
            values,
        });
        self.plan =
            strategy::ScanPlanner::new(&self.spec, &self.table_schema, &self.table_meta).build();
        self.remaining_limit = self.plan.scan_limit;
        true
    }

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.initialized {
            self.initialize()?;
        }

        if matches!(self.remaining_limit, Some(0)) {
            return Ok(None);
        }

        while self.segment_pos < self.segments.len() {
            if self.current_reader.is_none() {
                self.open_current_segment();
                if self.current_reader.is_none() {
                    self.segment_pos += 1;
                    continue;
                }
            }

            let reader = self.current_reader.as_ref().unwrap().clone();
            while self.candidate_pos < self.current_candidates.len()
                || !self.prefetch.is_empty()
            {
                if self.prefetch.is_empty() {
                    self.fill_prefetch(&reader);
                }
                let Some(batch) = self.prefetch.pop_front() else {
                    continue;
                };
                if let Some(batch) = self.limit_batch(batch) {
                    return Ok(Some(batch));
                }
            }

            self.current_reader = None;
            self.current_candidates.clear();
            self.candidate_pos = 0;
            self.segment_pos += 1;
        }

        while self.memtable_pos < self.memtable_batches.len() {
            let batch = self.memtable_batches[self.memtable_pos].clone();
            self.memtable_pos += 1;
            let batch = strategy::apply_ttl_filter_and_projection(
                batch,
                &self.table_meta,
                &self.plan.projection_cols,
                self.plan.strip_ttl_column,
            )?;
            if let Some(batch) = self.limit_batch(batch) {
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }

    /// Decode the next `window` candidate row groups of the current segment
    /// in parallel. `rayon` collect keeps candidate order, so ordered scans
    /// and LIMIT trimming behave exactly like the serial walk. Decode
    /// concurrency is bounded by the global cpu/io work permits.
    fn fill_prefetch(&mut self, reader: &Arc<SegmentReader>) {
        use rayon::prelude::*;
        let start = self.candidate_pos;
        let window = self.prefetch_window();
        let end = (start + window).min(self.current_candidates.len());
        self.candidate_pos = end;
        let rgs = self.current_candidates[start..end].to_vec();
        let spec = self.spec.clone();
        let plan = self.plan.clone();
        let table_schema = self.table_schema.clone();
        let reader = Arc::clone(reader);
        let batches: Vec<_> = rgs
            .into_par_iter()
            .filter_map(|rg_idx| {
                let _cpu = crate::common::work_limiter::acquire_cpu();
                let _io = crate::common::work_limiter::acquire_io();
                strategy::read_row_group_with_strategy(
                    &spec,
                    &plan,
                    &table_schema,
                    &reader,
                    rg_idx,
                )
                .ok()
            })
            .collect();
        for batch in batches {
            let filtered = strategy::apply_ttl_filter_and_projection(
                batch,
                &self.table_meta,
                &self.plan.projection_cols,
                self.plan.strip_ttl_column,
            );
            match filtered {
                Ok(batch) if batch.num_rows() > 0 => self.prefetch.push_back(batch),
                _ => {}
            }
        }
    }

    fn prefetch_window(&self) -> usize {
        let base = crate::common::config::query_limits()
            .max_parallel_row_groups
            .clamp(4, 16);
        // Bounded overshoot for LIMIT queries: decoding a small window keeps
        // early termination cheap.
        if self.remaining_limit.is_some() {
            base.min(4)
        } else {
            base
        }
    }

    fn open_current_segment(&mut self) {
        let segment = &self.segments[self.segment_pos];
        let meta_candidates = self
            .runtime
            .segment_mgr()
            .segment_meta(segment)
            .ok()
            .flatten()
            .map(|meta| {
                let pred = self.spec.to_row_group_predicate();
                if !super::row_group::prune_segment_by_zone_map(&pred, &meta.segment_zone_maps) {
                    return Vec::new();
                }
                super::row_group::prune_row_group_meta(
                    &meta.row_groups,
                    &pred,
                    &super::row_group::build_bloom_check_list(&self.spec),
                    &super::row_group::build_in_check_list(&self.spec),
                    &super::row_group::build_set_check_list(&self.spec),
                    &super::row_group::build_set_in_check_list(&self.spec),
                )
                .candidate_indices
            });
        if meta_candidates
            .as_ref()
            .is_some_and(|candidates| candidates.is_empty())
        {
            self.current_reader = None;
            return;
        }
        let reader = match self.runtime.segment_mgr().open_reader_cached(segment) {
            Ok(r) => r,
            Err(_) => {
                self.current_reader = None;
                return;
            }
        };
        let candidates = meta_candidates.unwrap_or_else(|| {
            strategy::prune_segment(&self.spec, &reader)
                .map(|r: RowGroupPruneResult| r.candidate_indices)
                .unwrap_or_default()
        });
        self.current_candidates = candidates;
        self.candidate_pos = 0;
        self.current_reader = Some(reader);
    }

    fn initialize(&mut self) -> Result<()> {
        let mut hot_spec = self.spec.clone();
        hot_spec.projection = Some(self.plan.projection_cols.clone());
        if let Some(batch) = memtable::scan_resident_hot_store(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            &self.db,
            &self.table_meta,
            &hot_spec,
        ) {
            self.segments.clear();
            self.memtable_batches = vec![batch];
            self.initialized = true;
            return Ok(());
        }

        let table_dir = self
            .runtime
            .data_dir()
            .join(&self.db)
            .join(&self.spec.table_name);
        self.segments = if table_dir.exists() {
            let all_segments = SegmentManager::discover_segments(&table_dir)?;
            partition::prune_segments_by_partition(all_segments, &self.spec, Some(&self.table_meta))
        } else {
            Vec::new()
        };

        let mut memtable_spec = self.spec.clone();
        memtable_spec.projection = Some(self.plan.projection_cols.clone());
        self.memtable_batches = memtable::scan_memtables(
            self.runtime.memtable_manager(),
            self.runtime.hot_store_manager(),
            &self.db,
            &self.spec.table_name,
            &memtable_spec,
        );
        self.initialized = true;
        Ok(())
    }

    fn limit_batch(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        if batch.num_rows() == 0 {
            return None;
        }

        let Some(remaining) = self.remaining_limit else {
            return Some(batch);
        };

        if remaining == 0 {
            return None;
        }

        if batch.num_rows() > remaining {
            self.remaining_limit = Some(0);
            let trimmed = crate::sql::execution::operators::limit::trim_batch(&batch, 0, remaining);
            return (trimmed.num_rows() > 0).then_some(trimmed);
        }

        self.remaining_limit = Some(remaining - batch.num_rows());
        Some(batch)
    }
}

fn is_streamable(spec: &ScanSpec) -> bool {
    !spec.is_count_star
        && !spec.has_aggregate
        && !spec.has_order_by
        && !spec.has_group_by
        && !spec.has_join
}
