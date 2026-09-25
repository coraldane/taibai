use crate::common::error::Result;
use crate::storage::segment::ScanStats;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use super::count;
use super::memtable;
use super::partition;
use super::row_filter;
use super::runtime::ScanRuntime;
use super::spec::ScanSpec;

pub struct ScanExecutor<'a> {
    /// Owned clone of the caller's spec: the hybrid partial-residency leg
    /// appends its complementary range clause to this copy at run time.
    spec: ScanSpec,
    runtime: ScanRuntime,
    table_schema: crate::types::batch::Schema,
    table_meta: crate::storage::catalog::schema::TableMeta,
    db: &'a str,
    stats: Option<&'a mut HashMap<String, ScanStats>>,
    /// True when the metadata aggregate path produced the final result row.
    /// When the route engaged but the path declined, the caller must run the
    /// regular aggregate pipeline over the raw scan batches instead of
    /// treating them as the answer.
    metadata_answered: bool,
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
            spec: spec.clone(),
            runtime: ScanRuntime::from_engine(engine),
            table_schema,
            table_meta,
            db,
            stats: None,
            metadata_answered: false,
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
            spec: spec.clone(),
            runtime,
            table_schema,
            table_meta,
            db,
            stats: None,
            metadata_answered: false,
        }
    }

    /// Whether [`Self::execute`] produced the final aggregate row through the
    /// metadata fast path (as opposed to falling back to a raw scan because
    /// the path declined).
    pub fn metadata_answered(&self) -> bool {
        self.metadata_answered
    }

    pub fn with_stats(mut self, stats: &'a mut HashMap<String, ScanStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        let segment_mgr = self.runtime.segment_mgr().clone();
        let db = self.db.to_string();
        let table = self.spec.table_name.clone();
        segment_mgr.with_table_flush_read_lock(&db, &table, || self.execute_unlocked())
    }

    fn execute_unlocked(&mut self) -> Result<Vec<RecordBatch>> {
        if let Some(column) = self.spec.distinct_column.clone() {
            if let Some(batch) = self.execute_distinct_metadata(&column)? {
                self.metadata_answered = true;
                return Ok(vec![batch]);
            }
        }
        if let Some(aggs) = self.spec.simple_metadata_aggregates.clone() {
            return self.execute_simple_metadata_aggregate(&aggs);
        }
        if self.spec.is_count_star {
            return self.execute_count_star();
        }
        self.execute_scan()
    }

    fn execute_count_star(&mut self) -> Result<Vec<RecordBatch>> {
        if let Some((mirror_count, pk_idx, covers)) = memtable::hybrid_hot_mirror_count(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            self.db,
            &self.table_meta,
            &self.spec,
        ) {
            return self.execute_hybrid_count_star(mirror_count, pk_idx, covers);
        }

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
                &self.spec,
            )
            .unwrap_or_else(|| {
                memtable::count_memtable_rows(
                    self.runtime.memtable_manager(),
                    self.db,
                    &self.spec.table_name,
                    &self.spec,
                )
            });
            let result = count::build_count_star_result(count);
            return match result {
                crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
                _ => Ok(Vec::new()),
            };
        }
        let count = count::count_star(
            &self.spec,
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

    /// Hybrid partial-residency count: the mirror answered the rows above the
    /// coverage bound (`mirror_count`); count the complementary
    /// `partition_column <= covers` range through the regular segment and
    /// memtable paths. The added range clause keeps zone-map pruning effective
    /// (date-pure row groups above the bound prune to nothing).
    fn execute_hybrid_count_star(
        &mut self,
        mirror_count: u64,
        pk_idx: usize,
        covers: crate::types::scalar::ScalarValue,
    ) -> Result<Vec<RecordBatch>> {
        self.spec
            .range_predicates
            .push(crate::storage::partition::pruning::RangeClause {
                column_index: pk_idx,
                op: crate::storage::partition::pruning::RangeOp::Le,
                value: covers,
            });
        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        let count = if !table_dir.exists() {
            memtable::count_hot_memtable_rows(
                self.runtime.memtable_manager(),
                self.runtime.hot_store_manager(),
                self.db,
                &self.spec.table_name,
                &self.spec,
            )
            .unwrap_or_else(|| {
                memtable::count_memtable_rows(
                    self.runtime.memtable_manager(),
                    self.db,
                    &self.spec.table_name,
                    &self.spec,
                )
            })
        } else {
            count::count_star(
                &self.spec,
                &self.table_meta,
                &table_dir,
                self.runtime.segment_mgr(),
                self.runtime.memtable_manager(),
                self.db,
                self.stats.as_deref_mut(),
            )?
        };
        let total = mirror_count.saturating_add(count);
        let result = count::build_count_star_result(total);
        Ok(match result {
            crate::sql::execution::engine::ExecResult::Query { batches, .. } => batches,
            _ => Vec::new(),
        })
    }

    /// Metadata-driven fast path for simple aggregate queries (no GROUP BY,
    /// select list is only COUNT/MIN/MAX). Answers from row-group zone maps /
    /// footers without streaming the data.
    /// Single-column DISTINCT answered from row-group zone maps. Returns
    /// `None` when the fast path declines (caller falls back to the normal
    /// scan + DistinctExec pipeline).
    fn execute_distinct_metadata(&mut self, column: &str) -> Result<Option<RecordBatch>> {
        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        super::aggregate_meta::compute_distinct_column(
            &self.spec,
            &self.table_meta,
            &self.table_schema,
            &table_dir,
            self.runtime.segment_mgr(),
            self.runtime.memtable_manager(),
            self.runtime.hot_store_manager(),
            self.db,
            column,
        )
    }

    fn execute_simple_metadata_aggregate(
        &mut self,
        aggs: &[crate::sql::planner::AggregateExpr],
    ) -> Result<Vec<RecordBatch>> {
        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        let batch = if table_dir.exists() {
            super::aggregate_meta::compute_simple_aggregate(
                &self.spec,
                aggs,
                &self.table_meta,
                &self.table_schema,
                &table_dir,
                self.runtime.segment_mgr(),
                self.runtime.memtable_manager(),
                self.runtime.hot_store_manager(),
                self.db,
            )?
        } else {
            // No persisted segments; only memtable/hot store rows.
            super::aggregate_meta::compute_simple_aggregate(
                &self.spec,
                aggs,
                &self.table_meta,
                &self.table_schema,
                std::path::Path::new(""),
                self.runtime.segment_mgr(),
                self.runtime.memtable_manager(),
                self.runtime.hot_store_manager(),
                self.db,
            )?
        };
        let Some(batch) = batch else {
            // Sum over a column the metadata path cannot answer: return the
            // raw scan batches and let the caller run the regular aggregate
            // pipeline over them (`metadata_answered()` stays false).
            return self.execute_scan();
        };
        self.metadata_answered = true;
        Ok(vec![batch])
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

        let mut plan =
            super::strategy::ScanPlanner::new(&self.spec, &self.table_schema, &self.table_meta)
                .build();
        let mut scan_limit = plan.scan_limit;

        if let Some(batch) = self.try_resident_hot_scan(&plan)? {
            output.set_limit(scan_limit);
            output.push(batch);
            return Ok(());
        }

        // Hybrid partial-residency leg: the mirror answers every row above
        // its coverage bound through the inverted index; the segment and
        // memtable legs below run with the complementary `<= covers` range
        // clause so the two sides never overlap.
        if let Some((mirror_batch, pk_idx, covers)) = memtable::hybrid_hot_mirror_scan(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            self.db,
            &self.table_meta,
            &self.spec,
        ) {
            let mirror_rows = mirror_batch.num_rows() as u64;
            self.spec
                .range_predicates
                .push(crate::storage::partition::pruning::RangeClause {
                    column_index: pk_idx,
                    op: crate::storage::partition::pruning::RangeOp::Le,
                    value: covers.clone(),
                });
            self.spec.hot_mirror_bound = Some((pk_idx, covers));
            plan =
                super::strategy::ScanPlanner::new(&self.spec, &self.table_schema, &self.table_meta)
                    .build();
            scan_limit = plan.scan_limit;
            output.set_limit(scan_limit);
            if let Some(s) = self.stats.as_deref_mut() {
                s.entry(self.spec.table_name.clone())
                    .or_default()
                    .hot_mirror_rows += mirror_rows;
            }
            let mirror_batch = super::strategy::apply_ttl_filter_and_projection(
                mirror_batch,
                &self.table_meta,
                &plan.projection_cols,
                plan.strip_ttl_column,
            )?;
            if !output.push(mirror_batch) {
                return Ok(());
            }
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
                &self.spec,
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

            let use_segment_parallel = scan_limit.is_none()
                && ordered_limit.is_none()
                && self.stats.is_none()
                && filtered_segments.len() > 1
                && crate::common::config::query_limits().max_parallel_segments > 1;
            if use_segment_parallel {
                for batch in self.scan_segments_parallel(&filtered_segments, &mgr, &plan)? {
                    crate::common::query_cancel::check()?;
                    if !output.push(batch) {
                        return Ok(());
                    }
                }
            } else {
                for seg_path in &filtered_segments {
                    crate::common::query_cancel::check()?;
                    let meta = mgr.segment_meta(seg_path).ok().flatten();
                    let meta_candidate_indices = meta.as_ref().map(|meta| {
                        let pred = self.spec.to_row_group_predicate();
                        if !super::row_group::prune_segment_by_zone_map(
                            &pred,
                            &meta.segment_zone_maps,
                        ) {
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
                        let Some(prune_result) =
                            super::strategy::prune_segment(&self.spec, &reader)
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
                    } else if ordered_limit.is_none()
                        && scan_limit.is_some()
                        && self.spec.has_predicates()
                    {
                        super::strategy::rank_row_groups_for_limit(
                            &self.spec,
                            &reader,
                            &mut candidate_indices,
                        );
                    }

                    if scan_limit.is_some() || ordered_limit.is_some() {
                        for rg_idx in &candidate_indices {
                            crate::common::query_cancel::check()?;
                            self.record_row_group_read(&reader, *rg_idx, &plan);
                            let mut batch = match super::strategy::read_row_group_with_strategy(
                                &self.spec,
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

                    let limits = crate::common::config::query_limits();
                    let max_parallel = limits.max_parallel_row_groups.max(1);
                    let min_parallel = limits.parallel_scan_min_row_groups.max(2);
                    // Rayon lanes do not inherit the ambient cancellation
                    // context, so capture the flag here and let the closure
                    // stop starting new row-group reads once it is set.
                    let cancel_flag = crate::common::query_cancel::current_flag();
                    let read_one = |pos: usize, rg_idx: &usize| {
                        if cancel_flag
                            .as_ref()
                            .is_some_and(|flag| flag.load(Ordering::Acquire))
                        {
                            return None;
                        }
                        let _cpu = crate::common::work_limiter::acquire_cpu();
                        let _io = crate::common::work_limiter::acquire_io();
                        super::strategy::read_row_group_with_strategy(
                            &self.spec,
                            &plan,
                            &self.table_schema,
                            &reader,
                            *rg_idx,
                        )
                        .ok()
                        .filter(|batch| batch.num_rows() > 0)
                        .map(|batch| (pos, batch))
                    };
                    let mut rg_batches: Vec<(usize, RecordBatch)> = if max_parallel == 1
                        || candidate_indices.len() < min_parallel
                    {
                        candidate_indices
                            .iter()
                            .enumerate()
                            .filter_map(|(pos, rg_idx)| read_one(pos, rg_idx))
                            .collect()
                    } else {
                        let mut batches = Vec::new();
                        for (chunk_idx, chunk) in candidate_indices.chunks(max_parallel).enumerate()
                        {
                            crate::common::query_cancel::check()?;
                            let base = chunk_idx * max_parallel;
                            let mut chunk_batches: Vec<(usize, RecordBatch)> = chunk
                                .par_iter()
                                .enumerate()
                                .filter_map(|(pos, rg_idx)| read_one(base + pos, rg_idx))
                                .collect();
                            batches.append(&mut chunk_batches);
                        }
                        batches
                    };
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
            crate::common::query_cancel::check()?;
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
        let mut hot_spec = self.spec.clone();
        hot_spec.projection = Some(plan.projection_cols.clone());
        let Some(batch) = memtable::scan_resident_hot_store(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            self.db,
            &self.table_meta,
            &hot_spec,
        ) else {
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

    fn scan_segments_parallel(
        &self,
        paths: &[PathBuf],
        manager: &std::sync::Arc<SegmentManager>,
        plan: &super::strategy::ScanPlan,
    ) -> Result<Vec<RecordBatch>> {
        use rayon::prelude::*;
        let max_parallel = crate::common::config::query_limits()
            .max_parallel_segments
            .max(1);
        // Rayon lanes do not inherit the ambient cancellation context, so
        // capture the flag and let lane closures stop starting new reads.
        let cancel_flag = crate::common::query_cancel::current_flag();
        let mut output = Vec::new();
        for (chunk_index, chunk) in paths.chunks(max_parallel).enumerate() {
            crate::common::query_cancel::check()?;
            let base = chunk_index * max_parallel;
            let mut scanned = chunk
                .par_iter()
                .enumerate()
                .filter_map(|(position, path)| {
                    if cancel_flag
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::Acquire))
                    {
                        return None;
                    }
                    let reader = {
                        let _io = crate::common::work_limiter::acquire_io();
                        manager.open_reader_cached(path).ok()?
                    };
                    let candidates = if let Ok(Some(meta)) = manager.segment_meta(path) {
                        let predicate = self.spec.to_row_group_predicate();
                        if !super::row_group::prune_segment_by_zone_map(
                            &predicate,
                            &meta.segment_zone_maps,
                        ) {
                            return Some((base + position, Vec::new()));
                        }
                        super::row_group::prune_row_group_meta(
                            &meta.row_groups,
                            &predicate,
                            &super::row_group::build_bloom_check_list(&self.spec),
                            &super::row_group::build_in_check_list(&self.spec),
                            &super::row_group::build_set_check_list(&self.spec),
                            &super::row_group::build_set_in_check_list(&self.spec),
                        )
                        .candidate_indices
                    } else {
                        super::strategy::prune_segment(&self.spec, &reader)?.candidate_indices
                    };
                    // Row groups inside a segment decode in parallel as
                    // well; actual concurrency is bounded by the global
                    // cpu/io permits, not by rayon lane count.
                    let batches: Vec<_> = candidates
                        .into_par_iter()
                        .filter_map(|row_group| {
                            if cancel_flag
                                .as_ref()
                                .is_some_and(|flag| flag.load(Ordering::Acquire))
                            {
                                return None;
                            }
                            let batch_result = {
                                let _cpu = crate::common::work_limiter::acquire_cpu();
                                let _io = crate::common::work_limiter::acquire_io();
                                super::strategy::read_row_group_with_strategy(
                                    &self.spec,
                                    plan,
                                    &self.table_schema,
                                    &reader,
                                    row_group,
                                )
                            };
                            let batch = batch_result.ok()?;
                            let batch = super::strategy::apply_ttl_filter_and_projection(
                                batch,
                                &self.table_meta,
                                &plan.projection_cols,
                                plan.strip_ttl_column,
                            )
                            .ok()?;
                            (batch.num_rows() > 0).then_some(batch)
                        })
                        .collect();
                    Some((base + position, batches))
                })
                .collect::<Vec<_>>();
            scanned.sort_by_key(|(position, _)| *position);
            for (_, mut batches) in scanned {
                output.append(&mut batches);
            }
        }
        Ok(output)
    }

    fn try_resident_hot_count(&self) -> Result<Option<u64>> {
        Ok(memtable::count_resident_hot_store(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            self.db,
            &self.table_meta,
            &self.spec,
        ))
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
        let key_cols = bounds.key_column_indices();
        if !self.cluster_segment_tuple_zone_maps_are_monotonic(segments, &key_cols) {
            return None;
        }

        // Per-column zone-map extrema underestimate/overestimate the true
        // composite tuple, so the tuple searches below prune conservatively:
        // a segment is skipped only when it provably cannot contribute rows.
        let lower_tuple = search_tuple_for_bounds(&bounds.prefix_eq, bounds.lower.as_ref());
        let upper_tuple = search_tuple_for_bounds(&bounds.prefix_eq, bounds.upper.as_ref());

        let start = match (&bounds.lower, &bounds.upper) {
            (Some(_), _) => {
                self.first_segment_with_max_tuple_at_least(segments, &key_cols, &lower_tuple)?
            }
            // No lower bound: only the equality prefix may narrow the start
            // (compared without the range value — every prefix row still
            // qualifies). With an empty prefix, scanning starts at 0.
            (None, Some(_)) if !bounds.prefix_eq.is_empty() => {
                let prefix_tuple: Vec<crate::types::scalar::ScalarValue> = bounds
                    .prefix_eq
                    .iter()
                    .map(|(_, value)| value.clone())
                    .collect();
                let prefix_cols = &key_cols[..bounds.prefix_eq.len()];
                self.first_segment_with_max_tuple_at_least(segments, prefix_cols, &prefix_tuple)?
            }
            _ => 0,
        };
        let end = match &bounds.upper {
            Some(_) => {
                let end =
                    self.last_segment_with_min_tuple_at_most(segments, &key_cols, &upper_tuple)?;
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

    /// Binary search for the first segment whose composite max tuple (per
    /// column zone-map maxima) is not less than the search tuple.
    fn first_segment_with_max_tuple_at_least(
        &self,
        segments: &[PathBuf],
        key_cols: &[usize],
        tuple: &[crate::types::scalar::ScalarValue],
    ) -> Option<usize> {
        let mut lo = 0usize;
        let mut hi = segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = self.compare_segment_zone_tuple(&segments[mid], key_cols, tuple, true)?;
            if cmp == std::cmp::Ordering::Less {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo)
    }

    /// Binary search for the last segment whose composite min tuple (per
    /// column zone-map minima) is not greater than the search tuple.
    fn last_segment_with_min_tuple_at_most(
        &self,
        segments: &[PathBuf],
        key_cols: &[usize],
        tuple: &[crate::types::scalar::ScalarValue],
    ) -> Option<isize> {
        let mut lo = 0usize;
        let mut hi = segments.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = self.compare_segment_zone_tuple(&segments[mid], key_cols, tuple, false)?;
            if cmp != std::cmp::Ordering::Greater {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo as isize - 1)
    }

    /// Lexicographic comparison of a segment's zone-map tuple (max side or
    /// min side) against the search tuple. Returns `None` when a zone map is
    /// missing.
    fn compare_segment_zone_tuple(
        &self,
        segment: &Path,
        key_cols: &[usize],
        tuple: &[crate::types::scalar::ScalarValue],
        use_max: bool,
    ) -> Option<std::cmp::Ordering> {
        for (&col_idx, value) in key_cols.iter().zip(tuple.iter()) {
            let zm = self.segment_cluster_zone_map(segment, col_idx)?;
            let side = if use_max {
                &zm.max_value
            } else {
                &zm.min_value
            };
            let cmp = crate::storage::index::sort_key::compare_scalars_for_sort(side, value);
            if cmp != std::cmp::Ordering::Equal {
                return Some(cmp);
            }
        }
        Some(std::cmp::Ordering::Equal)
    }

    /// Consecutive segments must be non-decreasing in the composite key
    /// tuple (checked on both the min and max zone tuples).
    fn cluster_segment_tuple_zone_maps_are_monotonic(
        &self,
        segments: &[PathBuf],
        key_cols: &[usize],
    ) -> bool {
        if segments.len() <= 1 {
            return true;
        }
        let mut prev_min: Option<Vec<crate::types::scalar::ScalarValue>> = None;
        let mut prev_max: Option<Vec<crate::types::scalar::ScalarValue>> = None;

        for segment in segments {
            let mut min_tuple = Vec::with_capacity(key_cols.len());
            let mut max_tuple = Vec::with_capacity(key_cols.len());
            for &col_idx in key_cols {
                let Some(zm) = self.segment_cluster_zone_map(segment, col_idx) else {
                    return false;
                };
                min_tuple.push(zm.min_value);
                max_tuple.push(zm.max_value);
            }
            if let Some(prev) = &prev_min {
                if compare_scalar_tuples(&min_tuple, prev) == std::cmp::Ordering::Less {
                    return false;
                }
            }
            if let Some(prev) = &prev_max {
                if compare_scalar_tuples(&max_tuple, prev) == std::cmp::Ordering::Less {
                    return false;
                }
            }
            prev_min = Some(min_tuple);
            prev_max = Some(max_tuple);
        }

        true
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

/// Prefix equality values plus the range bound value (if any).
fn search_tuple_for_bounds(
    prefix_eq: &[(usize, crate::types::scalar::ScalarValue)],
    bound: Option<&(crate::types::scalar::ScalarValue, bool)>,
) -> Vec<crate::types::scalar::ScalarValue> {
    let mut tuple: Vec<crate::types::scalar::ScalarValue> =
        prefix_eq.iter().map(|(_, value)| value.clone()).collect();
    if let Some((value, _inclusive)) = bound {
        tuple.push(value.clone());
    }
    tuple
}

fn compare_scalar_tuples(
    left: &[crate::types::scalar::ScalarValue],
    right: &[crate::types::scalar::ScalarValue],
) -> std::cmp::Ordering {
    for (l, r) in left.iter().zip(right.iter()) {
        let cmp = crate::storage::index::sort_key::compare_scalars_for_sort(l, r);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
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
