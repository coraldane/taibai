use crate::common::error::Result;
use crate::storage::segment::ScanStats;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use super::cluster_prune;
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
        // Metadata fast paths (DISTINCT / simple aggregates) fold zone maps
        // and footers directly and never apply the read-time TTL filter, so
        // a table with an active TTL must take the regular scan path.
        let ttl_active = self.table_meta.ttl.is_some();
        if let Some(column) = self.spec.distinct_column.clone() {
            if !ttl_active {
                if let Some(batch) = self.execute_distinct_metadata(&column)? {
                    self.metadata_answered = true;
                    return Ok(vec![batch]);
                }
            }
        }
        if let Some(aggs) = self.spec.simple_metadata_aggregates.clone() {
            if !ttl_active {
                return self.execute_simple_metadata_aggregate(&aggs);
            }
        }
        if self.spec.is_count_star {
            return self.execute_count_star();
        }
        self.execute_scan()
    }

    /// Wrap a row count as the single-batch COUNT(*) reply.
    fn count_star_batches(count: u64) -> Result<Vec<RecordBatch>> {
        let result = count::build_count_star_result(count);
        Ok(match result {
            crate::sql::execution::engine::ExecResult::Query { batches, .. } => batches,
            _ => Vec::new(),
        })
    }

    /// Rows visible in live memtables, preferring the hot-mirror-aware count
    /// (`None` means the hot store cannot prove coverage for this shape, so
    /// the plain memtable count answers).
    fn live_memtable_rows(&self) -> u64 {
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
    }

    /// COUNT(*) resolves through a chain of increasingly general strategies;
    /// the first that applies answers. Each is guarded by its own
    /// eligibility, so a later strategy never runs when an earlier one
    /// engaged.
    fn execute_count_star(&mut self) -> Result<Vec<RecordBatch>> {
        // 1. Hybrid partial residency: the hot mirror covers rows above its
        //    coverage bound; the complementary range is counted through
        //    segments and memtables.
        if let Some((mirror_count, pk_idx, covers)) = memtable::hybrid_hot_mirror_count(
            self.runtime.hot_store_manager(),
            self.runtime.memtable_manager(),
            self.db,
            &self.table_meta,
            &self.spec,
        ) {
            return self.execute_hybrid_count_star(mirror_count, pk_idx, covers);
        }

        // 2. Fully resident: the hot store answers alone.
        if let Some(count) = self.try_resident_hot_count()? {
            return Self::count_star_batches(count);
        }

        // 3. Active TTL: visibility is row-dependent, so COUNT(*) must scan
        //    (only) the TTL column and count the rows that survive it.
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
            return Self::count_star_batches(count);
        }

        // 4. No segment directory yet: only live memtable rows exist.
        let table_dir = self
            .runtime
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        if !table_dir.exists() {
            return Self::count_star_batches(self.live_memtable_rows());
        }

        // 5. General: segments plus live memtables.
        let count = count::count_star(
            &self.spec,
            &self.table_meta,
            &table_dir,
            self.runtime.segment_mgr(),
            self.runtime.memtable_manager(),
            self.db,
            self.stats.as_deref_mut(),
        )?;
        Self::count_star_batches(count)
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
            self.live_memtable_rows()
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
        Self::count_star_batches(total)
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
            super::aggregate_meta::compute_folded_aggregate(
                &self.spec,
                &[],
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
            super::aggregate_meta::compute_folded_aggregate(
                &self.spec,
                &[],
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
        if output.over_cap {
            return Err(crate::common::error::TaibaiError::ResourceBusy(format!(
                "scan input materialization exceeded the query memory budget ({} > {} bytes); raise query_memory_limit_mb",
                output.bytes, output.byte_cap
            )));
        }
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
                    // The gateway aligns the catalog-space spec onto the
                    // footer's column order before consulting its metadata;
                    // `Unaligned` (and an absent snapshot) fall back to
                    // reader-side pruning below.
                    let meta_candidate_indices = meta.as_ref().and_then(|meta| {
                        match super::row_filter::meta_prune_row_groups(meta, &self.spec) {
                            Ok(super::row_filter::MetaPrune::Candidates(indices)) => Some(indices),
                            Ok(super::row_filter::MetaPrune::Unaligned) | Err(_) => None,
                        }
                    });
                    if meta_candidate_indices
                        .as_ref()
                        .is_some_and(|candidates| candidates.is_empty())
                    {
                        continue;
                    }

                    let reader = match mgr.open_scan_reader(seg_path, self.spec.reader_txn_id) {
                        Ok(r) => r,
                        Err(error) => {
                            // A concurrent compaction may legitimately remove
                            // a segment mid-scan; anything else (IO error,
                            // corrupt footer) must fail the query instead of
                            // silently skipping the whole segment's rows.
                            if !seg_path.exists() {
                                tracing::warn!(
                                    segment = %seg_path.display(),
                                    "scan: segment vanished mid-scan; skipping"
                                );
                                continue;
                            }
                            return Err(error);
                        }
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
                                Err(error) => {
                                    // Decode failures propagate: silently
                                    // dropping a row group would return
                                    // partial results as success.
                                    return Err(error);
                                }
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
                    type ReadOutcome = std::result::Result<
                        (usize, RecordBatch),
                        crate::common::error::TaibaiError,
                    >;
                    let read_one = |pos: usize, rg_idx: &usize| -> Option<ReadOutcome> {
                        if cancel_flag
                            .as_ref()
                            .is_some_and(|flag| flag.load(Ordering::Acquire))
                        {
                            return None;
                        }
                        let _cpu = crate::common::work_limiter::acquire_cpu();
                        let _io = crate::common::work_limiter::acquire_io();
                        match super::strategy::read_row_group_with_strategy(
                            &self.spec,
                            &plan,
                            &self.table_schema,
                            &reader,
                            *rg_idx,
                        ) {
                            // Decode failures must fail the query: silently
                            // dropping a row group returned partial results
                            // as success.
                            Ok(batch) if batch.num_rows() > 0 => Some(Ok((pos, batch))),
                            Ok(_) => None,
                            Err(error) => Some(Err(error)),
                        }
                    };
                    let rg_outcomes: Vec<ReadOutcome> = if max_parallel == 1
                        || candidate_indices.len() < min_parallel
                    {
                        // Serial fallback: the parallel path checks
                        // cancellation at every chunk boundary; without the
                        // same check here, a KILL QUERY mid-scan looked like
                        // end-of-stream and returned a partial result.
                        let mut serial = Vec::new();
                        for (pos, rg_idx) in candidate_indices.iter().enumerate() {
                            crate::common::query_cancel::check()?;
                            if let Some(outcome) = read_one(pos, rg_idx) {
                                serial.push(outcome);
                            }
                        }
                        serial
                    } else {
                        let mut batches = Vec::new();
                        for (chunk_idx, chunk) in candidate_indices.chunks(max_parallel).enumerate()
                        {
                            crate::common::query_cancel::check()?;
                            let base = chunk_idx * max_parallel;
                            let mut chunk_batches: Vec<ReadOutcome> = chunk
                                .par_iter()
                                .enumerate()
                                .filter_map(|(pos, rg_idx)| read_one(base + pos, rg_idx))
                                .collect();
                            batches.append(&mut chunk_batches);
                        }
                        batches
                    };
                    let mut rg_batches: Vec<(usize, RecordBatch)> =
                        Vec::with_capacity(rg_outcomes.len());
                    for outcome in rg_outcomes {
                        rg_batches.push(outcome?);
                    }
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
        )? {
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
        // Budget is enforced DURING collection, not after: the sink at the
        // call site only noticed the overrun once every segment had already
        // been decoded into memory.
        let byte_cap = {
            let mb = crate::common::config::query_limits().query_memory_limit_mb;
            (mb as u64).saturating_mul(1024 * 1024)
        };
        let mut output_bytes = 0u64;
        let mut output = Vec::new();
        for (chunk_index, chunk) in paths.chunks(max_parallel).enumerate() {
            crate::common::query_cancel::check()?;
            let base = chunk_index * max_parallel;
            type ScanOutcome =
                std::result::Result<(usize, Vec<RecordBatch>), crate::common::error::TaibaiError>;
            let mut scanned: Vec<ScanOutcome> = chunk
                .par_iter()
                .enumerate()
                .filter_map(|(position, path)| -> Option<ScanOutcome> {
                    if cancel_flag
                        .as_ref()
                        .is_some_and(|flag| flag.load(Ordering::Acquire))
                    {
                        return None;
                    }
                    let reader = {
                        let _io = crate::common::work_limiter::acquire_io();
                        match manager.open_scan_reader(path, self.spec.reader_txn_id) {
                            Ok(reader) => reader,
                            // A segment removed by concurrent compaction is
                            // skipped; any other open failure fails the query.
                            Err(error) => {
                                return if path.exists() {
                                    Some(Err(error))
                                } else {
                                    tracing::warn!(
                                        segment = %path.display(),
                                        "parallel scan: segment vanished mid-scan; skipping"
                                    );
                                    None
                                };
                            }
                        }
                    };
                    let candidates = match manager.segment_meta(path) {
                        Ok(Some(meta)) => {
                            // Gateway prune: catalog-space spec aligned onto
                            // the footer's own column order first. An empty
                            // result means the zone maps pruned the whole
                            // segment; `Unaligned` falls back to the reader.
                            match super::row_filter::meta_prune_row_groups(&meta, &self.spec) {
                                Ok(super::row_filter::MetaPrune::Candidates(indices))
                                    if indices.is_empty() =>
                                {
                                    return Some(Ok((base + position, Vec::new())));
                                }
                                Ok(super::row_filter::MetaPrune::Candidates(indices)) => indices,
                                Ok(super::row_filter::MetaPrune::Unaligned) | Err(_) => {
                                    super::strategy::prune_segment(&self.spec, &reader)?
                                        .candidate_indices
                                }
                            }
                        }
                        _ => super::strategy::prune_segment(&self.spec, &reader)?.candidate_indices,
                    };
                    // Row groups inside a segment decode in parallel as
                    // well; actual concurrency is bounded by the global
                    // cpu/io permits, not by rayon lane count. Decode and
                    // TTL failures fail the query instead of silently
                    // dropping rows.
                    let batch_results: Vec<
                        std::result::Result<Option<RecordBatch>, crate::common::error::TaibaiError>,
                    > = candidates
                        .into_par_iter()
                        .map(|row_group| {
                            if cancel_flag
                                .as_ref()
                                .is_some_and(|flag| flag.load(Ordering::Acquire))
                            {
                                return Ok(None);
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
                            let batch = batch_result?;
                            let batch = super::strategy::apply_ttl_filter_and_projection(
                                batch,
                                &self.table_meta,
                                &plan.projection_cols,
                                plan.strip_ttl_column,
                            )?;
                            Ok((batch.num_rows() > 0).then_some(batch))
                        })
                        .collect();
                    let mut batches = Vec::with_capacity(batch_results.len());
                    for result in batch_results {
                        match result {
                            Ok(Some(batch)) => batches.push(batch),
                            Ok(None) => {}
                            Err(error) => return Some(Err(error)),
                        }
                    }
                    Some(Ok((base + position, batches)))
                })
                .collect();
            scanned.sort_by_key(|outcome| {
                outcome
                    .as_ref()
                    .map_or(usize::MAX, |(position, _)| *position)
            });
            for outcome in scanned {
                let (_, mut batches) = outcome?;
                for batch in batches.drain(..) {
                    output_bytes =
                        output_bytes.saturating_add(batch.estimated_size_bytes() as u64);
                    if output_bytes > byte_cap {
                        return Err(crate::common::error::TaibaiError::ResourceBusy(format!(
                            "parallel scan materialization exceeded the query memory budget ({} > {} bytes); raise query_memory_limit_mb",
                            output_bytes, byte_cap
                        )));
                    }
                    output.push(batch);
                }
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
        segments.len() <= 1
            || cluster_prune::cluster_segment_zone_maps_are_monotonic(
                segments,
                self.runtime.segment_mgr(),
                &self.spec.table_schema,
                col_idx,
            )
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

        let Some((start, end)) = cluster_prune::cluster_segment_range(
            &segments,
            self.runtime.segment_mgr(),
            &self.spec.table_schema,
            bounds,
        )
        else {
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
            // Borrowed footer access: the deep-cloning variant materialized
            // every row group's metadata (bloom filters + value sets) just to
            // read one entry's column sizes.
            let row_groups = reader.footer_row_groups();
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
    /// Bytes accumulated by the sink. The streaming cursor covers nearly
    /// every query shape now; what still lands here (COUNT fast paths,
    /// cursor-declined shapes) must not materialize past the query budget
    /// — 0 disables the cap.
    bytes: u64,
    byte_cap: u64,
    over_cap: bool,
}

impl LimitedBatchSink {
    fn new(limit: Option<usize>) -> Self {
        let byte_cap = {
            let mb = crate::common::config::query_limits().query_memory_limit_mb;
            if mb == 0 {
                u64::MAX
            } else {
                mb.saturating_mul(1024 * 1024) as u64
            }
        };
        Self {
            batches: Vec::new(),
            limit,
            rows_emitted: 0,
            bytes: 0,
            byte_cap,
            over_cap: false,
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

        self.bytes = self
            .bytes
            .saturating_add(batch.estimated_size_bytes() as u64);
        if self.bytes > self.byte_cap {
            // Remember and keep collecting would only deepen the overrun;
            // the executor turns this into a clean budget error.
            self.over_cap = true;
            return false;
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
    // One bulk gather instead of a per-row scalar() + push() round-trip,
    // which cloned every VarChar cell of the row group being reversed.
    // View shapes (Selected/Rle/Dictionary) have no arm in gather_into, so
    // they are flattened once first.
    let flattened;
    let col = match col {
        ColumnArray::Selected { .. } | ColumnArray::Rle { .. } | ColumnArray::Dictionary { .. } => {
            flattened = col.flattened();
            &flattened
        }
        other => other,
    };
    let rows: Vec<u32> = (0..col.len() as u32).rev().collect();
    Ok(col.take(&rows))
}
