use crate::common::error::Result;
use crate::storage::segment::ScanStats;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use super::cluster_prune;
use super::fast_path::FastPathOutcome;
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
    /// The fast-path chain's query-level answer (`FinalAnswer` outcome):
    /// `execute_scan_into` stashes it here and `execute_scan` unwraps it as
    /// the complete result, bypassing the sink exactly as the pre-chain
    /// routing did.
    final_answer: Option<RecordBatch>,
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
            final_answer: None,
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
            final_answer: None,
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
        segment_mgr.with_table_flush_read_lock(&db, &table, || self.execute_scan())
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
        if let Some(batch) = self.final_answer.take() {
            return Ok(vec![batch]);
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

        if self.try_fast_paths(&plan, output)? {
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
                    // Shared candidate enumeration: sidecar meta prune first
                    // (skips the segment without opening it when nothing
                    // survives), reader prune on fallback. A concurrent
                    // compaction may legitimately remove a segment mid-scan;
                    // anything else must fail the query instead of silently
                    // skipping the whole segment's rows.
                    let opened = match super::candidates::open_segment_with_candidates(
                        &mgr, seg_path, &self.spec,
                    ) {
                        Ok(Some(opened)) => opened,
                        Ok(None) => continue,
                        Err(error) => {
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
                    let reader = opened.reader;
                    let candidate_indices = opened.indices;

                    if let Some(s) = self.stats.as_deref_mut() {
                        s.entry(self.spec.table_name.clone())
                            .or_default()
                            .segments_opened += 1;
                    }

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
                        // No admission waits inside rayon lanes: concurrency
                        // is bounded by pool size (see
                        // docs/design/resource-admission-and-backpressure.md).
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

    /// The unified fast-path chain. Order mirrors the historic routing:
    /// DISTINCT zone-map answer, metadata-folded aggregate, resident
    /// hot-store shortcut; anything declined reaches the hybrid mirror leg
    /// and the general scan below.
    ///
    /// The universal TTL gate runs here once, before any node: every chain
    /// node declines TTL tables (truncation-style gathers would collect at
    /// the pushed LIMIT before TTL filtering runs and return fewer live rows
    /// than exist), so a new node cannot forget the gate. The nodes keep
    /// their internal gates as defense in depth for direct callers.
    ///
    /// Returns `true` when the chain answered and the general legs must not
    /// run. Stats recording uses direct field access on purpose: the context
    /// borrows `spec`/`table_meta`/`table_schema`/`runtime` immutably, so a
    /// `&mut self` helper would fight the borrow checker.
    fn try_fast_paths<S: BatchSink>(
        &mut self,
        plan: &super::strategy::ScanPlan,
        output: &mut S,
    ) -> Result<bool> {
        if super::fast_path::ttl_blocks(&self.spec, &self.table_meta) {
            self.record_fast_path_decline(super::fast_path::TTL_GATE_NODE);
            return Ok(false);
        }
        let ctx = super::fast_path::FastPathContext::from_runtime(
            &self.spec,
            &self.table_meta,
            &self.table_schema,
            self.db,
            &self.runtime,
        );
        let distinct = super::fast_path::DistinctMetadataFastPath;
        let folded = self
            .spec
            .metadata_fold
            .as_ref()
            .map(super::fast_path::FoldedAggregateFastPath::from_fold);
        let resident = super::fast_path::ResidentHotFastPath {
            projection_cols: &plan.projection_cols,
            strip_ttl_column: plan.strip_ttl_column,
        };
        let mut nodes: Vec<&dyn super::fast_path::ScanFastPath> = vec![&distinct];
        if let Some(node) = folded.as_ref() {
            nodes.push(node);
        }
        nodes.push(&resident);

        let mut declined_nodes: Vec<&'static str> = Vec::new();
        for node in nodes {
            match node.try_answer(&ctx)? {
                FastPathOutcome::FinalAnswer(batch) => {
                    self.final_answer = Some(batch);
                    self.metadata_answered = true;
                    if let Some(s) = self.stats.as_deref_mut() {
                        s.entry(self.spec.table_name.clone())
                            .or_default()
                            .fast_path_answered = Some(node.name());
                    }
                    return Ok(true);
                }
                FastPathOutcome::ScanRows(batch) => {
                    output.set_limit(plan.scan_limit);
                    output.push(batch);
                    if let Some(s) = self.stats.as_deref_mut() {
                        s.entry(self.spec.table_name.clone())
                            .or_default()
                            .fast_path_answered = Some(node.name());
                    }
                    return Ok(true);
                }
                FastPathOutcome::Declined(decline) => {
                    // A shape the node does not apply to was never a
                    // fast-path attempt; only record real declines.
                    if decline.reason != super::fast_path::DeclineReason::NotApplicable {
                        declined_nodes.push(decline.node);
                    }
                }
            }
        }
        // Recorded after the loop: the chain context borrows several fields
        // of self immutably, so stats writes wait until the borrows end.
        if !declined_nodes.is_empty() {
            if let Some(s) = self.stats.as_deref_mut() {
                s.entry(self.spec.table_name.clone())
                    .or_default()
                    .fast_path_declined
                    .extend(declined_nodes);
            }
        }
        Ok(false)
    }

    fn record_fast_path_decline(&mut self, node: &'static str) {
        if let Some(s) = self.stats.as_deref_mut() {
            s.entry(self.spec.table_name.clone())
                .or_default()
                .fast_path_declined
                .push(node);
        }
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
            mb.saturating_mul(1024 * 1024)
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
                    let reader = match manager.open_scan_reader(path, self.spec.reader_txn_id) {
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
                            let batch_result =
                                super::strategy::read_row_group_with_strategy(
                                    &self.spec,
                                    plan,
                                    &self.table_schema,
                                    &reader,
                                    row_group,
                                );
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
                    output_bytes = output_bytes.saturating_add(batch.estimated_size_bytes() as u64);
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
        ) else {
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
            // Layout-only decode of the single row group this call is
            // accounting for: the previous borrowed accessor existed only
            // because the whole graph used to be resident, and the cloning
            // variant before it materialised bloom filters and value sets
            // just to read column sizes.
            if let Ok(layout) = reader.row_group_layout(rg_idx) {
                for col_idx in cols {
                    if let Some(bytes) = layout.column_compressed_sizes.get(col_idx) {
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
                mb.saturating_mul(1024 * 1024)
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
            flattened
        }
        other => other,
    };
    let rows: Vec<u32> = (0..col.len() as u32).rev().collect();
    Ok(col.take(&rows))
}
