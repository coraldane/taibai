use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam_channel::Receiver;
use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::segment::SegmentReader;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

use super::memtable;
use super::partition;
use super::row_group::RowGroupPruneResult;
use super::runtime::ScanRuntime;

/// Dedicated pool for the background decoder: its multi-millisecond chunk
/// tasks must not queue the query thread's per-batch lane work (aggregate
/// lanes, parallel scans) behind them on the global pool. Half the global
/// threads, clamped so tiny deployments still get overlap.
fn background_decode_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let threads = (rayon::current_num_threads() / 2).clamp(2, 8);
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|idx| format!("taibai-bg-decode-{idx}"))
            .build()
            .expect("background decode thread pool")
    })
}
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
    /// Background decoder: streams the segment work list through a bounded
    /// channel so row-group decode overlaps with downstream consumption
    /// (aggregate/limit/encode) instead of alternating in window barriers.
    decode_rx: Option<Receiver<Result<RecordBatch>>>,
}

/// Minimum decoded row groups before the background decoder is worth a
/// thread; smaller scans decode inline.
const BACKGROUND_DECODE_MIN_GROUPS: usize = 4;
/// Bounded channel capacity: caps decoded-batch memory held ahead of the
/// consumer (batches are ~10^4 rows, so 64 is a few hundred MB worst case
/// for wide projections and far less for typical scans).
const DECODE_CHANNEL_CAPACITY: usize = 64;

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
            decode_rx: None,
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

        // Background decoder mode: batches arrive through the bounded
        // channel in work-list order; when the channel closes the memtable
        // leg follows.
        if self.decode_rx.is_some() {
            loop {
                if let Some(batch) = self.prefetch.pop_front() {
                    if let Some(batch) = self.limit_batch(batch) {
                        return Ok(Some(batch));
                    }
                    continue;
                }
                let received = match self
                    .decode_rx
                    .as_ref()
                    .and_then(|rx| rx.recv().ok())
                {
                    Some(batch) => batch,
                    None => {
                        self.decode_rx = None;
                        if self.prefetch.is_empty() {
                            break;
                        }
                        continue;
                    }
                };
                let batch = strategy::apply_ttl_filter_and_projection(
                    received?,
                    &self.table_meta,
                    &self.plan.projection_cols,
                    self.plan.strip_ttl_column,
                )?;
                if batch.num_rows() == 0 {
                    continue;
                }
                self.prefetch.push_back(batch);
            }
            return self.drain_memtable_leg();
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

    /// Build the segment work list; when it is large enough, hand it to a
    /// background decode thread fed through a bounded channel and report
    /// that the channel serves the scan. Returns false when the scan should
    /// stay on the inline sequential walk.
    fn try_spawn_background_decoder(&mut self) -> Result<bool> {
        // Operational escape hatch: force inline decoding to isolate
        // background-decode contention from consumer-side costs.
        if std::env::var("TAIBAI_INLINE_DECODE").as_deref() == Ok("1") {
            return Ok(false);
        }
        let total_candidates: usize = self
            .segments
            .iter()
            .filter_map(|segment| {
                let meta = self
                    .runtime
                    .segment_mgr()
                    .segment_meta(segment)
                    .ok()
                    .flatten()?;
                let pred = self.spec.to_row_group_predicate();
                if !super::row_group::prune_segment_by_zone_map(&pred, &meta.segment_zone_maps) {
                    return Some(0usize);
                }
                Some(
                    super::row_group::prune_row_group_meta(
                        &meta.row_groups,
                        &pred,
                        &super::row_group::build_bloom_check_list(&self.spec),
                        &super::row_group::build_in_check_list(&self.spec),
                        &super::row_group::build_set_check_list(&self.spec),
                        &super::row_group::build_set_in_check_list(&self.spec),
                    )
                    .candidate_indices
                    .len(),
                )
            })
            .sum();
        if total_candidates < BACKGROUND_DECODE_MIN_GROUPS {
            return Ok(false);
        }

        let (tx, rx) = crossbeam_channel::bounded(DECODE_CHANNEL_CAPACITY);
        let spec = self.spec.clone();
        let plan = self.plan.clone();
        let table_schema = self.table_schema.clone();
        let _table_meta = self.table_meta.clone();
        let runtime = self.runtime.clone();
        let db = self.db.clone();
        let segments = self.segments.clone();
        let _ = db;
        std::thread::Builder::new()
            .name("taibai-cursor-decode".into())
            .spawn(move || {
                for segment in &segments {
                    let reader =
                        match runtime.segment_mgr().open_reader_cached(segment) {
                            Ok(r) => r,
                            Err(_) => continue,
                        };
                    let candidates = if let Ok(Some(meta)) =
                        runtime.segment_mgr().segment_meta(segment)
                    {
                        let pred = spec.to_row_group_predicate();
                        if !super::row_group::prune_segment_by_zone_map(
                            &pred,
                            &meta.segment_zone_maps,
                        ) {
                            continue;
                        }
                        super::row_group::prune_row_group_meta(
                            &meta.row_groups,
                            &pred,
                            &super::row_group::build_bloom_check_list(&spec),
                            &super::row_group::build_in_check_list(&spec),
                            &super::row_group::build_set_check_list(&spec),
                            &super::row_group::build_set_in_check_list(&spec),
                        )
                        .candidate_indices
                    } else {
                        match strategy::prune_segment(&spec, &reader) {
                            Some(pruned) => pruned.candidate_indices,
                            None => continue,
                        }
                    };
                    // Decode in parallel chunks (bounded by the global
                    // cpu/io permits); send in candidate order so the
                    // consumer sees the same sequence as the inline walk.
                    // The dedicated pool keeps these chunk tasks from
                    // queueing the consumer's per-batch lane work behind
                    // multi-millisecond decode tasks on the global pool.
                    for chunk in candidates.chunks(32) {
                        let decoded: Vec<_> = background_decode_pool().install(|| {
                            chunk
                                .par_iter()
                                .filter_map(|row_group| {
                                    let _cpu = crate::common::work_limiter::acquire_cpu();
                                    let _io = crate::common::work_limiter::acquire_io();
                                    strategy::read_row_group_with_strategy(
                                        &spec,
                                        &plan,
                                        &table_schema,
                                        &reader,
                                        *row_group,
                                    )
                                    .ok()
                                })
                                .collect()
                        });
                        for batch in decoded {
                            if tx.send(Ok(batch)).is_err() {
                                // Consumer went away (LIMIT early exit / drop).
                                return;
                            }
                        }
                    }
                }
            })
            .map_err(|e| TaibaiError::Internal(format!("spawn cursor decoder failed: {e}")))?;
        self.decode_rx = Some(rx);
        Ok(true)
    }

    fn drain_memtable_leg(&mut self) -> Result<Option<RecordBatch>> {
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

        // Memtable rows are never covered by the background segment decoder,
        // so scan them up front and let both the inline and background modes
        // drain the same leg.
        let mut memtable_spec = self.spec.clone();
        memtable_spec.projection = Some(self.plan.projection_cols.clone());
        self.memtable_batches = memtable::scan_memtables(
            self.runtime.memtable_manager(),
            self.runtime.hot_store_manager(),
            &self.db,
            &self.spec.table_name,
            &memtable_spec,
        );

        if self.try_spawn_background_decoder()? {
            self.initialized = true;
            return Ok(());
        }

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
