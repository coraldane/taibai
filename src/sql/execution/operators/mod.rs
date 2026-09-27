pub mod aggregate;
pub mod filter;
pub mod join;
mod join_spill;
pub mod limit;
pub(crate) mod profile;
pub mod project;
pub mod scan;
pub mod semi_join;
pub mod sort;
pub mod topn;
pub mod window;

use std::collections::HashMap;

use crate::common::error::Result;
use crate::sql::planner::*;
use crate::types::batch::{RecordBatch, Schema};

use crate::sql::execution::subquery_runtime::SubqueryRegistry;

use aggregate::HashAggregateExec;
use filter::FilterExec;
use join::HashJoinExec;
use limit::LimitExec;
use project::ProjectExec;
pub use scan::RuntimeInFilter;
use scan::ScanExec;
use semi_join::SemiJoinExec;
use sort::SortExec;

pub trait ExecutionOp {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;

    fn add_runtime_in_filter(&mut self, _filter: RuntimeInFilter) -> bool {
        false
    }
}

pub struct DistinctExec {
    input: Box<dyn ExecutionOp>,
    seen: std::collections::HashSet<Vec<u8>>,
    key_buf: Vec<u8>,
    done: bool,
    /// Memory accounting for the global key set: DISTINCT over a
    /// high-cardinality input used to grow `seen` without any bound and
    /// could take the process down. The set is charged to the shared query
    /// memory budget; exceeding it fails the query with a clear error
    /// instead of an OOM kill.
    memory: crate::sql::execution::spill::MemoryReservation,
    seen_bytes: usize,
}

impl DistinctExec {
    pub fn new(input: Box<dyn ExecutionOp>) -> Self {
        Self {
            input,
            seen: std::collections::HashSet::new(),
            key_buf: Vec::new(),
            done: false,
            memory: crate::sql::execution::spill::MemoryReservation::default(),
            seen_bytes: 0,
        }
    }
}

impl Default for crate::sql::execution::spill::MemoryReservation {
    fn default() -> Self {
        crate::sql::execution::spill::MemoryManager::reservation()
    }
}

impl ExecutionOp for DistinctExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        while let Some(batch) = self.input.next_batch()? {
            crate::common::query_cancel::check()?;
            if batch.num_rows() == 0 {
                continue;
            }
            // Deduplicate on typed byte keys (type tag + null flag + raw
            // bytes, self-delimiting) built straight from column storage; a
            // global `HashSet` spans batches, and each batch is filtered
            // with one columnar gather instead of a per-cell scalar pass.
            let mut keep = Vec::with_capacity(batch.num_rows());
            let mut kept_count = 0usize;
            for row in 0..batch.num_rows() {
                self.key_buf.clear();
                for column in &batch.columns {
                    column.encode_key_at(row, &mut self.key_buf);
                }
                let is_new = self.seen.insert(self.key_buf.clone());
                if is_new {
                    // Vec header + heap buffer + hash-set slot overhead.
                    self.seen_bytes += self.key_buf.len() + 24 + 32;
                }
                keep.push(is_new);
                kept_count += is_new as usize;
            }
            if kept_count == 0 {
                continue;
            }
            if !self.memory.try_resize(self.seen_bytes) {
                return Err(crate::common::error::TaibaiError::ResourceBusy(format!(
                    "DISTINCT key set exceeds the query memory budget ({} bytes)",
                    self.seen_bytes
                )));
            }
            let kept_rows: Vec<u32> = keep
                .iter()
                .enumerate()
                .filter_map(|(row, &keep)| keep.then_some(row as u32))
                .collect();
            let columns = batch
                .columns
                .iter()
                .map(|column| column.take(&kept_rows))
                .collect();
            return Ok(Some(RecordBatch::try_new(batch.schema.clone(), columns)?));
        }
        self.done = true;
        Ok(None)
    }
}

pub fn apply_filter_mask(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    if mask.iter().all(|&m| m) {
        return batch.clone();
    }
    let selection = crate::types::selection::SelectionVector::from_mask(mask);
    if selection.is_empty() {
        return RecordBatch::new(batch.schema.clone());
    }
    selection.apply(batch)
}

pub fn build_pipeline(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
) -> Result<Box<dyn ExecutionOp>> {
    build_pipeline_with_subqueries(plan, table_batches, None)
}

/// Build a materialized pipeline. `subqueries` carries the per-query
/// snapshot registry that lets Filter/Project evaluate correlated subquery
/// nodes; nested subquery execution always passes `None` (nested correlated
/// subqueries are rejected at bind time).
pub fn build_pipeline_with_subqueries(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
    subqueries: Option<&SubqueryRegistry>,
) -> Result<Box<dyn ExecutionOp>> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let batches = table_batches
                .get(&scan.table_name)
                .cloned()
                .unwrap_or_default();
            Ok(Box::new(
                ScanExec::new_shared(batches, scan.projection)
                    .with_table_schema(scan.schema),
            ))
        }
        LogicalPlan::Filter(filter) => {
            let schema = get_scan_schema(&filter.input);
            let input = build_pipeline_with_subqueries(*filter.input, table_batches, subqueries)?;
            Ok(Box::new(FilterExec::new(
                filter.predicate,
                input,
                schema.clone(),
                subqueries.cloned(),
            )))
        }
        LogicalPlan::Project(proj) => {
            let input_schema = get_scan_schema(&proj.input);
            let input = build_pipeline_with_subqueries(*proj.input, table_batches, subqueries)?;
            Ok(Box::new(ProjectExec {
                expressions: proj.expressions,
                aliases: proj.aliases,
                input,
                input_schema,
                subqueries: subqueries.cloned(),
                remapped: None,
            }))
        }
        LogicalPlan::Sort(sort) => {
            let schema = get_scan_schema(&sort.input);
            let input = build_pipeline_with_subqueries(*sort.input, table_batches, subqueries)?;
            Ok(Box::new(SortExec::new(sort.sort_keys, input, schema)))
        }
        LogicalPlan::Limit(limit) => {
            let schema = get_scan_schema(&limit.input);
            let input = build_pipeline_with_subqueries(*limit.input, table_batches, subqueries)?;
            Ok(Box::new(LimitExec::new(
                limit.limit,
                limit.offset,
                input,
                schema,
            )))
        }
        LogicalPlan::Aggregate(agg) => {
            let input_schema = get_scan_schema(&agg.input);
            let input = build_pipeline_with_subqueries(*agg.input, table_batches, subqueries)?;
            let mut exec =
                HashAggregateExec::new(agg.group_by, agg.aggregates, input, input_schema);
            if let Some(having) = agg.having {
                exec = exec.with_having(having);
            }
            Ok(Box::new(exec))
        }
        LogicalPlan::Join(join) => {
            let left = build_pipeline_with_subqueries(*join.left, table_batches, subqueries)?;
            let right = build_pipeline_with_subqueries(*join.right, table_batches, subqueries)?;
            Ok(Box::new(HashJoinExec::new(
                left,
                right,
                join.join_type,
                join.condition,
                join.left_schema,
                join.right_schema,
                join.output_schema,
            )))
        }
        LogicalPlan::SemiJoin(semi) => {
            let left = build_pipeline_with_subqueries(*semi.left, table_batches, subqueries)?;
            let right = build_pipeline_with_subqueries(*semi.right, table_batches, subqueries)?;
            Ok(Box::new(SemiJoinExec::new(
                left,
                right,
                semi.condition,
                semi.left_schema,
                semi.right_schema,
                semi.is_anti,
            )))
        }
        LogicalPlan::TopN(topn) => {
            let schema = get_scan_schema(&topn.input);
            let input = build_pipeline_with_subqueries(*topn.input, table_batches, subqueries)?;
            let sorted = crate::sql::execution::operators::topn::TopNExec::new(
                topn.sort_keys,
                input,
                schema.clone(),
            )
            .with_capacity(topn.limit.saturating_add(topn.offset));
            if topn.offset == 0 {
                Ok(Box::new(sorted))
            } else {
                Ok(Box::new(LimitExec::new(
                    topn.limit,
                    topn.offset,
                    Box::new(sorted),
                    schema,
                )))
            }
        }
        LogicalPlan::Distinct(distinct) => {
            let input = build_pipeline_with_subqueries(*distinct.input, table_batches, subqueries)?;
            Ok(Box::new(DistinctExec::new(input)))
        }
        LogicalPlan::Window(window) => {
            // Window functions are computed here too (e.g. inside snapshot /
            // subquery pipelines) instead of silently dropping the columns.
            let input_schema = get_scan_schema(&window.input);
            let input = build_pipeline_with_subqueries(*window.input, table_batches, subqueries)?;
            Ok(Box::new(
                crate::sql::execution::operators::window::WindowExec::new(
                    input,
                    window.funcs,
                    input_schema,
                ),
            ))
        }
        LogicalPlan::Empty => Ok(Box::new(ScanExec::new(vec![], None))),
    }
}

fn get_scan_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_scan_schema(&f.input),
        LogicalPlan::Project(_) | LogicalPlan::Aggregate(_) => {
            crate::sql::execution::physical::planner::get_scan_schema(plan)
        }
        LogicalPlan::Sort(s) => get_scan_schema(&s.input),
        LogicalPlan::Limit(l) => get_scan_schema(&l.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::SemiJoin(s) => s.left_schema.clone(),
        LogicalPlan::TopN(t) => get_scan_schema(&t.input),
        LogicalPlan::Distinct(d) => get_scan_schema(&d.input),
        LogicalPlan::Window(w) => get_scan_schema(&w.input),
        LogicalPlan::Empty => Schema::empty(),
    }
}
