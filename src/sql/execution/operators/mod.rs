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

use std::collections::HashMap;

use crate::common::error::Result;
use crate::sql::planner::*;
use crate::types::batch::{RecordBatch, Schema};

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
    emitted: bool,
}

impl DistinctExec {
    pub fn new(input: Box<dyn ExecutionOp>) -> Self {
        Self {
            input,
            emitted: false,
        }
    }
}

impl ExecutionOp for DistinctExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let mut seen = std::collections::HashSet::new();
        let mut columns: Vec<crate::types::array::ColumnArray> = Vec::new();
        let mut output_schema: Option<Schema> = None;
        while let Some(batch) = self.input.next_batch()? {
            if batch.num_rows() == 0 {
                continue;
            }
            if output_schema.is_none() {
                output_schema = Some(batch.schema.clone());
                for def in &batch.schema.columns {
                    columns.push(crate::types::array::ColumnArray::empty(def.data_type));
                }
            }
            for row in 0..batch.num_rows() {
                let key = (0..batch.num_columns())
                    .map(|c| {
                        batch
                            .scalar(row, c)
                            .map(|v| v.to_string())
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join("\u{1f}");
                if seen.insert(key) {
                    for (col_idx, out) in columns.iter_mut().enumerate() {
                        if let Some(value) = batch.scalar(row, col_idx) {
                            out.push(&value)?;
                        }
                    }
                }
            }
        }
        match output_schema {
            Some(schema) => Ok(Some(RecordBatch::try_new(schema, columns)?)),
            None => Ok(None),
        }
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
    table_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Box<dyn ExecutionOp>> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let batches = table_batches
                .get(&scan.table_name)
                .cloned()
                .unwrap_or_default();
            Ok(Box::new(
                ScanExec::new(batches, scan.projection).with_table_schema(scan.schema),
            ))
        }
        LogicalPlan::Filter(filter) => {
            let schema = get_scan_schema(&filter.input);
            let input = build_pipeline(*filter.input, table_batches)?;
            Ok(Box::new(FilterExec {
                predicate: filter.predicate,
                input,
                schema: schema.clone(),
            }))
        }
        LogicalPlan::Project(proj) => {
            let input_schema = get_scan_schema(&proj.input);
            let input = build_pipeline(*proj.input, table_batches)?;
            Ok(Box::new(ProjectExec {
                expressions: proj.expressions,
                aliases: proj.aliases,
                input,
                input_schema,
            }))
        }
        LogicalPlan::Sort(sort) => {
            let schema = get_scan_schema(&sort.input);
            let input = build_pipeline(*sort.input, table_batches)?;
            Ok(Box::new(SortExec::new(sort.sort_keys, input, schema)))
        }
        LogicalPlan::Limit(limit) => {
            let schema = get_scan_schema(&limit.input);
            let input = build_pipeline(*limit.input, table_batches)?;
            Ok(Box::new(LimitExec::new(
                limit.limit,
                limit.offset,
                input,
                schema,
            )))
        }
        LogicalPlan::Aggregate(agg) => {
            let input_schema = get_scan_schema(&agg.input);
            let input = build_pipeline(*agg.input, table_batches)?;
            let mut exec =
                HashAggregateExec::new(agg.group_by, agg.aggregates, input, input_schema);
            if let Some(having) = agg.having {
                exec = exec.with_having(having);
            }
            Ok(Box::new(exec))
        }
        LogicalPlan::Join(join) => {
            let left = build_pipeline(*join.left, table_batches)?;
            let right = build_pipeline(*join.right, table_batches)?;
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
            let left = build_pipeline(*semi.left, table_batches)?;
            let right = build_pipeline(*semi.right, table_batches)?;
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
            let input = build_pipeline(*topn.input, table_batches)?;
            let sorted = SortExec::new(topn.sort_keys, input, schema.clone())
                .with_limit(topn.limit.saturating_add(topn.offset));
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
            let input = build_pipeline(*distinct.input, table_batches)?;
            Ok(Box::new(DistinctExec::new(input)))
        }
        LogicalPlan::Window(window) => {
            let input = build_pipeline(*window.input, table_batches)?;
            Ok(input)
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
