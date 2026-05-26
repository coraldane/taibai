#![allow(dead_code)]
pub mod aggregate;
pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod semi_join;
pub mod sort;
pub mod window;

use std::collections::HashMap;

use crate::common::error::Result;
use crate::sql::planner::*;
use crate::types::batch::{RecordBatch, Schema};

use aggregate::HashAggregateExec;
use filter::FilterExec;
use join::HashJoinExec;
use limit::LimitExec;
use project::ProjectExec;
use scan::ScanExec;
use semi_join::SemiJoinExec;
use sort::SortExec;

pub trait ExecutionOp {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

pub fn apply_filter_mask(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    if mask.iter().all(|&m| m) {
        return batch.clone();
    }
    let match_count = mask.iter().filter(|&&m| m).count();
    if match_count == 0 {
        return RecordBatch::new(batch.schema.clone());
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        columns.push(col.filter_by_mask_fast(mask, match_count));
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
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
            Ok(Box::new(ScanExec::new(batches, scan.projection)))
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
            Ok(Box::new(HashAggregateExec::new(
                agg.group_by,
                agg.aggregates,
                input,
                input_schema,
            )))
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
            Ok(Box::new(
                SortExec::new(topn.sort_keys, input, schema).with_limit(topn.limit),
            ))
        }
        LogicalPlan::Distinct(distinct) => {
            let input = build_pipeline(*distinct.input, table_batches)?;
            Ok(input)
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
        LogicalPlan::Project(p) => get_scan_schema(&p.input),
        LogicalPlan::Sort(s) => get_scan_schema(&s.input),
        LogicalPlan::Limit(l) => get_scan_schema(&l.input),
        LogicalPlan::Aggregate(a) => get_scan_schema(&a.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::SemiJoin(s) => s.left_schema.clone(),
        LogicalPlan::TopN(t) => get_scan_schema(&t.input),
        LogicalPlan::Distinct(d) => get_scan_schema(&d.input),
        LogicalPlan::Window(w) => get_scan_schema(&w.input),
        LogicalPlan::Empty => Schema::empty(),
    }
}
