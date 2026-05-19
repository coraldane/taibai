pub mod scan;
pub mod filter;
pub mod project;
pub mod aggregate;
pub mod sort;
pub mod limit;
pub mod join;

use std::collections::HashMap;

use crate::common::error::Result;
use crate::sql::planner::*;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::array::ColumnArray;

use scan::ScanExec;
use filter::FilterExec;
use project::ProjectExec;
use aggregate::HashAggregateExec;
use sort::SortExec;
use limit::LimitExec;
use join::HashJoinExec;

pub trait ExecutionOp {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

pub fn apply_filter_mask(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    if mask.iter().all(|&m| m) {
        return batch.clone();
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&col.scalar(i)).unwrap();
            }
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

pub fn build_pipeline(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Box<dyn ExecutionOp>> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let batches = table_batches.get(&scan.table_name).cloned().unwrap_or_default();
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
            Ok(Box::new(LimitExec::new(limit.limit, limit.offset, input, schema)))
        }
        LogicalPlan::Aggregate(agg) => {
            let input_schema = get_scan_schema(&agg.input);
            let input = build_pipeline(*agg.input, table_batches)?;
            Ok(Box::new(HashAggregateExec::new(agg.group_by, agg.aggregates, input, input_schema)))
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
        LogicalPlan::Empty => {
            Ok(Box::new(ScanExec::new(vec![], None)))
        }
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
        LogicalPlan::Empty => Schema::empty(),
    }
}
