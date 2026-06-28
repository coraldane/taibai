use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::HashAggregateExec;
use crate::sql::execution::operators::filter::FilterExec;
use crate::sql::execution::operators::join::HashJoinExec;
use crate::sql::execution::operators::limit::LimitExec;
use crate::sql::execution::operators::project::ProjectExec;
use crate::sql::execution::operators::scan::ScanExec;
use crate::sql::execution::operators::semi_join::SemiJoinExec;
use crate::sql::execution::operators::sort::SortExec;
use crate::sql::execution::operators::ExecutionOp;
use crate::sql::execution::scan::spec::compile_scan_spec;
use crate::sql::planner::{LogicalExpr, LogicalPlan};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::context::PhysicalContext;

pub fn can_use_physical_pipeline(plan: &LogicalPlan) -> bool {
    is_supported_pipeline(plan)
}

pub fn build_physical_pipeline(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
) -> Result<Box<dyn ExecutionOp>> {
    let plan = remove_redundant_pushed_filters(plan);
    let plan = try_skip_sort_for_ordered_scan(plan, ctx);
    build_operator_with_limit(plan, ctx, None)
}

pub fn drain_pipeline(
    mut pipeline: Box<dyn ExecutionOp + '_>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let mut result_batches = Vec::new();
    let mut result_schema = None;
    while let Some(batch) = pipeline.next_batch()? {
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        result_batches.push(batch);
    }
    Ok((result_schema.unwrap_or_else(Schema::empty), result_batches))
}

fn build_operator_with_limit(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
    limit_hint: Option<usize>,
) -> Result<Box<dyn ExecutionOp>> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let (physical_table_name, database) = {
                let table_ref = ctx.table_ref(&scan.table_name).ok_or_else(|| {
                    TaibaiError::Internal(format!("missing table ref for '{}'", scan.table_name))
                })?;
                (table_ref.table.clone(), table_ref.database.clone())
            };
            let schema = ctx.schema(&scan.table_name).cloned().ok_or_else(|| {
                TaibaiError::Internal(format!("missing schema for '{}'", scan.table_name))
            })?;
            let meta = ctx.meta(&scan.table_name).cloned().ok_or_else(|| {
                TaibaiError::Internal(format!("missing metadata for '{}'", scan.table_name))
            })?;
            let mut spec = compile_scan_spec(
                &LogicalPlan::Scan(scan.clone()),
                &scan.table_name,
                &schema,
                &meta,
            );
            spec.table_name = physical_table_name;
            if let Some(limit) = limit_hint {
                spec.limit = Some(spec.limit.map_or(limit, |current| current.min(limit)));
            }
            if ctx.is_count_star {
                spec.is_count_star = true;
            }
            Ok(Box::new(ScanExec::deferred(
                spec,
                ctx.scan_runtime.clone(),
                schema,
                meta,
                database,
            )))
        }
        LogicalPlan::Filter(filter) => {
            let schema = get_scan_schema(&filter.input);
            let input = build_operator_with_limit(*filter.input, ctx, None)?;
            Ok(Box::new(FilterExec {
                predicate: filter.predicate,
                input,
                schema,
            }))
        }
        LogicalPlan::Project(project) => {
            let input_schema = get_scan_schema(&project.input);
            let input = build_operator_with_limit(*project.input, ctx, limit_hint)?;
            Ok(Box::new(ProjectExec {
                expressions: project.expressions,
                aliases: project.aliases,
                input,
                input_schema,
            }))
        }
        LogicalPlan::Sort(sort) => {
            let schema = get_scan_schema(&sort.input);
            let input = build_operator_with_limit(*sort.input, ctx, None)?;
            Ok(Box::new(SortExec::new(sort.sort_keys, input, schema)))
        }
        LogicalPlan::Limit(limit) => {
            let schema = get_scan_schema(&limit.input);
            let scan_limit = limit.limit.saturating_add(limit.offset);
            let input = build_operator_with_limit(*limit.input, ctx, Some(scan_limit))?;
            Ok(Box::new(LimitExec::new(
                limit.limit,
                limit.offset,
                input,
                schema,
            )))
        }
        LogicalPlan::TopN(topn) => {
            let schema = get_scan_schema(&topn.input);
            let input = build_operator_with_limit(*topn.input, ctx, None)?;
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
        LogicalPlan::Aggregate(agg) => {
            let input_schema = get_scan_schema(&agg.input);
            let input = build_operator_with_limit(*agg.input, ctx, None)?;
            Ok(Box::new(HashAggregateExec::new(
                agg.group_by,
                agg.aggregates,
                input,
                input_schema,
            )))
        }
        LogicalPlan::Join(join) => {
            let left = build_operator_with_limit(*join.left, ctx, None)?;
            let right = build_operator_with_limit(*join.right, ctx, None)?;
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
            let left = build_operator_with_limit(*semi.left, ctx, None)?;
            let right = build_operator_with_limit(*semi.right, ctx, None)?;
            Ok(Box::new(SemiJoinExec::new(
                left,
                right,
                semi.condition,
                semi.left_schema,
                semi.right_schema,
                semi.is_anti,
            )))
        }
        LogicalPlan::Distinct(distinct) => build_operator_with_limit(*distinct.input, ctx, None),
        LogicalPlan::Window(window) => build_operator_with_limit(*window.input, ctx, None),
        LogicalPlan::Empty => Ok(Box::new(ScanExec::new(vec![], None))),
    }
}

fn is_supported_pipeline(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(_) | LogicalPlan::Empty => true,
        LogicalPlan::Filter(filter) => is_supported_pipeline(&filter.input),
        LogicalPlan::Project(project) => is_supported_pipeline(&project.input),
        LogicalPlan::Sort(sort) => is_supported_pipeline(&sort.input),
        LogicalPlan::Limit(limit) => is_supported_pipeline(&limit.input),
        LogicalPlan::TopN(topn) => is_supported_pipeline(&topn.input),
        LogicalPlan::Aggregate(agg) => is_supported_pipeline(&agg.input),
        LogicalPlan::Distinct(distinct) => is_supported_pipeline(&distinct.input),
        LogicalPlan::Window(window) => is_supported_pipeline(&window.input),
        LogicalPlan::Join(join) => {
            is_supported_pipeline(&join.left) && is_supported_pipeline(&join.right)
        }
        LogicalPlan::SemiJoin(semi) => {
            is_supported_pipeline(&semi.left) && is_supported_pipeline(&semi.right)
        }
    }
}

fn remove_redundant_pushed_filters(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            let input = remove_redundant_pushed_filters(*filter.input);
            match input {
                LogicalPlan::Scan(scan)
                    if scan
                        .pushed_filter
                        .as_ref()
                        .map(is_scan_filter_fully_rechecked_by_scan)
                        .unwrap_or(false) =>
                {
                    LogicalPlan::Scan(scan)
                }
                other => LogicalPlan::Filter(crate::sql::planner::FilterPlan {
                    predicate: filter.predicate,
                    input: Box::new(other),
                }),
            }
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(remove_redundant_pushed_filters(*project.input));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(remove_redundant_pushed_filters(*sort.input));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(remove_redundant_pushed_filters(*limit.input));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(remove_redundant_pushed_filters(*topn.input));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Aggregate(mut agg) => {
            agg.input = Box::new(remove_redundant_pushed_filters(*agg.input));
            LogicalPlan::Aggregate(agg)
        }
        other => other,
    }
}

fn is_scan_filter_fully_rechecked_by_scan(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            crate::sql::parser::ast::BinOp::And => {
                is_scan_filter_fully_rechecked_by_scan(left)
                    && is_scan_filter_fully_rechecked_by_scan(right)
            }
            crate::sql::parser::ast::BinOp::Eq
            | crate::sql::parser::ast::BinOp::Lt
            | crate::sql::parser::ast::BinOp::Gt
            | crate::sql::parser::ast::BinOp::Lte
            | crate::sql::parser::ast::BinOp::Gte => {
                matches!(
                    (&**left, &**right),
                    (LogicalExpr::Column(_), LogicalExpr::Literal(_))
                ) || matches!(
                    (&**left, &**right),
                    (LogicalExpr::Literal(_), LogicalExpr::Column(_))
                )
            }
            _ => false,
        },
        LogicalExpr::In { expr, .. } => matches!(&**expr, LogicalExpr::Column(_)),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            matches!(&**expr, LogicalExpr::Column(_))
                && matches!(&**low, LogicalExpr::Literal(_))
                && matches!(&**high, LogicalExpr::Literal(_))
        }
        _ => false,
    }
}

fn try_skip_sort_for_ordered_scan(plan: LogicalPlan, ctx: &PhysicalContext<'_>) -> LogicalPlan {
    let LogicalPlan::Sort(sort) = &plan else {
        return plan;
    };
    let Some(scan) = single_scan(&sort.input) else {
        return plan;
    };
    let Some(meta) = ctx.meta(&scan.table_name) else {
        return plan;
    };
    if meta.cluster_key.is_empty() || !sort.sort_keys.iter().all(|sk| !sk.desc) {
        return plan;
    }
    let Some(schema) = ctx.schema(&scan.table_name) else {
        return plan;
    };
    let matches = sort.sort_keys.len() == meta.cluster_key.len()
        && sort
            .sort_keys
            .iter()
            .zip(meta.cluster_key.iter())
            .all(|(sk, ck)| match &sk.expr {
                LogicalExpr::Column(idx) => schema
                    .columns
                    .get(*idx)
                    .map(|c| c.name == *ck)
                    .unwrap_or(false),
                _ => false,
            });
    if matches {
        *sort.input.clone()
    } else {
        plan
    }
}

fn single_scan(plan: &LogicalPlan) -> Option<&crate::sql::planner::ScanPlan> {
    match plan {
        LogicalPlan::Scan(scan) => Some(scan),
        LogicalPlan::Filter(filter) => single_scan(&filter.input),
        LogicalPlan::Project(project) => single_scan(&project.input),
        LogicalPlan::Limit(limit) => single_scan(&limit.input),
        LogicalPlan::TopN(topn) => single_scan(&topn.input),
        _ => None,
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
        LogicalPlan::Empty => Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)]),
    }
}
