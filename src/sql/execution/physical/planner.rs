use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::{aggregate_result_column_def, HashAggregateExec};
use crate::sql::execution::operators::filter::FilterExec;
use crate::sql::execution::operators::folded_aggregate::FoldedAggregateExec;
use crate::sql::execution::operators::join::HashJoinExec;
use crate::sql::execution::operators::limit::LimitExec;
use crate::sql::execution::operators::profile::{ProfiledExec, QueryProfile, ScanProfileSeed};
use crate::sql::execution::operators::project::ProjectExec;
use crate::sql::execution::operators::scan::ScanExec;
use crate::sql::execution::operators::semi_join::SemiJoinExec;
use crate::sql::execution::operators::sort::SortExec;
use crate::sql::execution::operators::topn::TopNExec;
use crate::sql::execution::operators::ExecutionOp;
use crate::sql::execution::scan::spec::compile_scan_spec;
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::{LogicalExpr, LogicalPlan, ProjectPlan};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::context::PhysicalContext;

pub fn can_use_physical_pipeline(plan: &LogicalPlan) -> bool {
    is_supported_pipeline(plan)
}

pub fn build_physical_pipeline(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
) -> Result<Box<dyn ExecutionOp>> {
    let plan = prepare_physical_plan(plan, ctx);
    build_operator_with_limit(plan, ctx, None, None, None, None, 0)
}

pub(crate) fn build_profiled_materialized_pipeline(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
    table_batches: &HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
    scan_seeds: &HashMap<String, ScanProfileSeed>,
    profile: &QueryProfile,
) -> Result<Box<dyn ExecutionOp>> {
    build_operator_with_limit(
        plan,
        ctx,
        None,
        Some(table_batches),
        Some(scan_seeds),
        Some(profile),
        0,
    )
}

pub(crate) fn prepare_physical_plan(plan: LogicalPlan, ctx: &PhysicalContext<'_>) -> LogicalPlan {
    let plan = remove_redundant_pushed_filters(plan);
    let plan = crate::sql::optimizer::join_order::reorder_inner_join_trees(plan, ctx);
    try_skip_sort_for_ordered_scan(plan, ctx)
}

pub fn drain_pipeline(
    mut pipeline: Box<dyn ExecutionOp + '_>,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let mut result_batches = Vec::new();
    let mut retained_bytes = 0;
    let result_limit = crate::common::query_memory::result_memory_limit_bytes();
    let mut result_schema = None;
    loop {
        crate::common::query_cancel::check()?;
        let Some(batch) = pipeline.next_batch()? else {
            break;
        };
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        crate::common::query_memory::push_result_batch(
            &mut result_batches,
            &mut retained_bytes,
            batch,
            result_limit,
        )?;
    }
    let schema = result_schema
        .or_else(|| pipeline.output_schema())
        .unwrap_or_else(Schema::empty);
    Ok((schema, result_batches))
}

/// Rewrite the input of `SELECT DISTINCT` into a zone-map-answering scan
/// when the shape is a single plain column reference over an unfiltered
/// scan: the scan's projection is narrowed to that column and flagged with
/// `distinct_column`, and the wrapping Project is remapped onto it. Returns
/// `None` when the fast path does not apply.
fn try_rewrite_distinct_scan(
    input: &LogicalPlan,
    ctx: &PhysicalContext<'_>,
) -> Option<LogicalPlan> {
    let (project, scan) = match input {
        LogicalPlan::Project(p) => {
            if p.expressions.len() != 1 || p.aliases.len() != p.expressions.len() {
                return None;
            }
            let LogicalExpr::Column(col_idx) = p.expressions[0] else {
                return None;
            };
            let LogicalPlan::Scan(s) = p.input.as_ref() else {
                return None;
            };
            (Some((p, col_idx)), s)
        }
        LogicalPlan::Scan(s) => (None, s),
        _ => return None,
    };

    // The zone-map answer is only exact for an unfiltered, unordered scan.
    let eligible = scan.pushed_filter.is_none()
        && scan.zone_map_filters.is_empty()
        && scan.pushed_limit.is_none()
        && scan.pushed_order.is_none()
        && scan.partition_filter.is_none()
        && scan.time_range.is_none()
        && scan.index_columns.is_empty()
        && scan.bloom_filter_columns.is_empty()
        && scan.minmax_index_columns.is_empty()
        && scan.set_index_columns.is_empty();
    if !eligible {
        return None;
    }

    // Resolve the projected column to its index in the table schema.
    let table_idx = match (&scan.projection, &project) {
        (Some(projection), Some((_, col_idx))) => *projection.get(*col_idx)?,
        (Some(projection), None) => {
            if projection.len() != 1 {
                return None;
            }
            projection[0]
        }
        (None, Some((_, col_idx))) => *col_idx,
        (None, None) => return None,
    };

    let table_schema = ctx.schema(&scan.table_name)?;
    let column = table_schema.columns.get(table_idx)?;
    let column_name = column.name.clone();

    let mut new_scan = scan.clone();
    new_scan.projection = Some(vec![table_idx]);
    new_scan.distinct_column = Some(column_name);

    match project {
        Some((p, _)) => Some(LogicalPlan::Project(ProjectPlan {
            expressions: vec![LogicalExpr::Column(0)],
            aliases: p.aliases.clone(),
            input: Box::new(LogicalPlan::Scan(new_scan)),
        })),
        None => Some(LogicalPlan::Scan(new_scan)),
    }
}

fn build_operator_with_limit(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
    limit_hint: Option<usize>,
    table_batches: Option<&HashMap<String, Vec<std::sync::Arc<RecordBatch>>>>,
    scan_seeds: Option<&HashMap<String, ScanProfileSeed>>,
    profile: Option<&QueryProfile>,
    depth: usize,
) -> Result<Box<dyn ExecutionOp>> {
    let operator_profile = profile.map(|profile| {
        let operator = profile.register(
            depth,
            profile_operator_name(&plan),
            crate::sql::optimizer::join_order::estimate_plan_rows(&plan, ctx),
        );
        if let LogicalPlan::Scan(scan) = &plan {
            if let Some(seed) = scan_seeds.and_then(|seeds| seeds.get(&scan.table_name)) {
                operator.add_elapsed(seed.elapsed);
                operator.observe_memory(seed.peak_memory_bytes);
            }
        }
        operator
    });
    let operator: Result<Box<dyn ExecutionOp>> = match plan {
        LogicalPlan::Scan(scan) => {
            if let Some(table_batches) = table_batches {
                // Materialized path: pseudo tables (derived tables,
                // decorrelated subquery snapshots) carry their schema in the
                // plan and have no storage metadata.
                let batches = table_batches
                    .get(&scan.table_name)
                    .cloned()
                    .unwrap_or_default();
                Ok(Box::new(
                    ScanExec::new_shared(batches, scan.projection).with_table_schema(scan.schema),
                ))
            } else {
                let (physical_table_name, database) = {
                    let table_ref = ctx.table_ref(&scan.table_name).ok_or_else(|| {
                        TaibaiError::Internal(format!(
                            "missing table ref for '{}'",
                            scan.table_name
                        ))
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
                Ok(Box::new(ScanExec::deferred(
                    spec,
                    ctx.scan_runtime.clone(),
                    schema,
                    meta,
                    database,
                )))
            }
        }
        LogicalPlan::Filter(filter) => {
            let schema = get_scan_schema(&filter.input);
            let input = build_operator_with_limit(
                *filter.input,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(FilterExec::new(
                filter.predicate,
                input,
                schema,
                ctx.subqueries.clone(),
            )))
        }
        LogicalPlan::Project(project) => {
            let input_schema = get_scan_schema(&project.input);
            let input = build_operator_with_limit(
                *project.input,
                ctx,
                limit_hint,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(ProjectExec {
                expressions: project.expressions,
                aliases: project.aliases,
                input,
                input_schema,
                subqueries: ctx.subqueries.clone(),
                remapped: None,
            }))
        }
        LogicalPlan::Sort(sort) => {
            let schema = get_scan_schema(&sort.input);
            let input_plan = annotate_single_scan_with_order(*sort.input, sort.sort_keys.clone());
            let input = build_operator_with_limit(
                input_plan,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(SortExec::new(sort.sort_keys, input, schema)))
        }
        LogicalPlan::Limit(limit) => {
            let schema = get_scan_schema(&limit.input);
            let scan_limit = limit.limit.saturating_add(limit.offset);
            let input = build_operator_with_limit(
                *limit.input,
                ctx,
                Some(scan_limit),
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(LimitExec::new(
                limit.limit,
                limit.offset,
                input,
                schema,
            )))
        }
        LogicalPlan::TopN(topn) => {
            let schema = get_scan_schema(&topn.input);
            let input_plan = annotate_single_scan_with_ordered_limit(
                *topn.input,
                topn.sort_keys.clone(),
                topn.limit.saturating_add(topn.offset),
            );
            let input = build_operator_with_limit(
                input_plan,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            let sorted = TopNExec::new(topn.sort_keys, input, schema.clone())
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
        LogicalPlan::Aggregate(agg) => {
            let agg = materialize_aggregate_arguments(agg);
            let input_schema = get_scan_schema(&agg.input);
            // Projection pushdown: if the aggregate feeds directly off a Scan,
            // restrict the scan to only the columns referenced by GROUP BY and
            // the aggregate arguments. The existing `remap_aggregate_for_batch`
            // / `remap_expr_for_batch` logic rewrites indices to the projected
            // batch schema. Predicate columns are still read because the scan
            // spec compilation expands projection to include them.
            let plan = LogicalPlan::Aggregate(pushdown_aggregate_projection(agg));
            let agg = match plan {
                LogicalPlan::Aggregate(a) => a,
                other => {
                    return build_operator_with_limit(
                        other,
                        ctx,
                        limit_hint,
                        table_batches,
                        scan_seeds,
                        profile,
                        depth,
                    )
                }
            };
            // Metadata-folded aggregate: when the aggregate reads a single
            // table scan directly, the folding engine may answer it per
            // group from zone maps / footers without streaming row data.
            // Shape mismatches decline here; runtime declines (inside the
            // operator) fall back to the HashAggregate pipeline below.
            let folded = if table_batches.is_none() {
                try_folded_aggregate_exec(&agg, ctx)
            } else {
                None
            };
            if let Some(folded) = folded {
                Ok(Box::new(folded))
            } else {
                let input = build_operator_with_limit(
                    *agg.input,
                    ctx,
                    None,
                    table_batches,
                    scan_seeds,
                    profile,
                    depth + 1,
                )?;
                let mut exec =
                    HashAggregateExec::new(agg.group_by, agg.aggregates, input, input_schema);
                if let Some(having) = agg.having {
                    exec = exec.with_having(having);
                }
                Ok(Box::new(exec))
            }
        }
        LogicalPlan::Join(join) => {
            let build_left = if join.join_type == JoinType::Inner {
                crate::sql::optimizer::join_order::estimate_plan_rows(&join.left, ctx)
                    <= crate::sql::optimizer::join_order::estimate_plan_rows(&join.right, ctx)
            } else {
                true
            };
            // Each child is wrapped to its FULL planned side schema: the
            // column-pruning rules may have narrowed the scan below to the
            // required columns, and the join's condition evaluation and row
            // assembly address the full-width layout. The wrapper is a zero
            // -cost pass-through for unpruned children.
            let left = super::planner::expand_to_schema(
                build_operator_with_limit(
                    *join.left,
                    ctx,
                    None,
                    table_batches,
                    scan_seeds,
                    profile,
                    depth + 1,
                )?,
                join.left_schema.clone(),
            );
            let right = super::planner::expand_to_schema(
                build_operator_with_limit(
                    *join.right,
                    ctx,
                    None,
                    table_batches,
                    scan_seeds,
                    profile,
                    depth + 1,
                )?,
                join.right_schema.clone(),
            );
            Ok(Box::new(
                HashJoinExec::new(
                    left,
                    right,
                    join.join_type,
                    join.condition,
                    join.left_schema,
                    join.right_schema,
                    join.output_schema,
                )
                .with_inner_build_left(build_left),
            ))
        }
        LogicalPlan::SemiJoin(semi) => {
            // Same full-width contract as the Join arm (see above).
            let left = super::planner::expand_to_schema(
                build_operator_with_limit(
                    *semi.left,
                    ctx,
                    None,
                    table_batches,
                    scan_seeds,
                    profile,
                    depth + 1,
                )?,
                semi.left_schema.clone(),
            );
            let right = super::planner::expand_to_schema(
                build_operator_with_limit(
                    *semi.right,
                    ctx,
                    None,
                    table_batches,
                    scan_seeds,
                    profile,
                    depth + 1,
                )?,
                semi.right_schema.clone(),
            );
            Ok(Box::new(SemiJoinExec::new(
                left,
                right,
                semi.condition,
                semi.left_schema,
                semi.right_schema,
                semi.is_anti,
            )))
        }
        LogicalPlan::Distinct(distinct) => {
            // `SELECT DISTINCT <single column>` over an unfiltered scan is
            // answered from row-group zone maps by flagging the scan; the
            // DistinctExec/Project above it then see one already-distinct
            // batch and pass it through unchanged.
            let input_plan = if table_batches.is_none() {
                match try_rewrite_distinct_scan(distinct.input.as_ref(), ctx) {
                    Some(rewritten) => rewritten,
                    None => *distinct.input,
                }
            } else {
                *distinct.input
            };
            let input = build_operator_with_limit(
                input_plan,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(
                crate::sql::execution::operators::DistinctExec::new(input),
            ))
        }
        LogicalPlan::Window(window) => {
            let input_schema = get_scan_schema_from_window(&window);
            let input = build_operator_with_limit(
                *window.input,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            Ok(Box::new(
                crate::sql::execution::operators::window::WindowExec::new(
                    input,
                    window.funcs,
                    input_schema,
                ),
            ))
        }
        LogicalPlan::Empty => Ok(Box::new(ScanExec::new(vec![], None))),
    };
    let operator = operator?;
    Ok(match operator_profile {
        Some(profile) => ProfiledExec::wrap(operator, profile),
        None => operator,
    })
}

fn profile_operator_name(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan(scan) => format!("Scan {}", scan.table_name),
        LogicalPlan::Filter(_) => "Filter".to_string(),
        LogicalPlan::Project(_) => "Project".to_string(),
        LogicalPlan::Aggregate(_) => "Aggregate".to_string(),
        LogicalPlan::Sort(_) => "Sort".to_string(),
        LogicalPlan::Limit(_) => "Limit".to_string(),
        LogicalPlan::TopN(_) => "TopN".to_string(),
        LogicalPlan::Distinct(_) => "Distinct".to_string(),
        LogicalPlan::Window(_) => "Window".to_string(),
        LogicalPlan::Join(join) => format!("Join {:?}", join.join_type),
        LogicalPlan::SemiJoin(join) => {
            if join.is_anti {
                "AntiJoin".to_string()
            } else {
                "SemiJoin".to_string()
            }
        }
        LogicalPlan::Empty => "Empty".to_string(),
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
    if matches && ordered_cluster_scan_is_safe(ctx, meta, schema, &meta.cluster_key[0]) {
        *sort.input.clone()
    } else {
        plan
    }
}

/// Removing a Sort on the strength of the cluster key alone is only valid
/// when the scan's output is globally ordered: segments must be mutually
/// monotonic on the leading cluster column AND no memtable may hold
/// unflushed rows (memtable batches restart the cluster-key ordering).
/// This mirrors the streaming executor's `ordered_cluster_segments_are_monotonic`
/// guard that the ordered-limit path has always required.
fn ordered_cluster_scan_is_safe(
    ctx: &PhysicalContext<'_>,
    meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    leading_cluster_column: &str,
) -> bool {
    use crate::storage::segment_manager::SegmentManager;
    let Some(col_idx) = schema
        .columns
        .iter()
        .position(|c| c.name == leading_cluster_column)
    else {
        return false;
    };
    let table_dir = ctx
        .scan_runtime
        .data_dir()
        .join(&meta.database)
        .join(&meta.name);
    let Ok(segments) = SegmentManager::discover_segments(&table_dir) else {
        return false;
    };
    if segments.len() > 1 {
        let mgr = ctx.scan_runtime.segment_mgr();
        let mut prev_min: Option<crate::types::scalar::ScalarValue> = None;
        let mut prev_max: Option<crate::types::scalar::ScalarValue> = None;
        for segment in &segments {
            // Same accessor the streaming executor's monotonicity guard
            // uses (cluster_meta_cache-backed).
            let Some(zm) = mgr.cluster_zone_map_cached(segment, schema, col_idx) else {
                return false;
            };
            let compare = crate::storage::index::sort_key::compare_scalars_for_sort;
            if let Some(prev) = &prev_min {
                if compare(&zm.min_value, prev) == std::cmp::Ordering::Less {
                    return false;
                }
            }
            if let Some(prev) = &prev_max {
                if compare(&zm.max_value, prev) == std::cmp::Ordering::Less {
                    return false;
                }
            }
            prev_min = Some(zm.min_value);
            prev_max = Some(zm.max_value);
        }
    }
    !ctx.scan_runtime
        .memtable_manager()
        .get_by_table(&meta.database, &meta.name)
        .iter()
        .any(|mt| {
            mt.read_snapshot_arc()
                .is_some_and(|batch| batch.num_rows() > 0)
        })
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

fn annotate_single_scan_with_ordered_limit(
    plan: LogicalPlan,
    sort_keys: Vec<crate::sql::planner::SortKey>,
    limit: usize,
) -> LogicalPlan {
    if single_scan(&plan).is_none() {
        return plan;
    }
    annotate_scan(plan, &sort_keys, limit)
}

fn annotate_single_scan_with_order(
    plan: LogicalPlan,
    sort_keys: Vec<crate::sql::planner::SortKey>,
) -> LogicalPlan {
    if single_scan(&plan).is_none() {
        return plan;
    }
    annotate_scan_order(plan, &sort_keys)
}

fn annotate_scan_order(
    plan: LogicalPlan,
    sort_keys: &[crate::sql::planner::SortKey],
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if scan.pushed_order.is_none() {
                scan.pushed_order = Some(sort_keys.to_vec());
            }
            LogicalPlan::Scan(scan)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(annotate_scan_order(*filter.input, sort_keys));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(annotate_scan_order(*project.input, sort_keys));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Limit(mut limit_plan) => {
            limit_plan.input = Box::new(annotate_scan_order(*limit_plan.input, sort_keys));
            LogicalPlan::Limit(limit_plan)
        }
        other => other,
    }
}

fn annotate_scan(
    plan: LogicalPlan,
    sort_keys: &[crate::sql::planner::SortKey],
    limit: usize,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            scan.pushed_order = Some(sort_keys.to_vec());
            scan.pushed_limit = Some(
                scan.pushed_limit
                    .map_or(limit, |current| current.min(limit)),
            );
            LogicalPlan::Scan(scan)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(annotate_scan(*filter.input, sort_keys, limit));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(annotate_scan(*project.input, sort_keys, limit));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Limit(mut limit_plan) => {
            limit_plan.input = Box::new(annotate_scan(*limit_plan.input, sort_keys, limit));
            LogicalPlan::Limit(limit_plan)
        }
        other => other,
    }
}

/// Schema of a Window node: its input's columns plus one result column per
/// window function.
fn get_scan_schema_from_window(window: &crate::sql::planner::WindowPlan) -> Schema {
    let mut schema = get_scan_schema(&window.input);
    for func in &window.funcs {
        schema.columns.push(ColumnDef::new(
            &func.output_name,
            crate::sql::execution::operators::window::window_result_type(&func.kind, &schema),
        ));
    }
    schema
}

pub(crate) fn get_scan_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_scan_schema(&f.input),
        LogicalPlan::Project(project) => {
            let input = get_scan_schema(&project.input);
            let columns = project
                .expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    let mut column = match expression {
                        LogicalExpr::Column(source) => {
                            input.columns.get(*source).cloned().unwrap_or_else(|| {
                                ColumnDef::new(
                                    &format!("expr_{index}"),
                                    crate::sql::execution::operators::project::infer_type(
                                        expression, &input,
                                    ),
                                )
                            })
                        }
                        _ => ColumnDef::new(
                            &format!("expr_{index}"),
                            crate::sql::execution::operators::project::infer_type(
                                expression, &input,
                            ),
                        ),
                    };
                    if let Some(alias) = project.aliases.get(index).and_then(Clone::clone) {
                        column.name = alias;
                    }
                    column
                })
                .collect();
            Schema::new(columns)
        }
        LogicalPlan::Sort(s) => get_scan_schema(&s.input),
        LogicalPlan::Limit(l) => get_scan_schema(&l.input),
        LogicalPlan::Aggregate(a) => {
            let input = get_scan_schema(&a.input);
            let mut columns = Vec::with_capacity(a.group_by.len() + a.aggregates.len());
            for (position, key) in a.group_by.iter().enumerate() {
                let def = match key {
                    LogicalExpr::Column(index) => input.columns.get(*index).cloned(),
                    _ => None,
                }
                .unwrap_or_else(|| {
                    ColumnDef::new(&format!("group_expr_{position}"), DataType::VarChar)
                });
                columns.push(def);
            }
            for agg in &a.aggregates {
                columns.push(aggregate_result_column_def(agg, &input));
            }
            Schema::new(columns)
        }
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::SemiJoin(s) => s.left_schema.clone(),
        LogicalPlan::TopN(t) => get_scan_schema(&t.input),
        LogicalPlan::Distinct(d) => get_scan_schema(&d.input),
        LogicalPlan::Window(w) => get_scan_schema_from_window(w),
        LogicalPlan::Empty => Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)]),
    }
}

fn materialize_aggregate_arguments(
    mut aggregate: crate::sql::planner::AggregatePlan,
) -> crate::sql::planner::AggregatePlan {
    let input_schema = get_scan_schema(&aggregate.input);
    let mut expressions = (0..input_schema.columns.len())
        .map(LogicalExpr::Column)
        .collect::<Vec<_>>();
    let mut aliases = input_schema
        .columns
        .iter()
        .map(|column| Some(column.name.clone()))
        .collect::<Vec<_>>();
    let mut changed = false;

    for (index, aggregate_expr) in aggregate.aggregates.iter_mut().enumerate() {
        if matches!(
            &*aggregate_expr.arg,
            LogicalExpr::Column(_) | LogicalExpr::Wildcard
        ) {
            continue;
        }
        let column = expressions.len();
        expressions.push((*aggregate_expr.arg).clone());
        aliases.push(Some(format!("__aggregate_expr_{index}")));
        *aggregate_expr.arg = LogicalExpr::Column(column);
        changed = true;
    }

    if changed {
        aggregate.input = Box::new(LogicalPlan::Project(crate::sql::planner::ProjectPlan {
            expressions,
            aliases,
            input: aggregate.input,
        }));
    }
    aggregate
}

/// Collect all column indices referenced by the aggregate's GROUP BY keys and
/// aggregate arguments. Returns None if any expression is non-column (in which
/// case projection pushdown is skipped for safety).
fn collect_aggregate_columns(agg: &crate::sql::planner::AggregatePlan) -> Option<Vec<usize>> {
    let mut cols: Vec<usize> = Vec::new();
    for e in &agg.group_by {
        if let LogicalExpr::Column(i) = e {
            cols.push(*i);
        } else {
            return None;
        }
    }
    for a in &agg.aggregates {
        match &*a.arg {
            LogicalExpr::Column(i) => cols.push(*i),
            LogicalExpr::Wildcard => {}
            _ => return None,
        }
    }
    cols.sort_unstable();
    cols.dedup();
    Some(cols)
}

/// If the aggregate reads directly from a Scan, install a projection on that
/// scan covering only the GROUP BY + aggregate-argument columns. This is only
/// done when the referenced column set is a strict subset of the table schema
/// (i.e. there is something to save) and at least one column is referenced.
fn pushdown_aggregate_projection(
    mut agg: crate::sql::planner::AggregatePlan,
) -> crate::sql::planner::AggregatePlan {
    let total_cols = match &*agg.input {
        LogicalPlan::Scan(s) => s.schema.columns.len(),
        LogicalPlan::Filter(f) => match &*f.input {
            LogicalPlan::Scan(s) => s.schema.columns.len(),
            _ => return agg,
        },
        _ => return agg,
    };
    let Some(cols) = collect_aggregate_columns(&agg) else {
        return agg;
    };
    if cols.is_empty() || cols.len() >= total_cols {
        return agg;
    }
    agg.input = Box::new(set_scan_projection(*agg.input, &cols));
    agg
}

fn set_scan_projection(plan: LogicalPlan, cols: &[usize]) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut s) => {
            if s.projection.is_none() {
                s.projection = Some(cols.to_vec());
            }
            LogicalPlan::Scan(s)
        }
        LogicalPlan::Filter(mut f) => {
            f.input = Box::new(set_scan_projection(*f.input, cols));
            LogicalPlan::Filter(f)
        }
        other => other,
    }
}

/// Try to compile an `Aggregate` over a single table scan into the
/// metadata-folded operator. Declines (returns `None`) for every shape the
/// folding engine cannot own: HAVING, DISTINCT aggregates, expression keys
/// or arguments, and any input that is not a plain scan — residual `Filter`
/// nodes carry predicates `compile_scan_spec` does not extract, so those
/// keep the regular HashAggregate pipeline. WHERE clauses the optimizer
/// pushed into `ScanPlan::pushed_filter` are covered: they land in the spec.
fn try_folded_aggregate_exec(
    agg: &crate::sql::planner::AggregatePlan,
    ctx: &PhysicalContext<'_>,
) -> Option<FoldedAggregateExec> {
    if agg.having.is_some() {
        return None;
    }
    if agg
        .group_by
        .iter()
        .any(|expr| !matches!(expr, LogicalExpr::Column(_)))
    {
        return None;
    }
    for aggregate in &agg.aggregates {
        if aggregate.distinct {
            return None;
        }
        match &*aggregate.arg {
            LogicalExpr::Column(_) | LogicalExpr::Wildcard => {}
            _ => return None,
        }
    }
    // `materialize_aggregate_arguments` inserts a Project for expression
    // arguments — already declined above, but stay defensive: only a bare
    // Scan compiles into a spec whose predicate set is complete.
    let LogicalPlan::Scan(scan) = &*agg.input else {
        return None;
    };
    let scan_name = scan.table_name.clone();
    let table_ref = ctx.table_ref(&scan_name)?;
    let schema = ctx.schema(&scan_name)?.clone();
    let meta = ctx.meta(&scan_name)?.clone();
    let mut spec = compile_scan_spec(&agg.input, &scan_name, &schema, &meta);
    spec.table_name = table_ref.table.clone();
    Some(FoldedAggregateExec::new(
        agg.group_by.clone(),
        agg.aggregates.clone(),
        spec,
        ctx.scan_runtime.clone(),
        schema,
        meta,
        table_ref.database.clone(),
    ))
}

/// Wrap a join child operator so it always presents the FULL planned side
/// schema (see the Join/SemiJoin arms). Kept tiny and `inline`-shaped: for
/// unpruned children this is a schema-equality pass-through.
fn expand_to_schema(
    input: Box<dyn crate::sql::execution::operators::ExecutionOp>,
    target: crate::types::batch::Schema,
) -> Box<dyn crate::sql::execution::operators::ExecutionOp> {
    Box::new(crate::sql::execution::operators::ExpandToSchemaExec::new(
        input, target,
    ))
}
