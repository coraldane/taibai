use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::{aggregate_result_column_def, HashAggregateExec};
use crate::sql::execution::operators::filter::FilterExec;
use crate::sql::execution::operators::join::HashJoinExec;
use crate::sql::execution::operators::limit::LimitExec;
use crate::sql::execution::operators::profile::{ProfiledExec, QueryProfile, ScanProfileSeed};
use crate::sql::execution::operators::project::ProjectExec;
use crate::sql::execution::operators::scan::ScanExec;
use crate::sql::execution::operators::semi_join::SemiJoinExec;
use crate::sql::execution::operators::sort::SortExec;
use crate::sql::execution::operators::ExecutionOp;
use crate::sql::execution::scan::spec::compile_scan_spec;
use crate::sql::optimizer::cost::{ColumnStats, CostEstimator, TableStats};
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::{
    merge_schemas, FilterPlan, JoinPlan, LogicalExpr, LogicalPlan, ProjectPlan,
};
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
    table_batches: &HashMap<String, Vec<RecordBatch>>,
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
    let plan = reorder_inner_join_trees(plan, ctx);
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
    Ok((result_schema.unwrap_or_else(Schema::empty), result_batches))
}

fn build_operator_with_limit(
    plan: LogicalPlan,
    ctx: &mut PhysicalContext<'_>,
    limit_hint: Option<usize>,
    table_batches: Option<&HashMap<String, Vec<RecordBatch>>>,
    scan_seeds: Option<&HashMap<String, ScanProfileSeed>>,
    profile: Option<&QueryProfile>,
    depth: usize,
) -> Result<Box<dyn ExecutionOp>> {
    let operator_profile = profile.map(|profile| {
        let operator = profile.register(
            depth,
            profile_operator_name(&plan),
            estimate_plan_rows(&plan, ctx),
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
            if let Some(table_batches) = table_batches {
                let batches = table_batches
                    .get(&scan.table_name)
                    .cloned()
                    .unwrap_or_default();
                Ok(Box::new(
                    ScanExec::new(batches, scan.projection).with_table_schema(schema),
                ))
            } else {
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
            Ok(Box::new(FilterExec {
                predicate: filter.predicate,
                input,
                schema,
            }))
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
        LogicalPlan::Join(join) => {
            let build_left = if join.join_type == JoinType::Inner {
                estimate_plan_rows(&join.left, ctx) <= estimate_plan_rows(&join.right, ctx)
            } else {
                true
            };
            let left = build_operator_with_limit(
                *join.left,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            let right = build_operator_with_limit(
                *join.right,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
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
            let left = build_operator_with_limit(
                *semi.left,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
            let right = build_operator_with_limit(
                *semi.right,
                ctx,
                None,
                table_batches,
                scan_seeds,
                profile,
                depth + 1,
            )?;
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
            let input = build_operator_with_limit(
                *distinct.input,
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
        LogicalPlan::Window(window) => build_operator_with_limit(
            *window.input,
            ctx,
            None,
            table_batches,
            scan_seeds,
            profile,
            depth + 1,
        ),
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

fn reorder_inner_join_trees(plan: LogicalPlan, ctx: &PhysicalContext<'_>) -> LogicalPlan {
    match plan {
        LogicalPlan::Join(mut join) if join.join_type == JoinType::Inner => {
            reorder_inner_join_tree(LogicalPlan::Join(join.clone()), ctx).unwrap_or_else(|| {
                *join.left = reorder_inner_join_trees(*join.left, ctx);
                *join.right = reorder_inner_join_trees(*join.right, ctx);
                LogicalPlan::Join(join)
            })
        }
        LogicalPlan::Join(mut join) => {
            join.left = Box::new(reorder_inner_join_trees(*join.left, ctx));
            join.right = Box::new(reorder_inner_join_trees(*join.right, ctx));
            LogicalPlan::Join(join)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(reorder_inner_join_trees(*filter.input, ctx));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(reorder_inner_join_trees(*project.input, ctx));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Aggregate(mut aggregate) => {
            aggregate.input = Box::new(reorder_inner_join_trees(*aggregate.input, ctx));
            LogicalPlan::Aggregate(aggregate)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(reorder_inner_join_trees(*sort.input, ctx));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(reorder_inner_join_trees(*limit.input, ctx));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(reorder_inner_join_trees(*topn.input, ctx));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Distinct(mut distinct) => {
            distinct.input = Box::new(reorder_inner_join_trees(*distinct.input, ctx));
            LogicalPlan::Distinct(distinct)
        }
        LogicalPlan::Window(mut window) => {
            window.input = Box::new(reorder_inner_join_trees(*window.input, ctx));
            LogicalPlan::Window(window)
        }
        LogicalPlan::SemiJoin(mut join) => {
            join.left = Box::new(reorder_inner_join_trees(*join.left, ctx));
            join.right = Box::new(reorder_inner_join_trees(*join.right, ctx));
            LogicalPlan::SemiJoin(join)
        }
        other => other,
    }
}

const DP_JOIN_REORDER_LIMIT: usize = 8;

#[derive(Clone)]
struct ReorderLeaf {
    plan: LogicalPlan,
    schema: Schema,
    globals: Vec<usize>,
    rows: u64,
    ndv: HashMap<usize, u64>,
    composite_ndv: Vec<CompositeEstimate>,
    ordinal: usize,
}

#[derive(Clone)]
struct JoinOrderState {
    plan: LogicalPlan,
    schema: Schema,
    globals: Vec<usize>,
    rows: u64,
    ndv: HashMap<usize, u64>,
    composite_ndv: Vec<CompositeEstimate>,
    cost: u128,
    leaf_order: Vec<usize>,
}

fn reorder_inner_join_tree(
    original: LogicalPlan,
    ctx: &PhysicalContext<'_>,
) -> Option<LogicalPlan> {
    let original_schema = get_scan_schema(&original);
    let original_globals = (0..original_schema.columns.len()).collect::<Vec<_>>();
    let mut leaves = Vec::new();
    let mut predicates = Vec::new();
    let mut next_global = 0usize;
    collect_inner_join_tree(
        original.clone(),
        ctx,
        &mut next_global,
        &mut leaves,
        &mut predicates,
    )?;
    if leaves.len() < 3 {
        return None;
    }

    let mut current = if leaves.len() <= DP_JOIN_REORDER_LIMIT {
        dynamic_programming_join_order(&leaves, &predicates)?
    } else {
        greedy_join_order(&leaves, &predicates)?
    };

    let residuals = predicates
        .iter()
        .filter(|predicate| !predicate_spans_multiple_leaves(predicate, &leaves))
        .cloned()
        .collect::<Vec<_>>();
    if !residuals.is_empty() {
        let predicate = combine_and(
            residuals
                .into_iter()
                .map(|predicate| remap_global_expression(predicate, &current.globals))
                .collect(),
        )?;
        current.plan = LogicalPlan::Filter(FilterPlan {
            predicate,
            input: Box::new(current.plan),
        });
    }
    if current.globals == original_globals {
        return Some(current.plan);
    }
    let expressions = original_globals
        .iter()
        .map(|global| {
            LogicalExpr::Column(
                current
                    .globals
                    .iter()
                    .position(|candidate| candidate == global)
                    .expect("join reorder lost output column"),
            )
        })
        .collect();
    let aliases = original_schema
        .columns
        .iter()
        .map(|column| Some(column.name.clone()))
        .collect();
    Some(LogicalPlan::Project(ProjectPlan {
        expressions,
        aliases,
        input: Box::new(current.plan),
    }))
}

fn dynamic_programming_join_order(
    leaves: &[ReorderLeaf],
    predicates: &[LogicalExpr],
) -> Option<JoinOrderState> {
    if leaves.is_empty() || leaves.len() > DP_JOIN_REORDER_LIMIT {
        return None;
    }
    let state_count = 1usize.checked_shl(leaves.len() as u32)?;
    let mut states = vec![None; state_count];
    for (index, leaf) in leaves.iter().enumerate() {
        states[1usize << index] = Some(leaf_state(leaf));
    }

    for mask in 1..state_count {
        let Some(current) = states[mask].clone() else {
            continue;
        };
        for (index, leaf) in leaves.iter().enumerate() {
            let bit = 1usize << index;
            if mask & bit != 0 {
                continue;
            }
            let ready = ready_join_predicates(predicates, &current.globals, &leaf.globals);
            if ready.is_empty() {
                continue;
            }
            let candidate = join_order_state(current.clone(), leaf, &ready)?;
            let slot = &mut states[mask | bit];
            if slot
                .as_ref()
                .map(|existing| join_order_is_better(&candidate, existing))
                .unwrap_or(true)
            {
                *slot = Some(candidate);
            }
        }
    }

    states[state_count - 1].take()
}

fn greedy_join_order(leaves: &[ReorderLeaf], predicates: &[LogicalExpr]) -> Option<JoinOrderState> {
    let first = leaves
        .iter()
        .enumerate()
        .min_by_key(|(_, leaf)| (leaf.rows, leaf.ordinal))
        .map(|(index, _)| index)?;
    let mut used = vec![false; leaves.len()];
    used[first] = true;
    let mut current = leaf_state(&leaves[first]);

    while used.iter().any(|used| !used) {
        let mut best: Option<(usize, JoinOrderState)> = None;
        for (index, leaf) in leaves.iter().enumerate() {
            if used[index] {
                continue;
            }
            let ready = ready_join_predicates(predicates, &current.globals, &leaf.globals);
            if ready.is_empty() {
                continue;
            }
            let candidate = join_order_state(current.clone(), leaf, &ready)?;
            if best
                .as_ref()
                .map(|(_, existing)| join_order_is_better(&candidate, existing))
                .unwrap_or(true)
            {
                best = Some((index, candidate));
            }
        }
        let (index, candidate) = best?;
        used[index] = true;
        current = candidate;
    }

    Some(current)
}

fn leaf_state(leaf: &ReorderLeaf) -> JoinOrderState {
    JoinOrderState {
        plan: leaf.plan.clone(),
        schema: leaf.schema.clone(),
        globals: leaf.globals.clone(),
        rows: leaf.rows,
        ndv: leaf.ndv.clone(),
        composite_ndv: leaf.composite_ndv.clone(),
        cost: leaf.rows as u128,
        leaf_order: vec![leaf.ordinal],
    }
}

fn join_order_state(
    left: JoinOrderState,
    right: &ReorderLeaf,
    predicates: &[&LogicalExpr],
) -> Option<JoinOrderState> {
    let mut globals = left.globals.clone();
    globals.extend(right.globals.iter().copied());
    let condition = combine_and(
        predicates
            .iter()
            .map(|predicate| remap_global_expression((*predicate).clone(), &globals))
            .collect(),
    )?;
    let rows = estimate_join_cardinality(
        left.rows,
        right.rows,
        predicates,
        &left.ndv,
        &right.ndv,
        &left.composite_ndv,
        &right.composite_ndv,
        &left.globals,
        &right.globals,
    );
    let ndv = merge_join_ndv(
        rows,
        predicates,
        &left.ndv,
        &right.ndv,
        &left.globals,
        &right.globals,
    );
    let composite_ndv = merge_join_composite_ndv(
        rows,
        predicates,
        &left.composite_ndv,
        &right.composite_ndv,
        &left.globals,
        &right.globals,
    );
    let output_schema = merge_schemas(&left.schema, &right.schema);
    let mut leaf_order = left.leaf_order;
    leaf_order.push(right.ordinal);
    Some(JoinOrderState {
        plan: LogicalPlan::Join(JoinPlan {
            left: Box::new(left.plan),
            right: Box::new(right.plan.clone()),
            join_type: JoinType::Inner,
            condition,
            left_schema: left.schema,
            right_schema: right.schema.clone(),
            output_schema: output_schema.clone(),
        }),
        schema: output_schema,
        globals,
        rows,
        ndv,
        composite_ndv,
        cost: left
            .cost
            .saturating_add(right.rows as u128)
            .saturating_add(rows as u128),
        leaf_order,
    })
}

fn join_order_is_better(candidate: &JoinOrderState, existing: &JoinOrderState) -> bool {
    (candidate.cost, candidate.rows, &candidate.leaf_order)
        < (existing.cost, existing.rows, &existing.leaf_order)
}

fn ready_join_predicates<'a>(
    predicates: &'a [LogicalExpr],
    left: &[usize],
    right: &[usize],
) -> Vec<&'a LogicalExpr> {
    let mut joined = left.to_vec();
    joined.extend(right.iter().copied());
    predicates
        .iter()
        .filter(|predicate| {
            predicate_connects(predicate, left, right)
                && expression_globals(predicate)
                    .iter()
                    .all(|global| joined.contains(global))
        })
        .collect()
}

fn predicate_spans_multiple_leaves(predicate: &LogicalExpr, leaves: &[ReorderLeaf]) -> bool {
    let globals = expression_globals(predicate);
    leaves
        .iter()
        .filter(|leaf| globals.iter().any(|global| leaf.globals.contains(global)))
        .take(2)
        .count()
        >= 2
}

#[allow(clippy::too_many_arguments)]
fn estimate_join_cardinality(
    left_rows: u64,
    right_rows: u64,
    predicates: &[&LogicalExpr],
    left_ndv: &HashMap<usize, u64>,
    right_ndv: &HashMap<usize, u64>,
    left_composite: &[CompositeEstimate],
    right_composite: &[CompositeEstimate],
    left_globals: &[usize],
    right_globals: &[usize],
) -> u64 {
    if left_rows == 0 || right_rows == 0 {
        return 0;
    }
    let mut denominator = 1u128;
    let mut equality_pairs = Vec::new();
    let mut oriented_pairs = Vec::new();
    for predicate in predicates {
        let Some((first, second)) = equality_column_pair(predicate) else {
            continue;
        };
        if left_globals.contains(&first) && right_globals.contains(&second) {
            oriented_pairs.push((first, second));
        } else if left_globals.contains(&second) && right_globals.contains(&first) {
            oriented_pairs.push((second, first));
        }
    }

    for group in left_composite
        .iter()
        .filter(|group| group.columns.len() > 1)
    {
        let Some(mapped) = map_composite_columns(&group.columns, &oriented_pairs, true) else {
            continue;
        };
        if group
            .columns
            .iter()
            .zip(&mapped)
            .any(|(left, right)| equality_pairs.contains(&normalized_pair(*left, *right)))
        {
            continue;
        }
        let other_ndv = find_composite_ndv(right_composite, &mapped).unwrap_or_else(|| {
            mapped
                .iter()
                .fold(1u64, |product, column| {
                    product.saturating_mul(right_ndv.get(column).copied().unwrap_or(10).max(1))
                })
                .min(right_rows)
        });
        denominator = denominator.saturating_mul(group.ndv.max(other_ndv).max(1) as u128);
        for (left, right) in group.columns.iter().zip(mapped) {
            equality_pairs.push(normalized_pair(*left, right));
        }
    }
    for group in right_composite
        .iter()
        .filter(|group| group.columns.len() > 1)
    {
        let Some(mapped) = map_composite_columns(&group.columns, &oriented_pairs, false) else {
            continue;
        };
        if mapped
            .iter()
            .zip(&group.columns)
            .any(|(left, right)| equality_pairs.contains(&normalized_pair(*left, *right)))
        {
            continue;
        }
        let other_ndv = find_composite_ndv(left_composite, &mapped).unwrap_or_else(|| {
            mapped
                .iter()
                .fold(1u64, |product, column| {
                    product.saturating_mul(left_ndv.get(column).copied().unwrap_or(10).max(1))
                })
                .min(left_rows)
        });
        denominator = denominator.saturating_mul(group.ndv.max(other_ndv).max(1) as u128);
        for (left, right) in mapped.into_iter().zip(&group.columns) {
            equality_pairs.push(normalized_pair(left, *right));
        }
    }
    for predicate in predicates {
        let factor = equality_column_pair(predicate)
            .filter(|(left, right)| columns_cross_sides(*left, *right, left_globals, right_globals))
            .map(|(left, right)| {
                let pair = normalized_pair(left, right);
                if equality_pairs.contains(&pair) {
                    return 1;
                }
                equality_pairs.push(pair);
                left_ndv
                    .get(&left)
                    .or_else(|| right_ndv.get(&left))
                    .copied()
                    .unwrap_or(10)
                    .max(
                        left_ndv
                            .get(&right)
                            .or_else(|| right_ndv.get(&right))
                            .copied()
                            .unwrap_or(10),
                    )
                    .max(1)
            })
            .unwrap_or(10);
        denominator = denominator.saturating_mul(factor as u128);
    }
    let cross = (left_rows as u128).saturating_mul(right_rows as u128);
    let estimated = cross.div_ceil(denominator.max(1)).max(1).min(cross);
    let mut estimated = estimated.min(u64::MAX as u128) as u64;
    if left_composite
        .iter()
        .any(|group| group.unique && composite_group_is_joined(group, &oriented_pairs, true))
    {
        estimated = estimated.min(right_rows);
    }
    if right_composite
        .iter()
        .any(|group| group.unique && composite_group_is_joined(group, &oriented_pairs, false))
    {
        estimated = estimated.min(left_rows);
    }
    estimated
}

fn normalized_pair(first: usize, second: usize) -> (usize, usize) {
    if first < second {
        (first, second)
    } else {
        (second, first)
    }
}

fn map_composite_columns(
    columns: &[usize],
    pairs: &[(usize, usize)],
    from_left: bool,
) -> Option<Vec<usize>> {
    columns
        .iter()
        .map(|column| {
            pairs.iter().find_map(|(left, right)| {
                if from_left && left == column {
                    Some(*right)
                } else if !from_left && right == column {
                    Some(*left)
                } else {
                    None
                }
            })
        })
        .collect()
}

fn find_composite_ndv(groups: &[CompositeEstimate], columns: &[usize]) -> Option<u64> {
    let mut expected = columns.to_vec();
    expected.sort_unstable();
    groups.iter().find_map(|group| {
        let mut candidate = group.columns.clone();
        candidate.sort_unstable();
        (candidate == expected).then_some(group.ndv)
    })
}

fn composite_group_is_joined(
    group: &CompositeEstimate,
    pairs: &[(usize, usize)],
    on_left: bool,
) -> bool {
    !group.columns.is_empty() && map_composite_columns(&group.columns, pairs, on_left).is_some()
}

fn merge_join_ndv(
    rows: u64,
    predicates: &[&LogicalExpr],
    left_ndv: &HashMap<usize, u64>,
    right_ndv: &HashMap<usize, u64>,
    left_globals: &[usize],
    right_globals: &[usize],
) -> HashMap<usize, u64> {
    let mut result = left_ndv.clone();
    result.extend(right_ndv.iter().map(|(column, ndv)| (*column, *ndv)));
    for predicate in predicates {
        let Some((left, right)) = equality_column_pair(predicate).filter(|(left, right)| {
            columns_cross_sides(*left, *right, left_globals, right_globals)
        }) else {
            continue;
        };
        let joined_ndv = result
            .get(&left)
            .copied()
            .unwrap_or(rows)
            .min(result.get(&right).copied().unwrap_or(rows))
            .min(rows);
        result.insert(left, joined_ndv);
        result.insert(right, joined_ndv);
    }
    for ndv in result.values_mut() {
        *ndv = (*ndv).min(rows);
    }
    result
}

fn merge_join_composite_ndv(
    rows: u64,
    predicates: &[&LogicalExpr],
    left: &[CompositeEstimate],
    right: &[CompositeEstimate],
    left_globals: &[usize],
    right_globals: &[usize],
) -> Vec<CompositeEstimate> {
    let mut result = left
        .iter()
        .chain(right)
        .cloned()
        .map(|mut group| {
            group.ndv = group.ndv.min(rows);
            group.unique = false;
            group
        })
        .collect::<Vec<_>>();
    let pairs = predicates
        .iter()
        .filter_map(|predicate| equality_column_pair(predicate))
        .filter_map(|(first, second)| {
            if left_globals.contains(&first) && right_globals.contains(&second) {
                Some((first, second))
            } else if left_globals.contains(&second) && right_globals.contains(&first) {
                Some((second, first))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    for left_group in left {
        let Some(mapped) = map_composite_columns(&left_group.columns, &pairs, true) else {
            continue;
        };
        let Some(right_ndv) = find_composite_ndv(right, &mapped) else {
            continue;
        };
        let joined_ndv = left_group.ndv.min(right_ndv).min(rows);
        for group in &mut result {
            if same_column_set(&group.columns, &left_group.columns)
                || same_column_set(&group.columns, &mapped)
            {
                group.ndv = joined_ndv;
            }
        }
    }
    result
}

fn same_column_set(first: &[usize], second: &[usize]) -> bool {
    let mut first = first.to_vec();
    let mut second = second.to_vec();
    first.sort_unstable();
    second.sort_unstable();
    first == second
}

fn equality_column_pair(expression: &LogicalExpr) -> Option<(usize, usize)> {
    let LogicalExpr::BinaryOp {
        left,
        op: crate::sql::parser::ast::BinOp::Eq,
        right,
    } = expression
    else {
        return None;
    };
    match (&**left, &**right) {
        (LogicalExpr::Column(left), LogicalExpr::Column(right)) => Some((*left, *right)),
        _ => None,
    }
}

fn columns_cross_sides(first: usize, second: usize, left: &[usize], right: &[usize]) -> bool {
    (left.contains(&first) && right.contains(&second))
        || (left.contains(&second) && right.contains(&first))
}

fn collect_inner_join_tree(
    plan: LogicalPlan,
    ctx: &PhysicalContext<'_>,
    next_global: &mut usize,
    leaves: &mut Vec<ReorderLeaf>,
    predicates: &mut Vec<LogicalExpr>,
) -> Option<Vec<usize>> {
    match plan {
        LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
            let left = collect_inner_join_tree(*join.left, ctx, next_global, leaves, predicates)?;
            let right = collect_inner_join_tree(*join.right, ctx, next_global, leaves, predicates)?;
            let mut mapping = left;
            mapping.extend(right);
            let global_condition = remap_expression_columns(join.condition, &mapping)?;
            flatten_and_expression(global_condition, predicates);
            Some(mapping)
        }
        leaf => {
            let schema = get_scan_schema(&leaf);
            let start = *next_global;
            *next_global = next_global.saturating_add(schema.columns.len());
            let globals = (start..*next_global).collect::<Vec<_>>();
            let estimate = estimate_plan_profile(&leaf, ctx);
            let rows = estimate.rows.max(1);
            let ndv = estimate
                .ndv
                .into_iter()
                .enumerate()
                .filter_map(|(local, ndv)| Some((globals.get(local).copied()?, ndv?)))
                .collect();
            let composite_ndv = estimate
                .composite_ndv
                .into_iter()
                .filter_map(|estimate| {
                    Some(CompositeEstimate {
                        columns: estimate
                            .columns
                            .into_iter()
                            .map(|local| globals.get(local).copied())
                            .collect::<Option<Vec<_>>>()?,
                        ndv: estimate.ndv,
                        unique: estimate.unique,
                    })
                })
                .collect();
            leaves.push(ReorderLeaf {
                plan: leaf,
                schema,
                globals: globals.clone(),
                rows,
                ndv,
                composite_ndv,
                ordinal: leaves.len(),
            });
            Some(globals)
        }
    }
}

fn flatten_and_expression(expression: LogicalExpr, output: &mut Vec<LogicalExpr>) {
    match expression {
        LogicalExpr::BinaryOp {
            left,
            op: crate::sql::parser::ast::BinOp::And,
            right,
        } => {
            flatten_and_expression(*left, output);
            flatten_and_expression(*right, output);
        }
        expression => output.push(expression),
    }
}

fn combine_and(mut expressions: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut result = expressions.pop()?;
    while let Some(expression) = expressions.pop() {
        result = LogicalExpr::BinaryOp {
            left: Box::new(expression),
            op: crate::sql::parser::ast::BinOp::And,
            right: Box::new(result),
        };
    }
    Some(result)
}

fn predicate_connects(predicate: &LogicalExpr, left: &[usize], right: &[usize]) -> bool {
    let globals = expression_globals(predicate);
    globals.iter().any(|global| left.contains(global))
        && globals.iter().any(|global| right.contains(global))
}

fn expression_globals(expression: &LogicalExpr) -> Vec<usize> {
    let mut globals = Vec::new();
    collect_expression_columns(expression, &mut globals);
    globals.sort_unstable();
    globals.dedup();
    globals
}

fn collect_expression_columns(expression: &LogicalExpr, output: &mut Vec<usize>) {
    match expression {
        LogicalExpr::Column(column) => output.push(*column),
        LogicalExpr::BinaryOp { left, right, .. } => {
            collect_expression_columns(left, output);
            collect_expression_columns(right, output);
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            collect_expression_columns(inner, output)
        }
        LogicalExpr::Aggregate(aggregate) => collect_expression_columns(&aggregate.arg, output),
        LogicalExpr::In { expr, .. } | LogicalExpr::Like { expr, .. } => {
            collect_expression_columns(expr, output)
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_expression_columns(expr, output);
            collect_expression_columns(low, output);
            collect_expression_columns(high, output);
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expression_columns(operand, output);
            }
            for (when, then) in when_clauses {
                collect_expression_columns(when, output);
                collect_expression_columns(then, output);
            }
            if let Some(otherwise) = else_clause {
                collect_expression_columns(otherwise, output);
            }
        }
        LogicalExpr::Function { args, .. } => {
            for argument in args {
                collect_expression_columns(argument, output);
            }
        }
        _ => {}
    }
}

fn remap_global_expression(expression: LogicalExpr, globals: &[usize]) -> LogicalExpr {
    let mapping = (0..globals.len())
        .map(|local| (globals[local], local))
        .collect::<std::collections::HashMap<_, _>>();
    remap_expression_with_lookup(expression, &mapping)
}

fn remap_expression_columns(expression: LogicalExpr, mapping: &[usize]) -> Option<LogicalExpr> {
    let lookup = mapping
        .iter()
        .enumerate()
        .map(|(local, global)| (local, *global))
        .collect::<std::collections::HashMap<_, _>>();
    Some(remap_expression_with_lookup(expression, &lookup))
}

fn remap_expression_with_lookup(
    expression: LogicalExpr,
    lookup: &std::collections::HashMap<usize, usize>,
) -> LogicalExpr {
    match expression {
        LogicalExpr::Column(column) => {
            LogicalExpr::Column(lookup.get(&column).copied().unwrap_or(column))
        }
        LogicalExpr::BinaryOp { left, op, right } => LogicalExpr::BinaryOp {
            left: Box::new(remap_expression_with_lookup(*left, lookup)),
            op,
            right: Box::new(remap_expression_with_lookup(*right, lookup)),
        },
        LogicalExpr::Not(inner) => {
            LogicalExpr::Not(Box::new(remap_expression_with_lookup(*inner, lookup)))
        }
        LogicalExpr::IsNull(inner) => {
            LogicalExpr::IsNull(Box::new(remap_expression_with_lookup(*inner, lookup)))
        }
        LogicalExpr::IsNotNull(inner) => {
            LogicalExpr::IsNotNull(Box::new(remap_expression_with_lookup(*inner, lookup)))
        }
        LogicalExpr::Aggregate(mut aggregate) => {
            aggregate.arg = Box::new(remap_expression_with_lookup(*aggregate.arg, lookup));
            LogicalExpr::Aggregate(aggregate)
        }
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => LogicalExpr::In {
            expr: Box::new(remap_expression_with_lookup(*expr, lookup)),
            values,
            negated,
        },
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => LogicalExpr::Between {
            expr: Box::new(remap_expression_with_lookup(*expr, lookup)),
            low: Box::new(remap_expression_with_lookup(*low, lookup)),
            high: Box::new(remap_expression_with_lookup(*high, lookup)),
            negated,
        },
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => LogicalExpr::Case {
            operand: operand
                .map(|operand| Box::new(remap_expression_with_lookup(*operand, lookup))),
            when_clauses: when_clauses
                .into_iter()
                .map(|(when, then)| {
                    (
                        remap_expression_with_lookup(when, lookup),
                        remap_expression_with_lookup(then, lookup),
                    )
                })
                .collect(),
            else_clause: else_clause
                .map(|otherwise| Box::new(remap_expression_with_lookup(*otherwise, lookup))),
        },
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => LogicalExpr::Like {
            expr: Box::new(remap_expression_with_lookup(*expr, lookup)),
            pattern,
            negated,
        },
        LogicalExpr::Function { name, args } => LogicalExpr::Function {
            name,
            args: args
                .into_iter()
                .map(|argument| remap_expression_with_lookup(argument, lookup))
                .collect(),
        },
        other => other,
    }
}

#[derive(Clone)]
struct PlanEstimate {
    rows: u64,
    ndv: Vec<Option<u64>>,
    composite_ndv: Vec<CompositeEstimate>,
}

#[derive(Clone, Debug)]
struct CompositeEstimate {
    columns: Vec<usize>,
    ndv: u64,
    unique: bool,
}

pub(crate) fn estimate_plan_rows(plan: &LogicalPlan, ctx: &PhysicalContext<'_>) -> u64 {
    estimate_plan_profile(plan, ctx).rows
}

fn estimate_plan_profile(plan: &LogicalPlan, ctx: &PhysicalContext<'_>) -> PlanEstimate {
    match plan {
        LogicalPlan::Scan(scan) => {
            let Some(meta) = ctx.meta(&scan.table_name) else {
                return PlanEstimate {
                    rows: u64::MAX / 4,
                    ndv: vec![None; scan.schema.columns.len()],
                    composite_ndv: Vec::new(),
                };
            };
            let valid_statistics = meta.valid_statistics();
            let column_stats = valid_statistics
                .map(|statistics| {
                    statistics
                        .columns
                        .iter()
                        .map(|(name, statistics)| {
                            (
                                name.clone(),
                                ColumnStats {
                                    distinct_count: statistics.distinct_count,
                                    null_count: statistics.null_count,
                                    min_value: statistics.min_value.clone(),
                                    max_value: statistics.max_value.clone(),
                                    histogram: statistics.histogram.clone(),
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            let estimator = CostEstimator::new(TableStats {
                row_count: meta.row_count,
                schema: scan.schema.clone(),
                column_stats,
            });
            let rows = estimator.estimate_rows(scan.pushed_filter.as_ref());
            let ndv = scan
                .schema
                .columns
                .iter()
                .map(|column| {
                    valid_statistics
                        .and_then(|statistics| statistics.columns.get(&column.name))
                        .map(|statistics| statistics.distinct_count.max(0) as u64)
                        .map(|ndv| ndv.min(rows))
                })
                .collect();
            let composite_ndv = scan_composite_estimates(meta, scan, rows, valid_statistics);
            PlanEstimate {
                rows,
                ndv,
                composite_ndv,
            }
        }
        LogicalPlan::Filter(filter) => {
            let input = estimate_plan_profile(&filter.input, ctx);
            let schema = get_scan_schema(&filter.input);
            let column_stats = schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(index, column)| {
                    Some((
                        column.name.clone(),
                        ColumnStats {
                            distinct_count: input.ndv.get(index).copied().flatten()? as i64,
                            null_count: 0,
                            min_value: None,
                            max_value: None,
                            histogram: Vec::new(),
                        },
                    ))
                })
                .collect();
            let estimator = CostEstimator::new(TableStats {
                row_count: input.rows.min(i64::MAX as u64) as i64,
                schema,
                column_stats,
            });
            let rows = estimator.estimate_rows(Some(&filter.predicate));
            PlanEstimate {
                rows,
                ndv: input
                    .ndv
                    .into_iter()
                    .map(|ndv| ndv.map(|ndv| ndv.min(rows)))
                    .collect(),
                composite_ndv: input
                    .composite_ndv
                    .into_iter()
                    .map(|mut group| {
                        group.ndv = group.ndv.min(rows);
                        group
                    })
                    .collect(),
            }
        }
        LogicalPlan::Project(project) => {
            let input = estimate_plan_profile(&project.input, ctx);
            let source_to_output = project
                .expressions
                .iter()
                .enumerate()
                .filter_map(|(output, expression)| match expression {
                    LogicalExpr::Column(source) => Some((*source, output)),
                    _ => None,
                })
                .collect::<HashMap<_, _>>();
            let ndv = project
                .expressions
                .iter()
                .map(|expression| match expression {
                    LogicalExpr::Column(column) => input.ndv.get(*column).copied().flatten(),
                    _ => None,
                })
                .collect();
            let composite_ndv = input
                .composite_ndv
                .into_iter()
                .filter_map(|group| {
                    Some(CompositeEstimate {
                        columns: group
                            .columns
                            .into_iter()
                            .map(|column| source_to_output.get(&column).copied())
                            .collect::<Option<Vec<_>>>()?,
                        ndv: group.ndv,
                        unique: group.unique,
                    })
                })
                .collect();
            PlanEstimate {
                rows: input.rows,
                ndv,
                composite_ndv,
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            let input = estimate_plan_profile(&aggregate.input, ctx);
            let rows = if aggregate.group_by.is_empty() {
                1
            } else {
                input.rows.div_ceil(10).max(1)
            };
            PlanEstimate {
                rows,
                ndv: vec![None; get_scan_schema(plan).columns.len()],
                composite_ndv: Vec::new(),
            }
        }
        LogicalPlan::Sort(sort) => estimate_plan_profile(&sort.input, ctx),
        LogicalPlan::Limit(limit) => {
            let mut input = estimate_plan_profile(&limit.input, ctx);
            input.rows = input
                .rows
                .min(limit.limit.saturating_add(limit.offset) as u64);
            for ndv in input.ndv.iter_mut().flatten() {
                *ndv = (*ndv).min(input.rows);
            }
            for group in &mut input.composite_ndv {
                group.ndv = group.ndv.min(input.rows);
            }
            input
        }
        LogicalPlan::TopN(topn) => {
            let mut input = estimate_plan_profile(&topn.input, ctx);
            input.rows = input
                .rows
                .min(topn.limit.saturating_add(topn.offset) as u64);
            for ndv in input.ndv.iter_mut().flatten() {
                *ndv = (*ndv).min(input.rows);
            }
            for group in &mut input.composite_ndv {
                group.ndv = group.ndv.min(input.rows);
            }
            input
        }
        LogicalPlan::Join(join) => {
            let left = estimate_plan_profile(&join.left, ctx);
            let right = estimate_plan_profile(&join.right, ctx);
            let left_globals = (0..left.ndv.len()).collect::<Vec<_>>();
            let right_globals =
                (left.ndv.len()..left.ndv.len() + right.ndv.len()).collect::<Vec<_>>();
            let left_ndv = left
                .ndv
                .iter()
                .enumerate()
                .filter_map(|(column, ndv)| Some((column, (*ndv)?)))
                .collect::<HashMap<_, _>>();
            let right_ndv = right
                .ndv
                .iter()
                .enumerate()
                .filter_map(|(column, ndv)| Some((left.ndv.len() + column, (*ndv)?)))
                .collect::<HashMap<_, _>>();
            let left_composite = left.composite_ndv.clone();
            let right_composite = right
                .composite_ndv
                .iter()
                .cloned()
                .map(|mut group| {
                    for column in &mut group.columns {
                        *column += left.ndv.len();
                    }
                    group
                })
                .collect::<Vec<_>>();
            let mut conjuncts = Vec::new();
            flatten_and_refs(&join.condition, &mut conjuncts);
            let connecting = conjuncts
                .into_iter()
                .filter(|predicate| predicate_connects(predicate, &left_globals, &right_globals))
                .collect::<Vec<_>>();
            let inner_rows = if connecting.is_empty() {
                left.rows.saturating_mul(right.rows)
            } else {
                estimate_join_cardinality(
                    left.rows,
                    right.rows,
                    &connecting,
                    &left_ndv,
                    &right_ndv,
                    &left_composite,
                    &right_composite,
                    &left_globals,
                    &right_globals,
                )
            };
            let rows = match join.join_type {
                JoinType::Inner => inner_rows,
                JoinType::Left => inner_rows.max(left.rows),
                JoinType::Right => inner_rows.max(right.rows),
                JoinType::Full => inner_rows.max(left.rows).max(right.rows),
                JoinType::Cross => left.rows.saturating_mul(right.rows),
                JoinType::Semi | JoinType::Anti | JoinType::Asof => left.rows,
            };
            let merged = merge_join_ndv(
                rows,
                &connecting,
                &left_ndv,
                &right_ndv,
                &left_globals,
                &right_globals,
            );
            let composite_ndv = merge_join_composite_ndv(
                rows,
                &connecting,
                &left_composite,
                &right_composite,
                &left_globals,
                &right_globals,
            );
            PlanEstimate {
                rows,
                ndv: (0..left.ndv.len() + right.ndv.len())
                    .map(|column| merged.get(&column).copied())
                    .collect(),
                composite_ndv,
            }
        }
        LogicalPlan::SemiJoin(join) => {
            let mut left = estimate_plan_profile(&join.left, ctx);
            for ndv in left.ndv.iter_mut().flatten() {
                *ndv = (*ndv).min(left.rows);
            }
            left
        }
        LogicalPlan::Distinct(distinct) => {
            let mut input = estimate_plan_profile(&distinct.input, ctx);
            input.rows = input.rows.div_ceil(2);
            for ndv in input.ndv.iter_mut().flatten() {
                *ndv = (*ndv).min(input.rows);
            }
            for group in &mut input.composite_ndv {
                group.ndv = group.ndv.min(input.rows);
                group.unique = false;
            }
            input
        }
        LogicalPlan::Window(window) => estimate_plan_profile(&window.input, ctx),
        LogicalPlan::Empty => PlanEstimate {
            rows: 0,
            ndv: Vec::new(),
            composite_ndv: Vec::new(),
        },
    }
}

fn scan_composite_estimates(
    meta: &crate::storage::catalog::schema::TableMeta,
    scan: &crate::sql::planner::ScanPlan,
    rows: u64,
    statistics: Option<&crate::storage::catalog::schema::TableStatisticsMeta>,
) -> Vec<CompositeEstimate> {
    let resolve_columns = |columns: &[String]| {
        columns
            .iter()
            .map(|column| {
                scan.schema
                    .columns
                    .iter()
                    .position(|candidate| candidate.name.eq_ignore_ascii_case(column))
            })
            .collect::<Option<Vec<_>>>()
    };
    let mut groups = statistics
        .into_iter()
        .flat_map(|statistics| &statistics.composite_columns)
        .filter_map(|statistics| {
            Some(CompositeEstimate {
                columns: resolve_columns(&statistics.columns)?,
                ndv: (statistics.distinct_count.max(0) as u64).min(rows),
                unique: statistics.unique,
            })
        })
        .collect::<Vec<_>>();

    let primary_key = meta
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let unique_groups = std::iter::once(primary_key)
        .chain(
            meta.unique_indexes
                .iter()
                .map(|index| index.columns.clone()),
        )
        .filter(|columns| !columns.is_empty());
    for columns in unique_groups {
        let Some(indices) = resolve_columns(&columns) else {
            continue;
        };
        if let Some(existing) = groups
            .iter_mut()
            .find(|group| same_column_set(&group.columns, &indices))
        {
            existing.unique = true;
            existing.ndv = existing.ndv.min(rows);
        } else {
            groups.push(CompositeEstimate {
                columns: indices,
                ndv: rows,
                unique: true,
            });
        }
    }
    groups
}

fn flatten_and_refs<'a>(expression: &'a LogicalExpr, output: &mut Vec<&'a LogicalExpr>) {
    match expression {
        LogicalExpr::BinaryOp {
            left,
            op: crate::sql::parser::ast::BinOp::And,
            right,
        } => {
            flatten_and_refs(left, output);
            flatten_and_refs(right, output);
        }
        expression => output.push(expression),
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
        LogicalPlan::Window(w) => get_scan_schema(&w.input),
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

#[cfg(test)]
#[path = "join_order_test.rs"]
mod join_order_tests;
