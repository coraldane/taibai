//! Decorrelation of correlated subqueries into semi / anti joins.
//!
//! Runs on the optimized plan before pipeline construction. Top-level WHERE
//! conjuncts that are bound correlated markers (`EXISTS` / `IN`) and whose
//! inner query is a plain scan/filter are rewritten into `SemiJoin` nodes:
//!
//! ```text
//! Filter(EXISTS(SELECT 1 FROM b WHERE b.k = a.k AND b.qty > 6) AND other)
//! ─►
//! Filter(other)
//!   SemiJoin(left = a, right = SELECT k FROM b WHERE qty > 6, cond: a.k = b.k)
//! ```
//!
//! Markers whose shape is not supported (scalar subqueries, inner aggregates
//! / GROUP BY / LIMIT, untranslatable expressions) are left in place and
//! keep evaluating per-row via the subquery runtime, so this pass is purely
//! a performance rewrite and can never change results.
//!
//! Right-side scans are renamed to `__corr_{id}__{table}` pseudo tables with
//! their own copies of the snapshot batches. This isolates them from the
//! outer query's scans of the same table (which share `table_batches` keys
//! and scan specs), so self-correlated subqueries work correctly.

use std::collections::HashMap;

use crate::sql::optimizer::optimize;
use crate::sql::parser::ast::{BinOp, CorrelatedKind, Expr, SelectItem, Statement};
use crate::sql::planner::{
    build_scope_bindings, CorrelatedSubqueryPlan, LogicalExpr, LogicalPlan, SemiJoinPlan,
};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::subquery_runtime::SubqueryRegistry;

pub(crate) struct DecorrelateOutcome {
    pub plan: LogicalPlan,
    /// Snapshot batches staged under the namespaced pseudo table names.
    pub extra_batches: HashMap<String, Vec<RecordBatch>>,
}

/// Rewrite correlated markers into semi / anti joins where possible.
/// Best-effort: unsupported shapes are left untouched for the per-row path.
pub(crate) fn decorrelate(plan: LogicalPlan, registry: &SubqueryRegistry) -> DecorrelateOutcome {
    let mut ctx = Ctx {
        registry,
        extra_batches: HashMap::new(),
    };
    let plan = rewrite_plan(plan, &mut ctx);
    DecorrelateOutcome {
        plan,
        extra_batches: ctx.extra_batches,
    }
}

struct Ctx<'a> {
    registry: &'a SubqueryRegistry,
    extra_batches: HashMap<String, Vec<RecordBatch>>,
}

fn rewrite_plan(plan: LogicalPlan, ctx: &mut Ctx) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => rewrite_filter(*filter.input, filter.predicate, ctx),
        LogicalPlan::Project(mut proj) => {
            proj.input = Box::new(rewrite_plan(*proj.input, ctx));
            LogicalPlan::Project(proj)
        }
        LogicalPlan::Aggregate(mut agg) => {
            agg.input = Box::new(rewrite_plan(*agg.input, ctx));
            LogicalPlan::Aggregate(agg)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(rewrite_plan(*sort.input, ctx));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(rewrite_plan(*limit.input, ctx));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(rewrite_plan(*topn.input, ctx));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Distinct(mut distinct) => {
            distinct.input = Box::new(rewrite_plan(*distinct.input, ctx));
            LogicalPlan::Distinct(distinct)
        }
        LogicalPlan::Window(mut window) => {
            window.input = Box::new(rewrite_plan(*window.input, ctx));
            LogicalPlan::Window(window)
        }
        LogicalPlan::Join(mut join) => {
            join.left = Box::new(rewrite_plan(*join.left, ctx));
            join.right = Box::new(rewrite_plan(*join.right, ctx));
            LogicalPlan::Join(join)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            semi.left = Box::new(rewrite_plan(*semi.left, ctx));
            semi.right = Box::new(rewrite_plan(*semi.right, ctx));
            LogicalPlan::SemiJoin(semi)
        }
        other => other,
    }
}

fn rewrite_filter(input: LogicalPlan, predicate: LogicalExpr, ctx: &mut Ctx) -> LogicalPlan {
    let input = rewrite_plan(input, ctx);
    let conjuncts = split_and_conjuncts(&predicate);
    let has_markers = conjuncts.iter().any(|c| {
        matches!(c, LogicalExpr::CorrelatedSubquery(_)) || single_scalar_marker(c).is_some()
    });
    if !has_markers {
        return LogicalPlan::Filter(crate::sql::planner::FilterPlan {
            predicate,
            input: Box::new(input),
        });
    }

    // The left side's column space must match the one the marker's `params`
    // were bound against (the query block's FROM schema). When it cannot be
    // determined — e.g. a HAVING filter above an Aggregate — every marker
    // here is kept and stays on the per-row path.
    let left_schema = plan_output_schema(&input);
    let mut left = input;
    // The current column space below the (yet to be built) filter. Semi
    // joins preserve it; scalar rewrites append their right side.
    let mut left_schema = left_schema;
    let mut kept: Vec<LogicalExpr> = Vec::new();

    for conjunct in conjuncts {
        // Whole-conjunct EXISTS / IN markers take the semi join path.
        let semi_candidate = match &conjunct {
            LogicalExpr::CorrelatedSubquery(node) if node.kind != CorrelatedKind::Scalar => {
                Some(node)
            }
            _ => None,
        };
        if let Some(node) = semi_candidate {
            let decorrelated = left_schema
                .as_ref()
                .and_then(|schema| try_decorrelate(node, schema, ctx));
            match decorrelated {
                Some((right, right_schema, condition, is_anti)) => {
                    left = LogicalPlan::SemiJoin(SemiJoinPlan {
                        left: Box::new(left),
                        right: Box::new(right),
                        condition,
                        left_schema: left_schema.clone().unwrap_or_else(Schema::empty),
                        right_schema,
                        is_anti,
                    });
                    continue;
                }
                None => {
                    kept.push(conjunct);
                    continue;
                }
            }
        }

        // Scalar markers — either alone (used as a boolean) or nested inside
        // a comparison — become a LEFT JOIN against the inner query grouped
        // by its correlation keys, with the marker replaced by the joined
        // aggregate column.
        let scalar_marker = single_scalar_marker(&conjunct);
        if let Some(node) = scalar_marker {
            let scalar_rewrite = left_schema.as_ref().and_then(|current_schema| {
                try_decorrelate_scalar(node, current_schema.columns.len(), ctx)
            });
            if let (Some(current_schema), Some(rewrite)) = (left_schema.as_ref(), scalar_rewrite) {
                let (right, right_schema, join_condition, agg_pos, is_count) = rewrite;
                left = LogicalPlan::Join(crate::sql::planner::JoinPlan {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: crate::sql::parser::ast::JoinType::Left,
                    condition: join_condition,
                    left_schema: current_schema.clone(),
                    right_schema: right_schema.clone(),
                    output_schema: crate::sql::planner::merge_schemas(
                        current_schema,
                        &right_schema,
                    ),
                });
                let agg_column = LogicalExpr::Column(current_schema.columns.len() + agg_pos);
                let value = if is_count {
                    LogicalExpr::Function {
                        name: "coalesce".to_string(),
                        args: vec![
                            agg_column,
                            LogicalExpr::Literal(ScalarValue::Int64(Some(0))),
                        ],
                    }
                } else {
                    agg_column
                };
                left_schema = Some(crate::sql::planner::merge_schemas(
                    current_schema,
                    &right_schema,
                ));
                kept.push(replace_scalar_marker(&conjunct, value));
                continue;
            }
        }

        kept.push(conjunct);
    }

    match combine_conjuncts(kept) {
        Some(predicate) => LogicalPlan::Filter(crate::sql::planner::FilterPlan {
            predicate,
            input: Box::new(left),
        }),
        None => left,
    }
}

#[allow(clippy::type_complexity)]
fn try_decorrelate(
    node: &CorrelatedSubqueryPlan,
    left_schema: &Schema,
    ctx: &mut Ctx,
) -> Option<(LogicalPlan, Schema, LogicalExpr, bool)> {
    let outer_len = left_schema.columns.len();
    let (is_anti, needs_value_column) = match node.kind {
        CorrelatedKind::Exists { negated } => (negated, false),
        CorrelatedKind::In { negated } => (negated, true),
        CorrelatedKind::Scalar => return None,
    };

    let Statement::Select {
        select_list,
        from,
        group_by,
        having,
        limit,
        offset,
        ..
    } = &node.stmt
    else {
        return None;
    };
    let from = from.as_ref()?;
    // Original GROUP BY / paging inside the inner query cannot move below
    // the semi join without changing semantics.
    if !group_by.is_empty() || limit.is_some() || offset.is_some() {
        return None;
    }

    // Classify the inner query. With aggregates in the select list or a
    // HAVING clause, the right side becomes grouped by the correlation keys
    // (per-outer-row aggregate + HAVING == per-key group + HAVING).
    let mut scratch: Vec<(crate::sql::planner::AggFunc, Option<Expr>, bool)> = Vec::new();
    let mut select_has_agg = false;
    for item in select_list {
        if let SelectItem::Expr(e, _) = item {
            // Errors here (window functions) also mean "unsupported".
            if crate::sql::planner::expr::collect_aggregate_calls(e, &mut scratch).is_err() {
                return None;
            }
            if !scratch.is_empty() {
                select_has_agg = true;
            }
            scratch.clear();
        }
    }
    let aggregate_mode = having.is_some() || select_has_agg;
    if aggregate_mode {
        // A HAVING that references outer columns cannot be grouped.
        if let Some(h) = having {
            if ast_has_placeholder(h) {
                return None;
            }
        }
        // EXISTS over a bare scalar aggregate (no HAVING) is always true
        // per-row; the grouped rewrite would change that. Fall back.
        if !needs_value_column && having.is_none() {
            return None;
        }
    }
    if needs_value_column && select_list.len() != 1 {
        return None;
    }

    let schemas: HashMap<String, Schema> = {
        let guard = ctx.registry.lock().unwrap();
        guard
            .snapshots
            .get(&node.id)
            .map(|snapshot| snapshot.schemas.clone())?
    };
    let (inner_schema, inner_alias) = build_scope_bindings(from, &schemas);
    if inner_schema.columns.is_empty() {
        return None;
    }

    let mut condition_parts: Vec<LogicalExpr> = Vec::with_capacity(node.conjuncts.len() + 1);
    let (mut right_select, right_group_by, right_having): (Vec<Expr>, Vec<Expr>, Option<Expr>);
    let mut value_pos_agg: Option<usize> = None;

    if aggregate_mode {
        // Every correlated conjunct must be an inner = outer equality; the
        // inner sides become the grouping keys.
        let mut keys: Vec<Expr> = Vec::new();
        for conjunct in &node.conjuncts {
            let (inner_side, outer_side) = split_equation(conjunct, &inner_schema, &inner_alias)?;
            let pos = match keys.iter().position(|k| k == &inner_side) {
                Some(pos) => pos,
                None => {
                    keys.push(inner_side.clone());
                    keys.len() - 1
                }
            };
            condition_parts.push(LogicalExpr::BinaryOp {
                left: Box::new(translate_outer(&outer_side, &node.params)?),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Column(outer_len + pos)),
            });
        }
        if keys.is_empty() {
            return None;
        }

        right_select = keys.clone();
        right_group_by = keys;
        right_having = having.clone();
        if needs_value_column {
            // The IN value is the aggregate in the single select item; it
            // lands at output position keys.len() (after the group keys).
            let SelectItem::Expr(value_expr, _) = &select_list[0] else {
                return None;
            };
            scratch.clear();
            if crate::sql::planner::expr::collect_aggregate_calls(value_expr, &mut scratch).is_err()
                || scratch.is_empty()
            {
                return None;
            }
            value_pos_agg = Some(right_select.len());
            right_select.push(value_expr.clone());
        }
    } else {
        // Plain mode: the value column (for IN) is output position 0 and the
        // remaining inner columns referenced by the conjuncts follow.
        let mut items: Vec<(usize, Expr)> = Vec::new();
        if needs_value_column {
            let SelectItem::Expr(value_expr, _) = &select_list[0] else {
                return None;
            };
            let value_idx = resolve_inner_column(value_expr, &inner_schema, &inner_alias)?;
            items.push((value_idx, value_expr.clone()));
        }
        for conjunct in &node.conjuncts {
            collect_inner_refs(conjunct, &inner_schema, &inner_alias, &mut items)?;
        }
        // EXISTS whose conjuncts reference no inner column ("inner is
        // non-empty under an outer-only predicate") is rare; leave it on
        // the per-row path.
        if !needs_value_column && items.is_empty() {
            return None;
        }
        right_select = items.iter().map(|(_, e)| e.clone()).collect();
        right_group_by = Vec::new();
        right_having = None;
        for conjunct in &node.conjuncts {
            condition_parts.push(translate(
                conjunct,
                &items,
                outer_len,
                &inner_schema,
                &inner_alias,
                &node.params,
            )?);
        }
    }
    if needs_value_column {
        let lhs = node.lhs.as_deref()?.clone();
        let value_pos = if aggregate_mode {
            value_pos_agg.expect("set in aggregate mode above")
        } else {
            0
        };
        condition_parts.push(LogicalExpr::BinaryOp {
            left: Box::new(lhs),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(outer_len + value_pos)),
        });
    }
    let condition = combine_conjuncts(condition_parts)?;

    // Build the right side: same FROM + local WHERE, projecting exactly the
    // referenced columns (grouped by the correlation keys in aggregate
    // mode). ORDER BY / DISTINCT are irrelevant for existence / membership
    // and dropped.
    let mut right_stmt = node.stmt.clone();
    if let Statement::Select {
        select_list,
        group_by,
        having,
        order_by,
        limit,
        offset,
        distinct,
        ..
    } = &mut right_stmt
    {
        *select_list = right_select
            .into_iter()
            .map(|e| SelectItem::Expr(e, None))
            .collect();
        *group_by = right_group_by;
        *having = right_having;
        *order_by = Vec::new();
        *limit = None;
        *offset = None;
        *distinct = false;
    }
    let right_plan = match crate::sql::planner::plan_select(&right_stmt, &schemas) {
        Ok(plan) => optimize(plan),
        Err(_) => return None,
    };
    let right_plan = rename_scans(right_plan, node.id, ctx);
    let right_schema = plan_output_schema(&right_plan)?;

    Some((right_plan, right_schema, condition, is_anti))
}

fn resolve_inner_column(
    expr: &Expr,
    inner_schema: &Schema,
    inner_alias: &crate::sql::planner::AliasMap,
) -> Option<usize> {
    match expr {
        Expr::Column(name) => inner_schema.index_of(name),
        Expr::QualifiedColumn { table, column } => {
            let binding = inner_alias.get(&table.to_lowercase())?;
            (binding.start..binding.end).find(|&idx| {
                inner_schema
                    .columns
                    .get(idx)
                    .map(|c| c.name.eq_ignore_ascii_case(column))
                    .unwrap_or(false)
            })
        }
        _ => None,
    }
}

/// Collect inner column references from a marked conjunct into `items`
/// (deduped by resolved column index). Returns None when the conjunct
/// contains something unresolvable, which should not happen after binding.
fn collect_inner_refs(
    expr: &Expr,
    inner_schema: &Schema,
    inner_alias: &crate::sql::planner::AliasMap,
    items: &mut Vec<(usize, Expr)>,
) -> Option<()> {
    match expr {
        Expr::Placeholder(_) | Expr::Literal(_) => Some(()),
        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            let idx = resolve_inner_column(expr, inner_schema, inner_alias)?;
            if !items.iter().any(|(i, _)| *i == idx) {
                items.push((idx, expr.clone()));
            }
            Some(())
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_inner_refs(left, inner_schema, inner_alias, items)?;
            collect_inner_refs(right, inner_schema, inner_alias, items)
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_inner_refs(inner, inner_schema, inner_alias, items),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_inner_refs(expr, inner_schema, inner_alias, items)?;
            collect_inner_refs(low, inner_schema, inner_alias, items)?;
            collect_inner_refs(high, inner_schema, inner_alias, items)
        }
        Expr::In { expr, values, .. } => {
            collect_inner_refs(expr, inner_schema, inner_alias, items)?;
            for value in values {
                collect_inner_refs(value, inner_schema, inner_alias, items)?;
            }
            Some(())
        }
        Expr::Like { expr, pattern, .. } => {
            collect_inner_refs(expr, inner_schema, inner_alias, items)?;
            collect_inner_refs(pattern, inner_schema, inner_alias, items)
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_inner_refs(arg, inner_schema, inner_alias, items)?;
            }
            Some(())
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_inner_refs(operand, inner_schema, inner_alias, items)?;
            }
            for (when, then) in when_clauses {
                collect_inner_refs(when, inner_schema, inner_alias, items)?;
                collect_inner_refs(then, inner_schema, inner_alias, items)?;
            }
            if let Some(else_expr) = else_clause {
                collect_inner_refs(else_expr, inner_schema, inner_alias, items)?;
            }
            Some(())
        }
        // Nested subquery nodes cannot appear inside a bound conjunct.
        _ => None,
    }
}

/// Translate a marked conjunct into the semi join condition column space:
/// outer references (`Placeholder`) become left columns, inner references
/// become `outer_len + position in right output`.
fn translate(
    expr: &Expr,
    items: &[(usize, Expr)],
    outer_len: usize,
    inner_schema: &Schema,
    inner_alias: &crate::sql::planner::AliasMap,
    params: &[(usize, usize)],
) -> Option<LogicalExpr> {
    match expr {
        Expr::Placeholder(id) => params
            .iter()
            .find(|(marker, _)| marker == id)
            .map(|(_, col)| LogicalExpr::Column(*col)),
        Expr::Column(_) | Expr::QualifiedColumn { .. } => {
            let idx = resolve_inner_column(expr, inner_schema, inner_alias)?;
            let pos = items.iter().position(|(i, _)| *i == idx)?;
            Some(LogicalExpr::Column(outer_len + pos))
        }
        Expr::Literal(v) => Some(LogicalExpr::Literal(v.clone())),
        Expr::BinaryOp { left, op, right } => Some(LogicalExpr::BinaryOp {
            left: Box::new(translate(
                left,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
            op: *op,
            right: Box::new(translate(
                right,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
        }),
        Expr::UnaryNot(inner) => Some(LogicalExpr::Not(Box::new(translate(
            inner,
            items,
            outer_len,
            inner_schema,
            inner_alias,
            params,
        )?))),
        Expr::UnaryMinus(inner) => Some(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
            op: BinOp::Sub,
            right: Box::new(translate(
                inner,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
        }),
        Expr::IsNull(inner) => Some(LogicalExpr::IsNull(Box::new(translate(
            inner,
            items,
            outer_len,
            inner_schema,
            inner_alias,
            params,
        )?))),
        Expr::IsNotNull(inner) => Some(LogicalExpr::IsNotNull(Box::new(translate(
            inner,
            items,
            outer_len,
            inner_schema,
            inner_alias,
            params,
        )?))),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Some(LogicalExpr::Between {
            expr: Box::new(translate(
                expr,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
            low: Box::new(translate(
                low,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
            high: Box::new(translate(
                high,
                items,
                outer_len,
                inner_schema,
                inner_alias,
                params,
            )?),
            negated: *negated,
        }),
        Expr::In {
            expr,
            values,
            negated,
        } => {
            let mut literals = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Expr::Literal(v) => literals.push(v.clone()),
                    _ => return None,
                }
            }
            Some(LogicalExpr::In {
                expr: Box::new(translate(
                    expr,
                    items,
                    outer_len,
                    inner_schema,
                    inner_alias,
                    params,
                )?),
                values: literals,
                negated: *negated,
            })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let pattern = match pattern.as_ref() {
                Expr::Literal(ScalarValue::VarChar(Some(s))) => s.clone(),
                _ => return None,
            };
            Some(LogicalExpr::Like {
                expr: Box::new(translate(
                    expr,
                    items,
                    outer_len,
                    inner_schema,
                    inner_alias,
                    params,
                )?),
                pattern,
                negated: *negated,
            })
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if *distinct {
                return None;
            }
            let mut translated = Vec::with_capacity(args.len());
            for arg in args {
                translated.push(translate(
                    arg,
                    items,
                    outer_len,
                    inner_schema,
                    inner_alias,
                    params,
                )?);
            }
            Some(LogicalExpr::Function {
                name: name.clone(),
                args: translated,
            })
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand = match operand {
                Some(e) => Some(Box::new(translate(
                    e,
                    items,
                    outer_len,
                    inner_schema,
                    inner_alias,
                    params,
                )?)),
                None => None,
            };
            let mut clauses = Vec::with_capacity(when_clauses.len());
            for (when, then) in when_clauses {
                clauses.push((
                    translate(when, items, outer_len, inner_schema, inner_alias, params)?,
                    translate(then, items, outer_len, inner_schema, inner_alias, params)?,
                ));
            }
            let else_clause = match else_clause {
                Some(e) => Some(Box::new(translate(
                    e,
                    items,
                    outer_len,
                    inner_schema,
                    inner_alias,
                    params,
                )?)),
                None => None,
            };
            Some(LogicalExpr::Case {
                operand,
                when_clauses: clauses,
                else_clause,
            })
        }
        // Nested subquery nodes cannot appear inside a bound conjunct.
        _ => None,
    }
}

/// Rename the right side's scans to per-marker pseudo tables and stage their
/// batches under those names.
fn rename_scans(plan: LogicalPlan, id: u64, ctx: &mut Ctx) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            let pseudo = format!("__corr_{}__{}", id, scan.table_name);
            let batches = {
                let guard = ctx.registry.lock().unwrap();
                guard
                    .snapshots
                    .get(&id)
                    .and_then(|snapshot| snapshot.batches.get(&scan.table_name).cloned())
            };
            if let Some(batches) = batches {
                ctx.extra_batches.insert(pseudo.clone(), batches);
            }
            scan.table_name = pseudo;
            LogicalPlan::Scan(scan)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(rename_scans(*filter.input, id, ctx));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut proj) => {
            proj.input = Box::new(rename_scans(*proj.input, id, ctx));
            LogicalPlan::Project(proj)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(rename_scans(*sort.input, id, ctx));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(rename_scans(*limit.input, id, ctx));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(rename_scans(*topn.input, id, ctx));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Distinct(mut distinct) => {
            distinct.input = Box::new(rename_scans(*distinct.input, id, ctx));
            LogicalPlan::Distinct(distinct)
        }
        // The scalar path's right side is an aggregate plan; its scan sits
        // below the Aggregate node.
        LogicalPlan::Aggregate(mut agg) => {
            agg.input = Box::new(rename_scans(*agg.input, id, ctx));
            LogicalPlan::Aggregate(agg)
        }
        LogicalPlan::Window(mut window) => {
            window.input = Box::new(rename_scans(*window.input, id, ctx));
            LogicalPlan::Window(window)
        }
        LogicalPlan::Join(mut join) => {
            join.left = Box::new(rename_scans(*join.left, id, ctx));
            join.right = Box::new(rename_scans(*join.right, id, ctx));
            LogicalPlan::Join(join)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            semi.left = Box::new(rename_scans(*semi.left, id, ctx));
            semi.right = Box::new(rename_scans(*semi.right, id, ctx));
            LogicalPlan::SemiJoin(semi)
        }
        other => other,
    }
}

/// Output schema of a plan built by `plan_select` over a plain FROM/WHERE
/// shape. Returns None for node types that cannot occur on a decorrelated
/// right side or on a decorrelation target input.
fn plan_output_schema(plan: &LogicalPlan) -> Option<Schema> {
    match plan {
        LogicalPlan::Scan(scan) => Some(scan.schema.clone()),
        LogicalPlan::Filter(filter) => plan_output_schema(&filter.input),
        LogicalPlan::Project(project) => {
            let input = plan_output_schema(&project.input)?;
            let columns = project
                .expressions
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    crate::sql::execution::operators::project::project_column_def(
                        expr,
                        &input,
                        project.aliases.get(i).cloned().flatten().as_deref(),
                    )
                })
                .collect();
            Some(Schema::new(columns))
        }
        LogicalPlan::Join(join) => Some(join.output_schema.clone()),
        LogicalPlan::SemiJoin(semi) => Some(semi.left_schema.clone()),
        // The scalar path builds right sides whose root IS an aggregate
        // (group keys + aggregate results), so its output layout matters.
        LogicalPlan::Aggregate(agg) => {
            let input = plan_output_schema(&agg.input)?;
            let mut columns = Vec::with_capacity(agg.group_by.len() + agg.aggregates.len());
            for key in &agg.group_by {
                match key {
                    LogicalExpr::Column(idx) => {
                        let def = input.columns.get(*idx)?;
                        columns.push(def.clone());
                    }
                    other => {
                        let dt = crate::sql::planner::expr::infer_type(other, &input);
                        columns.push(crate::types::batch::ColumnDef::new("group_expr", dt));
                    }
                }
            }
            for a in &agg.aggregates {
                columns.push(
                    crate::sql::execution::operators::aggregate::aggregate_result_column_def(
                        a, &input,
                    ),
                );
            }
            Some(Schema::new(columns))
        }
        LogicalPlan::Sort(sort) => plan_output_schema(&sort.input),
        LogicalPlan::Limit(limit) => plan_output_schema(&limit.input),
        LogicalPlan::Distinct(distinct) => plan_output_schema(&distinct.input),
        LogicalPlan::TopN(topn) => plan_output_schema(&topn.input),
        LogicalPlan::Window(_) | LogicalPlan::Empty => None,
    }
}

fn split_and_conjuncts(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut conjuncts = split_and_conjuncts(left);
            conjuncts.extend(split_and_conjuncts(right));
            conjuncts
        }
        other => vec![other.clone()],
    }
}

fn combine_conjuncts(conjuncts: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut combined: Option<LogicalExpr> = None;
    for conjunct in conjuncts {
        combined = Some(match combined {
            Some(prev) => LogicalExpr::BinaryOp {
                left: Box::new(prev),
                op: BinOp::And,
                right: Box::new(conjunct),
            },
            None => conjunct,
        });
    }
    combined
}

/// Find the single scalar marker inside a conjunct, if there is exactly one.
fn single_scalar_marker(expr: &LogicalExpr) -> Option<&CorrelatedSubqueryPlan> {
    fn walk<'a>(expr: &'a LogicalExpr, found: &mut Vec<&'a CorrelatedSubqueryPlan>) {
        match expr {
            LogicalExpr::CorrelatedSubquery(node) => {
                if node.kind == CorrelatedKind::Scalar {
                    found.push(node);
                }
            }
            LogicalExpr::BinaryOp { left, right, .. } => {
                walk(left, found);
                walk(right, found);
            }
            LogicalExpr::Not(inner)
            | LogicalExpr::IsNull(inner)
            | LogicalExpr::IsNotNull(inner) => {
                walk(inner, found);
            }
            LogicalExpr::In { expr, .. } => walk(expr, found),
            LogicalExpr::Between {
                expr, low, high, ..
            } => {
                walk(expr, found);
                walk(low, found);
                walk(high, found);
            }
            LogicalExpr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(o) = operand {
                    walk(o, found);
                }
                for (w, t) in when_clauses {
                    walk(w, found);
                    walk(t, found);
                }
                if let Some(e) = else_clause {
                    walk(e, found);
                }
            }
            LogicalExpr::Like { expr, .. } => walk(expr, found),
            LogicalExpr::Function { args, .. } => {
                for a in args {
                    walk(a, found);
                }
            }
            LogicalExpr::Aggregate(agg) => walk(&agg.arg, found),
            _ => {}
        }
    }
    let mut found = Vec::new();
    walk(expr, &mut found);
    if found.len() == 1 {
        Some(found[0])
    } else {
        None
    }
}

/// Replace the (single) scalar marker inside a conjunct with `replacement`.
fn replace_scalar_marker(expr: &LogicalExpr, replacement: LogicalExpr) -> LogicalExpr {
    fn walk(expr: &LogicalExpr, replacement: &LogicalExpr, done: &mut bool) -> LogicalExpr {
        if let LogicalExpr::CorrelatedSubquery(node) = expr {
            if node.kind == CorrelatedKind::Scalar && !*done {
                *done = true;
                return replacement.clone();
            }
            return expr.clone();
        }
        match expr {
            LogicalExpr::BinaryOp { left, op, right } => LogicalExpr::BinaryOp {
                left: Box::new(walk(left, replacement, done)),
                op: *op,
                right: Box::new(walk(right, replacement, done)),
            },
            LogicalExpr::Not(inner) => LogicalExpr::Not(Box::new(walk(inner, replacement, done))),
            LogicalExpr::IsNull(inner) => {
                LogicalExpr::IsNull(Box::new(walk(inner, replacement, done)))
            }
            LogicalExpr::IsNotNull(inner) => {
                LogicalExpr::IsNotNull(Box::new(walk(inner, replacement, done)))
            }
            LogicalExpr::In {
                expr,
                values,
                negated,
            } => LogicalExpr::In {
                expr: Box::new(walk(expr, replacement, done)),
                values: values.clone(),
                negated: *negated,
            },
            LogicalExpr::Between {
                expr,
                low,
                high,
                negated,
            } => LogicalExpr::Between {
                expr: Box::new(walk(expr, replacement, done)),
                low: Box::new(walk(low, replacement, done)),
                high: Box::new(walk(high, replacement, done)),
                negated: *negated,
            },
            LogicalExpr::Case {
                operand,
                when_clauses,
                else_clause,
            } => LogicalExpr::Case {
                operand: operand
                    .as_ref()
                    .map(|e| Box::new(walk(e, replacement, done))),
                when_clauses: when_clauses
                    .iter()
                    .map(|(w, t)| (walk(w, replacement, done), walk(t, replacement, done)))
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|e| Box::new(walk(e, replacement, done))),
            },
            LogicalExpr::Like {
                expr,
                pattern,
                negated,
            } => LogicalExpr::Like {
                expr: Box::new(walk(expr, replacement, done)),
                pattern: pattern.clone(),
                negated: *negated,
            },
            LogicalExpr::Function { name, args } => LogicalExpr::Function {
                name: name.clone(),
                args: args.iter().map(|a| walk(a, replacement, done)).collect(),
            },
            LogicalExpr::Aggregate(agg) => {
                LogicalExpr::Aggregate(crate::sql::planner::AggregateExpr {
                    func: agg.func,
                    arg: Box::new(walk(&agg.arg, replacement, done)),
                    distinct: agg.distinct,
                })
            }
            other => other.clone(),
        }
    }
    let mut done = false;
    walk(expr, &replacement, &mut done)
}

/// Does this AST expression contain a `Placeholder` (outer reference)?
fn ast_has_placeholder(expr: &Expr) -> bool {
    match expr {
        Expr::Placeholder(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            ast_has_placeholder(left) || ast_has_placeholder(right)
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => ast_has_placeholder(inner),
        Expr::In { expr, values, .. } => {
            ast_has_placeholder(expr) || values.iter().any(ast_has_placeholder)
        }
        Expr::Between {
            expr, low, high, ..
        } => ast_has_placeholder(expr) || ast_has_placeholder(low) || ast_has_placeholder(high),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| ast_has_placeholder(e))
                .unwrap_or(false)
                || when_clauses
                    .iter()
                    .any(|(w, t)| ast_has_placeholder(w) || ast_has_placeholder(t))
                || else_clause
                    .as_ref()
                    .map(|e| ast_has_placeholder(e))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. } => {
            ast_has_placeholder(expr) || ast_has_placeholder(pattern)
        }
        Expr::Function { args, .. } => args.iter().any(ast_has_placeholder),
        _ => false,
    }
}

/// Classify a top-level equality conjunct of a scalar subquery's correlated
/// WHERE into (inner side, outer side). The inner side carries only inner
/// column references; the outer side carries only placeholders / literals.
fn split_equation(
    conjunct: &Expr,
    inner_schema: &Schema,
    inner_alias: &crate::sql::planner::AliasMap,
) -> Option<(Expr, Expr)> {
    let Expr::BinaryOp {
        left,
        op: BinOp::Eq,
        right,
    } = conjunct
    else {
        return None;
    };
    let left_inner = resolve_inner_column(left, inner_schema, inner_alias).is_some();
    let left_outer = ast_has_placeholder(left);
    let right_inner = resolve_inner_column(right, inner_schema, inner_alias).is_some();
    let right_outer = ast_has_placeholder(right);
    if left_inner && !left_outer && right_outer && !right_inner {
        Some(((**left).clone(), (**right).clone()))
    } else if right_inner && !right_outer && left_outer && !left_inner {
        Some(((**right).clone(), (**left).clone()))
    } else {
        None
    }
}

/// Translate an outer-side expression (placeholders / literals) into the
/// LEFT column space of the join.
fn translate_outer(expr: &Expr, params: &[(usize, usize)]) -> Option<LogicalExpr> {
    match expr {
        Expr::Placeholder(id) => params
            .iter()
            .find(|(marker, _)| marker == id)
            .map(|(_, col)| LogicalExpr::Column(*col)),
        Expr::Literal(v) => Some(LogicalExpr::Literal(v.clone())),
        Expr::BinaryOp { left, op, right } => Some(LogicalExpr::BinaryOp {
            left: Box::new(translate_outer(left, params)?),
            op: *op,
            right: Box::new(translate_outer(right, params)?),
        }),
        Expr::UnaryNot(inner) => Some(LogicalExpr::Not(Box::new(translate_outer(inner, params)?))),
        Expr::UnaryMinus(inner) => Some(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
            op: BinOp::Sub,
            right: Box::new(translate_outer(inner, params)?),
        }),
        Expr::IsNull(inner) => Some(LogicalExpr::IsNull(Box::new(translate_outer(
            inner, params,
        )?))),
        Expr::IsNotNull(inner) => Some(LogicalExpr::IsNotNull(Box::new(translate_outer(
            inner, params,
        )?))),
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if *distinct {
                return None;
            }
            let mut translated = Vec::with_capacity(args.len());
            for arg in args {
                translated.push(translate_outer(arg, params)?);
            }
            Some(LogicalExpr::Function {
                name: name.clone(),
                args: translated,
            })
        }
        _ => None,
    }
}

/// Decorrelate a scalar subquery into a LEFT JOIN against the inner query
/// grouped by its correlation keys:
///
/// ```text
/// x > (SELECT AGG(b.v) FROM b WHERE b.k = outer.k [AND local])
/// ->
/// LEFT JOIN (SELECT k, AGG(v) FROM b WHERE local GROUP BY k) sub
///   ON outer.k = sub.k
/// [Filter] x > sub.agg      (COUNT is wrapped in coalesce(agg, 0))
/// ```
///
/// Requires every correlated conjunct to be a plain `inner = outer`
/// equality (grouping key) and the select list to be a single aggregate
/// call. Anything else falls back to per-row evaluation.
#[allow(clippy::type_complexity)]
fn try_decorrelate_scalar(
    node: &CorrelatedSubqueryPlan,
    left_len: usize,
    ctx: &mut Ctx,
) -> Option<(LogicalPlan, Schema, LogicalExpr, usize, bool)> {
    let Statement::Select {
        select_list,
        from,
        group_by,
        having,
        limit,
        offset,
        ..
    } = &node.stmt
    else {
        return None;
    };
    let from = from.as_ref()?;
    if !group_by.is_empty() || having.is_some() || limit.is_some() || offset.is_some() {
        return None;
    }
    if select_list.len() != 1 {
        return None;
    }
    let SelectItem::Expr(agg_expr, _) = &select_list[0] else {
        return None;
    };
    // The value must be a single non-distinct aggregate call over
    // inner-only input.
    let Expr::Function {
        name,
        args,
        distinct,
    } = agg_expr
    else {
        return None;
    };
    if *distinct {
        return None;
    }
    let is_count = name.eq_ignore_ascii_case("count");
    if !is_count
        && !name.eq_ignore_ascii_case("sum")
        && !name.eq_ignore_ascii_case("avg")
        && !name.eq_ignore_ascii_case("min")
        && !name.eq_ignore_ascii_case("max")
    {
        return None;
    }
    for arg in args {
        if ast_has_placeholder(arg) {
            return None;
        }
    }

    let schemas: HashMap<String, Schema> = {
        let guard = ctx.registry.lock().unwrap();
        guard
            .snapshots
            .get(&node.id)
            .map(|snapshot| snapshot.schemas.clone())?
    };
    let (inner_schema, inner_alias) = build_scope_bindings(from, &schemas);
    if inner_schema.columns.is_empty() {
        return None;
    }
    if node.conjuncts.is_empty() {
        return None; // uncorrelated scalars never become markers
    }

    // Every correlated conjunct must be an inner = outer equality; the
    // inner sides become the grouping keys.
    let mut keys: Vec<Expr> = Vec::new();
    let mut condition_parts: Vec<LogicalExpr> = Vec::new();
    for conjunct in &node.conjuncts {
        let (inner_side, outer_side) = split_equation(conjunct, &inner_schema, &inner_alias)?;
        let key_pos = match keys.iter().position(|k| k == &inner_side) {
            Some(pos) => pos,
            None => {
                keys.push(inner_side.clone());
                keys.len() - 1
            }
        };
        condition_parts.push(LogicalExpr::BinaryOp {
            left: Box::new(translate_outer(&outer_side, &node.params)?),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(left_len + key_pos)),
        });
    }
    let join_condition = combine_conjuncts(condition_parts)?;

    // Right side: SELECT <keys...>, <agg> FROM ... WHERE <local> GROUP BY <keys...>
    let mut right_stmt = node.stmt.clone();
    if let Statement::Select {
        select_list,
        group_by,
        order_by,
        limit,
        offset,
        distinct,
        ..
    } = &mut right_stmt
    {
        let mut items: Vec<SelectItem> = keys
            .iter()
            .map(|k| SelectItem::Expr(k.clone(), None))
            .collect();
        items.push(SelectItem::Expr(agg_expr.clone(), None));
        *select_list = items;
        *group_by = keys.clone();
        *order_by = Vec::new();
        *limit = None;
        *offset = None;
        *distinct = false;
    }
    let right_plan = match crate::sql::planner::plan_select(&right_stmt, &schemas) {
        Ok(plan) => optimize(plan),
        Err(_) => return None,
    };
    let right_plan = rename_scans(right_plan, node.id, ctx);
    let right_schema = plan_output_schema(&right_plan)?;
    if right_schema.columns.len() != keys.len() + 1 {
        return None;
    }

    Some((
        right_plan,
        right_schema,
        join_condition,
        keys.len(),
        is_count,
    ))
}
