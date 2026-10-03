//! Runtime support for correlated subqueries.
//!
//! The binder pass in `query.rs` rewrites a correlated subquery into an
//! `Expr::Correlated` marker: outer references become `Expr::Placeholder`
//! markers, correlated WHERE conjuncts are lifted out of the inner statement,
//! and a snapshot of the inner query's tables (schemas + materialized batches)
//! is registered here under a per-query id.
//!
//! Evaluation happens per outer row: parameter values are read from the outer
//! batch, substituted into the inner statement, and the inner query is planned
//! and executed against the snapshot with the ordinary pipeline builder. No
//! `Engine` handle is needed inside operators — the snapshot is plain data.
//! Results are memoized per parameter tuple so repeated correlation keys do
//! not re-execute the inner query.
//!
//! Marker-id contract: the binder assigns placeholder ids densely from 0 in
//! `node.params` order, so `params[marker_id]` is the value for marker id.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::common::error::{Result, TaibaiError};
use crate::sql::optimizer::optimize;
use crate::sql::parser::ast::{BinOp, CorrelatedKind, Expr, Statement};
use crate::sql::planner::CorrelatedSubqueryPlan;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::operators::build_pipeline_with_subqueries;

/// Snapshot of one correlated subquery's inner query.
pub struct CorrelatedSnapshot {
    /// Inner FROM tables (and materialized derived tables) keyed by plan key.
    pub schemas: HashMap<String, Schema>,
    pub batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
    pub cache: HashMap<String, CachedResult>,
}

pub struct SubqueryRegistryInner {
    pub snapshots: HashMap<u64, CorrelatedSnapshot>,
    /// Per-query cache of inner-table snapshots shared by every correlated
    /// subquery occurrence of one statement: N occurrences of subqueries
    /// over the same table materialize it ONCE instead of N times (each
    /// occurrence used to pay a full-table read + copy).
    pub table_snapshots: HashMap<String, (crate::types::batch::Schema, Vec<std::sync::Arc<crate::types::batch::RecordBatch>>)>,
    next_id: u64,
}

impl Default for SubqueryRegistryInner {
    fn default() -> Self {
        Self::new()
    }
}

impl SubqueryRegistryInner {
    pub fn new() -> Self {
        Self {
            snapshots: HashMap::new(),
            table_snapshots: HashMap::new(),
            next_id: 0,
        }
    }

    pub fn allocate(&mut self, snapshot: CorrelatedSnapshot) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.snapshots.insert(id, snapshot);
        id
    }
}

pub type SubqueryRegistry = Arc<Mutex<SubqueryRegistryInner>>;

pub fn new_registry() -> SubqueryRegistry {
    Arc::new(Mutex::new(SubqueryRegistryInner::new()))
}

pub enum CachedResult {
    Exists(bool),
    Scalar(ScalarValue),
    InList(Vec<ScalarValue>),
}

/// Evaluate a correlated subquery node for every row of `batch`, returning
/// one value per row. `registry` must own the snapshot registered under
/// `node.id` by the binding pass.
pub fn eval_correlated_column(
    node: &CorrelatedSubqueryPlan,
    batch: &RecordBatch,
    registry: Option<&SubqueryRegistry>,
) -> Result<Vec<ScalarValue>> {
    let Some(registry) = registry else {
        return Err(TaibaiError::Sql(
            "correlated subquery cannot be evaluated in this context (no subquery registry); \
             correlated subqueries are only supported in SELECT queries"
                .into(),
        ));
    };
    if !registry.lock().unwrap().snapshots.contains_key(&node.id) {
        return Err(TaibaiError::Internal(format!(
            "missing correlated subquery snapshot #{id}; the query was probably re-planned \
             outside the execution pass that bound it",
            id = node.id
        )));
    }

    let lhs_values = match (&node.kind, &node.lhs) {
        (CorrelatedKind::In { .. }, Some(lhs)) => Some(
            super::operators::filter::eval_expr_column_with(lhs, batch, Some(registry))?,
        ),
        _ => None,
    };

    let num_rows = batch.num_rows();
    let mut values = Vec::with_capacity(num_rows);
    for row in 0..num_rows {
        // Cheap cancellation point: a cache-miss row runs a full inner query,
        // so a high-cardinality correlation key made one batch arbitrarily
        // long and KILL QUERY could not act until the batch boundary.
        crate::common::query_cancel::check()?;
        let params: Vec<ScalarValue> = node
            .params
            .iter()
            .map(|(_, col_idx)| {
                batch
                    .scalar(row, *col_idx)
                    .unwrap_or(ScalarValue::Int64(None))
            })
            .collect();
        let lhs = lhs_values.as_ref().map(|vals| &vals[row]);
        values.push(eval_row(node, &params, lhs, registry)?);
    }
    Ok(values)
}

fn eval_row(
    node: &CorrelatedSubqueryPlan,
    params: &[ScalarValue],
    lhs: Option<&ScalarValue>,
    registry: &SubqueryRegistry,
) -> Result<ScalarValue> {
    let key = cache_key(params);
    let mut registry = registry.lock().unwrap();
    let snapshot = registry
        .snapshots
        .get_mut(&node.id)
        .ok_or_else(|| TaibaiError::Internal(format!("missing snapshot #{}", node.id)))?;

    let cached = snapshot.cache.get(&key);
    match (&node.kind, cached) {
        (CorrelatedKind::Exists { negated }, Some(CachedResult::Exists(found))) => {
            return Ok(ScalarValue::Bool(Some(if *negated {
                !*found
            } else {
                *found
            })));
        }
        (CorrelatedKind::Scalar, Some(CachedResult::Scalar(value))) => {
            return Ok(value.clone());
        }
        (CorrelatedKind::In { negated }, Some(CachedResult::InList(list))) => {
            let lhs = lhs.expect("IN subquery carries a lhs expression");
            return Ok(ScalarValue::Bool(in_list_contains(list, lhs, *negated)));
        }
        _ => {}
    }

    // Cache miss: execute the inner query for this parameter combination.
    let mut stmt = node.stmt.clone();
    substitute_stmt(&mut stmt, params);
    let mut conjuncts = node.conjuncts.clone();
    for conjunct in &mut conjuncts {
        substitute_expr(conjunct, params);
    }
    if !conjuncts.is_empty() {
        let existing = match &mut stmt {
            Statement::Select { where_clause, .. } => where_clause.take(),
            _ => None,
        };
        let combined = combine_conjuncts(existing, conjuncts);
        if let Statement::Select { where_clause, .. } = &mut stmt {
            *where_clause = combined;
        }
    }

    let plan = optimize(crate::sql::planner::plan_select(&stmt, &snapshot.schemas)?);
    let mut pipeline = build_pipeline_with_subqueries(plan, &snapshot.batches, None)?;

    let result = match node.kind {
        CorrelatedKind::Exists { .. } => {
            // EXISTS only needs to know whether any row comes back: stop at
            // the first non-empty batch instead of draining the whole inner
            // pipeline.
            let mut found = false;
            while let Some(batch) = pipeline.next_batch()? {
                if batch.num_rows() > 0 {
                    found = true;
                    break;
                }
            }
            CachedResult::Exists(found)
        }
        CorrelatedKind::Scalar | CorrelatedKind::In { .. } => {
            let mut batches = Vec::new();
            while let Some(batch) = pipeline.next_batch()? {
                batches.push(batch);
            }
            match node.kind {
                CorrelatedKind::Scalar => {
                    let mut rows = 0usize;
                    let mut first: Option<ScalarValue> = None;
                    for batch in &batches {
                        if batch.num_columns() == 0 {
                            continue;
                        }
                        for row_idx in 0..batch.num_rows() {
                            rows += 1;
                            if first.is_none() {
                                first = Some(
                                    batch
                                        .scalar(row_idx, 0)
                                        .unwrap_or(ScalarValue::Int64(None)),
                                );
                            }
                        }
                    }
                    if rows > 1 {
                        return Err(TaibaiError::Sql(format!(
                            "scalar subquery returned more than one row ({rows})"
                        )));
                    }
                    CachedResult::Scalar(first.unwrap_or(ScalarValue::Int64(None)))
                }
                CorrelatedKind::In { .. } => {
                    let mut list = Vec::new();
                    for batch in &batches {
                        if batch.num_columns() == 0 {
                            continue;
                        }
                        for row_idx in 0..batch.num_rows() {
                            list.push(
                                batch
                                    .scalar(row_idx, 0)
                                    .unwrap_or(ScalarValue::Int64(None)),
                            );
                        }
                    }
                    CachedResult::InList(list)
                }
                _ => unreachable!("kind matched outer arm"),
            }
        }
    };

    let value = match (&node.kind, &result) {
        (CorrelatedKind::Exists { negated }, CachedResult::Exists(found)) => {
            ScalarValue::Bool(Some(if *negated { !*found } else { *found }))
        }
        (CorrelatedKind::Scalar, CachedResult::Scalar(value)) => value.clone(),
        (CorrelatedKind::In { negated }, CachedResult::InList(list)) => {
            let lhs = lhs.expect("IN subquery carries a lhs expression");
            ScalarValue::Bool(in_list_contains(list, lhs, *negated))
        }
        _ => unreachable!("cached/result kind mismatch"),
    };
    // Memoization is a best-effort accelerator, not a guarantee: bound the
    // per-node cache so high-cardinality correlation keys cannot grow it
    // (and every cached IN list) without limit. Evict a single entry instead
    // of clearing the whole table — clear-all threw every hot correlation
    // key out at once, collapsing the hit rate whenever the cap cycled.
    if snapshot.cache.len() >= 65536 {
        if let Some(victim) = snapshot.cache.keys().next().cloned() {
            snapshot.cache.remove(&victim);
        }
    }
    snapshot.cache.insert(key, result);
    Ok(value)
}

/// Three-valued IN semantics, matching the literal `IN` evaluation in
/// helpers::eval_where_clause3: a NULL lhs is UNKNOWN (never "not in"), and a
/// list containing NULL makes a non-matching lhs UNKNOWN — `x NOT IN (1,
/// NULL)` must exclude every row, not keep them. `None` maps to Bool(None).
fn in_list_contains(list: &[ScalarValue], lhs: &ScalarValue, negated: bool) -> Option<bool> {
    if lhs.is_null() {
        return None;
    }
    let mut found = false;
    let mut saw_null = false;
    for candidate in list {
        if candidate.is_null() {
            saw_null = true;
            continue;
        }
        if super::operators::filter::values_equal(candidate, lhs) {
            found = true;
            break;
        }
    }
    let inner = if found {
        Some(true)
    } else if saw_null {
        None
    } else {
        Some(false)
    };
    inner.map(|b| if negated { !b } else { b })
}

fn cache_key(params: &[ScalarValue]) -> String {
    // Length-prefixed encoding: a separator character can appear inside
    // a VarChar parameter value, which made distinct parameter tuples
    // collide.
    let mut key = String::new();
    for value in params {
        let rendered = format!("{:?}", value);
        key.push_str(&rendered.len().to_string());
        key.push(':');
        key.push_str(&rendered);
    }
    key
}

fn combine_conjuncts(existing: Option<Expr>, conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut combined = existing;
    for conjunct in conjuncts {
        combined = Some(match combined {
            Some(prev) => Expr::BinaryOp {
                left: Box::new(prev),
                op: BinOp::And,
                right: Box::new(conjunct),
            },
            None => conjunct,
        });
    }
    combined
}

/// Replace `Placeholder(id)` markers with `params[id]`.
fn substitute_expr(expr: &mut Expr, params: &[ScalarValue]) {
    match expr {
        Expr::Placeholder(id) => {
            if let Some(value) = params.get(*id) {
                *expr = Expr::Literal(value.clone());
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            substitute_expr(left, params);
            substitute_expr(right, params);
        }
        Expr::UnaryNot(inner) | Expr::UnaryMinus(inner) => {
            substitute_expr(inner, params);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            substitute_expr(inner, params);
        }
        Expr::In { expr, values, .. } => {
            substitute_expr(expr, params);
            for value in values {
                substitute_expr(value, params);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            substitute_expr(expr, params);
            substitute_expr(low, params);
            substitute_expr(high, params);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                substitute_expr(operand, params);
            }
            for (when, then) in when_clauses {
                substitute_expr(when, params);
                substitute_expr(then, params);
            }
            if let Some(else_expr) = else_clause {
                substitute_expr(else_expr, params);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            substitute_expr(expr, params);
            substitute_expr(pattern, params);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                substitute_expr(arg, params);
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for part in partition_by {
                substitute_expr(part, params);
            }
            for item in order_by {
                substitute_expr(&mut item.expr, params);
            }
        }
        Expr::Exists(inner) => substitute_expr(inner, params),
        Expr::Subquery(stmt) => substitute_stmt(stmt, params),
        Expr::Correlated(_) | Expr::Literal(_) | Expr::Column(_) | Expr::QualifiedColumn { .. } => {
        }
    }
}

fn substitute_stmt(stmt: &mut Statement, params: &[ScalarValue]) {
    let (select_list, where_clause, group_by, having, order_by, limit, offset) = match stmt {
        Statement::Select {
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => (
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        ),
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            substitute_stmt(left, params);
            substitute_stmt(right, params);
            for item in order_by {
                substitute_expr(&mut item.expr, params);
            }
            if let Some(expr) = limit {
                substitute_expr(expr, params);
            }
            if let Some(expr) = offset {
                substitute_expr(expr, params);
            }
            return;
        }
        _ => return,
    };
    for item in select_list.iter_mut() {
        if let crate::sql::parser::ast::SelectItem::Expr(expr, _) = item {
            substitute_expr(expr, params);
        }
    }
    if let Some(expr) = where_clause {
        substitute_expr(expr, params);
    }
    for expr in group_by.iter_mut() {
        substitute_expr(expr, params);
    }
    if let Some(expr) = having {
        substitute_expr(expr, params);
    }
    for item in order_by.iter_mut() {
        substitute_expr(&mut item.expr, params);
    }
    if let Some(expr) = limit {
        substitute_expr(expr, params);
    }
    if let Some(expr) = offset {
        substitute_expr(expr, params);
    }
}

/// Convenience accessor used by `query.rs` to know whether any correlated
/// nodes were bound for the current query.
pub fn registry_has_snapshots(registry: &SubqueryRegistry) -> bool {
    !registry.lock().unwrap().snapshots.is_empty()
}
