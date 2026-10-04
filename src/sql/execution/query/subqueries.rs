//! Uncorrelated subquery resolution and derived tables: statement/expr
//! subquery detection, IN/scalar/EXISTS execution, `__derived__`
//! materialization, and the has-subquery probes the planner gates on.

use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::execution::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{Expr, FromClause, ObjectName, SelectItem, Statement, TableRef};

use super::select_exec::*;

/// True when the statement contains any subquery anywhere: scalar/IN/EXISTS
/// expressions (resolved or correlated) or derived tables in FROM. Prepared
/// SELECT plan caching is restricted to subquery-free statements — a cached
/// plan embeds either resolved subquery literals (re-executing them on the
/// cache-fill pass and freezing their result) or correlated snapshot ids
/// from a query-private registry that no later execution can satisfy.
pub(crate) fn statement_contains_subquery(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            select_list.iter().any(
                |item| matches!(item, SelectItem::Expr(expr, _) if expr_contains_subquery(expr)),
            ) || from.as_ref().is_some_and(from_contains_subquery)
                || where_clause.as_ref().is_some_and(expr_contains_subquery)
                || group_by.iter().any(expr_contains_subquery)
                || having.as_ref().is_some_and(expr_contains_subquery)
                || order_by
                    .iter()
                    .any(|item| expr_contains_subquery(&item.expr))
                || limit.as_ref().is_some_and(expr_contains_subquery)
                || offset.as_ref().is_some_and(expr_contains_subquery)
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            statement_contains_subquery(left)
                || statement_contains_subquery(right)
                || order_by
                    .iter()
                    .any(|item| expr_contains_subquery(&item.expr))
                || limit.as_ref().is_some_and(expr_contains_subquery)
                || offset.as_ref().is_some_and(expr_contains_subquery)
        }
        Statement::InsertSelect { query, .. } | Statement::ReplaceSelect { query, .. } => {
            statement_contains_subquery(query)
        }
        Statement::Update {
            assignments,
            where_clause,
            ..
        } => {
            assignments
                .iter()
                .any(|assign| expr_contains_subquery(&assign.value))
                || where_clause.as_ref().is_some_and(expr_contains_subquery)
        }
        Statement::Delete { where_clause, .. } => {
            where_clause.as_ref().is_some_and(expr_contains_subquery)
        }
        _ => false,
    }
}

pub(super) fn from_contains_subquery(from: &FromClause) -> bool {
    match from {
        FromClause::Table(table_ref) => table_ref_contains_subquery(table_ref),
        FromClause::Join(join) => {
            from_contains_subquery(&join.left)
                || join.joins.iter().any(|clause| {
                    table_ref_contains_subquery(&clause.right)
                        || clause
                            .condition
                            .as_ref()
                            .is_some_and(expr_contains_subquery)
                })
        }
    }
}

pub(super) fn table_ref_contains_subquery(table_ref: &TableRef) -> bool {
    match table_ref {
        TableRef::Table { .. } => false,
        TableRef::Subquery { query, .. } => statement_contains_subquery(query),
    }
}

pub(super) fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Exists(_) | Expr::Subquery(_) | Expr::Correlated(_) => true,
        Expr::In { expr, values, .. } => {
            expr_contains_subquery(expr) || values.iter().any(expr_contains_subquery)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_subquery(expr)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::UnaryNot(inner) | Expr::UnaryMinus(inner) => expr_contains_subquery(inner),
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => expr_contains_subquery(inner),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(expr_contains_subquery)
                || when_clauses.iter().any(|(when, then)| {
                    expr_contains_subquery(when) || expr_contains_subquery(then)
                })
                || else_clause.as_deref().is_some_and(expr_contains_subquery)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_subquery(expr) || expr_contains_subquery(pattern)
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_subquery),
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_contains_subquery)
                || order_by
                    .iter()
                    .any(|item| expr_contains_subquery(&item.expr))
        }
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => false,
    }
}

pub(crate) fn resolve_in_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
) -> Result<Statement> {
    let mut stmt = stmt.clone();
    resolve_stmt_in_subqueries(engine, db, &mut stmt)?;
    Ok(stmt)
}

/// Resolve subqueries inside a DML WHERE clause into literals — the
/// `Option<Expr>` counterpart of [`resolve_in_subqueries`]. Shared by the
/// DELETE, UPDATE, and transactional DML paths.
pub(crate) fn resolve_where_in_subqueries(
    engine: &Engine,
    db: &str,
    where_clause: &Option<Expr>,
) -> Result<Option<Expr>> {
    where_clause
        .as_ref()
        .map(|expr| resolve_expr_in_subqueries_clone(engine, db, expr))
        .transpose()
}

pub(super) fn resolve_stmt_in_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &mut Statement,
) -> Result<()> {
    match stmt {
        Statement::Select {
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            for item in select_list {
                if let SelectItem::Expr(expr, _) = item {
                    resolve_expr_in_subqueries(engine, db, expr)?;
                }
            }
            if let Some(expr) = where_clause {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for expr in group_by {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = having {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
            if let Some(expr) = limit {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = offset {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            resolve_stmt_in_subqueries(engine, db, left)?;
            resolve_stmt_in_subqueries(engine, db, right)?;
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
            if let Some(expr) = limit {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = offset {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn resolve_expr_in_subqueries(engine: &Engine, db: &str, expr: &mut Expr) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            resolve_expr_in_subqueries(engine, db, left)?;
            resolve_expr_in_subqueries(engine, db, right)?;
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => resolve_expr_in_subqueries(engine, db, inner)?,
        Expr::Exists(inner) => {
            if let Expr::Subquery(subquery) = inner.as_ref() {
                let exists = execute_exists_subquery(engine, db, subquery)?;
                *expr = Expr::Literal(ScalarValue::Bool(Some(exists)));
            } else {
                resolve_expr_in_subqueries(engine, db, inner)?;
            }
        }
        Expr::In {
            expr: inner,
            values,
            ..
        } => {
            resolve_expr_in_subqueries(engine, db, inner)?;
            if let [Expr::Subquery(subquery)] = values.as_mut_slice() {
                let literals = execute_in_subquery(engine, db, subquery)?;
                *values = literals.into_iter().map(Expr::Literal).collect();
            } else {
                for value in values {
                    resolve_expr_in_subqueries(engine, db, value)?;
                }
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            resolve_expr_in_subqueries(engine, db, expr)?;
            resolve_expr_in_subqueries(engine, db, low)?;
            resolve_expr_in_subqueries(engine, db, high)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                resolve_expr_in_subqueries(engine, db, operand)?;
            }
            for (when_expr, then_expr) in when_clauses {
                resolve_expr_in_subqueries(engine, db, when_expr)?;
                resolve_expr_in_subqueries(engine, db, then_expr)?;
            }
            if let Some(else_clause) = else_clause {
                resolve_expr_in_subqueries(engine, db, else_clause)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            resolve_expr_in_subqueries(engine, db, expr)?;
            resolve_expr_in_subqueries(engine, db, pattern)?;
        }
        Expr::Subquery(subquery) => {
            let value = execute_scalar_subquery(engine, db, subquery)?;
            *expr = Expr::Literal(value);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                resolve_expr_in_subqueries(engine, db, arg)?;
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
        }
        Expr::Correlated(_) => {}
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => {}
    }
    Ok(())
}

pub(super) fn execute_in_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<Vec<ScalarValue>> {
    // Resolve nested subqueries up front: `exec_select` would resolve them
    // again and find nothing left, so the total work is unchanged.
    let mut resolved = subquery.clone();
    resolve_stmt_in_subqueries(engine, db, &mut resolved)?;
    if let Some(values) = execute_simple_derived_subquery(engine, db, &resolved)? {
        return Ok(values);
    }
    let ExecResult::Query { batches, .. } = exec_select(engine, db, &resolved)? else {
        return Err(TaibaiError::Sql("IN subquery must be a SELECT".into()));
    };
    Ok(first_column_values(&batches, 0))
}

/// Fast path for `IN (SELECT c FROM (SELECT ...) AS alias)`: the derived
/// table is executed directly and the single projected column picked out of
/// its result, skipping the outer select pipeline. Only shapes whose
/// projection is provably a pass-through of one inner column take the fast
/// path — anything else returns `None` for the general pipeline.
pub(super) fn execute_simple_derived_subquery(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
) -> Result<Option<Vec<ScalarValue>>> {
    let Statement::Select {
        select_list,
        from: Some(FromClause::Table(TableRef::Subquery { query, alias })),
        where_clause: None,
        group_by,
        having: None,
        order_by: _,
        limit: None,
        offset: None,
        ..
    } = stmt
    else {
        return Ok(None);
    };

    if !group_by.is_empty() || select_list.len() != 1 {
        return Ok(None);
    }

    let mut inner = query.clone();
    resolve_stmt_in_subqueries(engine, db, &mut inner)?;
    let ExecResult::Query { schema, batches } = exec_select(engine, db, &inner)? else {
        return Err(TaibaiError::Sql(
            "derived IN subquery must be a SELECT".into(),
        ));
    };

    let col_idx = match &select_list[0] {
        SelectItem::Star => Some(0),
        SelectItem::Expr(Expr::Column(name), _) => schema.index_of(name),
        SelectItem::Expr(Expr::QualifiedColumn { table, column }, _)
            if table.eq_ignore_ascii_case(alias) =>
        {
            schema.index_of(column)
        }
        // A computed projection (e.g. `alias.col + 1`) — or a column the
        // inner query does not expose — is not a pass-through; the general
        // pipeline must evaluate (or reject) it.
        SelectItem::Expr(_, _) => None,
    };
    let Some(col_idx) = col_idx else {
        return Ok(None);
    };
    Ok(Some(first_column_values(&batches, col_idx)))
}

pub(super) fn first_column_values(batches: &[RecordBatch], col_idx: usize) -> Vec<ScalarValue> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() <= col_idx {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            values.push(
                batch
                    .scalar(row_idx, col_idx)
                    .unwrap_or(ScalarValue::Int64(None)),
            );
        }
    }
    values
}

/// Clone-and-resolve form used by the DML paths, which keep the original
/// expression for permission checks and undo and need a rewritten copy.
pub(crate) fn resolve_expr_in_subqueries_clone(
    engine: &Engine,
    db: &str,
    expr: &Expr,
) -> Result<Expr> {
    let mut resolved = expr.clone();
    resolve_expr_in_subqueries(engine, db, &mut resolved)?;
    Ok(resolved)
}

pub(crate) fn execute_scalar_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<ScalarValue> {
    // Stream the subquery and stop as soon as a second row proves it is not
    // scalar. The materialized path computed the entire result just to look
    // at its first row (and to prove there was no second), paying full
    // subquery-scan cost on every scalar subquery over a large table.
    const MORE_THAN_ONE: &str = "__taibai_scalar_subquery_more_than_one_row";
    pub(super) fn is_more_than_one(error: &TaibaiError) -> bool {
        matches!(error, TaibaiError::Internal(msg) if msg == MORE_THAN_ONE)
    }
    let mut first: Option<ScalarValue> = None;
    let mut rows = 0usize;
    let streamed = match exec_select_streaming(engine, db, subquery, |batch| {
        if batch.num_columns() == 0 {
            return Ok(());
        }
        for row_idx in 0..batch.num_rows() {
            rows += 1;
            if first.is_none() {
                first = Some(batch.scalar(row_idx, 0).unwrap_or(ScalarValue::Int64(None)));
            }
            if rows > 1 {
                return Err(TaibaiError::Internal(MORE_THAN_ONE.into()));
            }
        }
        Ok(())
    }) {
        Ok(streamed) => streamed,
        Err(error) if is_more_than_one(&error) => None,
        Err(error) => return Err(error),
    };
    if streamed.is_none() && rows <= 1 {
        // The streaming pipeline declined (subqueries/derived tables inside
        // the subquery, or an unsupported plan shape): fall back to the
        // materialized execution.
        let ExecResult::Query { batches, .. } = exec_select(engine, db, subquery)? else {
            return Err(TaibaiError::Sql("scalar subquery must be a SELECT".into()));
        };
        for batch in batches {
            if batch.num_columns() == 0 {
                continue;
            }
            for row_idx in 0..batch.num_rows() {
                rows += 1;
                if first.is_none() {
                    first = Some(batch.scalar(row_idx, 0).unwrap_or(ScalarValue::Int64(None)));
                }
            }
        }
    }
    if rows > 1 {
        return Err(TaibaiError::Sql(format!(
            "scalar subquery returned more than one row ({rows})"
        )));
    }
    Ok(first.unwrap_or(ScalarValue::Int64(None)))
}

pub(crate) fn execute_exists_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<bool> {
    // EXISTS only cares whether any row comes back: inject LIMIT 1 when the
    // subquery has none, so limit pushdown stops the scan at the first row
    // instead of materializing the whole subquery result. An existing LIMIT
    // is kept (LIMIT 0 legitimately yields EXISTS=false).
    let mut bounded = subquery.clone();
    match &mut bounded {
        Statement::Select { limit, .. } if limit.is_none() => {
            *limit = Some(Expr::Literal(ScalarValue::Int64(Some(1))));
        }
        _ => {}
    }
    let ExecResult::Query { batches, .. } = exec_select(engine, db, &bounded)? else {
        return Err(TaibaiError::Sql("EXISTS subquery must be a SELECT".into()));
    };
    Ok(batches.iter().any(|batch| batch.num_rows() > 0))
}

/// Synthetic database that backs materialized derived tables
/// (`FROM (SELECT ...) alias`). Queries never see it; it only keeps the
/// rewritten pseudo-table scans out of the real catalog namespace.
pub const DERIVED_DB: &str = "__derived__";

/// Rewrite every `FROM (SELECT ...) alias` of the query block into a scan of
/// a synthetic table, executing each subquery once and keeping its batches.
pub(super) fn materialize_derived_tables(
    engine: &Engine,
    db: &str,
    stmt: &mut Statement,
) -> Result<HashMap<String, (Schema, Vec<RecordBatch>)>> {
    let mut derived = HashMap::new();
    if let Statement::Select {
        from: Some(from), ..
    } = stmt
    {
        let mut counter = 0usize;
        rewrite_derived_in_from(from, engine, db, &mut counter, &mut derived)?;
    }
    Ok(derived)
}

pub(super) fn rewrite_derived_in_from(
    from: &mut FromClause,
    engine: &Engine,
    db: &str,
    counter: &mut usize,
    derived: &mut HashMap<String, (Schema, Vec<RecordBatch>)>,
) -> Result<()> {
    match from {
        FromClause::Table(table_ref) => {
            rewrite_derived_table_ref(table_ref, engine, db, counter, derived)
        }
        FromClause::Join(join_expr) => {
            rewrite_derived_in_from(&mut join_expr.left, engine, db, counter, derived)?;
            for join_clause in &mut join_expr.joins {
                rewrite_derived_table_ref(&mut join_clause.right, engine, db, counter, derived)?;
            }
            Ok(())
        }
    }
}

pub(super) fn rewrite_derived_table_ref(
    table_ref: &mut TableRef,
    engine: &Engine,
    db: &str,
    counter: &mut usize,
    derived: &mut HashMap<String, (Schema, Vec<RecordBatch>)>,
) -> Result<()> {
    if let TableRef::Subquery { query, alias } = table_ref {
        let subquery = (**query).clone();
        let (schema, batches) = exec_query_materialized(engine, db, &subquery)?;
        let pseudo = format!("__derived_{}", *counter);
        *counter += 1;
        let object = ObjectName {
            database: Some(DERIVED_DB.to_string()),
            name: pseudo,
        };
        derived.insert(object.plan_key(), (schema, batches));
        *table_ref = TableRef::Table {
            name: object,
            alias: Some(alias.clone()),
        };
    }
    Ok(())
}

pub(super) fn expr_has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::Correlated(_) => true,
        Expr::Exists(inner) => match inner.as_ref() {
            Expr::Subquery(_) => true,
            other => expr_has_subquery(other),
        },
        Expr::In { expr, values, .. } => {
            expr_has_subquery(expr)
                || values.iter().any(|v| matches!(v, Expr::Subquery(_)))
                || values.iter().any(expr_has_subquery)
        }
        Expr::BinaryOp { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_has_subquery(inner),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| expr_has_subquery(e))
                .unwrap_or(false)
                || when_clauses
                    .iter()
                    .any(|(w, t)| expr_has_subquery(w) || expr_has_subquery(t))
                || else_clause
                    .as_ref()
                    .map(|e| expr_has_subquery(e))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. } => expr_has_subquery(expr) || expr_has_subquery(pattern),
        Expr::Function { args, .. } => args.iter().any(expr_has_subquery),
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_has_subquery)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
        }
        _ => false,
    }
}

pub(super) fn from_has_derived(from: &FromClause) -> bool {
    match from {
        FromClause::Table(t) => matches!(t, TableRef::Subquery { .. }),
        FromClause::Join(join_expr) => {
            from_has_derived(&join_expr.left)
                || join_expr.joins.iter().any(|jc| {
                    matches!(&jc.right, TableRef::Subquery { .. })
                        || jc
                            .condition
                            .as_ref()
                            .map(expr_has_subquery)
                            .unwrap_or(false)
                })
        }
    }
}

/// Fast pre-check used by the streaming path to defer subquery/derived-table
/// queries to the materialized execution path.
pub(crate) fn stmt_has_subquery_or_derived(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            from.as_ref().map(from_has_derived).unwrap_or(false)
                || select_list
                    .iter()
                    .any(|item| matches!(item, SelectItem::Expr(e, _) if expr_has_subquery(e)))
                || where_clause
                    .as_ref()
                    .map(expr_has_subquery)
                    .unwrap_or(false)
                || group_by.iter().any(expr_has_subquery)
                || having.as_ref().map(expr_has_subquery).unwrap_or(false)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
                || limit.as_ref().map(expr_has_subquery).unwrap_or(false)
                || offset.as_ref().map(expr_has_subquery).unwrap_or(false)
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            stmt_has_subquery_or_derived(left)
                || stmt_has_subquery_or_derived(right)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
                || limit.as_ref().map(expr_has_subquery).unwrap_or(false)
                || offset.as_ref().map(expr_has_subquery).unwrap_or(false)
        }
        _ => false,
    }
}
