//! Statement-level SELECT execution, split by concern: the execution
//! pipeline (`select_exec`), uncorrelated subqueries and derived tables
//! (`subqueries`), and query-block/correlated resolution (`correlated`).
//! Table-reference extraction and conjunct utilities stay here in the host.
//! All `query::…` paths are preserved via glob re-exports.

mod correlated;
mod select_exec;
mod subqueries;

pub(crate) use correlated::*;
pub use select_exec::*;
pub use subqueries::*;

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::{RecordBatch, Schema};

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{Expr, FromClause, ObjectName, SelectItem, Statement, TableRef};

#[derive(Debug, Clone)]
pub struct ResolvedTableRef {
    pub key: String,
    pub database: String,
    pub table: String,
}

impl ResolvedTableRef {
    fn new(object: &ObjectName, default_db: &str) -> Self {
        Self {
            key: object.plan_key(),
            database: object.effective_database(default_db).to_string(),
            table: object.name.clone(),
        }
    }
}

pub fn extract_table_refs(from: &FromClause, default_db: &str) -> Vec<ResolvedTableRef> {
    match from {
        FromClause::Table(t) => match t {
            TableRef::Table { name, .. } => vec![ResolvedTableRef::new(name, default_db)],
            TableRef::Subquery { .. } => Vec::new(),
        },
        FromClause::Join(join_expr) => {
            let mut refs = Vec::new();
            collect_table_refs_from_join(join_expr, default_db, &mut refs);
            refs
        }
    }
}

fn collect_table_refs_from_join(
    join_expr: &crate::sql::parser::ast::JoinExpr,
    default_db: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match &*join_expr.left {
        FromClause::Table(t) => match t {
            TableRef::Table { name, .. } => refs.push(ResolvedTableRef::new(name, default_db)),
            TableRef::Subquery { .. } => {}
        },
        FromClause::Join(inner) => collect_table_refs_from_join(inner, default_db, refs),
    }
    for jc in &join_expr.joins {
        match &jc.right {
            TableRef::Table { name, .. } => refs.push(ResolvedTableRef::new(name, default_db)),
            TableRef::Subquery { .. } => {}
        }
    }
}

/// Collect every table a statement reads: FROM tables (recursing into
/// derived tables), plus any table referenced inside a subquery in the
/// select list, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, join ON
/// conditions, or — for UPDATE — the SET assignments. The engine's
/// statement-level permission gate walks this collection, so a subquery can
/// never read a table that a FROM-only check would miss.
pub(crate) fn collect_read_tables(
    stmt: &Statement,
    default_db: &str,
    out: &mut Vec<ResolvedTableRef>,
) {
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
            if let Some(from) = from {
                collect_from_tables(from, default_db, out);
            }
            for item in select_list {
                if let SelectItem::Expr(expr, _) = item {
                    collect_expr_tables(expr, default_db, out);
                }
            }
            if let Some(expr) = where_clause {
                collect_expr_tables(expr, default_db, out);
            }
            for expr in group_by {
                collect_expr_tables(expr, default_db, out);
            }
            if let Some(expr) = having {
                collect_expr_tables(expr, default_db, out);
            }
            for item in order_by {
                collect_expr_tables(&item.expr, default_db, out);
            }
            if let Some(expr) = limit {
                collect_expr_tables(expr, default_db, out);
            }
            if let Some(expr) = offset {
                collect_expr_tables(expr, default_db, out);
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
            collect_read_tables(left, default_db, out);
            collect_read_tables(right, default_db, out);
            for item in order_by {
                collect_expr_tables(&item.expr, default_db, out);
            }
            if let Some(expr) = limit {
                collect_expr_tables(expr, default_db, out);
            }
            if let Some(expr) = offset {
                collect_expr_tables(expr, default_db, out);
            }
        }
        Statement::Update {
            assignments,
            where_clause: Some(expr),
            ..
        } => {
            for assignment in assignments {
                collect_expr_tables(&assignment.value, default_db, out);
            }
            collect_expr_tables(expr, default_db, out);
        }
        Statement::Update { assignments, .. } => {
            for assignment in assignments {
                collect_expr_tables(&assignment.value, default_db, out);
            }
        }
        Statement::Delete {
            where_clause: Some(expr),
            ..
        } => {
            collect_expr_tables(expr, default_db, out);
        }
        Statement::Delete { .. } => {}
        Statement::Insert { values, .. } | Statement::Replace { values, .. } => {
            for row in values {
                for expr in row {
                    collect_expr_tables(expr, default_db, out);
                }
            }
        }
        _ => {}
    }
}

fn collect_from_tables(from: &FromClause, default_db: &str, out: &mut Vec<ResolvedTableRef>) {
    match from {
        FromClause::Table(TableRef::Table { name, .. }) => {
            out.push(ResolvedTableRef::new(name, default_db));
        }
        FromClause::Table(TableRef::Subquery { query, .. }) => {
            collect_read_tables(query, default_db, out);
        }
        FromClause::Join(join_expr) => {
            collect_from_tables(&join_expr.left, default_db, out);
            for join in &join_expr.joins {
                match &join.right {
                    TableRef::Table { name, .. } => {
                        out.push(ResolvedTableRef::new(name, default_db));
                    }
                    TableRef::Subquery { query, .. } => {
                        collect_read_tables(query, default_db, out);
                    }
                }
                if let Some(condition) = &join.condition {
                    collect_expr_tables(condition, default_db, out);
                }
            }
        }
    }
}

fn collect_expr_tables(expr: &Expr, default_db: &str, out: &mut Vec<ResolvedTableRef>) {
    match expr {
        Expr::Subquery(stmt) => collect_read_tables(stmt, default_db, out),
        Expr::Correlated(correlated) => {
            if let Some(lhs) = &correlated.lhs {
                collect_expr_tables(lhs, default_db, out);
            }
            collect_read_tables(&correlated.stmt, default_db, out);
            for conjunct in &correlated.conjuncts {
                collect_expr_tables(conjunct, default_db, out);
            }
        }
        Expr::Exists(inner) => collect_expr_tables(inner, default_db, out),
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => collect_expr_tables(inner, default_db, out),
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_tables(left, default_db, out);
            collect_expr_tables(right, default_db, out);
        }
        Expr::In { expr, values, .. } => {
            collect_expr_tables(expr, default_db, out);
            for value in values {
                collect_expr_tables(value, default_db, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_tables(expr, default_db, out);
            collect_expr_tables(low, default_db, out);
            collect_expr_tables(high, default_db, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_tables(expr, default_db, out);
            collect_expr_tables(pattern, default_db, out);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_tables(arg, default_db, out);
            }
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expr_tables(operand, default_db, out);
            }
            for (when, then) in when_clauses {
                collect_expr_tables(when, default_db, out);
                collect_expr_tables(then, default_db, out);
            }
            if let Some(else_expr) = else_clause {
                collect_expr_tables(else_expr, default_db, out);
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                collect_expr_tables(expr, default_db, out);
            }
            for item in order_by {
                collect_expr_tables(&item.expr, default_db, out);
            }
        }
        _ => {}
    }
}

// ============================================================================
// Derived tables and correlated subqueries
// ============================================================================

fn split_and_conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: crate::sql::parser::ast::BinOp::And,
            right,
        } => {
            let mut conjuncts = split_and_conjuncts(*left);
            conjuncts.extend(split_and_conjuncts(*right));
            conjuncts
        }
        other => vec![other],
    }
}

fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut combined: Option<Expr> = None;
    for conjunct in conjuncts {
        combined = Some(match combined {
            Some(prev) => Expr::BinaryOp {
                left: Box::new(prev),
                op: crate::sql::parser::ast::BinOp::And,
                right: Box::new(conjunct),
            },
            None => conjunct,
        });
    }
    combined
}

/// Materialize a full snapshot of one base table for the correlated-subquery
/// runtime. The snapshot is plain data, so per-row evaluation never needs the
/// engine.
fn snapshot_table(
    engine: &Engine,
    table_ref: &ResolvedTableRef,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let stmt = Statement::Select {
        select_list: vec![SelectItem::Star],
        from: Some(FromClause::Table(TableRef::Table {
            name: ObjectName::qualified(table_ref.database.clone(), table_ref.table.clone()),
            alias: None,
        })),
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: false,
    };
    match exec_select(engine, &table_ref.database, &stmt)? {
        ExecResult::Query { schema, batches } => Ok((schema, batches)),
        _ => Err(TaibaiError::Sql(format!(
            "failed to snapshot table '{}.{}'",
            table_ref.database, table_ref.table
        ))),
    }
}
