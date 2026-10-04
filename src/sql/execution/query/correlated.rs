//! Query-block resolution and correlated subqueries: `resolve_query`,
//! eager uncorrelated subquery folding, and the MarkerBinder that rewrites
//! outer references into per-row placeholder markers.

use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::execution::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{CorrelatedKind, CorrelatedSubquery, Expr, SelectItem, Statement};

use super::select_exec::*;
use super::subqueries::*;
use super::*;

/// One query block's binding scope: the concatenated FROM schema and the
/// table alias map, matching how the planner assigns column offsets.
pub(crate) struct QueryBlockScope {
    pub schema: Schema,
    pub alias_map: crate::sql::planner::AliasMap,
}

/// The fully resolved form of a SELECT: derived tables materialized into
/// pseudo-table scans, uncorrelated subqueries collapsed into literals, and
/// correlated subqueries bound into markers registered in `registry`.
pub(crate) struct ResolvedQuery {
    pub stmt: Statement,
    pub derived: HashMap<String, (Schema, Vec<RecordBatch>)>,
    pub registry: crate::sql::execution::subquery_runtime::SubqueryRegistry,
    pub has_correlated: bool,
}

/// Carries an already-resolved query (with its derived tables and subquery
/// registry) into an execution that would otherwise re-resolve. Used by
/// EXPLAIN ANALYZE, which plans/resolves up front and then executes the
/// cached plan for profiling.
pub(crate) struct PreResolvedExec<'a> {
    pub resolved: &'a ResolvedQuery,
    /// Decorrelated right-side pseudo-table batches produced alongside the
    /// cached plan.
    pub extra_batches: &'a HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
}

/// Resolve a SELECT statement for execution or planning (also used by
/// EXPLAIN).
pub(crate) fn resolve_query(engine: &Engine, db: &str, stmt: &Statement) -> Result<ResolvedQuery> {
    let mut working = stmt.clone();
    // 1. Materialize derived tables (FROM (SELECT ...) alias) into
    //    pseudo-table scans backed by the materialized batches.
    let derived = materialize_derived_tables(engine, db, &mut working)?;
    let working_from = match &working {
        Statement::Select {
            from: Some(from), ..
        } => from.clone(),
        _ => {
            return Err(TaibaiError::Internal(
                "resolve_query called with non-SELECT".into(),
            ))
        }
    };
    // 2. Build the query block scope (concatenated schema + alias map).
    let mut scope_schemas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        for table_ref in extract_table_refs(&working_from, db) {
            if table_ref.database == DERIVED_DB {
                let (schema, _) = derived.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                scope_schemas.insert(table_ref.key.clone(), schema.clone());
            } else {
                let catalog = catalogs.get(&table_ref.database).ok_or_else(|| {
                    TaibaiError::Sql(format!("database '{}' does not exist", table_ref.database))
                })?;
                let meta = catalog.get_table(&table_ref.table).ok_or_else(|| {
                    TaibaiError::Sql(format!(
                        "table '{}.{}' does not exist",
                        table_ref.database, table_ref.table
                    ))
                })?;
                scope_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
            }
        }
    }
    let (schema, alias_map) =
        crate::sql::planner::build_scope_bindings(&working_from, &scope_schemas);
    let scope = QueryBlockScope { schema, alias_map };
    // 3. Resolve expression-level subqueries: uncorrelated ones execute
    //    eagerly; correlated ones are bound into per-row markers.
    let registry = crate::sql::execution::subquery_runtime::new_registry();
    resolve_query_subqueries(engine, db, &mut working, Some(&scope), &registry, true)?;
    let has_correlated = crate::sql::execution::subquery_runtime::registry_has_snapshots(&registry);
    Ok(ResolvedQuery {
        stmt: working,
        derived,
        registry,
        has_correlated,
    })
}

/// Walk a statement, resolving expression-level subqueries. Uncorrelated
/// subqueries execute eagerly and collapse into literals (existing
/// behavior); correlated ones — when `allow_correlated` and an enclosing
/// scope are available — are rewritten into `Expr::Correlated` markers and
/// registered in `registry`.
pub(super) fn resolve_query_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &mut Statement,
    scope: Option<&QueryBlockScope>,
    registry: &crate::sql::execution::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
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
                    resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
                }
            }
            if let Some(expr) = where_clause {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            for expr in group_by {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            if let Some(expr) = having {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            for item in order_by {
                resolve_query_expr(
                    engine,
                    db,
                    &mut item.expr,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            }
            if let Some(expr) = limit {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            if let Some(expr) = offset {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
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
            // Each UNION branch is its own query block; correlated references
            // crossing the UNION boundary are not supported.
            resolve_query_subqueries(engine, db, left, None, registry, false)?;
            resolve_query_subqueries(engine, db, right, None, registry, false)?;
            for item in order_by {
                resolve_query_expr(engine, db, &mut item.expr, None, registry, false)?;
            }
            if let Some(expr) = limit {
                resolve_query_expr(engine, db, expr, None, registry, false)?;
            }
            if let Some(expr) = offset {
                resolve_query_expr(engine, db, expr, None, registry, false)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn resolve_query_expr(
    engine: &Engine,
    db: &str,
    expr: &mut Expr,
    scope: Option<&QueryBlockScope>,
    registry: &crate::sql::execution::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            resolve_query_expr(engine, db, left, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, right, scope, registry, allow_correlated)?;
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
        }
        Expr::Exists(inner) => {
            if let Expr::Subquery(subquery) = inner.as_ref() {
                let subquery = (**subquery).clone();
                *expr = resolve_subquery_node(
                    engine,
                    db,
                    CorrelatedKind::Exists { negated: false },
                    None,
                    &subquery,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            } else {
                resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
            }
        }
        Expr::In {
            expr: inner,
            values,
            negated,
        } => {
            resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
            let subquery = match values.as_slice() {
                [Expr::Subquery(sub)] => Some((**sub).clone()),
                _ => None,
            };
            if let Some(subquery) = subquery {
                let lhs = (**inner).clone();
                *expr = resolve_subquery_node(
                    engine,
                    db,
                    CorrelatedKind::In { negated: *negated },
                    Some(lhs),
                    &subquery,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            } else {
                for value in values {
                    resolve_query_expr(engine, db, value, scope, registry, allow_correlated)?;
                }
            }
        }
        Expr::Subquery(subquery) => {
            let subquery = (**subquery).clone();
            *expr = resolve_subquery_node(
                engine,
                db,
                CorrelatedKind::Scalar,
                None,
                &subquery,
                scope,
                registry,
                allow_correlated,
            )?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, low, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, high, scope, registry, allow_correlated)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                resolve_query_expr(engine, db, operand, scope, registry, allow_correlated)?;
            }
            for (when_expr, then_expr) in when_clauses {
                resolve_query_expr(engine, db, when_expr, scope, registry, allow_correlated)?;
                resolve_query_expr(engine, db, then_expr, scope, registry, allow_correlated)?;
            }
            if let Some(else_expr) = else_clause {
                resolve_query_expr(engine, db, else_expr, scope, registry, allow_correlated)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, pattern, scope, registry, allow_correlated)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                resolve_query_expr(engine, db, arg, scope, registry, allow_correlated)?;
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for part in partition_by {
                resolve_query_expr(engine, db, part, scope, registry, allow_correlated)?;
            }
            for item in order_by {
                resolve_query_expr(
                    engine,
                    db,
                    &mut item.expr,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            }
        }
        Expr::Correlated(_)
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => {}
    }
    Ok(())
}

/// Resolve one subquery occurrence: try eager uncorrelated execution first;
/// on a correlation-shaped binding error, bind it as a correlated subquery.
#[allow(clippy::too_many_arguments)]
pub(super) fn resolve_subquery_node(
    engine: &Engine,
    db: &str,
    kind: CorrelatedKind,
    lhs: Option<Expr>,
    subquery: &Statement,
    scope: Option<&QueryBlockScope>,
    registry: &crate::sql::execution::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
) -> Result<Expr> {
    let eager: Result<Expr> = (|| match kind {
        CorrelatedKind::Exists { negated } => {
            let exists = execute_exists_subquery(engine, db, subquery)?;
            Ok(Expr::Literal(ScalarValue::Bool(Some(if negated {
                !exists
            } else {
                exists
            }))))
        }
        CorrelatedKind::Scalar => {
            let value = execute_scalar_subquery(engine, db, subquery)?;
            Ok(Expr::Literal(value))
        }
        CorrelatedKind::In { negated } => {
            let lhs = lhs.clone().expect("IN subquery carries a lhs expression");
            let values = execute_in_subquery(engine, db, subquery)?
                .into_iter()
                .map(Expr::Literal)
                .collect();
            Ok(Expr::In {
                expr: Box::new(lhs),
                values,
                negated,
            })
        }
    })();
    match eager {
        Ok(resolved) => Ok(resolved),
        Err(e)
            if allow_correlated
                && scope.is_some()
                && crate::sql::planner::expr::is_correlation_binding_error(&e) =>
        {
            bind_correlated_subquery(
                engine,
                db,
                kind,
                lhs,
                subquery,
                scope.expect("scope checked above"),
                registry,
            )
        }
        Err(e) => Err(e),
    }
}

/// Bind a correlated subquery against its enclosing block:
/// 1. reject user parameter markers inside,
/// 2. materialize the inner block's own derived tables,
/// 3. eagerly resolve the inner block's own (uncorrelated) subqueries,
/// 4. snapshot the inner tables (schemas + batches) into the registry,
/// 5. split the inner WHERE into local and correlated conjuncts, rewriting
///    outer references to `Placeholder` markers,
/// 6. capture the scalar result type.
#[allow(clippy::too_many_arguments)]
pub(super) fn bind_correlated_subquery(
    engine: &Engine,
    db: &str,
    kind: CorrelatedKind,
    lhs: Option<Expr>,
    inner_stmt: &Statement,
    scope: &QueryBlockScope,
    registry: &crate::sql::execution::subquery_runtime::SubqueryRegistry,
) -> Result<Expr> {
    if !matches!(inner_stmt, Statement::Select { .. }) {
        return Err(TaibaiError::Sql(
            "correlated subqueries must be a single SELECT".into(),
        ));
    }
    let mut inner = inner_stmt.clone();
    if crate::sql::execution::prepared::has_placeholders(&inner) {
        return Err(TaibaiError::Sql(
            "parameter markers (?) are not supported inside correlated subqueries".into(),
        ));
    }

    let inner_derived = materialize_derived_tables(engine, db, &mut inner)?;
    if let Err(e) = resolve_query_subqueries(engine, db, &mut inner, None, registry, false) {
        if crate::sql::planner::expr::is_correlation_binding_error(&e) {
            return Err(TaibaiError::Sql(format!(
                "nested correlated subqueries are not supported (only one level of \
                 correlation is supported): {e}"
            )));
        }
        return Err(e);
    }

    // Snapshot the inner FROM tables so per-row evaluation never needs the
    // engine.
    let inner_from = match &inner {
        Statement::Select {
            from: Some(from), ..
        } => Some(from.clone()),
        _ => None,
    };
    let mut snapshot_schemas: HashMap<String, Schema> = HashMap::new();
    let mut snapshot_batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>> = HashMap::new();
    if let Some(from) = &inner_from {
        for table_ref in extract_table_refs(from, db) {
            if table_ref.database == DERIVED_DB {
                let (schema, batches) = inner_derived.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                snapshot_schemas.insert(table_ref.key.clone(), schema.clone());
                snapshot_batches.insert(
                    table_ref.key.clone(),
                    batches
                        .iter()
                        .map(|b| std::sync::Arc::new(b.clone()))
                        .collect::<Vec<_>>(),
                );
            } else {
                // Shared per query through the registry: every occurrence of
                // a correlated subquery over this table reuses one snapshot
                // instead of re-reading and re-materializing the full table.
                // The lock is held only around the map access — never across
                // the snapshot execution.
                let (schema, batches) = {
                    let cached = registry
                        .lock()
                        .unwrap()
                        .table_snapshots
                        .get(&table_ref.key)
                        .cloned();
                    match cached {
                        Some((schema, batches)) => (schema, batches),
                        None => {
                            let (schema, batches) = snapshot_table(engine, &table_ref)?;
                            let batches = batches
                                .into_iter()
                                .map(std::sync::Arc::new)
                                .collect::<Vec<_>>();
                            registry
                                .lock()
                                .unwrap()
                                .table_snapshots
                                .insert(table_ref.key.clone(), (schema.clone(), batches.clone()));
                            (schema, batches)
                        }
                    }
                };
                snapshot_schemas.insert(table_ref.key.clone(), schema);
                snapshot_batches.insert(table_ref.key.clone(), batches);
            }
        }
    }

    let (inner_schema, inner_alias_map) = match &inner_from {
        Some(from) => crate::sql::planner::build_scope_bindings(from, &snapshot_schemas),
        None => (Schema::empty(), HashMap::new()),
    };

    // Pre-resolve select-list aliases in GROUP BY / HAVING / ORDER BY the
    // same way the planner would, so alias references don't look like
    // unresolvable (correlated) columns during marking.
    if let Statement::Select {
        select_list,
        group_by,
        having,
        order_by,
        ..
    } = &mut inner
    {
        let alias_entries: Vec<(String, Expr)> = select_list
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expr(e, Some(alias)) => Some((alias.to_lowercase(), e.clone())),
                _ => None,
            })
            .collect();
        if !alias_entries.is_empty() {
            let substitute = |expr: &mut Expr| {
                *expr = crate::sql::planner::expr::substitute_select_aliases(
                    expr,
                    &alias_entries,
                    &inner_schema,
                );
            };
            for expr in group_by {
                substitute(expr);
            }
            if let Some(expr) = having {
                substitute(expr);
            }
            for item in order_by {
                substitute(&mut item.expr);
            }
        }
    }

    let mut binder = MarkerBinder {
        inner_schema: &inner_schema,
        inner_alias_map: &inner_alias_map,
        outer_schema: &scope.schema,
        outer_alias_map: &scope.alias_map,
        next_id: 0,
        params: Vec::new(),
    };

    // Split the inner WHERE into local and correlated conjuncts.
    let where_expr = match &mut inner {
        Statement::Select { where_clause, .. } => where_clause.take(),
        _ => None,
    };
    let mut correlated_conjuncts: Vec<Expr> = Vec::new();
    let mut local_conjuncts: Vec<Expr> = Vec::new();
    if let Some(where_expr) = where_expr {
        for conjunct in split_and_conjuncts(where_expr) {
            let marked = binder.mark_expr(conjunct)?;
            if crate::sql::execution::prepared::expr_contains_placeholder(&marked) {
                correlated_conjuncts.push(marked);
            } else {
                local_conjuncts.push(marked);
            }
        }
    }
    if let Statement::Select { where_clause, .. } = &mut inner {
        *where_clause = combine_conjuncts(local_conjuncts);
    }

    // Mark remaining expression positions (select list, GROUP BY, HAVING,
    // ORDER BY). Those markers stay in place and are substituted per row.
    if let Statement::Select {
        select_list,
        group_by,
        having,
        order_by,
        ..
    } = &mut inner
    {
        for item in select_list {
            if let SelectItem::Expr(expr, _) = item {
                *expr = binder.mark_expr(expr.clone())?;
            }
        }
        for expr in group_by {
            *expr = binder.mark_expr(expr.clone())?;
        }
        if let Some(expr) = having {
            *expr = binder.mark_expr(expr.clone())?;
        }
        for item in order_by {
            item.expr = binder.mark_expr(item.expr.clone())?;
        }
    }

    let result_type = match kind {
        CorrelatedKind::Exists { .. } | CorrelatedKind::In { .. } => {
            crate::protocol::types::DataType::Bool
        }
        CorrelatedKind::Scalar => {
            // Probe the output type with a single-row execution of the local
            // part. Fall back to Int64 when the probe cannot run cleanly
            // (e.g. markers remain in the select list); projection coerces.
            let mut probe = inner.clone();
            if let Statement::Select { limit, offset, .. } = &mut probe {
                *limit = Some(Expr::Literal(ScalarValue::Int64(Some(1))));
                *offset = None;
            }
            match exec_select(engine, db, &probe) {
                Ok(ExecResult::Query { schema, .. }) if !schema.columns.is_empty() => {
                    schema.columns[0].data_type
                }
                _ => crate::protocol::types::DataType::Int64,
            }
        }
    };

    let id = registry.lock().unwrap().allocate(
        crate::sql::execution::subquery_runtime::CorrelatedSnapshot {
            schemas: snapshot_schemas,
            batches: snapshot_batches,
            cache: HashMap::new(),
        },
    );

    Ok(Expr::Correlated(Box::new(CorrelatedSubquery {
        id,
        kind,
        lhs: lhs.map(Box::new),
        stmt: Box::new(inner),
        conjuncts: correlated_conjuncts,
        params: binder.params,
        result_type,
    })))
}

/// Rewrites references that resolve outside the inner block into
/// `Placeholder` markers and records their enclosing-block column indexes.
pub(super) struct MarkerBinder<'a> {
    inner_schema: &'a Schema,
    inner_alias_map: &'a crate::sql::planner::AliasMap,
    outer_schema: &'a Schema,
    outer_alias_map: &'a crate::sql::planner::AliasMap,
    next_id: usize,
    params: Vec<(usize, usize)>,
}

impl<'a> MarkerBinder<'a> {
    pub(super) fn new_marker(&mut self, outer_idx: usize) -> Expr {
        let id = self.next_id;
        self.next_id += 1;
        self.params.push((id, outer_idx));
        Expr::Placeholder(id)
    }

    pub(super) fn mark_expr(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            // `COUNT(*)` is spelled as a Function over Column("*"); the star
            // is not a real column reference.
            Expr::Column(ref name) if name == "*" => Ok(expr),
            Expr::Column(name) => {
                if self.inner_schema.index_of(&name).is_some() {
                    return Ok(Expr::Column(name));
                }
                if let Some(idx) = self.outer_schema.index_of(&name) {
                    return Ok(self.new_marker(idx));
                }
                Err(TaibaiError::Sql(format!("column '{name}' not found")))
            }
            Expr::QualifiedColumn { table, column } => {
                let table_lower = table.to_lowercase();
                if let Some(binding) = self.inner_alias_map.get(&table_lower) {
                    if col_in_range(self.inner_schema, binding, &column).is_some() {
                        return Ok(Expr::QualifiedColumn { table, column });
                    }
                    return Err(TaibaiError::Sql(format!(
                        "column '{column}' not found in table '{table}'"
                    )));
                }
                if let Some(binding) = self.outer_alias_map.get(&table_lower) {
                    if let Some(idx) = col_in_range(self.outer_schema, binding, &column) {
                        return Ok(self.new_marker(idx));
                    }
                    return Err(TaibaiError::Sql(format!(
                        "column '{column}' not found in table '{table}'"
                    )));
                }
                Err(TaibaiError::Sql(format!(
                    "unknown table or alias '{table}' (correlated references must resolve to \
                     the immediately enclosing query block)"
                )))
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.mark_expr(*left)?),
                op,
                right: Box::new(self.mark_expr(*right)?),
            }),
            Expr::UnaryNot(inner) => Ok(Expr::UnaryNot(Box::new(self.mark_expr(*inner)?))),
            Expr::UnaryMinus(inner) => Ok(Expr::UnaryMinus(Box::new(self.mark_expr(*inner)?))),
            Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(self.mark_expr(*inner)?))),
            Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(self.mark_expr(*inner)?))),
            Expr::In {
                expr,
                values,
                negated,
            } => Ok(Expr::In {
                expr: Box::new(self.mark_expr(*expr)?),
                values: values
                    .into_iter()
                    .map(|v| self.mark_expr(v))
                    .collect::<Result<Vec<_>>>()?,
                negated,
            }),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(Expr::Between {
                expr: Box::new(self.mark_expr(*expr)?),
                low: Box::new(self.mark_expr(*low)?),
                high: Box::new(self.mark_expr(*high)?),
                negated,
            }),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Ok(Expr::Case {
                operand: operand
                    .map(|e| self.mark_expr(*e))
                    .transpose()?
                    .map(Box::new),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(w, t)| Ok((self.mark_expr(w)?, self.mark_expr(t)?)))
                    .collect::<Result<Vec<_>>>()?,
                else_clause: else_clause
                    .map(|e| self.mark_expr(*e))
                    .transpose()?
                    .map(Box::new),
            }),
            Expr::Like {
                expr,
                pattern,
                negated,
            } => Ok(Expr::Like {
                expr: Box::new(self.mark_expr(*expr)?),
                pattern: Box::new(self.mark_expr(*pattern)?),
                negated,
            }),
            Expr::Function {
                name,
                args,
                distinct,
            } => Ok(Expr::Function {
                name,
                args: args
                    .into_iter()
                    .map(|a| self.mark_expr(a))
                    .collect::<Result<Vec<_>>>()?,
                distinct,
            }),
            other => Ok(other),
        }
    }
}

pub(super) fn col_in_range(
    schema: &Schema,
    binding: &crate::sql::planner::TableBinding,
    column: &str,
) -> Option<usize> {
    (binding.start..binding.end).find(|&idx| {
        schema
            .columns
            .get(idx)
            .map(|c| c.name.eq_ignore_ascii_case(column))
            .unwrap_or(false)
    })
}
