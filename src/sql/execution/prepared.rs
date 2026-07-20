#![allow(dead_code)]
use std::collections::{HashMap, VecDeque};

use crate::common::error::{Result, TaibaiError};
use crate::sql::parser::ast::{Expr, FromClause, JoinExpr, Statement, TableRef, WindowFunc};
use crate::sql::parser::parser::Parser;
use crate::sql::planner::LogicalPlan;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub id: u32,
    pub sql: String,
    pub stmt: Statement,
    pub param_types: Vec<crate::protocol::types::DataType>,
    pub param_count: usize,
    pub params: Vec<ScalarValue>,
    pub cached_plan: Option<LogicalPlan>,
    pub planned_database: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatementCache {
    next_id: u32,
    statements: HashMap<u32, PreparedStatement>,
    lru_order: VecDeque<u32>,
    max_size: usize,
}

impl Default for PreparedStatementCache {
    fn default() -> Self {
        Self::new()
    }
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            statements: HashMap::new(),
            lru_order: VecDeque::new(),
            max_size: 128,
        }
    }

    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            next_id: 1,
            statements: HashMap::new(),
            lru_order: VecDeque::new(),
            max_size,
        }
    }

    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(TaibaiError::Sql("empty query".into()));
        }

        let id = self.next_id;
        self.next_id += 1;

        let param_count = count_placeholders(&stmts[0]);

        let prepared = PreparedStatement {
            id,
            sql: sql.to_string(),
            stmt: stmts.into_iter().next().unwrap(),
            param_types: Vec::new(),
            param_count,
            params: Vec::new(),
            cached_plan: None,
            planned_database: None,
        };

        self.statements.insert(id, prepared.clone());
        self.lru_order.push_front(id);
        self.evict_if_needed();
        Ok(prepared)
    }

    pub fn get(&self, id: u32) -> Option<&PreparedStatement> {
        self.statements.get(&id)
    }

    pub fn get_mut(&mut self, id: u32) -> Option<&mut PreparedStatement> {
        if self.statements.contains_key(&id) {
            self.lru_order.retain(|&x| x != id);
            self.lru_order.push_front(id);
            self.statements.get_mut(&id)
        } else {
            None
        }
    }

    pub fn remove(&mut self, id: u32) -> Option<PreparedStatement> {
        self.lru_order.retain(|&x| x != id);
        self.statements.remove(&id)
    }

    pub fn insert(&mut self, stmt: PreparedStatement) {
        let id = stmt.id;
        self.statements.insert(id, stmt);
        self.lru_order.retain(|&x| x != id);
        self.lru_order.push_front(id);
        self.evict_if_needed();
    }

    pub fn close(&mut self, id: u32) -> bool {
        self.lru_order.retain(|&x| x != id);
        self.statements.remove(&id).is_some()
    }

    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    pub fn clear(&mut self) {
        self.statements.clear();
        self.lru_order.clear();
    }

    fn evict_if_needed(&mut self) {
        while self.statements.len() > self.max_size {
            if let Some(oldest) = self.lru_order.pop_back() {
                self.statements.remove(&oldest);
            } else {
                break;
            }
        }
    }
}

pub fn bind_params(stmt: &mut Statement, params: &[ScalarValue]) -> Result<()> {
    match stmt {
        // Select: replace placeholders in every field that can hold an Expr.
        // Coverage must stay in sync with count_placeholders / has_placeholders.
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
            for item in select_list.iter_mut() {
                if let crate::sql::parser::ast::SelectItem::Expr(e, _) = item {
                    substitute_placeholders(e, params)?;
                }
            }
            if let Some(from_clause) = from {
                bind_from_clause(from_clause, params)?;
            }
            if let Some(expr) = where_clause {
                substitute_placeholders(expr, params)?;
            }
            for e in group_by.iter_mut() {
                substitute_placeholders(e, params)?;
            }
            if let Some(expr) = having {
                substitute_placeholders(expr, params)?;
            }
            for item in order_by.iter_mut() {
                substitute_placeholders(&mut item.expr, params)?;
            }
            if let Some(expr) = limit {
                substitute_placeholders(expr, params)?;
            }
            if let Some(expr) = offset {
                substitute_placeholders(expr, params)?;
            }
        }
        // Union: bind placeholders in the trailing order_by / limit / offset
        // (when present on the Union node itself) AND recurse into the left /
        // right children, since each side is itself a Statement whose fields
        // (e.g. the right Select's trailing ORDER BY / LIMIT / OFFSET) may hold
        // placeholders. In practice this parser attaches trailing clauses to
        // the right-hand Select, so the recursion is what actually binds them.
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            bind_params(left, params)?;
            bind_params(right, params)?;
            for item in order_by.iter_mut() {
                substitute_placeholders(&mut item.expr, params)?;
            }
            if let Some(expr) = limit {
                substitute_placeholders(expr, params)?;
            }
            if let Some(expr) = offset {
                substitute_placeholders(expr, params)?;
            }
        }
        Statement::Insert { values, .. } => {
            for row in values.iter_mut() {
                for val in row.iter_mut() {
                    substitute_placeholders(val, params)?;
                }
            }
        }
        Statement::Replace { values, .. } => {
            for row in values.iter_mut() {
                for val in row.iter_mut() {
                    substitute_placeholders(val, params)?;
                }
            }
        }
        Statement::ReplaceSelect { query, .. } => bind_params(query, params)?,
        Statement::Update {
            where_clause,
            assignments,
            ..
        } => {
            if let Some(expr) = where_clause {
                substitute_placeholders(expr, params)?;
            }
            for assign in assignments.iter_mut() {
                substitute_placeholders(&mut assign.value, params)?;
            }
        }
        Statement::Delete {
            where_clause: Some(ref mut expr),
            ..
        } => {
            substitute_placeholders(expr, params)?;
        }
        // Explain wraps an inner statement that may itself hold placeholders.
        Statement::Explain { stmt: inner, .. } => {
            bind_params(inner, params)?;
        }
        _ => {}
    }
    Ok(())
}

/// Walk a FROM clause and bind placeholders in JOIN conditions and in any
/// derived-table subquery (`FROM (SELECT ...) AS dt`).
fn bind_from_clause(from: &mut FromClause, params: &[ScalarValue]) -> Result<()> {
    match from {
        FromClause::Join(JoinExpr { left, joins }) => {
            bind_from_clause(left, params)?;
            for j in joins.iter_mut() {
                if let Some(cond) = j.condition.as_mut() {
                    substitute_placeholders(cond, params)?;
                }
                bind_table_ref(&mut j.right, params)?;
            }
        }
        FromClause::Table(t) => bind_table_ref(t, params)?,
    }
    Ok(())
}

/// If a table reference is a derived-table subquery, recurse into it.
fn bind_table_ref(t: &mut TableRef, params: &[ScalarValue]) -> Result<()> {
    if let TableRef::Subquery { query, .. } = t {
        bind_params(query, params)?;
    }
    Ok(())
}

fn substitute_placeholders(expr: &mut Expr, params: &[ScalarValue]) -> Result<()> {
    match expr {
        Expr::Placeholder(idx) => {
            if *idx >= params.len() {
                return Err(TaibaiError::Sql(format!(
                    "parameter index {} out of bounds (only {} parameters provided)",
                    idx,
                    params.len()
                )));
            }
            *expr = Expr::Literal(params[*idx].clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            substitute_placeholders(left, params)?;
            substitute_placeholders(right, params)?;
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            substitute_placeholders(inner, params)?;
        }
        Expr::In {
            expr: inner,
            values,
            ..
        } => {
            substitute_placeholders(inner, params)?;
            for v in values.iter_mut() {
                substitute_placeholders(v, params)?;
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            substitute_placeholders(inner, params)?;
            substitute_placeholders(low, params)?;
            substitute_placeholders(high, params)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(ref mut op) = operand {
                substitute_placeholders(op, params)?;
            }
            for (when, then) in when_clauses.iter_mut() {
                substitute_placeholders(when, params)?;
                substitute_placeholders(then, params)?;
            }
            if let Some(ref mut el) = else_clause {
                substitute_placeholders(el, params)?;
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            substitute_placeholders(inner, params)?;
            substitute_placeholders(pattern, params)?;
        }
        Expr::Exists(inner) => {
            substitute_placeholders(inner, params)?;
        }
        // Subquery: the inner SELECT may hold placeholders (e.g.
        // `WHERE id IN (SELECT id FROM t2 WHERE x = ?)`).
        Expr::Subquery(inner) => {
            bind_params(inner, params)?;
        }
        Expr::Function { args, .. } => {
            for arg in args.iter_mut() {
                substitute_placeholders(arg, params)?;
            }
        }
        // Window function: placeholders may appear in PARTITION BY / ORDER BY
        // or in the aggregate args / LEAD|LAG default values.
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            ..
        } => {
            substitute_window_func(func, params)?;
            for e in partition_by.iter_mut() {
                substitute_placeholders(e, params)?;
            }
            for item in order_by.iter_mut() {
                substitute_placeholders(&mut item.expr, params)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Walk the inner Exprs of a WindowFunc (aggregate args / LEAD|LAG defaults).
fn substitute_window_func(wf: &mut WindowFunc, params: &[ScalarValue]) -> Result<()> {
    match wf {
        WindowFunc::Lead { default, .. } | WindowFunc::Lag { default, .. } => {
            if let Some(d) = default.as_mut() {
                substitute_placeholders(d, params)?;
            }
        }
        WindowFunc::Aggregate { args, .. } => {
            for arg in args.iter_mut() {
                substitute_placeholders(arg, params)?;
            }
        }
        // RowNumber / Rank / DenseRank carry no Expr.
        _ => {}
    }
    Ok(())
}

pub fn count_placeholders(stmt: &Statement) -> usize {
    let mut count = 0;
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
            for item in select_list {
                if let crate::sql::parser::ast::SelectItem::Expr(e, _) = item {
                    count_expr_placeholders(e, &mut count);
                }
            }
            if let Some(f) = from {
                count += count_from_clause(f);
            }
            if let Some(ref e) = where_clause {
                count_expr_placeholders(e, &mut count);
            }
            for e in group_by {
                count_expr_placeholders(e, &mut count);
            }
            if let Some(ref e) = having {
                count_expr_placeholders(e, &mut count);
            }
            for item in order_by {
                count_expr_placeholders(&item.expr, &mut count);
            }
            if let Some(ref e) = limit {
                count_expr_placeholders(e, &mut count);
            }
            if let Some(ref e) = offset {
                count_expr_placeholders(e, &mut count);
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
            count += count_placeholders(left);
            count += count_placeholders(right);
            for item in order_by {
                count_expr_placeholders(&item.expr, &mut count);
            }
            if let Some(ref e) = limit {
                count_expr_placeholders(e, &mut count);
            }
            if let Some(ref e) = offset {
                count_expr_placeholders(e, &mut count);
            }
        }
        Statement::Insert { values, .. } => {
            for row in values {
                for val in row {
                    count_expr_placeholders(val, &mut count);
                }
            }
        }
        Statement::Replace { values, .. } => {
            for row in values {
                for val in row {
                    count_expr_placeholders(val, &mut count);
                }
            }
        }
        Statement::ReplaceSelect { query, .. } => {
            count += count_placeholders(query);
        }
        Statement::Update {
            where_clause,
            assignments,
            ..
        } => {
            if let Some(ref e) = where_clause {
                count_expr_placeholders(e, &mut count);
            }
            for assign in assignments {
                count_expr_placeholders(&assign.value, &mut count);
            }
        }
        Statement::Delete {
            where_clause: Some(ref e),
            ..
        } => {
            count_expr_placeholders(e, &mut count);
        }
        Statement::Explain { stmt: inner, .. } => {
            count += count_placeholders(inner);
        }
        _ => {}
    }
    count
}

/// Count placeholders in a FROM clause (JOIN conditions + derived-table subqueries).
fn count_from_clause(from: &FromClause) -> usize {
    let mut count = 0;
    match from {
        FromClause::Join(JoinExpr { left, joins }) => {
            count += count_from_clause(left);
            for j in joins {
                if let Some(cond) = &j.condition {
                    count_expr_placeholders(cond, &mut count);
                }
                count += count_table_ref(&j.right);
            }
        }
        FromClause::Table(t) => count += count_table_ref(t),
    }
    count
}

/// Count placeholders inside a derived-table subquery (plain tables hold none).
fn count_table_ref(t: &TableRef) -> usize {
    match t {
        TableRef::Subquery { query, .. } => count_placeholders(query),
        TableRef::Table { .. } => 0,
    }
}

fn count_expr_placeholders(expr: &Expr, count: &mut usize) {
    match expr {
        Expr::Placeholder(_) => *count += 1,
        Expr::BinaryOp { left, right, .. } => {
            count_expr_placeholders(left, count);
            count_expr_placeholders(right, count);
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            count_expr_placeholders(inner, count);
        }
        Expr::In {
            expr: inner,
            values,
            ..
        } => {
            count_expr_placeholders(inner, count);
            for v in values {
                count_expr_placeholders(v, count);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            count_expr_placeholders(inner, count);
            count_expr_placeholders(low, count);
            count_expr_placeholders(high, count);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(ref op) = operand {
                count_expr_placeholders(op, count);
            }
            for (when, then) in when_clauses {
                count_expr_placeholders(when, count);
                count_expr_placeholders(then, count);
            }
            if let Some(ref el) = else_clause {
                count_expr_placeholders(el, count);
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            count_expr_placeholders(inner, count);
            count_expr_placeholders(pattern, count);
        }
        Expr::Exists(inner) => {
            count_expr_placeholders(inner, count);
        }
        Expr::Subquery(inner) => {
            *count += count_placeholders(inner);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                count_expr_placeholders(arg, count);
            }
        }
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            ..
        } => {
            count_window_func(func, count);
            for e in partition_by {
                count_expr_placeholders(e, count);
            }
            for item in order_by {
                count_expr_placeholders(&item.expr, count);
            }
        }
        _ => {}
    }
}

/// Count placeholders inside a WindowFunc's Expr fields.
fn count_window_func(wf: &WindowFunc, count: &mut usize) {
    match wf {
        WindowFunc::Lead { default, .. } | WindowFunc::Lag { default, .. } => {
            if let Some(d) = default {
                count_expr_placeholders(d, count);
            }
        }
        WindowFunc::Aggregate { args, .. } => {
            for arg in args {
                count_expr_placeholders(arg, count);
            }
        }
        _ => {}
    }
}

/// Check if a statement contains any placeholder expressions.
pub fn has_placeholders(stmt: &Statement) -> bool {
    let mut found = false;
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
            for item in select_list {
                if let crate::sql::parser::ast::SelectItem::Expr(e, _) = item {
                    check_expr_for_placeholders(e, &mut found);
                }
            }
            if let Some(f) = from {
                found = found || has_placeholders_from_clause(f);
            }
            if let Some(ref e) = where_clause {
                check_expr_for_placeholders(e, &mut found);
            }
            for e in group_by {
                check_expr_for_placeholders(e, &mut found);
            }
            if let Some(ref e) = having {
                check_expr_for_placeholders(e, &mut found);
            }
            for item in order_by {
                check_expr_for_placeholders(&item.expr, &mut found);
            }
            if let Some(ref e) = limit {
                check_expr_for_placeholders(e, &mut found);
            }
            if let Some(ref e) = offset {
                check_expr_for_placeholders(e, &mut found);
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
            // Recurse into children first (cheap short-circuit once found).
            found = found || has_placeholders(left) || has_placeholders(right);
            for item in order_by {
                check_expr_for_placeholders(&item.expr, &mut found);
            }
            if let Some(ref e) = limit {
                check_expr_for_placeholders(e, &mut found);
            }
            if let Some(ref e) = offset {
                check_expr_for_placeholders(e, &mut found);
            }
        }
        Statement::Insert { values, .. } => {
            for row in values {
                for val in row {
                    check_expr_for_placeholders(val, &mut found);
                }
            }
        }
        Statement::Replace { values, .. } => {
            for row in values {
                for val in row {
                    check_expr_for_placeholders(val, &mut found);
                }
            }
        }
        Statement::ReplaceSelect { query, .. } => {
            found |= has_placeholders(query);
        }
        Statement::Update {
            where_clause,
            assignments,
            ..
        } => {
            if let Some(ref e) = where_clause {
                check_expr_for_placeholders(e, &mut found);
            }
            for assign in assignments {
                check_expr_for_placeholders(&assign.value, &mut found);
            }
        }
        Statement::Delete {
            where_clause: Some(ref e),
            ..
        } => {
            check_expr_for_placeholders(e, &mut found);
        }
        Statement::Explain { stmt: inner, .. } => {
            found = found || has_placeholders(inner);
        }
        _ => {}
    }
    found
}

/// Check a FROM clause for placeholders (JOIN conditions + derived-table subqueries).
fn has_placeholders_from_clause(from: &FromClause) -> bool {
    match from {
        FromClause::Join(JoinExpr { left, joins }) => {
            if has_placeholders_from_clause(left) {
                return true;
            }
            for j in joins {
                if let Some(cond) = &j.condition {
                    let mut found = false;
                    check_expr_for_placeholders(cond, &mut found);
                    if found {
                        return true;
                    }
                }
                if has_placeholders_table_ref(&j.right) {
                    return true;
                }
            }
            false
        }
        FromClause::Table(t) => has_placeholders_table_ref(t),
    }
}

/// Check a table reference; derived-table subqueries may hold placeholders.
fn has_placeholders_table_ref(t: &TableRef) -> bool {
    match t {
        TableRef::Subquery { query, .. } => has_placeholders(query),
        TableRef::Table { .. } => false,
    }
}

fn check_expr_for_placeholders(expr: &Expr, found: &mut bool) {
    if *found {
        return;
    }
    match expr {
        Expr::Placeholder(_) => *found = true,
        Expr::BinaryOp { left, right, .. } => {
            check_expr_for_placeholders(left, found);
            check_expr_for_placeholders(right, found);
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            check_expr_for_placeholders(inner, found);
        }
        Expr::In {
            expr: inner,
            values,
            ..
        } => {
            check_expr_for_placeholders(inner, found);
            for v in values {
                check_expr_for_placeholders(v, found);
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            check_expr_for_placeholders(inner, found);
            check_expr_for_placeholders(low, found);
            check_expr_for_placeholders(high, found);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(ref op) = operand {
                check_expr_for_placeholders(op, found);
            }
            for (when, then) in when_clauses {
                check_expr_for_placeholders(when, found);
                check_expr_for_placeholders(then, found);
            }
            if let Some(ref el) = else_clause {
                check_expr_for_placeholders(el, found);
            }
        }
        Expr::Like {
            expr: inner,
            pattern,
            ..
        } => {
            check_expr_for_placeholders(inner, found);
            check_expr_for_placeholders(pattern, found);
        }
        Expr::Exists(inner) => {
            check_expr_for_placeholders(inner, found);
        }
        Expr::Subquery(inner) => {
            *found = *found || has_placeholders(inner);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                check_expr_for_placeholders(arg, found);
            }
        }
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            ..
        } => {
            check_window_func_for_placeholders(func, found);
            for e in partition_by {
                check_expr_for_placeholders(e, found);
            }
            for item in order_by {
                check_expr_for_placeholders(&item.expr, found);
            }
        }
        _ => {}
    }
}

/// Check a WindowFunc's inner Expr fields for placeholders.
fn check_window_func_for_placeholders(wf: &WindowFunc, found: &mut bool) {
    match wf {
        WindowFunc::Lead { default, .. } | WindowFunc::Lag { default, .. } => {
            if let Some(d) = default {
                check_expr_for_placeholders(d, found);
            }
        }
        WindowFunc::Aggregate { args, .. } => {
            for arg in args {
                check_expr_for_placeholders(arg, found);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{Expr, OrderByItem, Statement};
    use crate::sql::parser::parser::Parser;
    use crate::types::scalar::ScalarValue;

    /// Parse a single statement. Panics on parse error (test helper).
    fn parse_one(sql: &str) -> Statement {
        let mut stmts = Parser::parse(sql).expect("parse failed");
        assert_eq!(stmts.len(), 1, "expected exactly one statement");
        stmts.remove(0)
    }

    /// True if `expr` is still an unsubstituted placeholder.
    fn is_placeholder(expr: &Expr) -> bool {
        matches!(expr, Expr::Placeholder(_))
    }

    #[test]
    fn test_bind_limit_offset() {
        let mut stmt = parse_one("SELECT * FROM t LIMIT ? OFFSET ?");
        let params = [ScalarValue::Int64(Some(20)), ScalarValue::Int64(Some(5))];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Select { limit, offset, .. } => {
                assert!(
                    matches!(limit, Some(Expr::Literal(ScalarValue::Int64(Some(20))))),
                    "limit not bound: {:?}",
                    limit
                );
                assert!(
                    matches!(offset, Some(Expr::Literal(ScalarValue::Int64(Some(5))))),
                    "offset not bound: {:?}",
                    offset
                );
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_bind_group_by_having() {
        let mut stmt = parse_one("SELECT a, count(*) FROM t GROUP BY ? HAVING count(*) > ?");
        let params = [ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(10))];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Select {
                group_by, having, ..
            } => {
                assert_eq!(group_by.len(), 1);
                assert!(
                    matches!(&group_by[0], Expr::Literal(ScalarValue::Int64(Some(1)))),
                    "group_by not bound: {:?}",
                    group_by[0]
                );
                // having should no longer contain a placeholder
                if let Some(h) = &having {
                    assert!(!is_placeholder(h), "having still a placeholder");
                } else {
                    panic!("expected having clause");
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_bind_order_by() {
        let mut stmt = parse_one("SELECT * FROM t ORDER BY ? DESC");
        let params = [ScalarValue::Int64(Some(1))];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Select { order_by, .. } => {
                assert_eq!(order_by.len(), 1);
                assert!(
                    matches!(
                        &order_by[0],
                        OrderByItem {
                            expr: Expr::Literal(ScalarValue::Int64(Some(1))),
                            desc: true
                        }
                    ),
                    "order_by not bound: {:?}",
                    order_by[0]
                );
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_bind_where_clause_regression() {
        // where_clause was already supported before the fix — ensure it still works.
        let mut stmt = parse_one("SELECT * FROM t WHERE id = ?");
        let params = [ScalarValue::Int64(Some(42))];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Select {
                where_clause: Some(Expr::BinaryOp { right, .. }),
                ..
            } => assert!(
                matches!(&*right, Expr::Literal(ScalarValue::Int64(Some(42)))),
                "where not bound: {:?}",
                right
            ),
            _ => panic!("expected Select with where"),
        }
    }

    #[test]
    fn test_bind_union_trailing_clauses_on_right_select() {
        // NOTE: in this parser, trailing ORDER BY / LIMIT / OFFSET after
        // `... UNION SELECT ...` are consumed by the right-hand parse_select()
        // and attached to the right Select, NOT to the Union node itself.
        // So placeholder binding for them flows through the Select branch,
        // which we verify here by descending into the right child.
        let mut stmt =
            parse_one("SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY ? LIMIT ? OFFSET ?");
        let params = [
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(50)),
            ScalarValue::Int64(Some(5)),
        ];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Union { right, .. } => match &*right {
                Statement::Select {
                    order_by,
                    limit,
                    offset,
                    ..
                } => {
                    assert_eq!(order_by.len(), 1);
                    assert!(matches!(
                        &order_by[0].expr,
                        Expr::Literal(ScalarValue::Int64(Some(2)))
                    ));
                    assert!(matches!(
                        limit,
                        Some(Expr::Literal(ScalarValue::Int64(Some(50))))
                    ));
                    assert!(matches!(
                        offset,
                        Some(Expr::Literal(ScalarValue::Int64(Some(5))))
                    ));
                }
                _ => panic!("expected right child to be Select"),
            },
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_bind_too_few_params_errors() {
        let mut stmt = parse_one("SELECT * FROM t LIMIT ? OFFSET ?");
        // Only one parameter provided for two placeholders.
        let params = [ScalarValue::Int64(Some(20))];
        let res = bind_params(&mut stmt, &params);
        assert!(res.is_err(), "expected out-of-bounds error");
    }

    // --- Coverage for previously-missed locations ---

    #[test]
    fn test_bind_select_list() {
        // `SELECT ?` puts the placeholder in select_list (SelectItem::Expr).
        let mut stmt = parse_one("SELECT ? AS foo, a + ? FROM t");
        let params = [ScalarValue::Int64(Some(7)), ScalarValue::Int64(Some(3))];
        bind_params(&mut stmt, &params).expect("bind failed");

        match stmt {
            Statement::Select { select_list, .. } => {
                use crate::sql::parser::ast::SelectItem;
                assert!(matches!(
                    &select_list[0],
                    SelectItem::Expr(Expr::Literal(ScalarValue::Int64(Some(7))), _)
                ));
                assert!(matches!(
                    &select_list[1],
                    SelectItem::Expr(Expr::BinaryOp { .. }, _)
                ));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_bind_join_condition() {
        // Placeholder inside a JOIN ON condition lives in FromClause.
        let mut stmt = parse_one("SELECT * FROM a JOIN b ON a.id = ?");
        let params = [ScalarValue::Int64(Some(99))];
        bind_params(&mut stmt, &params).expect("bind failed");

        // After bind, no Expr in the whole statement should still be a Placeholder.
        assert!(
            !has_placeholders(&stmt),
            "join condition placeholder not bound"
        );
    }

    #[test]
    fn test_bind_derived_table_subquery() {
        // Placeholder inside a FROM-clause derived table (TableRef::Subquery).
        let mut stmt = parse_one("SELECT * FROM (SELECT * FROM t WHERE x = ?) AS dt");
        let params = [ScalarValue::Int64(Some(5))];
        bind_params(&mut stmt, &params).expect("bind failed");
        assert!(
            !has_placeholders(&stmt),
            "derived-table placeholder not bound"
        );
    }

    #[test]
    fn test_bind_scalar_subquery() {
        // Placeholder inside a scalar subquery in WHERE (Expr::Subquery).
        let mut stmt =
            parse_one("SELECT * FROM t WHERE total > (SELECT SUM(x) FROM t2 WHERE y = ?)");
        let params = [ScalarValue::Int64(Some(10))];
        bind_params(&mut stmt, &params).expect("bind failed");
        assert!(!has_placeholders(&stmt), "subquery placeholder not bound");
    }

    #[test]
    fn test_bind_in_subquery() {
        // Placeholder inside `x IN (SELECT ... WHERE ?)` — the Subquery sits in
        // Expr::In.values, exercising the In + Subquery recursion together.
        let mut stmt = parse_one("SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE z = ?)");
        let params = [ScalarValue::Int64(Some(42))];
        bind_params(&mut stmt, &params).expect("bind failed");
        assert!(
            !has_placeholders(&stmt),
            "IN-subquery placeholder not bound"
        );
    }

    #[test]
    fn test_bind_window_function() {
        // Placeholders in PARTITION BY and ORDER BY of a window function.
        let mut stmt = parse_one("SELECT *, ROW_NUMBER() OVER (PARTITION BY ? ORDER BY ?) FROM t");
        let params = [ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))];
        bind_params(&mut stmt, &params).expect("bind failed");
        assert!(
            !has_placeholders(&stmt),
            "window-function placeholders not bound"
        );
    }

    #[test]
    fn test_bind_explain_wrapped() {
        // Placeholder inside EXPLAIN's inner statement.
        let mut stmt = parse_one("EXPLAIN SELECT * FROM t WHERE id = ?");
        let params = [ScalarValue::Int64(Some(1))];
        bind_params(&mut stmt, &params).expect("bind failed");
        assert!(
            !has_placeholders(&stmt),
            "explain inner placeholder not bound"
        );
    }

    #[test]
    fn test_count_and_has_placeholders_consistent() {
        // For every top-level statement shape, count_placeholders and
        // has_placeholders must agree (both see placeholders, or neither does).
        let cases = [
            "SELECT * FROM t WHERE id = ?",
            "SELECT * FROM t LIMIT ? OFFSET ?",
            "SELECT a FROM t GROUP BY ? HAVING count(*) > ?",
            "SELECT * FROM t ORDER BY ?",
            // UNION: trailing clauses attach to the right-hand Select; the
            // three functions recurse into children, so they stay consistent.
            "SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY ? LIMIT ? OFFSET ?",
            // Previously-missed locations (now covered).
            "SELECT ? AS foo FROM t",
            "SELECT * FROM a JOIN b ON a.id = ?",
            "SELECT * FROM (SELECT * FROM t WHERE x = ?) AS dt",
            "SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE z = ?)",
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY ? ORDER BY ?) FROM t",
            "EXPLAIN SELECT * FROM t WHERE id = ?",
            "INSERT INTO t (a) VALUES (?)",
            "REPLACE INTO t (a) VALUES (?)",
            "REPLACE INTO t (a) SELECT a FROM s WHERE id = ?",
            "UPDATE t SET a = ? WHERE id = ?",
            "DELETE FROM t WHERE id = ?",
        ];
        for sql in cases {
            let stmt = parse_one(sql);
            let count = count_placeholders(&stmt);
            let has = has_placeholders(&stmt);
            assert_eq!(
                count > 0,
                has,
                "count_placeholders (={}) and has_placeholders (={}) disagree for: {}",
                count,
                has,
                sql
            );
            assert!(count > 0, "expected at least one placeholder in: {}", sql);
        }
    }
}
