#![allow(dead_code)]
use std::collections::{HashMap, VecDeque};

use crate::common::error::{MonaxError, Result};
use crate::sql::parser::ast::{Expr, Statement};
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
            return Err(MonaxError::Sql("empty query".into()));
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
        Statement::Select {
            where_clause: Some(ref mut expr),
            ..
        } => {
            substitute_placeholders(expr, params)?;
        }
        Statement::Insert { values, .. } => {
            for row in values.iter_mut() {
                for val in row.iter_mut() {
                    substitute_placeholders(val, params)?;
                }
            }
        }
        Statement::Update {
            where_clause: Some(ref mut expr),
            assignments,
            ..
        } => {
            substitute_placeholders(expr, params)?;
            for assign in assignments.iter_mut() {
                substitute_placeholders(&mut assign.value, params)?;
            }
        }
        Statement::Update {
            where_clause: None,
            assignments,
            ..
        } => {
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
        _ => {}
    }
    Ok(())
}

fn substitute_placeholders(expr: &mut Expr, params: &[ScalarValue]) -> Result<()> {
    match expr {
        Expr::Placeholder(idx) => {
            if *idx >= params.len() {
                return Err(MonaxError::Sql(format!(
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
        Expr::Function { args, .. } => {
            for arg in args.iter_mut() {
                substitute_placeholders(arg, params)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn count_placeholders(stmt: &Statement) -> usize {
    let mut count = 0;
    match stmt {
        Statement::Select {
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
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
        Statement::Insert { values, .. } => {
            for row in values {
                for val in row {
                    count_expr_placeholders(val, &mut count);
                }
            }
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
        _ => {}
    }
    count
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
        Expr::Function { args, .. } => {
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
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
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
        Statement::Insert { values, .. } => {
            for row in values {
                for val in row {
                    check_expr_for_placeholders(val, &mut found);
                }
            }
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
        _ => {}
    }
    found
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
        Expr::Function { args, .. } => {
            for arg in args {
                check_expr_for_placeholders(arg, found);
            }
        }
        _ => {}
    }
}
