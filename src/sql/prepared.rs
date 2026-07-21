use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

/// A prepared statement: parsed SQL + optionally planned, ready for execution.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// The statement ID (assigned by the server).
    pub id: u32,
    /// The original SQL string.
    pub sql: String,
    /// The parsed AST statement.
    pub stmt: Statement,
    /// The parameter types (for future parameterized queries).
    pub param_types: Vec<crate::protocol::types::DataType>,
}

/// Manages prepared statements for a session.
#[derive(Debug, Clone, Default)]
pub struct PreparedStatementCache {
    /// Next statement ID.
    next_id: u32,
    /// Map from statement ID to prepared statement.
    statements: HashMap<u32, PreparedStatement>,
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            statements: HashMap::new(),
        }
    }

    /// Prepare a SQL statement and store it in the cache.
    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(TaibaiError::Sql("empty query".into()));
        }

        let id = self.next_id;
        self.next_id += 1;

        let prepared = PreparedStatement {
            id,
            sql: sql.to_string(),
            stmt: stmts.into_iter().next().unwrap(),
            param_types: Vec::new(),
        };

        self.statements.insert(id, prepared.clone());
        Ok(prepared)
    }

    /// Get a prepared statement by ID.
    pub fn get(&self, id: u32) -> Option<&PreparedStatement> {
        self.statements.get(&id)
    }

    /// Remove a prepared statement by ID.
    pub fn close(&mut self, id: u32) -> bool {
        self.statements.remove(&id).is_some()
    }

    /// Returns the number of active prepared statements.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// Clear all prepared statements.
    pub fn clear(&mut self) {
        self.statements.clear();
    }
}

#[cfg(test)]
#[path = "prepared_tests.rs"]
mod tests;
