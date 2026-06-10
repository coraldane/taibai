use std::collections::HashMap;

use crate::common::error::{TaibaiError, Result};
use crate::sql::parser::parser::Parser;
use crate::sql::parser::ast::Statement;

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
mod tests {
    use super::*;

    #[test]
    fn test_prepare_select() {
        let mut cache = PreparedStatementCache::new();
        let stmt = cache.prepare("SELECT * FROM users WHERE id = 1").unwrap();
        assert_eq!(stmt.id, 1);
        assert!(matches!(stmt.stmt, Statement::Select { .. }));
    }

    #[test]
    fn test_prepare_insert() {
        let mut cache = PreparedStatementCache::new();
        let stmt = cache.prepare("INSERT INTO t (a) VALUES (1)").unwrap();
        assert_eq!(stmt.id, 1);
        assert!(matches!(stmt.stmt, Statement::Insert { .. }));
    }

    #[test]
    fn test_get_prepared() {
        let mut cache = PreparedStatementCache::new();
        cache.prepare("SELECT 1").unwrap();
        let stmt = cache.prepare("SELECT 2").unwrap();

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_none());
    }

    #[test]
    fn test_close_prepared() {
        let mut cache = PreparedStatementCache::new();
        cache.prepare("SELECT 1").unwrap();
        assert!(cache.close(1));
        assert!(!cache.close(1)); // already removed
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_auto_increment_id() {
        let mut cache = PreparedStatementCache::new();
        let s1 = cache.prepare("SELECT 1").unwrap();
        let s2 = cache.prepare("SELECT 2").unwrap();
        let s3 = cache.prepare("SELECT 3").unwrap();
        assert_eq!(s1.id, 1);
        assert_eq!(s2.id, 2);
        assert_eq!(s3.id, 3);
    }

    #[test]
    fn test_prepare_empty_sql() {
        let mut cache = PreparedStatementCache::new();
        let result = cache.prepare("");
        assert!(result.is_err());
    }

    #[test]
    fn test_clear() {
        let mut cache = PreparedStatementCache::new();
        cache.prepare("SELECT 1").unwrap();
        cache.prepare("SELECT 2").unwrap();
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert!(cache.is_empty());
    }
}
