use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::sql::parser::parser::Parser;
use crate::sql::parser::ast::Statement;

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub id: u32,
    pub sql: String,
    pub stmt: Statement,
    pub param_types: Vec<crate::protocol::types::DataType>,
}

#[derive(Debug, Clone, Default)]
pub struct PreparedStatementCache {
    next_id: u32,
    statements: HashMap<u32, PreparedStatement>,
}

impl PreparedStatementCache {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            statements: HashMap::new(),
        }
    }

    pub fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(MonaxError::Sql("empty query".into()));
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

    pub fn get(&self, id: u32) -> Option<&PreparedStatement> {
        self.statements.get(&id)
    }

    pub fn close(&mut self, id: u32) -> bool {
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
    }
}
