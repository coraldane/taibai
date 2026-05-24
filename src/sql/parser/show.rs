use crate::common::error::{MonaxError, Result};

use crate::sql::parser::ast::Statement;
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_show(&mut self) -> Result<Statement> {
        self.expect(&Token::Show)?;
        match self.peek() {
            Token::Tables => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            Token::Databases => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            Token::Columns => {
                self.advance();
                self.expect(&Token::From)?;
                let table = self.expect_ident()?;
                Ok(Statement::ShowColumns { table })
            }
            Token::Ident(word)
                if word.eq_ignore_ascii_case("INDEX") || word.eq_ignore_ascii_case("INDEXES") =>
            {
                self.advance();
                self.expect(&Token::From)?;
                let table = self.expect_ident()?;
                Ok(Statement::ShowIndexes { table })
            }
            Token::Create => {
                self.advance();
                self.expect(&Token::Table)?;
                let table = self.expect_ident()?;
                Ok(Statement::ShowCreateTable { table })
            }
            Token::User => {
                self.advance();
                Ok(Statement::ShowUsers)
            }
            Token::Role => {
                self.advance();
                Ok(Statement::ShowRoles)
            }
            Token::Grants => {
                self.advance();
                let user = if self.peek() == &Token::For {
                    self.advance();
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                Ok(Statement::ShowGrants { user })
            }
            Token::Partitions => {
                self.advance();
                let table = if self.peek() == &Token::From {
                    self.advance();
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                Ok(Statement::ShowPartitions { table })
            }
            Token::Stats => {
                self.advance();
                Ok(Statement::ShowStats)
            }
            Token::Engines => {
                self.advance();
                Ok(Statement::ShowEngines)
            }
            Token::Processlist => {
                self.advance();
                let full = self.peek() == &Token::Full;
                if full {
                    self.advance();
                }
                Ok(Statement::ShowProcesslist { full })
            }
            other => Err(MonaxError::Sql(format!(
                "expected TABLES, DATABASES, COLUMNS, INDEXES, CREATE, USER, ROLE, GRANTS, PARTITIONS, STATS, ENGINES, or PROCESSLIST after SHOW, got {}",
                other
            ))),
        }
    }

    pub fn parse_use(&mut self) -> Result<Statement> {
        self.expect(&Token::Use)?;
        let name = self.expect_ident()?;
        Ok(Statement::UseDatabase(name))
    }

    pub fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(&Token::Begin)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Begin)
    }

    pub fn parse_commit(&mut self) -> Result<Statement> {
        self.expect(&Token::Commit)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Commit)
    }

    pub fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(&Token::Rollback)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Rollback)
    }

    pub fn parse_flush(&mut self) -> Result<Statement> {
        self.expect(&Token::Flush)?;
        if self.peek() == &Token::Table {
            self.advance();
            let table = if matches!(self.peek(), Token::Ident(_)) {
                Some(self.expect_ident()?)
            } else {
                None
            };
            Ok(Statement::FlushTable { table })
        } else {
            Ok(Statement::FlushTable { table: None })
        }
    }

    pub fn parse_optimize(&mut self) -> Result<Statement> {
        self.expect(&Token::Optimize)?;
        self.expect(&Token::Table)?;
        let table = self.expect_ident()?;
        Ok(Statement::OptimizeTable { table })
    }

    pub fn parse_pause(&mut self) -> Result<Statement> {
        self.expect(&Token::Pause)?;
        Ok(Statement::PauseFlush)
    }

    pub fn parse_resume(&mut self) -> Result<Statement> {
        self.expect(&Token::Resume)?;
        Ok(Statement::ResumeFlush)
    }

    pub fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(&Token::Explain)?;
        let analyze = if self.peek() == &Token::Analyze {
            self.advance();
            true
        } else {
            false
        };
        let inner = self.parse_statement()?;
        Ok(Statement::Explain {
            analyze,
            stmt: Box::new(inner),
        })
    }
}