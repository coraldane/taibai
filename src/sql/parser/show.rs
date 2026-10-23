use crate::common::error::{Result, TaibaiError};

use crate::sql::parser::ast::Statement;
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_show(&mut self) -> Result<Statement> {
        self.expect(&Token::Show)?;
        match self.peek() {
            Token::Tables => {
                self.advance();
                let database = if self.peek() == &Token::From
                    || matches!(self.peek(), Token::Ident(word) if word.eq_ignore_ascii_case("IN"))
                {
                    self.advance();
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                Ok(Statement::ShowTables { database })
            }
            Token::Databases => {
                self.advance();
                Ok(Statement::ShowDatabases)
            }
            Token::Columns => {
                self.advance();
                self.expect(&Token::From)?;
                let table = self.parse_object_name()?;
                Ok(Statement::ShowColumns { table })
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("UNIQUE") => {
                self.advance();
                match self.peek() {
                    Token::Ident(word)
                        if word.eq_ignore_ascii_case("INDEX")
                            || word.eq_ignore_ascii_case("INDEXES") =>
                    {
                        self.advance();
                    }
                    other => {
                        return Err(TaibaiError::Sql(format!(
                            "expected INDEX after SHOW UNIQUE, got {}",
                            other
                        )))
                    }
                }
                match self.peek() {
                    Token::Ident(word) if word.eq_ignore_ascii_case("STATUS") => {
                        self.advance();
                    }
                    other => {
                        return Err(TaibaiError::Sql(format!(
                            "expected STATUS after SHOW UNIQUE INDEX, got {}",
                            other
                        )))
                    }
                }
                self.expect(&Token::From)?;
                let table = self.parse_object_name()?;
                Ok(Statement::ShowUniqueIndexStatus { table })
            }
            Token::Ident(word)
                if word.eq_ignore_ascii_case("INDEX") || word.eq_ignore_ascii_case("INDEXES") =>
            {
                self.advance();
                self.expect(&Token::From)?;
                let table = self.parse_object_name()?;
                Ok(Statement::ShowIndexes { table })
            }
            Token::Create => {
                self.advance();
                self.expect(&Token::Table)?;
                let table = self.parse_object_name()?;
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
                    Some(self.parse_object_name()?)
                } else {
                    None
                };
                Ok(Statement::ShowPartitions { table })
            }
            Token::Stats => {
                self.advance();
                Ok(Statement::ShowStats)
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("PERFORMANCE") => {
                self.advance();
                let baselines = self.expect_ident()?;
                if !baselines.eq_ignore_ascii_case("BASELINES") {
                    return Err(TaibaiError::Sql(format!(
                        "expected BASELINES after SHOW PERFORMANCE, got {baselines}"
                    )));
                }
                Ok(Statement::ShowPerformanceBaselines)
            }
            Token::Engines => {
                self.advance();
                Ok(Statement::ShowEngines)
            }
            Token::Ident(word) if word.eq_ignore_ascii_case("MEMORY") => {
                self.advance();
                Ok(Statement::ShowMemory)
            }
            Token::Full => {
                self.advance();
                self.expect(&Token::Processlist)?;
                Ok(Statement::ShowProcesslist { full: true })
            }
            Token::Processlist => {
                self.advance();
                let full = self.peek() == &Token::Full;
                if full {
                    self.advance();
                }
                Ok(Statement::ShowProcesslist { full })
            }
            other => Err(TaibaiError::Sql(format!(
                "expected TABLES, DATABASES, COLUMNS, INDEXES, CREATE, USER, ROLE, GRANTS, PARTITIONS, STATS, ENGINES, MEMORY, PROCESSLIST, after SHOW, got {}",
                other
            ))),
        }
    }

    pub fn parse_describe(&mut self) -> Result<Statement> {
        self.expect(&Token::Describe)?;
        self.expect(&Token::Table)?;
        let table = self.parse_object_name()?;
        Ok(Statement::ShowColumns { table })
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
                Some(self.parse_object_name()?)
            } else {
                None
            };
            Ok(Statement::FlushTable { table })
        } else {
            Ok(Statement::FlushTable { table: None })
        }
    }

    /// `KILL QUERY <id>` — QUERY is matched as a soft keyword so existing
    /// columns named `query` keep parsing.
    pub fn parse_kill(&mut self) -> Result<Statement> {
        self.expect(&Token::Kill)?;
        match self.peek() {
            Token::Ident(word) if word.eq_ignore_ascii_case("QUERY") => {
                self.advance();
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected QUERY after KILL, got {}",
                    other
                )))
            }
        }
        let query_id = match self.peek() {
            Token::Integer(value) if *value >= 0 => {
                let value = *value as u64;
                self.advance();
                value
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected query id after KILL QUERY, got {}",
                    other
                )))
            }
        };
        Ok(Statement::KillQuery { query_id })
    }

    pub fn parse_optimize(&mut self) -> Result<Statement> {
        self.expect(&Token::Optimize)?;
        self.expect(&Token::Table)?;
        let table = self.parse_object_name()?;
        let partition = if self.peek() == &Token::On {
            self.advance();
            self.expect(&Token::Partition)?;
            Some(self.expect_ident_or_string()?)
        } else {
            None
        };
        Ok(Statement::OptimizeTable { table, partition })
    }

    pub fn parse_analyze_table(&mut self) -> Result<Statement> {
        self.expect(&Token::Analyze)?;
        self.expect(&Token::Table)?;
        Ok(Statement::AnalyzeTable {
            table: self.parse_object_name()?,
        })
    }

    pub fn parse_rebuild(&mut self) -> Result<Statement> {
        self.expect(&Token::Rebuild)?;
        match self.peek() {
            Token::Ident(word) if word.eq_ignore_ascii_case("UNIQUE") => self.advance(),
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected UNIQUE after REBUILD, got {}",
                    other
                )))
            }
        };
        match self.peek() {
            Token::Ident(word)
                if word.eq_ignore_ascii_case("INDEX") || word.eq_ignore_ascii_case("INDEXES") =>
            {
                self.advance();
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected INDEX after REBUILD UNIQUE, got {}",
                    other
                )))
            }
        }
        let index_name = if matches!(self.peek(), Token::Ident(word) if !word.eq_ignore_ascii_case("ON"))
        {
            Some(self.expect_ident()?)
        } else {
            None
        };
        match self.peek() {
            Token::On => self.advance(),
            Token::Ident(word) if word.eq_ignore_ascii_case("ON") => self.advance(),
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected ON after REBUILD UNIQUE INDEX, got {}",
                    other
                )))
            }
        };
        let table = self.parse_object_name()?;
        Ok(Statement::RebuildUniqueIndex { table, index_name })
    }

    pub fn parse_pause(&mut self) -> Result<Statement> {
        self.expect(&Token::Pause)?;
        Ok(Statement::PauseFlush)
    }

    pub fn parse_resume(&mut self) -> Result<Statement> {
        self.expect(&Token::Resume)?;
        Ok(Statement::ResumeFlush)
    }

    pub fn parse_reset(&mut self) -> Result<Statement> {
        self.expect(&Token::Reset)?;
        let performance = self.expect_ident()?;
        if !performance.eq_ignore_ascii_case("PERFORMANCE") {
            return Err(TaibaiError::Sql(format!(
                "expected PERFORMANCE after RESET, got {performance}"
            )));
        }
        let baselines = self.expect_ident()?;
        if !baselines.eq_ignore_ascii_case("BASELINES") {
            return Err(TaibaiError::Sql(format!(
                "expected BASELINES after RESET PERFORMANCE, got {baselines}"
            )));
        }
        Ok(Statement::ResetPerformanceBaselines)
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
