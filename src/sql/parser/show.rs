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
            Token::Ident(word) if word.eq_ignore_ascii_case("CHANGE") => {
                self.advance();
                let kind = self.expect_ident()?;
                if kind.eq_ignore_ascii_case("CONSUMERS") {
                    Ok(Statement::ShowChangeConsumers)
                } else if kind.eq_ignore_ascii_case("STATUS") {
                    Ok(Statement::ShowChangeStatus)
                } else if kind.eq_ignore_ascii_case("EVENTS") {
                    let mut table = None;
                    let mut from_sequence = None;
                    let mut limit = None;
                    loop {
                        if self.peek() == &Token::For {
                            self.advance();
                            table = Some(self.parse_object_name()?);
                        } else if self.peek() == &Token::From {
                            self.advance();
                            from_sequence = Some(self.expect_u64()?);
                        } else if self.peek() == &Token::Limit {
                            self.advance();
                            limit = Some(self.expect_u64()? as usize);
                        } else {
                            break;
                        }
                    }
                    Ok(Statement::ShowChangeEvents {
                        table,
                        from_sequence,
                        limit,
                    })
                } else {
                    Err(MonaxError::Sql(format!(
                        "expected CONSUMERS, STATUS, or EVENTS after SHOW CHANGE, got {}",
                        kind
                    )))
                }
            }
            other => Err(MonaxError::Sql(format!(
                "expected TABLES, DATABASES, COLUMNS, INDEXES, CREATE, USER, ROLE, GRANTS, PARTITIONS, STATS, ENGINES, PROCESSLIST, or CHANGE after SHOW, got {}",
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

    pub fn parse_optimize(&mut self) -> Result<Statement> {
        self.expect(&Token::Optimize)?;
        self.expect(&Token::Table)?;
        let table = self.parse_object_name()?;
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

    pub fn parse_reset(&mut self) -> Result<Statement> {
        let reset = self.expect_ident()?;
        if !reset.eq_ignore_ascii_case("RESET") {
            return Err(MonaxError::Sql(format!("expected RESET, got {}", reset)));
        }
        let change = self.expect_ident()?;
        if !change.eq_ignore_ascii_case("CHANGE") {
            return Err(MonaxError::Sql(format!(
                "expected CHANGE after RESET, got {}",
                change
            )));
        }
        let consumer = self.expect_ident()?;
        if !consumer.eq_ignore_ascii_case("CONSUMER") {
            return Err(MonaxError::Sql(format!(
                "expected CONSUMER after RESET CHANGE, got {}",
                consumer
            )));
        }
        let group = self.expect_ident_or_string()?;
        self.expect(&Token::To)?;
        let sequence = self.expect_u64()?;
        Ok(Statement::ResetChangeConsumer { group, sequence })
    }

    pub fn parse_ack(&mut self) -> Result<Statement> {
        let ack = self.expect_ident()?;
        if !ack.eq_ignore_ascii_case("ACK") {
            return Err(MonaxError::Sql(format!("expected ACK, got {}", ack)));
        }
        let change = self.expect_ident()?;
        if !change.eq_ignore_ascii_case("CHANGE") {
            return Err(MonaxError::Sql(format!(
                "expected CHANGE after ACK, got {}",
                change
            )));
        }
        let consumer = self.expect_ident()?;
        if !consumer.eq_ignore_ascii_case("CONSUMER") {
            return Err(MonaxError::Sql(format!(
                "expected CONSUMER after ACK CHANGE, got {}",
                consumer
            )));
        }
        let group = self.expect_ident_or_string()?;
        self.expect(&Token::To)?;
        let sequence = self.expect_u64()?;
        Ok(Statement::AckChangeConsumer { group, sequence })
    }
}
