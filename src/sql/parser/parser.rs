use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;

use super::ast::*;
use super::lexer::{Lexer, Token};

#[path = "ddl.rs"]
mod ddl;
#[path = "dml.rs"]
mod dml;
#[path = "expr.rs"]
mod expr;
#[path = "query.rs"]
mod query;
#[path = "show.rs"]
mod show;

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    placeholder_count: usize,
}

impl Parser {
    pub fn parse(sql: &str) -> Result<Vec<Statement>> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser {
            tokens,
            pos: 0,
            placeholder_count: 0,
        };
        let mut stmts = Vec::new();
        loop {
            if parser.peek() == &Token::Eof {
                break;
            }
            let stmt = parser.parse_statement()?;
            stmts.push(stmt);
            while parser.peek() == &Token::Semicolon {
                parser.advance();
            }
        }
        Ok(stmts)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek() {
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
            Token::Truncate => self.parse_truncate(),
            Token::Grant => self.parse_grant(),
            Token::Revoke => self.parse_revoke(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Select => self.parse_select(),
            Token::Show => self.parse_show(),
            Token::Describe => self.parse_describe(),
            Token::Use => self.parse_use(),
            Token::Explain => self.parse_explain(),
            Token::Begin => self.parse_begin(),
            Token::Commit => self.parse_commit(),
            Token::Rollback => self.parse_rollback(),
            Token::Flush => self.parse_flush(),
            Token::Optimize => self.parse_optimize(),
            Token::Pause => self.parse_pause(),
            Token::Resume => self.parse_resume(),
            other => Err(MonaxError::Sql(format!("unexpected token: {}", other))),
        }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<Token> {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        if &tok == expected {
            self.pos += 1;
            Ok(tok)
        } else {
            Err(MonaxError::Sql(format!(
                "expected '{}', got '{}'",
                expected, tok
            )))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.tokens.get(self.pos) {
            Some(Token::Ident(s)) => {
                let s = s.clone();
                self.pos += 1;
                Ok(s)
            }
            other => Err(MonaxError::Sql(format!(
                "expected identifier, got '{}'",
                other
                    .as_ref()
                    .map(|t| t.to_string())
                    .unwrap_or("EOF".into())
            ))),
        }
    }

    fn parse_object_name(&mut self) -> Result<ObjectName> {
        let first = self.expect_ident()?;
        if self.peek() == &Token::Dot {
            self.advance();
            let second = self.expect_ident()?;
            Ok(ObjectName::qualified(first, second))
        } else {
            Ok(ObjectName::bare(first))
        }
    }

    fn expect_integer(&mut self) -> Result<i32> {
        match self.tokens.get(self.pos) {
            Some(Token::Integer(v)) => {
                let v = *v as i32;
                self.pos += 1;
                Ok(v)
            }
            other => Err(MonaxError::Sql(format!(
                "expected integer, got '{}'",
                other
                    .as_ref()
                    .map(|t| t.to_string())
                    .unwrap_or("EOF".into())
            ))),
        }
    }
}

pub fn parse_data_type(s: &str) -> Result<DataType> {
    match s {
        "BOOL" | "BOOLEAN" => Ok(DataType::Bool),
        "INT8" | "TINYINT" => Ok(DataType::Int8),
        "INT16" | "SMALLINT" => Ok(DataType::Int16),
        "INT32" | "INT" | "INTEGER" => Ok(DataType::Int32),
        "INT64" | "BIGINT" => Ok(DataType::Int64),
        "UINT8" => Ok(DataType::UInt8),
        "UINT16" => Ok(DataType::UInt16),
        "UINT32" => Ok(DataType::UInt32),
        "UINT64" => Ok(DataType::UInt64),
        "FLOAT32" | "FLOAT" | "REAL" => Ok(DataType::Float32),
        "FLOAT64" | "DOUBLE" => Ok(DataType::Float64),
        "DECIMAL" | "DECIMAL64" => Ok(DataType::Decimal),
        "DATE" => Ok(DataType::Date),
        "TIME" => Ok(DataType::Time),
        "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp),
        "INTERVAL" => Ok(DataType::Interval),
        "VARCHAR" | "TEXT" | "STRING" => Ok(DataType::VarChar),
        "BLOB" | "BYTES" => Ok(DataType::Blob),
        "VECTOR" => Ok(DataType::Vector),
        _ => Err(MonaxError::Sql(format!("unknown data type: '{}'", s))),
    }
}
