use crate::common::error::Result;

use crate::sql::parser::ast::{Assignment, Statement};
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_insert(&mut self) -> Result<Statement> {
        self.parse_insert_like(false)
    }

    pub fn parse_replace(&mut self) -> Result<Statement> {
        self.parse_insert_like(true)
    }

    fn parse_insert_like(&mut self, replace: bool) -> Result<Statement> {
        if replace {
            self.expect(&Token::Replace)?;
            // MySQL permits INTO to be omitted for REPLACE, while INSERT keeps
            // requiring it in Taibai's grammar.
            if self.peek() == &Token::Into {
                self.advance();
            }
        } else {
            self.expect(&Token::Insert)?;
            self.expect(&Token::Into)?;
        }
        let table = self.parse_object_name()?;

        let columns = if self.peek() == &Token::LeftParen {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_ident()?);
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(&Token::RightParen)?;
            cols
        } else {
            Vec::new()
        };

        if replace && self.peek() == &Token::Select {
            return Ok(Statement::ReplaceSelect {
                table,
                columns,
                query: Box::new(self.parse_select()?),
            });
        }

        self.expect(&Token::Values)?;
        let mut all_values = Vec::new();
        loop {
            self.expect(&Token::LeftParen)?;
            let mut row = Vec::new();
            if self.peek() != &Token::RightParen {
                loop {
                    row.push(self.parse_expr()?);
                    if self.peek() == &Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
            self.expect(&Token::RightParen)?;
            all_values.push(row);
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        if replace {
            Ok(Statement::Replace {
                table,
                columns,
                values: all_values,
            })
        } else {
            Ok(Statement::Insert {
                table,
                columns,
                values: all_values,
            })
        }
    }

    pub fn parse_update(&mut self) -> Result<Statement> {
        self.expect(&Token::Update)?;
        let table = self.parse_object_name()?;

        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_ident()?;
            self.expect(&Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = if self.peek() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Update {
            table,
            assignments,
            where_clause,
        })
    }

    pub fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.parse_object_name()?;

        let where_clause = if self.peek() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete {
            table,
            where_clause,
        })
    }
}
