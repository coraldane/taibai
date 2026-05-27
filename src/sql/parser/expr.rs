use crate::common::error::{MonaxError, Result};
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr};
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_addition()?;
        self.parse_comparison_rest(left)
    }

    fn parse_comparison_rest(&mut self, left: Expr) -> Result<Expr> {
        let op = match self.peek() {
            Token::Eq => Some(BinOp::Eq),
            Token::Neq => Some(BinOp::Neq),
            Token::Lt => Some(BinOp::Lt),
            Token::Gt => Some(BinOp::Gt),
            Token::Lte => Some(BinOp::Lte),
            Token::Gte => Some(BinOp::Gte),
            _ => None,
        };
        if let Some(op) = op {
            self.advance();
            let right = self.parse_addition()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "BETWEEN") {
            self.advance();
            let low = self.parse_addition()?;
            self.expect(&Token::And)?;
            let high = self.parse_addition()?;
            Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            })
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "IN") {
            self.advance();
            self.expect(&Token::LeftParen)?;
            let mut values = Vec::new();
            if self.peek() != &Token::RightParen {
                loop {
                    values.push(self.parse_expr()?);
                    if self.peek() == &Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
            self.expect(&Token::RightParen)?;
            Ok(Expr::In {
                expr: Box::new(left),
                values,
                negated: false,
            })
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "LIKE") {
            self.advance();
            let pattern = self.parse_addition()?;
            Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            })
        } else if self.peek() == &Token::Not {
            self.advance();
            if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "BETWEEN") {
                self.advance();
                let low = self.parse_addition()?;
                self.expect(&Token::And)?;
                let high = self.parse_addition()?;
                Ok(Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: true,
                })
            } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "IN") {
                self.advance();
                self.expect(&Token::LeftParen)?;
                let mut values = Vec::new();
                if self.peek() != &Token::RightParen {
                    loop {
                        values.push(self.parse_expr()?);
                        if self.peek() == &Token::Comma {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                }
                self.expect(&Token::RightParen)?;
                Ok(Expr::In {
                    expr: Box::new(left),
                    values,
                    negated: true,
                })
            } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "LIKE") {
                self.advance();
                let pattern = self.parse_addition()?;
                Ok(Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                })
            } else {
                Err(MonaxError::Sql(format!(
                    "expected BETWEEN, IN, or LIKE after NOT, got {}",
                    self.peek()
                )))
            }
        } else {
            Ok(left)
        }
    }

    fn parse_addition(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => Some(BinOp::Add),
                Token::Minus => Some(BinOp::Sub),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplication()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => Some(BinOp::Mul),
                t if *t == Token::Ident("/".into()) => Some(BinOp::Div),
                _ => None,
            };
            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek() == &Token::Not {
            self.advance();
            let expr = self.parse_comparison()?;
            Ok(Expr::UnaryNot(Box::new(expr)))
        } else if self.peek() == &Token::Minus {
            self.advance();
            let expr = self.parse_primary()?;
            Ok(Expr::UnaryMinus(Box::new(expr)))
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "CASE") {
            self.advance();
            self.parse_case()
        } else {
            self.parse_primary()
        }
    }

    fn parse_case(&mut self) -> Result<Expr> {
        let operand = if !matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "WHEN") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        let mut when_clauses = Vec::new();
        while matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "WHEN") {
            self.advance();
            let condition = self.parse_expr()?;
            match self.peek() {
                Token::Ident(s) if s.to_uppercase() == "THEN" => { self.advance(); }
                other => return Err(MonaxError::Sql(format!("expected 'THEN' in CASE, got {}", other))),
            }
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }
        let else_clause = if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "ELSE") {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        match self.peek() {
            Token::Ident(s) if s.to_uppercase() == "END" => { self.advance(); }
            other => return Err(MonaxError::Sql(format!("expected 'END' in CASE, got {}", other))),
        }
        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::Integer(v) => {
                self.advance();
                Ok(Expr::Literal(ScalarValue::Int64(Some(v))))
            }
            Token::Float(v) => {
                self.advance();
                Ok(Expr::Literal(ScalarValue::Float64(Some(v))))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::Literal(ScalarValue::VarChar(Some(s))))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(ScalarValue::Int64(None)))
            }
            Token::Ident(name) => {
                self.advance();
                if self.peek() == &Token::Dot {
                    self.advance();
                    let column = self.expect_ident()?;
                    Ok(Expr::QualifiedColumn {
                        table: name,
                        column,
                    })
                } else if self.peek() == &Token::LeftParen {
                    self.advance();
                    let distinct = if self.peek() == &Token::Distinct {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    let mut args = Vec::new();
                    if self.peek() != &Token::RightParen {
                        loop {
                            if self.peek() == &Token::Star {
                                self.advance();
                                args.push(Expr::Column("*".into()));
                            } else {
                                args.push(self.parse_expr()?);
                            }
                            if self.peek() == &Token::Comma {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::Function {
                        name,
                        args,
                        distinct,
                    })
                } else if self.peek() == &Token::Ident("IS".into()) {
                    self.advance();
                    if self.peek() == &Token::Not {
                        self.advance();
                        self.expect(&Token::Null)?;
                        Ok(Expr::IsNotNull(Box::new(Expr::Column(name))))
                    } else {
                        self.expect(&Token::Null)?;
                        Ok(Expr::IsNull(Box::new(Expr::Column(name))))
                    }
                } else {
                    Ok(Expr::Column(name))
                }
            }
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                let name = match self.peek() {
                    Token::Count => "COUNT",
                    Token::Sum => "SUM",
                    Token::Avg => "AVG",
                    Token::Min => "MIN",
                    Token::Max => "MAX",
                    _ => unreachable!(),
                }
                .to_string();
                self.advance();
                self.expect(&Token::LeftParen)?;
                let distinct = if self.peek() == &Token::Distinct {
                    self.advance();
                    true
                } else {
                    false
                };
                let mut args = Vec::new();
                if self.peek() != &Token::RightParen {
                    loop {
                        if self.peek() == &Token::Star {
                            self.advance();
                            args.push(Expr::Column("*".into()));
                        } else {
                            args.push(self.parse_expr()?);
                        }
                        if self.peek() == &Token::Comma {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                }
                self.expect(&Token::RightParen)?;
                Ok(Expr::Function {
                    name,
                    args,
                    distinct,
                })
            }
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RightParen)?;
                Ok(expr)
            }
            Token::Placeholder => {
                self.advance();
                let idx = self.placeholder_count;
                self.placeholder_count += 1;
                Ok(Expr::Placeholder(idx))
            }
            ref other => Err(MonaxError::Sql(format!(
                "unexpected token in expression: {}",
                other
            ))),
        }
    }
}
