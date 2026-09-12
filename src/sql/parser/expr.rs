use crate::common::error::{Result, TaibaiError};
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{
    BinOp, Expr, FrameBound, FrameType, OrderByItem, WindowFrame, WindowFunc,
};
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
        } else if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("IS")) {
            self.advance();
            if self.peek() == &Token::Not {
                self.advance();
                self.expect(&Token::Null)?;
                Ok(Expr::IsNotNull(Box::new(left)))
            } else {
                self.expect(&Token::Null)?;
                Ok(Expr::IsNull(Box::new(left)))
            }
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
            if self.peek() == &Token::Select {
                let stmt = self.parse_select()?;
                self.expect(&Token::RightParen)?;
                Ok(Expr::In {
                    expr: Box::new(left),
                    values: vec![Expr::Subquery(Box::new(stmt))],
                    negated: false,
                })
            } else {
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
            }
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
                if self.peek() == &Token::Select {
                    let stmt = self.parse_select()?;
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::In {
                        expr: Box::new(left),
                        values: vec![Expr::Subquery(Box::new(stmt))],
                        negated: true,
                    })
                } else {
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
                }
            } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "LIKE") {
                self.advance();
                let pattern = self.parse_addition()?;
                Ok(Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                })
            } else {
                Err(TaibaiError::Sql(format!(
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
                t if *t == Token::Ident("%".into()) => Some(BinOp::Mod),
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
                Token::Ident(s) if s.to_uppercase() == "THEN" => {
                    self.advance();
                }
                other => {
                    return Err(TaibaiError::Sql(format!(
                        "expected 'THEN' in CASE, got {}",
                        other
                    )))
                }
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
            Token::Ident(s) if s.to_uppercase() == "END" => {
                self.advance();
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected 'END' in CASE, got {}",
                    other
                )))
            }
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
            Token::Default => {
                self.advance();
                Ok(Expr::Function {
                    name: "__TAIBAI_DEFAULT__".to_string(),
                    args: Vec::new(),
                    distinct: false,
                })
            }
            Token::Exists => {
                self.advance();
                self.expect(&Token::LeftParen)?;
                if self.peek() == &Token::Select {
                    let stmt = self.parse_select()?;
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::Exists(Box::new(Expr::Subquery(Box::new(stmt)))))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::Exists(Box::new(expr)))
                }
            }
            Token::Ident(name) => {
                self.advance();
                match name.to_ascii_uppercase().as_str() {
                    "TRUE" => return Ok(Expr::Literal(ScalarValue::Bool(Some(true)))),
                    "FALSE" => return Ok(Expr::Literal(ScalarValue::Bool(Some(false)))),
                    _ => {}
                }
                if self.peek() == &Token::Dot {
                    self.advance();
                    let mut table = name;
                    let mut column = self.expect_ident()?;
                    if self.peek() == &Token::Dot {
                        self.advance();
                        table = format!("{}.{}", table, column);
                        column = self.expect_ident()?;
                    }
                    Ok(Expr::QualifiedColumn { table, column })
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

                    // Check for OVER clause for window function
                    if self.peek() == &Token::Over {
                        self.advance();
                        self.parse_window_over(name, args, distinct)
                    } else {
                        Ok(Expr::Function {
                            name,
                            args,
                            distinct,
                        })
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

                // Check for OVER clause for window function
                if self.peek() == &Token::Over {
                    self.advance();
                    self.parse_window_over(name, args, distinct)
                } else {
                    Ok(Expr::Function {
                        name,
                        args,
                        distinct,
                    })
                }
            }
            Token::LeftParen => {
                self.advance();
                if self.peek() == &Token::Select {
                    let stmt = self.parse_select()?;
                    self.expect(&Token::RightParen)?;
                    Ok(Expr::Subquery(Box::new(stmt)))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RightParen)?;
                    Ok(expr)
                }
            }
            Token::Placeholder => {
                self.advance();
                let idx = self.placeholder_count;
                self.placeholder_count += 1;
                Ok(Expr::Placeholder(idx))
            }
            ref other => Err(TaibaiError::Sql(format!(
                "unexpected token in expression: {}",
                other
            ))),
        }
    }

    /// Parse OVER clause for window functions
    fn parse_window_over(&mut self, name: String, args: Vec<Expr>, distinct: bool) -> Result<Expr> {
        self.expect(&Token::LeftParen)?;

        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;

        // Parse PARTITION BY
        if self.peek() == &Token::Partition {
            self.advance();
            self.expect(&Token::By)?;
            loop {
                partition_by.push(self.parse_expr()?);
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // Parse ORDER BY
        if self.peek() == &Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            loop {
                let expr = self.parse_expr()?;
                let desc = if self.peek() == &Token::Desc {
                    self.advance();
                    true
                } else {
                    if self.peek() == &Token::Asc {
                        self.advance();
                    }
                    false
                };
                order_by.push(OrderByItem { expr, desc });
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // Parse frame clause (ROWS/RANGE BETWEEN ...)
        if self.peek() == &Token::Rows || self.peek() == &Token::Range {
            let frame_type = if self.peek() == &Token::Rows {
                FrameType::Rows
            } else {
                FrameType::Range
            };
            self.advance();

            // Parse BETWEEN or single bound
            let (start, end) = if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "BETWEEN")
            {
                self.advance();
                let start = self.parse_frame_bound()?;
                // AND is tokenized as the logical keyword, not an identifier.
                if self.peek() == &Token::And {
                    self.advance();
                } else {
                    self.expect(&Token::Ident("AND".into()))?;
                }
                let end = self.parse_frame_bound()?;
                (start, end)
            } else {
                let start = self.parse_frame_bound()?;
                (start, FrameBound::CurrentRow)
            };

            frame = Some(WindowFrame {
                frame_type,
                start,
                end,
            });
        }

        self.expect(&Token::RightParen)?;

        // Determine the window function type
        let func = match name.to_uppercase().as_str() {
            "ROW_NUMBER" => WindowFunc::RowNumber,
            "RANK" => WindowFunc::Rank,
            "DENSE_RANK" => WindowFunc::DenseRank,
            "LEAD" => {
                // Standard form: LEAD(value[, offset[, default]]).
                let Some(arg) = args.first() else {
                    return Err(TaibaiError::Sql(
                        "LEAD requires a value argument".to_string(),
                    ));
                };
                let offset = match args.get(1) {
                    Some(Expr::Literal(ScalarValue::Int64(Some(v)))) => *v as usize,
                    Some(Expr::Literal(ScalarValue::Int32(Some(v)))) => *v as usize,
                    None => 1,
                    _ => {
                        return Err(TaibaiError::Sql(
                            "LEAD offset must be an integer literal".to_string(),
                        ))
                    }
                };
                let default = args.get(2).map(|d| Box::new(d.clone()));
                WindowFunc::Lead {
                    arg: Box::new(arg.clone()),
                    offset,
                    default,
                }
            }
            "LAG" => {
                let Some(arg) = args.first() else {
                    return Err(TaibaiError::Sql(
                        "LAG requires a value argument".to_string(),
                    ));
                };
                let offset = match args.get(1) {
                    Some(Expr::Literal(ScalarValue::Int64(Some(v)))) => *v as usize,
                    Some(Expr::Literal(ScalarValue::Int32(Some(v)))) => *v as usize,
                    None => 1,
                    _ => {
                        return Err(TaibaiError::Sql(
                            "LAG offset must be an integer literal".to_string(),
                        ))
                    }
                };
                let default = args.get(2).map(|d| Box::new(d.clone()));
                WindowFunc::Lag {
                    arg: Box::new(arg.clone()),
                    offset,
                    default,
                }
            }
            _ => WindowFunc::Aggregate {
                func: name,
                args,
                distinct,
            },
        };

        Ok(Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_frame_bound(&mut self) -> Result<FrameBound> {
        if self.peek() == &Token::Unbounded {
            self.advance();
            if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "PRECEDING") {
                self.advance();
                Ok(FrameBound::UnboundedPreceding)
            } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "FOLLOWING") {
                self.advance();
                Ok(FrameBound::UnboundedFollowing)
            } else {
                Err(TaibaiError::Sql(
                    "expected PRECEDING or FOLLOWING after UNBOUNDED".into(),
                ))
            }
        } else if self.peek() == &Token::Current
            || matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "CURRENT")
        {
            self.advance();
            // ROW may arrive as the plural keyword token (shared with
            // ROWS) or a plain identifier.
            if self.peek() == &Token::Rows {
                self.advance();
            } else {
                self.expect(&Token::Ident("ROW".into()))?;
            }
            Ok(FrameBound::CurrentRow)
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "PRECEDING") {
            self.advance();
            Ok(FrameBound::Preceding(1))
        } else if matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "FOLLOWING") {
            self.advance();
            Ok(FrameBound::Following(1))
        } else if matches!(self.peek(), Token::Integer(_)) {
            let n = match self.peek() {
                Token::Integer(v) => *v as usize,
                _ => unreachable!(),
            };
            self.advance();
            let is_preceding = self.peek() == &Token::Preceding
                || matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "PRECEDING");
            let is_following = self.peek() == &Token::Following
                || matches!(self.peek(), Token::Ident(s) if s.to_uppercase() == "FOLLOWING");
            if is_preceding {
                self.advance();
                Ok(FrameBound::Preceding(n))
            } else if is_following {
                self.advance();
                Ok(FrameBound::Following(n))
            } else {
                Err(TaibaiError::Sql(
                    "expected PRECEDING or FOLLOWING after integer".into(),
                ))
            }
        } else {
            Err(TaibaiError::Sql(format!(
                "unexpected frame bound: {}",
                self.peek()
            )))
        }
    }
}
