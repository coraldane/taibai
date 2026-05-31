use crate::common::error::{MonaxError, Result};

use crate::sql::parser::ast::{
    FromClause, JoinClause, JoinExpr, JoinType, OrderByItem, SelectItem, Statement, TableRef,
};
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_select(&mut self) -> Result<Statement> {
        self.expect(&Token::Select)?;

        let distinct = if self.peek() == &Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let select_list = self.parse_select_list()?;

        let from = if self.peek() == &Token::From {
            self.advance();
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.peek() == &Token::Where {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.peek() == &Token::Group {
            self.advance();
            self.expect(&Token::By)?;
            let mut cols = Vec::new();
            loop {
                cols.push(self.parse_expr()?);
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            cols
        } else {
            Vec::new()
        };

        let having = if self.peek() == &Token::Having {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.peek() == &Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            let mut items = Vec::new();
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
                items.push(OrderByItem { expr, desc });
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            items
        } else {
            Vec::new()
        };

        let limit = if self.peek() == &Token::Limit {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.peek() == &Token::Offset {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let select_stmt = Statement::Select {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            distinct,
        };

        self.parse_union_after_select(select_stmt)
    }

    fn parse_union_after_select(&mut self, left: Statement) -> Result<Statement> {
        if self.peek() != &Token::Union {
            return Ok(left);
        }
        self.advance();
        let all = if self.peek() == &Token::All {
            self.advance();
            true
        } else {
            false
        };
        let right_raw = self.parse_select()?;
        let right = self.parse_union_after_select(right_raw)?;

        let order_by = if self.peek() == &Token::Order {
            self.advance();
            self.expect(&Token::By)?;
            let mut items = Vec::new();
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
                items.push(OrderByItem { expr, desc });
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            items
        } else {
            Vec::new()
        };

        let limit = if self.peek() == &Token::Limit {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.peek() == &Token::Offset {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Union {
            left: Box::new(left),
            right: Box::new(right),
            all,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            if self.peek() == &Token::Star {
                self.advance();
                items.push(SelectItem::Star);
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.peek() == &Token::As {
                    self.advance();
                    Some(self.expect_ident()?)
                } else if matches!(self.peek(), Token::Ident(_)) {
                    let next = self.peek();
                    if !matches!(
                        next,
                        Token::From
                            | Token::Where
                            | Token::Group
                            | Token::Order
                            | Token::Limit
                            | Token::Offset
                            | Token::Comma
                            | Token::Semicolon
                            | Token::Eof
                            | Token::Union
                            | Token::Having
                    ) {
                        Some(self.expect_ident()?)
                    } else {
                        None
                    }
                } else {
                    None
                };
                items.push(SelectItem::Expr(expr, alias));
            }
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        Ok(items)
    }

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        let base_table = self.parse_table_ref()?;

        let mut joins = Vec::new();
        loop {
            let is_join = matches!(
                self.peek(),
                Token::Join | Token::Inner | Token::Left | Token::Right | Token::Cross
            );
            if !is_join {
                break;
            }
            joins.push(self.parse_join_clause()?);
        }

        if joins.is_empty() {
            Ok(FromClause::Table(base_table))
        } else {
            Ok(FromClause::Join(JoinExpr {
                left: Box::new(FromClause::Table(base_table)),
                joins,
            }))
        }
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        if self.peek() == &Token::LeftParen {
            self.advance();
            if self.peek() == &Token::Select {
                let stmt = self.parse_select()?;
                self.expect(&Token::RightParen)?;
                let alias = if self.peek() == &Token::As {
                    self.advance();
                    self.expect_ident()?
                } else if matches!(self.peek(), Token::Ident(_)) {
                    self.expect_ident()?
                } else {
                    return Err(MonaxError::Sql("derived table requires an alias".into()));
                };
                Ok(TableRef::Subquery {
                    query: Box::new(stmt),
                    alias,
                })
            } else {
                return Err(MonaxError::Sql("expected SELECT in derived table".into()));
            }
        } else {
            let name = self.parse_object_name()?;
            let alias = if self.peek() == &Token::As {
                self.advance();
                Some(self.expect_ident()?)
            } else if matches!(self.peek(), Token::Ident(_)) {
                let next = self.peek();
                if !matches!(
                    next,
                    Token::Join
                        | Token::Inner
                        | Token::Left
                        | Token::Right
                        | Token::Cross
                        | Token::On
                        | Token::Where
                        | Token::Group
                        | Token::Order
                        | Token::Limit
                        | Token::Offset
                        | Token::Semicolon
                        | Token::Eof
                ) {
                    Some(self.expect_ident()?)
                } else {
                    None
                }
            } else {
                None
            };
            Ok(TableRef::Table { name, alias })
        }
    }

    fn parse_join_clause(&mut self) -> Result<JoinClause> {
        let join_type = if self.peek() == &Token::Left {
            self.advance();
            if self.peek() == &Token::Outer {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Left
        } else if self.peek() == &Token::Right {
            self.advance();
            if self.peek() == &Token::Outer {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Right
        } else if self.peek() == &Token::Cross {
            self.advance();
            self.expect(&Token::Join)?;
            JoinType::Cross
        } else {
            if self.peek() == &Token::Inner {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Inner
        };

        let right = self.parse_table_ref()?;

        let condition = if join_type != JoinType::Cross {
            self.expect(&Token::On)?;
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(JoinClause {
            join_type,
            right,
            condition,
        })
    }
}
