use crate::common::error::{Result, TaibaiError};

use crate::sql::parser::ast::{
    Expr, FromClause, JoinClause, JoinExpr, JoinType, OrderByItem, SelectItem, SetOp, Statement,
    TableRef,
};
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::Parser;

impl Parser {
    pub fn parse_select(&mut self) -> Result<Statement> {
        let first = if self.peek() == &Token::LeftParen {
            self.parse_parenthesized_query()?
        } else {
            self.parse_select_core()?
        };
        self.finish_query(first)
    }

    /// `(...)`: a parenthesized query expression. Valid as a set-operation
    /// operand and at statement level; trailing ORDER BY / LIMIT / OFFSET
    /// inside the parentheses belong to the parenthesized query itself.
    pub fn parse_parenthesized_query(&mut self) -> Result<Statement> {
        self.expect(&Token::LeftParen)?;
        let inner = if self.peek() == &Token::LeftParen {
            self.parse_parenthesized_query()?
        } else {
            self.parse_select()?
        };
        self.expect(&Token::RightParen)?;
        Ok(inner)
    }

    /// Fold set operations over an already-parsed first operand.
    /// INTERSECT binds tighter than UNION/EXCEPT; UNION and EXCEPT share one
    /// precedence level and fold left-associatively, as in PostgreSQL.
    pub(crate) fn finish_query(&mut self, first: Statement) -> Result<Statement> {
        let mut node = first;
        while self.peek() == &Token::Intersect {
            self.advance();
            self.reject_set_op_all("INTERSECT")?;
            let right = self.parse_set_operand()?;
            node = Statement::Union {
                left: Box::new(node),
                right: Box::new(right),
                op: SetOp::Intersect,
                all: false,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
        }
        while matches!(self.peek(), Token::Union | Token::Except) {
            let (op, all) = if self.peek() == &Token::Union {
                self.advance();
                let all = if self.peek() == &Token::All {
                    self.advance();
                    true
                } else {
                    false
                };
                (SetOp::Union, all)
            } else {
                self.advance();
                self.reject_set_op_all("EXCEPT")?;
                (SetOp::Except, false)
            };
            let right = self.parse_set_operand()?;
            node = Statement::Union {
                left: Box::new(node),
                right: Box::new(right),
                op,
                all,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
        }
        self.parse_query_trailing_clauses(node)
    }

    /// One operand of a set operation: a bare SELECT (whose own INTERSECT
    /// tail folds tighter) or a parenthesized query.
    fn parse_set_operand(&mut self) -> Result<Statement> {
        if self.peek() == &Token::LeftParen {
            return self.parse_parenthesized_query();
        }
        // Left-fold every INTERSECT tail into the operand: the loop used to
        // return after the first one, leaving `a INTERSECT b INTERSECT c`
        // with an unconsumed tail that failed to parse.
        let mut left = self.parse_select_core()?;
        while self.peek() == &Token::Intersect {
            self.advance();
            self.reject_set_op_all("INTERSECT")?;
            let right = self.parse_select_core()?;
            left = Statement::Union {
                left: Box::new(left),
                right: Box::new(right),
                op: SetOp::Intersect,
                all: false,
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
        }
        Ok(left)
    }

    fn reject_set_op_all(&mut self, name: &str) -> Result<()> {
        if self.peek() == &Token::All {
            return Err(TaibaiError::Sql(format!(
                "{} ALL is not supported (plain {} applies set semantics already)",
                name, name
            )));
        }
        Ok(())
    }

    /// Attach trailing ORDER BY / LIMIT / OFFSET to the finished query. They
    /// always belong to the whole query: the top SELECT, or the outermost
    /// set-operation node (whose executor sorts/limits the combined rows).
    fn parse_query_trailing_clauses(&mut self, mut node: Statement) -> Result<Statement> {
        let mut order_by = Vec::new();
        let mut limit = None;
        let mut offset = None;
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
        if self.peek() == &Token::Limit {
            self.advance();
            limit = Some(self.parse_expr()?);
        }
        if self.peek() == &Token::Offset {
            self.advance();
            offset = Some(self.parse_expr()?);
        }
        if order_by.is_empty() && limit.is_none() && offset.is_none() {
            return Ok(node);
        }
        match &mut node {
            Statement::Select {
                order_by: node_order,
                limit: node_limit,
                offset: node_offset,
                ..
            }
            | Statement::Union {
                order_by: node_order,
                limit: node_limit,
                offset: node_offset,
                ..
            } => {
                *node_order = order_by;
                *node_limit = limit;
                *node_offset = offset;
            }
            _ => {}
        }
        Ok(node)
    }

    /// Parse `WITH name AS (query) [, name AS (query)]* query` by desugaring
    /// the CTEs into derived tables: every reference to a CTE name in FROM /
    /// JOIN / subquery positions of the main query (and of later CTE bodies)
    /// is replaced with a subquery table ref. Non-recursive only.
    pub fn parse_with(&mut self) -> Result<Statement> {
        self.expect(&Token::With)?;
        if self.peek() == &Token::Recursive {
            return Err(TaibaiError::Sql("RECURSIVE CTEs are not supported".into()));
        }
        let mut ctes: Vec<(String, Statement)> = Vec::new();
        loop {
            let name = self.expect_ident()?;
            self.expect(&Token::As)?;
            self.expect(&Token::LeftParen)?;
            let mut body = self.parse_select()?;
            self.expect(&Token::RightParen)?;
            // A later CTE may reference earlier ones: desugar against the
            // already-registered names before pushing, so self-reference
            // stays an unknown-table error instead of silent recursion.
            Self::substitute_cte_refs(&mut body, &ctes);
            ctes.push((name, body));
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        let mut main = self.parse_select()?;
        Self::substitute_cte_refs(&mut main, &ctes);
        Ok(main)
    }

    /// A single SELECT without set operations and without trailing
    /// ORDER BY / LIMIT / OFFSET (those belong to the enclosing chain; see
    /// `parse_query_trailing_clauses`).
    fn parse_select_core(&mut self) -> Result<Statement> {
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

        Ok(Statement::Select {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct,
        })
    }

    /// Rewrite every `TableRef::Table` whose bare name matches a CTE into a
    /// subquery table ref, recursing through joins, derived tables and
    /// expression-level subqueries. Unqualified names only: `db.cte` is left
    /// untouched, matching PostgreSQL's scoped-name rule.
    fn substitute_cte_refs(stmt: &mut Statement, ctes: &[(String, Statement)]) {
        match stmt {
            Statement::Select {
                select_list,
                from,
                where_clause,
                group_by,
                having,
                order_by,
                limit,
                offset,
                ..
            } => {
                if let Some(from_clause) = from {
                    Self::substitute_cte_from(from_clause, ctes);
                }
                for item in select_list.iter_mut() {
                    if let crate::sql::parser::ast::SelectItem::Expr(e, _) = item {
                        Self::substitute_cte_expr(e, ctes);
                    }
                }
                if let Some(e) = where_clause {
                    Self::substitute_cte_expr(e, ctes);
                }
                for e in group_by.iter_mut() {
                    Self::substitute_cte_expr(e, ctes);
                }
                if let Some(e) = having {
                    Self::substitute_cte_expr(e, ctes);
                }
                for item in order_by.iter_mut() {
                    Self::substitute_cte_expr(&mut item.expr, ctes);
                }
                if let Some(e) = limit {
                    Self::substitute_cte_expr(e, ctes);
                }
                if let Some(e) = offset {
                    Self::substitute_cte_expr(e, ctes);
                }
            }
            Statement::Union { left, right, .. } => {
                Self::substitute_cte_refs(left, ctes);
                Self::substitute_cte_refs(right, ctes);
            }
            _ => {}
        }
    }

    fn substitute_cte_from(from: &mut FromClause, ctes: &[(String, Statement)]) {
        match from {
            FromClause::Table(t) => Self::substitute_cte_table_ref(t, ctes),
            FromClause::Join(JoinExpr { left, joins }) => {
                Self::substitute_cte_from(left, ctes);
                for join in joins.iter_mut() {
                    Self::substitute_cte_table_ref(&mut join.right, ctes);
                    if let Some(cond) = join.condition.as_mut() {
                        Self::substitute_cte_expr(cond, ctes);
                    }
                }
            }
        }
    }

    fn substitute_cte_table_ref(t: &mut TableRef, ctes: &[(String, Statement)]) {
        match t {
            TableRef::Table { name, alias } if name.database.is_none() => {
                if let Some((cte_name, body)) = ctes
                    .iter()
                    .find(|(cte_name, _)| cte_name.eq_ignore_ascii_case(&name.name))
                {
                    *t = TableRef::Subquery {
                        query: Box::new(body.clone()),
                        alias: alias.take().unwrap_or_else(|| cte_name.clone()),
                    };
                }
            }
            TableRef::Subquery { query, .. } => Self::substitute_cte_refs(query, ctes),
            TableRef::Table { .. } => {}
        }
    }

    fn substitute_cte_expr(expr: &mut Expr, ctes: &[(String, Statement)]) {
        match expr {
            Expr::Subquery(stmt) => Self::substitute_cte_refs(stmt, ctes),
            Expr::Exists(inner) => Self::substitute_cte_expr(inner, ctes),
            Expr::In {
                expr: inner,
                values,
                ..
            } => {
                Self::substitute_cte_expr(inner, ctes);
                for v in values.iter_mut() {
                    Self::substitute_cte_expr(v, ctes);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::substitute_cte_expr(left, ctes);
                Self::substitute_cte_expr(right, ctes);
            }
            Expr::UnaryNot(inner)
            | Expr::UnaryMinus(inner)
            | Expr::IsNull(inner)
            | Expr::IsNotNull(inner) => Self::substitute_cte_expr(inner, ctes),
            Expr::Between {
                expr: inner,
                low,
                high,
                ..
            } => {
                Self::substitute_cte_expr(inner, ctes);
                Self::substitute_cte_expr(low, ctes);
                Self::substitute_cte_expr(high, ctes);
            }
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    Self::substitute_cte_expr(op, ctes);
                }
                for (when, then) in when_clauses.iter_mut() {
                    Self::substitute_cte_expr(when, ctes);
                    Self::substitute_cte_expr(then, ctes);
                }
                if let Some(el) = else_clause {
                    Self::substitute_cte_expr(el, ctes);
                }
            }
            Expr::Like {
                expr: inner,
                pattern,
                ..
            } => {
                Self::substitute_cte_expr(inner, ctes);
                Self::substitute_cte_expr(pattern, ctes);
            }
            Expr::Function { args, .. } => {
                for arg in args.iter_mut() {
                    Self::substitute_cte_expr(arg, ctes);
                }
            }
            Expr::WindowFunction {
                func: _,
                partition_by,
                order_by,
                frame: _,
            } => {
                for e in partition_by.iter_mut() {
                    Self::substitute_cte_expr(e, ctes);
                }
                for item in order_by.iter_mut() {
                    Self::substitute_cte_expr(&mut item.expr, ctes);
                }
            }
            // Literal / Column / QualifiedColumn / Placeholder / Correlated
            // carry no subqueries (Correlated is created after parsing).
            _ => {}
        }
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
            // SQL's comma-separated FROM list is the legacy spelling of a
            // cross join. Represent it with the same JoinClause used by
            // explicit CROSS JOIN so planning and execution share one path.
            if self.peek() == &Token::Comma {
                self.advance();
                joins.push(JoinClause {
                    join_type: JoinType::Cross,
                    right: self.parse_table_ref()?,
                    condition: None,
                });
                continue;
            }

            let is_join = matches!(
                self.peek(),
                Token::Join
                    | Token::Inner
                    | Token::Left
                    | Token::Right
                    | Token::Full
                    | Token::Cross
                    | Token::Asof
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
                    return Err(TaibaiError::Sql("derived table requires an alias".into()));
                };
                Ok(TableRef::Subquery {
                    query: Box::new(stmt),
                    alias,
                })
            } else {
                Err(TaibaiError::Sql("expected SELECT in derived table".into()))
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
                        | Token::Full
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
            // `LEFT ASOF JOIN` — asof matching that preserves left rows.
            if self.peek() == &Token::Asof {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::LeftAsof
            } else {
                self.expect(&Token::Join)?;
                JoinType::Left
            }
        } else if self.peek() == &Token::Asof {
            self.advance();
            // `ASOF [LEFT] JOIN` — both keyword orders are accepted.
            if self.peek() == &Token::Left {
                self.advance();
                self.expect(&Token::Join)?;
                JoinType::LeftAsof
            } else {
                self.expect(&Token::Join)?;
                JoinType::Asof
            }
        } else if self.peek() == &Token::Right {
            self.advance();
            if self.peek() == &Token::Outer {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Right
        } else if self.peek() == &Token::Full {
            self.advance();
            if self.peek() == &Token::Outer {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Full
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
