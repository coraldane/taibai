use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

use super::ast::*;
use super::lexer::{Lexer, Token};

/// SQL parser: hand-written recursive descent.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    placeholder_count: usize,
}

impl Parser {
    /// Parse a SQL string into a list of statements.
    pub fn parse(sql: &str) -> Result<Vec<Statement>> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser { tokens, pos: 0, placeholder_count: 0 };
        let mut stmts = Vec::new();
        loop {
            if parser.peek() == &Token::Eof {
                break;
            }
            let stmt = parser.parse_statement()?;
            stmts.push(stmt);
            // consume optional semicolons
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

    fn parse_explain(&mut self) -> Result<Statement> {
        self.expect(&Token::Explain)?;
        let analyze = if self.peek() == &Token::Analyze {
            self.advance();
            true
        } else {
            false
        };
        let inner = self.parse_statement()?;
        Ok(Statement::Explain { analyze, stmt: Box::new(inner) })
    }

    // ---- CREATE TABLE ----

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;
        match self.peek() {
            Token::Table => self.parse_create_table_rest(),
            Token::Database => self.parse_create_database_rest(),
            Token::User => self.parse_create_user(),
            Token::Role => self.parse_create_role(),
            other => Err(MonaxError::Sql(format!(
                "expected TABLE, DATABASE, USER, or ROLE after CREATE, got {}", other
            ))),
        }
    }

    fn parse_create_user(&mut self) -> Result<Statement> {
        self.expect(&Token::User)?;
        let name = self.expect_ident()?;
        if self.peek() == &Token::Identified {
            self.advance();
            self.expect(&Token::By)?;
        }
        let pw = match self.peek().clone() {
            Token::StringLit(s) => {
                self.advance();
                s.clone()
            }
            other => Err(MonaxError::Sql(format!(
                "expected credential string, got {}", other
            )))?,
        };
        Ok(Statement::CreateUser { name, password: pw })
    }

    fn parse_create_role(&mut self) -> Result<Statement> {
        self.expect(&Token::Role)?;
        let name = self.expect_ident()?;
        Ok(Statement::CreateRole { name })
    }

    fn parse_create_database_rest(&mut self) -> Result<Statement> {
        self.expect(&Token::Database)?;
        let if_not_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        Ok(Statement::CreateDatabase { name, if_not_exists })
    }

    fn parse_create_table_rest(&mut self) -> Result<Statement> {
        self.expect(&Token::Table)?;

        let if_not_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let name = self.expect_ident()?;

        self.expect(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_column_def()?);
            if self.peek() == &Token::Comma {
                self.advance();
                if self.peek() == &Token::Cluster {
                    break;
                }
                if self.peek() == &Token::RightParen {
                    break;
                }
            } else {
                break;
            }
        }
        self.expect(&Token::RightParen)?;

        let cluster_by = if self.peek() == &Token::Cluster {
            self.advance();
            self.expect(&Token::By)?;
            self.expect(&Token::LeftParen)?;
            let mut keys = Vec::new();
            loop {
                keys.push(self.expect_ident()?);
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(&Token::RightParen)?;
            Some(keys)
        } else {
            None
        };

        // Optional: PARTITION BY DAY/YEAR(column)
        let partition_by = if self.peek() == &Token::Partition {
            self.advance();
            self.expect(&Token::By)?;
            let first_token = self.expect_ident()?.to_uppercase();
            
            let (strategy, column, param) = if first_token == "HASH" {
                self.expect(&Token::LeftParen)?;
                let col = self.expect_ident()?;
                let num_buckets = if self.peek() == &Token::Comma {
                    self.advance();
                    self.expect_integer()? as i32
                } else {
                    10
                };
                self.expect(&Token::RightParen)?;
                (PartitionStrategy::Hash(num_buckets), col, Some(num_buckets))
            } else if matches!(first_token.as_str(), "DAY" | "YEAR" | "MONTH") {
                self.expect(&Token::LeftParen)?;
                let col = self.expect_ident()?;
                self.expect(&Token::RightParen)?;
                let strategy = match first_token.as_str() {
                    "DAY" => PartitionStrategy::Day,
                    "YEAR" => PartitionStrategy::Year,
                    "MONTH" => PartitionStrategy::Month,
                    _ => PartitionStrategy::Value,
                };
                (strategy, col, None)
            } else {
                (PartitionStrategy::Value, first_token, None)
            };
            
            Some(PartitionDef { column, strategy, param })
        } else {
            None
        };

        Ok(Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            cluster_by,
            partition_by,
        })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDefAst> {
        let name = self.expect_ident()?;
        let type_str = self.expect_ident()?.to_uppercase();
        let data_type = parse_data_type(&type_str)?;

        let mut nullable = true;
        let mut scale = 0i32;
        let mut precision = 0i32;
        let mut auto_increment = false;

        // Optional: (precision, scale)
        if self.peek() == &Token::LeftParen {
            self.advance();
            let p = self.expect_integer()?;
            if self.peek() == &Token::Comma {
                self.advance();
                scale = self.expect_integer()?;
            }
            precision = p;
            self.expect(&Token::RightParen)?;
        }

        // Optional: NOT NULL
        if self.peek() == &Token::Not {
            self.advance();
            self.expect(&Token::Null)?;
            nullable = false;
        }

        // Optional: AUTOINCREMENT / AUTO_INCREMENT
        if self.peek() == &Token::AutoIncrement {
            self.advance();
            auto_increment = true;
        }

        if data_type == DataType::Decimal && scale < 0 {
            return Err(MonaxError::Sql(format!("invalid decimal scale {}", scale)));
        }
        if data_type == DataType::Decimal && scale > 18 {
            return Err(MonaxError::Sql(format!("decimal scale {} exceeds supported maximum 18", scale)));
        }

        Ok(ColumnDefAst {
            name,
            data_type,
            nullable,
            scale,
            precision,
            auto_increment,
        })
    }

    // ---- DROP TABLE ----

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;
        match self.peek() {
            Token::Table => self.parse_drop_table_rest(),
            Token::Database => self.parse_drop_database_rest(),
            Token::User => self.parse_drop_user(),
            Token::Role => self.parse_drop_role(),
            other => Err(MonaxError::Sql(format!(
                "expected TABLE, DATABASE, USER, or ROLE after DROP, got {}", other
            ))),
        }
    }

    fn parse_drop_user(&mut self) -> Result<Statement> {
        self.expect(&Token::User)?;
        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        Ok(Statement::DropUser { name, if_exists })
    }

    fn parse_drop_role(&mut self) -> Result<Statement> {
        self.expect(&Token::Role)?;
        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        Ok(Statement::DropRole { name, if_exists })
    }

    fn parse_grant(&mut self) -> Result<Statement> {
        self.expect(&Token::Grant)?;
        let privileges = self.parse_privileges()?;
        let on = if self.peek() == &Token::On {
            self.advance();
            Some(self.parse_grant_object()?)
        } else {
            None
        };
        self.expect(&Token::To)?;
        let to = self.expect_ident()?;
        Ok(Statement::Grant { privileges, on, to })
    }

    fn parse_revoke(&mut self) -> Result<Statement> {
        self.expect(&Token::Revoke)?;
        let privileges = self.parse_privileges()?;
        let on = if self.peek() == &Token::On {
            self.advance();
            Some(self.parse_grant_object()?)
        } else {
            None
        };
        self.expect(&Token::From)?;
        let from = self.expect_ident()?;
        Ok(Statement::Revoke { privileges, on, from })
    }

    fn parse_privileges(&mut self) -> Result<Vec<Privilege>> {
        if self.peek() == &Token::Privileges || self.peek() == &Token::All {
            if self.peek() == &Token::All {
                self.advance();
                return Ok(vec![Privilege::All]);
            }
            self.advance();
        }
        let mut privileges = Vec::new();
        loop {
            let priv_name = self.expect_ident()?.to_uppercase();
            let privilege = match priv_name.as_str() {
                "ALL" | "ALL_PRIVILEGES" => Privilege::All,
                "SELECT" => Privilege::Select,
                "INSERT" => Privilege::Insert,
                "UPDATE" => Privilege::Update,
                "DELETE" => Privilege::Delete,
                "CREATE" => Privilege::Create,
                "DROP" => Privilege::Drop,
                "ALTER" => Privilege::Alter,
                "TRUNCATE" => Privilege::Truncate,
                _ => return Err(MonaxError::Sql(format!("unknown privilege: {}", priv_name))),
            };
            privileges.push(privilege);
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        Ok(privileges)
    }

    fn parse_grant_object(&mut self) -> Result<GrantObject> {
        match self.peek() {
            Token::Table => {
                self.advance();
                let name = self.expect_ident()?;
                Ok(GrantObject::Table(name))
            }
            Token::Database => {
                self.advance();
                let name = self.expect_ident()?;
                Ok(GrantObject::Database(name))
            }
            Token::Ident(_) => {
                let name = self.expect_ident()?.to_uppercase();
                match name.as_str() {
                    "ALL" | "ALL_TABLES" => Ok(GrantObject::AllTables),
                    "ALL_DATABASES" => Ok(GrantObject::AllDatabases),
                    _ => Ok(GrantObject::Table(name)),
                }
            }
            other => Err(MonaxError::Sql(format!(
                "expected TABLE, DATABASE, or identifier after ON, got {}", other
            ))),
        }
    }

    fn parse_drop_database_rest(&mut self) -> Result<Statement> {
        self.expect(&Token::Database)?;
        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        Ok(Statement::DropDatabase { name, if_exists })
    }

    fn parse_drop_table_rest(&mut self) -> Result<Statement> {
        self.expect(&Token::Table)?;

        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let name = self.expect_ident()?;
        Ok(Statement::DropTable { name, if_exists })
    }

    fn parse_alter(&mut self) -> Result<Statement> {
        self.expect(&Token::Alter)?;
        match self.peek() {
            Token::Table => {
                self.advance();
                self.parse_alter_table_rest()
            },
            Token::User => {
                self.advance();
                self.parse_alter_user_rest()
            },
            other => Err(MonaxError::Sql(format!(
                "expected TABLE or USER after ALTER, got {}", other
            ))),
        }
    }

    fn parse_alter_table_rest(&mut self) -> Result<Statement> {
        let name = self.expect_ident()?;
        let mut operations = Vec::new();
        loop {
            let op = if self.peek() == &Token::Add {
                self.advance();
                if self.peek() == &Token::Column {
                    self.advance();
                    let column = self.parse_column_def()?;
                    AlterOperation::AddColumn { column }
                } else if self.peek() == &Token::Partition {
                    self.advance();
                    let partition_key = self.expect_ident()?;
                    AlterOperation::DropPartition { partition_key }
                } else {
                    self.expect(&Token::Column)?;
                    let column = self.parse_column_def()?;
                    AlterOperation::AddColumn { column }
                }
            } else if self.peek() == &Token::Drop {
                self.advance();
                if self.peek() == &Token::Column {
                    self.advance();
                    let col_name = self.expect_ident()?;
                    AlterOperation::DropColumn { name: col_name }
                } else if self.peek() == &Token::Partition {
                    self.advance();
                    let partition_key = self.expect_ident()?;
                    AlterOperation::DropPartition { partition_key }
                } else {
                    self.expect(&Token::Column)?;
                    let col_name = self.expect_ident()?;
                    AlterOperation::DropColumn { name: col_name }
                }
            } else if self.peek() == &Token::Truncate {
                self.advance();
                if self.peek() == &Token::Partition {
                    self.advance();
                    let partition_key = self.expect_ident()?;
                    AlterOperation::TruncatePartition { partition_key }
                } else {
                    return Err(MonaxError::Sql("expected PARTITION after TRUNCATE in ALTER TABLE".into()));
                }
            } else if self.peek() == &Token::Rename {
                self.advance();
                if self.peek() == &Token::Column {
                    self.advance();
                    let old_name = self.expect_ident()?;
                    self.expect(&Token::To)?;
                    let new_name = self.expect_ident()?;
                    AlterOperation::RenameColumn { old_name, new_name }
                } else if self.peek() == &Token::To {
                    self.advance();
                    let new_name = self.expect_ident()?;
                    AlterOperation::RenameTable { new_name }
                } else {
                    return Err(MonaxError::Sql("expected COLUMN or TO after RENAME".into()));
                }
            } else if self.peek() == &Token::Modify {
                self.advance();
                self.expect(&Token::Column)?;
                let column = self.parse_column_def()?;
                AlterOperation::AddColumn { column }
            } else {
                break;
            };
            operations.push(op);
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        Ok(Statement::AlterTable { name, operations })
    }

    fn parse_alter_user_rest(&mut self) -> Result<Statement> {
        let name = self.expect_ident()?;
        let mut operations = Vec::new();
        loop {
            if self.peek() == &Token::Identified {
                self.advance();
                self.expect(&Token::By)?;
                let pw = match self.peek().clone() {
                    Token::StringLit(s) => {
                        self.advance();
                        s.clone()
                    }
                    other => Err(MonaxError::Sql(format!(
                        "expected password string, got {}", other
                    )))?,
                };
                operations.push(AlterUserOperation::SetPassword { password: pw });
            } else if self.peek() == &Token::Password {
                self.advance();
                self.expect(&Token::Eq)?;
                let pw = match self.peek().clone() {
                    Token::StringLit(s) => {
                        self.advance();
                        s.clone()
                    }
                    other => Err(MonaxError::Sql(format!(
                        "expected password string, got {}", other
                    )))?,
                };
                operations.push(AlterUserOperation::SetPassword { password: pw });
            } else if self.peek() == &Token::Lock {
                self.advance();
                operations.push(AlterUserOperation::Lock);
            } else if self.peek() == &Token::Unlock {
                self.advance();
                operations.push(AlterUserOperation::Unlock);
            } else {
                break;
            }
        }
        Ok(Statement::AlterUser { name, operations })
    }

    fn parse_truncate(&mut self) -> Result<Statement> {
        self.expect(&Token::Truncate)?;
        self.expect(&Token::Table)?;
        let table = self.expect_ident()?;
        Ok(Statement::Truncate { table })
    }

    // ---- INSERT ----

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;
        let table = self.expect_ident()?;

        // Optional column list
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

        self.expect(&Token::Values)?;
        let mut all_values = Vec::new();
        loop {
            self.expect(&Token::LeftParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_expr()?);
                if self.peek() == &Token::Comma {
                    self.advance();
                } else {
                    break;
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

        Ok(Statement::Insert {
            table,
            columns,
            values: all_values,
        })
    }

    // ---- UPDATE ----

    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(&Token::Update)?;
        let table = self.expect_ident()?;

        self.expect(&Token::Set)?;

        // Parse assignments: col1 = val1, col2 = val2, ...
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

        // Optional WHERE clause
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

    // ---- DELETE ----

    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;
        let table = self.expect_ident()?;

        // Optional WHERE clause
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

    // ---- SELECT ----

    fn parse_select(&mut self) -> Result<Statement> {
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
            having: None,
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
                    if !matches!(next,
                        Token::From | Token::Where | Token::Group |
                        Token::Order | Token::Limit | Token::Offset |
                        Token::Comma | Token::Semicolon | Token::Eof |
                        Token::Union | Token::Having
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

    // ---- FROM clause parsing ----

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        // Parse base table
        let base_table = self.parse_table_ref()?;

        // Check for JOINs (including INNER, LEFT, CROSS variations)
        let mut joins = Vec::new();
        loop {
            let is_join = matches!(self.peek(),
                Token::Join | Token::Inner | Token::Left | Token::Cross
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
        let name = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.advance();
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            // Check if next token could be a keyword that starts a new clause
            // If not, treat as implicit alias
            let next = self.peek();
            if !matches!(next,
                Token::Join | Token::Inner | Token::Left | Token::Right |
                Token::Cross | Token::On | Token::Where | Token::Group |
                Token::Order | Token::Limit | Token::Offset | Token::Semicolon | Token::Eof
            ) {
                Some(self.expect_ident()?)
            } else {
                None
            }
        } else {
            None
        };
        Ok(TableRef { name, alias })
    }

    fn parse_join_clause(&mut self) -> Result<JoinClause> {
        // Parse join type: [INNER] JOIN, LEFT [OUTER] JOIN, CROSS JOIN
        let join_type = if self.peek() == &Token::Left {
            self.advance();
            if self.peek() == &Token::Outer {
                self.advance();
            }
            self.expect(&Token::Join)?;
            JoinType::Left
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

        // ON clause (required for Inner/Left, not for Cross)
        let condition = if join_type != JoinType::Cross {
            self.expect(&Token::On)?;
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(JoinClause { join_type, right, condition })
    }

    // ---- SHOW ----

    fn parse_show(&mut self) -> Result<Statement> {
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
            Token::Ident(word) if word.eq_ignore_ascii_case("INDEX") || word.eq_ignore_ascii_case("INDEXES") => {
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
                "expected TABLES, DATABASES, COLUMNS, INDEXES, CREATE, USER, ROLE, GRANTS, PARTITIONS, STATS, ENGINES, or PROCESSLIST after SHOW, got {}", other
            ))),
        }
    }

    // ---- USE ----

    fn parse_use(&mut self) -> Result<Statement> {
        self.expect(&Token::Use)?;
        let name = self.expect_ident()?;
        Ok(Statement::UseDatabase(name))
    }

    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(&Token::Begin)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Begin)
    }

    fn parse_commit(&mut self) -> Result<Statement> {
        self.expect(&Token::Commit)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(&Token::Rollback)?;
        if self.peek() == &Token::Transaction {
            self.advance();
        }
        Ok(Statement::Rollback)
    }

    fn parse_flush(&mut self) -> Result<Statement> {
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

    fn parse_optimize(&mut self) -> Result<Statement> {
        self.expect(&Token::Optimize)?;
        self.expect(&Token::Table)?;
        let table = self.expect_ident()?;
        Ok(Statement::OptimizeTable { table })
    }

    fn parse_pause(&mut self) -> Result<Statement> {
        self.expect(&Token::Pause)?;
        Ok(Statement::PauseFlush)
    }

    fn parse_resume(&mut self) -> Result<Statement> {
        self.expect(&Token::Resume)?;
        Ok(Statement::ResumeFlush)
    }

    // ---- Expression parsing (Pratt-style precedence) ----

    fn parse_expr(&mut self) -> Result<Expr> {
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
        } else {
            Ok(left)
        }
    }

    fn parse_addition(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplication()?;
        loop {
            // Use Token matching here
            let op = match self.peek() {
                t if *t == Token::Ident("+".into()) => Some(BinOp::Add),
                t if *t == Token::Ident("-".into()) => Some(BinOp::Sub),
                _ => None,
            };
            if let Some(_op) = op {
                self.advance();
                let right = self.parse_multiplication()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op: _op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr> {
        // We don't support mul/div tokens at the lexer level yet,
        // so just parse unary/primary
        self.parse_unary()
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek() == &Token::Not {
            self.advance();
            let expr = self.parse_primary()?;
            Ok(Expr::UnaryNot(Box::new(expr)))
        } else {
            self.parse_primary()
        }
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
                // Check for table.column qualified reference
                if self.peek() == &Token::Dot {
                    self.advance();
                    let column = self.expect_ident()?;
                    Ok(Expr::QualifiedColumn { table: name, column })
                } else if self.peek() == &Token::LeftParen {
                    // Check for function call
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
                    Ok(Expr::Function { name, args, distinct })
                } else if self.peek() == &Token::Ident("IS".into()) {
                    // Check for IS NULL / IS NOT NULL
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
                }.to_string();
                self.advance();
                // Must be followed by (
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
                Ok(Expr::Function { name, args, distinct })
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
                "unexpected token in expression: {}", other
            ))),
        }
    }

    // ---- Helpers ----

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
                "expected '{}', got '{}'", expected, tok
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
                other.as_ref().map(|t| t.to_string()).unwrap_or("EOF".into())
            ))),
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
                other.as_ref().map(|t| t.to_string()).unwrap_or("EOF".into())
            ))),
        }
    }
}

fn parse_data_type(s: &str) -> Result<DataType> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let stmts = Parser::parse(
            "CREATE TABLE users (id INT64 NOT NULL, name VARCHAR, age INT32)"
        ).unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::CreateTable { name, columns, if_not_exists, .. } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].data_type, DataType::Int64);
                assert!(!columns[0].nullable);
                assert_eq!(columns[1].data_type, DataType::VarChar);
                assert!(columns[1].nullable);
                assert!(!if_not_exists);
            }
            _ => panic!("expected CREATE TABLE"),
        }
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let stmts = Parser::parse(
            "CREATE TABLE IF NOT EXISTS t (id INT64)"
        ).unwrap();
        match &stmts[0] {
            Statement::CreateTable { if_not_exists, .. } => {
                assert!(if_not_exists);
            }
            _ => panic!("expected CREATE TABLE"),
        }
    }

    #[test]
    fn test_drop_table() {
        let stmts = Parser::parse("DROP TABLE IF EXISTS foo").unwrap();
        match &stmts[0] {
            Statement::DropTable { name, if_exists } => {
                assert_eq!(name, "foo");
                assert!(if_exists);
            }
            _ => panic!("expected DROP TABLE"),
        }
    }

    #[test]
    fn test_insert() {
        let stmts = Parser::parse(
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        ).unwrap();
        match &stmts[0] {
            Statement::Insert { table, columns, values } => {
                assert_eq!(table, "users");
                assert_eq!(columns, &vec!["id", "name"]);
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected INSERT"),
        }
    }

    #[test]
    fn test_select_star() {
        let stmts = Parser::parse("SELECT * FROM users WHERE id = 1").unwrap();
        match &stmts[0] {
            Statement::Select { select_list, from, where_clause, .. } => {
                assert_eq!(select_list.len(), 1);
                assert!(matches!(&select_list[0], SelectItem::Star));
                assert!(from.is_some());
                assert!(where_clause.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_select_with_limit() {
        let stmts = Parser::parse("SELECT id, name FROM t ORDER BY id DESC LIMIT 10 OFFSET 5").unwrap();
        match &stmts[0] {
            Statement::Select { order_by, limit, offset, .. } => {
                assert_eq!(order_by.len(), 1);
                assert!(order_by[0].desc);
                assert!(limit.is_some());
                assert!(offset.is_some());
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_show_tables() {
        let stmts = Parser::parse("SHOW TABLES").unwrap();
        assert!(matches!(&stmts[0], Statement::ShowTables));
    }

    #[test]
    fn test_explain_plain() {
        let stmts = Parser::parse("EXPLAIN SELECT * FROM t").unwrap();
        match &stmts[0] {
            Statement::Explain { analyze, stmt } => {
                assert!(!*analyze);
                assert!(matches!(&**stmt, Statement::Select { .. }));
            }
            _ => panic!("expected EXPLAIN"),
        }
    }

    #[test]
    fn test_explain_analyze() {
        let stmts = Parser::parse("EXPLAIN ANALYZE SELECT * FROM t WHERE id > 10").unwrap();
        match &stmts[0] {
            Statement::Explain { analyze, stmt } => {
                assert!(*analyze, "ANALYZE flag should be true");
                assert!(matches!(&**stmt, Statement::Select { .. }));
            }
            _ => panic!("expected EXPLAIN ANALYZE"),
        }
    }

    #[test]
    fn test_use_database() {
        let stmts = Parser::parse("USE mydb").unwrap();
        assert!(matches!(&stmts[0], Statement::UseDatabase(ref s) if s == "mydb"));
    }

    #[test]
    fn test_multiple_statements() {
        let stmts = Parser::parse("CREATE TABLE t (id INT64); INSERT INTO t VALUES (1); SELECT * FROM t").unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn test_update() {
        let stmts = Parser::parse("UPDATE users SET name = 'Alice', age = 30 WHERE id = 1").unwrap();
        match &stmts[0] {
            Statement::Update { table, assignments, where_clause } => {
                assert_eq!(table, "users");
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].column, "name");
                assert_eq!(assignments[1].column, "age");
                assert!(where_clause.is_some());
            }
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn test_update_without_where() {
        let stmts = Parser::parse("UPDATE t SET col = 123").unwrap();
        match &stmts[0] {
            Statement::Update { table, assignments, where_clause } => {
                assert_eq!(table, "t");
                assert_eq!(assignments.len(), 1);
                assert!(where_clause.is_none());
            }
            _ => panic!("expected UPDATE"),
        }
    }

    #[test]
    fn test_delete() {
        let stmts = Parser::parse("DELETE FROM users WHERE id = 1").unwrap();
        match &stmts[0] {
            Statement::Delete { table, where_clause } => {
                assert_eq!(table, "users");
                assert!(where_clause.is_some());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_delete_without_where() {
        let stmts = Parser::parse("DELETE FROM t").unwrap();
        match &stmts[0] {
            Statement::Delete { table, where_clause } => {
                assert_eq!(table, "t");
                assert!(where_clause.is_none());
            }
            _ => panic!("expected DELETE"),
        }
    }

    #[test]
    fn test_inner_join() {
        let stmts = Parser::parse("SELECT * FROM a JOIN b ON a.id = b.id").unwrap();
        match &stmts[0] {
            Statement::Select { from, .. } => {
                let from_clause = from.as_ref().expect("expected FROM clause");
                match from_clause {
                    FromClause::Join(join_expr) => {
                        // Check left table
                        match &*join_expr.left {
                            FromClause::Table(t) => assert_eq!(t.name, "a"),
                            _ => panic!("expected Table as left"),
                        }
                        // Check joins
                        assert_eq!(join_expr.joins.len(), 1);
                        let jc = &join_expr.joins[0];
                        assert_eq!(jc.join_type, JoinType::Inner);
                        assert_eq!(jc.right.name, "b");
                        assert!(jc.condition.is_some());
                    }
                    _ => panic!("expected Join"),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_left_join() {
        let stmts = Parser::parse("SELECT * FROM a LEFT JOIN b ON a.id = b.id").unwrap();
        match &stmts[0] {
            Statement::Select { from, .. } => {
                let from_clause = from.as_ref().expect("expected FROM clause");
                match from_clause {
                    FromClause::Join(join_expr) => {
                        assert_eq!(join_expr.joins.len(), 1);
                        let jc = &join_expr.joins[0];
                        assert_eq!(jc.join_type, JoinType::Left);
                        assert_eq!(jc.right.name, "b");
                    }
                    _ => panic!("expected Join"),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_multi_table_join() {
        let stmts = Parser::parse(
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id"
        ).unwrap();
        match &stmts[0] {
            Statement::Select { from, .. } => {
                let from_clause = from.as_ref().expect("expected FROM clause");
                match from_clause {
                    FromClause::Join(join_expr) => {
                        // Should have 2 chained joins: b and c
                        assert_eq!(join_expr.joins.len(), 2);
                        assert_eq!(join_expr.joins[0].right.name, "b");
                        assert_eq!(join_expr.joins[1].right.name, "c");
                    }
                    _ => panic!("expected Join"),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_qualified_column() {
        let stmts = Parser::parse("SELECT a.id, b.name FROM a JOIN b ON a.id = b.id").unwrap();
        match &stmts[0] {
            Statement::Select { select_list, .. } => {
                assert_eq!(select_list.len(), 2);
                match &select_list[0] {
                    SelectItem::Expr(Expr::QualifiedColumn { table, column }, _) => {
                        assert_eq!(table, "a");
                        assert_eq!(column, "id");
                    }
                    _ => panic!("expected QualifiedColumn"),
                }
                match &select_list[1] {
                    SelectItem::Expr(Expr::QualifiedColumn { table, column }, _) => {
                        assert_eq!(table, "b");
                        assert_eq!(column, "name");
                    }
                    _ => panic!("expected QualifiedColumn"),
                }
            }
            _ => panic!("expected SELECT"),
        }
    }

    #[test]
    fn test_begin() {
        let stmts = Parser::parse("BEGIN").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Begin));
    }

    #[test]
    fn test_begin_transaction() {
        let stmts = Parser::parse("BEGIN TRANSACTION").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Begin));
    }

    #[test]
    fn test_begin_txn() {
        let stmts = Parser::parse("BEGIN TXN").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Begin));
    }

    #[test]
    fn test_commit() {
        let stmts = Parser::parse("COMMIT").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Commit));
    }

    #[test]
    fn test_commit_transaction() {
        let stmts = Parser::parse("COMMIT TRANSACTION").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Commit));
    }

    #[test]
    fn test_rollback() {
        let stmts = Parser::parse("ROLLBACK").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Rollback));
    }

    #[test]
    fn test_rollback_transaction() {
        let stmts = Parser::parse("ROLLBACK TRANSACTION").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(&stmts[0], Statement::Rollback));
    }

    #[test]
    fn test_transaction_statements_with_semicolons() {
        let stmts = Parser::parse("BEGIN; INSERT INTO t VALUES (1); COMMIT;").unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(matches!(&stmts[0], Statement::Begin));
        assert!(matches!(&stmts[1], Statement::Insert { .. }));
        assert!(matches!(&stmts[2], Statement::Commit));
    }

    #[test]
    fn test_transaction_rollback_flow() {
        let stmts = Parser::parse("BEGIN; DELETE FROM users; ROLLBACK").unwrap();
        assert_eq!(stmts.len(), 3);
        assert!(matches!(&stmts[0], Statement::Begin));
        assert!(matches!(&stmts[1], Statement::Delete { .. }));
        assert!(matches!(&stmts[2], Statement::Rollback));
    }
}
