use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{
    AlterOperation, AlterUserOperation, ColumnDefAst, DefaultValue, GrantObject, PartitionDef,
    PartitionStrategy, Privilege, Statement, TtlDef, UniqueKeyDef,
};
use crate::sql::parser::lexer::Token;
use crate::sql::parser::parser::{parse_data_type, Parser};

impl Parser {
    pub fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;
        let unique = matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("UNIQUE"));
        if unique {
            self.advance();
            return self.parse_create_index_rest(true);
        }
        match self.peek() {
            Token::Table => self.parse_create_table_rest(),
            Token::Ident(s) if s.eq_ignore_ascii_case("INDEX") => {
                self.parse_create_index_rest(false)
            }
            Token::Database => self.parse_create_database_rest(),
            Token::User => self.parse_create_user(),
            Token::Role => self.parse_create_role(),
            other => Err(TaibaiError::Sql(format!(
                "expected TABLE, DATABASE, USER, ROLE, or INDEX after CREATE, got {}",
                other
            ))),
        }
    }

    fn parse_create_index_rest(&mut self, unique: bool) -> Result<Statement> {
        match self.peek() {
            Token::Ident(s) if s.eq_ignore_ascii_case("INDEX") => {
                self.advance();
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected INDEX after CREATE, got {}",
                    other
                )))
            }
        }
        let if_not_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table = self.parse_object_name()?;
        self.expect(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_ident()?);
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(&Token::RightParen)?;
        let index_type = if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("TYPE"))
        {
            self.advance();
            Some(self.parse_index_type_name()?)
        } else {
            None
        };
        Ok(Statement::CreateIndex {
            name,
            table,
            columns,
            unique,
            if_not_exists,
            index_type,
        })
    }

    fn parse_index_type_name(&mut self) -> Result<String> {
        match self.peek().clone() {
            Token::Ident(s) => {
                self.advance();
                Ok(s.to_ascii_lowercase())
            }
            Token::Set => {
                self.advance();
                Ok("set".to_string())
            }
            other => Err(TaibaiError::Sql(format!(
                "expected index type name, got {}",
                other
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
            other => Err(TaibaiError::Sql(format!(
                "expected credential string, got {}",
                other
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
        Ok(Statement::CreateDatabase {
            name,
            if_not_exists,
        })
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

        let name = self.parse_object_name()?;

        self.expect(&Token::LeftParen)?;
        let mut columns = Vec::new();
        let mut primary_key = Vec::new();
        let mut unique_keys = Vec::new();
        loop {
            if self.peek() == &Token::Primary {
                self.advance();
                self.expect(&Token::Key)?;
                self.expect(&Token::LeftParen)?;
                loop {
                    primary_key.push(self.expect_ident()?);
                    if self.peek() == &Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.expect(&Token::RightParen)?;
            } else if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("UNIQUE")) {
                unique_keys.push(self.parse_unique_key_def()?);
            } else {
                columns.push(self.parse_column_def()?);
            }
            if self.peek() == &Token::Comma {
                self.advance();
                if self.peek() == &Token::RightParen {
                    break;
                }
            } else {
                break;
            }
        }
        self.expect(&Token::RightParen)?;

        let mut cluster_by: Option<Vec<String>> = None;
        let mut partition_by: Option<PartitionDef> = None;
        let mut ttl: Option<TtlDef> = None;

        loop {
            if self.peek() == &Token::Cluster && cluster_by.is_none() {
                self.advance();
                self.expect(&Token::By)?;
                let mut keys = Vec::new();
                if self.peek() == &Token::LeftParen {
                    self.advance();
                    loop {
                        keys.push(self.expect_ident()?);
                        if self.peek() == &Token::Comma {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    self.expect(&Token::RightParen)?;
                } else {
                    keys.push(self.expect_ident()?);
                    while self.peek() == &Token::Comma {
                        self.advance();
                        keys.push(self.expect_ident()?);
                    }
                }
                cluster_by = Some(keys);
            } else if self.peek() == &Token::Partition && partition_by.is_none() {
                self.advance();
                self.expect(&Token::By)?;

                let (strategy, column, param) = if self.peek() == &Token::Hash {
                    self.advance();
                    self.expect(&Token::LeftParen)?;
                    let col = self.expect_ident()?;
                    let num_buckets = if self.peek() == &Token::Comma {
                        self.advance();
                        self.expect_integer()?
                    } else {
                        10
                    };
                    self.expect(&Token::RightParen)?;
                    (PartitionStrategy::Hash(num_buckets), col, Some(num_buckets))
                } else {
                    let first_token = self.expect_ident()?;
                    let function_name = first_token.to_uppercase();
                    if matches!(function_name.as_str(), "DAY" | "YEAR" | "MONTH" | "MOD") {
                        self.expect(&Token::LeftParen)?;
                        let col = self.expect_ident()?;
                        let param = if function_name == "MOD" {
                            self.expect(&Token::Comma)?;
                            Some(self.expect_integer()?)
                        } else {
                            None
                        };
                        self.expect(&Token::RightParen)?;
                        let strategy = match function_name.as_str() {
                            "DAY" => PartitionStrategy::Day,
                            "YEAR" => PartitionStrategy::Year,
                            "MONTH" => PartitionStrategy::Month,
                            "MOD" => PartitionStrategy::Mod(param.unwrap_or_default()),
                            _ => PartitionStrategy::Value,
                        };
                        (strategy, col, param)
                    } else {
                        (PartitionStrategy::Value, first_token, None)
                    }
                };

                partition_by = Some(PartitionDef {
                    column,
                    strategy,
                    param,
                });
            } else if is_ttl_token(self.peek()) && ttl.is_none() {
                ttl = Some(self.parse_ttl_def()?);
            } else {
                break;
            }
        }

        Ok(Statement::CreateTable {
            name,
            columns,
            if_not_exists,
            primary_key,
            unique_keys,
            cluster_by,
            partition_by,
            ttl,
        })
    }

    fn parse_unique_key_def(&mut self) -> Result<UniqueKeyDef> {
        self.expect_ident()?;
        let mut name = None;
        if matches!(self.peek(), Token::Key)
            || matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("INDEX"))
        {
            self.advance();
            if self.peek() != &Token::LeftParen {
                name = Some(self.expect_ident()?);
            }
        }
        self.expect(&Token::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.expect_ident()?);
            if self.peek() == &Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(&Token::RightParen)?;
        Ok(UniqueKeyDef { name, columns })
    }

    fn parse_ttl_def(&mut self) -> Result<TtlDef> {
        match self.peek() {
            Token::Ident(s) if s.eq_ignore_ascii_case("TTL") => {
                self.advance();
            }
            other => {
                return Err(TaibaiError::Sql(format!(
                    "expected TTL table option, got {}",
                    other
                )))
            }
        }

        let parenthesized = self.peek() == &Token::LeftParen;
        if parenthesized {
            self.advance();
        }

        let column = self.expect_ident()?;
        if self.peek() == &Token::Comma
            || matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("INTERVAL"))
        {
            self.advance();
        }
        let duration_secs = self.expect_integer()? as i64;

        if parenthesized {
            self.expect(&Token::RightParen)?;
        }

        if duration_secs <= 0 {
            return Err(TaibaiError::Sql(
                "TTL duration must be a positive number of seconds".into(),
            ));
        }

        Ok(TtlDef {
            column,
            duration_secs,
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
        let mut primary_key = false;
        let mut unique = false;
        let mut default_value: Option<DefaultValue> = None;

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

        if self.peek() == &Token::Not {
            self.advance();
            self.expect(&Token::Null)?;
            nullable = false;
        }

        if self.peek() == &Token::AutoIncrement {
            self.advance();
            auto_increment = true;
        }

        if self.peek() == &Token::Primary {
            self.advance();
            self.expect(&Token::Key)?;
            primary_key = true;
        }

        if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("UNIQUE")) {
            self.advance();
            unique = true;
            if self.peek() == &Token::Key
                || matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("INDEX"))
            {
                self.advance();
            }
        }

        if self.peek() == &Token::Default {
            self.advance();
            default_value = Some(match self.peek().clone() {
                Token::CurrentTimestamp => {
                    self.advance();
                    DefaultValue::CurrentTimestamp
                }
                Token::StringLit(s) => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::VarChar(Some(s)))
                }
                Token::Integer(v) => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::Int64(Some(v)))
                }
                Token::Float(v) => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::Float64(Some(v)))
                }
                Token::Ident(s) if s.eq_ignore_ascii_case("TRUE") => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::Bool(Some(true)))
                }
                Token::Ident(s) if s.eq_ignore_ascii_case("FALSE") => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::Bool(Some(false)))
                }
                Token::Null => {
                    self.advance();
                    DefaultValue::Literal(ScalarValue::Int64(None))
                }
                other => {
                    return Err(TaibaiError::Sql(format!(
                        "unexpected token after DEFAULT: {}",
                        other
                    )));
                }
            });
        }

        if data_type == DataType::Decimal && scale < 0 {
            return Err(TaibaiError::Sql(format!("invalid decimal scale {}", scale)));
        }
        if data_type == DataType::Decimal && scale > 18 {
            return Err(TaibaiError::Sql(format!(
                "decimal scale {} exceeds supported maximum 18",
                scale
            )));
        }

        Ok(ColumnDefAst {
            name,
            data_type,
            nullable,
            scale,
            precision,
            auto_increment,
            primary_key,
            unique,
            default_value,
        })
    }

    pub fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;
        match self.peek() {
            Token::Table => self.parse_drop_table_rest(),
            Token::Ident(word) if word.eq_ignore_ascii_case("INDEX") => {
                self.parse_drop_index_rest()
            }
            Token::Database => self.parse_drop_database_rest(),
            Token::User => self.parse_drop_user(),
            Token::Role => self.parse_drop_role(),
            Token::Ident(word) if word.eq_ignore_ascii_case("CHANGE") => {
                self.parse_drop_change_consumer()
            }
            other => Err(TaibaiError::Sql(format!(
                "expected TABLE, DATABASE, USER, ROLE, INDEX, or CHANGE after DROP, got {}",
                other
            ))),
        }
    }

    fn parse_drop_change_consumer(&mut self) -> Result<Statement> {
        let change = self.expect_ident()?;
        if !change.eq_ignore_ascii_case("CHANGE") {
            return Err(TaibaiError::Sql(format!("expected CHANGE, got {}", change)));
        }
        let consumer = self.expect_ident()?;
        if !consumer.eq_ignore_ascii_case("CONSUMER") {
            return Err(TaibaiError::Sql(format!(
                "expected CONSUMER after DROP CHANGE, got {}",
                consumer
            )));
        }
        let group = self.expect_ident_or_string()?;
        Ok(Statement::DropChangeConsumer { group })
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

    pub fn parse_grant(&mut self) -> Result<Statement> {
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

    pub fn parse_revoke(&mut self) -> Result<Statement> {
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
        Ok(Statement::Revoke {
            privileges,
            on,
            from,
        })
    }

    fn parse_privileges(&mut self) -> Result<Vec<Privilege>> {
        if self.peek() == &Token::All {
            self.advance();
            if self.peek() == &Token::Privileges {
                self.advance();
            }
            return Ok(vec![Privilege::All]);
        }
        let mut privileges = Vec::new();
        loop {
            let privilege = match self.peek() {
                Token::Select => Privilege::Select,
                Token::Insert => Privilege::Insert,
                Token::Update => Privilege::Update,
                Token::Delete => Privilege::Delete,
                Token::Create => Privilege::Create,
                Token::Drop => Privilege::Drop,
                Token::Alter => Privilege::Alter,
                Token::Truncate => Privilege::Truncate,
                Token::Subscribe => Privilege::Subscribe,
                Token::Ident(s)
                    if s.to_uppercase() == "ALL" || s.to_uppercase() == "ALL_PRIVILEGES" =>
                {
                    Privilege::All
                }
                _ => {
                    return Err(TaibaiError::Sql(format!(
                        "expected privilege name, got {}",
                        self.peek()
                    )))
                }
            };
            self.advance();
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
                let name = self.parse_qualified_name()?;
                Ok(GrantObject::Table(name))
            }
            Token::Database => {
                self.advance();
                let name = self.expect_ident()?;
                Ok(GrantObject::Database(name))
            }
            Token::Star => {
                self.advance();
                if self.peek() == &Token::Dot {
                    self.advance();
                    if self.peek() == &Token::Star {
                        self.advance();
                        Ok(GrantObject::AllTables)
                    } else {
                        Err(TaibaiError::Sql("expected '*' after '*.'".into()))
                    }
                } else {
                    Ok(GrantObject::AllTables)
                }
            }
            Token::Ident(_) => {
                let name = self.parse_qualified_name()?;
                let upper = name.to_uppercase();
                match upper.as_str() {
                    "ALL" | "ALL_TABLES" => Ok(GrantObject::AllTables),
                    "ALL_DATABASES" => Ok(GrantObject::AllDatabases),
                    _ => Ok(GrantObject::Table(name)),
                }
            }
            other => Err(TaibaiError::Sql(format!(
                "expected TABLE, DATABASE, or identifier after ON, got {}",
                other
            ))),
        }
    }

    fn parse_qualified_name(&mut self) -> Result<String> {
        let first = self.expect_ident()?;
        if self.peek() == &Token::Dot {
            self.advance();
            if self.peek() == &Token::Star {
                self.advance();
                Ok(format!("{}.*", first))
            } else {
                let second = self.expect_ident()?;
                Ok(format!("{}.{}", first, second))
            }
        } else {
            Ok(first)
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

    fn parse_drop_index_rest(&mut self) -> Result<Statement> {
        self.expect_ident()?;
        let if_exists = if self.peek() == &Token::If {
            self.advance();
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };
        let name = self.expect_ident()?;
        self.expect(&Token::On)?;
        let table = self.parse_object_name()?;
        Ok(Statement::DropIndex {
            name,
            table,
            if_exists,
        })
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

        let name = self.parse_object_name()?;
        Ok(Statement::DropTable { name, if_exists })
    }

    pub fn parse_alter(&mut self) -> Result<Statement> {
        self.expect(&Token::Alter)?;
        match self.peek() {
            Token::Table => {
                self.advance();
                self.parse_alter_table_rest()
            }
            Token::User => {
                self.advance();
                self.parse_alter_user_rest()
            }
            other => Err(TaibaiError::Sql(format!(
                "expected TABLE or USER after ALTER, got {}",
                other
            ))),
        }
    }

    fn parse_alter_table_rest(&mut self) -> Result<Statement> {
        let name = self.parse_object_name()?;
        let mut operations = Vec::new();
        loop {
            let op = if self.peek() == &Token::Add {
                self.advance();
                if self.peek() == &Token::Column {
                    self.advance();
                    let column = self.parse_column_def()?;
                    AlterOperation::AddColumn { column }
                } else if matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("UNIQUE"))
                {
                    AlterOperation::AddUniqueKey {
                        key: self.parse_unique_key_def()?,
                    }
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
                } else if self.peek() == &Token::Key
                    || matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case("INDEX"))
                {
                    self.advance();
                    let name = self.expect_ident()?;
                    AlterOperation::DropIndex { name }
                } else if is_ttl_token(self.peek()) {
                    self.advance();
                    AlterOperation::DropTtl
                } else {
                    self.expect(&Token::Column)?;
                    let col_name = self.expect_ident()?;
                    AlterOperation::DropColumn { name: col_name }
                }
            } else if self.peek() == &Token::Set {
                self.advance();
                if is_ttl_token(self.peek()) {
                    AlterOperation::SetTtl {
                        ttl: self.parse_ttl_def()?,
                    }
                } else {
                    return Err(TaibaiError::Sql(
                        "expected TTL after SET in ALTER TABLE".into(),
                    ));
                }
            } else if self.peek() == &Token::Truncate {
                self.advance();
                if self.peek() == &Token::Partition {
                    self.advance();
                    let partition_key = self.expect_ident()?;
                    AlterOperation::TruncatePartition { partition_key }
                } else {
                    return Err(TaibaiError::Sql(
                        "expected PARTITION after TRUNCATE in ALTER TABLE".into(),
                    ));
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
                    return Err(TaibaiError::Sql(
                        "expected COLUMN or TO after RENAME".into(),
                    ));
                }
            } else if self.peek() == &Token::Modify {
                self.advance();
                self.expect(&Token::Column)?;
                let column = self.parse_column_def()?;
                AlterOperation::ModifyColumn { column }
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
                    other => Err(TaibaiError::Sql(format!(
                        "expected password string, got {}",
                        other
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
                    other => Err(TaibaiError::Sql(format!(
                        "expected password string, got {}",
                        other
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

    pub fn parse_truncate(&mut self) -> Result<Statement> {
        self.expect(&Token::Truncate)?;
        if self.peek() == &Token::Table {
            self.advance();
        }
        let table = self.parse_object_name()?;
        Ok(Statement::Truncate { table })
    }
}

fn is_ttl_token(token: &Token) -> bool {
    matches!(token, Token::Ident(s) if s.eq_ignore_ascii_case("TTL"))
}
