use crate::common::error::{Result, TaibaiError};

/// SQL token types.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Replace,
    Into,
    Values,
    Create,
    Table,
    Drop,
    Show,
    Describe,
    Tables,
    Databases,
    Use,
    Explain,
    Analyze,
    Not,
    Null,
    As,
    And,
    Or,
    Limit,
    Offset,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Distinct,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    If,
    Exists,
    Sort,
    Key,
    Cluster,
    Hash,
    Partition,
    Hot,
    Keep,
    Update,
    Set,
    Delete,
    Primary,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Alter,
    Truncate,
    Subscribe,
    Database,
    Column,
    Columns,
    Add,
    Rename,
    Modify,
    To,
    // Join keywords
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    On,
    // Window function keywords
    Over,
    Rows,
    Range,
    Unbounded,
    Preceding,
    Following,
    Current,
    // User/Role/Permission keywords
    User,
    Role,
    Grant,
    Revoke,
    Identified,
    Password,
    Privileges,
    All,
    // AutoIncrement
    AutoIncrement,
    // Default value
    Default,
    CurrentTimestamp,
    // New keywords for extended SQL commands
    Union,
    Flush,
    Optimize,
    Rebuild,
    Pause,
    Resume,
    Reset,
    Processlist,
    Engines,
    Stats,
    Partitions,
    Grants,
    Full,
    Lock,
    Unlock,
    For,
    // Literals
    Integer(i64),
    Float(f64),
    StringLit(String),
    // Identifiers
    Ident(String),
    // Symbols
    Star,
    Comma,
    LeftParen,
    RightParen,
    Semicolon,
    Dot,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Minus,
    Plus,
    // Parameter placeholder
    Placeholder,
    // Special
    Eof,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Select => write!(f, "SELECT"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Insert => write!(f, "INSERT"),
            Token::Replace => write!(f, "REPLACE"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Create => write!(f, "CREATE"),
            Token::Table => write!(f, "TABLE"),
            Token::Drop => write!(f, "DROP"),
            Token::Show => write!(f, "SHOW"),
            Token::Describe => write!(f, "DESCRIBE"),
            Token::Tables => write!(f, "TABLES"),
            Token::Databases => write!(f, "DATABASES"),
            Token::Use => write!(f, "USE"),
            Token::Explain => write!(f, "EXPLAIN"),
            Token::Analyze => write!(f, "ANALYZE"),
            Token::Not => write!(f, "NOT"),
            Token::Null => write!(f, "NULL"),
            Token::As => write!(f, "AS"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Offset => write!(f, "OFFSET"),
            Token::Order => write!(f, "ORDER"),
            Token::By => write!(f, "BY"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::Group => write!(f, "GROUP"),
            Token::Having => write!(f, "HAVING"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::Count => write!(f, "COUNT"),
            Token::Sum => write!(f, "SUM"),
            Token::Avg => write!(f, "AVG"),
            Token::Min => write!(f, "MIN"),
            Token::Max => write!(f, "MAX"),
            Token::If => write!(f, "IF"),
            Token::Exists => write!(f, "EXISTS"),
            Token::Sort => write!(f, "SORT"),
            Token::Key => write!(f, "KEY"),
            Token::Cluster => write!(f, "CLUSTER"),
            Token::Hash => write!(f, "HASH"),
            Token::Partition => write!(f, "PARTITION"),
            Token::Hot => write!(f, "HOT"),
            Token::Keep => write!(f, "KEEP"),
            Token::Update => write!(f, "UPDATE"),
            Token::Set => write!(f, "SET"),
            Token::Delete => write!(f, "DELETE"),
            Token::Primary => write!(f, "PRIMARY"),
            Token::Begin => write!(f, "BEGIN"),
            Token::Commit => write!(f, "COMMIT"),
            Token::Rollback => write!(f, "ROLLBACK"),
            Token::Transaction => write!(f, "TRANSACTION"),
            Token::Alter => write!(f, "ALTER"),
            Token::Truncate => write!(f, "TRUNCATE"),
            Token::Subscribe => write!(f, "SUBSCRIBE"),
            Token::Database => write!(f, "DATABASE"),
            Token::Column => write!(f, "COLUMN"),
            Token::Columns => write!(f, "COLUMNS"),
            Token::Add => write!(f, "ADD"),
            Token::Rename => write!(f, "RENAME"),
            Token::Modify => write!(f, "MODIFY"),
            Token::To => write!(f, "TO"),
            Token::Join => write!(f, "JOIN"),
            Token::Inner => write!(f, "INNER"),
            Token::Left => write!(f, "LEFT"),
            Token::Right => write!(f, "RIGHT"),
            Token::Outer => write!(f, "OUTER"),
            Token::Cross => write!(f, "CROSS"),
            Token::On => write!(f, "ON"),
            Token::Over => write!(f, "OVER"),
            Token::Rows => write!(f, "ROWS"),
            Token::Range => write!(f, "RANGE"),
            Token::Unbounded => write!(f, "UNBOUNDED"),
            Token::Preceding => write!(f, "PRECEDING"),
            Token::Following => write!(f, "FOLLOWING"),
            Token::Current => write!(f, "CURRENT"),
            Token::User => write!(f, "USER"),
            Token::Role => write!(f, "ROLE"),
            Token::Grant => write!(f, "GRANT"),
            Token::Revoke => write!(f, "REVOKE"),
            Token::Identified => write!(f, "IDENTIFIED"),
            Token::Password => write!(f, "PASSWORD"),
            Token::Privileges => write!(f, "PRIVILEGES"),
            Token::All => write!(f, "ALL"),
            Token::AutoIncrement => write!(f, "AUTOINCREMENT"),
            Token::Default => write!(f, "DEFAULT"),
            Token::CurrentTimestamp => write!(f, "CURRENT_TIMESTAMP"),
            Token::Union => write!(f, "UNION"),
            Token::Flush => write!(f, "FLUSH"),
            Token::Optimize => write!(f, "OPTIMIZE"),
            Token::Rebuild => write!(f, "REBUILD"),
            Token::Pause => write!(f, "PAUSE"),
            Token::Resume => write!(f, "RESUME"),
            Token::Reset => write!(f, "RESET"),
            Token::Processlist => write!(f, "PROCESSLIST"),
            Token::Engines => write!(f, "ENGINES"),
            Token::Stats => write!(f, "STATS"),
            Token::Partitions => write!(f, "PARTITIONS"),
            Token::Grants => write!(f, "GRANTS"),
            Token::Full => write!(f, "FULL"),
            Token::Lock => write!(f, "LOCK"),
            Token::Unlock => write!(f, "UNLOCK"),
            Token::For => write!(f, "FOR"),
            Token::Integer(v) => write!(f, "{}", v),
            Token::Float(v) => write!(f, "{}", v),
            Token::StringLit(v) => write!(f, "'{}'", v),
            Token::Ident(v) => write!(f, "{}", v),
            Token::Star => write!(f, "*"),
            Token::Placeholder => write!(f, "?"),
            Token::Comma => write!(f, ","),
            Token::LeftParen => write!(f, "("),
            Token::RightParen => write!(f, ")"),
            Token::Semicolon => write!(f, ";"),
            Token::Dot => write!(f, "."),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "!="),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Gte => write!(f, ">="),
            Token::Minus => write!(f, "-"),
            Token::Plus => write!(f, "+"),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

pub struct Lexer<'a> {
    input: &'a str,
    pos: usize,
    bytes: &'a [u8],
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos >= self.bytes.len() {
                tokens.push(Token::Eof);
                break;
            }

            let ch = self.bytes[self.pos];
            let token = match ch {
                b'(' => {
                    self.pos += 1;
                    Token::LeftParen
                }
                b')' => {
                    self.pos += 1;
                    Token::RightParen
                }
                b',' => {
                    self.pos += 1;
                    Token::Comma
                }
                b';' => {
                    self.pos += 1;
                    Token::Semicolon
                }
                b'.' => {
                    self.pos += 1;
                    Token::Dot
                }
                b'*' => {
                    self.pos += 1;
                    Token::Star
                }
                b'?' => {
                    self.pos += 1;
                    Token::Placeholder
                }
                b'+' => {
                    self.pos += 1;
                    Token::Plus
                }
                b'=' => {
                    self.pos += 1;
                    Token::Eq
                }
                b'<' => {
                    self.pos += 1;
                    if self.peek_byte() == Some(b'=') {
                        self.pos += 1;
                        Token::Lte
                    } else if self.peek_byte() == Some(b'>') {
                        self.pos += 1;
                        Token::Neq
                    } else {
                        Token::Lt
                    }
                }
                b'>' => {
                    self.pos += 1;
                    if self.peek_byte() == Some(b'=') {
                        self.pos += 1;
                        Token::Gte
                    } else {
                        Token::Gt
                    }
                }
                b'!' => {
                    self.pos += 1;
                    if self.peek_byte() == Some(b'=') {
                        self.pos += 1;
                        Token::Neq
                    } else {
                        return Err(TaibaiError::Sql("unexpected character '!'".into()));
                    }
                }
                b'/' => {
                    self.pos += 1;
                    Token::Ident("/".into())
                }
                b'%' => {
                    self.pos += 1;
                    Token::Ident("%".into())
                }
                b'-' => {
                    self.pos += 1;
                    if self
                        .peek_byte()
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false)
                    {
                        self.read_negative_number()?
                    } else {
                        Token::Minus
                    }
                }
                b'\'' => self.read_string()?,
                c if c.is_ascii_digit() => self.read_number()?,
                c if c.is_ascii_alphabetic() || c == b'_' => self.read_ident_or_keyword()?,
                c => {
                    let ch = c as char;
                    if ch.is_whitespace() {
                        continue;
                    }
                    return Err(TaibaiError::Sql(format!("unexpected character '{}'", ch)));
                }
            };
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len() && {
            let b = self.bytes[self.pos];
            b == b' ' || b == b'\t' || b == b'\n' || b == b'\r'
        } {
            self.pos += 1;
        }
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn read_string(&mut self) -> Result<Token> {
        let start = self.pos + 1;
        let mut end = start;
        let mut has_escape = false;
        while end < self.bytes.len() {
            if self.bytes[end] == b'\'' {
                if end + 1 < self.bytes.len() && self.bytes[end + 1] == b'\'' {
                    has_escape = true;
                    end += 2;
                } else {
                    break;
                }
            } else {
                end += 1;
            }
        }
        if end >= self.bytes.len() {
            self.pos = end;
            return Err(TaibaiError::Sql("unterminated string literal".into()));
        }
        let s = if has_escape {
            let mut result = String::with_capacity(end - start);
            let mut i = start;
            while i < end {
                if self.bytes[i] == b'\'' && i + 1 < end && self.bytes[i + 1] == b'\'' {
                    result.push('\'');
                    i += 2;
                } else {
                    let c = unsafe { self.input.as_bytes().get_unchecked(i) };
                    if *c < 128 {
                        result.push(*c as char);
                        i += 1;
                    } else {
                        let ch = self.input[i..].chars().next().unwrap();
                        result.push(ch);
                        i += ch.len_utf8();
                    }
                }
            }
            result
        } else {
            self.input[start..end].to_string()
        };
        self.pos = end + 1;
        Ok(Token::StringLit(s))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.bytes.len() && self.bytes[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let s = &self.input[start..self.pos];
            let v = s
                .parse::<f64>()
                .map_err(|e| TaibaiError::Sql(format!("invalid float '{}': {}", s, e)))?;
            Ok(Token::Float(v))
        } else {
            let s = &self.input[start..self.pos];
            let v = s
                .parse::<i64>()
                .map_err(|e| TaibaiError::Sql(format!("invalid integer '{}': {}", s, e)))?;
            Ok(Token::Integer(v))
        }
    }

    fn read_negative_number(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.bytes.len() && self.bytes[self.pos] == b'.' {
            self.pos += 1;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let s = &self.input[start..self.pos];
            let v = s
                .parse::<f64>()
                .map_err(|e| TaibaiError::Sql(format!("invalid float '{}': {}", s, e)))?;
            Ok(Token::Float(-v))
        } else {
            let s = &self.input[start..self.pos];
            let v = s
                .parse::<i64>()
                .map_err(|e| TaibaiError::Sql(format!("invalid integer '{}': {}", s, e)))?;
            Ok(Token::Integer(-v))
        }
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.bytes.len() {
            let ch = self.bytes[self.pos];
            if ch.is_ascii_alphanumeric() || ch == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let word = &self.input[start..self.pos];
        Ok(match word.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "REPLACE" => Token::Replace,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "DROP" => Token::Drop,
            "SHOW" => Token::Show,
            "DESCRIBE" => Token::Describe,
            "TABLES" => Token::Tables,
            "DATABASES" => Token::Databases,
            "USE" => Token::Use,
            "EXPLAIN" => Token::Explain,
            "ANALYZE" => Token::Analyze,
            "NOT" => Token::Not,
            "NULL" => Token::Null,
            "AS" => Token::As,
            "AND" => Token::And,
            "OR" => Token::Or,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "GROUP" => Token::Group,
            "HAVING" => Token::Having,
            "DISTINCT" => Token::Distinct,
            "COUNT" => Token::Count,
            "SUM" => Token::Sum,
            "AVG" => Token::Avg,
            "MIN" => Token::Min,
            "MAX" => Token::Max,
            "IF" => Token::If,
            "EXISTS" => Token::Exists,
            "SORT" => Token::Sort,
            "KEY" => Token::Key,
            "CLUSTER" => Token::Cluster,
            "HASH" => Token::Hash,
            "PARTITION" => Token::Partition,
            "HOT" => Token::Hot,
            "KEEP" => Token::Keep,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "PRIMARY" => Token::Primary,
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "TRANSACTION" | "TXN" => Token::Transaction,
            "ALTER" => Token::Alter,
            "TRUNCATE" => Token::Truncate,
            "SUBSCRIBE" => Token::Subscribe,
            "DATABASE" => Token::Database,
            "COLUMN" => Token::Column,
            "COLUMNS" => Token::Columns,
            "ADD" => Token::Add,
            "RENAME" => Token::Rename,
            "MODIFY" => Token::Modify,
            "TO" => Token::To,
            // Join keywords
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "OUTER" => Token::Outer,
            "CROSS" => Token::Cross,
            "ON" => Token::On,
            "OVER" => Token::Over,
            "ROWS" => Token::Rows,
            "RANGE" => Token::Range,
            "UNBOUNDED" => Token::Unbounded,
            "PRECEDING" => Token::Preceding,
            "FOLLOWING" => Token::Following,
            "CURRENT" => Token::Current,
            "ROW_NUMBER" => Token::Ident("ROW_NUMBER".into()),
            "RANK" => Token::Ident("RANK".into()),
            "DENSE_RANK" => Token::Ident("DENSE_RANK".into()),
            "LEAD" => Token::Ident("LEAD".into()),
            "LAG" => Token::Ident("LAG".into()),
            "USER" => Token::User,
            "ROLE" => Token::Role,
            "GRANT" => Token::Grant,
            "REVOKE" => Token::Revoke,
            "IDENTIFIED" => Token::Identified,
            "PASSWORD" => Token::Password,
            "PRIVILEGES" => Token::Privileges,
            "ALL" => Token::All,
            "AUTOINCREMENT" | "AUTO_INCREMENT" => Token::AutoIncrement,
            "DEFAULT" => Token::Default,
            "CURRENT_TIMESTAMP" => Token::CurrentTimestamp,
            "UNION" => Token::Union,
            "FLUSH" => Token::Flush,
            "OPTIMIZE" => Token::Optimize,
            "REBUILD" => Token::Rebuild,
            "PAUSE" => Token::Pause,
            "RESUME" => Token::Resume,
            "RESET" => Token::Reset,
            "PROCESSLIST" => Token::Processlist,
            "ENGINES" => Token::Engines,
            "STATS" => Token::Stats,
            "PARTITIONS" => Token::Partitions,
            "GRANTS" => Token::Grants,
            "FULL" => Token::Full,
            "LOCK" => Token::Lock,
            "UNLOCK" => Token::Unlock,
            "FOR" => Token::For,
            // Data types as identifiers
            "BOOL" | "BOOLEAN" => Token::Ident("BOOL".into()),
            "INT8" | "TINYINT" => Token::Ident("INT8".into()),
            "INT16" | "SMALLINT" => Token::Ident("INT16".into()),
            "INT32" | "INT" | "INTEGER" => Token::Ident("INT32".into()),
            "INT64" | "BIGINT" => Token::Ident("INT64".into()),
            "UINT8" => Token::Ident("UINT8".into()),
            "UINT16" => Token::Ident("UINT16".into()),
            "UINT32" => Token::Ident("UINT32".into()),
            "UINT64" => Token::Ident("UINT64".into()),
            "FLOAT32" | "FLOAT" | "REAL" => Token::Ident("FLOAT32".into()),
            "FLOAT64" | "DOUBLE" => Token::Ident("FLOAT64".into()),
            "DECIMAL" | "DECIMAL64" => Token::Ident("DECIMAL".into()),
            "DATE" => Token::Ident("DATE".into()),
            "TIME" => Token::Ident("TIME".into()),
            "TIMESTAMP" | "DATETIME" => Token::Ident("TIMESTAMP".into()),
            "INTERVAL" => Token::Ident("INTERVAL".into()),
            "VARCHAR" | "TEXT" | "STRING" => Token::Ident("VARCHAR".into()),
            "BLOB" | "BYTES" => Token::Ident("BLOB".into()),
            "VECTOR" => Token::Ident("VECTOR".into()),
            "DAY" => Token::Ident("DAY".into()),
            "MONTH" => Token::Ident("MONTH".into()),
            "YEAR" => Token::Ident("YEAR".into()),
            _ => Token::Ident(word.to_string()),
        })
    }
}

#[cfg(test)]
#[path = "lexer_test.rs"]
mod tests;
