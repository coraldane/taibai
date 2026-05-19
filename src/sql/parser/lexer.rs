use crate::common::error::{MonaxError, Result};

/// SQL token types.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Create,
    Table,
    Drop,
    Show,
    Tables,
    Databases,
    Use,
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
    Partition,
    ByKW,
    Update,
    Set,
    Delete,
    Primary,
    // Join keywords
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Cross,
    On,
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
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Create => write!(f, "CREATE"),
            Token::Table => write!(f, "TABLE"),
            Token::Drop => write!(f, "DROP"),
            Token::Show => write!(f, "SHOW"),
            Token::Tables => write!(f, "TABLES"),
            Token::Databases => write!(f, "DATABASES"),
            Token::Use => write!(f, "USE"),
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
            Token::Partition => write!(f, "PARTITION"),
            Token::ByKW => write!(f, "BY"),
            Token::Update => write!(f, "UPDATE"),
            Token::Set => write!(f, "SET"),
            Token::Delete => write!(f, "DELETE"),
            Token::Primary => write!(f, "PRIMARY"),
            Token::Join => write!(f, "JOIN"),
            Token::Inner => write!(f, "INNER"),
            Token::Left => write!(f, "LEFT"),
            Token::Right => write!(f, "RIGHT"),
            Token::Outer => write!(f, "OUTER"),
            Token::Cross => write!(f, "CROSS"),
            Token::On => write!(f, "ON"),
            Token::Integer(v) => write!(f, "{}", v),
            Token::Float(v) => write!(f, "{}", v),
            Token::StringLit(v) => write!(f, "'{}'", v),
            Token::Ident(v) => write!(f, "{}", v),
            Token::Star => write!(f, "*"),
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
            Token::Eof => write!(f, "EOF"),
        }
    }
}

/// SQL lexer: tokenizes SQL input.
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    /// Tokenize the entire input.
    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            if self.pos >= self.input.len() {
                tokens.push(Token::Eof);
                break;
            }

            let ch = self.input[self.pos];
            let token = match ch {
                '(' => { self.pos += 1; Token::LeftParen }
                ')' => { self.pos += 1; Token::RightParen }
                ',' => { self.pos += 1; Token::Comma }
                ';' => { self.pos += 1; Token::Semicolon }
                '.' => { self.pos += 1; Token::Dot }
                '*' => { self.pos += 1; Token::Star }
                '=' => { self.pos += 1; Token::Eq }
                '<' => {
                    self.pos += 1;
                    if self.peek_char() == Some('=') { self.pos += 1; Token::Lte }
                    else { Token::Lt }
                }
                '>' => {
                    self.pos += 1;
                    if self.peek_char() == Some('=') { self.pos += 1; Token::Gte }
                    else { Token::Gt }
                }
                '!' => {
                    self.pos += 1;
                    if self.peek_char() == Some('=') { self.pos += 1; Token::Neq }
                    else { return Err(MonaxError::Sql("unexpected character '!'".into())); }
                }
                '\'' => self.read_string()?,
                c if c.is_ascii_digit() => self.read_number()?,
                c if c.is_ascii_alphabetic() || c == '_' => self.read_ident_or_keyword()?,
                c => return Err(MonaxError::Sql(format!("unexpected character '{}'", c))),
            };
            tokens.push(token);
        }
        Ok(tokens)
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_whitespace() {
            self.pos += 1;
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn read_string(&mut self) -> Result<Token> {
        self.pos += 1; // skip opening quote
        let mut s = String::new();
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch == '\'' {
                self.pos += 1;
                // Check for escaped quote ''
                if self.pos < self.input.len() && self.input[self.pos] == '\'' {
                    s.push('\'');
                    self.pos += 1;
                    continue;
                }
                return Ok(Token::StringLit(s));
            }
            s.push(ch);
            self.pos += 1;
        }
        Err(MonaxError::Sql("unterminated string literal".into()))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
            self.pos += 1;
        }
        if self.pos < self.input.len() && self.input[self.pos] == '.' {
            self.pos += 1;
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let s: String = self.input[start..self.pos].iter().collect();
            let v = s.parse::<f64>().map_err(|e| MonaxError::Sql(format!("invalid float '{}': {}", s, e)))?;
            Ok(Token::Float(v))
        } else {
            let s: String = self.input[start..self.pos].iter().collect();
            let v = s.parse::<i64>().map_err(|e| MonaxError::Sql(format!("invalid integer '{}': {}", s, e)))?;
            Ok(Token::Integer(v))
        }
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token> {
        let start = self.pos;
        while self.pos < self.input.len() {
            let ch = self.input[self.pos];
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        let word: String = self.input[start..self.pos].iter().collect();
        Ok(match word.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "DROP" => Token::Drop,
            "SHOW" => Token::Show,
            "TABLES" => Token::Tables,
            "DATABASES" => Token::Databases,
            "USE" => Token::Use,
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
            "PARTITION" => Token::Partition,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "PRIMARY" => Token::Primary,
            // Join keywords
            "JOIN" => Token::Join,
            "INNER" => Token::Inner,
            "LEFT" => Token::Left,
            "RIGHT" => Token::Right,
            "OUTER" => Token::Outer,
            "CROSS" => Token::Cross,
            "ON" => Token::On,
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
            "YEAR" => Token::Ident("YEAR".into()),
            _ => Token::Ident(word),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokenize() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = 42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Ident("users".into()));
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::Ident("id".into()));
        assert_eq!(tokens[6], Token::Eq);
        assert_eq!(tokens[7], Token::Integer(42));
    }

    #[test]
    fn test_string_literal() {
        let mut lexer = Lexer::new("INSERT INTO t VALUES ('hello', 'it''s')");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[5], Token::StringLit("hello".into()));
        assert_eq!(tokens[7], Token::StringLit("it's".into()));
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("a <= b AND c != d OR e > f");
        let tokens = lexer.tokenize().unwrap();
        // 0:Ident("a") 1:Lte 2:Ident("b") 3:And 4:Ident("c")
        // 5:Neq 6:Ident("d") 7:Or 8:Ident("e") 9:Gt 10:Ident("f")
        assert_eq!(tokens[1], Token::Lte);
        assert_eq!(tokens[5], Token::Neq);
        assert_eq!(tokens[9], Token::Gt);
    }

    #[test]
    fn test_float() {
        let mut lexer = Lexer::new("3.14");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Float(3.14));
    }

    #[test]
    fn test_keywords() {
        let mut lexer = Lexer::new("CREATE TABLE IF NOT EXISTS foo");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::If);
        assert_eq!(tokens[3], Token::Not);
        assert_eq!(tokens[4], Token::Exists);
    }
}
