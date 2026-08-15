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
    assert_eq!(tokens[1], Token::Lte);
    assert_eq!(tokens[5], Token::Neq);
    assert_eq!(tokens[9], Token::Gt);
}

#[test]
fn test_float() {
    let mut lexer = Lexer::new("2.71");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Float(2.71));
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

#[test]
fn test_negative_integer() {
    let mut lexer = Lexer::new("-42");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Integer(-42));
}

#[test]
fn test_negative_float() {
    let mut lexer = Lexer::new("-2.71");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Float(-2.71));
}

#[test]
fn test_minus_operator() {
    let mut lexer = Lexer::new("a - b");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Ident("a".into()));
    assert_eq!(tokens[1], Token::Minus);
    assert_eq!(tokens[2], Token::Ident("b".into()));
}

#[test]
fn test_negative_in_values() {
    let mut lexer = Lexer::new("VALUES (-1, -2.5, 'test')");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[2], Token::Integer(-1));
    assert_eq!(tokens[4], Token::Float(-2.5));
    assert_eq!(tokens[6], Token::StringLit("test".into()));
}

#[test]
fn test_transaction_keywords() {
    let mut lexer = Lexer::new("BEGIN TRANSACTION COMMIT ROLLBACK");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Begin);
    assert_eq!(tokens[1], Token::Transaction);
    assert_eq!(tokens[2], Token::Commit);
    assert_eq!(tokens[3], Token::Rollback);
}

#[test]
fn test_txn_keyword() {
    let mut lexer = Lexer::new("BEGIN TXN");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Begin);
    assert_eq!(tokens[1], Token::Transaction);
}
