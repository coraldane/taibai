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

#[test]
fn test_line_comment() {
    let mut lexer = Lexer::new("SELECT 1 -- trailing comment\n, 2 -- another\n");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Select);
    assert_eq!(tokens[1], Token::Integer(1));
    assert_eq!(tokens[2], Token::Comma);
    assert_eq!(tokens[3], Token::Integer(2));
    assert_eq!(tokens[4], Token::Eof);
}

#[test]
fn test_block_comment() {
    let mut lexer = Lexer::new("SELECT /* inline * comment */ 1");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Select);
    assert_eq!(tokens[1], Token::Integer(1));
}

#[test]
fn test_multiline_block_comment() {
    let mut lexer = Lexer::new("SELECT 1 /* spans\nmultiple\nlines */ , 2");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[2], Token::Comma);
    assert_eq!(tokens[3], Token::Integer(2));
}

#[test]
fn test_unterminated_block_comment() {
    let mut lexer = Lexer::new("SELECT 1 /* never closed");
    assert!(lexer.tokenize().is_err());
}

#[test]
fn test_division_still_works_after_comment_support() {
    let mut lexer = Lexer::new("a / b");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::Ident("/".into()));
}

#[test]
fn test_double_quoted_ident_not_keyword() {
    let mut lexer = Lexer::new("SELECT \"select\" FROM t");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Select);
    assert_eq!(tokens[1], Token::Ident("select".into()));
}

#[test]
fn test_double_quoted_ident_with_space_and_escape() {
    let mut lexer = Lexer::new("SELECT \"total \"\"amount\"\"\" FROM t");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[1], Token::Ident("total \"amount\"".into()));
}

#[test]
fn test_backtick_ident() {
    let mut lexer = Lexer::new("SELECT `group by` FROM `order`");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[0], Token::Select);
    assert_eq!(tokens[1], Token::Ident("group by".into()));
    assert_eq!(tokens[2], Token::From);
    assert_eq!(tokens[3], Token::Ident("order".into()));
}

#[test]
fn test_unterminated_quoted_ident() {
    let mut lexer = Lexer::new("SELECT \"oops FROM t");
    assert!(lexer.tokenize().is_err());
}

#[test]
fn test_dollar_numbered_placeholders() {
    let mut lexer = Lexer::new("INSERT INTO t VALUES ($1, $2, $10)");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens[5], Token::NumberedPlaceholder(1));
    assert_eq!(tokens[7], Token::NumberedPlaceholder(2));
    assert_eq!(tokens[9], Token::NumberedPlaceholder(10));
}

#[test]
fn test_dollar_zero_rejected() {
    let mut lexer = Lexer::new("SELECT $0");
    assert!(lexer.tokenize().is_err());
}

#[test]
fn test_bare_dollar_rejected() {
    let mut lexer = Lexer::new("SELECT $ FROM t");
    assert!(lexer.tokenize().is_err());
}
