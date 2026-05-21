use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

use super::ast::*;

pub fn try_fast_parse_insert(sql: &str) -> Option<Result<Statement>> {
    let bytes = sql.as_bytes();
    let len = bytes.len();

    let upper_prefix_len = "INSERT INTO ".len();
    if len < upper_prefix_len {
        return None;
    }

    let mut pos = 0usize;
    while pos < upper_prefix_len {
        let a = bytes[pos].to_ascii_uppercase();
        let b = b"INSERT INTO "[pos];
        if a != b {
            return None;
        }
        pos += 1;
    }

    while pos < len && bytes[pos] == b' ' {
        pos += 1;
    }

    let table_start = pos;
    while pos < len && bytes[pos] != b' ' && bytes[pos] != b'(' {
        pos += 1;
    }
    if pos == table_start {
        return None;
    }
    let table = sql[table_start..pos].to_string();

    while pos < len && bytes[pos] == b' ' {
        pos += 1;
    }

    let columns = if pos < len && bytes[pos] == b'(' {
        pos += 1;
        let mut cols = Vec::new();
        loop {
            while pos < len && bytes[pos] == b' ' {
                pos += 1;
            }
            let col_start = pos;
            while pos < len && bytes[pos] != b',' && bytes[pos] != b')' && bytes[pos] != b' ' {
                pos += 1;
            }
            if pos == col_start {
                return None;
            }
            cols.push(sql[col_start..pos].to_string());
            while pos < len && bytes[pos] == b' ' {
                pos += 1;
            }
            if pos < len && bytes[pos] == b',' {
                pos += 1;
            } else if pos < len && bytes[pos] == b')' {
                pos += 1;
                break;
            } else {
                return None;
            }
        }
        while pos < len && bytes[pos] == b' ' {
            pos += 1;
        }
        cols
    } else {
        Vec::new()
    };

    let values_kw = b"VALUES ";
    if pos + values_kw.len() > len {
        return None;
    }
    for i in 0..values_kw.len() {
        if bytes[pos + i].to_ascii_uppercase() != values_kw[i] {
            return None;
        }
    }
    pos += values_kw.len();

    let mut all_values: Vec<Vec<Expr>> = Vec::new();

    loop {
        while pos < len && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= len || bytes[pos] != b'(' {
            return None;
        }
        pos += 1;

        let mut row: Vec<Expr> = Vec::new();
        loop {
            while pos < len && bytes[pos] == b' ' {
                pos += 1;
            }
            if pos >= len {
                return None;
            }

            let expr = match bytes[pos] {
                b'\'' => {
                    pos += 1;
                    let start = pos;
                    let mut end = start;
                    while end < len {
                        if bytes[end] == b'\'' {
                            if end + 1 < len && bytes[end + 1] == b'\'' {
                                end += 2;
                            } else {
                                break;
                            }
                        } else {
                            end += 1;
                        }
                    }
                    if end >= len {
                        return None;
                    }
                    let has_escape = sql[start..end].contains("''");
                    let s = if has_escape {
                        sql[start..end].replace("''", "'")
                    } else {
                        sql[start..end].to_string()
                    };
                    pos = end + 1;
                    Expr::Literal(ScalarValue::VarChar(Some(s)))
                }
                b'N' | b'n' if pos + 4 <= len
                    && (bytes[pos+1] == b'U' || bytes[pos+1] == b'u')
                    && (bytes[pos+2] == b'L' || bytes[pos+2] == b'l')
                    && (bytes[pos+3] == b'L' || bytes[pos+3] == b'l') => {
                    pos += 4;
                    Expr::Literal(ScalarValue::Int64(None))
                }
                b'-' if pos + 1 < len && bytes[pos + 1].is_ascii_digit() => {
                    let (val, new_pos) = parse_number(sql, bytes, pos, len)?;
                    pos = new_pos;
                    Expr::Literal(val)
                }
                c if c.is_ascii_digit() => {
                    let (val, new_pos) = parse_number(sql, bytes, pos, len)?;
                    pos = new_pos;
                    Expr::Literal(val)
                }
                _ => {
                    return None;
                }
            };
            row.push(expr);

            while pos < len && bytes[pos] == b' ' {
                pos += 1;
            }
            if pos < len && bytes[pos] == b',' {
                pos += 1;
            } else if pos < len && bytes[pos] == b')' {
                pos += 1;
                break;
            } else {
                return None;
            }
        }

        all_values.push(row);

        while pos < len && bytes[pos] == b' ' {
            pos += 1;
        }
        if pos >= len || bytes[pos] == b';' || bytes[pos] == b')' {
            break;
        }
        if bytes[pos] == b',' {
            pos += 1;
        } else {
            return None;
        }
    }

    Some(Ok(Statement::Insert {
        table,
        columns,
        values: all_values,
    }))
}

#[inline]
fn parse_number(sql: &str, bytes: &[u8], start: usize, len: usize) -> Option<(ScalarValue, usize)> {
    let mut pos = start;
    if bytes[pos] == b'-' {
        pos += 1;
    }
    while pos < len && bytes[pos].is_ascii_digit() {
        pos += 1;
    }
    let mut has_fraction = false;
    if pos < len && bytes[pos] == b'.' {
        has_fraction = true;
        pos += 1;
        let fraction_start = pos;
        while pos < len && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        if pos == fraction_start {
            return None;
        }
    }
    let s = &sql[start..pos];
    if has_fraction {
        Some((ScalarValue::Float64(Some(s.parse::<f64>().ok()?)), pos))
    } else {
        Some((ScalarValue::Int64(Some(s.parse::<i64>().ok()?)), pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_insert_simple() {
        let sql = "INSERT INTO t VALUES (1, 'hello', -42)";
        let result = try_fast_parse_insert(sql).unwrap().unwrap();
        match result {
            Statement::Insert { table, columns, values } => {
                assert_eq!(table, "t");
                assert!(columns.is_empty());
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 3);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_fast_insert_multi_row() {
        let sql = "INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')";
        let result = try_fast_parse_insert(sql).unwrap().unwrap();
        match result {
            Statement::Insert { table, columns, values } => {
                assert_eq!(table, "t");
                assert_eq!(columns, vec!["a", "b"]);
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_fast_insert_null() {
        let sql = "INSERT INTO t VALUES (NULL)";
        let result = try_fast_parse_insert(sql).unwrap().unwrap();
        match result {
            Statement::Insert { values, .. } => {
                assert_eq!(values.len(), 1);
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_fast_insert_escaped_quote() {
        let sql = "INSERT INTO t VALUES ('it''s')";
        let result = try_fast_parse_insert(sql).unwrap().unwrap();
        match result {
            Statement::Insert { values, .. } => {
                match &values[0][0] {
                    Expr::Literal(ScalarValue::VarChar(Some(s))) => assert_eq!(s, "it's"),
                    _ => panic!("expected string"),
                }
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_fast_insert_decimal_literal() {
        let sql = "INSERT INTO t VALUES (12.34, -0.56)";
        let result = try_fast_parse_insert(sql).unwrap().unwrap();
        match result {
            Statement::Insert { values, .. } => {
                assert_eq!(values.len(), 1);
                assert_eq!(values[0].len(), 2);
                match &values[0][0] {
                    Expr::Literal(ScalarValue::Float64(Some(v))) => assert_eq!(*v, 12.34),
                    _ => panic!("expected float"),
                }
                match &values[0][1] {
                    Expr::Literal(ScalarValue::Float64(Some(v))) => assert_eq!(*v, -0.56),
                    _ => panic!("expected float"),
                }
            }
            _ => panic!("expected insert"),
        }
    }

    #[test]
    fn test_fast_insert_not_insert() {
        assert!(try_fast_parse_insert("SELECT * FROM t").is_none());
    }
}
