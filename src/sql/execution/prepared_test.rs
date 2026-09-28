use super::*;
use crate::sql::parser::ast::{Expr, OrderByItem, Statement};
use crate::sql::parser::parser::Parser;
use crate::types::scalar::ScalarValue;

/// Parse a single statement. Panics on parse error (test helper).
fn parse_one(sql: &str) -> Statement {
    let mut stmts = Parser::parse(sql).expect("parse failed");
    assert_eq!(stmts.len(), 1, "expected exactly one statement");
    stmts.remove(0)
}

/// True if `expr` is still an unsubstituted placeholder.
fn is_placeholder(expr: &Expr) -> bool {
    matches!(expr, Expr::Placeholder(_))
}

#[test]
fn test_bind_limit_offset() {
    let mut stmt = parse_one("SELECT * FROM t LIMIT ? OFFSET ?");
    let params = [ScalarValue::Int64(Some(20)), ScalarValue::Int64(Some(5))];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Select { limit, offset, .. } => {
            assert!(
                matches!(limit, Some(Expr::Literal(ScalarValue::Int64(Some(20))))),
                "limit not bound: {:?}",
                limit
            );
            assert!(
                matches!(offset, Some(Expr::Literal(ScalarValue::Int64(Some(5))))),
                "offset not bound: {:?}",
                offset
            );
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_bind_group_by_having() {
    let mut stmt = parse_one("SELECT a, count(*) FROM t GROUP BY ? HAVING count(*) > ?");
    let params = [ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(10))];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Select {
            group_by, having, ..
        } => {
            assert_eq!(group_by.len(), 1);
            assert!(
                matches!(&group_by[0], Expr::Literal(ScalarValue::Int64(Some(1)))),
                "group_by not bound: {:?}",
                group_by[0]
            );
            // having should no longer contain a placeholder
            if let Some(h) = &having {
                assert!(!is_placeholder(h), "having still a placeholder");
            } else {
                panic!("expected having clause");
            }
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_bind_order_by() {
    let mut stmt = parse_one("SELECT * FROM t ORDER BY ? DESC");
    let params = [ScalarValue::Int64(Some(1))];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Select { order_by, .. } => {
            assert_eq!(order_by.len(), 1);
            assert!(
                matches!(
                    &order_by[0],
                    OrderByItem {
                        expr: Expr::Literal(ScalarValue::Int64(Some(1))),
                        desc: true
                    }
                ),
                "order_by not bound: {:?}",
                order_by[0]
            );
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_bind_where_clause_regression() {
    // where_clause was already supported before the fix 鈥?ensure it still works.
    let mut stmt = parse_one("SELECT * FROM t WHERE id = ?");
    let params = [ScalarValue::Int64(Some(42))];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Select {
            where_clause: Some(Expr::BinaryOp { right, .. }),
            ..
        } => assert!(
            matches!(&*right, Expr::Literal(ScalarValue::Int64(Some(42)))),
            "where not bound: {:?}",
            right
        ),
        _ => panic!("expected Select with where"),
    }
}

#[test]
fn test_bind_union_trailing_clauses_on_right_select() {
    // NOTE: in this parser, trailing ORDER BY / LIMIT / OFFSET after
    // `... UNION SELECT ...` are consumed by the right-hand parse_select()
    // and attached to the right Select, NOT to the Union node itself.
    // So placeholder binding for them flows through the Select branch,
    // which we verify here by descending into the right child.
    let mut stmt = parse_one("SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY ? LIMIT ? OFFSET ?");
    let params = [
        ScalarValue::Int64(Some(2)),
        ScalarValue::Int64(Some(50)),
        ScalarValue::Int64(Some(5)),
    ];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Union { right, .. } => match &*right {
            Statement::Select {
                order_by,
                limit,
                offset,
                ..
            } => {
                assert_eq!(order_by.len(), 1);
                assert!(matches!(
                    &order_by[0].expr,
                    Expr::Literal(ScalarValue::Int64(Some(2)))
                ));
                assert!(matches!(
                    limit,
                    Some(Expr::Literal(ScalarValue::Int64(Some(50))))
                ));
                assert!(matches!(
                    offset,
                    Some(Expr::Literal(ScalarValue::Int64(Some(5))))
                ));
            }
            _ => panic!("expected right child to be Select"),
        },
        _ => panic!("expected Union"),
    }
}

#[test]
fn test_bind_too_few_params_errors() {
    let mut stmt = parse_one("SELECT * FROM t LIMIT ? OFFSET ?");
    // Only one parameter provided for two placeholders.
    let params = [ScalarValue::Int64(Some(20))];
    let res = bind_params(&mut stmt, &params);
    assert!(res.is_err(), "expected out-of-bounds error");
}

// --- Coverage for previously-missed locations ---

#[test]
fn test_bind_select_list() {
    // `SELECT ?` puts the placeholder in select_list (SelectItem::Expr).
    let mut stmt = parse_one("SELECT ? AS foo, a + ? FROM t");
    let params = [ScalarValue::Int64(Some(7)), ScalarValue::Int64(Some(3))];
    bind_params(&mut stmt, &params).expect("bind failed");

    match stmt {
        Statement::Select { select_list, .. } => {
            use crate::sql::parser::ast::SelectItem;
            assert!(matches!(
                &select_list[0],
                SelectItem::Expr(Expr::Literal(ScalarValue::Int64(Some(7))), _)
            ));
            assert!(matches!(
                &select_list[1],
                SelectItem::Expr(Expr::BinaryOp { .. }, _)
            ));
        }
        _ => panic!("expected Select"),
    }
}

#[test]
fn test_bind_join_condition() {
    // Placeholder inside a JOIN ON condition lives in FromClause.
    let mut stmt = parse_one("SELECT * FROM a JOIN b ON a.id = ?");
    let params = [ScalarValue::Int64(Some(99))];
    bind_params(&mut stmt, &params).expect("bind failed");

    // After bind, no Expr in the whole statement should still be a Placeholder.
    assert!(
        !has_placeholders(&stmt),
        "join condition placeholder not bound"
    );
}

#[test]
fn test_bind_derived_table_subquery() {
    // Placeholder inside a FROM-clause derived table (TableRef::Subquery).
    let mut stmt = parse_one("SELECT * FROM (SELECT * FROM t WHERE x = ?) AS dt");
    let params = [ScalarValue::Int64(Some(5))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(
        !has_placeholders(&stmt),
        "derived-table placeholder not bound"
    );
}

#[test]
fn test_bind_scalar_subquery() {
    // Placeholder inside a scalar subquery in WHERE (Expr::Subquery).
    let mut stmt = parse_one("SELECT * FROM t WHERE total > (SELECT SUM(x) FROM t2 WHERE y = ?)");
    let params = [ScalarValue::Int64(Some(10))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(!has_placeholders(&stmt), "subquery placeholder not bound");
}

#[test]
fn test_bind_in_subquery() {
    // Placeholder inside `x IN (SELECT ... WHERE ?)` 鈥?the Subquery sits in
    // Expr::In.values, exercising the In + Subquery recursion together.
    let mut stmt = parse_one("SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE z = ?)");
    let params = [ScalarValue::Int64(Some(42))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(
        !has_placeholders(&stmt),
        "IN-subquery placeholder not bound"
    );
}

#[test]
fn test_bind_window_function() {
    // Placeholders in PARTITION BY and ORDER BY of a window function.
    let mut stmt = parse_one("SELECT *, ROW_NUMBER() OVER (PARTITION BY ? ORDER BY ?) FROM t");
    let params = [ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(
        !has_placeholders(&stmt),
        "window-function placeholders not bound"
    );
}

#[test]
fn test_bind_explain_wrapped() {
    // Placeholder inside EXPLAIN's inner statement.
    let mut stmt = parse_one("EXPLAIN SELECT * FROM t WHERE id = ?");
    let params = [ScalarValue::Int64(Some(1))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(
        !has_placeholders(&stmt),
        "explain inner placeholder not bound"
    );
}

#[test]
fn test_bind_numbered_placeholders() {
    // `$n` binds positionally: $2 -> the first supplied param is unused by it;
    // here $2 receives the Int64 and $1 the VarChar, regardless of order.
    let mut stmt = parse_one("SELECT * FROM t WHERE id = $2 AND name = $1");
    let params = [
        ScalarValue::VarChar(Some("beta".into())),
        ScalarValue::Int64(Some(42)),
    ];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(!has_placeholders(&stmt));
    match stmt {
        Statement::Select {
            where_clause: Some(Expr::BinaryOp { left, right, .. }),
            ..
        } => {
            // left: id = 42 ($2), right: name = 'beta' ($1).
            assert!(matches!(
                &*left,
                Expr::BinaryOp {
                    right: boxed,
                    ..
                } if matches!(&**boxed, Expr::Literal(ScalarValue::Int64(Some(42))))
            ));
            assert!(matches!(
                &*right,
                Expr::BinaryOp {
                    right: boxed,
                    ..
                } if matches!(&**boxed, Expr::Literal(ScalarValue::VarChar(Some(s))) if s == "beta")
            ));
        }
        _ => panic!("expected Select with where"),
    }
}

#[test]
fn test_mixed_numbered_and_sequential_placeholders() {
    // A `?` after `$2` continues at index 2 (the third parameter). Node count
    // stays 2 (two placeholder expressions), but binding requires 3 params.
    let mut stmt = parse_one("SELECT * FROM t WHERE a = $2 OR b = ?");
    assert_eq!(count_placeholders(&stmt), 2);
    let params = [
        ScalarValue::Int64(Some(0)),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Int64(Some(3)),
    ];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(!has_placeholders(&stmt));
}

#[test]
fn test_bind_insert_select_query() {
    // Placeholders inside INSERT ... SELECT's source query must bind.
    let mut stmt = parse_one("INSERT INTO t (a, b) SELECT a, b FROM s WHERE id = ?");
    let params = [ScalarValue::Int64(Some(7))];
    bind_params(&mut stmt, &params).expect("bind failed");
    assert!(!has_placeholders(&stmt), "insert-select placeholder not bound");
    match stmt {
        Statement::InsertSelect { table, query, .. } => {
            assert_eq!(table.name, "t");
            assert!(matches!(&*query, Statement::Select { .. }));
        }
        _ => panic!("expected InsertSelect"),
    }
}

#[test]
fn test_count_and_has_placeholders_consistent() {
    // For every top-level statement shape, count_placeholders and
    // has_placeholders must agree (both see placeholders, or neither does).
    let cases = [
        "SELECT * FROM t WHERE id = ?",
        "SELECT * FROM t LIMIT ? OFFSET ?",
        "SELECT a FROM t GROUP BY ? HAVING count(*) > ?",
        "SELECT * FROM t ORDER BY ?",
        // UNION: trailing clauses attach to the right-hand Select; the
        // three functions recurse into children, so they stay consistent.
        "SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY ? LIMIT ? OFFSET ?",
        // Previously-missed locations (now covered).
        "SELECT ? AS foo FROM t",
        "SELECT * FROM a JOIN b ON a.id = ?",
        "SELECT * FROM (SELECT * FROM t WHERE x = ?) AS dt",
        "SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE z = ?)",
        "SELECT *, ROW_NUMBER() OVER (PARTITION BY ? ORDER BY ?) FROM t",
        "EXPLAIN SELECT * FROM t WHERE id = ?",
        "INSERT INTO t (a) VALUES (?)",
        "REPLACE INTO t (a) VALUES (?)",
        "REPLACE INTO t (a) SELECT a FROM s WHERE id = ?",
        "INSERT INTO t (a, b) SELECT a, b FROM s WHERE id = ?",
        "INSERT INTO t (a) SELECT a FROM s WHERE id = $1",
        "UPDATE t SET a = ? WHERE id = ?",
        "DELETE FROM t WHERE id = ?",
    ];
    for sql in cases {
        let stmt = parse_one(sql);
        let count = count_placeholders(&stmt);
        let has = has_placeholders(&stmt);
        assert_eq!(
            count > 0,
            has,
            "count_placeholders (={}) and has_placeholders (={}) disagree for: {}",
            count,
            has,
            sql
        );
        assert!(count > 0, "expected at least one placeholder in: {}", sql);
    }
}

// --- PreparedStatementCache behaviour ---

#[test]
fn cache_assigns_monotonic_ids_from_one() {
    let mut cache = PreparedStatementCache::new();
    let a = cache.prepare("INSERT INTO t (a) VALUES (?)").unwrap();
    let b = cache.prepare("SELECT * FROM t WHERE id = ?").unwrap();
    assert_eq!(a.id, 1);
    assert_eq!(b.id, 2);
    assert_eq!(cache.len(), 2);
}

#[test]
fn cache_keeps_statements_execute_after_reinsert() {
    let mut cache = PreparedStatementCache::new();
    let stmt = cache.prepare("INSERT INTO t (a) VALUES (?)").unwrap();
    let id = stmt.id;
    let removed = cache.remove(id).expect("statement should exist");
    cache.insert(removed);
    assert!(cache.get(id).is_some());
}

#[test]
fn cache_does_not_evict_live_statements_beyond_legacy_lru_limit() {
    let mut cache = PreparedStatementCache::new();
    let first_id = cache.prepare("INSERT INTO t (a) VALUES (?)").unwrap().id;
    for _ in 0..300 {
        cache.prepare("SELECT * FROM t WHERE id = ?").unwrap();
    }
    assert_eq!(cache.len(), 301);
    assert!(
        cache.get(first_id).is_some(),
        "oldest statement must stay usable; silent eviction caused 'prepared statement not found'"
    );
}

#[test]
fn cache_rejects_prepare_over_explicit_limit() {
    let mut cache = PreparedStatementCache::with_max_size(3);
    for _ in 0..3 {
        cache.prepare("SELECT * FROM t WHERE id = ?").unwrap();
    }
    let err = cache
        .prepare("SELECT * FROM t WHERE id = ?")
        .expect_err("prepare over limit must fail loudly");
    assert!(err.to_string().contains("too many prepared statements"));
    assert_eq!(cache.len(), 3);
}

#[test]
fn cache_clear_then_prepare_never_reuses_ids() {
    let mut cache = PreparedStatementCache::new();
    cache.prepare("SELECT * FROM t WHERE id = ?").unwrap();
    cache.clear();
    let next = cache.prepare("SELECT * FROM t WHERE id = ?").unwrap();
    assert_eq!(next.id, 2);
    assert!(!cache.is_empty());
}
