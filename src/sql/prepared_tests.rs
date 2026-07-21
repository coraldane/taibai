use super::*;

#[test]
fn test_prepare_select() {
    let mut cache = PreparedStatementCache::new();
    let stmt = cache.prepare("SELECT * FROM users WHERE id = 1").unwrap();
    assert_eq!(stmt.id, 1);
    assert!(matches!(stmt.stmt, Statement::Select { .. }));
}

#[test]
fn test_prepare_insert() {
    let mut cache = PreparedStatementCache::new();
    let stmt = cache.prepare("INSERT INTO t (a) VALUES (1)").unwrap();
    assert_eq!(stmt.id, 1);
    assert!(matches!(stmt.stmt, Statement::Insert { .. }));
}

#[test]
fn test_get_prepared() {
    let mut cache = PreparedStatementCache::new();
    cache.prepare("SELECT 1").unwrap();
    let stmt = cache.prepare("SELECT 2").unwrap();

    assert!(cache.get(1).is_some());
    assert!(cache.get(2).is_some());
    assert!(cache.get(3).is_none());
}

#[test]
fn test_close_prepared() {
    let mut cache = PreparedStatementCache::new();
    cache.prepare("SELECT 1").unwrap();
    assert!(cache.close(1));
    assert!(!cache.close(1)); // already removed
    assert!(cache.get(1).is_none());
}

#[test]
fn test_auto_increment_id() {
    let mut cache = PreparedStatementCache::new();
    let s1 = cache.prepare("SELECT 1").unwrap();
    let s2 = cache.prepare("SELECT 2").unwrap();
    let s3 = cache.prepare("SELECT 3").unwrap();
    assert_eq!(s1.id, 1);
    assert_eq!(s2.id, 2);
    assert_eq!(s3.id, 3);
}

#[test]
fn test_prepare_empty_sql() {
    let mut cache = PreparedStatementCache::new();
    let result = cache.prepare("");
    assert!(result.is_err());
}

#[test]
fn test_clear() {
    let mut cache = PreparedStatementCache::new();
    cache.prepare("SELECT 1").unwrap();
    cache.prepare("SELECT 2").unwrap();
    assert_eq!(cache.len(), 2);
    cache.clear();
    assert!(cache.is_empty());
}
