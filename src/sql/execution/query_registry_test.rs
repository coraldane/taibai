use super::*;
use std::sync::atomic::AtomicBool;

fn entry(sql: &str) -> (Arc<QueryRegistry>, QueryRegistration, Arc<AtomicBool>) {
    let registry = Arc::new(QueryRegistry::new());
    let flag = Arc::new(AtomicBool::new(false));
    let context = Arc::new(QueryContext::new(Arc::clone(&flag)));
    let guard = registry.register(context, "s".into(), "root".into(), "main".into(), sql);
    (registry, guard, flag)
}

#[test]
fn register_list_kill_unregister_roundtrip() {
    let (registry, _guard, flag) = entry("SELECT * FROM t WHERE id = 1");
    assert_eq!(registry.active_count(), 1);
    let infos = registry.list();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].username, "root");
    assert_eq!(infos[0].sql, "SELECT * FROM t WHERE id = 1");

    assert!(registry.kill(infos[0].query_id, "killed by test".into()));
    assert!(!registry.kill(999_999, "missing".into()));
    assert!(flag.load(std::sync::atomic::Ordering::Acquire));

    drop(_guard);
    assert_eq!(registry.active_count(), 0);
}

#[test]
fn sql_snippet_is_bounded_and_single_line() {
    let long_sql = format!("SELECT {} 1", "x".repeat(SQL_SNIPPET_LEN * 4));
    let (registry, guard, _flag) = entry(&long_sql);
    assert_eq!(registry.list()[0].sql.len(), SQL_SNIPPET_LEN);
    assert!(!registry.list()[0].sql.contains('\n'));
    drop(guard);
}
