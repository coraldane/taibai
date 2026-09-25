//! Registry of currently running queries, backing `SHOW PROCESSLIST` and
//! `KILL QUERY`. The request handler registers every heavy request before
//! execution and drops the returned guard afterwards; entries keyed by the
//! query id reported in the processlist.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::common::query_cancel::{QueryContext, QueryPhase};

/// How much of the statement text is retained for the processlist. INSERT
/// payloads can be megabytes; only this prefix is ever stored.
const SQL_SNIPPET_LEN: usize = 512;

pub struct RunningQuery {
    pub context: Arc<QueryContext>,
    pub session_id: String,
    pub username: String,
    pub database: String,
    pub sql: String,
    pub started_at: Instant,
}

/// Point-in-time view of one running query for `SHOW PROCESSLIST`.
pub struct QueryInfo {
    pub query_id: u64,
    pub session_id: String,
    pub username: String,
    pub database: String,
    pub sql: String,
    pub elapsed_secs: u64,
    pub phase: QueryPhase,
    pub cancelled: bool,
}

#[derive(Default)]
pub struct QueryRegistry {
    inner: RwLock<HashMap<u64, Arc<RunningQuery>>>,
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a running query and return the guard that unregisters it.
    /// The SQL text is trimmed to a bounded snippet at registration time.
    pub fn register(
        self: &Arc<Self>,
        context: Arc<QueryContext>,
        session_id: String,
        username: String,
        database: String,
        sql: &str,
    ) -> QueryRegistration {
        let query_id = context.query_id();
        let sql = collapse_sql(sql);
        let entry = Arc::new(RunningQuery {
            context,
            session_id,
            username,
            database,
            sql,
            started_at: Instant::now(),
        });
        self.inner.write().unwrap().insert(query_id, entry);
        QueryRegistration {
            registry: Arc::clone(self),
            query_id,
        }
    }

    fn unregister(&self, query_id: u64) {
        self.inner.write().unwrap().remove(&query_id);
    }

    /// Cancel the target query at its next safe point. Returns `false` when
    /// no query with that id is running (it may have just finished).
    pub fn kill(&self, query_id: u64, reason: String) -> bool {
        let inner = self.inner.read().unwrap();
        match inner.get(&query_id) {
            Some(entry) => {
                entry.context.cancel_with_reason(reason);
                true
            }
            None => false,
        }
    }

    pub fn list(&self) -> Vec<QueryInfo> {
        let inner = self.inner.read().unwrap();
        let mut infos: Vec<QueryInfo> = inner
            .values()
            .map(|entry| QueryInfo {
                query_id: entry.context.query_id(),
                session_id: entry.session_id.clone(),
                username: entry.username.clone(),
                database: entry.database.clone(),
                sql: entry.sql.clone(),
                elapsed_secs: entry.started_at.elapsed().as_secs(),
                phase: entry.context.phase(),
                cancelled: entry.context.is_cancelled(),
            })
            .collect();
        infos.sort_by_key(|info| std::cmp::Reverse(info.query_id));
        infos
    }

    pub fn active_count(&self) -> usize {
        self.inner.read().unwrap().len()
    }
}

/// RAII handle: dropping it removes the query from the registry, including on
/// panic unwinding, so `SHOW PROCESSLIST` never lists finished work.
pub struct QueryRegistration {
    registry: Arc<QueryRegistry>,
    query_id: u64,
}

impl Drop for QueryRegistration {
    fn drop(&mut self) {
        self.registry.unregister(self.query_id);
    }
}

/// Collapse whitespace and cap the length so the processlist shows one
/// readable line per statement regardless of payload size.
fn collapse_sql(sql: &str) -> String {
    let mut snippet: String = sql.split_whitespace().collect::<Vec<_>>().join(" ");
    snippet.truncate(SQL_SNIPPET_LEN);
    snippet
}

#[cfg(test)]
mod tests {
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
}
