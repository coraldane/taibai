use std::time::Instant;
use uuid::Uuid;

use crate::sql::execution::prepared::PreparedStatementCache;
use crate::storage::txn::txn::{IsolationLevel, Transaction};

pub struct Session {
    pub id: Uuid,
    pub database: String,
    pub username: String,
    pub last_activity: Instant,
    pub prepared_statements: PreparedStatementCache,
    pub active_txn: Option<Transaction>,
    pub streaming_results: bool,
    pub bulk_import_mode: bool,
    pub timezone_offset_hours: i32,
    pub had_writes: bool,
    pub default_isolation_level: IsolationLevel,
}

impl Session {
    pub fn new(username: String, database: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            database,
            username,
            last_activity: Instant::now(),
            prepared_statements: PreparedStatementCache::new(),
            active_txn: None,
            streaming_results: false,
            bulk_import_mode: false,
            timezone_offset_hours: 0,
            had_writes: false,
            default_isolation_level: IsolationLevel::default(),
        }
    }

    pub fn session_id_string(&self) -> String {
        self.id.to_string()
    }

    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    pub fn set_timezone(&mut self, offset_hours: i32) {
        self.timezone_offset_hours = offset_hours;
    }

    pub fn timezone_offset(&self) -> i32 {
        self.timezone_offset_hours
    }

    pub fn begin_txn(&mut self, txn_id: u64) {
        self.active_txn = Some(Transaction::new(txn_id));
    }

    pub fn commit_txn(&mut self) {
        if let Some(ref mut txn) = self.active_txn {
            txn.commit();
        }
        self.active_txn = None;
    }

    pub fn rollback_txn(&mut self) {
        if let Some(ref mut txn) = self.active_txn {
            txn.rollback();
        }
        self.active_txn = None;
    }

    pub fn active_txn_id(&self) -> u64 {
        self.active_txn.as_ref().map(|t| t.id).unwrap_or(0)
    }

    pub fn in_txn(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn mark_txn_write(&mut self) {
        if let Some(ref mut txn) = self.active_txn {
            txn.mark_write();
        }
    }
}
