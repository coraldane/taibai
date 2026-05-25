use std::time::Instant;
use uuid::Uuid;

use crate::sql::execution::prepared::PreparedStatementCache;
use crate::storage::txn::txn::{AccessMode, IsolationLevel, Transaction};

pub struct Session {
    pub id: Uuid,
    pub database: String,
    pub username: String,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub prepared_statements: PreparedStatementCache,
    pub active_txn: Option<Transaction>,
    pub binary_results: bool,
    pub compression_enabled: bool,
    pub bulk_import_mode: bool,
    pub disable_wal: bool,
    pub timezone_offset_hours: i32,
    pub had_writes: bool,
    pub default_isolation_level: IsolationLevel,
    pub default_access_mode: AccessMode,
}

impl Session {
    pub fn new(username: String, database: String) -> Self {
        let now = Instant::now();
        Self {
            id: Uuid::new_v4(),
            database,
            username,
            created_at: now,
            last_activity: now,
            prepared_statements: PreparedStatementCache::new(),
            active_txn: None,
            binary_results: false,
            compression_enabled: false,
            bulk_import_mode: false,
            disable_wal: false,
            timezone_offset_hours: 0,
            had_writes: false,
            default_isolation_level: IsolationLevel::default(),
            default_access_mode: AccessMode::default(),
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

    pub fn set_default_isolation_level(&mut self, level: IsolationLevel) {
        self.default_isolation_level = level;
    }

    pub fn set_default_access_mode(&mut self, mode: AccessMode) {
        self.default_access_mode = mode;
    }

    pub fn begin_txn(&mut self, txn_id: u64) {
        self.active_txn = Some(
            Transaction::new(txn_id)
                .with_isolation(self.default_isolation_level)
                .with_access_mode(self.default_access_mode),
        );
    }

    pub fn begin_txn_with_options(
        &mut self,
        txn_id: u64,
        isolation_level: IsolationLevel,
        access_mode: AccessMode,
    ) {
        self.active_txn = Some(
            Transaction::new(txn_id)
                .with_isolation(isolation_level)
                .with_access_mode(access_mode),
        );
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

    pub fn txn_isolation_level(&self) -> Option<IsolationLevel> {
        self.active_txn.as_ref().map(|t| t.isolation_level)
    }

    pub fn txn_access_mode(&self) -> Option<AccessMode> {
        self.active_txn.as_ref().map(|t| t.access_mode)
    }

    pub fn txn_is_read_only(&self) -> bool {
        self.active_txn
            .as_ref()
            .map(|t| t.is_read_only())
            .unwrap_or(false)
    }
}
