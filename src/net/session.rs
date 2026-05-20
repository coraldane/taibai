use std::time::Instant;
use uuid::Uuid;

use crate::sql::execution::prepared::PreparedStatementCache;

pub struct Session {
    pub id: Uuid,
    pub database: String,
    pub username: String,
    pub created_at: Instant,
    pub last_activity: Instant,
    pub prepared_statements: PreparedStatementCache,
    pub active_txn_id: u64,
    pub binary_results: bool,
    pub compression_enabled: bool,
    pub bulk_import_mode: bool,
    pub disable_wal: bool,
    pub timezone_offset_hours: i32,
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
            active_txn_id: 0,
            binary_results: false,
            compression_enabled: false,
            bulk_import_mode: false,
            disable_wal: false,
            timezone_offset_hours: 0,
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
}