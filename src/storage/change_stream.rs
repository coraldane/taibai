use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

const DEFAULT_RECENT_EVENTS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub sequence: u64,
    pub timestamp_micros: i64,
    pub database: String,
    pub table: String,
    pub operation: ChangeOperation,
    pub rows_affected: u64,
}

pub struct ChangeStream {
    next_sequence: AtomicU64,
    sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    recent: Mutex<VecDeque<ChangeEvent>>,
    recent_capacity: usize,
}

impl ChangeStream {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_RECENT_EVENTS)
    }

    pub fn with_capacity(recent_capacity: usize) -> Self {
        let channel_capacity = recent_capacity.max(16);
        let (sender, _) = tokio::sync::broadcast::channel(channel_capacity);
        Self {
            next_sequence: AtomicU64::new(1),
            sender,
            recent: Mutex::new(VecDeque::with_capacity(recent_capacity)),
            recent_capacity,
        }
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    pub fn publish(
        &self,
        database: &str,
        table: &str,
        operation: ChangeOperation,
        rows_affected: u64,
    ) -> ChangeEvent {
        let event = ChangeEvent {
            sequence: self.next_sequence.fetch_add(1, Ordering::AcqRel),
            timestamp_micros: now_micros(),
            database: database.to_string(),
            table: table.to_string(),
            operation,
            rows_affected,
        };

        if self.recent_capacity > 0 {
            let mut recent = self.recent.lock().unwrap();
            if recent.len() == self.recent_capacity {
                recent.pop_front();
            }
            recent.push_back(event.clone());
        }

        let _ = self.sender.send(event.clone());
        event
    }

    pub fn recent(&self, database: Option<&str>, table: Option<&str>) -> Vec<ChangeEvent> {
        self.recent
            .lock()
            .unwrap()
            .iter()
            .filter(|event| {
                database.map_or(true, |db| event.database == db)
                    && table.map_or(true, |table_name| event.table == table_name)
            })
            .cloned()
            .collect()
    }
}

impl Default for ChangeStream {
    fn default() -> Self {
        Self::new()
    }
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
