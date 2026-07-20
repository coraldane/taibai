#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock,
};

use crate::common::error::{Result, TaibaiError};

pub struct AutoIncrementCounter {
    current: AtomicU64,
    table: String,
    column: String,
}

impl AutoIncrementCounter {
    pub fn new(table: &str, column: &str, start: u64) -> Self {
        Self {
            current: AtomicU64::new(start),
            table: table.to_string(),
            column: column.to_string(),
        }
    }

    pub fn next_value(&self) -> u64 {
        self.current.fetch_add(1, Ordering::SeqCst)
    }

    pub fn current_value(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    pub fn set_value(&self, value: u64) {
        self.current.store(value, Ordering::SeqCst);
    }

    pub fn advance_past(&self, value: u64) {
        let next = value.saturating_add(1);
        let mut current = self.current.load(Ordering::SeqCst);
        while current < next {
            match self
                .current
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

pub struct AutoIncrementManager {
    counters: RwLock<HashMap<String, Arc<AutoIncrementCounter>>>,
}

impl AutoIncrementManager {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(
        &self,
        db: &str,
        table: &str,
        column: &str,
        start_value: u64,
    ) -> Result<Arc<AutoIncrementCounter>> {
        let key = format!("{}:{}", db, table);
        {
            let counters = self.counters.read().unwrap();
            if let Some(counter) = counters.get(&key) {
                if counter.column() != column {
                    return Err(TaibaiError::Internal(format!(
                        "auto_increment column mismatch: expected {}, got {}",
                        counter.column(),
                        column
                    )));
                }
                return Ok(Arc::clone(counter));
            }
        }

        let counter = Arc::new(AutoIncrementCounter::new(table, column, start_value));
        {
            let mut counters = self.counters.write().unwrap();
            counters.insert(key, Arc::clone(&counter));
        }
        Ok(counter)
    }

    pub fn get(&self, db: &str, table: &str) -> Option<Arc<AutoIncrementCounter>> {
        let key = format!("{}:{}", db, table);
        let counters = self.counters.read().unwrap();
        counters.get(&key).map(Arc::clone)
    }

    pub fn next_value(&self, db: &str, table: &str) -> Result<u64> {
        let counter = self.get(db, table).ok_or_else(|| {
            TaibaiError::Internal(format!(
                "no auto_increment counter for table {}.{}",
                db, table
            ))
        })?;
        Ok(counter.next_value())
    }

    pub fn unregister(&self, db: &str, table: &str) {
        let key = format!("{}:{}", db, table);
        {
            let mut counters = self.counters.write().unwrap();
            counters.remove(&key);
        }
    }

    pub fn rename_table(&self, db: &str, old_name: &str, new_name: &str) {
        let old_key = format!("{}:{}", db, old_name);
        let new_key = format!("{}:{}", db, new_name);
        let mut counters = self.counters.write().unwrap();
        if let Some(counter) = counters.remove(&old_key) {
            counters.insert(new_key, counter);
        }
    }

    pub fn unregister_database(&self, db: &str) {
        let prefix = format!("{}:", db);
        let mut counters = self.counters.write().unwrap();
        counters.retain(|key, _| !key.starts_with(&prefix));
    }

    pub fn reset(&self, db: &str, table: &str, value: u64) -> Result<()> {
        let counter = self.get(db, table).ok_or_else(|| {
            TaibaiError::Internal(format!(
                "no auto_increment counter for table {}.{}",
                db, table
            ))
        })?;
        counter.set_value(value);
        Ok(())
    }

    pub fn advance_past(&self, db: &str, table: &str, value: u64) -> Result<()> {
        let counter = self.get(db, table).ok_or_else(|| {
            TaibaiError::Internal(format!(
                "no auto_increment counter for table {}.{}",
                db, table
            ))
        })?;
        counter.advance_past(value);
        Ok(())
    }

    pub fn counters(&self) -> &RwLock<HashMap<String, Arc<AutoIncrementCounter>>> {
        &self.counters
    }
}

impl Default for AutoIncrementManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "auto_increment_tests.rs"]
mod tests;
