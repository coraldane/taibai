use std::collections::HashMap;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, Ordering}};

use crate::common::error::{MonaxError, Result};

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

    pub fn register(&self, db: &str, table: &str, column: &str, start_value: u64) -> Result<Arc<AutoIncrementCounter>> {
        let key = format!("{}:{}", db, table);
        {
            let counters = self.counters.read().unwrap();
            if let Some(counter) = counters.get(&key) {
                if counter.column() != column {
                    return Err(MonaxError::Internal(format!(
                        "auto_increment column mismatch: expected {}, got {}",
                        counter.column(), column
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
        let counter = self.get(db, table)
            .ok_or_else(|| MonaxError::Internal(format!(
                "no auto_increment counter for table {}.{}", db, table
            )))?;
        Ok(counter.next_value())
    }

    pub fn unregister(&self, db: &str, table: &str) {
        let key = format!("{}:{}", db, table);
        {
            let mut counters = self.counters.write().unwrap();
            counters.remove(&key);
        }
    }

    pub fn reset(&self, db: &str, table: &str, value: u64) -> Result<()> {
        let counter = self.get(db, table)
            .ok_or_else(|| MonaxError::Internal(format!(
                "no auto_increment counter for table {}.{}", db, table
            )))?;
        counter.set_value(value);
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
mod tests {
    use super::*;

    #[test]
    fn test_counter_sequence() {
        let counter = AutoIncrementCounter::new("t", "id", 1);
        assert_eq!(counter.next_value(), 1);
        assert_eq!(counter.next_value(), 2);
        assert_eq!(counter.next_value(), 3);
        assert_eq!(counter.current_value(), 4);
    }

    #[test]
    fn test_counter_thread_safety() {
        use std::thread;
        let counter = Arc::new(AutoIncrementCounter::new("t", "id", 1));
        let mut handles = vec![];
        
        for _ in 0..10 {
            let c = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                c.next_value()
            }));
        }
        
        let results: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let mut sorted = results.clone();
        sorted.sort();
        
        for i in 0..10 {
            assert_eq!(sorted[i], i + 1);
        }
        assert_eq!(counter.current_value(), 11);
    }

    #[test]
    fn test_manager_register() {
        let manager = AutoIncrementManager::new();
        let counter = manager.register("db", "users", "id", 100).unwrap();
        assert_eq!(counter.next_value(), 100);
        
        let counter2 = manager.get("db", "users").unwrap();
        assert_eq!(counter2.next_value(), 101);
    }

    #[test]
    fn test_manager_unregister() {
        let manager = AutoIncrementManager::new();
        manager.register("db", "temp", "id", 1).unwrap();
        assert!(manager.get("db", "temp").is_some());
        manager.unregister("db", "temp");
        assert!(manager.get("db", "temp").is_none());
    }
}