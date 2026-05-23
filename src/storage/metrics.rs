use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metrics {
    queries_executed: AtomicU64,
    rows_read: AtomicU64,
    rows_written: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            queries_executed: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
            rows_written: AtomicU64::new(0),
        }
    }

    pub fn increment_queries(&self) {
        self.queries_executed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_rows_read(&self, count: u64) {
        self.rows_read.fetch_add(count, Ordering::Relaxed);
    }

    pub fn add_rows_written(&self, count: u64) {
        self.rows_written.fetch_add(count, Ordering::Relaxed);
    }

    pub fn queries_executed(&self) -> u64 {
        self.queries_executed.load(Ordering::Relaxed)
    }

    pub fn rows_read(&self) -> u64 {
        self.rows_read.load(Ordering::Relaxed)
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }
}