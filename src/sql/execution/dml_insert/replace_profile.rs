//! Statement-scoped timing that also reports early returns and rollback paths.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static NEXT_REPLACE_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct ReplaceProfile<'a> {
    replace_id: u64,
    db: &'a str,
    table: &'a str,
    input_rows: usize,
    pub txn_id: u64,
    pub success: bool,
    started: Instant,
    phase_started: Instant,
    phase: &'static str,
    phases_us: Vec<(&'static str, u64)>,
    table_locked_at: Option<Instant>,
    hold_us: u64,
}

impl<'a> ReplaceProfile<'a> {
    pub fn new(db: &'a str, table: &'a str, input_rows: usize) -> Self {
        let now = Instant::now();
        Self {
            replace_id: NEXT_REPLACE_ID.fetch_add(1, Ordering::Relaxed),
            db,
            table,
            input_rows,
            txn_id: 0,
            success: false,
            started: now,
            phase_started: now,
            phase: "validation",
            phases_us: Vec::with_capacity(20),
            table_locked_at: None,
            hold_us: 0,
        }
    }

    pub fn phase(&mut self, next: &'static str) {
        let now = Instant::now();
        self.phases_us.push((
            self.phase,
            now.duration_since(self.phase_started).as_micros() as u64,
        ));
        self.phase = next;
        self.phase_started = now;
        tracing::debug!(
            replace_id = self.replace_id,
            db = self.db,
            table = self.table,
            txn_id = self.txn_id,
            phase = next,
            "REPLACE phase started"
        );
    }

    pub fn table_acquired(&mut self) {
        self.table_locked_at = Some(Instant::now());
        self.phase("location_indexes");
    }

    pub fn table_released(&mut self) {
        if let Some(start) = self.table_locked_at.take() {
            self.hold_us += start.elapsed().as_micros() as u64;
        }
    }
}

impl Drop for ReplaceProfile<'_> {
    fn drop(&mut self) {
        // On a `?` inside the mutation closure, its lock has unwound before
        // this statement guard. Include the failing phase and rollback work.
        self.table_released();
        let last_phase = self.phase;
        self.phase("finished");
        let elapsed = self.started.elapsed();
        let warn_secs = crate::common::config::storage_timeouts()
            .mutation_lock_warn_secs
            .max(1);
        if elapsed.as_secs() >= warn_secs {
            tracing::warn!(replace_id = self.replace_id, db = self.db, table = self.table, txn_id = self.txn_id,
                input_rows = self.input_rows, success = self.success, last_phase,
                total_us = elapsed.as_micros() as u64, table_hold_us = self.hold_us,
                phases_us = ?self.phases_us, "slow REPLACE execution");
        } else {
            tracing::debug!(replace_id = self.replace_id, db = self.db, table = self.table, txn_id = self.txn_id,
                input_rows = self.input_rows, success = self.success, last_phase,
                total_us = elapsed.as_micros() as u64, table_hold_us = self.hold_us,
                phases_us = ?self.phases_us, "REPLACE execution");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[derive(Clone)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);
    impl Write for LogBuffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn replace_profile_reports_failed_phase_on_early_return() {
        let buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let fail = || -> std::result::Result<(), ()> {
                let mut profile = ReplaceProfile::new("main", "failed", 7);
                profile.phase("table_wait");
                Err(())?;
                profile.success = true;
                Ok(())
            };
            assert!(fail().is_err());
        });
        let log = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains("success=false"), "{log}");
        assert!(log.contains("last_phase=\"table_wait\""), "{log}");
        assert!(log.contains("table_hold_us=0"), "{log}");
        assert!(log.contains("replace_id="), "{log}");
    }

    #[test]
    fn replace_profile_warns_on_slow_success_without_counting_wait_as_hold() {
        let buffer = LogBuffer(Arc::new(Mutex::new(Vec::new())));
        let writer = buffer.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let mut profile = ReplaceProfile::new("main", "slow", 3);
            let secs = crate::common::config::storage_timeouts()
                .mutation_lock_warn_secs
                .max(1)
                + 1;
            profile.started -= Duration::from_secs(secs);
            profile.phase_started = profile.started;
            profile.phase = "table_wait";
            profile.table_acquired();
            profile.table_released();
            assert!(profile.hold_us < Duration::from_secs(secs).as_micros() as u64);
            profile.success = true;
        });
        let log = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains("slow REPLACE execution"), "{log}");
        assert!(log.contains("success=true"), "{log}");
        assert!(log.contains("table_wait"), "{log}");
    }
}
