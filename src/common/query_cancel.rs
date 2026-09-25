use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

thread_local! {
    static CURRENT: RefCell<Option<Arc<QueryContext>>> = const { RefCell::new(None) };
}

static NEXT_QUERY_ID: AtomicU64 = AtomicU64::new(1);

/// How far a query has progressed. Once it enters the `Committing` phase the
/// work is no longer safely undoable (WAL append, memtable install, index
/// maintenance), so cancellation checks become no-ops and the query runs to
/// the end of the statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum QueryPhase {
    Planning = 0,
    Executing = 1,
    Committing = 2,
}

impl QueryPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            2 => QueryPhase::Committing,
            1 => QueryPhase::Executing,
            _ => QueryPhase::Planning,
        }
    }
}

/// Explicit cancellation context for one query execution: the disconnect flag
/// shared with the connection task, an optional statement deadline, and the
/// current safe-point phase.
///
/// The ambient (thread-local) copy is installed by the request handler and is
/// what `check()` observes on the executing thread. Parallel lanes (rayon
/// workers) do not inherit thread-locals — capture the context (or just the
/// cancelled flag) *before* entering the closure and call
/// [`QueryContext::check`] inside it.
#[derive(Debug)]
pub struct QueryContext {
    query_id: u64,
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
    phase: AtomicU8,
    /// Human-readable cancellation cause (`KILL QUERY`). Only read on the
    /// error path, so the hot check stays lock-free.
    reason: std::sync::Mutex<Option<String>>,
}

impl QueryContext {
    pub fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::Relaxed),
            cancelled,
            deadline: None,
            phase: AtomicU8::new(QueryPhase::Planning as u8),
            reason: std::sync::Mutex::new(None),
        }
    }

    pub fn with_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn query_id(&self) -> u64 {
        self.query_id
    }

    /// The disconnect flag shared with the connection task. Capture this into
    /// rayon closures that must stop starting new work after cancellation.
    pub fn cancelled_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancelled)
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Cancel and record the cause, which the aborted query reports instead
    /// of the generic disconnect message.
    pub fn cancel_with_reason(&self, reason: String) {
        if let Ok(mut slot) = self.reason.lock() {
            if slot.is_none() {
                *slot = Some(reason);
            }
        }
        self.cancelled.store(true, Ordering::Release);
    }

    fn cancellation_error(&self) -> crate::common::error::TaibaiError {
        let reason = self.reason.lock().ok().and_then(|slot| slot.clone());
        crate::common::error::TaibaiError::Sql(
            reason.unwrap_or_else(|| "query cancelled because the client disconnected".to_string()),
        )
    }

    pub fn phase(&self) -> QueryPhase {
        QueryPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    pub fn enter_phase(&self, phase: QueryPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    /// True when the query should stop. Always false in the `Committing`
    /// phase: an in-flight write must never be abandoned mid-statement.
    pub fn is_cancelled(&self) -> bool {
        if self.phase() == QueryPhase::Committing {
            return false;
        }
        if self.cancelled.load(Ordering::Acquire) {
            return true;
        }
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Safe-point check: `Err` when the query must stop. Near-zero cost when
    /// the flag is clear and no deadline is configured (one atomic load, no
    /// clock read).
    pub fn check(&self) -> crate::common::error::Result<()> {
        if self.phase() == QueryPhase::Committing {
            return Ok(());
        }
        if self.cancelled.load(Ordering::Acquire) {
            return Err(self.cancellation_error());
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(crate::common::error::TaibaiError::Sql(
                "query exceeded the statement timeout".into(),
            ));
        }
        Ok(())
    }
}

/// Mark the ambient query as having entered an irreversible write phase
/// (memtable install, segment rewrite, WAL append). Safe points after this
/// call no longer cancel, so statements finish atomically. No-op when there
/// is no ambient context (embedded usage, background threads).
pub fn enter_committing_phase() {
    if let Some(context) = current() {
        context.enter_phase(QueryPhase::Committing);
    }
}

pub struct QueryCancellationGuard {
    previous: Option<Arc<QueryContext>>,
}

impl QueryCancellationGuard {
    pub fn enter(context: Arc<QueryContext>) -> Self {
        let previous = CURRENT.with(|current| current.replace(Some(context)));
        Self { previous }
    }

    /// Compatibility entry for callers that only hold the raw disconnect
    /// flag; wraps it into a fresh context.
    pub fn enter_flag(cancelled: Arc<AtomicBool>) -> Self {
        Self::enter(Arc::new(QueryContext::new(cancelled)))
    }
}

impl Drop for QueryCancellationGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

/// The ambient context on this thread, if a request handler installed one.
/// Capture the returned handle *before* entering a rayon closure so parallel
/// lanes can observe cancellation.
pub fn current() -> Option<Arc<QueryContext>> {
    CURRENT.with(|current| current.borrow().clone())
}

/// The ambient context's disconnect flag, for capture into parallel closures.
pub fn current_flag() -> Option<Arc<AtomicBool>> {
    current().map(|context| context.cancelled_flag())
}

pub fn is_cancelled() -> bool {
    CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .is_some_and(|context| context.is_cancelled())
    })
}

pub fn check() -> crate::common::error::Result<()> {
    CURRENT.with(|current| match current.borrow().as_ref() {
        Some(context) => context.check(),
        None => Ok(()),
    })
}

#[cfg(test)]
#[path = "query_cancel_test.rs"]
mod tests;
