//! Deterministic fault injection for statement-atomicity tests.
//!
//! Mirrors the `force_next_replace_insert_failure` seam: `#[cfg(test)]` +
//! thread-local state, so parallel `cargo test --lib` threads never arm each
//! other's points. One-shot — `should_fail` consumes the arm on first check.
//!
//! Contract for instrumented sites: the injected failure must surface as a
//! plain `Err` from the statement, letting the statement's compensation path
//! (undo/staging/drop) run exactly as a real IO failure would.

#[cfg(test)]
mod imp {
    use std::cell::RefCell;
    use std::collections::HashSet;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum FaultPoint {
        /// Before the second staged image append of an auto-commit UPDATE
        /// (the first durable step whose failure the statement must fully
        /// roll back: staged rows discarded, footers never rewritten).
        UpdateImageAppend,
        /// Inside the transactional UPDATE/DELETE image-append loop, after the
        /// first append succeeded (statement staging must be discarded).
        TxnImageAppend,
    }

    thread_local! {
        static ARMED: RefCell<HashSet<FaultPoint>> = RefCell::new(HashSet::new());
    }

    /// Arm a one-shot failure at `point` on this thread.
    pub fn arm(point: FaultPoint) {
        ARMED.with(|armed| armed.borrow_mut().insert(point));
    }

    /// Whether the caller should fail now; consumes the arm.
    pub fn should_fail(point: FaultPoint) -> bool {
        ARMED.with(|armed| armed.borrow_mut().remove(&point))
    }
}

#[cfg(test)]
pub use imp::{arm, should_fail, FaultPoint};
