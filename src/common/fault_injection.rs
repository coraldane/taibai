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
pub use imp::{arm, should_fail, FaultPoint};

#[cfg(test)]
#[path = "fault_injection_imp.rs"]
mod imp;
