//! Durable tail ("spilled memtable") storage, split by concern from the
//! former 3,326-line single file: wire format, group committer,
//! generation arbitration, sidecars/guards, flush intents, recovery,
//! replication bridge, and the public API. All `durable_tail::…` paths are
//! preserved via glob re-exports.

mod api;
mod committer;
mod flush_intent;
mod format;
mod generations;
mod recovery;
mod replication;
mod sidecar;

pub use api::*;
// committer has no `pub` items; its pub(crate) types still need the
// `durable_tail::` path for the batch writer, so re-export explicitly.
pub(crate) use committer::{PreparedTailAppend, TailCommitTicket, TailCommitter};
pub use flush_intent::*;
pub use format::*;
pub use recovery::*;
pub use replication::*;
pub use sidecar::*;

#[cfg(test)]
#[path = "../durable_tail_test.rs"]
mod tests;
