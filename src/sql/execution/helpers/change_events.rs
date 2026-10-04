//! Pub/sub change-event buffering and its in-memory budget.

use crate::types::batch::RecordBatch;

pub const CHANGE_EVENT_BUFFER_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// has committed; without a cap, a no-WHERE UPDATE/DELETE on a big table
/// buffered the whole table inside the mutation-lock critical section.
/// Beyond the budget the remaining images are not published — the pubsub
/// channel already has drop-on-overflow semantics and subscribers resync
/// from the change log.
/// Budget gate for one buffered change-event batch: returns true while
/// `buffered + incoming` fits, otherwise warns once per statement (via
/// `*warned`) and returns false so the caller skips buffering.
pub fn change_event_budget_allows(
    buffered_bytes: usize,
    incoming: usize,
    warned: &mut bool,
    what: &str,
    db: &str,
    table: &str,
) -> bool {
    if buffered_bytes.saturating_add(incoming) <= CHANGE_EVENT_BUFFER_BUDGET_BYTES {
        return true;
    }
    if !*warned {
        *warned = true;
        tracing::warn!(
            db,
            table,
            what,
            budget = CHANGE_EVENT_BUFFER_BUDGET_BYTES,
            "change-event buffer budget exceeded; remaining row images are not published (subscribers re-sync from the change log)"
        );
    }
    false
}

/// Buffered change-event row images for one auto-commit DML statement:
/// materializes selected rows out of source batches via
/// [`RecordBatch::take_rows`], enforces the pubsub buffer budget, and
/// accumulates the batches the statement publishes once its durable work
/// has committed. This is the single collection path for DELETE/UPDATE
/// events; the transactional paths defer instead (charging the transaction
/// budget) and publish through the same pubsub chokepoint at COMMIT.
pub(crate) struct ChangeEventBuffer {
    what: &'static str,
    db: String,
    table: String,
    buffered_bytes: usize,
    warned: bool,
    pub batches: Vec<RecordBatch>,
}

impl ChangeEventBuffer {
    pub(crate) fn new(what: &'static str, db: &str, table: &str) -> Self {
        Self {
            what,
            db: db.to_string(),
            table: table.to_string(),
            buffered_bytes: 0,
            warned: false,
            batches: Vec::new(),
        }
    }

    /// Materialize `positions` out of `source` into the buffer (no-op past
    /// the budget; build failures are logged and skipped — a failed event
    /// batch must not fail the statement that already made the change).
    pub(crate) fn push(&mut self, source: &RecordBatch, positions: &[u32]) {
        if positions.is_empty() {
            return;
        }
        match source.take_rows(positions) {
            Ok(batch) => {
                let bytes = batch.estimated_size_bytes();
                if change_event_budget_allows(
                    self.buffered_bytes,
                    bytes,
                    &mut self.warned,
                    self.what,
                    &self.db,
                    &self.table,
                ) {
                    self.buffered_bytes += bytes;
                    self.batches.push(batch);
                }
            }
            Err(error) => tracing::warn!(
                db = %self.db,
                table = %self.table,
                what = self.what,
                error = %error,
                "failed to build change-event batch"
            ),
        }
    }
}
