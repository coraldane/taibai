//! Single staging entry for multi-leg DML statements (UPDATE / REPLACE).
//!
//! Both statements stage their replacement rows into a private transaction
//! and must pass a *durability watermark* of that staging to every deferred
//! journal they write. The watermark is only meaningful when it is collected
//! AFTER the staging writes created the transaction's memtables — collecting
//! it before once shipped an always-empty watermark that silently defeated
//! crash recovery (committed images resurrected their pre-statement rows as
//! duplicates). Statement code therefore cannot construct an
//! [`ImageWatermark`] by hand: the only producers are this module's entry
//! points, whose collection point is structurally tied to the staging act.

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment::reader::ImageDurabilityCheck;

use super::engine::Engine;

/// Durability watermark of one staged image transaction: the memtables the
/// staging created, with the row counts recovery must find durable before
/// the statement's tombstone legs may survive a crash.
#[derive(Debug, Clone, Default)]
pub(crate) struct ImageWatermark {
    checks: Vec<ImageDurabilityCheck>,
}

impl ImageWatermark {
    pub(crate) fn as_slice(&self) -> &[ImageDurabilityCheck] {
        &self.checks
    }
}

/// Outcome of a successful staging: the private transaction id and the
/// watermark of the memtables it created.
pub(crate) struct StagedImages {
    pub(crate) txn_id: u64,
    pub(crate) watermark: ImageWatermark,
}

/// Run `stage`'s writes under the pre-allocated `txn_id` and collect the
/// durability watermark at the structurally-correct point: after the writes
/// created the transaction's memtables. On failure the transaction id is
/// returned alongside the error so the caller can roll the (still
/// invisible) staged rows back.
pub(crate) fn stage_image_transaction<F>(
    engine: &Engine,
    db: &str,
    table: &str,
    txn_id: u64,
    stage: F,
) -> std::result::Result<StagedImages, (u64, TaibaiError)>
where
    F: FnOnce(u64) -> Result<()>,
{
    if let Err(error) = stage(txn_id) {
        return Err((txn_id, error));
    }
    Ok(StagedImages {
        txn_id,
        watermark: collect_txn_watermark(engine, db, table, txn_id),
    })
}

/// Collect the watermark of `txn_id`'s memtables for one table.
///
/// Contract: the transaction's writes for this table must already have run —
/// they are what creates the memtables. Statement code gets this guarantee
/// structurally through [`stage_image_transaction`]; the only other sanctioned
/// caller is transaction COMMIT, where the session's memtables were created
/// by the transaction's earlier statements.
pub(crate) fn collect_txn_watermark(
    engine: &Engine,
    db: &str,
    table: &str,
    txn_id: u64,
) -> ImageWatermark {
    ImageWatermark {
        checks: engine
            .memtable_manager()
            .get_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.txn_id() == txn_id)
            .map(|memtable| ImageDurabilityCheck {
                memtable_id: memtable.id(),
                partition: memtable.partition_key().to_string(),
                expected_rows: memtable.row_count(),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_is_opaque_and_defaults_empty() {
        let watermark = ImageWatermark::default();
        assert!(watermark.as_slice().is_empty());
    }
}
