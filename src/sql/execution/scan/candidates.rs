//! Shared per-segment row-group candidate enumeration.
//!
//! Every scan consumer (streaming cursor, materialized executor, folding
//! engine) discovers candidate row groups the same way: prune from the
//! sidecar meta snapshot first — no segment file is opened when nothing
//! survives — and fall back to the reader's own footer when the snapshot is
//! absent or its prune checks cannot align onto the footer's column order.
//! The two pruning primitives live in `row_filter` (meta side) and
//! `strategy` (reader side); this module owns the precedence orchestration.
//! The folding engine additionally needs the aligned checks themselves for
//! per-clause vacuity classification, so it calls the primitives directly.

use std::path::Path;
use std::sync::Arc;

use crate::common::error::Result;
use crate::storage::segment::SegmentReader;
use crate::storage::segment_manager::SegmentManager;

use super::spec::ScanSpec;
use super::strategy;

/// One segment's live row groups: the reader plus candidate row-group
/// indices (meta-aligned when the snapshot allowed, reader-pruned otherwise).
pub(super) struct SegmentCandidates {
    pub(super) reader: Arc<SegmentReader>,
    pub(super) indices: Vec<usize>,
}

/// Open `segment` and enumerate its candidate row groups. Returns `Ok(None)`
/// when the metadata prune proves no row group can match — the segment is
/// skipped without being opened. Errors bubble to the caller, which owns the
/// vanished-vs-corrupt distinction (a compaction-removed segment skips, an
/// IO or footer error fails the query).
///
/// `min_row_groups` is the row-group count the scan pinned at snapshot time:
/// the reader must expose at least that many (see
/// `SegmentManager::open_scan_reader_at_least`), otherwise a footer that was
/// still being republished when the scan opened the file would silently hide
/// the rows of the newest group.
pub(super) fn open_segment_with_candidates(
    mgr: &SegmentManager,
    segment: &Path,
    spec: &ScanSpec,
    min_row_groups: usize,
) -> Result<Option<SegmentCandidates>> {
    let meta_candidates = mgr.segment_meta(segment).ok().flatten().and_then(|meta| {
        match super::row_filter::meta_prune_row_groups(&meta, spec) {
            Ok(super::row_filter::MetaPrune::Candidates(indices)) => Some(indices),
            Ok(super::row_filter::MetaPrune::Unaligned) | Err(_) => None,
        }
    });
    if meta_candidates.as_ref().is_some_and(Vec::is_empty) {
        return Ok(None);
    }
    let reader = mgr.open_scan_reader_at_least(segment, spec.reader_txn_id, min_row_groups)?;
    let indices = match meta_candidates {
        Some(indices) => indices,
        None => strategy::prune_segment(spec, &reader)
            .map(|result| result.candidate_indices)
            .unwrap_or_default(),
    };
    let indices = skip_fully_deleted_row_groups(&reader, indices);
    Ok(Some(SegmentCandidates { reader, indices }))
}

/// Drop row groups appended after the scan's snapshot point.
///
/// An active segment keeps its row-group set growing: every memtable flush
/// appends one and republishes the footer. The scan only pins the set that was
/// published under the flush read lock, but it opens the file later, in the
/// decode phase, so its footer may list row groups whose source memtable was
/// already part of the same snapshot. Reading them would return those rows
/// twice — once from the segment and once from the memtable leg.
pub(super) fn clamp_to_snapshot_bound(indices: Vec<usize>, bound: Option<usize>) -> Vec<usize> {
    match bound {
        Some(bound) => indices.into_iter().filter(|&idx| idx < bound).collect(),
        None => indices,
    }
}

/// Drop row groups the delete bitmap already empties: they decode to
/// zero-row batches, which defeats LIMIT-style early termination exactly
/// like an expired row group does. Bitmap-range check only — never touches
/// column data; the read path keeps its own delete filtering for partial
/// groups.
pub(super) fn skip_fully_deleted_row_groups(
    reader: &Arc<SegmentReader>,
    indices: Vec<usize>,
) -> Vec<usize> {
    if reader.delete_bitmap().is_empty() {
        return indices;
    }
    indices
        .into_iter()
        .filter(|&rg_idx| {
            let start = reader.row_group_start_row(rg_idx);
            let rows = reader.row_group_rows(rg_idx);
            !crate::storage::segment::delete_bitmap::row_range_all_deleted(
                reader.delete_bitmap(),
                start,
                rows,
            )
        })
        .collect()
}
