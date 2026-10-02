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
pub(super) fn open_segment_with_candidates(
    mgr: &SegmentManager,
    segment: &Path,
    spec: &ScanSpec,
) -> Result<Option<SegmentCandidates>> {
    let meta_candidates = mgr
        .segment_meta(segment)
        .ok()
        .flatten()
        .and_then(|meta| match super::row_filter::meta_prune_row_groups(&meta, spec) {
            Ok(super::row_filter::MetaPrune::Candidates(indices)) => Some(indices),
            Ok(super::row_filter::MetaPrune::Unaligned) | Err(_) => None,
        });
    if meta_candidates.as_ref().is_some_and(Vec::is_empty) {
        return Ok(None);
    }
    let reader = mgr.open_scan_reader(segment, spec.reader_txn_id)?;
    let indices = match meta_candidates {
        Some(indices) => indices,
        None => strategy::prune_segment(spec, &reader)
            .map(|result| result.candidate_indices)
            .unwrap_or_default(),
    };
    Ok(Some(SegmentCandidates { reader, indices }))
}
