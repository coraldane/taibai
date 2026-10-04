//! Startup recovery of spilled memtables plus the tail damage metrics.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::{RecordBatch, Schema};

use super::format::*;
use super::committer::*;
use super::generations::*;
use super::sidecar::*;
use super::api::*;

pub struct RecoveredTail {
    pub id: Uuid,
    pub partition_key: String,
    pub batch: RecordBatch,
}

/// Metadata-only view of a recovered tail: the rows stay on disk until a
/// query, DML, or flush materializes them.
pub struct RecoveredTailIndex {
    pub id: Uuid,
    pub partition_key: String,
    pub rows: u64,
    pub path: PathBuf,
}

/// Materialize a spilled memtable's live content: the tail file plus every
/// delete/update sidecar generation above it, replayed oldest-first — the
/// same arbitration recovery applies. Standby-registered memtables are the
/// raison d'être (their sidecars arrived by replication); the primary's own
/// spill path stays gated to plain tails (`tail_is_plain`), so this is a
/// no-op for it.
pub fn decode_tail_with_sidecars(
    table_dir: &Path,
    id: Uuid,
    schema: &Schema,
) -> Result<Option<(String, RecordBatch)>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut generations = generations_for_id(&tail_dir, id)?;
    let Some((path, sidecars)) =
        live_tail_with_sidecars(&tail_dir, table_dir, id, &mut generations)?
    else {
        return Ok(None);
    };
    let (partition_key, batch) = decode_tail_file(&path, schema)?;
    Ok(Some((partition_key, apply_sidecars(batch, &sidecars)?)))
}

/// Resolve the live tail for one id, collecting the sidecars above it and
/// decoding them oldest-first. A corrupt sidecar is an error (fail closed:
/// applying a partial chain would resurrect rows or drop updates).
pub(super) fn live_tail_with_sidecars(
    tail_dir: &Path,
    table_dir: &Path,
    id: Uuid,
    generations: &mut [Generation],
) -> Result<Option<(PathBuf, Vec<DecodedSidecar>)>> {
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    let mut sidecars_rev = Vec::new();
    match resolve_live_tail(table_dir, generations, true, true, Some(&mut sidecars_rev)) {
        LiveTail::Found(path) => {
            let mut applied = Vec::with_capacity(sidecars_rev.len());
            for file in sidecars_rev.iter().rev() {
                // Statement-guard arbitration: a guarded sidecar belongs to a
                // multi-leg statement whose replacement rows may not be
                // durable. Keep it only when the watermark is satisfied;
                // otherwise drop sidecar and guard — the crash rolled the
                // statement back and the deleted rows must come back.
                if let Some(sequence) =
                    parse_generation(&file.path).map(|(_, generation)| generation.sequence)
                {
                    let guard = guard_path(tail_dir, id, sequence);
                    if guard.exists() {
                        let checks = fs::read(&guard)
                            .map_err(TaibaiError::from)
                            .and_then(|bytes| decode_guard(&bytes))?;
                        if guard_satisfied(table_dir, &checks) {
                            let _ = fs::remove_file(&guard);
                        } else {
                            tracing::warn!(
                                memtable = %id,
                                sequence,
                                "statement guard unsatisfied; dropping guarded sidecar (multi-leg statement rolled back by crash)"
                            );
                            let _ = fs::remove_file(&guard);
                            let _ = fs::remove_file(&file.path);
                            continue;
                        }
                    }
                }
                applied.push(decode_sidecar(file)?);
            }
            Ok(Some((path, applied)))
        }
        // The tail's rows are durable in the target segment: retire the
        // id's remaining generations (sidecars preserved — see
        // `remove_other_generations_ex`) and the satisfied intent itself.
        LiveTail::Suppressed(Suppression::Flush(intent)) => {
            remove_other_generations_ex(tail_dir, id, &intent, true)?;
            remove_file_if_exists(&intent)?;
            Ok(None)
        }
        LiveTail::Suppressed(Suppression::Clear) | LiveTail::Absent => Ok(None),
    }
}

pub(super) fn resolve_live_tail_paths(table_dir: &Path) -> Result<Vec<(Uuid, PathBuf, Vec<DecodedSidecar>)>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut by_id = HashMap::<Uuid, Vec<Generation>>::new();
    for entry in fs::read_dir(&tail_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("tmp") {
            let _ = fs::remove_file(path);
            continue;
        }
        let Some((id, generation)) = parse_generation(&path) else {
            continue;
        };
        // Seed the monotonic high-water so sequences minted after a clock
        // rollback still sort after every generation already on disk.
        TAIL_SEQUENCE_HIGH_WATER.fetch_max(generation.sequence, Ordering::AcqRel);
        by_id.entry(id).or_default().push(generation);
    }

    let mut resolved = Vec::new();
    for (id, mut generations) in by_id {
        let Some((path, sidecars)) =
            live_tail_with_sidecars(&tail_dir, table_dir, id, &mut generations)?
        else {
            continue;
        };
        // Sidecars must survive this cleanup: they are only applied (and
        // then superseded) by the caller, and a crash in between must find
        // them again on the next recovery.
        remove_other_generations_ex(&tail_dir, id, &path, true)?;
        resolved.push((id, path, sidecars));
    }
    sweep_orphan_guards(&tail_dir);
    remove_empty_dir(&tail_dir);
    Ok(resolved)
}

pub fn recover(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTail>> {
    let mut recovered = Vec::new();
    for (id, path, sidecars) in resolve_live_tail_paths(table_dir)? {
        let (partition_key, batch) = decode_tail_file(&path, schema)?;
        // Replay the pending sidecars oldest-first: each one's positions
        // refer to the batch state its statement observed.
        let batch = apply_sidecars(batch, &sidecars)?;
        recovered.push(RecoveredTail {
            id,
            partition_key,
            batch,
        });
    }
    Ok(recovered)
}

/// Recover tail metadata: current-format tails are indexed from frame
/// headers without decoding their payloads.
pub fn recover_index(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTailIndex>> {
    let mut recovered = Vec::new();
    for (id, mut path, sidecars) in resolve_live_tail_paths(table_dir)? {
        // The lazy materialization path reads only the tail file, so pending
        // sidecars must be folded into a fresh full tail HERE. The decode
        // cost is confined to memtables that restarted with unflushed
        // DELETE/UPDATE statements; plain tails stay lazy.
        if !sidecars.is_empty() {
            let (partition_key, batch) = match decode_tail_file(&path, schema) {
                Ok(decoded) => decoded,
                // A layout that predates an ALTER COLUMN (legacy data from an
                // older version) cannot fold: quarantine it — the tail and
                // sidecars stay on disk, the memtable is skipped — instead of
                // failing the whole startup or silently folding garbage.
                Err(error) if is_schema_drift_error(&error) => {
                    report_tail_schema_drift(&path, &error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let batch = apply_sidecars(batch, &sidecars)?;
            persist(table_dir, id, &partition_key, &batch)?;
            path = newest_live_tail_path(&table_dir.join(TAIL_DIR), id)?.ok_or_else(|| {
                TaibaiError::Storage("durable tail vanished after sidecar consolidation".into())
            })?;
        }
        let bytes = fs::read(&path)?;
        if bytes.len() < TAIL_LOG_HEADER_LEN as usize {
            report_tail_discarded(
                &path,
                "file shorter than the log header",
                bytes.len() as u64,
            );
            continue;
        }
        let rows = match scan_tail_log_rows(&bytes) {
            Some(rows) => rows,
            None => {
                // A crash can tear the file inside the variable-length
                // header (magic + key-length present, key bytes missing).
                // Like a sub-header file, nothing was fsynced into it, so
                // treat it as absent instead of failing the whole table's
                // recovery.
                let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                if bytes.len() < 8usize.saturating_add(partition_len) {
                    report_tail_discarded(
                        &path,
                        "torn inside the variable-length header",
                        bytes.len() as u64,
                    );
                    continue;
                }
                return Err(TaibaiError::Storage(
                    "unsupported or corrupt durable tail version".into(),
                ));
            }
        };
        let partition_key = read_tail_log_partition_key(&path)?.ok_or_else(|| {
            TaibaiError::Storage("durable tail is missing its partition key".into())
        })?;
        recovered.push(RecoveredTailIndex {
            id,
            partition_key,
            rows,
            path,
        });
    }
    Ok(recovered)
}

pub(super) static TAILS_DISCARDED: AtomicU64 = AtomicU64::new(0);
pub(super) static TAIL_BYTES_DISCARDED: AtomicU64 = AtomicU64::new(0);
pub(super) static INTENTS_UNREADABLE: AtomicU64 = AtomicU64::new(0);
pub(super) static SEGMENTS_UNREADABLE: AtomicU64 = AtomicU64::new(0);
pub(super) static ROWCOUNT_MISMATCHES: AtomicU64 = AtomicU64::new(0);
pub(super) static TAILS_SCHEMA_DRIFT: AtomicU64 = AtomicU64::new(0);

/// Point-in-time aggregate of durability damage seen since process start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TailDamageSnapshot {
    /// Tail files removed because they could not hold a committed frame
    /// (torn header / truncated body before first fsync).
    pub tails_discarded: u64,
    /// Their combined size in bytes.
    pub tail_bytes_discarded: u64,
    /// Flush-intent markers whose bytes were unreadable (torn marker).
    pub intents_unreadable: u64,
    /// Named flush-target segments that exist but failed to open (damaged
    /// segment — the intent stays unsatisfied, the tail stays authoritative).
    pub segments_unreadable: u64,
    /// Spilled memtables whose decoded row count disagreed with the recorded
    /// one (the decoded content is authoritative and kept).
    pub rowcount_mismatches: u64,
    /// Durable tails whose frame layout predates an ALTER TABLE column
    /// change (data written by an older version, before width-changing DDL
    /// converged hot state). Quarantined: the file stays on disk, the
    /// memtable's rows are unavailable until the table is reloaded.
    pub tails_schema_drift: u64,
}

/// Aggregate of every durability damage event since process start.
pub fn tail_damage_snapshot() -> TailDamageSnapshot {
    TailDamageSnapshot {
        tails_discarded: TAILS_DISCARDED.load(Ordering::Relaxed),
        tail_bytes_discarded: TAIL_BYTES_DISCARDED.load(Ordering::Relaxed),
        intents_unreadable: INTENTS_UNREADABLE.load(Ordering::Relaxed),
        segments_unreadable: SEGMENTS_UNREADABLE.load(Ordering::Relaxed),
        rowcount_mismatches: ROWCOUNT_MISMATCHES.load(Ordering::Relaxed),
        tails_schema_drift: TAILS_SCHEMA_DRIFT.load(Ordering::Relaxed),
    }
}

/// Whether a tail decode error is the schema-drift signature (frames written
/// before an ALTER TABLE column change) rather than plain corruption. Only
/// drift gets the quarantine-and-continue recovery treatment; corruption
/// keeps failing closed.
pub(crate) fn is_schema_drift_error(error: &TaibaiError) -> bool {
    error.to_string().contains("column count mismatch")
}

pub(super) fn report_tail_schema_drift(path: &Path, error: &TaibaiError) {
    TAILS_SCHEMA_DRIFT.fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        tail = %path.display(),
        error = %error,
        "recovery quarantined a durable tail whose layout predates an ALTER TABLE column change (data written by an older version); its rows stay on disk but are unavailable — reload the table from a backup taken before the ALTER"
    );
}

/// Counted for a spilled memtable whose tail cannot materialize against the
/// current catalog because its layout predates an ALTER COLUMN.
pub(crate) fn note_tail_schema_drift(path: &Path, error: &TaibaiError) {
    report_tail_schema_drift(path, error);
}

pub(super) fn report_tail_discarded(path: &Path, reason: &str, bytes: u64) {
    TAILS_DISCARDED.fetch_add(1, Ordering::Relaxed);
    TAIL_BYTES_DISCARDED.fetch_add(bytes, Ordering::Relaxed);
    tracing::warn!(
        tail = %path.display(),
        bytes,
        reason,
        "recovery discarded a tail file that could not hold a committed frame (torn before first fsync); its rows were never visible"
    );
}

pub(super) fn report_intent_unreadable(path: &Path, reason: &str) {
    INTENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        intent = %path.display(),
        reason,
        "flush intent marker unreadable after crash; treated as unsatisfied and the tail stays authoritative"
    );
}

pub(super) fn report_segment_unreadable(path: &Path, error: &TaibaiError) {
    SEGMENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        segment = %path.display(),
        error = %error,
        "flush intent names a segment that failed to open; the intent stays unsatisfied and the tail stays authoritative — the segment may be damaged"
    );
}

/// Counter hook for the spilled-memtable row-count correction in
/// `MemTable::ensure_resident` (which logs its own error with the details).
pub(crate) fn note_rowcount_mismatch() {
    ROWCOUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
}

pub(super) fn tail_file_has_header(path: &Path) -> bool {
    use std::io::Read;
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut header = [0u8; TAIL_LOG_HEADER_LEN as usize];
    if file.read_exact(&mut header).is_err() {
        return false;
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if TailFormat::from_magic(magic).is_none() {
        return false;
    }
    let partition_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
    fs::metadata(path)
        .map(|meta| meta.len() >= TAIL_LOG_HEADER_LEN.saturating_add(partition_len))
        .unwrap_or(false)
}

pub(super) fn discard_unusable_tail(path: &Path, reason: &str) {
    let bytes = fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let _ = fs::remove_file(path);
    report_tail_discarded(path, reason, bytes);
}
