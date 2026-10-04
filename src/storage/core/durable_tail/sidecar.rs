//! Delete/update sidecars and multi-statement guards (the `.del`/`.upd`/
//! guard files layered on top of plain tail logs).

use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::dml_update::rebuild_update_column;
use crate::storage::segment::reader::ImageDurabilityCheck;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::format::*;
use super::committer::*;
use super::generations::*;
use super::replication::*;
use super::api::*;

/// The two sidecar kinds that can sit above a live tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SidecarKind {
    Delete,
    Update,
}

/// A sidecar generation seen above the live tail, in walk order
/// (newest-first). Callers decode and apply them oldest-first.
#[derive(Clone)]
pub(super) struct SidecarFile {
    pub(super) kind: SidecarKind,
    pub(super) path: PathBuf,
}

pub(super) const DELETE_GEN_MAGIC: u32 = 0x5441_4944; // "TAID"

pub(super) fn encode_delete_positions(positions: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + positions.len() * 4);
    bytes.extend_from_slice(&DELETE_GEN_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(positions.len() as u32).to_le_bytes());
    for position in positions {
        bytes.extend_from_slice(&position.to_le_bytes());
    }
    append_crc(bytes)
}

pub(super) fn decode_delete_positions(bytes: &[u8]) -> Result<Vec<u32>> {
    let body = verify_crc(bytes)?;
    if body.len() < 8 || u32::from_le_bytes(body[0..4].try_into().unwrap()) != DELETE_GEN_MAGIC {
        return Err(TaibaiError::Storage(
            "durable tail delete sidecar is corrupt".into(),
        ));
    }
    let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    if body.len() != 8 + count * 4 {
        return Err(TaibaiError::Storage(
            "durable tail delete sidecar length mismatch".into(),
        ));
    }
    Ok(body[8..]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

pub(super) const UPDATE_GEN_MAGIC: u32 = 0x5441_4955; // "TAIU"

/// One memtable UPDATE's durable image: the ascending tail-row positions a
/// statement matched, and the constant value each assigned column takes on
/// those rows (UPDATE assignments are statement constants, evaluated without
/// row context). Applied to the tail generations BELOW the sidecar, replayed
/// oldest sidecar first, exactly like the delete sidecars it shares the
/// generation arbitration with.
#[derive(Debug, Clone)]
pub(crate) struct TailUpdateSet {
    /// `(physical column index, new value)` pairs in the tail batch's column
    /// space, ascending by column index.
    pub assigned: Vec<(usize, ScalarValue)>,
    /// Ascending, deduplicated row positions in the sidecar's coordinate
    /// space: the batch state produced by applying every OLDER generation.
    pub positions: Vec<u32>,
}

pub(super) fn encode_update_set(set: &TailUpdateSet) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(16 + set.assigned.len() * 24 + set.positions.len() * 4);
    bytes.extend_from_slice(&UPDATE_GEN_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(set.assigned.len() as u32).to_le_bytes());
    for (col_idx, value) in &set.assigned {
        let col_idx = u32::try_from(*col_idx).map_err(|_| {
            TaibaiError::Storage("update sidecar column index overflows u32".into())
        })?;
        bytes.extend_from_slice(&col_idx.to_le_bytes());
        encode_scalar_cell(&mut bytes, value);
    }
    bytes.extend_from_slice(&(set.positions.len() as u32).to_le_bytes());
    for position in &set.positions {
        bytes.extend_from_slice(&position.to_le_bytes());
    }
    Ok(append_crc(bytes))
}

pub(super) fn decode_update_set(raw: &[u8]) -> Result<TailUpdateSet> {
    let body = verify_crc(raw)?;
    if body.len() < 8 || u32::from_le_bytes(body[0..4].try_into().unwrap()) != UPDATE_GEN_MAGIC {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is corrupt".into(),
        ));
    }
    let mut offset = 4usize;
    let assigned_count = read_le_u32(body, &mut offset)? as usize;
    let mut assigned = Vec::with_capacity(assigned_count);
    for _ in 0..assigned_count {
        let col_idx = read_le_u32(body, &mut offset)? as usize;
        let value = decode_scalar_cell(body, &mut offset)?;
        assigned.push((col_idx, value));
    }
    let position_count = read_le_u32(body, &mut offset)? as usize;
    if body.len() != offset + position_count * 4 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar length mismatch".into(),
        ));
    }
    let positions = body[offset..]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    Ok(TailUpdateSet {
        assigned,
        positions,
    })
}

/// Durably record row deletions for a memtable without rewriting its tail.
///
/// The sidecar generation (`<id>.<seq>.del`) applies to the tail below it:
/// recovery loads the tail, then removes the recorded positions oldest
/// sidecar first. Appends to the tail only extend it, so positions recorded
/// against an earlier prefix stay valid. Returns `false` when the memtable
/// has no live tail (caller falls back to a full `persist`).
pub fn delete_rows(table_dir: &Path, id: Uuid, positions: &[u32]) -> Result<bool> {
    delete_rows_impl(table_dir, id, positions, None)
}

/// Guarded variant for multi-leg statements (REPLACE's memtable delete leg,
/// transaction COMMIT's materialization): the sidecar is written under a
/// statement guard naming the statement's image watermark, so recovery drops
/// it while the images are not durable — the crash rolled the statement
/// back — instead of keeping a delete whose replacement rows were lost.
pub fn delete_rows_guarded(
    table_dir: &Path,
    id: Uuid,
    positions: &[u32],
    watermark: &[ImageDurabilityCheck],
) -> Result<bool> {
    delete_rows_impl(table_dir, id, positions, Some(watermark))
}

pub(super) fn delete_rows_impl(
    table_dir: &Path,
    id: Uuid,
    positions: &[u32],
    watermark: Option<&[ImageDurabilityCheck]>,
) -> Result<bool> {
    if positions.is_empty() {
        return Ok(true);
    }
    let mut positions = positions.to_vec();
    positions.sort_unstable();
    positions.dedup();
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(false);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    if !matches!(
        resolve_live_tail(table_dir, &generations, true, true, None),
        LiveTail::Found(_)
    ) {
        return Ok(false);
    }
    let sequence = next_sequence();
    // The guard must be durable BEFORE the sidecar: a crash between the two
    // leaves an orphan guard (swept by recovery) and no delete applied,
    // while the reverse order could leave an unguarded delete of a
    // statement whose images were lost.
    if let Some(watermark) = watermark {
        write_guard(&tail_dir, id, sequence, watermark)?;
    }
    let tmp_path = tail_dir.join(format!(".{}.{}.del.tmp", id.simple(), sequence));
    let final_path = tail_dir.join(format!("{}.{}.del", id.simple(), sequence));
    let payload = encode_delete_positions(&positions);
    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    // The sidecar must be durable before the statement acks the delete;
    // otherwise a crash restores rows the caller believes are gone.
    sync_dir_best_effort(&tail_dir);
    emit_sidecar_replication(table_dir, id, false, sequence, &payload);
    Ok(true)
}

// ---------------------------------------------------------------------------
// Statement guards: crash arbitration for multi-leg memtable writes
//
// A multi-leg statement (REPLACE, transaction COMMIT) deletes rows from
// committed memtables while its replacement rows are still becoming durable
// elsewhere. An unguarded `.del`/`.upd` sidecar is irreversible at recovery:
// a crash in the window kept the delete and lost the images. The guard
// (`<id>.<seq>.guard`) records the statement's image watermark next to the
// sidecar; recovery keeps the sidecar only when the watermark is durable,
// otherwise both are dropped and the deleted rows come back — the same
// roll-forward/roll-back arbitration the segment `.del` V02 journal applies
// to footer rewrites.
// ---------------------------------------------------------------------------

pub(super) const GUARD_MAGIC: u32 = 0x5442_4752; // "TBGR"

/// `<id>.<seq>.guard` — deliberately not a generation kind, so generation
/// arbitration never sees it; it is found only by name.
pub(super) fn guard_path(tail_dir: &Path, id: Uuid, sequence: u64) -> PathBuf {
    tail_dir.join(format!("{}.{}.guard", id.simple(), sequence))
}

/// Owner and sequence of a guard file, mirroring parse_generation's naming.
pub(super) fn parse_guard_path(path: &Path) -> Option<(Uuid, u64)> {
    let name = path.file_name()?.to_str()?;
    let mut parts = name.split('.');
    let id = Uuid::parse_str(parts.next()?).ok()?;
    let sequence = parts.next()?.parse::<u64>().ok()?;
    if parts.next()? != "guard" || parts.next().is_some() {
        return None;
    }
    Some((id, sequence))
}

pub(super) fn encode_guard(watermark: &[ImageDurabilityCheck]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + watermark.len() * 40);
    bytes.extend_from_slice(&GUARD_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(watermark.len() as u32).to_le_bytes());
    for check in watermark {
        bytes.extend_from_slice(check.memtable_id.as_bytes());
        bytes.extend_from_slice(&(check.partition.len() as u32).to_le_bytes());
        bytes.extend_from_slice(check.partition.as_bytes());
        bytes.extend_from_slice(&check.expected_rows.to_le_bytes());
    }
    append_crc(bytes)
}

pub(super) fn decode_guard(bytes: &[u8]) -> Result<Vec<ImageDurabilityCheck>> {
    let corrupt =
        |detail: &str| TaibaiError::Storage(format!("durable tail guard is corrupt: {detail}"));
    let body = verify_crc(bytes)?;
    if body.len() < 8 || u32::from_le_bytes(body[0..4].try_into().unwrap()) != GUARD_MAGIC {
        return Err(corrupt("bad magic"));
    }
    let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize;
    let mut checks = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 16 > body.len() {
            return Err(corrupt("truncated memtable id"));
        }
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&body[offset..offset + 16]);
        offset += 16;
        if offset + 4 > body.len() {
            return Err(corrupt("truncated partition length"));
        }
        let partition_len =
            u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + partition_len + 8 > body.len() {
            return Err(corrupt("truncated partition"));
        }
        let partition = String::from_utf8(body[offset..offset + partition_len].to_vec())
            .map_err(|_| corrupt("invalid partition utf-8"))?;
        offset += partition_len;
        let expected_rows = u64::from_le_bytes(body[offset..offset + 8].try_into().unwrap());
        offset += 8;
        checks.push(ImageDurabilityCheck {
            memtable_id: Uuid::from_bytes(id_bytes),
            partition,
            expected_rows,
        });
    }
    if offset != body.len() {
        return Err(corrupt("trailing bytes"));
    }
    Ok(checks)
}

/// Write the statement guard for `sequence`. An empty watermark (single-leg
/// statement) needs no arbitration and writes nothing.
pub(super) fn write_guard(
    tail_dir: &Path,
    id: Uuid,
    sequence: u64,
    watermark: &[ImageDurabilityCheck],
) -> Result<()> {
    if watermark.is_empty() {
        return Ok(());
    }
    let final_path = guard_path(tail_dir, id, sequence);
    let tmp_path = tail_dir.join(format!(".{}.{}.guard.tmp", id.simple(), sequence));
    write_new_synced(&tmp_path, &encode_guard(watermark))?;
    fs::rename(&tmp_path, &final_path)?;
    sync_dir_best_effort(tail_dir);
    Ok(())
}

/// A guard's watermark is satisfied when every referenced image memtable is
/// durable — tail rows frame-walked up to the recorded count, or retired
/// into a segment by a satisfied flush intent.
pub(super) fn guard_satisfied(table_dir: &Path, checks: &[ImageDurabilityCheck]) -> bool {
    checks.iter().all(|check| {
        durable_tail_rows_for_memtable(table_dir, check.memtable_id) >= check.expected_rows
    })
}

/// Remove guards whose sidecar no longer exists: a crash between the guard
/// and sidecar writes (or a sidecar already consumed by arbitration) leaves
/// a guard that protects nothing. Without this sweep such an orphan would
/// gate the memtable's flush forever.
pub(super) fn sweep_orphan_guards(tail_dir: &Path) {
    let Ok(entries) = fs::read_dir(tail_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some((guard_id, sequence)) = parse_guard_path(&path) else {
            continue;
        };
        if !tail_dir
            .join(format!("{}.{}.del", guard_id.simple(), sequence))
            .exists()
            && !tail_dir
                .join(format!("{}.{}.upd", guard_id.simple(), sequence))
                .exists()
        {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Remove every statement guard of `id`: the caller's images are durable, so
/// the guarded sidecars are unconditionally correct now. Best-effort — a
/// leftover guard is judged (and removed) by the next recovery.
pub fn clear_statement_guards(table_dir: &Path, id: Uuid) {
    let Ok(entries) = fs::read_dir(table_dir.join(TAIL_DIR)) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if parse_guard_path(&path).is_some_and(|(guard_id, _)| guard_id == id) {
            let _ = fs::remove_file(&path);
        }
    }
}

/// True while a statement guard of `id` is pending. The flusher must not
/// retire this memtable's tail into a segment while a multi-leg statement
/// can still be rolled back: a flushed delete is irreversible, and the
/// guard's recovery arbitration would have nothing left to arbitrate.
pub fn memtable_has_statement_guard(table_dir: &Path, id: Uuid) -> bool {
    let Ok(entries) = fs::read_dir(table_dir.join(TAIL_DIR)) else {
        return false;
    };
    entries
        .flatten()
        .any(|entry| parse_guard_path(&entry.path()).is_some_and(|(guard_id, _)| guard_id == id))
}

/// Durably record a memtable UPDATE without rewriting its tail.
///
/// The sidecar generation (`<id>.<seq>.upd`) applies to the tail generations
/// BELOW it: recovery decodes the tail and replays every generation
/// oldest-first — appends extend the tail, updates overwrite values in place
/// at stable positions, deletes remove rows. Each sidecar's positions refer
/// to the batch state its statement observed, which the ordered replay
/// reproduces exactly. Repeated small UPDATEs on a large memtable therefore
/// cost O(affected rows) per statement instead of a whole-tail rewrite
/// (quadratic). Returns `false` when the memtable has no live tail (caller
/// falls back to a full `persist` of the updated batch).
pub fn update_rows(
    table_dir: &Path,
    id: Uuid,
    positions: &[u32],
    assigned: &[(usize, ScalarValue)],
) -> Result<bool> {
    if positions.is_empty() || assigned.is_empty() {
        return Ok(true);
    }
    let mut positions = positions.to_vec();
    positions.sort_unstable();
    positions.dedup();
    // Later assignments to the same column never win (the statement planner
    // already keeps only the first); normalize to ascending column order so
    // the on-disk layout is canonical.
    let mut assigned = assigned.to_vec();
    assigned.sort_by_key(|(col_idx, _)| *col_idx);
    assigned.dedup_by_key(|(col_idx, _)| *col_idx);
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(false);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    if !matches!(
        resolve_live_tail(table_dir, &generations, true, true, None),
        LiveTail::Found(_)
    ) {
        return Ok(false);
    }
    let set = TailUpdateSet {
        assigned,
        positions,
    };
    let sequence = next_sequence();
    let tmp_path = tail_dir.join(format!(".{}.{}.upd.tmp", id.simple(), sequence));
    let final_path = tail_dir.join(format!("{}.{}.upd", id.simple(), sequence));
    let payload = encode_update_set(&set)?;
    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    // The sidecar must be durable before the statement acks the UPDATE;
    // otherwise a crash restores the pre-update values.
    sync_dir_best_effort(&tail_dir);
    emit_sidecar_replication(table_dir, id, true, sequence, &payload);
    Ok(true)
}

/// Apply generation arbitration (newest tail wins, clear tombstones, flush
/// intents) and return the live tail file for every recoverable memtable id.
/// Shared by `recover` (eager decode) and `recover_index` (metadata only);
/// performs the same on-disk cleanup as before.
/// One pending sidecar decoded from disk, in application order.
pub(super) enum DecodedSidecar {
    Delete(Vec<u32>),
    Update(TailUpdateSet),
}

pub(super) fn decode_sidecar(file: &SidecarFile) -> Result<DecodedSidecar> {
    let bytes = fs::read(&file.path)?;
    match file.kind {
        SidecarKind::Delete => Ok(DecodedSidecar::Delete(decode_delete_positions(&bytes)?)),
        SidecarKind::Update => Ok(DecodedSidecar::Update(decode_update_set(&bytes)?)),
    }
}

/// Replay decoded sidecars (oldest-first) onto a decoded tail batch: deletes
/// remove rows, updates overwrite values in place. Each sidecar's positions
/// refer to the batch state produced by the generations before it, so the
/// ordered replay reproduces exactly what the statements left in memory.
pub(super) fn apply_sidecars(mut batch: RecordBatch, sidecars: &[DecodedSidecar]) -> Result<RecordBatch> {
    for sidecar in sidecars {
        batch = match sidecar {
            DecodedSidecar::Delete(positions) => filter_out_positions(&batch, positions)?,
            DecodedSidecar::Update(set) => apply_update_set(&batch, set)?,
        };
    }
    Ok(batch)
}

/// Apply one update sidecar to a decoded tail batch: matched rows take the
/// assigned constant values, every other row is untouched. Uses the same
/// image constructor as the UPDATE statement itself so the recovered batch
/// cannot diverge from the one the statement swapped into memory.
pub(super) fn apply_update_set(batch: &RecordBatch, set: &TailUpdateSet) -> Result<RecordBatch> {
    let mut mask = vec![false; batch.num_rows()];
    for &position in &set.positions {
        let position = position as usize;
        if position >= batch.num_rows() {
            return Err(TaibaiError::Storage(
                "durable tail update sidecar references a row outside the tail".into(),
            ));
        }
        mask[position] = true;
    }
    let all_rows: Vec<u32> = (0..batch.num_rows() as u32).collect();
    let mut columns = batch.columns.clone();
    for (col_idx, value) in &set.assigned {
        if *col_idx >= columns.len() {
            return Err(TaibaiError::Storage(format!(
                "durable tail update sidecar references column {col_idx} outside the tail"
            )));
        }
        if columns[*col_idx].data_type() != value.data_type() {
            return Err(TaibaiError::Storage(
                "durable tail update sidecar value type does not match the tail column".into(),
            ));
        }
        columns[*col_idx] =
            rebuild_update_column(&columns[*col_idx], Some(value), &all_rows, &mask)?;
    }
    RecordBatch::try_new(batch.schema.clone(), columns)
}
