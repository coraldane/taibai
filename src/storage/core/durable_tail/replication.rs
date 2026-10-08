//! Replication bridge: sidecar emission to the replication log and
//! application of replicated raw frames on standbys.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use uuid::Uuid;

use crate::common::error::Result;

use super::api::*;
use super::committer::*;
use super::format::*;
use super::sidecar::*;

/// Replication log for memtable-level mutations: every sidecar this module
/// writes (`.del` / `.upd`) ships verbatim to standbys, whose tail files
/// hold the identical frame history. Set only on replication primaries —
/// a standby never writes sidecars of its own (read-only), so the hook
/// cannot loop.
pub(super) static REPLICATION_LOG: OnceLock<Arc<crate::storage::replication::RepLog>> =
    OnceLock::new();

/// Wire the replication log for sidecar shipping (primary only).
pub fn set_rep_log(log: Arc<crate::storage::replication::RepLog>) {
    let _ = REPLICATION_LOG.set(log);
}

/// Ship one just-written sidecar to standbys. Best-effort enqueue: the
/// event ring persists events in dequeue order, and callers drain the tail
/// committer before writing a sidecar, so the standby applies prior append
/// frames first — exactly the primary's on-disk order.
pub(super) fn emit_sidecar_replication(
    table_dir: &Path,
    id: Uuid,
    is_update: bool,
    sequence: u64,
    payload: &[u8],
) {
    let Some(log) = REPLICATION_LOG.get() else {
        return;
    };
    let db = table_dir
        .parent()
        .and_then(|parent| parent.file_name())
        .map(|name| name.to_string_lossy().into_owned());
    let table = table_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let (Some(db), Some(table)) = (db, table) else {
        return;
    };
    log.enqueue(
        crate::storage::replication::ReplicationEvent::TailMutation {
            db,
            table,
            memtable_id: id,
            is_update,
            sequence,
            payload: payload.to_vec(),
        },
    );
}

/// Standby apply of a `ReplicationEvent::TailMutation`: validate the
/// payload decodes (fail loudly on corruption), then write it as this
/// node's own sidecar generation against the same tail. Replays are
/// idempotent — a duplicate delete removes already-removed positions, a
/// duplicate update rewrites the same values.
pub fn apply_replicated_sidecar(
    table_dir: &Path,
    id: Uuid,
    is_update: bool,
    sequence: u64,
    payload: &[u8],
) -> Result<()> {
    // Decode purely to validate: applying garbage positions or an invalid
    // update set at recovery would corrupt the standby's tail state.
    if is_update {
        decode_update_set(payload)?;
    } else {
        decode_delete_positions(payload)?;
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    // The PRIMARY's sequence names the file: replaying an event (crash
    // before the seq watermark persists) replaces the same generation
    // instead of duplicating it — delete positions are not idempotent on
    // their own. Events apply in primary order, so the borrowed sequences
    // order the standby's generations exactly as the primary's.
    let extension = if is_update { "upd" } else { "del" };
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.{extension}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.{extension}"));
    write_new_synced(&tmp_path, payload)?;
    fs::rename(&tmp_path, &final_path)?;
    sync_dir_best_effort(&tail_dir);
    Ok(())
}

/// Newest live tail file for a memtable, applying the same clear/flush-intent
/// arbitration as crash recovery. Used by the spill path.
/// Standby replication apply: append already-encoded V3 frames to the
/// memtable's tail file, creating the generation on first append. Durable
/// before returning (fsync), matching the standby's strict write semantics.
pub fn append_raw_frames(
    table_dir: &Path,
    id: Uuid,
    partition_key: &str,
    frames: &[u8],
) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    if let Some(path) = live_tail_path(table_dir, id)? {
        let mut file = fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(frames)?;
        file.sync_all()?;
        return Ok(());
    }
    let (mut file, tmp_path, final_path) = create_tail_log_file(&tail_dir, id, partition_key)?;
    file.write_all(frames)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Sum of the `rows` fields across V3 frames in a buffer (header walk only).
pub fn count_v3_frame_rows(frames: &[u8]) -> u64 {
    let mut rows = 0u64;
    let mut offset = 0usize;
    while offset + 32 <= frames.len() {
        let payload_len =
            u64::from_le_bytes(frames[offset..offset + 8].try_into().unwrap()) as usize;
        let frame_rows = u64::from_le_bytes(frames[offset + 8..offset + 16].try_into().unwrap());
        let frame_end = offset + 32 + payload_len + 4;
        if frame_end > frames.len() {
            break;
        }
        rows = rows.saturating_add(frame_rows);
        offset = frame_end;
    }
    rows
}
