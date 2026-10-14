//! The durable-tail public surface: persist/append, per-table queries,
//! and batched tail-path resolution.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::RecordBatch;

use super::committer::*;
use super::flush_intent::*;
use super::format::*;
use super::generations::*;

pub(super) const TAIL_DIR: &str = "memtable_tail";

pub fn persist(table_dir: &Path, id: Uuid, partition_key: &str, batch: &RecordBatch) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let sequence = next_sequence();
    let payload = encode_tail_log(partition_key, batch)?;
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));

    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    // The rename must be durable BEFORE the older generation is removed,
    // otherwise a crash on a filesystem that has not yet persisted the
    // rename's directory entry leaves no tail at all — every acknowledged
    // row of this memtable would vanish on recovery. Failure to fsync a
    // directory (not supported everywhere) degrades to the previous
    // behavior rather than failing the write.
    sync_dir_best_effort(&tail_dir);
    remove_other_generations(&tail_dir, id, &final_path)?;
    Ok(())
}

/// Best-effort `fsync` of a directory entry so a completed rename survives a
/// crash before the old generation is removed. Unix needs this for
/// durability; Windows either no-ops or refuses, which we tolerate.
pub(super) fn sync_dir_best_effort(dir: &Path) {
    let result = fs::File::open(dir).and_then(|handle| handle.sync_all());
    if let Err(error) = result {
        tracing::debug!(
            dir = %dir.display(),
            error = %error,
            "directory fsync unsupported; rename durability relies on the filesystem journal"
        );
    }
}

/// Durably append only the newly inserted rows to an existing memtable tail.
///
/// The previous implementation rewrote the complete accumulated memtable for
/// every INSERT. Repeated single-row INSERTs therefore produced quadratic I/O
/// and one fsync/rename/delete cycle per ever-growing snapshot. A tail log keeps
/// the same acknowledgement durability while writing only the statement delta.
pub fn append(table_dir: &Path, id: Uuid, partition_key: &str, delta: &RecordBatch) -> Result<()> {
    if delta.num_rows() == 0 {
        return Ok(());
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let existing = newest_live_tail_path(&tail_dir, id)?;
    let frame = encode_tail_frame(delta)?;

    if let Some(path) = existing {
        if read_tail_log_partition_key(&path)?.as_deref() == Some(partition_key) {
            let mut file = fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&frame)?;
            file.sync_all()?;
            return Ok(());
        }

        return Err(TaibaiError::Storage(
            "unsupported durable tail version".into(),
        ));
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    write_new_synced(&tmp_path, &encode_tail_log(partition_key, delta)?)?;
    fs::rename(&tmp_path, &final_path)?;
    sync_dir_best_effort(&tail_dir);
    Ok(())
}

/// True when no sidecars pend above the memtable's live tail. A spill (which
/// hands the memtable a lazily-materialized view of the RAW tail file) must
/// not run while sidecars pend: `ensure_resident` decodes only the tail, so
/// pending delete/update generations would be silently dropped.
///
/// Not expressed through `resolve_live_tail`: a sidecar is terminal here
/// (answers "plain?"), not transparent as in the live-tail searches.
pub fn tail_is_plain(table_dir: &Path, id: Uuid) -> Result<bool> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(true);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return Ok(true),
            GenerationKind::Clear => return Ok(true),
            GenerationKind::Delete | GenerationKind::Update => return Ok(false),
            GenerationKind::Flush => {
                let satisfied = fs::read(&generation.path)
                    .ok()
                    .and_then(|bytes| flush_intent_satisfied(table_dir, &bytes).ok())
                    .unwrap_or(false);
                if satisfied {
                    return Ok(true);
                }
                let _ = fs::remove_file(&generation.path);
            }
        }
    }
    Ok(true)
}

/// Persist a tombstone before removing a tail. Keeping the tombstone makes a
/// failed cleanup fail closed: an older tail cannot be resurrected on restart.
pub fn clear(table_dir: &Path, id: Uuid) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(());
    }
    let sequence = next_sequence();
    let marker = tail_dir.join(format!("{}.{}.clear", id.simple(), sequence));
    write_new_synced(&marker, &TAIL_MAGIC.to_le_bytes())?;
    // Make the marker's directory entry durable before the tails it
    // tombstones are removed (same ordering rule as `persist`).
    sync_dir_best_effort(&tail_dir);
    remove_other_generations(&tail_dir, id, &marker)?;
    Ok(())
}

pub fn has_tail(table_dir: &Path, id: Uuid) -> bool {
    let Ok(mut generations) = generations_for_id(&table_dir.join(TAIL_DIR), id) else {
        return false;
    };
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    // Read-only view: no header validation, no intent pruning.
    matches!(
        resolve_live_tail(table_dir, &generations, false, false, None),
        LiveTail::Found(_)
    )
}

pub fn generation_file_count(table_dir: &Path) -> Result<usize> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(tail_dir)? {
        if parse_generation(&entry?.path()).is_some() {
            count += 1;
        }
    }
    Ok(count)
}

/// Count generations that a memtable flush can reclaim. Clear tombstones
/// deliberately survive cleanup to prevent older tails from resurrecting;
/// flushing unrelated live memtables cannot remove these markers.
pub(crate) fn flushable_generation_file_count(table_dir: &Path) -> Result<usize> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(0);
    }
    let mut count = 0;
    for entry in fs::read_dir(tail_dir)? {
        if let Some((_, generation)) = parse_generation(&entry?.path()) {
            if !matches!(generation.kind, GenerationKind::Clear) {
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Histogram of tail generation kinds in the table's tail dir. Used by the
/// durable-tail pressure path: when a forced flush fails to bring the
/// generation count under the cap, the kind distribution identifies which
/// subsystem keeps appending (live-memtable spill vs orphaned flush intents
/// vs delete sidecars), turning the next production day into a diagnosis
/// instead of a mystery.
pub(crate) fn generation_kind_histogram(table_dir: &Path) -> Result<Vec<(&'static str, usize)>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut histogram = vec![
        ("tail", 0usize),
        ("clear", 0usize),
        ("flush", 0usize),
        ("del", 0usize),
        ("upd", 0usize),
    ];
    if !tail_dir.exists() {
        return Ok(histogram);
    }
    for entry in fs::read_dir(tail_dir)? {
        if let Some((_, generation)) = parse_generation(&entry?.path()) {
            let slot = match generation.kind {
                GenerationKind::Tail => 0,
                GenerationKind::Clear => 1,
                GenerationKind::Flush => 2,
                GenerationKind::Delete => 3,
                GenerationKind::Update => 4,
            };
            histogram[slot].1 += 1;
        }
    }
    Ok(histogram)
}

/// Batch form of `has_tail`: a single directory scan answers for every
/// requested id, applying the same generation arbitration (newest tail wins,
/// clear tombstones, flush intents). With thousands of live tails, per-id
/// scans made flush grouping quadratic in the size of the tail directory.
pub fn has_tail_batch(table_dir: &Path, ids: &[Uuid]) -> Result<HashMap<Uuid, bool>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut by_id = generations_by_id(&tail_dir)?;
    let mut answers = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(mut generations) = by_id.remove(id) else {
            answers.insert(*id, false);
            continue;
        };
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        // Read-only view, same arbitration as `has_tail`.
        let has = matches!(
            resolve_live_tail(table_dir, &generations, false, false, None),
            LiveTail::Found(_)
        );
        answers.insert(*id, has);
    }
    Ok(answers)
}

/// Batch form of `live_tail_path`: one directory scan resolves the newest
/// live tail file of every requested id with the same arbitration rules.
pub fn live_tail_paths_batch(
    table_dir: &Path,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Option<PathBuf>>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut by_id = generations_by_id(&tail_dir)?;
    let mut answers = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(mut generations) = by_id.remove(id) else {
            answers.insert(*id, None);
            continue;
        };
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        // Read-only view, same arbitration as `live_tail_path`.
        let live = match resolve_live_tail(table_dir, &generations, false, false, None) {
            LiveTail::Found(path) => Some(path),
            _ => None,
        };
        answers.insert(*id, live);
    }
    Ok(answers)
}

pub fn live_tail_path(table_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    newest_live_tail_path(&table_dir.join(TAIL_DIR), id)
}

/// Row count of a current-format tail log without decompressing any frame.
/// Returns `None` when the file is invalid or corrupt.
pub fn tail_log_rows(path: &Path) -> Result<Option<u64>> {
    Ok(scan_tail_log_rows(&fs::read(path)?))
}

/// LSN of the last complete frame in a current-format tail log, again by
/// walking frame headers only (LSNs are allocated in append order, so the
/// last frame carries the maximum). `Ok(None)` for an invalid/corrupt file or
/// a V2 (LSN-less) log.
///
/// The replication applier uses this to make replay idempotent: its applied
/// watermark persists only AFTER a tail append is fsynced, so a standby crash
/// in that window replays the event — and without this check the same frames
/// would be appended a second time, doubling the tail's rows.
pub fn tail_last_lsn(path: &Path) -> Result<Option<u64>> {
    // A tail removed concurrently (compaction/flush) reads as "no lsn" —
    // the applier treats that as not-yet-applied and re-appends.
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let format = match bytes.len() {
        len if len < 8 => return Ok(None),
        _ => match TailFormat::from_magic(u32::from_le_bytes(bytes[0..4].try_into().unwrap())) {
            Some(format) => format,
            None => return Ok(None),
        },
    };
    if !format.has_commit_metadata() {
        // V2 frames carry no LSN: nothing to dedup on.
        return Ok(None);
    }
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return Ok(None);
    }
    let mut last_lsn = None;
    let frame_min_len = format.frame_header_len + 4;
    while offset < bytes.len() {
        // Mirror decode_tail_log / scan_tail_log_rows: a crash-truncated
        // final frame ends the valid prefix instead of failing the scan.
        if bytes.len().saturating_sub(offset) < frame_min_len {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + format.frame_header_len;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return Ok(None);
        }
        last_lsn = Some(u64::from_le_bytes(
            bytes[offset + 16..offset + 24].try_into().unwrap(),
        ));
        offset = frame_end;
    }
    Ok(last_lsn)
}

/// Durable rows of one memtable's live tail generations — the effect
/// watermark that statement-journal recovery verifies (header walk only,
/// frames are never decoded; a crash-truncated final frame ends the valid
/// prefix, so page-cache-only frames count as absent, exactly the crash
/// semantics the check wants). A memtable that never spilled or flushed owns
/// exactly one generation; taking the MAX across generations avoids
/// double-counting rows a flush intent already retired into a segment.
pub(crate) fn durable_tail_rows_for_memtable(table_dir: &Path, id: Uuid) -> u64 {
    let Ok(entries) = fs::read_dir(table_dir.join(TAIL_DIR)) else {
        return 0;
    };
    let prefix = format!("{}.", id.simple());
    let mut best = 0u64;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        // Only finalized tails (plain .tail); pending-rename tmp files are not
        // durable yet, and sidecar generations never match the suffix.
        if !name.starts_with(&prefix) {
            continue;
        }
        if name.ends_with(".tail") {
            if let Ok(bytes) = fs::read(entry.path()) {
                if let Some(rows) = scan_tail_log_rows(&bytes) {
                    best = best.max(rows);
                }
            }
        } else if name.ends_with(".flush") {
            // A satisfied flush intent retires the tail into a segment: the
            // recorded row-count watermark proves those rows durable even
            // though the tail file is gone. Without this, a flush that raced
            // a statement window would fail every watermark check and
            // recovery would roll back a correctly committed statement.
            if let Ok(bytes) = fs::read(entry.path()) {
                if let Ok((_, Some(expected))) = decode_flush_target(&bytes) {
                    if flush_intent_satisfied(table_dir, &bytes).unwrap_or(false) {
                        best = best.max(expected);
                    }
                }
            }
        }
    }
    best
}
