//! Flush intents: the begin/finish/cancel protocol and satisfaction check
//! used by the flush epoch machinery.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment::SegmentReader;

use super::api::*;
use super::committer::*;
use super::format::*;
use super::generations::*;
use super::recovery::*;

pub struct FlushIntent {
    id: Uuid,
    marker_path: PathBuf,
}

/// Record the destination before a durable tail is copied into a segment.
/// Recovery can then distinguish "rows durable in the segment" from "flush
/// never completed", avoiding duplicate rows. `expected_rows` is the row
/// count the segment footer must reach for the intent to be satisfied; this
/// allows the target to be a shared active segment that also holds rows from
/// other flushes.
pub fn begin_flush(
    table_dir: &Path,
    id: Uuid,
    segment_file_name: &str,
    expected_rows: u64,
) -> Result<FlushIntent> {
    if Path::new(segment_file_name)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(segment_file_name)
    {
        return Err(TaibaiError::Storage(
            "durable tail flush target must be a file name".into(),
        ));
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let marker_path = tail_dir.join(format!("{}.{}.flush", id.simple(), next_sequence()));
    write_new_synced(
        &marker_path,
        &encode_flush_target(segment_file_name, expected_rows),
    )?;
    Ok(FlushIntent { id, marker_path })
}

/// Finish a flush pass by cleaning up every intent with a single directory
/// scan. Per-intent scans made group cleanup quadratic in the number of live
/// tail files. Ordering per intent is preserved exactly: source tails are
/// removed first and intent markers last, so an interrupted batch still fails
/// closed during recovery (the surviving marker points at the durable segment).
pub fn finish_flush_batch(table_dir: &Path, intents: Vec<FlushIntent>) -> Result<()> {
    if intents.is_empty() {
        return Ok(());
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(());
    }
    let keep: HashSet<PathBuf> = intents
        .iter()
        .map(|intent| intent.marker_path.clone())
        .collect();
    let ids: HashSet<Uuid> = intents.iter().map(|intent| intent.id).collect();
    for entry in fs::read_dir(&tail_dir)? {
        let path = entry?.path();
        let Some((id, _)) = parse_generation(&path) else {
            continue;
        };
        if ids.contains(&id) && !keep.contains(&path) {
            archive_or_remove(&tail_dir, &path)?;
        }
    }
    // Persist source removals before retiring the markers that suppress them.
    sync_dir_best_effort(&tail_dir);
    for intent in intents {
        remove_file_if_exists(&intent.marker_path)?;
    }
    // The directory is left in place even when this flush consumed the last
    // live generation: the tail committer mints fresh generations here
    // concurrently, and removing it under them lost the create race as a
    // transient EEXIST/EINVAL (macOS) — failing the append's durability
    // ticket and rerouting acknowledged rows through the segment fallback.
    // Recovery tidies an empty directory instead.
    Ok(())
}

pub fn cancel_flush(intent: FlushIntent) -> Result<()> {
    remove_file_if_exists(&intent.marker_path)
}

pub(super) fn encode_flush_target(segment_file_name: &str, expected_rows: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20 + segment_file_name.len());
    bytes.extend_from_slice(&FLUSH_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(segment_file_name.len() as u32).to_le_bytes());
    bytes.extend_from_slice(segment_file_name.as_bytes());
    bytes.extend_from_slice(&expected_rows.to_le_bytes());
    append_crc(bytes)
}

pub(super) fn decode_flush_target(bytes: &[u8]) -> Result<(String, Option<u64>)> {
    let body = verify_crc(bytes)?;
    if body.len() < 8 {
        return Err(TaibaiError::Storage("flush intent is truncated".into()));
    }
    if u32::from_le_bytes(body[0..4].try_into().unwrap()) != FLUSH_MAGIC {
        return Err(TaibaiError::Storage("invalid flush intent magic".into()));
    }
    let name_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    let name_end = 8usize.saturating_add(name_len);
    let expected_rows = if name_end.saturating_add(8) == body.len() {
        Some(u64::from_le_bytes(
            body[name_end..name_end + 8].try_into().unwrap(),
        ))
    } else {
        return Err(TaibaiError::Storage("invalid flush intent length".into()));
    };
    let name = std::str::from_utf8(&body[8..name_end])
        .map_err(|_| TaibaiError::Storage("invalid flush intent target".into()))?
        .to_string();
    if Path::new(&name)
        .file_name()
        .and_then(|value| value.to_str())
        != Some(name.as_str())
    {
        return Err(TaibaiError::Storage("unsafe flush intent target".into()));
    }
    Ok((name, expected_rows))
}

/// A recorded flush intent proves its tail is durable once the named segment
/// is readable and holds at least the recorded row-count watermark.
pub(super) fn flush_intent_satisfied(table_dir: &Path, bytes: &[u8]) -> Result<bool> {
    let (segment_name, expected_rows) = match decode_flush_target(bytes) {
        Ok(target) => target,
        // A torn marker proves nothing about the tail: unsatisfied, and the
        // preceding tail stays authoritative. Counted so the debris stays
        // visible in aggregate.
        Err(error) => {
            INTENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                error = %error,
                "flush intent marker undecodable; treated as unsatisfied"
            );
            return Ok(false);
        }
    };
    let segment_path = table_dir.join(segment_name);
    if !segment_path.is_file() {
        return Ok(false);
    }
    let reader = match SegmentReader::open(&segment_path) {
        Ok(reader) => reader,
        // The segment exists but is damaged: the intent legitimately stays
        // unsatisfied (the tail stays authoritative), but a damaged segment
        // is worth more than a debug line — surfaces at warn, counted.
        Err(error) => {
            report_segment_unreadable(&segment_path, &error);
            return Ok(false);
        }
    };
    Ok(match expected_rows {
        Some(expected) => reader.row_count() >= expected,
        None => true,
    })
}
