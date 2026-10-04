//! Multi-generation tail arbitration: resolving the live generation per
//! memtable, removing superseded ones, archive/dispose.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::common::error::Result;

use super::api::*;
use super::flush_intent::*;
use super::recovery::*;
use super::sidecar::*;

#[derive(Clone)]
pub(super) enum GenerationKind {
    Tail,
    Clear,
    Flush,
    /// Delete sidecar: positions to remove from the tail generations BELOW
    /// it. Appended by DELETE/REPLACE so removing memtable rows costs
    /// O(affected rows) instead of rewriting the whole tail snapshot (which
    /// made repeated small deletes quadratic in the memtable size).
    Delete,
    /// Update sidecar: matched row positions plus the constant value each
    /// assigned column takes on them. Appended by UPDATE so changing memtable
    /// rows costs O(affected rows) instead of a whole-tail rewrite (UPDATE
    /// never changes row count or row order, so positions stay stable under
    /// it — the property that makes the sidecar chain replayable).
    Update,
}

#[derive(Clone)]
pub(super) struct Generation {
    pub(super) sequence: u64,
    pub(super) path: PathBuf,
    pub(super) kind: GenerationKind,
}

/// Why a generation walk stopped without finding a live tail.
pub(super) enum Suppression {
    /// A clear tombstone: the tail was explicitly dropped.
    Clear,
    /// A satisfied flush intent: the tail's rows are durable in a segment.
    Flush(PathBuf),
}

/// Result of walking one id's generations under the shared arbitration: the
/// newest usable tail wins, clear tombstones and satisfied flush intents
/// suppress everything below, delete/update sidecars are transparent.
pub(super) enum LiveTail {
    Found(PathBuf),
    Suppressed(Suppression),
    Absent,
}

/// Walk `generations` (must be pre-sorted newest→oldest) applying the
/// generation arbitration shared by every live-tail resolution.
///
/// * `validate_tails` discards tail files without a usable header and keeps
///   walking (the mutation paths); read-only views skip the check.
/// * `prune_intents` removes unsatisfied flush intents encountered along
///   the walk (they prove nothing about the tail below); read-only views
///   leave them for the mutation paths.
/// * `sidecars` collects the delete/update sidecar paths seen above the
///   live tail, newest-first.
pub(super) fn resolve_live_tail(
    table_dir: &Path,
    generations: &[Generation],
    validate_tails: bool,
    prune_intents: bool,
    mut sidecars: Option<&mut Vec<SidecarFile>>,
) -> LiveTail {
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => {
                if validate_tails && !tail_file_has_header(&generation.path) {
                    discard_unusable_tail(&generation.path, "header shorter than the log header");
                    continue;
                }
                return LiveTail::Found(generation.path.clone());
            }
            GenerationKind::Clear => return LiveTail::Suppressed(Suppression::Clear),
            GenerationKind::Delete | GenerationKind::Update => {
                if let Some(list) = sidecars.as_deref_mut() {
                    list.push(SidecarFile {
                        kind: match generation.kind {
                            GenerationKind::Delete => SidecarKind::Delete,
                            _ => SidecarKind::Update,
                        },
                        path: generation.path.clone(),
                    });
                }
            }
            GenerationKind::Flush => {
                // A torn or corrupt intent marker proves nothing about the
                // tail: treat it as unsatisfied so the preceding tail stays
                // authoritative.
                let satisfied = match fs::read(&generation.path) {
                    Ok(bytes) => flush_intent_satisfied(table_dir, &bytes).unwrap_or(false),
                    Err(error) => {
                        report_intent_unreadable(&generation.path, &error.to_string());
                        false
                    }
                };
                if satisfied {
                    return LiveTail::Suppressed(Suppression::Flush(generation.path.clone()));
                }
                if prune_intents {
                    let _ = fs::remove_file(&generation.path);
                }
            }
        }
    }
    LiveTail::Absent
}

pub(super) fn parse_generation(path: &Path) -> Option<(Uuid, Generation)> {
    let name = path.file_name()?.to_str()?;
    let mut parts = name.split('.');
    let id = Uuid::parse_str(parts.next()?).ok()?;
    let sequence = parts.next()?.parse::<u64>().ok()?;
    let kind = match parts.next()? {
        "tail" => GenerationKind::Tail,
        "clear" => GenerationKind::Clear,
        "flush" => GenerationKind::Flush,
        "del" => GenerationKind::Delete,
        "upd" => GenerationKind::Update,
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((
        id,
        Generation {
            sequence,
            path: path.to_path_buf(),
            kind,
        },
    ))
}

pub(super) fn generations_for_id(tail_dir: &Path, id: Uuid) -> Result<Vec<Generation>> {
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut generations = Vec::new();
    for entry in fs::read_dir(tail_dir)? {
        let path = entry?.path();
        if let Some((entry_id, generation)) = parse_generation(&path) {
            if entry_id == id {
                generations.push(generation);
            }
        }
    }
    Ok(generations)
}

/// Index the whole tail directory in one `read_dir` pass, grouped by memtable
/// id. Shared by the batched flush/spill helpers so that answering for N
/// memtables costs one scan instead of N.
pub(super) fn generations_by_id(tail_dir: &Path) -> Result<HashMap<Uuid, Vec<Generation>>> {
    let mut by_id = HashMap::new();
    if !tail_dir.exists() {
        return Ok(by_id);
    }
    for entry in fs::read_dir(tail_dir)? {
        let path = entry?.path();
        if let Some((id, generation)) = parse_generation(&path) {
            by_id.entry(id).or_default().push(generation);
        }
    }
    Ok(by_id)
}

pub(super) fn newest_live_tail_path(tail_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    let mut generations = generations_for_id(tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    let table_dir = tail_dir.parent().unwrap_or(tail_dir);
    // Appending insert deltas below a delete sidecar is safe: appends only
    // extend the batch, so recorded positions keep referring to the same
    // prefix — hence deletions are not collected here.
    Ok(
        match resolve_live_tail(table_dir, &generations, true, true, None) {
            LiveTail::Found(path) => Some(path),
            _ => None,
        },
    )
}

pub(super) fn remove_other_generations(tail_dir: &Path, id: Uuid, keep_path: &Path) -> Result<()> {
    remove_other_generations_ex(tail_dir, id, keep_path, false)
}

/// `preserve_sidecars` keeps delete AND update sidecars: callers that
/// already collected their contents but have not applied them yet (or
/// crashed before doing so) must find them again on the next recovery. A
/// fresh full tail written by `persist` supersedes them instead
/// (`preserve_sidecars = false`): the snapshot already contains their effect.
pub(super) fn remove_other_generations_ex(
    tail_dir: &Path,
    id: Uuid,
    keep_path: &Path,
    preserve_sidecars: bool,
) -> Result<()> {
    for generation in generations_for_id(tail_dir, id)? {
        if generation.path != keep_path {
            if matches!(generation.kind, GenerationKind::Tail) {
                archive_or_remove(tail_dir, &generation.path);
            } else if matches!(
                generation.kind,
                GenerationKind::Delete | GenerationKind::Update
            ) && preserve_sidecars
            {
                continue;
            } else {
                remove_file_if_exists(&generation.path)?;
            }
        }
    }
    Ok(())
}

pub(super) fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Dispose of a durable-tail generation file after its rows became durable
/// in a segment. With `storage.tail_archive` enabled, `.tail` files are
/// MOVED into `<data>/wal/<db>/<table>/memtable_tail/` instead of deleted:
/// the archive is the incremental-log source for point-in-time recovery and
/// replication bootstrap. The top-level `wal` directory is reserved (never
/// scanned as a database), so archived logs stay invisible to SQL. Archive
/// failures degrade to deletion with a warning - losing an archive copy is
/// recoverable from a fresh backup, while failing the clear would resurrect
/// rows.
pub(super) fn archive_or_remove(tail_dir: &Path, path: &Path) {
    dispose_tail_file(
        tail_dir,
        path,
        crate::common::config::tail_archive_enabled(),
    );
}

/// Testable core of [`archive_or_remove`]: `archive` selects between the
/// move-to-archive and delete behaviors.
pub(super) fn dispose_tail_file(tail_dir: &Path, path: &Path, archive: bool) {
    let is_tail = path.extension().and_then(|value| value.to_str()) == Some("tail");
    if !is_tail || !archive {
        if let Err(error) = remove_file_if_exists(path) {
            tracing::warn!(file = %path.display(), error = %error, "tail cleanup failed");
        }
        return;
    }
    if let Some(archive_dir) = archive_dir_for(tail_dir) {
        if fs::create_dir_all(&archive_dir).is_ok() {
            let file_name = path
                .file_name()
                .map(|name| name.to_os_string())
                .unwrap_or_default();
            let destination = archive_dir.join(file_name);
            match fs::rename(path, &destination) {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(
                        file = %path.display(),
                        error = %error,
                        "tail archive move failed; deleting instead"
                    );
                }
            }
        }
    }
    let _ = remove_file_if_exists(path);
}

/// `<data>/wal/<db>/<table>/memtable_tail` for a tail dir of
/// `<data>/<db>/<table>/memtable_tail`. `None` when the layout is
/// unexpected (custom data roots in tests).
pub(super) fn archive_dir_for(tail_dir: &Path) -> Option<std::path::PathBuf> {
    let table_dir = tail_dir.parent()?;
    let table_name = table_dir.file_name()?;
    let db_dir = table_dir.parent()?;
    let db_name = db_dir.file_name()?;
    let data_dir = db_dir.parent()?;
    Some(
        data_dir
            .join("wal")
            .join(db_name)
            .join(table_name)
            .join(TAIL_DIR),
    )
}

pub(super) fn remove_empty_dir(path: &Path) {
    if fs::read_dir(path)
        .ok()
        .is_some_and(|mut entries| entries.next().is_none())
    {
        let _ = fs::remove_dir(path);
    }
}
