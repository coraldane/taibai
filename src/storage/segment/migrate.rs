//! Segment format migration: verify and upgrade on-disk segments to the
//! format version written by this binary.
//!
//! The reader accepts a range of header versions, so running a new binary
//! against old data works without migration. Migration exists for the
//! opposite direction: after upgrading, `upgrade_segment_file` rewrites each
//! segment's version markers to the current [`SEGMENT_VERSION`] so old
//! binaries fail the header gate loudly (with a pointer to this tool)
//! instead of silently misparsing anything a future format adds.
//!
//! Safety: the upgrade writes a fully patched copy to `<segment>.migrate.tmp`,
//! fsyncs it, and atomically renames it over the original — a crash leaves
//! either the old file or the new file, never a torn one. The copy is
//! deliberately simple (stream + rename) because a version bump must be
//! possible even when the surrounding journal formats changed.
//!
//! Only sealed segments (embedded footer, no `.meta` sidecar) are migrated.
//! Active segments belong to a running server; migrate offline after a
//! clean shutdown, which seals every active writer.

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment::constants::{
    is_supported_segment_version, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION,
};

/// Outcome of one [`upgrade_segment_file`] call.
#[derive(Debug)]
pub struct MigrationOutcome {
    pub path: PathBuf,
    pub from_version: u32,
    pub to_version: u32,
}

/// Read the segment header version without touching the footer.
pub fn segment_header_version(path: &Path) -> Result<u32> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != SEGMENT_MAGIC {
        return Err(TaibaiError::Storage(format!(
            "invalid segment magic: {:08X} ({})",
            magic,
            path.display()
        )));
    }
    Ok(u32::from_le_bytes(header[4..8].try_into().unwrap()))
}

/// True when the header is readable by this binary but not written by it.
pub fn needs_upgrade(path: &Path) -> Result<bool> {
    let version = segment_header_version(path)?;
    Ok(is_supported_segment_version(version) && version != SEGMENT_VERSION)
}

/// Upgrade one segment file's version markers to [`SEGMENT_VERSION`].
///
/// Returns `Ok(None)` when the file is already current (or unsupported and
/// left alone), and `Ok(Some(outcome))` after a successful rewrite. Active
/// segments (with a `.meta` sidecar) are refused: migrate offline after a
/// clean shutdown instead.
pub fn upgrade_segment_file(path: &Path) -> Result<Option<MigrationOutcome>> {
    let from_version = segment_header_version(path)?;
    if !is_supported_segment_version(from_version) {
        return Err(TaibaiError::Storage(format!(
            "segment version {} is not readable by this binary ({})",
            from_version,
            path.display()
        )));
    }
    if from_version == SEGMENT_VERSION {
        return Ok(None);
    }
    let sidecar = sidecar_path(path);
    if sidecar.exists() {
        return Err(TaibaiError::Storage(format!(
            "segment {} still has an active sidecar; shut down the server cleanly (seals all writers) and re-run",
            path.display()
        )));
    }

    let mut source = fs::File::open(path)?;
    let file_len = source.metadata()?.len();
    if file_len < SEGMENT_HEADER_SIZE as u64 + 8 {
        return Err(TaibaiError::Storage(format!(
            "segment file too small to migrate: {} ({})",
            file_len,
            path.display()
        )));
    }

    // Only the header version is patched: it is the reader's gate and is
    // not covered by any checksum. The embedded footer carries its own
    // version field, but it is informational and protected by the
    // whole-footer checksum, so rewriting it would require re-encoding the
    // footer for zero gate value. It stays as a historical record.
    let tmp_path = tmp_path(path);
    let mut target = fs::File::create(&tmp_path)?;
    source.seek(SeekFrom::Start(0))?;
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut copied: u64 = 0;
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let chunk: &mut [u8] = &mut buffer[..read];
        patch_version(chunk, copied, 4, path)?;
        target.write_all(chunk)?;
        copied += read as u64;
    }
    if copied != file_len {
        return Err(TaibaiError::Storage(format!(
            "segment file changed while migrating: expected {} bytes, read {} ({})",
            file_len,
            copied,
            path.display()
        )));
    }
    target.sync_all()?;
    drop(target);
    // Windows cannot replace a file while any handle (including our own
    // source handle and any mmap) is open.
    drop(source);
    super::meta::atomic_replace(&tmp_path, path)?;
    Ok(Some(MigrationOutcome {
        path: path.to_path_buf(),
        from_version,
        to_version: SEGMENT_VERSION,
    }))
}

fn patch_version(
    chunk: &mut [u8],
    chunk_start: u64,
    target_offset: u64,
    path: &Path,
) -> Result<()> {
    let end = target_offset + 4;
    if chunk_start + 4 > target_offset || chunk_start >= end {
        return Ok(());
    }
    let from = (target_offset - chunk_start) as usize;
    if from + 4 > chunk.len() {
        // Version field split across chunk boundaries cannot happen with a
        // 1 MiB buffer and 32-byte-aligned offsets, but refuse rather than
        // guess.
        return Err(TaibaiError::Storage(format!(
            "version field straddles copy buffer boundary ({})",
            path.display()
        )));
    }
    chunk[from..from + 4].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    Ok(())
}

fn sidecar_path(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .map(|name| format!("{}.meta", name.to_string_lossy()))
        .unwrap_or_default();
    segment.with_file_name(name)
}

fn tmp_path(segment: &Path) -> PathBuf {
    let name = segment
        .file_name()
        .map(|name| format!("{}.migrate.tmp", name.to_string_lossy()))
        .unwrap_or_default();
    segment.with_file_name(name)
}

#[cfg(test)]
#[path = "migrate_test.rs"]
mod tests;
