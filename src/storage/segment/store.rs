use std::path::{Path, PathBuf};

use crate::common::error::Result;
use crate::storage::partition::partition::parse_partition_key;

pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
    super::snapshot::with_directory(table_dir, |_| discover_segments_unlocked(table_dir))
}

pub(crate) fn discover_segments_unlocked(table_dir: &Path) -> Result<Vec<PathBuf>> {
    if !table_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    let mut retired = std::collections::HashSet::new();
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            if name.ends_with(".retire.corrupt") || name.ends_with(".replace.corrupt") {
                return Err(crate::common::error::TaibaiError::Storage(format!(
                    "unresolved corrupt segment retirement journal: {}",
                    path.display()
                )));
            }
            let committed = name.ends_with(".retire")
                || name
                    .strip_suffix(".replace")
                    .is_some_and(|final_name| table_dir.join(final_name).exists());
            if committed {
                let old = crate::storage::segment_manager::read_replace_journal(&path).ok_or_else(
                    || {
                        crate::common::error::TaibaiError::Storage(format!(
                            "unreadable segment retirement journal: {}",
                            path.display()
                        ))
                    },
                )?;
                retired.extend(
                    old.into_iter()
                        .filter_map(|p| p.file_name().map(|n| n.to_os_string())),
                );
            }
            if is_segment_filename(name) {
                // Complete or discard an interrupted delete-footer rewrite
                // before the segment is handed to readers. Without this, a
                // crash mid-rewrite left the segment headerless and scans
                // silently skipped it.
                if let Err(error) =
                    crate::storage::segment::SegmentReader::repair_pending_footer_rewrite(&path)
                {
                    tracing::warn!(
                        file = %path.display(),
                        error = %error,
                        "segment delete-journal repair failed"
                    );
                }
                files.push(path);
            }
        }
    }
    files.retain(|path| !path.file_name().is_some_and(|name| retired.contains(name)));
    files.sort();
    Ok(files)
}

pub fn is_segment_filename(name: &str) -> bool {
    is_regular_segment_filename(name) || parse_partition_key(name).is_some()
}

pub fn is_regular_segment_filename(name: &str) -> bool {
    parse_regular_seq(name).is_some()
}

pub fn regular_segment_filename(seq: u32) -> String {
    format!("seg_{:05}.dat", seq)
}

pub fn partition_segment_filename(partition_key: &str, seq: u32) -> String {
    format!("p{}_{:04}.dat", partition_key, seq)
}

pub fn next_segment_path_by_scan(table_dir: &Path, partition_key: Option<&str>) -> Result<PathBuf> {
    let next_seq = max_segment_seq_by_scan(table_dir, partition_key)? + 1;
    Ok(match partition_key {
        Some(key) => table_dir.join(partition_segment_filename(key, next_seq)),
        None => table_dir.join(regular_segment_filename(next_seq)),
    })
}

/// Highest segment sequence currently present in `table_dir` for the given
/// namespace (regular or one partition). 0 when none exist. This is the
/// on-disk source of truth that the in-process counters must never fall
/// behind — a counter below this value hands out a path that already holds a
/// compaction replacement segment.
pub fn max_segment_seq_by_scan(table_dir: &Path, partition_key: Option<&str>) -> Result<u32> {
    let mut max_seq = 0u32;
    for name in allocated_segment_names(table_dir)? {
        let seq = match partition_key {
            Some(key) => parse_partition_seq(&name, key),
            None => parse_regular_seq(&name),
        };
        if let Some(seq) = seq {
            max_seq = max_seq.max(seq);
        }
    }
    Ok(max_seq)
}

/// Retirement journals reserve every old sequence until cleanup finishes.
/// Otherwise restart could reuse a deleted old name still present in a
/// partially completed journal and later hide/delete the new incarnation.
pub(crate) fn allocated_segment_names(table_dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".retire") || name.ends_with(".replace") {
                if let Some(paths) =
                    crate::storage::segment_manager::read_replace_journal(&entry.path())
                {
                    names.extend(paths.iter().filter_map(|path| {
                        path.file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                    }));
                }
            }
            names.push(name);
        }
    }
    Ok(names)
}

pub fn replacement_tmp_path(final_path: &Path, suffix: &str) -> PathBuf {
    let file_name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.{}.{}", file_name, uuid::Uuid::new_v4(), suffix))
}

pub fn parse_regular_seq(name: &str) -> Option<u32> {
    let seq = name.strip_prefix("seg_")?.strip_suffix(".dat")?;
    seq.parse().ok()
}

pub fn parse_segment_seq(name: &str) -> Option<u32> {
    parse_regular_seq(name).or_else(|| {
        let partition_key = parse_partition_key(name)?;
        parse_partition_seq(name, &partition_key)
    })
}

pub fn parse_partition_seq(name: &str, partition_key: &str) -> Option<u32> {
    let prefix = format!("p{}_", partition_key);
    let seq = name.strip_prefix(&prefix)?.strip_suffix(".dat")?;
    seq.parse().ok()
}

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;
