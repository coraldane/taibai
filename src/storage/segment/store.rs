use std::path::{Path, PathBuf};

use crate::common::error::Result;
use crate::storage::partition::partition::parse_partition_key;

pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
    if !table_dir.exists() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    for entry in std::fs::read_dir(table_dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            if is_segment_filename(name) {
                files.push(path);
            }
        }
    }
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
    let mut max_seq = 0u32;
    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let seq = match partition_key {
                Some(key) => parse_partition_seq(&name, key),
                None => parse_regular_seq(&name),
            };
            if let Some(seq) = seq {
                max_seq = max_seq.max(seq);
            }
        }
    }

    let next_seq = max_seq + 1;
    Ok(match partition_key {
        Some(key) => table_dir.join(partition_segment_filename(key, next_seq)),
        None => table_dir.join(regular_segment_filename(next_seq)),
    })
}

pub fn replacement_tmp_path(final_path: &Path, suffix: &str) -> PathBuf {
    let file_name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.{}", file_name, suffix))
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
mod tests {
    use super::*;

    #[test]
    fn parses_regular_segment_names() {
        assert!(is_segment_filename("seg_00042.dat"));
        assert_eq!(parse_segment_seq("seg_00042.dat"), Some(42));
        assert!(!is_segment_filename("seg_00042.tmp"));
    }

    #[test]
    fn parses_partition_segment_names() {
        assert!(is_segment_filename("p20260601_0007.dat"));
        assert_eq!(parse_segment_seq("p20260601_0007.dat"), Some(7));
        assert_eq!(
            parse_partition_seq("p20260601_0007.dat", "20260601"),
            Some(7)
        );
        assert_eq!(parse_partition_seq("p20260601_0007.dat", "20260602"), None);
    }
}
