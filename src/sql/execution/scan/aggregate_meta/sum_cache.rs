//! Segment partial-sum cache and lifecycle invalidation.

use super::*;

/// Per-segment partial sums for the metadata aggregate fast path. Keyed by
/// (segment path, column, row count, deleted row count, float?) so appends,
/// compaction, and deletes naturally invalidate entries. In-process only.
/// Drop every cached per-segment SUM entry whose segment lives under
/// `table_dir`. Segment numbering restarts at 1 once a table directory is
/// recreated (DROP/CREATE same name, TRUNCATE) or compaction frees path
/// numbers, so a surviving entry can be hit by a NEW segment with the same
/// (path, column, row_count, deleted) tuple and answer with the previous
/// incarnation's SUM. Key embeds no table generation, so lifecycle events
/// must invalidate explicitly.
pub(crate) fn invalidate_segment_sum_cache_under(table_dir: &Path) {
    let mut cache = segment_sum_cache().lock().unwrap();
    cache.retain(|(path, _, _, _, _), _| !Path::new(path).starts_with(table_dir));
}

pub(super) type SegmentSumCacheMap =
    std::collections::HashMap<(String, String, u64, u64, bool), SumCacheValue>;

pub(super) fn segment_sum_cache() -> &'static std::sync::Mutex<SegmentSumCacheMap> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<std::sync::Mutex<SegmentSumCacheMap>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SumCacheValue {
    pub(super) i: Option<i64>,
    pub(super) f: Option<f64>,
}

pub(super) fn segment_sum_cache_lookup(
    segment: &str,
    row_count: u64,
    deleted_row_count: u64,
    columns: &[(String, bool)],
) -> Option<Vec<SumCacheValue>> {
    let cache = segment_sum_cache().lock().unwrap();
    columns
        .iter()
        .map(|(name, is_float)| {
            cache
                .get(&(
                    segment.to_string(),
                    name.clone(),
                    row_count,
                    deleted_row_count,
                    *is_float,
                ))
                .copied()
        })
        .collect()
}

pub(super) fn segment_sum_cache_store(
    segment: &str,
    row_count: u64,
    deleted_row_count: u64,
    columns: &[(String, bool)],
    values: &[SumCacheValue],
) {
    let mut cache = segment_sum_cache().lock().unwrap();
    // Bound the cache: keys embed (path, row_count, deleted) tuples, so
    // appends and compactions keep producing fresh keys and the map would
    // otherwise grow for the process lifetime.
    if cache.len() >= 4096 {
        cache.clear();
    }
    for ((name, is_float), value) in columns.iter().zip(values.iter()) {
        cache.insert(
            (
                segment.to_string(),
                name.clone(),
                row_count,
                deleted_row_count,
                *is_float,
            ),
            *value,
        );
    }
}
