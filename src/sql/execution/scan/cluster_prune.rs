//! Segment-level cluster-key pruning, shared by the scan executor and the
//! DELETE/UPDATE segment collection.
//!
//! Segments written in cluster-key order have non-decreasing zone-map
//! tuples, so a range on the cluster key can prune to one contiguous
//! `[start, end]` window of the segment list via binary search. All pruning
//! here is conservative: a segment is skipped only when its zone map proves
//! it cannot contribute rows, and any missing/unusable zone map disables
//! pruning for the whole list (the caller then scans every segment).

use std::path::{Path, PathBuf};

use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

use super::row_filter::ClusterKeyBounds;

/// Inclusive `[start, end]` window of `segments` that may contain rows
/// matching `bounds`, or `None` when pruning cannot be applied.
pub(crate) fn cluster_segment_range(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    table_schema: &crate::types::batch::Schema,
    bounds: &ClusterKeyBounds,
) -> Option<(usize, usize)> {
    let key_cols = bounds.key_column_indices();
    if !cluster_segment_tuple_zone_maps_are_monotonic(segments, mgr, table_schema, &key_cols) {
        return None;
    }

    // Per-column zone-map extrema underestimate/overestimate the true
    // composite tuple, so the tuple searches below prune conservatively:
    // a segment is skipped only when it provably cannot contribute rows.
    let lower_tuple = search_tuple_for_bounds(&bounds.prefix_eq, bounds.lower.as_ref());
    let upper_tuple = search_tuple_for_bounds(&bounds.prefix_eq, bounds.upper.as_ref());

    let start = match (&bounds.lower, &bounds.upper) {
        (Some(_), _) => first_segment_with_max_tuple_at_least(
            segments,
            mgr,
            table_schema,
            &key_cols,
            &lower_tuple,
        )?,
        // No lower bound: only the equality prefix may narrow the start
        // (compared without the range value — every prefix row still
        // qualifies). With an empty prefix, scanning starts at 0.
        (None, Some(_)) if !bounds.prefix_eq.is_empty() => {
            let prefix_tuple: Vec<ScalarValue> = bounds
                .prefix_eq
                .iter()
                .map(|(_, value)| value.clone())
                .collect();
            let prefix_cols = &key_cols[..bounds.prefix_eq.len()];
            first_segment_with_max_tuple_at_least(
                segments,
                mgr,
                table_schema,
                prefix_cols,
                &prefix_tuple,
            )?
        }
        _ => 0,
    };
    let end = match &bounds.upper {
        Some(_) => {
            let end = last_segment_with_min_tuple_at_most(
                segments,
                mgr,
                table_schema,
                &key_cols,
                &upper_tuple,
            )?;
            if end < 0 {
                return Some((1, 0));
            }
            end as usize
        }
        None => segments.len().saturating_sub(1),
    };
    Some((start, end))
}

/// Single-column monotonicity gate used by ordered-limit scans: segment zone
/// maps must be non-decreasing in both min and max.
pub(crate) fn cluster_segment_zone_maps_are_monotonic(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    table_schema: &crate::types::batch::Schema,
    col_idx: usize,
) -> bool {
    let mut prev_min: Option<ScalarValue> = None;
    let mut prev_max: Option<ScalarValue> = None;

    for segment in segments {
        let Some(zm) = mgr.cluster_zone_map_cached(segment, table_schema, col_idx) else {
            return false;
        };
        if let Some(prev) = &prev_min {
            if sort_cmp(&zm.min_value, prev) == std::cmp::Ordering::Less {
                return false;
            }
        }
        if let Some(prev) = &prev_max {
            if sort_cmp(&zm.max_value, prev) == std::cmp::Ordering::Less {
                return false;
            }
        }
        prev_min = Some(zm.min_value);
        prev_max = Some(zm.max_value);
    }

    true
}

/// Binary search for the first segment whose composite max tuple (per column
/// zone-map maxima) is not less than the search tuple.
fn first_segment_with_max_tuple_at_least(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    table_schema: &crate::types::batch::Schema,
    key_cols: &[usize],
    tuple: &[ScalarValue],
) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp =
            compare_segment_zone_tuple(mgr, &segments[mid], table_schema, key_cols, tuple, true)?;
        if cmp == std::cmp::Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Some(lo)
}

/// Binary search for the last segment whose composite min tuple (per column
/// zone-map minima) is not greater than the search tuple.
fn last_segment_with_min_tuple_at_most(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    table_schema: &crate::types::batch::Schema,
    key_cols: &[usize],
    tuple: &[ScalarValue],
) -> Option<isize> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp =
            compare_segment_zone_tuple(mgr, &segments[mid], table_schema, key_cols, tuple, false)?;
        if cmp != std::cmp::Ordering::Greater {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Some(lo as isize - 1)
}

/// Lexicographic comparison of a segment's zone-map tuple (max side or min
/// side) against the search tuple. Returns `None` when a zone map is
/// missing.
fn compare_segment_zone_tuple(
    mgr: &SegmentManager,
    segment: &Path,
    table_schema: &crate::types::batch::Schema,
    key_cols: &[usize],
    tuple: &[ScalarValue],
    use_max: bool,
) -> Option<std::cmp::Ordering> {
    for (&col_idx, value) in key_cols.iter().zip(tuple.iter()) {
        let zm = mgr.cluster_zone_map_cached(segment, table_schema, col_idx)?;
        let side = if use_max {
            &zm.max_value
        } else {
            &zm.min_value
        };
        let cmp = sort_cmp(side, value);
        if cmp != std::cmp::Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(std::cmp::Ordering::Equal)
}

/// Consecutive segments must be non-decreasing in the composite key tuple
/// (checked on both the min and max zone tuples).
fn cluster_segment_tuple_zone_maps_are_monotonic(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    table_schema: &crate::types::batch::Schema,
    key_cols: &[usize],
) -> bool {
    if segments.len() <= 1 {
        return true;
    }
    let mut prev_min: Option<Vec<ScalarValue>> = None;
    let mut prev_max: Option<Vec<ScalarValue>> = None;

    for segment in segments {
        let mut min_tuple = Vec::with_capacity(key_cols.len());
        let mut max_tuple = Vec::with_capacity(key_cols.len());
        for &col_idx in key_cols {
            let Some(zm) = mgr.cluster_zone_map_cached(segment, table_schema, col_idx) else {
                return false;
            };
            min_tuple.push(zm.min_value);
            max_tuple.push(zm.max_value);
        }
        if let Some(prev) = &prev_min {
            if compare_scalar_tuples(&min_tuple, prev) == std::cmp::Ordering::Less {
                return false;
            }
        }
        if let Some(prev) = &prev_max {
            if compare_scalar_tuples(&max_tuple, prev) == std::cmp::Ordering::Less {
                return false;
            }
        }
        prev_min = Some(min_tuple);
        prev_max = Some(max_tuple);
    }

    true
}

/// Prefix equality values plus the range bound value (if any). The inclusive
/// flag is deliberately ignored: treating an exclusive bound as inclusive
/// only keeps a boundary segment that the row filter then discards.
fn search_tuple_for_bounds(
    prefix_eq: &[(usize, ScalarValue)],
    bound: Option<&(ScalarValue, bool)>,
) -> Vec<ScalarValue> {
    let mut tuple: Vec<ScalarValue> = prefix_eq.iter().map(|(_, value)| value.clone()).collect();
    if let Some((value, _inclusive)) = bound {
        tuple.push(value.clone());
    }
    tuple
}

fn compare_scalar_tuples(left: &[ScalarValue], right: &[ScalarValue]) -> std::cmp::Ordering {
    for (l, r) in left.iter().zip(right.iter()) {
        let cmp = sort_cmp(l, r);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

#[inline]
fn sort_cmp(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    crate::storage::index::sort_key::compare_scalars_for_sort(a, b)
}
