# CLUSTER BY Binary Search Integration Design

## Executive Summary

This document outlines the design for integrating CLUSTER BY binary search optimization into `ScanExecutor::execute_scan`. The optimization enables fast equality predicate lookups on CLUSTER BY columns using segment-level and row-group-level binary search.

**IMPLEMENTATION STATUS: ✅ COMPLETED (2026-05-25)**

The segment-level binary search optimization has been fully integrated into `ScanExecutor::execute_scan()`.

## Current State Analysis

### Existing Implementation

**1. Row-group level binary search (Already Implemented)**

Location: `src/sql/execution/scan/executor.rs`

```rust
fn cluster_key_binary_search_read(
    &mut self,
    reader: &SegmentReader,
    rg_idx: usize,
    cluster_col_idx: usize,
    value: &ScalarValue,
    projection_cols: &[usize],
) -> Result<Option<RecordBatch>>
```

This method:
- Reads only the cluster key column
- Uses `FastScanEQ::binary_search_range()` to find matching row range (O(log n))
- Slices the matching rows directly (no full scan)
- Applies remaining predicates if needed

**2. Post-scan filtering (Currently Redundant)**

Location: `src/sql/execution/query.rs` and `src/sql/execution/scan/row_filter.rs`

```rust
apply_cluster_key_fast_scan(&mut batches, ck, &spec.eq_predicates, &table_schemas[table_name]);
```

This is called AFTER `ScanExecutor::execute()` returns batches, which is inefficient since binary search is already performed inside the executor.

### Gap Analysis (Updated 2026-05-25)

| Component | Current State | Status |
|-----------|--------------|--------|
| Segment discovery | Sorted by filename (`files.sort()`) | ✅ Complete - segments are sorted |
| Segment zone maps | Real zone map computation | ✅ Complete - `compute_zone_maps_from_bulk_payload()` |
| Segment-level pruning | Binary search across segments | ✅ Complete - `binary_search_segments_by_cluster_key()` |
| Row-group-level pruning | Zone map + bloom filter + binary search | ✅ Complete - optimal |
| Row-level lookup | Binary search implemented | ✅ Complete - `cluster_key_binary_search_read()` |

## Integration Points in ScanExecutor::execute_scan

### Location 1: Segment-Level Binary Search (New)

```rust
// In execute_scan(), after partition pruning:
let filtered_segments = partition::prune_segments_by_partition(
    all_segments,
    self.spec,
    Some(&self.table_meta),
);

// NEW: Binary search across segments using segment zone maps
let cluster_eq = self.cluster_key_eq_predicate();
if let Some((col_idx, value)) = &cluster_eq {
    filtered_segments = self.binary_search_segments(
        &filtered_segments,
        *col_idx,
        value,
    );
}
```

### Location 2: Row-Group-Level Binary Search (Already Exists)

Current code path in `execute_scan()`:
```rust
for seg_path in &filtered_segments {
    let reader = mgr.open_reader(seg_path)?;
    
    // Row group pruning via zone maps
    let prune_result = row_group::prune_row_groups(...);
    
    for rg_idx in &prune_result.candidate_indices {
        // Already using binary search for CLUSTER BY eq predicate
        if let Some((col_idx, ref value)) = cluster_eq {
            match self.cluster_key_binary_search_read(...) {
                // Fast lookup
            }
        }
    }
}
```

### Location 3: Remove Redundant Post-Scan Call (Cleanup)

Location: `src/sql/execution/query.rs` line 168

```rust
// REMOVE this call - it's redundant after integration
if let Some(ck) = table_cluster_keys.get(table_name) {
    crate::sql::execution::scan::row_filter::apply_cluster_key_fast_scan(...);
}
```

## Data Flow for Segment-Level Binary Search Optimization

### Phase 1: Segment Discovery and Sorting

```
Table Directory
├── seg_00001.dat  (cluster_key range: [A, F])
├── seg_00002.dat  (cluster_key range: [G, L])
├── seg_00003.dat  (cluster_key range: [M, R])
├── seg_00004.dat  (cluster_key range: [S, Z])
└── seg_00005.dat  (cluster_key range: [AA, ZZ])

SegmentManager::discover_segments() → returns sorted by filename
```

### Phase 2: Segment Zone Map Extraction (Requires P1 Fix)

For each segment, extract segment-level zone map from footer:
```rust
struct ZoneMap {
    min_value: ScalarValue,
    max_value: ScalarValue,
    null_count: u64,
    row_count: u64,
}
```

### Phase 3: Segment-Level Binary Search

```
Query: SELECT * FROM table WHERE cluster_key = 'M'

Step 1: Binary search across segment zone maps
  - seg_00001: [A, F] → 'M' > 'F' → skip
  - seg_00002: [G, L] → 'M' > 'L' → skip  
  - seg_00003: [M, R] → 'M' in range → candidate
  - seg_00004: [S, Z] → 'M' < 'S' → skip (and stop searching further)
  - Result: Only seg_00003 needs to be opened

Step 2: Binary search within seg_00003 row groups
  - rg_0: [M, N] → candidate
  - rg_1: [O, R] → 'M' < 'O' → skip
  - Result: Only rg_0 needs binary search

Step 3: Binary search within rg_0 rows
  - Use FastScanEQ::binary_search_range()
  - O(log n) lookup within the row group
```

### Phase 4: Binary Search Algorithm for Segments

```rust
fn binary_search_segments(
    &self,
    segments: &[PathBuf],
    cluster_col_idx: usize,
    target: &ScalarValue,
) -> Vec<PathBuf> {
    let mgr = self.engine.segment_mgr();
    
    // Collect segment zone maps for cluster column
    let seg_zone_maps: Vec<(PathBuf, ZoneMap)> = segments
        .iter()
        .filter_map(|p| {
            let reader = mgr.open_reader(p)?;
            let zm = reader.segment_zone_maps().get(cluster_col_idx)?;
            Some((p.clone(), zm.clone()))
        })
        .collect();
    
    // Binary search: find first segment where max >= target
    let first_candidate = binary_search_lower_bound(&seg_zone_maps, target);
    
    // Binary search: find last segment where min <= target
    let last_candidate = binary_search_upper_bound(&seg_zone_maps, target);
    
    // Return segment range that may contain target
    seg_zone_maps[first_candidate..=last_candidate]
        .iter()
        .map(|(p, _)| p.clone())
        .collect()
}
```

## Implementation Plan

### Task Breakdown

| Task | Priority | Description | Dependencies |
|------|----------|-------------|--------------|
| 1. Remove redundant post-scan call | P0 | Remove `apply_cluster_key_fast_scan` call in query.rs | None |
| 2. Add segment-level binary search | P0 | Implement `binary_search_segments()` in ScanExecutor | P1 (zone maps) |
| 3. Fix bulk import zone maps | P1 | Compute real zone maps during bulk insert | None |
| 4. Segment zone map index | P2 | Cache segment zone maps for faster lookup | Task 2 |

### Implementation Sequence

**Step 1: Cleanup (Immediate)**

Remove the redundant call in `query.rs`:
```rust
// Before:
if let Some(ck) = table_cluster_keys.get(table_name) {
    crate::sql::execution::scan::row_filter::apply_cluster_key_fast_scan(
        &mut batches, ck, &spec.eq_predicates, &table_schemas[table_name]);
}

// After: Remove this block entirely
```

**Step 2: Segment-Level Binary Search (After P1 Zone Map Fix)**

Add new method to ScanExecutor:
```rust
fn binary_search_segments_by_cluster_key(
    &self,
    segments: &[PathBuf],
    cluster_col_idx: usize,
    target: &ScalarValue,
) -> Result<Vec<PathBuf>> {
    // Implementation as described above
}
```

Integration in `execute_scan()`:
```rust
let filtered_segments = if let Some((col_idx, value)) = &cluster_eq {
    self.binary_search_segments_by_cluster_key(&filtered_segments, *col_idx, value)?
} else {
    filtered_segments
};
```

**Step 3: Optimization for Sorted Segment Discovery**

For CLUSTER BY tables, segment filenames encode the sort order. We can optimize by:
1. Only opening segments that have overlapping zone maps
2. Using binary search to find the range of candidate segments
3. Stopping early when we find segments with min > target

## Performance Impact Analysis

### Before Optimization

```
Query: SELECT * FROM orders WHERE order_id = 123456

Table: 10 segments × 10 row groups × 100K rows = 10M rows

Current path:
1. Open all 10 segments
2. Read zone maps for all 100 row groups
3. Prune 90 row groups (zone map)
4. Binary search in 10 candidate row groups
5. Read ~100 rows total
```

### After Optimization

```
Optimized path:
1. Read segment zone maps (cached or footer-only read)
2. Binary search → 1 segment candidate
3. Open 1 segment
4. Binary search → 1 row group candidate
5. Binary search → ~10 rows total
```

### Performance Gains

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Segments opened | 10 | 1 | 90% reduction |
| Zone map reads | 100 | 10 | 90% reduction |
| Row groups processed | 10 | 1 | 90% reduction |
| Binary search calls | 10 | 2 | 80% reduction |

## Edge Cases and Considerations

### 1. Non-Equality Predicates

For range predicates (`<`, `<=`, `>`, `>=`), binary search can still be used:
- Find segments where range overlaps with target range
- Use modified `binary_search_range_bound()` for range queries

### 2. Multi-Column Cluster Keys

Current implementation supports single-column cluster keys. For multi-column:
- Binary search on first column (primary sort key)
- Secondary column filtering after primary lookup

### 3. Null Values

Null values in cluster key column are sorted first (by convention). Binary search handles this:
- Null range check via zone map `null_count`
- Separate handling for `IS NULL` queries

### 4. Unsorted Data (No CLUSTER BY)

If table has no CLUSTER BY, fall back to:
- Full segment scan
- Zone map pruning at row group level
- Row-level predicate evaluation

## API Contract

### New Method: binary_search_segments_by_cluster_key

```rust
pub fn binary_search_segments_by_cluster_key(
    &self,
    segments: &[PathBuf],
    cluster_col_idx: usize,
    target: &ScalarValue,
) -> Result<Vec<PathBuf>>
```

**Preconditions:**
- Table has CLUSTER BY defined
- Query has equality predicate on cluster key column
- Segment zone maps are computed (not placeholder)

**Postconditions:**
- Returns segments that may contain target value
- Empty result means target value not in table
- Non-empty result guarantees at least one candidate segment

**Error Handling:**
- If zone map read fails, fall back to full segment list
- If binary search fails, return all segments as candidates

## Verification Plan

### Test Cases

1. **Basic equality lookup**
   - Single segment with matching value
   - Binary search returns correct segment

2. **Multi-segment distribution**
   - Value spans multiple segments
   - Returns all candidate segments

3. **No matching segment**
   - Value outside all segment ranges
   - Returns empty list

4. **Zone map placeholder handling**
   - Bulk imported segment with placeholder zone map
   - Falls back to full scan

### Performance Verification

Benchmark queries with:
- Small tables (1-2 segments)
- Medium tables (10-50 segments)
- Large tables (100+ segments)

Measure:
- Segment open count
- Row group read count
- Total execution time

## Dependencies on Other Tasks

| Task | Dependency Type | Impact |
|------|-----------------|--------|
| P1: Fix bulk import zone maps | Hard dependency | Segment-level binary search requires real zone maps |
| P1: SegmentReader LRU cache | Soft dependency | Performance improvement for repeated queries |
| P2: Segment-level .idx file | Enhancement | Faster zone map access without opening segment |

## Conclusion

The CLUSTER BY binary search integration consists of three parts:

1. **Cleanup**: Remove redundant post-scan filtering in `query.rs`
2. **Row-group integration**: Already implemented in `ScanExecutor::cluster_key_binary_search_read`
3. **Segment-level binary search**: New optimization requiring P1 zone map fix

The current implementation already provides row-group-level binary search optimization. The segment-level optimization requires proper zone map computation during bulk imports (P1 task).