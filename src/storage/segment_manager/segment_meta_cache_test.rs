use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::meta::read_embedded_segment_meta;
use crate::storage::segment::SegmentWriter;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

fn write_segment(dir: &Path, name: &str, rows: usize) -> PathBuf {
    write_segment_with_columns(dir, name, rows, 2)
}

fn write_segment_with_columns(dir: &Path, name: &str, rows: usize, columns: usize) -> PathBuf {
    let path = dir.join(name);
    let mut defs = vec![
        ColumnDef::new("sym", DataType::VarChar),
        ColumnDef::new("px", DataType::Int64),
    ];
    for col in 2..columns {
        defs.push(ColumnDef::new(&format!("c{col}"), DataType::Int64));
    }
    let schema = Schema::new(defs);
    let mut writer = SegmentWriter::create(&path, schema.clone(), 8192, 1).unwrap();
    let mut batch = RecordBatch::new(schema.clone());
    for row in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!("SYM{row:04}"))))
            .unwrap();
        for col in 1..columns {
            batch
                .column_mut(col)
                .unwrap()
                .push(&ScalarValue::Int64(Some(row as i64)))
                .unwrap();
        }
    }
    writer.write_batch(&batch).unwrap();
    writer.seal().unwrap();
    path
}

fn snapshot(path: &Path) -> SegmentMetaSnapshot {
    read_embedded_segment_meta(path).unwrap()
}

/// Cost of one entry, used to size the budget in units of "how many segments
/// fit", which is the only thing the eviction tests care about.
fn entry_cost(path: &Path) -> u64 {
    snapshot(path).estimated_heap_bytes()
}

/// The parsed footer is dominated by per-column, per-row-group bloom filters
/// (8192 expected items at a 1% false-positive rate is ~9.8 KiB each). If
/// the estimate ever stops charging them the budget under-counts by more
/// than an order of magnitude — which is exactly how a 24 MiB entry was
/// authorised by a 1024-ENTRY limit.
#[test]
fn cost_charges_one_bloom_filter_per_column() {
    let temp = tempfile::tempdir().unwrap();
    let narrow = write_segment_with_columns(temp.path(), "narrow.seg", 64, 2);
    let wide = write_segment_with_columns(temp.path(), "wide.seg", 64, 8);
    let narrow_cost = entry_cost(&narrow);
    let wide_cost = entry_cost(&wide);
    let bloom_bytes = 9_800;

    assert!(
        narrow_cost >= 2 * bloom_bytes,
        "two columns must charge two bloom filters, got {narrow_cost}"
    );
    assert!(
        wide_cost >= narrow_cost + 5 * bloom_bytes,
        "six extra columns must add six bloom filters \
         (narrow={narrow_cost}, wide={wide_cost})"
    );
}

#[test]
fn put_charges_the_estimated_heap_size() {
    let temp = tempfile::tempdir().unwrap();
    let path = write_segment(temp.path(), "a.seg", 16);
    let cost = entry_cost(&path);
    let mut cache = SegmentMetaCache::new(cost * 4);

    cache.put(path.clone(), snapshot(&path));

    let (entries, used, budget) = cache.stats();
    assert_eq!(entries, 1);
    assert_eq!(used, cost, "the charge must be the snapshot's estimate");
    assert_eq!(budget, cost * 4);
    assert!(cache.get(&path).is_some());
}

#[test]
fn evicts_the_least_recently_used_entry_when_over_budget() {
    let temp = tempfile::tempdir().unwrap();
    let a = write_segment(temp.path(), "a.seg", 16);
    let b = write_segment(temp.path(), "b.seg", 16);
    let c = write_segment(temp.path(), "c.seg", 16);
    let cost = entry_cost(&a);
    assert_eq!(cost, entry_cost(&b), "identical segments must cost the same");

    let mut cache = SegmentMetaCache::new(cost * 2);
    cache.put(a.clone(), snapshot(&a));
    cache.put(b.clone(), snapshot(&b));
    // Make `a` the most recently used so `b` becomes the victim.
    assert!(cache.get(&a).is_some());

    cache.put(c.clone(), snapshot(&c));

    assert!(cache.get(&b).is_none(), "the LRU entry must be evicted");
    assert!(cache.get(&a).is_some(), "the recently used entry must stay");
    assert!(cache.get(&c).is_some());
    let (entries, used, _) = cache.stats();
    assert_eq!(entries, 2);
    assert_eq!(used, cost * 2, "the released charge must be reclaimed");
}

#[test]
fn an_entry_larger_than_the_whole_budget_is_handed_back_uncached() {
    let temp = tempfile::tempdir().unwrap();
    let path = write_segment(temp.path(), "huge.seg", 4096);
    let cost = entry_cost(&path);
    // Budget holds half an entry: caching it would evict everything and
    // still leave the cache over budget, so it must not be stored.
    let mut cache = SegmentMetaCache::new(cost / 2);

    let returned = cache.put(path.clone(), snapshot(&path));

    assert_eq!(returned.estimated_heap_bytes(), cost);
    assert_eq!(cache.stats().0, 0, "an oversized entry must not be cached");
    assert!(cache.get(&path).is_none());
}

#[test]
fn replacing_an_entry_does_not_double_charge() {
    let temp = tempfile::tempdir().unwrap();
    let path = write_segment(temp.path(), "a.seg", 16);
    let cost = entry_cost(&path);
    let mut cache = SegmentMetaCache::new(cost + cost / 2);

    cache.put(path.clone(), snapshot(&path));
    cache.put(path.clone(), snapshot(&path));

    let (entries, used, _) = cache.stats();
    assert_eq!(entries, 1);
    assert_eq!(used, cost, "a same-key re-put must swap, not accumulate");
}

#[test]
fn invalidate_releases_the_charge() {
    let temp = tempfile::tempdir().unwrap();
    let path = write_segment(temp.path(), "a.seg", 16);
    let cost = entry_cost(&path);
    let mut cache = SegmentMetaCache::new(cost * 8);
    cache.put(path.clone(), snapshot(&path));

    cache.invalidate(&path);

    let (entries, used, _) = cache.stats();
    assert_eq!(entries, 0);
    assert_eq!(used, 0, "invalidation must release the budget it charged");
}

#[test]
fn invalidate_under_drops_only_that_directory() {
    let temp = tempfile::tempdir().unwrap();
    let kept_dir = temp.path().join("kept");
    let dropped_dir = temp.path().join("dropped");
    std::fs::create_dir_all(&kept_dir).unwrap();
    std::fs::create_dir_all(&dropped_dir).unwrap();
    let kept = write_segment(&kept_dir, "a.seg", 16);
    let dropped = write_segment(&dropped_dir, "b.seg", 16);

    let mut cache = SegmentMetaCache::new(entry_cost(&kept) * 8);
    cache.put(kept.clone(), snapshot(&kept));
    cache.put(dropped.clone(), snapshot(&dropped));

    cache.invalidate_under(&dropped_dir);

    assert!(cache.get(&kept).is_some());
    assert!(cache.get(&dropped).is_none());
    let (entries, used, _) = cache.stats();
    assert_eq!(entries, 1);
    assert_eq!(used, entry_cost(&kept));
}
