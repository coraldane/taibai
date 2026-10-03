use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

const KIB: u64 = 1024;

/// Capacity 4 -> budget 4 * 256 KiB = 1 MiB, small enough to exercise
/// eviction with hand-picked costs.
fn cache() -> ReaderCache {
    ReaderCache::new(4)
}

fn write_segment(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut writer =
        crate::storage::segment::SegmentWriter::create(&path, schema.clone(), 8192, 1).unwrap();
    let mut batch = RecordBatch::new(schema);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(7)))
        .unwrap();
    writer.write_batch(&batch).unwrap();
    writer.seal().unwrap();
    path
}

fn owned_reader(path: &Path) -> SegmentReader {
    SegmentReader::open_owned(path).unwrap()
}

#[test]
fn lru_order_moves_and_removes_in_constant_time_shape() {
    let mut lru = LruOrder::new();
    let a = PathBuf::from("a");
    let b = PathBuf::from("b");
    let c = PathBuf::from("c");
    lru.push_back(a.clone());
    lru.push_back(b.clone());
    lru.push_back(c.clone());
    lru.touch(&a);
    assert_eq!(lru.pop_front(), Some(b.clone()));
    lru.remove(&c);
    assert_eq!(lru.pop_front(), Some(a.clone()));
    assert_eq!(lru.pop_front(), None);
    assert_eq!(lru.index.len(), 0);
    // Slot recycling: a remove+re-push reuses the freed slot instead of
    // growing the slab, so its size tracks the live count.
    for i in 0..10 {
        lru.push_back(PathBuf::from(format!("p{i}")));
    }
    assert_eq!(lru.slots.len(), 10);
    lru.remove(Path::new("p3"));
    lru.push_back(PathBuf::from("p_new"));
    assert_eq!(
        lru.slots.len(),
        10,
        "the freed slot is recycled, not regrown"
    );
}

#[test]
fn budget_derives_from_configured_capacity() {
    assert_eq!(
        ReaderCache::new(1024).budget_bytes,
        1024 * AVG_READER_COST_BYTES
    );
    // A degenerate capacity still buys one average entry's worth.
    assert_eq!(ReaderCache::new(0).budget_bytes, AVG_READER_COST_BYTES);
}

#[test]
fn entries_within_budget_coexist() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache();
    let paths: Vec<PathBuf> = (0..3)
        .map(|i| write_segment(temp.path(), &format!("seg_{i}.seg")))
        .collect();
    for (path, cost) in paths.iter().zip([100 * KIB, 200 * KIB, 300 * KIB]) {
        cache.put(path.clone(), owned_reader(path), cost);
    }
    for path in &paths {
        assert!(
            cache.get(path).is_some(),
            "entry {} should stay cached while the total fits the budget",
            path.display()
        );
    }
}

#[test]
fn putting_over_budget_evicts_oldest_first() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache(); // 1 MiB budget
    let a = write_segment(temp.path(), "a.seg");
    let b = write_segment(temp.path(), "b.seg");
    let c = write_segment(temp.path(), "c.seg");
    let d = write_segment(temp.path(), "d.seg");

    cache.put(a.clone(), owned_reader(&a), 400 * KIB);
    cache.put(b.clone(), owned_reader(&b), 300 * KIB);
    cache.put(c.clone(), owned_reader(&c), 300 * KIB); // 1000 KiB == budget
    for path in [&a, &b, &c] {
        assert!(cache.get(path).is_some());
    }

    // 1000 + 300 > 1024 KiB: the oldest entry (a) must go, not arbitrary
    // ones — previously the cache only bounded the entry count, so 4
    // entries of 255 MB each would all have been kept.
    cache.put(d.clone(), owned_reader(&d), 300 * KIB);
    assert!(cache.get(&a).is_none(), "oldest entry must be evicted");
    for path in [&b, &c, &d] {
        assert!(cache.get(path).is_some());
    }
}

#[test]
fn entry_larger_than_total_budget_is_not_cached() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache(); // 1 MiB budget
    let small = write_segment(temp.path(), "small.seg");
    let huge = write_segment(temp.path(), "huge.seg");

    cache.put(small.clone(), owned_reader(&small), 100 * KIB);
    let returned = cache.put(huge.clone(), owned_reader(&huge), 2 * 1024 * KIB);

    assert!(
        cache.get(&huge).is_none(),
        "an entry costing more than the whole budget must not be cached"
    );
    assert!(
        cache.get(&small).is_some(),
        "an oversized put must not disturb existing entries"
    );
    // The caller still receives a usable reader for the oversized entry.
    assert_eq!(returned.row_count(), 1);
}

#[test]
fn get_refreshes_lru_order_on_touch() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache(); // 1 MiB budget
    let a = write_segment(temp.path(), "a.seg");
    let b = write_segment(temp.path(), "b.seg");
    let c = write_segment(temp.path(), "c.seg");
    let d = write_segment(temp.path(), "d.seg");

    cache.put(a.clone(), owned_reader(&a), 300 * KIB);
    cache.put(b.clone(), owned_reader(&b), 300 * KIB);
    cache.put(c.clone(), owned_reader(&c), 300 * KIB); // 900 KiB

    // Touch a: b becomes the least recently used entry.
    assert!(cache.get(&a).is_some());

    cache.put(d.clone(), owned_reader(&d), 300 * KIB); // 1200 > 1024 KiB
    assert!(
        cache.get(&b).is_none(),
        "untouched entry must be evicted first"
    );
    for path in [&a, &c, &d] {
        assert!(cache.get(path).is_some());
    }
}

#[test]
fn same_path_reput_releases_the_previous_charge() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache(); // 1 MiB budget
    let a = write_segment(temp.path(), "a.seg");
    let b = write_segment(temp.path(), "b.seg");

    cache.put(a.clone(), owned_reader(&a), 900 * KIB);
    // Re-put the same path with a much smaller charge: the 900 KiB must
    // be released, not double-counted.
    cache.put(a.clone(), owned_reader(&a), 100 * KIB);
    cache.put(b.clone(), owned_reader(&b), 900 * KIB); // 1000 KiB <= budget

    assert!(cache.get(&a).is_some());
    assert!(cache.get(&b).is_some());
    assert_eq!(cache.used_bytes, 1000 * KIB);
}

#[test]
fn invalidate_releases_budget_for_reuse() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = cache(); // 1 MiB budget
    let a = write_segment(temp.path(), "a.seg");
    let b = write_segment(temp.path(), "b.seg");

    cache.put(a.clone(), owned_reader(&a), 900 * KIB);
    cache.invalidate(&a);
    assert!(cache.get(&a).is_none());
    assert_eq!(cache.used_bytes, 0);

    // Only fits because a's bytes were released.
    cache.put(b.clone(), owned_reader(&b), 900 * KIB);
    assert!(cache.get(&b).is_some());
}
