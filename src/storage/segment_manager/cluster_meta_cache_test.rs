use super::*;

fn map(min: i64) -> Option<ZoneMap> {
    Some(ZoneMap {
        min_value: crate::types::scalar::ScalarValue::Int64(Some(min)),
        max_value: crate::types::scalar::ScalarValue::Int64(Some(min + 10)),
        null_count: 0,
        row_count: 1,
    })
}

#[test]
fn eviction_removes_only_the_oldest_half() {
    let mut cache = ClusterMetaCache::new();
    let paths: Vec<PathBuf> = (0..CAPACITY)
        .map(|i| PathBuf::from(format!("seg_{i}.dat")))
        .collect();
    for path in &paths {
        cache.put(path.clone(), 0, map(1));
    }
    assert_eq!(cache.entries.len(), CAPACITY);

    // One more entry: the oldest HALF must go, the newest half stays.
    cache.put(PathBuf::from("seg_new.dat"), 0, map(2));
    assert_eq!(
        cache.entries.len(),
        CAPACITY / 2 + 1,
        "eviction keeps the recent half instead of clearing everything"
    );
    assert!(
        cache.get(&paths[0], 0).is_none(),
        "the oldest entry was evicted"
    );
    assert!(
        cache.get(&paths[CAPACITY - 1], 0).is_some(),
        "the newest entries stay warm across the eviction cycle"
    );
    assert!(cache.get(Path::new("seg_new.dat"), 0).is_some());
}

#[test]
fn invalidate_prunes_order_too() {
    let mut cache = ClusterMetaCache::new();
    let path = PathBuf::from("seg_1.dat");
    cache.put(path.clone(), 0, map(1));
    cache.invalidate(&path);
    cache.put(path.clone(), 0, map(2));
    assert_eq!(
        cache.order.len(),
        1,
        "stale order entries do not accumulate"
    );
    assert!(cache.get(&path, 0).is_some());
}
