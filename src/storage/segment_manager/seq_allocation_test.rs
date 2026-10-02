//! P0 (storage): the compaction replacement allocator (directory scan) and
//! the active-segment allocator (in-process counter) share one filename
//! namespace. They must never hand out the same sequence: creating an active
//! segment truncates, which would silently destroy a replacement segment.

use super::*;

fn table_dir(dir: &std::path::Path) -> std::path::PathBuf {
    let dir = dir.join("main").join("t");
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn seq_of(path: &std::path::Path) -> u32 {
    let name = path.file_name().unwrap().to_str().unwrap();
    crate::storage::segment::store::parse_regular_seq(name)
        .unwrap_or_else(|| panic!("regular segment name expected: {name}"))
}

#[test]
fn replacement_allocation_advances_the_active_counter() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let table_dir = table_dir(dir.path());

    // An active segment allocated and materialized by this process.
    let first = manager.next_segment_path(&table_dir, "").unwrap();
    assert_eq!(seq_of(&first), 1);
    std::fs::File::create(&first).unwrap();

    // A compaction replacement allocated by directory scan: it must see the
    // active segment and take seq 2, then fold that back into the counter.
    let replacement = manager.next_replacement_segment_path(&table_dir, None).unwrap();
    assert_eq!(seq_of(&replacement), 2);
    std::fs::File::create(&replacement).unwrap();

    // The next active allocation must not collide with the replacement
    // segment — before the fix the counter still sat at 1 and this returned
    // seg_00002, which create_new_active_segment would truncate.
    let next = manager.next_segment_path(&table_dir, "").unwrap();
    assert_ne!(
        next, replacement,
        "active allocator must not reuse the replacement segment's path"
    );
    assert_eq!(seq_of(&next), 3);
}

#[test]
fn partition_replacement_allocation_advances_the_partition_counter() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let table_dir = table_dir(dir.path());

    let first = manager
        .next_partition_segment_path(&table_dir, "20260101")
        .unwrap();
    assert!(first.to_string_lossy().ends_with("p20260101_0001.dat"));
    std::fs::File::create(&first).unwrap();

    let replacement = manager
        .next_replacement_segment_path(&table_dir, Some("20260101"))
        .unwrap();
    assert!(replacement.to_string_lossy().ends_with("p20260101_0002.dat"));
    std::fs::File::create(&replacement).unwrap();

    let next = manager
        .next_partition_segment_path(&table_dir, "20260101")
        .unwrap();
    assert_ne!(next, replacement);
    assert!(next.to_string_lossy().ends_with("p20260101_0003.dat"));
}

#[test]
fn active_allocator_self_heals_past_directory_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let table_dir = table_dir(dir.path());

    // Sequences the in-process counter never handed out (legacy
    // dual-allocator artifact, manual copy, restored backup) occupying BOTH
    // the counter's next slot and a higher one: the exists-check must reseed
    // from the directory instead of handing out a colliding path.
    std::fs::File::create(table_dir.join("seg_00001.dat")).unwrap();
    std::fs::File::create(table_dir.join("seg_00007.dat")).unwrap();

    let next = manager.next_segment_path(&table_dir, "").unwrap();
    assert_eq!(seq_of(&next), 8);
    assert!(!next.exists());
}
