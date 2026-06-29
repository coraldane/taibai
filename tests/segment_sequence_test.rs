use tempfile::TempDir;

use taibai::storage::segment_manager::SegmentManager;

fn file_name(path: &std::path::Path) -> String {
    path.file_name()
        .expect("path should have file name")
        .to_string_lossy()
        .into_owned()
}

#[test]
fn segment_sequence_is_restored_per_table_from_table_dirs() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let alpha_dir = temp_dir.path().join("main").join("alpha");
    let beta_dir = temp_dir.path().join("main").join("beta");
    std::fs::create_dir_all(&alpha_dir).expect("create alpha dir");
    std::fs::create_dir_all(&beta_dir).expect("create beta dir");
    std::fs::File::create(alpha_dir.join("seg_00007.dat")).expect("seed alpha segment");
    std::fs::File::create(beta_dir.join("seg_00003.dat")).expect("seed beta segment");

    let mgr = SegmentManager::new(temp_dir.path()).expect("create segment manager");

    assert_eq!(
        file_name(&mgr.next_segment_path(&alpha_dir, "").unwrap()),
        "seg_00008.dat"
    );
    assert_eq!(
        file_name(&mgr.next_segment_path(&beta_dir, "").unwrap()),
        "seg_00004.dat"
    );
    assert_eq!(
        file_name(&mgr.next_segment_path(&alpha_dir, "").unwrap()),
        "seg_00009.dat"
    );
}

#[test]
fn partition_segment_sequence_is_restored_per_partition() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let table_dir = temp_dir.path().join("main").join("partitioned");
    std::fs::create_dir_all(&table_dir).expect("create table dir");
    std::fs::File::create(table_dir.join("p2024_0003.dat")).expect("seed p2024 segment");
    std::fs::File::create(table_dir.join("p2025_0001.dat")).expect("seed p2025 segment");

    let mgr = SegmentManager::new(temp_dir.path()).expect("create segment manager");

    assert_eq!(
        file_name(&mgr.next_partition_segment_path(&table_dir, "2024").unwrap()),
        "p2024_0004.dat"
    );
    assert_eq!(
        file_name(&mgr.next_partition_segment_path(&table_dir, "2025").unwrap()),
        "p2025_0002.dat"
    );
    assert_eq!(
        file_name(&mgr.next_partition_segment_path(&table_dir, "2026").unwrap()),
        "p2026_0001.dat"
    );
}

#[test]
fn startup_removes_incomplete_compact_temp_files() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let table_dir = temp_dir.path().join("main").join("ticks");
    std::fs::create_dir_all(&table_dir).expect("create table dir");
    let segment = table_dir.join("p2024_0001.dat");
    let compact_tmp = table_dir.join("p2024_0002.dat.compact_tmp");
    let meta_compact_tmp = table_dir.join("p2024.meta.compact.tmp");
    std::fs::File::create(&segment).expect("seed segment");
    std::fs::File::create(&compact_tmp).expect("seed compact tmp");
    std::fs::File::create(&meta_compact_tmp).expect("seed meta compact tmp");

    let _mgr = SegmentManager::new(temp_dir.path()).expect("create segment manager");

    assert!(segment.exists());
    assert!(!compact_tmp.exists());
    assert!(!meta_compact_tmp.exists());
}
