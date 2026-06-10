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
