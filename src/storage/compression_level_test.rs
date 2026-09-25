use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

fn compressible_batch() -> RecordBatch {
    let schema = Schema::new(vec![ColumnDef::new("payload", DataType::VarChar)]);
    let mut batch = RecordBatch::with_capacity(schema, 64);
    for _ in 0..64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("taibai-archive-row|".repeat(64))))
            .unwrap();
    }
    batch
}

fn standalone_segment_bytes(data_dir: &Path, level: Option<i32>) -> u64 {
    let mgr = SegmentManager::new(data_dir).expect("create segment manager");
    mgr.set_table_compression_level("main", "ticks", level);
    let table_dir = data_dir.join("main").join("ticks");
    std::fs::create_dir_all(&table_dir).expect("create table dir");
    let path = table_dir.join("seg_000001.dat");
    let batch = compressible_batch();
    mgr.write_standalone_active_encoded_segment(&path, &batch)
        .expect("write standalone segment");
    std::fs::metadata(&path).expect("segment file").len()
}

#[test]
fn table_compression_level_follows_override_then_global() {
    let temp = tempfile::tempdir().expect("temp dir");
    let mgr = SegmentManager::new(temp.path()).expect("segment manager");

    // Unset tables follow the global configured level.
    let global_default = mgr.table_compression_level("main", "ticks");
    assert_eq!(global_default, bulk_compression_level());

    mgr.set_table_compression_level("main", "ticks", Some(19));
    assert_eq!(mgr.table_compression_level("main", "ticks"), 19);

    // Resetting to the global default.
    mgr.set_table_compression_level("main", "ticks", None);
    assert_eq!(
        mgr.table_compression_level("main", "ticks"),
        bulk_compression_level()
    );
}

#[test]
fn standalone_segment_uses_table_compression_level() {
    let hot = tempfile::tempdir().expect("hot temp dir");
    let archive = tempfile::tempdir().expect("archive temp dir");

    let hot_bytes = standalone_segment_bytes(hot.path(), Some(1));
    let archive_bytes = standalone_segment_bytes(archive.path(), Some(22));

    assert!(
        archive_bytes < hot_bytes,
        "level 22 segment ({archive_bytes} bytes) must be smaller than level 1 ({hot_bytes} bytes)"
    );
}

#[test]
fn standalone_segment_without_override_matches_global_level() {
    let global_dir = tempfile::tempdir().expect("global temp dir");
    let default_dir = tempfile::tempdir().expect("default temp dir");

    let global_bytes = standalone_segment_bytes(global_dir.path(), None);
    let default_bytes = standalone_segment_bytes(default_dir.path(), None);

    assert_eq!(global_bytes, default_bytes);
}
