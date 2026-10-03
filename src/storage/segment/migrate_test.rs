use super::*;
use crate::storage::segment::{SegmentReader, SegmentWriter};
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

fn write_segment(path: &Path) {
    let schema = Schema::new(vec![ColumnDef::new(
        "id",
        crate::protocol::types::DataType::Int64,
    )]);
    let mut writer = SegmentWriter::create(path, schema, 8192, 1).expect("create");
    let schema = Schema::new(vec![ColumnDef::new(
        "id",
        crate::protocol::types::DataType::Int64,
    )]);
    let mut batch = crate::types::batch::RecordBatch::new(schema.clone());
    for value in 0..100i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value)))
            .expect("push");
    }
    writer.write_batch(&batch).expect("append");
    writer.seal().expect("seal");
}

#[test]
fn upgrade_rewrites_version_markers_and_stays_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg_00001.dat");
    write_segment(&path);
    assert_eq!(segment_header_version(&path).unwrap(), SEGMENT_VERSION);
    assert!(!needs_upgrade(&path).unwrap());

    // Simulate a segment written by an older binary.
    downgrade_version_markers(&path, 1);
    assert_eq!(segment_header_version(&path).unwrap(), 1);
    assert!(needs_upgrade(&path).unwrap());
    {
        let check = SegmentReader::open(&path).unwrap();
        assert_eq!(check.row_count(), 100);
        // Drop before the upgrade: the mmap would block the rename.
    }

    let outcome = upgrade_segment_file(&path).unwrap().expect("migrated");
    assert_eq!(outcome.from_version, 1);
    assert_eq!(outcome.to_version, SEGMENT_VERSION);
    assert!(!needs_upgrade(&path).unwrap());

    // Data survives the rewrite byte-for-byte in the data area.
    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.row_count(), 100);
    let batch = reader.read_row_group(0).unwrap();
    let first = batch.scalar(0, 0).unwrap();
    assert_eq!(first, ScalarValue::Int64(Some(0)));
    // No temp or sidecar files left behind.
    assert!(!tmp_path(&path).exists());
    assert!(!sidecar_path(&path).exists());
}

#[test]
fn upgrade_refuses_current_version_as_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("seg_00001.dat");
    write_segment(&path);
    assert!(upgrade_segment_file(&path).unwrap().is_none());
}

/// Flip the header version to `version` (what an older binary wrote).
/// The embedded footer version is left alone: it is informational and
/// covered by the whole-footer checksum, so an old binary's file only
/// differs at the header gate.
fn downgrade_version_markers(path: &Path, version: u32) {
    let mut bytes = fs::read(path).unwrap();
    bytes[4..8].copy_from_slice(&version.to_le_bytes());
    fs::write(path, bytes).unwrap();
}
