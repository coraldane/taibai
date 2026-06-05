use monax::protocol::types::DataType;
use monax::storage::segment::meta::{compact_sidecar_footer_if_needed, sidecar_meta_path};
use monax::storage::segment::{encode_segment_footer, SegmentReader};
use monax::storage::segment_manager::{SegmentConfig, SegmentManager};
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;
use tempfile::TempDir;

fn schema() -> Schema {
    Schema::new(vec![ColumnDef::new("id", DataType::Int64)])
}

fn batch(start: i64, rows: i64) -> RecordBatch {
    let schema = schema();
    let mut batch = RecordBatch::new(schema);
    for value in start..start + rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value)))
            .unwrap();
    }
    batch
}

#[test]
fn active_flush_writes_sidecar_without_dat_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3), false)
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    assert_eq!(segments.len(), 1);

    let seg_path = &segments[0];
    let meta_path = sidecar_meta_path(seg_path);
    assert!(meta_path.exists());

    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.row_count(), 3);
    assert_eq!(
        std::fs::metadata(seg_path).unwrap().len(),
        reader.data_end()
    );
}

#[test]
fn sidecar_journal_replays_row_group_deltas() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3), false)
        .unwrap();
    mgr.bulk_insert_batch("main", "t", batch(4, 3), false)
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    assert_eq!(segments.len(), 1);

    let reader = SegmentReader::open(&segments[0]).unwrap();
    assert_eq!(reader.row_count(), 6);
    assert_eq!(reader.num_row_groups(), 2);
}

#[test]
fn sidecar_compaction_replaces_journal_with_full_snapshot() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3), false)
        .unwrap();
    mgr.bulk_insert_batch("main", "t", batch(4, 3), false)
        .unwrap();
    mgr.bulk_insert_batch("main", "t", batch(7, 3), false)
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let seg_path = &segments[0];
    let meta_path = sidecar_meta_path(seg_path);
    let before_len = std::fs::metadata(&meta_path).unwrap().len();

    let reader = SegmentReader::open(seg_path).unwrap();
    let footer = encode_segment_footer(
        reader.schema(),
        reader.row_count(),
        &reader.footer_row_groups_info(),
        reader.delete_bitmap(),
        reader.deleted_row_count(),
        "t",
        reader.segment_zone_maps(),
    );

    assert!(
        compact_sidecar_footer_if_needed(seg_path, reader.data_end(), &footer, 1, u64::MAX)
            .unwrap()
    );

    let after_len = std::fs::metadata(&meta_path).unwrap().len();
    assert!(after_len < before_len);

    let compacted_reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(compacted_reader.row_count(), 9);
    assert_eq!(compacted_reader.num_row_groups(), 3);
}

#[test]
fn seal_merges_sidecar_back_into_dat_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::with_config(
        dir.path(),
        SegmentConfig {
            min_segment_size: 1,
            max_segment_size: 1024 * 1024,
        },
    )
    .unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3), false)
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let seg_path = &segments[0];
    assert!(sidecar_meta_path(seg_path).exists());

    mgr.flush_all_active().unwrap();

    assert!(!sidecar_meta_path(seg_path).exists());
    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.row_count(), 3);
    assert!(std::fs::metadata(seg_path).unwrap().len() > reader.data_end());
}

#[test]
fn delete_footer_rewrite_updates_sidecar_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3), false)
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let seg_path = &segments[0];

    mgr.rewrite_delete_footer(seg_path, &[0b0000_0001], 1)
        .unwrap();

    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.deleted_row_count(), 1);
    assert!(reader.is_row_deleted(0));
    assert!(!reader.is_row_deleted(1));
}
