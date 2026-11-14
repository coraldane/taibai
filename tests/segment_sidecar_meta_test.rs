use std::sync::Arc;
use taibai::net::bulk_importer::encode_columnar_bulk_insert;
use taibai::protocol::types::DataType;
use taibai::storage::segment::meta::{
    append_sidecar_footer, compact_sidecar_footer_if_needed, remove_sidecar_meta, sidecar_meta_path,
};
use taibai::storage::segment::{encode_segment_footer, SegmentReader, SegmentWriter};
use taibai::storage::segment_manager::{SegmentConfig, SegmentManager};
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;
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
fn active_flush_writes_sidecar_and_dat_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    assert_eq!(segments.len(), 1);

    let seg_path = &segments[0];
    let meta_path = sidecar_meta_path(seg_path);
    assert!(meta_path.exists());

    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.row_count(), 3);
    assert!(std::fs::metadata(seg_path).unwrap().len() > reader.data_end());
}

#[test]
fn partition_active_flush_writes_segment_sidecar_meta() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch_with_partition("main", "t", "2024", batch(1, 3))
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    assert_eq!(segments.len(), 1);

    let seg_path = &segments[0];
    assert_eq!(
        sidecar_meta_path(seg_path)
            .file_name()
            .unwrap()
            .to_string_lossy(),
        "p2024_0001.meta"
    );
    assert!(seg_path.with_extension("meta").exists());
    assert!(!table_dir.join("p2024.meta").exists());

    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.row_count(), 3);
}

#[test]
fn removing_one_active_partition_segment_sidecar_preserves_its_sibling() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch_with_partition("main", "t", "2024", batch(1, 3))
        .unwrap();
    mgr.bulk_insert_batch_with_partition("main", "t", "2025", batch(4, 3))
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    assert_eq!(segments.len(), 2);
    let sibling_meta = sidecar_meta_path(&segments[1]);
    assert!(sibling_meta.exists());

    remove_sidecar_meta(&segments[0]).unwrap();
    assert!(sibling_meta.exists());
    assert_eq!(SegmentReader::open(&segments[1]).unwrap().row_count(), 3);
}

#[test]
fn invalid_sidecar_data_end_falls_back_to_embedded_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();
    mgr.bulk_insert_batch_with_partition("main", "t", "2024", batch(1, 3))
        .unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segment = SegmentManager::discover_segments(&table_dir)
        .unwrap()
        .remove(0);
    let original = SegmentReader::open(&segment).unwrap();
    let footer = encode_segment_footer(
        original.schema(),
        original.row_count(),
        &original.footer_row_groups_info().unwrap(),
        original.delete_bitmap(),
        original.deleted_row_count(),
        "t",
        original.segment_zone_maps(),
    );
    let invalid_data_end = std::fs::metadata(&segment).unwrap().len() + 1024;
    append_sidecar_footer(&segment, invalid_data_end, &footer).unwrap();

    let recovered = SegmentReader::open(&segment).unwrap();
    assert_eq!(recovered.row_count(), 3);
    assert_eq!(recovered.data_end(), original.data_end());
}

#[test]
fn sidecar_journal_replays_row_group_deltas() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();
    mgr.bulk_insert_batch("main", "t", batch(4, 3)).unwrap();

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

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();
    mgr.bulk_insert_batch("main", "t", batch(4, 3)).unwrap();
    mgr.bulk_insert_batch("main", "t", batch(7, 3)).unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let seg_path = &segments[0];
    let meta_path = sidecar_meta_path(seg_path);
    let before_len = std::fs::metadata(&meta_path).unwrap().len();

    let reader = SegmentReader::open(seg_path).unwrap();
    let footer = encode_segment_footer(
        reader.schema(),
        reader.row_count(),
        &reader.footer_row_groups_info().unwrap(),
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
fn seal_removes_sidecar_and_keeps_dat_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::with_config(
        dir.path(),
        SegmentConfig {
            min_segment_size: 1,
            max_segment_size: 1024 * 1024,
            row_group_target_rows: 131_072,
        },
    )
    .unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let seg_path = &segments[0];
    assert!(sidecar_meta_path(seg_path).exists());
    let len_before_seal = std::fs::metadata(seg_path).unwrap().len();

    mgr.flush_all_active().unwrap();

    assert!(!sidecar_meta_path(seg_path).exists());
    let reader = SegmentReader::open(seg_path).unwrap();
    assert_eq!(reader.row_count(), 3);
    assert_eq!(std::fs::metadata(seg_path).unwrap().len(), len_before_seal);
    assert!(len_before_seal > reader.data_end());
}

#[test]
fn standalone_sealed_segment_has_no_sidecar() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seg_0001.dat");
    let mut writer = SegmentWriter::create(&path, schema(), 1024, 3).unwrap();
    writer.write_batch(&batch(1, 3)).unwrap();
    writer.seal().unwrap();

    assert!(!sidecar_meta_path(&path).exists());
    assert_eq!(SegmentReader::open(&path).unwrap().row_count(), 3);
}

#[test]
fn sealed_segment_metadata_is_cached_without_sidecar() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::with_config(
        dir.path(),
        SegmentConfig {
            min_segment_size: 1,
            max_segment_size: 1024 * 1024,
            row_group_target_rows: 131_072,
        },
    )
    .unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();
    mgr.flush_all_active().unwrap();
    let segment = SegmentManager::discover_segments(&dir.path().join("main").join("t"))
        .unwrap()
        .remove(0);

    assert!(!sidecar_meta_path(&segment).exists());
    let first = mgr.segment_meta(&segment).unwrap().unwrap();
    let second = mgr.segment_meta(&segment).unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(second.row_count, 3);
}

#[test]
fn sealed_metadata_cache_is_invalidated_by_delete_rewrite() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::with_config(
        dir.path(),
        SegmentConfig {
            min_segment_size: 1,
            max_segment_size: 1024 * 1024,
            row_group_target_rows: 131_072,
        },
    )
    .unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();
    mgr.flush_all_active().unwrap();
    let segment = SegmentManager::discover_segments(&dir.path().join("main").join("t"))
        .unwrap()
        .remove(0);
    let before = mgr.segment_meta(&segment).unwrap().unwrap();

    mgr.rewrite_delete_footer(&segment, &[0b0000_0001], 1)
        .unwrap();

    let after = mgr.segment_meta(&segment).unwrap().unwrap();
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(after.deleted_row_count, 1);
    assert!(!sidecar_meta_path(&segment).exists());
}

#[test]
fn delete_footer_rewrite_updates_sidecar_footer() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();

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

#[test]
fn direct_bulk_row_group_writes_value_sets() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::new(dir.path()).unwrap();
    let payload = encode_columnar_bulk_insert("t", &batch(1, 3)).unwrap();

    mgr.bulk_insert_payload("main", &payload).unwrap();

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir).unwrap();
    let reader = SegmentReader::open(&segments[0]).unwrap();
    let value_sets = reader.value_sets(0);
    let values = value_sets[0].as_ref().unwrap();

    assert_eq!(
        values,
        &vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ]
    );
}

#[test]
fn small_inactive_segment_reuses_cached_owned_reader() {
    let dir = TempDir::new().unwrap();
    let mgr = SegmentManager::with_config(
        dir.path(),
        SegmentConfig {
            min_segment_size: 500 * 1024 * 1024,
            max_segment_size: 1024 * 1024 * 1024,
            row_group_target_rows: 131_072,
        },
    )
    .unwrap();

    mgr.bulk_insert_batch("main", "t", batch(1, 3)).unwrap();
    mgr.flush_all_active().unwrap();
    mgr.clear_active_segments("main", "t");

    let table_dir = dir.path().join("main").join("t");
    let segment = SegmentManager::discover_segments(&table_dir)
        .unwrap()
        .remove(0);
    assert!(std::fs::metadata(&segment).unwrap().len() < 500 * 1024 * 1024);

    let first = mgr.open_reader_cached(&segment).unwrap();
    let second = mgr.open_reader_cached(&segment).unwrap();

    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(second.row_count(), 3);
}
