use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::SegmentWriter;
use crate::storage::segment_manager::catalog::{
    load_catalog_file, store_catalog_file, CatalogEntryRecord, PlanTtlStats, PlanTtlZone,
};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

fn write_segment(path: &Path) -> Result<()> {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(1)))?;
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(2)))?;
    let mut writer = SegmentWriter::create(path, schema, 8192, 3)?;
    writer.write_batch(&batch)?;
    writer.seal()
}

/// Break the embedded footer without changing the file length: any footer
/// read fails, while a stat-based plan hit cannot notice the difference.
fn corrupt_footer_in_place(path: &Path) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
    let len = file.metadata()?.len();
    assert!(len > 8);
    file.seek(SeekFrom::End(-8))?;
    file.write_all(&[0xFFu8; 8])?;
    file.sync_all()?;
    Ok(())
}

fn sample_records() -> Vec<CatalogEntryRecord> {
    vec![
        CatalogEntryRecord {
            name: "seg_00001.dat".to_string(),
            file_len: 4096,
            row_count: 10,
            deleted_row_count: 2,
            row_group_rows: vec![5, 5],
            ttl: None,
        },
        CatalogEntryRecord {
            name: "p2026_0001.dat".to_string(),
            file_len: 8192,
            row_count: 7,
            deleted_row_count: 0,
            row_group_rows: vec![4, 3],
            ttl: Some(PlanTtlStats {
                column: "trade_date".to_string(),
                zones: vec![
                    PlanTtlZone {
                        row_count: 4,
                        null_count: 0,
                        max_timestamp_us: Some(1_700_000_000_000_000),
                    },
                    PlanTtlZone {
                        row_count: 3,
                        null_count: 1,
                        max_timestamp_us: None,
                    },
                ],
            }),
        },
    ]
}

fn assert_records_match(expected: &[CatalogEntryRecord], actual: &[CatalogEntryRecord]) {
    assert_eq!(expected.len(), actual.len());
    for (expected, actual) in expected.iter().zip(actual.iter()) {
        assert_eq!(expected.name, actual.name);
        assert_eq!(expected.file_len, actual.file_len);
        assert_eq!(expected.row_count, actual.row_count);
        assert_eq!(expected.deleted_row_count, actual.deleted_row_count);
        assert_eq!(expected.row_group_rows, actual.row_group_rows);
        match (&expected.ttl, &actual.ttl) {
            (None, None) => {}
            (Some(expected), Some(actual)) => {
                assert_eq!(expected.column, actual.column);
                assert_eq!(expected.zones.len(), actual.zones.len());
                for (expected, actual) in expected.zones.iter().zip(actual.zones.iter()) {
                    assert_eq!(expected.row_count, actual.row_count);
                    assert_eq!(expected.null_count, actual.null_count);
                    assert_eq!(expected.max_timestamp_us, actual.max_timestamp_us);
                }
            }
            _ => panic!("ttl presence mismatch"),
        }
    }
}

#[test]
fn catalog_file_roundtrips_entries() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let records = sample_records();
    store_catalog_file(tmp.path(), &records)?;
    let loaded = load_catalog_file(tmp.path())?.expect("catalog file exists");
    assert_records_match(&records, &loaded);

    store_catalog_file(tmp.path(), &[])?;
    assert!(load_catalog_file(tmp.path())?.unwrap().is_empty());
    Ok(())
}

#[test]
fn corrupt_or_unsupported_catalog_files_are_rejected() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let catalog_path = tmp.path().join("segment_catalog");
    store_catalog_file(tmp.path(), &sample_records())?;

    let mut bytes = std::fs::read(&catalog_path)?;
    let body_last = bytes.len() - 2;
    bytes[body_last] ^= 0xFF;
    std::fs::write(&catalog_path, &bytes)?;
    assert!(load_catalog_file(tmp.path()).is_err(), "bad checksum");

    std::fs::write(&catalog_path, &bytes[..bytes.len() / 2])?;
    assert!(load_catalog_file(tmp.path()).is_err(), "truncated");

    let mut wrong_version = bytes.clone();
    wrong_version[4..8].copy_from_slice(&999u32.to_le_bytes());
    let crc = {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&wrong_version[..wrong_version.len() - 4]);
        hasher.finalize()
    };
    let crc_bytes = crc.to_le_bytes();
    let tail = wrong_version.len() - 4;
    wrong_version[tail..].copy_from_slice(&crc_bytes);
    std::fs::write(&catalog_path, &wrong_version)?;
    assert!(
        load_catalog_file(tmp.path()).is_err(),
        "unsupported version"
    );

    assert!(load_catalog_file(&tmp.path().join("missing"))?.is_none());
    Ok(())
}

#[test]
fn plan_ttl_expiry_mirrors_footer_semantics() {
    let plan = SegmentPlanMeta {
        file_len: 1,
        row_count: 2,
        deleted_row_count: 0,
        row_group_rows: vec![1, 1],
        ttl: Some(PlanTtlStats {
            column: "ts".to_string(),
            zones: vec![
                PlanTtlZone {
                    row_count: 1,
                    null_count: 0,
                    max_timestamp_us: Some(1_000),
                },
                PlanTtlZone {
                    row_count: 1,
                    null_count: 0,
                    max_timestamp_us: Some(2_000),
                },
            ],
        }),
    };
    // All groups past the horizon.
    assert!(plan_ttl_fully_expired(&plan, "ts", 500, 2_500));
    // One group still fresh.
    assert!(!plan_ttl_fully_expired(&plan, "ts", 500, 2_400));
    // Column mismatch stays conservative.
    assert!(!plan_ttl_fully_expired(&plan, "other", 500, 2_500));

    let missing = SegmentPlanMeta {
        ttl: None,
        ..plan.clone()
    };
    assert!(!plan_ttl_fully_expired(&missing, "ts", 500, 2_500));

    let null_zone = SegmentPlanMeta {
        ttl: Some(PlanTtlStats {
            column: "ts".to_string(),
            zones: vec![PlanTtlZone {
                row_count: 1,
                null_count: 1,
                max_timestamp_us: Some(1_000),
            }],
        }),
        ..plan
    };
    assert!(!plan_ttl_fully_expired(&null_zone, "ts", 500, 2_500));
}

#[test]
fn steady_state_snapshot_serves_segments_without_footer_reads() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;

    let first = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(first.planned_segments().len(), 1);
    assert!(
        table_dir.join("segment_catalog").exists(),
        "the first round persists the catalog file"
    );

    corrupt_footer_in_place(&path)?;
    let second = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert!(
        second.segment_errors().is_empty(),
        "a catalog plan hit must not re-read the footer"
    );
    assert_eq!(second.planned_segments().len(), 1);
    Ok(())
}

#[test]
fn invalidation_forces_footer_rebuild_of_the_plan() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;
    let first = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(first.planned_segments().len(), 1);

    // Simulate a delete-footer rewrite invalidation, then break the footer:
    // the plan was cleared, so the next round MUST attempt a footer read and
    // surface the corruption instead of trusting the stale plan.
    manager.invalidate_reader(&path);
    corrupt_footer_in_place(&path)?;
    let second = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert!(
        !second.segment_errors().is_empty(),
        "the rebuild must read the footer, not the stale plan"
    );
    assert_eq!(second.planned_segments().len(), 0);
    Ok(())
}

#[test]
fn reconcile_picks_up_out_of_band_changes() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let kept = table_dir.join("seg_00001.dat");
    write_segment(&kept)?;
    let manager = SegmentManager::new(tmp.path())?;
    let first = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(first.planned_segments().len(), 1);
    let state = manager.get_or_create_state(&table_key("db", "t"));

    // Out-of-band add and remove, as a replication applier or external tool
    // would do.
    let added = table_dir.join("seg_00002.dat");
    write_segment(&added)?;
    std::fs::remove_file(&kept)?;
    state.request_catalog_reconcile();

    let snapshot = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    let planned: Vec<_> = snapshot
        .planned_segments()
        .into_iter()
        .map(|segment| segment.path)
        .collect();
    assert_eq!(planned, vec![added]);
    assert!(
        !state
            .maintenance_entries
            .lock()
            .unwrap()
            .contains_key(&kept),
        "reconcile must drop entries for vanished files"
    );
    Ok(())
}

#[test]
fn restart_serves_plans_from_the_persisted_catalog() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;
    let first = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(first.planned_segments().len(), 1);
    drop(manager);

    corrupt_footer_in_place(&path)?;
    let restarted = SegmentManager::new(tmp.path())?;
    let snapshot = restarted.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert!(
        snapshot.segment_errors().is_empty(),
        "the persisted plan must serve the restarted process without a footer read"
    );
    assert_eq!(snapshot.planned_segments().len(), 1);
    Ok(())
}
