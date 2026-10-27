use super::*;
use crate::protocol::types::DataType;
use crate::storage::core::txn_mutations::TxnMutationRegistry;
use crate::storage::segment::SegmentWriter;
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

fn fully_delete(manager: &SegmentManager, path: &Path) -> Result<()> {
    manager.rewrite_delete_footer(path, &[0b11], 2)
}

#[test]
fn reclaim_waits_for_snapshot_lease_before_physical_delete() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;
    fully_delete(&manager, &path)?;

    let snapshot = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(snapshot.fully_deleted_segments().len(), 1);
    assert_eq!(
        manager.cleanup_fully_deleted_segments("db", "t", &table_dir, &snapshot)?,
        1
    );
    assert!(
        path.exists(),
        "the snapshot lease must keep the old file readable"
    );
    assert!(SegmentManager::discover_segments(&table_dir)?.is_empty());
    drop(snapshot);
    assert!(!path.exists());
    Ok(())
}

#[test]
fn changed_or_retired_candidates_are_not_committed() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let changed = table_dir.join("seg_00001.dat");
    write_segment(&changed)?;
    let manager = SegmentManager::new(tmp.path())?;
    fully_delete(&manager, &changed)?;
    let stale = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    fully_delete(&manager, &changed)?;
    assert_eq!(
        manager.cleanup_fully_deleted_segments("db", "t", &table_dir, &stale)?,
        0
    );
    assert!(changed.exists());
    drop(stale);

    let retired = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    manager.retire_segments(&table_dir, std::slice::from_ref(&changed))?;
    assert_eq!(
        manager.cleanup_fully_deleted_segments("db", "t", &table_dir, &retired)?,
        0
    );
    drop(retired);
    assert!(!changed.exists());
    Ok(())
}

#[test]
fn retired_history_follows_plan_lifetime_and_allows_path_reuse() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;
    fully_delete(&manager, &path)?;
    let snapshot = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    let candidates = snapshot.fully_deleted_segments();
    let state = manager.get_or_create_state(&table_key("db", "t"));

    manager.retire_segments(&table_dir, std::slice::from_ref(&path))?;
    drop(snapshot);
    assert!(path.exists(), "candidates must retain the segment lease");
    assert_eq!(state.maintenance_entries.lock().unwrap().len(), 1);

    drop(candidates);
    assert!(!path.exists());
    assert!(state.maintenance_entries.lock().unwrap().is_empty());

    write_segment(&path)?;
    // The file was written directly, bypassing the manager's publish hooks:
    // the next maintenance round only sees it after a reconcile.
    state.request_catalog_reconcile();
    let recycled = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(recycled.planned_segments().len(), 1);
    Ok(())
}

#[test]
fn only_conflicting_transaction_pins_block_reclaim() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let table_dir = tmp.path().join("db/t");
    std::fs::create_dir_all(&table_dir)?;
    let path = table_dir.join("seg_00001.dat");
    write_segment(&path)?;
    let manager = SegmentManager::new(tmp.path())?;
    fully_delete(&manager, &path)?;
    let registry = Arc::new(TxnMutationRegistry::new());
    manager.set_txn_mutations(Arc::clone(&registry));
    registry.register_segment_tombstones(7, "db", "t", None, &path, &[], [0])?;
    let pinned = manager.capture_maintenance_snapshot("db", "t", &table_dir, None)?;
    assert_eq!(
        manager.cleanup_fully_deleted_segments("db", "t", &table_dir, &pinned)?,
        0
    );
    registry.remove(7);

    registry.register_segment_tombstones(
        8,
        "db",
        "t",
        None,
        &table_dir.join("seg_99999.dat"),
        &[],
        [0],
    )?;
    assert_eq!(
        manager.cleanup_fully_deleted_segments("db", "t", &table_dir, &pinned)?,
        1
    );
    Ok(())
}
