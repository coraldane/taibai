use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

fn batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    let mut batch = RecordBatch::new(schema.clone());
    for value in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(*value)))
            .unwrap();
    }
    batch
}

fn key_set(keys: &[i64]) -> Vec<(Vec<usize>, std::collections::HashSet<Vec<ScalarValue>>)> {
    vec![(
        vec![0],
        keys.iter()
            .map(|value| vec![ScalarValue::Int64(Some(*value))])
            .collect(),
    )]
}

#[test]
fn replace_key_conflicts_indexes_incrementally_across_appends() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    memtable.append(&batch(&schema, &[1, 2, 3, 4])).unwrap();

    assert_eq!(
        manager.replace_key_conflicts(
            &memtable,
            &key_set(&[3, 99]),
            memtable.read().as_ref().unwrap()
        ),
        vec![2]
    );

    memtable.append(&batch(&schema, &[5, 6])).unwrap();
    assert_eq!(
        manager.replace_key_conflicts(
            &memtable,
            &key_set(&[6, 1]),
            memtable.read().as_ref().unwrap()
        ),
        vec![0, 5]
    );
}

#[test]
fn replace_key_conflicts_rebuilds_after_batch_replacement() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    memtable.append(&batch(&schema, &[1, 2, 3])).unwrap();
    let version_before = memtable.content_version();

    assert!(memtable.replace_pending_batch(batch(&schema, &[7, 8])));
    assert!(memtable.content_version() > version_before);
    assert_eq!(
        manager.replace_key_conflicts(
            &memtable,
            &key_set(&[2, 8]),
            memtable.read().as_ref().unwrap()
        ),
        vec![1]
    );
}

#[test]
fn rollback_txn_prunes_replace_key_indexes() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create_with_txn("main", "t", "", &schema, 77);
    memtable.append(&batch(&schema, &[1, 2])).unwrap();
    assert_eq!(
        manager.replace_key_conflicts(&memtable, &key_set(&[1]), memtable.read().as_ref().unwrap()),
        vec![0]
    );
    assert_eq!(manager.replace_key_indexes.read().unwrap().len(), 1);

    assert_eq!(manager.rollback_txn(77), 2);
    assert_eq!(
        manager.replace_key_indexes.read().unwrap().len(),
        0,
        "rolled-back memtables must not keep replace-key index entries"
    );
}

#[test]
fn compact_replace_key_index_shifts_and_verifies() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    let original = batch(&schema, &[1, 2, 3, 4, 5]);
    memtable.append(&original).unwrap();
    assert_eq!(
        manager.replace_key_conflicts(&memtable, &key_set(&[1]), &original),
        vec![0]
    );

    // Remove rows 0 and 3 (keys 1 and 4): filtered batch is [2, 3, 5].
    assert!(memtable.replace_pending_batch(batch(&schema, &[2, 3, 5])));
    manager.compact_replace_key_index(&memtable, &[0, 3], 5);

    let filtered = memtable.read().unwrap();
    assert_eq!(
        manager.replace_key_conflicts(&memtable, &key_set(&[2, 5, 1]), &filtered),
        vec![0, 2]
    );
}

#[test]
fn compact_replace_key_index_drops_entry_on_state_mismatch() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    memtable.append(&batch(&schema, &[1, 2])).unwrap();
    assert_eq!(
        manager.replace_key_conflicts(&memtable, &key_set(&[1]), memtable.read().as_ref().unwrap()),
        vec![0]
    );

    // A second replacement (rollback restore) bumps the version again, so
    // the compaction guard must refuse to shift positions.
    assert!(memtable.replace_pending_batch(batch(&schema, &[1, 2, 9])));
    assert!(memtable.replace_pending_batch(batch(&schema, &[1, 2])));
    manager.compact_replace_key_index(&memtable, &[0], 3);

    assert_eq!(
        manager.replace_key_conflicts(&memtable, &key_set(&[2]), memtable.read().as_ref().unwrap()),
        vec![1]
    );
}

#[test]
fn low_rate_memtable_uses_extended_age_before_becoming_flushable() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 0, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    memtable.append(&batch(&schema, &[1])).unwrap();
    assert!(manager.get_flushable().is_empty());

    memtable.append(&batch(&schema, &[2, 3])).unwrap();
    assert_eq!(manager.get_flushable().len(), 1);
}

#[test]
fn spilled_memtable_flushes_at_regular_age() {
    let temp = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 0, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "2026", &schema);
    let rows = batch(&schema, &[1]);
    memtable.append(&rows).unwrap();
    assert!(manager.get_flushable().is_empty());

    crate::storage::core::durable_tail::persist(temp.path(), memtable.id(), "2026", &rows).unwrap();
    let tail_path = crate::storage::core::durable_tail::live_tail_path(temp.path(), memtable.id())
        .unwrap()
        .expect("tail should exist");
    assert!(memtable.spill_to_tail(tail_path));

    // The extended low-rate age exists to batch resident rows; a spilled
    // memtable holds no memory, so it must flush at the regular age.
    assert_eq!(manager.get_flushable().len(), 1);
}

#[test]
fn pending_memory_uses_column_capacity_and_excludes_flushed_tables() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let memtable = manager.get_or_create("main", "t", "", &schema);
    memtable.append(&batch(&schema, &[1, 2, 3])).unwrap();

    assert!(manager.total_estimated_memory_bytes() >= 3 * std::mem::size_of::<i64>() as u64);
    memtable.set_state(MemTableState::Flushed);
    assert_eq!(manager.total_estimated_memory_bytes(), 0);
}

#[test]
fn writable_index_replaces_full_memtable_without_reusing_history() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let manager = MemTableManager::new(2, 60, 2, 3600);

    let first = manager.get_or_create("main", "t", "2026", &schema);
    first.append_owned(batch(&schema, &[1, 2])).unwrap();
    let second = manager.get_or_create("main", "t", "2026", &schema);
    assert_ne!(first.id(), second.id());
    assert_eq!(
        manager.get_or_create("main", "t", "2026", &schema).id(),
        second.id()
    );

    second.append_owned(batch(&schema, &[3, 4])).unwrap();
    let third = manager.get_or_create("main", "t", "2026", &schema);
    assert_ne!(second.id(), third.id());
    assert_eq!(manager.get_by_table("main", "t").len(), 3);
}

#[test]
fn spilled_memtable_materializes_lazily() {
    let temp = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let memtable = MemTable::new("main".into(), "t".into(), "2026".into(), schema.clone());
    let rows = batch(&schema, &[1, 2, 3]);
    memtable.append(&rows).unwrap();
    assert!(memtable.estimated_memory_bytes() > 0);

    crate::storage::core::durable_tail::persist(temp.path(), memtable.id(), "2026", &rows).unwrap();
    let tail_path = crate::storage::core::durable_tail::live_tail_path(temp.path(), memtable.id())
        .unwrap()
        .expect("tail should exist");

    assert!(memtable.spill_to_tail(tail_path));
    assert!(memtable.is_spilled());
    assert_eq!(memtable.estimated_memory_bytes(), 0);
    assert_eq!(memtable.row_count(), 3);
    // A second spill is a no-op.
    assert!(!memtable.spill_to_tail(std::path::PathBuf::from("unused")));

    let snapshot = memtable.read_snapshot().expect("spilled rows materialize");
    assert_eq!(snapshot.num_rows(), 3);
    assert_eq!(snapshot.scalar(2, 0), Some(ScalarValue::Int64(Some(3))));
    assert!(!memtable.is_spilled());

    // Appending after materialization keeps all rows visible.
    memtable.append(&batch(&schema, &[4])).unwrap();
    let snapshot = memtable.read_snapshot().unwrap();
    assert_eq!(snapshot.num_rows(), 4);
}

#[test]
fn spill_rejects_empty_or_flushed_memtables() {
    let temp = tempfile::tempdir().unwrap();
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let memtable = MemTable::new("main".into(), "t".into(), "".into(), schema.clone());
    assert!(!memtable.spill_to_tail(temp.path().join("none.tail")));

    memtable.append(&batch(&schema, &[1])).unwrap();
    memtable.set_state(MemTableState::Flushing);
    assert!(!memtable.spill_to_tail(temp.path().join("none.tail")));
}
