use super::*;
use crate::types::batch::ColumnDef;

#[test]
fn unseen_string_partition_does_not_drop_partition_filter() -> Result<()> {
    let schema = Schema::new(vec![
        ColumnDef::new("key", DataType::Int64),
        ColumnDef::new("partition", DataType::VarChar),
    ]);
    // Exercise both the sealed summary and the open chunk.
    let store = HotStore::with_chunk_rows(schema.clone(), 0, Some(1), 1024 * 1024, 2);
    let mut batch = RecordBatch::new(schema);
    for partition in [Some("known"), None, Some("known")] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(1)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(partition.map(str::to_owned)))?;
    }
    store.append(&batch)?;
    let key = ScalarValue::Int64(Some(1));
    let missing = ScalarValue::VarChar(Some("missing".into()));
    assert!(store.lookup(&key, Some(&missing), None, 0, false).is_none());
    assert_eq!(
        store.count_by_key_partition_from(&key, Some(&missing), 0),
        None
    );
    assert_eq!(
        store.count_filtered(&key, Some(&missing), 0, &[], &[]),
        None
    );
    assert_eq!(store.count_by_key_partition_from(&key, None, 0), Some(3));
    assert_eq!(
        store.count_by_key_partition_from(&key, Some(&ScalarValue::VarChar(None)), 0),
        Some(1)
    );
    Ok(())
}
