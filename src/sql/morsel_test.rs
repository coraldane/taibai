use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

fn make_batch(n_rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("val", DataType::Int32),
    ]);
    let mut batch = RecordBatch::new(schema);
    for i in 0..n_rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i as i64)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int32(Some((i * 2) as i32)))
            .unwrap();
    }
    batch
}

#[test]
fn test_split_small_batch() {
    let batch = make_batch(100);
    let morsels = split_into_morsels(vec![batch], 1000);
    assert_eq!(morsels.len(), 1);
    assert_eq!(morsels[0].num_rows(), 100);
}

#[test]
fn test_split_large_batch() {
    let batch = make_batch(500);
    let morsels = split_into_morsels(vec![batch], 200);
    assert_eq!(morsels.len(), 3); // 200 + 200 + 100
    assert_eq!(morsels[0].num_rows(), 200);
    assert_eq!(morsels[1].num_rows(), 200);
    assert_eq!(morsels[2].num_rows(), 100);
}

#[test]
fn test_split_multiple_batches() {
    let b1 = make_batch(100);
    let b2 = make_batch(200);
    let morsels = split_into_morsels(vec![b1, b2], 150);
    assert_eq!(morsels.len(), 3); // 100, 150, 50
    assert_eq!(morsels[0].source_idx, 0);
    assert_eq!(morsels[1].source_idx, 1);
    assert_eq!(morsels[2].source_idx, 1);
}

#[test]
fn test_morsel_queue_steal() {
    let morsels = vec![
        Morsel::new(make_batch(10), 0),
        Morsel::new(make_batch(10), 1),
        Morsel::new(make_batch(10), 2),
    ];
    let queue = MorselQueue::new(morsels);
    assert_eq!(queue.len(), 3);

    let m = queue.steal().unwrap();
    assert_eq!(m.source_idx, 2); // LIFO
    assert_eq!(queue.len(), 2);

    queue.steal();
    queue.steal();
    assert!(queue.steal().is_none());
}

#[test]
fn test_morsel_result_drain() {
    let result = MorselResult::new();
    result.push(make_batch(10));
    result.push(make_batch(20));

    let merged = result.drain().unwrap();
    assert_eq!(merged.num_rows(), 30);
}

#[test]
fn test_execute_morsels_sequential() {
    let batches = vec![make_batch(100), make_batch(50)];
    let morsels = split_into_morsels(batches, 75);

    let results = execute_morsels_sequential(morsels, |morsel| {
        // Simple pass-through filter: keep even rows
        Ok(Some(morsel.batch))
    })
    .unwrap();

    let total: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 150);
}

#[test]
fn test_morsel_config_default() {
    let config = MorselConfig::default();
    assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);
    assert!(config.effective_workers() > 0);
}

#[test]
fn test_split_preserves_data() {
    let batch = make_batch(10);
    // Verify specific values
    assert_eq!(batch.scalar(5, 0).unwrap(), ScalarValue::Int64(Some(5)));

    let morsels = split_into_morsels(vec![batch], 3);
    assert_eq!(morsels.len(), 4); // 3 + 3 + 3 + 1

    // First morsel: rows 0,1,2
    assert_eq!(
        morsels[0].batch.scalar(0, 0).unwrap(),
        ScalarValue::Int64(Some(0))
    );
    assert_eq!(
        morsels[0].batch.scalar(2, 0).unwrap(),
        ScalarValue::Int64(Some(2))
    );

    // Last morsel: row 9
    assert_eq!(
        morsels[3].batch.scalar(0, 0).unwrap(),
        ScalarValue::Int64(Some(9))
    );
}
