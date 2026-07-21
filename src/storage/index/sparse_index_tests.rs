use super::*;

#[test]
fn test_sparse_index_basic() {
    let index = SparseIndexImpl::new();

    index.add_block(SparseBlock::new(0, 0, 8192));
    index.add_block(SparseBlock::new(1, 8192, 8192));

    index.insert(&ScalarValue::Int64(Some(42)), 100);
    index.insert(&ScalarValue::Int64(Some(100)), 9000);

    let results = index.search(&ScalarValue::Int64(Some(42)));
    assert!(!results.is_empty());
    assert!(results.contains(&100));
}

#[test]
fn test_sparse_index_range_search() {
    let index = SparseIndexImpl::new();

    index.add_block(SparseBlock::new(0, 0, 8192));

    let results = index.range_search(
        &ScalarValue::Int64(Some(0)),
        &ScalarValue::Int64(Some(10000)),
    );
    assert!(!results.is_empty());
}

#[test]
fn test_sparse_index_serialize_deserialize() {
    let index = SparseIndexImpl::new();

    index.add_block(SparseBlock::new(0, 0, 100));
    index.insert(&ScalarValue::Int64(Some(42)), 10);

    let serialized = index.serialize();
    let deserialized = SparseIndexImpl::deserialize(&serialized).unwrap();

    assert_eq!(deserialized.block_count(), 1);
    let results = deserialized.search(&ScalarValue::Int64(Some(42)));
    assert!(!results.is_empty());
}
