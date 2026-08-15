use super::*;

#[test]
fn test_schema() {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
    ]);
    assert_eq!(schema.num_columns(), 2);
    assert_eq!(schema.index_of("id"), Some(0));
    assert_eq!(schema.index_of("name"), Some(1));
    assert_eq!(schema.index_of("missing"), None);
}

#[test]
fn test_empty_batch() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = RecordBatch::new(schema);
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1);
    assert!(batch.is_empty());
}

#[test]
fn test_batch_type_mismatch() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let columns = vec![ColumnArray::empty(DataType::VarChar)];
    let result = RecordBatch::try_new(schema, columns);
    assert!(result.is_err());
}
