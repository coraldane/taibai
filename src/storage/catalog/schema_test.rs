use super::*;
use crate::protocol::types::DataType;
use tempfile::TempDir;

#[test]
fn test_create_and_get_table() {
    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table(
            "users",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("name", DataType::VarChar),
            ],
        )
        .unwrap();

    let table = catalog.get_table("users").unwrap();
    assert_eq!(table.name, "users");
    assert_eq!(table.columns.len(), 2);
    assert_eq!(table.row_count, 0);
}

#[test]
fn test_duplicate_table() {
    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table("t", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();

    let result = catalog.create_table("t", vec![ColumnDef::new("id", DataType::Int64)]);
    assert!(result.is_err());
}

#[test]
fn test_drop_table() {
    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table("t", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();

    catalog.drop_table("t").unwrap();
    assert!(catalog.get_table("t").is_none());
}

#[test]
fn test_drop_nonexistent() {
    let mut catalog = DatabaseCatalog::new("testdb");
    let result = catalog.drop_table("nope");
    assert!(result.is_err());
}

#[test]
fn test_save_and_load() {
    let dir = TempDir::new().unwrap();
    let meta_dir = dir.path().join("meta");

    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table(
            "users",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("name", DataType::VarChar),
            ],
        )
        .unwrap();
    catalog.save(&meta_dir).unwrap();

    let loaded = DatabaseCatalog::load_or_create(&meta_dir, "testdb").unwrap();
    assert_eq!(loaded.name, "testdb");
    assert_eq!(loaded.tables.len(), 1);
    let table = loaded.get_table("users").unwrap();
    assert_eq!(table.columns.len(), 2);
}

#[test]
fn test_load_or_create_new() {
    let dir = TempDir::new().unwrap();
    let meta_dir = dir.path().join("new_meta");

    let catalog = DatabaseCatalog::load_or_create(&meta_dir, "newdb").unwrap();
    assert_eq!(catalog.name, "newdb");
    assert!(catalog.tables.is_empty());

    assert!(DatabaseCatalog::catalog_path(&meta_dir, "newdb").exists());
}

#[test]
fn test_catalogs_are_isolated_per_database() {
    let dir = TempDir::new().unwrap();
    let meta_dir = dir.path().join("meta");

    let mut db1 = DatabaseCatalog::new("db1");
    db1.create_table("t1", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();
    db1.save(&meta_dir).unwrap();

    let mut db2 = DatabaseCatalog::new("db2");
    db2.create_table("t2", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();
    db2.save(&meta_dir).unwrap();

    let loaded_db1 = DatabaseCatalog::load_or_create(&meta_dir, "db1").unwrap();
    let loaded_db2 = DatabaseCatalog::load_or_create(&meta_dir, "db2").unwrap();

    assert!(loaded_db1.get_table("t1").is_some());
    assert!(loaded_db1.get_table("t2").is_none());
    assert!(loaded_db2.get_table("t1").is_none());
    assert!(loaded_db2.get_table("t2").is_some());
}

#[test]
fn test_table_names() {
    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table("a", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();
    catalog
        .create_table("b", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();

    let mut names = catalog.table_names();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
}

#[test]
fn test_rename_table() {
    let mut catalog = DatabaseCatalog::new("testdb");
    catalog
        .create_table("old", vec![ColumnDef::new("id", DataType::Int64)])
        .unwrap();

    catalog.rename_table("old", "new").unwrap();
    assert!(catalog.get_table("old").is_none());
    assert!(catalog.get_table("new").is_some());
}

#[test]
fn test_partition_type_enum() {
    let mut meta = TableMeta::new(
        "testdb",
        "test_table",
        vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("ts", DataType::Timestamp),
        ],
    );

    meta.partition_type = PartitionType::Day;
    meta.partition_key = "ts".to_string();
    assert_eq!(
        meta.partition_strategy(),
        crate::storage::partition::partition_function::PartitionFunction::Day
    );

    meta.partition_type = PartitionType::Year;
    assert_eq!(
        meta.partition_strategy(),
        crate::storage::partition::partition_function::PartitionFunction::Year
    );

    meta.partition_type = PartitionType::Month;
    assert_eq!(
        meta.partition_strategy(),
        crate::storage::partition::partition_function::PartitionFunction::Month
    );

    meta.partition_type = PartitionType::Hash;
    meta.partition_param = 32;
    assert_eq!(
        meta.partition_strategy(),
        crate::storage::partition::partition_function::PartitionFunction::Hash(32)
    );

    meta.partition_type = PartitionType::Mod;
    meta.partition_param = 4;
    assert_eq!(
        meta.partition_strategy(),
        crate::storage::partition::partition_function::PartitionFunction::Mod(4)
    );
}

#[test]
fn test_is_partitioned() {
    let mut meta = TableMeta::new(
        "testdb",
        "test_table",
        vec![ColumnDef::new("id", DataType::Int64)],
    );

    assert!(!meta.is_partitioned());

    meta.partition_type = PartitionType::Day;
    meta.partition_key = "ts".to_string();
    assert!(meta.is_partitioned());

    meta.partition_type = PartitionType::None;
    assert!(!meta.is_partitioned());
}

#[test]
fn test_cluster_key() {
    let mut meta = TableMeta::new(
        "testdb",
        "test_table",
        vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ],
    );

    assert!(meta.cluster_key.is_empty());

    meta.cluster_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(meta.cluster_key.len(), 2);
    assert_eq!(meta.cluster_key[0], "id");
    assert_eq!(meta.cluster_key[1], "name");
}

#[test]
fn test_partition_column_index() {
    let mut meta = TableMeta::new(
        "testdb",
        "test_table",
        vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("ts", DataType::Timestamp),
            ColumnDef::new("name", DataType::VarChar),
        ],
    );

    assert!(meta.get_partition_column_index().is_none());

    meta.partition_key = "ts".to_string();
    let idx = meta.get_partition_column_index();
    assert!(idx.is_some());
    assert_eq!(idx.unwrap(), 1);

    meta.partition_key = "TS".to_string();
    let idx = meta.get_partition_column_index();
    assert!(idx.is_some());
    assert_eq!(idx.unwrap(), 1);

    meta.partition_key = "nonexistent".to_string();
    assert!(meta.get_partition_column_index().is_none());
}

#[test]
fn table_statistics_are_used_only_for_matching_row_and_schema_versions() {
    let mut meta = TableMeta::new(
        "testdb",
        "events",
        vec![ColumnDef::new("id", DataType::Int64)],
    );
    meta.row_count = 10;
    meta.statistics = Some(TableStatisticsMeta {
        row_count: 10,
        schema_version: meta.schema_version,
        collected_at: 1,
        columns: HashMap::from([(
            "id".to_string(),
            ColumnStatisticsMeta {
                distinct_count: 10,
                null_count: 0,
                min_value: Some(crate::types::scalar::ScalarValue::Int64(Some(1))),
                max_value: Some(crate::types::scalar::ScalarValue::Int64(Some(10))),
                histogram: Vec::new(),
            },
        )]),
        composite_columns: Vec::new(),
    });
    assert!(meta.valid_statistics().is_some());

    meta.row_count += 1;
    assert!(meta.valid_statistics().is_none());
    meta.row_count = 10;
    meta.schema_version += 1;
    assert!(meta.valid_statistics().is_none());
}

#[test]
fn compression_level_round_trips_and_defaults_to_none() {
    let mut meta = TableMeta::new(
        "testdb",
        "archive",
        vec![ColumnDef::new("id", DataType::Int64)],
    );
    assert_eq!(meta.compression_level, None);
    meta.compression_level = Some(19);

    let json = serde_json::to_string(&meta).unwrap();
    assert!(json.contains("compression_level"));
    let restored: TableMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.compression_level, Some(19));

    // Catalog JSON written before the field existed must keep loading.
    let legacy_json = serde_json::to_value(&restored).unwrap();
    let mut legacy = legacy_json.as_object().unwrap().clone();
    legacy.remove("compression_level");
    let without_field: TableMeta = serde_json::from_value(serde_json::Value::Object(legacy))
        .unwrap();
    assert_eq!(without_field.compression_level, None);
}
