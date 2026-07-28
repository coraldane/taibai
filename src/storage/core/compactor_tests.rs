use super::*;
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::PartitionType;
use crate::storage::segment::SegmentReader;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

const TEST_ROW_GROUP_SIZE: usize = 8192;

fn test_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
    ])
}

fn test_meta(schema: &Schema) -> TableMeta {
    let mut meta = TableMeta::new("default", "t", schema.columns.clone());
    meta.cluster_key = vec!["id".to_string()];
    meta
}

fn composite_cluster_meta(schema: &Schema) -> TableMeta {
    let mut meta = TableMeta::new("default", "t", schema.columns.clone());
    meta.cluster_key = vec!["name".to_string(), "id".to_string()];
    meta
}

fn partitioned_test_meta(schema: &Schema) -> TableMeta {
    let mut meta = test_meta(schema);
    meta.partition_type = PartitionType::Year;
    meta.partition_key = "id".to_string();
    meta.update_partition_key_idx();
    meta
}

fn test_meta_without_cluster(schema: &Schema) -> TableMeta {
    TableMeta::new("default", "t", schema.columns.clone())
}

fn make_batch(schema: &Schema, rows: &[(i64, &str)]) -> Result<RecordBatch> {
    let mut batch = RecordBatch::new(schema.clone());
    for (id, name) in rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(*id)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some((*name).to_string())))?;
    }
    Ok(batch)
}

fn write_segment(path: &Path, schema: &Schema, rows: &[(i64, &str)]) -> Result<()> {
    let batch = make_batch(schema, rows)?;
    let mut writer = SegmentWriter::create(path, schema.clone(), TEST_ROW_GROUP_SIZE)?;
    writer.write_batch(&batch)?;
    writer.seal()
}

fn write_segment_with_row_group_size(
    path: &Path,
    schema: &Schema,
    rows: &[(i64, &str)],
    row_group_size: usize,
) -> Result<()> {
    let batch = make_batch(schema, rows)?;
    let mut writer = SegmentWriter::create(path, schema.clone(), row_group_size)?;
    writer.write_batch(&batch)?;
    writer.seal()
}

fn read_ids(path: &Path) -> Result<Vec<i64>> {
    let reader = SegmentReader::open(path)?;
    let mut ids = Vec::new();
    for rg_idx in 0..reader.num_row_groups() {
        let batch = reader.read_row_group(rg_idx)?;
        for row_idx in 0..batch.num_rows() {
            if let ScalarValue::Int64(Some(id)) = batch.scalar(row_idx, 0).unwrap() {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

#[test]
fn optimize_table_merges_and_sorts_segments() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(3, "c"), (1, "a")],
    )?;
    write_segment(
        &table_dir.join("seg_00002.dat"),
        &schema,
        &[(4, "d"), (2, "b")],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

    assert_eq!(stats.segments_read, 2);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 4);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(read_ids(&segments[0])?, vec![1, 2, 3, 4]);
    Ok(())
}

#[test]
fn optimize_table_sorts_clean_single_segment_when_clustered() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(3, "c"), (1, "a"), (2, "b")],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

    assert_eq!(stats.segments_read, 1);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 3);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(read_ids(&segments[0])?, vec![1, 2, 3]);
    Ok(())
}

#[test]
fn optimize_interleaved_rows_across_more_than_cache_capacity() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    // Forty source row groups exceed the legacy 32-entry cache. Every group
    // contains interleaved keys, matching time-ordered tick ingestion where a
    // clustered output chunk touches nearly every source group.
    let owned_rows = (0..160)
        .map(|row| {
            let key = (row * 37) % 160;
            (key as i64, format!("value-{row}"))
        })
        .collect::<Vec<_>>();
    let rows = owned_rows
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
        .collect::<Vec<_>>();
    write_segment_with_row_group_size(&table_dir.join("seg_00001.dat"), &schema, &rows, 4)?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    compactor.optimize_table("default", "t", &test_meta(&schema))?;

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(read_ids(&segments[0])?, (0..160).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn optimize_table_sorts_by_composite_cluster_key() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(3, "b"), (2, "a")],
    )?;
    write_segment(
        &table_dir.join("seg_00002.dat"),
        &schema,
        &[(1, "b"), (4, "a")],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    compactor.optimize_table("default", "t", &composite_cluster_meta(&schema))?;

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    // ORDER BY name, id => (a,2), (a,4), (b,1), (b,3).
    assert_eq!(read_ids(&segments[0])?, vec![2, 4, 1, 3]);
    let reader = SegmentReader::open(&segments[0])?;
    let zone_maps = reader.zone_maps(0);
    assert_eq!(zone_maps[0].min_value, ScalarValue::Int64(Some(1)));
    assert_eq!(zone_maps[0].max_value, ScalarValue::Int64(Some(4)));
    assert_eq!(
        zone_maps[1].min_value,
        ScalarValue::VarChar(Some("a".to_string()))
    );
    assert_eq!(
        zone_maps[1].max_value,
        ScalarValue::VarChar(Some("b".to_string()))
    );
    assert!(reader.value_sets(0)[0]
        .as_ref()
        .unwrap()
        .contains(&ScalarValue::Int64(Some(3))));
    assert!(reader.bloom_filters(0)[1]
        .as_ref()
        .unwrap()
        .might_contain(&ScalarValue::VarChar(Some("b".to_string()))));
    Ok(())
}

#[test]
fn optimize_table_drops_deleted_rows() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let seg_path = table_dir.join("seg_00001.dat");
    write_segment(&seg_path, &schema, &[(2, "b"), (1, "a"), (3, "c")])?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    segment_mgr.rewrite_delete_footer(&seg_path, &[0b0000_0010], 1)?;

    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

    assert_eq!(stats.rows_written, 2);
    assert_eq!(stats.rows_dropped, 1);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(read_ids(&segments[0])?, vec![2, 3]);
    let reader = SegmentReader::open(&segments[0])?;
    assert_eq!(reader.deleted_row_count(), 0);
    Ok(())
}

#[test]
fn background_delete_cleanup_does_not_sort_clustered_table() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let seg_path = table_dir.join("seg_00001.dat");
    write_segment(&seg_path, &schema, &[(3, "c"), (1, "a"), (2, "b")])?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    segment_mgr.rewrite_delete_footer(&seg_path, &[0b0000_0010], 1)?;

    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.compact_table_for_delete_cleanup("default", "t", &test_meta(&schema))?;

    assert_eq!(stats.rows_written, 2);
    assert_eq!(stats.rows_dropped, 1);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(read_ids(&segments[0])?, vec![3, 2]);
    Ok(())
}

#[test]
fn optimize_table_without_cluster_key_preserves_row_order() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(3, "c"), (1, "a")],
    )?;
    write_segment(
        &table_dir.join("seg_00002.dat"),
        &schema,
        &[(4, "d"), (2, "b")],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

    assert_eq!(stats.segments_read, 2);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 4);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(read_ids(&segments[0])?, vec![3, 1, 4, 2]);
    Ok(())
}

#[test]
fn optimize_table_rewrites_fragmented_clean_single_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let rows = (0..32)
        .map(|id| (id, format!("value-{id}")))
        .collect::<Vec<_>>();
    let borrowed_rows = rows
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
        .collect::<Vec<_>>();
    write_segment_with_row_group_size(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &borrowed_rows,
        1,
    )?;

    let before = SegmentReader::open(&table_dir.join("seg_00001.dat"))?;
    assert_eq!(before.num_row_groups(), 32);

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

    assert_eq!(stats.segments_read, 1);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 32);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    let reader = SegmentReader::open(&segments[0])?;
    assert_eq!(reader.num_row_groups(), 1);
    assert_eq!(read_ids(&segments[0])?, (0..32).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn background_fragmentation_compaction_seals_and_rewrites_active_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);

    for id in 0..40 {
        let name = format!("row-{id}");
        let batch = make_batch(&schema, &[(id, name.as_str())])?;
        segment_mgr.bulk_insert_batch_ref("default", "t", &batch, false)?;
    }

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(SegmentReader::open(&segments[0])?.num_row_groups(), 40);
    assert!(!segment_mgr.active_partition_keys("default", "t").is_empty());

    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.compact_table_for_fragmentation(
        "default",
        "t",
        &test_meta_without_cluster(&schema),
    )?;
    assert_eq!(stats.rows_written, 40);
    assert!(segment_mgr.active_partition_keys("default", "t").is_empty());

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    let reader = SegmentReader::open(&segments[0])?;
    assert_eq!(reader.row_count(), 40);
    assert_eq!(reader.num_row_groups(), 1);
    Ok(())
}

#[test]
fn optimize_partition_only_rewrites_requested_partition() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("p2024_0001.dat"),
        &schema,
        &[(3, "c"), (1, "a")],
    )?;
    write_segment(
        &table_dir.join("p2024_0002.dat"),
        &schema,
        &[(4, "d"), (2, "b")],
    )?;
    write_segment(&table_dir.join("p2025_0001.dat"), &schema, &[(30, "z")])?;
    write_segment(&table_dir.join("p2025_0002.dat"), &schema, &[(20, "y")])?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats =
        compactor.optimize_partition("default", "t", &partitioned_test_meta(&schema), "P2024")?;

    assert_eq!(stats.segments_read, 2);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 4);

    let mut files = SegmentManager::discover_segments(&table_dir)?
        .into_iter()
        .filter_map(|path| {
            path.file_name()
                .map(|name| name.to_string_lossy().to_string())
        })
        .collect::<Vec<_>>();
    files.sort();

    assert_eq!(
        files,
        vec![
            "p2024_0003.dat".to_string(),
            "p2025_0001.dat".to_string(),
            "p2025_0002.dat".to_string(),
        ]
    );
    assert_eq!(
        read_ids(&table_dir.join("p2024_0003.dat"))?,
        vec![1, 2, 3, 4]
    );
    Ok(())
}

#[test]
fn optimize_table_sorts_each_partition_independently() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("p2024_0001.dat"),
        &schema,
        &[(4, "d"), (2, "b")],
    )?;
    write_segment(
        &table_dir.join("p2025_0001.dat"),
        &schema,
        &[(30, "z"), (10, "x"), (20, "y")],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &partitioned_test_meta(&schema))?;

    assert_eq!(stats.segments_read, 2);
    assert_eq!(stats.segments_written, 2);
    assert_eq!(stats.rows_written, 5);
    assert_eq!(read_ids(&table_dir.join("p2024_0002.dat"))?, vec![2, 4]);
    assert_eq!(
        read_ids(&table_dir.join("p2025_0002.dat"))?,
        vec![10, 20, 30]
    );
    Ok(())
}
#[test]
fn cleanup_fully_deleted_segments_removes_inactive_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let seg_path = table_dir.join("seg_00001.dat");
    write_segment(&seg_path, &schema, &[(1, "a"), (2, "b")])?;

    let segment_mgr = SegmentManager::new(tmp.path())?;
    segment_mgr.rewrite_delete_footer(&seg_path, &[0b0000_0011], 2)?;
    let removed = segment_mgr.cleanup_fully_deleted_segments("default", "t", &table_dir)?;

    assert_eq!(removed, 1);
    assert!(!seg_path.exists());
    Ok(())
}

#[test]
fn compaction_materializes_value_sets_in_rewritten_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(2, "b"), (1, "a")],
    )?;
    write_segment(&table_dir.join("seg_00002.dat"), &schema, &[(3, "c")])?;

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    compactor.optimize_table("default", "t", &test_meta(&schema))?;

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    let reader = SegmentReader::open(&segments[0])?;
    let value_sets = reader.value_sets(0);
    assert!(value_sets[0]
        .as_ref()
        .is_some_and(|values| values.len() == 3));
    assert!(value_sets[1]
        .as_ref()
        .is_some_and(|values| values.len() == 3));
    Ok(())
}
