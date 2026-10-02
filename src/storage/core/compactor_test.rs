use super::*;
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::PartitionType;
use crate::storage::segment::SegmentReader;
use crate::storage::segment_manager::SegmentConfig;
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
    let mut writer = SegmentWriter::create(path, schema.clone(), TEST_ROW_GROUP_SIZE, 3)?;
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
    let mut writer = SegmentWriter::create(path, schema.clone(), row_group_size, 3)?;
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
    // Forty source row groups exceed the former 32-entry cache. Every group
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
        segment_mgr.bulk_insert_batch_ref("default", "t", &batch)?;
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
fn optimize_uses_configured_row_group_target() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    write_segment_with_row_group_size(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(7, "g"), (5, "e"), (3, "c"), (1, "a")],
        1,
    )?;
    write_segment_with_row_group_size(
        &table_dir.join("seg_00002.dat"),
        &schema,
        &[(8, "h"), (6, "f"), (4, "d"), (2, "b")],
        1,
    )?;

    let segment_mgr = Arc::new(SegmentManager::with_config(
        tmp.path(),
        SegmentConfig {
            row_group_target_rows: 3,
            ..SegmentConfig::default()
        },
    )?);
    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;
    assert_eq!(stats.rows_written, 8);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    let reader = SegmentReader::open(&segments[0])?;
    assert_eq!(reader.num_row_groups(), 3);
    assert_eq!(read_ids(&segments[0])?, vec![1, 2, 3, 4, 5, 6, 7, 8]);
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

// ---------------------------------------------------------------------------
// Dirty-segment-only compaction (non-clustered tables)
// ---------------------------------------------------------------------------

/// Manager whose target row group is 2 rows, so 2-row test segments count as
/// "full" (not under-full) and only tombstones/fragmentation make them dirty.
fn noncluster_dirty_only_manager(tmp: &tempfile::TempDir) -> Result<Arc<SegmentManager>> {
    Ok(Arc::new(SegmentManager::with_config(
        tmp.path(),
        SegmentConfig {
            row_group_target_rows: 2,
            ..SegmentConfig::default()
        },
    )?))
}

fn file_mtime(path: &Path) -> std::time::SystemTime {
    std::fs::metadata(path).unwrap().modified().unwrap()
}

#[test]
fn noncluster_compaction_rewrites_only_dirty_segments() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let clean1 = table_dir.join("seg_00001.dat");
    let clean2 = table_dir.join("seg_00002.dat");
    let dirty = table_dir.join("seg_00003.dat");
    write_segment(&clean1, &schema, &[(1, "a"), (2, "b")])?;
    write_segment(&clean2, &schema, &[(3, "c"), (4, "d")])?;
    write_segment(&dirty, &schema, &[(5, "e"), (6, "f"), (7, "g")])?;

    let segment_mgr = noncluster_dirty_only_manager(&tmp)?;
    segment_mgr.rewrite_delete_footer(&dirty, &[0b0000_0010], 1)?;

    let clean1_mtime = file_mtime(&clean1);
    let clean2_mtime = file_mtime(&clean2);

    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

    // Only the dirty segment entered the input set and was replaced.
    assert_eq!(stats.segments_read, 1);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 2);
    assert_eq!(stats.rows_dropped, 1);

    // The clean segments were neither replaced nor touched.
    assert!(clean1.exists());
    assert!(clean2.exists());
    assert!(!dirty.exists());
    assert_eq!(file_mtime(&clean1), clean1_mtime);
    assert_eq!(file_mtime(&clean2), clean2_mtime);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 3);
    // The replacement keeps the dirty segment's surviving rows, purged of
    // tombstones.
    assert_eq!(read_ids(&table_dir.join("seg_00004.dat"))?, vec![5, 7]);
    let reader = SegmentReader::open(&table_dir.join("seg_00004.dat"))?;
    assert_eq!(reader.deleted_row_count(), 0);
    Ok(())
}

#[test]
fn noncluster_compaction_skips_when_all_segments_clean() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let clean1 = table_dir.join("seg_00001.dat");
    let clean2 = table_dir.join("seg_00002.dat");
    write_segment(&clean1, &schema, &[(1, "a"), (2, "b")])?;
    write_segment(&clean2, &schema, &[(3, "c"), (4, "d")])?;

    let segment_mgr = noncluster_dirty_only_manager(&tmp)?;
    let clean1_mtime = file_mtime(&clean1);
    let clean2_mtime = file_mtime(&clean2);

    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

    // Clean, compact, full-size segments: no rewrite at all.
    assert_eq!(stats.segments_read, 0);
    assert_eq!(stats.segments_written, 0);
    assert_eq!(stats.rows_written, 0);
    assert_eq!(stats.rows_dropped, 0);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments, vec![clean1.clone(), clean2.clone()]);
    assert_eq!(file_mtime(&clean1), clean1_mtime);
    assert_eq!(file_mtime(&clean2), clean2_mtime);
    Ok(())
}

#[test]
fn noncluster_compaction_keeps_lone_small_clean_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let seg = table_dir.join("seg_00001.dat");
    write_segment(&seg, &schema, &[(1, "a"), (2, "b")])?;

    // Default manager: one 8192-row target row group, so the 2-row segment is
    // under-full but has no merge partner.
    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let seg_mtime = file_mtime(&seg);

    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

    // Rewriting a lone small segment would reproduce the same lone small
    // file: skipped, exactly like the old clean single-segment case.
    assert_eq!(stats.segments_read, 0);
    assert_eq!(stats.segments_written, 0);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments, vec![seg.clone()]);
    assert_eq!(file_mtime(&seg), seg_mtime);
    Ok(())
}

#[test]
fn delete_cleanup_on_clustered_table_still_merges_full_partition() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let clean1 = table_dir.join("seg_00001.dat");
    let clean2 = table_dir.join("seg_00002.dat");
    let dirty = table_dir.join("seg_00003.dat");
    write_segment(&clean1, &schema, &[(1, "a"), (2, "b")])?;
    write_segment(&clean2, &schema, &[(3, "c"), (4, "d")])?;
    write_segment(&dirty, &schema, &[(5, "e"), (6, "f"), (7, "g")])?;

    let segment_mgr = Arc::new(SegmentManager::with_config(
        tmp.path(),
        SegmentConfig {
            row_group_target_rows: 2,
            ..SegmentConfig::default()
        },
    )?);
    segment_mgr.rewrite_delete_footer(&dirty, &[0b0000_0010], 1)?;

    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.compact_table_for_delete_cleanup("default", "t", &test_meta(&schema))?;

    // Clustered tables keep the full-partition merge (global cluster order
    // is an invariant): the clean segments join the rewrite too, and the
    // dirtiness gate (30%/8192) already justified the IO.
    assert_eq!(stats.segments_read, 3);
    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 6);
    assert_eq!(stats.rows_dropped, 1);

    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    // Source order preserved across the whole partition (delete cleanup does
    // not re-sort).
    assert_eq!(read_ids(&segments[0])?, vec![1, 2, 3, 4, 5, 7]);
    let reader = SegmentReader::open(&segments[0])?;
    assert_eq!(reader.deleted_row_count(), 0);
    Ok(())
}

// ---------------------------------------------------------------------------
// External merge sort (bounded-memory clustered optimization)
// ---------------------------------------------------------------------------

#[test]
fn external_merge_sort_matches_in_memory_result() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    // Two shuffled segments with interleaved keys.
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(5, "e"), (1, "a"), (9, "i"), (3, "c")],
    )?;
    write_segment(
        &table_dir.join("seg_00002.dat"),
        &schema,
        &[(8, "h"), (2, "b"), (7, "g"), (4, "d"), (6, "f")],
    )?;

    // Force the external path with a tiny budget so the data spills into
    // multiple sorted runs (and force small run chunks so more than one run
    // is produced).
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(
        estimated_row_width_bytes(&schema) * 2,
        std::sync::atomic::Ordering::Relaxed,
    );

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(stats.segments_written, 1);
    assert_eq!(stats.rows_written, 9);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    assert_eq!(
        read_ids(&segments[0])?,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
        "external merge output must be fully sorted"
    );
    // No leftover merge temp files.
    for entry in std::fs::read_dir(&table_dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        assert!(!name.contains(".merge.tmp."), "leftover run file {name}");
    }
    Ok(())
}

#[test]
fn external_merge_sort_drops_deleted_rows() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    write_segment(
        &table_dir.join("seg_00001.dat"),
        &schema,
        &[(4, "d"), (2, "b"), (6, "f")],
    )?;

    // Tombstone the middle row of the single row group.
    {
        let reader = SegmentReader::open(&table_dir.join("seg_00001.dat"))?;
        let mut bitmap = vec![0u8; 3u64.div_ceil(8) as usize];
        crate::storage::segment::delete_bitmap::mark_deleted(&mut bitmap, 1);
        drop(reader);
        SegmentReader::rewrite_footer_with_deletes(&table_dir.join("seg_00001.dat"), &bitmap, 1, crate::storage::segment::reader::FooterRewriteMode::Immediate)?;
    }

    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(
        estimated_row_width_bytes(&schema) * 2,
        std::sync::atomic::Ordering::Relaxed,
    );
    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(stats.rows_written, 2);
    assert_eq!(stats.rows_dropped, 1);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(read_ids(&segments[0])?, vec![4, 6]);
    Ok(())
}

// ---------------------------------------------------------------------------
// TTL expiry vs. clean-segment retention (non-clustered tables)
// ---------------------------------------------------------------------------

fn ttl_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("ts", DataType::Timestamp),
    ])
}

fn ttl_meta(schema: &Schema, duration_secs: i64) -> TableMeta {
    let mut meta = test_meta_without_cluster(schema);
    meta.set_ttl("ts", duration_secs);
    meta
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn write_ttl_segment(path: &Path, schema: &Schema, stamps: &[i64]) -> Result<()> {
    let mut batch = RecordBatch::new(schema.clone());
    for (row, ts) in stamps.iter().enumerate() {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(row as i64)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Timestamp(Some(*ts)))?;
    }
    let mut writer = SegmentWriter::create(path, schema.clone(), TEST_ROW_GROUP_SIZE, 3)?;
    writer.write_batch(&batch)?;
    writer.seal()
}

#[test]
fn noncluster_compaction_purges_ttl_expired_clean_segment() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = ttl_schema();
    let now = now_micros();
    // Both segments are clean (no tombstones), full, and unfragmented under
    // the 2-row target — pure Keep candidates without the TTL exemption.
    // One holds only rows expired an hour ago; the other only fresh rows.
    let hour_ago = now - 3_600 * 1_000_000;
    let expired = table_dir.join("seg_00001.dat");
    let fresh = table_dir.join("seg_00002.dat");
    write_ttl_segment(&expired, &schema, &[hour_ago, hour_ago + 1])?;
    write_ttl_segment(&fresh, &schema, &[now, now + 1])?;
    let fresh_mtime = file_mtime(&fresh);

    let segment_mgr = noncluster_dirty_only_manager(&tmp)?;
    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &ttl_meta(&schema, 60))?;

    // The zone map proves the first segment fully expired, so the Keep
    // classification was overridden and it entered the rewrite; every row
    // was physically dropped, removing the input entirely.
    assert_eq!(stats.segments_read, 1);
    assert_eq!(stats.segments_written, 0);
    assert_eq!(stats.rows_written, 0);
    // rows_dropped_ttl is exactly the value the engine's
    // adjust_row_count_for_ttl_drops subtracts from the catalog row_count —
    // one subtraction for the two rows physically removed here.
    assert_eq!(stats.rows_dropped, 2);
    assert_eq!(stats.rows_dropped_ttl, 2);
    assert!(!expired.exists());

    // The provably-unexpired clean segment stays untouched: the exemption
    // only pulls in segments whose zone map proves full expiry.
    assert_eq!(file_mtime(&fresh), fresh_mtime);
    assert_eq!(
        SegmentManager::discover_segments(&table_dir)?,
        vec![fresh.clone()]
    );
    Ok(())
}

#[test]
fn noncluster_compaction_keeps_ttl_segment_with_visible_rows() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("default").join("t");
    std::fs::create_dir_all(&table_dir)?;

    let schema = ttl_schema();
    let now = now_micros();
    // Mixed expired/fresh rows: the fresh timestamp lifts the zone-map max
    // past the TTL horizon, so full expiry cannot be proven and the clean
    // segment must stay Keep — rewriting it would drop the expired row but
    // is not the rewrite path's call to make without a proof.
    let seg = table_dir.join("seg_00001.dat");
    write_ttl_segment(&seg, &schema, &[now - 3_600 * 1_000_000, now])?;
    let seg_mtime = file_mtime(&seg);

    let segment_mgr = noncluster_dirty_only_manager(&tmp)?;
    let compactor = Compactor::new(Arc::clone(&segment_mgr), tmp.path().to_path_buf());
    let stats = compactor.optimize_table("default", "t", &ttl_meta(&schema, 60))?;

    assert_eq!(stats.segments_read, 0);
    assert_eq!(stats.segments_written, 0);
    assert_eq!(stats.rows_written, 0);
    assert_eq!(stats.rows_dropped, 0);
    assert_eq!(file_mtime(&seg), seg_mtime);
    assert_eq!(SegmentManager::discover_segments(&table_dir)?, vec![seg.clone()]);
    Ok(())
}

// ---------------------------------------------------------------------------
// Budgeted unsorted rewrite (bounded row-ref buffer)
// ---------------------------------------------------------------------------

fn read_ids_and_row_group_sizes(path: &Path) -> Result<(Vec<i64>, Vec<u64>)> {
    let reader = SegmentReader::open(path)?;
    let mut ids = Vec::new();
    let mut sizes = Vec::new();
    for rg_idx in 0..reader.num_row_groups() {
        sizes.push(reader.row_group_rows(rg_idx));
        let batch = reader.read_row_group(rg_idx)?;
        for row_idx in 0..batch.num_rows() {
            if let ScalarValue::Int64(Some(id)) = batch.scalar(row_idx, 0).unwrap() {
                ids.push(id);
            }
        }
    }
    Ok((ids, sizes))
}

#[test]
fn unsorted_budgeted_rewrite_matches_full_buffer_result() -> Result<()> {
    // Builds the same fragmented three-segment table (each row group one
    // row, so all inputs are dirty and selected) in its own directory and
    // returns the compacted output's row order and row-group layout. The
    // unsorted rewrite preserves source order, so the expected ids read
    // back are exactly the insert order.
    let build = |tmp: tempfile::TempDir| -> Result<(Vec<i64>, Vec<u64>)> {
        let table_dir = tmp.path().join("default").join("t");
        std::fs::create_dir_all(&table_dir)?;
        let schema = test_schema();
        let rows_of = |ids: &[i64]| -> Vec<(i64, String)> {
            ids.iter()
                .map(|&id| (id, format!("value-{id}")))
                .collect::<Vec<_>>()
        };
        for (seq, ids) in [&[5i64, 1, 8][..], &[3, 9, 2], &[7, 4, 6]].iter().enumerate() {
            let owned = rows_of(ids);
            let rows = owned
                .iter()
                .map(|(id, name)| (*id, name.as_str()))
                .collect::<Vec<_>>();
            write_segment_with_row_group_size(
                &table_dir.join(format!("seg_{:05}.dat", seq + 1)),
                &schema,
                &rows,
                1,
            )?;
        }

        // Target row group of 2 rows: the rewrite gathers and writes in
        // 2-row chunks, so the output layout is 2/2/2/2/1.
        let segment_mgr = Arc::new(SegmentManager::with_config(
            tmp.path(),
            SegmentConfig {
                row_group_target_rows: 2,
                ..SegmentConfig::default()
            },
        )?);
        let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
        let stats = compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;
        assert_eq!(stats.rows_written, 9);
        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        read_ids_and_row_group_sizes(&segments[0])
    };

    // Default budget: the whole 9-row ref buffer fits, single pass.
    let single_pass = build(tempfile::tempdir()?)?;

    // Tiny budget (3 refs < 9 live rows): the buffer must drain mid-scan in
    // whole-chunk flushes instead of collecting every ref up front.
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(
        ROW_REF_BYTES * 3,
        std::sync::atomic::Ordering::Relaxed,
    );
    let budgeted = build(tempfile::tempdir()?)?;
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);

    // Same rows, same source order, same row-group layout as the
    // single-pass rewrite.
    assert_eq!(
        single_pass,
        (
            vec![5, 1, 8, 3, 9, 2, 7, 4, 6],
            vec![2, 2, 2, 2, 1]
        )
    );
    assert_eq!(budgeted, single_pass);
    Ok(())
}
