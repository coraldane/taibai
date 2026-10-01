use super::*;
use crate::types::batch::ColumnDef;

#[test]
fn typed_zone_maps_preserve_ranges_and_null_counts() {
    let schema = Schema::new(vec![
        ColumnDef::new("number", DataType::Int32),
        ColumnDef::new("text", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema);
    for (number, text) in [
        (
            ScalarValue::Int32(Some(9)),
            ScalarValue::VarChar(Some("z".into())),
        ),
        (
            ScalarValue::Int32(None),
            ScalarValue::VarChar(Some("b".into())),
        ),
        (ScalarValue::Int32(Some(-4)), ScalarValue::VarChar(None)),
        (
            ScalarValue::Int32(Some(3)),
            ScalarValue::VarChar(Some("m".into())),
        ),
    ] {
        batch.column_mut(0).unwrap().push(&number).unwrap();
        batch.column_mut(1).unwrap().push(&text).unwrap();
    }

    let maps: Vec<ZoneMap> = (0..batch.num_columns())
        .map(|col_idx| compute_zone_map_from_column(batch.column(col_idx).unwrap(), 0, 4))
        .collect();
    assert_eq!(maps[0].min_value, ScalarValue::Int32(Some(-4)));
    assert_eq!(maps[0].max_value, ScalarValue::Int32(Some(9)));
    assert_eq!(maps[0].null_count, 1);
    assert_eq!(maps[1].min_value, ScalarValue::VarChar(Some("b".into())));
    assert_eq!(maps[1].max_value, ScalarValue::VarChar(Some("z".into())));
    assert_eq!(maps[1].null_count, 1);
    assert!(maps.iter().all(|map| map.row_count == 4));
}

/// P0 (audit): `cluster_zone_map_cached` indexed footer zone maps with the
/// CATALOG column index. After a DROP COLUMN shifted catalog indices while
/// sealed footers kept their layout, cluster pruning consulted the wrong
/// column's zone map and could skip segments holding matching rows. The
/// catalog index must be resolved against the footer schema BY NAME.
#[test]
fn cluster_zone_map_resolves_catalog_index_by_name_after_drop_column() {
    use crate::protocol::types::DataType;
    use crate::types::scalar::ScalarValue;

    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();

    // Footer layout (dropme, k, v): k's zone map is [1, 3] while dropme's
    // is [100, 102] — far apart, so a positional mix-up is observable.
    let footer_schema = Schema::new(vec![
        ColumnDef::new("dropme", DataType::Int64),
        ColumnDef::new("k", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ]);
    let seg_path = table_dir.join("segment_000001.seg");
    let mut writer = crate::storage::segment::SegmentWriter::create(
        &seg_path,
        footer_schema.clone(),
        8192,
        1,
    )
    .unwrap();
    let mut batch = RecordBatch::new(footer_schema);
    for (dropme, k, v) in [(100, 1, 10), (101, 2, 20), (102, 3, 30)] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(dropme)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(k)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int64(Some(v)))
            .unwrap();
    }
    writer.write_batch(&batch).unwrap();
    writer.seal().unwrap();

    let mgr = crate::storage::segment_manager::SegmentManager::new(temp.path()).unwrap();
    // Catalog after DROP COLUMN dropme: (k, v). `k` sits at catalog index 0,
    // which positionally would read dropme's [100, 102].
    let catalog = Schema::new(vec![
        ColumnDef::new("k", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ]);
    let zm = mgr
        .cluster_zone_map_cached(&seg_path, &catalog, 0)
        .expect("zone map for catalog column k");
    assert_eq!(zm.min_value, ScalarValue::Int64(Some(1)));
    assert_eq!(zm.max_value, ScalarValue::Int64(Some(3)));
}
