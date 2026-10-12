//! In-memory/external merge equivalence and bounded rewrite regressions.

use super::*;

fn run_external_for_test(c: &Compactor, dir: &Path, schema: &Schema) -> Result<CompactStats> {
    let files = SegmentManager::discover_segments(dir)?;
    let final_path = c.segment_mgr.next_replacement_segment_path(dir, None)?;
    let tmp = store::replacement_tmp_path(&final_path, "compact_tmp");
    let (rows, dropped, ttl) = c.prepare_clustered_external(
        "default",
        "t",
        "none",
        &tmp,
        &final_path,
        schema,
        &[0],
        &[0],
        &files,
        None,
        TEST_ROW_GROUP_SIZE,
        3,
    )?;
    c.segment_mgr
        .replace_segments_with_tmp(&tmp, &final_path, &files)?;
    Ok(CompactStats {
        segments_read: files.len(),
        segments_written: 1,
        rows_written: rows as u64,
        rows_dropped: dropped,
        rows_dropped_ttl: ttl,
    })
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

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = run_external_for_test(&compactor, &table_dir, &schema)?;

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
        SegmentReader::rewrite_footer_with_deletes(
            &table_dir.join("seg_00001.dat"),
            &bitmap,
            1,
            crate::storage::segment::reader::FooterRewriteMode::Immediate,
        )?;
    }

    let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
    let stats = run_external_for_test(&compactor, &table_dir, &schema)?;
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(stats.rows_written, 2);
    assert_eq!(stats.rows_dropped, 1);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(read_ids(&segments[0])?, vec![4, 6]);
    Ok(())
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
        for (seq, ids) in [&[5i64, 1, 8][..], &[3, 9, 2], &[7, 4, 6]]
            .iter()
            .enumerate()
        {
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
        let stats =
            compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;
        assert_eq!(stats.rows_written, 9);
        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        read_ids_and_row_group_sizes(&segments[0])
    };

    // Default budget: the whole 9-row ref buffer fits, single pass.
    let single_pass = build(tempfile::tempdir()?)?;

    // Tiny budget (3 refs < 9 live rows): the buffer must drain mid-scan in
    // whole-chunk flushes instead of collecting every ref up front.
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(ROW_REF_BYTES * 3, std::sync::atomic::Ordering::Relaxed);
    let budgeted = build(tempfile::tempdir()?)?;
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, std::sync::atomic::Ordering::Relaxed);

    // Same rows, same source order, same row-group layout as the
    // single-pass rewrite.
    assert_eq!(
        single_pass,
        (vec![5, 1, 8, 3, 9, 2, 7, 4, 6], vec![2, 2, 2, 2, 1])
    );
    assert_eq!(budgeted, single_pass);
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
fn exhausted_runs_still_flush_output_chunks() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let schema = test_schema();
    let mut paths = Vec::new();
    for id in 0..7 {
        let path = tmp.path().join(format!("run-{id}.dat"));
        write_segment(&path, &schema, &[(id, "value")])?;
        paths.push(path);
    }
    let output = tmp.path().join("output.dat");
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(mgr, tmp.path().to_path_buf());
    // A larger writer target exposes the merge buffer's own chunk bound.
    let mut writer = SegmentWriter::create(&output, schema.clone(), 100, 3)?;
    assert_eq!(
        compactor.merge_runs_into_writer(&paths, &schema, &[0], 2, &mut writer)?,
        7
    );
    writer.seal()?;
    let (ids, sizes) = read_ids_and_row_group_sizes(&output)?;
    assert_eq!(ids, (0..7).collect::<Vec<_>>());
    assert_eq!(sizes, vec![2, 2, 2, 1]);
    Ok(())
}

#[test]
fn compaction_preserves_catalog_column_identity_after_schema_changes() -> Result<()> {
    for mode in 0..3 {
        let tmp = tempfile::tempdir()?;
        let table_dir = tmp.path().join("default/t");
        std::fs::create_dir_all(&table_dir)?;
        let old_schema = Schema::new(vec![
            ColumnDef::new("removed", DataType::Int64),
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("payload", DataType::Int64),
        ]);
        let mut renamed = ColumnDef::new("renamed_id", DataType::Int64);
        renamed.previous_names.push("id".into());
        let schema = Schema::new(vec![
            renamed,
            ColumnDef::new("payload", DataType::Int64),
            ColumnDef::new("added", DataType::Int64),
        ]);
        for (seq, id) in [2i64, 1].into_iter().enumerate() {
            let mut batch = RecordBatch::new(old_schema.clone());
            for (col, value) in [999, id, id * 10].into_iter().enumerate() {
                batch
                    .column_mut(col)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(value)))?;
            }
            let path = table_dir.join(format!("seg_{:05}.dat", seq + 1));
            let mut writer = SegmentWriter::create(&path, old_schema.clone(), 8192, 3)?;
            writer.write_batch(&batch)?;
            writer.seal()?;
        }
        let mgr = Arc::new(SegmentManager::new(tmp.path())?);
        let compactor = Compactor::new(mgr, tmp.path().to_path_buf());
        let mut meta = test_meta_without_cluster(&schema);
        if mode != 0 {
            meta.cluster_key = vec!["renamed_id".into()];
        }
        if mode == 2 {
            let inputs = SegmentManager::discover_segments(&table_dir)?;
            let output = table_dir.join("seg_00003.dat");
            let stats = compactor.prepare_clustered_external(
                "default",
                "t",
                "none",
                &output,
                &output,
                &schema,
                &[0],
                &[0],
                &inputs,
                None,
                2,
                3,
            )?;
            assert_eq!(stats, (2, 0, 0));
            for path in inputs {
                std::fs::remove_file(path)?;
            }
        } else {
            compactor.optimize_table("default", "t", &meta)?;
        }
        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        let reader = SegmentReader::open(&segments[0])?;
        let batch = reader.read_row_group(0)?;
        let expected = if mode != 0 { [1, 2] } else { [2, 1] };
        for (row, id) in expected.into_iter().enumerate() {
            assert_eq!(batch.scalar(row, 0), Some(ScalarValue::Int64(Some(id))));
            assert_eq!(
                batch.scalar(row, 1),
                Some(ScalarValue::Int64(Some(id * 10)))
            );
            assert_eq!(batch.scalar(row, 2), Some(ScalarValue::Int64(None)));
        }
    }
    Ok(())
}

#[test]
fn variable_width_gather_preserves_interleaved_values_and_nulls() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let schema = Schema::new(vec![
        ColumnDef::new("text", DataType::VarChar),
        ColumnDef::new("bytes", DataType::Blob),
    ]);
    let rows = [
        (Some("长字符串".repeat(200)), Some(vec![1u8; 1000])),
        (None, Some(Vec::new())),
        (Some(String::new()), None),
        (Some("tail".into()), Some(vec![0, 255])),
    ];
    let mut sources = Vec::new();
    for (idx, values) in rows.chunks(2).enumerate() {
        let mut batch = RecordBatch::new(schema.clone());
        for (text, bytes) in values {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(text.clone()))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::Blob(bytes.clone()))?;
        }
        let path = tmp.path().join(format!("source-{idx}.dat"));
        let mut writer = SegmentWriter::create(&path, schema.clone(), 2, 3)?;
        writer.write_batch(&batch)?;
        writer.seal()?;
        sources.push(SourceRowGroup {
            segment_path: path,
            row_group_idx: 0,
        });
    }
    let order = [3, 0, 2, 1];
    let refs = order.map(|row| RowRef {
        batch_idx: row / 2,
        row_idx: row % 2,
    });
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let compactor = Compactor::new(mgr, tmp.path().to_path_buf());
    let mut cache = RowGroupCache::new(1, schema.clone());
    let batch = compactor.gather_cached_chunk(&schema, &sources, &refs, &mut cache)?;
    for (out, row) in order.into_iter().enumerate() {
        assert_eq!(
            batch.scalar(out, 0),
            Some(ScalarValue::VarChar(rows[row].0.clone()))
        );
        assert_eq!(
            batch.scalar(out, 1),
            Some(ScalarValue::Blob(rows[row].1.clone()))
        );
    }
    Ok(())
}
