use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::constants::DEFAULT_ROW_GROUP_SIZE;
use crate::storage::segment::SegmentWriter;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

fn test_batch(schema: &Schema, values: &[i64]) -> Result<RecordBatch> {
    let mut batch = RecordBatch::new(schema.clone());
    for value in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(*value)))?;
    }
    Ok(batch)
}

#[test]
fn latest_tail_generation_round_trips_and_clear_tombstones_it() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, id);
    assert_eq!(recovered[0].partition_key, "2026");
    assert_eq!(recovered[0].batch.num_rows(), 2);

    clear(temp.path(), id)?;
    assert!(recover(temp.path(), &schema)?.is_empty());
    assert!(!has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn appended_tail_deltas_recover_as_one_batch() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].partition_key, "2026");
    assert_eq!(recovered[0].batch.num_rows(), 3);
    assert_eq!(
        recovered[0].batch.scalar(2, 0),
        Some(ScalarValue::Int64(Some(3)))
    );
    Ok(())
}

#[test]
fn group_committer_acknowledges_only_recoverable_appends() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    let first = test_batch(&schema, &[1, 2])?;
    let second = test_batch(&schema, &[3, 4])?;
    let committer = TailCommitter::start(false);
    let first_ticket = committer.submit(
        temp.path().to_path_buf(),
        id,
        "2026".into(),
        PreparedTailAppend::new(&first),
    );
    let second_ticket = committer.submit(
        temp.path().to_path_buf(),
        id,
        "2026".into(),
        PreparedTailAppend::new(&second),
    );
    first_ticket.wait()?;
    second_ticket.wait()?;
    committer.barrier(temp.path())?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 4);
    assert_eq!(
        recovered[0].batch.scalar(3, 0),
        Some(ScalarValue::Int64(Some(4)))
    );
    committer.stop();
    Ok(())
}

#[test]
fn async_fsync_committer_still_leaves_recoverable_tail() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    let batch = test_batch(&schema, &[7, 8, 9])?;
    let committer = TailCommitter::start(true);
    let ticket = committer.submit(
        temp.path().to_path_buf(),
        id,
        "2026".into(),
        PreparedTailAppend::new(&batch),
    );
    ticket.wait()?;
    committer.barrier(temp.path())?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 3);
    assert_eq!(
        recovered[0].batch.scalar(2, 0),
        Some(ScalarValue::Int64(Some(9)))
    );
    committer.stop();
    Ok(())
}

#[test]
fn recovery_ignores_incomplete_final_tail_frame() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    let tail_path =
        newest_live_tail_path(&temp.path().join(TAIL_DIR), id)?.expect("tail should exist");
    let mut file = fs::OpenOptions::new().append(true).open(tail_path)?;
    file.write_all(&[0x08, 0x00, 0x00, 0x00])?;
    drop(file);

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 1);
    Ok(())
}

#[test]
fn incomplete_flush_recovers_tail_and_completed_flush_suppresses_it() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = test_batch(&schema, &[1, 2])?;
    let id = Uuid::new_v4();
    persist(temp.path(), id, "", &batch)?;

    let incomplete = begin_flush(temp.path(), id, "seg_00001.dat", 2)?;
    assert_eq!(recover(temp.path(), &schema)?.len(), 1);
    let _ = cancel_flush(incomplete);

    let _completed = begin_flush(temp.path(), id, "seg_00001.dat", 2)?;
    let mut writer = SegmentWriter::create(
        &temp.path().join("seg_00001.dat"),
        schema.clone(),
        DEFAULT_ROW_GROUP_SIZE,
        3,
    )?;
    writer.write_batch(&batch)?;
    writer.seal()?;
    assert!(recover(temp.path(), &schema)?.is_empty());
    assert!(!has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn flush_intent_watermark_requires_segment_row_count() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = test_batch(&schema, &[1, 2])?;
    let id = Uuid::new_v4();
    persist(temp.path(), id, "", &batch)?;

    let _intent = begin_flush(temp.path(), id, "seg_00001.dat", 6)?;
    let mut writer = SegmentWriter::create(
        &temp.path().join("seg_00001.dat"),
        schema.clone(),
        DEFAULT_ROW_GROUP_SIZE,
        3,
    )?;
    writer.write_batch(&batch)?;
    writer.seal()?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert!(has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn flush_intent_watermark_satisfied_by_shared_segment_growth() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let first = test_batch(&schema, &[1, 2])?;
    let second = test_batch(&schema, &[3, 4])?;
    let id = Uuid::new_v4();
    persist(temp.path(), id, "", &second)?;

    let _intent = begin_flush(temp.path(), id, "seg_00001.dat", 4)?;
    let mut writer = SegmentWriter::create(
        &temp.path().join("seg_00001.dat"),
        schema.clone(),
        DEFAULT_ROW_GROUP_SIZE,
        3,
    )?;
    writer.write_batch(&first)?;
    writer.write_batch(&second)?;
    writer.seal()?;

    assert!(recover(temp.path(), &schema)?.is_empty());
    assert!(!has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn batched_tail_queries_match_single_id_semantics() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let live = Uuid::new_v4();
    let cleared = Uuid::new_v4();
    let flushed = Uuid::new_v4();
    let unknown = Uuid::new_v4();
    persist(temp.path(), live, "2026", &test_batch(&schema, &[1])?)?;
    persist(temp.path(), cleared, "2026", &test_batch(&schema, &[2])?)?;
    clear(temp.path(), cleared)?;
    let flushed_batch = test_batch(&schema, &[3, 4])?;
    persist(temp.path(), flushed, "", &flushed_batch)?;
    let _intent = begin_flush(temp.path(), flushed, "seg_00001.dat", 2)?;
    let mut writer = SegmentWriter::create(
        &temp.path().join("seg_00001.dat"),
        schema.clone(),
        DEFAULT_ROW_GROUP_SIZE,
        3,
    )?;
    writer.write_batch(&flushed_batch)?;
    writer.seal()?;

    let answers = has_tail_batch(temp.path(), &[live, cleared, flushed, unknown])?;
    assert!(answers[&live]);
    assert!(!answers[&cleared]);
    assert!(!answers[&flushed]);
    assert!(!answers[&unknown]);

    let paths = live_tail_paths_batch(temp.path(), &[live, cleared, unknown])?;
    assert!(paths[&live].is_some());
    assert!(paths[&cleared].is_none());
    assert!(paths[&unknown].is_none());
    Ok(())
}

#[test]
fn finish_flush_batch_removes_tails_and_markers_in_one_pass() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let ids = [Uuid::new_v4(), Uuid::new_v4()];
    for id in ids {
        persist(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    }
    let intents = ids
        .iter()
        .map(|id| begin_flush(temp.path(), *id, "seg_00001.dat", 1))
        .collect::<Result<Vec<_>>>()?;

    finish_flush_batch(temp.path(), intents)?;

    for id in ids {
        assert!(!has_tail(temp.path(), id));
    }
    assert!(!temp.path().join(TAIL_DIR).exists());
    Ok(())
}

#[test]
fn v2_tail_log_reports_rows_without_decode() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

    let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
    assert_eq!(tail_log_rows(&path)?, Some(3));

    // A crash-truncated final frame still yields the valid prefix count.
    let mut bytes = fs::read(&path)?;
    bytes.extend_from_slice(&[0x08, 0x00, 0x00, 0x00]);
    assert_eq!(scan_tail_log_rows(&bytes), Some(3));
    Ok(())
}

#[test]
fn recover_index_counts_rows_and_keeps_tail_on_disk() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
    append(temp.path(), id, "2026", &test_batch(&schema, &[3])?)?;

    let index = recover_index(temp.path(), &schema)?;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].id, id);
    assert_eq!(index[0].partition_key, "2026");
    assert_eq!(index[0].rows, 3);
    assert!(index[0].path.is_file(), "indexing must keep the tail file");

    let (partition_key, batch) = decode_tail_file(&index[0].path, &schema)?;
    assert_eq!(partition_key, "2026");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.scalar(2, 0), Some(ScalarValue::Int64(Some(3))));

    // Arbitration cleanup is idempotent across repeated index passes.
    assert_eq!(recover_index(temp.path(), &schema)?.len(), 1);
    Ok(())
}

fn write_raw_tail_file(
    table_dir: &Path,
    id: Uuid,
    sequence: u64,
    kind: &str,
    bytes: &[u8],
) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    fs::write(
        tail_dir.join(format!("{}.{}.{}", id.simple(), sequence, kind)),
        bytes,
    )?;
    Ok(())
}

#[test]
fn crash_truncated_tail_headers_are_treated_as_absent() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let healthy = Uuid::new_v4();
    let empty = Uuid::new_v4();
    let partial_header = Uuid::new_v4();
    persist(temp.path(), healthy, "2026", &test_batch(&schema, &[1])?)?;
    write_raw_tail_file(temp.path(), empty, 10, "tail", &[])?;
    write_raw_tail_file(temp.path(), partial_header, 11, "tail", &[0x48, 0x4F, 0x4C])?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].id, healthy);
    assert!(!has_tail(temp.path(), empty));
    assert!(!has_tail(temp.path(), partial_header));
    Ok(())
}

#[test]
fn append_creates_fresh_tail_after_truncated_header_crash() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    write_raw_tail_file(temp.path(), id, 4, "tail", &[0x01, 0x02, 0x03])?;

    append(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 2);
    Ok(())
}

#[test]
fn mid_file_checksum_corruption_fails_recovery_loudly() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "", &test_batch(&schema, &[2, 3])?)?;

    let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
    let mut bytes = fs::read(&path)?;
    let last = bytes.len() - 6;
    bytes[last] ^= 0xFF;
    fs::write(&path, &bytes)?;

    assert!(
        recover(temp.path(), &schema).is_err(),
        "a corrupted committed frame must fail loudly instead of dropping rows"
    );
    let raw = fs::read(&path)?;
    assert_eq!(scan_tail_log_rows(&raw), None);
    assert!(recover_index(temp.path(), &schema).is_err());
    Ok(())
}

#[test]
fn torn_final_frame_payload_keeps_valid_prefix() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "", &test_batch(&schema, &[2, 3])?)?;

    let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
    let mut bytes = fs::read(&path)?;
    bytes.truncate(bytes.len() - 3);
    fs::write(&path, &bytes)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 1);
    assert_eq!(
        scan_tail_log_rows(&fs::read(&path)?),
        Some(1),
        "row watermark must count only the recoverable prefix"
    );
    Ok(())
}

#[test]
fn garbage_frame_length_is_treated_as_torn_frame() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "", &test_batch(&schema, &[2, 3])?)?;

    let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
    let mut bytes = fs::read(&path)?;
    let first_payload_len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let second_frame = 8 + 16 + first_payload_len + 4;
    bytes[second_frame..second_frame + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    fs::write(&path, &bytes)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 1);
    Ok(())
}

#[test]
fn flush_intent_with_missing_segment_keeps_tail_recoverable() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "", &test_batch(&schema, &[1, 2])?)?;

    let _intent = begin_flush(temp.path(), id, "seg_never_written.dat", 2)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert!(has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn corrupt_flush_marker_falls_back_to_tail() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
    write_raw_tail_file(temp.path(), id, 99, "flush", &[0xDE, 0xAD, 0xBE, 0xEF])?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 1);
    assert!(has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn newer_tail_generation_overrides_older_clear() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
    clear(temp.path(), id)?;
    assert!(!has_tail(temp.path(), id));

    append(temp.path(), id, "2026", &test_batch(&schema, &[2])?)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].partition_key, "2026");
    assert_eq!(
        recovered[0].batch.num_rows(),
        1,
        "a newer tail generation revives recovery after a clear tombstone; the new tail holds only its own delta"
    );
    assert_eq!(
        recovered[0].batch.scalar(0, 0),
        Some(ScalarValue::Int64(Some(2)))
    );
    Ok(())
}

#[test]
fn header_only_tail_indexes_as_zero_rows_and_blocks_decode() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    let header_only = [TAIL_LOG_MAGIC_V2.to_le_bytes(), 0u32.to_le_bytes()].concat();
    write_raw_tail_file(temp.path(), id, 7, "tail", &header_only)?;

    assert!(has_tail(temp.path(), id));
    let index = recover_index(temp.path(), &schema)?;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].rows, 0, "startup must skip a tail without frames");

    assert!(
        recover(temp.path(), &schema).is_err(),
        "eager decode rejects a header-only log: zero complete frames"
    );
    Ok(())
}

#[test]
fn recovered_tail_preserves_null_values_in_mixed_columns() -> Result<()> {
    let schema = Schema::new(vec![
        ColumnDef::new("sym", DataType::VarChar),
        ColumnDef::new("qty", DataType::Int64),
    ]);
    let temp = tempfile::tempdir()?;
    let id = Uuid::new_v4();
    let mut batch = RecordBatch::new(schema.clone());
    for row in 0..6i64 {
        let sym = if row % 2 == 0 {
            ScalarValue::VarChar(None)
        } else {
            ScalarValue::VarChar(Some(format!("SYM{}", row)))
        };
        let qty = if row == 0 {
            ScalarValue::Int64(None)
        } else {
            ScalarValue::Int64(Some(row * 1000))
        };
        batch.column_mut(0).unwrap().push(&sym).unwrap();
        batch.column_mut(1).unwrap().push(&qty).unwrap();
    }
    append(temp.path(), id, "", &batch)?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    let restored = &recovered[0].batch;
    assert_eq!(restored.num_rows(), 6);
    for row in 0..6usize {
        assert_eq!(
            restored.scalar(row, 0),
            batch.scalar(row, 0),
            "sym row {}",
            row
        );
        assert_eq!(
            restored.scalar(row, 1),
            batch.scalar(row, 1),
            "qty row {}",
            row
        );
    }
    Ok(())
}
