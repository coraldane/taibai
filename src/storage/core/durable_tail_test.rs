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
fn appending_migrates_legacy_snapshot_tail() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    let partition_key = "legacy";
    let legacy_batch = test_batch(&schema, &[1])?;
    let batch_payload = serialize_batch_for_wal(&legacy_batch);
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&TAIL_MAGIC.to_le_bytes());
    legacy.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    legacy.extend_from_slice(partition_key.as_bytes());
    legacy.extend_from_slice(&(batch_payload.len() as u64).to_le_bytes());
    legacy.extend_from_slice(&batch_payload);
    let legacy = append_crc(legacy);
    let tail_dir = temp.path().join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    fs::write(tail_dir.join(format!("{}.1.tail", id.simple())), legacy)?;

    append(temp.path(), id, partition_key, &test_batch(&schema, &[2])?)?;
    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert_eq!(
        recovered[0].batch.scalar(1, 0),
        Some(ScalarValue::Int64(Some(2)))
    );
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

#[test]
fn recover_index_counts_legacy_snapshot_tail_rows() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    let partition_key = "legacy";
    let legacy_batch = test_batch(&schema, &[7, 8])?;
    let batch_payload = serialize_batch_for_wal(&legacy_batch);
    let mut legacy = Vec::new();
    legacy.extend_from_slice(&TAIL_MAGIC.to_le_bytes());
    legacy.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    legacy.extend_from_slice(partition_key.as_bytes());
    legacy.extend_from_slice(&(batch_payload.len() as u64).to_le_bytes());
    legacy.extend_from_slice(&batch_payload);
    let legacy = append_crc(legacy);
    let tail_dir = temp.path().join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    fs::write(tail_dir.join(format!("{}.1.tail", id.simple())), legacy)?;

    let index = recover_index(temp.path(), &schema)?;
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].partition_key, "legacy");
    assert_eq!(index[0].rows, 2);
    assert!(index[0].path.is_file());

    let (_, batch) = decode_tail_file(&index[0].path, &schema)?;
    assert_eq!(batch.num_rows(), 2);
    Ok(())
}
