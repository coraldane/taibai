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
        PreparedTailAppend::new(&first)?,
    );
    let second_ticket = committer.submit(
        temp.path().to_path_buf(),
        id,
        "2026".into(),
        PreparedTailAppend::new(&second)?,
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
        PreparedTailAppend::new(&batch)?,
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
    let frame_header_len = tail_frame_header_len(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
    )
    .expect("current tail format");
    let second_frame = 8 + frame_header_len + first_payload_len + 4;
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

#[test]
fn v3_frames_carry_monotonic_lsn_and_commit_timestamp() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
    append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

    let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
    let bytes = fs::read(&path)?;
    let (partition, batch, frames) = decode_tail_with_meta(&bytes, &schema)?;
    assert_eq!(partition, "2026");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].rows, 1);
    assert_eq!(frames[1].rows, 2);
    assert!(frames[0].lsn < frames[1].lsn, "LSNs must be monotonic");
    // Timestamps are wall-clock micros: after 2023-11-01 and not in the future.
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64;
    for frame in &frames {
        assert!(frame.commit_ts_micros > 1_699_000_000_000_000);
        assert!(frame.commit_ts_micros <= now_micros);
    }
    // The row-count scan still answers from headers alone.
    assert_eq!(tail_log_rows(&path)?, Some(3));
    Ok(())
}

#[test]
fn v2_tail_log_still_decodes_with_zero_metadata() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let batch = test_batch(&schema, &[7, 8])?;
    // Hand-build a V2-format tail log: 16-byte frame header.
    let payload = serialize_batch_for_wal(&batch)?;
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&2u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&frame);
    frame.extend_from_slice(&hasher.finalize().to_le_bytes());
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TAIL_LOG_MAGIC_V2.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&frame);
    let (partition, decoded, frames) = decode_tail_with_meta(&bytes, &schema)?;
    assert_eq!(partition, "");
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].lsn, 0);
    assert_eq!(frames[0].commit_ts_micros, 0);
    assert_eq!(frames[0].rows, 2);
    Ok(())
}

#[test]
fn dispose_tail_file_archives_only_when_enabled() -> Result<()> {
    let temp = tempfile::tempdir()?;
    // Proper <data>/<db>/<table> nesting so the archive root derives to
    // <data>/wal/<db>/<table>/memtable_tail.
    let table_dir = temp.path().join("data").join("main").join("t");
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(&table_dir, id, "", &test_batch(&schema, &[1])?)?;
    let tail_dir = table_dir.join(TAIL_DIR);
    let tail_file = live_tail_path(&table_dir, id)?.expect("tail should exist");

    // Disabled: the tail is deleted (historic behavior).
    dispose_tail_file(&tail_dir, &tail_file, false);
    assert!(!tail_file.exists());

    // Re-create and dispose with archive enabled: the file moves under
    // <data>/wal/<db>/<table>/memtable_tail/.
    persist(&table_dir, id, "", &test_batch(&schema, &[2])?)?;
    let tail_file = live_tail_path(&table_dir, id)?.expect("tail should exist");
    dispose_tail_file(&tail_dir, &tail_file, true);
    assert!(!tail_file.exists(), "moved out of the tail dir");
    // db dir inside tempdir = temp/<db>; persist created temp path directly,
    // so the archive layout derives the db name from the parent chain.
    let archived = find_archived_tail(temp.path());
    if archived.is_none() {
        let wal = temp.path().join("data").join("wal");
        eprintln!("wal tree: {}", wal.display());
        for entry in walk(&wal) {
            eprintln!("  {}", entry.display());
        }
        panic!("archived copy must exist");
    }
    Ok(())
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

fn find_archived_tail(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let wal = root.join("data").join("wal");
    for db in fs::read_dir(&wal).ok()? {
        let db = db.ok()?.path();
        for table in fs::read_dir(&db).ok()? {
            let table = table.ok()?.path().join(TAIL_DIR);
            for entry in fs::read_dir(&table).ok()? {
                let entry = entry.ok()?.path();
                if entry.extension().and_then(|e| e.to_str()) == Some("tail") {
                    return Some(entry);
                }
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Delete sidecars (`<id>.<seq>.del`)
// ---------------------------------------------------------------------------

#[test]
fn delete_sidecar_round_trips_through_recovery() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[0, 1, 2, 3, 4, 5])?)?;

    assert!(delete_rows(temp.path(), id, &mut [1, 3])?);
    // A sidecar pend: rows still exist, but the tail is no longer "plain"
    // (a spill must not run over it).
    assert!(has_tail(temp.path(), id));
    assert!(!tail_is_plain(temp.path(), id)?);

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 4);
    let values: Vec<Option<ScalarValue>> =
        (0..4).map(|row| recovered[0].batch.scalar(row, 0)).collect();
    assert_eq!(
        values,
        vec![
            Some(ScalarValue::Int64(Some(0))),
            Some(ScalarValue::Int64(Some(2))),
            Some(ScalarValue::Int64(Some(4))),
            Some(ScalarValue::Int64(Some(5))),
        ]
    );
    Ok(())
}

#[test]
fn stacked_delete_sidecars_apply_oldest_first() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[10, 20, 30, 40])?)?;
    // First delete speaks in the original batch's coordinates (removes 20)...
    delete_rows(temp.path(), id, &mut [1])?;
    // ...the second in the post-first-delete batch's coordinates:
    // [10, 30, 40], position 0 removes 10.
    delete_rows(temp.path(), id, &mut [0])?;

    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert_eq!(
        recovered[0].batch.scalar(0, 0),
        Some(ScalarValue::Int64(Some(30)))
    );
    assert_eq!(
        recovered[0].batch.scalar(1, 0),
        Some(ScalarValue::Int64(Some(40)))
    );
    Ok(())
}

#[test]
fn insert_delta_after_delete_keeps_positions_prefix_stable() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2, 3])?)?;
    delete_rows(temp.path(), id, &mut [1])?;
    // New rows append at the end; recorded positions must keep referring to
    // the original prefix.
    append(temp.path(), id, "2026", &test_batch(&schema, &[4])?)?;

    let recovered = recover(temp.path(), &schema)?;
    let values: Vec<i64> = (0..recovered[0].batch.num_rows())
        .filter_map(|row| match recovered[0].batch.scalar(row, 0) {
            Some(ScalarValue::Int64(Some(v))) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(values, vec![1, 3, 4]);
    Ok(())
}

#[test]
fn torn_delete_sidecar_fails_recovery_closed() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2, 3])?)?;
    delete_rows(temp.path(), id, &mut [0])?;

    // Simulate on-disk corruption: a truncated sidecar must fail the whole
    // recovery instead of silently resurrecting the deleted row.
    let tail_dir = temp.path().join(TAIL_DIR);
    let sidecar = std::fs::read_dir(&tail_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .find(|path| path.extension().and_then(|e| e.to_str()) == Some("del"))
        .expect("sidecar exists");
    let bytes = std::fs::read(&sidecar)?;
    std::fs::write(&sidecar, &bytes[..bytes.len() - 3])?;

    assert!(recover(temp.path(), &schema).is_err());
    Ok(())
}

#[test]
fn persist_supersedes_pending_sidecars() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2, 3])?)?;
    delete_rows(temp.path(), id, &mut [0])?;
    // The DELETE statement's in-memory result, persisted as a fresh
    // generation: sidecars below it are superseded and must not re-apply.
    persist(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

    assert!(tail_is_plain(temp.path(), id)?);
    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert_eq!(
        recovered[0].batch.scalar(0, 0),
        Some(ScalarValue::Int64(Some(2)))
    );
    Ok(())
}

#[test]
fn clear_tombstone_removes_sidecars_too() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
    delete_rows(temp.path(), id, &mut [0])?;
    clear(temp.path(), id)?;
    assert!(recover(temp.path(), &schema)?.is_empty());
    assert!(!has_tail(temp.path(), id));
    Ok(())
}

#[test]
fn recover_index_folds_pending_sidecars_into_a_plain_tail() -> Result<()> {
    // The engine's lazy recovery reads only the tail file, so recover_index
    // must consolidate pending delete sidecars at startup: the deleted row
    // must not resurrect when the memtable materializes later.
    let temp = tempfile::tempdir()?;
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let id = Uuid::new_v4();
    persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2, 3])?)?;
    delete_rows(temp.path(), id, &mut [0])?;

    let indexed = recover_index(temp.path(), &schema)?;
    assert_eq!(indexed.len(), 1);
    assert_eq!(indexed[0].rows, 2);

    // After folding, the tail is plain and decodes to the filtered batch.
    assert!(tail_is_plain(temp.path(), id)?);
    let recovered = recover(temp.path(), &schema)?;
    assert_eq!(recovered[0].batch.num_rows(), 2);
    assert_eq!(
        recovered[0].batch.scalar(0, 0),
        Some(ScalarValue::Int64(Some(2)))
    );
    Ok(())
}
