use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::SegmentReader;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;
use std::path::{Path, PathBuf};

const TEST_DB: &str = "main";
const TEST_TABLE: &str = "t";

fn test_schema() -> Schema {
    Schema::new(vec![ColumnDef::new("id", DataType::Int64)])
}

fn make_batch(schema: &Schema, start_id: i64, rows: usize) -> Result<RecordBatch> {
    let mut batch = RecordBatch::new(schema.clone());
    for offset in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(start_id + offset as i64)))?;
    }
    Ok(batch)
}

fn test_config(max_rows_per_memtable: u64) -> BatchWriterConfig {
    BatchWriterConfig {
        max_rows_per_memtable,
        flush_interval_secs: 3600,
        max_total_memory: 1 << 30,
        worker_count: 1,
        low_rate_max_rows: 1024,
        low_rate_max_age_secs: 3600,
        spill_enabled: true,
        spill_min_age_secs: 0,
        tail_async_fsync: false,
    }
}

fn segment_files(table_dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(table_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("dat"))
        .collect();
    files.sort();
    files
}

fn total_segment_rows(table_dir: &Path) -> u64 {
    segment_files(table_dir)
        .iter()
        .map(|path| SegmentReader::open(path).unwrap().row_count())
        .sum()
}

fn tail_entries(table_dir: &Path) -> usize {
    let tail_dir = table_dir.join("memtable_tail");
    if !tail_dir.is_dir() {
        return 0;
    }
    std::fs::read_dir(tail_dir).unwrap().flatten().count()
}

#[test]
fn full_memtables_flush_while_write_queue_stays_active() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(4), Arc::clone(&segment_mgr), None);

    for batch_idx in 0..64 {
        writer.write_columns(
            TEST_DB,
            TEST_TABLE,
            "20251231",
            make_batch(&schema, batch_idx * 4, 4)?,
            schema.clone(),
        )?;
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while crate::storage::core::durable_tail::generation_file_count(&table_dir)? > 0
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    assert_eq!(
        crate::storage::core::durable_tail::generation_file_count(&table_dir)?,
        0
    );
    assert_eq!(total_segment_rows(&table_dir), 256);
    writer.stop();
    Ok(())
}

#[test]
fn tail_file_pressure_forces_table_flush() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);

    for partition in 0..4 {
        writer.write_columns(
            TEST_DB,
            TEST_TABLE,
            &partition.to_string(),
            make_batch(&schema, partition, 1)?,
            schema.clone(),
        )?;
    }
    assert_eq!(
        crate::storage::core::durable_tail::generation_file_count(&table_dir)?,
        4
    );

    let flushed = BatchWriter::flush_tail_file_pressure(
        &segment_mgr,
        writer.memtable_manager(),
        &writer.tail_committer,
        4,
    )?;

    assert_eq!(flushed, 1);
    assert_eq!(
        crate::storage::core::durable_tail::generation_file_count(&table_dir)?,
        0
    );
    assert_eq!(total_segment_rows(&table_dir), 4);
    writer.stop();
    Ok(())
}

/// Repeated durable memtable flushes must append into one shared active
/// segment instead of sealing one small segment per memtable.
#[test]
fn durable_flushes_share_one_active_segment() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;

    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(4), Arc::clone(&segment_mgr), None);

    for batch_idx in 0..3 {
        let batch = make_batch(&schema, batch_idx * 4, 4)?;
        writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
        writer.flush_table(TEST_DB, TEST_TABLE)?;
    }
    // A final partial memtable also lands in the same active segment.
    let batch = make_batch(&schema, 12, 2)?;
    writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
    writer.flush_table(TEST_DB, TEST_TABLE)?;
    writer.stop();

    let files = segment_files(&table_dir);
    assert_eq!(
        files.len(),
        1,
        "durable flushes should share one active segment, got {files:?}"
    );
    assert_eq!(total_segment_rows(&table_dir), 14);
    assert_eq!(tail_entries(&table_dir), 0, "flushed tails must be cleared");
    Ok(())
}

/// A crash before any flush leaves the durable tail on disk. Startup
/// recovery replays it exactly once into the active segment.
#[test]
fn crash_before_flush_replays_tail_exactly_once() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();

    {
        let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
        let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);
        let batch = make_batch(&schema, 100, 5)?;
        writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
        // No flush: stop only halts the background threads, leaving the
        // group-committed tail behind like a crash would.
        writer.stop();
    }
    assert!(tail_entries(&table_dir) > 0, "tail must survive the crash");
    assert_eq!(total_segment_rows(&table_dir), 0);

    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);
    let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].batch.num_rows(), 5);
    for tail in recovered {
        writer.memtable_manager().restore_committed_tail(
            tail.id,
            TEST_DB,
            TEST_TABLE,
            &tail.partition_key,
            tail.batch,
        );
    }
    writer.flush_table(TEST_DB, TEST_TABLE)?;
    writer.stop();

    assert_eq!(total_segment_rows(&table_dir), 5);
    assert!(
        crate::storage::core::durable_tail::recover(&table_dir, &schema)?.is_empty(),
        "replayed tail must not be recovered a second time"
    );
    Ok(())
}

/// A crash after the rows reached the segment but before tail cleanup is
/// detected through the flush-intent watermark: no replay, no duplicates.
#[test]
fn crash_after_segment_append_suppresses_tail_replay() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();

    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);
    let batch = make_batch(&schema, 200, 6)?;
    writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
    writer.flush_table(TEST_DB, TEST_TABLE)?;
    writer.stop();
    assert_eq!(total_segment_rows(&table_dir), 6);
    assert_eq!(tail_entries(&table_dir), 0);

    // Rebuild the crash window artificially: a tail plus a flush intent
    // whose watermark is already satisfied by the segment contents.
    let id = uuid::Uuid::new_v4();
    let batch = make_batch(&schema, 200, 6)?;
    crate::storage::core::durable_tail::persist(&table_dir, id, "", &batch)?;
    let segment_name = segment_files(&table_dir)[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let _intent =
        crate::storage::core::durable_tail::begin_flush(&table_dir, id, &segment_name, 6)?;

    let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
    assert!(
        recovered.is_empty(),
        "satisfied watermark must suppress replay; got {} tail(s)",
        recovered.len()
    );
    assert_eq!(tail_entries(&table_dir), 0, "recovery cleans up the intent");
    assert_eq!(total_segment_rows(&table_dir), 6);
    Ok(())
}

/// A crash in the middle of the append (segment below the watermark) must
/// replay the tail so no acknowledged rows are lost.
#[test]
fn crash_mid_append_replays_tail() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();

    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);
    let batch = make_batch(&schema, 300, 3)?;
    writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
    writer.flush_table(TEST_DB, TEST_TABLE)?;
    writer.stop();
    assert_eq!(total_segment_rows(&table_dir), 3);

    // A second memtable (3 more rows) began flushing: intent demands 6
    // rows in the shared segment, but the crash left only 3 durable.
    let id = uuid::Uuid::new_v4();
    let batch = make_batch(&schema, 303, 3)?;
    crate::storage::core::durable_tail::persist(&table_dir, id, "", &batch)?;
    let segment_name = segment_files(&table_dir)[0]
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let _intent =
        crate::storage::core::durable_tail::begin_flush(&table_dir, id, &segment_name, 6)?;

    let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
    assert_eq!(recovered.len(), 1, "unsatisfied watermark must replay");
    assert_eq!(recovered[0].batch.num_rows(), 3);
    Ok(())
}

/// Under memory pressure a cold committed memtable spills to its durable
/// tail instead of being flushed into a small row group; a later flush
/// materializes the rows exactly once.
#[test]
fn memory_pressure_spills_memtable_then_flush_materializes() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);

    let batch = make_batch(&schema, 0, 5)?;
    writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
    assert!(writer.memtable_manager().total_estimated_memory_bytes() > 0);

    // A 1-byte high-water mark forces the pass to spill everything.
    let spilled = BatchWriter::spill_pressure_pass(
        &segment_mgr,
        writer.memtable_manager(),
        &writer.tail_committer,
        1,
        0,
    );
    assert_eq!(spilled, 1);
    assert_eq!(writer.memtable_manager().total_estimated_memory_bytes(), 0);
    assert_eq!(
        total_segment_rows(&table_dir),
        0,
        "spilling must not cut a row group"
    );
    assert!(
        tail_entries(&table_dir) > 0,
        "spilled rows stay in the tail"
    );

    writer.flush_table(TEST_DB, TEST_TABLE)?;
    writer.stop();

    assert_eq!(total_segment_rows(&table_dir), 5);
    assert_eq!(tail_entries(&table_dir), 0, "flushed tails must be cleared");
    Ok(())
}

/// A spilled memtable keeps serving reads by materializing from its tail,
/// and new inserts land on top of the recovered rows.
#[test]
fn spilled_memtable_serves_reads_and_new_inserts() -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
    let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr), None);

    writer.write_columns(
        TEST_DB,
        TEST_TABLE,
        "",
        make_batch(&schema, 0, 3)?,
        schema.clone(),
    )?;
    let spilled = BatchWriter::spill_pressure_pass(
        &segment_mgr,
        writer.memtable_manager(),
        &writer.tail_committer,
        1,
        0,
    );
    assert_eq!(spilled, 1);

    let memtables = writer.memtable_manager().get_by_table(TEST_DB, TEST_TABLE);
    assert_eq!(memtables.len(), 1);
    assert!(memtables[0].is_spilled());
    let snapshot = memtables[0]
        .read_snapshot()
        .expect("spilled rows materialize");
    assert_eq!(snapshot.num_rows(), 3);

    writer.write_columns(TEST_DB, TEST_TABLE, "", make_batch(&schema, 3, 2)?, schema)?;
    let snapshot = writer.memtable_manager().get_by_table(TEST_DB, TEST_TABLE)[0]
        .read_snapshot()
        .unwrap();
    assert_eq!(snapshot.num_rows(), 5);
    drop(snapshot);
    writer.stop();
    Ok(())
}
