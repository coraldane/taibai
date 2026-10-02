//! Deferred (V02) delete-footer journal: discovery skip and the startup
//! three-state judgement (roll back / roll forward / interrupted apply).
//! This is the crash-atomicity leg of the multi-leg UPDATE protocol (P0: a
//! crash between the tombstone fsync and the image commit used to leave the
//! matched rows deleted with no replacement and no trace).

use super::reader::{
    clear_deferred_delete_journal, recover_statement_footer_journals, FooterRewriteMode,
    ImageDurabilityCheck, SegmentReader,
};
use super::store::discover_segments;
use super::writer::SegmentWriter;
use crate::protocol::types::DataType;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;
use std::path::{Path, PathBuf};

fn journal_path(segment: &Path) -> PathBuf {
    let mut name = segment.as_os_str().to_os_string();
    name.push(".del");
    PathBuf::from(name)
}

fn seeded_table_dir(dir: &Path) -> PathBuf {
    let table_dir = dir.join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();
    table_dir
}

fn seed_sealed_segment(segment: &Path) {
    let schema = Schema::new(vec![crate::types::batch::ColumnDef::new(
        "id",
        DataType::Int64,
    )]);
    let mut batch = RecordBatch::with_capacity(schema.clone(), 4);
    for id in 1..=4i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
    }
    let mut writer = SegmentWriter::create(segment, schema, 8192, 3).unwrap();
    writer.write_batch(&batch).unwrap();
    writer.seal().unwrap();
}

fn rows_visible(segment: &Path) -> u64 {
    let reader = SegmentReader::open(segment).unwrap();
    reader.row_count() - reader.deleted_row_count()
}

fn deferred(checks: Vec<ImageDurabilityCheck>) -> FooterRewriteMode {
    FooterRewriteMode::Deferred(checks)
}

fn random_check(expected_rows: u64) -> ImageDurabilityCheck {
    ImageDurabilityCheck {
        memtable_id: uuid::Uuid::new_v4(),
        partition: String::new(),
        expected_rows,
    }
}

/// The deferred journal survives the rewrite (unlike the immediate
/// lifecycle) and the tombstones are visible in the segment.
#[test]
fn deferred_journal_survives_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_0011],
        2,
        deferred(vec![random_check(1)]),
    )
    .unwrap();

    assert!(journal_path(&segment).exists(), "deferred journal survives");
    assert_eq!(rows_visible(&segment), 2);
}

/// Discovery must NOT judge a deferred journal (a statement may be in
/// flight); the segment stays readable with the tombstoned footer.
#[test]
fn discovery_leaves_deferred_journals_alone() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_0001],
        1,
        deferred(vec![random_check(1)]),
    )
    .unwrap();

    let discovered = discover_segments(&table_dir).unwrap();
    assert_eq!(discovered.len(), 1);
    assert!(
        journal_path(&segment).exists(),
        "discovery must not consume a deferred journal"
    );
    assert_eq!(rows_visible(&segment), 3);
}

/// Startup judgement, roll-BACK case: footer applied but the image watermark
/// is not durable (the crash window) — recovery restores the pre-statement
/// footer and consumes the journal.
#[test]
fn startup_recovery_rolls_back_when_images_not_durable() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);
    let before = rows_visible(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_1111],
        4,
        deferred(vec![random_check(4)]),
    )
    .unwrap();
    assert_eq!(rows_visible(&segment), 0, "all rows tombstoned");

    recover_statement_footer_journals(&table_dir);

    assert_eq!(rows_visible(&segment), before, "footer rolled back");
    assert!(!journal_path(&segment).exists(), "journal consumed");
    // Idempotent: a second pass finds nothing to do.
    recover_statement_footer_journals(&table_dir);
    assert_eq!(rows_visible(&segment), before);
}

/// Startup judgement, interrupted-apply case: the footer bytes never landed
/// (a crash between set_len and the write) — recovery restores from the
/// journal's UNDO, not its redo.
#[test]
fn startup_recovery_restores_torn_footer_from_undo() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);
    let before = rows_visible(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_0111],
        3,
        deferred(vec![random_check(3)]),
    )
    .unwrap();

    // Tear the embedded footer the way a crash between set_len and the write
    // would: truncate the file into the middle of the data block.
    let torn_len = segment.metadata().unwrap().len() / 2;
    let file = std::fs::OpenOptions::new().write(true).open(&segment).unwrap();
    file.set_len(torn_len).unwrap();
    drop(file);
    assert!(SegmentReader::open(&segment).is_err());

    recover_statement_footer_journals(&table_dir);

    assert_eq!(rows_visible(&segment), before, "undo footer restored");
    assert!(!journal_path(&segment).exists());
}

/// The caller-side clear after a durable commit is the roll-forward path's
/// in-process twin: clearing removes the journal so recovery never judges a
/// completed statement.
#[test]
fn caller_clear_removes_deferred_journal() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_0001],
        1,
        deferred(vec![random_check(1)]),
    )
    .unwrap();
    assert!(journal_path(&segment).exists());

    clear_deferred_delete_journal(&segment);
    assert!(!journal_path(&segment).exists());
    assert_eq!(rows_visible(&segment), 3);

    // Idempotent clear + recovery no-op.
    clear_deferred_delete_journal(&segment);
    recover_statement_footer_journals(&table_dir);
    assert_eq!(rows_visible(&segment), 3);
}
