//! Deferred (V02) delete-footer journal: discovery skip and the startup
//! three-state judgement (roll back / roll forward / interrupted apply).
//! This is the crash-atomicity leg of the multi-leg UPDATE protocol (P0: a
//! crash between the tombstone fsync and the image commit used to leave the
//! matched rows deleted with no replacement and no trace).

use super::reader::{
    clear_deferred_delete_journal, clear_deferred_delete_journal_for_statement,
    clear_deferred_delete_journal_when_satisfied, drain_footer_rollback_compensations,
    footer_rollback_log_path, recover_statement_footer_journals, FooterRewriteMode,
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
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
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

/// The conditional in-process clear must NOT remove a journal whose
/// watermark is unmet — that decision belongs to startup recovery (or to
/// the owning statement after its own images are durable).
#[test]
fn conditional_clear_keeps_unmet_journal() {
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

    // Watermark points at a memtable with no durable tail: never satisfied.
    clear_deferred_delete_journal_when_satisfied(&table_dir, &segment);
    assert!(
        journal_path(&segment).exists(),
        "unmet watermark keeps the journal"
    );
    assert_eq!(rows_visible(&segment), 3);

    // Startup recovery still owns the final judgement.
    recover_statement_footer_journals(&table_dir);
    assert_eq!(rows_visible(&segment), 4);
}

/// The statement-owned clear removes the journal when its redo matches the
/// caller's rewrite (even with an unmet tail watermark — the images may be
/// durable in a rescued segment flush), but leaves a LATER statement's
/// journal to its own watermark judgement.
#[test]
fn statement_clear_matches_ownership_not_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);

    // Our rewrite: rows 0-1 tombstoned, watermark bogus (unmet).
    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_0011],
        2,
        deferred(vec![random_check(2)]),
    )
    .unwrap();

    // A foreign state (a later statement wrote more tombstones): kept.
    clear_deferred_delete_journal_for_statement(&table_dir, &segment, &[0b0000_0111], 3);
    assert!(
        journal_path(&segment).exists(),
        "a later statement's journal is judged on its own watermark"
    );

    // Our exact state: removed despite the unmet watermark.
    clear_deferred_delete_journal_for_statement(&table_dir, &segment, &[0b0000_0011], 2);
    assert!(
        !journal_path(&segment).exists(),
        "the owning statement clears its journal unconditionally"
    );
    assert_eq!(rows_visible(&segment), 2);
}

/// Standby compensation (P3): a rollback performed by startup recovery is
/// recorded durably as `db\ttable\tsegment`; draining the log re-emits the
/// segment's CURRENT (restored) footer state and truncates the log.
#[test]
fn rollback_recorded_then_drained_with_restored_footer_state() {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = seeded_table_dir(dir.path());
    let segment = table_dir.join("seg_00001.dat");
    seed_sealed_segment(&segment);

    SegmentReader::rewrite_footer_with_deletes(
        &segment,
        &[0b0000_1111],
        4,
        deferred(vec![random_check(4)]),
    )
    .unwrap();
    let rollback_log = footer_rollback_log_path(dir.path());
    assert!(!rollback_log.exists(), "no rollback recorded yet");

    recover_statement_footer_journals(&table_dir);

    let recorded = std::fs::read_to_string(&rollback_log).unwrap();
    assert_eq!(
        recorded,
        format!("main\tt\t{}\n", segment.display()),
        "rollback record format is db<TAB>table<TAB>segment"
    );

    // Drain: the emitted state is the RESTORED footer (no tombstones), and
    // the log is truncated afterwards.
    let mut emitted: Vec<(PathBuf, Vec<u8>, u64)> = Vec::new();
    drain_footer_rollback_compensations(dir.path(), |path, bitmap, deleted_count| {
        emitted.push((path.to_path_buf(), bitmap.to_vec(), deleted_count));
    });
    assert_eq!(emitted.len(), 1, "one compensation per rollback record");
    assert_eq!(emitted[0].0, segment);
    assert_eq!(emitted[0].1, Vec::<u8>::new(), "restored bitmap is empty");
    assert_eq!(emitted[0].2, 0, "restored deleted count is zero");
    assert!(
        std::fs::read_to_string(&rollback_log)
            .unwrap()
            .trim()
            .is_empty(),
        "drain truncates the rollback log"
    );

    // Draining again finds nothing.
    drain_footer_rollback_compensations(dir.path(), |_, _, _| panic!("log is empty"));
}

/// A missing rollback log is a no-op (standby/none deployments never write
/// one), and records whose segment vanished (dropped table) are skipped
/// without failing the drain.
#[test]
fn drain_handles_missing_log_and_missing_segments() {
    let dir = tempfile::tempdir().unwrap();
    // Missing log: no panic, no file created.
    drain_footer_rollback_compensations(dir.path(), |_, _, _| panic!("no records"));
    assert!(!footer_rollback_log_path(dir.path()).exists());

    // A record whose segment no longer exists is skipped; the log is still
    // truncated so it cannot grow forever.
    let rollback_log = footer_rollback_log_path(dir.path());
    std::fs::write(
        &rollback_log,
        format!(
            "main\tt\t{}\n",
            dir.path().join("main").join("t").join("gone.dat").display()
        ),
    )
    .unwrap();
    drain_footer_rollback_compensations(dir.path(), |_, _, _| panic!("segment is gone"));
    assert!(rollback_log.exists());
    assert!(std::fs::read_to_string(&rollback_log).unwrap().is_empty());
}
