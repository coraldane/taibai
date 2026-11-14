//! Deterministic regression for the segment/memtable overlap window.
//!
//! A materialized scan pins its view of a table under the table flush read
//! lock — the segment directory listing plus the readable memtable batches —
//! and then decodes the segments with that lock released. An active segment
//! republishes its footer on every memtable flush, so a flush landing between
//! the snapshot and the decode used to make the scan read the flushed rows
//! twice: once from the segment's new row group and once from the memtable
//! batch the snapshot had already taken.
//!
//! The pause hook makes that window observable instead of relying on a
//! microsecond race, so this test fails deterministically without the
//! row-group ceiling in the snapshot.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::sql::execution::scan::executor::set_snapshot_pause_for_tests;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn int64_column_values(result: ExecResult) -> Vec<i64> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            if let Some(taibai::types::scalar::ScalarValue::Int64(Some(value))) =
                batch.scalar(row_idx, 0)
            {
                values.push(value);
            }
        }
    }
    values
}

#[test]
fn scan_does_not_read_row_groups_flushed_after_its_snapshot() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_snapshot_bound (id INT64 AUTO_INCREMENT PRIMARY KEY, pad INT32)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_snapshot_bound (pad) VALUES (1), (2), (3)",
        )
        .expect("seed rows");
    // Materialize ids 1..=3 as the active segment's first row group.
    engine
        .flush_table_buffers("main", "t_snapshot_bound")
        .expect("seed flush");
    // ids 4..=6 stay in a live memtable for the scan's snapshot to pick up.
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_snapshot_bound (pad) VALUES (7), (8), (9)",
        )
        .expect("pending rows");

    set_snapshot_pause_for_tests(800);
    let reader_engine = Arc::clone(&engine);
    let reader = thread::spawn(move || {
        let mut s = session();
        int64_column_values(
            reader_engine
                .execute(&mut s, "SELECT id FROM t_snapshot_bound WHERE id >= 0")
                .expect("scan across the flush"),
        )
    });

    // Land the flush strictly inside the scan's snapshot-to-decode window, so
    // the segment the scan opens carries a row group that did not exist when
    // it snapshotted — the rows 4..=6 it already holds from the memtable.
    thread::sleep(Duration::from_millis(200));
    engine
        .flush_table_buffers("main", "t_snapshot_bound")
        .expect("flush during the scan");
    let ids = reader.join().expect("reader thread");
    set_snapshot_pause_for_tests(0);

    let mut sorted = ids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        ids.len(),
        "scan returned the same row twice across the segment/memtable legs: {ids:?}"
    );
    assert_eq!(
        sorted,
        vec![1, 2, 3, 4, 5, 6],
        "scan must see the pre-snapshot rows and nothing appended after it"
    );

    let mut verifier = session();
    let final_ids = int64_column_values(
        engine
            .execute(
                &mut verifier,
                "SELECT id FROM t_snapshot_bound WHERE id >= 0",
            )
            .expect("final scan"),
    );
    assert_eq!(final_ids.len(), 6, "flush must not lose or duplicate rows");
    engine.segment_mgr().shutdown();
}
