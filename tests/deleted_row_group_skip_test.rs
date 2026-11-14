//! Row groups whose every row the delete bitmap removes are skipped at
//! candidate enumeration: they decode to zero-row batches and would
//! otherwise defeat LIMIT early termination exactly like an expired TTL
//! row group does. The delete-filtering read path is unchanged for
//! partially deleted groups.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn collect_text(result: ExecResult) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut out = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            if let ScalarValue::VarChar(Some(line)) = batch.scalar(row, 0).expect("value") {
                out.push_str(&line);
                out.push('\n');
            }
        }
    }
    out
}

/// `row_groups    : A total, B read, C pruned (P%)` → field by name.
fn row_groups_field(report: &str, which: &str) -> u64 {
    let line = report
        .lines()
        .find(|l| l.trim_start().starts_with("row_groups"))
        .expect("row_groups line");
    let value = line.split_once(':').expect("counter").1;
    let numbers: Vec<u64> = value
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .filter_map(|p| p.parse().ok())
        .collect();
    let idx = match which {
        "total" => 0,
        "read" => 1,
        "pruned" => 2,
        other => panic!("unknown row_groups field {other}"),
    };
    numbers[idx]
}

fn scalar_i64(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut out = None;
    for batch in batches {
        for row in 0..batch.num_rows() {
            match batch.scalar(row, 0).expect("value") {
                ScalarValue::Int64(Some(v)) => out = Some(v),
                other => panic!("unexpected value {other:?}"),
            }
        }
    }
    out.expect("at least one row")
}

#[test]
fn fully_deleted_row_group_is_skipped_at_enumeration() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE TABLE delrg (id INT64, v DOUBLE)")
        .expect("create delrg");

    // Two row groups' worth of rows (default row_group_size = 8192) in one
    // bulk import, then sealed into a segment.
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    let values = (0..16_400)
        .map(|id| format!("({id}, 0.5)"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(&mut session, &format!("INSERT INTO delrg VALUES {values}"))
        .expect("bulk insert");
    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine
        .execute(&mut session, "FLUSH TABLE delrg")
        .expect("flush");
    engine
        .segment_mgr()
        .seal_table_active("main", "delrg")
        .expect("seal");

    // Every row of the first row group deleted: id 0..8191 covers exactly
    // row group 0 (rows are appended in id order).
    let deleted = engine
        .execute(&mut session, "DELETE FROM delrg WHERE id < 8192")
        .expect("delete");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 8192),
        other => panic!("expected ok, got {other:?}"),
    }

    let report = collect_text(
        engine
            .execute(&mut session, "EXPLAIN ANALYZE SELECT * FROM delrg LIMIT 1")
            .expect("explain"),
    );
    let read = row_groups_field(&report, "read");
    assert_eq!(
        read, 1,
        "only the live row group may be read; the fully-deleted one must be skipped at enumeration\n{report}"
    );

    // Correctness: the remaining rows are all there.
    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM delrg")
        .expect("count");
    assert_eq!(scalar_i64(count), 16_400 - 8_192);

    engine.segment_mgr().shutdown();
}
