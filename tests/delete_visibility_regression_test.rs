//! Regression tests for delete visibility:
//! rows deleted via DELETE must disappear from GROUP BY scans, point selects
//! and aggregate fast paths alike, both before and after REPLACE reinserts.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

const DEAL_DATE: i64 = 20260904;
const KEY_A: &str = "A6666.TST";
const KEY_B: &str = "B2610.TST";

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn create_repro_table(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE t_repro (
                security_id VARCHAR(20),
                deal_date INT,
                deal_time INT,
                gmt_create TIMESTAMP,
                gmt_trade TIMESTAMP,
                high_px DECIMAL(12,4),
                low_px DECIMAL(12,4),
                trade_volume BIGINT,
                trade_value DECIMAL(18,2),
                PRIMARY KEY (security_id, deal_date, deal_time)
            ) CLUSTER BY (security_id, deal_time)
            PARTITION BY DAY(deal_date)
            HOT BY (gmt_trade) INDEX (security_id) CLOSE AT '21:00'",
        )
        .expect("create table");
}

fn insert_rows(engine: &Engine, session: &mut Session, key: &str, count: i64) {
    insert_rows_offset(engine, session, key, count, 0);
}

fn insert_rows_offset(engine: &Engine, session: &mut Session, key: &str, count: i64, offset: i64) {
    let mut next = 1i64;
    while next <= count {
        let end = (next + 99).min(count);
        let values: Vec<String> = (next..=end)
            .map(|t| {
                format!(
                    "('{}', {}, {}, 1788485400000, 1788485400000, 100.5, 99.5, 1, 1.0)",
                    key, DEAL_DATE, offset + t
                )
            })
            .collect();
        let sql = format!("INSERT INTO t_repro VALUES {}", values.join(","));
        engine.execute(session, &sql).expect("insert rows");
        next = end + 1;
    }
}

fn collect_rows(result: ExecResult) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|col_idx| batch.scalar(row_idx, col_idx).expect("scalar"))
                    .collect(),
            );
        }
    }
    rows
}

fn group_by_counts(engine: &Engine, session: &mut Session) -> Vec<(String, i64)> {
    let sql = format!(
        "SELECT security_id, COUNT(*) c FROM t_repro WHERE deal_date = {} GROUP BY deal_date, security_id",
        DEAL_DATE
    );
    let rows = collect_rows(engine.execute(session, &sql).expect("group by"));
    let mut out: Vec<(String, i64)> = rows
        .into_iter()
        .map(|r| match (&r[0], &r[1]) {
            (ScalarValue::VarChar(Some(k)), ScalarValue::Int64(Some(c))) => (k.clone(), *c),
            other => panic!("unexpected group row {:?}", other),
        })
        .collect();
    out.sort();
    out
}

fn point_count(engine: &Engine, session: &mut Session, key: &str) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM t_repro WHERE deal_date = {} AND security_id = '{}'",
        DEAL_DATE, key
    );
    let rows = collect_rows(engine.execute(session, &sql).expect("count"));
    match &rows[0][0] {
        ScalarValue::Int64(Some(v)) => *v,
        other => panic!("unexpected count {:?}", other),
    }
}

fn point_select(engine: &Engine, session: &mut Session, key: &str) -> usize {
    let sql = format!(
        "SELECT security_id, deal_time FROM t_repro WHERE deal_date = {} AND security_id = '{}' LIMIT 3",
        DEAL_DATE, key
    );
    collect_rows(engine.execute(session, &sql).expect("point select")).len()
}

fn delete_key(engine: &Engine, session: &mut Session, key: &str) {
    let sql = format!(
        "DELETE FROM t_repro WHERE deal_date = {} AND security_id = '{}'",
        DEAL_DATE, key
    );
    engine.execute(session, &sql).expect("delete");
}

fn replace_rows(engine: &Engine, session: &mut Session, key: &str, count: i64) {
    let mut next = 1i64;
    while next <= count {
        let end = (next + 99).min(count);
        let values: Vec<String> = (next..=end)
            .map(|t| {
                format!(
                    "('{}', {}, {}, 1788485700000, 1788485400000, 100.5, 99.5, 1, 1.0)",
                    key, DEAL_DATE, t
                )
            })
            .collect();
        let sql = format!("REPLACE INTO t_repro VALUES {}", values.join(","));
        engine.execute(session, &sql).expect("replace rows");
        next = end + 1;
    }
}

fn assert_groups(label: &str, engine: &Engine, session: &mut Session, expected: &[( &str, i64)]) {
    let mut want: Vec<(String, i64)> = expected
        .iter()
        .map(|(k, c)| (k.to_string(), *c))
        .collect();
    want.sort();
    let got = group_by_counts(engine, session);
    assert_eq!(got, want, "{}: group by mismatch", label);
}

#[test]
fn delete_visibility_after_flush() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_repro_table(&engine, &mut session);
    insert_rows(&engine, &mut session, KEY_A, 200);
    insert_rows(&engine, &mut session, KEY_B, 200);
    engine
        .execute(&mut session, "FLUSH TABLE t_repro")
        .expect("flush");

    assert_groups("baseline", &engine, &mut session, &[(KEY_A, 200), (KEY_B, 200)]);

    delete_key(&engine, &mut session, KEY_A);
    assert_eq!(point_count(&engine, &mut session, KEY_A), 0, "count after delete");
    assert_eq!(point_select(&engine, &mut session, KEY_A), 0, "point select after delete");
    assert_groups("after delete", &engine, &mut session, &[(KEY_B, 200)]);

    replace_rows(&engine, &mut session, KEY_A, 200);
    assert_eq!(point_count(&engine, &mut session, KEY_A), 200, "count after replace");
    assert_groups("after replace", &engine, &mut session, &[(KEY_A, 200), (KEY_B, 200)]);

    delete_key(&engine, &mut session, KEY_A);
    assert_groups("after second delete", &engine, &mut session, &[(KEY_B, 200)]);
    assert_eq!(point_select(&engine, &mut session, KEY_A), 0, "point select after second delete");

    engine.segment_mgr().shutdown();
}

#[test]
fn delete_visibility_memtable_only() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_repro_table(&engine, &mut session);
    insert_rows(&engine, &mut session, KEY_A, 200);
    insert_rows(&engine, &mut session, KEY_B, 200);

    delete_key(&engine, &mut session, KEY_A);
    assert_eq!(point_count(&engine, &mut session, KEY_A), 0, "count after delete");
    assert_eq!(point_select(&engine, &mut session, KEY_A), 0, "point select after delete");
    assert_groups("after delete", &engine, &mut session, &[(KEY_B, 200)]);

    replace_rows(&engine, &mut session, KEY_A, 200);
    assert_groups("after replace", &engine, &mut session, &[(KEY_A, 200), (KEY_B, 200)]);

    engine.segment_mgr().shutdown();
}

#[test]
fn delete_visibility_replace_after_flush() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_repro_table(&engine, &mut session);
    insert_rows(&engine, &mut session, KEY_A, 200);
    insert_rows(&engine, &mut session, KEY_B, 200);
    engine
        .execute(&mut session, "FLUSH TABLE t_repro")
        .expect("flush");
    assert_groups("baseline", &engine, &mut session, &[(KEY_A, 200), (KEY_B, 200)]);

    for round in 1..=3u32 {
        replace_rows(&engine, &mut session, KEY_A, 200);
        assert_groups(
            "after replace",
            &engine,
            &mut session,
            &[(KEY_A, 200), (KEY_B, 200)],
        );
        delete_key(&engine, &mut session, KEY_A);
        assert_eq!(
            point_count(&engine, &mut session, KEY_A),
            0,
            "count after delete round {}",
            round
        );
        assert_eq!(
            point_select(&engine, &mut session, KEY_A),
            0,
            "point select after delete round {}",
            round
        );
        assert_groups(
            "after delete",
            &engine,
            &mut session,
            &[(KEY_B, 200)],
        );
    }

    engine.segment_mgr().shutdown();
}

#[test]
fn delete_visibility_mixed_memtable_and_segment() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_repro_table(&engine, &mut session);
    insert_rows(&engine, &mut session, KEY_A, 200);
    insert_rows(&engine, &mut session, KEY_B, 200);
    engine
        .execute(&mut session, "FLUSH TABLE t_repro")
        .expect("flush");
    insert_rows_offset(&engine, &mut session, KEY_A, 200, 200);
    replace_rows(&engine, &mut session, KEY_B, 200);
    assert_groups(
        "mixed baseline",
        &engine,
        &mut session,
        &[(KEY_A, 400), (KEY_B, 200)],
    );
    delete_key(&engine, &mut session, KEY_A);
    assert_eq!(point_count(&engine, &mut session, KEY_A), 0, "count after delete");
    assert_eq!(point_select(&engine, &mut session, KEY_A), 0, "point select after delete");
    assert_groups("after delete", &engine, &mut session, &[(KEY_B, 200)]);

    engine.segment_mgr().shutdown();
}
