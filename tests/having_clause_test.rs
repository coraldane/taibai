use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
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

fn seed_minute_table(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE t_stock_minute (
                security_id VARCHAR(20),
                deal_date INT,
                deal_time INT
            )",
        )
        .expect("create t_stock_minute");
    engine
        .execute(
            session,
            "INSERT INTO t_stock_minute VALUES
                ('000909.SZ', 20260904, 930),
                ('000909.SZ', 20260904, 1130),
                ('000909.SZ', 20260904, 1301),
                ('000909.SZ', 20260904, 1459),
                ('603050.SH', 20260904, 930),
                ('603050.SH', 20260904, 1459),
                ('000862.SZ', 20260904, 925),
                ('000862.SZ', 20260904, 1459)",
        )
        .expect("insert t_stock_minute");
}

#[test]
fn having_filters_groups_by_aggregate_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT security_id, MIN(deal_time) min_deal_time, MAX(deal_time) max_deal_time
                 FROM t_stock_minute
                 WHERE deal_date = 20260904
                 GROUP BY security_id
                 HAVING MIN(deal_time) < 930 OR MAX(deal_time) > 1459",
            )
            .expect("having query"),
    );

    assert_eq!(rows.len(), 1, "only the violating group must survive: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("000862.SZ".to_string())));
    assert_eq!(rows[0][1], ScalarValue::Int32(Some(925)));
    assert_eq!(rows[0][2], ScalarValue::Int32(Some(1459)));
}

#[test]
fn having_false_predicate_returns_no_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT security_id, MIN(deal_time) min_deal_time
                 FROM t_stock_minute
                 WHERE deal_date = 20260904
                 GROUP BY security_id
                 HAVING MIN(deal_time) > 9999",
            )
            .expect("having query"),
    );

    assert!(rows.is_empty(), "always-false HAVING must filter everything: {rows:?}");
}

#[test]
fn having_supports_group_column_equality() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT security_id, COUNT(*) cnt
                 FROM t_stock_minute
                 WHERE deal_date = 20260904
                 GROUP BY security_id
                 HAVING security_id = '603050.SH'",
            )
            .expect("having group column query"),
    );

    assert_eq!(rows.len(), 1, "HAVING on group column must keep exactly one group: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("603050.SH".to_string())));
}

#[test]
fn having_without_group_by_filters_global_aggregate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*) cnt FROM t_stock_minute WHERE deal_date = 20260904 HAVING COUNT(*) > 100",
            )
            .expect("having global aggregate query"),
    );

    assert!(rows.is_empty(), "global HAVING count > 100 must filter the single row: {rows:?}");
}

#[test]
fn having_and_predicate_combinations() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT security_id, MIN(deal_time) min_deal_time, MAX(deal_time) max_deal_time
                 FROM t_stock_minute
                 WHERE deal_date = 20260904
                 GROUP BY security_id
                 HAVING MIN(deal_time) >= 925 AND MAX(deal_time) <= 1459 AND MIN(deal_time) < 930",
            )
            .expect("having and query"),
    );

    assert_eq!(rows.len(), 1, "AND combination must keep only the 925 group: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("000862.SZ".to_string())));
}

#[test]
fn group_by_without_having_still_returns_all_groups() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_minute_table(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT security_id, MIN(deal_time) min_deal_time, MAX(deal_time) max_deal_time
                 FROM t_stock_minute
                 WHERE deal_date = 20260904
                 GROUP BY security_id",
            )
            .expect("group by query"),
    );

    assert_eq!(rows.len(), 3, "no HAVING must keep all groups: {rows:?}");
}
