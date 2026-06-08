use tempfile::TempDir;

use monax::net::session::Session;
use monax::sql::execution::engine::{Engine, ExecResult};
use monax::types::scalar::ScalarValue;

#[test]
fn join_scan_spec_only_collects_filters_for_the_target_table() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE stock_daily (
                id INT64,
                security_id VARCHAR,
                deal_date INT64
            )",
        )
        .expect("create stock_daily");
    engine
        .execute(
            &mut session,
            "CREATE TABLE security_info (
                id INT64,
                security_id VARCHAR,
                marker INT64,
                display_name VARCHAR
            )",
        )
        .expect("create security_info");

    engine
        .execute(
            &mut session,
            "INSERT INTO stock_daily VALUES (1, '000001.SZ', 20260605)",
        )
        .expect("insert stock_daily");
    engine
        .execute(
            &mut session,
            "INSERT INTO security_info VALUES (10, '000001.SZ', 0, 'Ping An Bank')",
        )
        .expect("insert security_info");

    let result = engine
        .execute(
            &mut session,
            "SELECT d.security_id, s.display_name
             FROM stock_daily d
             LEFT JOIN security_info s ON d.security_id = s.security_id
             WHERE d.deal_date = 20260605",
        )
        .expect("join query");

    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query");
    };
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        batches[0].scalar(0, 0).expect("security_id"),
        ScalarValue::VarChar(Some("000001.SZ".to_string()))
    );
    assert_eq!(
        batches[0].scalar(0, 1).expect("display_name"),
        ScalarValue::VarChar(Some("Ping An Bank".to_string()))
    );
}
