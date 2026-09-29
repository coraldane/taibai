//! Regression: repeated direct-bulk import sessions into the same partitions
//! must keep aggregates (SUM/AVG routed through footer col_sums) correct, and
//! zero-copy row groups must preserve every row exactly once across
//! contiguous and interleaved partition runs.

use taibai::net::bulk_importer::encode_columnar_bulk_insert;
use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::Engine;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

fn build_payload(table: &str, day: i32, symbols: &[&str], rows_per_symbol: usize) -> Vec<u8> {
    let schema = Schema::new(vec![
        ColumnDef::new("deal_date", DataType::Int32).with_nullable(false),
        ColumnDef::new("security_id", DataType::VarChar).with_nullable(false),
        ColumnDef::new("trans_volume", DataType::Int64).with_nullable(false),
    ]);
    let mut batch = RecordBatch::new(schema);
    for &symbol in symbols {
        for i in 0..rows_per_symbol {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(day)))
                .unwrap();
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(symbol.to_string())))
                .unwrap();
            // 1 + row index within the whole import, so sums are recomputable.
            let volume = 1 + (i as i64);
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::Int64(Some(volume)))
                .unwrap();
        }
    }
    encode_columnar_bulk_insert(table, &batch).unwrap().to_vec()
}

#[test]
fn repeated_bulk_sessions_keep_sum_and_rows_exact() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                deal_date INT32 NOT NULL,
                security_id VARCHAR NOT NULL,
                trans_volume INT64 NOT NULL
            ) PARTITION BY deal_date CLUSTER BY (security_id)",
        )
        .expect("create table");

    // Session 1: day 20260101, two symbols, rows 1..=100 each per symbol.
    // Session 2: SAME day and one new day, different symbol set. Contiguous
    // runs and partition reorders both occur across the two sessions.
    let payload1 = build_payload("ticks", 20260101, &["000001.SZ", "000002.SZ"], 100);
    let payload2 = build_payload("ticks", 20260101, &["000001.SZ", "600000.SH"], 100);
    let payload3 = build_payload("ticks", 20260102, &["600000.SH"], 100);

    for (idx, payload) in [&payload1, &payload2, &payload3].iter().enumerate() {
        let inserted = engine
            .exec_bulk_insert_payload_with_options("main", payload, true)
            .unwrap_or_else(|e| panic!("bulk import {} failed: {}", idx, e));
        let expected = if idx == 2 { 100 } else { 200 };
        assert_eq!(inserted, expected, "bulk import {} row count", idx);
    }

    // Mirror the real client flow: each import session ends with a STOP,
    // then the disconnect flush persists deferred bulk metadata (sidecar
    // metas / footers) for every dirty table.
    engine.end_bulk_import();
    let flushed = engine
        .flush_bulk_dirty_tables()
        .expect("flush dirty tables");
    assert_eq!(flushed, 500);

    // Five symbol-imports of rows 1..=100 (sum 5050 each): sessions 1 and 2
    // carry two symbols, session 3 one.
    let expect_sum: i128 = 5 * (1..=100i64).map(|v| v as i128).sum::<i128>();

    let result = engine
        .execute(
            &mut session,
            "SELECT COUNT(*), SUM(trans_volume), AVG(trans_volume) FROM ticks",
        )
        .expect("aggregate query");
    match result {
        taibai::sql::execution::ExecResult::Query { batches, .. } => {
            let batch = batches.first().expect("one result batch");
            let count = batch.scalar(0, 0).unwrap().to_string();
            let sum = batch.scalar(0, 1).unwrap().to_string();
            let avg = batch.scalar(0, 2).unwrap().to_string();
            assert_eq!(count, "500");
            assert_eq!(sum, expect_sum.to_string());
            assert_eq!(
                avg,
                format!("{}", expect_sum as f64 / 500.0),
                "AVG must match SUM/COUNT on double-imported data"
            );
        }
        other => panic!("expected query result, got {:?}", other_name(&other)),
    }
}

fn other_name(result: &taibai::sql::execution::ExecResult) -> String {
    match result {
        taibai::sql::execution::ExecResult::Query { .. } => "query".into(),
        _ => "non-query".into(),
    }
}
