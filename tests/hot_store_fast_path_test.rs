use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn bulk_payload(table: &str, row_count: u32, columns: Vec<(&str, DataType, Vec<u8>)>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(table.len() as u16).to_le_bytes());
    payload.extend_from_slice(table.as_bytes());
    payload.extend_from_slice(&row_count.to_le_bytes());
    payload.extend_from_slice(&(columns.len() as u32).to_le_bytes());

    for (name, dtype, data) in columns {
        payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.push(dtype as u8);
        payload.push(0);
        payload.push(0);
        payload.extend_from_slice(&row_count.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&data);
    }
    payload
}

fn int32_col(values: &[i32]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn int64_col(values: &[i64]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn varchar_col(values: &[&str]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value.as_bytes());
    }
    data
}

/// Collect the `ts` (INT32) values returned by a query as a multiset (Vec),
/// preserving duplicates so that double-counting regressions are caught.
fn collect_ts_values(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => out.push(v),
                other => panic!("expected int32 ts, got {:?}", other),
            }
        }
    }
    out.sort();
    out
}

fn collect_i32_values_in_order(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => out.push(v),
                other => panic!("expected int32, got {:?}", other),
            }
        }
    }
    out
}

fn collect_i32_column_values(result: ExecResult, col_idx: usize) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, col_idx) {
                Some(ScalarValue::Int32(Some(v))) => out.push(v),
                other => panic!("expected int32 at column {}, got {:?}", col_idx, other),
            }
        }
    }
    out
}

fn collect_count(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) result")
}

fn explain_text(result: ExecResult) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = String::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            if let Some(ScalarValue::VarChar(Some(line))) = batch.scalar(row_idx, 0) {
                out.push_str(&line);
                out.push('\n');
            }
        }
    }
    out
}

/// End-to-end check that the hot-store fast path stays correct across a flush.
///
/// The hot store mirrors auto-commit inserts but is never cleared on flush, so
/// once rows are persisted to a segment they would be served twice (once by the
/// segment scan, once by the hot gather) unless the gather applies a watermark
/// that excludes already-flushed rows. This test pins that behaviour: after a
/// flush and a fresh insert, a point lookup on the leading cluster key returns
/// exactly the union of segment + live rows, with no duplication.
#[test]
fn hot_fast_path_does_not_double_count_after_flush() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    // Single active partition (all rows share deal_date) so flushed rows form a
    // contiguous append-prefix of the hot store -- the watermark invariant.
    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                sym VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                ts INT32 NOT NULL
            ) PARTITION BY deal_date CLUSTER BY (sym, deal_date) HOT BY (trade_time) INDEX (sym)",
        )
        .expect("create ticks");

    // Auto-commit inserts feed the hot store.
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000000, 1), ('A', 20260108, 1782718238000001, 2), ('B', 20260108, 1782718238000002, 1)",
        )
        .expect("insert initial rows");

    // Fast path: live memtable data only, watermark == 0.
    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select before flush"),
    );
    assert_eq!(rows, vec![1, 2]);

    // Persist the initial rows to a segment; the hot store is NOT cleared.
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush initial rows");

    // After flush there is no live memtable data, so the fast path is skipped
    // and the segment scan serves the query alone.
    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select after flush"),
    );
    assert_eq!(rows, vec![1, 2]);

    // A fresh auto-commit insert creates a new live memtable while the hot store
    // still holds the flushed prefix. Without the watermark the gather would
    // return A@1, A@2 (stale) plus A@3 (live), and the segment scan would also
    // return A@1, A@2 -- duplicating them into [1, 1, 2, 2, 3].
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000003, 3)",
        )
        .expect("insert live row after flush");

    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select after post-flush insert"),
    );
    assert_eq!(
        rows,
        vec![1, 2, 3],
        "post-flush point lookup must union segment + live rows without duplication"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn restart_warmup_serves_resident_partition_point_lookup_without_segment_scan() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE stock_tick (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create stock_tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO stock_tick VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10),
                    ('688256.SH', 20260709, 103913, 1783564753000000, 30),
                    ('000001.SZ', 20260709, 103999, 1783564799000000, 99),
                    ('688256.SH', 20260709, 103910, 1783564750000000, 20)",
            )
            .expect("insert rows");
        engine
            .execute(&mut session, "FLUSH TABLE stock_tick")
            .expect("flush rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let store = engine
        .hot_store_manager()
        .get("main", "stock_tick")
        .expect("warmup should rebuild hot store");
    assert!(store.is_resident_for_partition(Some("20260709")));

    let rows = collect_i32_values_in_order(
        engine
            .execute(
                &mut session,
                "SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("resident hot point lookup"),
    );
    assert_eq!(rows, vec![30, 20, 10]);

    let top_rows = collect_i32_values_in_order(
        engine
            .execute(
                &mut session,
                "SELECT price FROM stock_tick
                 WHERE deal_date = 20260709
                   AND security_id = '688256.SH'
                   AND deal_time >= 103905
                 ORDER BY deal_time DESC
                 LIMIT 1",
            )
            .expect("resident hot range lookup with top-N"),
    );
    assert_eq!(top_rows, vec![30]);

    let range_count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM stock_tick
                 WHERE deal_date = 20260709
                   AND security_id = '688256.SH'
                   AND deal_time >= 103905",
            )
            .expect("resident hot range count"),
    );
    assert_eq!(range_count, 2);

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("explain analyze resident hot point lookup"),
    );
    assert!(
        !plan.contains("segments      :"),
        "resident hot lookup should skip segment scan, got:\n{}",
        plan
    );

    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'",
            )
            .expect("resident hot count"),
    );
    assert_eq!(count, 3);

    let count_plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT count(*) FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'",
            )
            .expect("explain analyze resident hot count"),
    );
    assert!(
        !count_plan.contains("segments      :"),
        "resident hot count should skip segment scan, got:\n{}",
        count_plan
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn physical_cursor_select_star_uses_resident_hot_partition() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE hot_tick (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create hot_tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO hot_tick VALUES
                    ('688256.SH', 20260710, 103904, 1783651144000000, 10),
                    ('688256.SH', 20260710, 103913, 1783651153000000, 30),
                    ('000001.SZ', 20260710, 103999, 1783651199000000, 99)",
            )
            .expect("insert rows");
        engine
            .execute(&mut session, "FLUSH TABLE hot_tick")
            .expect("flush rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert!(engine
        .hot_store_manager()
        .get("main", "hot_tick")
        .expect("warmup should rebuild hot store")
        .is_resident_for_partition(Some("20260710")));

    let table_dir = engine.segment_mgr().table_dir("main", "hot_tick");
    for entry in std::fs::read_dir(&table_dir).expect("read table dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("dat") {
            std::fs::rename(&path, path.with_extension("dat.hidden"))
                .expect("hide segment to prove resident hot path");
        }
    }

    let mut prices = collect_i32_column_values(
        engine
            .execute(
                &mut session,
                "SELECT * FROM hot_tick t
                 WHERE t.deal_date = 20260710 AND t.security_id = '688256.SH'",
            )
            .expect("resident hot select star"),
        4,
    );
    prices.sort();
    assert_eq!(prices, vec![10, 30]);

    engine.segment_mgr().shutdown();
}

#[test]
fn ordinary_insert_marks_new_hot_partition_resident_after_flush() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE hot_tick (
                security_id VARCHAR,
                deal_date INT64,
                deal_time INT32,
                gmt_trade TIMESTAMP,
                price INT32
            ) CLUSTER BY (security_id, deal_date, deal_time)
              HOT BY (gmt_trade) INDEX (security_id)
              PARTITION BY deal_date",
        )
        .expect("create hot_tick");
    engine
        .execute(
            &mut session,
            "INSERT INTO hot_tick VALUES
                ('688256.SH', 20260710, 103904, 1783651144000000, 10),
                ('688256.SH', 20260710, 103913, 1783651153000000, 30),
                ('000001.SZ', 20260710, 103999, 1783651199000000, 99)",
        )
        .expect("insert rows");
    assert!(engine
        .hot_store_manager()
        .get("main", "hot_tick")
        .expect("insert should create hot store")
        .is_resident_for_partition(Some("20260710")));

    engine
        .execute(&mut session, "FLUSH TABLE hot_tick")
        .expect("flush rows");

    let table_dir = engine.segment_mgr().table_dir("main", "hot_tick");
    for entry in std::fs::read_dir(&table_dir).expect("read table dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("dat") {
            std::fs::rename(&path, path.with_extension("dat.hidden"))
                .expect("hide segment to prove resident hot path");
        }
    }

    let mut prices = collect_i32_column_values(
        engine
            .execute(
                &mut session,
                "SELECT * FROM hot_tick t
                 WHERE t.deal_date = 20260710 AND t.security_id = '688256.SH'",
            )
            .expect("resident hot select star after flush"),
        4,
    );
    prices.sort();
    assert_eq!(prices, vec![10, 30]);

    engine.segment_mgr().shutdown();
}

#[test]
fn direct_bulk_keeps_resident_hot_partition_query_off_segments() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE stock_tick (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create stock_tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO stock_tick VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10),
                    ('688256.SH', 20260709, 103910, 1783564750000000, 20)",
            )
            .expect("insert seed rows");
        engine
            .execute(&mut session, "FLUSH TABLE stock_tick")
            .expect("flush seed rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert!(engine
        .hot_store_manager()
        .get("main", "stock_tick")
        .expect("warmup should rebuild hot store")
        .is_resident_for_partition(Some("20260709")));

    let payload = bulk_payload(
        "stock_tick",
        1,
        vec![
            (
                "security_id",
                DataType::VarChar,
                varchar_col(&["688256.SH"]),
            ),
            ("deal_date", DataType::Int64, int64_col(&[20260709])),
            ("deal_time", DataType::Int32, int32_col(&[103913])),
            (
                "gmt_trade",
                DataType::Timestamp,
                int64_col(&[1783564753000000]),
            ),
            ("price", DataType::Int32, int32_col(&[30])),
        ],
    );
    assert_eq!(
        engine
            .exec_bulk_insert_payload_with_options("main", &payload, false)
            .expect("direct bulk insert"),
        1
    );

    let rows = collect_i32_values_in_order(
        engine
            .execute(
                &mut session,
                "SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("resident hot point lookup after direct bulk"),
    );
    assert_eq!(rows, vec![30, 20, 10]);

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("explain analyze after direct bulk"),
    );
    assert!(
        !plan.contains("segments      :"),
        "resident hot lookup should still skip segment scan after direct bulk, got:\n{}",
        plan
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn sql_bulk_import_keeps_resident_hot_partition_query_off_segments() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE stock_tick (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create stock_tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO stock_tick VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10),
                    ('688256.SH', 20260709, 103910, 1783564750000000, 20)",
            )
            .expect("insert seed rows");
        engine
            .execute(&mut session, "FLUSH TABLE stock_tick")
            .expect("flush seed rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert!(engine
        .hot_store_manager()
        .get("main", "stock_tick")
        .expect("warmup should rebuild hot store")
        .is_resident_for_partition(Some("20260709")));

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(
            &mut session,
            "INSERT INTO stock_tick VALUES
                ('688256.SH', 20260709, 103913, 1783564753000000, 30)",
        )
        .expect("bulk-mode SQL insert");
    session.bulk_import_mode = false;
    assert_eq!(engine.end_bulk_import(), 0);
    engine.flush_all_buffers().expect("flush bulk rows");

    let rows = collect_i32_values_in_order(
        engine
            .execute(
                &mut session,
                "SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("resident hot point lookup after SQL bulk insert"),
    );
    assert_eq!(rows, vec![30, 20, 10]);

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("explain analyze after SQL bulk insert"),
    );
    assert!(
        !plan.contains("segments      :"),
        "resident hot lookup should still skip segment scan after SQL bulk insert, got:\n{}",
        plan
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_indexes_named_column_and_echoes() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE tick_hot (
                security_id VARCHAR NOT NULL,
                trade_time TIMESTAMP,
                price INT32
            ) CLUSTER BY (trade_time) HOT BY (trade_time) INDEX (security_id) CLOSE AT '15:30'",
        )
        .expect("create with HOT clause");

    let result = engine
        .execute(&mut session, "SHOW CREATE TABLE tick_hot")
        .expect("show create");
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut create_sql = String::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            if let Some(ScalarValue::VarChar(Some(s))) = batch.scalar(r, 1) {
                create_sql.push_str(&s);
            }
        }
    }
    assert!(
        create_sql.contains("HOT BY (trade_time) INDEX (security_id) CLOSE AT '15:30'"),
        "SHOW CREATE TABLE must echo HOT clause, got: {}",
        create_sql
    );

    engine
        .execute(
            &mut session,
            "INSERT INTO tick_hot (security_id, trade_time, price) VALUES \
             ('A', 1782718238000000, 10), \
             ('B', 1782718238000001, 20), \
             ('A', 1782718238000002, 30)",
        )
        .expect("insert rows");

    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT price FROM tick_hot WHERE security_id = 'A'",
            )
            .expect("select by hot index column"),
    );
    assert_eq!(rows, vec![10, 30]);

    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_set_hot_preserves_existing_rows_and_accelerates_future_inserts() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE t_stock_minute (
                    id INT64,
                    gmt_create TIMESTAMP,
                    security_id VARCHAR NOT NULL,
                    deal_date INT32 NOT NULL,
                    deal_time INT32 NOT NULL,
                    close_px INT32
                ) CLUSTER BY (deal_date, security_id, deal_time)
                  PARTITION BY YEAR(deal_date)",
            )
            .expect("create ordinary minute table");
        engine
            .execute(
                &mut session,
                "INSERT INTO t_stock_minute VALUES
                 (1, 1785310200000000, '000001.SZ', 20260729, 93000, 10)",
            )
            .expect("insert row before enabling HOT");

        let before = engine
            .get_table_meta("main", "t_stock_minute")
            .expect("ordinary table metadata");
        assert_eq!(before.row_count, 1);
        assert!(before.hot_time_column.is_none());
        assert!(engine
            .hot_store_manager()
            .get("main", "t_stock_minute")
            .is_none());

        engine
            .execute(
                &mut session,
                "ALTER TABLE t_stock_minute
                 SET HOT BY (gmt_create)
                 INDEX (security_id)
                 CLOSE AT '15:30'",
            )
            .expect("enable HOT on existing table");

        let altered = engine
            .get_table_meta("main", "t_stock_minute")
            .expect("altered table metadata");
        assert_eq!(altered.row_count, before.row_count);
        assert_eq!(altered.segment_count, before.segment_count);
        assert_eq!(altered.hot_time_column.as_deref(), Some("gmt_create"));
        assert_eq!(altered.hot_index_column.as_deref(), Some("security_id"));
        assert_eq!(altered.hot_close_time.as_deref(), Some("15:30"));
        assert!(
            engine
                .hot_store_manager()
                .get("main", "t_stock_minute")
                .is_none(),
            "ALTER must not backfill historical rows into the resident hot store"
        );
        assert_eq!(
            collect_count(
                engine
                    .execute(&mut session, "SELECT COUNT(*) FROM t_stock_minute")
                    .expect("count after ALTER")
            ),
            1
        );

        let show = engine
            .execute(&mut session, "SHOW CREATE TABLE t_stock_minute")
            .expect("show altered definition");
        let ExecResult::Query { batches, .. } = show else {
            panic!("expected SHOW CREATE query result");
        };
        let create_sql = batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).filter_map(|row| batch.scalar(row, 1)))
            .find_map(|value| match value {
                ScalarValue::VarChar(Some(sql)) => Some(sql),
                _ => None,
            })
            .expect("SHOW CREATE SQL");
        assert!(create_sql.contains("HOT BY (gmt_create) INDEX (security_id) CLOSE AT '15:30'"));

        engine
            .execute(
                &mut session,
                "INSERT INTO t_stock_minute VALUES
                 (2, 1785310260000000, '000001.SZ', 20260729, 93100, 11)",
            )
            .expect("insert row after enabling HOT");
        assert_eq!(
            engine
                .hot_store_manager()
                .get("main", "t_stock_minute")
                .expect("future insert should create hot store")
                .row_count(),
            1,
            "the historical row must stay on disk while only the new row becomes resident"
        );
        assert_eq!(
            collect_count(
                engine
                    .execute(
                        &mut session,
                        "SELECT COUNT(*) FROM t_stock_minute
                         WHERE security_id = '000001.SZ' AND deal_date = 20260729",
                    )
                    .expect("query persisted and resident rows together")
            ),
            2
        );
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let restored = engine
        .get_table_meta("main", "t_stock_minute")
        .expect("restored HOT metadata");
    assert_eq!(restored.hot_time_column.as_deref(), Some("gmt_create"));
    assert_eq!(restored.hot_index_column.as_deref(), Some("security_id"));
    assert_eq!(restored.hot_close_time.as_deref(), Some("15:30"));
    assert_eq!(
        collect_count(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM t_stock_minute")
                .expect("count after restart")
        ),
        2
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_set_hot_rejects_invalid_definition_without_changing_table() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE ordinary_ticks (
                id INT64,
                gmt_create VARCHAR,
                security_id VARCHAR
            )",
        )
        .expect("create ordinary table");
    engine
        .execute(
            &mut session,
            "INSERT INTO ordinary_ticks VALUES (1, '2026-07-29 09:30:00', '000001.SZ')",
        )
        .expect("insert ordinary row");

    let error = engine
        .execute(
            &mut session,
            "ALTER TABLE ordinary_ticks
             SET HOT BY (gmt_create)
             INDEX (security_id)
             CLOSE AT '15:30'",
        )
        .expect_err("non-timestamp HOT column must fail");
    assert!(error.to_string().contains("must be TIMESTAMP"));

    let unchanged = engine
        .get_table_meta("main", "ordinary_ticks")
        .expect("unchanged metadata");
    assert!(unchanged.hot_time_column.is_none());
    assert!(unchanged.hot_index_column.is_none());
    assert!(unchanged.hot_close_time.is_none());
    assert_eq!(unchanged.row_count, 1);
    assert_eq!(
        collect_count(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM ordinary_ticks")
                .expect("count unchanged table")
        ),
        1
    );
    assert!(engine
        .hot_store_manager()
        .get("main", "ordinary_ticks")
        .is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn hot_count_by_index_column_uses_resident_index() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE tick_hot_count (
                security_id VARCHAR NOT NULL,
                trade_time TIMESTAMP,
                price INT32
            ) CLUSTER BY (trade_time) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create hot count table");

    engine
        .execute(
            &mut session,
            "INSERT INTO tick_hot_count (security_id, trade_time, price) VALUES \
             ('A', 1782718238000000, 10), \
             ('B', 1782718238000001, 20), \
             ('A', 1782718238000002, 30)",
        )
        .expect("insert rows");

    let result = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM tick_hot_count WHERE security_id = 'A'",
        )
        .expect("count by hot index column");
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    match batches[0].scalar(0, 0) {
        Some(ScalarValue::Int64(Some(v))) => assert_eq!(v, 2),
        other => panic!("expected count int64, got {:?}", other),
    }

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_rejects_invalid_close_time() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let err = engine
        .execute(
            &mut session,
            "CREATE TABLE bad_close (security_id VARCHAR, trade_time TIMESTAMP) \
             HOT BY (trade_time) INDEX (security_id) CLOSE AT '25:99'",
        )
        .expect_err("invalid HOT CLOSE AT time should be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("HH:MM"),
        "expected close time validation error, got: {msg}"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_requires_timestamp_time_column() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let err = engine
        .execute(
            &mut session,
            "CREATE TABLE bad_tick (security_id VARCHAR, trade_time VARCHAR) \
             HOT BY (trade_time) INDEX (security_id)",
        )
        .expect_err("non-timestamp HOT column should be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be TIMESTAMP"),
        "expected timestamp validation error, got: {msg}"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn drop_table_removes_resident_hot_store_before_same_name_recreate() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                security_id VARCHAR NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                local_time VARCHAR
            ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create old ticks");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 1783492992000000, '2026-07-08 14:43:12.139')",
        )
        .expect("insert old schema row");

    engine
        .execute(&mut session, "DROP TABLE ticks")
        .expect("drop old ticks");
    assert!(engine.hot_store_manager().get("main", "ticks").is_none());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                security_id VARCHAR NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                local_time TIMESTAMP
            ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create new ticks");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 1783492992000000, 1783492992139000)",
        )
        .expect("insert new schema row");

    engine.segment_mgr().shutdown();
}

/// The realtime gap this pins down: a hot-fed partition stays served by the
/// hot store even after its memtable has been flushed into segments. The
/// flushed rows are mirrored (every auto-commit append feeds the store), so
/// the segment's existence must not retire the fast path — previously the
/// point lookup fell back to an unclustered full segment scan once the flush
/// landed.
#[test]
fn flushed_partition_still_served_by_resident_hot_store() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                sym VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                ts INT32 NOT NULL
            ) PARTITION BY deal_date CLUSTER BY (sym, deal_date) HOT BY (trade_time) INDEX (sym)",
        )
        .expect("create ticks");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000000, 1), ('A', 20260108, 1782718238000001, 2), ('B', 20260108, 1782718238000002, 7)",
        )
        .expect("insert rows");

    // Persist the memtable into a segment. The partition keeps its resident
    // mark and no write has bypassed the mirror, so the fast path must keep
    // answering from the hot store alone.
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush rows");

    let store = engine
        .hot_store_manager()
        .get("main", "ticks")
        .expect("hot store present");
    assert!(
        store.is_resident_for_partition(Some("20260108")),
        "flushed partition must stay resident"
    );
    assert_eq!(
        engine
            .batch_writer()
            .memtable_manager()
            .hot_unfed_rows("main", "ticks", "20260108"),
        0,
        "auto-commit appends are fully mirrored"
    );

    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM ticks WHERE sym = 'A' AND deal_date = 20260108",
            )
            .expect("post-flush point lookup"),
    );
    assert_eq!(
        rows,
        vec![1, 2],
        "hot store must serve the flushed partition"
    );

    // Row counts stay consistent after the flush too.
    assert_eq!(
        collect_count(
            engine
                .execute(
                    &mut session,
                    "SELECT count(*) FROM ticks WHERE deal_date = 20260108"
                )
                .expect("post-flush count")
        ),
        3
    );

    engine.segment_mgr().shutdown();
}

/// A committed transaction never mirrors into the hot store, so once one
/// lands in a partition the resident fast path must stand down for it while
/// results stay correct.
#[test]
fn committed_transaction_disables_resident_fast_path_for_partition() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                sym VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                ts INT32 NOT NULL
            ) PARTITION BY deal_date CLUSTER BY (sym, deal_date) HOT BY (trade_time) INDEX (sym)",
        )
        .expect("create ticks");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000000, 1)",
        )
        .expect("auto-commit row");

    engine.execute(&mut session, "BEGIN").expect("begin txn");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000001, 2)",
        )
        .expect("txn row");
    engine.execute(&mut session, "COMMIT").expect("commit txn");

    assert!(
        engine
            .batch_writer()
            .memtable_manager()
            .hot_unfed_rows("main", "ticks", "20260108")
            > 0,
        "committed txn rows must be accounted as unmirrored"
    );

    // Correctness first: the point lookup still sees both rows.
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM ticks WHERE sym = 'A' AND deal_date = 20260108",
            )
            .expect("post-txn point lookup"),
    );
    assert_eq!(rows, vec![1, 2]);

    engine.segment_mgr().shutdown();
}

#[test]
fn restart_warmup_after_close_only_reloads_post_close_segments() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE tick_close (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id) CLOSE AT '23:59'
                  PARTITION BY deal_date",
            )
            .expect("create tick_close");
        engine
            .execute(
                &mut session,
                "INSERT INTO tick_close VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10),
                    ('688256.SH', 20260709, 103913, 1783564753000000, 30)",
            )
            .expect("insert pre-close rows");
        engine
            .execute(&mut session, "FLUSH TABLE tick_close")
            .expect("flush pre-close rows");

        // Archive the pre-close partition: seals segments, evicts the resident
        // mirror and records hot_last_close_at.
        engine
            .close_of_day_flush_table("main", "tick_close")
            .expect("close-of-day flush");

        // Post-close writes land in a new partition whose segments stay
        // eligible for the restart warmup. Sleep so the segment mtime clearly
        // postdates the recorded close instant (mtime has ms granularity).
        std::thread::sleep(std::time::Duration::from_millis(20));
        engine
            .execute(
                &mut session,
                "INSERT INTO tick_close VALUES
                    ('688256.SH', 20260710, 103904, 1783651144000000, 50),
                    ('000001.SZ', 20260710, 103999, 1783651199000000, 70)",
            )
            .expect("insert post-close rows");
        engine
            .execute(&mut session, "FLUSH TABLE tick_close")
            .expect("flush post-close rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let store = engine
        .hot_store_manager()
        .get("main", "tick_close")
        .expect("post-close segments should rebuild a hot store");
    assert_eq!(
        store.row_count(),
        2,
        "only post-close rows may be reloaded; rows archived by the close must stay cold"
    );
    assert!(store.is_resident_for_partition(Some("20260710")));
    assert!(
        !store.is_resident_for_partition(Some("20260709")),
        "the archived partition must not be marked resident"
    );

    // The post-close partition keeps the memory fast path.
    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM tick_close
                 WHERE deal_date = 20260710 AND security_id = '688256.SH'",
            )
            .expect("explain post-close resident lookup"),
    );
    assert!(
        !plan.contains("segments      :"),
        "post-close partition lookup should skip segment scan, got:\n{}",
        plan
    );

    // Archived rows remain queryable through the segment scan.
    let archived = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM tick_close WHERE deal_date = 20260709",
            )
            .expect("archived partition still queryable"),
    );
    assert_eq!(archived, 2);

    engine.segment_mgr().shutdown();
}

#[test]
fn restart_warmup_skips_table_closed_without_recorded_instant() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE tick_legacy (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create tick_legacy");
        engine
            .execute(
                &mut session,
                "INSERT INTO tick_legacy VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10)",
            )
            .expect("insert rows");
        engine
            .execute(&mut session, "FLUSH TABLE tick_legacy")
            .expect("flush rows");
        engine.shutdown().expect("shutdown engine");
    }

    // Simulate a catalog written by a legacy binary: the table was closed,
    // but only the close date was recorded, not the completion instant.
    let catalog_path = temp_dir
        .path()
        .join("meta")
        .join("catalogs")
        .join("main.json");
    let raw = std::fs::read_to_string(&catalog_path).expect("read catalog");
    let mut json: serde_json::Value = serde_json::from_str(&raw).expect("parse catalog");
    json["tables"]["tick_legacy"]["hot_last_close_date"] =
        serde_json::Value::String("2026-09-16".to_string());
    json["tables"]["tick_legacy"]
        .as_object_mut()
        .expect("table object")
        .remove("hot_last_close_at");
    std::fs::write(
        &catalog_path,
        serde_json::to_vec(&json).expect("serialize catalog"),
    )
    .expect("write catalog");

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    assert!(
        engine
            .hot_store_manager()
            .get("main", "tick_legacy")
            .is_none(),
        "a table closed without a recorded instant must stay cold until its next close"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn warmup_disabled_keeps_restart_cold() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE tick_switch (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create tick_switch");
        engine
            .execute(
                &mut session,
                "INSERT INTO tick_switch VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10)",
            )
            .expect("insert rows");
        engine
            .execute(&mut session, "FLUSH TABLE tick_switch")
            .expect("flush rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new_with_storage_config_and_auto_compact(
        temp_dir.path(),
        "main",
        taibai::storage::segment_manager::SegmentConfig::default(),
        taibai::sql::execution::AutoCompactConfig::default(),
        64 * 1024 * 1024,
        false,
        taibai::common::config::PubSubSettings::default(),
        false,
    )
    .expect("restart with warmup disabled");
    let mut session = Session::new("root".to_string(), "main".to_string());

    assert!(
        engine
            .hot_store_manager()
            .get("main", "tick_switch")
            .is_none(),
        "warmup disabled must not rebuild a resident store"
    );
    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM tick_switch WHERE deal_date = 20260709",
            )
            .expect("segment scan still serves the data"),
    );
    assert_eq!(count, 1);

    engine.segment_mgr().shutdown();
}

/// EXPLAIN ANALYZE must surface the fast-path chain's answer: a resident
/// shortcut lookup renders `fast_path : answered by resident_hot` and, per
/// the no-segment-scan contract, no scan counter block at all.
#[test]
fn explain_analyze_renders_fast_path_answer() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE stock_tick (
                    security_id VARCHAR,
                    deal_date INT64,
                    deal_time INT32,
                    gmt_trade TIMESTAMP,
                    price INT32
                ) CLUSTER BY (security_id, deal_date, deal_time)
                  HOT BY (gmt_trade) INDEX (security_id)
                  PARTITION BY deal_date",
            )
            .expect("create stock_tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO stock_tick VALUES
                    ('688256.SH', 20260709, 103904, 1783564744000000, 10),
                    ('688256.SH', 20260709, 103913, 1783564753000000, 30),
                    ('000001.SZ', 20260709, 103999, 1783564799000000, 99),
                    ('688256.SH', 20260709, 103910, 1783564750000000, 20)",
            )
            .expect("insert rows");
        engine
            .execute(&mut session, "FLUSH TABLE stock_tick")
            .expect("flush rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let store = engine
        .hot_store_manager()
        .get("main", "stock_tick")
        .expect("warmup should rebuild hot store");
    assert!(store.is_resident_for_partition(Some("20260709")));

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM stock_tick
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'
                 ORDER BY deal_time DESC",
            )
            .expect("explain analyze resident hot point lookup"),
    );
    assert!(
        plan.contains("fast_path     : answered by resident_hot"),
        "EXPLAIN ANALYZE should name the answering fast-path node, got:\n{}",
        plan
    );
    assert!(
        !plan.contains("segments      :"),
        "resident hot lookup should skip segment scan, got:\n{}",
        plan
    );

    // A TTL table exercises the decline rendering: the chain's universal TTL
    // gate blocks every node before any runs, the general scan serves the
    // rows, and EXPLAIN ANALYZE shows both the decline reason and the real
    // scan counters.
    engine
        .execute(
            &mut session,
            "CREATE TABLE tick_ttl (
                security_id VARCHAR,
                ts TIMESTAMP,
                price INT32
            ) TTL (ts, 3600)",
        )
        .expect("create ttl table");
    engine
        .execute(
            &mut session,
            "INSERT INTO tick_ttl VALUES ('688256.SH', 1783564744000000, 10)",
        )
        .expect("insert ttl row");

    let ttl_plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT price FROM tick_ttl
                 WHERE security_id = '688256.SH'",
            )
            .expect("explain analyze ttl table"),
    );
    assert!(
        ttl_plan.contains("fast_path     : declined by ttl_gate"),
        "EXPLAIN ANALYZE should list the TTL gate decline, got:\n{}",
        ttl_plan
    );

    engine.segment_mgr().shutdown();
}
