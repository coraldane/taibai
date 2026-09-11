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
