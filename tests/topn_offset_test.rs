use std::path::Path;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::segment::SegmentWriter;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

fn collect_first_column(result: ExecResult) -> Vec<ScalarValue> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            values.push(batch.scalar(row_idx, 0).expect("first column"));
        }
    }
    values
}

#[test]
fn topn_limit_offset_applies_offset_after_sorting_join_results() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE t_stock_daily (
                security_id VARCHAR,
                deal_date INT64,
                open_px DOUBLE,
                high_px DOUBLE,
                low_px DOUBLE,
                close_px DOUBLE,
                pre_close DOUBLE,
                trade_value DOUBLE
            )",
        )
        .expect("create daily");
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_security_info (
                security_id VARCHAR,
                display_name VARCHAR
            )",
        )
        .expect("create security");
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_valuation (
                deal_date INT64,
                security_id VARCHAR,
                turnover_ratio DOUBLE,
                pe_ratio DOUBLE,
                pb_ratio DOUBLE,
                market_cap DOUBLE,
                circulating_market_cap DOUBLE
            )",
        )
        .expect("create valuation");

    engine
        .execute(
            &mut session,
            "INSERT INTO t_stock_daily VALUES
                ('A', 20260608, 10.0, 15.0, 10.0, 15.0, 10.0, 100.0),
                ('B', 20260608, 10.0, 14.0, 10.0, 14.0, 10.0, 100.0),
                ('C', 20260608, 10.0, 13.0, 10.0, 13.0, 10.0, 100.0),
                ('D', 20260608, 10.0, 12.0, 10.0, 12.0, 10.0, 100.0),
                ('E', 20260608, 10.0, 11.0, 10.0, 11.0, 10.0, 100.0)",
        )
        .expect("insert daily");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_security_info VALUES
                ('A', 'Alpha'),
                ('B', 'Beta'),
                ('C', 'Charlie'),
                ('D', 'Delta'),
                ('E', 'Echo')",
        )
        .expect("insert security");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_valuation VALUES
                (20260608, 'A', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'B', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'C', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'D', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'E', 1.0, 10.0, 1.0, 100.0, 90.0)",
        )
        .expect("insert valuation");

    let rows = collect_first_column(
        engine
            .execute(
                &mut session,
                "SELECT d.security_id, s.display_name,
                        d.open_px, d.high_px, d.low_px, d.close_px, d.pre_close,
                        d.trade_value,
                        v.turnover_ratio, v.pe_ratio, v.pb_ratio, v.market_cap, v.circulating_market_cap
                FROM t_stock_daily d
                LEFT JOIN t_security_info s ON d.security_id = s.security_id
                LEFT JOIN t_valuation v ON v.deal_date = 20260608 AND d.security_id = v.security_id
                WHERE d.deal_date = 20260608
                ORDER BY (d.close_px - d.pre_close) / d.pre_close DESC
                LIMIT 2 OFFSET 2",
            )
            .expect("select second page"),
    );

    assert_eq!(
        rows,
        vec![
            ScalarValue::VarChar(Some("C".to_string())),
            ScalarValue::VarChar(Some("D".to_string())),
        ]
    );
}

#[test]
fn topn_pushdown_keeps_offset_semantics_when_left_join_multiplies_rows() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE fact_rows (
                id VARCHAR,
                bucket_id INT64,
                score DOUBLE
            )",
        )
        .expect("create fact");
    engine
        .execute(
            &mut session,
            "CREATE TABLE dim_rows (
                id VARCHAR,
                label VARCHAR
            )",
        )
        .expect("create dim");
    engine
        .execute(
            &mut session,
            "CREATE TABLE dim_more (
                id VARCHAR,
                extra VARCHAR
            )",
        )
        .expect("create dim_more");

    engine
        .execute(
            &mut session,
            "INSERT INTO fact_rows VALUES
                ('A', 1, 5.0),
                ('B', 1, 4.0),
                ('C', 1, 3.0),
                ('D', 1, 2.0),
                ('E', 1, 1.0)",
        )
        .expect("insert fact");
    engine
        .execute(
            &mut session,
            "INSERT INTO dim_rows VALUES
                ('A', 'a1'),
                ('A', 'a2'),
                ('B', 'b1'),
                ('C', 'c1'),
                ('D', 'd1'),
                ('E', 'e1')",
        )
        .expect("insert dim");
    engine
        .execute(
            &mut session,
            "INSERT INTO dim_more VALUES
                ('A', 'ma'),
                ('B', 'mb'),
                ('C', 'mc'),
                ('D', 'md'),
                ('E', 'me')",
        )
        .expect("insert dim_more");

    let rows = collect_first_column(
        engine
            .execute(
                &mut session,
                "SELECT f.id, d.label, m.extra
                 FROM fact_rows f
                 LEFT JOIN dim_rows d ON f.id = d.id
                 LEFT JOIN dim_more m ON f.id = m.id
                 WHERE f.bucket_id = 1
                 ORDER BY f.score DESC
                 LIMIT 2 OFFSET 2",
            )
            .expect("select second page"),
    );

    assert_eq!(
        rows,
        vec![
            ScalarValue::VarChar(Some("B".to_string())),
            ScalarValue::VarChar(Some("C".to_string())),
        ]
    );
}

fn collect_i32_rows(result: ExecResult) -> Vec<Vec<i32>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::new();
            for col_idx in 0..batch.num_columns() {
                match batch.scalar(row_idx, col_idx) {
                    Some(ScalarValue::Int32(Some(v))) => row.push(v),
                    other => panic!("expected int32, got {:?}", other),
                }
            }
            rows.push(row);
        }
    }
    rows
}

fn create_ordered_ticks(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE ordered_ticks (
                deal_date INT32 NOT NULL,
                deal_time INT32 NOT NULL
            ) CLUSTER BY (deal_date, deal_time) PARTITION BY deal_date",
        )
        .expect("create ordered_ticks");
}

fn flush_ordered_ticks(engine: &Engine, session: &mut Session) {
    engine
        .execute(session, "FLUSH TABLE ordered_ticks")
        .expect("flush ordered_ticks");
}

fn insert_ordered_tick(engine: &Engine, session: &mut Session, date: i32, time: i32) {
    engine
        .execute(
            session,
            &format!("INSERT INTO ordered_ticks VALUES ({}, {})", date, time),
        )
        .expect("insert ordered tick");
}

fn write_ordered_segment(path: &Path, rows: &[(i32, i32)]) {
    let schema = Schema::new(vec![
        ColumnDef::new("deal_date", DataType::Int32).with_nullable(false),
        ColumnDef::new("deal_time", DataType::Int32).with_nullable(false),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    for (deal_date, deal_time) in rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int32(Some(*deal_date)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int32(Some(*deal_time)))
            .unwrap();
    }
    let mut writer = SegmentWriter::create(path, schema, 8192).expect("create segment");
    writer.write_batch(&batch).expect("write segment");
    writer.seal().expect("seal segment");
}

#[test]
fn topn_on_first_cluster_key_limit_reads_ordered_segments() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    create_ordered_ticks(&engine, &mut session);

    for time in 1..=4 {
        insert_ordered_tick(&engine, &mut session, 20260601, time);
    }
    flush_ordered_ticks(&engine, &mut session);
    for time in 1..=4 {
        insert_ordered_tick(&engine, &mut session, 20260602, time);
    }
    flush_ordered_ticks(&engine, &mut session);

    let asc = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM ordered_ticks ORDER BY deal_date LIMIT 3",
            )
            .expect("asc limit"),
    );
    assert_eq!(
        asc,
        vec![vec![20260601, 1], vec![20260601, 2], vec![20260601, 3]]
    );

    let desc = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM ordered_ticks ORDER BY deal_date DESC LIMIT 3",
            )
            .expect("desc limit"),
    );
    assert_eq!(
        desc,
        vec![vec![20260602, 4], vec![20260602, 3], vec![20260602, 2]]
    );

    let offset = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM ordered_ticks ORDER BY deal_date LIMIT 3 OFFSET 2",
            )
            .expect("offset limit"),
    );
    assert_eq!(
        offset,
        vec![vec![20260601, 3], vec![20260601, 4], vec![20260602, 1]]
    );

    let ranged = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM ordered_ticks
                 WHERE deal_date <= 20260602
                 ORDER BY deal_date LIMIT 5",
            )
            .expect("ranged limit"),
    );
    assert_eq!(
        ranged,
        vec![
            vec![20260601, 1],
            vec![20260601, 2],
            vec![20260601, 3],
            vec![20260601, 4],
            vec![20260602, 1],
        ]
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn topn_ordered_limit_falls_back_when_first_key_segments_not_monotonic() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE fallback_ticks (
                deal_date INT32 NOT NULL,
                deal_time INT32 NOT NULL
            ) CLUSTER BY (deal_date, deal_time)",
        )
        .expect("create fallback_ticks");

    let table_dir = temp_dir.path().join("main").join("fallback_ticks");
    std::fs::create_dir_all(&table_dir).expect("create table dir");
    write_ordered_segment(
        &table_dir.join("seg_0001.dat"),
        &[(20260612, 1), (20260612, 2)],
    );
    write_ordered_segment(&table_dir.join("seg_0002.dat"), &[(20260610, 3)]);
    write_ordered_segment(&table_dir.join("seg_0003.dat"), &[(20260611, 4)]);

    let asc = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM fallback_ticks ORDER BY deal_date LIMIT 2",
            )
            .expect("asc limit over non-monotonic segments"),
    );
    assert_eq!(asc, vec![vec![20260610, 3], vec![20260611, 4]]);

    let desc = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM fallback_ticks ORDER BY deal_date DESC LIMIT 1",
            )
            .expect("desc limit over non-monotonic segments"),
    );
    assert_eq!(desc, vec![vec![20260612, 1]]);

    engine.segment_mgr().shutdown();
}

#[test]
fn topn_ordered_limit_keeps_memtable_rows_below_sealed_cluster_keys() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    create_ordered_ticks(&engine, &mut session);

    let table_dir = temp_dir.path().join("main").join("ordered_ticks");
    std::fs::create_dir_all(&table_dir).expect("create table dir");
    write_ordered_segment(&table_dir.join("p20260605_0001.dat"), &[(20260605, 1)]);

    insert_ordered_tick(&engine, &mut session, 20260601, 9);

    let rows = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, deal_time FROM ordered_ticks ORDER BY deal_date LIMIT 2",
            )
            .expect("limit with memtable tail"),
    );
    assert_eq!(rows, vec![vec![20260601, 9], vec![20260605, 1]]);

    engine.flush_all_buffers().expect("flush all buffers");
    engine.segment_mgr().shutdown();
}
