use std::fs;
use std::path::Path;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::segment::meta::sidecar_meta_path;
use taibai::storage::segment::SegmentWriter;
use taibai::storage::segment_manager::SegmentManager;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
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

fn varchar_col(values: &[&str]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value.as_bytes());
    }
    data
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

fn count_star(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) value")
}

fn make_two_i32_batch(schema: &Schema, rows: &[(i32, i32)]) -> RecordBatch {
    let mut batch = RecordBatch::new(schema.clone());
    for (deal_date, id) in rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int32(Some(*deal_date)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int32(Some(*id)))
            .unwrap();
    }
    batch
}

fn write_two_i32_segment(path: &Path, schema: &Schema, rows: &[(i32, i32)]) {
    let batch = make_two_i32_batch(schema, rows);
    let mut writer = SegmentWriter::create(path, schema.clone(), 8192).expect("create segment");
    writer.write_batch(&batch).expect("write segment");
    writer.seal().expect("seal segment");
}

#[test]
fn auto_compact_can_be_stopped_and_started_by_sql() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    assert!(engine.auto_compact_enabled());
    engine
        .execute(&mut session, "STOP AUTO COMPACT")
        .expect("stop auto compact");
    assert!(!engine.auto_compact_enabled());
    engine
        .execute(&mut session, "START AUTO COMPACT")
        .expect("start auto compact");
    assert!(engine.auto_compact_enabled());

    engine.segment_mgr().shutdown();
}

#[test]
fn bulk_import_honors_year_partition_on_int32_yyyymmdd() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                deal_date INT32 NOT NULL,
                security_id VARCHAR NOT NULL,
                deal_time INT32 NOT NULL
            ) CLUSTER BY (deal_date, security_id, deal_time) PARTITION BY YEAR(deal_date)",
        )
        .expect("create ticks");

    let payload = bulk_payload(
        "ticks",
        4,
        vec![
            (
                "deal_date",
                DataType::Int32,
                int32_col(&[20250102, 20240101, 20250101, 20240102]),
            ),
            (
                "security_id",
                DataType::VarChar,
                varchar_col(&["B", "A", "A", "A"]),
            ),
            (
                "deal_time",
                DataType::Int32,
                int32_col(&[930, 930, 925, 915]),
            ),
        ],
    );

    assert_eq!(
        engine
            .exec_bulk_insert_payload_with_wal_control("main", &payload, false)
            .expect("bulk insert"),
        4
    );
    engine.flush_all_buffers().expect("flush bulk rows");

    let table_dir = temp_dir.path().join("main").join("ticks");
    let mut files: Vec<String> = fs::read_dir(&table_dir)
        .expect("read table dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".dat"))
        .collect();
    files.sort();

    assert!(
        files.iter().any(|name| name.starts_with("p2024_")),
        "files: {:?}",
        files
    );
    assert!(
        files.iter().any(|name| name.starts_with("p2025_")),
        "files: {:?}",
        files
    );
    assert!(
        !files.iter().any(|name| name.starts_with("p0_")),
        "files: {:?}",
        files
    );

    let rows = collect_i32_rows(
        engine
            .execute(&mut session, "SELECT deal_date, deal_time FROM ticks")
            .expect("select rows"),
    );
    assert_eq!(
        rows,
        vec![
            vec![20240101, 930],
            vec![20240102, 915],
            vec![20250101, 925],
            vec![20250102, 930],
        ]
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn select_limit_falls_back_when_cluster_segment_order_is_not_monotonic() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                deal_date INT32 NOT NULL,
                id INT32 NOT NULL
            ) CLUSTER BY (deal_date, id) PARTITION BY YEAR(deal_date)",
        )
        .expect("create ticks");

    let schema = Schema::new(vec![
        ColumnDef::new("deal_date", DataType::Int32).with_nullable(false),
        ColumnDef::new("id", DataType::Int32).with_nullable(false),
    ]);
    let table_dir = temp_dir.path().join("main").join("ticks");
    fs::create_dir_all(&table_dir).expect("create table dir");

    write_two_i32_segment(
        &table_dir.join("p2026_0001.dat"),
        &schema,
        &[(20260612, 1), (20260612, 2)],
    );
    write_two_i32_segment(&table_dir.join("p2026_0002.dat"), &schema, &[(20260610, 3)]);
    write_two_i32_segment(&table_dir.join("p2026_0003.dat"), &schema, &[(20260611, 4)]);

    let count = count_star(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*) FROM ticks WHERE deal_date = 20260612",
            )
            .expect("count rows"),
    );
    assert_eq!(count, 2);

    let rows = collect_i32_rows(
        engine
            .execute(
                &mut session,
                "SELECT deal_date, id FROM ticks WHERE deal_date = 20260612 LIMIT 10",
            )
            .expect("select rows"),
    );
    assert_eq!(rows, vec![vec![20260612, 1], vec![20260612, 2]]);

    engine.segment_mgr().shutdown();
}

#[test]
fn bulk_import_mode_defers_direct_payload_metadata_flush() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE bulk_ticks (id INT32, price INT32)",
        )
        .expect("create table");

    let payload = bulk_payload(
        "bulk_ticks",
        2,
        vec![
            ("id", DataType::Int32, int32_col(&[1, 2])),
            ("price", DataType::Int32, int32_col(&[10, 20])),
        ],
    );

    assert_eq!(
        engine
            .exec_bulk_insert_payload_with_options("main", &payload, true, true)
            .expect("bulk insert"),
        2
    );

    let table_dir = temp_dir.path().join("main").join("bulk_ticks");
    let segments = SegmentManager::discover_segments(&table_dir).expect("discover segments");
    assert_eq!(segments.len(), 1);
    assert!(
        !sidecar_meta_path(&segments[0]).exists(),
        "bulk import mode should defer sidecar metadata flush"
    );

    engine.flush_all_buffers().expect("flush bulk rows");
    assert!(sidecar_meta_path(&segments[0]).exists());

    engine.segment_mgr().shutdown();
}

#[test]
fn bulk_import_mode_defers_sql_insert_metadata_flush() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE sql_bulk_ticks (id INT32, price INT32)",
        )
        .expect("create table");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(
            &mut session,
            "INSERT INTO sql_bulk_ticks VALUES (1, 10), (2, 20)",
        )
        .expect("insert rows");

    let table_dir = temp_dir.path().join("main").join("sql_bulk_ticks");
    let segments = SegmentManager::discover_segments(&table_dir).expect("discover segments");
    assert_eq!(segments.len(), 1);
    assert!(
        !sidecar_meta_path(&segments[0]).exists(),
        "SQL bulk import mode should defer sidecar metadata flush"
    );

    session.bulk_import_mode = false;
    assert_eq!(engine.end_bulk_import(), 0);
    engine.flush_all_buffers().expect("flush bulk rows");
    assert!(sidecar_meta_path(&segments[0]).exists());

    engine.segment_mgr().shutdown();
}

#[test]
fn multi_value_insert_splits_rows_across_partitions() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE daily_ticks (deal_date INT32, deal_time INT32) PARTITION BY YEAR(deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO daily_ticks VALUES (20240101, 930), (20250101, 931)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE daily_ticks")
        .expect("flush inserted rows");

    let table_dir = temp_dir.path().join("main").join("daily_ticks");
    let mut files: Vec<String> = fs::read_dir(&table_dir)
        .expect("read table dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".dat"))
        .collect();
    files.sort();

    assert!(
        files.iter().any(|name| name.starts_with("p2024_")),
        "files: {:?}",
        files
    );
    assert!(
        files.iter().any(|name| name.starts_with("p2025_")),
        "files: {:?}",
        files
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn year_partition_supports_varchar_yyyy_mm_dd() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE date_text_ticks (deal_date VARCHAR, deal_time INT32) PARTITION BY YEAR(deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO date_text_ticks VALUES ('2024-01-01', 930), ('2025-01-01', 931)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE date_text_ticks")
        .expect("flush inserted rows");

    let table_dir = temp_dir.path().join("main").join("date_text_ticks");
    let mut files: Vec<String> = fs::read_dir(&table_dir)
        .expect("read table dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".dat"))
        .collect();
    files.sort();

    assert!(
        files.iter().any(|name| name.starts_with("p2024_")),
        "files: {:?}",
        files
    );
    assert!(
        files.iter().any(|name| name.starts_with("p2025_")),
        "files: {:?}",
        files
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hash_and_mod_partition_syntax_create_expected_buckets() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE hash_ticks (user_id INT32, value INT32) PARTITION BY HASH(user_id, 10)",
        )
        .expect("create hash table");
    engine
        .execute(
            &mut session,
            "INSERT INTO hash_ticks VALUES (1, 10), (11, 20)",
        )
        .expect("insert hash rows");
    engine
        .execute(&mut session, "FLUSH TABLE hash_ticks")
        .expect("flush hash rows");

    let hash_dir = temp_dir.path().join("main").join("hash_ticks");
    let hash_files: Vec<String> = fs::read_dir(&hash_dir)
        .expect("read hash table dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".dat"))
        .collect();
    assert!(
        hash_files.iter().any(|name| name.starts_with("p1_")),
        "files: {:?}",
        hash_files
    );

    engine
        .execute(
            &mut session,
            "CREATE TABLE mod_ticks (id INT32, value INT32) PARTITION BY MOD(id, 4)",
        )
        .expect("create mod table");
    engine
        .execute(
            &mut session,
            "INSERT INTO mod_ticks VALUES (1, 10), (4, 20), (-1, 30)",
        )
        .expect("insert mod rows");
    engine
        .execute(&mut session, "FLUSH TABLE mod_ticks")
        .expect("flush mod rows");

    let mod_dir = temp_dir.path().join("main").join("mod_ticks");
    let mod_files: Vec<String> = fs::read_dir(&mod_dir)
        .expect("read mod table dir")
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(|s| s.to_string()))
        .filter(|name| name.ends_with(".dat"))
        .collect();
    assert!(
        mod_files.iter().any(|name| name.starts_with("p0_")),
        "files: {:?}",
        mod_files
    );
    assert!(
        mod_files.iter().any(|name| name.starts_with("p1_")),
        "files: {:?}",
        mod_files
    );
    assert!(
        mod_files.iter().any(|name| name.starts_with("p3_")),
        "files: {:?}",
        mod_files
    );

    engine.segment_mgr().shutdown();
}
