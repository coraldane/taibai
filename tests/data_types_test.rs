use tempfile::TempDir;

use monax::net::session::Session;
use monax::protocol::types::DataType;
use monax::sql::execution::engine::{Engine, ExecResult};
use monax::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn collect_rows(result: ExecResult) -> (Vec<DataType>, Vec<Vec<ScalarValue>>) {
    let ExecResult::Query { schema, batches } = result else {
        panic!("expected query result");
    };
    let types = schema.columns.iter().map(|c| c.data_type).collect();
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
    (types, rows)
}

#[test]
fn sql_create_insert_select_supports_taibai_integer_aliases() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE numeric_types (
                c_tiny TINYINT,
                c_small SMALLINT,
                c_int INT,
                c_big BIGINT,
                c_u8 UINT8,
                c_u16 UINT16,
                c_u32 UINT32,
                c_u64 UINT64
            )",
        )
        .expect("create table");

    engine
        .execute(
            &mut session,
            "INSERT INTO numeric_types VALUES (-8, -1600, 32000, 6400000000, 8, 1600, 32000, 6400000000)",
        )
        .expect("insert row");
    engine
        .execute(&mut session, "FLUSH TABLE numeric_types")
        .expect("flush rows");

    let (types, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT c_tiny, c_small, c_int, c_big, c_u8, c_u16, c_u32, c_u64 FROM numeric_types",
            )
            .expect("select row"),
    );

    assert_eq!(
        types,
        vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ]
    );
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::Int8(Some(-8)),
            ScalarValue::Int16(Some(-1600)),
            ScalarValue::Int32(Some(32000)),
            ScalarValue::Int64(Some(6400000000)),
            ScalarValue::UInt8(Some(8)),
            ScalarValue::UInt16(Some(1600)),
            ScalarValue::UInt32(Some(32000)),
            ScalarValue::UInt64(Some(6400000000)),
        ]]
    );
}

#[test]
fn unsigned_small_ints_filter_sort_and_aggregate() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE unsigned_metrics (id UINT8, score UINT16)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO unsigned_metrics VALUES (2, 20), (1, 10), (3, 30)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE unsigned_metrics")
        .expect("flush rows");

    let (_, filtered) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id, score FROM unsigned_metrics WHERE id >= 2 ORDER BY score",
            )
            .expect("filter and sort"),
    );
    assert_eq!(
        filtered,
        vec![
            vec![ScalarValue::UInt8(Some(2)), ScalarValue::UInt16(Some(20))],
            vec![ScalarValue::UInt8(Some(3)), ScalarValue::UInt16(Some(30))],
        ]
    );

    let (_, aggregated) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT SUM(score), AVG(score), MIN(id), MAX(id) FROM unsigned_metrics",
            )
            .expect("aggregate rows"),
    );
    assert_eq!(
        aggregated,
        vec![vec![
            ScalarValue::Int64(Some(60)),
            ScalarValue::Float64(Some(20.0)),
            ScalarValue::UInt8(Some(1)),
            ScalarValue::UInt8(Some(3)),
        ]]
    );
}

#[test]
fn unsigned_insert_rejects_negative_and_overflow_values() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(&mut session, "CREATE TABLE limits (u8 UINT8, i8 INT8)")
        .expect("create table");

    let negative = engine.execute(&mut session, "INSERT INTO limits VALUES (-1, 1)");
    assert!(negative.is_err(), "negative UINT8 insert should fail");

    let overflow = engine.execute(&mut session, "INSERT INTO limits VALUES (1, 128)");
    assert!(overflow.is_err(), "overflowing INT8 insert should fail");
}

#[test]
fn string_bool_and_float_aliases_work_end_to_end() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE mixed_types (
                name STRING,
                note TEXT,
                active BOOLEAN,
                ratio FLOAT,
                score REAL,
                precise DOUBLE
            )",
        )
        .expect("create table");

    engine
        .execute(
            &mut session,
            "INSERT INTO mixed_types VALUES
                ('alpha', 'first', true, 1.5, 2.25, 3.75),
                ('beta', 'second', false, 4.5, 5.25, 6.75)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE mixed_types")
        .expect("flush rows");

    let (types, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT name, note, active, ratio, score, precise FROM mixed_types WHERE active = true",
            )
            .expect("select active row"),
    );
    assert_eq!(
        types,
        vec![
            DataType::VarChar,
            DataType::VarChar,
            DataType::Bool,
            DataType::Float32,
            DataType::Float32,
            DataType::Float64,
        ]
    );
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::VarChar(Some("alpha".to_string())),
            ScalarValue::VarChar(Some("first".to_string())),
            ScalarValue::Bool(Some(true)),
            ScalarValue::Float32(Some(1.5)),
            ScalarValue::Float32(Some(2.25)),
            ScalarValue::Float64(Some(3.75)),
        ]]
    );

    let (_, sorted) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT name FROM mixed_types WHERE precise > 4.0 ORDER BY name",
            )
            .expect("filter float and sort string"),
    );
    assert_eq!(
        sorted,
        vec![vec![ScalarValue::VarChar(Some("beta".to_string()))]]
    );

    let (_, aggregated) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT SUM(ratio), AVG(score), MIN(name), MAX(name) FROM mixed_types",
            )
            .expect("aggregate mixed types"),
    );
    assert_eq!(
        aggregated,
        vec![vec![
            ScalarValue::Float64(Some(6.0)),
            ScalarValue::Float64(Some(3.75)),
            ScalarValue::VarChar(Some("alpha".to_string())),
            ScalarValue::VarChar(Some("beta".to_string())),
        ]]
    );
}

#[test]
fn bool_accepts_numeric_and_string_literals() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE flags (from_int BOOL, from_text BOOL)",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO flags VALUES (1, 'false')")
        .expect("insert coerced bools");

    let (_, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT from_int, from_text FROM flags WHERE from_int = true",
            )
            .expect("select coerced bools"),
    );
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::Bool(Some(true)),
            ScalarValue::Bool(Some(false)),
        ]]
    );
}
