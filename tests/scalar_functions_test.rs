use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

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
fn literal_select_supports_ifnull_and_coalesce() {
    let (_temp_dir, engine, mut session) = create_engine();

    let (types, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COALESCE(NULL, 'fallback') AS text_value, IFNULL(NULL, 7) AS int_value",
            )
            .expect("select scalar functions"),
    );

    assert_eq!(types, vec![DataType::VarChar, DataType::Int64]);
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::VarChar(Some("fallback".to_string())),
            ScalarValue::Int64(Some(7)),
        ]]
    );
}

#[test]
fn registered_scalar_functions_share_the_same_sql_path() {
    let (_temp_dir, engine, mut session) = create_engine();

    let (types, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT UPPER('abc') AS upper_value, LENGTH('abcd') AS len_value, TO_INT('42') AS int_value",
            )
            .expect("select registered scalar functions"),
    );

    assert_eq!(
        types,
        vec![DataType::VarChar, DataType::Int64, DataType::Int64]
    );
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::VarChar(Some("ABC".to_string())),
            ScalarValue::Int64(Some(4)),
            ScalarValue::Int64(Some(42)),
        ]]
    );
}

#[test]
fn table_query_supports_control_functions_in_projection_filter_and_sort() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE scalar_func_t (
                id INT,
                nickname VARCHAR,
                score INT
            )",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO scalar_func_t VALUES
                (1, NULL, NULL),
                (2, 'bee', 5),
                (3, NULL, 3)",
        )
        .expect("insert rows");

    let (types, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id, COALESCE(nickname, 'missing') AS name, IFNULL(score, 0) AS safe_score
                 FROM scalar_func_t
                 WHERE COALESCE(nickname, 'missing') = 'missing'
                 ORDER BY IFNULL(score, 0)",
            )
            .expect("select with scalar functions"),
    );

    assert_eq!(
        types,
        vec![DataType::Int32, DataType::VarChar, DataType::Int64]
    );
    assert_eq!(
        rows,
        vec![
            vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::VarChar(Some("missing".to_string())),
                ScalarValue::Int64(Some(0)),
            ],
            vec![
                ScalarValue::Int32(Some(3)),
                ScalarValue::VarChar(Some("missing".to_string())),
                ScalarValue::Int64(Some(3)),
            ],
        ]
    );
}

#[test]
fn scalar_functions_work_in_join_context() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(&mut session, "CREATE TABLE left_t (id INT, label VARCHAR)")
        .expect("create left");
    engine
        .execute(&mut session, "CREATE TABLE right_t (id INT, label VARCHAR)")
        .expect("create right");
    engine
        .execute(
            &mut session,
            "INSERT INTO left_t VALUES (1, NULL), (2, 'two')",
        )
        .expect("insert left");
    engine
        .execute(
            &mut session,
            "INSERT INTO right_t VALUES (1, 'one'), (2, NULL)",
        )
        .expect("insert right");

    let (_, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT l.id, COALESCE(l.label, r.label, 'unknown') AS label
                 FROM left_t l
                 LEFT JOIN right_t r ON IFNULL(l.id, 0) = r.id
                 ORDER BY l.id",
            )
            .expect("select join"),
    );

    assert_eq!(
        rows,
        vec![
            vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::VarChar(Some("one".to_string())),
            ],
            vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::VarChar(Some("two".to_string())),
            ],
        ]
    );
}

#[test]
fn insert_values_can_use_ifnull_and_coalesce() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE insert_scalar_func_t (id INT, label VARCHAR)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO insert_scalar_func_t VALUES (IFNULL(NULL, 3), COALESCE(NULL, 'three'))",
        )
        .expect("insert with scalar functions");

    let (types, rows) = collect_rows(
        engine
            .execute(&mut session, "SELECT id, label FROM insert_scalar_func_t")
            .expect("select inserted row"),
    );

    assert_eq!(types, vec![DataType::Int32, DataType::VarChar]);
    assert_eq!(
        rows,
        vec![vec![
            ScalarValue::Int32(Some(3)),
            ScalarValue::VarChar(Some("three".to_string())),
        ]]
    );
}
