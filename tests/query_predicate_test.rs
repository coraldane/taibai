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

fn seed_predicate_tables(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE predicate_t (
                id INT,
                kind VARCHAR
            )",
        )
        .expect("create predicate_t");
    engine
        .execute(
            session,
            "INSERT INTO predicate_t VALUES
                (1, 'a'),
                (2, 'b'),
                (3, 'c'),
                (4, 'd')",
        )
        .expect("insert predicate_t");
}

#[test]
fn in_inside_or_filters_correctly() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id FROM predicate_t WHERE id IN (1, 3) OR kind = 'b' ORDER BY id",
            )
            .expect("select with IN OR"),
    );

    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int32(Some(1))],
            vec![ScalarValue::Int32(Some(2))],
            vec![ScalarValue::Int32(Some(3))],
        ]
    );
}

#[test]
fn not_in_falls_back_to_row_filter_correctly() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id FROM predicate_t WHERE id NOT IN (2, 4) ORDER BY id",
            )
            .expect("select with NOT IN"),
    );

    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int32(Some(1))],
            vec![ScalarValue::Int32(Some(3))],
        ]
    );
}

#[test]
fn in_subquery_can_be_used_with_or_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);
    engine
        .execute(&mut session, "CREATE TABLE allowed_ids (id INT)")
        .expect("create allowed_ids");
    engine
        .execute(&mut session, "INSERT INTO allowed_ids VALUES (1), (3)")
        .expect("insert allowed_ids");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id FROM predicate_t
                 WHERE id IN (SELECT id FROM allowed_ids) OR kind = 'd'
                 ORDER BY id",
            )
            .expect("select with IN subquery OR"),
    );

    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::Int32(Some(1))],
            vec![ScalarValue::Int32(Some(3))],
            vec![ScalarValue::Int32(Some(4))],
        ]
    );
}
