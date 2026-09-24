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
fn qualified_column_supports_is_null_predicates() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE nullable_t (id INT, note VARCHAR)",
        )
        .expect("create nullable_t");
    engine
        .execute(
            &mut session,
            "INSERT INTO nullable_t VALUES (1, 'value'), (2, NULL)",
        )
        .expect("insert nullable_t");

    let non_null_rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id FROM nullable_t t WHERE t.note IS NOT NULL ORDER BY id",
            )
            .expect("qualified IS NOT NULL query"),
    );
    assert_eq!(non_null_rows, vec![vec![ScalarValue::Int32(Some(1))]]);

    let null_rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id FROM nullable_t t WHERE t.note IS NULL ORDER BY id",
            )
            .expect("qualified IS NULL query"),
    );
    assert_eq!(null_rows, vec![vec![ScalarValue::Int32(Some(2))]]);

    engine.segment_mgr().shutdown();
}

#[test]
fn count_star_matches_select_rows_for_constant_true_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);
    engine
        .execute(&mut session, "FLUSH TABLE predicate_t")
        .expect("flush predicate_t");

    let count = count_star(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM predicate_t WHERE 1 = 1")
            .expect("count rows"),
    );
    let rows = collect_rows(
        engine
            .execute(&mut session, "SELECT * FROM predicate_t WHERE 1 = 1")
            .expect("select rows"),
    );

    assert_eq!(count, 4);
    assert_eq!(rows.len() as i64, count);
    engine.segment_mgr().shutdown();
}

#[test]
fn count_star_matches_select_rows_for_constant_false_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);
    engine
        .execute(&mut session, "FLUSH TABLE predicate_t")
        .expect("flush predicate_t");

    let count = count_star(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM predicate_t WHERE 1 = 0")
            .expect("count rows"),
    );
    let rows = collect_rows(
        engine
            .execute(&mut session, "SELECT * FROM predicate_t WHERE 1 = 0")
            .expect("select rows"),
    );

    assert_eq!(count, 0);
    assert_eq!(rows.len() as i64, count);
    engine.segment_mgr().shutdown();
}

#[test]
fn count_star_matches_select_rows_across_segments_and_memtables() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_predicate_tables(&engine, &mut session);
    engine
        .execute(&mut session, "FLUSH TABLE predicate_t")
        .expect("flush predicate_t");
    engine
        .execute(
            &mut session,
            "INSERT INTO predicate_t VALUES
                (5, 'e'),
                (6, 'f')",
        )
        .expect("insert memtable rows");

    let count = count_star(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM predicate_t WHERE 1 = 1")
            .expect("count rows"),
    );
    let rows = collect_rows(
        engine
            .execute(&mut session, "SELECT * FROM predicate_t WHERE 1 = 1")
            .expect("select rows"),
    );
    assert_eq!(count, 6);
    assert_eq!(rows.len() as i64, count);

    let false_count = count_star(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM predicate_t WHERE 1 = 0")
            .expect("count false rows"),
    );
    let false_rows = collect_rows(
        engine
            .execute(&mut session, "SELECT * FROM predicate_t WHERE 1 = 0")
            .expect("select false rows"),
    );
    assert_eq!(false_count, 0);
    assert_eq!(false_rows.len() as i64, false_count);
    engine.segment_mgr().shutdown();
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

/// Regression: a GROUP BY scan narrowed to its group/aggregate columns must
/// still filter rows correctly on segments whose writer schema differs from
/// the current table schema (e.g. after ALTER TABLE). The schema-differs
/// fallback used to apply full-table predicate indices against the narrow
/// projected batch: an equality on a column outside the projection zeroed
/// every row, and a range on one was silently skipped (row-group zone-map
/// granularity only).
#[test]
fn group_by_predicates_on_schema_differring_segment() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE monitor_t (
                id INT64,
                deal_date INT32,
                table_name VARCHAR,
                status VARCHAR,
                target_count INT64
            )",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO monitor_t VALUES
                (1, 20250102, 't_a', 'COMPLETED', 100),
                (2, 20250102, 't_b', 'COMPLETED', 200),
                (3, 20250604, 't_a', 'COMPLETED', 300),
                (4, 20250604, 't_b', 'COMPLETED', 400),
                (5, 20240701, 't_a', 'COMPLETED', 500),
                (6, 20240701, 't_b', 'COMPLETED', 600),
                (7, 20260301, 't_a', 'COMPLETED', 700),
                (8, 20260301, 't_b', 'COMPLETED', 800)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE monitor_t")
        .expect("flush rows");

    // The segment now holds 5 columns while the table schema has 6, so
    // every row group read takes the schema-differs fallback path.
    engine
        .execute(&mut session, "ALTER TABLE monitor_t ADD COLUMN extra VARCHAR")
        .expect("add column");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT table_name, COUNT(*), MIN(deal_date), MAX(deal_date), SUM(target_count)
                 FROM monitor_t
                 WHERE status = 'COMPLETED' AND deal_date >= 20250101 AND deal_date <= 20251231
                 GROUP BY table_name",
            )
            .expect("grouped aggregate over schema-differring segment"),
    );

    assert_eq!(
        rows,
        vec![
            vec![
                ScalarValue::VarChar(Some("t_a".into())),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int32(Some(20250102)),
                ScalarValue::Int32(Some(20250604)),
                ScalarValue::Int64(Some(400)),
            ],
            vec![
                ScalarValue::VarChar(Some("t_b".into())),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int32(Some(20250102)),
                ScalarValue::Int32(Some(20250604)),
                ScalarValue::Int64(Some(600)),
            ],
        ]
    );

    // Range-only variant: the 2024 and 2026 rows must not leak in.
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT table_name, COUNT(*), MIN(deal_date), MAX(deal_date)
                 FROM monitor_t
                 WHERE deal_date >= 20250101 AND deal_date <= 20251231
                 GROUP BY table_name",
            )
            .expect("range grouped aggregate over schema-differring segment"),
    );
    for row in &rows {
        assert_eq!(
            row[1],
            ScalarValue::Int64(Some(2)),
            "range must filter to exactly the two 2025 rows per group"
        );
        assert_eq!(row[2], ScalarValue::Int32(Some(20250102)));
        assert_eq!(row[3], ScalarValue::Int32(Some(20250604)));
    }
}
