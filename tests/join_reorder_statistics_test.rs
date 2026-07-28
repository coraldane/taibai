use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

#[test]
fn three_way_inner_join_uses_statistics_without_changing_result_layout() {
    let temp = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE large_fact (id INT64, fact_value VARCHAR)",
        )
        .expect("create large_fact");
    engine
        .execute(
            &mut session,
            "CREATE TABLE medium_dim (id INT64, dim_value VARCHAR)",
        )
        .expect("create medium_dim");
    engine
        .execute(
            &mut session,
            "CREATE TABLE tiny_filter (id INT64, filter_value VARCHAR)",
        )
        .expect("create tiny_filter");

    engine
        .execute(
            &mut session,
            "INSERT INTO large_fact VALUES
                (1, 'fact-1'), (2, 'fact-2'), (3, 'fact-3'),
                (4, 'fact-4'), (5, 'fact-5')",
        )
        .expect("insert large_fact");
    engine
        .execute(
            &mut session,
            "INSERT INTO medium_dim VALUES
                (1, 'dim-1'), (2, 'dim-2'), (3, 'dim-3')",
        )
        .expect("insert medium_dim");
    engine
        .execute(&mut session, "INSERT INTO tiny_filter VALUES (2, 'keep')")
        .expect("insert tiny_filter");

    for table in ["large_fact", "medium_dim", "tiny_filter"] {
        engine
            .execute(&mut session, &format!("ANALYZE TABLE {table}"))
            .unwrap_or_else(|error| panic!("analyze {table}: {error}"));
    }
    assert_eq!(
        engine
            .get_table_meta("main", "large_fact")
            .unwrap()
            .valid_statistics()
            .unwrap()
            .row_count,
        5
    );
    assert_eq!(
        engine
            .get_table_meta("main", "tiny_filter")
            .unwrap()
            .valid_statistics()
            .unwrap()
            .row_count,
        1
    );

    let explained = engine
        .execute(
            &mut session,
            "EXPLAIN SELECT f.fact_value, d.dim_value, t.filter_value
             FROM large_fact f
             JOIN medium_dim d ON f.id = d.id
             JOIN tiny_filter t ON d.id = t.id",
        )
        .expect("explain reordered join");
    let ExecResult::Query { batches, .. } = explained else {
        panic!("expected explain result");
    };
    let plan = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).filter_map(|row| match batch.scalar(row, 0) {
                Some(ScalarValue::VarChar(Some(line))) => Some(line),
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join("\n");
    let tiny = plan.find("Scan: table=tiny_filter").expect("tiny scan");
    let medium = plan.find("Scan: table=medium_dim").expect("medium scan");
    let large = plan.find("Scan: table=large_fact").expect("large scan");
    assert!(
        tiny < large && medium < large,
        "unexpected CBO plan: {plan}"
    );
    assert!(plan.contains("Join: Inner, estimated_rows=1"));

    let analyzed = engine
        .execute(
            &mut session,
            "EXPLAIN ANALYZE
             SELECT f.fact_value, d.dim_value, t.filter_value
             FROM large_fact f
             JOIN medium_dim d ON f.id = d.id
             JOIN tiny_filter t ON d.id = t.id",
        )
        .expect("explain analyze reordered join");
    let ExecResult::Query { batches, .. } = analyzed else {
        panic!("expected explain analyze result");
    };
    let analyzed_plan = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).filter_map(|row| match batch.scalar(row, 0) {
                Some(ScalarValue::VarChar(Some(line))) => Some(line),
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        analyzed_plan
            .matches("Join Inner: estimated_rows=1, actual_rows=1")
            .count(),
        2,
        "unexpected join metrics: {analyzed_plan}"
    );
    assert!(
        analyzed_plan.contains("Project: estimated_rows=1, actual_rows=1"),
        "{analyzed_plan}"
    );
    assert!(analyzed_plan.contains("peak_memory="), "{analyzed_plan}");

    let result = engine
        .execute(
            &mut session,
            "SELECT f.fact_value, d.dim_value, t.filter_value
             FROM large_fact f
             JOIN medium_dim d ON f.id = d.id
             JOIN tiny_filter t ON d.id = t.id",
        )
        .expect("three-way join");
    let ExecResult::Query { schema, batches } = result else {
        panic!("expected query result");
    };

    assert_eq!(
        schema
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["fact_value", "dim_value", "filter_value"]
    );
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..3)
                    .map(|column| batch.scalar(row, column).expect("join value"))
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1, "unexpected join rows: {rows:?}");
    assert_eq!(
        batches[0].scalar(0, 0),
        Some(ScalarValue::VarChar(Some("fact-2".to_string())))
    );
    assert_eq!(
        batches[0].scalar(0, 1),
        Some(ScalarValue::VarChar(Some("dim-2".to_string())))
    );
    assert_eq!(
        batches[0].scalar(0, 2),
        Some(ScalarValue::VarChar(Some("keep".to_string())))
    );
}
