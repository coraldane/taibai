use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

#[test]
fn analyze_table_collects_persists_and_invalidates_statistics() {
    let temp = TempDir::new().unwrap();
    {
        let engine = Engine::new(temp.path(), "main").unwrap();
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE metrics (id INT, category VARCHAR(16), score INT)",
            )
            .unwrap();
        engine
            .execute(
                &mut session,
                "INSERT INTO metrics VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)",
            )
            .unwrap();
        engine
            .execute(&mut session, "INSERT INTO metrics VALUES (4, NULL, NULL)")
            .unwrap();
        let analyzed = engine
            .execute(&mut session, "ANALYZE TABLE main.metrics")
            .unwrap();
        assert!(matches!(analyzed, ExecResult::Ok { rows_affected: 4 }));

        let meta = engine.get_table_meta("main", "metrics").unwrap();
        let statistics = meta.valid_statistics().expect("fresh statistics");
        let id = statistics.columns.get("id").unwrap();
        assert!((3..=5).contains(&id.distinct_count));
        assert_eq!(id.null_count, 0);
        assert_eq!(id.min_value, Some(ScalarValue::Int32(Some(1))));
        assert_eq!(id.max_value, Some(ScalarValue::Int32(Some(4))));
        assert!(!id.histogram.is_empty());
        assert_eq!(
            id.histogram
                .iter()
                .map(|bucket| bucket.sample_count as u64)
                .sum::<u64>(),
            4
        );
        let category = statistics.columns.get("category").unwrap();
        assert!((2..=3).contains(&category.distinct_count));
        let score = statistics.columns.get("score").unwrap();
        assert_eq!(score.null_count, 0);
        assert_eq!(score.min_value, Some(ScalarValue::Int32(Some(0))));

        let explained = engine
            .execute(
                &mut session,
                "EXPLAIN SELECT id FROM metrics WHERE score < 15",
            )
            .expect("explain with statistics");
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
        assert!(plan.contains("estimated_rows=2"), "unexpected plan: {plan}");

        let analyzed_plan = engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT id FROM metrics WHERE score < 15",
            )
            .expect("explain analyze with operator metrics");
        let ExecResult::Query { batches, .. } = analyzed_plan else {
            panic!("expected explain analyze result");
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
        assert!(plan.contains("=== OPERATOR METRICS ==="), "{plan}");
        assert!(
            plan.contains("Project: estimated_rows=2, actual_rows=2"),
            "{plan}"
        );
        assert!(
            plan.contains("Scan metrics: estimated_rows=2, actual_rows=2"),
            "{plan}"
        );
        assert!(plan.contains("row_ratio=1.00x"), "{plan}");
        assert!(plan.contains("spill_files=0"), "{plan}");
        assert!(plan.contains("=== PERFORMANCE BASELINE ==="), "{plan}");
        assert!(plan.contains("samples          : 1"), "{plan}");
        assert!(plan.contains("status           : warming"), "{plan}");

        let count_plan = engine
            .execute(&mut session, "EXPLAIN ANALYZE SELECT COUNT(*) FROM metrics")
            .expect("explain analyze count");
        let ExecResult::Query { batches, .. } = count_plan else {
            panic!("expected count explain analyze result");
        };
        let count_plan = batches
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows()).filter_map(|row| match batch.scalar(row, 0) {
                    Some(ScalarValue::VarChar(Some(line))) => Some(line),
                    _ => None,
                })
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            count_plan.contains("Aggregate: estimated_rows=1, actual_rows=1"),
            "{count_plan}"
        );
        assert!(
            count_plan.contains("Scan metrics: estimated_rows=4, actual_rows=4"),
            "{count_plan}"
        );

        engine
            .execute(
                &mut session,
                "CREATE TABLE tenant_items (
                    tenant_id INT64,
                    item_id INT64,
                    payload VARCHAR,
                    PRIMARY KEY (tenant_id, item_id)
                )",
            )
            .expect("create composite primary key table");
        engine
            .execute(
                &mut session,
                "INSERT INTO tenant_items VALUES
                    (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c')",
            )
            .expect("insert composite keys");
        engine
            .execute(&mut session, "ANALYZE TABLE tenant_items")
            .expect("analyze composite keys");
        let composite_meta = engine.get_table_meta("main", "tenant_items").unwrap();
        let composite = &composite_meta.valid_statistics().unwrap().composite_columns;
        assert_eq!(composite.len(), 1);
        assert_eq!(composite[0].columns, vec!["tenant_id", "item_id"]);
        assert!(composite[0].unique);
        assert!((2..=4).contains(&composite[0].distinct_count));
        assert_eq!(composite[0].null_count, 0);
    }

    let engine = Engine::new(temp.path(), "main").unwrap();
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert!(engine
        .get_table_meta("main", "metrics")
        .unwrap()
        .valid_statistics()
        .is_some());
    assert!(engine
        .get_table_meta("main", "tenant_items")
        .unwrap()
        .valid_statistics()
        .is_some());
    engine
        .execute(&mut session, "INSERT INTO metrics VALUES (5, 'c', 50)")
        .unwrap();
    assert!(engine
        .get_table_meta("main", "metrics")
        .unwrap()
        .valid_statistics()
        .is_none());
}
