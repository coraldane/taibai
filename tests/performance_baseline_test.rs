use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn explain_text(result: ExecResult) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).filter_map(|row| match batch.scalar(row, 0) {
                Some(ScalarValue::VarChar(Some(value))) => Some(value),
                _ => None,
            })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn performance_baseline_normalizes_literals_persists_and_resets() {
    let temp = TempDir::new().unwrap();
    let fingerprint;
    {
        let engine = Engine::new(temp.path(), "main").unwrap();
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE metrics (id INT64, value INT64)")
            .unwrap();
        engine
            .execute(
                &mut session,
                "INSERT INTO metrics VALUES (1, 10), (2, 20), (3, 30)",
            )
            .unwrap();
        engine
            .execute(&mut session, "ANALYZE TABLE metrics")
            .unwrap();

        let mut fingerprints = Vec::new();
        for id in 1..=3 {
            let output = explain_text(
                engine
                    .execute(
                        &mut session,
                        &format!("EXPLAIN ANALYZE SELECT value FROM metrics WHERE id = {id}"),
                    )
                    .unwrap(),
            );
            let current = output
                .lines()
                .find_map(|line| line.strip_prefix("fingerprint      : "))
                .expect("baseline fingerprint")
                .to_string();
            fingerprints.push(current);
            if id < 3 {
                assert!(output.contains("status           : warming"), "{output}");
            } else {
                assert!(output.contains("status           : stable"), "{output}");
            }
        }
        assert!(fingerprints.windows(2).all(|pair| pair[0] == pair[1]));
        fingerprint = fingerprints[0].clone();
        assert_eq!(fingerprint.len(), 16);

        let shown = engine
            .execute(&mut session, "SHOW PERFORMANCE BASELINES")
            .unwrap();
        let ExecResult::Query { schema, batches } = shown else {
            panic!("expected baseline rows");
        };
        assert_eq!(schema.columns[0].name, "Fingerprint");
        assert_eq!(schema.columns[1].name, "Environment");
        assert_eq!(schema.columns[2].name, "Version");
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some(fingerprint.clone())))
        );
        assert_eq!(batches[0].scalar(0, 4), Some(ScalarValue::Int64(Some(3))));
        assert_eq!(
            batches[0].scalar(0, 5),
            Some(ScalarValue::VarChar(Some("stable".to_string())))
        );
        engine.segment_mgr().shutdown();
    }

    {
        let engine = Engine::new(temp.path(), "main").unwrap();
        let mut session = Session::new("root".to_string(), "main".to_string());
        let shown = engine
            .execute(&mut session, "SHOW PERFORMANCE BASELINES")
            .unwrap();
        let ExecResult::Query { batches, .. } = shown else {
            panic!("expected reloaded baseline rows");
        };
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some(fingerprint)))
        );

        let reset = engine
            .execute(&mut session, "RESET PERFORMANCE BASELINES")
            .unwrap();
        assert!(matches!(reset, ExecResult::Ok { rows_affected: 1 }));
        let shown = engine
            .execute(&mut session, "SHOW PERFORMANCE BASELINES")
            .unwrap();
        let ExecResult::Query { batches, .. } = shown else {
            panic!("expected empty baseline rows");
        };
        assert_eq!(batches[0].num_rows(), 0);
        engine.segment_mgr().shutdown();
    }
}
