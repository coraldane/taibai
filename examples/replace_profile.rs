//! Reproducible local REPLACE benchmark. Uses temporary data only.
//! cargo run --release --example replace_profile
//! Add -- --eager-checkpoint to simulate the former whole-table preflush policy.
use std::time::Instant;
use taibai::{
    net::session::Session,
    sql::execution::engine::{Engine, ExecResult},
    types::scalar::ScalarValue,
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let eager_checkpoint = std::env::args()
        .skip(1)
        .any(|arg| arg == "--eager-checkpoint");
    eprintln!("POLICY eager_checkpoint={eager_checkpoint}");
    for (n, batch, flushed, conflict) in [
        (1000, 1, false, true),
        (4095, 1, false, true),
        (4096, 1, false, true),
        (4096, 1, false, false),
        (20000, 1, false, true),
        (20000, 1, false, false),
        (20000, 100, false, true),
        (20000, 5000, false, true),
        (20000, 1, true, true),
        (20000, 100, true, true),
    ] {
        let dir = tempfile::tempdir()?;
        let mut engine = Engine::new(dir.path(), "main")?;
        let mut session = Session::new("root".into(), "main".into());
        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 PRIMARY KEY, payload VARCHAR)",
        )?;
        let values = (0..n)
            .map(|i| format!("({},'{}')", i, "x".repeat(128)))
            .collect::<Vec<_>>()
            .join(",");
        engine.execute(&mut session, &format!("INSERT INTO t VALUES {}", values))?;
        if flushed {
            engine.execute(&mut session, "FLUSH TABLE t")?;
        }
        // Automatic flushing may already have moved the seed rows to segments.
        // `flushed=false` means no explicit FLUSH, not guaranteed residency.
        let resident_rows: u64 = engine
            .memtable_manager()
            .get_by_table("main", "t")
            .iter()
            .map(|mt| mt.row_count())
            .sum();
        eprintln!("SEED n={n} resident_rows={resident_rows}");
        let start_key = if conflict { 0 } else { n };
        let values = (start_key..start_key + batch)
            .map(|i| format!("({},'{}')", i, "y".repeat(128)))
            .collect::<Vec<_>>()
            .join(",");
        for iteration in 0..4 {
            eprintln!("CASE n={n} batch={batch} flushed={flushed} initial_conflict={conflict} iteration={iteration}");
            let t = Instant::now();
            if eager_checkpoint {
                engine.batch_writer().checkpoint_before_rewrite(
                    "main",
                    "t",
                    taibai::storage::core::batch_writer::RewriteEstimate::Inserts(batch as usize),
                )?;
            }
            engine.execute(&mut session, &format!("REPLACE INTO t VALUES {}", values))?;
            println!("RESULT n={n} batch={batch} flushed={flushed} initial_conflict={conflict} iteration={iteration} us={}",t.elapsed().as_micros());
        }
        let ExecResult::Query { batches, .. } =
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?
        else {
            panic!("expected query")
        };
        assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::Int64(Some(if conflict {
                n
            } else {
                n + batch
            })))
        );
        let ExecResult::Query { batches, .. } = engine.execute(
            &mut session,
            &format!("SELECT payload FROM t WHERE id = {start_key}"),
        )?
        else {
            panic!("expected query")
        };
        assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some("y".repeat(128))))
        );
        // Validate physical scan results too: catalog COUNT alone could hide duplicates.
        let ExecResult::Query { batches, .. } =
            engine.execute(&mut session, "SELECT id, payload FROM t")?
        else {
            panic!("expected query")
        };
        let mut seen = std::collections::HashSet::new();
        for b in batches {
            for row in 0..b.num_rows() {
                let Some(ScalarValue::Int64(Some(id))) = b.scalar(row, 0) else {
                    panic!("expected id")
                };
                assert!(seen.insert(id), "duplicate id {id}");
                let expected = if (start_key..start_key + batch).contains(&id) {
                    "y"
                } else {
                    "x"
                };
                assert_eq!(
                    b.scalar(row, 1),
                    Some(ScalarValue::VarChar(Some(expected.repeat(128))))
                );
            }
        }
        assert_eq!(seen.len() as i64, if conflict { n } else { n + batch });
        engine.shutdown()?;
    }
    Ok(())
}
