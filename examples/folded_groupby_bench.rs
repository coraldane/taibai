//! In-process benchmark: whole-table `GROUP BY <day partition key>` through
//! the metadata folding engine vs the forced streaming pipeline
//! (`HAVING COUNT(*) > 0` declines the folded operator while keeping every
//! group), plus plain `COUNT(*)`.
//!
//! Run: cargo run --release --example folded_groupby_bench [days] [rows/day]

use std::time::Instant;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};

fn timed_count(engine: &Engine, session: &mut Session, sql: &str) -> (u64, u128) {
    let start = Instant::now();
    let result = engine.execute(session, sql).expect("query");
    let elapsed = start.elapsed().as_micros();
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result for {sql}");
    };
    let rows: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
    let total: i64 = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).filter_map(|row| match batch.scalar(row, 1) {
            Some(taibai::types::scalar::ScalarValue::Int64(Some(v))) => Some(v),
            _ => None,
        }))
        .sum();
    println!(
        "  {sql:<62} rows={rows:<4} sum(count)={total:<8} {elapsed:>9} µs"
    );
    (rows, elapsed)
}

fn day_string(day: i64) -> String {
    // Civil-from-days algorithm (Howard Hinnant) — no chrono dependency.
    let z = 19723i64 + day; // days since 1970-01-01 for 2024-01-01 + day
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

fn main() {
    let days: i64 = std::env::args()
        .nth(1)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(180);
    let per_day: i64 = std::env::args()
        .nth(2)
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(20_000);

    let dir = tempfile::tempdir().expect("temp dir");
    let engine = Engine::new(dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                security_id VARCHAR,
                deal_date DATE,
                price INT64,
                PRIMARY KEY (security_id, deal_date)
            ) PARTITION BY DAY(deal_date)",
        )
        .expect("create");

    let insert_start = Instant::now();
    for day in 0..days {
        let base = day_string(day);
        for chunk_start in (0..per_day).step_by(2_000) {
            let chunk_end = (chunk_start + 2_000).min(per_day);
            let mut sql = String::from("INSERT INTO ticks (security_id, deal_date, price) VALUES ");
            for row in chunk_start..chunk_end {
                if row > chunk_start {
                    sql.push_str(", ");
                }
                sql.push_str(&format!("('s{}', '{}', {})", row, base, row + 1));
            }
            engine.execute(&mut session, &sql).expect("insert");
        }
    }
    println!(
        "inserted {} rows across {days} days in {:.1}s; flushing",
        days * per_day,
        insert_start.elapsed().as_secs_f64()
    );
    engine.flush_table_buffers("main", "ticks").expect("flush");

    // Warm any lazy caches (footer/sidecar parses) so both paths measure the
    // same steady state.
    let (_, _) = timed_count(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date",
    );

    println!("folded (metadata) path:");
    let (_, folded_us) = timed_count(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date",
    );

    // Diagnostics: does the folding engine accept this shape directly?
    {
        use taibai::sql::execution::scan::aggregate_meta::compute_folded_aggregate;
        use taibai::sql::execution::scan::spec::ScanSpec;
        use taibai::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
        use taibai::types::batch::Schema;
        let table_meta = engine.get_table_meta("main", "ticks").expect("meta");
        let table_schema = Schema::new(table_meta.columns.clone());
        let mut spec = ScanSpec::new("ticks");
        spec.table_schema = table_schema.clone();
        let date_idx = table_schema.index_of("deal_date").expect("deal_date");
        let group_by = vec![LogicalExpr::Column(date_idx)];
        let aggregates = vec![AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Wildcard),
            distinct: false,
        }];
        let engine_start = Instant::now();
        let answer = compute_folded_aggregate(
            &spec,
            &group_by,
            &aggregates,
            &table_meta,
            &table_schema,
            &dir.path().join("main").join("ticks"),
            engine.segment_mgr(),
            engine.memtable_manager(),
            engine.hot_store_manager(),
            "main",
        )
        .expect("engine call");
        let engine_us = engine_start.elapsed().as_micros();
        match answer {
            Some(batch) => println!(
                "  engine direct: ACCEPTED, {} rows, first={:?}, {engine_us} µs",
                batch.num_rows(),
                batch.scalar(0, 0)
            ),
            None => println!("  engine direct: DECLINED, {engine_us} µs"),
        }
    }
    println!("streaming pipeline path (HAVING forces the fallback):");
    let (_, streamed_us) = timed_count(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date HAVING COUNT(*) > 0",
    );
    println!("plain COUNT(*):");
    let start = Instant::now();
    let result = engine
        .execute(&mut session, "SELECT COUNT(*) FROM ticks")
        .expect("count");
    let count_us = start.elapsed().as_micros();
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let total = match batches.first().and_then(|b| b.scalar(0, 0)) {
        Some(taibai::types::scalar::ScalarValue::Int64(Some(v))) => v,
        other => panic!("unexpected count result: {other:?}"),
    };
    println!("  SELECT COUNT(*) FROM ticks                                   count={total:<8} {count_us:>9} µs");

    println!(
        "\nGROUP BY speedup (streamed / folded): {:.1}x",
        streamed_us as f64 / folded_us as f64
    );

    engine.segment_mgr().shutdown();
}
