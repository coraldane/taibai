//! Performance harness for the hybrid hot-mirror scan on a month-partitioned
//! hot table (the `stock_minute` shape). Ignored by default; run explicitly:
//!
//! ```text
//! TAIBAI_HYBRID=0 cargo test --test hybrid_bench_test -- --ignored --nocapture --test-threads=1
//! TAIBAI_HYBRID=1 cargo test --test hybrid_bench_test -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Workload mirrors the production intraday pattern: pre-close days flushed
//! and archived by a close-of-day, then a post-close day streamed in
//! time-major order (each batch is one market-wide snapshot), and a point
//! lookup for one security on the post-close day.

use std::time::Instant;

use tempfile::TempDir;

use taibai::common::config::{set_query_limits, QueryLimits};
use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

const SECURITIES: usize = 5000;
const PRE_CLOSE_DAYS: usize = 5;
const PRE_CLOSE_SNAPSHOTS_PER_DAY: usize = 6;
const POST_CLOSE_SNAPSHOTS: usize = 120;
const TARGET_SECURITY: &str = "102500.SZ";

fn sec_id(idx: usize) -> String {
    // Six-digit prefix like the exchange's numbering.
    format!("{:06}.SZ", 100000 + idx)
}

fn new_engine(temp_dir: &TempDir) -> Engine {
    Engine::new_with_storage_config_and_auto_compact(
        temp_dir.path(),
        "main",
        taibai::storage::segment_manager::SegmentConfig::default(),
        taibai::sql::execution::AutoCompactConfig {
            enabled: false,
            ..taibai::sql::execution::AutoCompactConfig::default()
        },
        512 * 1024 * 1024,
        false,
        taibai::common::config::PubSubSettings::default(),
        true,
    )
    .expect("failed to create engine")
}

fn snapshot_insert(engine: &Engine, session: &mut Session, table: &str, day: i32, snap: usize) {
    let base_ts: i64 = 1_782_711_000_000_000;
    let mut values = String::with_capacity(SECURITIES * 48);
    for idx in 0..SECURITIES {
        values.push_str(&format!(
            "('{}', {}, {}, {}, {}),",
            sec_id(idx),
            day,
            (930 + snap) as i32,
            base_ts + (day as i64 - 20260900) * 86_400_000_000 + snap as i64 * 60_000_000,
            (idx % 97) as i32,
        ));
    }
    values.pop();
    engine
        .execute(session, &format!("INSERT INTO {table} VALUES {}", values))
        .expect("insert snapshot");
}

fn collect_rows(result: ExecResult) -> usize {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches.iter().map(|b| b.num_rows()).sum()
}

fn collect_count(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|b| b.scalar(0, 0))
        .and_then(|v| match v {
            ScalarValue::Int64(Some(c)) => Some(c),
            _ => None,
        })
        .expect("count")
}

fn timed<F: FnMut()>(mut f: F) -> f64 {
    f(); // warm-up
    let start = Instant::now();
    f();
    start.elapsed().as_secs_f64() * 1000.0
}

#[test]
#[ignore]
fn perf_month_partition_point_lookup() {
    let hybrid = std::env::var("TAIBAI_HYBRID").unwrap_or_else(|_| "1".into()) != "0";
    let _ = set_query_limits(QueryLimits {
        hybrid_hot_scan_enabled: hybrid,
        ..QueryLimits::default()
    });

    let temp_dir = TempDir::new().expect("temp dir");
    let engine = new_engine(&temp_dir);
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE bench_minute (
                security_id VARCHAR,
                deal_date INT32,
                deal_time INT32,
                gmt_create TIMESTAMP,
                ts INT32
            ) CLUSTER BY (security_id, deal_time)
              HOT BY (gmt_create) INDEX (security_id) CLOSE AT '23:59'
              PARTITION BY MONTH(deal_date)",
        )
        .expect("create bench table");

    println!(
        "loading: {PRE_CLOSE_DAYS} pre-close days x {} snapshots x {SECURITIES} securities ...",
        PRE_CLOSE_SNAPSHOTS_PER_DAY
    );
    for day_offset in 0..PRE_CLOSE_DAYS {
        let day = (20260910 + day_offset as i32);
        for snap in 0..PRE_CLOSE_SNAPSHOTS_PER_DAY {
            snapshot_insert(&engine, &mut session, "bench_minute", day, snap);
        }
    }
    engine
        .execute(&mut session, "FLUSH TABLE bench_minute")
        .expect("flush pre-close rows");
    engine
        .close_of_day_flush_table("main", "bench_minute")
        .expect("close-of-day");

    println!(
        "loading: post-close day with {POST_CLOSE_SNAPSHOTS} snapshots x {SECURITIES} securities ..."
    );
    for snap in 0..POST_CLOSE_SNAPSHOTS {
        snapshot_insert(&engine, &mut session, "bench_minute", 20260917, snap);
    }

    let total = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM bench_minute WHERE deal_date = 20260917",
            )
            .expect("count post-close day"),
    );
    println!(
        "hybrid={} rows_total={}",
        hybrid,
        PRE_CLOSE_DAYS * PRE_CLOSE_SNAPSHOTS_PER_DAY * SECURITIES + total as usize
    );

    // Point lookup on the post-close day: the production shape.
    let point_sql = format!(
        "SELECT * FROM bench_minute WHERE deal_date = 20260917 AND security_id = '{TARGET_SECURITY}'"
    );
    let rows = collect_rows(
        engine
            .execute(&mut session, &point_sql)
            .expect("point lookup"),
    );
    let ms = timed(|| {
        let r = engine
            .execute(&mut session, &point_sql)
            .expect("point lookup");
        assert_eq!(collect_rows(r), rows);
    });
    println!("hybrid={hybrid} point_lookup rows={rows} ms={ms:.2}",);

    let count_sql = format!(
        "SELECT count(*) FROM bench_minute WHERE deal_date = 20260917 AND security_id = '{TARGET_SECURITY}'"
    );
    let count = collect_count(engine.execute(&mut session, &count_sql).expect("count"));
    let ms = timed(|| {
        assert_eq!(
            collect_count(engine.execute(&mut session, &count_sql).expect("count")),
            count
        );
    });
    println!("hybrid={hybrid} point_count count={count} ms={ms:.2}");

    // Key-only lookup across the coverage boundary (mirror + segment legs).
    let key_sql =
        format!("SELECT count(*) FROM bench_minute WHERE security_id = '{TARGET_SECURITY}'");
    let count = collect_count(engine.execute(&mut session, &key_sql).expect("key count"));
    let ms = timed(|| {
        assert_eq!(
            collect_count(engine.execute(&mut session, &key_sql).expect("key count")),
            count
        );
    });
    println!("hybrid={hybrid} key_only_count count={count} ms={ms:.2}");

    // Archived-day lookup: must not take the hybrid leg, must stay exact.
    let archived_sql = format!(
        "SELECT count(*) FROM bench_minute WHERE deal_date = 20260910 AND security_id = '{TARGET_SECURITY}'"
    );
    let count = collect_count(
        engine
            .execute(&mut session, &archived_sql)
            .expect("archived"),
    );
    let ms = timed(|| {
        assert_eq!(
            collect_count(
                engine
                    .execute(&mut session, &archived_sql)
                    .expect("archived")
            ),
            count
        );
    });
    println!("hybrid={hybrid} archived_day_count count={count} ms={ms:.2}");

    engine.segment_mgr().shutdown();
}
