use std::sync::{Arc, Barrier};
use std::time::Instant;

use tempfile::TempDir;

use taibai::common::config::set_insert_profile_enabled;
use taibai::net::session::Session;
use taibai::sql::execution::engine::Engine;
use taibai::sql::execution::AutoCompactConfig;
use taibai::storage::segment_manager::SegmentConfig;

const DEAL_DATE: i32 = 20260911;
const ROWS_PER_STATEMENT: usize = 100;
const SYMBOLS: usize = 100;

fn create_table_sql() -> &'static str {
    "CREATE TABLE rt_bench (
        security_id VARCHAR,
        deal_date INT32,
        deal_time INT32,
        px DECIMAL(9,2),
        qty INT64,
        ts TIMESTAMP
    ) PARTITION BY deal_date CLUSTER BY (security_id, deal_date)"
}

fn build_insert_stmt(table: &str, stmt_idx: usize, rows: usize) -> String {
    let mut sql = format!("INSERT INTO {table} VALUES ");
    for row in 0..rows {
        let seq = (stmt_idx * rows + row) as i64;
        let symbol = format!("S{:03}", seq as usize % SYMBOLS);
        if row > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "('{symbol}', {DEAL_DATE}, {}, {}.{}, {}, {})",
            34_000 + (seq % 14_400) as i32,
            10 + seq % 90,
            (seq % 100),
            100 + seq % 900,
            1_783_700_000_000_000 + seq * 1_000
        ));
    }
    sql
}

fn start_engine_strict(dir: &TempDir) -> Engine {
    Engine::new(dir.path(), "main").expect("failed to create strict engine")
}

fn start_engine_async_fsync(dir: &TempDir) -> Engine {
    Engine::new_with_storage_config_and_auto_compact(
        dir.path(),
        "main",
        SegmentConfig::default(),
        AutoCompactConfig::default(),
        64 * 1024 * 1024,
        true,
        taibai::common::config::PubSubSettings::default(),
    )
    .expect("failed to create async-fsync engine")
}

struct PhaseReport {
    wall_ms: u128,
    statements: usize,
    rows: usize,
}

impl PhaseReport {
    fn print(&self, label: &str) {
        let stmt_rate = self.statements as f64 / (self.wall_ms as f64 / 1000.0);
        let row_rate = self.rows as f64 / (self.wall_ms as f64 / 1000.0);
        eprintln!(
            "[rt-bench] phase={label} statements={} rows={} wall_ms={} stmt/s={:.0} rows/s={:.0}",
            self.statements, self.rows, self.wall_ms, stmt_rate, row_rate
        );
    }
}

fn run_warmup(engine: &Engine, session: &mut Session, statements: usize) {
    for stmt_idx in 0..statements {
        let sql = build_insert_stmt("rt_bench", 100_000 + stmt_idx, ROWS_PER_STATEMENT);
        engine.execute(session, &sql).expect("warmup insert");
    }
}

fn run_serial_phase(engine: &Engine, statements: usize) -> PhaseReport {
    let mut session = Session::new("root".to_string(), "main".to_string());
    let start = Instant::now();
    for stmt_idx in 0..statements {
        let sql = build_insert_stmt("rt_bench", stmt_idx, ROWS_PER_STATEMENT);
        engine.execute(&mut session, &sql).expect("serial insert");
    }
    PhaseReport {
        wall_ms: start.elapsed().as_millis(),
        statements,
        rows: statements * ROWS_PER_STATEMENT,
    }
}

fn run_concurrent_phase(
    engine: &Arc<Engine>,
    threads: usize,
    statements_per_thread: usize,
) -> PhaseReport {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for thread_idx in 0..threads {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut session = Session::new("root".to_string(), "main".to_string());
            barrier.wait();
            for stmt_idx in 0..statements_per_thread {
                let global_idx = thread_idx * 1_000 + stmt_idx;
                let sql = build_insert_stmt("rt_bench", global_idx, ROWS_PER_STATEMENT);
                engine
                    .execute(&mut session, &sql)
                    .expect("concurrent insert");
            }
        }));
    }
    barrier.wait();
    let start = Instant::now();
    for handle in handles {
        handle.join().expect("worker thread panicked");
    }
    let wall_ms = start.elapsed().as_millis().max(1);
    let statements = threads * statements_per_thread;
    PhaseReport {
        wall_ms,
        statements,
        rows: statements * ROWS_PER_STATEMENT,
    }
}

/// Diagnostic for realtime-shape INSERT throughput. Run with:
/// `cargo test --test realtime_write_scaling_test realtime_group_commit_scaling_diagnostic -- --ignored --nocapture`
///
/// Measures the same 100-row autocommit INSERT shape as the realtime bench in
/// three modes: one synchronous client (strict fsync), eight concurrent
/// clients (strict fsync, exercises durable-tail group commit), and eight
/// concurrent clients with `tail_async_fsync` enabled. Insert profiling is
/// enabled so `stage=tail_group requests=N` lines show the realised group
/// sizes on the single-owner tail commit thread.
#[ignore]
#[test]
fn realtime_group_commit_scaling_diagnostic() {
    set_insert_profile_enabled(true);

    let serial_dir = TempDir::new().expect("serial temp dir");
    let engine = start_engine_strict(&serial_dir);
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, create_table_sql())
        .expect("create table (serial)");
    run_warmup(&engine, &mut session, 10);
    let serial = run_serial_phase(&engine, 200);
    serial.print("serial_1client_strict_fsync");
    engine.segment_mgr().shutdown();

    let concurrent_dir = TempDir::new().expect("concurrent temp dir");
    let engine = Arc::new(start_engine_strict(&concurrent_dir));
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, create_table_sql())
        .expect("create table (concurrent)");
    run_warmup(&engine, &mut session, 10);
    drop(session);
    let concurrent = run_concurrent_phase(&engine, 8, 25);
    concurrent.print("concurrent8_strict_fsync");
    engine.segment_mgr().shutdown();

    let async_dir = TempDir::new().expect("async temp dir");
    let engine = Arc::new(start_engine_async_fsync(&async_dir));
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, create_table_sql())
        .expect("create table (async)");
    run_warmup(&engine, &mut session, 10);
    drop(session);
    let async_report = run_concurrent_phase(&engine, 8, 25);
    async_report.print("concurrent8_async_fsync");
    engine.segment_mgr().shutdown();
}

/// Permanent regression guard for the realtime write path: concurrent
/// autocommit INSERTs must all become visible exactly once, and every
/// acknowledged statement must survive a restart via the durable tail.
#[test]
fn concurrent_autocommit_inserts_are_visible_and_durable() {
    concurrent_autocommit_inserts_are_visible_and_durable_with(false);
}

/// Same contract under `tail_async_fsync`: acknowledgements may precede the
/// storage barrier, but an orderly shutdown must sync the deferred dirty
/// tails so acknowledged rows still survive a restart.
#[test]
fn concurrent_async_fsync_inserts_are_visible_and_durable_after_shutdown() {
    concurrent_autocommit_inserts_are_visible_and_durable_with(true);
}

fn concurrent_autocommit_inserts_are_visible_and_durable_with(async_fsync: bool) {
    let dir = TempDir::new().expect("temp dir");
    let engine = Arc::new(if async_fsync {
        start_engine_async_fsync(&dir)
    } else {
        start_engine_strict(&dir)
    });
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, create_table_sql())
        .expect("create table");

    let threads = 8;
    let statements_per_thread = 5;
    let rows_per_statement = 10;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::with_capacity(threads);
    for thread_idx in 0..threads {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut session = Session::new("root".to_string(), "main".to_string());
            barrier.wait();
            for stmt_idx in 0..statements_per_thread {
                let global_idx = thread_idx * 100 + stmt_idx;
                let sql = build_insert_stmt("rt_bench", global_idx, rows_per_statement);
                engine
                    .execute(&mut session, &sql)
                    .expect("concurrent insert");
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("worker thread panicked");
    }

    let expected_rows = (threads * statements_per_thread * rows_per_statement) as i64;
    let counted = count_rows(&engine, &mut session);
    assert_eq!(
        counted, expected_rows,
        "all concurrent inserts must be visible"
    );
    drop(session);
    let mut engine = Arc::try_unwrap(engine)
        .unwrap_or_else(|_| panic!("worker threads must release the engine before shutdown"));
    engine
        .shutdown()
        .expect("ordered engine shutdown must checkpoint the durable tail");

    let restarted = if async_fsync {
        start_engine_async_fsync(&dir)
    } else {
        start_engine_strict(&dir)
    };
    let mut session = Session::new("root".to_string(), "main".to_string());
    let counted_after_restart = count_rows(&restarted, &mut session);
    assert_eq!(
        counted_after_restart, expected_rows,
        "acknowledged rows must survive restart via the durable tail"
    );
    let mut restarted = restarted;
    restarted
        .shutdown()
        .expect("restart engine shutdown must succeed");
}

fn count_rows(engine: &Engine, session: &mut Session) -> i64 {
    let result = engine
        .execute(session, "SELECT COUNT(*) FROM rt_bench")
        .expect("count query");
    let taibai::sql::execution::engine::ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches[0]
        .scalar(0, 0)
        .and_then(|value| match value {
            taibai::types::scalar::ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count scalar")
}
