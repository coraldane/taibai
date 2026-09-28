//! Diagnostic for change-subscription (pub/sub) write-path overhead. Run with:
//! `cargo test --test pubsub_overhead_bench_test pubsub_overhead_diagnostic -- --ignored --nocapture`
//!
//! Measures sequential single-row autocommit INSERT throughput against a real
//! server in five scenarios:
//!
//! 1. `pubsub_disabled`  — master switch off (the floor).
//! 2. `no_subscribers`   — pubsub enabled, nobody listening (the common case).
//! 3. `v2_capture_only`  — a reliable-subscription channel exists but no
//!                         subscriber is attached: every commit is cloned and
//!                         mirrored into the retention log.
//! 4. `hub_draining`     — a SUBSCRIBE (notification) subscriber draining in
//!                         lockstep.
//! 5. `v2_draining`      — a reliable subscriber draining and acking.
//!
//! The engine runs with async tail fsync so the numbers show pub/sub cost
//! rather than disk latency.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;

use taibai::common::config::ServerConfig;
use taibai::net::client::{Client, SubEvent};
use taibai::net::server::Server;
use taibai::sql::execution::Engine;

struct TestServer {
    addr: String,
    _temp_dir: TempDir,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

async fn start_server(pubsub_enabled: bool) -> TestServer {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    let temp_dir = TempDir::new().expect("temp dir");
    let mut config = ServerConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.port = port;
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
    config.pubsub.enabled = pubsub_enabled;

    let engine = Arc::new(
        Engine::new_with_storage_config_and_auto_compact(
            std::path::Path::new(&config.storage.data_dir),
            "main",
            taibai::storage::segment_manager::SegmentConfig::default(),
            taibai::sql::execution::AutoCompactConfig::default(),
            256 * 1024 * 1024,
            true,
            config.pubsub.clone(),
            false,
        )
        .expect("engine"),
    );
    let server = Server::new(Arc::new(config), engine);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        server
            .run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("server run");
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    TestServer {
        addr: format!("127.0.0.1:{port}"),
        _temp_dir: temp_dir,
        shutdown_tx,
    }
}

async fn connect(addr: &str) -> Client {
    let mut client = Client::connect(addr).await.expect("connect");
    client.login("root", "taibai", "main").await.expect("login");
    client
}

/// Sequential autocommit INSERTs with `rows_per_stmt` rows each.
/// Returns (statements/sec, µs/stmt, rows/sec).
async fn measure_inserts(
    client: &mut Client,
    table: &str,
    statements: usize,
    rows_per_stmt: usize,
) -> (f64, f64, f64) {
    // Warm up code paths and the table's segment state.
    client
        .query(&format!(
            "INSERT INTO {table} VALUES {}",
            (0..rows_per_stmt)
                .map(|i| format!("({i}, 1.5)"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .await
        .expect("warmup insert");
    let start = Instant::now();
    for s in 0..statements {
        let values = (0..rows_per_stmt)
            .map(|r| {
                let v = (1_000 + s * rows_per_stmt + r) as i64;
                format!("({v}, 1.5)")
            })
            .collect::<Vec<_>>()
            .join(", ");
        client
            .query(&format!("INSERT INTO {table} VALUES {values}"))
            .await
            .expect("insert");
    }
    let elapsed = start.elapsed();
    let per_stmt_us = elapsed.as_micros() as f64 / statements as f64;
    (
        statements as f64 / elapsed.as_secs_f64(),
        per_stmt_us,
        (statements * rows_per_stmt) as f64 / elapsed.as_secs_f64(),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn pubsub_overhead_diagnostic() {
    // Single-row statements expose per-commit overhead including the round
    // trip; 100-row batches amortize the round trip and expose the per-row
    // capture/encode cost (batch clone, retention-log append, fan-out).
    for (rows_per_stmt, statements, label) in [(1usize, 800usize, "1-row"), (100, 200, "100-row")] {
        println!("=== workload: {label} statements, {rows_per_stmt} rows each ===");

        // ---- Scenario 1: pubsub master switch off.
        let server = start_server(false).await;
        let mut writer = connect(&server.addr).await;
        writer
            .query("CREATE TABLE bench_off (id INT32, v FLOAT64)")
            .await
            .expect("create");
        let (rate_off, us_off, rows_off) =
            measure_inserts(&mut writer, "bench_off", statements, rows_per_stmt).await;
        let _ = server.shutdown_tx.send(());
        println!(
            "{:>18}  {:>8.0} stmt/s  {:>9.0} rows/s  {:>7.1} us/stmt",
            "pubsub_disabled", rate_off, rows_off, us_off
        );

        // ---- Scenario 2: enabled, no subscribers.
        let server = start_server(true).await;
        let mut writer = connect(&server.addr).await;
        writer
            .query("CREATE TABLE bench_idle (id INT32, v FLOAT64)")
            .await
            .expect("create");
        let (rate_idle, us_idle, _) =
            measure_inserts(&mut writer, "bench_idle", statements, rows_per_stmt).await;
        let _ = server.shutdown_tx.send(());
        println!(
            "{:>18}  {:>8.0} stmt/s  {:>9.0} rows/s  {:>7.1} us/stmt  ({:+.1}% vs disabled)",
            "no_subscribers",
            rate_idle,
            rate_idle * rows_per_stmt as f64,
            us_idle,
            (us_idle / us_off - 1.0) * 100.0
        );

        // ---- Scenario 3: reliable channel active, nobody attached.
        let server = start_server(true).await;
        let mut writer = connect(&server.addr).await;
        writer
            .query("CREATE TABLE bench_capture (id INT32, v FLOAT64)")
            .await
            .expect("create");
        {
            let mut sub = connect(&server.addr).await;
            let grant = sub
                .subscribe_v2("main", "bench_capture", None, None, None)
                .await
                .expect("subscribe");
            sub.unsubscribe_v2("main", "bench_capture", grant.sub_id)
                .await
                .expect("unsubscribe");
            // The channel stays active (not retired until the idle timeout),
            // so the write path keeps mirroring commits (batch clone +
            // retention-log append) with nobody draining.
        }
        let (rate_capture, us_capture, _) =
            measure_inserts(&mut writer, "bench_capture", statements, rows_per_stmt).await;
        let _ = server.shutdown_tx.send(());
        println!(
            "{:>18}  {:>8.0} stmt/s  {:>9.0} rows/s  {:>7.1} us/stmt  ({:+.1}% vs disabled)",
            "v2_capture_only",
            rate_capture,
            rate_capture * rows_per_stmt as f64,
            us_capture,
            (us_capture / us_off - 1.0) * 100.0
        );

        // ---- Scenario 4: notification subscriber draining.
        let server = start_server(true).await;
        let mut writer = connect(&server.addr).await;
        writer
            .query("CREATE TABLE bench_hub (id INT32, v FLOAT64)")
            .await
            .expect("create");
        let mut hub_sub = connect(&server.addr).await;
        hub_sub.subscribe("main.bench_hub").await.expect("subscribe");
        let stop = Arc::new(AtomicBool::new(false));
        let received = Arc::new(AtomicU64::new(0));
        {
            let stop = Arc::clone(&stop);
            let received = Arc::clone(&received);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    match hub_sub
                        .recv_notification_timeout(Duration::from_millis(50))
                        .await
                    {
                        Ok(Some(_)) => {
                            received.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(None) => {}
                        Err(_) => return,
                    }
                }
            });
        }
        let (rate_hub, us_hub, _) =
            measure_inserts(&mut writer, "bench_hub", statements, rows_per_stmt).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.store(true, Ordering::Relaxed);
        let hub_received = received.load(Ordering::Relaxed);
        let _ = server.shutdown_tx.send(());
        println!(
            "{:>18}  {:>8.0} stmt/s  {:>9.0} rows/s  {:>7.1} us/stmt  ({:+.1}% vs disabled)  [{} notifications]",
            "hub_draining",
            rate_hub,
            rate_hub * rows_per_stmt as f64,
            us_hub,
            (us_hub / us_off - 1.0) * 100.0,
            hub_received
        );
        // +1 for the warmup insert's notification.
        assert_eq!(
            hub_received,
            statements as u64 + 1,
            "hub subscriber must receive every notification"
        );

        // ---- Scenario 5: reliable subscriber draining + acking.
        let server = start_server(true).await;
        let mut writer = connect(&server.addr).await;
        writer
            .query("CREATE TABLE bench_v2 (id INT32, v FLOAT64)")
            .await
            .expect("create");
        let mut v2_sub = connect(&server.addr).await;
        let grant = v2_sub
            .subscribe_v2("main", "bench_v2", None, None, None)
            .await
            .expect("subscribe");
        let stop = Arc::new(AtomicBool::new(false));
        let received = Arc::new(AtomicU64::new(0));
        {
            let stop = Arc::clone(&stop);
            let received = Arc::clone(&received);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    match v2_sub.recv_sub_event_timeout(Duration::from_millis(50)).await {
                        Ok(Some(SubEvent::Data(frame))) => {
                            received.fetch_add(1, Ordering::Relaxed);
                            if v2_sub
                                .ack_subscription("main", "bench_v2", grant.sub_id, frame.seq_end)
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(Some(SubEvent::ResyncRequired { .. })) | Ok(None) => {}
                        Err(_) => return,
                    }
                }
            });
        }
        let (rate_v2, us_v2, _) =
            measure_inserts(&mut writer, "bench_v2", statements, rows_per_stmt).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        stop.store(true, Ordering::Relaxed);
        let v2_received = received.load(Ordering::Relaxed);
        let _ = server.shutdown_tx.send(());
        println!(
            "{:>18}  {:>8.0} stmt/s  {:>9.0} rows/s  {:>7.1} us/stmt  ({:+.1}% vs disabled)  [{} data frames]",
            "v2_draining",
            rate_v2,
            rate_v2 * rows_per_stmt as f64,
            us_v2,
            (us_v2 / us_off - 1.0) * 100.0,
            v2_received
        );
        assert!(v2_received >= 1, "v2 subscriber must receive data frames");
        println!();
    }

    println!("workload: sequential autocommit INSERTs, async tail fsync. Profile matters:");
    println!("run with --release for production-shaped numbers.");
}
