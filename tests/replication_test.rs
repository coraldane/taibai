//! End-to-end replication test: a primary server streams its physical
//! operation events to an in-process standby engine, which converges to the
//! same data and survives restarts.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use taibai::common::config::{ReplicationSettings, ServerConfig};
use taibai::net::client::Client;
use taibai::net::server::Server;
use taibai::sql::execution::engine::ExecResult;
use taibai::sql::execution::Engine;
use taibai::storage::replication::standby::{self, StandbyConfig};

struct TestServer {
    addr: String,
    _temp_dir: TempDir,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

async fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn start_primary(data_dir: &std::path::Path) -> (TestServer, Arc<Engine>) {
    let port = free_port().await;
    let mut config = ServerConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.port = port;
    config.storage.data_dir = data_dir.to_string_lossy().to_string();
    let engine = Arc::new(Engine::new(std::path::Path::new(data_dir), "main").expect("engine"));
    assert!(
        engine.rep_log().is_some(),
        "primary role must create the replication log"
    );
    let server = Server::new(Arc::new(config), Arc::clone(&engine));
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
    (
        TestServer {
            addr: format!("127.0.0.1:{port}"),
            _temp_dir: TempDir::new().expect("primary temp anchor"),
            shutdown_tx,
        },
        engine,
    )
}

async fn client(addr: &str) -> Client {
    let mut client = Client::connect(addr).await.expect("connect");
    client.login("root", "taibai", "main").await.expect("login");
    client
}

async fn execute_ok(client: &mut Client, sql: &str) {
    if let Err(error) = client.execute(sql).await {
        panic!("'{sql}' failed: {error}");
    }
}

/// Poll the standby until `predicate` holds, so the async stream does not
/// need fixed sleeps.
async fn wait_for(
    engine: &Engine,
    what: &str,
    timeout: Duration,
    predicate: impl Fn(&Engine) -> bool,
) {
    let deadline = std::time::Instant::now() + timeout;
    let mut polls = 0usize;
    while std::time::Instant::now() < deadline {
        if predicate(engine) {
            println!("WAIT-FOR '{what}': converged after {polls} polls");
            return;
        }
        polls += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "WAIT-FOR '{what}': TIMED OUT after {polls} polls, count={}",
        count_rows(engine, "SELECT COUNT(v) FROM t")
    );
    let mut session = taibai::net::session::Session::new("root".into(), "main".into());
    if let Ok(ExecResult::Query { batches, .. }) =
        engine.execute(&mut session, "SELECT id, v FROM t")
    {
        let mut rows = Vec::new();
        for b in &batches {
            for r in 0..b.num_rows() {
                rows.push(format!("{:?}/{:?}", b.scalar(r, 0), b.scalar(r, 1)));
            }
        }
        println!("WAIT-FOR dump rows({}): {rows:?}", rows.len());
        let tail_root = engine
            .data_dir()
            .join("main")
            .join("t")
            .join("memtable_tail");
        for e in std::fs::read_dir(&tail_root)
            .into_iter()
            .flatten()
            .flatten()
        {
            println!(
                "TAIL-FILE {} ({} bytes)",
                e.file_name().to_string_lossy(),
                e.metadata().map(|m| m.len()).unwrap_or(0)
            );
        }
    }
    panic!("standby did not converge within {timeout:?}: {what}");
}

fn count_rows(engine: &Engine, sql: &str) -> i64 {
    let mut session = taibai::net::session::Session::new("root".into(), "main".into());
    match engine.execute(&mut session, sql) {
        Ok(result) => count_from(result),
        Err(_) => i64::MIN, // table not present yet
    }
}

fn count_from(result: ExecResult) -> i64 {
    match result {
        ExecResult::Query { batches, .. } => {
            let mut total = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    match batch.scalar(row, 0) {
                        Some(taibai::types::scalar::ScalarValue::Int64(Some(v))) => total += v,
                        Some(taibai::types::scalar::ScalarValue::Int32(Some(v))) => {
                            total += v as i64
                        }
                        Some(taibai::types::scalar::ScalarValue::Int64(None)) => {}
                        other => panic!("unexpected count scalar {other:?}"),
                    }
                }
            }
            total
        }
        other => panic!("expected query, got {other:?}"),
    }
}

/// One shared role install per test process (the config is process-global).
static ROLE: std::sync::Once = std::sync::Once::new();

fn install_primary_role() {
    ROLE.call_once(|| {
        taibai::common::config::set_replication_settings(ReplicationSettings {
            role: "primary".to_string(),
            ..ReplicationSettings::default()
        });
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn standby_converges_and_survives_restart() {
    install_primary_role();
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();
    let primary_dir = TempDir::new().expect("primary dir");
    let (primary, primary_engine) = start_primary(primary_dir.path()).await;
    let standby_dir = TempDir::new().expect("standby dir");

    // Primary writes: inserts (tail events) + a flush (segment events).
    {
        let mut client = client(&primary.addr).await;
        execute_ok(&mut client, "CREATE TABLE t (id INT32, v INT64)").await;
        for chunk in 0..5i32 {
            let values = (0..100)
                .map(|i| {
                    let id = chunk * 100 + i;
                    format!("({id}, {id})")
                })
                .collect::<Vec<_>>()
                .join(", ");
            execute_ok(&mut client, &format!("INSERT INTO t VALUES {values}")).await;
        }
        execute_ok(&mut client, "FLUSH").await;
    }

    // Standby engine + applier. R1 does not ship DDL events: the standby is
    // "bootstrapped" with the schema, exactly like starting from a backup
    // taken after the CREATE TABLE (docs/operations/replication.md).
    let standby_engine =
        Arc::new(Engine::new(std::path::Path::new(standby_dir.path()), "main").expect("standby"));
    {
        let mut bootstrap = taibai::net::session::Session::new("root".into(), "main".into());
        standby_engine
            .execute(&mut bootstrap, "CREATE TABLE t (id INT32, v INT64)")
            .expect("bootstrap create");
    }
    standby_engine.set_read_only();
    let first_applier = standby::spawn(
        Arc::clone(&standby_engine),
        StandbyConfig {
            primary_addr: primary.addr.clone(),
            user: "root".to_string(),
            password: "taibai".to_string(),
            database: "main".to_string(),
        },
    );

    wait_for(
        &standby_engine,
        "500 replicated rows",
        Duration::from_secs(30),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t") == 500,
    )
    .await;
    let expect_sum: i64 = (0..500i32).map(|i| i as i64).sum();
    let probe = {
        let mut session = taibai::net::session::Session::new("root".into(), "main".into());
        match standby_engine.execute(&mut session, "SELECT id, v FROM t ORDER BY id LIMIT 3") {
            Ok(ExecResult::Query { batches, .. }) => {
                let mut rows = Vec::new();
                for batch in batches {
                    for r in 0..batch.num_rows() {
                        rows.push(format!("{:?} {:?}", batch.scalar(r, 0), batch.scalar(r, 1)));
                    }
                }
                rows
            }
            other => panic!("probe failed: {other:?}"),
        }
    };
    println!("PROBE rows: {probe:?}");
    {
        let table_root = standby_engine.data_dir().join("main").join("t");
        if let Ok(entries) = std::fs::read_dir(&table_root) {
            for e in entries.flatten() {
                println!(
                    "PROBE table file {} ({} bytes)",
                    e.file_name().to_string_lossy(),
                    e.metadata().map(|m| m.len()).unwrap_or(0)
                );
            }
        }
        let tail_root = standby_engine
            .data_dir()
            .join("main")
            .join("t")
            .join("memtable_tail");
        let mut listing = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&tail_root) {
            for e in entries.flatten() {
                let meta = e.metadata().ok();
                listing.push(format!(
                    "{} ({} bytes)",
                    e.file_name().to_string_lossy(),
                    meta.map(|m| m.len()).unwrap_or(0)
                ));
            }
        }
        println!("PROBE tail dir {tail_root:?}: {listing:?}");
        let mut s2 = taibai::net::session::Session::new("root".into(), "main".into());
        for sql in [
            "SELECT COUNT(*) FROM t",
            "SELECT COUNT(v) FROM t",
            "SELECT id FROM t LIMIT 3",
        ] {
            match standby_engine.execute(&mut s2, sql) {
                Ok(ExecResult::Query { batches, .. }) => {
                    let mut out = Vec::new();
                    for b in batches {
                        for r in 0..b.num_rows() {
                            out.push(format!("{:?}", b.scalar(r, 0)));
                        }
                    }
                    println!("PROBE '{sql}' -> {out:?}");
                }
                other => println!("PROBE '{sql}' -> {other:?}"),
            }
        }
    }
    assert_eq!(
        count_rows(&standby_engine, "SELECT SUM(v) FROM t"),
        expect_sum
    );

    // More writes converge without a flush.
    {
        let mut client = client(&primary.addr).await;
        execute_ok(&mut client, "INSERT INTO t VALUES (999, 999)").await;
    }
    wait_for(
        &standby_engine,
        "post-insert row",
        Duration::from_secs(20),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t") == 501,
    )
    .await;

    // Standby rejects writes.
    let mut standby_session = taibai::net::session::Session::new("root".into(), "main".into());
    let error = standby_engine
        .execute(&mut standby_session, "INSERT INTO t VALUES (1, 1)")
        .expect_err("standby must reject writes");
    assert!(
        error.to_string().contains("read-only"),
        "unexpected error: {error}"
    );

    // Primary rejects bulk-mode inserts (the direct-write WAL bypass).
    let mut bulk_session = taibai::net::session::Session::new("root".into(), "main".into());
    bulk_session.bulk_import_mode = true;
    let error = primary_engine
        .execute(&mut bulk_session, "INSERT INTO t VALUES (1, 1)")
        .expect_err("bulk insert must be rejected on a primary");
    assert!(
        error.to_string().contains("replication primary"),
        "unexpected error: {error}"
    );

    // Standby restart: stop the first applier (releases the engine), then
    // reopen and resume the stream.
    first_applier.stop();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(standby_engine);
    let reopened =
        Arc::new(Engine::new(std::path::Path::new(standby_dir.path()), "main").expect("reopen"));
    reopened.set_read_only();
    wait_for(
        &reopened,
        "rows after restart (tail recovery)",
        Duration::from_secs(10),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t") == 501,
    )
    .await;
    standby::spawn(
        Arc::clone(&reopened),
        StandbyConfig {
            primary_addr: primary.addr.clone(),
            user: "root".to_string(),
            password: "taibai".to_string(),
            database: "main".to_string(),
        },
    );
    {
        let mut client = client(&primary.addr).await;
        execute_ok(&mut client, "INSERT INTO t VALUES (1000, 1000)").await;
    }
    // R2 phases continue against the resumed standby engine.
    let standby_engine = Arc::clone(&reopened);
    wait_for(
        &reopened,
        "row after standby restart",
        Duration::from_secs(20),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t") == 502,
    )
    .await;

    // ---- R2: DELETE / UPDATE / compaction / DDL replication ----
    // Recreate the standby from the primary's state snapshot: simplest R2
    // bootstrap here is a fresh standby dir replaying from seq 0 is NOT
    // possible (the ring may have evicted); instead keep the SAME standby
    // that already converged and continue the stream.
    {
        let mut client = client(&primary.addr).await;
        // UPDATE through segment rewrite (replaces a segment on the primary).
        execute_ok(&mut client, "UPDATE t SET v = 5000 WHERE id = 499").await;
        // DELETE with flushed rows (tombstone rewrite).
        execute_ok(&mut client, "DELETE FROM t WHERE id <= 2").await;
        // Compaction (segment replacement event).
        execute_ok(&mut client, "OPTIMIZE TABLE t").await;
        // DDL replication: a NEW table becomes queryable on the standby.
        execute_ok(&mut client, "CREATE TABLE r2 (id INT32, v INT64)").await;
        execute_ok(&mut client, "INSERT INTO r2 VALUES (7, 77)").await;
    }
    // wait for DELETE effect: rows 0,1,2 gone (was 502 rows: 0..500 + 999 + 1000)
    wait_for(
        &standby_engine,
        "DELETE replicated",
        Duration::from_secs(20),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t") == 499,
    )
    .await;
    // UPDATE effect visible.
    wait_for(
        &standby_engine,
        "UPDATE replicated",
        Duration::from_secs(20),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM t WHERE id = 499 AND v = 5000") == 1,
    )
    .await;
    {
        let mut session = taibai::net::session::Session::new("root".into(), "main".into());
        match standby_engine.execute(&mut session, "SELECT v FROM t WHERE id = 499") {
            Ok(ExecResult::Query { batches, .. }) => {
                let mut values = Vec::new();
                for b in &batches {
                    for r in 0..b.num_rows() {
                        values.push(b.scalar(r, 0));
                    }
                }
                assert!(
                    values.contains(&Some(taibai::types::scalar::ScalarValue::Int64(Some(5000)))),
                    "updated value must be visible on standby: {values:?}"
                );
            }
            other => panic!("probe failed: {other:?}"),
        }
    }
    // DDL + new-table rows replicated.
    wait_for(
        &standby_engine,
        "DDL + rows replicated",
        Duration::from_secs(20),
        |engine| count_rows(engine, "SELECT COUNT(v) FROM r2") == 1,
    )
    .await;

    let _ = primary.shutdown_tx.send(());
}
