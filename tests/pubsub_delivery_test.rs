//! Delivery-semantics regression test: a change subscriber receives every
//! committed row exactly once, even across row-group flushes, and
//! large (compressible) notifications decode correctly.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use taibai::common::config::ServerConfig;
use taibai::net::client::Client;
use taibai::net::server::Server;
use taibai::sql::execution::Engine;

#[tokio::test(flavor = "multi_thread")]
async fn hub_subscriber_delivers_rows_exactly_once() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let temp_dir = TempDir::new().unwrap();
    let mut config = ServerConfig::default();
    config.server.host = "127.0.0.1".into();
    config.server.port = port;
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();
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
        .unwrap(),
    );
    let server = Server::new(Arc::new(config), engine);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        server
            .run_with_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let addr = format!("127.0.0.1:{port}");

    let mut writer = Client::connect(&addr).await.unwrap();
    writer.login("root", "taibai", "main").await.unwrap();
    let mut sub = Client::connect(&addr).await.unwrap();
    sub.login("root", "taibai", "main").await.unwrap();

    writer
        .query("CREATE TABLE r (id INT32, v FLOAT64)")
        .await
        .unwrap();
    sub.subscribe("main.r").await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));
    let delivered_rows = Arc::new(AtomicU64::new(0));
    {
        let stop = Arc::clone(&stop);
        let count = Arc::clone(&count);
        let delivered_rows = Arc::clone(&delivered_rows);
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                match sub
                    .recv_notification_timeout(Duration::from_millis(100))
                    .await
                {
                    Ok(Some(n)) => {
                        count.fetch_add(1, Ordering::Relaxed);
                        delivered_rows.fetch_add(n.num_rows() as u64, Ordering::Relaxed);
                        assert_eq!(n.schema.columns.len(), 2);
                    }
                    Ok(None) => {}
                    Err(e) => panic!("recv failed after {}: {e}", count.load(Ordering::Relaxed)),
                }
            }
        });
    }

    for s in 0..200usize {
        let values = (0..100usize)
            .map(|r| {
                let v = (s * 100 + r) as i64;
                format!("({v}, 1.5)")
            })
            .collect::<Vec<_>>()
            .join(", ");
        writer
            .query(&format!("INSERT INTO r VALUES {values}"))
            .await
            .unwrap_or_else(|e| panic!("insert {s} failed: {e}"));
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    let notifications = count.load(Ordering::Relaxed);
    let delivered = delivered_rows.load(Ordering::Relaxed);
    // 200 statements x 100 rows, exactly once each: the row-group flush that
    // happens along the way must not re-publish already-notified rows.
    assert_eq!(notifications, 200, "one notification per statement");
    assert_eq!(delivered, 20_000, "every row delivered exactly once");
    println!("validated {notifications} notifications, {delivered} rows");
    let _ = shutdown_tx.send(());
}
