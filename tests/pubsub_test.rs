//! End-to-end tests for the pubsub (change subscription) feature: a real
//! server instance, real client connections, and notifications delivered
//! asynchronously while the request/response flows keep working.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use taibai::common::config::ServerConfig;
use taibai::net::client::{Client, QueryResult};
use taibai::net::server::Server;
use taibai::sql::execution::Engine;

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

async fn start_server() -> TestServer {
    let port = free_port().await;
    let temp_dir = TempDir::new().expect("temp dir");
    let mut config = ServerConfig::default();
    config.server.host = "127.0.0.1".to_string();
    config.server.port = port;
    config.storage.data_dir = temp_dir.path().to_string_lossy().to_string();

    let engine = Arc::new(
        Engine::new(std::path::Path::new(&config.storage.data_dir), "main").expect("engine"),
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
    // Give the accept loop a moment to bind.
    tokio::time::sleep(Duration::from_millis(150)).await;
    TestServer {
        addr: format!("127.0.0.1:{port}"),
        _temp_dir: temp_dir,
        shutdown_tx,
    }
}

async fn connect_and_login(addr: &str) -> Client {
    let mut client = Client::connect(addr).await.expect("connect");
    client.login("root", "taibai", "main").await.expect("login");
    client
}

fn rows_affected(result: &QueryResult) -> u64 {
    match result {
        QueryResult::Ok { rows_affected, .. } => *rows_affected,
        other => panic!("expected Ok result, got {other:?}"),
    }
}

async fn exec(client: &mut Client, sql: &str) -> u64 {
    let result = client.query(sql).await.expect("query");
    rows_affected(&result)
}

#[tokio::test(flavor = "current_thread")]
async fn subscriber_receives_insert_notifications() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(
        &mut writer,
        "CREATE TABLE ticks (symbol VARCHAR, price FLOAT64)",
    )
    .await;

    let sub_id = subscriber.subscribe("main.ticks").await.expect("subscribe");
    assert_eq!(sub_id, 1);

    // Two inserts produce two notifications in order with a per-topic seq.
    exec(
        &mut writer,
        "INSERT INTO ticks VALUES ('AAPL', 191.2), ('MSFT', 420.1)",
    )
    .await;
    exec(&mut writer, "INSERT INTO ticks VALUES ('AAPL', 191.5)").await;

    let first = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("first notification");
    assert_eq!(first.sub_id, sub_id);
    assert_eq!(first.seq, 1);
    assert_eq!(first.topic, "main.ticks");
    assert_eq!(first.num_rows(), 2);
    assert_eq!(first.schema.columns[0].name, "symbol");

    let second = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("second notification");
    assert_eq!(second.seq, 2);
    assert_eq!(second.num_rows(), 1);

    // The request/response flow still works on the subscriber connection
    // even though notifications may interleave.
    let count = subscriber
        .query("SELECT COUNT(*) FROM ticks")
        .await
        .expect("count query");
    match count {
        QueryResult::Rows { batches, .. } => {
            let value = batches[0].scalar(0, 0).expect("scalar");
            match value {
                taibai::types::scalar::ScalarValue::Int64(Some(3)) => {}
                other => panic!("expected count 3, got {other:?}"),
            }
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // After unsubscribing no more notifications arrive.
    subscriber.unsubscribe(sub_id).await.expect("unsubscribe");
    exec(&mut writer, "INSERT INTO ticks VALUES ('TSLA', 250.0)").await;
    let none = subscriber
        .recv_notification_timeout(Duration::from_millis(300))
        .await
        .expect("no error");
    assert!(none.is_none(), "no notification expected after unsubscribe");

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn transactional_writes_publish_at_commit() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(&mut writer, "CREATE TABLE txn_t (id INT32, v INT32)").await;
    subscriber.subscribe("main.txn_t").await.expect("subscribe");

    exec(&mut writer, "BEGIN").await;
    exec(&mut writer, "INSERT INTO txn_t VALUES (1, 10), (2, 20)").await;
    exec(&mut writer, "COMMIT").await;

    let notification = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("commit notification");
    assert_eq!(notification.num_rows(), 2);

    // The next event continues the sequence.
    exec(&mut writer, "INSERT INTO txn_t VALUES (3, 30)").await;
    let notification = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("autocommit notification");
    assert_eq!(notification.seq, 2);
    assert_eq!(notification.num_rows(), 1);

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn unqualified_topic_resolves_to_session_database() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(&mut writer, "CREATE TABLE bare_t (id INT32)").await;

    // A bare table name subscribes in the session database.
    subscriber.subscribe("bare_t").await.expect("subscribe");

    exec(&mut writer, "INSERT INTO bare_t VALUES (1)").await;
    let notification = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("notification");
    assert_eq!(notification.topic, "main.bare_t");
    assert_eq!(notification.num_rows(), 1);

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn wildcard_subscription_and_permission_denial() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(&mut writer, "CREATE TABLE w1 (id INT32)").await;
    exec(&mut writer, "CREATE TABLE w2 (id INT32)").await;
    exec(&mut writer, "CREATE USER alice IDENTIFIED BY 'pw'").await;
    exec(&mut writer, "GRANT SELECT ON TABLE main.w1 TO alice").await;

    // Wildcard: both tables deliver on one subscription.
    subscriber.subscribe("main.*").await.expect("subscribe");
    exec(&mut writer, "INSERT INTO w1 VALUES (1)").await;
    exec(&mut writer, "INSERT INTO w2 VALUES (2)").await;
    let first = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("w1 notification");
    assert_eq!(first.topic, "main.w1");
    let second = subscriber
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("w2 notification");
    assert_eq!(second.topic, "main.w2");

    // Unknown table is rejected up front.
    let err = subscriber.subscribe("main.nope").await.expect_err("denied");
    assert!(err.to_string().contains("does not exist"), "{err}");

    // A user without SUBSCRIBE rights is denied; root grants then allow.
    let mut alice = Client::connect(&server.addr).await.expect("alice connect");
    alice
        .login("alice", "pw", "main")
        .await
        .expect("alice login");
    let err = alice.subscribe("main.w1").await.expect_err("denied");
    assert!(
        err.to_string().to_uppercase().contains("ACCESS DENIED"),
        "{err}"
    );

    exec(&mut writer, "GRANT SUBSCRIBE ON TABLE main.w1 TO alice").await;
    alice
        .subscribe("main.w1")
        .await
        .expect("allowed after grant");
    exec(&mut writer, "INSERT INTO w1 VALUES (7)").await;
    let granted = alice
        .recv_notification_timeout(Duration::from_secs(5))
        .await
        .expect("no error")
        .expect("alice notification");
    assert_eq!(granted.topic, "main.w1");

    // Subscribing to a whole database requires a database-level grant.
    let err = alice.subscribe("main.w2").await.expect_err("denied");
    assert!(
        err.to_string().to_uppercase().contains("ACCESS DENIED"),
        "{err}"
    );

    let _ = server.shutdown_tx.send(());
}
