//! End-to-end tests for the reliable pub/sub protocol (SUBSCRIBE_V2):
//! resumable sequences over a real server, acks, named bookmarks, column
//! filters, and permission enforcement.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use taibai::common::config::ServerConfig;
use taibai::net::client::{Client, QueryResult, SubEvent};
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

async fn exec(client: &mut Client, sql: &str) {
    client.query(sql).await.expect("query ok");
}

async fn next_data(client: &mut Client, what: &str) -> taibai::net::pubsub_session::SubDataFrame {
    for _ in 0..100 {
        match client
            .recv_sub_event_timeout(Duration::from_millis(100))
            .await
        {
            Ok(Some(SubEvent::Data(frame))) => return frame,
            Ok(Some(SubEvent::ResyncRequired { .. })) => panic!("unexpected resync for {what}"),
            Ok(None) => {}
            Err(err) => panic!("{what}: {err}"),
        }
    }
    panic!("timed out waiting for data event ({what})");
}

#[tokio::test(flavor = "current_thread")]
async fn reliable_subscription_delivers_sequences_and_acks() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(
        &mut writer,
        "CREATE TABLE ticks (symbol VARCHAR, price FLOAT64)",
    )
    .await;

    let grant = subscriber
        .subscribe_v2("main", "ticks", None, None, None)
        .await
        .expect("subscribe");
    assert_eq!(grant.granted_from, 0);

    exec(
        &mut writer,
        "INSERT INTO ticks VALUES ('AAPL', 191.2), ('MSFT', 420.1)",
    )
    .await;
    exec(&mut writer, "INSERT INTO ticks VALUES ('TSLA', 250.0)").await;

    let first = next_data(&mut subscriber, "first").await;
    assert_eq!(first.sub_id, grant.sub_id);
    assert_eq!((first.seq_start, first.seq_end), (1, 1));
    assert_eq!(first.table, "ticks");
    assert_eq!(first.batches.len(), 1);
    assert_eq!(first.batches[0].num_rows(), 2);

    let second = next_data(&mut subscriber, "second").await;
    assert_eq!((second.seq_start, second.seq_end), (2, 2));
    assert_eq!(second.batches[0].num_rows(), 1);

    // Acks are accepted and keep the request/response flow intact.
    subscriber
        .ack_subscription("main", "ticks", grant.sub_id, second.seq_end)
        .await
        .expect("ack");

    // The request path still works with events flowing.
    match subscriber
        .query("SELECT COUNT(*) FROM ticks")
        .await
        .expect("count")
    {
        QueryResult::Rows { batches, .. } => {
            assert_eq!(
                batches[0].scalar(0, 0).expect("scalar"),
                taibai::types::scalar::ScalarValue::Int64(Some(3))
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }

    subscriber
        .unsubscribe_v2("main", "ticks", grant.sub_id)
        .await
        .expect("unsubscribe");
    exec(&mut writer, "INSERT INTO ticks VALUES ('BYE', 1.0)").await;
    let quiet = subscriber
        .recv_sub_event_timeout(Duration::from_millis(300))
        .await
        .expect("no error");
    assert!(quiet.is_none(), "nothing should arrive after unsubscribe");

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn resubscribe_from_seq_replays_retained_window() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(&mut writer, "CREATE TABLE feed (id INT32)").await;
    let grant = subscriber
        .subscribe_v2("main", "feed", None, None, None)
        .await
        .expect("first subscribe");

    exec(&mut writer, "INSERT INTO feed VALUES (1)").await;
    let first = next_data(&mut subscriber, "seq1").await;
    assert_eq!((first.seq_start, first.seq_end), (1, 1));

    subscriber
        .ack_subscription("main", "ticks-feed-irrelevant-name", grant.sub_id, 1)
        .await
        .expect_err("ack for wrong table must fail");
    subscriber
        .ack_subscription("main", "feed", grant.sub_id, 1)
        .await
        .expect("ack seq 1");

    // Simulate a reconnect: drop the subscription, more writes happen while
    // nobody is attached, then resume from the last processed sequence.
    subscriber
        .unsubscribe_v2("main", "feed", grant.sub_id)
        .await
        .expect("unsubscribe");
    exec(&mut writer, "INSERT INTO feed VALUES (2), (3)").await;
    exec(&mut writer, "INSERT INTO feed VALUES (4)").await;

    // from_seq = 1: the server replays everything after sequence 1.
    let resumed = subscriber
        .subscribe_v2("main", "feed", Some(1), None, None)
        .await
        .expect("resubscribe");
    assert_eq!(resumed.granted_from, 1);
    // The delivery task coalesces every retained entry after seq 1 into as
    // few frames as possible: seq_start..=seq_end spans 2..=3 here.
    let replay = next_data(&mut subscriber, "replay").await;
    assert_eq!((replay.seq_start, replay.seq_end), (2, 3));
    assert_eq!(replay.batches.len(), 2);
    assert_eq!(replay.batches[0].num_rows(), 2);
    assert_eq!(replay.batches[1].num_rows(), 1);

    // A resume request beyond the current head is clamped to the head.
    let ahead = subscriber
        .subscribe_v2("main", "feed", Some(999), None, None)
        .await
        .expect("subscribe ahead of head");
    assert_eq!(
        ahead.granted_from, 3,
        "from_seq beyond the head must clamp to the head"
    );
    subscriber
        .unsubscribe_v2("main", "feed", ahead.sub_id)
        .await
        .expect("unsubscribe ahead");

    subscriber
        .unsubscribe_v2("main", "feed", resumed.sub_id)
        .await
        .expect("unsubscribe resumed");

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn named_bookmark_resumes_from_acked_sequence() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(&mut writer, "CREATE TABLE events (id INT32)").await;
    let grant = subscriber
        .subscribe_v2("main", "events", None, Some("worker-a"), None)
        .await
        .expect("named subscribe");

    exec(&mut writer, "INSERT INTO events VALUES (1)").await;
    let frame = next_data(&mut subscriber, "named").await;
    assert_eq!((frame.seq_start, frame.seq_end), (1, 1));
    subscriber
        .ack_subscription("main", "events", grant.sub_id, frame.seq_end)
        .await
        .expect("ack");

    subscriber
        .unsubscribe_v2("main", "events", grant.sub_id)
        .await
        .expect("unsubscribe");

    // The named bookmark was flushed on ack; a fresh subscribe with the same
    // name resumes after the acked sequence without replaying it.
    let resumed = subscriber
        .subscribe_v2("main", "events", None, Some("worker-a"), None)
        .await
        .expect("resume with bookmark");
    assert_eq!(
        resumed.granted_from, 1,
        "bookmark should skip the acked sequence"
    );

    exec(&mut writer, "INSERT INTO events VALUES (2)").await;
    let frame = next_data(&mut subscriber, "after resume").await;
    assert_eq!((frame.seq_start, frame.seq_end), (2, 2));

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn column_filter_restricts_delivered_rows() {
    let server = start_server().await;
    let mut subscriber = connect_and_login(&server.addr).await;
    let mut writer = connect_and_login(&server.addr).await;

    exec(
        &mut writer,
        "CREATE TABLE trades (symbol VARCHAR, qty INT32)",
    )
    .await;
    subscriber
        .subscribe_v2(
            "main",
            "trades",
            None,
            None,
            Some(("symbol", &["AAPL"][..])),
        )
        .await
        .expect("filtered subscribe");

    exec(
        &mut writer,
        "INSERT INTO trades VALUES ('MSFT', 10), ('AAPL', 20), ('AAPL', 30)",
    )
    .await;

    let frame = next_data(&mut subscriber, "filtered").await;
    assert_eq!(frame.batches.len(), 1);
    assert_eq!(
        frame.batches[0].num_rows(),
        2,
        "only AAPL rows should be delivered"
    );

    let _ = server.shutdown_tx.send(());
}

#[tokio::test(flavor = "current_thread")]
async fn permission_and_existence_errors_surface_as_sub_error() {
    let server = start_server().await;
    let mut admin = connect_and_login(&server.addr).await;

    exec(&mut admin, "CREATE TABLE guarded (id INT32)").await;
    exec(&mut admin, "CREATE USER bob IDENTIFIED BY 'pw'").await;
    exec(&mut admin, "GRANT SELECT ON TABLE main.guarded TO bob").await;

    let mut bob = Client::connect(&server.addr).await.expect("bob connect");
    bob.login("bob", "pw", "main").await.expect("bob login");

    // Missing SUBSCRIBE right.
    let err = bob
        .subscribe_v2("main", "guarded", None, None, None)
        .await
        .expect_err("denied");
    assert!(
        err.to_string().to_uppercase().contains("ACCESS DENIED"),
        "{err}"
    );

    exec(&mut admin, "GRANT SUBSCRIBE ON TABLE main.guarded TO bob").await;
    bob.subscribe_v2("main", "guarded", None, None, None)
        .await
        .expect("allowed after grant");

    // Unknown table and unknown filter column are rejected before any grant
    // matters.
    let err = admin
        .subscribe_v2("main", "missing_table", None, None, None)
        .await
        .expect_err("unknown table");
    assert!(err.to_string().contains("does not exist"), "{err}");

    let err = admin
        .subscribe_v2("main", "guarded", None, None, Some(("nope", &[][..])))
        .await
        .expect_err("unknown filter column");
    assert!(err.to_string().contains("filter column"), "{err}");

    let _ = server.shutdown_tx.send(());
}
