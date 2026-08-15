use super::*;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::io::DuplexStream;
use tokio::task::JoinHandle;

use crate::net::session::Session;
use crate::protocol::message::{
    write_len_prefixed_string, Message, MAX_MESSAGE_SIZE, MSG_TYPE_AUTH_OK, MSG_TYPE_LOGIN,
    MSG_TYPE_PING, MSG_TYPE_PONG, MSG_TYPE_QUERY,
};

fn test_engine() -> (TempDir, Arc<Engine>) {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("engine"));
    (temp_dir, engine)
}

fn spawn_test_connection(
    engine: Arc<Engine>,
) -> (Framed<DuplexStream, MessageCodec>, JoinHandle<Result<()>>) {
    spawn_test_connection_with_limiter(engine, Arc::new(Semaphore::new(4)))
}

fn spawn_test_connection_with_limiter(
    engine: Arc<Engine>,
    request_limiter: Arc<Semaphore>,
) -> (Framed<DuplexStream, MessageCodec>, JoinHandle<Result<()>>) {
    let (server_stream, client_stream) = tokio::io::duplex(1024 * 1024);
    let config = Arc::new(ServerConfig {
        compression: crate::common::config::CompressionSettings {
            enabled: false,
            bulk_zstd_level: 1,
        },
        ..ServerConfig::default()
    });
    let server = tokio::spawn(handle_framed(
        server_stream,
        config,
        engine,
        request_limiter,
    ));
    let client = Framed::new(client_stream, MessageCodec::new(MAX_MESSAGE_SIZE));
    (client, server)
}

fn query(sql: &str) -> Message {
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "main");
    write_len_prefixed_string(&mut payload, sql);
    Message::new(MSG_TYPE_QUERY, Bytes::from(payload))
}

async fn login(framed: &mut Framed<DuplexStream, MessageCodec>, user: &str) {
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, user);
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    framed
        .send(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .await
        .expect("send login");
    let resp = framed
        .next()
        .await
        .expect("login response")
        .expect("login frame");
    assert_eq!(resp.msg_type, MSG_TYPE_AUTH_OK);
}

#[test]
fn lightweight_requests_do_not_use_the_blocking_pool() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv().unwrap();

        let (_dir, engine) = test_engine();
        let (mut client, server) = spawn_test_connection(engine);
        timeout(Duration::from_secs(2), login(&mut client, "root"))
            .await
            .expect("login should not use the saturated blocking pool");
        client.send(Message::empty(MSG_TYPE_PING)).await.unwrap();
        let pong = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("ping should not use the saturated blocking pool")
            .unwrap()
            .unwrap();
        assert_eq!(pong.msg_type, MSG_TYPE_PONG);

        drop(client);
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .expect("connection cleanup should complete after blocking capacity returns")
            .unwrap()
            .unwrap();
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lightweight_requests_remain_responsive_when_heavy_requests_are_queued() {
    let (_dir, engine) = test_engine();
    let limiter = Arc::new(Semaphore::new(1));
    let held_permit = Arc::clone(&limiter).acquire_owned().await.unwrap();

    let (mut blocked_client, blocked_server) =
        spawn_test_connection_with_limiter(Arc::clone(&engine), Arc::clone(&limiter));
    login(&mut blocked_client, "root").await;
    blocked_client.send(query("SELECT 1")).await.unwrap();

    let (mut responsive_client, responsive_server) =
        spawn_test_connection_with_limiter(engine, limiter);
    timeout(
        Duration::from_secs(2),
        login(&mut responsive_client, "root"),
    )
    .await
    .expect("login should not wait for the heavy request limiter");
    responsive_client
        .send(Message::empty(MSG_TYPE_PING))
        .await
        .unwrap();
    let pong = timeout(Duration::from_secs(2), responsive_client.next())
        .await
        .expect("ping should remain responsive")
        .unwrap()
        .unwrap();
    assert_eq!(pong.msg_type, MSG_TYPE_PONG);

    drop(responsive_client);
    timeout(Duration::from_secs(2), responsive_server)
        .await
        .expect("read-only connection cleanup should not wait for heavy requests")
        .unwrap()
        .unwrap();

    drop(held_permit);
    timeout(Duration::from_secs(2), blocked_client.next())
        .await
        .expect("queued query should resume")
        .unwrap()
        .unwrap();
    drop(blocked_client);
    let _ = blocked_server.await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_while_waiting_for_request_capacity_closes_connection() {
    let (_dir, engine) = test_engine();
    let limiter = Arc::new(Semaphore::new(1));
    let _held_permit = Arc::clone(&limiter).acquire_owned().await.unwrap();
    let (mut client, server) = spawn_test_connection_with_limiter(engine, limiter);
    login(&mut client, "root").await;
    client.send(query("SELECT 1")).await.unwrap();
    drop(client);

    timeout(Duration::from_secs(2), server)
        .await
        .expect("disconnected connection must not wait for request capacity")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_errors_still_release_bulk_import_session_state() {
    let (_dir, engine) = test_engine();
    let (mut client, server) = spawn_test_connection(Arc::clone(&engine));
    login(&mut client, "root").await;

    client
        .send(query("START BULK IMPORT {\"disable_wal\": true}"))
        .await
        .unwrap();
    client.next().await.unwrap().unwrap();
    assert_eq!(engine.bulk_mode_session_count(), 1);

    client
        .send(Message::new(MSG_TYPE_LOGIN, Bytes::from_static(&[0])))
        .await
        .unwrap();
    let response = timeout(Duration::from_secs(2), client.next())
        .await
        .expect("malformed protocol request should receive an error response")
        .expect("server should respond before closing the connection")
        .expect("error response should be decodable");
    assert_eq!(response.msg_type, crate::protocol::message::MSG_TYPE_ERROR);
    assert!(String::from_utf8_lossy(&response.payload).contains("protocol error"));
    let connection_result = timeout(Duration::from_secs(2), server)
        .await
        .expect("connection should terminate after malformed protocol request")
        .unwrap();
    assert!(connection_result.is_err());
    assert_eq!(engine.bulk_mode_session_count(), 0);
}
