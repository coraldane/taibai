use super::*;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tempfile::TempDir;
use tokio::io::DuplexStream;
use tokio::task::JoinHandle;

use crate::protocol::message::{
    write_len_prefixed_string, Message, MAX_MESSAGE_SIZE, MSG_TYPE_AUTH_OK, MSG_TYPE_ERROR,
    MSG_TYPE_LOGIN, MSG_TYPE_OK, MSG_TYPE_PING, MSG_TYPE_PONG, MSG_TYPE_QUERY,
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
    let config = Arc::new(ServerConfig {
        compression: crate::common::config::CompressionSettings {
            enabled: false,
            bulk_zstd_level: 1,
        },
        ..ServerConfig::default()
    });
    spawn_test_connection_with_limiter_and_config(engine, request_limiter, config)
}

fn spawn_test_connection_with_config(
    engine: Arc<Engine>,
    config: Arc<ServerConfig>,
) -> (Framed<DuplexStream, MessageCodec>, JoinHandle<Result<()>>) {
    spawn_test_connection_with_limiter_and_config(engine, Arc::new(Semaphore::new(4)), config)
}

fn spawn_test_connection_with_limiter_and_config(
    engine: Arc<Engine>,
    request_limiter: Arc<Semaphore>,
    config: Arc<ServerConfig>,
) -> (Framed<DuplexStream, MessageCodec>, JoinHandle<Result<()>>) {
    let (server_stream, client_stream) = tokio::io::duplex(1024 * 1024);
    let limiters = RequestLimiters::from_shared(request_limiter);
    let server = tokio::spawn(handle_framed(
        server_stream,
        config,
        engine,
        limiters,
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
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
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

/// bulk 导入会话存续期间，连接不受常规空闲超时约束（走
/// `bulk_import_idle_timeout_secs`，0 = 完全豁免）；会话结束后恢复
/// 常规空闲断连。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_import_sessions_survive_the_regular_idle_timeout() {
    let (_dir, engine) = test_engine();
    let mut config = ServerConfig::default();
    config.server.connection_idle_timeout_secs = 1;
    config.server.bulk_import_idle_timeout_secs = 0;
    let config = Arc::new(config);

    // 基线：常规连接空闲 1 秒即被断开。
    let (mut plain, plain_server) =
        spawn_test_connection_with_config(Arc::clone(&engine), Arc::clone(&config));
    login(&mut plain, "root").await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    timeout(Duration::from_secs(2), plain_server)
        .await
        .expect("regular idle connection should be closed")
        .unwrap()
        .unwrap();

    // bulk 会话连接：空闲超过常规窗口仍存活（PING 有 PONG）；
    // STOP BULK 结束会话后，常规空闲窗口重新生效。
    let (mut bulk, bulk_server) = spawn_test_connection_with_config(engine, config);
    login(&mut bulk, "root").await;
    bulk.send(query("START BULK IMPORT")).await.unwrap();
    let started = timeout(Duration::from_secs(2), bulk.next())
        .await
        .expect("bulk import should start")
        .unwrap()
        .unwrap();
    assert_eq!(started.msg_type, MSG_TYPE_OK);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    bulk.send(Message::empty(MSG_TYPE_PING)).await.unwrap();
    let pong = timeout(Duration::from_secs(2), bulk.next())
        .await
        .expect("bulk session must outlive the regular idle timeout")
        .unwrap()
        .unwrap();
    assert_eq!(pong.msg_type, MSG_TYPE_PONG);

    bulk.send(query("STOP BULK IMPORT")).await.unwrap();
    let stopped = timeout(Duration::from_secs(2), bulk.next())
        .await
        .expect("bulk import should stop")
        .unwrap()
        .unwrap();
    assert_eq!(stopped.msg_type, MSG_TYPE_OK);

    tokio::time::sleep(Duration::from_millis(1300)).await;
    timeout(Duration::from_secs(2), bulk_server)
        .await
        .expect("idle timeout should apply again after the bulk session ends")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_errors_still_release_bulk_import_session_state() {
    let (_dir, engine) = test_engine();
    let (mut client, server) = spawn_test_connection(Arc::clone(&engine));
    login(&mut client, "root").await;

    client.send(query("START BULK IMPORT")).await.unwrap();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_during_capacity_wait_still_flushes_bulk_import() {
    let (_dir, engine) = test_engine();
    let limiter = Arc::new(Semaphore::new(1));
    let (mut client, server) =
        spawn_test_connection_with_limiter(Arc::clone(&engine), Arc::clone(&limiter));
    login(&mut client, "root").await;

    client.send(query("START BULK IMPORT")).await.unwrap();
    client.next().await.unwrap().unwrap();
    assert_eq!(engine.bulk_mode_session_count(), 1);

    // Hold the only permit so the next heavy request queues, then disconnect.
    // The queued request must be cancelled promptly (cancel-aware queueing)
    // and the bulk-import flush must still run instead of leaking the session.
    let _held_permit = Arc::clone(&limiter).acquire_owned().await.unwrap();
    client.send(query("SELECT 1")).await.unwrap();
    drop(client);

    timeout(Duration::from_secs(2), server)
        .await
        .expect("cancel-aware queueing must not wait for the full queue timeout")
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.bulk_mode_session_count(),
        0,
        "bulk import session must be released by the disconnect flush"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_query_cancels_a_queued_request() {
    let (_dir, engine) = test_engine();
    // The victim's limiter is fully held, so its query stays queued (and
    // listed) until the KILL arrives. The killer connection gets its own
    // limiter so it can run immediately.
    let limiter = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&limiter).acquire_owned().await.unwrap();

    let (mut victim, victim_server) =
        spawn_test_connection_with_limiter(Arc::clone(&engine), Arc::clone(&limiter));
    login(&mut victim, "root").await;
    victim.send(query("SELECT 1")).await.unwrap();

    // Queued requests are listed in the registry, so the test can discover
    // the query id the same way SHOW PROCESSLIST would report it.
    let registry = engine.query_registry();
    timeout(Duration::from_secs(2), async {
        while registry.active_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queued request should be listed in the query registry");
    let query_id = registry.list()[0].query_id;

    let (mut killer, killer_server) =
        spawn_test_connection_with_limiter(Arc::clone(&engine), Arc::new(Semaphore::new(1)));
    login(&mut killer, "root").await;
    killer
        .send(query(&format!("KILL QUERY {}", query_id)))
        .await
        .unwrap();
    let kill_response = timeout(Duration::from_secs(2), killer.next())
        .await
        .expect("KILL QUERY should be answered")
        .unwrap()
        .unwrap();
    assert_eq!(kill_response.msg_type, MSG_TYPE_OK);

    let victim_response = timeout(Duration::from_secs(5), victim.next())
        .await
        .expect("killed request should receive a response")
        .unwrap()
        .unwrap();
    assert_eq!(victim_response.msg_type, MSG_TYPE_ERROR);
    let message = String::from_utf8_lossy(&victim_response.payload);
    assert!(
        message.contains("killed by user 'root'"),
        "unexpected error: {message}"
    );

    drop(victim);
    drop(killer);
    drop(held);
    timeout(Duration::from_secs(2), victim_server)
        .await
        .expect("victim connection should close")
        .unwrap()
        .unwrap();
    let _ = killer_server.await;
    assert_eq!(
        registry.active_count(),
        0,
        "killed request must be unregistered"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipelined_backlog_limit_closes_connection() {
    let (_dir, engine) = test_engine();
    let (mut client, server) = spawn_test_connection(engine);
    login(&mut client, "root").await;

    // Flood with heavy requests so the queue outpaces the drain and the
    // pending backlog hits its cap. The server closes the connection as soon
    // as the cap trips, so late sends may fail with a broken pipe.
    for _ in 0..(MAX_PENDING_MESSAGES * 3) {
        if client.send(query("SELECT 1")).await.is_err() {
            break;
        }
    }

    let mut saw_error = false;
    for _ in 0..(MAX_PENDING_MESSAGES * 3) {
        match timeout(Duration::from_secs(5), client.next())
            .await
            .expect("server should keep answering during the flood")
        {
            Some(Ok(msg)) if msg.msg_type == crate::protocol::message::MSG_TYPE_ERROR => {
                assert!(String::from_utf8_lossy(&msg.payload).contains("pipelined"));
                saw_error = true;
                break;
            }
            Some(Ok(_)) => {}
            Some(Err(error)) => panic!("unexpected decode error: {error}"),
            None => panic!("connection closed before the backlog error"),
        }
    }
    assert!(saw_error, "backlog limit should produce an error response");

    let closed = timeout(Duration::from_secs(5), client.next())
        .await
        .expect("server should close the connection after the backlog error");
    assert!(
        closed.is_none(),
        "connection should close after the backlog error"
    );

    let server_result = timeout(Duration::from_secs(2), server)
        .await
        .expect("server task should finish")
        .expect("server task should not panic");
    assert!(
        server_result.is_err(),
        "backlog violation should fail the connection"
    );
}

#[test]
fn reserved_bulk_slots_cap_query_pool_below_shared_pool() {
    let limiters = RequestLimiters::new(8, 2);
    assert_eq!(limiters.shared.available_permits(), 8);
    assert_eq!(limiters.query.available_permits(), 6);
}

#[tokio::test]
async fn saturated_queries_cannot_exhaust_slots_needed_by_bulk() {
    let limiters = RequestLimiters::new(8, 2);
    let mut query_permits = Vec::new();
    for _ in 0..6 {
        query_permits.push(
            limiters
                .query
                .clone()
                .acquire_owned()
                .await
                .expect("query permit"),
        );
    }
    assert_eq!(limiters.query.available_permits(), 0);

    let bulk_a = limiters
        .shared
        .clone()
        .acquire_owned()
        .await
        .expect("bulk permit a");
    let bulk_b = limiters
        .shared
        .clone()
        .acquire_owned()
        .await
        .expect("bulk permit b");
    assert_eq!(limiters.shared.available_permits(), 6);

    drop((query_permits, bulk_a, bulk_b));
}

#[test]
fn zero_reserved_bulk_slots_restores_single_pool_behavior() {
    let limiters = RequestLimiters::new(8, 0);
    assert_eq!(limiters.shared.available_permits(), 8);
    assert_eq!(limiters.query.available_permits(), 8);
}

#[test]
fn reserved_bulk_slots_clamped_to_shared_pool() {
    let limiters = RequestLimiters::new(4, 10);
    assert_eq!(limiters.shared.available_permits(), 4);
    assert_eq!(limiters.query.available_permits(), 1);
}
