//! End-to-end retry semantics for `[RETRYABLE]` (resource-busy) server
//! errors.
//!
//! A minimal scripted TCP peer speaks just enough of the wire protocol
//! (7-byte header: magic `0x01 0xDB`, msg_type, u32-LE payload length) to
//! drive `Client::query`: each request it receives pops the next scripted
//! response. That isolates the client's retry decision from the engine —
//! real resource-busy conditions (ALTER convergence window, hot-store
//! backpressure, join budget exhaustion) are not deterministically
//! triggerable from a test.
//!
//! Contract under test:
//! * read-only statements (SELECT/SHOW/EXPLAIN) auto-retry with backoff
//!   (10ms/40ms/160ms + jitter, 3 retries) and succeed once the transient
//!   error clears;
//! * write statements (INSERT/...) are NEVER auto-retried — a retried
//!   INSERT may commit twice — and surface the `[RETRYABLE]` marker to the
//!   application verbatim.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use taibai::net::client::{Client, QueryResult, RETRYABLE_ERROR_PREFIX};
use taibai::protocol::message::{MAGIC, MSG_TYPE_ERROR, MSG_TYPE_OK};

struct MockServer {
    addr: String,
    requests_seen: Arc<AtomicUsize>,
}

/// One scripted reply per received request frame.
async fn spawn_mock_server(script: Vec<(u8, Vec<u8>)>) -> MockServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral mock port");
    let addr = listener.local_addr().expect("mock local addr").to_string();
    let requests_seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests_seen);
    tokio::spawn(async move {
        let (mut socket, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(_) => return,
        };
        let mut script = script.into_iter();
        loop {
            // Read one request frame: 7-byte header + payload.
            let mut header = [0u8; 7];
            if tokio::io::AsyncReadExt::read_exact(&mut socket, &mut header)
                .await
                .is_err()
            {
                return;
            }
            let payload_len =
                u32::from_le_bytes([header[3], header[4], header[5], header[6]]) as usize;
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0
                && tokio::io::AsyncReadExt::read_exact(&mut socket, &mut payload)
                    .await
                    .is_err()
            {
                return;
            }
            counter.fetch_add(1, Ordering::SeqCst);
            let Some((msg_type, body)) = script.next() else {
                return;
            };
            let mut frame = Vec::with_capacity(7 + body.len());
            frame.extend_from_slice(&MAGIC);
            frame.push(msg_type);
            frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
            frame.extend_from_slice(&body);
            if tokio::io::AsyncWriteExt::write_all(&mut socket, &frame)
                .await
                .is_err()
            {
                return;
            }
        }
    });
    MockServer {
        addr,
        requests_seen,
    }
}

fn error_frame(message: &str) -> (u8, Vec<u8>) {
    (MSG_TYPE_ERROR, message.as_bytes().to_vec())
}

fn ok_frame(rows_affected: u64) -> (u8, Vec<u8>) {
    (MSG_TYPE_OK, rows_affected.to_le_bytes().to_vec())
}

#[tokio::test(flavor = "current_thread")]
async fn select_retries_retryable_error_and_succeeds() {
    let server = spawn_mock_server(vec![
        error_frame(&format!(
            "{RETRYABLE_ERROR_PREFIX} resource busy: mocked ALTER convergence window"
        )),
        ok_frame(1),
    ]).await;
    let mut client = Client::connect(&server.addr).await.expect("connect");
    let result = client.query("SELECT 1").await.expect("query");
    match result {
        QueryResult::Ok { rows_affected, .. } => assert_eq!(rows_affected, 1),
        other => panic!("expected Ok after retry, got {other:?}"),
    }
    assert_eq!(
        server.requests_seen.load(Ordering::SeqCst),
        2,
        "first attempt hit the retryable error, second succeeded"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn insert_is_not_auto_retried_and_keeps_the_marker() {
    let server = spawn_mock_server(vec![
        error_frame(&format!("{RETRYABLE_ERROR_PREFIX} resource busy: mocked")),
        ok_frame(1),
    ]).await;
    let mut client = Client::connect(&server.addr).await.expect("connect");
    let result = client
        .query("INSERT INTO t VALUES (1)")
        .await
        .expect("query transport");
    match result {
        QueryResult::Error(message) => {
            assert!(
                message.starts_with(RETRYABLE_ERROR_PREFIX),
                "write errors pass the marker through for the application, got {message:?}"
            );
        }
        other => panic!("expected Error for a write statement, got {other:?}"),
    }
    assert_eq!(
        server.requests_seen.load(Ordering::SeqCst),
        1,
        "a retried INSERT could commit twice; the client must not re-issue writes"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn select_gives_up_after_three_retries_and_surfaces_the_error() {
    let server = spawn_mock_server(vec![
        error_frame(&format!("{RETRYABLE_ERROR_PREFIX} resource busy: 1")),
        error_frame(&format!("{RETRYABLE_ERROR_PREFIX} resource busy: 2")),
        error_frame(&format!("{RETRYABLE_ERROR_PREFIX} resource busy: 3")),
        error_frame(&format!("{RETRYABLE_ERROR_PREFIX} resource busy: 4")),
    ]).await;
    let mut client = Client::connect(&server.addr).await.expect("connect");
    let result = client.query("SHOW TABLES").await.expect("query transport");
    match result {
        QueryResult::Error(message) => {
            assert!(message.starts_with(RETRYABLE_ERROR_PREFIX));
            assert!(message.contains("resource busy: 4"), "got {message:?}");
        }
        other => panic!("expected Error after exhausted retries, got {other:?}"),
    }
    assert_eq!(
        server.requests_seen.load(Ordering::SeqCst),
        4,
        "1 initial attempt + 3 retries"
    );
}
