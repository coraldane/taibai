use futures::SinkExt;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;

use crate::common::config::ServerConfig;
use crate::common::error::{Result, TaibaiError};
use crate::net::handler::{build_error, Handler};
use crate::net::tls::TlsMode;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{
    Message, MSG_TYPE_BATCH_EXECUTE, MSG_TYPE_BULK_INSERT_COLUMNAR, MSG_TYPE_COMMIT_TXN,
    MSG_TYPE_CURSOR, MSG_TYPE_EXECUTE, MSG_TYPE_FLUSH, MSG_TYPE_QUERY, MSG_TYPE_ROLLBACK_TXN,
};
use crate::sql::execution::Engine;

pub struct Server {
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    tls_acceptor: Option<TlsAcceptor>,
    connection_limiter: Arc<Semaphore>,
    request_limiter: Arc<Semaphore>,
}

impl Server {
    pub fn new(config: Arc<ServerConfig>, engine: Arc<Engine>) -> Self {
        let tls_acceptor = match config.tls_mode() {
            TlsMode::Enable(tls_config) => match tls_config.validate() {
                Ok(()) => match tls_config.load_server_config() {
                    Ok(server_config) => Some(TlsAcceptor::from(server_config)),
                    Err(e) => {
                        tracing::error!("Failed to load TLS configuration: {}", e);
                        None
                    }
                },
                Err(e) => {
                    tracing::error!("TLS configuration invalid: {}", e);
                    None
                }
            },
            TlsMode::Disable => None,
        };
        Self {
            connection_limiter: Arc::new(Semaphore::new(config.server.max_connections.max(1))),
            request_limiter: Arc::new(Semaphore::new(config.server.max_concurrent_requests.max(1))),
            config,
            engine,
            tls_acceptor,
        }
    }

    pub async fn run_with_shutdown<S>(&self, shutdown: S) -> Result<()>
    where
        S: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let listener = TcpListener::bind(&self.config.listen_addr()).await?;
        tracing::info!("taibai listening on {}", self.config.listen_addr());
        let mut accept_error_delay = Duration::from_millis(50);
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = match result {
                        Ok(accepted) => {
                            accept_error_delay = Duration::from_millis(50);
                            accepted
                        }
                        Err(error) => {
                            tracing::error!(%error, retry_ms = accept_error_delay.as_millis(), "accept failed; retrying");
                            sleep(accept_error_delay).await;
                            accept_error_delay = accept_error_delay
                                .saturating_mul(2)
                                .min(Duration::from_secs(1));
                            continue;
                        }
                    };
                    let connection_permit = match Arc::clone(&self.connection_limiter).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(peer = %addr, "connection limit reached; rejecting client");
                            drop(stream);
                            continue;
                        }
                    };
                    let config = self.config.clone();
                    let engine = self.engine.clone();
                    let tls_acceptor = self.tls_acceptor.clone();
                    let request_limiter = Arc::clone(&self.request_limiter);
                    tokio::spawn(async move {
                        let _connection_permit = connection_permit;
                        if let Err(e) = handle_connection(
                            stream,
                            config,
                            engine,
                            tls_acceptor,
                            request_limiter,
                        ).await {
                            tracing::error!(peer = %addr, error = %e, "connection error");
                        }
                    });
                }
                _ = &mut shutdown => return Ok(()),
            }
        }
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    tls_acceptor: Option<TlsAcceptor>,
    request_limiter: Arc<Semaphore>,
) -> Result<()> {
    if let Some(acceptor) = tls_acceptor {
        tracing::debug!("Performing TLS handshake");
        let handshake = acceptor.accept(stream);
        let tls_stream = if config.server.tls_handshake_timeout_secs == 0 {
            handshake.await
        } else {
            timeout(
                Duration::from_secs(config.server.tls_handshake_timeout_secs),
                handshake,
            )
            .await
            .map_err(|_| TaibaiError::Network("TLS handshake timed out".into()))?
        }
        .map_err(|e| TaibaiError::Network(format!("TLS handshake failed: {}", e)))?;
        tracing::debug!("TLS handshake completed");
        handle_framed(tls_stream, config, engine, request_limiter).await
    } else {
        handle_framed(stream, config, engine, request_limiter).await
    }
}

async fn handle_framed<S>(
    stream: S,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    request_limiter: Arc<Semaphore>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let compress_threshold = if config.compression.enabled {
        4096
    } else {
        usize::MAX
    };
    let idle_timeout_secs = config.server.connection_idle_timeout_secs;
    let request_queue_timeout_ms = config.server.request_queue_timeout_ms;
    let notification_capacity = config.pubsub.notification_channel_capacity.max(1);
    let mut framed = Framed::new(
        stream,
        MessageCodec::new(config.server.max_message_size).with_compression(compress_threshold),
    );
    let mut handler = Some(Handler::new(config, engine.clone()));
    // Per-connection push queue for pubsub notifications. The pubsub fan-out
    // thread only ever `try_send`s, so a stalled client drops notifications
    // (counted) instead of blocking writers or growing memory unboundedly.
    // `keep_push_tx` anchors the receiver for the connection lifetime even
    // while the handler (holding the other clone) is moved into a worker.
    let (push_tx, mut push_rx) = tokio::sync::mpsc::channel::<Message>(notification_capacity);
    let keep_push_tx = push_tx.clone();
    handler
        .as_mut()
        .expect("handler exists before the first request")
        .set_push_channel(push_tx);
    let mut pending_messages = VecDeque::new();

    let outcome = 'connection: loop {
        // Fetch the next request frame, giving the pending queue priority so
        // frames buffered while a request was in flight are not starved.
        let msg = 'fetch: loop {
            if let Some(msg) = pending_messages.pop_front() {
                break 'fetch msg;
            }
            // Delivering a queued notification counts as connection activity
            // and restarts the idle window, so a pure subscriber that pushes
            // data but sends no frames is not disconnected mid-stream.
            let wait = next_frame_or_push(&mut framed, &mut push_rx);
            let incoming = if idle_timeout_secs == 0 {
                wait.await
            } else {
                match timeout(Duration::from_secs(idle_timeout_secs), wait).await {
                    Ok(incoming) => incoming,
                    Err(_) => {
                        tracing::warn!(idle_timeout_secs, "closing idle connection");
                        break 'connection Ok(());
                    }
                }
            };
            match incoming {
                Incoming::Pushed => continue,
                Incoming::Closed | Incoming::Frame(None) => break 'connection Ok(()),
                Incoming::Frame(Some(Err(error))) => {
                    tracing::error!(error = %error, "decode error");
                    break 'connection Err(error);
                }
                Incoming::Frame(Some(Ok(msg))) => break 'fetch msg,
            }
        };

        tracing::debug!(msg_type = msg.msg_type, "message received");

        let cancelled = Arc::new(AtomicBool::new(false));
        let (stream_sender, mut stream_receiver) = tokio::sync::mpsc::channel(1);
        let worker = run_handler(
            handler.take().expect("handler available between requests"),
            msg,
            Arc::clone(&request_limiter),
            Arc::clone(&cancelled),
            stream_sender,
            request_queue_timeout_ms,
        );
        tokio::pin!(worker);
        // Single wait loop for the whole request lifetime (queueing for a
        // permit and executing): stream result chunks, forward pubsub pushes,
        // and buffer pipelined frames. Every disconnect/error exit sets
        // `disconnect_outcome` and falls through to the same bounded reaping,
        // so the handler always reaches the normal `flush_on_disconnect`
        // cleanup instead of being dropped mid-flight.
        let mut disconnect_outcome: Option<Result<()>> = None;
        let (returned_handler, request_outcome) = 'request: loop {
            tokio::select! {
                joined = &mut worker => {
                    let (handler, responses) = unpack_worker_output(joined);
                    break 'request (handler, RequestOutcome::Completed(responses));
                }
                chunk = stream_receiver.recv() => {
                    if let Some(chunk) = chunk {
                        if let Err(error) = framed.send(chunk).await {
                            tracing::error!(%error, "query stream send error");
                            cancelled.store(true, Ordering::Release);
                            disconnect_outcome = Some(Ok(()));
                        }
                    }
                    // `None`: the worker finished racing this recv; the worker
                    // arm is now ready and wins the next iteration.
                }
                push = push_rx.recv() => {
                    match push {
                        Some(push) => {
                            if !send_pubsub_push(&mut framed, push).await {
                                cancelled.store(true, Ordering::Release);
                                disconnect_outcome = Some(Ok(()));
                            }
                        }
                        None => {
                            cancelled.store(true, Ordering::Release);
                            disconnect_outcome = Some(Ok(()));
                        }
                    }
                }
                next = framed.next() => {
                    match next {
                        Some(Ok(msg)) => {
                            if pending_messages.len() >= MAX_PENDING_MESSAGES {
                                tracing::warn!(
                                    limit = MAX_PENDING_MESSAGES,
                                    "pipelined request backlog limit reached; closing connection"
                                );
                                let error = TaibaiError::ResourceBusy(format!(
                                    "too many pipelined requests (limit {MAX_PENDING_MESSAGES})"
                                ));
                                cancelled.store(true, Ordering::Release);
                                if let Err(send_error) =
                                    framed.send(build_error(&error.to_string())).await
                                {
                                    tracing::error!(error = %send_error, "send backlog error response failed");
                                }
                                disconnect_outcome = Some(Err(error));
                            } else {
                                pending_messages.push_back(msg);
                            }
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, "decode error while query was running");
                            cancelled.store(true, Ordering::Release);
                            disconnect_outcome = Some(Err(error));
                        }
                        None => {
                            cancelled.store(true, Ordering::Release);
                            disconnect_outcome = Some(Ok(()));
                        }
                    }
                }
            }
            if let Some(outcome) = disconnect_outcome.take() {
                // Give the worker a bounded window to observe the cancellation
                // flag and hand the handler back. `abort` is advisory for
                // blocking workers; the cancelled flag does the real work.
                match timeout(DISCONNECT_WORKER_WAIT, &mut worker).await {
                    Ok(joined) => {
                        let (handler, responses) = unpack_worker_output(joined);
                        if let Err(error) = responses {
                            tracing::error!(%error, "request worker failed while disconnecting");
                        }
                        break 'request (handler, RequestOutcome::Disconnected(outcome));
                    }
                    Err(_) => {
                        tracing::warn!(
                            "client disconnected but the request worker did not stop; abandoning the worker and closing the connection"
                        );
                        worker.abort();
                        break 'request (None, RequestOutcome::Disconnected(outcome));
                    }
                }
            }
        };
        handler = returned_handler;
        let responses = match request_outcome {
            RequestOutcome::Disconnected(outcome) => break 'connection outcome,
            RequestOutcome::Completed(responses) => responses,
        };
        while let Ok(chunk) = stream_receiver.try_recv() {
            if let Err(error) = framed.send(chunk).await {
                tracing::error!(%error, "query stream send error");
                break 'connection Err(error);
            }
        }
        let responses = match responses {
            Ok(responses) => responses,
            Err(error) => {
                tracing::error!(%error, "request handler error");
                if let Err(send_error) = framed.send(build_error(&error.to_string())).await {
                    tracing::error!(error = %send_error, "send error response failed");
                    break 'connection Err(send_error);
                }
                break 'connection Err(error);
            }
        };
        if responses.is_empty() {
            break 'connection Ok(());
        }

        for resp in responses {
            if let Err(e) = framed.send(resp).await {
                tracing::error!(error = %e, "send error");
                break 'connection Err(e);
            }
        }
    };

    if let Some(handler) = handler.take() {
        cleanup_handler_blocking(handler, request_limiter).await;
    }
    tracing::info!("connection closed");
    drop(keep_push_tx);
    outcome
}

/// Send one pubsub push frame. `false` means the socket is dead and the
/// connection must wind down.
async fn send_pubsub_push<S>(framed: &mut Framed<S, MessageCodec>, push: Message) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match framed.send(push).await {
        Ok(()) => true,
        Err(error) => {
            tracing::error!(%error, "pubsub push send error");
            false
        }
    }
}

enum Incoming {
    /// A decoded protocol frame; `None` means the client closed the stream.
    Frame(Option<std::result::Result<Message, TaibaiError>>),
    /// One queued notification was delivered; the caller should resume
    /// waiting with a fresh (idle) timeout window.
    Pushed,
    /// The push channel vanished or a queued notification could not be
    /// delivered; the connection must close.
    Closed,
}

/// Wait for the next client frame, forwarding any queued pubsub notifications
/// to the socket along the way. Used where the loop would otherwise block on
/// `framed.next()` alone, so subscribers receive pushes between requests.
async fn next_frame_or_push<S>(
    framed: &mut Framed<S, MessageCodec>,
    push_rx: &mut tokio::sync::mpsc::Receiver<Message>,
) -> Incoming
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::select! {
        next = framed.next() => Incoming::Frame(next),
        push = push_rx.recv() => {
            match push {
                Some(push) => {
                    if send_pubsub_push(framed, push).await {
                        Incoming::Pushed
                    } else {
                        Incoming::Closed
                    }
                }
                None => Incoming::Closed,
            }
        }
    }
}

/// Pipelined frames buffered while a request is in flight. A malicious or
/// runaway client must not be able to grow this without bound merely by
/// flooding frames faster than the server drains them.
const MAX_PENDING_MESSAGES: usize = 1024;

/// How often a queued heavy request re-checks the cancellation flag while
/// waiting for a limiter slot, so a disconnect does not have to wait out the
/// full queue timeout.
const QUEUE_CANCEL_POLL: Duration = Duration::from_millis(50);

/// How long disconnect cleanup waits for an in-flight request worker to hand
/// the handler back before abandoning it.
const DISCONNECT_WORKER_WAIT: Duration = Duration::from_secs(5);

/// How the request wait loop ended.
enum RequestOutcome {
    /// The worker finished on its own; deliver its responses.
    Completed(Result<Vec<Message>>),
    /// The connection is going away; this is the final connection result.
    Disconnected(Result<()>),
}

/// Normalize a finished worker into its handler and responses. `None` means
/// the worker panicked; the handler was dropped together with the panic,
/// which already released its pubsub subscriptions, so nothing is left to
/// clean up.
fn unpack_worker_output(
    joined: std::result::Result<(Option<Handler>, Result<Vec<Message>>), tokio::task::JoinError>,
) -> (Option<Handler>, Result<Vec<Message>>) {
    match joined {
        Ok(output) => output,
        Err(join_error) => {
            tracing::error!(%join_error, "request worker task failed");
            (
                None,
                Err(TaibaiError::Internal("request worker failed".into())),
            )
        }
    }
}

/// Spawn the request worker. The returned task always terminates with the
/// handler (`None` only when the worker panicked), so disconnect cleanup can
/// recover it after a bounded wait.
fn run_handler(
    mut handler: Handler,
    msg: Message,
    request_limiter: Arc<Semaphore>,
    cancelled: Arc<AtomicBool>,
    stream_sender: tokio::sync::mpsc::Sender<Message>,
    queue_timeout_ms: u64,
) -> tokio::task::JoinHandle<(Option<Handler>, Result<Vec<Message>>)> {
    tokio::task::spawn(async move {
        if !is_heavy_request(msg.msg_type) {
            let responses = handler.handle_message_cancellable(msg, cancelled, Some(stream_sender));
            return (Some(handler), responses);
        }

        // Wait for a request slot while watching the cancellation flag, so a
        // disconnect during a full queue does not stall cleanup for the whole
        // queue timeout.
        let deadline = Instant::now() + Duration::from_millis(queue_timeout_ms.max(1));
        let permit = loop {
            if cancelled.load(Ordering::Acquire) {
                return (
                    Some(handler),
                    Ok(vec![build_error(
                        "request cancelled because the client disconnected",
                    )]),
                );
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    msg_type = msg.msg_type,
                    queue_timeout_ms,
                    "request queue wait timeout; rejecting request with overload error"
                );
                return (
                    Some(handler),
                    Ok(vec![build_error(
                        "server overloaded: request queue wait timeout",
                    )]),
                );
            }
            match timeout(
                remaining.min(QUEUE_CANCEL_POLL),
                Arc::clone(&request_limiter).acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => break permit,
                Ok(Err(_)) => {
                    return (
                        Some(handler),
                        Err(TaibaiError::Internal("request limiter closed".into())),
                    );
                }
                Err(_elapsed) => {} // poll tick: re-check cancelled / deadline
            }
        };

        let joined = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let responses = handler.handle_message_cancellable(msg, cancelled, Some(stream_sender));
            (handler, responses)
        })
        .await;
        match joined {
            Ok((handler, responses)) => (Some(handler), responses),
            Err(join_error) => {
                tracing::error!(%join_error, "request worker panicked");
                (
                    None,
                    Err(TaibaiError::Internal("request worker panicked".into())),
                )
            }
        }
    })
}

async fn cleanup_handler_blocking(mut handler: Handler, request_limiter: Arc<Semaphore>) {
    let needs_heavy_permit = handler
        .session
        .as_ref()
        .is_some_and(|session| session.had_writes);
    let permit = if needs_heavy_permit {
        match timeout(DISCONNECT_WORKER_WAIT, request_limiter.acquire_owned()).await {
            Ok(Ok(permit)) => Some(permit),
            Ok(Err(_)) => return,
            Err(_) => {
                tracing::warn!("request limiter busy; skipping disconnect flush");
                return;
            }
        }
    } else {
        None
    };
    if let Err(error) = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handler.flush_on_disconnect();
    })
    .await
    {
        tracing::error!(%error, "connection cleanup worker failed");
    }
}

fn is_heavy_request(msg_type: u8) -> bool {
    matches!(
        msg_type,
        MSG_TYPE_QUERY
            | MSG_TYPE_BULK_INSERT_COLUMNAR
            | MSG_TYPE_FLUSH
            | MSG_TYPE_EXECUTE
            | MSG_TYPE_BATCH_EXECUTE
            | MSG_TYPE_COMMIT_TXN
            | MSG_TYPE_ROLLBACK_TXN
            | MSG_TYPE_CURSOR
    )
}

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
