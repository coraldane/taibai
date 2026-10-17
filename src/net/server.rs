use futures::SinkExt;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
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
use crate::net::handler::{build_error, is_control_request, is_heavy_request, Handler};
use crate::net::tls::TlsMode;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{Message, MSG_TYPE_QUERY};
use crate::sql::execution::Engine;

pub struct Server {
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    tls_acceptor: Option<TlsAcceptor>,
    connection_limiter: Arc<Semaphore>,
    limiters: RequestLimiters,
}

#[derive(Clone)]
struct RequestLimiters {
    shared: Arc<Semaphore>,
    query: Arc<Semaphore>,
    control: Arc<Semaphore>,
}

impl RequestLimiters {
    fn new(max_concurrent_requests: usize, reserved_bulk_slots: usize) -> Self {
        let shared = max_concurrent_requests.max(1);
        let reserved = reserved_bulk_slots.min(shared - 1);
        Self {
            shared: Arc::new(Semaphore::new(shared)),
            query: Arc::new(Semaphore::new(shared - reserved)),
            control: Arc::new(Semaphore::new(2)),
        }
    }

    #[cfg(test)]
    fn from_shared(shared: Arc<Semaphore>) -> Self {
        let size = shared.available_permits().max(1);
        Self {
            shared,
            query: Arc::new(Semaphore::new(size)),
            control: Arc::new(Semaphore::new(2)),
        }
    }
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
            limiters: RequestLimiters::new(
                config.server.max_concurrent_requests,
                config.server.reserved_bulk_slots,
            ),
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
                            // Request/response protocol: small replies must
                            // not wait on Nagle/delayed-ACK interaction.
                            let _ = accepted.0.set_nodelay(true);
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
                    let limiters = self.limiters.clone();
                    tokio::spawn(async move {
                        let _connection_permit = connection_permit;
                        if let Err(e) = handle_connection(
                            stream,
                            config,
                            engine,
                            tls_acceptor,
                            limiters,
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
    limiters: RequestLimiters,
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
        handle_framed(tls_stream, config, engine, limiters).await
    } else {
        handle_framed(stream, config, engine, limiters).await
    }
}

async fn handle_framed<S>(
    stream: S,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    limiters: RequestLimiters,
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
    let bulk_idle_timeout_secs = config.server.bulk_import_idle_timeout_secs;
    let request_queue_timeout_secs = config.server.request_queue_timeout_secs;
    let notification_capacity = config.pubsub.notification_channel_capacity.max(1);
    let mut framed = Framed::new(
        stream,
        MessageCodec::new(config.server.max_message_size)
            .with_bulk_max_message_size(config.server.max_bulk_message_size)
            .with_compression(compress_threshold),
    );
    let mut handler = Some(Handler::new(Arc::clone(&config), engine.clone()));
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
    let mut pending_messages: VecDeque<Message> = VecDeque::new();
    let mut pending_bytes = 0usize;
    // Requests the connection has accepted (dispatched to a worker) whose
    // responses have not been delivered yet. The idle close must treat such
    // requests — and frames still sitting in `pending_messages` — as
    // connection activity: a client blocked waiting for a slow response
    // (bulk writes queue on table locks for a long time) legitimately sends
    // nothing, and must not lose its connection on top of that.
    let mut requests_in_flight: usize = 0;

    let outcome = 'connection: loop {
        // Fetch the next request frame, giving the pending queue priority so
        // frames buffered while a request was in flight are not starved.
        let msg = 'fetch: loop {
            if let Some(msg) = pending_messages.pop_front() {
                pending_bytes = pending_bytes.saturating_sub(msg.payload.len());
                break 'fetch msg;
            }
            // Delivering a queued notification counts as connection activity
            // and restarts the idle window, so a pure subscriber that pushes
            // data but sends no frames is not disconnected mid-stream.
            let wait = next_frame_or_push(&mut framed, &mut push_rx);
            // A bulk-import client legitimately stays silent for long
            // stretches (decoding input files, filling batches) and depends
            // on keepalive PINGs; a PING interval racing the regular idle
            // timeout would kill a live import mid-file. Widen (or drop,
            // when set to 0) the idle window while the bulk session is open.
            let effective_idle_timeout_secs =
                if handler.as_ref().is_some_and(|h| h.is_bulk_import_mode()) {
                    bulk_idle_timeout_secs
                } else {
                    idle_timeout_secs
                };
            let incoming = if effective_idle_timeout_secs == 0 {
                wait.await
            } else {
                match timeout(Duration::from_secs(effective_idle_timeout_secs), wait).await {
                    Ok(incoming) => incoming,
                    Err(_) => {
                        // In-flight or queued requests count as activity:
                        // restart the idle window instead of closing. With
                        // the current one-worker-at-a-time dispatch this is
                        // defense in depth (the fetch wait is only reached
                        // once the previous request's responses went out and
                        // the pending queue is empty), but it keeps the idle
                        // close correct by construction if dispatch ever
                        // overlaps response delivery.
                        if requests_in_flight > 0 || !pending_messages.is_empty() {
                            tracing::debug!(
                                requests_in_flight,
                                queued_frames = pending_messages.len(),
                                "idle timeout deferred: connection still has in-flight or queued requests"
                            );
                            continue;
                        }
                        tracing::warn!(
                            idle_timeout_secs = effective_idle_timeout_secs,
                            "closing idle connection"
                        );
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
        requests_in_flight += 1;

        let context = build_query_context(&config, Arc::new(AtomicBool::new(false)));
        let (stream_sender, mut stream_receiver) = tokio::sync::mpsc::channel(1);
        let mut worker = run_handler(
            handler.take().expect("handler available between requests"),
            msg,
            limiters.clone(),
            Arc::clone(&context),
            stream_sender,
            request_queue_timeout_secs,
        );
        // `JoinHandle` is `Unpin`, so `&mut worker` can be polled directly in
        // the select below; the handle is also movable into the disconnect
        // reaper if the worker outlives the graceful window.
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
                            context.cancel();
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
                                context.cancel();
                                disconnect_outcome = Some(Ok(()));
                            }
                        }
                        None => {
                            context.cancel();
                            disconnect_outcome = Some(Ok(()));
                        }
                    }
                }
                next = framed.next() => {
                    match next {
                        Some(Ok(msg)) => {
                            let frame_bytes = msg.payload.len();
                            if pending_messages.len() >= MAX_PENDING_MESSAGES
                                || pending_bytes + frame_bytes > MAX_PENDING_BYTES
                            {
                                tracing::warn!(
                                    limit = MAX_PENDING_MESSAGES,
                                    byte_limit = MAX_PENDING_BYTES,
                                    pending_bytes,
                                    "pipelined request backlog limit reached; closing connection"
                                );
                                let error = TaibaiError::ResourceBusy(format!(
                                    "too many pipelined requests (limit {MAX_PENDING_MESSAGES} frames / {} bytes)",
                                    MAX_PENDING_BYTES
                                ));
                                context.cancel();
                                if let Err(send_error) =
                                    framed.send(build_error(&error.to_string())).await
                                {
                                    tracing::error!(error = %send_error, "send backlog error response failed");
                                }
                                disconnect_outcome = Some(Err(error));
                            } else {
                                pending_bytes += frame_bytes;
                                pending_messages.push_back(msg);
                            }
                        }
                        Some(Err(error)) => {
                            tracing::error!(%error, "decode error while query was running");
                            context.cancel();
                            disconnect_outcome = Some(Err(error));
                        }
                        None => {
                            context.cancel();
                            disconnect_outcome = Some(Ok(()));
                        }
                    }
                }
            }
            if let Some(outcome) = disconnect_outcome.take() {
                // Give the worker a bounded window to observe the cancellation
                // flag and hand the handler back. `abort` cannot stop a
                // blocking worker; the cancelled flag does the real work.
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
                            "client disconnected but the request worker did not stop; a reaper task will clean up when it does"
                        );
                        // The blocking worker keeps running until it reaches a
                        // safe point and observes the cancellation flag, so it
                        // cannot be torn down here. Hand it to a detached
                        // reaper: when the query finally stops, the handler
                        // still reaches `flush_on_disconnect` instead of being
                        // dropped with bulk-import dirty data unflushed.
                        let reaper_limiter = Arc::clone(&limiters.shared);
                        tokio::spawn(async move {
                            let (handler, responses) = unpack_worker_output(worker.await);
                            if let Err(error) = responses {
                                tracing::error!(%error, "abandoned request worker failed");
                            }
                            if let Some(handler) = handler {
                                cleanup_handler_blocking(handler, reaper_limiter).await;
                            }
                        });
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
        requests_in_flight = requests_in_flight.saturating_sub(1);
    };

    if let Some(handler) = handler.take() {
        cleanup_handler_blocking(handler, Arc::clone(&limiters.shared)).await;
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

/// Byte ceiling for the same queue: 1024 small frames and a handful of
/// maximum-size (64MB) bulk frames are very different memory footprints, so
/// the count limit alone was not a real bound.
const MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;

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

/// Build the per-request cancellation context: the connection-owned
/// disconnect flag plus the configured statement deadline (0 = no limit).
fn build_query_context(
    config: &ServerConfig,
    cancelled: Arc<AtomicBool>,
) -> Arc<crate::common::query_cancel::QueryContext> {
    let deadline_secs = config.server.statement_timeout_secs;
    let deadline = if deadline_secs == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(deadline_secs))
    };
    Arc::new(crate::common::query_cancel::QueryContext::new(cancelled).with_deadline(deadline))
}

/// Spawn the request worker. The returned task always terminates with the
/// handler (`None` only when the worker panicked), so disconnect cleanup can
/// recover it after a bounded wait. Heavy requests are listed in the engine's
/// query registry before queueing, so queued work shows in `SHOW PROCESSLIST`
/// and can be cancelled via `KILL QUERY`.
enum SlotAcquisition {
    Permit(tokio::sync::OwnedSemaphorePermit),
    Rejected(String),
}

async fn acquire_slot(
    limiter: &Arc<Semaphore>,
    context: &Arc<crate::common::query_cancel::QueryContext>,
    msg_type: u8,
    queue_timeout_secs: u64,
    slot_pool: &'static str,
) -> std::result::Result<SlotAcquisition, TaibaiError> {
    // Wait for a request slot while watching the cancellation context, so
    // a disconnect (or KILL) during a full queue does not stall cleanup
    // for the whole queue timeout.
    let deadline = Instant::now() + Duration::from_secs(queue_timeout_secs.max(1));
    // Retain the same waiter across cancellation ticks: dropping and recreating
    // it loses the semaphore FIFO position and can starve older requests.
    let acquisition = Arc::clone(limiter).acquire_owned();
    tokio::pin!(acquisition);
    loop {
        if let Err(error) = context.check() {
            return Ok(SlotAcquisition::Rejected(error.to_string()));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            tracing::warn!(
                msg_type,
                slot_pool,
                queue_timeout_secs,
                "request queue wait timeout; rejecting request with overload error"
            );
            return Ok(SlotAcquisition::Rejected(
                "server overloaded: request queue wait timeout".into(),
            ));
        }
        match timeout(remaining.min(QUEUE_CANCEL_POLL), &mut acquisition).await {
            Ok(Ok(permit)) => return Ok(SlotAcquisition::Permit(permit)),
            Ok(Err(_)) => {
                return Err(TaibaiError::Internal("request limiter closed".into()));
            }
            Err(_elapsed) => {} // poll tick: re-check cancelled / deadline
        }
    }
}

fn run_handler(
    mut handler: Handler,
    msg: Message,
    limiters: RequestLimiters,
    context: Arc<crate::common::query_cancel::QueryContext>,
    stream_sender: tokio::sync::mpsc::Sender<Message>,
    queue_timeout_secs: u64,
) -> tokio::task::JoinHandle<(Option<Handler>, Result<Vec<Message>>)> {
    // Registered on the connection task before the worker spawns; the guard
    // lives across the queue wait and the execution.
    let registration = handler.register_running_query(&context, &msg);
    tokio::task::spawn(async move {
        // Keep the registration alive for the whole request (queue wait and
        // execution); dropping it unlists the query.
        let _registration = registration;
        if !is_heavy_request(msg.msg_type) {
            let responses = handler.handle_message_cancellable(msg, context, Some(stream_sender));
            return (Some(handler), responses);
        }

        // Queries admit through the capped pool first, so a burst of
        // analytical scans can never occupy the slots bulk writes need to
        // keep committing (`reserved_bulk_slots`).
        let control_request = is_control_request(&msg);
        let _query_cap = if msg.msg_type == MSG_TYPE_QUERY && !control_request {
            match acquire_slot(
                &limiters.query,
                &context,
                msg.msg_type,
                queue_timeout_secs,
                "query",
            )
            .await
            {
                Ok(SlotAcquisition::Permit(permit)) => Some(permit),
                Ok(SlotAcquisition::Rejected(reason)) => {
                    return (Some(handler), Ok(vec![build_error(&reason)]));
                }
                Err(error) => return (Some(handler), Err(error)),
            }
        } else {
            None
        };

        // KILL/PROCESSLIST must remain available when ordinary work fills
        // every slot, while their own bounded pool prevents unbounded work.
        let request_pool = if control_request {
            &limiters.control
        } else {
            &limiters.shared
        };
        let permit = match acquire_slot(
            request_pool,
            &context,
            msg.msg_type,
            queue_timeout_secs,
            "shared",
        )
        .await
        {
            Ok(SlotAcquisition::Permit(permit)) => permit,
            Ok(SlotAcquisition::Rejected(reason)) => {
                return (Some(handler), Ok(vec![build_error(&reason)]));
            }
            Err(error) => return (Some(handler), Err(error)),
        };

        let joined = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let responses = handler.handle_message_cancellable(msg, context, Some(stream_sender));
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

#[cfg(test)]
#[path = "server_test.rs"]
mod tests;
