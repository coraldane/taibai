use futures::SinkExt;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    let mut framed = Framed::new(
        stream,
        MessageCodec::new(config.server.max_message_size).with_compression(compress_threshold),
    );
    let mut handler = Handler::new(config, engine.clone());
    let mut pending_messages = VecDeque::new();

    let outcome = 'connection: loop {
        let next = if let Some(msg) = pending_messages.pop_front() {
            Some(Ok(msg))
        } else if idle_timeout_secs == 0 {
            framed.next().await
        } else {
            match timeout(Duration::from_secs(idle_timeout_secs), framed.next()).await {
                Ok(next) => next,
                Err(_) => {
                    tracing::warn!(idle_timeout_secs, "closing idle connection");
                    break Ok(());
                }
            }
        };
        let Some(result) = next else {
            break Ok(());
        };
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                tracing::error!(error = %e, "decode error");
                break Err(e);
            }
        };

        tracing::debug!(msg_type = msg.msg_type, "message received");

        let cancelled = Arc::new(AtomicBool::new(false));
        let (stream_sender, mut stream_receiver) = tokio::sync::mpsc::channel(1);
        let worker = run_handler(
            handler,
            msg,
            Arc::clone(&request_limiter),
            Arc::clone(&cancelled),
            stream_sender,
        );
        tokio::pin!(worker);
        let mut worker = loop {
            tokio::select! {
                result = &mut worker => break result?,
                next = framed.next() => {
                    match next {
                        Some(Ok(msg)) => pending_messages.push_back(msg),
                        Some(Err(error)) => {
                            tracing::error!(%error, "decode error while waiting for request capacity");
                            cancelled.store(true, Ordering::Release);
                            return Err(error);
                        }
                        None => {
                            cancelled.store(true, Ordering::Release);
                            return Ok(());
                        }
                    }
                }
            }
        };
        let mut disconnected = false;
        let (returned_handler, responses) = loop {
            tokio::select! {
                result = &mut worker => {
                    break result.map_err(|error| {
                        TaibaiError::Internal(format!("request worker failed: {error}"))
                    })?;
                }
                chunk = stream_receiver.recv() => {
                    if let Some(chunk) = chunk {
                        if let Err(error) = framed.send(chunk).await {
                            tracing::error!(%error, "query stream send error");
                            cancelled.store(true, Ordering::Release);
                            worker.abort();
                            return Ok(());
                        }
                    }
                }
                next = framed.next() => {
                    match next {
                        Some(Ok(msg)) => pending_messages.push_back(msg),
                        Some(Err(error)) => {
                            tracing::error!(%error, "decode error while query was running");
                            cancelled.store(true, Ordering::Release);
                            disconnected = true;
                        }
                        None => {
                            cancelled.store(true, Ordering::Release);
                            disconnected = true;
                        }
                    }
                    if disconnected {
                        match timeout(DISCONNECT_WORKER_WAIT, &mut worker).await {
                            Ok(result) => {
                                break result.map_err(|error| {
                                    TaibaiError::Internal(format!("request worker failed: {error}"))
                                })?;
                            }
                            Err(_) => {
                                tracing::warn!(
                                    "client disconnected but the request worker did not stop; aborting the worker and closing the connection"
                                );
                                worker.abort();
                                return Ok(());
                            }
                        }
                    }
                }
            }
        };
        handler = returned_handler;
        if disconnected {
            break Ok(());
        }
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
                    break Err(send_error);
                }
                break Err(error);
            }
        };
        if responses.is_empty() {
            break Ok(());
        }

        for resp in responses {
            if let Err(e) = framed.send(resp).await {
                tracing::error!(error = %e, "send error");
                break 'connection Err(e);
            }
        }
    };

    cleanup_handler_blocking(handler, request_limiter).await;
    tracing::info!("connection closed");
    outcome
}

const DISCONNECT_WORKER_WAIT: Duration = Duration::from_secs(5);

async fn run_handler(
    mut handler: Handler,
    msg: Message,
    request_limiter: Arc<Semaphore>,
    cancelled: Arc<AtomicBool>,
    stream_sender: tokio::sync::mpsc::Sender<Message>,
) -> Result<tokio::task::JoinHandle<(Handler, Result<Vec<Message>>)>> {
    if is_heavy_request(msg.msg_type) {
        let permit = request_limiter
            .acquire_owned()
            .await
            .map_err(|_| TaibaiError::Internal("request limiter closed".into()))?;
        Ok(tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let responses = handler.handle_message_cancellable(msg, cancelled, Some(stream_sender));
            (handler, responses)
        }))
    } else {
        Ok(tokio::spawn(async move {
            let responses = handler.handle_message_cancellable(msg, cancelled, Some(stream_sender));
            (handler, responses)
        }))
    }
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
#[allow(dead_code, unused_imports)]
#[path = "server_test.rs"]
mod tests;
