use futures::SinkExt;
use futures::StreamExt;
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
use crate::net::handler::Handler;
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

    let outcome = 'connection: loop {
        let next = if idle_timeout_secs == 0 {
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

        let (returned_handler, responses) =
            run_handler_blocking(handler, msg, Arc::clone(&request_limiter)).await?;
        handler = returned_handler;
        let responses = match responses {
            Ok(responses) => responses,
            Err(error) => break Err(error),
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

async fn run_handler_blocking(
    mut handler: Handler,
    msg: Message,
    request_limiter: Arc<Semaphore>,
) -> Result<(Handler, Result<Vec<Message>>)> {
    let permit = if is_heavy_request(msg.msg_type) {
        Some(
            request_limiter
                .acquire_owned()
                .await
                .map_err(|_| TaibaiError::Internal("request limiter closed".into()))?,
        )
    } else {
        None
    };

    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let responses = handler.handle_message(msg);
        (handler, responses)
    })
    .await
    .map_err(|error| TaibaiError::Internal(format!("request worker failed: {error}")))
}

async fn cleanup_handler_blocking(mut handler: Handler, request_limiter: Arc<Semaphore>) {
    let needs_heavy_permit = handler
        .session
        .as_ref()
        .is_some_and(|session| session.had_writes);
    let permit = if needs_heavy_permit {
        let Ok(permit) = request_limiter.acquire_owned().await else {
            return;
        };
        Some(permit)
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
mod tests {
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
            compression: crate::common::config::CompressionSettings { enabled: false },
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
        let connection_result = timeout(Duration::from_secs(2), server)
            .await
            .expect("connection should terminate after malformed protocol request")
            .unwrap();
        assert!(connection_result.is_err());
        assert_eq!(engine.bulk_mode_session_count(), 0);
    }
}
