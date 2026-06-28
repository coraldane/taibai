use futures::SinkExt;
use futures::StreamExt;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;

use crate::common::config::ServerConfig;
use crate::common::error::{Result, TaibaiError};
use crate::net::handler::Handler;
use crate::net::tls::TlsMode;
use crate::protocol::codec::MessageCodec;
use crate::sql::execution::Engine;

pub struct Server {
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    tls_acceptor: Option<TlsAcceptor>,
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
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
                    let config = self.config.clone();
                    let engine = self.engine.clone();
                    let tls_acceptor = self.tls_acceptor.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, config, engine, tls_acceptor).await {
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
) -> Result<()> {
    if let Some(acceptor) = tls_acceptor {
        tracing::debug!("Performing TLS handshake");
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| TaibaiError::Network(format!("TLS handshake failed: {}", e)))?;
        tracing::debug!("TLS handshake completed");
        handle_framed(tls_stream, config, engine).await
    } else {
        handle_framed(stream, config, engine).await
    }
}

async fn handle_framed<S>(stream: S, config: Arc<ServerConfig>, engine: Arc<Engine>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let compress_threshold = if config.compression.enabled {
        4096
    } else {
        usize::MAX
    };
    let mut framed = Framed::new(
        stream,
        MessageCodec::new(config.server.max_message_size).with_compression(compress_threshold),
    );
    let mut handler = Handler::new(config, engine.clone());

    while let Some(result) = framed.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                tracing::error!(error = %e, "decode error");
                break;
            }
        };

        tracing::debug!(msg_type = msg.msg_type, "message received");

        let responses = handler.handle_message(msg)?;
        if responses.is_empty() {
            break;
        }

        for resp in responses {
            if let Err(e) = framed.send(resp).await {
                tracing::error!(error = %e, "send error");
                return Err(e);
            }
        }
    }

    handler.flush_on_disconnect();
    tracing::info!("connection closed");
    Ok(())
}

#[cfg(test)]
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
    };

    fn test_engine() -> (TempDir, Arc<Engine>) {
        let temp_dir = TempDir::new().expect("temp dir");
        let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("engine"));
        (temp_dir, engine)
    }

    fn spawn_test_connection(
        engine: Arc<Engine>,
    ) -> (Framed<DuplexStream, MessageCodec>, JoinHandle<Result<()>>) {
        let (server_stream, client_stream) = tokio::io::duplex(1024 * 1024);
        let config = Arc::new(ServerConfig {
            compression: crate::common::config::CompressionSettings { enabled: false },
            ..ServerConfig::default()
        });
        let server = tokio::spawn(handle_framed(server_stream, config, engine));
        let client = Framed::new(client_stream, MessageCodec::new(MAX_MESSAGE_SIZE));
        (client, server)
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
}
