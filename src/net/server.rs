use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::StreamExt;
use futures::SinkExt;
use tokio_rustls::TlsAcceptor;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::common::config::ServerConfig;
use crate::common::error::{MonaxError, Result};
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
            TlsMode::Enable(tls_config) => {
                match tls_config.validate() {
                    Ok(()) => {
                        tracing::info!(
                            "TLS enabled: cert={:?}, key={:?}",
                            tls_config.cert_path,
                            tls_config.key_path
                        );
                        match tls_config.load_server_config() {
                            Ok(server_config) => {
                                tracing::info!("TLS configuration loaded successfully");
                                Some(TlsAcceptor::from(server_config))
                            }
                            Err(e) => {
                                tracing::error!("Failed to load TLS configuration: {}", e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("TLS configuration invalid: {}", e);
                        None
                    }
                }
            }
            TlsMode::Disable => None,
        };

        Self { config, engine, tls_acceptor }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.config.listen_addr()).await?;
        tracing::info!("monax listening on {}", self.config.listen_addr());

        if self.tls_acceptor.is_some() {
            tracing::info!("TLS handshake will be performed for incoming connections");
        }

        loop {
            let (stream, addr) = listener.accept().await?;
            let config = self.config.clone();
            let engine = self.engine.clone();
            let tls_acceptor = self.tls_acceptor.clone();
            tracing::debug!(peer = %addr, "connection accepted");

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, config, engine, tls_acceptor).await {
                    tracing::error!(peer = %addr, error = %e, "connection error");
                }
            });
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
        let tls_stream = acceptor.accept(stream).await
            .map_err(|e| MonaxError::Network(
                format!("TLS handshake failed: {}", e)
            ))?;
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
    let compress_threshold = if config.compression.enabled { 4096 } else { usize::MAX };
    let mut framed = Framed::new(
        stream,
        MessageCodec::new(config.server.max_message_size).with_compression(compress_threshold),
    );
    let mut handler = Handler::new(config, engine);

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
