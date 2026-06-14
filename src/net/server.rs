use futures::SinkExt;
use futures::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;

use crate::common::config::ServerConfig;
use crate::common::error::{Result, TaibaiError};
use crate::net::handler::{build_error, Handler};
use crate::net::tls::TlsMode;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{
    Message, MSG_TYPE_CHANGE_ACK, MSG_TYPE_CHANGE_ACK_STATE, MSG_TYPE_CHANGE_EVENT,
    MSG_TYPE_CHANGE_SUBSCRIBE,
};
use crate::sql::execution::Engine;
use crate::storage::catalog::auth::{PermissionObjectType, PermissionType};
use crate::storage::change_stream::ChangeEvent;

#[derive(Debug, Default, Deserialize)]
struct ChangeSubscribeRequest {
    database: Option<String>,
    table: Option<String>,
    include_recent: Option<bool>,
    from_sequence: Option<u64>,
    consumer_group: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChangeAckRequest {
    group: String,
    sequence: u64,
}

pub struct Server {
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Server {
    pub fn new(config: Arc<ServerConfig>, engine: Arc<Engine>) -> Self {
        let tls_acceptor = match config.tls_mode() {
            TlsMode::Enable(tls_config) => match tls_config.validate() {
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
            },
            TlsMode::Disable => None,
        };

        Self {
            config,
            engine,
            tls_acceptor,
        }
    }

    pub async fn run(&self) -> Result<()> {
        self.run_with_shutdown(std::future::pending::<()>()).await
    }

    pub async fn run_with_shutdown<S>(&self, shutdown: S) -> Result<()>
    where
        S: std::future::Future<Output = ()>,
    {
        tokio::pin!(shutdown);
        let listener = TcpListener::bind(&self.config.listen_addr()).await?;
        tracing::info!("taibai listening on {}", self.config.listen_addr());

        if self.tls_acceptor.is_some() {
            tracing::info!("TLS handshake will be performed for incoming connections");
        }

        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = result?;
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
                _ = &mut shutdown => {
                    tracing::info!("server shutdown requested");
                    return Ok(());
                }
            }
        }
    }
}

fn parse_change_subscribe_request(payload: &[u8]) -> Result<ChangeSubscribeRequest> {
    if payload.is_empty() {
        return Ok(ChangeSubscribeRequest::default());
    }
    serde_json::from_slice(payload)
        .map_err(|e| TaibaiError::Protocol(format!("invalid change subscribe request: {}", e)))
}

fn change_event_matches(event: &ChangeEvent, database: Option<&str>, table: Option<&str>) -> bool {
    database.is_none_or(|db| event.database == db)
        && table.is_none_or(|table_name| event.table == table_name)
}

fn build_change_event(event: &ChangeEvent) -> Result<Message> {
    let payload = serde_json::to_vec(event)
        .map_err(|e| TaibaiError::Protocol(format!("failed to encode change event: {}", e)))?;
    Ok(Message::new(
        MSG_TYPE_CHANGE_EVENT,
        bytes::Bytes::from(payload),
    ))
}

fn parse_change_ack_request(payload: &[u8]) -> Result<ChangeAckRequest> {
    serde_json::from_slice(payload)
        .map_err(|e| TaibaiError::Protocol(format!("invalid change ack request: {}", e)))
}

fn build_change_ack_state(group: &str, sequence: u64) -> Result<Message> {
    let payload = serde_json::json!({
        "group": group,
        "sequence": sequence,
    });
    let payload = serde_json::to_vec(&payload)
        .map_err(|e| TaibaiError::Protocol(format!("failed to encode change ack state: {}", e)))?;
    Ok(Message::new(
        MSG_TYPE_CHANGE_ACK_STATE,
        bytes::Bytes::from(payload),
    ))
}

fn authorize_change_subscription(
    engine: &Engine,
    session: &crate::net::session::Session,
    request: &mut ChangeSubscribeRequest,
) -> Result<()> {
    if request
        .table
        .as_ref()
        .is_some_and(|table| !table.is_empty())
        && request.database.is_none()
    {
        request.database = Some(session.database.clone());
    }

    match (
        request.database.as_ref().filter(|db| !db.is_empty()),
        request.table.as_ref().filter(|table| !table.is_empty()),
    ) {
        (Some(db), Some(table)) => require_change_permission(
            engine,
            session,
            PermissionObjectType::Table,
            &format!("{}.{}", db, table),
        ),
        (Some(db), None) => {
            require_change_permission(engine, session, PermissionObjectType::Database, db)
        }
        (None, None) => {
            require_change_permission(engine, session, PermissionObjectType::Global, "*")
        }
        (None, Some(_)) => unreachable!(),
    }
}

fn require_change_permission(
    engine: &Engine,
    session: &crate::net::session::Session,
    object_type: PermissionObjectType,
    object_name: &str,
) -> Result<()> {
    if engine.auth_manager().check_permission(
        &session.username,
        PermissionType::Subscribe,
        object_type.clone(),
        object_name,
    ) {
        return Ok(());
    }
    Err(TaibaiError::Sql(format!(
        "access denied for user '{}' to subscribe changes on {:?} '{}'",
        session.username, object_type, object_name
    )))
}

async fn stream_changes<S>(
    mut framed: Framed<S, MessageCodec>,
    engine: Arc<Engine>,
    request: ChangeSubscribeRequest,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let database = request.database.filter(|s| !s.is_empty());
    let table = request.table.filter(|s| !s.is_empty());
    let from_sequence = request.from_sequence.or_else(|| {
        request
            .consumer_group
            .as_deref()
            .map(|group| engine.change_consumer_sequence(group) + 1)
            .filter(|sequence| *sequence > 1)
    });
    let mut rx = engine.subscribe_changes();
    let replay = if let Some(sequence) = from_sequence {
        match engine.changes_since(sequence, database.as_deref(), table.as_deref()) {
            Ok(events) => events,
            Err(e) => {
                framed.send(build_error(&e.to_string())).await?;
                return Ok(());
            }
        }
    } else if request.include_recent.unwrap_or(false) {
        engine.recent_changes(database.as_deref(), table.as_deref())
    } else {
        Vec::new()
    };
    let mut last_replayed_sequence = 0;

    for event in replay {
        last_replayed_sequence = last_replayed_sequence.max(event.sequence);
        framed.send(build_change_event(&event)?).await?;
    }

    loop {
        match rx.recv().await {
            Ok(event) => {
                if event.sequence > last_replayed_sequence
                    && change_event_matches(&event, database.as_deref(), table.as_deref())
                {
                    framed.send(build_change_event(&event)?).await?;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                framed
                    .send(build_error(&format!(
                        "change stream lagged, skipped {} events",
                        skipped
                    )))
                    .await?;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
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

        if msg.msg_type == MSG_TYPE_CHANGE_ACK {
            if handler.session.is_none() {
                framed.send(build_error("not logged in")).await?;
                continue;
            }
            let ack = match parse_change_ack_request(&msg.payload) {
                Ok(ack) => ack,
                Err(e) => {
                    framed.send(build_error(&e.to_string())).await?;
                    continue;
                }
            };
            let session = handler.session.as_mut().unwrap();
            match engine.ack_change_consumer_for_session(session, &ack.group, ack.sequence) {
                Ok(sequence) => {
                    framed
                        .send(build_change_ack_state(&ack.group, sequence)?)
                        .await?
                }
                Err(e) => framed.send(build_error(&e.to_string())).await?,
            }
            continue;
        }

        if msg.msg_type == MSG_TYPE_CHANGE_SUBSCRIBE {
            if handler.session.is_none() {
                framed.send(build_error("not logged in")).await?;
                continue;
            }
            let mut request = match parse_change_subscribe_request(&msg.payload) {
                Ok(request) => request,
                Err(e) => {
                    framed.send(build_error(&e.to_string())).await?;
                    continue;
                }
            };
            if let Err(e) = authorize_change_subscription(
                &engine,
                handler.session.as_ref().unwrap(),
                &mut request,
            ) {
                framed.send(build_error(&e.to_string())).await?;
                continue;
            }
            return stream_changes(framed, engine.clone(), request).await;
        }

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
    use tokio::time::{timeout, Duration};

    use crate::net::session::Session;
    use crate::protocol::message::{
        write_len_prefixed_string, MAX_MESSAGE_SIZE, MSG_TYPE_AUTH_OK, MSG_TYPE_ERROR,
        MSG_TYPE_LOGIN,
    };
    use crate::storage::change_stream::{ChangeEvent, ChangeOperation};

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

    async fn recv_change_event(framed: &mut Framed<DuplexStream, MessageCodec>) -> ChangeEvent {
        let msg = timeout(Duration::from_secs(2), framed.next())
            .await
            .expect("change event timeout")
            .expect("change event response")
            .expect("change event frame");
        assert_eq!(msg.msg_type, MSG_TYPE_CHANGE_EVENT);
        serde_json::from_slice(&msg.payload).expect("change event payload")
    }

    #[tokio::test]
    async fn change_subscription_replays_recent_and_streams_live_events() {
        let (_temp_dir, engine) = test_engine();
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO t VALUES (1)")
            .expect("insert first row");

        let (mut framed, server) = spawn_test_connection(engine.clone());
        login(&mut framed, "root").await;
        let payload = serde_json::json!({
            "database": "main",
            "table": "t",
            "include_recent": true,
        });
        framed
            .send(Message::new(
                MSG_TYPE_CHANGE_SUBSCRIBE,
                Bytes::from(serde_json::to_vec(&payload).unwrap()),
            ))
            .await
            .expect("send subscribe");

        let first = recv_change_event(&mut framed).await;
        let second = recv_change_event(&mut framed).await;
        assert_eq!(first.operation, ChangeOperation::CreateTable);
        assert_eq!(second.operation, ChangeOperation::Insert);

        engine
            .execute(&mut session, "INSERT INTO t VALUES (2)")
            .expect("insert second row");
        let live = recv_change_event(&mut framed).await;
        assert_eq!(live.operation, ChangeOperation::Insert);
        assert_eq!(live.rows_affected, 1);

        server.abort();
    }

    #[tokio::test]
    async fn change_consumer_ack_controls_group_replay_start() {
        let (_temp_dir, engine) = test_engine();
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO t VALUES (1)")
            .expect("insert first");
        engine
            .execute(&mut session, "INSERT INTO t VALUES (2)")
            .expect("insert second");

        let events = engine
            .change_events(None, None, Some("main"), Some("t"))
            .expect("events");
        let first_insert_sequence = events
            .iter()
            .find(|event| event.operation == ChangeOperation::Insert)
            .expect("first insert")
            .sequence;
        let second_insert_sequence = events
            .iter()
            .filter(|event| event.operation == ChangeOperation::Insert)
            .nth(1)
            .expect("second insert")
            .sequence;

        let (mut ack_framed, ack_server) = spawn_test_connection(engine.clone());
        login(&mut ack_framed, "root").await;
        let ack = serde_json::json!({
            "group": "etl",
            "sequence": first_insert_sequence,
        });
        ack_framed
            .send(Message::new(
                MSG_TYPE_CHANGE_ACK,
                Bytes::from(serde_json::to_vec(&ack).unwrap()),
            ))
            .await
            .expect("send ack");
        let ack_resp = ack_framed
            .next()
            .await
            .expect("ack response")
            .expect("ack frame");
        assert_eq!(ack_resp.msg_type, MSG_TYPE_CHANGE_ACK_STATE);
        drop(ack_framed);
        ack_server.abort();

        let (mut sub_framed, sub_server) = spawn_test_connection(engine.clone());
        login(&mut sub_framed, "root").await;
        let subscribe = serde_json::json!({
            "database": "main",
            "table": "t",
            "consumer_group": "etl",
        });
        sub_framed
            .send(Message::new(
                MSG_TYPE_CHANGE_SUBSCRIBE,
                Bytes::from(serde_json::to_vec(&subscribe).unwrap()),
            ))
            .await
            .expect("send subscribe");
        let replayed = recv_change_event(&mut sub_framed).await;
        assert_eq!(replayed.sequence, second_insert_sequence);
        assert_eq!(replayed.operation, ChangeOperation::Insert);

        sub_server.abort();
    }

    #[tokio::test]
    async fn change_subscription_requires_subscribe_permission() {
        let (_temp_dir, engine) = test_engine();
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE USER alice IDENTIFIED BY 'pw'")
            .expect("create user");

        let (mut framed, server) = spawn_test_connection(engine.clone());
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "alice");
        write_len_prefixed_string(&mut payload, "pw");
        write_len_prefixed_string(&mut payload, "main");
        framed
            .send(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
            .await
            .expect("send login");
        let login_resp = framed
            .next()
            .await
            .expect("login response")
            .expect("login frame");
        assert_eq!(login_resp.msg_type, MSG_TYPE_AUTH_OK);

        let subscribe = serde_json::json!({
            "database": "main",
            "table": "t",
        });
        framed
            .send(Message::new(
                MSG_TYPE_CHANGE_SUBSCRIBE,
                Bytes::from(serde_json::to_vec(&subscribe).unwrap()),
            ))
            .await
            .expect("send subscribe");
        let resp = framed
            .next()
            .await
            .expect("subscribe response")
            .expect("subscribe frame");
        assert_eq!(resp.msg_type, MSG_TYPE_ERROR);
        assert!(String::from_utf8_lossy(&resp.payload).contains("access denied"));

        server.abort();
    }
}
