use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use monax::common::error::{MonaxError, Result};
use monax::protocol::codec::MessageCodec;
use monax::protocol::message::*;

pub struct BulkClient {
    framed: Framed<TcpStream, MessageCodec>,
    default_timeout: Duration,
}

impl BulkClient {
    pub async fn connect<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let framed = Framed::new(
            stream,
            MessageCodec::new(MAX_MESSAGE_SIZE).with_compression(4096),
        );
        Ok(Self {
            framed,
            default_timeout: Duration::from_secs(30),
        })
    }

    pub async fn login(&mut self, username: &str, password: &str, database: &str) -> Result<()> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, username);
        write_len_prefixed_string(&mut payload, password);
        write_len_prefixed_string(&mut payload, database);
        payload.push(0);

        self.send(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_AUTH_OK => Ok(()),
            MSG_TYPE_AUTH_FAIL => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(MonaxError::Auth(msg))
            }
            other => Err(MonaxError::Protocol(format!(
                "unexpected login response type: {other:#04X}"
            ))),
        }
    }

    pub async fn send_recv(&mut self, msg: Message) -> Result<Message> {
        self.send(msg).await?;
        self.recv().await
    }

    pub async fn logout(&mut self) -> Result<()> {
        self.send(Message::empty(MSG_TYPE_LOGOUT)).await?;
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<u64> {
        self.send(Message::empty(MSG_TYPE_FLUSH)).await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() >= 8 {
                    Ok(u64::from_le_bytes(resp.payload[0..8].try_into().unwrap()))
                } else {
                    Ok(0)
                }
            }
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(MonaxError::Protocol(msg))
            }
            other => Err(MonaxError::Protocol(format!(
                "unexpected flush response type: {other:#04X}"
            ))),
        }
    }

    pub async fn bulk_insert_columnar(&mut self, payload: Bytes) -> Result<u64> {
        self.send(Message::new(MSG_TYPE_BULK_INSERT_COLUMNAR, payload)).await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() >= 8 {
                    Ok(u64::from_le_bytes(resp.payload[0..8].try_into().unwrap()))
                } else {
                    Ok(0)
                }
            }
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(MonaxError::Protocol(msg))
            }
            other => Err(MonaxError::Protocol(format!(
                "unexpected response type: {other:#04X}"
            ))),
        }
    }

    pub async fn close(mut self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let _ = self.framed.into_inner().shutdown().await;
        Ok(())
    }

    async fn send(&mut self, msg: Message) -> Result<()> {
        use tokio::time::timeout;
        timeout(self.default_timeout, self.framed.send(msg))
            .await
            .map_err(|_| MonaxError::Network("send timeout".into()))?
            .map_err(|e| MonaxError::Network(format!("send error: {}", e)))
    }

    async fn recv(&mut self) -> Result<Message> {
        use tokio::time::timeout;
        timeout(self.default_timeout, self.framed.next())
            .await
            .map_err(|_| MonaxError::Network("recv timeout".into()))?
            .ok_or_else(|| MonaxError::Network("connection closed".into()))?
            .map_err(|e| MonaxError::Network(format!("recv error: {}", e)))
    }
}