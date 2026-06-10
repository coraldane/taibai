use std::fmt;

#[derive(Debug)]
pub enum TaibaiError {
    Io(std::io::Error),
    Protocol(String),
    Auth(String),
    Config(String),
    Storage(String),
    Sql(String),
    Internal(String),
    Network(String),
}

pub type Result<T> = std::result::Result<T, TaibaiError>;

impl fmt::Display for TaibaiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaibaiError::Io(e) => write!(f, "I/O error: {e}"),
            TaibaiError::Protocol(msg) => write!(f, "protocol error: {msg}"),
            TaibaiError::Auth(msg) => write!(f, "auth error: {msg}"),
            TaibaiError::Config(msg) => write!(f, "config error: {msg}"),
            TaibaiError::Storage(msg) => write!(f, "storage error: {msg}"),
            TaibaiError::Sql(msg) => write!(f, "SQL error: {msg}"),
            TaibaiError::Internal(msg) => write!(f, "internal error: {msg}"),
            TaibaiError::Network(msg) => write!(f, "network error: {msg}"),
        }
    }
}

impl std::error::Error for TaibaiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TaibaiError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TaibaiError {
    fn from(e: std::io::Error) -> Self {
        TaibaiError::Io(e)
    }
}

impl From<tokio::sync::AcquireError> for TaibaiError {
    fn from(e: tokio::sync::AcquireError) -> Self {
        TaibaiError::Internal(format!("semaphore error: {}", e))
    }
}

impl From<TaibaiError> for std::io::Error {
    fn from(e: TaibaiError) -> Self {
        match e {
            TaibaiError::Io(io) => io,
            other => std::io::Error::other(other.to_string()),
        }
    }
}
