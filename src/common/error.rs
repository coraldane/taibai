use std::fmt;

#[derive(Debug)]
pub enum MonaxError {
    Io(std::io::Error),
    Protocol(String),
    Auth(String),
    Config(String),
    Storage(String),
    Sql(String),
    Internal(String),
    Network(String),
}

pub type Result<T> = std::result::Result<T, MonaxError>;

impl fmt::Display for MonaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MonaxError::Io(e) => write!(f, "I/O error: {e}"),
            MonaxError::Protocol(msg) => write!(f, "protocol error: {msg}"),
            MonaxError::Auth(msg) => write!(f, "auth error: {msg}"),
            MonaxError::Config(msg) => write!(f, "config error: {msg}"),
            MonaxError::Storage(msg) => write!(f, "storage error: {msg}"),
            MonaxError::Sql(msg) => write!(f, "SQL error: {msg}"),
            MonaxError::Internal(msg) => write!(f, "internal error: {msg}"),
            MonaxError::Network(msg) => write!(f, "network error: {msg}"),
        }
    }
}

impl std::error::Error for MonaxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MonaxError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for MonaxError {
    fn from(e: std::io::Error) -> Self {
        MonaxError::Io(e)
    }
}

impl From<tokio::sync::AcquireError> for MonaxError {
    fn from(e: tokio::sync::AcquireError) -> Self {
        MonaxError::Internal(format!("semaphore error: {}", e))
    }
}

impl From<MonaxError> for std::io::Error {
    fn from(e: MonaxError) -> Self {
        match e {
            MonaxError::Io(io) => io,
            other => std::io::Error::new(std::io::ErrorKind::Other, other.to_string()),
        }
    }
}