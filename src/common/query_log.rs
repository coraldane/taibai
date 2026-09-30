use chrono::Utc;
use crossbeam_channel::{bounded, Sender};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

const QUERY_LOG_QUEUE_CAPACITY: usize = 8192;

static QUERY_LOG_SENDER: OnceLock<Sender<Vec<u8>>> = OnceLock::new();

struct QueryLogState {
    path: PathBuf,
    file: Option<File>,
    max_bytes: u64,
}

pub fn init(path: &Path, max_size_mb: u64) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let max_bytes = max_size_mb.saturating_mul(1024 * 1024);
    if should_rotate(path, max_bytes, 0) {
        rotate_path(path)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let mut state = QueryLogState {
        path: path.to_path_buf(),
        file: Some(file),
        max_bytes,
    };
    let (sender, receiver) = bounded::<Vec<u8>>(QUERY_LOG_QUEUE_CAPACITY);
    std::thread::Builder::new()
        .name("taibai-query-log".into())
        .spawn(move || {
            while let Ok(line) = receiver.recv() {
                if let Err(error) = state.write(&line) {
                    eprintln!("taibai query log write failed: {error}");
                }
            }
        })?;
    QUERY_LOG_SENDER.set(sender).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "query log is already initialized",
        )
    })?;
    Ok(())
}

/// True when the query log is configured. Callers use this to skip
/// log-related statement analysis (e.g. the extra SQL parse in the request
/// handler) when the feature is off.
pub fn enabled() -> bool {
    QUERY_LOG_SENDER.get().is_some()
}

pub fn log_query(user: &str, database: &str, sql: &str, status: &str, elapsed: Duration) {
    let Some(sender) = QUERY_LOG_SENDER.get() else {
        return;
    };
    let line = format!(
        "{}\tuser={}\tdatabase={}\tstatus={}\telapsed_ms={}\tsql={}\n",
        Utc::now().to_rfc3339(),
        escape_field(user),
        escape_field(database),
        escape_field(status),
        elapsed.as_millis(),
        escape_field(sql)
    );
    // Query logging is diagnostic and must never apply backpressure to query
    // execution. If the disk writer falls behind, drop the log entry instead
    // of blocking a Tokio or request-worker thread.
    let _ = sender.try_send(line.into_bytes());
}

impl QueryLogState {
    fn write(&mut self, line: &[u8]) -> std::io::Result<()> {
        if should_rotate(&self.path, self.max_bytes, line.len() as u64) {
            self.rotate()?;
        }
        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&self.path)?,
            );
        }
        if let Some(file) = self.file.as_mut() {
            file.write_all(line)?;
            file.flush()?;
        }
        Ok(())
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        if let Some(mut file) = self.file.take() {
            let _ = file.flush();
        }
        rotate_path(&self.path)?;
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        Ok(())
    }
}

fn should_rotate(path: &Path, max_bytes: u64, incoming_bytes: u64) -> bool {
    if max_bytes == 0 {
        return false;
    }
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    meta.len().saturating_add(incoming_bytes) > max_bytes
}

fn rotate_path(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let backup = rotated_path(path);
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    std::fs::rename(path, backup)
}

fn rotated_path(path: &Path) -> PathBuf {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
        return path.with_extension("1");
    };
    path.with_file_name(format!("{}.1", file_name))
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}
