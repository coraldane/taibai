use chrono::Utc;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

static QUERY_LOG_FILE: OnceLock<Mutex<File>> = OnceLock::new();

pub fn init(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let _ = QUERY_LOG_FILE.set(Mutex::new(file));
    Ok(())
}

pub fn log_query(user: &str, database: &str, sql: &str, status: &str, elapsed: Duration) {
    let Some(file) = QUERY_LOG_FILE.get() else {
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
    if let Ok(mut file) = file.lock() {
        let _ = file.write_all(line.as_bytes());
        let _ = file.flush();
    }
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
        .replace('\n', "\\n")
}
