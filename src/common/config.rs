use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::net::tls::TlsMode;

fn default_port() -> u16 {
    5151
}

/// Process-global runtime limits consulted by query operators (aggregation,
/// streaming scans, ...). Populated once at startup from `BufferSettings` via
/// [`set_query_limits`]. Operators read via [`query_limits`]; before startup
/// the [`QueryLimits::default`] values apply.
#[derive(Debug, Clone, Copy)]
pub struct QueryLimits {
    /// Soft per-query memory ceiling in MB. Surfaced to operators so future
    /// streaming-scan enforcement can consult it; currently the
    /// `max_aggregate_groups` cap is the primary OOM safeguard for grouped
    /// aggregation, while no-GROUP-BY aggregation is already O(1) memory.
    #[allow(dead_code)]
    pub query_memory_limit_mb: u64,
    pub max_aggregate_groups: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            query_memory_limit_mb: default_query_memory_limit_mb(),
            max_aggregate_groups: default_max_aggregate_groups(),
        }
    }
}

static QUERY_LIMITS: OnceLock<QueryLimits> = OnceLock::new();

/// Install the process-wide query limits. Called once during startup.
/// Returns an error if called more than once (limits are immutable thereafter).
pub fn set_query_limits(limits: QueryLimits) -> Result<(), QueryLimits> {
    QUERY_LIMITS.set(limits)
}

/// Read the process-wide query limits (defaults if unset).
pub fn query_limits() -> QueryLimits {
    *QUERY_LIMITS.get().unwrap_or(&QueryLimits {
        query_memory_limit_mb: default_query_memory_limit_mb(),
        max_aggregate_groups: default_max_aggregate_groups(),
    })
}

fn default_max_message_size() -> usize {
    64 * 1024 * 1024
}

fn default_data_dir() -> String {
    "./data".to_string()
}

fn default_database() -> String {
    "main".to_string()
}

fn default_flush_threshold_rows() -> u32 {
    65536
}

fn default_min_segment_seal_size_mb() -> u32 {
    500
}

fn default_max_segment_size_mb() -> u32 {
    2048
}

fn default_delete_cleanup_enabled() -> bool {
    true
}

fn default_delete_cleanup_min_deleted_ratio() -> f64 {
    0.30
}

fn default_delete_cleanup_min_deleted_rows() -> u64 {
    8192
}

fn default_delete_cleanup_interval_ms() -> u64 {
    60_000
}

fn default_delete_bitmap_cleanup_interval_secs() -> Option<u64> {
    None
}

fn default_delete_cleanup_max_segments_per_run() -> usize {
    8
}

fn default_hot_store_memory_limit_mb() -> u64 {
    8192
}

fn default_batch_size() -> usize {
    4096
}

fn default_query_memory_limit_mb() -> u64 {
    4096
}

fn default_max_aggregate_groups() -> usize {
    1_000_000
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_query_log_max_size_mb() -> u64 {
    1024
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSettings,
    pub storage: StorageSettings,
    pub buffer: BufferSettings,
    pub compression: CompressionSettings,
    pub tls: TlsConfigYaml,
    pub log: LogSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_database")]
    pub default_database: String,
    #[serde(default = "default_flush_threshold_rows")]
    pub flush_threshold_rows: u32,
    #[serde(default = "default_min_segment_seal_size_mb")]
    pub min_segment_seal_size_mb: u32,
    #[serde(default = "default_max_segment_size_mb")]
    pub max_segment_size_mb: u32,
    #[serde(default = "default_delete_cleanup_enabled")]
    pub delete_cleanup_enabled: bool,
    #[serde(default = "default_delete_cleanup_min_deleted_ratio")]
    pub delete_cleanup_min_deleted_ratio: f64,
    #[serde(default = "default_delete_cleanup_min_deleted_rows")]
    pub delete_cleanup_min_deleted_rows: u64,
    #[serde(default = "default_delete_cleanup_interval_ms")]
    pub delete_cleanup_interval_ms: u64,
    #[serde(default = "default_delete_bitmap_cleanup_interval_secs")]
    pub delete_bitmap_cleanup_interval_secs: Option<u64>,
    #[serde(default = "default_delete_cleanup_max_segments_per_run")]
    pub delete_cleanup_max_segments_per_run: usize,
    #[serde(default = "default_hot_store_memory_limit_mb")]
    pub hot_store_memory_limit_mb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferSettings {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Soft per-query memory ceiling in MB. Aggregation/streaming operators
    /// abort the query (rather than OOM-crash the process) when an internal
    /// estimate of rows-since-start times average row width crosses this.
    #[serde(default = "default_query_memory_limit_mb")]
    pub query_memory_limit_mb: u64,
    /// Maximum number of GROUP BY distinct keys before a grouped aggregate
    /// errors out. Caps memory for high-cardinality GROUP BY queries.
    #[serde(default = "default_max_aggregate_groups")]
    pub max_aggregate_groups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionSettings {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfigYaml {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSettings {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub query_file: Option<String>,
    #[serde(default = "default_query_log_max_size_mb")]
    pub query_max_size_mb: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: ServerSettings {
                host: "0.0.0.0".to_string(),
                port: 5151,
                max_message_size: 64 * 1024 * 1024,
            },
            storage: StorageSettings {
                data_dir: "./data".to_string(),
                default_database: "main".to_string(),
                flush_threshold_rows: 65536,
                min_segment_seal_size_mb: 500,
                max_segment_size_mb: 2048,
                delete_cleanup_enabled: default_delete_cleanup_enabled(),
                delete_cleanup_min_deleted_ratio: default_delete_cleanup_min_deleted_ratio(),
                delete_cleanup_min_deleted_rows: default_delete_cleanup_min_deleted_rows(),
                delete_cleanup_interval_ms: default_delete_cleanup_interval_ms(),
                delete_bitmap_cleanup_interval_secs: default_delete_bitmap_cleanup_interval_secs(),
                delete_cleanup_max_segments_per_run: default_delete_cleanup_max_segments_per_run(),
                hot_store_memory_limit_mb: default_hot_store_memory_limit_mb(),
            },
            buffer: BufferSettings {
                batch_size: 4096,
                query_memory_limit_mb: default_query_memory_limit_mb(),
                max_aggregate_groups: default_max_aggregate_groups(),
            },
            compression: CompressionSettings { enabled: true },
            tls: TlsConfigYaml::default(),
            log: LogSettings {
                level: "info".to_string(),
                file: None,
                query_file: None,
                query_max_size_mb: default_query_log_max_size_mb(),
            },
        }
    }
}

impl ServerConfig {
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    pub fn from_yaml_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file {}: {}", path.display(), e))?;

        let mut config: ServerConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse config file {}: {}", path.display(), e))?;

        config.resolve_relative_paths(path);
        Ok(config)
    }

    fn resolve_relative_paths(&mut self, config_path: &Path) {
        let Some(config_dir) = config_path.parent() else {
            return;
        };

        self.storage.data_dir = resolve_config_relative(config_dir, &self.storage.data_dir);
        if let Some(log_file) = self.log.file.as_mut() {
            *log_file = resolve_config_relative(config_dir, log_file);
        }
        if let Some(query_file) = self.log.query_file.as_mut() {
            *query_file = resolve_config_relative(config_dir, query_file);
        } else if let Some(log_file) = self.log.file.as_deref() {
            self.log.query_file = Some(default_query_log_file(log_file));
        }
    }

    pub fn tls_mode(&self) -> TlsMode {
        if self.tls.enabled && !self.tls.cert_path.is_empty() && !self.tls.key_path.is_empty() {
            TlsMode::Enable(crate::net::tls::TlsConfig::new(
                self.tls.cert_path.clone(),
                self.tls.key_path.clone(),
            ))
        } else {
            TlsMode::Disable
        }
    }
}

fn default_query_log_file(log_file: &str) -> String {
    let path = Path::new(log_file);
    path.parent()
        .unwrap_or_else(|| Path::new(""))
        .join("taibai-query.log")
        .to_string_lossy()
        .to_string()
}

fn resolve_config_relative(config_dir: &Path, value: &str) -> String {
    let path = Path::new(value);
    if path.is_absolute() {
        return value.to_string();
    }

    normalize_path(config_dir.join(path))
        .to_string_lossy()
        .to_string()
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

#[derive(Parser, Debug)]
#[command(name = "taibai", about = "A columnar database server", version)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(long, default_value = "conf/config.yaml")]
    config: String,

    #[arg(long)]
    addr: Option<String>,

    #[arg(long)]
    data_dir: Option<String>,

    #[arg(long)]
    database: Option<String>,

    #[arg(long)]
    tls_cert: Option<String>,

    #[arg(long)]
    tls_key: Option<String>,

    #[arg(long)]
    no_compression: bool,

    #[arg(long)]
    service: bool,

    #[arg(long)]
    install_service: bool,

    #[arg(long)]
    uninstall_service: bool,

    #[arg(long, default_value = "TaibaiDB")]
    service_name: String,

    #[arg(long, default_value = "Taibai Database")]
    service_display_name: String,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    Client {
        #[arg(short = 'H', long, default_value = "127.0.0.1:5151")]
        host: String,

        #[arg(short, long, default_value = "root")]
        user: String,

        #[arg(short, long, default_value = "taibai")]
        password: String,

        #[arg(short, long, default_value = "main")]
        database: String,

        #[arg(short, long)]
        query: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum RunMode {
    Server(ServerConfig),
    Service {
        service_name: String,
        config_path: String,
        config: ServerConfig,
    },
    InstallService {
        service_name: String,
        display_name: String,
        config_path: String,
    },
    UninstallService {
        service_name: String,
    },
    Client {
        host: String,
        user: String,
        password: String,
        database: String,
        query: Option<String>,
    },
}

impl RunMode {
    pub fn log_level(&self) -> &str {
        match self {
            RunMode::Server(config) | RunMode::Service { config, .. } => &config.log.level,
            RunMode::InstallService { .. }
            | RunMode::UninstallService { .. }
            | RunMode::Client { .. } => "info",
        }
    }

    pub fn from_args() -> Self {
        let args = Args::parse();

        if let Some(Commands::Client {
            host,
            user,
            password,
            database,
            query,
        }) = args.command
        {
            return RunMode::Client {
                host,
                user,
                password,
                database,
                query,
            };
        }

        if args.uninstall_service {
            return RunMode::UninstallService {
                service_name: args.service_name,
            };
        }

        let config_path = args.config.clone();

        let mut config = Path::new(&args.config)
            .exists()
            .then(|| ServerConfig::from_yaml_file(Path::new(&args.config)))
            .transpose()
            .unwrap_or_else(|e| {
                tracing::warn!("Config load error: {}, using defaults", e);
                Some(ServerConfig::default())
            })
            .unwrap_or_default();

        if let Some(addr) = args.addr {
            let parts: Vec<&str> = addr.rsplitn(2, ':').collect();
            if parts.len() == 2 {
                config.server.host = parts[1].to_string();
                config.server.port = parts[0].parse().unwrap_or(5151);
            } else {
                config.server.host = addr;
            }
        }
        if let Some(data_dir) = args.data_dir {
            config.storage.data_dir = data_dir;
        }
        if let Some(database) = args.database {
            config.storage.default_database = database;
        }

        if let (Some(cert), Some(key)) = (args.tls_cert, args.tls_key) {
            config.tls = TlsConfigYaml {
                enabled: true,
                cert_path: cert,
                key_path: key,
            };
        }

        if args.no_compression {
            config.compression.enabled = false;
        }

        if args.install_service {
            return RunMode::InstallService {
                service_name: args.service_name,
                display_name: args.service_display_name,
                config_path,
            };
        }

        if args.service {
            return RunMode::Service {
                service_name: args.service_name,
                config_path,
                config,
            };
        }

        RunMode::Server(config)
    }
}

#[cfg(test)]
mod tests {
    use super::ServerConfig;

    #[test]
    fn config_relative_storage_and_log_paths_resolve_from_config_file() {
        let temp = tempfile::tempdir().unwrap();
        let conf_dir = temp.path().join("conf");
        std::fs::create_dir_all(&conf_dir).unwrap();
        let config_path = conf_dir.join("windows.yaml");
        std::fs::write(
            &config_path,
            r#"
server:
  host: "0.0.0.0"
  port: 5151
storage:
  data_dir: "../data"
  default_database: "main"
buffer:
  batch_size: 4096
compression:
  enabled: true
tls:
  enabled: false
  cert_path: ""
  key_path: ""
log:
  level: "info"
  file: "../logs/taibai.log"
"#,
        )
        .unwrap();

        let config = ServerConfig::from_yaml_file(&config_path).unwrap();
        let expected_data_dir = temp.path().join("data").to_string_lossy().to_string();
        let expected_log_file = temp
            .path()
            .join("logs")
            .join("taibai.log")
            .to_string_lossy()
            .to_string();
        let expected_query_file = temp
            .path()
            .join("logs")
            .join("taibai-query.log")
            .to_string_lossy()
            .to_string();

        assert_eq!(config.storage.data_dir, expected_data_dir);
        assert_eq!(config.log.file.as_deref(), Some(expected_log_file.as_str()));
        assert_eq!(
            config.log.query_file.as_deref(),
            Some(expected_query_file.as_str())
        );
    }
}
