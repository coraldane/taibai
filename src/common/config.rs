use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::net::tls::TlsMode;

fn default_port() -> u16 {
    5151
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

fn default_batch_size() -> usize {
    4096
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_query_log_max_size_mb() -> u64 {
    1024
}

fn default_auto_compact_enabled() -> bool {
    true
}

fn default_auto_compact_check_interval_secs() -> u64 {
    60
}

fn default_auto_compact_idle_threshold_secs() -> u64 {
    30
}

fn default_auto_compact_min_segments() -> usize {
    4
}

fn default_auto_compact_min_deleted_rows() -> u64 {
    100_000
}

fn default_auto_compact_max_tables_per_run() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSettings,
    pub storage: StorageSettings,
    pub buffer: BufferSettings,
    pub compression: CompressionSettings,
    pub tls: TlsConfigYaml,
    #[serde(default)]
    pub auto_compact: AutoCompactSettings,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferSettings {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
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
pub struct AutoCompactSettings {
    #[serde(default = "default_auto_compact_enabled")]
    pub enabled: bool,
    #[serde(default = "default_auto_compact_check_interval_secs")]
    pub check_interval_secs: u64,
    #[serde(default = "default_auto_compact_idle_threshold_secs")]
    pub idle_threshold_secs: u64,
    #[serde(default = "default_auto_compact_min_segments")]
    pub min_segments: usize,
    #[serde(default = "default_auto_compact_min_deleted_rows")]
    pub min_deleted_rows: u64,
    #[serde(default = "default_auto_compact_max_tables_per_run")]
    pub max_tables_per_run: usize,
}

impl Default for AutoCompactSettings {
    fn default() -> Self {
        Self {
            enabled: default_auto_compact_enabled(),
            check_interval_secs: default_auto_compact_check_interval_secs(),
            idle_threshold_secs: default_auto_compact_idle_threshold_secs(),
            min_segments: default_auto_compact_min_segments(),
            min_deleted_rows: default_auto_compact_min_deleted_rows(),
            max_tables_per_run: default_auto_compact_max_tables_per_run(),
        }
    }
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
            },
            buffer: BufferSettings { batch_size: 4096 },
            compression: CompressionSettings { enabled: true },
            tls: TlsConfigYaml::default(),
            auto_compact: AutoCompactSettings::default(),
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
