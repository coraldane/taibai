use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::net::tls::TlsMode;

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub data_dir: String,
    pub default_database: String,
    pub max_message_size: usize,
    pub flush_threshold_rows: u32,
    pub batch_size: usize,
    /// TLS configuration
    pub tls: TlsConfigYaml,
    /// Whether to enable protocol compression.
    pub enable_compression: bool,
}

/// TLS configuration for YAML
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TlsConfigYaml {
    pub enabled: bool,
    pub cert_path: String,
    pub key_path: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:5151".to_string(),
            data_dir: "./data".to_string(),
            default_database: "main".to_string(),
            max_message_size: 64 * 1024 * 1024, // 64MB
            flush_threshold_rows: 65536,
            batch_size: 4096,
            tls: TlsConfigYaml::default(),
            enable_compression: true,
        }
    }
}

impl ServerConfig {
    /// Load configuration from a YAML file
    pub fn from_yaml_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file {}: {}", path.display(), e))?;

        let config: ServerConfig = serde_yaml::from_str(&content)
            .map_err(|e| format!("Failed to parse config file {}: {}", path.display(), e))?;

        Ok(config)
    }

    /// Get TLS mode from configuration
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

/// CLI arguments parsed via clap.
#[derive(Parser, Debug)]
#[command(name = "monax", about = "A columnar database server", version)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Path to configuration file (YAML format)
    #[arg(long, default_value = "conf/config.yaml")]
    config: String,

    /// Listen address (host:port), overrides config file
    #[arg(long)]
    addr: Option<String>,

    /// Data directory, overrides config file
    #[arg(long)]
    data_dir: Option<String>,

    /// Default database name, overrides config file
    #[arg(long)]
    database: Option<String>,

    /// TLS certificate path, overrides config file
    #[arg(long)]
    tls_cert: Option<String>,

    /// TLS private key path, overrides config file
    #[arg(long)]
    tls_key: Option<String>,

    /// Disable protocol compression, overrides config file
    #[arg(long)]
    no_compression: bool,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// Run in client mode (connect to a monax/taibai server)
    Client {
        /// Server host:port to connect to
        #[arg(short, long, default_value = "127.0.0.1:5151")]
        host: String,

        /// Username for login
        #[arg(short, long, default_value = "root")]
        user: String,

        /// Password for login
        #[arg(short, long, default_value = "")]
        password: String,

        /// Database to use
        #[arg(short, long, default_value = "main")]
        database: String,

        /// SQL query to execute (if not provided, enters interactive mode)
        #[arg(short, long)]
        query: Option<String>,
    },
}

/// Parsed CLI mode.
#[derive(Debug, Clone)]
pub enum RunMode {
    Server(ServerConfig),
    Client {
        host: String,
        user: String,
        password: String,
        database: String,
        query: Option<String>,
    },
}

impl RunMode {
    /// Parse CLI arguments and determine run mode.
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

        // Server mode: load config as before
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
            config.listen_addr = addr;
        }
        if let Some(data_dir) = args.data_dir {
            config.data_dir = data_dir;
        }
        if let Some(database) = args.database {
            config.default_database = database;
        }

        match (args.tls_cert, args.tls_key) {
            (Some(cert), Some(key)) => {
                config.tls = TlsConfigYaml {
                    enabled: true,
                    cert_path: cert,
                    key_path: key,
                };
            }
            _ => {}
        }

        if args.no_compression {
            config.enable_compression = false;
        }

        RunMode::Server(config)
    }
}
