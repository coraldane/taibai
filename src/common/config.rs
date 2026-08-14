use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use crate::net::tls::TlsMode;

fn default_port() -> u16 {
    5151
}

fn default_max_connections() -> usize {
    1024
}

fn default_max_concurrent_requests() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().min(8))
        .unwrap_or(4)
        .max(4)
}

fn default_connection_idle_timeout_secs() -> u64 {
    300
}

fn default_tls_handshake_timeout_secs() -> u64 {
    10
}

/// Process-global runtime limits consulted by query operators (aggregation,
/// streaming scans, ...). Populated once at startup from `BufferSettings` via
/// [`set_query_limits`]. Operators read via [`query_limits`]; before startup
/// the [`QueryLimits::default`] values apply.
#[derive(Debug, Clone, Copy)]
pub struct QueryLimits {
    /// Soft per-query memory ceiling in MB. Query operators and final result
    /// materialization enforce this limit to abort oversized queries before
    /// they can exhaust process memory.
    #[allow(dead_code)]
    pub query_memory_limit_mb: u64,
    pub max_result_message_bytes: usize,
    pub max_aggregate_groups: usize,
    pub max_parallel_row_groups: usize,
    pub parallel_scan_min_row_groups: usize,
    pub spill_batch_rows: usize,
    pub join_spill_partitions: usize,
    pub max_parallel_segments: usize,
    pub global_io_permits: usize,
    pub global_cpu_permits: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            query_memory_limit_mb: default_query_memory_limit_mb(),
            max_result_message_bytes: default_max_message_size(),
            max_aggregate_groups: default_max_aggregate_groups(),
            max_parallel_row_groups: default_max_parallel_row_groups(),
            parallel_scan_min_row_groups: default_parallel_scan_min_row_groups(),
            spill_batch_rows: default_spill_batch_rows(),
            join_spill_partitions: default_join_spill_partitions(),
            max_parallel_segments: default_max_parallel_segments(),
            global_io_permits: default_global_io_permits(),
            global_cpu_permits: default_global_cpu_permits(),
        }
    }
}

static QUERY_LIMITS: OnceLock<QueryLimits> = OnceLock::new();
static PERFORMANCE_BASELINE_CONFIG: OnceLock<PerformanceBaselineConfig> = OnceLock::new();
static INSERT_PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static BULK_ZSTD_LEVEL: OnceLock<i32> = OnceLock::new();
static READER_CACHE_CAPACITY: OnceLock<usize> = OnceLock::new();

/// Enable per-stage INSERT profiling logs. Configured from `log.insert_profile`
/// in the config file at startup; defaults to off.
pub fn set_insert_profile_enabled(enabled: bool) {
    let _ = INSERT_PROFILE_ENABLED.set(enabled);
}

pub fn insert_profile_enabled() -> bool {
    *INSERT_PROFILE_ENABLED.get().unwrap_or(&false)
}

/// Install storage runtime tunables sourced from `compression.bulk_zstd_level`
/// and `storage.reader_cache_capacity` in the config file. Called once during
/// server startup, before any storage component reads them.
pub fn set_storage_runtime_settings(bulk_zstd_level: i32, reader_cache_capacity: usize) {
    let _ = BULK_ZSTD_LEVEL.set(bulk_zstd_level.clamp(1, 22));
    let _ = READER_CACHE_CAPACITY.set(reader_cache_capacity.max(1));
}

pub fn bulk_zstd_level() -> i32 {
    *BULK_ZSTD_LEVEL.get().unwrap_or(&1)
}

pub fn reader_cache_capacity() -> usize {
    *READER_CACHE_CAPACITY.get().unwrap_or(&1024)
}

/// Install the process-wide query limits. Called once during startup.
/// Returns an error if called more than once (limits are immutable thereafter).
pub fn set_query_limits(limits: QueryLimits) -> Result<(), QueryLimits> {
    QUERY_LIMITS.set(limits)
}

/// Read the process-wide query limits (defaults if unset).
pub fn query_limits() -> QueryLimits {
    *QUERY_LIMITS.get().unwrap_or(&QueryLimits {
        query_memory_limit_mb: default_query_memory_limit_mb(),
        max_result_message_bytes: default_max_message_size(),
        max_aggregate_groups: default_max_aggregate_groups(),
        max_parallel_row_groups: default_max_parallel_row_groups(),
        parallel_scan_min_row_groups: default_parallel_scan_min_row_groups(),
        spill_batch_rows: default_spill_batch_rows(),
        join_spill_partitions: default_join_spill_partitions(),
        max_parallel_segments: default_max_parallel_segments(),
        global_io_permits: default_global_io_permits(),
        global_cpu_permits: default_global_cpu_permits(),
    })
}

#[derive(Debug, Clone)]
pub struct PerformanceBaselineConfig {
    pub max_entries: usize,
    pub warmup_samples: u64,
    pub ewma_alpha: f64,
    pub elapsed_regression_ratio: f64,
    pub elapsed_regression_min_ms: f64,
    pub memory_regression_ratio: f64,
    pub memory_regression_min_bytes: u64,
    pub spill_regression_ratio: f64,
    pub row_error_regression_ratio: f64,
    pub row_error_regression_min: f64,
    pub environment: String,
    pub version: String,
}

impl Default for PerformanceBaselineConfig {
    fn default() -> Self {
        Self {
            max_entries: default_performance_max_entries(),
            warmup_samples: default_performance_warmup_samples(),
            ewma_alpha: default_performance_ewma_alpha(),
            elapsed_regression_ratio: default_performance_elapsed_ratio(),
            elapsed_regression_min_ms: default_performance_elapsed_min_ms(),
            memory_regression_ratio: default_performance_memory_ratio(),
            memory_regression_min_bytes: default_performance_memory_min_mb()
                .saturating_mul(1024 * 1024),
            spill_regression_ratio: default_performance_spill_ratio(),
            row_error_regression_ratio: default_performance_row_error_ratio(),
            row_error_regression_min: default_performance_row_error_min(),
            environment: default_performance_environment(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

pub fn set_performance_baseline_config(
    config: PerformanceBaselineConfig,
) -> Result<(), PerformanceBaselineConfig> {
    PERFORMANCE_BASELINE_CONFIG.set(config.sanitized())
}

pub fn performance_baseline_config() -> PerformanceBaselineConfig {
    PERFORMANCE_BASELINE_CONFIG
        .get()
        .cloned()
        .unwrap_or_default()
}

impl PerformanceBaselineConfig {
    pub(crate) fn sanitized(mut self) -> Self {
        self.max_entries = self.max_entries.max(1);
        self.warmup_samples = self.warmup_samples.max(1);
        self.ewma_alpha = finite_clamp(self.ewma_alpha, 0.01, 1.0, 0.20);
        self.elapsed_regression_ratio =
            finite_clamp(self.elapsed_regression_ratio, 1.0, 1000.0, 1.50);
        self.elapsed_regression_min_ms =
            finite_clamp(self.elapsed_regression_min_ms, 0.0, f64::MAX, 5.0);
        self.memory_regression_ratio =
            finite_clamp(self.memory_regression_ratio, 1.0, 1000.0, 1.50);
        self.spill_regression_ratio = finite_clamp(self.spill_regression_ratio, 1.0, 1000.0, 2.0);
        self.row_error_regression_ratio =
            finite_clamp(self.row_error_regression_ratio, 1.0, 1000.0, 2.0);
        self.row_error_regression_min =
            finite_clamp(self.row_error_regression_min, 0.0, f64::MAX, 2.0);
        self.environment = normalize_label(&self.environment, "default");
        self.version = normalize_label(&self.version, env!("CARGO_PKG_VERSION"));
        self
    }
}

fn finite_clamp(value: f64, min: f64, max: f64, fallback: f64) -> f64 {
    if value.is_finite() {
        value.clamp(min, max)
    } else {
        fallback
    }
}

fn normalize_label(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.chars().take(128).collect()
    }
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

fn default_fragmentation_cleanup_enabled() -> bool {
    true
}

fn default_fragmentation_min_row_groups() -> usize {
    32
}

fn default_fragmentation_max_overhead_ratio() -> f64 {
    2.0
}

fn default_fragmentation_min_small_row_group_ratio() -> f64 {
    0.50
}

fn default_fragmentation_cooldown_secs() -> u64 {
    3600
}

fn default_fragmentation_scan_interval_secs() -> u64 {
    300
}

fn default_low_rate_memtable_max_rows() -> u64 {
    1024
}

fn default_low_rate_memtable_max_age_secs() -> u64 {
    3600
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

fn default_execution_threads() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4)
        .max(1)
}

fn default_max_parallel_row_groups() -> usize {
    default_execution_threads().min(8).max(1)
}

fn default_parallel_scan_min_row_groups() -> usize {
    4
}

fn default_spill_batch_rows() -> usize {
    4096
}

fn default_join_spill_partitions() -> usize {
    16
}

fn default_max_parallel_segments() -> usize {
    default_execution_threads().min(4).max(1)
}
fn default_global_io_permits() -> usize {
    default_execution_threads().min(4).max(1)
}
fn default_global_cpu_permits() -> usize {
    default_execution_threads().max(1)
}

fn default_row_group_target_rows() -> usize {
    8_192
}

fn default_reader_cache_capacity() -> usize {
    1024
}

fn default_bulk_zstd_level() -> i32 {
    1
}

fn default_insert_profile() -> bool {
    false
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_query_log_max_size_mb() -> u64 {
    1024
}

fn default_performance_max_entries() -> usize {
    1024
}

fn default_performance_warmup_samples() -> u64 {
    3
}

fn default_performance_ewma_alpha() -> f64 {
    0.20
}

fn default_performance_elapsed_ratio() -> f64 {
    1.50
}

fn default_performance_elapsed_min_ms() -> f64 {
    5.0
}

fn default_performance_memory_ratio() -> f64 {
    1.50
}

fn default_performance_memory_min_mb() -> u64 {
    1
}

fn default_performance_spill_ratio() -> f64 {
    2.0
}

fn default_performance_row_error_ratio() -> f64 {
    2.0
}

fn default_performance_row_error_min() -> f64 {
    2.0
}

fn default_performance_environment() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_server_settings")]
    pub server: ServerSettings,
    #[serde(default = "default_storage_settings")]
    pub storage: StorageSettings,
    #[serde(default = "default_buffer_settings")]
    pub buffer: BufferSettings,
    #[serde(default = "default_compression_settings")]
    pub compression: CompressionSettings,
    #[serde(default)]
    pub tls: TlsConfigYaml,
    #[serde(default = "default_log_settings")]
    pub log: LogSettings,
    #[serde(default)]
    pub performance: PerformanceBaselineSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// Maximum number of established client sockets. Excess connections are
    /// accepted and closed immediately so the OS accept queue keeps draining.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Maximum number of CPU/disk-heavy protocol requests executing at once.
    /// These requests run outside Tokio's networking worker threads.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Disconnect clients that send no protocol frame for this many seconds.
    /// Set to 0 to disable the idle timeout.
    #[serde(default = "default_connection_idle_timeout_secs")]
    pub connection_idle_timeout_secs: u64,
    /// Maximum time allowed for an incoming TLS handshake. Set to 0 to disable.
    #[serde(default = "default_tls_handshake_timeout_secs")]
    pub tls_handshake_timeout_secs: u64,
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
    #[serde(default = "default_fragmentation_cleanup_enabled")]
    pub fragmentation_cleanup_enabled: bool,
    #[serde(default = "default_fragmentation_min_row_groups")]
    pub fragmentation_min_row_groups: usize,
    #[serde(default = "default_fragmentation_max_overhead_ratio")]
    pub fragmentation_max_overhead_ratio: f64,
    #[serde(default = "default_fragmentation_min_small_row_group_ratio")]
    pub fragmentation_min_small_row_group_ratio: f64,
    #[serde(default = "default_fragmentation_cooldown_secs")]
    pub fragmentation_cooldown_secs: u64,
    #[serde(default = "default_fragmentation_scan_interval_secs")]
    pub fragmentation_scan_interval_secs: u64,
    #[serde(default = "default_low_rate_memtable_max_rows")]
    pub low_rate_memtable_max_rows: u64,
    #[serde(default = "default_low_rate_memtable_max_age_secs")]
    pub low_rate_memtable_max_age_secs: u64,
    #[serde(default = "default_hot_store_memory_limit_mb")]
    pub hot_store_memory_limit_mb: u64,
    #[serde(default = "default_row_group_target_rows")]
    pub row_group_target_rows: usize,
    /// Ack durable-tail appends once the bytes reach the page cache. The tail
    /// committer thread still fsyncs every group in the background, so the
    /// durability lag stays within one group-commit window (sub-second), but a
    /// crash in that window can lose already-acknowledged INSERTs. Trades a
    /// synchronous fsync on the INSERT latency path for throughput on
    /// fsync-bound disks. Default false keeps fsync-before-ack semantics.
    #[serde(default)]
    pub tail_async_fsync: bool,
    /// Number of segments kept in the reader and segment-metadata caches.
    #[serde(default = "default_reader_cache_capacity")]
    pub reader_cache_capacity: usize,
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
    #[serde(default = "default_execution_threads")]
    pub execution_threads: usize,
    #[serde(default = "default_max_parallel_row_groups")]
    pub max_parallel_row_groups: usize,
    #[serde(default = "default_parallel_scan_min_row_groups")]
    pub parallel_scan_min_row_groups: usize,
    #[serde(default = "default_spill_batch_rows")]
    pub spill_batch_rows: usize,
    #[serde(default = "default_join_spill_partitions")]
    pub join_spill_partitions: usize,
    #[serde(default = "default_max_parallel_segments")]
    pub max_parallel_segments: usize,
    /// Process-wide storage work permits shared by query scans and OPTIMIZE
    /// partition workers. These bound concurrent decode/read/write pressure.
    #[serde(default = "default_global_io_permits")]
    pub global_io_permits: usize,
    /// Process-wide CPU work permits shared by query kernels and OPTIMIZE
    /// partition workers.
    #[serde(default = "default_global_cpu_permits")]
    pub global_cpu_permits: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionSettings {
    pub enabled: bool,
    /// zstd compression level applied to bulk segment writes. Valid range is
    /// clamped to 1..=22 at startup; higher levels trade CPU for size.
    #[serde(default = "default_bulk_zstd_level")]
    pub bulk_zstd_level: i32,
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
    /// Emit per-stage INSERT profiling diagnostics (`[insert-profile]` lines).
    #[serde(default = "default_insert_profile")]
    pub insert_profile: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceBaselineSettings {
    #[serde(default = "default_performance_max_entries")]
    pub max_entries: usize,
    #[serde(default = "default_performance_warmup_samples")]
    pub warmup_samples: u64,
    #[serde(default = "default_performance_ewma_alpha")]
    pub ewma_alpha: f64,
    #[serde(default = "default_performance_elapsed_ratio")]
    pub elapsed_regression_ratio: f64,
    #[serde(default = "default_performance_elapsed_min_ms")]
    pub elapsed_regression_min_ms: f64,
    #[serde(default = "default_performance_memory_ratio")]
    pub memory_regression_ratio: f64,
    #[serde(default = "default_performance_memory_min_mb")]
    pub memory_regression_min_mb: u64,
    #[serde(default = "default_performance_spill_ratio")]
    pub spill_regression_ratio: f64,
    #[serde(default = "default_performance_row_error_ratio")]
    pub row_error_regression_ratio: f64,
    #[serde(default = "default_performance_row_error_min")]
    pub row_error_regression_min: f64,
    #[serde(default = "default_performance_environment")]
    pub environment: String,
    #[serde(default)]
    pub version_label: Option<String>,
}

impl Default for PerformanceBaselineSettings {
    fn default() -> Self {
        Self {
            max_entries: default_performance_max_entries(),
            warmup_samples: default_performance_warmup_samples(),
            ewma_alpha: default_performance_ewma_alpha(),
            elapsed_regression_ratio: default_performance_elapsed_ratio(),
            elapsed_regression_min_ms: default_performance_elapsed_min_ms(),
            memory_regression_ratio: default_performance_memory_ratio(),
            memory_regression_min_mb: default_performance_memory_min_mb(),
            spill_regression_ratio: default_performance_spill_ratio(),
            row_error_regression_ratio: default_performance_row_error_ratio(),
            row_error_regression_min: default_performance_row_error_min(),
            environment: default_performance_environment(),
            version_label: None,
        }
    }
}

impl From<&PerformanceBaselineSettings> for PerformanceBaselineConfig {
    fn from(settings: &PerformanceBaselineSettings) -> Self {
        Self {
            max_entries: settings.max_entries,
            warmup_samples: settings.warmup_samples,
            ewma_alpha: settings.ewma_alpha,
            elapsed_regression_ratio: settings.elapsed_regression_ratio,
            elapsed_regression_min_ms: settings.elapsed_regression_min_ms,
            memory_regression_ratio: settings.memory_regression_ratio,
            memory_regression_min_bytes: settings
                .memory_regression_min_mb
                .saturating_mul(1024 * 1024),
            spill_regression_ratio: settings.spill_regression_ratio,
            row_error_regression_ratio: settings.row_error_regression_ratio,
            row_error_regression_min: settings.row_error_regression_min,
            environment: settings.environment.clone(),
            version: settings
                .version_label
                .clone()
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
        }
        .sanitized()
    }
}

fn default_server_settings() -> ServerSettings {
    ServerSettings {
        host: "0.0.0.0".to_string(),
        port: 5151,
        max_message_size: 64 * 1024 * 1024,
        max_connections: default_max_connections(),
        max_concurrent_requests: default_max_concurrent_requests(),
        connection_idle_timeout_secs: default_connection_idle_timeout_secs(),
        tls_handshake_timeout_secs: default_tls_handshake_timeout_secs(),
    }
}

fn default_storage_settings() -> StorageSettings {
    StorageSettings {
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
        fragmentation_cleanup_enabled: default_fragmentation_cleanup_enabled(),
        fragmentation_min_row_groups: default_fragmentation_min_row_groups(),
        fragmentation_max_overhead_ratio: default_fragmentation_max_overhead_ratio(),
        fragmentation_min_small_row_group_ratio: default_fragmentation_min_small_row_group_ratio(),
        fragmentation_cooldown_secs: default_fragmentation_cooldown_secs(),
        fragmentation_scan_interval_secs: default_fragmentation_scan_interval_secs(),
        low_rate_memtable_max_rows: default_low_rate_memtable_max_rows(),
        low_rate_memtable_max_age_secs: default_low_rate_memtable_max_age_secs(),
        hot_store_memory_limit_mb: default_hot_store_memory_limit_mb(),
        row_group_target_rows: default_row_group_target_rows(),
        reader_cache_capacity: default_reader_cache_capacity(),
        tail_async_fsync: false,
    }
}

fn default_buffer_settings() -> BufferSettings {
    BufferSettings {
        batch_size: 4096,
        query_memory_limit_mb: default_query_memory_limit_mb(),
        max_aggregate_groups: default_max_aggregate_groups(),
        execution_threads: default_execution_threads(),
        max_parallel_row_groups: default_max_parallel_row_groups(),
        parallel_scan_min_row_groups: default_parallel_scan_min_row_groups(),
        spill_batch_rows: default_spill_batch_rows(),
        join_spill_partitions: default_join_spill_partitions(),
        max_parallel_segments: default_max_parallel_segments(),
        global_io_permits: default_global_io_permits(),
        global_cpu_permits: default_global_cpu_permits(),
    }
}

fn default_compression_settings() -> CompressionSettings {
    CompressionSettings {
        enabled: true,
        bulk_zstd_level: default_bulk_zstd_level(),
    }
}

fn default_log_settings() -> LogSettings {
    LogSettings {
        level: "info".to_string(),
        file: None,
        query_file: None,
        query_max_size_mb: default_query_log_max_size_mb(),
        insert_profile: default_insert_profile(),
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: default_server_settings(),
            storage: default_storage_settings(),
            buffer: default_buffer_settings(),
            compression: default_compression_settings(),
            tls: TlsConfigYaml::default(),
            log: default_log_settings(),
            performance: PerformanceBaselineSettings::default(),
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
#[path = "config_tests.rs"]
mod tests;
