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

fn default_reserved_bulk_slots() -> usize {
    2
}

fn default_connection_idle_timeout_secs() -> u64 {
    300
}

fn default_tls_handshake_timeout_secs() -> u64 {
    10
}

fn default_request_queue_timeout_secs() -> u64 {
    30
}

fn default_mutation_lock_timeout_secs() -> u64 {
    60
}

fn default_mutation_lock_warn_secs() -> u64 {
    1
}

fn default_tail_commit_timeout_secs() -> u64 {
    120
}

fn default_tail_commit_warn_secs() -> u64 {
    2
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
    /// Hybrid partial-residency scans: let the resident hot mirror answer
    /// rows above its close-aware coverage bound while segment scans handle
    /// the complement. See `TableMeta::hot_mirror_covers_from`.
    pub hybrid_hot_scan_enabled: bool,
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
            hybrid_hot_scan_enabled: default_hybrid_hot_scan_enabled(),
        }
    }
}

static QUERY_LIMITS: OnceLock<QueryLimits> = OnceLock::new();
static PERFORMANCE_BASELINE_CONFIG: OnceLock<PerformanceBaselineConfig> = OnceLock::new();
static INSERT_PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static BULK_ZSTD_LEVEL: OnceLock<i32> = OnceLock::new();
static READER_CACHE_CAPACITY: OnceLock<usize> = OnceLock::new();
static STORAGE_TIMEOUTS: OnceLock<StorageTimeouts> = OnceLock::new();

/// Process-wide deadlines for the blocking waits on the write path (table
/// mutation lock acquisition and durable-tail commit acknowledgement). Every
/// such wait must be bounded so a stalled lock holder or a slow disk surfaces
/// as an explicit timeout error instead of an indefinite hang.
#[derive(Debug, Clone, Copy)]
pub struct StorageTimeouts {
    pub mutation_lock_timeout_secs: u64,
    pub mutation_lock_warn_secs: u64,
    pub tail_commit_timeout_secs: u64,
    pub tail_commit_warn_secs: u64,
}

impl Default for StorageTimeouts {
    fn default() -> Self {
        Self {
            mutation_lock_timeout_secs: default_mutation_lock_timeout_secs(),
            mutation_lock_warn_secs: default_mutation_lock_warn_secs(),
            tail_commit_timeout_secs: default_tail_commit_timeout_secs(),
            tail_commit_warn_secs: default_tail_commit_warn_secs(),
        }
    }
}

/// Install storage wait timeouts sourced from the `storage.*` config section.
/// Called once during server startup; defaults apply when unset.
pub fn set_storage_timeouts(timeouts: StorageTimeouts) {
    let _ = STORAGE_TIMEOUTS.set(timeouts);
}

pub fn storage_timeouts() -> StorageTimeouts {
    *STORAGE_TIMEOUTS.get_or_init(StorageTimeouts::default)
}

/// Enable per-stage INSERT profiling logs. Configured from `log.insert_profile`
/// in the config file at startup; defaults to off.
pub fn set_insert_profile_enabled(enabled: bool) {
    let _ = INSERT_PROFILE_ENABLED.set(enabled);
}

pub fn insert_profile_enabled() -> bool {
    *INSERT_PROFILE_ENABLED.get().unwrap_or(&false)
}

static TAIL_ARCHIVE_ENABLED: OnceLock<bool> = OnceLock::new();

/// Install the tail-archive switch sourced from `storage.tail_archive`.
/// Called once during server startup; defaults to off.
pub fn set_tail_archive_enabled(enabled: bool) {
    let _ = TAIL_ARCHIVE_ENABLED.set(enabled);
}

pub fn tail_archive_enabled() -> bool {
    *TAIL_ARCHIVE_ENABLED.get().unwrap_or(&false)
}

/// Install storage runtime tunables sourced from `compression.bulk_zstd_level`
/// and `storage.reader_cache_capacity` in the config file. Called once during
/// server startup, before any storage component reads them.
pub fn set_storage_runtime_settings(bulk_zstd_level: i32, reader_cache_capacity: usize) {
    let _ = BULK_ZSTD_LEVEL.set(bulk_zstd_level.clamp(1, 22));
    let _ = READER_CACHE_CAPACITY.set(reader_cache_capacity.max(1));
}

pub fn bulk_zstd_level() -> i32 {
    *BULK_ZSTD_LEVEL.get().unwrap_or(&3)
}

pub fn reader_cache_capacity() -> usize {
    *READER_CACHE_CAPACITY.get().unwrap_or(&1024)
}

static MUTABLE_READER_CACHE_BUDGET_BYTES: OnceLock<u64> = OnceLock::new();

/// Install the mutable-segment reader cache byte budget
/// (`storage.mutable_reader_cache_mb` in the config file), sourced from the
/// same startup pass as the other storage runtime tunables.
pub fn set_mutable_reader_cache_budget_bytes(budget_bytes: u64) {
    let _ = MUTABLE_READER_CACHE_BUDGET_BYTES.set(budget_bytes.max(1024 * 1024));
}

pub fn mutable_reader_cache_budget_bytes() -> u64 {
    *MUTABLE_READER_CACHE_BUDGET_BYTES
        .get()
        .unwrap_or(&(4096 * 1024 * 1024))
}

static READER_CACHE_BUDGET_BYTES: OnceLock<u64> = OnceLock::new();

/// Install the sealed-reader cache byte budget (`storage.reader_cache_mb`).
pub fn set_reader_cache_budget_bytes(budget_bytes: u64) {
    let _ = READER_CACHE_BUDGET_BYTES.set(budget_bytes.max(1024 * 1024));
}

/// Byte ceiling for the sealed-segment reader cache.
pub fn reader_cache_budget_bytes() -> u64 {
    *READER_CACHE_BUDGET_BYTES
        .get()
        .unwrap_or(&(2048 * 1024 * 1024))
}

static SEGMENT_META_CACHE_BUDGET_BYTES: OnceLock<u64> = OnceLock::new();

/// Install the segment-metadata cache byte budget
/// (`storage.segment_meta_cache_mb`), sourced from the same startup pass as
/// the other storage runtime tunables.
pub fn set_segment_meta_cache_budget_bytes(budget_bytes: u64) {
    let _ = SEGMENT_META_CACHE_BUDGET_BYTES.set(budget_bytes.max(1024 * 1024));
}

/// Byte ceiling for the parsed segment-metadata cache (`SHOW MEMORY` reports
/// the live figure as `segment_meta.cache_budget_bytes`).
pub fn segment_meta_cache_budget_bytes() -> u64 {
    *SEGMENT_META_CACHE_BUDGET_BYTES
        .get()
        .unwrap_or(&(2048 * 1024 * 1024))
}

/// Install the process-wide query limits. Called once during startup.
/// Returns an error if called more than once (limits are immutable thereafter).
pub fn set_query_limits(limits: QueryLimits) -> Result<(), QueryLimits> {
    QUERY_LIMITS.set(limits)
}

/// Read the process-wide query limits (defaults if unset).
pub fn query_limits() -> QueryLimits {
    *QUERY_LIMITS.get().unwrap_or(&QueryLimits {
        hybrid_hot_scan_enabled: default_hybrid_hot_scan_enabled(),
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

#[allow(clippy::result_large_err)]
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

fn default_max_bulk_message_size() -> usize {
    128 * 1024 * 1024
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

fn default_hybrid_hot_scan_enabled() -> bool {
    true
}

fn default_hot_warmup_enabled() -> bool {
    true
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
    default_execution_threads().clamp(1, 8)
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
    default_execution_threads().clamp(1, 8)
}
fn default_global_io_permits() -> usize {
    default_execution_threads().clamp(1, 16)
}
fn default_global_cpu_permits() -> usize {
    default_execution_threads().max(1)
}

fn default_row_group_target_rows() -> usize {
    8_192
}

fn default_mutable_reader_cache_mb() -> u64 {
    4096
}

fn default_segment_meta_cache_mb() -> u64 {
    2048
}

fn default_reader_cache_capacity() -> usize {
    1024
}

fn default_reader_cache_mb() -> u64 {
    2048
}

fn default_bulk_zstd_level() -> i32 {
    3
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
    #[serde(default)]
    pub pubsub: PubSubSettings,
    #[serde(default)]
    pub replication: ReplicationSettings,
}

/// Primary/standby replication (see `src/storage/replication`). `role` is
/// one of `"none"` (default), `"primary"` (emit the replication event
/// stream, reject bulk/RELAXED direct writes) or `"standby"` (read-only
/// engine; a background applier replays the primary's stream; local
/// schedulers and auto-compaction are disabled).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSettings {
    /// `"none" | "primary" | "standby"`.
    #[serde(default)]
    pub role: String,
    /// Standby only: primary's `host:port`.
    #[serde(default)]
    pub primary_addr: String,
    /// Standby only: credentials used for `REPL_HELLO`.
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// Primary only: events retained for reconnecting standbys.
    #[serde(default = "default_replication_ring_capacity")]
    pub ring_capacity: usize,
    /// Primary only: byte ceiling for the retained-event ring. The ring is
    /// also entry-bounded (`ring_capacity`), but entries carry whole
    /// compressed write groups (up to 16 MiB each), so without a byte
    /// ceiling the retained history can reach hundreds of GB on write-heavy
    /// primaries. Oldest events are evicted past the ceiling; a lagging
    /// standby re-bootstraps via segment fetch instead.
    #[serde(default = "default_replication_ring_max_bytes")]
    pub ring_max_bytes: u64,
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self {
            role: "none".to_string(),
            primary_addr: String::new(),
            user: String::new(),
            password: String::new(),
            ring_capacity: default_replication_ring_capacity(),
            ring_max_bytes: default_replication_ring_max_bytes(),
        }
    }
}

fn default_replication_ring_capacity() -> usize {
    65_536
}

fn default_replication_ring_max_bytes() -> u64 {
    1024 * 1024 * 1024
}

impl ReplicationSettings {
    pub fn role_is_primary(&self) -> bool {
        self.role.eq_ignore_ascii_case("primary")
    }

    pub fn role_is_standby(&self) -> bool {
        self.role.eq_ignore_ascii_case("standby")
    }
}

static REPLICATION_SETTINGS: OnceLock<ReplicationSettings> = OnceLock::new();

/// Install the replication role at startup (main). Engines read it during
/// construction; tests may install it once before creating engines.
pub fn set_replication_settings(settings: ReplicationSettings) {
    let _ = REPLICATION_SETTINGS.set(settings);
}

pub fn replication_settings() -> ReplicationSettings {
    REPLICATION_SETTINGS.get().cloned().unwrap_or_default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSettings {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    /// Receive cap for columnar bulk-import messages (0x23). Bulk payloads are
    /// proportional to batch rows, so they get their own ceiling instead of
    /// inheriting the general per-message cap.
    #[serde(default = "default_max_bulk_message_size")]
    pub max_bulk_message_size: usize,
    /// Maximum number of established client sockets. Excess connections are
    /// accepted and closed immediately so the OS accept queue keeps draining.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Maximum number of CPU/disk-heavy protocol requests executing at once.
    /// These requests run outside Tokio's networking worker threads.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    /// Heavy-request slots kept out of reach of ad-hoc queries so bulk
    /// imports keep committing while analytical queries saturate the rest
    /// of the pool. Only MSG_TYPE_QUERY admission is capped (to
    /// `max_concurrent_requests - reserved_bulk_slots`); bulk writes and
    /// every other heavy request keep using the full pool. Clamped to
    /// `max_concurrent_requests - 1`; set to 0 to restore the legacy
    /// single-pool behavior.
    #[serde(default = "default_reserved_bulk_slots")]
    pub reserved_bulk_slots: usize,
    /// Disconnect clients that send no protocol frame for this many seconds.
    /// Set to 0 to disable the idle timeout.
    #[serde(default = "default_connection_idle_timeout_secs")]
    pub connection_idle_timeout_secs: u64,
    /// Idle timeout applied while the connection holds an open bulk-import
    /// session. Bulk import clients legitimately stay silent for long
    /// stretches (decoding input files, filling batches) and rely on
    /// keepalive PINGs; a keepalive interval that races the regular idle
    /// timeout kills live imports mid-file. Set to 0 to exempt bulk sessions
    /// from idle disconnects entirely.
    #[serde(default = "default_bulk_import_idle_timeout_secs")]
    pub bulk_import_idle_timeout_secs: u64,
    /// Maximum time allowed for an incoming TLS handshake. Set to 0 to disable.
    #[serde(default = "default_tls_handshake_timeout_secs")]
    pub tls_handshake_timeout_secs: u64,
    /// Maximum time a heavy request may wait for an execution slot before the
    /// server rejects it with an overload error. Bounds queueing so a
    /// saturated engine surfaces backpressure instead of silently hanging.
    #[serde(default = "default_request_queue_timeout_secs")]
    pub request_queue_timeout_secs: u64,
    /// Maximum wall-clock time one statement may run before it is cancelled
    /// at the next safe point (irreversible commit phases run to completion).
    /// Set to 0 to disable the statement timeout.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
}

fn default_statement_timeout_secs() -> u64 {
    0
}

fn default_bulk_import_idle_timeout_secs() -> u64 {
    3600
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
    /// Rebuild resident hot stores from segments at startup. Close-aware:
    /// segments archived by a close-of-day flush are not reloaded, only
    /// segments written after the last close. Disable to keep every restart
    /// cold, trading resident point-lookup speed for zero warmup I/O and
    /// memory.
    #[serde(default = "default_hot_warmup_enabled")]
    pub hot_warmup_enabled: bool,
    /// Hybrid partial-residency scans (see `QueryLimits`). Disable to serve
    /// every query from segments and memtables only.
    #[serde(default = "default_hybrid_hot_scan_enabled")]
    pub hybrid_hot_scan_enabled: bool,
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
    /// Archive cleared durable-tail files under `<data>/wal_archive/<db>/<table>/`
    /// instead of deleting them. The archive is the incremental-log source for
    /// point-in-time recovery and future replication bootstrap. Default false
    /// keeps the previous delete behavior (and the disk usage).
    #[serde(default)]
    pub tail_archive: bool,
    /// Number of segments kept in the reader and segment-metadata caches.
    #[serde(default = "default_reader_cache_capacity")]
    pub reader_cache_capacity: usize,
    /// Byte ceiling for the sealed-segment reader cache. Each entry is a
    /// parsed footer plus (for owned readers) the file copy; the footer of a
    /// 2 GiB segment measures ~68 MiB, so this is the ceiling that actually
    /// binds. `reader_cache_capacity` remains an entry-count cap on top.
    #[serde(default = "default_reader_cache_mb")]
    pub reader_cache_mb: u64,
    /// Byte ceiling for the mutable-segment reader cache. Each entry is a
    /// full heap copy of an active segment file (up to `min_segment_seal_size`),
    /// and the cache is additionally entry-bounded at 4x the reader cache
    /// capacity; without a byte ceiling many concurrently-written partitions
    /// pin one copy each, which showed up as unbounded anon RSS growth.
    #[serde(default = "default_mutable_reader_cache_mb")]
    pub mutable_reader_cache_mb: u64,
    /// Byte ceiling for the parsed segment-metadata (footer) cache. One entry
    /// is a segment's parsed footer: row-group metadata, zone maps and bloom
    /// filters — tens of MiB for a multi-GiB segment.
    ///
    /// Sized in BYTES on purpose. The cache used to be sized in ENTRIES
    /// (`reader_cache_capacity`, default 1024) while the per-entry cost grew
    /// with segment size: a 24 MiB footer turned a stable-looking knob into a
    /// 25 GiB resident set, with no metric anywhere to show it.
    #[serde(default = "default_segment_meta_cache_mb")]
    pub segment_meta_cache_mb: u64,
    /// Maximum time a write may wait for the per-table mutation lock before
    /// failing with a lock timeout error instead of blocking forever.
    #[serde(default = "default_mutation_lock_timeout_secs")]
    pub mutation_lock_timeout_secs: u64,
    /// Log a warning when acquiring the per-table mutation lock takes longer
    /// than this threshold, making lock queueing visible in the server log.
    #[serde(default = "default_mutation_lock_warn_secs")]
    pub mutation_lock_warn_secs: u64,
    /// Maximum time a request waits for durable-tail group commit (fsync)
    /// acknowledgement before failing with a commit timeout error.
    #[serde(default = "default_tail_commit_timeout_secs")]
    pub tail_commit_timeout_secs: u64,
    /// Log a warning when durable-tail commit acknowledgement takes longer
    /// than this threshold, surfacing slow fsync/disks.
    #[serde(default = "default_tail_commit_warn_secs")]
    pub tail_commit_warn_secs: u64,
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

/// Change-subscription (pubsub) settings. The feature is enabled by default
/// but costs nothing while no client subscribes: the write path only does a
/// relaxed atomic load, and every queue is bounded so memory use stays
/// proportional to the configured capacities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PubSubSettings {
    /// Master switch. When false, SUBSCRIBE requests are rejected and the
    /// write path short-circuits before touching the event channel.
    #[serde(default = "default_pubsub_enabled")]
    pub enabled: bool,
    /// Capacity of the write-path -> fan-out event channel. Overflow drops
    /// events (counted) instead of blocking writers.
    #[serde(default = "default_pubsub_event_channel_capacity")]
    pub event_channel_capacity: usize,
    /// Maximum concurrent subscriptions server-wide.
    #[serde(default = "default_pubsub_max_subscriptions")]
    pub max_subscriptions: usize,
    /// Per-connection notification queue capacity. A client that cannot keep
    /// up has excess notifications dropped (counted per subscription) instead
    /// of growing memory unboundedly.
    #[serde(default = "default_pubsub_notification_channel_capacity")]
    pub notification_channel_capacity: usize,
    /// Payloads above this size are compressed once by the fan-out thread and
    /// shared by all subscribers of the change. `usize::MAX` disables
    /// notification compression. Follows `compression.enabled` at startup.
    #[serde(default = "default_pubsub_compress_threshold")]
    pub compress_threshold: usize,
    /// Capacity of the commit ring between the write path and the reliable
    /// pub/sub publisher thread (per-table retention log, resumable
    /// subscriptions). Overflow marks a gap and asks subscribers to resync
    /// instead of blocking writers.
    #[serde(default = "default_pubsub_commit_ring_capacity")]
    pub commit_ring_capacity: usize,
    /// Fallback poll interval (ms) for reliable-subscription delivery tasks
    /// when no change-notification wake-up arrives.
    #[serde(default = "default_pubsub_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Per-table retention log capacity, in committed batches. Subscribers
    /// resuming from a sequence below the retained floor are asked to resync.
    #[serde(default = "default_pubsub_log_max_batches")]
    pub log_max_batches: usize,
    /// Per-table retention log capacity, in MiB (approximate batch bytes).
    #[serde(default = "default_pubsub_log_max_bytes_mb")]
    pub log_max_bytes_mb: u64,
    /// Maximum number of committed batches coalesced into one reliable
    /// data push frame.
    #[serde(default = "default_pubsub_max_batches_per_frame")]
    pub max_batches_per_frame: usize,
}

impl Default for PubSubSettings {
    fn default() -> Self {
        Self {
            enabled: default_pubsub_enabled(),
            event_channel_capacity: default_pubsub_event_channel_capacity(),
            max_subscriptions: default_pubsub_max_subscriptions(),
            notification_channel_capacity: default_pubsub_notification_channel_capacity(),
            compress_threshold: default_pubsub_compress_threshold(),
            commit_ring_capacity: default_pubsub_commit_ring_capacity(),
            poll_interval_ms: default_pubsub_poll_interval_ms(),
            log_max_batches: default_pubsub_log_max_batches(),
            log_max_bytes_mb: default_pubsub_log_max_bytes_mb(),
            max_batches_per_frame: default_pubsub_max_batches_per_frame(),
        }
    }
}

fn default_pubsub_enabled() -> bool {
    true
}

fn default_pubsub_event_channel_capacity() -> usize {
    65_536
}

fn default_pubsub_max_subscriptions() -> usize {
    1024
}

fn default_pubsub_notification_channel_capacity() -> usize {
    1024
}

fn default_pubsub_compress_threshold() -> usize {
    4096
}

fn default_pubsub_commit_ring_capacity() -> usize {
    65_536
}

fn default_pubsub_poll_interval_ms() -> u64 {
    50
}

fn default_pubsub_log_max_batches() -> usize {
    1024
}

fn default_pubsub_log_max_bytes_mb() -> u64 {
    64
}

fn default_pubsub_max_batches_per_frame() -> usize {
    32
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
        max_bulk_message_size: default_max_bulk_message_size(),
        max_connections: default_max_connections(),
        max_concurrent_requests: default_max_concurrent_requests(),
        reserved_bulk_slots: default_reserved_bulk_slots(),
        connection_idle_timeout_secs: default_connection_idle_timeout_secs(),
        bulk_import_idle_timeout_secs: default_bulk_import_idle_timeout_secs(),
        tls_handshake_timeout_secs: default_tls_handshake_timeout_secs(),
        request_queue_timeout_secs: default_request_queue_timeout_secs(),
        statement_timeout_secs: default_statement_timeout_secs(),
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
        hot_warmup_enabled: default_hot_warmup_enabled(),
        hybrid_hot_scan_enabled: default_hybrid_hot_scan_enabled(),
        row_group_target_rows: default_row_group_target_rows(),
        reader_cache_capacity: default_reader_cache_capacity(),
        reader_cache_mb: default_reader_cache_mb(),
        mutable_reader_cache_mb: default_mutable_reader_cache_mb(),
        segment_meta_cache_mb: default_segment_meta_cache_mb(),
        tail_async_fsync: false,
        tail_archive: false,
        mutation_lock_timeout_secs: default_mutation_lock_timeout_secs(),
        mutation_lock_warn_secs: default_mutation_lock_warn_secs(),
        tail_commit_timeout_secs: default_tail_commit_timeout_secs(),
        tail_commit_warn_secs: default_tail_commit_warn_secs(),
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
            pubsub: PubSubSettings::default(),
            replication: ReplicationSettings::default(),
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
#[path = "config_test.rs"]
mod tests;
