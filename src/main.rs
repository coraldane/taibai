use taibai::{common, net, sql, storage, types, version};

use std::sync::Arc;

use common::config::{RunMode, ServerConfig};
use net::server::Server;
use sql::execution::{AutoCompactConfig, Engine};
use storage::segment_manager::SegmentConfig;

#[derive(Clone)]
struct AsyncTracingWriter {
    sender: crossbeam_channel::Sender<Vec<u8>>,
}

impl std::io::Write for AsyncTracingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Tracing must not stall networking or query workers when the log disk
        // is slow. Dropping diagnostics is preferable to freezing the server.
        let _ = self.sender.try_send(buf.to_vec());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn spawn_async_tracing_writer<W>(
    output: W,
    thread_name: &str,
) -> std::io::Result<AsyncTracingWriter>
where
    W: std::io::Write + Send + 'static,
{
    use std::io::Write;

    let (sender, receiver) = crossbeam_channel::bounded::<Vec<u8>>(8192);
    std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || {
            let mut writer = std::io::BufWriter::new(output);
            loop {
                match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                    Ok(bytes) => {
                        if let Err(error) = writer.write_all(&bytes) {
                            eprintln!("taibai log write failed: {error}");
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        let _ = writer.flush();
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        let _ = writer.flush();
                        break;
                    }
                }
            }
        })?;
    Ok(AsyncTracingWriter { sender })
}

fn init_tracing(level: &str) {
    match spawn_async_tracing_writer(std::io::stdout(), "taibai-console-log") {
        Ok(async_writer) => {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(env_filter(level))
                .with_writer(move || async_writer.clone())
                .try_init();
        }
        Err(error) => {
            eprintln!("failed to start asynchronous console logger: {error}");
            let _ = tracing_subscriber::fmt()
                .with_env_filter(env_filter(level))
                .try_init();
        }
    }
}

#[cfg(windows)]
fn init_file_tracing(level: &str, log_file: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = log_file.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)?;
    let async_writer = spawn_async_tracing_writer(file, "taibai-server-log")?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter(level))
        .with_ansi(false)
        .with_writer(move || async_writer.clone())
        .try_init();
    Ok(())
}

fn env_filter(level: &str) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::new(level)
}

#[tokio::main]
async fn main() {
    let run_mode = RunMode::from_args();

    if !matches!(&run_mode, RunMode::Service { .. }) {
        init_tracing(run_mode.log_level());
    }
    tracing::info!("Taibai {}", version::get_full_version());

    match run_mode {
        RunMode::Server(config) => run_server(config).await,
        RunMode::Service {
            service_name,
            config_path,
            config,
        } => run_windows_service(service_name, config_path, config).await,
        RunMode::InstallService {
            service_name,
            display_name,
            config_path,
        } => install_windows_service(&service_name, &display_name, &config_path),
        RunMode::UninstallService { service_name } => uninstall_windows_service(&service_name),
        RunMode::Client {
            host,
            user,
            password,
            database,
            query,
        } => run_client(host, user, password, database, query).await,
    }
}

async fn run_server(config: ServerConfig) {
    run_server_until(config, async {
        if tokio::signal::ctrl_c().await.is_err() {
            tracing::warn!(
                "Ctrl+C handler unavailable (headless session); server runs until killed"
            );
            std::future::pending::<()>().await;
        }
        tracing::info!("received Ctrl+C, flushing active segments...");
    })
    .await;
}

async fn run_server_until<S>(config: ServerConfig, shutdown: S)
where
    S: std::future::Future<Output = ()>,
{
    tracing::info!(
        addr = %config.listen_addr(),
        data_dir = %config.storage.data_dir,
        database = %config.storage.default_database,
        tls_enabled = config.tls.enabled,
        compression = config.compression.enabled,
        "starting taibai server"
    );

    if let Some(query_file) = config.log.query_file.as_deref() {
        if let Err(e) = common::query_log::init(
            std::path::Path::new(query_file),
            config.log.query_max_size_mb,
        ) {
            tracing::error!(error = %e, query_file, "failed to initialize query log");
            eprintln!("taibai: failed to initialize query log: {e}");
            std::process::exit(1);
        }
        tracing::info!(
            query_file,
            query_max_size_mb = config.log.query_max_size_mb,
            "query log initialized"
        );
    }

    if let Err(e) = std::fs::create_dir_all(&config.storage.data_dir) {
        tracing::error!(error = %e, "failed to create data directory");
        eprintln!("taibai: failed to create data directory: {e}");
        std::process::exit(1);
    }

    let _ = common::config::set_performance_baseline_config(
        common::config::PerformanceBaselineConfig::from(&config.performance),
    );
    common::config::set_insert_profile_enabled(config.log.insert_profile);
    common::config::set_storage_runtime_settings(
        config.compression.bulk_zstd_level,
        config.storage.reader_cache_capacity,
    );
    common::config::set_storage_timeouts(common::config::StorageTimeouts {
        mutation_lock_timeout_ms: config.storage.mutation_lock_timeout_ms,
        mutation_lock_warn_ms: config.storage.mutation_lock_warn_ms,
        tail_commit_timeout_ms: config.storage.tail_commit_timeout_ms,
        tail_commit_warn_ms: config.storage.tail_commit_warn_ms,
    });

    let mut pubsub_settings = config.pubsub.clone();
    if !config.compression.enabled {
        pubsub_settings.compress_threshold = usize::MAX;
    }
    crate::common::config::set_tail_archive_enabled(config.storage.tail_archive);
    let engine = match Engine::new_with_storage_config_and_auto_compact(
        std::path::Path::new(&config.storage.data_dir),
        &config.storage.default_database,
        segment_config_from_server_config(&config),
        auto_compact_config_from_server_config(&config),
        config
            .storage
            .hot_store_memory_limit_mb
            .saturating_mul(1024 * 1024),
        config.storage.tail_async_fsync,

        pubsub_settings,
        config.storage.hot_warmup_enabled,
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize engine");
            eprintln!("taibai: failed to initialize engine: {e}");
            std::process::exit(1);
        }
    };

    let config = Arc::new(config);
    // Publish process-global query runtime limits for operators (aggregation,
    // streaming scans) to consult.
    let _ = common::config::set_query_limits(common::config::QueryLimits {
        query_memory_limit_mb: config.buffer.query_memory_limit_mb,
        max_result_message_bytes: config.server.max_message_size,
        max_aggregate_groups: config.buffer.max_aggregate_groups,
        max_parallel_row_groups: config.buffer.max_parallel_row_groups.max(1),
        parallel_scan_min_row_groups: config.buffer.parallel_scan_min_row_groups.max(1),
        spill_batch_rows: config.buffer.spill_batch_rows.max(1),
        join_spill_partitions: config.buffer.join_spill_partitions.max(2),
        max_parallel_segments: config.buffer.max_parallel_segments.max(1),
        global_io_permits: config.buffer.global_io_permits.max(1),
        global_cpu_permits: config.buffer.global_cpu_permits.max(1),
        hybrid_hot_scan_enabled: config.storage.hybrid_hot_scan_enabled,
    });
    let engine = Arc::new(engine);
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(config.buffer.execution_threads.max(1))
        .thread_name(|idx| format!("taibai-exec-{idx}"))
        .build_global();
    sql::execution::engine::start_close_of_day_scheduler(engine.clone());
    tracing::info!("close-of-day scheduler started");

    let server = Server::new(config, engine.clone());

    if let Err(e) = server.run_with_shutdown(shutdown).await {
        tracing::error!(error = %e, "server error");
        eprintln!("taibai server error: {e}");
    }

    match engine.flush_shutdown_buffers() {
        Ok(n) => tracing::info!(rows_flushed = n, "shutdown buffers checkpointed"),
        Err(e) => tracing::error!(error = %e, "failed to flush segments on shutdown"),
    }
}

fn segment_config_from_server_config(config: &ServerConfig) -> SegmentConfig {
    let mb = 1024_u64 * 1024_u64;
    let min_segment_size = config.storage.min_segment_seal_size_mb as u64 * mb;
    let max_segment_size = (config.storage.max_segment_size_mb as u64 * mb).max(min_segment_size);
    SegmentConfig {
        min_segment_size,
        max_segment_size,
        row_group_target_rows: config.storage.row_group_target_rows.max(1),
    }
}
fn auto_compact_config_from_server_config(config: &ServerConfig) -> AutoCompactConfig {
    let interval = config
        .storage
        .delete_bitmap_cleanup_interval_secs
        .map(std::time::Duration::from_secs)
        .unwrap_or_else(|| {
            std::time::Duration::from_millis(config.storage.delete_cleanup_interval_ms.max(1))
        });
    AutoCompactConfig {
        enabled: config.storage.delete_cleanup_enabled,
        min_deleted_ratio: config.storage.delete_cleanup_min_deleted_ratio,
        min_deleted_rows: config.storage.delete_cleanup_min_deleted_rows,
        fragmentation_enabled: config.storage.fragmentation_cleanup_enabled,
        min_fragmented_row_groups: config.storage.fragmentation_min_row_groups.max(2),
        max_row_group_overhead_ratio: config.storage.fragmentation_max_overhead_ratio.max(1.0),
        min_small_row_group_ratio: config
            .storage
            .fragmentation_min_small_row_group_ratio
            .clamp(0.0, 1.0),
        fragmentation_cooldown: std::time::Duration::from_secs(
            config.storage.fragmentation_cooldown_secs,
        ),
        fragmentation_scan_interval: std::time::Duration::from_secs(
            config.storage.fragmentation_scan_interval_secs.max(1),
        ),
        low_rate_max_rows: config.storage.low_rate_memtable_max_rows.max(1),
        low_rate_max_age: std::time::Duration::from_secs(
            config.storage.low_rate_memtable_max_age_secs.max(30),
        ),
        interval: interval.max(std::time::Duration::from_millis(1)),
        max_tables_per_run: config.storage.delete_cleanup_max_segments_per_run.max(1),
    }
}

async fn run_client(
    host: String,
    user: String,
    password: String,
    database: String,
    query: Option<String>,
) {
    use net::client::{Client, QueryResult};

    let mut client = match Client::connect(&host).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", host, e);
            std::process::exit(1);
        }
    };

    if let Err(e) = client.login(&user, &password, &database).await {
        eprintln!("Login failed: {}", e);
        std::process::exit(1);
    }
    println!("Connected to {} as {}", host, user);

    match client.ping().await {
        Ok(rtt) => println!("Ping: {:?}", rtt),
        Err(e) => eprintln!("Ping failed: {}", e),
    }

    let start = std::time::Instant::now();
    if let Some(sql) = query {
        match client.query(&sql).await {
            Ok(QueryResult::Ok {
                rows_affected,
                server_elapsed,
                server_timings,
                client_timings,
            }) => {
                let output_start = std::time::Instant::now();
                println!("OK, {} row(s) affected", rows_affected);
                let output_elapsed = output_start.elapsed();
                print_elapsed(
                    start.elapsed(),
                    server_elapsed,
                    server_timings.as_ref(),
                    client_timings.as_ref(),
                    output_elapsed,
                    None,
                );
            }
            Ok(QueryResult::Rows {
                schema,
                batches,
                server_elapsed,
                server_timings,
                client_timings,
            }) => {
                let output_start = std::time::Instant::now();
                let stdout = std::io::stdout();
                let mut out = std::io::BufWriter::new(stdout.lock());
                if let Err(e) = write_query_rows(&mut out, &schema, &batches) {
                    eprintln!("IO error: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = std::io::Write::flush(&mut out) {
                    eprintln!("IO error: {}", e);
                    std::process::exit(1);
                }
                let output_elapsed = output_start.elapsed();
                let row_count = total_batch_rows(&batches);
                print_elapsed(
                    start.elapsed(),
                    server_elapsed,
                    server_timings.as_ref(),
                    client_timings.as_ref(),
                    output_elapsed,
                    Some(row_count),
                );
            }
            Ok(QueryResult::Error(msg)) => {
                eprintln!("Server error: {}", msg);
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("Query failed: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        println!("taibai client interactive mode. Type SQL queries or 'quit' to exit.");
        let stdin = std::io::stdin();
        let mut buf = String::new();
        loop {
            buf.clear();
            print!("taibai> ");
            if let Err(e) = std::io::Write::flush(&mut std::io::stdout()) {
                eprintln!("IO error: {}", e);
                break;
            }
            if stdin.read_line(&mut buf).is_err() {
                break;
            }
            let sql = buf.trim();
            if sql.eq_ignore_ascii_case("quit")
                || sql.eq_ignore_ascii_case("exit")
                || sql.eq_ignore_ascii_case("\\q")
            {
                break;
            }
            if sql.is_empty() {
                continue;
            }

            let start = std::time::Instant::now();
            match client.query(sql).await {
                Ok(QueryResult::Ok {
                    rows_affected,
                    server_elapsed,
                    server_timings,
                    client_timings,
                }) => {
                    let output_start = std::time::Instant::now();
                    println!("OK, {} row(s) affected", rows_affected);
                    let output_elapsed = output_start.elapsed();
                    print_elapsed(
                        start.elapsed(),
                        server_elapsed,
                        server_timings.as_ref(),
                        client_timings.as_ref(),
                        output_elapsed,
                        None,
                    );
                }
                Ok(QueryResult::Rows {
                    schema,
                    batches,
                    server_elapsed,
                    server_timings,
                    client_timings,
                }) => {
                    let output_start = std::time::Instant::now();
                    let stdout = std::io::stdout();
                    let mut out = std::io::BufWriter::new(stdout.lock());
                    if let Err(e) = write_query_rows(&mut out, &schema, &batches) {
                        eprintln!("IO error: {}", e);
                        break;
                    }
                    if let Err(e) = std::io::Write::flush(&mut out) {
                        eprintln!("IO error: {}", e);
                        break;
                    }
                    let output_elapsed = output_start.elapsed();
                    let row_count = total_batch_rows(&batches);
                    print_elapsed(
                        start.elapsed(),
                        server_elapsed,
                        server_timings.as_ref(),
                        client_timings.as_ref(),
                        output_elapsed,
                        Some(row_count),
                    );
                }
                Ok(QueryResult::Error(msg)) => {
                    eprintln!("Server error: {} ({:?})", msg, start.elapsed());
                }
                Err(e) => {
                    eprintln!("Query failed: {} ({:?})", e, start.elapsed());
                }
            }
        }
    }

    let _ = client.logout().await;
    let _ = client.close().await;
    println!("Disconnected.");
}

fn print_elapsed(
    total: std::time::Duration,
    server: Option<std::time::Duration>,
    _timings: Option<&net::client::ServerTimings>,
    client: Option<&net::client::ClientTimings>,
    output: std::time::Duration,
    rows: Option<usize>,
) {
    let rows_text = rows
        .map(|rows| format!(", rows: {}", rows))
        .unwrap_or_default();
    match (server, client) {
        (Some(server), Some(client)) => println!(
            "(total: {:?}, server: {:?}, client_recv: {:?}, output: {:?}{})",
            total, server, client.recv, output, rows_text
        ),
        (Some(server), None) => println!(
            "(total: {:?}, server: {:?}, client_recv: n/a, output: {:?}{})",
            total, server, output, rows_text
        ),
        (None, Some(client)) => println!(
            "(total: {:?}, server: n/a, client_recv: {:?}, output: {:?}{})",
            total, client.recv, output, rows_text
        ),
        (None, None) => println!(
            "(total: {:?}, server: n/a, client_recv: n/a, output: {:?}{})",
            total, output, rows_text
        ),
    }
}

fn total_batch_rows(batches: &[types::batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn write_query_rows<W: std::io::Write>(
    out: &mut W,
    schema: &types::batch::Schema,
    batches: &[types::batch::RecordBatch],
) -> std::io::Result<()> {
    let mut line = String::with_capacity(schema.num_columns().saturating_mul(24));

    for (idx, col) in schema.columns.iter().enumerate() {
        if idx > 0 {
            line.push('\t');
        }
        line.push_str(&col.name);
    }
    line.push('\n');
    out.write_all(line.as_bytes())?;

    for batch in batches {
        for row in 0..batch.num_rows() {
            line.clear();
            for col_idx in 0..schema.num_columns() {
                if col_idx > 0 {
                    line.push('\t');
                }
                match batch.column(col_idx) {
                    Some(col) => {
                        append_column_cell(&mut line, col, row, schema.columns[col_idx].scale)
                    }
                    None => line.push_str("NULL"),
                }
            }
            line.push('\n');
            out.write_all(line.as_bytes())?;
        }
    }
    out.flush()
}

fn append_column_cell(line: &mut String, col: &types::array::ColumnArray, row: usize, scale: i32) {
    use std::fmt::Write as _;

    if col.bitmap().is_null(row) {
        line.push_str("NULL");
        return;
    }

    match col {
        types::array::ColumnArray::Selected { .. } | types::array::ColumnArray::Rle { .. } => {
            return append_column_cell(line, col.flattened(), row, scale);
        }
        types::array::ColumnArray::Dictionary { .. } => {
            return append_column_cell(line, &col.materialize_varchar(), row, scale);
        }
        types::array::ColumnArray::Bool { data, .. } => {
            line.push_str(if data[row] != 0 { "true" } else { "false" });
        }
        types::array::ColumnArray::Int8 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int16 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt8 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt16 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Float32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Float64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Decimal { data, .. } if scale > 0 => {
            append_decimal_scaled(line, data[row], scale);
        }
        types::array::ColumnArray::Decimal { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Date { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Time { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Timestamp { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Interval { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::VarChar { data, .. } => line.push_str(&data[row]),
        types::array::ColumnArray::Blob { data, .. } => {
            let _ = write!(line, "{:?}", &data[row]);
        }
        types::array::ColumnArray::Vector { data, .. } => {
            let _ = write!(line, "{:?}", data[row]);
        }
    }
}

fn append_decimal_scaled(line: &mut String, value: i64, scale: i32) {
    use std::fmt::Write as _;

    let divisor = 10_i64.pow(scale as u32);
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.abs();
    let _ = write!(
        line,
        "{}{}.{:0width$}",
        sign,
        abs / divisor,
        abs % divisor,
        width = scale as usize
    );
}

#[cfg(windows)]
fn install_windows_service(service_name: &str, display_name: &str, config_path: &str) {
    use std::ffi::OsString;
    use windows_service::service::{
        ServiceAccess, ServiceErrorControl, ServiceInfo, ServiceStartType, ServiceType,
    };
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    let exe_path = std::env::current_exe().unwrap_or_else(|e| {
        eprintln!("failed to locate current executable: {}", e);
        std::process::exit(1);
    });
    let config_path = std::fs::canonicalize(config_path)
        .unwrap_or_else(|_| std::path::PathBuf::from(config_path));
    let manager =
        ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CREATE_SERVICE)
            .unwrap_or_else(|e| {
                eprintln!("failed to open service manager: {}", e);
                std::process::exit(1);
            });
    let service_info = ServiceInfo {
        name: OsString::from(service_name),
        display_name: OsString::from(display_name),
        service_type: ServiceType::OWN_PROCESS,
        start_type: ServiceStartType::AutoStart,
        error_control: ServiceErrorControl::Normal,
        executable_path: exe_path,
        launch_arguments: vec![
            OsString::from("--service"),
            OsString::from("--service-name"),
            OsString::from(service_name),
            OsString::from("--config"),
            config_path.into_os_string(),
        ],
        dependencies: vec![],
        account_name: None,
        account_password: None,
    };
    manager
        .create_service(&service_info, ServiceAccess::CHANGE_CONFIG)
        .unwrap_or_else(|e| {
            eprintln!("failed to create service {}: {}", service_name, e);
            std::process::exit(1);
        });
    println!("service {} installed", service_name);
}

#[cfg(not(windows))]
fn install_windows_service(_service_name: &str, _display_name: &str, _config_path: &str) {
    eprintln!("Windows service installation is only supported on Windows");
    std::process::exit(1);
}

#[cfg(windows)]
fn uninstall_windows_service(service_name: &str) {
    use windows_service::service::ServiceAccess;
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)
        .unwrap_or_else(|e| {
            eprintln!("failed to open service manager: {}", e);
            std::process::exit(1);
        });
    let service = manager
        .open_service(service_name, ServiceAccess::STOP | ServiceAccess::DELETE)
        .unwrap_or_else(|e| {
            eprintln!("failed to open service {}: {}", service_name, e);
            std::process::exit(1);
        });
    let _ = service.stop();
    service.delete().unwrap_or_else(|e| {
        eprintln!("failed to delete service {}: {}", service_name, e);
        std::process::exit(1);
    });
    println!("service {} uninstalled", service_name);
}

#[cfg(not(windows))]
fn uninstall_windows_service(_service_name: &str) {
    eprintln!("Windows service uninstallation is only supported on Windows");
    std::process::exit(1);
}

#[cfg(windows)]
windows_service::define_windows_service!(ffi_service_main, windows_service_main);

#[cfg(windows)]
static SERVICE_STOP_TX: std::sync::OnceLock<
    std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
> = std::sync::OnceLock::new();

#[cfg(windows)]
static SERVICE_CONTEXT: std::sync::OnceLock<ServiceContext> = std::sync::OnceLock::new();

#[cfg(windows)]
struct ServiceContext {
    service_name: String,
    config_path: String,
}

#[cfg(windows)]
async fn run_windows_service(service_name: String, config_path: String, _config: ServerConfig) {
    let _ = SERVICE_CONTEXT.set(ServiceContext {
        service_name: service_name.clone(),
        config_path,
    });
    if let Err(e) = windows_service::service_dispatcher::start(service_name, ffi_service_main) {
        eprintln!("failed to start Windows service dispatcher: {}", e);
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
async fn run_windows_service(_service_name: String, _config_path: String, config: ServerConfig) {
    run_server_until(config, std::future::pending::<()>()).await;
}

#[cfg(windows)]
fn windows_service_main(arguments: Vec<std::ffi::OsString>) {
    if let Err(e) = run_windows_service_main(arguments) {
        eprintln!("Windows service error: {}", e);
    }
}

#[cfg(windows)]
fn run_windows_service_main(
    arguments: Vec<std::ffi::OsString>,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;
    use windows_service::service::{
        ServiceControl, ServiceControlAccept, ServiceExitCode, ServiceState, ServiceStatus,
        ServiceType,
    };
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};

    let config_path = parse_service_config_path(&arguments);
    let service_name = parse_service_name(&arguments);
    let mut config_load_error = None;
    let config = match ServerConfig::from_yaml_file(std::path::Path::new(&config_path)) {
        Ok(config) => config,
        Err(e) => {
            config_load_error = Some(e);
            default_service_config(&config_path)
        }
    };
    let log_file = service_log_file(&config_path, &config);
    if let Err(e) = init_file_tracing(&config.log.level, &log_file) {
        eprintln!(
            "failed to initialize Windows service log {}: {}",
            log_file.display(),
            e
        );
    }
    if let Some(e) = config_load_error {
        tracing::warn!("Config load error: {}, using service defaults", e);
    }
    tracing::info!(
        service_name = %service_name,
        config_path = %config_path,
        log_file = %log_file.display(),
        "starting Taibai Windows service"
    );

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stop_slot = SERVICE_STOP_TX.get_or_init(|| std::sync::Mutex::new(None));
    *stop_slot.lock().unwrap() = Some(stop_tx);

    let status_handle =
        service_control_handler::register(
            service_name,
            move |control_event| match control_event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    if let Some(slot) = SERVICE_STOP_TX.get() {
                        if let Some(tx) = slot.lock().unwrap().take() {
                            let _ = tx.send(());
                        }
                    }
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            },
        )?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StartPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(30),
        process_id: None,
    })?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Running,
        controls_accepted: ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(0),
        process_id: None,
    })?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(run_server_until(config, async {
        let _ = stop_rx.await;
        tracing::info!("received Windows service stop signal");
    }));

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::StopPending,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 1,
        wait_hint: Duration::from_secs(30),
        process_id: None,
    })?;

    status_handle.set_service_status(ServiceStatus {
        service_type: ServiceType::OWN_PROCESS,
        current_state: ServiceState::Stopped,
        controls_accepted: ServiceControlAccept::empty(),
        exit_code: ServiceExitCode::Win32(0),
        checkpoint: 0,
        wait_hint: Duration::from_secs(0),
        process_id: None,
    })?;

    Ok(())
}

#[cfg(windows)]
fn parse_service_config_path(arguments: &[std::ffi::OsString]) -> String {
    arguments
        .windows(2)
        .find_map(|pair| {
            (pair[0].to_string_lossy() == "--config").then(|| pair[1].to_string_lossy().to_string())
        })
        .or_else(|| {
            SERVICE_CONTEXT
                .get()
                .map(|context| context.config_path.clone())
        })
        .unwrap_or_else(|| "conf/windows.yaml".to_string())
}

#[cfg(windows)]
fn parse_service_name(arguments: &[std::ffi::OsString]) -> String {
    arguments
        .windows(2)
        .find_map(|pair| {
            (pair[0].to_string_lossy() == "--service-name")
                .then(|| pair[1].to_string_lossy().to_string())
        })
        .or_else(|| {
            SERVICE_CONTEXT
                .get()
                .map(|context| context.service_name.clone())
        })
        .unwrap_or_else(|| "TaibaiDB".to_string())
}

#[cfg(windows)]
fn default_service_config(config_path: &str) -> ServerConfig {
    let mut config = ServerConfig::default();
    if let Some(install_dir) = install_dir_from_config_path(config_path) {
        config.storage.data_dir = install_dir.join("data").to_string_lossy().to_string();
        config.log.file = Some(
            install_dir
                .join("logs")
                .join("taibai.log")
                .to_string_lossy()
                .to_string(),
        );
        config.log.query_file = Some(
            install_dir
                .join("logs")
                .join("taibai-query.log")
                .to_string_lossy()
                .to_string(),
        );
    }
    config
}

#[cfg(windows)]
fn service_log_file(config_path: &str, config: &ServerConfig) -> std::path::PathBuf {
    if let Some(log_file) = config.log.file.as_deref() {
        return std::path::PathBuf::from(log_file);
    }

    install_dir_from_config_path(config_path)
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("logs")
        .join("taibai.log")
}

#[cfg(windows)]
fn install_dir_from_config_path(config_path: &str) -> Option<std::path::PathBuf> {
    let config_path = std::path::Path::new(config_path);
    let config_dir = config_path.parent()?;
    config_dir.parent().map(std::path::Path::to_path_buf)
}
