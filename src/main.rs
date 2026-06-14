mod common;
mod net;
mod protocol;
mod sql;
mod storage;
mod types;
mod version;

use std::sync::Arc;

use common::config::{RunMode, ServerConfig};
use net::server::Server;
use sql::execution::Engine;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

#[tokio::main]
async fn main() {
    init_tracing();

    tracing::info!("Taibai {}", version::get_full_version());

    match RunMode::from_args() {
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
        let _ = tokio::signal::ctrl_c().await;
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

    if let Err(e) = std::fs::create_dir_all(&config.storage.data_dir) {
        tracing::error!(error = %e, "failed to create data directory");
        std::process::exit(1);
    }

    let engine = match Engine::new_with_change_stream_retention(
        std::path::Path::new(&config.storage.data_dir),
        &config.storage.default_database,
        config.change_stream.retention_events,
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize engine");
            std::process::exit(1);
        }
    };

    let config = Arc::new(config);
    let engine = Arc::new(engine);
    let _ = rayon::ThreadPoolBuilder::new().build_global();
    let mut auto_compactor = storage::core::auto_compact::AutoCompactorHandle::start(
        engine.clone(),
        config.auto_compact.clone(),
    );
    let server = Server::new(config, engine.clone());

    if let Err(e) = server.run_with_shutdown(shutdown).await {
        tracing::error!(error = %e, "server error");
    }

    if let Some(handle) = auto_compactor.as_mut() {
        handle.stop();
    }

    match engine.flush_all_buffers() {
        Ok(n) => tracing::info!(rows_flushed = n, "all active segments sealed"),
        Err(e) => tracing::error!(error = %e, "failed to flush segments on shutdown"),
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
            let _ = write!(line, "{:?}", data[row]);
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
async fn run_windows_service(service_name: String, _config_path: String, _config: ServerConfig) {
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

    init_tracing();

    let config_path = parse_service_config_path(&arguments);
    let service_name = parse_service_name(&arguments);
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

    let config =
        ServerConfig::from_yaml_file(std::path::Path::new(&config_path)).unwrap_or_else(|e| {
            tracing::warn!("Config load error: {}, using defaults", e);
            ServerConfig::default()
        });

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
        .unwrap_or_else(|| "TaibaiDB".to_string())
}
