use super::{PerformanceBaselineConfig, ServerConfig};

#[test]
fn minimal_config_uses_defaults_for_optional_sections() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.yaml");
    std::fs::write(
        &config_path,
        r#"
server:
  host: "127.0.0.1"
  port: 5252
storage:
  data_dir: "./data"
"#,
    )
    .unwrap();

    let config = ServerConfig::from_yaml_file(&config_path).unwrap();

    assert_eq!(config.listen_addr(), "127.0.0.1:5252");
    assert_eq!(
        config.storage.data_dir,
        temp.path().join("data").to_string_lossy()
    );
    assert_eq!(config.storage.default_database, "main");
    assert_eq!(config.buffer.batch_size, 4096);
    assert!(config.compression.enabled);
    assert_eq!(config.compression.bulk_zstd_level, 3);
    assert_eq!(config.storage.reader_cache_capacity, 1024);
    assert!(!config.tls.enabled);
    assert_eq!(config.log.level, "info");
    assert!(!config.log.insert_profile);
    assert!(config.performance.max_entries > 0);
}

#[test]
fn config_overrides_storage_caches_bulk_level_and_insert_profile() {
    let temp = tempfile::tempdir().unwrap();
    let config_path = temp.path().join("config.yaml");
    std::fs::write(
        &config_path,
        r#"
storage:
  data_dir: "./data"
  reader_cache_capacity: 4096
compression:
  enabled: true
  bulk_zstd_level: 3
log:
  level: "debug"
  insert_profile: true
"#,
    )
    .unwrap();

    let config = ServerConfig::from_yaml_file(&config_path).unwrap();

    assert_eq!(config.storage.reader_cache_capacity, 4096);
    assert_eq!(config.compression.bulk_zstd_level, 3);
    assert_eq!(config.log.level, "debug");
    assert!(config.log.insert_profile);
}

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
performance:
  max_entries: 0
  warmup_samples: 0
  ewma_alpha: 2.0
  elapsed_regression_ratio: 0.5
  elapsed_regression_min_ms: 7.0
  memory_regression_ratio: 1.8
  memory_regression_min_mb: 2
  spill_regression_ratio: 2.5
  row_error_regression_ratio: 3.0
  row_error_regression_min: 4.0
  environment: "  "
  version_label: " release-candidate "
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
    assert_eq!(config.server.max_connections, 1024);
    assert!(config.server.max_concurrent_requests >= 4);
    assert_eq!(config.server.connection_idle_timeout_secs, 300);
    assert_eq!(config.server.bulk_import_idle_timeout_secs, 3600);
    assert_eq!(config.server.tls_handshake_timeout_secs, 10);

    let performance = PerformanceBaselineConfig::from(&config.performance);
    assert_eq!(performance.max_entries, 1);
    assert_eq!(performance.warmup_samples, 1);
    assert_eq!(performance.ewma_alpha, 1.0);
    assert_eq!(performance.elapsed_regression_ratio, 1.0);
    assert_eq!(performance.elapsed_regression_min_ms, 7.0);
    assert_eq!(performance.memory_regression_ratio, 1.8);
    assert_eq!(performance.memory_regression_min_bytes, 2 * 1024 * 1024);
    assert_eq!(performance.spill_regression_ratio, 2.5);
    assert_eq!(performance.row_error_regression_ratio, 3.0);
    assert_eq!(performance.row_error_regression_min, 4.0);
    assert_eq!(performance.environment, "default");
    assert_eq!(performance.version, "release-candidate");
}
