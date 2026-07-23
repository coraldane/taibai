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
    assert_eq!(config.server.max_connections, 1024);
    assert!(config.server.max_concurrent_requests >= 4);
    assert_eq!(config.server.connection_idle_timeout_secs, 300);
    assert_eq!(config.server.tls_handshake_timeout_secs, 10);
}
