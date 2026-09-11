use super::*;
use std::io::Write;

#[test]
fn test_tls_config_validate_missing_cert() {
    let config = TlsConfig::new("/nonexistent/cert.pem", "/nonexistent/key.pem");
    assert!(config.validate().is_err());
}

#[test]
fn test_tls_config_validate_existing_files() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    let mut f = std::fs::File::create(&cert_path).unwrap();
    f.write_all(b"fake cert").unwrap();
    let mut f = std::fs::File::create(&key_path).unwrap();
    f.write_all(b"fake key").unwrap();

    let config = TlsConfig::new(&cert_path, &key_path);
    assert!(config.validate().is_ok());
}

#[test]
fn test_tls_mode_default() {
    let mode = TlsMode::default();
    assert!(matches!(mode, TlsMode::Disable));
}
