use std::path::PathBuf;
use std::sync::Arc;

use crate::common::error::{Result, TaibaiError};

/// TLS configuration for the server.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to the PEM-encoded certificate file.
    pub cert_path: PathBuf,
    /// Path to the PEM-encoded private key file.
    pub key_path: PathBuf,
}

impl TlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }

    /// Validate that the cert and key files exist.
    pub fn validate(&self) -> Result<()> {
        if !self.cert_path.exists() {
            return Err(TaibaiError::Config(format!(
                "TLS certificate file not found: {:?}",
                self.cert_path
            )));
        }
        if !self.key_path.exists() {
            return Err(TaibaiError::Config(format!(
                "TLS private key file not found: {:?}",
                self.key_path
            )));
        }
        Ok(())
    }

    /// Load the TLS configuration and create a rustls server config.
    pub fn load_server_config(&self) -> Result<Arc<rustls::ServerConfig>> {
        use rustls_pemfile::{certs, private_key};
        use std::io::BufReader;

        // Load certificate chain
        let cert_file = std::fs::File::open(&self.cert_path)
            .map_err(|e| TaibaiError::Config(format!("Failed to open cert file: {}", e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        let cert_chain: Vec<rustls::pki_types::CertificateDer> = certs(&mut cert_reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| TaibaiError::Config(format!("Failed to read certificates: {}", e)))?;

        if cert_chain.is_empty() {
            return Err(TaibaiError::Config(
                "No certificates found in cert file".into(),
            ));
        }

        // Load private key
        let key_file = std::fs::File::open(&self.key_path)
            .map_err(|e| TaibaiError::Config(format!("Failed to open key file: {}", e)))?;
        let mut key_reader = BufReader::new(key_file);
        let key = private_key(&mut key_reader)
            .map_err(|e| TaibaiError::Config(format!("Failed to read private key: {}", e)))?
            .ok_or_else(|| TaibaiError::Config("No private key found in key file".into()))?;

        // Create server config
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| TaibaiError::Config(format!("Failed to create TLS config: {}", e)))?;

        Ok(Arc::new(server_config))
    }
}

/// Whether to use TLS for incoming connections.
#[derive(Debug, Clone, Default)]
pub enum TlsMode {
    /// No TLS.
    #[default]
    Disable,
    /// TLS enabled with the given configuration.
    Enable(TlsConfig),
}

#[cfg(test)]
#[path = "tls_tests.rs"]
mod tests;
