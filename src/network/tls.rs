// ============================================================================
// NeonDB tls.rs — TLS configuration helpers
//
// Provides:
//   - `load_tls_config`    — load a PEM cert chain + PKCS8 key from disk
//   - `generate_self_signed` — create an ephemeral self-signed cert via rcgen
//
// Both return types usable directly with tokio-rustls.
// ============================================================================

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::error::{NeonDBError, Result};

/// Load a TLS `ServerConfig` from PEM-encoded certificate chain and PKCS8
/// private key files on disk.
///
/// # Errors
/// Returns `NeonDBError` when either file cannot be read, the cert chain is
/// empty, or the private key cannot be parsed.
pub fn load_tls_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>> {
    // ── Load certificate chain ────────────────────────────────────────────────
    let cert_file = std::fs::File::open(cert_path).map_err(|e| {
        NeonDBError::network_error(format!(
            "TLS: cannot open cert file '{}': {}",
            cert_path.display(),
            e
        ))
    })?;
    let mut cert_reader = BufReader::new(cert_file);
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|e| NeonDBError::network_error(format!("TLS: failed to parse cert PEM: {}", e)))?;

    if certs.is_empty() {
        return Err(NeonDBError::network_error(
            "TLS: cert file contains no valid certificates".to_string(),
        ));
    }

    // ── Load private key ──────────────────────────────────────────────────────
    let key_file = std::fs::File::open(key_path).map_err(|e| {
        NeonDBError::network_error(format!(
            "TLS: cannot open key file '{}': {}",
            key_path.display(),
            e
        ))
    })?;
    let mut key_reader = BufReader::new(key_file);

    // Try the generic private_key reader first (handles PKCS8 and RSA).
    let private_key: PrivateKeyDer<'static> = {
        let result = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| NeonDBError::network_error(format!("TLS: failed to parse key PEM: {}", e)))?;
        match result {
            Some(k) => k,
            None => {
                // Fallback: try PKCS8-specific reader
                let kf = std::fs::File::open(key_path).map_err(|e| {
                    NeonDBError::network_error(format!("TLS: cannot open key file: {}", e))
                })?;
                let mut kr = BufReader::new(kf);
                let mut pkcs8_keys: Vec<PrivateKeyDer<'static>> = Vec::new();
                for item in rustls_pemfile::pkcs8_private_keys(&mut kr) {
                    if let Ok(k) = item {
                        pkcs8_keys.push(PrivateKeyDer::Pkcs8(k));
                    }
                }
                if !pkcs8_keys.is_empty() {
                    pkcs8_keys.remove(0)
                } else {
                    return Err(NeonDBError::network_error(
                        "TLS: key file contains no valid private key".to_string(),
                    ));
                }
            }
        }
    };

    // ── Build ServerConfig ────────────────────────────────────────────────────
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .map_err(|e| NeonDBError::network_error(format!("TLS: invalid cert/key pair: {}", e)))?;

    Ok(Arc::new(config))
}

/// Generate an ephemeral self-signed certificate for localhost testing.
///
/// Returns `(cert_pem_bytes, key_pem_bytes)`.  Both are valid PEM strings.
/// The certificate is valid for DNS `localhost` and IP `127.0.0.1`.
///
/// This is intentionally NOT suitable for production.  For production, use
/// `load_tls_config` with a real certificate from a trusted CA (e.g. Let's
/// Encrypt).
pub fn generate_self_signed() -> (Vec<u8>, Vec<u8>) {
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];

    let cert = rcgen::generate_simple_self_signed(subject_alt_names)
        .expect("rcgen: failed to generate self-signed certificate");

    let cert_pem = cert.serialize_pem()
        .expect("rcgen: failed to serialize cert to PEM")
        .into_bytes();
    let key_pem = cert.serialize_private_key_pem().into_bytes();

    (cert_pem, key_pem)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ── Helpers ───────────────────────────────────────────────────────────────

    /// Write PEM bytes to a named temp file and return its path.
    fn write_temp_pem(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).expect("create temp file");
        f.write_all(bytes).expect("write temp file");
        path
    }

    // ── Test 1: self-signed roundtrip ─────────────────────────────────────────

    #[test]
    fn test_generate_self_signed_roundtrip() {
        let (cert_pem, key_pem) = generate_self_signed();

        // Must be non-empty PEM blocks
        assert!(!cert_pem.is_empty(), "cert_pem is empty");
        assert!(!key_pem.is_empty(),  "key_pem is empty");

        let cert_str = std::str::from_utf8(&cert_pem).expect("cert_pem is not UTF-8");
        let key_str  = std::str::from_utf8(&key_pem).expect("key_pem is not UTF-8");

        assert!(cert_str.contains("-----BEGIN CERTIFICATE-----"),   "cert missing PEM header");
        assert!(cert_str.contains("-----END CERTIFICATE-----"),     "cert missing PEM footer");
        assert!(key_str.contains("PRIVATE KEY"),                    "key missing PEM header");
    }

    // ── Test 2: load_tls_config succeeds with a valid self-signed cert ────────

    #[test]
    fn test_load_tls_config_self_signed_succeeds() {
        let (cert_pem, key_pem) = generate_self_signed();

        let cert_path = write_temp_pem("neondb_test_cert.pem", &cert_pem);
        let key_path  = write_temp_pem("neondb_test_key.pem",  &key_pem);

        let result = load_tls_config(&cert_path, &key_path);
        assert!(result.is_ok(), "load_tls_config failed: {:?}", result.err());
    }

    // ── Test 3: load_tls_config returns error for a missing cert file ─────────

    #[test]
    fn test_load_tls_config_bad_cert_path_returns_error() {
        let (_cert_pem, key_pem) = generate_self_signed();
        let key_path  = write_temp_pem("neondb_test_key2.pem", &key_pem);

        let missing_cert = std::path::Path::new("/nonexistent/path/cert.pem");
        let result = load_tls_config(missing_cert, &key_path);

        assert!(result.is_err(), "expected error for missing cert path");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("TLS"),
            "error message should mention TLS, got: {}", err_msg
        );
    }

    // ── Test 4: load_tls_config returns error for a missing key file ──────────

    #[test]
    fn test_load_tls_config_bad_key_path_returns_error() {
        let (cert_pem, _key_pem) = generate_self_signed();
        let cert_path = write_temp_pem("neondb_test_cert2.pem", &cert_pem);

        let missing_key = std::path::Path::new("/nonexistent/path/key.pem");
        let result = load_tls_config(&cert_path, missing_key);

        assert!(result.is_err(), "expected error for missing key path");
    }
}
