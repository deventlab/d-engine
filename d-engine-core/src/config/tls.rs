use std::fs;
use std::path::Path;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
pub struct TlsConfig {
    /// Enables TLS encryption for network communication
    /// Default: false (disabled)
    #[serde(default = "default_enable_tls")]
    pub enable_tls: bool,

    /// Automatically generates self-signed certificates on startup
    /// Default: false (requires pre-configured certificates)
    #[serde(default = "default_generate_self_signed")]
    pub generate_self_signed_certificates: bool,

    /// Path to Certificate Authority root certificate
    /// Default: "/etc/ssl/certs/ca.pem"
    #[serde(default = "default_ca_path")]
    pub certificate_authority_root_path: String,

    /// Server certificate chain path in PEM format
    /// Default: "./certs/server.pem"
    #[serde(default = "default_server_cert_path")]
    pub server_certificate_path: String,

    /// Server private key path in PEM format
    /// Default: "./certs/server.key"
    #[serde(default = "default_server_key_path")]
    pub server_private_key_path: String,

    /// Client CA certificate path for mTLS authentication
    /// Default: "/etc/ssl/certs/ca.pem"
    #[serde(default = "default_client_ca_path")]
    pub client_certificate_authority_root_path: String,

    /// Enables mutual TLS (mTLS) for bidirectional authentication
    /// Default: false (server-side TLS only)
    #[serde(default = "default_enable_mtls")]
    pub enable_mtls: bool,
}
impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enable_tls: default_enable_tls(),
            generate_self_signed_certificates: default_generate_self_signed(),
            certificate_authority_root_path: default_ca_path(),
            server_certificate_path: default_server_cert_path(),
            server_private_key_path: default_server_key_path(),
            client_certificate_authority_root_path: default_client_ca_path(),
            enable_mtls: default_enable_mtls(),
        }
    }
}

impl TlsConfig {
    /// Validates TLS configuration consistency and file existence
    /// # Errors
    /// Returns `Error::InvalidConfig` when:
    /// - mTLS is enabled without base TLS
    /// - Required certificate files are missing
    /// - Self-signed generation conflicts with existing paths
    /// - Invalid certificate file permissions
    pub fn validate(&self) -> Result<()> {
        // Check mTLS dependency
        if self.enable_mtls && !self.enable_tls {
            return Err(Error::Config(ConfigError::Message(
                "mTLS requires enable_tls to be true".into(),
            )));
        }

        // No further validation needed if TLS is disabled
        if !self.enable_tls {
            return Ok(());
        }

        // Self-signed certificate logic
        if self.generate_self_signed_certificates {
            if !self.server_certificate_path.is_empty() || !self.server_private_key_path.is_empty()
            {
                return Err(Error::Config(ConfigError::Message(
                    "Cannot specify certificate paths with generate_self_signed_certificates=true"
                        .into(),
                )));
            }

            if self.enable_mtls {
                self.validate_cert_file(
                    &self.client_certificate_authority_root_path,
                    "client CA certificate",
                )?;
            }

            return Ok(());
        }

        // Validate certificate files
        self.validate_cert_file(&self.server_certificate_path, "server certificate")?;
        self.validate_key_file(&self.server_private_key_path, "server private key")?;
        self.validate_cert_file(&self.certificate_authority_root_path, "CA certificate")?;

        // mTLS requires additional client CA verification
        if self.enable_mtls {
            self.validate_cert_file(
                &self.client_certificate_authority_root_path,
                "client CA certificate",
            )?;
        }

        Ok(())
    }

    /// Validates a certificate file existence and readability
    fn validate_cert_file(
        &self,
        path: &str,
        name: &str,
    ) -> Result<()> {
        let path = Path::new(path);

        if path.exists() {
            {
                // Check file readability
                fs::File::open(path).map_err(|e| {
                    Error::Config(ConfigError::Message(format!(
                        "{} file {} is unreadable: {}",
                        name,
                        path.display(),
                        e
                    )))
                })?;
            }
            Ok(())
        } else {
            Err(Error::Config(ConfigError::Message(format!(
                "{} file {} not found",
                name,
                path.display()
            ))))
        }
    }

    /// Validates a private key file existence and permissions
    fn validate_key_file(
        &self,
        path: &str,
        name: &str,
    ) -> Result<()> {
        let path = Path::new(path);

        if path.exists() {
            {
                // Check key file permissions (should be 600)
                let metadata = fs::metadata(path).map_err(|e| {
                    Error::Config(ConfigError::Message(format!(
                        "Cannot access {} permissions: {}",
                        path.display(),
                        e
                    )))
                })?;

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = metadata.permissions().mode();
                    if mode & 0o777 != 0o600 {
                        return Err(Error::Config(ConfigError::Message(format!(
                            "Insecure permissions {:o} for {} (should be 600)",
                            mode & 0o777,
                            path.display()
                        ))));
                    }
                }
            }
            Ok(())
        } else {
            Err(Error::Config(ConfigError::Message(format!(
                "{} file {} not found",
                name,
                path.display()
            ))))
        }
    }
}
// Default implementations
fn default_enable_tls() -> bool {
    false
}
fn default_generate_self_signed() -> bool {
    false
}
fn default_ca_path() -> String {
    "/etc/ssl/certs/ca.pem".into()
}
fn default_server_cert_path() -> String {
    "./certs/server.pem".into()
}
fn default_server_key_path() -> String {
    "./certs/server.key".into()
}
fn default_client_ca_path() -> String {
    "/etc/ssl/certs/ca.pem".into()
}
fn default_enable_mtls() -> bool {
    false
}
