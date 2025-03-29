use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
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
