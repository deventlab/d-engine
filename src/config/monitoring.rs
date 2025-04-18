//! Monitoring configuration settings for the application.
//!
//! This module defines configuration parameters related to system monitoring,
//! particularly for Prometheus metrics collection.
use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

/// Configuration structure for monitoring features
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MonitoringConfig {
    /// Enables Prometheus metrics endpoint when set to true
    /// Default value: false (via default_prometheus_enabled)
    #[serde(default = "default_prometheus_enabled")]
    pub prometheus_enabled: bool,

    /// TCP port number for Prometheus metrics endpoint
    /// Default value: 8080 (via default_prometheus_port)
    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
}
impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            prometheus_enabled: default_prometheus_enabled(),
            prometheus_port: default_prometheus_port(),
        }
    }
}
impl MonitoringConfig {
    /// Validates monitoring configuration
    /// # Errors
    /// Returns `Error::InvalidConfig` when:
    /// - Prometheus is enabled with invalid port
    /// - Port conflicts with well-known services
    pub fn validate(&self) -> Result<()> {
        if self.prometheus_enabled {
            // Validate port range
            if self.prometheus_port == 0 {
                return Err(Error::Config(ConfigError::Message(
                    "prometheus_port cannot be 0 when enabled".into(),
                )));
            }

            // Check privileged ports (requires root)
            if self.prometheus_port < 1024 {
                return Err(Error::Config(ConfigError::Message(format!(
                    "prometheus_port {} is a privileged port (requires root)",
                    self.prometheus_port
                ))));
            }

            // Verify port availability (runtime check)
            #[cfg(not(test))]
            {
                use std::net::TcpListener;
                if let Err(e) = TcpListener::bind(("0.0.0.0", self.prometheus_port)) {
                    return Err(Error::Config(ConfigError::Message(format!(
                        "prometheus_port {} unavailable: {}",
                        self.prometheus_port, e
                    ))));
                }
            }
        } else {
            // Warn about unused port configuration
            #[cfg(debug_assertions)]
            if self.prometheus_port != default_prometheus_port() {
                log::warn!(
                    "prometheus_port configured to {} but monitoring is disabled",
                    self.prometheus_port
                );
            }
        }

        Ok(())
    }
}
fn default_prometheus_enabled() -> bool {
    false
}

fn default_prometheus_port() -> u16 {
    8080
}
