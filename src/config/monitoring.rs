use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MonitoringConfig {
    #[serde(default = "default_prometheus_enabled")]
    pub prometheus_enabled: bool,

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
                return Err(Error::InvalidConfig("prometheus_port cannot be 0 when enabled".into()));
            }

            // Check privileged ports (requires root)
            if self.prometheus_port < 1024 {
                return Err(Error::InvalidConfig(format!(
                    "prometheus_port {} is a privileged port (requires root)",
                    self.prometheus_port
                )));
            }

            // Verify port availability (runtime check)
            #[cfg(not(test))]
            {
                use std::net::TcpListener;
                if let Err(e) = TcpListener::bind(("0.0.0.0", self.prometheus_port)) {
                    return Err(Error::InvalidConfig(format!(
                        "prometheus_port {} unavailable: {}",
                        self.prometheus_port, e
                    )));
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
