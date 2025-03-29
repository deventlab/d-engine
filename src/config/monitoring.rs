use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MonitoringConfig {
    #[serde(default = "default_prometheus_enabled")]
    pub prometheus_enabled: bool,

    #[serde(default = "default_prometheus_port")]
    pub prometheus_port: u16,
}

fn default_prometheus_enabled() -> bool {
    false
}

fn default_prometheus_port() -> u16 {
    8080
}
