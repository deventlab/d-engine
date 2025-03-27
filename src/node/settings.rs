use config::{Config, Environment, File};
use serde::Deserialize;
use std::env;

use crate::{grpc::rpc_service::NodeMeta, Error, Result};

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct ServerSettings {
    pub node_id: u32,
    pub listen_address: String,
    pub initial_cluster: Vec<NodeMeta>,
    pub db_root_dir: String,
    pub log_dir: String,

    pub prometheus_metrics_port: u16,
    pub prometheus_enabled: bool,

    #[cfg(test)]
    pub tokio_console_enabled: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct RpcConnectionSettings {
    pub connect_timeout_in_ms: u64,
    pub request_timeout_in_ms: u64,
    pub concurrency_limit_per_connection: usize,
    pub max_concurrent_streams: u32,
    pub tcp_nodelay: bool,
    pub http2_adaptive_window: bool,
    pub tcp_keepalive_in_secs: u64,
    pub http2_keep_alive_interval_in_secs: u64,
    pub http2_keep_alive_timeout_in_secs: u64,
    pub max_frame_size: u32,
    pub initial_connection_window_size: u32,
    pub initial_stream_window_size: u32,
    pub buffer_size: usize,
    pub enable_tls: bool,
    pub generate_self_signed_certificates: bool,
    pub certificate_authority_root_path: String,

    pub server_certificate_path: String,
    pub server_private_key_path: String,
    pub client_certificate_authority_root_path: String,
    pub enable_mtls: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct ElectionTimeoutControlSettings {
    // pub election_timeout_control: bool,
    pub is_on: bool,

    pub check_cpu: bool,
    pub total_cpu_threshold: f32,

    pub check_system_load: bool,
    pub system_load_threshold: f64,
    // pub election_timeout_legal_min: u64,
    // pub election_timeout_legal_max: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct RaftSettings {
    pub learner_raft_log_gap: u64,
    pub rpc_append_entries_clock_in_ms: u64,
    pub rpc_append_entries_in_batch_threshold: usize,
    pub rpc_append_entries_batch_process_delay_in_ms: u64,
    pub rpc_append_entries_max_retries: usize,
    pub rpc_append_entries_timeout_duration_in_ms: u64,
    pub rpc_append_entries_exponential_backoff_duration_in_ms: u64,

    pub append_entries_max_entries_per_replication: u64,

    pub election_timeout_min: u64,
    pub election_timeout_max: u64,

    pub rpc_election_max_retries: usize,
    pub rpc_election_timeout_duration_in_ms: u64,
    pub rpc_election_exponential_backoff_duration_in_ms: u64,
    pub rpc_peer_connectinon_monitor_interval_in_sec: u64,

    pub internal_rpc_client_request_id: u32,

    pub leader_propose_timeout_duration_in_ms: u64,

    pub cluster_membership_sync_max_retries: usize,
    pub cluster_membership_sync_exponential_backoff_duration_in_ms: u64,
    pub cluster_membership_sync_timeout_duration_in_ms: u64,
    pub cluster_healtcheck_max_retries: usize,
    pub cluster_healtcheck_timeout_duration_in_ms: u64,
    pub cluster_healtcheck_exponential_backoff_duration_in_ms: u64,
    pub cluster_healthcheck_probe_service_name: String,

    pub general_raft_timeout_duration_in_ms: u64,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct CommitHandlerSettings {
    pub batch_size_threshold: u64,
    pub commit_handle_interval_in_ms: u64,
    pub max_entries_per_chunk: usize,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[allow(unused)]
pub struct Settings {
    pub cluster: ServerSettings,
    pub election_timeout_controller_settings: ElectionTimeoutControlSettings,
    pub raft_settings: RaftSettings,
    pub rpc_connection_settings: RpcConnectionSettings,
    pub commit_handler_settings: CommitHandlerSettings,
}

impl Settings {
    pub fn new() -> Result<Self> {
        let config_path = env::var("CONFIG_PATH").unwrap_or_else(|_| "dev".into());

        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name("config/default"))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(File::with_name(&format!("{}", config_path)).required(false))
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            .add_source(File::with_name("config/local").required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("app"))
            .build()
            .map_err(|e| Error::ConfigError(e))?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize().map_err(|e| Error::ConfigError(e))
    }

    pub fn from_file(config_path: &str) -> Result<Self> {
        let s = Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(File::with_name("config/default"))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(File::with_name(&format!("{}", config_path)).required(false))
            // Add in a local configuration file
            // This file shouldn't be checked in to git
            .add_source(File::with_name("config/local").required(false))
            // Add in settings from the environment (with a prefix of APP)
            // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("app"))
            .build()
            .map_err(|e| Error::ConfigError(e))?;

        // You can deserialize (and thus freeze) the entire configuration as
        s.try_deserialize().map_err(|e| Error::ConfigError(e))
    }
}
