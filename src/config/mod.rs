//! Configuration management module for distributed Raft cluster.
//!
//! Provides hierarchical configuration loading from multiple sources with priority:
//! 1. Default values (hardcoded)
//! 2. Main config file
//! 3. Included config files
//! 4. Environment-specific config
//! 5. Node-specific cluster config
//! 6. Local overrides
//! 7. Environment variables (highest priority)
//!

mod cluster;
mod monitoring;
mod network;
mod raft;
mod retry;
mod tls;
pub use cluster::*;
pub use monitoring::*;
pub use network::*;
pub use raft::*;
pub use retry::*;
pub use tls::*;

//---
use crate::{Error, Result};
use config::{Config, Environment, File};
use serde::Deserialize;
use std::{collections::HashMap, env};

#[derive(Debug, Deserialize)]
struct MainConfig {
    /// List of configuration files to include
    includes: Vec<String>,
    /// Environment-to-config mapping (e.g., "production" -> "prod-config.toml")
    #[serde(rename = "env_config")]
    environments: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    /// Cluster topology and node configuration
    pub cluster: ClusterConfig,
    /// Metrics and monitoring settings
    pub monitoring: MonitoringConfig,
    /// Network communication parameters
    pub network: NetworkConfig,
    /// Core Raft algorithm parameters
    pub raft: RaftConfig,
    /// Retry policies for distributed operations
    pub retry: RetryPolicies,
    /// TLS/SSL security configuration
    pub tls: TlsConfig,
}

impl Settings {
    /// Load configuration from multiple sources with priority:
    /// 1. Base config files
    /// 2. Environment-specific config
    /// 3. Node-specific cluster config
    /// 4. Local overrides
    /// 5. Environment variables
    ///
    /// # Arguments
    /// * `cluster_path` - Optional path to node-specific cluster configuration
    ///
    /// # Returns
    /// Merged configuration with proper priority ordering
    pub fn load(cluster_path: Option<&str>) -> Result<Self> {
        let mut config = Config::builder();

        // 1. Load main config
        let main_config: MainConfig = Config::builder()
            .add_source(File::with_name("config/main"))
            .build()?
            .try_deserialize()?;

        // 2. Load base configs
        for path in &main_config.includes {
            config = config.add_source(File::with_name(&format!("config/{}", path)));
        }

        // 3. Overwrite with node cluster config
        if let Some(custom_cluster) = cluster_path {
            config = config.add_source(File::with_name(custom_cluster).required(true));
        }

        // 4. Environment overlay
        if let Ok(env) = env::var("RAFT_ENV") {
            if let Some(env_path) = main_config.environments.get(&env) {
                config = config.add_source(File::with_name(&format!("config/{}", env_path)));
            }
        }
        if let Ok(path) = env::var("CONFIG_PATH") {
            config = config.add_source(File::with_name(&format!("{}", path)));
        }

        // 5. Local overrides
        config = config.add_source(File::with_name("config/local").required(false));

        // 6. Environment variables (highest priority)
        config = config.add_source(
            Environment::with_prefix("RAFT")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        );

        config
            .build()?
            .try_deserialize()
            .map_err(|e| Error::ConfigError(e.into()))
    }
}
