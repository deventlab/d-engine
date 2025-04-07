//! Configuration management module for distributed Raft consensus engine.
//!
//! Provides hierarchical configuration loading and validation with:
//! - Default values as code base
//! - Environment variable overrides
//! - Configuration file support
//! - Component-wise validation
mod cluster;
use std::fmt::Debug;
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

#[cfg(test)]
mod config_test;
#[cfg(test)]
mod raft_test;

//---
use std::env;

use config::Config;
use config::Environment;
use config::File;
use serde::Deserialize;
use serde::Serialize;

use crate::Result;

/// Main configuration container for Raft consensus engine components
///
/// Combines all subsystem configurations with hierarchical override support:
/// 1. Default values from code implementation
/// 2. Configuration file specified by `CONFIG_PATH`
/// 3. Environment variables (highest priority)
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct RaftNodeConfig {
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
impl Debug for RaftNodeConfig {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftNodeConfig")
            .field("cluster", &self.cluster)
            .finish()
    }
}
impl RaftNodeConfig {
    /// Creates a new configuration with hierarchical override support:
    ///
    /// Configuration sources are merged in the following order (later sources
    /// override earlier ones):
    /// 1. Type defaults (lowest priority)
    /// 2. Configuration file from `CONFIG_PATH` environment variable
    /// 3. Environment variables with `RAFT__` prefix (highest priority)
    ///
    /// # Returns
    /// Merged configuration instance or error if:
    /// - Config file parsing fails
    /// - Validation rules are violated
    ///
    /// # Example
    /// ```ignore
    /// // Load with default values only
    /// let cfg = RaftNodeConfig::new()?;
    ///
    /// // Load with config file and environment variables
    /// std::env::set_var("CONFIG_PATH", "config/cluster.toml");
    /// std::env::set_var("RAFT__CLUSTER__NODE_ID", "100");
    /// let cfg = RaftNodeConfig::new()?;
    /// ```
    pub fn new() -> Result<Self> {
        // Create a basic configuration builder
        // 1. Default values ​​as the base layer
        let mut builder = Config::builder().add_source(Config::try_from(&Self::default())?);

        // 2. Conditionally add configuration files
        if let Ok(config_path) = env::var("CONFIG_PATH") {
            builder = builder.add_source(File::with_name(&config_path));
        }

        // 3. Add environment variable source
        builder = builder.add_source(
            Environment::with_prefix("RAFT")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        );

        // Build and deserialize
        let config: Self = builder.build()?.try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    /// Creates a new configuration with additional overrides:
    ///
    /// Merging order (later sources override earlier ones):
    /// 1. Current configuration values
    /// 2. New configuration file
    /// 3. Latest environment variables (highest priority)
    ///
    /// # Example
    /// ```ignore
    /// // Initial configuration
    /// let base = RaftNodeConfig::new()?;
    ///
    /// // Apply runtime overrides
    /// let final_cfg = base.with_override_config("runtime_overrides.toml")?;
    /// ```
    pub fn with_override_config(
        &self,
        path: &str,
    ) -> Result<Self> {
        let config: Self = Config::builder()
            .add_source(Config::try_from(self)?) // Current config
            .add_source(File::with_name(path)) // New overrides
            .add_source(
                // Fresh environment
                Environment::with_prefix("RAFT")
                    .separator("__")
                    .ignore_empty(true)
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;
        config.validate()?;
        Ok(config)
    }

    /// Validates cross-component configuration rules
    ///
    /// # Returns
    /// `Ok(())` if all configurations are valid, or
    /// `Err(Error)` containing validation failure details
    ///
    /// # Errors
    /// Returns validation errors from any subsystem:
    /// - Invalid port bindings
    /// - Conflicting node IDs
    /// - Expired certificates
    /// - Retry policy conflicts
    pub fn validate(&self) -> Result<()> {
        self.cluster.validate()?;
        self.monitoring.validate()?;
        self.raft.validate()?;
        self.network.validate()?;
        self.tls.validate()?;
        self.retry.validate()?;
        Ok(())
    }
}
