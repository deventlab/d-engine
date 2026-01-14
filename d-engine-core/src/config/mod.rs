//! Configuration management module for distributed Raft consensus engine.
//!
//! Provides hierarchical configuration loading and validation with:
//! - Default values as code base
//! - Environment variable overrides
//! - Configuration file support
//! - Component-wise validation
mod cluster;
use std::fmt::Debug;
use std::path::Path;
mod lease;
mod network;
mod raft;
mod retry;
mod tls;
pub use cluster::*;
use config::ConfigError;
pub use lease::*;
pub use network::*;
pub use raft::*;
pub use retry::*;
pub use tls::*;
#[cfg(test)]
mod config_test;
#[cfg(test)]
mod lease_test;
#[cfg(test)]
mod network_test;
#[cfg(test)]
mod raft_test;
#[cfg(test)]
mod tls_test;
use std::env;

use config::Config;
use config::Environment;
use config::File;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
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
        f.debug_struct("RaftNodeConfig").field("cluster", &self.cluster).finish()
    }
}
impl RaftNodeConfig {
    /// Loads configuration from hierarchical sources without validation.
    ///
    /// Configuration sources are merged in the following order (later sources override earlier):
    /// 1. Type defaults (lowest priority)
    /// 2. Configuration file from `CONFIG_PATH` environment variable (if set)
    /// 3. Environment variables with `RAFT__` prefix (highest priority)
    ///
    /// # Note
    /// This method does NOT validate the configuration. Validation is deferred to allow
    /// further overrides via `with_override_config()`. Callers MUST call `validate()`
    /// before using the configuration.
    ///
    /// # Returns
    /// Merged configuration instance or error if config file parsing fails.
    ///
    /// # Examples
    /// ```ignore
    /// // Load with default values only
    /// let cfg = RaftNodeConfig::new()?.validate()?;
    ///
    /// // Load with config file and environment variables
    /// std::env::set_var("CONFIG_PATH", "config/cluster.toml");
    /// std::env::set_var("RAFT__CLUSTER__NODE_ID", "100");
    /// let cfg = RaftNodeConfig::new()?.validate()?;
    ///
    /// // Apply runtime overrides
    /// let cfg = RaftNodeConfig::new()?
    ///     .with_override_config("custom.toml")?
    ///     .validate()?;
    /// ```
    pub fn new() -> Result<Self> {
        let mut builder = Config::builder().add_source(Config::try_from(&Self::default())?);

        if let Ok(config_path) = env::var("CONFIG_PATH") {
            builder = builder.add_source(File::with_name(&config_path).required(true));
        }

        builder = builder.add_source(
            Environment::with_prefix("RAFT")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        );

        let config: Self = builder.build()?.try_deserialize()?;
        Ok(config) // No validation - deferred to validate()
    }

    /// Applies additional configuration overrides from file without validation.
    ///
    /// Merging order (later sources override earlier):
    /// 1. Current configuration values
    /// 2. New configuration file
    /// 3. Latest environment variables (highest priority)
    ///
    /// # Note
    /// This method does NOT validate the configuration. Callers MUST call `validate()`
    /// after all overrides are applied.
    ///
    /// # Example
    /// ```ignore
    /// let cfg = RaftNodeConfig::new()?
    ///     .with_override_config("runtime_overrides.toml")?
    ///     .validate()?;
    /// ```
    pub fn with_override_config(
        &self,
        path: &str,
    ) -> Result<Self> {
        let config: Self = Config::builder()
            .add_source(Config::try_from(self)?)
            .add_source(File::with_name(path))
            .add_source(
                Environment::with_prefix("RAFT")
                    .separator("__")
                    .ignore_empty(true)
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()?;
        Ok(config) // No validation - deferred to validate()
    }

    /// Validates configuration and returns validated instance.
    ///
    /// Consumes self and performs validation of all subsystems. Must be called
    /// after all configuration overrides to ensure the final config is valid.
    ///
    /// # Returns
    /// Validated configuration or error if validation fails.
    ///
    /// # Errors
    /// Returns validation errors from any subsystem:
    /// - Invalid port bindings
    /// - Conflicting node IDs
    /// - Expired certificates
    /// - Invalid directory paths
    ///
    /// # Example
    /// ```ignore
    /// let config = RaftNodeConfig::new()?
    ///     .with_override_config("app.toml")?
    ///     .validate()?; // Validation happens here
    /// ```
    pub fn validate(self) -> Result<Self> {
        self.cluster.validate()?;
        self.raft.validate()?;
        self.network.validate()?;
        self.tls.validate()?;
        self.retry.validate()?;
        Ok(self)
    }

    /// Checks if this node is configured as a learner (not a voter)
    ///
    /// A learner node has role=Learner. Only learners can join via JoinCluster RPC.
    /// Voters have role=Follower/Candidate/Leader and are added via config change (AddNode).
    ///
    /// Note: Status (Promotable/ReadOnly/Active) is separate from role.
    /// This method checks **role only**.
    pub fn is_learner(&self) -> bool {
        use d_engine_proto::common::NodeRole;

        self.cluster
            .initial_cluster
            .iter()
            .find(|n| n.id == self.cluster.node_id)
            .map(|n| n.role == NodeRole::Learner as i32)
            .unwrap_or(false)
    }
}

/// Ensures directory path is valid and writable
pub(super) fn validate_directory(
    path: &Path,
    name: &str,
) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(Error::Config(ConfigError::Message(format!(
            "{name} path cannot be empty"
        ))));
    }

    #[cfg(not(test))]
    {
        use std::fs;
        // Check directory existence or create ability
        if !path.exists() {
            fs::create_dir_all(path).map_err(|e| {
                Error::Config(ConfigError::Message(format!(
                    "Failed to create {} directory at {}: {}",
                    name,
                    path.display(),
                    e
                )))
            })?;
        }

        // Check write permissions
        let test_file = path.join(".permission_test");
        fs::write(&test_file, b"test").map_err(|e| {
            Error::Config(ConfigError::Message(format!(
                "No write permission in {} directory {}: {}",
                name,
                path.display(),
                e
            )))
        })?;
        fs::remove_file(&test_file).ok();
    }

    Ok(())
}
