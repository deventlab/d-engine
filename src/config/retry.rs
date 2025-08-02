use std::fmt::Debug;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::Error;
use crate::Result;

/// Configuration for exponential backoff retry strategy
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct BackoffPolicy {
    /// Maximum number of retries (0 means unlimited retries)
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Single operation timeout (unit: milliseconds)
    #[serde(default = "default_op_timeout_ms")]
    pub timeout_ms: u64,

    /// Backoff base (unit: milliseconds)
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,

    /// Maximum backoff time (unit: milliseconds)
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,
}

/// Configuration for exponential backoff retry strategy
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct InstallSnapshotBackoffPolicy {
    /// Maximum number of retries (0 means unlimited retries)
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,

    /// Single operation timeout (unit: milliseconds)
    #[serde(default = "default_op_timeout_ms")]
    pub timeout_ms: u64,

    /// Backoff base (unit: milliseconds)
    #[serde(default = "default_base_delay_ms")]
    pub base_delay_ms: u64,

    /// Maximum backoff time (unit: milliseconds)
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,

    // New fields for snapshot transfers
    /// Timeout per chunk during transfer (milliseconds)
    #[serde(default = "default_per_chunk_timeout_ms")]
    pub per_chunk_timeout_ms: u64,

    /// Minimum overall timeout for snapshot RPC (milliseconds)
    #[serde(default = "default_min_timeout_ms")]
    pub min_timeout_ms: u64,

    /// Maximum overall timeout for snapshot RPC (milliseconds)
    #[serde(default = "default_max_timeout_ms")]
    pub max_timeout_ms: u64,

    /// Timeout between chunks on receiver side (milliseconds)
    #[serde(default = "default_between_chunk_timeout_ms")]
    pub between_chunk_timeout_ms: u64,
}

/// Domain-specific retry strategy configurations for Raft subsystems
/// Enables fine-grained control over different RPC types and operations
#[derive(Serialize, Deserialize, Clone)]
pub struct RetryPolicies {
    /// Retry policy for AppendEntries RPC operations
    /// Governs log replication attempts between leader and followers
    #[serde(default)]
    pub append_entries: BackoffPolicy,

    /// Retry policy for RequestVote RPC operations
    /// Controls election-related communication retry behavior
    #[serde(default)]
    pub election: BackoffPolicy,

    /// Retry policy for cluster membership changes
    /// Requires higher reliability for configuration change operations
    #[serde(default)]
    pub membership: BackoffPolicy,

    /// Retry policy for leader auto discovery request
    #[serde(default)]
    pub auto_discovery: BackoffPolicy,

    /// Retry policy for join cluster request
    #[serde(default)]
    pub join_cluster: BackoffPolicy,

    /// Retry policy for install snapshot requests
    #[serde(default)]
    pub install_snapshot: InstallSnapshotBackoffPolicy,

    /// Retry policy for purge log requests
    #[serde(default)]
    pub purge_log: BackoffPolicy,

    /// Retry policy for node health checks
    /// Optimized for frequent liveness detection with lower overhead
    #[serde(default)]
    pub healthcheck: BackoffPolicy,

    /// Retry policy for internal quorum verification
    /// Used to confirm leadership status through internal consensus checks
    #[serde(default)]
    pub internal_quorum: BackoffPolicy,
}

impl Debug for RetryPolicies {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RetryPolicies").finish()
    }
}
// Default value implementation
impl Default for RetryPolicies {
    fn default() -> Self {
        Self {
            append_entries: BackoffPolicy {
                max_retries: 1,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 1000,
            },
            election: BackoffPolicy {
                // Note: `retries` > 3 might prevent a successful election.
                max_retries: 3,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 5000,
            },
            membership: BackoffPolicy {
                max_retries: 120,
                timeout_ms: 500,
                base_delay_ms: 3000,
                max_delay_ms: 60000,
            },
            healthcheck: BackoffPolicy {
                max_retries: 10000,
                timeout_ms: 100,
                base_delay_ms: 1000,
                max_delay_ms: 10000,
            },

            auto_discovery: BackoffPolicy {
                max_retries: 3,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 1000,
            },

            join_cluster: BackoffPolicy {
                max_retries: 3,
                timeout_ms: 500,
                base_delay_ms: 3000,
                max_delay_ms: 60000,
            },

            // Recommended configuration examples for different network scenarios:
            //
            // Scenario                      Recommended Settings
            // ------------------------------------------------------------
            // Local Data Center (Low Latency)     -> max_retries=3, timeout_ms=500,
            // max_delay_ms=2000 Cross-Region Network (High Latency) -> max_retries=10,
            // timeout_ms=5000, max_delay_ms=10000 Edge Network (Unstable)
            // -> max_retries=10, timeout_ms=3000, max_delay_ms=30000
            //
            // Current config:
            install_snapshot: InstallSnapshotBackoffPolicy {
                max_retries: 3,
                timeout_ms: 500,
                base_delay_ms: 50,
                max_delay_ms: 2000,
                per_chunk_timeout_ms: default_per_chunk_timeout_ms(),
                min_timeout_ms: default_min_timeout_ms(),
                max_timeout_ms: default_max_timeout_ms(),
                between_chunk_timeout_ms: default_between_chunk_timeout_ms(),
            },

            purge_log: BackoffPolicy {
                max_retries: 1,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 1000,
            },
            internal_quorum: BackoffPolicy {
                // Minimum must be 3: the first quorum check may fail if the leader is newly elected
                // and followers haven't yet advanced their next_index
                max_retries: 3,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 1000,
            },
        }
    }
}
impl BackoffPolicy {
    /// Validates backoff policy parameters
    /// # Errors
    /// Returns `Error::InvalidConfig` when:
    /// - Timeout exceeds maximum delay
    /// - Base delay > max delay
    /// - Infinite retries without proper safeguards
    pub fn validate(
        &self,
        policy_name: &str,
    ) -> Result<()> {
        // Validate retry limits
        if self.max_retries == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: max_retries=0 means infinite retries - dangerous for {policy_name} operations"
            ))));
        }

        // Validate timeout constraints
        if self.timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: timeout_ms cannot be 0"
            ))));
        }

        // Validate delay progression
        if self.base_delay_ms >= self.max_delay_ms {
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: base_delay_ms({}) must be less than max_delay_ms({})",
                policy_name, self.base_delay_ms, self.max_delay_ms
            ))));
        }

        // Ensure reasonable maximums
        if self.max_delay_ms > 120_000 {
            // 2 minutes
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: max_delay_ms({}) exceeds 2min limit",
                policy_name, self.max_delay_ms
            ))));
        }

        Ok(())
    }
}

impl InstallSnapshotBackoffPolicy {
    /// Validates snapshot backoff policy parameters
    /// # Errors
    /// Returns `Error::InvalidConfig` when:
    /// - Inherited backoff parameters fail validation (see BackoffPolicy)
    /// - Timeouts for snapshot chunks/transfers are invalid
    /// - Snapshot RPC timeout constraints are violated
    pub fn validate(
        &self,
        policy_name: &str,
    ) -> Result<()> {
        // First validate common backoff parameters
        self.validate_base_policy(policy_name)?;

        // Validate chunk-related timeouts
        self.validate_chunk_timeouts(policy_name)?;

        // Validate snapshot RPC timeout constraints
        self.validate_rpc_timeouts(policy_name)?;

        Ok(())
    }

    /// Validate parameters inherited from BackoffPolicy
    fn validate_base_policy(
        &self,
        policy_name: &str,
    ) -> Result<()> {
        if self.max_retries == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: max_retries=0 means infinite retries - dangerous for {policy_name} operations"
            ))));
        }

        if self.timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: timeout_ms cannot be 0"
            ))));
        }

        if self.base_delay_ms >= self.max_delay_ms {
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: base_delay_ms({}) must be less than max_delay_ms({})",
                policy_name, self.base_delay_ms, self.max_delay_ms
            ))));
        }

        if self.max_delay_ms > 120_000 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: max_delay_ms({}) exceeds 2min limit",
                policy_name, self.max_delay_ms
            ))));
        }

        Ok(())
    }

    /// Validate snapshot chunk transfer parameters
    fn validate_chunk_timeouts(
        &self,
        policy_name: &str,
    ) -> Result<()> {
        if self.per_chunk_timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: per_chunk_timeout_ms cannot be 0"
            ))));
        }

        if self.between_chunk_timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: between_chunk_timeout_ms cannot be 0"
            ))));
        }

        Ok(())
    }

    /// Validate snapshot RPC timeout hierarchy
    fn validate_rpc_timeouts(
        &self,
        policy_name: &str,
    ) -> Result<()> {
        const MAX_RPC_TIMEOUT: u64 = 86_400_000; // 24 hours

        if self.min_timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: min_timeout_ms cannot be 0"
            ))));
        }

        if self.max_timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "{policy_name}: max_timeout_ms cannot be 0"
            ))));
        }

        if self.min_timeout_ms > self.max_timeout_ms {
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: min_timeout_ms({}) > max_timeout_ms({})",
                policy_name, self.min_timeout_ms, self.max_timeout_ms
            ))));
        }

        if self.max_timeout_ms > MAX_RPC_TIMEOUT {
            return Err(Error::Config(ConfigError::Message(format!(
                "{}: max_timeout_ms({}) exceeds 24-hour limit",
                policy_name, self.max_timeout_ms
            ))));
        }

        Ok(())
    }
}

impl RetryPolicies {
    /// Validates all retry policies according to Raft protocol requirements
    pub fn validate(&self) -> Result<()> {
        self.validate_append_entries()?;
        self.validate_election()?;
        self.validate_membership()?;
        self.validate_healthcheck()?;
        self.validate_auto_discovery()?;
        self.validate_join_cluster()?;
        self.validate_install_snapshot()?;
        self.validate_purge_log()?;
        self.validate_internal_quorum()?;
        Ok(())
    }

    fn validate_append_entries(&self) -> Result<()> {
        self.append_entries.validate("append_entries")?;

        Ok(())
    }

    fn validate_election(&self) -> Result<()> {
        self.election.validate("election")?;

        Ok(())
    }

    fn validate_membership(&self) -> Result<()> {
        self.membership.validate("membership")?;

        Ok(())
    }

    fn validate_healthcheck(&self) -> Result<()> {
        self.healthcheck.validate("healthcheck")?;

        Ok(())
    }

    fn validate_purge_log(&self) -> Result<()> {
        self.purge_log.validate("purge_log")?;

        Ok(())
    }

    fn validate_install_snapshot(&self) -> Result<()> {
        self.install_snapshot.validate("install_snapshot")?;

        Ok(())
    }

    fn validate_join_cluster(&self) -> Result<()> {
        self.join_cluster.validate("join_cluster")?;

        Ok(())
    }

    fn validate_auto_discovery(&self) -> Result<()> {
        self.auto_discovery.validate("auto_discovery")?;

        Ok(())
    }

    fn validate_internal_quorum(&self) -> Result<()> {
        self.internal_quorum.validate("internal_quorum")?;

        Ok(())
    }
}

fn default_max_retries() -> usize {
    3
}
fn default_op_timeout_ms() -> u64 {
    100
}
fn default_base_delay_ms() -> u64 {
    50
}
fn default_max_delay_ms() -> u64 {
    1000
}
fn default_per_chunk_timeout_ms() -> u64 {
    100
}
fn default_min_timeout_ms() -> u64 {
    100
}
fn default_max_timeout_ms() -> u64 {
    30_000
}
fn default_between_chunk_timeout_ms() -> u64 {
    30_000
}
