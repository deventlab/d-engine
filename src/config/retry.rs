use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Basic retry policy template
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

/// Divide strategies by business domain
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RetryPolicies {
    // Log replication strategy (AppendEntries RPC)
    #[serde(default)]
    pub append_entries: BackoffPolicy,

    // Election strategy (RequestVote RPC)
    #[serde(default)]
    pub election: BackoffPolicy,

    // Member change strategy (high reliability requirement)
    #[serde(default)]
    pub membership: BackoffPolicy,

    // Health check strategy (high frequency detection)
    #[serde(default)]
    pub healthcheck: BackoffPolicy,
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
                max_retries: 3, // Note: `retries` > 3 might prevent a successful election.
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
    pub fn validate(&self, policy_name: &str) -> Result<()> {
        // Validate retry limits
        if self.max_retries == 0 {
            return Err(Error::InvalidConfig(format!(
                "{}: max_retries=0 means infinite retries - dangerous for {} operations",
                policy_name, policy_name
            )));
        }

        // Validate timeout constraints
        if self.timeout_ms == 0 {
            return Err(Error::InvalidConfig(format!(
                "{}: timeout_ms cannot be 0",
                policy_name
            )));
        }

        // Validate delay progression
        if self.base_delay_ms >= self.max_delay_ms {
            return Err(Error::InvalidConfig(format!(
                "{}: base_delay_ms({}) must be less than max_delay_ms({})",
                policy_name, self.base_delay_ms, self.max_delay_ms
            )));
        }

        // Ensure reasonable maximums
        if self.max_delay_ms > 120_000 {
            // 2 minutes
            return Err(Error::InvalidConfig(format!(
                "{}: max_delay_ms({}) exceeds 2min limit",
                policy_name, self.max_delay_ms
            )));
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
