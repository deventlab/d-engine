use serde::Deserialize;

/// Basic retry policy template
#[derive(Debug, Deserialize, Clone, Copy, Default)]
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
#[derive(Debug, Deserialize, Clone)]
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
                max_retries: 3,
                timeout_ms: 100,
                base_delay_ms: 50,
                max_delay_ms: 1000,
            },
            election: BackoffPolicy {
                max_retries: 10,
                timeout_ms: 200,
                base_delay_ms: 100,
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
