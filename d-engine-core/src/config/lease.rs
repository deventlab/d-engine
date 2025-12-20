//! Lease configuration for time-based key expiration management
//!
//! Lease provides TTL (time-to-live) functionality for keys with automatic
//! background cleanup of expired entries.
//!
//! # Design Philosophy
//!
//! - **Simple**: Single `enabled` flag - no strategy choices
//! - **Best Practice**: Fixed background cleanup (industry standard)
//! - **Zero Overhead**: When disabled, no lease components are initialized
//! - **Graceful Degradation**: TTL requests are rejected when disabled
//!
//! # Usage
//!
//! ```toml
//! # Enable TTL feature (default: disabled)
//! [raft.state_machine.lease]
//! enabled = true
//! interval_ms = 1000  # Optional, default 1 second
//! ```

use crate::errors::{Error, Result};
use config::ConfigError;
use serde::{Deserialize, Serialize};

/// Lease configuration for TTL-based key expiration
///
/// When `enabled = true`:
/// - Server initializes lease manager at startup
/// - Background worker periodically cleans expired keys
/// - Client TTL requests (ttl_secs > 0) are accepted
///
/// When `enabled = false` (default):
/// - No lease components initialized (zero overhead)
/// - Client TTL requests (ttl_secs > 0) are rejected with error
/// - All keys are permanent (ttl_secs = 0 is always allowed)
///
/// # Performance
///
/// Background cleanup overhead: ~0.001% CPU
/// - Wakes up every `interval_ms` (default 1000ms)
/// - Scans expired keys (limited by `max_cleanup_duration_ms`)
/// - Deletes expired entries from storage
///
/// # Examples
///
/// ```rust
/// use d_engine_core::config::LeaseConfig;
///
/// // Default: disabled
/// let config = LeaseConfig::default();
/// assert!(!config.enabled);
///
/// // Enable with defaults
/// let config = LeaseConfig {
///     enabled: true,
///     ..Default::default()
/// };
/// assert_eq!(config.interval_ms, 1000);
/// ```
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaseConfig {
    /// Enable TTL feature
    ///
    /// - `true`: Initialize lease manager + start background cleanup worker
    /// - `false`: No TTL support, reject requests with ttl_secs > 0
    ///
    /// Default: false (zero overhead when TTL not needed)
    #[serde(default)]
    pub enabled: bool,

    /// Background cleanup interval in milliseconds
    ///
    /// How often the background worker wakes up to scan and delete expired keys.
    ///
    /// Range: 100-3600000 (100ms to 1 hour)
    /// Default: 1000 (1 second)
    ///
    /// Tuning guide:
    /// - 1000ms (default): Balanced for most workloads
    /// - 100-500ms: Aggressive cleanup for memory-sensitive apps
    /// - 5000-10000ms: Relaxed cleanup for low TTL usage
    ///
    /// Only used when `enabled = true`
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,

    /// Maximum cleanup duration per cycle (milliseconds)
    ///
    /// Limits how long a single cleanup cycle can run to prevent
    /// excessive CPU usage in the background worker.
    ///
    /// Range: 1-100ms
    /// Default: 1ms
    ///
    /// Tuning guide:
    /// - 1ms (default): Safe for latency-sensitive apps
    /// - 5-10ms: Aggressive cleanup when backlog exists
    /// - >10ms: Not recommended (can impact foreground operations)
    ///
    /// Only used when `enabled = true`
    #[serde(default = "default_max_cleanup_duration_ms")]
    pub max_cleanup_duration_ms: u64,
}

fn default_interval_ms() -> u64 {
    1000
}

fn default_max_cleanup_duration_ms() -> u64 {
    1
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Default: TTL disabled (zero overhead)
            interval_ms: default_interval_ms(),
            max_cleanup_duration_ms: default_max_cleanup_duration_ms(),
        }
    }
}

impl LeaseConfig {
    /// Validates configuration parameters
    ///
    /// Returns error if:
    /// - `interval_ms` is out of range (100-3600000)
    /// - `max_cleanup_duration_ms` is out of range (1-100)
    pub fn validate(&self) -> Result<()> {
        // Skip validation if disabled
        if !self.enabled {
            return Ok(());
        }

        // Validate interval_ms
        // Range: 100ms to 1 hour
        // - Lower bound (100ms): Prevents excessive wakeups (CPU waste)
        // - Upper bound (3600000ms = 1h): Ensures reasonable cleanup frequency
        if !(100..=3_600_000).contains(&self.interval_ms) {
            return Err(Error::Config(ConfigError::Message(format!(
                "lease cleanup interval_ms must be between 100 and 3600000, got {}",
                self.interval_ms
            ))));
        }

        // Validate max_cleanup_duration_ms
        // Range: 1-100ms
        // - Lower bound (1ms): Minimum useful duration
        // - Upper bound (100ms): Prevents blocking too long
        if !(1..=100).contains(&self.max_cleanup_duration_ms) {
            return Err(Error::Config(ConfigError::Message(format!(
                "max_cleanup_duration_ms must be between 1 and 100, got {}",
                self.max_cleanup_duration_ms
            ))));
        }

        Ok(())
    }
}
