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
//! cleanup_interval_ms = 1000  # Optional, default 1 second
//! ```

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::errors::Error;
use crate::errors::Result;

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
/// - Wakes up every `cleanup_interval_ms` (default 1000ms)
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
/// assert_eq!(config.cleanup_interval_ms, 1000);
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

    /// How often the background worker wakes up to scan and delete expired keys (milliseconds).
    ///
    /// This setting controls MEMORY EFFICIENCY only, not TTL correctness.
    /// Expired keys are always rejected at read time via `is_expired()` regardless of cleanup
    /// timing. Cleanup only reclaims the memory occupied by stale DashMap entries.
    ///
    /// Range: 100–60000 (100ms to 60 seconds)
    /// Default: 1000 (1 second)
    ///
    /// Upper bound rationale: at 60s, a workload of 10K TTL writes/s with TTL=1s accumulates
    /// ~600K stale entries (~30MB) between cleanups — acceptable for most deployments.
    /// Beyond 60s, memory growth becomes unbounded in high-throughput short-TTL workloads.
    ///
    /// Tuning guide:
    /// - 1000ms (default): Good for most workloads
    /// - 100-500ms: High-throughput short-TTL workloads (e.g., rate limiting, sessions)
    /// - 5000-60000ms: Low-frequency TTL usage where memory pressure is not a concern
    ///
    /// Only used when `enabled = true`
    #[serde(default = "default_cleanup_interval_ms")]
    pub cleanup_interval_ms: u64,

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

fn default_cleanup_interval_ms() -> u64 {
    1000
}

fn default_max_cleanup_duration_ms() -> u64 {
    1
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Default: TTL disabled (zero overhead)
            cleanup_interval_ms: default_cleanup_interval_ms(),
            max_cleanup_duration_ms: default_max_cleanup_duration_ms(),
        }
    }
}

impl LeaseConfig {
    /// Validates configuration parameters
    ///
    /// Returns error if:
    /// - `cleanup_interval_ms` is out of range (100-60000)
    /// - `max_cleanup_duration_ms` is out of range (1-100)
    pub fn validate(&self) -> Result<()> {
        // Skip validation if disabled
        if !self.enabled {
            return Ok(());
        }

        // Validate cleanup_interval_ms
        // Range: 100ms to 60s
        // - Lower bound (100ms): Prevents excessive wakeups (CPU waste)
        // - Upper bound (60000ms): Caps stale-entry accumulation; beyond 60s, high-throughput
        //   short-TTL workloads accumulate unbounded memory (see field doc comment)
        if !(100..=60_000).contains(&self.cleanup_interval_ms) {
            return Err(Error::Config(ConfigError::Message(format!(
                "lease cleanup_interval_ms must be between 100 and 60000, got {}",
                self.cleanup_interval_ms
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
