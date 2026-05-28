//! Lease configuration for time-based key expiration management
//!
//! Lease provides TTL (time-to-live) functionality for keys with automatic
//! background cleanup of expired entries.
//!
//! # Design Philosophy
//!
//! - **Always On**: TTL is part of the public API — no feature flag needed
//! - **Best Practice**: Fixed background cleanup (industry standard)
//! - **Low Overhead**: When no TTL keys exist, cleanup cycle is a single atomic load (~10ns)
//!
//! # Usage
//!
//! ```toml
//! # Optional: tune cleanup behaviour (TTL is always active)
//! [raft.state_machine.lease]
//! cleanup_interval_ms = 1000  # default 1 second
//! ```

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

use crate::errors::Error;
use crate::errors::Result;

/// Lease configuration for TTL-based key expiration.
///
/// TTL is always active — no enable flag. Background cleanup runs automatically.
///
/// # Performance
///
/// Background cleanup overhead: ~0.001% CPU
/// - Wakes up every `cleanup_interval_ms` (default 1000ms)
/// - No-op when no TTL keys exist (~10ns atomic load)
/// - Deletes expired entries when keys are present
///
/// # Examples
///
/// ```rust
/// use d_engine_core::config::LeaseConfig;
///
/// let config = LeaseConfig::default();
/// assert_eq!(config.cleanup_interval_ms, 1000);
/// ```
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LeaseConfig {
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
    /// Always active — no enable flag needed.
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
    /// Always active — no enable flag needed.
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
