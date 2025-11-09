//! Storage and data lifecycle configuration
//!
//! Provides configuration for storage backends and data management:
//! - TTL (Time-To-Live) cleanup strategies
//! - Compaction policies
//! - Storage optimization parameters

use serde::{Deserialize, Serialize};

use crate::Result;

/// Storage and data lifecycle configuration
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StorageConfig {
    /// TTL cleanup configuration
    pub ttl: TtlConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            ttl: TtlConfig::default(),
        }
    }
}

impl StorageConfig {
    pub fn validate(&self) -> Result<()> {
        self.ttl.validate()?;
        Ok(())
    }
}

/// TTL (Time-To-Live) cleanup strategy configuration
///
/// d-engine provides multiple TTL cleanup strategies to balance
/// resource efficiency with timely expiration:
///
/// - `disabled`: No automatic cleanup (zero overhead)
/// - `passive`: Cleanup only on read access
/// - `piggyback`: Cleanup during Raft apply events (default, recommended)
/// - `background`: Dedicated background task (not recommended)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TtlConfig {
    /// TTL cleanup strategy
    ///
    /// Available strategies:
    /// - `"disabled"`: No TTL cleanup. Use if you don't use TTL at all.
    ///   - Overhead: 0%
    ///   - Memory: Expired keys remain until read
    ///
    /// - `"passive"`: Cleanup only when keys are accessed via get()
    ///   - Overhead: ~0.01% (35ns per read)
    ///   - Memory: Cold expired keys remain indefinitely
    ///   - Best for: Cache-like workloads, hot key access patterns
    ///
    /// - `"piggyback"`: Cleanup during Raft apply events (default)
    ///   - Overhead: ~0.01% (<1ms per 100 applies)
    ///   - Memory: Expired keys cleaned within N applies
    ///   - Best for: Most production use cases
    ///   - Note: Requires write traffic to trigger cleanup
    ///
    /// - `"background"`: Dedicated background cleanup task
    ///   - Overhead: ~0.001% (periodic wakeup)
    ///   - Memory: Expired keys cleaned within interval
    ///   - Best for: Large cold data with TTL
    ///   - Warning: Conflicts with "resource efficiency" design
    #[serde(default = "default_cleanup_strategy")]
    pub cleanup_strategy: String,

    /// Piggyback cleanup frequency (applies every N Raft applies)
    ///
    /// Only used when `cleanup_strategy = "piggyback"`
    ///
    /// Default: 100 (cleanup every 100 Raft applies)
    /// Range: 10-10000
    ///
    /// Tuning guide:
    /// - Lower value (e.g., 10):
    ///   - More frequent cleanup
    ///   - Lower memory usage
    ///   - Higher CPU overhead (~0.1%)
    ///
    /// - Higher value (e.g., 1000):
    ///   - Less frequent cleanup
    ///   - Higher memory usage
    ///   - Lower CPU overhead (~0.001%)
    ///
    /// Recommended:
    /// - High-frequency writes (>1K/sec): 50-100
    /// - Medium-frequency writes (100-1K/sec): 100-500
    /// - Low-frequency writes (<100/sec): 500-1000
    #[serde(default = "default_piggyback_frequency")]
    pub piggyback_frequency: u64,

    /// Maximum cleanup duration per cycle (milliseconds)
    ///
    /// Default: 1ms (prevents blocking Raft apply)
    /// Range: 0-100ms
    ///
    /// This limits how long cleanup can run in a single cycle.
    /// If cleanup exceeds this duration, it will pause and continue
    /// in the next cycle.
    ///
    /// Tuning guide:
    /// - 0ms: Cleanup disabled (same as "passive" strategy)
    /// - 1ms: Balanced (default, recommended)
    /// - 5-10ms: Aggressive cleanup (may impact latency)
    /// - >10ms: Not recommended (can block Raft)
    #[serde(default = "default_max_cleanup_duration_ms")]
    pub max_cleanup_duration_ms: u64,

    /// Background cleanup interval (seconds)
    ///
    /// Only used when `cleanup_strategy = "background"`
    ///
    /// Default: 60 seconds
    /// Range: 1-3600 seconds
    ///
    /// Note: Background cleanup uses a dedicated tokio task,
    /// which conflicts with d-engine's "resource efficiency" design.
    /// Only use if you have specific requirements for cold data cleanup.
    #[serde(default = "default_background_interval_secs")]
    pub background_interval_secs: u64,
}

fn default_cleanup_strategy() -> String {
    "piggyback".to_string()
}

fn default_piggyback_frequency() -> u64 {
    100
}

fn default_max_cleanup_duration_ms() -> u64 {
    1
}

fn default_background_interval_secs() -> u64 {
    60
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            cleanup_strategy: default_cleanup_strategy(),
            piggyback_frequency: default_piggyback_frequency(),
            max_cleanup_duration_ms: default_max_cleanup_duration_ms(),
            background_interval_secs: default_background_interval_secs(),
        }
    }
}

impl TtlConfig {
    pub fn validate(&self) -> Result<()> {
        // Validate strategy
        match self.cleanup_strategy.as_str() {
            "disabled" | "passive" | "piggyback" | "background" => {}
            _ => {
                return Err(crate::Error::Config(config::ConfigError::Message(format!(
                    "Invalid TTL cleanup strategy: '{}'. Valid options: disabled, passive, piggyback, background",
                    self.cleanup_strategy
                ))));
            }
        }

        // Validate piggyback_frequency
        if !(10..=10000).contains(&self.piggyback_frequency) {
            return Err(crate::Error::Config(config::ConfigError::Message(format!(
                "piggyback_frequency must be between 10 and 10000, got {}",
                self.piggyback_frequency
            ))));
        }

        // Validate max_cleanup_duration_ms
        if self.max_cleanup_duration_ms > 100 {
            return Err(crate::Error::Config(config::ConfigError::Message(format!(
                "max_cleanup_duration_ms cannot exceed 100ms, got {}ms",
                self.max_cleanup_duration_ms
            ))));
        }

        // Validate background_interval_secs
        if !(1..=3600).contains(&self.background_interval_secs) {
            return Err(crate::Error::Config(config::ConfigError::Message(format!(
                "background_interval_secs must be between 1 and 3600, got {}",
                self.background_interval_secs
            ))));
        }

        Ok(())
    }

    /// Returns true if TTL cleanup is completely disabled
    pub fn is_disabled(&self) -> bool {
        self.cleanup_strategy == "disabled"
    }

    /// Returns true if passive cleanup strategy is enabled
    pub fn is_passive(&self) -> bool {
        self.cleanup_strategy == "passive"
    }

    /// Returns true if piggyback cleanup strategy is enabled
    pub fn is_piggyback(&self) -> bool {
        self.cleanup_strategy == "piggyback"
    }

    /// Returns true if background cleanup strategy is enabled
    pub fn is_background(&self) -> bool {
        self.cleanup_strategy == "background"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TtlConfig::default();
        assert_eq!(config.cleanup_strategy, "piggyback");
        assert_eq!(config.piggyback_frequency, 100);
        assert_eq!(config.max_cleanup_duration_ms, 1);
        assert_eq!(config.background_interval_secs, 60);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_strategy() {
        let config = TtlConfig {
            cleanup_strategy: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_frequency() {
        let config = TtlConfig {
            piggyback_frequency: 5,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = TtlConfig {
            piggyback_frequency: 20000,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_duration() {
        let config = TtlConfig {
            max_cleanup_duration_ms: 200,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_strategy_checks() {
        let config = TtlConfig {
            cleanup_strategy: "disabled".to_string(),
            ..Default::default()
        };
        assert!(config.is_disabled());
        assert!(!config.is_passive());

        let config = TtlConfig {
            cleanup_strategy: "passive".to_string(),
            ..Default::default()
        };
        assert!(config.is_passive());
        assert!(!config.is_piggyback());

        let config = TtlConfig {
            cleanup_strategy: "piggyback".to_string(),
            ..Default::default()
        };
        assert!(config.is_piggyback());
        assert!(!config.is_background());
    }
}
