#![doc = include_str!("../../docs/components/purge_executor.md")]
//! Log compaction and purge management.
//!
//! Defines the interface for customizing log purge behavior in Raft leaders and followers.
//! Implementations can control how log entries are physically removed from storage while
//! maintaining Raft's safety guarantees.

mod executor;
pub(crate) use executor::*;
#[cfg(test)]
use mockall::automock;
use tonic::async_trait;

use crate::proto::common::LogId;
use crate::Result;

/// Defines the behavior for log entry compaction and physical deletion.
///
/// # Safety Requirements
/// Implementations MUST guarantee:
/// 1. All entries up to `last_included.index` are preserved in snapshots
/// 2. No concurrent modifications during purge execution
/// 3. Atomic persistence of purge metadata
#[cfg_attr(test, automock)]
#[async_trait]
pub trait PurgeExecutor: Send + Sync + 'static {
    /// Physically removes log entries up to specified index (inclusive)
    ///
    /// # Arguments
    /// - `last_included` The last log index included in the latest snapshot
    ///
    /// # Implementation Notes
    /// - Should be atomic with updating `last_purged_index`
    /// - Must not delete any entries required for log matching properties
    async fn execute_purge(
        &self,
        last_included: LogId,
    ) -> Result<()>;

    /// Validates if purge can be safely executed
    ///
    /// # Key Characteristics
    /// - **Stateless**: Does NOT validate Raft protocol state (commit index/purge order/cluster
    ///   progress)
    /// - **Storage-focused**: Performs physical storage checks (disk space/snapshot integrity/I/O
    ///   health)
    ///
    /// Default implementation checks:
    /// - No pending purge operations
    #[allow(dead_code)]
    async fn validate_purge(
        &self,
        last_included: LogId,
    ) -> Result<()> {
        let _ = last_included; // Default no-op
        Ok(())
    }

    /// Lifecycle hook before purge execution
    ///
    /// Use for:
    /// - Acquiring resource locks
    /// - Starting transactions
    #[allow(dead_code)]
    async fn pre_purge(&self) -> Result<()> {
        Ok(())
    }

    /// Lifecycle hook after purge execution
    ///
    /// Use for:
    /// - Releasing resource locks
    /// - Finalizing transactions
    /// - Triggering backups
    async fn post_purge(&self) -> Result<()> {
        Ok(())
    }
}
