use std::fmt::Debug;
use std::path::PathBuf;
use std::time::Duration;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use super::lease::LeaseConfig;
use super::validate_directory;
use crate::Error;
use crate::Result;

/// Configuration parameters for the Raft consensus algorithm implementation
#[derive(Serialize, Deserialize, Clone)]
pub struct RaftConfig {
    /// Configuration settings related to log replication
    /// Includes parameters like replication batch size and network retry behavior
    #[serde(default)]
    pub replication: ReplicationConfig,

    /// Configuration settings for leader election mechanism
    /// Controls timeouts and randomization factors for election timing
    #[serde(default)]
    pub election: ElectionConfig,

    /// Configuration settings for cluster membership changes
    /// Handles joint consensus transitions and cluster reconfiguration rules
    #[serde(default)]
    pub membership: MembershipConfig,

    /// Configuration settings for commit application handling
    /// Controls how committed log entries are applied to the state machine
    #[serde(default)]
    pub commit_handler: CommitHandlerConfig,

    /// Configuration settings for state machine behavior
    /// Controls state machine operations like lease management, compaction, etc.
    /// For backward compatibility, can also be configured via `storage` in TOML files.
    #[serde(default, alias = "storage")]
    pub state_machine: StateMachineConfig,

    /// Configuration settings for snapshot feature
    #[serde(default)]
    pub snapshot: SnapshotConfig,

    /// Configuration settings for log persistence behavior
    /// Controls how and when log entries are persisted to stable storage
    #[serde(default)]
    pub persistence: PersistenceConfig,

    /// Maximum allowed log entry gap between leader and learner nodes
    /// Learners with larger gaps than this value will trigger catch-up replication
    /// Default value is set via default_learner_catchup_threshold() function
    #[serde(default = "default_learner_catchup_threshold")]
    pub learner_catchup_threshold: u64,

    /// Throttle interval (milliseconds) for learner progress checks
    /// Prevents excessive checking of learner promotion eligibility
    /// Default value is set via default_learner_check_throttle_ms() function
    #[serde(default = "default_learner_check_throttle_ms")]
    pub learner_check_throttle_ms: u64,

    /// Base timeout duration (in milliseconds) for general Raft operations
    /// Used as fallback timeout when operation-specific timeouts are not set
    /// Default value is set via default_general_timeout() function
    #[serde(default = "default_general_timeout")]
    pub general_raft_timeout_duration_in_ms: u64,

    /// Timeout for snapshot RPC operations (milliseconds)
    #[serde(default = "default_snapshot_rpc_timeout_ms")]
    pub snapshot_rpc_timeout_ms: u64,

    /// Configuration settings for new node auto join feature
    #[serde(default)]
    pub auto_join: AutoJoinConfig,

    /// Configuration for read operation consistency behavior
    /// Controls the trade-off between read performance and consistency guarantees
    #[serde(default)]
    pub read_consistency: ReadConsistencyConfig,

    /// RPC compression configuration for different service types
    ///
    /// Controls which RPC service types use response compression.
    /// Allows fine-tuning for performance optimization based on
    /// deployment environment and traffic patterns.
    #[serde(default)]
    pub rpc_compression: RpcCompressionConfig,

    /// Configuration for Watch mechanism that monitors key changes
    /// Controls event queue sizes and metrics behavior
    #[serde(default)]
    pub watch: WatchConfig,
}

impl Debug for RaftConfig {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftConfig").finish()
    }
}
impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            replication: ReplicationConfig::default(),
            election: ElectionConfig::default(),
            membership: MembershipConfig::default(),
            commit_handler: CommitHandlerConfig::default(),
            state_machine: StateMachineConfig::default(),
            snapshot: SnapshotConfig::default(),
            persistence: PersistenceConfig::default(),
            learner_catchup_threshold: default_learner_catchup_threshold(),
            learner_check_throttle_ms: default_learner_check_throttle_ms(),
            general_raft_timeout_duration_in_ms: default_general_timeout(),
            auto_join: AutoJoinConfig::default(),
            snapshot_rpc_timeout_ms: default_snapshot_rpc_timeout_ms(),
            read_consistency: ReadConsistencyConfig::default(),
            rpc_compression: RpcCompressionConfig::default(),
            watch: WatchConfig::default(),
        }
    }
}
impl RaftConfig {
    /// Validates all Raft subsystem configurations
    pub fn validate(&self) -> Result<()> {
        if self.learner_catchup_threshold == 0 {
            return Err(Error::Config(ConfigError::Message(
                "learner_catchup_threshold must be greater than 0".into(),
            )));
        }

        if self.general_raft_timeout_duration_in_ms < 1 {
            return Err(Error::Config(ConfigError::Message(
                "general_raft_timeout_duration_in_ms must be at least 1ms".into(),
            )));
        }

        self.replication.validate()?;
        self.election.validate()?;
        self.membership.validate()?;
        self.commit_handler.validate()?;
        self.state_machine.validate()?;
        self.snapshot.validate()?;
        self.read_consistency.validate()?;
        self.watch.validate()?;

        // Warn if lease duration is too long compared to election timeout
        if self.read_consistency.lease_duration_ms > self.election.election_timeout_min / 2 {
            warn!(
                "read_consistency.lease_duration_ms ({}) is greater than half of election_timeout_min ({}ms). \
                     This may cause lease expiration during normal operation.",
                self.read_consistency.lease_duration_ms,
                self.election.election_timeout_min / 2
            );
        }

        Ok(())
    }
}

fn default_learner_catchup_threshold() -> u64 {
    1
}

fn default_learner_check_throttle_ms() -> u64 {
    1000 // 1 second
}

// in ms
fn default_general_timeout() -> u64 {
    50
}
fn default_snapshot_rpc_timeout_ms() -> u64 {
    // 1 hour - sufficient for large snapshots
    3_600_000
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReplicationConfig {
    #[serde(default = "default_append_interval")]
    pub rpc_append_entries_clock_in_ms: u64,

    #[serde(default = "default_batch_threshold")]
    pub rpc_append_entries_in_batch_threshold: usize,

    #[serde(default = "default_batch_delay")]
    pub rpc_append_entries_batch_process_delay_in_ms: u64,

    #[serde(default = "default_entries_per_replication")]
    pub append_entries_max_entries_per_replication: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            rpc_append_entries_clock_in_ms: default_append_interval(),
            rpc_append_entries_in_batch_threshold: default_batch_threshold(),
            rpc_append_entries_batch_process_delay_in_ms: default_batch_delay(),
            append_entries_max_entries_per_replication: default_entries_per_replication(),
        }
    }
}
impl ReplicationConfig {
    fn validate(&self) -> Result<()> {
        if self.rpc_append_entries_clock_in_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "rpc_append_entries_clock_in_ms cannot be 0".into(),
            )));
        }

        if self.rpc_append_entries_in_batch_threshold == 0 {
            return Err(Error::Config(ConfigError::Message(
                "rpc_append_entries_in_batch_threshold must be > 0".into(),
            )));
        }

        if self.append_entries_max_entries_per_replication == 0 {
            return Err(Error::Config(ConfigError::Message(
                "append_entries_max_entries_per_replication must be > 0".into(),
            )));
        }

        if self.rpc_append_entries_batch_process_delay_in_ms >= self.rpc_append_entries_clock_in_ms
        {
            return Err(Error::Config(ConfigError::Message(format!(
                "batch_delay {}ms should be less than append_interval {}ms",
                self.rpc_append_entries_batch_process_delay_in_ms,
                self.rpc_append_entries_clock_in_ms
            ))));
        }

        Ok(())
    }
}
fn default_append_interval() -> u64 {
    100
}
fn default_batch_threshold() -> usize {
    100
}
fn default_batch_delay() -> u64 {
    1
}
fn default_entries_per_replication() -> u64 {
    100
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ElectionConfig {
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min: u64,

    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max: u64,

    #[serde(default = "default_peer_monitor_interval")]
    pub rpc_peer_connectinon_monitor_interval_in_sec: u64,

    #[serde(default = "default_client_request_id")]
    pub internal_rpc_client_request_id: u32,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            rpc_peer_connectinon_monitor_interval_in_sec: default_peer_monitor_interval(),
            internal_rpc_client_request_id: default_client_request_id(),
        }
    }
}
impl ElectionConfig {
    fn validate(&self) -> Result<()> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(Error::Config(ConfigError::Message(format!(
                "election_timeout_min {}ms must be less than election_timeout_max {}ms",
                self.election_timeout_min, self.election_timeout_max
            ))));
        }

        if self.rpc_peer_connectinon_monitor_interval_in_sec == 0 {
            return Err(Error::Config(ConfigError::Message(
                "rpc_peer_connectinon_monitor_interval_in_sec cannot be 0".into(),
            )));
        }

        Ok(())
    }
}
fn default_election_timeout_min() -> u64 {
    500
}
fn default_election_timeout_max() -> u64 {
    1000
}
fn default_peer_monitor_interval() -> u64 {
    30
}
fn default_client_request_id() -> u32 {
    0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MembershipConfig {
    #[serde(default = "default_probe_service")]
    pub cluster_healthcheck_probe_service_name: String,

    #[serde(default = "default_verify_leadership_persistent_timeout")]
    pub verify_leadership_persistent_timeout: Duration,

    #[serde(default = "default_membership_maintenance_interval")]
    pub membership_maintenance_interval: Duration,

    #[serde(default)]
    pub zombie: ZombieConfig,

    /// Configuration settings for ready learners promotion
    #[serde(default)]
    pub promotion: PromotionConfig,
}
impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            cluster_healthcheck_probe_service_name: default_probe_service(),
            verify_leadership_persistent_timeout: default_verify_leadership_persistent_timeout(),
            membership_maintenance_interval: default_membership_maintenance_interval(),
            zombie: ZombieConfig::default(),
            promotion: PromotionConfig::default(),
        }
    }
}
fn default_probe_service() -> String {
    "d_engine.server.cluster.ClusterManagementService".to_string()
}

// 30 seconds
fn default_membership_maintenance_interval() -> Duration {
    Duration::from_secs(30)
}

/// Default timeout for leader to keep verifying its leadership.
///
/// In Raft, the leader may retry sending no-op entries to confirm it still holds leadership.
/// This timeout defines how long the leader will keep retrying before stepping down.
///
/// Default: 1 hour.
fn default_verify_leadership_persistent_timeout() -> Duration {
    Duration::from_secs(3600)
}

impl MembershipConfig {
    fn validate(&self) -> Result<()> {
        if self.cluster_healthcheck_probe_service_name.is_empty() {
            return Err(Error::Config(ConfigError::Message(
                "cluster_healthcheck_probe_service_name cannot be empty".into(),
            )));
        }
        Ok(())
    }
}

/// Submit processor-specific configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommitHandlerConfig {
    #[serde(default = "default_batch_size_threshold")]
    pub batch_size_threshold: u64,

    #[serde(default = "default_process_interval_ms")]
    pub process_interval_ms: u64,

    #[serde(default = "default_max_entries_per_chunk")]
    pub max_entries_per_chunk: usize,
}
impl Default for CommitHandlerConfig {
    fn default() -> Self {
        Self {
            batch_size_threshold: default_batch_size_threshold(),
            process_interval_ms: default_process_interval_ms(),
            max_entries_per_chunk: default_max_entries_per_chunk(),
        }
    }
}
impl CommitHandlerConfig {
    fn validate(&self) -> Result<()> {
        if self.batch_size_threshold == 0 {
            return Err(Error::Config(ConfigError::Message(
                "batch_size_threshold must be > 0".into(),
            )));
        }

        if self.process_interval_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "process_interval_ms must be > 0".into(),
            )));
        }

        if self.max_entries_per_chunk == 0 {
            return Err(Error::Config(ConfigError::Message(
                "max_entries_per_chunk must be > 0".into(),
            )));
        }

        Ok(())
    }
}
fn default_batch_size_threshold() -> u64 {
    100
}
fn default_process_interval_ms() -> u64 {
    10
}
fn default_max_entries_per_chunk() -> usize {
    10
}

/// State machine behavior configuration
///
/// Controls state machine operations including lease management, compaction policies,
/// and other data lifecycle features. This configuration affects how the state machine
/// processes applied log entries and manages data.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[derive(Default)]
pub struct StateMachineConfig {
    /// Lease (time-based expiration) configuration
    ///
    /// For backward compatibility, can also be configured via `ttl` in TOML files.
    #[serde(alias = "ttl")]
    pub lease: LeaseConfig,
}

impl StateMachineConfig {
    pub fn validate(&self) -> Result<()> {
        self.lease.validate()?;
        Ok(())
    }
}

/// Submit processor-specific configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// If enable the snapshot or not
    #[serde(default = "default_snapshot_enabled")]
    pub enable: bool,

    /// Maximum number of log entries to accumulate before triggering snapshot creation
    /// This helps control memory usage by enforcing periodic state compaction
    #[serde(default = "default_max_log_entries_before_snapshot")]
    pub max_log_entries_before_snapshot: u64,

    /// Minimum duration to wait between consecutive snapshot checks.
    /// Acts as a cooldown period to avoid overly frequent snapshot evaluations.
    #[serde(default = "default_snapshot_cool_down_since_last_check")]
    pub snapshot_cool_down_since_last_check: Duration,

    /// Number of historical snapshot versions to retain during cleanup
    /// Ensures we maintain a safety buffer of previous states for recovery
    #[serde(default = "default_cleanup_retain_count")]
    pub cleanup_retain_count: u64,

    /// Snapshot storage directory
    ///
    /// Default: `default_snapshots_dir()` (/tmp/snapshots)
    #[serde(default = "default_snapshots_dir")]
    pub snapshots_dir: PathBuf,

    #[serde(default = "default_snapshots_dir_prefix")]
    pub snapshots_dir_prefix: String,

    /// Size (in bytes) of individual chunks when transferring snapshots
    ///
    /// Default: `default_chunk_size()` (typically 1MB)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Number of log entries to retain (0 = disable retention)
    #[serde(default = "default_retained_log_entries")]
    pub retained_log_entries: u64,

    /// Number of chunks to process before yielding the task
    #[serde(default = "default_sender_yield_every_n_chunks")]
    pub sender_yield_every_n_chunks: usize,

    /// Number of chunks to process before yielding the task
    #[serde(default = "default_receiver_yield_every_n_chunks")]
    pub receiver_yield_every_n_chunks: usize,

    #[serde(default = "default_max_bandwidth_mbps")]
    pub max_bandwidth_mbps: u32,

    #[serde(default = "default_push_queue_size")]
    pub push_queue_size: usize,

    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_transfer_timeout_in_sec")]
    pub transfer_timeout_in_sec: u64,

    #[serde(default = "default_retry_interval_in_ms")]
    pub retry_interval_in_ms: u64,

    #[serde(default = "default_snapshot_push_backoff_in_ms")]
    pub snapshot_push_backoff_in_ms: u64,

    #[serde(default = "default_snapshot_push_max_retry")]
    pub snapshot_push_max_retry: u32,

    #[serde(default = "default_push_timeout_in_ms")]
    pub push_timeout_in_ms: u64,
}
impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            max_log_entries_before_snapshot: default_max_log_entries_before_snapshot(),
            snapshot_cool_down_since_last_check: default_snapshot_cool_down_since_last_check(),
            cleanup_retain_count: default_cleanup_retain_count(),
            snapshots_dir: default_snapshots_dir(),
            snapshots_dir_prefix: default_snapshots_dir_prefix(),
            chunk_size: default_chunk_size(),
            retained_log_entries: default_retained_log_entries(),
            sender_yield_every_n_chunks: default_sender_yield_every_n_chunks(),
            receiver_yield_every_n_chunks: default_receiver_yield_every_n_chunks(),
            max_bandwidth_mbps: default_max_bandwidth_mbps(),
            push_queue_size: default_push_queue_size(),
            cache_size: default_cache_size(),
            max_retries: default_max_retries(),
            transfer_timeout_in_sec: default_transfer_timeout_in_sec(),
            retry_interval_in_ms: default_retry_interval_in_ms(),
            snapshot_push_backoff_in_ms: default_snapshot_push_backoff_in_ms(),
            snapshot_push_max_retry: default_snapshot_push_max_retry(),
            push_timeout_in_ms: default_push_timeout_in_ms(),
            enable: default_snapshot_enabled(),
        }
    }
}
impl SnapshotConfig {
    fn validate(&self) -> Result<()> {
        if self.max_log_entries_before_snapshot == 0 {
            return Err(Error::Config(ConfigError::Message(
                "max_log_entries_before_snapshot must be greater than 0".into(),
            )));
        }

        if self.cleanup_retain_count == 0 {
            return Err(Error::Config(ConfigError::Message(
                "cleanup_retain_count must be greater than 0".into(),
            )));
        }
        // Validate storage paths
        validate_directory(&self.snapshots_dir, "snapshots_dir")?;

        // chunk_size should be > 0
        if self.chunk_size == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "chunk_size must be at least {} bytes (got {})",
                0, self.chunk_size
            ))));
        }

        if self.retained_log_entries < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "retained_log_entries must be >= 1, (got {})",
                self.retained_log_entries
            ))));
        }

        if self.sender_yield_every_n_chunks < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "sender_yield_every_n_chunks must be >= 1, (got {})",
                self.sender_yield_every_n_chunks
            ))));
        }

        if self.receiver_yield_every_n_chunks < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "receiver_yield_every_n_chunks must be >= 1, (got {})",
                self.receiver_yield_every_n_chunks
            ))));
        }

        if self.push_queue_size < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "push_queue_size must be >= 1, (got {})",
                self.push_queue_size
            ))));
        }

        if self.snapshot_push_max_retry < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "snapshot_push_max_retry must be >= 1, (got {})",
                self.snapshot_push_max_retry
            ))));
        }

        Ok(())
    }
}

fn default_snapshot_enabled() -> bool {
    true
}

/// Default threshold for triggering snapshot creation
fn default_max_log_entries_before_snapshot() -> u64 {
    1000
}

/// Default cooldown duration between snapshot checks.
///
/// Prevents constant evaluation of snapshot conditions in tight loops.
/// Currently set to 1 hour (3600 seconds).
fn default_snapshot_cool_down_since_last_check() -> Duration {
    Duration::from_secs(3600)
}

/// Default number of historical snapshots to retain
fn default_cleanup_retain_count() -> u64 {
    2
}
/// Default snapshots storage path
fn default_snapshots_dir() -> PathBuf {
    PathBuf::from("/tmp/snapshots")
}
/// Default snapshots directory prefix
fn default_snapshots_dir_prefix() -> String {
    "snapshot-".to_string()
}

/// 1KB chunks by default
fn default_chunk_size() -> usize {
    1024
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoJoinConfig {
    #[serde(default = "default_rpc_enable_compression")]
    pub rpc_enable_compression: bool,
}
impl Default for AutoJoinConfig {
    fn default() -> Self {
        Self {
            rpc_enable_compression: default_rpc_enable_compression(),
        }
    }
}
fn default_rpc_enable_compression() -> bool {
    true
}

fn default_retained_log_entries() -> u64 {
    1
}

fn default_sender_yield_every_n_chunks() -> usize {
    1
}

fn default_receiver_yield_every_n_chunks() -> usize {
    1
}

fn default_max_bandwidth_mbps() -> u32 {
    1
}

fn default_push_queue_size() -> usize {
    100
}

fn default_cache_size() -> usize {
    10000
}
fn default_max_retries() -> u32 {
    1
}
fn default_transfer_timeout_in_sec() -> u64 {
    600
}
fn default_retry_interval_in_ms() -> u64 {
    10
}
fn default_snapshot_push_backoff_in_ms() -> u64 {
    100
}
fn default_snapshot_push_max_retry() -> u32 {
    3
}
fn default_push_timeout_in_ms() -> u64 {
    300_000
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZombieConfig {
    /// zombie connection failed threshold
    #[serde(default = "default_zombie_threshold")]
    pub threshold: u32,

    #[serde(default = "default_zombie_purge_interval")]
    pub purge_interval: Duration,
}

impl Default for ZombieConfig {
    fn default() -> Self {
        Self {
            threshold: default_zombie_threshold(),
            purge_interval: default_zombie_purge_interval(),
        }
    }
}

fn default_zombie_threshold() -> u32 {
    3
}
// 30 seconds
fn default_zombie_purge_interval() -> Duration {
    Duration::from_secs(30)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromotionConfig {
    #[serde(default = "default_stale_learner_threshold")]
    pub stale_learner_threshold: Duration,
    #[serde(default = "default_stale_check_interval")]
    pub stale_check_interval: Duration,
}

impl Default for PromotionConfig {
    fn default() -> Self {
        Self {
            stale_learner_threshold: default_stale_learner_threshold(),
            stale_check_interval: default_stale_check_interval(),
        }
    }
}

// 5 minutes
fn default_stale_learner_threshold() -> Duration {
    Duration::from_secs(300)
}
// 30 seconds
fn default_stale_check_interval() -> Duration {
    Duration::from_secs(30)
}
/// Defines how Raft log entries are persisted and accessed.
///
/// All strategies use a configurable [`FlushPolicy`] to control when memory contents
/// are flushed to disk, affecting write latency and durability guarantees.
///
/// **Note:** Both strategies now fully load all log entries from disk into memory at startup.
/// The in-memory `SkipMap` serves as the primary data structure for reads in all modes.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PersistenceStrategy {
    /// Disk-first persistence strategy.
    ///
    /// - **Write path**: On append, the log entry is first written to disk. Only after a successful
    ///   disk write is it acknowledged and stored in the in-memory `SkipMap`.
    ///
    /// - **Read path**: Reads are always served from the in-memory `SkipMap`.
    ///
    /// - **Startup behavior**: All log entries are loaded from disk into memory at startup,
    ///   ensuring consistent access speed regardless of disk state.
    ///
    /// - Suitable for systems prioritizing strong durability while still providing in-memory
    ///   performance for reads.
    DiskFirst,

    /// Memory-first persistence strategy.
    ///
    /// - **Write path**: On append, the log entry is first written to the in-memory `SkipMap` and
    ///   acknowledged immediately. Disk persistence happens asynchronously in the background,
    ///   governed by [`FlushPolicy`].
    ///
    /// - **Read path**: Reads are always served from the in-memory `SkipMap`.
    ///
    /// - **Startup behavior**: All log entries are loaded from disk into memory at startup, the
    ///   same as `DiskFirst`.
    ///
    /// - Suitable for systems that favor lower write latency and faster failover, while still
    ///   retaining a disk-backed log for crash recovery.
    MemFirst,
}

/// Controls when in-memory logs should be flushed to disk.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum FlushPolicy {
    /// Flush each log write immediately to disk.
    ///
    /// - Guarantees the highest durability.
    /// - Each append operation causes a disk write.
    Immediate,

    /// Flush entries to disk when either of two conditions is met:
    /// - The number of unflushed entries reaches the given threshold.
    /// - The elapsed time since the last flush exceeds the configured interval.
    ///
    /// - Balances performance and durability.
    /// - Recent unflushed entries may be lost in the event of a crash or power failure.
    Batch { threshold: usize, interval_ms: u64 },
}

/// Configuration parameters for log persistence behavior
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistenceConfig {
    /// Strategy for persisting Raft logs
    ///
    /// This controls the trade-off between durability guarantees and performance
    /// characteristics. The choice impacts both write throughput and recovery
    /// behavior after node failures.
    #[serde(default = "default_persistence_strategy")]
    pub strategy: PersistenceStrategy,

    /// Flush policy for asynchronous strategies
    ///
    /// This controls when log entries are flushed to disk. The choice impacts
    /// write performance and durability guarantees.
    #[serde(default = "default_flush_policy")]
    pub flush_policy: FlushPolicy,

    /// Maximum number of in-memory log entries to buffer when using async strategies
    ///
    /// This acts as a safety valve to prevent memory exhaustion during periods of
    /// high write throughput or when disk persistence is slow.
    #[serde(default = "default_max_buffered_entries")]
    pub max_buffered_entries: usize,

    /// Number of flush worker threads to use for log persistence.
    ///
    /// - If set to 0, the system falls back to spawning a new task per flush (legacy behavior,
    ///   lower latency but less stable under high load).
    /// - If set to a positive number, a worker pool of that size will be created to process flush
    ///   requests (more stable and efficient under high load).
    ///
    /// This parameter allows tuning between throughput and latency depending on
    /// workload characteristics.
    #[serde(default = "default_flush_workers")]
    pub flush_workers: usize,

    /// Capacity of the internal task channel for flush workers.
    ///
    /// - Provides **backpressure** during high write throughput.
    /// - Prevents unbounded task accumulation in memory when disk I/O is slow.
    /// - Larger values improve throughput at the cost of higher memory usage, while smaller values
    ///   apply stricter flow control but may reduce parallelism.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

/// Default persistence strategy (optimized for balanced workloads)
fn default_persistence_strategy() -> PersistenceStrategy {
    PersistenceStrategy::MemFirst
}

/// Default value for flush_workers
fn default_flush_workers() -> usize {
    2
}

/// Default value for channel_capacity
fn default_channel_capacity() -> usize {
    100
}

/// Default flush policy for asynchronous strategies
///
/// This controls when log entries are flushed to disk. The choice impacts
/// write performance and durability guarantees.
fn default_flush_policy() -> FlushPolicy {
    FlushPolicy::Batch {
        threshold: 1024,
        interval_ms: 100,
    }
}

/// Default maximum buffered log entries
fn default_max_buffered_entries() -> usize {
    10_000
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            strategy: default_persistence_strategy(),
            flush_policy: default_flush_policy(),
            max_buffered_entries: default_max_buffered_entries(),
            flush_workers: default_flush_workers(),
            channel_capacity: default_channel_capacity(),
        }
    }
}

/// Policy for read operation consistency guarantees
///
/// Determines the trade-off between read consistency and performance.
/// Clients can choose the appropriate level based on their requirements.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistencyPolicy {
    /// Lease-based reads for better performance with weaker consistency
    ///
    /// The leader serves reads locally without contacting followers
    /// during the valid lease period. Assumes bounded clock drift between nodes.
    /// Provides lower latency but slightly weaker consistency guarantees
    /// compared to LinearizableRead.
    LeaseRead,

    /// Fully linearizable reads for strongest consistency
    ///
    /// The leader verifies its leadership with a quorum before serving
    /// the read, ensuring strict linearizability. This guarantees that
    /// all reads reflect the most recent committed value in the cluster.
    #[default]
    LinearizableRead,

    /// Eventually consistent reads from any node
    ///
    /// Allows reading from any node (leader, follower, or candidate) without
    /// additional consistency checks. May return stale data but provides
    /// best read performance and availability. Suitable for scenarios where
    /// eventual consistency is acceptable.
    /// **Can be served by non-leader nodes.**
    EventualConsistency,
}

/// Configuration for read operation consistency behavior
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadConsistencyConfig {
    /// Default read consistency policy for the cluster
    ///
    /// This sets the cluster-wide default behavior. Individual read requests
    /// can still override this setting when needed for specific use cases.
    #[serde(default)]
    pub default_policy: ReadConsistencyPolicy,

    /// Lease duration in milliseconds for LeaseRead policy
    ///
    /// Only applicable when using the LeaseRead policy. The leader considers
    /// itself valid for this duration after successfully heartbeating to a quorum.
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,

    /// Whether to allow clients to override the default policy per request
    ///
    /// When true, clients can specify consistency requirements per read request.
    /// When false, all reads use the cluster's default_policy setting.
    #[serde(default = "default_allow_client_override")]
    pub allow_client_override: bool,

    /// Timeout in milliseconds to wait for state machine to catch up with commit index
    ///
    /// Used by LinearizableRead to ensure the state machine has applied all committed
    /// entries before serving reads. Typical apply latency is <1ms on local SSD.
    /// Default: 10ms (safe buffer for single-node local deployments)
    #[serde(default = "default_state_machine_sync_timeout_ms")]
    pub state_machine_sync_timeout_ms: u64,
}

impl Default for ReadConsistencyConfig {
    fn default() -> Self {
        Self {
            default_policy: ReadConsistencyPolicy::default(),
            lease_duration_ms: default_lease_duration_ms(),
            allow_client_override: default_allow_client_override(),
            state_machine_sync_timeout_ms: default_state_machine_sync_timeout_ms(),
        }
    }
}

fn default_lease_duration_ms() -> u64 {
    // Conservative default: half of a typical heartbeat interval (~300ms)
    250
}

fn default_allow_client_override() -> bool {
    // Allow flexibility by default — clients can choose stronger consistency when needed
    true
}

fn default_state_machine_sync_timeout_ms() -> u64 {
    10 // 10ms is safe for typical <1ms apply latency on local SSD
}

impl ReadConsistencyConfig {
    fn validate(&self) -> Result<()> {
        // Validate read consistency configuration
        if self.lease_duration_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "read_consistency.lease_duration_ms must be greater than 0".into(),
            )));
        }
        Ok(())
    }
}

impl From<d_engine_proto::client::ReadConsistencyPolicy> for ReadConsistencyPolicy {
    fn from(proto_policy: d_engine_proto::client::ReadConsistencyPolicy) -> Self {
        match proto_policy {
            d_engine_proto::client::ReadConsistencyPolicy::LeaseRead => Self::LeaseRead,
            d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead => {
                Self::LinearizableRead
            }
            d_engine_proto::client::ReadConsistencyPolicy::EventualConsistency => {
                Self::EventualConsistency
            }
        }
    }
}

impl From<ReadConsistencyPolicy> for d_engine_proto::client::ReadConsistencyPolicy {
    fn from(config_policy: ReadConsistencyPolicy) -> Self {
        match config_policy {
            ReadConsistencyPolicy::LeaseRead => {
                d_engine_proto::client::ReadConsistencyPolicy::LeaseRead
            }
            ReadConsistencyPolicy::LinearizableRead => {
                d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead
            }
            ReadConsistencyPolicy::EventualConsistency => {
                d_engine_proto::client::ReadConsistencyPolicy::EventualConsistency
            }
        }
    }
}

/// Configuration for controlling gRPC compression settings per service type
///
/// Provides fine-grained control over when to enable compression based on
/// the RPC service type and deployment environment. Each service can be
/// independently configured to use compression based on its data
/// characteristics and frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCompressionConfig {
    /// Controls compression for Raft replication response data
    ///
    /// Replication traffic is typically high-frequency with small payloads
    /// in LAN environments, making compression less beneficial. In WAN
    /// deployments with bandwidth constraints, enabling may help.
    ///
    /// **Default**: `false` - Optimized for LAN/same-VPC deployments
    #[serde(default = "default_replication_compression")]
    pub replication_response: bool,

    /// Controls compression for Raft election response data
    ///
    /// Election traffic is low-frequency but time-sensitive. Compression
    /// rarely benefits election traffic due to small payload size.
    ///
    /// **Default**: `true` for backward compatibility
    #[serde(default = "default_election_compression")]
    pub election_response: bool,

    /// Controls compression for snapshot transfer response data
    ///
    /// Snapshot transfers involve large data volumes where compression
    /// is typically beneficial, even in low-latency environments.
    ///
    /// **Default**: `true` - Recommended for all environments
    #[serde(default = "default_snapshot_compression")]
    pub snapshot_response: bool,

    /// Controls compression for cluster management response data
    ///
    /// Cluster operations are infrequent but may contain configuration data.
    /// Compression is generally beneficial for these operations.
    ///
    /// **Default**: `true` for backward compatibility
    #[serde(default = "default_cluster_compression")]
    pub cluster_response: bool,

    /// Controls compression for client request response data
    ///
    /// Client responses may vary in size. In LAN/VPC environments,
    /// compression CPU overhead typically outweighs network benefits.
    ///
    /// **Default**: `false` - Optimized for LAN/same-VPC deployments
    #[serde(default = "default_client_compression")]
    pub client_response: bool,
}

impl Default for RpcCompressionConfig {
    fn default() -> Self {
        Self {
            replication_response: default_replication_compression(),
            election_response: default_election_compression(),
            snapshot_response: default_snapshot_compression(),
            cluster_response: default_cluster_compression(),
            client_response: default_client_compression(),
        }
    }
}

// Default values for RPC compression settings
fn default_replication_compression() -> bool {
    // Replication traffic is high-frequency with typically small payloads
    // For LAN/VPC deployments, compression adds CPU overhead without significant benefit
    false
}

fn default_election_compression() -> bool {
    // Kept enabled for backward compatibility, though minimal benefit
    true
}

fn default_snapshot_compression() -> bool {
    // Snapshot data is large and benefits from compression in all environments
    true
}

fn default_cluster_compression() -> bool {
    // Kept enabled for backward compatibility
    true
}

fn default_client_compression() -> bool {
    // Client responses in LAN/VPC environments typically benefit from no compression
    false
}

/// Configuration for the Watch mechanism that monitors key changes
///
/// The watch system allows clients to monitor specific keys for changes with
/// minimal overhead on the write path. It uses a lock-free event queue and
/// configurable buffer sizes to balance performance and memory usage.
///
/// # Performance Characteristics
///
/// - Write path overhead: < 0.01% with 100+ watchers
/// - Event notification latency: typically < 100μs end-to-end
/// - Memory per watcher: ~2.4KB with default buffer size
///
/// # Configuration Example
///
/// ```toml
/// [raft.watch]
/// event_queue_size = 1000
/// watcher_buffer_size = 10
/// enable_metrics = false
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WatchConfig {
    /// Buffer size for the global event queue shared across all watchers
    ///
    /// This queue sits between the write path and the dispatcher thread.
    /// A larger queue reduces the chance of dropped events under burst load,
    /// but increases memory usage.
    ///
    /// **Performance Impact**:
    /// - Memory: ~24 bytes per slot (key + value pointers + event type)
    /// - Default 1000 slots ≈ 24KB memory
    ///
    /// **Tuning Guidelines**:
    /// - Low traffic (< 1K writes/sec): 500-1000
    /// - Medium traffic (1K-10K writes/sec): 1000-2000
    /// - High traffic (> 10K writes/sec): 2000-5000
    ///
    /// **Default**: 1000
    #[serde(default = "default_event_queue_size")]
    pub event_queue_size: usize,

    /// Buffer size for each individual watcher's channel
    ///
    /// Each registered watcher gets its own channel to receive events.
    /// Smaller buffers reduce memory usage but increase the risk of
    /// dropping events for slow consumers.
    ///
    /// **Performance Impact**:
    /// - Memory: ~240 bytes per slot per watcher
    /// - 10 slots × 100 watchers = ~240KB total
    ///
    /// **Tuning Guidelines**:
    /// - Fast consumers (< 1ms processing): 5-10
    /// - Normal consumers (1-10ms processing): 10-20
    /// - Slow consumers (> 10ms processing): 20-50
    ///
    /// **Default**: 10
    #[serde(default = "default_watcher_buffer_size")]
    pub watcher_buffer_size: usize,

    /// Enable detailed metrics and logging for watch operations
    ///
    /// When enabled, logs warnings for dropped events and tracks watch
    /// performance metrics. Adds minimal overhead (~0.001%) but useful
    /// for debugging and monitoring.
    ///
    /// **Default**: false (minimal overhead in production)
    #[serde(default = "default_enable_watch_metrics")]
    pub enable_metrics: bool,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            event_queue_size: default_event_queue_size(),
            watcher_buffer_size: default_watcher_buffer_size(),
            enable_metrics: default_enable_watch_metrics(),
        }
    }
}

impl WatchConfig {
    /// Validates watch configuration parameters
    pub fn validate(&self) -> Result<()> {
        if self.event_queue_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "watch.event_queue_size must be greater than 0".into(),
            )));
        }

        if self.event_queue_size > 100_000 {
            warn!(
                "watch.event_queue_size ({}) is very large and may consume significant memory (~{}MB)",
                self.event_queue_size,
                (self.event_queue_size * 24) / 1_000_000
            );
        }

        if self.watcher_buffer_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "watch.watcher_buffer_size must be greater than 0".into(),
            )));
        }

        if self.watcher_buffer_size > 1000 {
            warn!(
                "watch.watcher_buffer_size ({}) is very large. Each watcher will consume ~{}KB memory",
                self.watcher_buffer_size,
                (self.watcher_buffer_size * 240) / 1000
            );
        }

        Ok(())
    }
}

const fn default_event_queue_size() -> usize {
    1000
}

const fn default_watcher_buffer_size() -> usize {
    10
}

const fn default_enable_watch_metrics() -> bool {
    false
}
