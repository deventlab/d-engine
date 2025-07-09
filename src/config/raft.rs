use std::fmt::Debug;
use std::path::PathBuf;
use std::time::Duration;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;

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

    /// Configuration settings for snapshot feature
    #[serde(default)]
    pub snapshot: SnapshotConfig,

    /// Maximum allowed log entry gap between leader and learner nodes
    /// Learners with larger gaps than this value will trigger catch-up replication
    /// Default value is set via default_learner_catchup_threshold() function
    #[serde(default = "default_learner_catchup_threshold")]
    pub learner_catchup_threshold: u64,

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
            snapshot: SnapshotConfig::default(),
            learner_catchup_threshold: default_learner_catchup_threshold(),
            general_raft_timeout_duration_in_ms: default_general_timeout(),
            auto_join: AutoJoinConfig::default(),
            snapshot_rpc_timeout_ms: default_snapshot_rpc_timeout_ms(),
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
        self.snapshot.validate()?;

        Ok(())
    }
}

fn default_learner_catchup_threshold() -> u64 {
    1
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

        if self.rpc_append_entries_batch_process_delay_in_ms >= self.rpc_append_entries_clock_in_ms {
            return Err(Error::Config(ConfigError::Message(format!(
                "batch_delay {}ms should be less than append_interval {}ms",
                self.rpc_append_entries_batch_process_delay_in_ms, self.rpc_append_entries_clock_in_ms
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
}
impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            cluster_healthcheck_probe_service_name: default_probe_service(),
        }
    }
}
fn default_probe_service() -> String {
    "raft.cluster.ClusterManagementService".to_string()
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
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,

    #[serde(default = "default_process_interval_ms")]
    pub process_interval_ms: u64,

    #[serde(default = "default_max_entries_per_chunk")]
    pub max_entries_per_chunk: usize,
}
impl Default for CommitHandlerConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            process_interval_ms: default_process_interval_ms(),
            max_entries_per_chunk: default_max_entries_per_chunk(),
        }
    }
}
impl CommitHandlerConfig {
    fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            return Err(Error::Config(ConfigError::Message("batch_size must be > 0".into())));
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
fn default_batch_size() -> u64 {
    100
}
fn default_process_interval_ms() -> u64 {
    10
}
fn default_max_entries_per_chunk() -> usize {
    100
}

/// Submit processor-specific configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
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
/// Default threshold for triggering snapshot creation
fn default_max_log_entries_before_snapshot() -> u64 {
    1000
}

/// Default cooldown duration between snapshot checks.
///
/// Prevents constant evaluation of snapshot conditions in tight loops.
/// Currently set to 60 days (5,184,000 seconds).
fn default_snapshot_cool_down_since_last_check() -> Duration {
    Duration::from_secs(5_184_000)
}

/// Default number of historical snapshots to retain
fn default_cleanup_retain_count() -> u64 {
    2
}
/// Default snapshots storage path
fn default_snapshots_dir() -> PathBuf {
    PathBuf::from("/tmp/snapshots")
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
