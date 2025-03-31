use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RaftConfig {
    #[serde(default)]
    pub replication: ReplicationConfig,

    #[serde(default)]
    pub election: ElectionConfig,

    #[serde(default)]
    pub membership: MembershipConfig,

    #[serde(default)]
    pub commit_handler: CommitHandlerConfig,

    #[serde(default = "default_learner_gap")]
    pub learner_raft_log_gap: u64,

    #[serde(default = "default_general_timeout")]
    pub general_raft_timeout_duration_in_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            replication: ReplicationConfig::default(),
            election: ElectionConfig::default(),
            membership: MembershipConfig::default(),
            commit_handler: CommitHandlerConfig::default(),
            learner_raft_log_gap: default_learner_gap(),
            general_raft_timeout_duration_in_ms: default_general_timeout(),
        }
    }
}
impl RaftConfig {
    /// Validates all Raft subsystem configurations
    pub fn validate(&self) -> Result<()> {
        if self.learner_raft_log_gap == 0 {
            return Err(Error::InvalidConfig(
                "learner_raft_log_gap must be greater than 0".into(),
            ));
        }

        if self.general_raft_timeout_duration_in_ms < 1 {
            return Err(Error::InvalidConfig(
                "general_raft_timeout_duration_in_ms must be at least 1ms".into(),
            ));
        }

        self.replication.validate()?;
        self.election.validate()?;
        self.membership.validate()?;
        self.commit_handler.validate()?;

        Ok(())
    }
}

fn default_learner_gap() -> u64 {
    10
}
// in ms
fn default_general_timeout() -> u64 {
    50
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
            return Err(Error::InvalidConfig(
                "rpc_append_entries_clock_in_ms cannot be 0".into(),
            ));
        }

        if self.rpc_append_entries_in_batch_threshold == 0 {
            return Err(Error::InvalidConfig(
                "rpc_append_entries_in_batch_threshold must be > 0".into(),
            ));
        }

        if self.append_entries_max_entries_per_replication == 0 {
            return Err(Error::InvalidConfig(
                "append_entries_max_entries_per_replication must be > 0".into(),
            ));
        }

        if self.rpc_append_entries_batch_process_delay_in_ms >= self.rpc_append_entries_clock_in_ms
        {
            return Err(Error::InvalidConfig(format!(
                "batch_delay {}ms should be less than append_interval {}ms",
                self.rpc_append_entries_batch_process_delay_in_ms,
                self.rpc_append_entries_clock_in_ms
            )));
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
            return Err(Error::InvalidConfig(format!(
                "election_timeout_min {}ms must be less than election_timeout_max {}ms",
                self.election_timeout_min, self.election_timeout_max
            )));
        }

        if self.rpc_peer_connectinon_monitor_interval_in_sec == 0 {
            return Err(Error::InvalidConfig(
                "rpc_peer_connectinon_monitor_interval_in_sec cannot be 0".into(),
            ));
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
    "rpc_service.RpcService".to_string()
}

impl MembershipConfig {
    fn validate(&self) -> Result<()> {
        if self.cluster_healthcheck_probe_service_name.is_empty() {
            return Err(Error::InvalidConfig(
                "cluster_healthcheck_probe_service_name cannot be empty".into(),
            ));
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
            return Err(Error::InvalidConfig("batch_size must be > 0".into()));
        }

        if self.process_interval_ms == 0 {
            return Err(Error::InvalidConfig(
                "process_interval_ms must be > 0".into(),
            ));
        }

        if self.max_entries_per_chunk == 0 {
            return Err(Error::InvalidConfig(
                "max_entries_per_chunk must be > 0".into(),
            ));
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
