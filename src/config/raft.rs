use serde::Deserialize;

#[derive(Debug, Deserialize, Clone, Default)]
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

fn default_learner_gap() -> u64 {
    10
}
// in ms
fn default_general_timeout() -> u64 {
    100
}

#[derive(Debug, Deserialize, Clone, Default)]
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
#[derive(Debug, Deserialize, Clone, Default)]
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

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MembershipConfig {
    #[serde(default = "default_probe_service")]
    pub cluster_healthcheck_probe_service_name: String,
}
fn default_probe_service() -> String {
    "rpc_service.RpcService".to_string()
}

/// Submit processor-specific configuration
#[derive(Debug, Deserialize, Clone, Default)]
pub struct CommitHandlerConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,

    #[serde(default = "default_process_interval_ms")]
    pub process_interval_ms: u64,

    #[serde(default = "default_max_entries_per_chunk")]
    pub max_entries_per_chunk: usize,
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
