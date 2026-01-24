// Integration tests organized by business domain
mod cas_operations;
mod cluster_lifecycle;
mod cluster_membership;
mod cluster_state_and_metadata;
mod common;
mod consistent_reads;
mod failover_and_recovery;
mod leader_election;
mod local_kv_client;
mod replication_and_sync;

// Storage layer integration tests
mod storage_buffered_raft_log;

#[cfg(feature = "rocksdb")]
mod readonly_and_learner_mode;

#[cfg(feature = "rocksdb")]
mod snapshot_and_recovery;

#[cfg(all(feature = "watch", feature = "rocksdb"))]
mod watch_and_subscriptions;

// Support modules
mod client_manager;
