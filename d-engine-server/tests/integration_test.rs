// Integration tests organized by business domain
#[cfg(feature = "rocksdb")]
mod cas_operations;
#[cfg(feature = "rocksdb")]
mod cluster_lifecycle;
mod cluster_membership;
mod cluster_state_and_metadata;
mod common;
#[cfg(feature = "rocksdb")]
mod consistent_reads;
#[cfg(feature = "rocksdb")]
mod drain_batching;
#[cfg(feature = "rocksdb")]
mod embedded_client;
mod failover_and_recovery;
mod leader_election;
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

mod leader_tick_heartbeat;
