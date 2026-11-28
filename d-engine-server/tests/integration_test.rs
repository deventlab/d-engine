// Integration tests organized by business domain
//
// Embedded tests run a whole cluster in one process. They use
// `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`, as a real deployment does: on the
// default single-thread runtime, one blocking call in any node (opening RocksDB takes hundreds of
// ms under load) stops every node's timers and triggers elections a real cluster would not see.
#[cfg(feature = "rocksdb")]
mod cas_operations;
#[cfg(feature = "rocksdb")]
mod cluster_lifecycle;
mod cluster_membership;
#[cfg(feature = "rocksdb")]
mod cluster_state_and_metadata;
mod common;
#[cfg(feature = "rocksdb")]
mod consistent_reads;
#[cfg(feature = "rocksdb")]
mod drain_batching;
#[cfg(feature = "rocksdb")]
mod embedded_client;
#[cfg(feature = "rocksdb")]
mod failover_and_recovery;
#[cfg(feature = "rocksdb")]
mod reliability;

mod leader_election;
mod replication_and_sync;

// Storage layer integration tests
// mod storage_buffered_raft_log;

mod storage_raft_log;

#[cfg(feature = "rocksdb")]
mod readonly_and_learner_mode;

#[cfg(feature = "rocksdb")]
mod snapshot_and_recovery;

#[cfg(all(feature = "watch", feature = "rocksdb"))]
mod watch_and_subscriptions;

// Support modules
mod client_manager;

mod leader_tick_heartbeat;
