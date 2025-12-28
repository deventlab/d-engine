#[cfg(any(test, feature = "test-utils"))]
mod components;

// Integration tests moved from d-engine/tests/
mod append_entries;
mod client_manager;
mod cluster_start_stop;
mod common;
mod election;
mod embedded;
mod join_cluster;
mod readonly_mode;
mod snapshot;
