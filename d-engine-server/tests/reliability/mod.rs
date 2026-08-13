//! # Startup Reliability Tests
//!
//! Verifies data_dir startup guarantees that are orthogonal to Raft consensus
//! itself — e.g. the exclusive lock preventing two processes from sharing a
//! data_dir, and that it survives (releases cleanly after) a process crash.

mod data_dir_lock_crash_recovery_embedded;
mod data_dir_lock_rejects_concurrent_start_embedded;
mod data_dir_lock_released_after_stop_embedded;
mod data_dir_lock_released_after_storage_init_failure_embedded;
mod network_listener_released_after_stop_embedded;
