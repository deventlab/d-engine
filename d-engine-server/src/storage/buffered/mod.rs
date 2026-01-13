// Re-export BufferedRaftLog from core (now lives in d-engine-core)
pub use d_engine_core::BufferedRaftLog;

#[cfg(test)]
mod buffered_raft_log_test;
