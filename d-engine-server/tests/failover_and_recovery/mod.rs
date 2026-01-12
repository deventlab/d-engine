//! # Failover and Recovery Tests
//!
//! This module verifies the failover behavior and recovery mechanisms in d-engine clusters,
//! ensuring safety and liveness when nodes fail or become temporarily unavailable.
//!
//! ## Business Scenarios Covered
//!
//! 1. **Leader Failover (Embedded)**: Kill the leader node in a 3-node cluster and verify
//!    automatic re-election of a new leader, data consistency, and cluster operational status
//! 2. **Leader Failover (Standalone)**: Same scenario via gRPC for remote nodes
//! 3. **Node Rejoin (Embedded)**: Kill a follower, verify cluster still works with 2/3 nodes,
//!    then restart the follower and confirm it syncs all missing data
//! 4. **Minority Failure Blocks Writes (Both Modes)**: Kill 2 out of 3 nodes to lose majority,
//!    verify that remaining node cannot serve writes (Raft safety property)
//!
//! ## Raft Protocol Robustness
//!
//! - **Quorum-Based Consensus**: Leader requires majority (2/3) of nodes to confirm before committing
//! - **Election Timeout**: Followers detect leader failure and trigger new election
//! - **New Leader Confirmation**: Verify new leader's committed entries match previous leader
//! - **No Data Loss on Failover**: All committed entries survive leader failure
//! - **Safety Under Partition**: Minority partition cannot make progress (no split-brain)
//! - **Liveness After Majority Return**: Cluster resumes operation when quorum restored
//! - **Log Catch-Up**: Rejoining node receives all missing log entries via replication
//!
//! ## Developer API Coverage
//!
//! **Embedded Mode**:
//! - `EmbeddedEngine::stop()` - Graceful shutdown (simulates failure)
//! - `engine.leader_change_notifier()` - Watch for leader changes
//! - `Client::put/get_eventual` - Verify writes succeed/fail based on quorum
//! - `Client::get_linearizable()` - Strong consistency reads
//!
//! **Standalone Mode (gRPC)**:
//! - Process kill simulation
//! - Client connection pooling and reconnection
//! - Failover detection via timeouts
//! - Leader discovery after failover
//!
//! ## Integration Modes
//!
//! - ✅ **Embedded Mode**: Full coverage
//!   - `leader_failover_embedded.rs` - Leader failure and re-election
//!   - `node_rejoin_embedded.rs` - ⚠️ IN FILE (needs phase 2 split)
//!   - `minority_failure_blocks_writes_embedded.rs` - ⚠️ IN FILE (needs phase 2 split)
//!
//! - ✅ **Standalone Mode**: Full coverage
//!   - `leader_failover_standalone.rs` - Leader failure via gRPC
//!   - `node_rejoin_standalone.rs` - ❌ MISSING
//!   - `minority_failure_blocks_writes_standalone.rs` - ⚠️ IN FILE (needs phase 2 split)
//!
//! ## Test Files (Current Structure)
//!
//! **Phase 1 (Current)**:
//! - `leader_failover_embedded.rs` - COMBINED: test_embedded_leader_failover() + test_embedded_node_rejoin() + test_minority_failure_blocks_writes()
//! - `leader_failover_standalone.rs` - COMBINED: test_3_node_failover() + test_minority_failure()
//!
//! **Phase 2 (Planned Refactor)**:
//! Will split into individual files:
//! - `leader_failover_embedded.rs` - test_embedded_leader_failover() only
//! - `node_rejoin_embedded.rs` - test_embedded_node_rejoin() only
//! - `minority_failure_blocks_writes_embedded.rs` - test_minority_failure_blocks_writes() only
//! - `leader_failover_standalone.rs` - test_3_node_failover() only
//! - `minority_failure_blocks_writes_standalone.rs` - test_minority_failure() only
//! - `node_rejoin_standalone.rs` - ❌ TO BE IMPLEMENTED
//!
//! ## Failure Scenarios
//!
//! ### Hard Failure (Process Crash)
//! - **Simulation**: `engine.stop()` without graceful shutdown
//! - **Detection**: Election timeout (typically 1-2 seconds)
//! - **Recovery**: New leader elected, cluster continues
//!
//! ### Temporary Failure (Network Partition)
//! - **Simulation**: Process stops but data persists
//! - **Impact**: Minority partition cannot make progress
//! - **Majority**: Can continue to serve requests
//! - **Recovery**: Partition rejoins, syncs missing entries
//!
//! ## Note on File Organization
//!
//! Currently, some test files contain multiple test functions covering different scenarios.
//! This is a temporary state. In Phase 2 refactor, files will be split so that:
//! - Each file has ONE primary test scenario
//! - Test function names match file names
//! - Clear 1:1 mapping between files and test objectives
//!
//! This allows for better organization and easier discovery of specific test cases.

// Current test modules - combined functions will be split in phase 2
mod leader_failover_embedded;
mod leader_failover_standalone;
// mod minority_failure_blocks_writes_embedded;
// mod minority_failure_blocks_writes_standalone;
// mod node_rejoin_embedded;
