//! LeaderState Performance Benchmarks
//!
//! ## Purpose
//!
//! This benchmark suite measures the performance of critical LeaderState operations
//! to ensure no performance regressions are introduced during refactoring or feature
//! additions. It focuses on operations that directly impact cluster throughput and
//! latency:
//!
//! 1. **LeaderState Creation**: Measures instantiation overhead
//! 2. **Pending Promotion Processing**: Measures learner-to-voter promotion latency
//! 3. **Batch Promotion Scaling**: Validates performance characteristics across different batch sizes
//!
//! ## Performance Acceptance Criteria
//!
//! ### Benchmark 1: LeaderState Creation
//! - **Target**: < 500 ns per operation
//! - **Failure Criteria**: > 1000 ns indicates memory allocation issues or lock contention
//! - **Why It Matters**: Leader election happens during network partitions; slow creation
//!   delays cluster recovery
//!
//! ### Benchmark 2: Process Pending Promotions (2 nodes)
//! - **Target**: < 20 ms per operation
//! - **Failure Criteria**: > 50 ms indicates quorum verification slowdown
//! - **Why It Matters**: Directly impacts auto-scaling responsiveness in production
//!
//! ### Benchmark 3: Batch Promotion Scaling
//! - **Target**: Linear scaling O(n) where n = batch size
//! - **Failure Criteria**: Quadratic scaling O(n²) or worse
//! - **Expected Behavior**:
//!   - batch_1: ~10 ms (baseline)
//!   - batch_2: ~20 ms (2x baseline)
//!   - batch_5: ~50 ms (5x baseline)
//!   - batch_10: ~100 ms (10x baseline)
//! - **Why It Matters**: Non-linear scaling would cause cluster instability during
//!   large-scale node additions
//!
//! ## How to Identify Failures
//!
//! ### Running Benchmarks
//!
//! ```bash
//! # Run all benchmarks
//! make bench
//!
//! # Run only LeaderState benchmarks
//! cargo bench --bench leader_state_bench
//!
//! # Save baseline for future comparison
//! cargo bench --bench leader_state_bench -- --save-baseline main
//!
//! # Compare against baseline
//! cargo bench --bench leader_state_bench -- --baseline main
//! ```
//!
//! ### Interpreting Results
//!
//! #### Example: Successful Run
//! ```
//! LeaderState::new        time:   [425.32 ns 437.45 ns 449.78 ns]
//!                         change: [-2.5% -1.2% +0.3%] (p = 0.15 > 0.05)
//!                         No significant change detected ✅
//!
//! process_pending_promotions_2_nodes
//!                         time:   [18.452 ms 18.591 ms 18.740 ms]
//!                         change: [-3.234% -1.456% +0.823%] (p = 0.25 > 0.05)
//!                         No significant change detected ✅
//! ```
//! **Verdict**: PASS - Both within target thresholds and no regression
//!
//! #### Example: Performance Regression
//! ```
//! LeaderState::new        time:   [1.2453 µs 1.2678 µs 1.2901 µs]
//!                         change: [+180.234% +185.456% +190.823%] (p = 0.00 < 0.05)
//!                         Performance regressed! ❌
//! ```
//! **Verdict**: FAIL - Exceeded 1000 ns threshold and shows 185% regression
//! **Action Required**:
//! 1. Review recent changes to LeaderState constructor
//! 2. Check for new heap allocations or lock acquisitions
//! 3. Revert if no valid justification
//!
//! #### Example: Non-Linear Scaling Detected
//! ```
//! batch_promotion/batch_1     time:   [10.123 ms ...]
//! batch_promotion/batch_2     time:   [22.456 ms ...] (2.2x instead of 2.0x)
//! batch_promotion/batch_5     time:   [75.890 ms ...] (7.5x instead of 5.0x) ❌
//! batch_promotion/batch_10    time:   [210.345 ms ...] (20.8x instead of 10.0x) ❌
//! ```
//! **Verdict**: FAIL - Indicates O(n²) complexity or worse
//! **Action Required**:
//! 1. Profile `process_pending_promotions` with flamegraph
//! 2. Check for nested loops or redundant quorum verifications
//! 3. Investigate membership update inefficiencies
//!
//! ## Continuous Integration
//!
//! This benchmark should be run in CI on every PR to prevent performance regressions.
//! Configure GitHub Actions to fail if:
//! - Any operation exceeds failure threshold
//! - Performance degrades by > 10% compared to main branch
//! - Scaling characteristics become non-linear
//!
//! ## Historical Context
//!
//! - **v0.1.0**: Initial benchmark suite
//! - **v0.2.0** (#236): Added after ReadIndex batching refactor to ensure no regression
//!   in promotion path

use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use d_engine_core::leader_state::{LeaderState, PendingPromotion};
use d_engine_core::{
    AppendResults, MockElectionCore, MockMembership, MockPurgeExecutor, MockRaftLog,
    MockReplicationCore, MockStateMachine, MockStateMachineHandler, MockTransport, MockTypeConfig,
    PeerUpdate, RaftContext, RaftCoreHandlers, RaftNodeConfig, RaftStorageHandles,
};
use d_engine_proto::common::{LogId, NodeStatus};
use d_engine_proto::server::cluster::{ClusterMembership, NodeMeta};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Benchmark 1: LeaderState Creation Performance
///
/// Measures the overhead of instantiating a new LeaderState.
/// This is critical during leader election scenarios.
///
/// Target: < 500 ns
/// Failure: > 1000 ns
fn bench_leader_state_creation(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut node_config = RaftNodeConfig::default();
    node_config.cluster.db_root_dir = temp_dir.path().to_path_buf();
    let config = Arc::new(node_config);

    c.bench_function("LeaderState::new", |b| {
        b.iter(|| {
            let state = LeaderState::<MockTypeConfig>::new(1, config.clone());
            black_box(state)
        })
    });
}

/// Test fixture for async benchmarks
struct BenchFixture {
    leader_state: LeaderState<MockTypeConfig>,
    raft_context: RaftContext<MockTypeConfig>,
    #[allow(dead_code)]
    role_tx: mpsc::UnboundedSender<d_engine_core::RoleEvent>,
}

impl BenchFixture {
    async fn new(test_name: &str) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut node_config = RaftNodeConfig::default();
        node_config.cluster.db_root_dir = temp_dir.path().join(test_name);
        node_config.raft.membership.verify_leadership_persistent_timeout =
            Duration::from_millis(100);

        // Build mock storage
        let mut raft_log = MockRaftLog::new();
        raft_log.expect_last_entry_id().returning(|| 0);
        raft_log.expect_last_log_id().returning(|| None);
        raft_log.expect_flush().returning(|| Ok(()));
        raft_log.expect_load_hard_state().returning(|| Ok(None));
        raft_log.expect_save_hard_state().returning(|_| Ok(()));
        raft_log.expect_calculate_majority_matched_index().returning(|_, _, _| Some(5));

        let mut state_machine = MockStateMachine::new();
        state_machine.expect_start().returning(|| Ok(()));
        state_machine.expect_stop().returning(|| Ok(()));
        state_machine.expect_is_running().returning(|| true);
        state_machine.expect_get().returning(|_| Ok(None));
        state_machine.expect_entry_term().returning(|_| None);
        state_machine.expect_apply_chunk().returning(|_| Ok(()));
        state_machine.expect_len().returning(|| 0);
        state_machine.expect_update_last_applied().returning(|_| ());
        state_machine.expect_last_applied().return_const(LogId::default());
        state_machine.expect_persist_last_applied().returning(|_| Ok(()));
        state_machine.expect_update_last_snapshot_metadata().returning(|_| Ok(()));
        state_machine.expect_snapshot_metadata().returning(|| None);
        state_machine.expect_persist_last_snapshot_metadata().returning(|_| Ok(()));
        state_machine.expect_apply_snapshot_from_file().returning(|_, _| Ok(()));
        state_machine
            .expect_generate_snapshot_data()
            .returning(|_, _| Ok(bytes::Bytes::copy_from_slice(&[0u8; 32])));
        state_machine.expect_save_hard_state().returning(|| Ok(()));
        state_machine.expect_flush().returning(|| Ok(()));

        let storage = RaftStorageHandles {
            raft_log: Arc::new(raft_log),
            state_machine: Arc::new(state_machine),
        };

        // Build mock transport
        let transport = Arc::new(MockTransport::new());

        // Build mock handlers
        let mut election_handler = MockElectionCore::new();
        election_handler
            .expect_broadcast_vote_requests()
            .returning(|_, _, _, _, _| Ok(()));

        let mut replication_handler = MockReplicationCore::new();
        replication_handler
            .expect_handle_raft_request_in_batch()
            .returning(|_, _, _, _, _| {
                Ok(AppendResults {
                    commit_quorum_achieved: true,
                    learner_progress: std::collections::HashMap::new(),
                    peer_updates: std::collections::HashMap::from([
                        (2, PeerUpdate::success(5, 6)),
                        (3, PeerUpdate::success(5, 6)),
                    ]),
                })
            });

        let mut state_machine_handler = MockStateMachineHandler::new();
        state_machine_handler.expect_update_pending().returning(|_| {});
        state_machine_handler.expect_read_from_state_machine().returning(|_| None);

        let mut purge_executor = MockPurgeExecutor::new();
        purge_executor.expect_execute_purge().returning(|_| Ok(()));

        let handlers = RaftCoreHandlers {
            election_handler,
            replication_handler,
            state_machine_handler: Arc::new(state_machine_handler),
            purge_executor: Arc::new(purge_executor),
        };

        // Build mock membership
        let mut membership = MockMembership::new();
        membership.expect_can_rejoin().returning(|_, _| Ok(()));
        membership.expect_pre_warm_connections().returning(|| Ok(()));
        membership.expect_get_node_status().returning(|_| Some(NodeStatus::Active));
        membership.expect_voters().returning(|| {
            vec![NodeMeta {
                id: 2,
                address: "".to_string(),
                status: NodeStatus::Active as i32,
                role: d_engine_proto::common::NodeRole::Follower.into(),
            }]
        });
        membership.expect_replication_peers().returning(Vec::new);
        membership.expect_members().returning(Vec::new);
        membership.expect_check_cluster_is_ready().returning(|| Ok(()));
        membership
            .expect_retrieve_cluster_membership_config()
            .returning(|_| ClusterMembership {
                version: 1,
                nodes: vec![],
                current_leader_id: None,
            });
        membership.expect_get_zombie_candidates().returning(Vec::new);
        membership.expect_get_peers_id_with_condition().returning(|_| vec![]);
        membership.expect_is_single_node_cluster().returning(|| false);
        membership.expect_initial_cluster_size().returning(|| 3);

        let raft_context = RaftContext {
            node_id: 1,
            storage,
            transport,
            membership: Arc::new(membership),
            handlers,
            node_config: Arc::new(node_config.clone()),
        };

        let (role_tx, _role_rx) = mpsc::unbounded_channel();
        let leader_state = LeaderState::new(1, Arc::new(node_config));

        BenchFixture {
            leader_state,
            raft_context,
            role_tx,
        }
    }
}

/// Benchmark 2: Process Pending Promotions (2 nodes)
///
/// Measures the latency of promoting 2 ready learners to voters.
/// This represents the common case in auto-scaling scenarios.
///
/// Target: < 20 ms
/// Failure: > 50 ms
fn bench_process_promotions_2_nodes(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    c.bench_function("process_pending_promotions_2_nodes", |b| {
        b.to_async(&rt).iter(|| async {
            let mut fixture = BenchFixture::new("bench_promotions_2").await;
            fixture.leader_state.pending_promotions =
                (1..=2).map(|id| PendingPromotion::new(id, Instant::now())).collect();

            black_box(
                fixture
                    .leader_state
                    .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
                    .await,
            )
        })
    });
}

/// Benchmark 3: Batch Promotion Scaling
///
/// Validates that promotion performance scales linearly with batch size.
/// Non-linear scaling indicates algorithmic inefficiency.
///
/// Expected: O(n) linear scaling
/// Failure: O(n²) or worse
fn bench_batch_promotion_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("batch_promotion");

    // Test batch sizes: 1, 2, 5, 10, 20
    // Each should take ~10ms * batch_size if scaling linearly
    for batch_size in [1, 2, 5, 10, 20].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("batch_{batch_size}")),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let mut fixture = BenchFixture::new(&format!("bench_batch_{size}")).await;
                    fixture.leader_state.pending_promotions =
                        (1..=size).map(|id| PendingPromotion::new(id, Instant::now())).collect();

                    black_box(
                        fixture
                            .leader_state
                            .process_pending_promotions(&fixture.raft_context, &fixture.role_tx)
                            .await,
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_leader_state_creation,
    bench_process_promotions_2_nodes,
    bench_batch_promotion_scaling
);
criterion_main!(benches);
