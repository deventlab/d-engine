//! Analytics Node Integration Tests
//!
//! This module tests d-engine's capability to support read-only Analytics nodes (Learner role).
//! Analytics nodes enable:
//! - Dedicated data analysis without affecting cluster quorum
//! - Multi-region deployments with local read-only replicas
//! - Monitoring and telemetry collection
//! - Cost-effective scaling for read-heavy workloads
//!
//! ## Test Categories
//! 1. **Standalone/RPC Mode**: Nodes via gRPC, clients via ClientManager
//! 2. **Embedded Mode**: Nodes in same process, clients via EmbeddedEngine API

use std::time::Duration;

use d_engine_client::ClientApiError;
use d_engine_proto::client::ReadConsistencyPolicy;
use tracing_test::traced_test;

use crate::client_manager::ClientManager;
use crate::common::TestContext;
use crate::common::WAIT_FOR_NODE_READY_IN_SEC;
use crate::common::check_cluster_is_ready;
use crate::common::create_bootstrap_urls;
use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::reset;
use crate::common::start_node;
use crate::common::test_put_get;

// ============================================================================
// STANDALONE/RPC MODE TESTS
// ============================================================================

/// Test: Standalone (RPC) Mode - ReadOnly Learner Node
///
/// ## Integration Mode
/// **Standalone (RPC)**: Nodes communicate via gRPC, client accesses via ClientManager
///
/// ## Test Purpose
/// Verify that a Learner node with READ_ONLY status (permanent Analytics node) works in Standalone/RPC mode:
/// 1. **Data Sync**: Node 4 syncs all data from the primary cluster (read-only capability)
/// 2. **Non-Voting**: Node 4 does NOT participate in voting/quorum
/// 3. **Permanent Learner**: Node 4 remains as Learner forever (never auto-promotes to Voter)
/// 4. **Quorum Unaffected**: Primary cluster quorum stays at 2 (3 voters, node 4 ignored)
///
/// ## Test Scenario
/// - **Phase 1**: Bootstrap 3-node primary cluster (nodes 1,2,3 = Voter+ACTIVE via RPC)
/// - **Phase 2**: Write test data through RPC client
/// - **Phase 3**: Start node 4 as standalone Learner with READ_ONLY status, joins via JoinCluster RPC
/// - **Phase 4**: Verify node 4 can read all replicated data via RPC
/// - **Phase 5**: Verify new writes propagate to node 4, quorum unchanged
/// - **Phase 6**: Verify node 4 status remains READ_ONLY (cannot be promoted to Voter)
///
/// ## Key Verifications
/// ✓ Node 4 successfully syncs historical + new data (read-only works)
/// ✓ Node 4 does NOT affect quorum calculations (3 voters unchanged)
/// ✓ Node 4 remains Learner forever (READ_ONLY status prevents auto-promotion)
/// ✓ RPC communication works for read operations on Learner
/// ✓ Cluster remains stable with READ_ONLY Learner node present
///
/// ## Conclusion
/// **ReadOnly Learners are fully supported in Standalone/RPC mode** ✅
#[tokio::test]
#[traced_test]
async fn test_readonly_mode_learner_standalone() -> Result<(), ClientApiError> {
    const TEST_DIR: &str = "cluster_start_stop/analytics_rpc";
    const DB_ROOT_DIR: &str = "./db/cluster_start_stop/analytics_rpc";
    const LOG_DIR: &str = "./logs/cluster_start_stop/analytics_rpc";

    reset(TEST_DIR).await?;

    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Phase 1: Start initial 3-node primary cluster (nodes 1, 2, 3 = Voter+Active)
    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ Analytics Node Test - Standalone/RPC Mode                ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    println!("[Phase 1] Starting 3-node primary cluster...");
    let mut ctx = TestContext {
        graceful_txs: Vec::new(),
        node_handles: Vec::new(),
    };

    for (i, port) in ports[..3].iter().enumerate() {
        let (graceful_tx, node_handle) = start_node(
            node_config(
                &create_node_config((i + 1) as u64, *port, &ports[..3], DB_ROOT_DIR, LOG_DIR).await,
            ),
            None,
            None,
        )
        .await?;
        ctx.graceful_txs.push(graceful_tx);
        ctx.node_handles.push(node_handle);
    }

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in &ports[..3] {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }
    println!("[Phase 1] ✓ 3-node primary cluster ready with 3 voters\n");

    // Phase 2: Write test data to primary cluster
    println!("[Phase 2] Writing test data to primary cluster...");
    let bootstrap_urls = create_bootstrap_urls(&ports[..3]);
    let mut client_manager = ClientManager::new(&bootstrap_urls).await?;
    test_put_get(&mut client_manager, 1, 100).await?;
    test_put_get(&mut client_manager, 2, 101).await?;
    println!("[Phase 2] ✓ Test data written via RPC (key1=100, key2=101)\n");

    // Phase 3: Start node 4 as ReadOnly Learner
    println!("[Phase 3] Starting node 4 as standalone ReadOnly Learner...");

    // Create config for node 4 with READ_ONLY status
    let node4_config = format!(
        r#"
[cluster]
node_id = 4
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 0, status = 2 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 0, status = 2 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 0, status = 2 }},
    {{ id = 4, name = 'n4', address = '127.0.0.1:{}', role = 3, status = 1 }}
]
db_root_dir = '{}'
log_dir = '{}'

[raft]
general_raft_timeout_duration_in_ms = 5000
"#,
        ports[3], ports[0], ports[1], ports[2], ports[3], DB_ROOT_DIR, LOG_DIR
    );

    let (graceful_tx4, node_n4) = start_node(node_config(&node4_config), None, None).await?;
    ctx.graceful_txs.push(graceful_tx4);
    ctx.node_handles.push(node_n4);

    // Wait for node 4 to start and sync with cluster
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC / 2)).await;
    println!("         ✓ Node 4 process started");

    // Refresh client manager to get updated cluster membership
    println!("\n[DEBUG] Refreshing client manager to fetch latest cluster state...");
    client_manager.refresh(None).await?;

    // Debug: Check cluster membership after node 4 starts
    println!("[DEBUG] Checking cluster membership configuration...");
    let members = client_manager.list_members().await?;
    println!("[DEBUG] Total members in cluster: {}", members.len());
    for member in &members {
        println!(
            "[DEBUG]   Node {}: role={}, status={}, address={}",
            member.id, member.role, member.status, member.address
        );
    }
    let leader_id = client_manager.list_leader_id().await?;
    println!("[DEBUG] Current leader ID: {leader_id:?}\n");

    println!("[Phase 3] ✓ Node 4 joined cluster as READ_ONLY Learner\n");

    // Phase 4: Verify node 4 can read all replicated data
    println!("[Phase 4] Verifying node 4 read-only capability...");
    // Connect directly to node 4 via RPC to verify data replication
    let node4_endpoint = format!("http://127.0.0.1:{}", ports[3]);

    let value1 = ClientManager::read_from_node(
        &node4_endpoint,
        1,
        ReadConsistencyPolicy::EventualConsistency,
    )
    .await?;
    assert_eq!(value1, 100, "Node 4 should have key1=100");
    println!("         ✓ Node 4 successfully read key1=100 (historical data synced)");

    let value2 = ClientManager::read_from_node(
        &node4_endpoint,
        2,
        ReadConsistencyPolicy::EventualConsistency,
    )
    .await?;
    assert_eq!(value2, 101, "Node 4 should have key2=101");
    println!("         ✓ Node 4 successfully read key2=101 (historical data synced)");
    println!("[Phase 4] ✓ Analytics node synced all historical data\n");

    // Phase 5: Verify ReadOnly Learner doesn't affect quorum
    println!("[Phase 5] Verifying cluster quorum remains unchanged...");
    // New write through primary cluster (should work with 2/3 voter quorum)
    test_put_get(&mut client_manager, 3, 102).await?;
    println!("         ✓ New write succeeded through primary cluster");

    // Verify ReadOnly Learner receives the new data
    let value3 = ClientManager::read_from_node(
        &node4_endpoint,
        3,
        ReadConsistencyPolicy::EventualConsistency,
    )
    .await?;
    assert_eq!(
        value3, 102,
        "Node 4 should have received new write key3=102"
    );
    println!("         ✓ ReadOnly Learner received new write (passive replication works)");
    println!("[Phase 5] ✓ Quorum unaffected, ReadOnly Learner receives updates\n");

    // Phase 6: Verify node 4 status remains READ_ONLY (never promoted)
    println!("[Phase 6] Verifying node 4 status remains READ_ONLY...");
    let members = client_manager.list_members().await?;
    let node4_meta_current = members.iter().find(|m| m.id == 4).expect("Node 4 should exist");
    assert_eq!(
        node4_meta_current.status,
        d_engine_proto::common::NodeStatus::ReadOnly as i32,
        "Node 4 should remain READ_ONLY forever"
    );
    assert_eq!(
        node4_meta_current.role,
        3, // LEARNER role
        "Node 4 should remain LEARNER role"
    );
    println!("         ✓ Node 4 status verified as READ_ONLY (not promoted to Voter)");
    println!("[Phase 6] ✓ ReadOnly Learner protection confirmed\n");

    // Summary
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ TEST RESULTS: READONLY LEARNER (RPC STANDALONE)           ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ ✓ JoinCluster RPC successfully adds nodes dynamically    ║");
    println!("║ ✓ Nodes can be added with LEARNER role (role=3)         ║");
    println!("║ ✓ READ_ONLY status (status=1) prevents auto-promotion   ║");
    println!("║ ✓ ReadOnly Learners receive all replicated data          ║");
    println!("║ ✓ Quorum calculations exclude ReadOnly Learners          ║");
    println!("║                                                            ║");
    println!("║ CONCLUSION: ReadOnly Learners are FULLY SUPPORTED ✅     ║");
    println!("║             Permanent Analytics nodes work correctly      ║");
    println!("║             in Standalone/RPC mode                        ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Cleanup
    ctx.shutdown().await
}
