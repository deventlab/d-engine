//! Integration tests for `GrpcClient::watch_membership()` — Ticket #328
//!
//! Verifies that a gRPC client receives `MembershipSnapshot` events via server-side
//! streaming when cluster membership changes in standalone (gRPC) mode.
//!
//! ## What is tested
//!
//! | # | Test | Key assertion |
//! |---|------|---------------|
//! | 1 | [`test_grpc_watch_membership_receives_snapshot_on_learner_join`] | Stream yields updated snapshot with the new learner after JoinCluster |
//!
//! ## Standalone mode specifics
//!
//! Unlike the embedded test (`watch_membership_embedded.rs`), this exercises the full
//! gRPC path:
//!
//! ```text
//! GrpcClient::watch_membership()
//!   → HTTP/2 server-streaming RPC
//!   → Node::watch_membership() handler
//!   → WatchStream::new(membership_rx)  [mark_changed() → immediate initial snapshot]
//!   → protobuf serialize
//!   → tonic::Streaming<MembershipSnapshot> on the client side
//! ```
//!
//! ## Stream delivery guarantee
//!
//! `Node::watch_membership()` calls `rx.mark_changed()` before creating the stream,
//! so the first item yielded is always the current cluster state at subscribe time.
//! Subsequent items arrive for each committed ConfChange (AddNode, Promote, Remove).
//! The stream terminates with `Err(UNAVAILABLE)` when the server shuts down.

use std::time::Duration;

use d_engine_client::ClientBuilder;
use futures::StreamExt;
use tempfile::TempDir;
use tokio::time::timeout;

use crate::common::TestContext;
use crate::common::create_node_config;
use crate::common::create_node_config_with_role;
use crate::common::get_available_ports;
use crate::common::node_config;
use crate::common::start_node;

/// How long to wait for leader election before opening the watch stream.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(8);

/// How long to wait for a single membership-change snapshot to arrive on the stream.
const MEMBERSHIP_CHANGE_TIMEOUT: Duration = Duration::from_secs(10);

/// `GrpcClient::watch_membership()` receives a `MembershipSnapshot` when a learner joins.
///
/// ## Scenario
///
/// 1. Start a 2-voter standalone cluster (nodes 1 and 2).
/// 2. Wait for leader election (8 s).
/// 3. Open a `watch_membership()` gRPC stream — server delivers the current snapshot
///    immediately due to `mark_changed()`.
/// 4. Start node 3 as Learner (role=4, status=Active): it calls `JoinCluster` on boot,
///    causing the leader to commit an `AddNode` ConfChange via Raft consensus.
/// 5. Wait for the next snapshot on the stream.
///
/// ## Assertions
///
/// - Initial snapshot: at least one voter, no learners.
/// - Change snapshot: node 3 in `learners`; `committed_index > 0`.
#[tokio::test]
async fn test_grpc_watch_membership_receives_snapshot_on_learner_join()
-> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let base_path = temp_dir.path();
    let db_root = base_path.join("db");
    let log_dir = base_path.join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Start a 2-voter cluster. Nodes 1 and 2 do not know about node 3 yet.
    let two_ports = &ports[..2];
    let mut graceful_txs = Vec::new();
    let mut node_handles = Vec::new();

    for (i, &port) in two_ports.iter().enumerate() {
        let node_id = (i + 1) as u64;
        let node_dir = db_root.join(node_id.to_string());
        let node_log = log_dir.join(node_id.to_string());
        tokio::fs::create_dir_all(&node_dir).await?;
        tokio::fs::create_dir_all(&node_log).await?;

        let toml = create_node_config(
            node_id,
            port,
            two_ports,
            node_dir.to_str().unwrap(),
            node_log.to_str().unwrap(),
        )
        .await;
        let config = node_config(&toml);
        let (tx, handle) = start_node(config, None, None).await?;
        graceful_txs.push(tx);
        node_handles.push(handle);
    }

    // Allow time for leader election before subscribing.
    tokio::time::sleep(ELECTION_TIMEOUT).await;

    // Connect the gRPC client to the 2-node cluster.
    let endpoints: Vec<String> =
        two_ports.iter().map(|&p| format!("http://127.0.0.1:{p}")).collect();
    let client = ClientBuilder::new(endpoints)
        .connect_timeout(Duration::from_secs(10))
        .request_timeout(Duration::from_secs(30))
        .build()
        .await?;

    // Open the watch_membership stream. Server delivers current snapshot immediately.
    let mut stream = client.watch_membership().await?;

    // Receive the initial snapshot — 2 voters, no learners.
    let initial = timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timed out waiting for initial snapshot")
        .expect("stream ended before initial snapshot")?;

    assert!(
        !initial.members.is_empty(),
        "Initial snapshot must contain at least one voter; got members={:?}",
        initial.members
    );
    assert!(
        initial.learners.is_empty(),
        "No learners expected before node 3 joins; got learners={:?}",
        initial.learners
    );

    // Start node 3 as Learner (role=4, status=Active).
    // On boot it sends JoinCluster RPC to the leader, which commits an AddNode ConfChange.
    // Active status (3) prevents auto-promotion — exactly one membership event fires.
    {
        let node_id = 3u64;
        let port = ports[2];
        let node_dir = db_root.join(node_id.to_string());
        let node_log = log_dir.join(node_id.to_string());
        tokio::fs::create_dir_all(&node_dir).await?;
        tokio::fs::create_dir_all(&node_log).await?;

        let toml = create_node_config_with_role(
            node_id,
            port,
            ports, // all 3 ports — node 3 needs to know where to send JoinCluster
            4,     // role=4 → Learner
            node_dir.to_str().unwrap(),
            node_log.to_str().unwrap(),
        )
        .await;
        let config = node_config(&toml);
        let (tx, handle) = start_node(config, None, None).await?;
        graceful_txs.push(tx);
        node_handles.push(handle);
    }

    // Wait for the AddNode ConfChange to commit and the updated snapshot to arrive.
    let snapshot = timeout(MEMBERSHIP_CHANGE_TIMEOUT, stream.next())
        .await
        .expect("timed out waiting for membership snapshot after learner join")
        .expect("stream ended before membership change snapshot")?;

    assert!(
        snapshot.learners.contains(&3),
        "Snapshot after node 3 joins must include node 3 in learners; got learners={:?}",
        snapshot.learners
    );
    assert!(
        snapshot.committed_index > 0,
        "committed_index must be > 0 after an AddNode ConfChange commits; got: {}",
        snapshot.committed_index
    );

    TestContext {
        graceful_txs,
        node_handles,
    }
    .shutdown()
    .await?;
    Ok(())
}
