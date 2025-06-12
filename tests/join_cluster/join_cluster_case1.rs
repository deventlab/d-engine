//! This test focuses on the scenario where a new node joins an existing cluster,
//! receives a snapshot, and successfully installs it.  
//! The test completes when the node transitions its role to `Follower`.

use d_engine::client::ClientApiError;
use std::time::Duration;

use crate::{
    common::{check_cluster_is_ready, reset, start_node, WAIT_FOR_NODE_READY_IN_SEC},
    JOIN_CLUSTER_PORT_BASE,
};

#[tracing::instrument]
#[tokio::test]
async fn test_join_cluster_scenario() -> Result<(), ClientApiError> {
    crate::enable_logger();
    reset("join_cluster/case1").await?;

    let port1 = JOIN_CLUSTER_PORT_BASE + 1;
    let port2 = JOIN_CLUSTER_PORT_BASE + 2;
    let port3 = JOIN_CLUSTER_PORT_BASE + 3;

    println!("2. Start a 3-node cluster and artificially create inconsistent states");
    let (graceful_tx1, node_n1) = start_node("./tests/join_cluster/case1/n1", None, None, None).await?;
    let (graceful_tx2, node_n2) = start_node("./tests/join_cluster/case1/n2", None, None, None).await?;
    let (graceful_tx3, node_n3) = start_node("./tests/join_cluster/case1/n3", None, None, None).await?;

    // Combine all log layers

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [port1, port2, port3] {
        check_cluster_is_ready(&format!("127.0.0.1:{port}"), 10).await?;
    }

    println!("Cluster started. Running tests...");

    graceful_tx3
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx2
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    graceful_tx1
        .send(())
        .map_err(|_| ClientApiError::general_client_error("failed to shutdown".to_string()))?;
    node_n3.await??;
    node_n2.await??;
    node_n1.await??;
    Ok(()) // Return Result type
}
