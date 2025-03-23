mod commons;
use commons::{execute_command, start_node, ClientCommands};
use dengine::Error;
use log::error;
use std::path::Path;
use std::time::Duration;
use tokio::fs::{self, remove_dir_all};
use tokio::net::TcpStream;
use tokio::time;

const WAIT_FOR_NODE_READY_IN_SEC: u64 = 6;
const LATENCY_IN_MS: u64 = 10; // we are testing linearizable read from Leader directly, the latency should less than 1ms ideally
const ITERATIONS: u64 = 10; // to make sure the result is consistent

async fn reset(case_name: &str) -> Result<(), std::io::Error> {
    // Define path
    let logs_dir = format!("tests/logs/{}", case_name);
    let db_dir = format!("tests/db/{}", case_name);

    // Make sure the parent directory exists
    fs::create_dir_all("tests/logs").await?;
    fs::create_dir_all("tests/db").await?;

    // Clean up the log directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&logs_dir)).await;

    // Clean up the database directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&db_dir)).await;

    Ok(())
}

async fn check_cluster_is_ready(peer_addr: &str, timeout_secs: u64) -> Result<(), std::io::Error> {
    let timeout_duration = Duration::from_secs(timeout_secs);
    let retry_interval = Duration::from_millis(500);

    let result = time::timeout(timeout_duration, async {
        loop {
            if TcpStream::connect(peer_addr).await.is_ok() {
                println!("Node is ready!");
                return Ok::<(), std::io::Error>(());
            } else {
                eprintln!("Node({:?}) not ready, retrying...", peer_addr);
                time::sleep(retry_interval).await;
            }
        }
    })
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            let err_msg = format!(
                "Node({:?}) did not become ready within {} seconds.",
                peer_addr, timeout_secs
            );
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, err_msg))
        }
    }
}
/// Case 1: start 3 node cluster and test simple get/put, and then stop the cluster
///
#[cfg(not(tarpaulin))]
#[tokio::test]
async fn test_cluster_put_and_lread_case1() -> Result<(), dengine::Error> {
    reset("case1").await?;
    let bootstrap_urls: Vec<String> = vec![
        "http://127.0.0.1:9083".to_string(),
        "http://127.0.0.1:9082".to_string(),
        "http://127.0.0.1:9081".to_string(),
    ];
    let (graceful_tx3, node_n3) = start_node("tests/config/case1/n3").await?;
    let (graceful_tx2, node_n2) = start_node("tests/config/case1/n2").await?;
    let (graceful_tx1, node_n1) = start_node("tests/config/case1/n1").await?;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [9081, 9082, 9083] {
        check_cluster_is_ready(&format!("127.0.0.1:{}", port), 10).await?;
    }

    // Perform test actions (e.g., CLI commands, cluster verification, etc.)
    println!("Cluster started. Running tests...");

    // Testing `put` command
    println!("Testing put command...");
    println!("put 2 202");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 2, Some(202))
            .await
            .is_ok(),
        "Put command failed!"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    // Testing `get` command
    println!("Testing get command...");
    verify_read(&bootstrap_urls, 2, 202, ITERATIONS).await;

    graceful_tx3.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx2.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx1.send(()).map_err(|_| Error::ServerError)?;
    node_n3.await??;
    node_n2.await??;
    node_n1.await??;
    Ok(()) // Return Result type
}

/// # Case 2: test one of the node restart, but with linearizable read from Leader only
///
/// In this case, performance is not something we want to test, so we want to test
///     if client's linearizable read could be achieved by reading from Leader only.
///
/// Give each put call with 10ms latency is enough for single testing on macmini16g/8c.
///
/// ## T1: L1, F2, F31
/// - put 1 1
/// - get 1
///  1
/// - put 1 2
/// - get 1
/// 2
///
/// ## T2: F2, L3
/// - put 1 3
/// - get 1
/// 3
///
/// ## T3: F1, F2, L3
/// - put 1 4
/// - get 1
/// 4
/// - put 2 20
/// - put 2 21
/// - get 2
/// 21
///
/// ## T4: stop cluster
/// ## T5: start cluster
/// - get 1
/// 4
/// - get 2
/// 21
/// - put 1 5
/// - get 1
/// 5
/// - get 2
/// 21
///
///
#[cfg(not(tarpaulin))]
#[tokio::test]
async fn test_cluster_put_and_lread_case2() -> Result<(), Error> {
    // env_logger::Builder::new()
    //     .filter_level(log::LevelFilter::Debug)
    //     .init();

    reset("case2").await?;

    let bootstrap_urls: Vec<String> = vec![
        "http://127.0.0.1:19083".to_string(),
        "http://127.0.0.1:19082".to_string(),
        "http://127.0.0.1:19081".to_string(),
    ];

    let bootstrap_urls_without_n1: Vec<String> = vec![
        "http://127.0.0.1:19083".to_string(),
        "http://127.0.0.1:19082".to_string(),
    ];

    let (graceful_tx1, node_n1) = start_node("tests/config/case2/n1").await?;
    let (graceful_tx2, node_n2) = start_node("tests/config/case2/n2").await?;
    let (graceful_tx3, node_n3) = start_node("tests/config/case2/n3").await?;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    for port in [19081, 19082, 19083] {
        check_cluster_is_ready(&format!("127.0.0.1:{}", port), 10).await?;
    }
    // T1: PUT and linearizable reads
    println!("------------------T1-----------------");
    println!("put 1 1");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 1, Some(1))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls, 1, 1, ITERATIONS).await;

    println!("put 1 2");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 1, Some(2))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls, 1, 2, ITERATIONS).await;

    // T2: Stop one node and verify reads
    println!("------------------T2-----------------");
    graceful_tx1.send(()).map_err(|e| {
        error!("Failed to send shutdown signal: {}", e);
        Error::SignalSenderClosed(format!("Failed to send shutdown signal: {}", e))
    })?;
    node_n1.await??;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    verify_read(&bootstrap_urls_without_n1, 1, 2, ITERATIONS).await;

    println!("put 1 3");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls_without_n1, 1, Some(3))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls_without_n1, 1, 3, ITERATIONS).await;

    // T3: Restart the node, perform PUT, and verify reads
    println!("------------------T3-----------------");
    let (graceful_tx1, node_n1) = start_node("tests/config/case2/n1").await?;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    verify_read(&bootstrap_urls, 1, 3, ITERATIONS).await;

    println!("put 1 4");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 1, Some(4))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls, 1, 4, ITERATIONS).await;

    println!("put 2 20");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 2, Some(20))
            .await
            .is_ok()
    );
    println!("put 2 21");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 2, Some(21))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls, 2, 21, ITERATIONS).await;

    // T4: stop cluster
    println!("------------------T4-----------------");
    // Stop the nodes and notify the parent
    graceful_tx3.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx2.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx1.send(()).map_err(|_| Error::ServerError)?;
    node_n1.await??;
    node_n2.await??;
    node_n3.await??;

    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;

    //T5: Start cluster again
    println!("------------------T5-----------------");
    let (graceful_tx1, node_n1) = start_node("tests/config/case2/n1").await?;
    let (graceful_tx2, node_n2) = start_node("tests/config/case2/n2").await?;
    let (graceful_tx3, node_n3) = start_node("tests/config/case2/n3").await?;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    tokio::time::sleep(Duration::from_secs(WAIT_FOR_NODE_READY_IN_SEC)).await;
    verify_read(&bootstrap_urls, 1, 4, ITERATIONS).await;
    verify_read(&bootstrap_urls, 2, 21, ITERATIONS).await;
    println!("put 1 5");
    assert!(
        execute_command(ClientCommands::PUT, &bootstrap_urls, 1, Some(5))
            .await
            .is_ok()
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    verify_read(&bootstrap_urls, 2, 21, ITERATIONS).await;
    verify_read(&bootstrap_urls, 1, 5, ITERATIONS).await;

    // Finally: stop cluster
    graceful_tx3.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx2.send(()).map_err(|_| Error::ServerError)?;
    graceful_tx1.send(()).map_err(|_| Error::ServerError)?;
    node_n1.await??;
    node_n2.await??;
    node_n3.await??;

    Ok(())
}

// Helper function to verify linearizable reads
async fn verify_read(urls: &Vec<String>, key: u64, expected_value: u64, iterations: u64) {
    for _ in 0..iterations {
        match execute_command(ClientCommands::LREAD, urls, key, None).await {
            Ok(v) => assert_eq!(
                v, expected_value,
                "Linearizable read failed for key {}!",
                key
            ),
            Err(e) => {
                eprintln!("{:?}", e);
                assert!(false);
            }
        }
    }
}
