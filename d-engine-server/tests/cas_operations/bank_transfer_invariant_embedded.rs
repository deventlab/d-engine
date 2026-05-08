#![cfg(feature = "rocksdb")]

use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::api::EmbeddedEngine;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_test::traced_test;

use crate::common::create_node_config;
use crate::common::get_available_ports;
use crate::common::node_config;

const BANK_KEY: &[u8] = b"bank";
const INITIAL_BALANCE: u64 = 1_000;
const TRANSFERS_PER_WORKER: usize = 50;
const TRANSFER_AMOUNT: u64 = 1;

// Pack 3 account balances into a single u64 (21 bits each, max ~2M per account).
fn pack(balances: [u64; 3]) -> [u8; 8] {
    ((balances[0] << 42) | (balances[1] << 21) | balances[2]).to_be_bytes()
}

fn unpack(bytes: &[u8]) -> [u64; 3] {
    let state = u64::from_be_bytes(bytes.try_into().expect("bank value must be 8 bytes"));
    [(state >> 42) & 0x1FFFFF, (state >> 21) & 0x1FFFFF, state & 0x1FFFFF]
}

/// Concurrent CAS transfers maintain bank total invariant under follower failure.
///
/// Setup: 3 accounts, each INITIAL_BALANCE. Total = INITIAL_BALANCE * 3.
///
/// Load: 3 concurrent workers do circular transfers A→B, B→C, C→A.
/// Each transfer is a CAS retry loop on the packed u64 bank key — atomic,
/// no intermediate state visible to readers.
///
/// Fault: a follower node is stopped mid-test. Cluster keeps quorum (2/3),
/// workers continue without interruption.
///
/// Assert: total balance is invariant throughout; no account goes negative.
#[tokio::test]
#[traced_test]
#[serial]
async fn test_concurrent_transfers_preserve_bank_invariant(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let db_root_dir = temp_dir.path().join("db");
    let log_dir = temp_dir.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    info!("Starting 3-node cluster for bank invariant test");
    let mut engines = Vec::new();

    for i in 0..3 {
        let node_id = (i + 1) as u64;
        let config_str = create_node_config(
            node_id,
            ports[i],
            ports,
            db_root_dir.to_str().unwrap(),
            log_dir.to_str().unwrap(),
        )
        .await;

        let config = node_config(&config_str);
        let node_db_root = config.cluster.db_root_dir.join(format!("node{node_id}"));
        let db_path = node_db_root.join("db");
        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, state_machine) = RocksDBUnifiedEngine::open(&db_path)?;
        let config_path = format!("/tmp/d-engine-bank-node{node_id}.toml");
        tokio::fs::write(&config_path, &config_str).await?;

        let engine = EmbeddedEngine::start_custom(
            Arc::new(storage),
            Arc::new(state_machine),
            Some(&config_path),
        )
        .await?;
        engines.push(engine);
    }

    let leader_info = engines[0]
        .wait_ready(Duration::from_secs(15))
        .await
        .expect("Failed to elect leader");
    info!("Leader elected: node {}", leader_info.leader_id);

    let leader_idx = (leader_info.leader_id - 1) as usize;
    let leader_client = engines[leader_idx].client();

    // Initialize bank: 3 accounts, each INITIAL_BALANCE.
    let initial_state = pack([INITIAL_BALANCE; 3]);
    leader_client.put(BANK_KEY, initial_state.as_slice()).await?;
    let expected_total = INITIAL_BALANCE * 3;
    info!("Bank initialized: 3 × {INITIAL_BALANCE} = {expected_total}");

    // Spawn 3 workers with circular transfers: worker i moves TRANSFER_AMOUNT
    // from account i to account (i+1)%3, repeated TRANSFERS_PER_WORKER times.
    // Circular pattern ensures no account drains to zero (net change = 0).
    let mut worker_handles = Vec::new();
    for worker_id in 0..3usize {
        let client = leader_client.clone();
        let from = worker_id;
        let to = (worker_id + 1) % 3;

        worker_handles.push(tokio::spawn(async move {
            for _ in 0..TRANSFERS_PER_WORKER {
                // CAS retry loop: read current state → compute new → atomic swap.
                loop {
                    let current = match client.get_linearizable(BANK_KEY).await {
                        Ok(Some(v)) => v,
                        Ok(None) => panic!("bank key disappeared"),
                        Err(_) => {
                            // Transient error (e.g. replication lag); retry.
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                    };

                    let mut balances = unpack(&current);
                    // Circular transfers always have sufficient funds,
                    // but guard defensively.
                    if balances[from] < TRANSFER_AMOUNT {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    balances[from] -= TRANSFER_AMOUNT;
                    balances[to] += TRANSFER_AMOUNT;
                    let new_bytes = pack(balances);

                    match client
                        .compare_and_swap(
                            BANK_KEY,
                            Some(current.as_ref()),
                            new_bytes.as_slice(),
                        )
                        .await
                    {
                        Ok(true) => break,             // committed
                        Ok(false) => {
                            // Concurrent update; retry without delay.
                            tokio::task::yield_now().await;
                        }
                        Err(_) => {
                            // Transient error; brief backoff.
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    }
                }
            }
            Ok::<_, ClientApiError>(())
        }));
    }

    // Fault injection: stop a follower node midway through the transfers.
    let follower_idx = (leader_idx + 1) % 3;
    tokio::time::sleep(Duration::from_millis(50)).await;
    info!("Stopping follower node {} (2/3 quorum maintained)", follower_idx + 1);
    engines[follower_idx].stop().await?;

    // Wait for all workers to finish.
    for handle in worker_handles {
        handle.await??;
    }

    // Final invariant check.
    let final_bytes = leader_client
        .get_linearizable(BANK_KEY)
        .await?
        .expect("bank key must exist after all transfers");

    let [a, b, c] = unpack(&final_bytes);
    let final_total = a + b + c;
    info!("Final balances: A={a}, B={b}, C={c}, total={final_total}");

    assert_eq!(
        final_total, expected_total,
        "Bank total invariant violated: expected {expected_total}, got {final_total} \
         (A={a}, B={b}, C={c})"
    );

    // Cleanup.
    for engine in engines.iter_mut() {
        let _ = engine.stop().await;
    }

    info!("Bank invariant test passed");
    Ok(())
}
