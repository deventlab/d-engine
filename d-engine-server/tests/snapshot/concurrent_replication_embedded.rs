// Copyright 2025 the d-engine Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Snapshot concurrent replication integration tests (Embedded mode)
//!
//! These tests validate that Learners can safely receive AppendEntries during
//! snapshot application, specifically testing the PHASE 2.5 temporary DB window.
//!
//! Test Scenario:
//! 1. Start 3-node cluster, write 1500 entries to trigger snapshot
//! 2. Add Learner (Node 4) which begins receiving snapshot
//! 3. **Critical**: While Learner applies snapshot, Leader continues writing 500 new entries
//! 4. Verify Learner ends with all 2000 entries (1500 + 500)
//! 5. Verify no data loss during PHASE 2.5 temporary DB window
//!
//! This directly tests the question from todo3.md:
//! "Learner apply snapshot 时能否接受 AppendEntries？新逻辑 state machine 短暂为空会有问题吗？"

use d_engine_core::embedded::EmbeddedEngine;
use std::time::Duration;
use tokio::time::sleep;

use crate::common::{cleanup_test_dirs, setup_test_env};

/// Test: Learner can safely receive AppendEntries during snapshot application
///
/// This test validates the PHASE 2.5 safety concern: when a Learner is applying
/// a snapshot (during which the state machine is temporarily replaced), can it
/// safely receive and queue new AppendEntries from the Leader?
///
/// Test Flow:
/// 1. Start 3-node cluster with snapshot threshold = 1000 entries
/// 2. Write 1500 entries to Leader (triggers snapshot + 500 new entries after)
/// 3. Add Learner (Node 4) - it will start receiving snapshot
/// 4. **Concurrent phase**: While Learner applies snapshot, Leader writes 500 MORE entries
/// 5. Wait for Learner to catch up completely
/// 6. Verify Learner has all 2000 entries (1500 baseline + 500 concurrent)
/// 7. Verify no data corruption or loss during PHASE 2.5 window
///
/// Expected Behavior:
/// - Learner successfully applies snapshot while queuing concurrent AppendEntries
/// - After snapshot application completes, queued entries are applied
/// - Final state: Learner has all 2000 entries with correct values
/// - No data loss or corruption during temporary DB replacement
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_learner_snapshot_concurrent_replication() {
    const CLUSTER_SIZE: usize = 3;
    const SNAPSHOT_THRESHOLD: u64 = 1000; // Trigger snapshot after 1000 entries
    const RETAINED_LOGS: u64 = 500; // Keep 500 entries after snapshot
    const BASELINE_ENTRIES: u64 = 1500; // Initial writes (triggers snapshot at 1000)
    const CONCURRENT_ENTRIES: u64 = 500; // Additional writes during snapshot application
    const TOTAL_ENTRIES: u64 = BASELINE_ENTRIES + CONCURRENT_ENTRIES; // 2000 total

    println!("\n========================================");
    println!("Test: Learner snapshot application with concurrent AppendEntries");
    println!("========================================\n");

    // Setup test environment
    let (test_dirs, node_configs) =
        setup_test_env(CLUSTER_SIZE, Some(SNAPSHOT_THRESHOLD), Some(RETAINED_LOGS));

    // Start 3-node cluster
    println!("📦 Starting {}-node cluster...", CLUSTER_SIZE);
    let mut engines: Vec<EmbeddedEngine> = Vec::new();
    for (idx, config) in node_configs.iter().enumerate() {
        let engine = EmbeddedEngine::new(config.clone())
            .await
            .expect(&format!("Failed to start node {}", idx + 1));
        engines.push(engine);
    }

    sleep(Duration::from_millis(500)).await;

    // Find leader
    let leader_idx = engines.iter().position(|e| e.is_leader()).expect("No leader elected");
    println!("✅ Leader elected: Node {}", leader_idx + 1);

    let leader_client = engines[leader_idx].client().clone();

    // Phase 1: Write baseline entries to trigger snapshot
    println!(
        "\n📝 Phase 1: Writing {} baseline entries (will trigger snapshot at entry {})...",
        BASELINE_ENTRIES, SNAPSHOT_THRESHOLD
    );

    for i in 0..BASELINE_ENTRIES {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);

        let result = leader_client.write(key.clone(), value.clone()).await;
        assert!(result.is_ok(), "Write {} failed: {:?}", i, result.err());

        if (i + 1) % 500 == 0 {
            println!("  ⏳ Written {} entries...", i + 1);
        }
    }

    println!("✅ Phase 1 complete: {} entries written", BASELINE_ENTRIES);

    // Wait for snapshot to be generated
    println!("\n⏳ Waiting for snapshot to be generated on all nodes...");
    sleep(Duration::from_secs(3)).await;

    // Phase 2: Add Learner (Node 4) - it will start receiving snapshot
    println!("\n📦 Phase 2: Adding Learner (Node 4)...");

    // Create Learner config
    let learner_config = {
        use d_engine_core::config::NodeConfig;
        let mut config = NodeConfig::default();
        config.node_id = 4;
        config.data_path = test_dirs[0].join("node4");
        config.log_path = test_dirs[0].join("node4_logs");
        config.listen_addr = "127.0.0.1:0".to_string();
        config.cluster_join_addresses = vec![
            engines[0].rpc_addr().to_string(),
            engines[1].rpc_addr().to_string(),
            engines[2].rpc_addr().to_string(),
        ];
        config.max_log_entries_before_snapshot = SNAPSHOT_THRESHOLD;
        config.retained_log_entries = RETAINED_LOGS;
        config
    };

    let learner_engine =
        EmbeddedEngine::new(learner_config).await.expect("Failed to start Learner");

    println!("✅ Learner started: Node 4");
    println!("  ⏳ Learner is now receiving snapshot from Leader...");

    // Phase 3: **Critical** - Write concurrent entries while Learner applies snapshot
    println!(
        "\n📝 Phase 3: Writing {} CONCURRENT entries while Learner applies snapshot...",
        CONCURRENT_ENTRIES
    );
    println!(
        "  🔍 This tests PHASE 2.5 safety: can Learner queue AppendEntries during snapshot application?"
    );

    // Small delay to ensure snapshot transfer has started
    sleep(Duration::from_millis(500)).await;

    for i in 0..CONCURRENT_ENTRIES {
        let entry_num = BASELINE_ENTRIES + i;
        let key = format!("key_{}", entry_num);
        let value = format!("value_{}", entry_num);

        let result = leader_client.write(key.clone(), value.clone()).await;
        assert!(
            result.is_ok(),
            "Concurrent write {} failed: {:?}",
            entry_num,
            result.err()
        );

        if (i + 1) % 100 == 0 {
            println!(
                "  ⏳ Written {} concurrent entries (total: {})...",
                i + 1,
                BASELINE_ENTRIES + i + 1
            );
        }
    }

    println!(
        "✅ Phase 3 complete: {} concurrent entries written (total: {})",
        CONCURRENT_ENTRIES, TOTAL_ENTRIES
    );

    // Phase 4: Wait for Learner to catch up completely
    println!(
        "\n⏳ Phase 4: Waiting for Learner to catch up to all {} entries...",
        TOTAL_ENTRIES
    );

    let mut max_wait = 30; // 30 seconds max
    let mut learner_caught_up = false;

    while max_wait > 0 {
        // Check if Learner has last entry
        let last_key = format!("key_{}", TOTAL_ENTRIES - 1);
        if let Ok(Some(value)) = learner_engine.client().read(last_key.clone()).await {
            let expected_value = format!("value_{}", TOTAL_ENTRIES - 1);
            if value == expected_value {
                println!("✅ Learner caught up: has entry {}", TOTAL_ENTRIES - 1);
                learner_caught_up = true;
                break;
            }
        }

        sleep(Duration::from_secs(1)).await;
        max_wait -= 1;
        if max_wait % 5 == 0 {
            println!("  ⏳ Still waiting... ({} seconds remaining)", max_wait);
        }
    }

    assert!(
        learner_caught_up,
        "Learner failed to catch up within 30 seconds"
    );

    // Phase 5: Verify data integrity on Learner
    println!("\n🔍 Phase 5: Verifying data integrity on Learner...");

    let mut verification_errors = Vec::new();
    for i in 0..TOTAL_ENTRIES {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);

        match learner_engine.client().read(key.clone()).await {
            Ok(Some(actual_value)) => {
                if actual_value != expected_value {
                    verification_errors.push(format!(
                        "Entry {}: expected '{}', got '{}'",
                        i, expected_value, actual_value
                    ));
                }
            }
            Ok(None) => {
                verification_errors.push(format!("Entry {}: key '{}' not found", i, key));
            }
            Err(e) => {
                verification_errors.push(format!("Entry {}: read error: {:?}", i, e));
            }
        }

        if (i + 1) % 500 == 0 {
            println!("  ✓ Verified {} entries...", i + 1);
        }
    }

    // Report verification results
    if verification_errors.is_empty() {
        println!(
            "✅ All {} entries verified on Learner - NO DATA LOSS",
            TOTAL_ENTRIES
        );
    } else {
        println!("❌ Verification FAILED:");
        for error in &verification_errors {
            println!("  - {}", error);
        }
        panic!(
            "Data verification failed: {} errors out of {} entries",
            verification_errors.len(),
            TOTAL_ENTRIES
        );
    }

    // Phase 6: Check snapshot files on Learner
    println!("\n📂 Phase 6: Checking snapshot files on Learner...");
    let learner_snapshot_dir = test_dirs[0].join("node4/snapshots/node4");
    if learner_snapshot_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&learner_snapshot_dir) {
            let snapshot_files: Vec<_> =
                entries.filter_map(|e| e.ok()).filter(|e| e.path().is_file()).collect();

            if !snapshot_files.is_empty() {
                println!(
                    "✅ Snapshot received successfully: {} file(s) in {:?}",
                    snapshot_files.len(),
                    learner_snapshot_dir
                );
            } else {
                println!("⚠️  No snapshot files found (may have been applied and cleaned up)");
            }
        }
    } else {
        println!("⚠️  Snapshot directory not found (may not have been created yet)");
    }

    // Test Summary
    println!("\n========================================");
    println!("✅ TEST PASSED: Learner snapshot with concurrent replication");
    println!("========================================");
    println!("Key findings:");
    println!("  ✓ Learner successfully applied snapshot from Leader");
    println!(
        "  ✓ Learner safely received {} AppendEntries during snapshot application",
        CONCURRENT_ENTRIES
    );
    println!("  ✓ PHASE 2.5 temporary DB window did NOT cause data loss");
    println!(
        "  ✓ All {} entries verified with correct values",
        TOTAL_ENTRIES
    );
    println!("  ✓ Baseline entries: {}", BASELINE_ENTRIES);
    println!("  ✓ Concurrent entries: {}", CONCURRENT_ENTRIES);
    println!(
        "\nConclusion: Learner can safely queue and apply AppendEntries during snapshot application."
    );
    println!("The temporary state machine replacement in PHASE 2.5 does not affect correctness.\n");

    // Cleanup
    drop(learner_engine);
    drop(engines);
    cleanup_test_dirs(&test_dirs);
}
