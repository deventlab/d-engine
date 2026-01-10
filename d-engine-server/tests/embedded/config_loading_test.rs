//! Integration tests for configuration loading with EmbeddedEngine.
//!
//! Verifies the bug fix for start_with() failing when CONFIG_PATH env is not set.

use d_engine_server::EmbeddedEngine;
use std::io::Write;
use std::time::Duration;

#[tokio::test]
async fn test_start_with_no_config_path_env() {
    // Clear CONFIG_PATH to reproduce the bug scenario
    unsafe {
        std::env::remove_var("CONFIG_PATH");
    }

    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("engine.toml");
    let db_path = temp_dir.path().join("db");

    // Create valid config file
    let mut file = std::fs::File::create(&config_path).unwrap();
    write!(
        file,
        r#"
[cluster]
node_id = 1
db_root_dir = "{}"

[[cluster.initial_cluster]]
id = 1
address = "127.0.0.1:0"
role = 2
status = 2
"#,
        db_path.display()
    )
    .unwrap();
    drop(file);

    // Core test: start_with should work without CONFIG_PATH env var
    let engine = EmbeddedEngine::start_with(config_path.to_str().unwrap())
        .await
        .expect("Should start without CONFIG_PATH env");

    // Verify engine works correctly
    let leader = engine.wait_ready(Duration::from_secs(5)).await.expect("Should elect leader");

    assert_eq!(leader.leader_id, 1);

    engine.stop().await.expect("Should stop cleanly");
}
