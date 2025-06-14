use serial_test::serial;
use temp_env::with_vars;

use super::*;

#[test]
fn default_config_should_initialize_with_hardcoded_values() {
    let config = RaftNodeConfig::default();

    assert_eq!(config.cluster.node_id, 1);
    assert_eq!(config.raft.election.election_timeout_min, 500);
    assert_eq!(config.network.request_timeout_in_ms, 100);
    assert!(!config.tls.enable_tls);
}

#[test]
#[serial]
fn new_should_merge_environment_overrides() {
    with_vars(
        vec![
            ("RAFT__NETWORK__BUFFER_SIZE", Some("1024")),
            ("RAFT__TLS__ENABLE_MTLS", Some("true")),
        ],
        || {
            let config = RaftNodeConfig::new().unwrap();

            assert_eq!(config.network.buffer_size, 1024);
            assert!(config.tls.enable_mtls);
        },
    );
}

#[test]
fn with_override_config_should_merge_file_settings() {
    // Create temporary directory and configuration file
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("dynamic_config.toml");

    // Dynamically generate TOML configuration content
    std::fs::write(
        &config_path,
        r#"
        [cluster]
        db_root_dir = "/tmp/xx/db" # Override default value

        [raft.election]
        election_timeout_min = 1000 # Override default value
        election_timeout_max = 3000 # Add new field
        "#,
    )
    .unwrap();

    let empty_vars: Vec<(&str, Option<&str>)> = vec![];
    with_vars(empty_vars, || {
        // Execute test logic
        let base_config = RaftNodeConfig::new().expect("success");
        let result = base_config.with_override_config(config_path.to_str().unwrap());

        // Verify result
        assert!(result.is_ok());
        let config = result.unwrap();

        assert_eq!(config.cluster.db_root_dir.as_os_str().to_str(), Some("/tmp/xx/db"));
        assert_eq!(config.raft.election.election_timeout_min, 1000);
        assert_eq!(config.raft.election.election_timeout_max, 3000);
    });
}

#[test]
fn validation_should_fail_with_invalid_cluster_config() {
    let mut config = RaftNodeConfig::default();
    config.cluster.node_id = 0;

    assert!(config.validate().is_err());
}

#[test]
fn validation_should_detect_invalid_tls_settings() {
    let mut config = RaftNodeConfig::default();
    config.tls.enable_mtls = true;
    config.tls.enable_tls = false;

    assert!(config.validate().is_err());
}

#[test]
fn environment_variables_should_have_highest_priority() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("test_config.toml");
    std::fs::write(
        &config_path,
        r#"
        [cluster]
        node_id = 100
        initial_cluster = [
            { id = 100, name = "n1", address = "127.0.0.1:8081", role = 1, status = 0 },
            { id = 200, name = "n2", address = "127.0.0.1:9082", role = 1, status = 0 },
            { id = 300, name = "n3", address = "127.0.0.1:9083", role = 1, status = 0 },
        ]
        "#,
    )
    .unwrap();

    with_vars(
        vec![
            ("CONFIG_PATH", Some(config_path.to_str().unwrap())),
            ("RAFT__CLUSTER__NODE_ID", Some("200")),
        ],
        || {
            let config = RaftNodeConfig::new().unwrap();
            assert_eq!(config.cluster.node_id, 200);
        },
    );
}

#[test]
fn invalid_config_file_should_return_descriptive_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("invalid.toml");
    std::fs::write(
        &config_path,
        r#"
        invalid_toml = [ should_fail
        "#,
    )
    .unwrap();

    with_vars(vec![("CONFIG_PATH", Some(config_path.to_str().unwrap()))], || {
        assert!(RaftNodeConfig::new().is_err());
    });
}

#[test]
fn config_should_handle_nested_structures_correctly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("nested.toml");
    std::fs::write(
        &config_path,
        r#"
        [retry.election]
        max_retries = 10
        [retry]
        append_entries.max_retries = 250
        "#,
    )
    .unwrap();

    with_vars(vec![("CONFIG_PATH", Some(config_path.to_str().unwrap()))], || {
        let config = RaftNodeConfig::new().unwrap();
        assert_eq!(config.retry.election.max_retries, 10);
        assert_eq!(config.retry.append_entries.max_retries, 250);
    });
}

#[test]
fn type_mismatch_in_config_should_fail_gracefully() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("invalid_type.toml");
    std::fs::write(
        &config_path,
        r#"
        [cluster]
        initial_cluster = []
        [network]
        connect_timeout_in_ms = 0
        "#,
    )
    .unwrap();

    with_vars(vec![("CONFIG_PATH", Some(config_path.to_str().unwrap()))], || {
        assert!(RaftNodeConfig::new().is_err());
    });
}

/// Tests for node join status detection
mod join_status_tests {
    use super::*;
    use crate::proto::cluster::{NodeMeta, NodeStatus};

    /// # Case 1: Node is in joining status
    #[test]
    fn test_is_joining_case1_active_joining() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                address: "127.0.0.1:8080".to_string(),
                role: 1, // FOLLOWER
                status: NodeStatus::Joining as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                address: "127.0.0.1:8081".to_string(),
                role: 1, // FOLLOWER
                status: NodeStatus::Active as i32,
                ..Default::default()
            },
        ];

        assert!(config.is_joining(), "Node 100 should be in joining status");
    }

    /// # Case 2: Node is active, not joining
    #[test]
    fn test_is_joining_case2_active_not_joining() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 200;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                status: NodeStatus::Joining as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                status: NodeStatus::Active as i32,
                ..Default::default()
            },
        ];

        assert!(!config.is_joining(), "Node 200 should not be joining");
    }

    /// # Case 3: Node not in initial cluster
    #[test]
    fn test_is_joining_case3_node_not_found() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 300;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                status: NodeStatus::Joining as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                status: NodeStatus::Active as i32,
                ..Default::default()
            },
        ];

        assert!(!config.is_joining(), "Node 300 not in cluster should return false");
    }

    /// # Case 4: Empty initial cluster
    #[test]
    fn test_is_joining_case4_empty_cluster() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = Vec::new();

        assert!(!config.is_joining(), "Empty cluster should return false");
    }

    /// # Case 5: Multiple joining nodes (shouldn't happen but test anyway)
    #[test]
    fn test_is_joining_case5_multiple_joining() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                status: NodeStatus::Joining as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                status: NodeStatus::Joining as i32,
                ..Default::default()
            },
        ];

        assert!(config.is_joining(), "Node 100 should still be joining");
    }

    /// # Case 6: Draining status
    #[test]
    fn test_is_joining_case6_draining_status() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![NodeMeta {
            id: 100,
            status: NodeStatus::Draining as i32,
            ..Default::default()
        }];

        assert!(!config.is_joining(), "Draining status should not be joining");
    }

    /// # Case 7: Invalid status value
    #[test]
    fn test_is_joining_case7_invalid_status() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![NodeMeta {
            id: 100,
            status: 99, // Invalid status
            ..Default::default()
        }];

        assert!(!config.is_joining(), "Invalid status should not be joining");
    }
}
