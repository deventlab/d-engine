use super::*;
use serial_test::serial;
use std::io::Write;
use temp_env::with_vars;

fn cleanup_all_raft_env_vars() {
    for (key, _) in std::env::vars() {
        if key.starts_with("RAFT__") || key == "CONFIG_PATH" {
            // SAFETY: Test-only cleanup in single-threaded test context
            unsafe {
                std::env::remove_var(&key);
            }
        }
    }
}

#[test]
#[serial]
fn default_config_should_initialize_with_hardcoded_values() {
    let config = RaftNodeConfig::default();

    assert_eq!(config.cluster.node_id, 1);
    assert_eq!(config.raft.election.election_timeout_min, 500);
    assert_eq!(config.network.control.request_timeout_in_ms, 100);
    assert!(!config.tls.enable_tls);
}

#[test]
#[serial]
fn new_should_merge_environment_overrides() {
    cleanup_all_raft_env_vars();
    with_vars(
        vec![
            ("RAFT__NETWORK__BUFFER_SIZE", Some("1025")),
            ("RAFT__CLUSTER__DATA_DIR", Some("test-data")),
        ],
        || {
            let config = RaftNodeConfig::new().unwrap().validate().unwrap();

            assert_eq!(config.network.buffer_size, 1025);
        },
    );
}

#[test]
#[serial]
fn with_override_config_should_merge_file_settings() {
    cleanup_all_raft_env_vars();
    // Create temporary directory and configuration file
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("dynamic_config.toml");

    // Dynamically generate TOML configuration content
    std::fs::write(
        &config_path,
        r#"
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
        let result = base_config
            .with_override_config(config_path.to_str().unwrap())
            .and_then(|c| c.validate());

        // Verify result
        assert!(result.is_ok());
        let config = result.unwrap();

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
#[serial]
fn environment_variables_should_have_highest_priority() {
    cleanup_all_raft_env_vars();
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("test_config.toml");
    std::fs::write(
        &config_path,
        r#"
        [cluster]
        node_id = 100
        data_dir = "test-data"
        initial_cluster = [
            { id = 100, name = "n1", address = "127.0.0.1:8081", role = 1, status = 1 },
            { id = 200, name = "n2", address = "127.0.0.1:9082", role = 1, status = 1 },
            { id = 300, name = "n3", address = "127.0.0.1:9083", role = 1, status = 1 },
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
            let config = RaftNodeConfig::new().unwrap().validate().unwrap();

            // Debug output to see what's in the configuration
            println!("Final node_id: {}", config.cluster.node_id);
            println!(
                "Initial cluster nodes: {:?}",
                config.cluster.initial_cluster.iter().map(|n| n.id).collect::<Vec<_>>()
            );

            assert_eq!(config.cluster.node_id, 200);
        },
    );
}

#[ignore = "TODO"]
#[test]
#[serial]
fn invalid_config_file_should_return_descriptive_error() {
    cleanup_all_raft_env_vars();
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("invalid.toml");
    std::fs::write(
        &config_path,
        r#"
        invalid_toml = [ should_fail
        "#,
    )
    .unwrap();

    with_vars(
        vec![("CONFIG_PATH", Some(config_path.to_str().unwrap()))],
        || {
            assert!(RaftNodeConfig::new().is_err());
        },
    );
}

#[test]
#[serial]
fn config_should_handle_nested_structures_correctly() {
    cleanup_all_raft_env_vars();
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("nested.toml");
    std::fs::write(
        &config_path,
        r#"
        [cluster]
        data_dir = "test-data"
        [retry.election]
        max_retries = 10
        [retry]
        append_entries.max_retries = 250
        "#,
    )
    .unwrap();

    with_vars(
        vec![("CONFIG_PATH", Some(config_path.to_str().unwrap()))],
        || {
            let config = RaftNodeConfig::new().unwrap().validate().unwrap();
            assert_eq!(config.retry.election.max_retries, 10);
            assert_eq!(config.retry.append_entries.max_retries, 250);
        },
    );
}

#[ignore = "TODO"]
#[test]
#[serial]
fn type_mismatch_in_config_should_fail_gracefully() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("invalid_type.toml");

    std::fs::write(
        &config_path,
        r#"
        [network.control]
        connect_timeout_in_ms = "oops"
        "#,
    )
    .unwrap();

    let raw = Config::builder()
        .add_source(File::with_name(config_path.to_str().unwrap()))
        .build()
        .unwrap();

    let result = raw.try_deserialize::<NetworkConfig>();
    assert!(
        result.is_err(),
        "Expected parsing to fail due to type mismatch"
    );
}

/// Tests for node join status detection
mod join_status_tests {
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    use super::*;

    /// # Case 1: Node is a learner with promotable status
    #[test]
    fn test_is_joining_case1_active_promotable() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                address: "127.0.0.1:8080".to_string(),
                role: d_engine_proto::common::NodeRole::Learner as i32,
                status: NodeStatus::Promotable as i32,
            },
            NodeMeta {
                id: 200,
                address: "127.0.0.1:8081".to_string(),
                role: d_engine_proto::common::NodeRole::Follower as i32,
                status: NodeStatus::Active as i32,
            },
        ];

        assert!(
            config.is_learner(),
            "Node 100 with role=Learner should return true"
        );
    }

    /// # Case 2: Node is active, not promotable
    #[test]
    fn test_is_joining_case2_active_not_promotable() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 200;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                status: NodeStatus::Promotable as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                status: NodeStatus::Active as i32,
                ..Default::default()
            },
        ];

        assert!(!config.is_learner(), "Node 200 should not be promotable");
    }

    /// # Case 3: Node not in initial cluster
    #[test]
    fn test_is_joining_case3_node_not_found() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 300;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                status: NodeStatus::Promotable as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                status: NodeStatus::Active as i32,
                ..Default::default()
            },
        ];

        assert!(
            !config.is_learner(),
            "Node 300 not in cluster should return false"
        );
    }

    /// # Case 4: Empty initial cluster
    #[test]
    fn test_is_joining_case4_empty_cluster() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = Vec::new();

        assert!(!config.is_learner(), "Empty cluster should return false");
    }

    /// # Case 5: Multiple learner nodes (shouldn't happen but test anyway)
    #[test]
    fn test_is_joining_case5_multiple_promotable() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![
            NodeMeta {
                id: 100,
                role: d_engine_proto::common::NodeRole::Learner as i32,
                status: NodeStatus::Promotable as i32,
                ..Default::default()
            },
            NodeMeta {
                id: 200,
                role: d_engine_proto::common::NodeRole::Learner as i32,
                status: NodeStatus::Promotable as i32,
                ..Default::default()
            },
        ];

        assert!(
            config.is_learner(),
            "Node 100 with role=Learner should return true"
        );
    }

    /// # Case 6: ReadOnly status
    #[test]
    fn test_is_joining_case6_readonly_status() {
        let mut config = RaftNodeConfig::default();
        config.cluster.node_id = 100;
        config.cluster.initial_cluster = vec![NodeMeta {
            id: 100,
            status: NodeStatus::ReadOnly as i32,
            ..Default::default()
        }];

        assert!(
            !config.is_learner(),
            "ReadOnly status should not be promotable"
        );
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

        assert!(!config.is_learner(), "Invalid status should not be joining");
    }
}

// ============================================================================
// Delayed Validation Tests (validate() mechanism)
// ============================================================================

#[test]
fn test_new_returns_unvalidated_config() {
    cleanup_all_raft_env_vars();

    // new() should succeed even if config might be invalid in release builds
    let cfg = RaftNodeConfig::new().expect("new() should succeed");

    // Config exists but not validated yet
    assert!(cfg.cluster.node_id > 0);
}

#[test]
fn test_invalid_config_fails_on_validate() {
    let mut cfg = RaftNodeConfig::default();
    cfg.cluster.node_id = 0; // Invalid node_id

    let result = cfg.validate();
    assert!(result.is_err(), "validate() should fail for invalid config");
}

#[test]
fn test_override_then_validate_succeeds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("valid.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    write!(
        file,
        r#"
[cluster]
node_id = 1

[[cluster.initial_cluster]]
id = 1
address = "127.0.0.1:9091"
role = 3
status = 3
"#
    )
    .unwrap();
    drop(file);

    let cfg = RaftNodeConfig::new()
        .expect("new() should succeed")
        .with_override_config(config_path.to_str().unwrap())
        .expect("override should succeed")
        .validate()
        .expect("validate should succeed");

    assert_eq!(cfg.cluster.node_id, 1);
}

// ============================================================================
// Config Loading Method Tests
// ============================================================================

#[test]
#[serial]
fn test_config_path_env_loads_and_validates() {
    cleanup_all_raft_env_vars();

    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("test.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    write!(
        file,
        r#"
[cluster]
node_id = 99

[[cluster.initial_cluster]]
id = 99
address = "127.0.0.1:9091"
role = 3
status = 3
"#
    )
    .unwrap();
    drop(file);

    unsafe {
        std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
    }

    let cfg = RaftNodeConfig::new()
        .expect("Should load from CONFIG_PATH")
        .validate()
        .expect("Should validate");

    assert_eq!(cfg.cluster.node_id, 99);

    unsafe {
        std::env::remove_var("CONFIG_PATH");
    }
}

#[test]
#[serial]
fn test_config_path_env_with_env_override() {
    cleanup_all_raft_env_vars();

    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("base.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    write!(
        file,
        r#"
[cluster]
node_id = 1

[[cluster.initial_cluster]]
id = 1
address = "127.0.0.1:9091"
role = 3
status = 3

[[cluster.initial_cluster]]
id = 200
address = "127.0.0.1:9092"
role = 3
status = 3
"#
    )
    .unwrap();
    drop(file);

    unsafe {
        std::env::set_var("CONFIG_PATH", config_path.to_str().unwrap());
    }
    unsafe {
        std::env::set_var("RAFT__CLUSTER__NODE_ID", "200");
    }

    let cfg = RaftNodeConfig::new().expect("Should load").validate().expect("Should validate");

    assert_eq!(cfg.cluster.node_id, 200); // Env var overrides file

    unsafe {
        std::env::remove_var("CONFIG_PATH");
    }
    unsafe {
        std::env::remove_var("RAFT__CLUSTER__NODE_ID");
    }
}

#[test]
fn test_explicit_override_config_file() {
    cleanup_all_raft_env_vars();

    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("custom.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    write!(
        file,
        r#"
[cluster]
node_id = 42

[[cluster.initial_cluster]]
id = 42
address = "127.0.0.1:9091"
role = 3
status = 3
"#
    )
    .unwrap();
    drop(file);

    let cfg = RaftNodeConfig::new()
        .expect("new() should succeed")
        .with_override_config(config_path.to_str().unwrap())
        .expect("override should succeed")
        .validate()
        .expect("validate should succeed");

    assert_eq!(cfg.cluster.node_id, 42);
}

// ============================================================================
// API Design Test
// ============================================================================

#[test]
fn test_validate_consumes_self_returns_self() {
    let cfg = RaftNodeConfig::new().unwrap();

    // validate() consumes cfg and returns Result<Self>
    let validated = cfg.validate().expect("Should validate");

    // Can use validated
    assert!(validated.cluster.node_id > 0);
}

// ============================================================================
// initial_cluster: duplicate node_id / listen_address must be rejected
// ============================================================================

mod initial_cluster_duplicate_tests {
    use d_engine_proto::common::NodeRole;
    use d_engine_proto::common::NodeStatus;
    use d_engine_proto::server::cluster::NodeMeta;

    use super::*;

    fn voter(
        id: u32,
        address: &str,
    ) -> NodeMeta {
        NodeMeta {
            id,
            address: address.to_string(),
            role: NodeRole::Follower as i32,
            status: NodeStatus::Active as i32,
        }
    }

    fn base_config() -> RaftNodeConfig {
        RaftNodeConfig::default()
    }

    /// Two nodes with the same id must be rejected — this check already existed but had no
    /// dedicated test before this change.
    #[test]
    fn test_validate_rejects_duplicate_node_id() {
        let mut cfg = base_config();
        cfg.cluster.node_id = 1;
        cfg.cluster.initial_cluster = vec![voter(1, "127.0.0.1:9081"), voter(1, "127.0.0.1:9082")];

        let result = cfg.validate();

        assert!(result.is_err(), "duplicate node_id must be rejected");
    }

    /// Two nodes sharing the same listen_address must be rejected. Left unchecked, this fails
    /// silently in a real multi-host deployment: each node binds its own local address
    /// successfully, and the cluster just never elects a leader — a much harder failure to
    /// diagnose than a loud startup error.
    #[test]
    fn test_validate_rejects_duplicate_listen_address() {
        let mut cfg = base_config();
        cfg.cluster.node_id = 1;
        cfg.cluster.initial_cluster = vec![
            voter(1, "127.0.0.1:9081"),
            voter(2, "127.0.0.1:9081"), // same address as node 1
        ];

        let result = cfg.validate();

        assert!(result.is_err(), "duplicate listen_address must be rejected");
    }

    /// Distinct ids and addresses must still validate successfully — the happy path this
    /// check must not break.
    #[test]
    fn test_validate_accepts_unique_node_id_and_listen_address() {
        let mut cfg = base_config();
        cfg.cluster.node_id = 1;
        cfg.cluster.initial_cluster = vec![
            voter(1, "127.0.0.1:9081"),
            voter(2, "127.0.0.1:9082"),
            voter(3, "127.0.0.1:9083"),
        ];

        let result = cfg.validate();

        assert!(
            result.is_ok(),
            "unique id/address must pass validation: {:?}",
            result.err()
        );
    }
}

// ============================================================================
// initial_cluster: shorthand ({id, address}) deserialization
// ============================================================================

mod initial_cluster_shorthand_tests {
    use d_engine_proto::common::NodeRole::Follower;
    use d_engine_proto::common::NodeStatus;

    use super::*;

    /// Deserializes `initial_cluster_toml` (a bare TOML array literal, e.g.
    /// `"[{ id = 1, address = \"a\" }]"`) through the real `config` crate
    /// pipeline (file source, not a struct literal) — struct literals bypass
    /// `deserialize_with` entirely, so this is the only way to actually
    /// exercise the shorthand-parsing code path.
    fn deserialize_cluster(
        initial_cluster_toml: &str
    ) -> std::result::Result<ClusterConfig, ConfigError> {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("cluster.toml");
        std::fs::write(
            &config_path,
            format!("initial_cluster = {initial_cluster_toml}\n"),
        )
        .unwrap();

        Config::builder()
            .add_source(File::with_name(config_path.to_str().unwrap()))
            .build()?
            .try_deserialize::<ClusterConfig>()
    }

    #[test]
    fn test_field_missing_uses_default_single_node_cluster() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config_path = temp_dir.path().join("cluster.toml");
        std::fs::write(&config_path, "node_id = 1\n").unwrap();

        let cfg = Config::builder()
            .add_source(File::with_name(config_path.to_str().unwrap()))
            .build()
            .unwrap()
            .try_deserialize::<ClusterConfig>()
            .expect("missing field falls back to default, not an error");

        assert_eq!(cfg.initial_cluster.len(), 1);
        assert_eq!(cfg.initial_cluster[0].id, 1);
    }

    #[test]
    fn test_empty_array_deserializes_to_empty_vec() {
        let cfg = deserialize_cluster("[]").expect("empty array is not a deserialize error");
        assert!(cfg.initial_cluster.is_empty());
    }

    #[test]
    fn test_single_shorthand_defaults_role_and_status() {
        let cfg = deserialize_cluster(r#"[{ id = 1, address = "127.0.0.1:9081" }]"#).unwrap();

        assert_eq!(cfg.initial_cluster.len(), 1);
        assert_eq!(cfg.initial_cluster[0].id, 1);
        assert_eq!(cfg.initial_cluster[0].address, "127.0.0.1:9081");
        assert_eq!(cfg.initial_cluster[0].role, Follower as i32);
        assert_eq!(cfg.initial_cluster[0].status, NodeStatus::Active as i32);
    }

    #[test]
    fn test_single_full_form_keeps_explicit_role_and_status() {
        let cfg = deserialize_cluster(
            r#"[{ id = 1, address = "127.0.0.1:9081", role = 2, status = 1 }]"#,
        )
        .unwrap();

        assert_eq!(cfg.initial_cluster[0].role, 2);
        assert_eq!(cfg.initial_cluster[0].status, 1);
    }

    #[test]
    fn test_mixed_shorthand_and_full_form_in_same_array() {
        let cfg = deserialize_cluster(
            r#"[
                { id = 1, address = "127.0.0.1:9081" },
                { id = 2, address = "127.0.0.1:9082", role = 2, status = 1 }
            ]"#,
        )
        .unwrap();

        assert_eq!(cfg.initial_cluster.len(), 2);
        assert_eq!(
            cfg.initial_cluster[0].role, Follower as i32,
            "element 1: shorthand defaults"
        );
        assert_eq!(cfg.initial_cluster[0].status, NodeStatus::Active as i32);
        assert_eq!(
            cfg.initial_cluster[1].role, 2,
            "element 2: full form keeps explicit value"
        );
        assert_eq!(cfg.initial_cluster[1].status, 1);
    }

    #[test]
    fn test_missing_id_is_a_deserialize_error() {
        let result = deserialize_cluster(r#"[{ address = "127.0.0.1:9081" }]"#);
        assert!(result.is_err(), "id has no default — must be required");
    }

    #[test]
    fn test_missing_address_is_a_deserialize_error() {
        let result = deserialize_cluster(r#"[{ id = 1 }]"#);
        assert!(result.is_err(), "address has no default — must be required");
    }

    #[test]
    fn test_invalid_role_type_is_a_deserialize_error() {
        let result = deserialize_cluster(
            r#"[{ id = 1, address = "127.0.0.1:9081", role = "not-a-number" }]"#,
        );
        assert!(result.is_err(), "role must be an integer, not a string");
    }

    #[test]
    fn test_invalid_status_type_is_a_deserialize_error() {
        let result = deserialize_cluster(
            r#"[{ id = 1, address = "127.0.0.1:9081", status = "not-a-number" }]"#,
        );
        assert!(result.is_err(), "status must be an integer, not a string");
    }

    /// `#[serde(default = ...)]` only applies when the key is absent. An
    /// explicit `null` for a non-`Option` field is still a type error — this
    /// is a real serde subtlety (flagged during review), worth pinning down
    /// with a test rather than assuming.
    #[test]
    fn test_explicit_null_role_is_a_deserialize_error_not_default() {
        let result =
            deserialize_cluster(r#"[{ id = 1, address = "127.0.0.1:9081", role = null }]"#);
        assert!(
            result.is_err(),
            "explicit null must not silently become the default"
        );
    }

    #[test]
    fn test_explicit_null_status_is_a_deserialize_error_not_default() {
        let result =
            deserialize_cluster(r#"[{ id = 1, address = "127.0.0.1:9081", status = null }]"#);
        assert!(
            result.is_err(),
            "explicit null must not silently become the default"
        );
    }

    #[test]
    fn test_full_form_missing_only_role_defaults_role_keeps_status() {
        let cfg =
            deserialize_cluster(r#"[{ id = 1, address = "127.0.0.1:9081", status = 1 }]"#).unwrap();

        assert_eq!(cfg.initial_cluster[0].role, Follower as i32);
        assert_eq!(cfg.initial_cluster[0].status, 1);
    }

    #[test]
    fn test_full_form_missing_only_status_defaults_status_keeps_role() {
        let cfg =
            deserialize_cluster(r#"[{ id = 1, address = "127.0.0.1:9081", role = 2 }]"#).unwrap();

        assert_eq!(cfg.initial_cluster[0].role, 2);
        assert_eq!(cfg.initial_cluster[0].status, NodeStatus::Active as i32);
    }
}
