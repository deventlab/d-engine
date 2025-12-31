use crate::ElectionConfig;
use crate::RaftConfig;
use crate::RpcCompressionConfig;

#[test]
fn test_invalid_election_timeout() {
    let mut config = RaftConfig::default();
    config.election.election_timeout_min = 1000;
    config.election.election_timeout_max = 500;

    assert!(config.validate().is_err());
}

#[test]
fn test_valid_config() {
    let config = RaftConfig {
        election: ElectionConfig {
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        },
        ..Default::default()
    };

    assert!(config.validate().is_ok());
}

#[test]
fn test_default_compression_config_aws_vpc_optimized() {
    // Test that the default compression configuration is optimized for AWS VPC environments
    let config = RpcCompressionConfig::default();

    // For AWS VPC environment, these are the recommended settings:
    assert!(
        !config.replication_response,
        "Replication compression should be disabled for high-frequency traffic in VPC environments"
    );
    assert!(
        config.election_response,
        "Election compression can be enabled as it's low frequency"
    );
    assert!(
        config.snapshot_response,
        "Snapshot compression should be enabled for large data transfers"
    );
    assert!(
        config.cluster_response,
        "Cluster management compression should be enabled for configuration data"
    );
    assert!(
        !config.client_response,
        "Client compression should be disabled for better performance in VPC environments"
    );
}

#[test]
fn test_custom_compression_config() {
    // Test that custom compression configurations can be created
    let config = RpcCompressionConfig {
        replication_response: true, // Override default for testing
        election_response: false,   // Override default for testing
        snapshot_response: true,    // Keep default
        cluster_response: true,     // Keep default
        client_response: true,      // Override default for testing
    };

    // Verify our custom configuration
    assert!(config.replication_response);
    assert!(!config.election_response);
    assert!(config.snapshot_response);
    assert!(config.cluster_response);
    assert!(config.client_response);
}
