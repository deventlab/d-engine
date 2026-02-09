use crate::BackpressureConfig;
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

#[test]
fn test_backpressure_default_config() {
    let config = BackpressureConfig::default();

    assert_eq!(config.max_pending_writes, 10_000);
    assert_eq!(config.max_pending_reads, 50_000);
}

#[test]
fn test_backpressure_unlimited_when_zero() {
    let config = BackpressureConfig {
        max_pending_writes: 0,
        max_pending_reads: 0,
    };

    // 0 = unlimited, should never reject
    assert!(!config.should_reject_write(1_000_000));
    assert!(!config.should_reject_read(1_000_000));
}

#[test]
fn test_backpressure_write_limit_enforcement() {
    let config = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 200,
    };

    // Below limit - allow
    assert!(!config.should_reject_write(50));
    assert!(!config.should_reject_write(99));

    // At limit - reject
    assert!(config.should_reject_write(100));

    // Above limit - reject
    assert!(config.should_reject_write(101));
    assert!(config.should_reject_write(10000));
}

#[test]
fn test_backpressure_read_limit_enforcement() {
    let config = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 200,
    };

    // Below limit - allow
    assert!(!config.should_reject_read(100));
    assert!(!config.should_reject_read(199));

    // At limit - reject
    assert!(config.should_reject_read(200));

    // Above limit - reject
    assert!(config.should_reject_read(201));
    assert!(config.should_reject_read(50000));
}

#[test]
fn test_backpressure_write_and_read_independent() {
    let config = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 200,
    };

    // Write limit doesn't affect read checks
    assert!(config.should_reject_write(100));
    assert!(!config.should_reject_read(100));

    // Read limit doesn't affect write checks
    assert!(config.should_reject_read(200));
    assert!(!config.should_reject_write(200));
}

#[test]
fn test_backpressure_zero_vs_nonzero() {
    let config_unlimited_writes = BackpressureConfig {
        max_pending_writes: 0,
        max_pending_reads: 100,
    };

    let config_limited_writes = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 0,
    };

    // Unlimited writes, limited reads
    assert!(!config_unlimited_writes.should_reject_write(1_000_000));
    assert!(config_unlimited_writes.should_reject_read(100));

    // Limited writes, unlimited reads
    assert!(config_limited_writes.should_reject_write(100));
    assert!(!config_limited_writes.should_reject_read(1_000_000));
}
