use std::time::Duration;

use crate::BackpressureConfig;
use crate::ElectionConfig;
use crate::RaftConfig;
use crate::ReadConsistencyConfig;
use crate::RpcCompressionConfig;
use crate::config::raft::ReadActorConfig;
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
fn test_snapshot_config_zero_chunk_timeout_is_invalid() {
    let mut config = RaftConfig::default();
    config.snapshot.receive_chunk_timeout_in_sec = 0;
    assert!(
        config.validate().is_err(),
        "receive_chunk_timeout_in_sec = 0 must be rejected"
    );
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    };

    // Write limit doesn't affect read checks
    assert!(config.should_reject_write(100));
    assert!(!config.should_reject_read(100));

    // Read limit doesn't affect write checks
    // pending=200 triggers read limit (200 >= max_pending_reads=200)
    // but write check uses its own limit (50 < max_pending_writes=100)
    assert!(config.should_reject_read(200));
    assert!(!config.should_reject_write(50));
}

#[test]
fn test_backpressure_zero_vs_nonzero() {
    let config_unlimited_writes = BackpressureConfig {
        max_pending_writes: 0,
        max_pending_reads: 100,
        ..Default::default()
    };

    let config_limited_writes = BackpressureConfig {
        max_pending_writes: 100,
        max_pending_reads: 0,
        ..Default::default()
    };

    // Unlimited writes, limited reads
    assert!(!config_unlimited_writes.should_reject_write(1_000_000));
    assert!(config_unlimited_writes.should_reject_read(100));

    // Limited writes, unlimited reads
    assert!(config_limited_writes.should_reject_write(100));
    assert!(!config_limited_writes.should_reject_read(1_000_000));
}

// ============================================================================
// N4 — Config validation: lease_duration must be < election_timeout (#390 Gap 2)
// ============================================================================

/// `RaftConfig::validate()` must reject configurations where `lease_duration_ms`
/// is greater than or equal to `election_timeout_min`.
///
/// ## Why this constraint is load-bearing
/// The lease fast path for LinearizableRead relies on the timeline invariant:
///   `lease_duration < election_timeout - max_clock_drift`
/// At minimum, `lease_duration < election_timeout` must hold.  If this is
/// violated, a partitioned leader's lease could outlive the election timeout,
/// causing "lease valid" and "new leader elected" to overlap — breaking the
/// mutual-exclusion guarantee that makes lease-based linearizable reads safe.
///
/// A runtime `assert!` is insufficient on its own: it fires only when the node
/// starts, which is too late if the operator misconfigures the values.  Config
/// validation at construction time catches the error before any Raft activity.
#[test]
fn test_config_rejects_lease_duration_not_less_than_election_timeout() {
    // Case 1: lease_duration_ms == election_timeout_min → must be rejected.
    let mut config = RaftConfig {
        election: ElectionConfig {
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        },
        read_consistency: ReadConsistencyConfig {
            lease_duration_ms: 300, // equal to election_timeout_min → invalid
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(
        config.validate().is_err(),
        "lease_duration_ms == election_timeout_min must be rejected"
    );

    // Case 2: lease_duration_ms > election_timeout_min → must be rejected.
    config.read_consistency.lease_duration_ms = 400;
    assert!(
        config.validate().is_err(),
        "lease_duration_ms > election_timeout_min must be rejected"
    );

    // Case 3: lease_duration_ms < election_timeout_min → must be accepted.
    config.read_consistency.lease_duration_ms = 150;
    assert!(
        config.validate().is_ok(),
        "lease_duration_ms < election_timeout_min must be accepted"
    );
}

/// Direct unit test for ReadConsistencyConfig::validate() — lease safety constraint.
///
/// This pins the Raft §6.4 invariant at the config layer:
///   lease_duration_ms < election_timeout_min
///
/// Tested directly on ReadConsistencyConfig (not via RaftConfig) so the constraint
/// is verified at the closest possible layer to the data.
#[test]
fn test_read_consistency_config_lease_duration_rejects_unsafe_values() {
    let election_timeout_min = 500u64;

    // equal → unsafe, must reject
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 500,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_err(),
        "lease_duration_ms == election_timeout_min violates safety invariant"
    );

    // greater → unsafe, must reject
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 600,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_err(),
        "lease_duration_ms > election_timeout_min violates safety invariant"
    );

    // strictly less → safe, must accept
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 250,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_ok(),
        "lease_duration_ms < election_timeout_min must be accepted"
    );

    // zero → must reject (separate guard)
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 0,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_err(),
        "lease_duration_ms = 0 must be rejected"
    );
}

#[test]
fn test_read_actor_channel_capacity_zero_is_invalid() {
    let cfg = ReadActorConfig {
        channel_capacity: 0,
        max_drain: 100,
    };
    assert!(
        cfg.validate().is_err(),
        "channel_capacity = 0 must be rejected (mpsc::channel(0) panics at startup)"
    );
}

#[test]
fn test_read_actor_max_drain_zero_is_invalid() {
    let cfg = ReadActorConfig {
        channel_capacity: 512,
        max_drain: 0,
    };
    assert!(
        cfg.validate().is_err(),
        "max_drain = 0 must be rejected (drains 0 commands per wakeup — disables batching)"
    );
}

#[test]
fn test_read_actor_default_config_is_valid() {
    assert!(
        ReadActorConfig::default().validate().is_ok(),
        "default ReadActorConfig must be valid"
    );
}

#[test]
fn test_raft_config_propagates_read_actor_validation() {
    let mut config = RaftConfig::default();
    config.read_actor.channel_capacity = 0;
    assert!(
        config.validate().is_err(),
        "RaftConfig::validate must propagate ReadActorConfig::validate failures"
    );
}

#[test]
fn test_raft_config_max_pending_append_responses_zero_is_invalid() {
    let config = RaftConfig {
        max_pending_append_responses: 0,
        ..Default::default()
    };
    assert!(
        config.validate().is_err(),
        "max_pending_append_responses = 0 must be rejected (mpsc::channel(0) panics, \
         and the stream can never read a request)"
    );
}

#[test]
fn test_raft_config_max_pending_append_responses_one_is_valid() {
    let config = RaftConfig {
        max_pending_append_responses: 1,
        ..Default::default()
    };
    assert!(
        config.validate().is_ok(),
        "max_pending_append_responses = 1 is the smallest legal value"
    );
}

/// lease_duration_ms + network_rtt_p99_ms/2 must account for RTT/2 in the safety bound.
///
/// Invariant (Raft §6.4): lease_duration_ms + rtt_p99_ms/2 < election_timeout_min
/// Even if lease_duration_ms alone is safe, adding rtt/2 can push it over the bound.
#[test]
fn test_read_consistency_config_rtt_p99_tightens_lease_safety_bound() {
    let election_timeout_min = 500u64;

    // lease=498, rtt=4 → rtt/2=2 → 498+2=500 >= 500 → must reject
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 498,
        network_rtt_p99_ms: 4,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_err(),
        "lease + rtt/2 == election_timeout_min must be rejected"
    );

    // lease=497, rtt=4 → rtt/2=2 → 497+2=499 < 500 → must accept
    let cfg = ReadConsistencyConfig {
        lease_duration_ms: 497,
        network_rtt_p99_ms: 4,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_ok(),
        "lease + rtt/2 < election_timeout_min must be accepted"
    );
}

/// Overflow-safe: u64::MAX lease_duration_ms must be rejected, not wrap around.
///
/// Without saturating_add, u64::MAX + rtt_half wraps to a small value in release
/// builds, allowing an invalid config to pass validate() and violate lease safety.
#[test]
fn test_read_consistency_config_lease_duration_overflow_is_rejected() {
    let election_timeout_min = 500u64;

    let cfg = ReadConsistencyConfig {
        lease_duration_ms: u64::MAX,
        network_rtt_p99_ms: 4,
        ..Default::default()
    };
    assert!(
        cfg.validate(election_timeout_min).is_err(),
        "u64::MAX lease_duration_ms must be rejected (saturating_add prevents wrap-around)"
    );
}

/// #428: startup_quorum_timeout defaults to 30s — independent of, and far
/// shorter than, retry.membership's ~2 hour worst-case exhaustion budget.
#[test]
fn test_startup_quorum_timeout_default_is_30_seconds() {
    let config = RaftConfig::default();
    assert_eq!(
        config.membership.startup_quorum_timeout,
        Duration::from_secs(30)
    );
}

#[test]
fn test_startup_quorum_timeout_zero_is_invalid() {
    let mut config = RaftConfig::default();
    config.membership.startup_quorum_timeout = Duration::ZERO;
    assert!(
        config.validate().is_err(),
        "startup_quorum_timeout = 0 must be rejected"
    );
}

#[test]
fn test_startup_quorum_timeout_is_developer_configurable() {
    let mut config = RaftConfig::default();
    config.membership.startup_quorum_timeout = Duration::from_secs(90);
    assert!(
        config.validate().is_ok(),
        "a custom positive startup_quorum_timeout must be accepted"
    );
    assert_eq!(
        config.membership.startup_quorum_timeout,
        Duration::from_secs(90)
    );
}

#[test]
fn test_max_inflight_append_requests_default_is_256() {
    let config = RaftConfig::default();
    assert_eq!(config.replication.max_inflight_append_requests, 256);
    assert!(config.validate().is_ok());
}

#[test]
fn test_max_inflight_append_requests_zero_is_invalid() {
    let mut config = RaftConfig::default();
    config.replication.max_inflight_append_requests = 0;

    let err = config.validate().expect_err("a zero window must be rejected");
    assert!(
        err.to_string().contains("max_inflight_append_requests"),
        "the error must name the offending field, got: {err}"
    );
}

#[test]
fn test_max_inflight_append_requests_missing_key_uses_default() {
    // A config file written before this key existed must still load with the default window.
    let config: RaftConfig = load_toml("[replication]\nrpc_append_entries_clock_in_ms = 100\n");
    assert_eq!(config.replication.max_inflight_append_requests, 256);
}

#[test]
fn test_max_inflight_append_requests_explicit_value_is_honored() {
    let config: RaftConfig = load_toml("[replication]\nmax_inflight_append_requests = 3\n");
    assert_eq!(config.replication.max_inflight_append_requests, 3);
}

#[test]
fn test_replication_send_queue_capacity_default_is_1024() {
    let config = RaftConfig::default();
    assert_eq!(config.replication.replication_send_queue_capacity, 1024);
}

#[test]
fn test_replication_send_queue_capacity_default_covers_default_window() {
    let config = RaftConfig::default();
    assert!(
        config.replication.replication_send_queue_capacity
            >= config.replication.max_inflight_append_requests,
        "defaults must not report Full before the in-flight window does"
    );
}

#[test]
fn test_replication_send_queue_capacity_below_window_is_invalid() {
    let mut config = RaftConfig::default();
    config.replication.max_inflight_append_requests = 256;
    config.replication.replication_send_queue_capacity = 255;
    let err = config.validate().expect_err("capacity below the window must be rejected");
    assert!(
        err.to_string().contains("replication_send_queue_capacity"),
        "error must name the offending key, got: {err}"
    );
}

#[test]
fn test_replication_send_queue_capacity_equal_to_window_is_valid() {
    let mut config = RaftConfig::default();
    config.replication.max_inflight_append_requests = 256;
    config.replication.replication_send_queue_capacity = 256;
    config.validate().expect("capacity == window is the tightest valid setting");
}

#[test]
fn test_replication_send_queue_capacity_missing_key_uses_default() {
    let config: RaftConfig = load_toml("[replication]\nmax_inflight_append_requests = 3\n");
    assert_eq!(config.replication.replication_send_queue_capacity, 1024);
}

#[test]
fn test_replication_send_queue_capacity_explicit_value_is_honored() {
    let config: RaftConfig = load_toml("[replication]\nreplication_send_queue_capacity = 2048\n");
    assert_eq!(config.replication.replication_send_queue_capacity, 2048);
}

fn load_toml(text: &str) -> RaftConfig {
    config::Config::builder()
        .add_source(config::File::from_str(text, config::FileFormat::Toml))
        .build()
        .expect("toml must parse")
        .try_deserialize()
        .expect("config must deserialize")
}

// ============================================================================
// Write admission / step-down config and the cross-field checks around it
// ============================================================================

#[test]
fn test_write_admission_multiple_default_is_two() {
    assert_eq!(
        BackpressureConfig::default().write_admission_election_timeout_multiple,
        2
    );
}

/// A missing key parses to the default, so existing config files keep working.
#[test]
fn test_write_admission_multiple_missing_key_parses_to_default() {
    let parsed: BackpressureConfig = config::Config::builder()
        .add_source(config::File::from_str(
            "max_pending_writes = 500",
            config::FileFormat::Toml,
        ))
        .build()
        .unwrap()
        .try_deserialize()
        .unwrap();

    assert_eq!(parsed.max_pending_writes, 500);
    assert_eq!(parsed.write_admission_election_timeout_multiple, 2);
    assert_eq!(parsed.max_pending_reads, 50_000);
}

#[test]
fn test_write_admission_multiple_zero_is_rejected() {
    let mut config = RaftConfig::default();
    config.backpressure.write_admission_election_timeout_multiple = 0;

    assert!(config.validate().is_err());
}

#[test]
fn test_write_admission_multiple_one_and_large_values_are_accepted() {
    for multiple in [1, 2, 10, 1_000] {
        let mut config = RaftConfig::default();
        config.backpressure.write_admission_election_timeout_multiple = multiple;

        assert!(
            config.validate().is_ok(),
            "multiple {multiple} must be accepted"
        );
    }
}

/// `max_pending_writes` / `max_pending_reads` must exceed `batching.max_batch_size` (one loop
/// round pushes up to max_batch_size + 1 commands), unless 0 = unlimited.
#[test]
fn test_pending_limits_must_exceed_max_batch_size() {
    let batch = 100;
    let with = |writes: usize, reads: usize| {
        let mut config = RaftConfig::default();
        config.batching.max_batch_size = batch;
        config.backpressure.max_pending_writes = writes;
        config.backpressure.max_pending_reads = reads;
        config.validate().is_ok()
    };

    assert!(
        !with(batch, 50_000),
        "writes limit == max_batch_size must be rejected"
    );
    assert!(
        !with(batch - 1, 50_000),
        "writes limit below max_batch_size must be rejected"
    );
    assert!(
        with(batch + 1, 50_000),
        "writes limit == max_batch_size + 1 must be accepted"
    );
    assert!(
        !with(10_000, batch),
        "reads limit == max_batch_size must be rejected"
    );
    assert!(
        with(10_000, batch + 1),
        "reads limit == max_batch_size + 1 must be accepted"
    );
    assert!(with(0, 0), "0 = unlimited must be accepted for both");
    assert!(
        with(0, 50_000) && with(10_000, 0),
        "each limit is independent"
    );
}

/// Write admission rejects after `election_timeout_min` without a quorum ACK, so that window must
/// hold at least 3 heartbeats (two can be lost).
#[test]
fn test_election_timeout_min_must_hold_three_heartbeats() {
    let validate_with = |election_min: u64, heartbeat: u64| {
        let mut config = RaftConfig::default();
        // Keep the read-lease rule out of the way: only the heartbeat ratio is under test.
        config.read_consistency.lease_duration_ms = 10;
        config.election.election_timeout_min = election_min;
        config.election.election_timeout_max = 3_000;
        config.replication.rpc_append_entries_clock_in_ms = heartbeat;
        config.validate()
    };

    let too_small = validate_with(299, 100).expect_err("299 ms holds fewer than 3 heartbeats");
    assert!(
        too_small.to_string().contains("rpc_append_entries_clock_in_ms"),
        "the error must name the heartbeat setting, got: {too_small}"
    );
    assert!(
        validate_with(300, 100).is_ok(),
        "exactly 3 heartbeats is accepted"
    );
    assert!(
        validate_with(500, 100).is_ok(),
        "the defaults (500 / 100) are accepted"
    );
    assert!(
        validate_with(30, 10).is_ok(),
        "the rule is a ratio, not a fixed size"
    );
    assert!(validate_with(29, 10).is_err());
}

#[test]
fn test_default_config_is_valid() {
    assert!(RaftConfig::default().validate().is_ok());
}
