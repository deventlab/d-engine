use crate::Error;
use crate::config::network::ConnectionParams;
use crate::config::network::NetworkConfig;
use crate::config::network::ServerTransportParams;

// Helper function to create a valid base ConnectionParams for tests
fn valid_base_params() -> ConnectionParams {
    ConnectionParams {
        connect_timeout_in_ms: 100,
        request_timeout_in_ms: 200,
        http2_keep_alive_interval_in_secs: 60,
        http2_keep_alive_timeout_in_secs: 30,
        stream_window_size: 65535,
        connection_window_size: 131070,
        tcp_keepalive_in_secs: 300,
    }
}

// Helper function to create a valid base ServerTransportParams for tests
fn valid_server_params() -> ServerTransportParams {
    ServerTransportParams {
        concurrency_limit_per_connection: 1024,
        max_concurrent_streams: 1024,
        max_pending_accept_reset_streams: 1000,
        http2_keepalive_interval_in_secs: 60,
        http2_keepalive_timeout_in_secs: 30,
        initial_connection_window_size: 131070,
        initial_stream_window_size: 65535,
        max_decoding_message_size: 67_108_864,
    }
}

#[test]
fn test_network_config_default_values() {
    let config = NetworkConfig::default();

    // Test common network parameters
    assert!(config.tcp_nodelay);
    assert_eq!(config.buffer_size, 65536);

    // Test control plane defaults
    assert_eq!(config.control.connect_timeout_in_ms, 20);
    assert_eq!(config.control.request_timeout_in_ms, 100);
    assert_eq!(config.control.tcp_keepalive_in_secs, 300);
    assert_eq!(config.control.http2_keep_alive_interval_in_secs, 30);
    assert_eq!(config.control.http2_keep_alive_timeout_in_secs, 5);
    assert_eq!(config.control.connection_window_size, 1048576);
    assert_eq!(config.control.stream_window_size, 262144);

    // Test data plane defaults
    assert_eq!(config.data.connect_timeout_in_ms, 50);
    assert_eq!(config.data.request_timeout_in_ms, 500);
    assert_eq!(config.data.tcp_keepalive_in_secs, 600);
    assert_eq!(config.data.http2_keep_alive_interval_in_secs, 120);
    assert_eq!(config.data.http2_keep_alive_timeout_in_secs, 30);
    assert_eq!(config.data.connection_window_size, 6291456);
    assert_eq!(config.data.stream_window_size, 1048576);

    // Test bulk transfer defaults
    assert_eq!(config.bulk.connect_timeout_in_ms, 500000);
    assert_eq!(config.bulk.request_timeout_in_ms, 5000000);
    assert_eq!(config.bulk.tcp_keepalive_in_secs, 3600);
    assert_eq!(config.bulk.http2_keep_alive_interval_in_secs, 600);
    assert_eq!(config.bulk.http2_keep_alive_timeout_in_secs, 60);
    assert_eq!(config.bulk.connection_window_size, 67108864);
    assert_eq!(config.bulk.stream_window_size, 16777216);

    // Test server transport defaults (PR #442 review: server no longer borrows
    // control/data's client-tuned values — it has its own dedicated profile,
    // since a single listener serves every RPC type uniformly and has no
    // "plane" to pick per incoming connection).
    assert_eq!(config.server.concurrency_limit_per_connection, 1024);
    assert_eq!(config.server.max_concurrent_streams, 1024);
    assert_eq!(config.server.max_pending_accept_reset_streams, 1000);
    assert_eq!(config.server.http2_keepalive_interval_in_secs, 60);
    assert_eq!(config.server.http2_keepalive_timeout_in_secs, 30);
    assert_eq!(config.server.initial_connection_window_size, 20_971_520);
    assert_eq!(config.server.initial_stream_window_size, 10_485_760);
    assert_eq!(config.server.max_decoding_message_size, 67_108_864);
}

#[test]
fn test_network_config_validate_success() {
    let config = NetworkConfig::default();
    let result = config.validate();
    assert!(
        result.is_ok(),
        "Default network config should validate successfully"
    );
}

#[test]
fn test_network_config_validate_buffer_size_too_small() {
    let config = NetworkConfig {
        buffer_size: 512, // Too small
        ..Default::default()
    };

    let result = config.validate();
    assert!(result.is_err(), "Should fail with buffer size too small");

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("Buffer size 512 too small, minimum 1024 bytes"));
}

#[test]
fn test_connection_params_validate_success() {
    let params = ConnectionParams {
        connect_timeout_in_ms: 100,
        request_timeout_in_ms: 500,
        http2_keep_alive_interval_in_secs: 60,
        http2_keep_alive_timeout_in_secs: 30,
        stream_window_size: 65535,
        connection_window_size: 131070,
        ..Default::default()
    };

    let result = params.validate("test");
    assert!(result.is_ok(), "Valid connection params should succeed");
}

#[test]
fn test_connection_params_validate_zero_connect_timeout() {
    let mut params = valid_base_params();
    params.connect_timeout_in_ms = 0; // Should fail

    let result = params.validate("test");
    assert!(result.is_err(), "Should fail with zero connect timeout");

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("connection timeout must be > 0"));
}

#[test]
fn test_connection_params_validate_request_timeout_too_small() {
    let mut params = valid_base_params();
    params.connect_timeout_in_ms = 100;
    params.request_timeout_in_ms = 100; // Equal to connect timeout (invalid)

    let result = params.validate("test");
    assert!(
        result.is_err(),
        "Should fail when request timeout <= connect timeout"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("request timeout"));
    assert!(error.to_string().contains("must exceed connect timeout"));
}

#[test]
fn test_connection_params_validate_zero_request_timeout_allowed() {
    let mut params = valid_base_params();
    params.request_timeout_in_ms = 0; // Zero is allowed (disabled)

    // Validate with test prefix for error messages
    let result = params.validate("test");
    assert!(
        result.is_ok(),
        "Zero request timeout should be allowed (disabled timeout)"
    );
}

#[test]
fn test_connection_params_validate_keepalive_timeout_too_large() {
    let mut params = valid_base_params();
    params.http2_keep_alive_interval_in_secs = 30;
    params.http2_keep_alive_timeout_in_secs = 30; // Equal to interval (invalid)

    let result = params.validate("test");
    assert!(
        result.is_err(),
        "Should fail when keepalive timeout >= interval"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("must be <"));
}
#[test]
fn test_connection_params_validate_keepalive_timeout_larger_than_interval() {
    let mut params = valid_base_params();
    params.http2_keep_alive_interval_in_secs = 30;
    params.http2_keep_alive_timeout_in_secs = 60; // Larger than interval (invalid)

    let result = params.validate("test");
    assert!(
        result.is_err(),
        "Should fail when keepalive timeout > interval"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("must be <"));
}

#[test]
fn test_connection_params_validate_stream_window_too_small() {
    let mut params = valid_base_params();
    params.stream_window_size = 65534; // Below minimum 65535 (invalid)
    params.connection_window_size = 131070;

    let result = params.validate("test");
    assert!(result.is_err(), "Should fail when stream window too small");

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("below minimum"));
}

#[test]
fn test_connection_params_validate_connection_window_smaller_than_stream() {
    let mut params = valid_base_params();
    params.stream_window_size = 131070;
    params.connection_window_size = 65535; // Smaller than stream window (invalid)

    let result = params.validate("test");
    assert!(
        result.is_err(),
        "Should fail when connection window smaller than stream window"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("smaller than stream window"));
}

#[test]
fn test_connection_params_validate_minimum_window_sizes() {
    let mut params = valid_base_params();
    params.stream_window_size = 65535; // Exactly minimum
    params.connection_window_size = 65535; // Equal to stream (allowed)

    let result = params.validate("test");
    assert!(result.is_ok(), "Minimum window sizes should be valid");
}

#[test]
fn test_connection_params_validate_larger_connection_window() {
    let mut params = valid_base_params();
    params.stream_window_size = 65535;
    params.connection_window_size = 131070; // Larger than stream (valid)

    let result = params.validate("test");
    assert!(result.is_ok(), "Larger connection window should be valid");
}

#[test]
fn test_control_params_optimized_for_low_latency() {
    let control = NetworkConfig::default().control;

    // Control plane should have fast timeouts for leader elections
    assert_eq!(control.connect_timeout_in_ms, 20);
    assert_eq!(control.request_timeout_in_ms, 100);

    // Frequent keepalives for quick failure detection
    assert_eq!(control.http2_keep_alive_interval_in_secs, 30);
    assert_eq!(control.http2_keep_alive_timeout_in_secs, 5);
}

#[test]
fn test_data_params_optimized_for_throughput() {
    let data = NetworkConfig::default().data;

    // Data plane should have balanced timeouts for log replication
    assert_eq!(data.connect_timeout_in_ms, 50);
    assert_eq!(data.request_timeout_in_ms, 500);

    // Moderate keepalives for efficiency
    assert_eq!(data.http2_keep_alive_interval_in_secs, 120);
    assert_eq!(data.http2_keep_alive_timeout_in_secs, 30);
}

#[test]
fn test_bulk_params_optimized_for_large_transfers() {
    let bulk = NetworkConfig::default().bulk;

    // Bulk transfer should have long timeouts for large operations
    assert_eq!(bulk.connect_timeout_in_ms, 500000);
    assert_eq!(bulk.request_timeout_in_ms, 5000000); // Effectively disabled

    // Long keepalives for persistent connections
    assert_eq!(bulk.http2_keep_alive_interval_in_secs, 600);
    assert_eq!(bulk.http2_keep_alive_timeout_in_secs, 60);

    // Large window sizes for bulk data
    assert_eq!(bulk.connection_window_size, 67108864); // 64MB
    assert_eq!(bulk.stream_window_size, 16777216); // 16MB
}

#[test]
fn test_network_config_validation_cascades_to_all_connection_types() {
    let mut config = NetworkConfig::default();

    // Make data connection invalid (zero connect timeout)
    config.data.connect_timeout_in_ms = 0;

    let result = config.validate();
    assert!(
        result.is_err(),
        "Should fail when any connection type is invalid"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("data connection timeout must be > 0"));
}

#[test]
fn test_network_config_validation_multiple_errors() {
    let mut config = NetworkConfig {
        buffer_size: 512, // Too small
        ..Default::default()
    };

    // Make multiple things invalid
    config.control.connect_timeout_in_ms = 0; // Zero timeout
    config.data.http2_keep_alive_timeout_in_secs = config.data.http2_keep_alive_interval_in_secs; // Equal timeouts

    let result = config.validate();
    assert!(
        result.is_err(),
        "Should fail with multiple validation errors"
    );

    // The validation stops at the first error, so we'll get the buffer size error first
    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("Buffer size 512 too small"));
}

#[test]
fn test_connection_params_edge_case_timeouts() {
    let mut params = valid_base_params();
    params.connect_timeout_in_ms = 100;
    params.request_timeout_in_ms = 101; // Just 1ms above (valid)

    let result = params.validate("test");
    assert!(
        result.is_ok(),
        "Request timeout just above connect timeout should be valid"
    );
}

#[test]
fn test_connection_params_edge_case_keepalives() {
    let mut params = valid_base_params();
    params.http2_keep_alive_interval_in_secs = 30;
    params.http2_keep_alive_timeout_in_secs = 29; // Just 1s below (valid)

    let result = params.validate("test");
    assert!(
        result.is_ok(),
        "Keepalive timeout just below interval should be valid"
    );
}

#[test]
fn test_server_transport_params_validate_success() {
    let params = valid_server_params();

    let result = params.validate();
    assert!(
        result.is_ok(),
        "Valid server transport params should succeed"
    );
}

#[test]
fn test_server_transport_params_validate_default_is_valid() {
    let params = ServerTransportParams::default();

    let result = params.validate();
    assert!(
        result.is_ok(),
        "Default server transport params should validate successfully"
    );
}

#[test]
fn test_server_transport_params_validate_zero_concurrency_limit() {
    let mut params = valid_server_params();
    params.concurrency_limit_per_connection = 0;

    let result = params.validate();
    assert!(result.is_err(), "Should fail with zero concurrency limit");

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("concurrency_limit_per_connection must be > 0"));
}

#[test]
fn test_server_transport_params_validate_zero_max_concurrent_streams() {
    let mut params = valid_server_params();
    params.max_concurrent_streams = 0;

    let result = params.validate();
    assert!(
        result.is_err(),
        "Should fail with zero max_concurrent_streams"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("max_concurrent_streams must be > 0"));
}

#[test]
fn test_server_transport_params_validate_keepalive_timeout_too_large() {
    let mut params = valid_server_params();
    params.http2_keepalive_interval_in_secs = 30;
    params.http2_keepalive_timeout_in_secs = 30; // Equal to interval (invalid)

    let result = params.validate();
    assert!(
        result.is_err(),
        "Should fail when keepalive timeout >= interval"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("must be <"));
}

#[test]
fn test_server_transport_params_validate_stream_window_too_small() {
    let mut params = valid_server_params();
    params.initial_stream_window_size = 65534; // Below minimum 65535 (invalid)

    let result = params.validate();
    assert!(result.is_err(), "Should fail when stream window too small");

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("below minimum"));
}

#[test]
fn test_server_transport_params_validate_connection_window_smaller_than_stream() {
    let mut params = valid_server_params();
    params.initial_stream_window_size = 131070;
    params.initial_connection_window_size = 65535; // Smaller than stream window (invalid)

    let result = params.validate();
    assert!(
        result.is_err(),
        "Should fail when connection window smaller than stream window"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("smaller than stream window"));
}

#[test]
fn test_server_transport_params_validate_zero_max_decoding_message_size() {
    let mut params = valid_server_params();
    params.max_decoding_message_size = 0;

    let result = params.validate();
    assert!(
        result.is_err(),
        "Should fail with zero max_decoding_message_size"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("max_decoding_message_size must be > 0"));
}

#[test]
fn test_network_config_validation_cascades_to_server_params() {
    let mut config = NetworkConfig::default();
    config.server.max_concurrent_streams = 0;

    let result = config.validate();
    assert!(
        result.is_err(),
        "Should fail when server transport params are invalid"
    );

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config(_)));
    assert!(error.to_string().contains("max_concurrent_streams must be > 0"));
}
