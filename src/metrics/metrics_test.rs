use super::*;

fn create_test_registry() -> Registry {
    let registry = Registry::new_custom(Some("dengine".to_string()), None).unwrap();
    register_custom_metrics(&registry);
    registry
}

#[test]
fn test_custom_registry() {
    let registry = create_test_registry();

    COMMITTED_LOG_METRIC
        .with_label_values(&[&1.to_string(), &100.to_string()])
        .inc();
    let metrics = &registry.gather();
    assert!(!metrics.is_empty());

    // Verify the number of indicators
    let metric_names: Vec<_> = metrics.iter().map(|m| m.get_name()).collect();
    // Verify that key indicators exist
    assert!(
        metric_names.contains(&"dengine_committed_log"),
        "Missing dengine_committed_log"
    );
}
// Test the correctness of the indicator update logic
#[test]
fn test_counter_increment() {
    // Reset the counter to avoid test pollution
    FAILED_COMMIT_MESSAGES.reset();

    // Simulate business scenarios to trigger indicator updates
    FAILED_COMMIT_MESSAGES.with_label_values(&["123"]).inc();
    FAILED_COMMIT_MESSAGES.with_label_values(&["123"]).inc();

    // Verify the counter value
    let value = FAILED_COMMIT_MESSAGES.with_label_values(&["123"]).get();
    assert_eq!(value, 2, "Counter should increment correctly");
}

// Test the correctness of histogram labels
#[test]
fn test_histogram_labels() {
    SLOW_RESPONSE_DURATION_METRIC.reset();

    // Simulate data records with different labels
    SLOW_RESPONSE_DURATION_METRIC
        .with_label_values(&["peer1"])
        .observe(100.0);
    SLOW_RESPONSE_DURATION_METRIC
        .with_label_values(&["peer2"])
        .observe(200.0);

    // Verify label distinguishability
    let peer1_count = SLOW_RESPONSE_DURATION_METRIC
        .with_label_values(&["peer1"])
        .get_sample_count();
    let peer2_count = SLOW_RESPONSE_DURATION_METRIC
        .with_label_values(&["peer2"])
        .get_sample_count();

    assert_eq!(peer1_count, 1);
    assert_eq!(peer2_count, 1);
}
#[tokio::test]
async fn test_metrics_endpoint_format() {
    let registry = create_test_registry();
    COMMITTED_LOG_METRIC
        .with_label_values(&[&1.to_string(), &100.to_string()])
        .inc();
    // Construct test route
    let metrics_route = warp::path!("metrics")
        .map(move || registry.clone()) // Clone the registry to the closure
        .and_then(metrics_handler);

    // Simulate request
    let response = warp::test::request()
        .method("GET")
        .path("/metrics")
        .reply(&metrics_route)
        .await;

    // Verify basic response properties
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("Content-Type"),
        Some(&"text/plain; charset=utf-8".parse().unwrap())
    );

    // Verify indicator format
    let body = String::from_utf8(response.body().to_vec()).unwrap();

    println!("body: {:?}", &body);
    // Check custom indicator characteristics
    assert!(body.contains("dengine_committed_log")); // Verify prefix
}

// Test timestamp indicator logic
#[test]
fn test_timestamp_metric_calculation() {
    let before = get_current_ms();
    let metric_time = get_current_ms();
    let after = get_current_ms();

    assert!(
        metric_time >= before && metric_time <= after,
        "Timestamp should be in valid range"
    );
}
