#[cfg(test)]
mod metrics_test;

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

///----------------------------
use autometrics::prometheus_exporter::PrometheusResponse;
///----------------------------
use autometrics::prometheus_exporter::{self};
use lazy_static::lazy_static;
use prometheus::exponential_buckets;
use prometheus::GaugeVec;
use prometheus::HistogramOpts;
use prometheus::HistogramVec;
use prometheus::IntCounterVec;
use prometheus::Opts;
use prometheus::Registry;
use tokio::sync::watch;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

lazy_static! {
    pub static ref SLOW_RESPONSE_DURATION_METRIC: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "slow_res_duration_metric",
            "Histogram of append request slow response duration in ms"
        )
        .buckets(exponential_buckets(1.0, 2.0, 10).unwrap()),
        &["peer_id"]
    )
    .expect("metric can not be created");
    pub static ref MESSAGE_COMMIT_LATENCY_METRIC: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "message_commit_latency_metric",
            "Histogram of message commit latency in ms"
        )
        .buckets(exponential_buckets(1.0, 2.0, 20).unwrap()),
        &["msg_id"]
    )
    .expect("metric can not be created");
    pub static ref FAILED_COMMIT_MESSAGES: IntCounterVec =
        IntCounterVec::new(Opts::new("failed_commit_messages", "failed_commit_messages"), &["id"])
            .expect("Should succeed to create metric");
    pub static ref CLUSTER_FATAL_ERROR: GaugeVec = GaugeVec::new(
        Opts::new("cluster_fatal_error_metric", "cluster_fatal_error_metric"),
        &["event_type"]
    )
    .expect("Should succeed to create metric");
    pub static ref UNSYNCED_MSG_METRIC: GaugeVec =
        GaugeVec::new(Opts::new("unsynced_msg_metric", "unsynced_msg_metric"), &["peer_id"])
            .expect("metric can not be created");
    pub static ref LOG_RECEIVE_AT_METRIC: GaugeVec =
        GaugeVec::new(Opts::new("log_receive_at", "log_receive_at"), &["msg_id"]).expect("metric can not be created");
    pub static ref LOG_COMMIT_AT_METRIC: GaugeVec =
        GaugeVec::new(Opts::new("log_commit_at", "log_commit_at"), &["msg_id"]).expect("metric can not be created");
    pub static ref COMMITTED_LOG_METRIC: IntCounterVec =
        IntCounterVec::new(Opts::new("committed_log", "committed_log"), &["id", "msg_id"])
            .expect("Should succeed to create metric");
    pub static ref MESSAGE_SIZE_IN_BYTES_METRIC: HistogramVec = HistogramVec::new(
        HistogramOpts::new("message_size", "message_size").buckets(exponential_buckets(10.0, 5.0, 10).unwrap()),
        &["msg_id"]
    )
    .expect("metric can not be created");
    pub static ref CUSTOM_REGISTRY: Registry = Registry::new_custom(Some("d_engine".to_string()), None).unwrap();
}

pub(crate) fn register_custom_metrics(registry: &Registry) {
    registry
        .register(Box::new(SLOW_RESPONSE_DURATION_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(MESSAGE_COMMIT_LATENCY_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(FAILED_COMMIT_MESSAGES.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(CLUSTER_FATAL_ERROR.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(UNSYNCED_MSG_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(LOG_RECEIVE_AT_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(LOG_COMMIT_AT_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(COMMITTED_LOG_METRIC.clone()))
        .expect("collector can be registered");
    registry
        .register(Box::new(MESSAGE_SIZE_IN_BYTES_METRIC.clone()))
        .expect("collector can be registered");
}

pub async fn start_server(
    port: u16,
    mut shutdown_signal: watch::Receiver<()>,
) {
    register_custom_metrics(&CUSTOM_REGISTRY);

    // let metrics_route = warp::path!("metrics").and_then(metrics_handler);
    let registry = CUSTOM_REGISTRY.clone();
    let metrics_route = warp::path!("metrics")
        .map(move || registry.clone()) // Clone the registry to the closure
        .and_then(metrics_handler);

    let (_, server) = warp::serve(metrics_route).bind_with_graceful_shutdown(([0, 0, 0, 0], port), async move {
        let _ = shutdown_signal.changed().await;
    });
    server.await;
}

async fn metrics_handler(registry: Registry) -> Result<impl Reply, Rejection> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&registry.gather(), &mut buffer) {
        eprintln!("could not encode custom metrics: {}", e);
    };
    let mut res = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("custom metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&prometheus::gather(), &mut buffer) {
        eprintln!("could not encode prometheus metrics: {}", e);
    };
    let res_custom = match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("prometheus metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    };
    buffer.clear();

    let autometrics_metrics = get_metrics_body();
    res.push_str(&res_custom);
    res.push_str(&autometrics_metrics);
    Ok(res)
}

/// Export metrics for Prometheus to scrape
pub fn get_metrics_body() -> String {
    let autometrics_response = prometheus_exporter::encode_http_response();
    autometrics_response.into_body()
}
/// Export metrics for Prometheus to scrape
pub fn get_metrics() -> PrometheusResponse {
    prometheus_exporter::encode_http_response()
}

pub fn get_current_ms() -> f64 {
    let start_time = SystemTime::now();
    let since_epoch = start_time.duration_since(UNIX_EPOCH).expect("Time went backwards");
    let current_time_ms = (since_epoch.as_secs() * 1000) as f64 + since_epoch.subsec_nanos() as f64 / 1_000_000.0;
    current_time_ms.round() / 1.0
}
