use super::*;
use crate::proto::cluster::{cluster_conf_update_response::ErrorCode, ClusterConfUpdateResponse};

async fn async_ok(number: u64) -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
    sleep(Duration::from_millis(number)).await;
    let c = ClusterConfUpdateResponse {
        id: 1,
        term: 1,
        version: 1,
        success: true,
        error_code: ErrorCode::None.into(),
    };
    let response: tonic::Response<ClusterConfUpdateResponse> = tonic::Response::new(c);
    Ok(response)
}

async fn async_err() -> std::result::Result<tonic::Response<ClusterConfUpdateResponse>, tonic::Status> {
    sleep(Duration::from_millis(100)).await;
    Err(tonic::Status::aborted("message"))
}

#[tokio::test]
async fn test_rpc_task_with_exponential_backoff() {
    tokio::time::pause();

    // Case 1: when ok task return ok
    let policy = BackoffPolicy {
        max_retries: 3,
        timeout_ms: 100,
        base_delay_ms: 1000,
        max_delay_ms: 3000,
    };

    assert!(grpc_task_with_timeout_and_exponential_backoff(
        "test_rpc_task_with_exponential_backoff",
        || async { async_ok(3).await },
        policy,
    )
    .await
    .is_ok());

    // Case 2: when err task return error
    assert!(grpc_task_with_timeout_and_exponential_backoff(
        "test_rpc_task_with_exponential_backoff",
        async_err,
        policy
    )
    .await
    .is_err());

    // Case 3: when ok task always failed on timeout error
    let policy = BackoffPolicy {
        max_retries: 3,
        timeout_ms: 1,
        base_delay_ms: 1,
        max_delay_ms: 3,
    };
    assert!(grpc_task_with_timeout_and_exponential_backoff(
        "test_rpc_task_with_exponential_backoff",
        || async { async_ok(3).await },
        policy
    )
    .await
    .is_err());
}
