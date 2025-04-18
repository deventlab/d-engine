use crate::BackoffPolicy;
use crate::NetworkError;
use crate::Result;
use log::debug;
use log::error;
use log::warn;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::timeout;

/// General one
pub(crate) async fn task_with_timeout_and_exponential_backoff<F, T, P>(
    task: F,
    policy: BackoffPolicy,
) -> Result<P>
where
    F: Fn() -> T,                               // The type of the async function
    T: std::future::Future<Output = Result<P>>, // The future returned by the async function
{
    // let max_retries = 5;
    let mut retries = 0;
    let mut current_delay = Duration::from_millis(policy.base_delay_ms);
    let timeout_duration = Duration::from_millis(policy.timeout_ms);
    let max_delay = Duration::from_millis(policy.max_delay_ms);
    let max_retries = policy.max_retries;

    let mut last_error = NetworkError::TaskBackoffFailed("Task failed after max retries".to_string());
    while retries < max_retries {
        match timeout(timeout_duration, task()).await {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(error)) => {
                warn!("failed with error: {:?}", &error);
                last_error = NetworkError::TaskBackoffFailed(format!("failed with error: {:?}", &error));
            }
            Err(error) => {
                warn!("Task timed out after {:?}", timeout_duration);
                last_error = NetworkError::RetryTimeoutError(timeout_duration);
            }
        };

        if retries < max_retries - 1 {
            debug!("Retrying in {:?}...", current_delay);
            sleep(current_delay).await;

            // Exponential backoff (double the delay each time)
            current_delay = (current_delay * 2).min(max_delay);
        } else {
            warn!("Task failed after {} retries", retries);
            // Return the last error after max retries
        }
        retries += 1;
    }
    warn!("Task failed after {} retries", max_retries);
    Err(last_error.into()) // Fallback error message if no task returns Ok
}

// Helper function to spawn tasks and track their JoinHandles
pub(crate) async fn spawn_task<F, Fut>(
    name: &str,
    task_fn: F,
    handles: Option<&mut Vec<tokio::task::JoinHandle<()>>>,
) where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    // Clone the name so it can be safely moved into the async block
    let name = name.to_string();
    let handle = tokio::spawn(async move {
        if let Err(e) = task_fn().await {
            error!("spawned task: {name} stopped or encountered an error: {:?}", e);
        }
    });

    // Push the handle into the vector inside the Option
    if let Some(h) = handles {
        h.push(handle);
    }
}
