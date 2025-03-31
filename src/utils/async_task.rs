use std::time::Duration;

use log::error;
use log::warn;
use tokio::time::sleep;
use tokio::time::timeout;

use crate::Error;
use crate::Result;

/// General one
pub(crate) async fn task_with_timeout_and_exponential_backoff<F, T, P>(
    task: F,
    max_retries: usize,
    delay_duration: Duration,
    timeout_duration: Duration,
) -> Result<P>
where
    F: Fn() -> T,                               // The type of the async function
    T: std::future::Future<Output = Result<P>>, // The future returned by the async function
{
    // let max_retries = 5;
    let mut retries = 0;
    let mut delay = delay_duration; // Initial delay
    let mut e = Error::RetryTaskFailed("Task failed after max retries".to_string());
    while retries < max_retries {
        match timeout(timeout_duration, task()).await {
            Ok(Ok(r)) => {
                return Ok(r); // Exit on success
            }
            Ok(Err(error)) => {
                warn!("failed with error: {:?}", &error);
                e = error;
            }
            Err(error) => {
                warn!("task_with_timeout_and_exponential_backoff timeout: {:?}", &error);
                e = Error::RetryTimeoutError;
            }
        };

        retries += 1;
        if retries < max_retries {
            sleep(delay).await;
            delay = delay * 2; // Exponential backoff (double the delay each
                               // time)
        } else {
            warn!("Task failed after {} retries", retries);
            e = Error::RetryTaskFailed("Task failed after max retries".to_string());
            // Return the last error after max retries
        }
    }
    Err(e) // Fallback error message if no task returns Ok
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
