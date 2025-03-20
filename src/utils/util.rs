use crate::grpc::rpc_service::Entry;
use crate::{Error, Result, CLUSTER_FATAL_ERROR};
use dashmap::DashSet;
use log::{debug, error, warn};
use prost::Message;
use sled::IVec;
use std::collections::hash_map::DefaultHasher;
use std::fs::{self, create_dir_all, File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    str::FromStr,
};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

#[cfg(test)]
use crate::RaftLog;

pub fn str_to_u64(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// accept ip either like 127.0.0.1 or docker host name: node1
pub(crate) fn address_str(ip: &str, port: u16) -> String {
    let addr: SocketAddr = match Ipv4Addr::from_str(ip) {
        Ok(ipv4) => SocketAddr::V4(SocketAddrV4::new(ipv4, port)),
        Err(_) => {
            // If parsing fails, try resolving the hostname
            (ip, port)
                .to_socket_addrs()
                .map_err(|e| {
                    panic!("Failed to resolve hostname {}: {:?}", ip, e);
                })
                .unwrap()
                .next()
                .expect("No addresses found")
            // panic!("error to call Ipv4Addr::from_str({:?}), {:?}", ip, e);
        }
    };
    format!("http://{}", addr)
}

///size in bytes
pub fn value_with_size(size_in_bytes: usize) -> Vec<u8> {
    vec![0; size_in_bytes]
}
//max of u64
pub fn kv(i: u64) -> Vec<u8> {
    // let i = i % SPACE;
    // let k = [(i >> 16) as u8, (i >> 8) as u8, i as u8];
    let k = [
        ((i >> 56) & 0xFF) as u8,
        ((i >> 48) & 0xFF) as u8,
        ((i >> 40) & 0xFF) as u8,
        ((i >> 32) & 0xFF) as u8,
        ((i >> 24) & 0xFF) as u8,
        ((i >> 16) & 0xFF) as u8,
        ((i >> 8) & 0xFF) as u8,
        (i & 0xFF) as u8,
    ];
    k.to_vec()
}

pub fn vk(v: &Vec<u8>) -> u64 {
    if v.is_empty() {
        error!("v is empty");
    }

    // Expand the vector to length 8
    let mut expanded = vec![0; 8]; // Create a vector of 8 zeros
    let len = v.len();

    // Copy the original vector into the expanded vector
    expanded[..len].copy_from_slice(v); // Copy the elements

    assert_eq!(expanded.len(), 8); // Ensure the length is correct

    let mut result: u64 = 0;
    for &byte in expanded.iter() {
        result = (result << 8) | byte as u64; // Shift left by 8 bits and add the byte
    }
    result
}

pub fn vki(v: &IVec) -> u64 {
    // Convert `IVec` to a byte slice
    let bytes: &[u8] = v.as_ref();

    // Check if the byte slice is empty
    if bytes.is_empty() {
        error!("v is empty");
    }

    // Ensure the length is correct
    assert_eq!(bytes.len(), 8); // Expecting exactly 8 bytes for u64

    // Compute the u64 value from the byte slice
    let mut result: u64 = 0;
    for &byte in bytes.iter() {
        result = (result << 8) | byte as u64; // Shift left by 8 bits and add the byte
    }
    result
}

pub fn skv(name: String) -> Vec<u8> {
    name.encode_to_vec()
}

pub(crate) fn collect_ids(entries: &Vec<Entry>) -> Vec<u64> {
    entries.into_iter().map(|e| e.index).collect()
}

/// abs_ceil(0.3) = 1
/// abs_ceil(0.5) = 1
/// abs_ceil(1.1) = 2
/// abs_ceil(1.9) = 2
pub fn abs_ceil(x: f64) -> u64 {
    x.abs().ceil() as u64
}

pub(crate) async fn is_server_ready(addr: &str) -> bool {
    TcpStream::connect(addr).await.is_ok()
}

pub fn crate_parent_dir_if_not_exist(path: &PathBuf) -> Result<()> {
    if let Some(parent_dir) = path.parent() {
        if !parent_dir.exists() {
            if let Err(e) = create_dir_all(parent_dir) {
                error!("Failed to create log directory: {:?}", e);
                return Err(Error::IoError(e));
            }
        }
    }
    Ok(())
}

pub fn open_file_for_append(path: PathBuf) -> Result<File> {
    crate_parent_dir_if_not_exist(&path)?;
    let log_file = match OpenOptions::new().append(true).create(true).open(&path) {
        Ok(f) => f,
        Err(e) => {
            return Err(Error::IoError(e));
        }
    };
    Ok(log_file)
}

/// record down cluster level error for debug and code optimization purpose.
///
pub(crate) fn record_down_cluster_error(event_id: u64) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs_f64();
    CLUSTER_FATAL_ERROR
        .with_label_values(&[&event_id.to_string()])
        .set(timestamp);
}

pub(crate) fn is_majority(num: usize, count: usize) -> bool {
    num >= count / 2 + 1
}

pub(crate) fn find_nearest_lower_number(
    target_index: u64,
    set_of_index: Arc<DashSet<u64>>,
) -> Option<u64> {
    set_of_index
        .iter()
        .filter(|index| **index <= target_index) // Filter only numbers <= learner_next_index
        .max_by_key(|index| **index) // Find the maximum of the filtered numbers
        .map(|index| *index) // Convert from reference to value
}

pub(crate) async fn write_into_file(path: PathBuf, buf: Vec<u8>) {
    if let Some(parent) = path.parent() {
        if let Err(e) = tokio::fs::create_dir_all(parent).await {
            error!("failed to crate dir with error({})", e);
        } else {
            debug!("created successfully: {:?}", path);
        }
    }

    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .unwrap();
    let mut active_file = BufWriter::new(file);
    active_file.write_all(&buf).await.unwrap();
    active_file.flush().await.unwrap();
}

/// return minisecond
pub(crate) fn get_now_as_u128() -> u128 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_millis()
}

/// return second
pub(crate) fn get_now_as_u64() -> u64 {
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
    since_epoch.as_secs()
}

/// return second as u32
pub(crate) fn get_now_as_u32() -> u32 {
    get_now_as_u64() as u32
}

/// return (high, low)
pub(crate) fn convert_u128_to_u64_with_high_and_low(n: u128) -> (u64, u64) {
    ((n >> 64) as u64, n as u64)
}
/// return (high, low)
pub(crate) fn convert_high_and_low_fromu64_to_u128(high: u64, low: u64) -> u128 {
    ((high as u128) << 64) | (low as u128)
}

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
                warn!(
                    "task_with_timeout_and_exponential_backoff timeout: {:?}",
                    &error
                );
                e = Error::RetryTimeoutError;
            }
        };

        retries += 1;
        if retries < max_retries {
            sleep(delay).await;
            delay = delay * 2; // Exponential backoff (double the delay each time)
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
            error!(
                "spawned task: {name} stopped or encountered an error: {:?}",
                e
            );
        }
    });

    // Push the handle into the vector inside the Option
    if let Some(h) = handles {
        h.push(handle);
    }
}

/// Format error logging
pub fn error(func_name: &str, e: &dyn std::fmt::Debug) {
    error!("{}::{} failed: {:?}", module_path!(), func_name, e);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{abs_ceil, kv, vk};

    #[test]
    fn test_kv_1() {
        let v = kv(1);
        assert_eq!(1, vk(&v));
        let v = kv(25);
        assert_eq!(25, vk(&v));
    }

    #[test]
    fn test_kv_2() {
        let i = std::u64::MAX; //max of u64
        let v = kv(i);
        assert_eq!(i, vk(&v));
    }
    #[test]
    fn test_kv_3() {
        let v = kv(1);
        assert_eq!(1, vk(&v));
    }

    #[test]
    fn test_abs_ceil() {
        assert_eq!(1, abs_ceil(0.3));
        assert_eq!(1, abs_ceil(0.5));
        assert_eq!(2, abs_ceil(1.1));
        assert_eq!(2, abs_ceil(1.9));

        let n = 4 as f64 / 10.0;
        assert_eq!(1, abs_ceil(n));
    }
    #[test]
    fn test_is_majority() {
        assert_eq!(is_majority(0, 3), false);
        assert_eq!(is_majority(1, 3), false);
        assert_eq!(is_majority(2, 3), true);
        assert_eq!(is_majority(3, 3), true);
    }

    #[test]
    fn test_find_nearest_lower_number() {
        let pool: Arc<DashSet<u64>> = Arc::new(DashSet::new());
        // Populate the DashSet with initial values
        pool.insert(1);
        pool.insert(1000);
        pool.insert(3210);
        pool.insert(1382);
        pool.insert(1483);
        pool.insert(2678);
        pool.insert(1637);
        pool.insert(1902);

        // Target number
        let a: u64 = 1701;

        match find_nearest_lower_number(a, pool.clone()) {
            Some(nearest) => assert_eq!(nearest, 1637),
            None => assert!(false),
        }
    }

    async fn async_ok() -> Result<()> {
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }
    async fn async_err() -> Result<()> {
        sleep(Duration::from_millis(100)).await;
        Err(Error::RPCServerDies)
    }

    #[tokio::test]
    async fn test_task_with_exponential_backoff() {
        // Case 1: when ok task return ok
        if let Ok(_) = task_with_timeout_and_exponential_backoff(
            async_ok,
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .await
        {
            assert!(true);
        } else {
            assert!(false);
        }
        // Case 2: when err task return error
        if let Ok(_) = task_with_timeout_and_exponential_backoff(
            async_err,
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .await
        {
            assert!(false);
        } else {
            assert!(true);
        }

        // Case 3: when ok task always failed on timeout error
        if let Ok(_) = task_with_timeout_and_exponential_backoff(
            async_ok,
            3,
            Duration::from_millis(100),
            Duration::from_millis(1),
        )
        .await
        {
            assert!(false);
        } else {
            assert!(true);
        }
    }
}
