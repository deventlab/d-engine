use std::sync::Arc;

use tracing_test::traced_test;

use super::*;
use crate::Error;
use crate::MockRaftLog;
use crate::MockTypeConfig;

// Assuming RaftLog is defined and can be mocked with mockall
// For example, if RaftLog is a trait, you can mock it as follows:
// But if RaftLog is a struct, you'll need to adjust accordingly

#[tokio::test]
#[traced_test]
async fn test_default_purge_executor_execute_purge_success() {
    // Create a mock for RaftLog
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log.expect_purge_logs_up_to().returning(|_| Ok(()));

    // Create the executor with the mock
    let executor = DefaultPurgeExecutor::<MockTypeConfig>::new(Arc::new(mock_raft_log));

    // Call the method with a sample LogId
    let last_included = LogId {
        index: 100,
        term: 1,
    };
    let result = executor.execute_purge(last_included).await;

    // Assert that the operation was successful
    assert!(result.is_ok());
    assert!(executor.validate_purge(LogId::default()).await.is_ok());
    assert!(executor.pre_purge().await.is_ok());
    assert!(executor.post_purge().await.is_ok());
}

#[tokio::test]
#[traced_test]
async fn test_default_purge_executor_execute_purge_error() {
    // Create a mock for RaftLog that returns an error
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log
        .expect_purge_logs_up_to()
        .returning(|_| Err(Error::Fatal("test".to_string())));

    // Create the executor with the mock
    let executor = DefaultPurgeExecutor::<MockTypeConfig>::new(Arc::new(mock_raft_log));

    // Call the method with a sample LogId
    let last_included = LogId {
        index: 100,
        term: 1,
    };
    let result = executor.execute_purge(last_included).await;

    // Assert that the operation returned an error
    assert!(result.is_err());
}

#[tokio::test]
#[traced_test]
async fn test_default_purge_executor_pending_purge() {
    // Create a mock for RaftLog
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log.expect_purge_logs_up_to().returning(|_| Ok(()));

    // Create the executor with the mock
    let executor = DefaultPurgeExecutor::<MockTypeConfig>::new(Arc::new(mock_raft_log));

    // Verify initial state
    assert_eq!(executor.pending_purge, None);
}

#[tokio::test]
#[traced_test]
async fn test_default_purge_executor_multiple_purges() {
    // Create a mock for RaftLog that returns Ok for multiple calls
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log.expect_purge_logs_up_to().times(3).returning(|_| Ok(()));

    // Create the executor with the mock
    let executor = DefaultPurgeExecutor::<MockTypeConfig>::new(Arc::new(mock_raft_log));

    // Call execute_purge multiple times
    let last_included1 = LogId {
        index: 100,
        term: 1,
    };
    let last_included2 = LogId {
        index: 200,
        term: 1,
    };
    let last_included3 = LogId {
        index: 300,
        term: 1,
    };

    executor.execute_purge(last_included1).await.unwrap();
    executor.execute_purge(last_included2).await.unwrap();
    executor.execute_purge(last_included3).await.unwrap();
}

#[tokio::test]
#[traced_test]
async fn test_default_purge_executor_with_large_log_id() {
    // Create a mock for RaftLog
    let mut mock_raft_log = MockRaftLog::new();
    mock_raft_log.expect_purge_logs_up_to().returning(|_| Ok(()));

    // Create the executor with the mock
    let executor = DefaultPurgeExecutor::<MockTypeConfig>::new(Arc::new(mock_raft_log));

    // Call the method with a very large LogId
    let last_included = LogId {
        index: u64::MAX,
        term: 1,
    }; // Maximum possible value
    let result = executor.execute_purge(last_included).await;

    // Assert that the operation was successful
    assert!(result.is_ok());
}
