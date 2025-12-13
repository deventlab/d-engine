use crate::client::{
    ClientReadRequest, ClientResponse, ClientResult, ReadConsistencyPolicy, WriteCommand,
    client_response::SuccessResult,
};
use crate::error::ErrorCode;
use bytes::Bytes;

#[test]
fn test_client_read_request_has_consistency_policy_false() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: None,
    };

    assert!(!request.has_consistency_policy());
}

#[test]
fn test_client_read_request_has_consistency_policy_true() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };

    assert!(request.has_consistency_policy());
}

#[test]
fn test_client_read_request_get_consistency_policy_none() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: None,
    };

    assert_eq!(request.get_consistency_policy(), None);
}

#[test]
fn test_client_read_request_get_consistency_policy_lease_read() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(ReadConsistencyPolicy::LeaseRead as i32),
    };

    assert_eq!(
        request.get_consistency_policy(),
        Some(ReadConsistencyPolicy::LeaseRead)
    );
}

#[test]
fn test_client_read_request_get_consistency_policy_linearizable_read() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(ReadConsistencyPolicy::LinearizableRead as i32),
    };

    assert_eq!(
        request.get_consistency_policy(),
        Some(ReadConsistencyPolicy::LinearizableRead)
    );
}

#[test]
fn test_client_read_request_get_consistency_policy_invalid() {
    let request = ClientReadRequest {
        client_id: 1,
        keys: vec![],
        consistency_policy: Some(999), // Invalid value
    };

    assert_eq!(request.get_consistency_policy(), None);
}

#[test]
fn test_write_command_insert_creation() {
    let key = "test_key";
    let value = "test_value";
    let cmd = WriteCommand::insert(key, value);

    match cmd.operation {
        Some(crate::client::write_command::Operation::Insert(insert_cmd)) => {
            assert_eq!(insert_cmd.key, Bytes::from("test_key"));
            assert_eq!(insert_cmd.value, Bytes::from("test_value"));
        }
        _ => panic!("Expected Insert operation"),
    }
}

#[test]
fn test_write_command_insert_with_bytes() {
    let key = Bytes::from("key");
    let value = Bytes::from("value");
    let cmd = WriteCommand::insert(key.clone(), value.clone());

    match cmd.operation {
        Some(crate::client::write_command::Operation::Insert(insert_cmd)) => {
            assert_eq!(insert_cmd.key, key);
            assert_eq!(insert_cmd.value, value);
        }
        _ => panic!("Expected Insert operation"),
    }
}

#[test]
fn test_write_command_delete_creation() {
    let key = "delete_key";
    let cmd = WriteCommand::delete(key);

    match cmd.operation {
        Some(crate::client::write_command::Operation::Delete(delete_cmd)) => {
            assert_eq!(delete_cmd.key, Bytes::from("delete_key"));
        }
        _ => panic!("Expected Delete operation"),
    }
}

#[test]
fn test_write_command_delete_with_bytes() {
    let key = Bytes::from("my_key");
    let cmd = WriteCommand::delete(key.clone());

    match cmd.operation {
        Some(crate::client::write_command::Operation::Delete(delete_cmd)) => {
            assert_eq!(delete_cmd.key, key);
        }
        _ => panic!("Expected Delete operation"),
    }
}

#[test]
fn test_client_response_write_success() {
    let response = ClientResponse::write_success();

    assert_eq!(response.error, ErrorCode::Success as i32);
    assert!(response.success_result.is_some());
    assert_eq!(response.metadata, None);

    match response.success_result {
        Some(SuccessResult::WriteAck(true)) => {
            // Success
        }
        _ => panic!("Expected WriteAck success"),
    }
}

#[test]
fn test_client_response_is_write_success() {
    let response = ClientResponse::write_success();
    assert!(response.is_write_success());
}

#[test]
fn test_client_response_is_write_success_false() {
    let response = ClientResponse::client_error(ErrorCode::ProposeFailed);
    assert!(!response.is_write_success());
}

#[test]
fn test_client_response_read_results() {
    let results = vec![
        ClientResult {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        },
        ClientResult {
            key: Bytes::from("key2"),
            value: Bytes::from("value2"),
        },
    ];

    let response = ClientResponse::read_results(results.clone());

    assert_eq!(response.error, ErrorCode::Success as i32);
    match response.success_result {
        Some(SuccessResult::ReadData(read_results)) => {
            assert_eq!(read_results.results.len(), 2);
            assert_eq!(read_results.results[0].key, Bytes::from("key1"));
            assert_eq!(read_results.results[0].value, Bytes::from("value1"));
        }
        _ => panic!("Expected ReadData success"),
    }
}

#[test]
fn test_client_response_client_error() {
    let response = ClientResponse::client_error(ErrorCode::ConnectionTimeout);

    assert_eq!(response.error, ErrorCode::ConnectionTimeout as i32);
    assert_eq!(response.success_result, None);
    assert_eq!(response.metadata, None);
}

#[test]
fn test_client_response_not_leader_with_metadata() {
    let response = ClientResponse::not_leader(
        Some("node-1".to_string()),
        Some("127.0.0.1:9081".to_string()),
    );

    assert_eq!(response.error, ErrorCode::NotLeader as i32);
    assert_eq!(response.success_result, None);
    assert!(response.metadata.is_some());

    let metadata = response.metadata.unwrap();
    assert_eq!(metadata.leader_id, Some("node-1".to_string()));
    assert_eq!(metadata.leader_address, Some("127.0.0.1:9081".to_string()));
}

#[test]
fn test_client_response_not_leader_without_metadata() {
    let response = ClientResponse::not_leader(None, None);

    assert_eq!(response.error, ErrorCode::NotLeader as i32);
    assert_eq!(response.success_result, None);
    assert_eq!(response.metadata, None);
}

#[test]
fn test_client_response_not_leader_partial_metadata() {
    let response = ClientResponse::not_leader(Some("node-2".to_string()), None);

    assert_eq!(response.error, ErrorCode::NotLeader as i32);
    assert!(response.metadata.is_some());

    let metadata = response.metadata.unwrap();
    assert_eq!(metadata.leader_id, Some("node-2".to_string()));
    assert_eq!(metadata.leader_address, None);
}

#[test]
fn test_client_response_is_term_outdated() {
    let response = ClientResponse::client_error(ErrorCode::TermOutdated);
    assert!(response.is_term_outdated());
}

#[test]
fn test_client_response_is_term_outdated_false() {
    let response = ClientResponse::client_error(ErrorCode::ConnectionTimeout);
    assert!(!response.is_term_outdated());
}

#[test]
fn test_client_response_is_quorum_timeout_or_failure() {
    assert!(
        ClientResponse::client_error(ErrorCode::ConnectionTimeout).is_quorum_timeout_or_failure()
    );
    assert!(ClientResponse::client_error(ErrorCode::ProposeFailed).is_quorum_timeout_or_failure());
    assert!(
        ClientResponse::client_error(ErrorCode::ClusterUnavailable).is_quorum_timeout_or_failure()
    );
}

#[test]
fn test_client_response_is_propose_failure() {
    let response = ClientResponse::client_error(ErrorCode::ProposeFailed);
    assert!(response.is_propose_failure());
}

#[test]
fn test_client_response_is_retry_required() {
    let response = ClientResponse::client_error(ErrorCode::RetryRequired);
    assert!(response.is_retry_required());
}

#[test]
fn test_error_code_is_term_outdated() {
    assert!(ErrorCode::TermOutdated.is_term_outdated());
    assert!(!ErrorCode::Success.is_term_outdated());
}

#[test]
fn test_error_code_is_propose_failure() {
    assert!(ErrorCode::ProposeFailed.is_propose_failure());
    assert!(!ErrorCode::Success.is_propose_failure());
}

#[test]
fn test_error_code_is_retry_required() {
    assert!(ErrorCode::RetryRequired.is_retry_required());
    assert!(!ErrorCode::Success.is_retry_required());
}

#[test]
fn test_write_command_insert_empty_key_value() {
    let cmd = WriteCommand::insert("", "");

    match cmd.operation {
        Some(crate::client::write_command::Operation::Insert(insert_cmd)) => {
            assert_eq!(insert_cmd.key, Bytes::from(""));
            assert_eq!(insert_cmd.value, Bytes::from(""));
        }
        _ => panic!("Expected Insert operation"),
    }
}

#[test]
fn test_write_command_insert_large_data() {
    // Create byte vectors directly instead of going through String
    let large_key = vec![b'a'; 1000];
    let large_value = vec![b'b'; 10000];

    // Convert to Bytes before passing to insert
    let cmd = WriteCommand::insert(Bytes::from(large_key), Bytes::from(large_value));

    match cmd.operation {
        Some(crate::client::write_command::Operation::Insert(insert_cmd)) => {
            assert_eq!(insert_cmd.key.len(), 1000);
            assert_eq!(insert_cmd.value.len(), 10000);
            // Verify the content is correct
            assert!(insert_cmd.key.iter().all(|&b| b == b'a'));
            assert!(insert_cmd.value.iter().all(|&b| b == b'b'));
        }
        _ => panic!("Expected Insert operation"),
    }
}

#[test]
fn test_client_response_read_results_empty() {
    let response = ClientResponse::read_results(vec![]);

    assert_eq!(response.error, ErrorCode::Success as i32);
    match response.success_result {
        Some(SuccessResult::ReadData(read_results)) => {
            assert_eq!(read_results.results.len(), 0);
        }
        _ => panic!("Expected ReadData success"),
    }
}
