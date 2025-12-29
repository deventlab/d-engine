use std::path::PathBuf;
use std::time::Duration;

use super::*;

#[test]
fn test_error_fatal() {
    let err = Error::Fatal("critical failure".to_string());
    assert_eq!(err.to_string(), "Fatal error: critical failure");
}

#[test]
fn test_consensus_error_role_violation() {
    let err = ConsensusError::RoleViolation {
        current_role: "Follower",
        required_role: "Leader",
        context: "Cannot accept write requests".to_string(),
    };
    let msg = err.to_string();
    assert!(msg.contains("Follower"));
    assert!(msg.contains("Leader"));
}

#[test]
fn test_state_transition_error_not_enough_votes() {
    let err = StateTransitionError::NotEnoughVotes;
    assert_eq!(err.to_string(), "Not enough votes to transition to leader.");
}

#[test]
fn test_state_transition_error_invalid_transition() {
    let err = StateTransitionError::InvalidTransition;
    assert_eq!(err.to_string(), "Invalid state transition.");
}

#[test]
fn test_state_transition_error_lock_error() {
    let err = StateTransitionError::LockError;
    assert_eq!(err.to_string(), "Lock error.");
}

#[test]
fn test_network_error_service_unavailable() {
    let err = NetworkError::ServiceUnavailable("node down".to_string());
    assert_eq!(err.to_string(), "Service unavailable: node down");
}

#[test]
fn test_network_error_timeout() {
    let err = NetworkError::Timeout {
        node_id: 42,
        duration: Duration::from_secs(5),
    };
    let msg = err.to_string();
    assert!(msg.contains("42"));
    assert!(msg.contains("5s"));
}

#[test]
fn test_network_error_empty_peer_list() {
    let err = NetworkError::EmptyPeerList {
        request_type: "AppendEntries",
    };
    let msg = err.to_string();
    assert!(msg.contains("AppendEntries"));
}

#[test]
fn test_network_error_signal_send_failed() {
    let err = NetworkError::SingalSendFailed("channel closed".to_string());
    let msg = err.to_string();
    assert!(msg.contains("channel closed"));
}

#[test]
fn test_storage_error_data_corruption() {
    let err = StorageError::DataCorruption {
        location: "raft_log_entry_42".to_string(),
    };
    let msg = err.to_string();
    assert!(msg.contains("raft_log_entry_42"));
}

#[test]
fn test_storage_error_path_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err = StorageError::PathError {
        path: PathBuf::from("/data/raft.db"),
        source: io_err,
    };
    let msg = err.to_string();
    assert!(msg.contains("/data/raft.db"));
}

#[test]
fn test_id_allocation_error_overflow() {
    let err = IdAllocationError::Overflow {
        start: 100,
        end: 50,
    };
    let msg = err.to_string();
    assert!(msg.contains("100"));
    assert!(msg.contains("50"));
}

#[test]
fn test_id_allocation_error_invalid_range() {
    let err = IdAllocationError::InvalidRange { start: 10, end: 5 };
    let msg = err.to_string();
    assert!(msg.contains("10"));
    assert!(msg.contains("5"));
}

#[test]
fn test_id_allocation_error_no_ids_available() {
    let err = IdAllocationError::NoIdsAvailable;
    let msg = err.to_string();
    assert!(msg.contains("IDs") || msg.contains("available"));
}

#[test]
fn test_file_error_not_found() {
    let err = FileError::NotFound("/path/to/file".to_string());
    let msg = err.to_string();
    assert!(msg.contains("/path/to/file"));
}

#[test]
fn test_file_error_is_directory() {
    let err = FileError::IsDirectory("/path/to/dir".to_string());
    let msg = err.to_string();
    assert!(msg.contains("/path/to/dir"));
}

#[test]
fn test_file_error_permission_denied() {
    let err = FileError::PermissionDenied("/restricted/file".to_string());
    let msg = err.to_string();
    assert!(msg.contains("/restricted/file"));
}

#[test]
fn test_file_error_invalid_gzip_header() {
    let err = FileError::InvalidGzipHeader("header mismatch".to_string());
    let msg = err.to_string();
    assert!(msg.contains("header mismatch"));
}

#[test]
fn test_file_error_too_small() {
    let err = FileError::TooSmall(42);
    let msg = err.to_string();
    assert!(msg.contains("42"));
}

#[test]
fn test_convert_error_invalid_length() {
    let err = ConvertError::InvalidLength(4);
    let msg = err.to_string();
    assert!(msg.contains("4"));
    assert!(msg.contains("8"));
}

#[test]
fn test_convert_error_conversion_failure() {
    let err = ConvertError::ConversionFailure("Cannot convert to u64".to_string());
    let msg = err.to_string();
    assert!(msg.contains("Cannot convert to u64"));
}

#[test]
fn test_write_send_error_not_leader() {
    let err = WriteSendError::NotLeader;
    let msg = err.to_string();
    assert!(msg.contains("leader"));
}

#[test]
fn test_write_send_error_payload_exceeded() {
    let err = WriteSendError::PayloadExceeded;
    let msg = err.to_string();
    assert!(msg.contains("large") || msg.contains("Payload"));
}

#[test]
fn test_write_send_error_unreachable() {
    let err = WriteSendError::Unreachable;
    let msg = err.to_string();
    assert!(msg.contains("unreachable"));
}

#[test]
fn test_system_error_server_unavailable() {
    let err = SystemError::ServerUnavailable;
    let msg = err.to_string();
    assert!(msg.contains("unavailable") || msg.contains("server"));
}

#[test]
fn test_election_error_higher_term() {
    let err = ElectionError::HigherTerm(10);
    let msg = err.to_string();
    assert!(msg.contains("10"));
}

#[test]
fn test_election_error_term_conflict() {
    let err = ElectionError::TermConflict {
        current: 5,
        received: 3,
    };
    let msg = err.to_string();
    assert!(msg.contains("5"));
    assert!(msg.contains("3"));
}

#[test]
fn test_election_error_log_conflict() {
    let err = ElectionError::LogConflict {
        index: 100,
        expected_term: 5,
        actual_term: 4,
    };
    let msg = err.to_string();
    assert!(msg.contains("100"));
    assert!(msg.contains("5"));
    assert!(msg.contains("4"));
}

#[test]
fn test_election_error_quorum_failure() {
    let err = ElectionError::QuorumFailure {
        required: 3,
        succeed: 2,
    };
    let msg = err.to_string();
    assert!(msg.contains("3"));
    assert!(msg.contains("2"));
}

#[test]
fn test_election_error_no_voting_member_found() {
    let err = ElectionError::NoVotingMemberFound { candidate_id: 42 };
    let msg = err.to_string();
    assert!(msg.contains("42"));
}

#[test]
fn test_replication_error_higher_term() {
    let err = ReplicationError::HigherTerm(15);
    let msg = err.to_string();
    assert!(msg.contains("15"));
}

#[test]
fn test_replication_error_quorum_not_reached() {
    let err = ReplicationError::QuorumNotReached;
    let msg = err.to_string();
    assert!(msg.contains("Quorum") && msg.contains("replication"));
}

#[test]
fn test_replication_error_node_unreachable() {
    let err = ReplicationError::NodeUnreachable { node_id: 123 };
    let msg = err.to_string();
    assert!(msg.contains("123"));
}

#[test]
fn test_replication_error_log_conflict() {
    let err = ReplicationError::LogConflict {
        index: 50,
        expected_term: 3,
        actual_term: 2,
    };
    let msg = err.to_string();
    assert!(msg.contains("50"));
    assert!(msg.contains("3"));
    assert!(msg.contains("2"));
}

#[test]
fn test_replication_error_not_leader() {
    let err = ReplicationError::NotLeader { leader_id: Some(1) };
    let msg = err.to_string();
    assert!(msg.contains("1"));
}

#[test]
fn test_membership_error_not_leader() {
    let err = MembershipError::NotLeader;
    let msg = err.to_string();
    assert!(msg.contains("leader"));
}

#[test]
fn test_membership_error_no_leader_found() {
    let err = MembershipError::NoLeaderFound;
    let msg = err.to_string();
    assert!(msg.contains("leader"));
}

#[test]
fn test_membership_error_node_already_exists() {
    let err = MembershipError::NodeAlreadyExists(42);
    let msg = err.to_string();
    assert!(msg.contains("42"));
}

#[test]
fn test_membership_error_invalid_promotion() {
    let err = MembershipError::InvalidPromotion {
        node_id: 5,
        role: 1, // Use i32 instead of String
    };
    let msg = err.to_string();
    assert!(msg.contains("5"));
    assert!(msg.contains("1"));
}

#[test]
fn test_membership_error_remove_node_is_leader() {
    let err = MembershipError::RemoveNodeIsLeader(3);
    let msg = err.to_string();
    assert!(msg.contains("3"));
}

#[test]
fn test_membership_error_no_metadata_found() {
    let err = MembershipError::NoMetadataFoundForNode { node_id: 99 };
    let msg = err.to_string();
    assert!(msg.contains("99"));
}

#[test]
fn test_snapshot_error_rejected() {
    let err = SnapshotError::Rejected { last_chunk: 42 };
    let msg = err.to_string();
    assert!(msg.contains("42"));
}

#[test]
fn test_snapshot_error_checksum_mismatch() {
    let err = SnapshotError::ChecksumMismatch;
    let msg = err.to_string();
    assert!(msg.contains("checksum") && msg.contains("mismatch"));
}

#[test]
fn test_snapshot_error_invalid_snapshot() {
    let err = SnapshotError::InvalidSnapshot;
    let msg = err.to_string();
    assert!(msg.contains("snapshot") || msg.contains("Invalid"));
}

#[test]
fn test_snapshot_error_incomplete_snapshot() {
    let err = SnapshotError::IncompleteSnapshot;
    let msg = err.to_string();
    assert!(msg.contains("snapshot") || msg.contains("Incomplete"));
}

// Test From trait implementations for error conversions
#[test]
fn test_from_network_error_to_error() {
    let network_err = NetworkError::ServiceUnavailable("test".to_string());
    let err: Error = network_err.into();
    assert!(matches!(err, Error::System(SystemError::Network(_))));
}

#[test]
fn test_from_storage_error_to_error() {
    let storage_err = StorageError::LogStorage("test error".to_string());
    let err: Error = storage_err.into();
    assert!(matches!(err, Error::System(SystemError::Storage(_))));
}

#[test]
fn test_from_state_transition_error_to_error() {
    let state_err = StateTransitionError::NotEnoughVotes;
    let err: Error = state_err.into();
    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::StateTransition(_))
    ));
}

#[test]
fn test_from_election_error_to_error() {
    let election_err = ElectionError::Failed("election timeout".to_string());
    let err: Error = election_err.into();
    assert!(matches!(err, Error::Consensus(ConsensusError::Election(_))));
}

#[test]
fn test_from_replication_error_to_error() {
    let replication_err = ReplicationError::QuorumNotReached;
    let err: Error = replication_err.into();
    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::Replication(_))
    ));
}

#[test]
fn test_from_membership_error_to_error() {
    let membership_err = MembershipError::NotLeader;
    let err: Error = membership_err.into();
    assert!(matches!(
        err,
        Error::Consensus(ConsensusError::Membership(_))
    ));
}

#[test]
fn test_from_snapshot_error_to_error() {
    let snapshot_err = SnapshotError::InvalidSnapshot;
    let err: Error = snapshot_err.into();
    assert!(matches!(err, Error::Consensus(ConsensusError::Snapshot(_))));
}

#[test]
fn test_from_io_error_to_error() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
    let err: Error = io_err.into();
    assert!(matches!(err, Error::System(SystemError::Storage(_))));
}

#[test]
fn test_from_id_allocation_error_to_error() {
    let id_err = IdAllocationError::NoIdsAvailable;
    let err: Error = id_err.into();
    assert!(matches!(err, Error::System(SystemError::Storage(_))));
}

#[test]
fn test_error_display_formatting() {
    let err = Error::Fatal("system failure".to_string());
    let display = format!("{err}");
    assert_eq!(display, "Fatal error: system failure");
}

#[test]
fn test_consensus_error_nesting() {
    let state_err = StateTransitionError::InvalidTransition;
    let consensus_err: ConsensusError = state_err.into();
    let top_err: Error = consensus_err.into();

    assert!(matches!(
        top_err,
        Error::Consensus(ConsensusError::StateTransition(
            StateTransitionError::InvalidTransition
        ))
    ));
}

#[test]
fn test_network_error_connect_error() {
    let err = NetworkError::ConnectError("connection refused".to_string());
    let msg = err.to_string();
    assert!(msg.contains("connection refused"));
}

#[test]
fn test_network_error_invalid_uri() {
    let err = NetworkError::InvalidURI("malformed://uri".to_string());
    let msg = err.to_string();
    assert!(msg.contains("malformed://uri"));
}

#[test]
fn test_system_error_general_server() {
    let err = SystemError::GeneralServer("server error".to_string());
    let msg = err.to_string();
    assert!(msg.contains("server error"));
}

#[test]
fn test_system_error_node_start_failed_with_message() {
    let err = SystemError::NodeStartFailed("initialization failed".to_string());
    let msg = err.to_string();
    assert!(msg.contains("initialization failed"));
}
