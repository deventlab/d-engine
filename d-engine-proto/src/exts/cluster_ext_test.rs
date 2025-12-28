use crate::common::NodeStatus;
use crate::server::cluster::{ClusterConfUpdateResponse, cluster_conf_update_response::ErrorCode};

#[test]
fn test_cluster_conf_update_response_success() {
    let response = ClusterConfUpdateResponse::success(1, 5, 10);

    assert_eq!(response.id, 1);
    assert_eq!(response.term, 5);
    assert_eq!(response.version, 10);
    assert!(response.success);
    assert_eq!(response.error_code, ErrorCode::None as i32);
}

#[test]
fn test_cluster_conf_update_response_higher_term() {
    let response = ClusterConfUpdateResponse::higher_term(2, 6, 11);

    assert_eq!(response.id, 2);
    assert_eq!(response.term, 6);
    assert_eq!(response.version, 11);
    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::TermOutdated as i32);
}

#[test]
fn test_cluster_conf_update_response_not_leader() {
    let response = ClusterConfUpdateResponse::not_leader(3, 7, 12);

    assert_eq!(response.id, 3);
    assert_eq!(response.term, 7);
    assert_eq!(response.version, 12);
    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::NotLeader as i32);
}

#[test]
fn test_cluster_conf_update_response_version_conflict() {
    let response = ClusterConfUpdateResponse::version_conflict(4, 8, 13);

    assert_eq!(response.id, 4);
    assert_eq!(response.term, 8);
    assert_eq!(response.version, 13);
    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::VersionConflict as i32);
}

#[test]
fn test_cluster_conf_update_response_invalid_change() {
    let response = ClusterConfUpdateResponse::invalid_change(5, 9, 14);

    assert_eq!(response.id, 5);
    assert_eq!(response.term, 9);
    assert_eq!(response.version, 14);
    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::InvalidChange as i32);
}

#[test]
fn test_cluster_conf_update_response_internal_error() {
    let response = ClusterConfUpdateResponse::internal_error(6, 10, 15);

    assert_eq!(response.id, 6);
    assert_eq!(response.term, 10);
    assert_eq!(response.version, 15);
    assert!(!response.success);
    assert_eq!(response.error_code, ErrorCode::InternalError as i32);
}

#[test]
fn test_cluster_conf_update_response_is_higher_term() {
    let response = ClusterConfUpdateResponse::higher_term(1, 5, 10);
    assert!(response.is_higher_term());
}

#[test]
fn test_cluster_conf_update_response_is_higher_term_false() {
    let response = ClusterConfUpdateResponse::success(1, 5, 10);
    assert!(!response.is_higher_term());
}

#[test]
fn test_cluster_conf_update_response_all_error_types() {
    let success = ClusterConfUpdateResponse::success(1, 1, 1);
    let higher_term = ClusterConfUpdateResponse::higher_term(2, 2, 2);
    let not_leader = ClusterConfUpdateResponse::not_leader(3, 3, 3);
    let version_conflict = ClusterConfUpdateResponse::version_conflict(4, 4, 4);
    let internal_error = ClusterConfUpdateResponse::internal_error(5, 5, 5);

    assert!(success.success);
    assert!(!higher_term.success);
    assert!(!not_leader.success);
    assert!(!version_conflict.success);
    assert!(!internal_error.success);
}

#[test]
fn test_node_status_is_promotable_promotable() {
    // Promotable is the only promotable status according to is_promotable implementation
    assert!(NodeStatus::Promotable.is_promotable());
}

#[test]
fn test_node_status_is_promotable_other_variants() {
    // Other variants should not be promotable
    assert!(!NodeStatus::ReadOnly.is_promotable());
    assert!(!NodeStatus::Active.is_promotable());
}

#[test]
fn test_node_status_is_i32_promotable() {
    assert!(NodeStatus::is_i32_promotable(NodeStatus::Promotable as i32));
    assert!(!NodeStatus::is_i32_promotable(NodeStatus::ReadOnly as i32));
    assert!(!NodeStatus::is_i32_promotable(NodeStatus::Active as i32));
}

#[test]
fn test_cluster_conf_update_response_large_values() {
    let response = ClusterConfUpdateResponse::success(u32::MAX, u64::MAX, u64::MAX);

    assert_eq!(response.id, u32::MAX);
    assert_eq!(response.term, u64::MAX);
    assert_eq!(response.version, u64::MAX);
    assert!(response.success);
}

#[test]
fn test_cluster_conf_update_response_zero_values() {
    let response = ClusterConfUpdateResponse::success(0, 0, 0);

    assert_eq!(response.id, 0);
    assert_eq!(response.term, 0);
    assert_eq!(response.version, 0);
    assert!(response.success);
}
