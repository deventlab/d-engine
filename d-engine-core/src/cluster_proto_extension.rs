use d_engine_proto::{
    common::NodeStatus, error::ErrorCode, server::cluster::ClusterConfUpdateResponse,
};

pub trait ClusterConfUpdateResponseExt {
    /// Generate a successful response (full success)
    fn success(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    /// Generate a failed response (Stale leader term)
    fn higher_term(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    /// Generate a failed response (Request sent to non-leader or an out-dated leader)
    fn not_leader(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    /// Generate a failed response (Stale configuration version)
    fn version_conflict(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    /// Generate a failed response (Malformed change request)
    #[allow(unused)]
    fn invalid_change(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    /// Generate a failed response (Server-side processing error)
    fn internal_error(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self;

    #[allow(unused)]
    fn is_higher_term(&self) -> bool;
}

pub trait NodeStatusExt {
    fn is_promotable(&self) -> bool;

    fn is_i32_promotable(value: i32) -> bool;
}

impl ClusterConfUpdateResponseExt for ClusterConfUpdateResponse {
    /// Generate a successful response (full success)
    fn success(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: true,
            error_code: ErrorCode::None.into(),
        }
    }

    /// Generate a failed response (Stale leader term)
    fn higher_term(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::TermOutdated.into(),
        }
    }

    /// Generate a failed response (Request sent to non-leader or an out-dated leader)
    fn not_leader(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::NotLeader.into(),
        }
    }

    /// Generate a failed response (Stale configuration version)
    fn version_conflict(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::VersionConflict.into(),
        }
    }

    /// Generate a failed response (Malformed change request)
    #[allow(unused)]
    fn invalid_change(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::InvalidChange.into(),
        }
    }

    /// Generate a failed response (Server-side processing error)
    fn internal_error(
        node_id: u32,
        term: u64,
        version: u64,
    ) -> Self {
        Self {
            id: node_id,
            term,
            version,
            success: false,
            error_code: ErrorCode::InternalError.into(),
        }
    }

    #[allow(unused)]
    fn is_higher_term(&self) -> bool {
        self.error_code == <ErrorCode as Into<i32>>::into(ErrorCode::TermOutdated)
    }
}

impl NodeStatusExt for NodeStatus {
    fn is_promotable(&self) -> bool {
        matches!(self, NodeStatus::Syncing)
    }

    fn is_i32_promotable(value: i32) -> bool {
        matches!(value, v if v == (NodeStatus::Syncing as i32))
    }
}
