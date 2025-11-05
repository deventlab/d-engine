use crate::{
    common::NodeStatus, server::cluster::cluster_conf_update_response::ErrorCode,
    server::cluster::ClusterConfUpdateResponse,
};

impl ClusterConfUpdateResponse {
    /// Generate a successful response (full success)
    pub fn success(
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
    pub fn higher_term(
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
    pub fn not_leader(
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
    pub fn version_conflict(
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
    pub fn invalid_change(
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
    pub fn internal_error(
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
    pub fn is_higher_term(&self) -> bool {
        self.error_code == <ErrorCode as Into<i32>>::into(ErrorCode::TermOutdated)
    }
}

impl NodeStatus {
    pub fn is_promotable(&self) -> bool {
        matches!(self, NodeStatus::Syncing)
    }

    pub fn is_i32_promotable(value: i32) -> bool {
        matches!(value, v if v == (NodeStatus::Syncing as i32))
    }
}
