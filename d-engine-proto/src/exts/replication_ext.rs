use crate::{
    common::LogId,
    server::replication::{
        AppendEntriesResponse, ConflictResult, SuccessResult, append_entries_response,
    },
};

impl AppendEntriesResponse {
    /// Generate a successful response (full success)
    pub fn success(
        node_id: u32,
        term: u64,
        last_match: Option<LogId>,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::Success(SuccessResult {
                last_match,
            })),
        }
    }

    /// Generate conflict response (with conflict details)
    pub fn conflict(
        node_id: u32,
        term: u64,
        conflict_term: Option<u64>,
        conflict_index: Option<u64>,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::Conflict(ConflictResult {
                conflict_term,
                conflict_index,
            })),
        }
    }

    /// Generate a conflict response (Higher term found)
    pub fn higher_term(
        node_id: u32,
        term: u64,
    ) -> Self {
        Self {
            node_id,
            term,
            result: Some(append_entries_response::Result::HigherTerm(term)),
        }
    }

    /// Check if it is a success response
    pub fn is_success(&self) -> bool {
        matches!(
            &self.result,
            Some(append_entries_response::Result::Success(_))
        )
    }

    /// Check if it is a conflict response
    pub fn is_conflict(&self) -> bool {
        matches!(
            &self.result,
            Some(append_entries_response::Result::Conflict(_conflict))
        )
    }

    /// Check if it is a response of a higher Term
    pub fn is_higher_term(&self) -> bool {
        matches!(
            &self.result,
            Some(append_entries_response::Result::HigherTerm(_))
        )
    }
}
