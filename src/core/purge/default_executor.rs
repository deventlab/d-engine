use std::sync::Arc;

use tonic::async_trait;

use crate::alias::ROF;
use crate::proto::common::LogId;
use crate::PurgeExecutor;
use crate::RaftLog;
use crate::Result;
use crate::TypeConfig;

/// Default implementation using the configured log storage
pub struct DefaultPurgeExecutor<T: TypeConfig> {
    raft_log: Arc<ROF<T>>,

    /// === Volatile State ===
    /// Background log purge task status
    ///
    /// When present, indicates an asynchronous cleanup task is in progress
    /// targeting the specified log index.
    #[allow(dead_code)]
    pub(super) pending_purge: Option<LogId>,
}

#[async_trait]
impl<T: TypeConfig> PurgeExecutor for DefaultPurgeExecutor<T> {
    async fn execute_purge(
        &self,
        last_included: LogId,
    ) -> Result<()> {
        self.raft_log.purge_logs_up_to(last_included)
    }
}

impl<T: TypeConfig> DefaultPurgeExecutor<T> {
    pub(crate) fn new(raft_log: Arc<ROF<T>>) -> Self {
        DefaultPurgeExecutor {
            raft_log,
            pending_purge: None,
        }
    }
}
