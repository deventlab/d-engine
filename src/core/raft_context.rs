use crate::alias::EOF;
use crate::alias::MOF;
use crate::alias::PE;
use crate::alias::REPOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::alias::SMOF;
use crate::alias::TROF;
use crate::RaftNodeConfig;
use crate::TypeConfig;
use std::fmt::Debug;
use std::sync::Arc;

pub(crate) struct RaftStorageHandles<T: TypeConfig> {
    pub(crate) raft_log: Arc<ROF<T>>,
    pub(crate) state_machine: Arc<SMOF<T>>,
}

pub(crate) struct RaftCoreHandlers<T: TypeConfig> {
    pub(crate) election_handler: EOF<T>,
    pub(crate) replication_handler: REPOF<T>,
    pub(crate) state_machine_handler: Arc<SMHOF<T>>,

    // Raft Log Purge Executor
    pub(crate) purge_executor: Arc<PE<T>>,
}

pub(crate) struct RaftContext<T>
where
    T: TypeConfig,
{
    pub(crate) node_id: u32,

    // Storages
    pub(crate) storage: RaftStorageHandles<T>,

    // Network
    pub(crate) transport: Arc<TROF<T>>,

    // Cluster Membership
    pub(crate) membership: Arc<MOF<T>>,

    // Handlers
    pub(crate) handlers: RaftCoreHandlers<T>,

    // RaftNodeConfig
    pub(crate) node_config: Arc<RaftNodeConfig>,
}

impl<T> RaftContext<T>
where
    T: TypeConfig,
{
    pub fn raft_log(&self) -> &Arc<ROF<T>> {
        &self.storage.raft_log
    }

    pub fn state_machine(&self) -> &SMOF<T> {
        &self.storage.state_machine
    }

    pub fn transport(&self) -> &Arc<TROF<T>> {
        &self.transport
    }
    pub fn replication_handler(&self) -> &REPOF<T> {
        &self.handlers.replication_handler
    }

    pub fn election_handler(&self) -> &EOF<T> {
        &self.handlers.election_handler
    }

    pub fn state_machine_handler(&self) -> &Arc<SMHOF<T>> {
        &self.handlers.state_machine_handler
    }

    pub fn node_config(&self) -> Arc<RaftNodeConfig> {
        self.node_config.clone()
    }

    pub fn membership(&self) -> Arc<MOF<T>> {
        self.membership.clone()
    }

    pub fn purge_executor(&self) -> Arc<PE<T>> {
        self.handlers.purge_executor.clone()
    }

    pub fn membership_ref(&self) -> &Arc<MOF<T>> {
        &self.membership
    }

    #[cfg(test)]
    pub fn set_membership(
        &mut self,
        membership: Arc<MOF<T>>,
    ) {
        self.membership = membership;
    }

    #[cfg(test)]
    pub fn set_transport(
        &mut self,
        transport: Arc<TROF<T>>,
    ) {
        self.transport = transport
    }
}

impl<T> Debug for RaftContext<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftContext").field("node_id", &self.node_id).finish()
    }
}
