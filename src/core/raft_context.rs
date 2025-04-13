use std::fmt::Debug;
use std::sync::Arc;

use crate::alias::EOF;
use crate::alias::MOF;
use crate::alias::POF;
use crate::alias::REPOF;
use crate::alias::ROF;
use crate::alias::SMHOF;
use crate::alias::SMOF;
use crate::alias::SSOF;
use crate::alias::TROF;
use crate::ChannelWithAddressAndRole;
use crate::Membership;
use crate::RaftNodeConfig;
use crate::TypeConfig;

pub struct RaftContext<T>
where
    T: TypeConfig,
{
    pub node_id: u32,

    // Storages
    pub raft_log: Arc<ROF<T>>,
    pub state_machine: Arc<SMOF<T>>,
    pub state_storage: Box<SSOF<T>>,

    // Network
    pub transport: Arc<TROF<T>>,

    // Cluster Membership
    pub membership: Arc<MOF<T>>,

    // Handlers
    pub election_handler: EOF<T>,
    pub replication_handler: REPOF<T>,
    pub state_machine_handler: Arc<SMHOF<T>>, /* it was used in both commit_handlers and other
                                               * places */

    // RaftNodeConfig
    pub settings: Arc<RaftNodeConfig>,
}

impl<T> RaftContext<T>
where
    T: TypeConfig,
{
    pub fn raft_log(&self) -> &Arc<ROF<T>> {
        &self.raft_log
    }

    pub fn state_machine(&self) -> &SMOF<T> {
        &self.state_machine
    }

    pub fn state_storage(&self) -> &SSOF<T> {
        &self.state_storage
    }

    pub fn transport(&self) -> &Arc<TROF<T>> {
        &self.transport
    }
    pub fn replication_handler(&self) -> &REPOF<T> {
        &self.replication_handler
    }
    pub fn election_handler(&self) -> &EOF<T> {
        &self.election_handler
    }
    pub fn settings(&self) -> Arc<RaftNodeConfig> {
        self.settings.clone()
    }

    pub fn membership(&self) -> Arc<MOF<T>> {
        self.membership.clone()
    }

    pub fn membership_ref(&self) -> &Arc<MOF<T>> {
        &self.membership
    }

    pub fn voting_members(
        &self,
        peer_channels: Arc<POF<T>>,
    ) -> Vec<ChannelWithAddressAndRole> {
        self.membership.voting_members(peer_channels)
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
