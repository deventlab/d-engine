use std::sync::Arc;

use crate::MockCommitHandler;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockPeerChannels;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockStateStorage;
use crate::MockTransport;
use crate::PeerChannelsFactory;
use crate::RaftNodeConfig;
use crate::TypeConfig;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct MockTypeConfig;

impl TypeConfig for MockTypeConfig {
    type R = MockRaftLog;

    type E = MockElectionCore<Self>;

    type P = MockPeerChannels;

    type TR = MockTransport;

    type SM = MockStateMachine;

    type SS = MockStateStorage;

    type M = MockMembership<Self>;

    type REP = MockReplicationCore<Self>;

    type C = MockCommitHandler;

    type SMH = MockStateMachineHandler<Self>;

    type SNP = MockSnapshotPolicy;
}

impl Clone for MockRaftLog {
    fn clone(&self) -> Self {
        MockRaftLog::new()
    }
}
impl Clone for MockElectionCore<MockTypeConfig> {
    fn clone(&self) -> Self {
        MockElectionCore::new()
    }
}
impl Clone for MockPeerChannels {
    fn clone(&self) -> Self {
        MockPeerChannels::new()
    }
}

impl PeerChannelsFactory for MockPeerChannels {
    fn create(
        _id: u32,
        _settings: Arc<RaftNodeConfig>,
    ) -> Self {
        let mut peer_channels = MockPeerChannels::new();
        peer_channels.expect_connect_with_peers().times(1).returning(|_| Ok(()));
        peer_channels
            .expect_check_cluster_is_ready()
            .times(1)
            .returning(|| Ok(()));
        peer_channels
    }
}
