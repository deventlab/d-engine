use std::sync::Arc;

use tokio::sync::mpsc;

use crate::MockCommitHandler;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockPeerChannels;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockStateStorage;
use crate::MockTransport;
use crate::PeerChannelsFactory;
use crate::RaftEvent;
use crate::Settings;
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
}

impl Clone for MockRaftLog {
    fn clone(&self) -> Self {
        let mut new_mock = MockRaftLog::new();

        new_mock
    }
}
impl Clone for MockElectionCore<MockTypeConfig> {
    fn clone(&self) -> Self {
        let mut new_mock = MockElectionCore::new();

        new_mock
    }
}
impl Clone for MockPeerChannels {
    fn clone(&self) -> Self {
        let mut new_mock = MockPeerChannels::new();

        new_mock
    }
}

impl PeerChannelsFactory for MockPeerChannels {
    fn create(_id: u32, _settings: Arc<Settings>) -> Self {
        MockPeerChannels::new()
    }
}
