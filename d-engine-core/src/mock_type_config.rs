use crate::MockCommitHandler;
use crate::MockElectionCore;
use crate::MockMembership;
use crate::MockPurgeExecutor;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::MockSnapshotPolicy;
use crate::MockStateMachine;
use crate::MockStateMachineHandler;
use crate::MockTransport;
use crate::TypeConfig;
use crate::mock_storage_engine::MockStorageEngine;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct MockTypeConfig;

impl TypeConfig for MockTypeConfig {
    type R = MockRaftLog;

    type SE = MockStorageEngine;

    type E = MockElectionCore<Self>;

    type TR = MockTransport<Self>;

    type SM = MockStateMachine;

    type M = MockMembership<Self>;

    type REP = MockReplicationCore<Self>;

    type C = MockCommitHandler;

    type SMH = MockStateMachineHandler<Self>;

    type SNP = MockSnapshotPolicy;

    type PE = MockPurgeExecutor;
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
