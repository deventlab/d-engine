use crate::grpc::grpc_transport::GrpcTransport;
use crate::DefaultCommitHandler;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::RaftMembership;
use crate::RaftStateMachine;
use crate::ReplicationHandler;
use crate::RpcPeerChannels;
use crate::SledRaftLog;
use crate::SledStateStorage;
use crate::TypeConfig;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct RaftTypeConfig;

impl TypeConfig for RaftTypeConfig {
    type R = SledRaftLog;

    type TR = GrpcTransport;

    type SM = RaftStateMachine;

    type SS = SledStateStorage;

    type M = RaftMembership<Self>;

    type P = RpcPeerChannels;

    type REP = ReplicationHandler<Self>;

    type E = ElectionHandler<Self>;

    type C = DefaultCommitHandler<Self>;

    type SMH = DefaultStateMachineHandler<Self>;
}
