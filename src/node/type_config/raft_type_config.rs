use crate::{
    grpc::grpc_transport::GrpcTransport, DefaultCommitHandler, DefaultStateMachineHandler,
    ElectionHandler, RaftMembership, RaftStateMachine, ReplicationHandler, RpcPeerChannels,
    SledRaftLog, SledStateStorage, TypeConfig,
};

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
