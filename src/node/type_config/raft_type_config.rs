use crate::grpc::grpc_transport::GrpcTransport;
use crate::BufferedRaftLog;
use crate::DefaultCommitHandler;
use crate::DefaultPurgeExecutor;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::LogSizePolicy;
use crate::RaftMembership;
use crate::ReplicationHandler;
use crate::SledStateMachine;
use crate::SledStorageEngine;
use crate::TypeConfig;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct RaftTypeConfig;

impl TypeConfig for RaftTypeConfig {
    type R = BufferedRaftLog<Self>;

    type S = SledStorageEngine;

    type TR = GrpcTransport<Self>;

    type SM = SledStateMachine;

    type M = RaftMembership<Self>;

    type REP = ReplicationHandler<Self>;

    type E = ElectionHandler<Self>;

    type C = DefaultCommitHandler<Self>;

    type SMH = DefaultStateMachineHandler<Self>;

    type SNP = LogSizePolicy;

    type PE = DefaultPurgeExecutor<Self>;
}
