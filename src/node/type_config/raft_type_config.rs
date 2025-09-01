use std::fmt::Debug;

use crate::grpc::grpc_transport::GrpcTransport;
use crate::BufferedRaftLog;
use crate::DefaultCommitHandler;
use crate::DefaultPurgeExecutor;
use crate::DefaultStateMachineHandler;
use crate::ElectionHandler;
use crate::LogSizePolicy;
use crate::RaftMembership;
use crate::ReplicationHandler;
use crate::StateMachine;
use crate::StorageEngine;
use crate::TypeConfig;

#[derive(Debug)]
pub struct RaftTypeConfig<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    _marker: std::marker::PhantomData<(SE, SM)>,
}

impl<SE, SM> TypeConfig for RaftTypeConfig<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    type SE = SE;

    type SM = SM;

    type R = BufferedRaftLog<Self>;

    type TR = GrpcTransport<Self>;

    type M = RaftMembership<Self>;

    type REP = ReplicationHandler<Self>;

    type E = ElectionHandler<Self>;

    type C = DefaultCommitHandler<Self>;

    type SMH = DefaultStateMachineHandler<Self>;

    type SNP = LogSizePolicy;

    type PE = DefaultPurgeExecutor<Self>;
}
