use std::fmt::Debug;

use crate::BufferedRaftLog;
use crate::RaftMembership;
use crate::grpc::grpc_transport::GrpcTransport;
use d_engine_core::DefaultCommitHandler;
use d_engine_core::DefaultPurgeExecutor;
use d_engine_core::DefaultStateMachineHandler;
use d_engine_core::ElectionHandler;
use d_engine_core::LogSizePolicy;
use d_engine_core::ReplicationHandler;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use d_engine_core::TypeConfig;

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
