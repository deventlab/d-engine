use d_engine_core::DefaultPurgeExecutor;
use d_engine_core::DefaultStateMachineHandler;
use d_engine_core::DefaultStateMachineWriter;
use d_engine_core::ElectionHandler;
use d_engine_core::LogSizePolicy;
use d_engine_core::RaftLogCore;
use d_engine_core::ReplicationHandler;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use d_engine_core::TypeConfig;

use crate::membership::RaftMembership;
use crate::network::grpc::grpc_transport::GrpcTransport;

#[derive(Debug)]
pub struct RaftTypeConfig<SE, SM>
where
    SE: StorageEngine + std::fmt::Debug,
    SM: StateMachine + std::fmt::Debug,
{
    _marker: std::marker::PhantomData<(SE, SM)>,
}

impl<SE, SM> TypeConfig for RaftTypeConfig<SE, SM>
where
    SE: StorageEngine + std::fmt::Debug,
    SM: StateMachine + std::fmt::Debug,
{
    type SE = SE;

    type SM = SM;

    type R = RaftLogCore<Self>;

    type TR = GrpcTransport<Self>;

    type M = RaftMembership<Self>;

    type REP = ReplicationHandler<Self>;

    type E = ElectionHandler<Self>;

    type SMH = DefaultStateMachineHandler<Self>;

    type SMW = DefaultStateMachineWriter<Self>;

    type SNP = LogSizePolicy;

    type PE = DefaultPurgeExecutor<Self>;
}
