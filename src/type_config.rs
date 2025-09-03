use std::fmt::Debug;

use crate::CommitHandler;
use crate::ElectionCore;
use crate::Membership;
use crate::PurgeExecutor;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::SnapshotPolicy;
use crate::StateMachine;
use crate::StateMachineHandler;
use crate::StorageEngine;
use crate::Transport;

pub trait TypeConfig: Sync + Send + Sized + Debug + 'static {
    // Required type - specified by the application layer
    //
    type SE: StorageEngine + Debug;

    type SM: StateMachine + Debug;

    // Optional type - has default implementation
    //
    type R: RaftLog + Debug;

    type M: Membership<Self> + Debug;

    type TR: Transport<Self>;

    type E: ElectionCore<Self>;

    type REP: ReplicationCore<Self>;

    type C: CommitHandler;

    type SMH: StateMachineHandler<Self> + Debug;

    type SNP: SnapshotPolicy + Debug;

    type PE: PurgeExecutor + Send + Sync + 'static;
}

pub mod alias {
    use super::TypeConfig;

    pub type ROF<T> = <T as TypeConfig>::R;

    pub type SOF<T> = <T as TypeConfig>::SE;

    pub type TROF<T> = <T as TypeConfig>::TR;

    pub type SMOF<T> = <T as TypeConfig>::SM;

    pub type MOF<T> = <T as TypeConfig>::M;

    pub type EOF<T> = <T as TypeConfig>::E;

    pub type REPOF<T> = <T as TypeConfig>::REP;

    pub type COF<T> = <T as TypeConfig>::C;

    pub type SMHOF<T> = <T as TypeConfig>::SMH;

    pub type SNP<T> = <T as TypeConfig>::SNP;

    pub type PE<T> = <T as TypeConfig>::PE;
}
