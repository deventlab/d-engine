use std::fmt::Debug;

use crate::CommitHandler;
use crate::ElectionCore;
use crate::Membership;
use crate::PeerChannels;
use crate::PeerChannelsFactory;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::StateMachine;
use crate::StateMachineHandler;
use crate::StateStorage;
use crate::Transport;

pub trait TypeConfig:
    Sync + Send + Sized + Debug + Clone + Copy + Default + Eq + PartialEq + Ord + PartialOrd + 'static
{
    type R: RaftLog + Debug;

    type TR: Transport;

    type SM: StateMachine + Debug;

    type SS: StateStorage;

    type M: Membership<Self>;

    type P: PeerChannels + PeerChannelsFactory + Clone + Debug;

    type E: ElectionCore<Self> + Clone;

    type REP: ReplicationCore<Self>;

    type C: CommitHandler;

    type SMH: StateMachineHandler<Self> + Debug;
}

pub mod alias {
    use super::TypeConfig;

    pub type ROF<T> = <T as TypeConfig>::R;

    pub type TROF<T> = <T as TypeConfig>::TR;

    pub type SMOF<T> = <T as TypeConfig>::SM;

    pub type SSOF<T> = <T as TypeConfig>::SS;

    pub type MOF<T> = <T as TypeConfig>::M;

    pub type EOF<T> = <T as TypeConfig>::E;

    pub type REPOF<T> = <T as TypeConfig>::REP;

    pub type POF<T> = <T as TypeConfig>::P;

    pub type COF<T> = <T as TypeConfig>::C;

    pub type SMHOF<T> = <T as TypeConfig>::SMH;
}
