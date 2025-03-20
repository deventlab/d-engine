mod default_state_machine_handler;
pub use default_state_machine_handler::*;

#[cfg(test)]
mod default_state_machine_handler_test;

//----------------------------------------
use crate::{
    alias::ROF,
    grpc::rpc_service::{ClientCommand, ClientResult},
    Result, TypeConfig,
};
use std::sync::Arc;
use tonic::async_trait;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
#[async_trait]
pub trait StateMachineHandler<T>: Send + Sync + 'static
where
    T: TypeConfig,
{
    fn update_pending(&self, new_commit: u64);
    async fn apply_batch(&self, raft_log: Arc<ROF<T>>) -> Result<()>;

    fn read_from_state_machine(
        &self,
        client_command: Vec<ClientCommand>,
    ) -> Option<Vec<ClientResult>>;
}
