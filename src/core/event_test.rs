use super::RaftEvent;
use crate::grpc::rpc_service::AppendEntriesRequest;
use crate::grpc::rpc_service::ClientProposeRequest;
use crate::grpc::rpc_service::ClientReadRequest;
use crate::grpc::rpc_service::ClusteMembershipChangeRequest;
use crate::grpc::rpc_service::MetadataRequest;
use crate::grpc::rpc_service::VoteRequest;
use crate::MaybeCloneOneshot;
use crate::RaftOneshot;
impl RaftEvent {
    pub fn to_code(&self) -> i32 {
        match self {
            RaftEvent::ReceiveVoteRequest(_, _) => 3,
            RaftEvent::ClusterConf(_, _) => 5,
            RaftEvent::ClusterConfUpdate(_, _) => 6,
            RaftEvent::AppendEntries(_, _) => 7,
            RaftEvent::ClientPropose(_, _) => 8,
            RaftEvent::ClientReadRequest(_, _) => 9,
        }
    }

    pub fn mock_from_code(code: i32) -> Option<Self> {
        match code {
            3 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::ReceiveVoteRequest(VoteRequest::default(), dummy_tx))
            }
            5 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::ClusterConf(MetadataRequest::default(), dummy_tx))
            }
            6 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::ClusterConfUpdate(
                    ClusteMembershipChangeRequest::default(),
                    dummy_tx,
                ))
            }
            7 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::AppendEntries(AppendEntriesRequest::default(), dummy_tx))
            }
            8 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::ClientPropose(ClientProposeRequest::default(), dummy_tx))
            }
            9 => {
                let (dummy_tx, _) = MaybeCloneOneshot::new();
                Some(RaftEvent::ClientReadRequest(ClientReadRequest::default(), dummy_tx))
            }

            _ => None,
        }
    }
}
