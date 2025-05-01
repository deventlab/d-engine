use super::RaftEvent;

impl RaftEvent {
    pub fn to_code(&self) -> i32 {
        match self {
            RaftEvent::ReceiveVoteRequest(_, _) => 3,
            RaftEvent::ClusterConf(_, _) => 5,
            RaftEvent::ClusterConfUpdate(_, _) => 6,
            RaftEvent::AppendEntries(_, _) => 7,
            RaftEvent::ClientPropose(_, _) => 8,
            RaftEvent::ClientReadRequest(_, _) => 9,
            RaftEvent::InstallSnapshotChunk(_, _) => 10,
        }
    }
}
