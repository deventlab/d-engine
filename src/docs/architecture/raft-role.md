# Raft Role Responsibilities

## Leader

- **Heartbeat Authority**: Send periodic heartbeats to all followers/learners
- **Log Replication**: Manage log entry replication pipeline
- **Conflict Resolution**: Overwrite inconsistent follower logs
- **Membership Changes**: Process configuration changes (learner promotion/demotion)
- **Snapshot Coordination**: Trigger snapshot creation and send via InstallSnapshot RPC

## Follower

- **Request Processing**:
    - Respond to Leader heartbeats
    - Append received log entries
    - Redirect client writes to Leader
- **Election Participation**: Convert to candidate if election timeout expires
- **Log Consistency**: Maintain locally persisted log entries
- **Snapshot Handling**: Apply received snapshots and truncate obsolete logs

## Candidate

- **Election Initiation**: Start new election by incrementing term
- **Vote Solicitation**: Send RequestVote RPCs to all nodes
- **State Transition**:
    - Become Leader if receiving majority votes
    - Revert to Follower if newer term discovered

## Learner

- **Bootstrapping**:
    - Default role for new nodes joining cluster
    - Receive initial state via snapshot transfer
- **Log Synchronization**:
    - Replicate logs without voting rights
    - Can be promoted to Follower when catching up
- **Failure Adaptation**:
    - Followers with significant log lag (> `max_lag_threshold`) become Learners
    - Auto-revert to Follower when `match_index` reaches configurable threshold
- **Snapshot Protocol**:
    - Leaders send full snapshots on receiving `LearnerEvent`
    - Maintains separate replication progress tracking