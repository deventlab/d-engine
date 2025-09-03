# Raft Roles and Leader Election: Responsibilities and Step-Down Conditions

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
  - Default initial role for new nodes joining the cluster.
  - Receive initial cluster state via snapshot transfer.
- **Log Synchronization**:
  - Replicate logs but without voting rights.
  - Eligible for promotion to follower once caught up.
- **Failure Adaptation**:
  - Followers with excessive log lag (beyond max_lag_threshold) demote themselves to learners.
  - Learners auto-promote back to follower when their match_index reaches a configurable threshold.
- **Snapshot Protocol**:
  - Leaders send full snapshots triggered by LearnerEvent.
  - Track replication progress separately for learners.

## **Conditions That Cause a Leader to Step Down**

A leader must relinquish authority and revert to follower state under these conditions:

1. **Receives an RPC with a Higher Term**

   If the leader receives any RPC (AppendEntries, RequestVote, etc.) with a term greater than its current term, it must step down immediately.

2. **Grants a Vote to Another Candidate**

   If the leader grants a vote to another candidate (usually due to the candidate having a more up-to-date log), it must step down.

3. **Fails to Contact the Majority for Too Long**

   If the leader cannot successfully reach a quorum of nodes for a duration longer than one election timeout, it must assume it has lost authority and step down.
