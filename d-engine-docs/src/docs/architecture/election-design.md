## Conditions That Cause a Leader to Step Down

A leader will step down in the following situations:

1. Receives an RPC with a Higher Term
   If the leader receives an AppendEntries, RequestVote, or other RPC with a term number greater than its current term, it must step down and revert to a follower.
2. Grants a Vote to Another Candidate
   If the leader receives a RequestVote RPC and grants the vote to another candidate (usually because the candidate has a more up-to-date log), the leader must step down.
3. Fails to Contact the Majority for Too Long
   If the leader cannot reach a quorum (majority of nodes) for longer than one election timeout, it must assume it no longer holds authority and step down.
