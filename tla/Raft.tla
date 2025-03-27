-------------------------------- MODULE Raft --------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANT NODES
CONSTANT NULL, VALUES
CONSTANT Leader, Candidate, Follower, Died
CONSTANT Election_Max_Rounds, Append_Max_Rounds, Exceptions_Max
CONSTANT VoteRequest, VoteResponse, AppendEntriesRequest, AppendEntriesResponse, Heartbeat

ASSUME Cardinality(NODES) >= 3

VARIABLES nodeState

VARIABLES messages
VARIABLES localLogs
StorageVars == <<messages, localLogs>>

VARIABLES nodeLeaders, currentTerms
NodeVars == <<nodeLeaders, currentTerms>>

VARIABLES receivedVotes, nodeVotedTerms
CandidateVars == <<receivedVotes, nodeVotedTerms>>

VARIABLES matchIndex, nextIndex, commitIndex
LeaderVars == <<nextIndex, matchIndex, commitIndex>>

VARIABLES electionControl, clientReqControl
SystemControlVars == <<electionControl, clientReqControl>>

VARIABLES debug, exceptions
DebugVars == <<debug, exceptions>>

VARIABLES values
ValueVars == <<values>>

Vars == <<nodeState, StorageVars, 
    NodeVars, CandidateVars, LeaderVars, SystemControlVars, DebugVars, ValueVars>>
---------------------------------------

MajorityNodeNum == Cardinality(NODES) \div 2 + 1
          
TypeOK ==
    \* “every function where the domain is S and all of the values are in T.”
  /\ nodeState \in [NODES -> {Follower, Candidate, Leader, Died}] 
  /\ MajorityNodeNum > 0
               
Init == 
        /\ nodeState = [n \in NODES |-> Follower]
        /\ nodeLeaders = [n \in NODES |-> NULL]
        /\ currentTerms = [n \in NODES |-> 1]
        /\ receivedVotes = [n \in NODES |-> 0]
        /\ nodeVotedTerms = [n \in NODES |-> [j \in NODES |-> [term |-> 0, voted |-> FALSE]]]
        /\ messages = [m \in {} |-> 0]
        
        /\ localLogs = [n \in NODES |-> <<>> ] 
        /\ commitIndex = [n \in NODES |-> 0 ]
        /\ matchIndex = [i \in NODES |-> [j \in NODES |-> 0] ]
        /\ nextIndex = [i \in NODES |-> [j \in NODES |-> 0] ]
        
        /\ debug = <<>>
        /\ electionControl = 0
        /\ clientReqControl = 0
        /\ exceptions = 0
        /\ values = <<"c1","c2">>

(***************************************************************************
    Utils
 ***************************************************************************)
NoLeader == \A key \in DOMAIN nodeState: nodeState[key] # Leader

RandomNumber(p) == IF p >= 0 THEN RandomElement(0..p) ELSE 0

Min(set) == CHOOSE x \in set: \A y \in set: x <= y
\*Max(a, b) == IF a > b THEN a ELSE b
Max(set) == CHOOSE x \in set: \A y \in set: x >= y 

DoNothing == /\ UNCHANGED Vars

LastTerm(index) == IF Len(localLogs[index]) = 0 THEN 0 ELSE localLogs[index][Len(localLogs[index])].term
LastLogIndex(index) == Len(localLogs[index])
    
MessageLost == 
    /\ exceptions < Exceptions_Max
    /\ exceptions' = exceptions + 1
    /\ \E m \in DOMAIN messages:
         /\ messages[m] > 0
            /\ messages' = [messages EXCEPT ![m] = 0]
    /\ UNCHANGED <<nodeState, localLogs, debug, NodeVars, CandidateVars, LeaderVars, SystemControlVars, ValueVars>> 
        
Send(req, res) == 
    /\ res \notin DOMAIN messages
    /\ messages' = [messages EXCEPT ![req] = @ - 1]  @@ (res :> 1) 

SendMulti(msgs) == 
    /\ \A m \in msgs: m \notin DOMAIN messages
    /\ messages' = messages @@ [m \in msgs |-> 1] 
(***************************************************************************)
(* Node restart means all memory lost of this node                         *)
(***************************************************************************)     
NodeRestart(n) == 
            /\ receivedVotes' = [receivedVotes EXCEPT ![n] = 0]
            /\ currentTerms' = [currentTerms EXCEPT ![n] = 0]
            /\ nodeVotedTerms' = [nodeVotedTerms EXCEPT ![n] = [j \in NODES |->[term |-> 0, voted |-> FALSE]]]
            /\ commitIndex' = [commitIndex EXCEPT ![n] = 0 ]
            /\ localLogs'= [localLogs EXCEPT ![n] = <<>>]
            /\ nodeLeaders' = [nodeLeaders EXCEPT ![n] = NULL]
            /\ matchIndex' = [matchIndex EXCEPT ![n] = [j \in NODES |-> 0]]
            /\ nextIndex' = [nextIndex EXCEPT ![n] = [j \in NODES |-> 0]]

(****************************************************************************
To simulate some node dies inside network.
But it doesn't make sense to simulate majority node dies.
When servers start up, they begin as followers.
****************************************************************************)
NodeDies == 
        /\ exceptions < Exceptions_Max
        /\ exceptions' = exceptions + 1
        /\ Cardinality({key \in DOMAIN nodeState: nodeState[key] # Died}) > MajorityNodeNum
        /\ \E x \in NODES: 
            /\ nodeState[x] = Candidate 
            /\ NodeRestart(x)
            /\ nodeState' = [nodeState EXCEPT ![x] = Died]   
        /\ UNCHANGED <<messages, debug, SystemControlVars, ValueVars>>
(***************************************************************************)
(* To simulate some node recovers which dies before                        *)
(***************************************************************************)
NodeRecovers ==
        /\ \E x \in NODES:
            /\ nodeState[x] = Died 
            /\ nodeState' = [nodeState EXCEPT ![x] = Follower]
            /\ NodeRestart(x)
        /\ UNCHANGED <<messages, DebugVars, SystemControlVars, ValueVars>>

(***************************************************************************
Check if 'node' has voted for this node or not.

If my term is bigger than to be voted term, then ignore this vote
if I have voted for this term, then ignore this vote 
1. init voteTerm[A] = <<0, 0>>, Node A's currentTerm is 0
1.1. node=A, myTerm=0, toBeVotedTerm=0,nodeVotedTerms[A] = <0,0> : 
     When I vote for myself,  <= 0+1(vote for myself)>, A should continue vote for itself
1.2. node=A, myTerm=1, toBeVotedTerm=1,nodeVotedTerms[A] = <1,1> : 
     When I try to vote for myself again, Node A's currentTerm is 1,
        the voteTerm[A] = <1,1>> will stop my vote
--
When node B try to vote for Node A:
1. node B's term is 3:
1.1 node=A, myTerm=3, toBeVotedTerm=1, nodeVotedTerms[B] = <0,0> :
    if it receives node A's term is 1(toBeVotedTerm=1), it should not vote for A.
1.2 node=A, myTerm=3, toBeVotedTerm=3, nodeVotedTerms[B] = <0,0> :    
    if it receives A's vote is 3(toBeVotedTerm=3), and voteTerms[B] = <<0,0>>, then it should vote
1.3 node=A, myTerm=3, toBeVotedTerm=4, nodeVotedTerms[B] = <3,1> :    
    if it receives A's vote is 4(toBeVotedTerm=4), and voteTerms[B] = <<3,1>>, then it should vote
1.4 node=A, myTerm=3, toBeVotedTerm=3, nodeVotedTerms[B] = <3,1> :    
    if it receives A's vote is 3, and voteTerms[B] = <<3,1>>, then it should not vote

 ***************************************************************************)
CheckIfShouldVote(node,to, toBeVotedTerm, myTerm) ==
    IF toBeVotedTerm < myTerm THEN FALSE 
    ELSE  
        LET vote == nodeVotedTerms[node][to] IN
            IF toBeVotedTerm > vote.term \/ (/\ toBeVotedTerm = vote.term /\ vote.voted = FALSE) 
                THEN TRUE
                ELSE FALSE 

            
RequestVote(n) == LET 
    CheckMaxRounds == /\ electionControl < Election_Max_Rounds
                      /\ electionControl' = electionControl + 1
   IN                      
   \/ 
      /\ nodeLeaders[n] = NULL \* in my thread, I have detected Leader yet.
      /\ CheckMaxRounds
      /\ nodeState[n] = Follower
      /\ currentTerms' = [currentTerms EXCEPT ![n] = (@+1)]
      /\ nodeState' = [nodeState EXCEPT ![n] = Candidate]
      /\ UNCHANGED <<clientReqControl, StorageVars, 
            nodeLeaders, CandidateVars, LeaderVars, DebugVars, ValueVars>>
   \/ 
      /\ nodeLeaders[n] = NULL \* in my thread, I have detected Leader yet.
      /\ CheckMaxRounds 
      /\ nodeState[n] = Candidate
      /\ CheckIfShouldVote(n, n, currentTerms[n], currentTerms[n])
      /\ receivedVotes' = [receivedVotes EXCEPT ![n] = (@+1)]
      /\ currentTerms' = [currentTerms EXCEPT ![n] = (@+1)] 
      /\ nodeVotedTerms' = [nodeVotedTerms EXCEPT ![n][n] =[term |-> currentTerms[n]+1, voted |-> TRUE] ]
      /\ SendMulti({
            [
                mfrom |-> n,
                mto |-> to,
                mterm |-> currentTerms[n]+1,
                mtype |-> VoteRequest,
                mlastLogIndex |-> LastLogIndex(to),
                mlastLogTerm |-> LastTerm(to)
            ]: to \in NODES \{n} 
         })
      /\ UNCHANGED <<nodeState, nodeLeaders, clientReqControl, localLogs, LeaderVars, DebugVars, ValueVars>>  
              
ElectionStuttering == LET
        SameVotes == \E x, y, z \in DOMAIN receivedVotes: 
            /\ x # y 
            /\ y # z 
            /\ x # z 
            /\ receivedVotes[x]=receivedVotes[y]
            /\ receivedVotes[x]=receivedVotes[z]
            /\ receivedVotes[x] > 0
        AllCandidates == \A x \in DOMAIN nodeState: nodeState[x] = Candidate
         
        Stuttering == /\ NoLeader /\ SameVotes /\ AllCandidates
    IN
    /\ Stuttering
    /\ \E i \in NODES:
        /\ currentTerms' = [currentTerms EXCEPT ![i] = @+1] 
        /\ nodeVotedTerms' = [nodeVotedTerms EXCEPT ![i] = [j \in NODES |-> [term |-> 0, voted |-> FALSE]]]
        /\ receivedVotes' = [receivedVotes EXCEPT ![i] = 0]
        /\ nodeState' = [nodeState EXCEPT ![i] = Follower]
    /\ UNCHANGED <<nodeLeaders, StorageVars, LeaderVars, SystemControlVars, DebugVars, ValueVars>>
       
HandleRequestVoteRequest == 
    \E m \in DOMAIN messages : 
       LET from == m.mfrom
            to == m.mto
\*              If the candidate’s log is at least as up-to-date as 
\*                  any other log in that majority (where “up-to-date” is defined precisely below), 
\*                  then it will hold all the committed entries. 
\*             Raft determines which of two logs is more up-to-date by comparing the index 
\*                  and term of the last entries in the logs. If the logs have last entries with different terms, 
\*                  then the log with the later term is more up-to-date. 
\*                  If the logs end with the same term, then whichever log is longer is more up-to-date.
            prevLogOk == 
                 \/ m.mlastLogTerm > LastTerm(to)
                 \/ /\ m.mlastLogTerm = LastTerm(to)
                    /\ m.mlastLogIndex >= Len(localLogs[to])
       IN 
       /\ messages[m] > 0
       /\ m.mtype = VoteRequest
       /\ nodeState[to] # Died
       /\ nodeLeaders[to] = NULL \* in my thread, I have detected Leader yet.
       /\ nodeState[to] \in {Candidate, Follower}
       /\ prevLogOk
       /\ CheckIfShouldVote(to, from, m.mterm, currentTerms[to])
       /\ nodeVotedTerms' = [nodeVotedTerms EXCEPT ![to][from] = [term |-> m.mterm, voted |-> TRUE]]
       /\ currentTerms' = [currentTerms EXCEPT ![to] = m.mterm]
       /\ Send(m, [
                 mfrom |-> to,
                 mto |-> from,
                 mterm |-> currentTerms[to],
                 mtype |-> VoteResponse,
                 mlastLogIndex |-> LastLogIndex(to),
                 mlastLogTerm |-> LastTerm(to)
                 ]) 
       /\ UNCHANGED <<nodeState, receivedVotes, nodeLeaders,  localLogs, LeaderVars, SystemControlVars, DebugVars, ValueVars>>


HandleRequestVoteResponse == 
    \E m \in DOMAIN messages : 
        LET from == m.mfrom
            to == m.mto
        IN 
        /\ messages[m] > 0
        /\ m.mtype = VoteResponse
        /\ nodeState[to] # Died
        /\ nodeLeaders[to] = NULL \* in my thread, I have detected Leader yet.
        /\ nodeState[to] = Candidate
        /\ receivedVotes' = [receivedVotes EXCEPT ![m.mto] = (@+1)]
        /\ messages' = [messages EXCEPT ![m] = @ - 1]
        /\ UNCHANGED <<nodeState, nodeVotedTerms, localLogs, NodeVars, LeaderVars, SystemControlVars, DebugVars, ValueVars>>
                                                                               
CheckIfReceivedMajorityVotes(n) == 
   /\ nodeState[n] = Candidate 
   /\ nodeLeaders[n] = NULL
   /\ receivedVotes[n] >= MajorityNodeNum
   /\ nodeState' = [nodeState EXCEPT ![n] = Leader]
   /\ nodeLeaders' = [nodeLeaders EXCEPT ![n] = n]
   /\ currentTerms' = currentTerms
   /\ commitIndex' = commitIndex
   /\ matchIndex' = matchIndex
   /\ nextIndex' = [nextIndex EXCEPT ![n] = [j \in NODES |-> Len(localLogs[j]) + 1]]
   /\ UNCHANGED <<StorageVars, CandidateVars, SystemControlVars, DebugVars, ValueVars>>

SendHeartbeat(n) == LET
        PrevLogIndex(nodeIndex) == IF commitIndex[nodeIndex] > 0 
                THEN commitIndex[nodeIndex] - 1 ELSE 0
        PrevLogTerm(nodeIndex) == LET index == PrevLogIndex(nodeIndex) IN
                IF index > 0 THEN localLogs[nodeIndex][index].term ELSE 0
    IN
    /\ nodeState[n] = Leader
    /\ clientReqControl <= Append_Max_Rounds
    /\ clientReqControl' = clientReqControl + 1
    /\ SendMulti({
        [
            mfrom |-> n,
            mto |-> to,
            mterm |-> currentTerms[n],
            mtype |-> Heartbeat,
            mleaderId |-> n,
            mprevLogIndex |-> PrevLogIndex(n),
            mprevLogTerm |-> PrevLogTerm(n),
            mentries |-> <<>>,
            mleaderCommit |-> commitIndex[n]
        ]: to \in NODES \{n} 
     })
     /\ UNCHANGED <<nodeState, localLogs, electionControl, NodeVars, CandidateVars, LeaderVars, DebugVars, ValueVars>>


HandleClientRequest(n) == LET 
    v == Head(values)
    entry == [term |-> currentTerms[n], command |-> v ]
    IN
    /\ Len(values) > 0
    /\ nodeState[n] = Leader
    /\ localLogs' = [localLogs EXCEPT ![n] = Append(localLogs[n], entry) ]
    /\ messages' = messages
    /\ values' = Tail(values)
    /\ UNCHANGED <<nodeState, NodeVars, CandidateVars, LeaderVars, SystemControlVars, DebugVars>>
                                             
\* ----------------- LEADER ACTIONS -----------------
\* Simulate leader append entries RPC requests   
DistributeAppendEntriesRequests(n) == LET
        PrevLogIndex(j) == nextIndex[n][j] - 1
        PrevLogTerm(j) == LET index == PrevLogIndex(j) IN
                IF index > 0 THEN localLogs[j][index].term ELSE 0
    
        lastEntry(i) == Min({Len(localLogs[n]), nextIndex[n][i]})
        \* i stands for node i, send log one by one.
        logEntries(i) == SubSeq(localLogs[n], nextIndex[n][i], lastEntry(i))  
    IN
        /\ Len(localLogs[n]) > 0
        /\ nodeState[n] = Leader
        /\ clientReqControl <= Append_Max_Rounds
        /\ clientReqControl' = clientReqControl + 1
        /\ SendMulti({
                [
                    mfrom |-> n,
                    mto |-> to,
                    mterm |-> currentTerms[n],
                    mtype |-> AppendEntriesRequest,
                    mleaderId |-> n,
                    mprevLogIndex |-> PrevLogIndex(to),
                    mprevLogTerm |-> PrevLogTerm(to),
                    mentries |-> logEntries(to),
                    mleaderCommit |-> Min({commitIndex[n], lastEntry(to)})
                ]: to \in { t \in NODES\ {n}: Len(logEntries(t)) > 0} 
             })                 
        /\ localLogs' = localLogs             
        /\ UNCHANGED <<nodeState, electionControl, NodeVars, CandidateVars, LeaderVars, DebugVars, ValueVars>>                                         

ConfirmLeaderCommitIndex(n) ==
    LET
        \* for any log i, if majority server accepts the requests 
        AgreedServersForLogIndex(i) == {n} \cup {x \in NODES: matchIndex[n][x] >= i}
        agreeIndexes == {i \in 1..Len(localLogs[n]): Cardinality(AgreedServersForLogIndex(i)) >= MajorityNodeNum}
        newCommitIndex == Max(agreeIndexes)
    IN
    /\ nodeState[n] = Leader
    /\ Len(localLogs[n]) > 0
    /\ Cardinality(agreeIndexes) > 0
    /\ commitIndex[n] < newCommitIndex
\*    /\ debug' = Append(debug, newCommitIndex)
    /\ commitIndex' = [commitIndex EXCEPT ![n] = newCommitIndex]
    /\ UNCHANGED <<nodeState, nextIndex, matchIndex, StorageVars, NodeVars, CandidateVars, SystemControlVars, DebugVars, ValueVars>>

\* Any RPC with a newer term causes the recipient to advance its term first.
\*UpdateTerm(n) == LET 
\*            m == Head(appendEntriesReqMQ[n]) IN
\*    /\ appendEntriesReqMQ[n] # <<>> 
\*    /\ m.mterm > currentTerms[n]
\*    /\ currentTerms'    = [currentTerms EXCEPT ![n] = m.mterm]
\*    /\ nodeState'      = [nodeState   EXCEPT ![n] = Follower]
\*    /\ nodeVotedTerms' = [nodeVotedTerms    EXCEPT ![n] = <<0, 0>>]
\*    /\ nodeLeaders' = [nodeLeaders EXCEPT ![n] = m.mleaderId] \* update follower leader Id
\*       \* messages is unchanged so m can be processed further.
\*    /\ UNCHANGED <<receivedVotes, voteReqMQ,
\*                    Phase2Vars, SystemVars, debug>>

\* ----------------- FOLLOWER ACTIONS -----------------      
\* If an existing entry conflicts with a new one (same index but different terms), 
\*    delete the existing entry and all that follow it (§5.3)
HandleHeartbeat == \E m \in DOMAIN messages : 
    LET from == m.mfrom
        to == m.mto
    IN 
        /\ messages[m] > 0
        /\ m.mtype = Heartbeat
        /\ nodeState[to] \notin {Died, Leader}
        /\ nodeState'       = [nodeState         EXCEPT ![m.mto] = Follower]
        /\ nodeVotedTerms'  = [nodeVotedTerms    EXCEPT ![m.mto] = [j \in NODES |->[term |-> 0, voted |-> FALSE]]]
        /\ nodeLeaders'     = [nodeLeaders       EXCEPT ![m.mto] = m.mfrom]
        /\ receivedVotes'   = [receivedVotes     EXCEPT ![m.mto] = 0]
        /\ messages' = [messages EXCEPT ![m] = @ - 1]    
        /\ UNCHANGED <<currentTerms, localLogs, LeaderVars, SystemControlVars, DebugVars, ValueVars>>
    
            
HandleAppendEntriesRequest == 
\E m \in DOMAIN messages : 
    LET from == m.mfrom
        to == m.mto
        PrevLogIndex(nodeIndex) == IF commitIndex[nodeIndex] > 0 
            THEN commitIndex[nodeIndex] - 1 ELSE 0
        PrevLogTerm(nodeIndex) == LET index == PrevLogIndex(nodeIndex) IN
            IF index > 0 THEN localLogs[nodeIndex][index].term ELSE 0
        
        prevLogOk == \/ m.mprevLogIndex = 0
                     \/ /\ m.mprevLogIndex > 0
                        /\ m.mprevLogIndex <= Len(localLogs[to]) 
                        /\ m.mprevLogTerm = localLogs[to][m.mprevLogIndex].term \* "<" won't exist, ">" might exist
        mIndex == m.mprevLogIndex + Len(m.mentries)
        leaderToBeCommitIndex == m.mprevLogIndex + 1 \* current log index
        filterOutConflicts(logs) == LET
                filteredIndices == {i \in DOMAIN logs: 
                    \/ i < leaderToBeCommitIndex
                    \/ /\ i = leaderToBeCommitIndex 
                       /\ logs[i].term = m.mterm }
            IN
            [i \in filteredIndices |-> logs[i]]
    IN 
    /\ messages[m] > 0
    /\ m.mtype = AppendEntriesRequest
    /\ nodeState[to] \notin {Died, Leader}
    /\ currentTerms[to] >= m.mterm
    /\ \/ /\ \/ m.mterm < currentTerms[to] \/ ~prevLogOk  
          /\ Send(m,
                [
                    mfrom |-> to,
                    mto |-> from,
                    mterm |-> currentTerms[to],
                    msuccess |-> FALSE,
                    mmatchIndex |-> 0,
                    mtype |-> AppendEntriesResponse,
                    mprevLogIndex |-> PrevLogIndex(to),
                    mprevLogTerm |-> PrevLogTerm(to)
                ] 
             )
          /\ debug' = Append(debug, "~prevLogOk")
          
          /\ UNCHANGED <<nodeState, localLogs, exceptions,
                NodeVars, CandidateVars, LeaderVars, SystemControlVars, ValueVars>>
\*       \/ \* already append the new entries
\*          /\ prevLogOk 
\*          /\ nodeState[to] \in {Follower, Candidate}
\*          /\ Len(m.mentries) > 0
\*          /\ Len(localLogs[to]) >= leaderToBeCommitIndex
\*          /\ localLogs[to][leaderToBeCommitIndex].term = m.mterm
\*          /\ commitIndex' = [commitIndex EXCEPT ![to] = m.mleaderCommit]
\*          /\ nodeLeaders' = [nodeLeaders EXCEPT ![to] = from]
\*          /\ nodeState' = [nodeState EXCEPT ![to] = Follower] 
\*          /\ Send(m,
\*                [
\*                    mfrom |-> to,
\*                    mto |-> from,
\*                    mterm |-> currentTerms[to],
\*                    msuccess |-> TRUE,
\*                    mmatchIndex |-> mIndex,
\*                    mtype |-> AppendEntriesResponse,
\*                    mprevLogIndex |-> PrevLogIndex(to),
\*                    mprevLogTerm |-> PrevLogTerm(to)
\*                ] 
\*             )
\*          /\ debug' = Append(debug, "append")
\*          /\ UNCHANGED <<localLogs, nextIndex, matchIndex, exceptions,
\*                currentTerms, CandidateVars, SystemControlVars, ValueVars>>
       \/ \* normal operation
         /\ prevLogOk 
         /\ nodeState[to] \in {Follower, Candidate}
         /\ m.mterm = currentTerms[to] 
         /\ Len(localLogs[to]) < leaderToBeCommitIndex 
         /\ Len(m.mentries) > 0
         /\ localLogs' = [localLogs EXCEPT ![to] = Append(localLogs[to] , m.mentries[1])]
         /\ debug' = Append(debug, "normal")  
         /\ nodeLeaders' = [nodeLeaders EXCEPT ![to] = from]
         /\ nodeState' = [nodeState EXCEPT ![to] = Follower] 
         /\ commitIndex' = [commitIndex EXCEPT ![to] = m.mleaderCommit]
         /\ Send(m,
                [
                    mfrom |-> to,
                    mto |-> from,
                    mterm |-> currentTerms[to],
                    msuccess |-> TRUE,
                    mmatchIndex |-> mIndex,
                    mtype |-> AppendEntriesResponse,
                    mprevLogIndex |-> PrevLogIndex(to),
                    mprevLogTerm |-> PrevLogTerm(to)
                ] 
             )  
         /\ UNCHANGED <<currentTerms, nextIndex, matchIndex, CandidateVars, SystemControlVars, ValueVars, exceptions>> 
       \/ \* conflicts
         /\ prevLogOk 
         /\ nodeState[to] \in {Follower, Candidate}
         /\ Len(localLogs[to]) >= leaderToBeCommitIndex
         /\ \E logIndex \in DOMAIN localLogs[to]: /\ logIndex = leaderToBeCommitIndex /\ localLogs[to][logIndex].term # m.mterm \* there is conflict.
         /\ localLogs' = [localLogs EXCEPT ![to] = filterOutConflicts(localLogs[to])]
         /\ debug' = Append(debug, "conflict")
\*         /\ messages' = [messages EXCEPT ![m] = @ - 1] 
         /\ nodeLeaders' = [nodeLeaders EXCEPT ![to] = from]
         /\ nodeState' = [nodeState EXCEPT ![to] = Follower] 
         /\ UNCHANGED <<exceptions, messages,
                currentTerms, CandidateVars, LeaderVars, SystemControlVars, ValueVars>> 

    
(***************************************************************************
Listening sending append entries request result.

CRITERIAs:
1. the node itself is Leader which means it is waiting for commitIndex
2. commitIndex is not empty
 ***************************************************************************)
HandleAppendEntriesResponse == \E m \in DOMAIN messages : 
    LET from == m.mfrom
        to == m.mto
    IN 
    /\ messages[m] > 0
    /\ m.mtype = AppendEntriesResponse
    /\ nodeState[to] # Died
    /\ nodeState[to] = Leader
    /\ m.mterm = currentTerms[to]
    /\ IF  m.msuccess THEN
          /\ nextIndex'  = [nextIndex EXCEPT ![to][from] = m.mmatchIndex+1]
          /\ matchIndex' = [matchIndex EXCEPT ![to][from] = m.mmatchIndex] 
          /\ debug' = Append(debug, "success")                     
       ELSE \* case b,e,f in Figure 7
          /\ \lnot m.msuccess
          /\ nextIndex' = [nextIndex EXCEPT ![to][from] =
                               Max({nextIndex[to][from] - 1, 1})]
          /\ matchIndex' = matchIndex
          /\ debug' = Append(debug, "fail")
    /\ messages' = [messages EXCEPT ![m] = @ - 1]          
    /\ UNCHANGED <<nodeState, commitIndex, localLogs, exceptions,
                NodeVars, CandidateVars, SystemControlVars, ValueVars>> 
        
\* Current terms are exchanged whenever servers communicate; 
\*  if one server’s current term is smaller than the other’s, 
\*  then it updates its current term to the larger value. 
UpdateTerm ==
    \E m \in DOMAIN messages :
        /\ messages[m] > 0
        /\ m.mtype \in {AppendEntriesRequest, Heartbeat}
        /\ m.mterm > currentTerms[m.mto]
        /\ currentTerms'    = [currentTerms      EXCEPT ![m.mto] = m.mterm]
        /\ nodeState'       = [nodeState         EXCEPT ![m.mto] = Follower]
        /\ nodeVotedTerms'  = [nodeVotedTerms    EXCEPT ![m.mto] = [j \in NODES |->[term |-> 0, voted |-> FALSE]]]
        /\ nodeLeaders'     = [nodeLeaders       EXCEPT ![m.mto] = m.mfrom]
        /\ receivedVotes'   = [receivedVotes     EXCEPT ![m.mto] = 0]
        /\ UNCHANGED <<StorageVars, LeaderVars, SystemControlVars, DebugVars, ValueVars>>


\*Safety == \E n \in NODES: [](nodeState[n] \in {Follower, Candidate})
Safety == \E n \in NODES: nodeState[n] \in {Follower, Candidate, Leader}

\* INVARIANTS -------------------------

Invariant == \E n \in NODES: nodeState[n] \in {Leader, Candidate, Follower}

MinCommitIndex(s1, s2) ==
    IF commitIndex[s1] < commitIndex[s2]
    THEN commitIndex[s1]
    ELSE commitIndex[s2]

\*borrowed from: https://github.com/Vanlightly/raft-tlaplus
\* INV: NoLogDivergence
\* The log index is consistent across all servers (on those
\* servers whose commitIndex is equal or higher than the index).
NoLogDivergence ==
    \A s1, s2 \in NODES :
        IF s1 = s2
        THEN TRUE
        ELSE
            LET lowest_common_ci == MinCommitIndex(s1, s2)
            IN IF lowest_common_ci > 0
               THEN \A index \in 1..lowest_common_ci : localLogs[s1][index] = localLogs[s2][index]
               ELSE TRUE
               
CommittedEntriesReachMajority ==
    IF \E i \in NODES : nodeState[i] = Leader /\ commitIndex[i] > 0
    THEN \E i \in NODES :
           /\ nodeState[i] = Leader
           /\ commitIndex[i] > 0
           /\ \E quorum \in SUBSET NODES :
               /\ Cardinality(quorum) = (Cardinality(NODES) \div 2) + 1
               /\ i \in quorum
               /\ \A j \in quorum :
                   /\ Len(localLogs[j]) >= commitIndex[i]
                   /\ localLogs[j][commitIndex[i]] = localLogs[i][commitIndex[i]]
    ELSE TRUE

    
    
thread(n) ==
            \/ RequestVote(n)
            \/ SendHeartbeat(n)
            \/ CheckIfReceivedMajorityVotes(n)
            \/ HandleClientRequest(n)
            \/ DistributeAppendEntriesRequests(n)
            \/ ConfirmLeaderCommitIndex(n)

Next == \/ \E n \in NODES: 
            \/ RequestVote(n)
\*            \/ SendHeartbeat(n)
            \/ CheckIfReceivedMajorityVotes(n)
            \/ HandleClientRequest(n)
            \/ DistributeAppendEntriesRequests(n)
            \/ ConfirmLeaderCommitIndex(n)
        \/ HandleRequestVoteRequest 
        \/ HandleRequestVoteResponse
        \/ HandleAppendEntriesRequest
        \/ HandleAppendEntriesResponse
        \/ UpdateTerm
\*        \/ ElectionStuttering
        \/ MessageLost
        \/ NodeDies
        \/ NodeRecovers

\*Fairness == 
\*    \/ \A n \in NODES: WF_Vars(thread(n))
\*    \/ WF_Vars(ElectionStuttering)          
\*\*    \/ WF_Vars(Receive)  
        
Spec == Init /\ [][Next]_Vars /\ WF_Vars(Next)

LeaderLiveness == <>(\E key \in DOMAIN nodeState: nodeState[key] = Died)
 
CommitIndexLiveness == <>(\E x \in DOMAIN nodeState:  /\ nodeState[x] = Leader /\ commitIndex[x] > 0)
Termination == <>(\E x \in DOMAIN commitIndex: commitIndex[x] > 0)  

ValueInServerLog(i, v) ==
    \E index \in DOMAIN localLogs[i] :
        localLogs[i][index].command = v

ValueAllOrNothing(v) ==
    IF /\ electionControl = Election_Max_Rounds-1
       /\ ~\E i \in NODES : nodeState[i] = Leader
    THEN TRUE
    ELSE \/ \A i \in NODES : ValueInServerLog(i, v)
         \/ ~\E i \in NODES : ValueInServerLog(i, v)

\* "[]<>" 表示在未来的某个时间点，性质会成立至少一次。换句话说，它表示全局性质的存在性。
ValuesNotStuck ==
    \A v \in VALUES : []<>ValueAllOrNothing(v)
    
-------------------------
\* ------------- Safety ---------------------
\* If two entries in different logs have the same index and term, 
\*  then they store the same command.
\* If two entries in different logs have the same index and term, 
\*  then the logs are identical in all preceding entries.
LogSafety == <>(\E x, y \in NODES:
    /\ x # y
    /\ Len(localLogs[x]) > 0
    /\ Len(localLogs[y]) > 0
    /\ \E i \in 1..Len(localLogs[x]):
           \E j \in 1..Len(localLogs[y]):
              IF
                /\ i = j
                /\ localLogs[x][i].term = localLogs[y][i].term
                /\ localLogs[x][i].command = localLogs[y][i].command
              THEN TRUE
              ELSE FALSE )

====
\* Modification History
\* Last modified Thu Mar 27 16:42:51 CST 2025 by joshua
\* Created Sat Mar 16 15:43:42 CST 2024 by joshua