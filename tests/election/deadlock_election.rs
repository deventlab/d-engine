// #[tokio::test]
// async fn test_term_higher_but_log_stale() {
//     // Initialize cluster with 3 nodes: node1 (term=56, log=6/29), node2 (term=55, log=7/30),
// node3 (term=55).     let mut cluster = TestCluster::new(3).await;
//     cluster.set_log(1, vec![(1..=6).map(|i| LogEntry::new(29, i, b"data")).collect()]); // node1
//     cluster.set_log(2, vec![(1..=7).map(|i| LogEntry::new(30, i, b"data")).collect()]); // node2
//     cluster.set_log(3, vec![]); // node3

//     // Node1 (term=56) initiates election.
//     cluster.trigger_election(1).await;

//     // Verify node2 steps down to Follower (term=56) and rejects node1’s vote.
//     assert_eq!(cluster.node_term(2), 56);
//     let vote_response = cluster.send_vote_request(1, 2).await;
//     assert!(!vote_response.vote_granted);

//     // Simulate election timeout for node2.
//     cluster.advance_time(ELECTION_TIMEOUT + 1).await;

//     // Node2 increments term to 57 and starts a new election.
//     cluster.trigger_election(2).await;

//     // Verify node2 wins the election (logs are newer).
//     assert_eq!(cluster.node_term(2), 57);
//     assert!(cluster.is_leader(2));
// }
