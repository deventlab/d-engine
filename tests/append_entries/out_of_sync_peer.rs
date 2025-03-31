// #[tokio::test]
// async fn test_out_of_sync_peer_scenario() {
//     // 1. Start a 3-node cluster
//     let nodes = Cluster::start(3).await;

//     // 2. Artificially create inconsistent states
//     nodes[0].manipulate_log(vec![1, 2, 3]); // Peer1
//     nodes[1].manipulate_log(vec![1, 2, 3, 4]); // Peer2
//     nodes[2].manipulate_log(vec![1, 2, 3, 4]); // Leader

//     // 3. Trigger client request
//     let client = Client::connect(nodes[2].address());
//     client.send(Command::Insert).await.unwrap();

//     // 4. Verify global state
//     tokio::time::sleep(Duration::from_secs(1)).await; // Wait for synchronization
//     assert_log_consistent(&nodes); // Custom consistency check
//     assert_eq!(nodes[2].commit_index(), 5);
// }
