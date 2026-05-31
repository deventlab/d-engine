/// Fast-path tests for `handle_client_read` via `ReadHandle`.
///
/// Routing rules under test:
///   Eventual/LeaseRead (any key count) → ReadActor fast path; fallback on LeaseInvalid/SmStopped
///   LinearizableRead                   → cmd_tx always
///   SmError                            → error returned directly (no cmd_tx fallback)
///
/// Proof technique (identical to read_handle_test.rs):
///   The ReadHandle's cmd_rx is dropped. Any fallback to cmd_tx produces a tonic
///   Status error (channel closed). An Ok response with cmd_rx dropped proves the
///   fast path was taken.
#[cfg(test)]
mod grpc_fast_path_tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::read_actor::run_read_actor;
    use bytes::Bytes;
    use d_engine_core::{MockStateMachine, ReadLease, now_ms};
    use d_engine_proto::client::ClientReadRequest;
    use d_engine_proto::client::ReadConsistencyPolicy as ProtoPolicy;
    use d_engine_proto::client::client_response::SuccessResult;
    use d_engine_proto::client::raft_client_service_server::RaftClientService;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tonic::Request;

    use crate::Node;
    use crate::api::StandaloneReadHandle;
    use crate::test_utils::mock_node;
    use d_engine_core::MockTypeConfig;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn valid_lease() -> Arc<ReadLease> {
        let l = Arc::new(ReadLease::new());
        l.renew(1, now_ms() + 60_000);
        l
    }

    fn revoked_lease() -> Arc<ReadLease> {
        let l = Arc::new(ReadLease::new());
        l.revoke();
        l
    }

    fn sm_running_with(value: Bytes) -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(move |keys| Ok(keys.iter().map(|_| Some(value.clone())).collect()));
        sm
    }

    fn sm_stopped() -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| false);
        sm
    }

    fn sm_with_error() -> MockStateMachine {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(|_| Err(d_engine_core::Error::Fatal("disk failure".into())));
        sm
    }

    /// Build a Node wired to a live ReadActor.
    ///
    /// `drop_cmd_rx=true` closes the ReadHandle's cmd_rx — any fallback to cmd_tx
    /// returns a tonic Status error (channel closed), proving the fast path was NOT taken.
    async fn make_node(
        sm: MockStateMachine,
        lease: Arc<ReadLease>,
        drop_cmd_rx: bool,
    ) -> (Node<MockTypeConfig>, JoinHandle<()>) {
        let (read_tx, read_rx) = mpsc::channel(8);
        let (cmd_tx, cmd_rx) = mpsc::channel(1);
        if drop_cmd_rx {
            drop(cmd_rx);
        }
        let handle = tokio::spawn(run_read_actor(read_rx, lease, Arc::new(sm), 64));
        let rh = StandaloneReadHandle::new(Some(read_tx), cmd_tx);

        let (_, graceful_rx) = tokio::sync::watch::channel(());
        let mut node = mock_node("/tmp/grpc_fast_path_test", graceful_rx, None);
        node.read_handle = rh;
        node.ready.store(true, Ordering::SeqCst);
        (node, handle)
    }

    fn req(
        key: &[u8],
        policy: ProtoPolicy,
    ) -> ClientReadRequest {
        ClientReadRequest {
            client_id: 1,
            keys: vec![Bytes::copy_from_slice(key)],
            consistency_policy: Some(policy as i32),
        }
    }

    fn multi_key_req(
        keys: &[&[u8]],
        policy: ProtoPolicy,
    ) -> ClientReadRequest {
        ClientReadRequest {
            client_id: 1,
            keys: keys.iter().map(|k| Bytes::copy_from_slice(k)).collect(),
            consistency_policy: Some(policy as i32),
        }
    }

    // ── Eventual fast path ────────────────────────────────────────────────────

    /// Eventual, SM running, key exists → fast path returns Ok with the value.
    /// cmd_rx dropped: Ok proves fast path was taken, not cmd_tx.
    #[tokio::test]
    async fn test_eventual_single_key_fast_path_returns_value() {
        let (node, handle) =
            make_node(sm_running_with(Bytes::from("fast_v")), valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(req(b"k", ProtoPolicy::EventualConsistency)))
            .await;

        let resp = result.expect("fast path must succeed with cmd_rx dropped");
        let cr = resp.into_inner();
        assert_eq!(cr.error, 0, "error must be Success");
        if let Some(SuccessResult::ReadData(read_data)) = cr.success_result {
            assert_eq!(read_data.results.len(), 1);
            assert_eq!(read_data.results[0].value, Bytes::from("fast_v"));
        } else {
            panic!("expected ReadData payload");
        }
        drop(node);
        handle.await.unwrap();
    }

    /// Eventual, SM running, key missing → fast path returns Ok with empty entries.
    #[tokio::test]
    async fn test_eventual_single_key_fast_path_returns_empty_for_missing_key() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi().returning(|keys| Ok(keys.iter().map(|_| None).collect()));

        let (node, handle) = make_node(sm, valid_lease(), true).await;
        let result = node
            .handle_client_read(Request::new(req(
                b"missing",
                ProtoPolicy::EventualConsistency,
            )))
            .await;

        let resp = result.expect("fast path must succeed");
        let cr = resp.into_inner();
        assert_eq!(cr.error, 0);
        if let Some(SuccessResult::ReadData(read_data)) = cr.success_result {
            assert!(read_data.results.is_empty(), "missing key → empty results");
        } else {
            panic!("expected ReadData payload");
        }
        drop(node);
        handle.await.unwrap();
    }

    /// Eventual, SM stopped → SmStopped → fallback to cmd_tx.
    /// cmd_rx dropped: error proves fallback was triggered.
    #[tokio::test]
    async fn test_eventual_sm_stopped_falls_back_to_cmd_tx() {
        let (node, handle) = make_node(sm_stopped(), valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(req(b"k", ProtoPolicy::EventualConsistency)))
            .await;

        assert!(
            result.is_err(),
            "SM stopped must fall back to closed cmd_tx"
        );
        drop(node);
        handle.await.unwrap();
    }

    // ── LeaseRead fast path ───────────────────────────────────────────────────

    /// LeaseRead, valid lease, SM running → fast path returns Ok.
    #[tokio::test]
    async fn test_lease_read_valid_lease_fast_path_returns_ok() {
        let (node, handle) =
            make_node(sm_running_with(Bytes::from("lease_v")), valid_lease(), true).await;

        let result = node.handle_client_read(Request::new(req(b"k", ProtoPolicy::LeaseRead))).await;

        assert!(result.is_ok(), "fast path must succeed: {:?}", result.err());
        drop(node);
        handle.await.unwrap();
    }

    /// LeaseRead, revoked lease → LeaseInvalid → fallback to cmd_tx.
    #[tokio::test]
    async fn test_lease_read_revoked_falls_back_to_cmd_tx() {
        let (node, handle) =
            make_node(sm_running_with(Bytes::from("x")), revoked_lease(), true).await;

        let result = node.handle_client_read(Request::new(req(b"k", ProtoPolicy::LeaseRead))).await;

        assert!(
            result.is_err(),
            "revoked lease must fall back to closed cmd_tx"
        );
        drop(node);
        handle.await.unwrap();
    }

    /// LeaseRead, SM stopped (even valid lease) → LeaseInvalid → fallback.
    #[tokio::test]
    async fn test_lease_read_sm_stopped_falls_back_to_cmd_tx() {
        let (node, handle) = make_node(sm_stopped(), valid_lease(), true).await;

        let result = node.handle_client_read(Request::new(req(b"k", ProtoPolicy::LeaseRead))).await;

        assert!(
            result.is_err(),
            "SM stopped must fall back to closed cmd_tx"
        );
        drop(node);
        handle.await.unwrap();
    }

    // ── SmError — direct return, no cmd_tx fallback ───────────────────────────

    /// sm.get() returns Err → SmError → error returned directly without cmd_tx fallback.
    /// cmd_rx dropped: if fallback were taken, the error would come from channel-closed.
    /// Either way the result is Err, but the important invariant (no fallback) is
    /// proven at the ReadHandle layer; here we verify the error propagates to gRPC.
    #[tokio::test]
    async fn test_sm_error_propagates_as_grpc_error() {
        let (node, handle) = make_node(sm_with_error(), valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(req(b"k", ProtoPolicy::EventualConsistency)))
            .await;

        assert!(
            result.is_err(),
            "SmError must surface as tonic Status error"
        );
        drop(node);
        handle.await.unwrap();
    }

    // ── Linearizable — always cmd_tx ─────────────────────────────────────────

    /// LinearizableRead must never touch ReadActor — always routed to cmd_tx.
    /// No expect_get_multi() on SM: mockall panics if get_multi() is called (implicit safety net).
    /// cmd_rx dropped: error proves cmd_tx was used (not fast path).
    #[tokio::test]
    async fn test_linearizable_always_uses_cmd_tx() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        // no expect_get_multi() — any get_multi() call panics

        let (node, handle) = make_node(sm, valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(req(b"k", ProtoPolicy::LinearizableRead)))
            .await;

        assert!(result.is_err(), "LinearizableRead must always use cmd_tx");
        drop(node);
        handle.await.unwrap();
    }

    // ── Multi-key — ReadActor fast path ──────────────────────────────────────

    /// Multi-key Eventual read routes through ReadActor fast path (same as single-key).
    /// cmd_rx dropped: Ok proves ReadActor was used, not cmd_tx.
    #[tokio::test]
    async fn test_multi_key_eventual_fast_path_returns_values() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(|keys| Ok(keys.iter().map(|_| Some(Bytes::from_static(b"val"))).collect()));

        let (node, handle) = make_node(sm, valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(multi_key_req(
                &[b"k1", b"k2"],
                ProtoPolicy::EventualConsistency,
            )))
            .await;

        assert!(
            result.is_ok(),
            "multi-key Eventual must use ReadActor fast path: {:?}",
            result.err()
        );
        drop(node);
        handle.await.unwrap();
    }

    /// Multi-key LeaseRead with valid lease routes through ReadActor fast path.
    /// cmd_rx dropped: Ok proves ReadActor was used, not cmd_tx.
    #[tokio::test]
    async fn test_multi_key_lease_read_fast_path_returns_values() {
        let mut sm = MockStateMachine::new();
        sm.expect_is_running().returning(|| true);
        sm.expect_get_multi()
            .returning(|keys| Ok(keys.iter().map(|_| Some(Bytes::from_static(b"val"))).collect()));

        let (node, handle) = make_node(sm, valid_lease(), true).await;

        let result = node
            .handle_client_read(Request::new(multi_key_req(
                &[b"k1", b"k2"],
                ProtoPolicy::LeaseRead,
            )))
            .await;

        assert!(
            result.is_ok(),
            "multi-key LeaseRead must use ReadActor fast path: {:?}",
            result.err()
        );
        drop(node);
        handle.await.unwrap();
    }

    // ── No read_tx — always cmd_tx ────────────────────────────────────────────

    /// ReadHandle without read_tx (no ReadActor wired) falls through to cmd_tx for all policies.
    #[tokio::test]
    async fn test_no_read_tx_all_policies_use_cmd_tx() {
        let (cmd_tx, cmd_rx) = mpsc::channel::<d_engine_core::ClientCmd>(1);
        drop(cmd_rx);

        let (_, graceful_rx) = tokio::sync::watch::channel(());
        let mut node = mock_node("/tmp/grpc_fast_path_test_no_rt", graceful_rx, None);
        node.read_handle = StandaloneReadHandle::new(None, cmd_tx);
        node.ready.store(true, Ordering::SeqCst);

        for policy in [
            ProtoPolicy::EventualConsistency,
            ProtoPolicy::LeaseRead,
            ProtoPolicy::LinearizableRead,
        ] {
            let result = node.handle_client_read(Request::new(req(b"k", policy))).await;
            assert!(
                result.is_err(),
                "no read_tx must always use cmd_tx (closed → error), policy={policy:?}"
            );
        }
    }
}
