use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_core::{Error, MockStateMachine, ReadLease, now_ms};
use tokio::sync::{mpsc, oneshot};

use crate::read_actor::{ReadActorError, ReadCmd, run_read_actor};

fn make_sm_running() -> MockStateMachine {
    let mut sm = MockStateMachine::new();
    sm.expect_is_running().returning(|| true);
    sm
}

fn make_sm_stopped() -> MockStateMachine {
    let mut sm = MockStateMachine::new();
    sm.expect_is_running().returning(|| false);
    sm
}

fn valid_lease() -> Arc<ReadLease> {
    let lease = Arc::new(ReadLease::new());
    lease.renew(1, now_ms() + 60_000);
    lease
}

fn revoked_lease() -> Arc<ReadLease> {
    let lease = Arc::new(ReadLease::new());
    lease.revoke();
    lease
}

fn send_read(
    tx: &mpsc::Sender<ReadCmd>,
    key: &[u8],
    consistency: ReadConsistencyPolicy,
) -> oneshot::Receiver<Result<Vec<Option<Bytes>>, ReadActorError>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.try_send(ReadCmd {
        keys: vec![Bytes::copy_from_slice(key)],
        consistency,
        reply: reply_tx,
    })
    .expect("channel should have capacity");
    reply_rx
}

fn send_read_multi(
    tx: &mpsc::Sender<ReadCmd>,
    keys: Vec<Bytes>,
    consistency: ReadConsistencyPolicy,
) -> oneshot::Receiver<Result<Vec<Option<Bytes>>, ReadActorError>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.try_send(ReadCmd {
        keys,
        consistency,
        reply: reply_tx,
    })
    .expect("channel should have capacity");
    reply_rx
}

/// Eventual read succeeds when SM is running and key exists
#[tokio::test]
async fn test_read_actor_eventual_read_returns_value() {
    let mut sm = make_sm_running();
    sm.expect_get_multi().returning(|keys| {
        Ok(keys
            .iter()
            .map(|k| {
                if &k[..] == b"k1" {
                    Some(Bytes::from_static(b"v1"))
                } else {
                    None
                }
            })
            .collect())
    });

    let lease = valid_lease();
    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, lease, Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k1", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let values = reply_rx.await.expect("reply sent").unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
    handle.await.unwrap();
}

/// Eventual read returns None for missing key
#[tokio::test]
async fn test_read_actor_eventual_read_missing_key_returns_none() {
    let mut sm = make_sm_running();
    sm.expect_get_multi().returning(|keys| Ok(keys.iter().map(|_| None).collect()));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"missing", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let values = reply_rx.await.unwrap().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], None);
    handle.await.unwrap();
}

/// Multi-key Eventual read returns all values in positional order
#[tokio::test]
async fn test_read_actor_eventual_multi_key_returns_ordered_values() {
    let mut sm = make_sm_running();
    sm.expect_get_multi().returning(|keys| {
        Ok(keys
            .iter()
            .map(|k| {
                if &k[..] == b"k1" {
                    Some(Bytes::from_static(b"v1"))
                } else {
                    None
                }
            })
            .collect())
    });

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
    let reply_rx = send_read_multi(&tx, keys, ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let values = reply_rx.await.unwrap().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
    assert_eq!(values[1], None);
    handle.await.unwrap();
}

/// Eventual read fails when SM is stopped
#[tokio::test]
async fn test_read_actor_eventual_sm_stopped_returns_error() {
    let sm = make_sm_stopped();
    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::SmStopped));
    handle.await.unwrap();
}

/// LeaseRead succeeds when lease is valid and SM is running
#[tokio::test]
async fn test_read_actor_lease_read_valid_lease_returns_value() {
    let mut sm = make_sm_running();
    sm.expect_get_multi()
        .returning(|keys| Ok(keys.iter().map(|_| Some(Bytes::from_static(b"val"))).collect()));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    assert!(reply_rx.await.unwrap().is_ok());
    handle.await.unwrap();
}

/// LeaseRead returns LeaseInvalid when lease is revoked
#[tokio::test]
async fn test_read_actor_lease_revoked_returns_lease_invalid() {
    let sm = make_sm_running();
    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, revoked_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}

/// LeaseRead returns LeaseInvalid when lease has expired (deadline in the past)
#[tokio::test]
async fn test_read_actor_lease_expired_returns_lease_invalid() {
    let sm = make_sm_running();
    let lease = Arc::new(ReadLease::new());
    lease.renew(1, now_ms().saturating_sub(1));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, lease, Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}

/// LeaseRead returns LeaseInvalid when SM is stopped (even if lease is valid)
#[tokio::test]
async fn test_read_actor_lease_valid_but_sm_stopped_returns_error() {
    let sm = make_sm_stopped();
    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}

/// ReadActor exits cleanly when all senders are dropped (lifecycle guarantee)
#[tokio::test]
async fn test_read_actor_exits_when_channel_closed() {
    let sm = make_sm_running();
    let sm_arc = Arc::new(sm);
    let sm_weak = Arc::downgrade(&sm_arc);

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), sm_arc, 64));

    drop(tx);

    handle.await.unwrap();

    assert!(
        sm_weak.upgrade().is_none(),
        "Arc<SM> should be dropped when ReadActor exits"
    );
}

/// serve_read calls sm.get_multi() — verified by explicit expect_get_multi() expectation.
///
/// Tests that only set up expect_get() rely on MockStateMachine's default-impl delegation
/// (get_multi → get). This test sets up expect_get_multi() directly to verify that
/// serve_read → sm.get_multi() is the actual call path, not just an indirect get() chain.
#[tokio::test]
async fn test_read_actor_eventual_multi_key_uses_get_multi() {
    let mut sm = make_sm_running();
    sm.expect_get_multi().returning(|keys| {
        Ok(keys
            .iter()
            .map(|k| {
                if &k[..] == b"k1" {
                    Some(Bytes::from_static(b"v1"))
                } else {
                    None
                }
            })
            .collect())
    });

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let keys = vec![Bytes::from_static(b"k1"), Bytes::from_static(b"k2")];
    let reply_rx = send_read_multi(&tx, keys, ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let values = reply_rx.await.unwrap().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], Some(Bytes::from_static(b"v1")));
    assert_eq!(values[1], None);
    handle.await.unwrap();
}

/// sm.get_multi() returning Err propagates as SmError through serve_read.
#[tokio::test]
async fn test_read_actor_sm_get_multi_error_returns_sm_error() {
    let mut sm = make_sm_running();
    sm.expect_get_multi().returning(|_| Err(Error::Fatal("disk failure".into())));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::SmError(_)));
    handle.await.unwrap();
}

/// LinearizableRead must never reach ReadActor — defensive guard returns LeaseInvalid.
/// If get_multi() were called, mockall would panic (no expectation set) — implicit safety net.
#[tokio::test]
async fn test_read_actor_linearizable_read_returns_lease_invalid() {
    let sm = make_sm_running(); // no expect_get_multi() — any get_multi() call panics

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LinearizableRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}

/// revoke() immediately invalidates an in-flight lease
#[tokio::test]
async fn test_read_actor_revoke_invalidates_in_flight_reads() {
    let sm = make_sm_running();
    let lease = valid_lease();
    let lease_clone = Arc::clone(&lease);

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, lease, Arc::new(sm), 64));

    lease_clone.revoke();

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}
