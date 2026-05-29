use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::MockStateMachine;
use crate::ReadLease;
use crate::config::ReadConsistencyPolicy;
use crate::now_ms;
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
) -> oneshot::Receiver<Result<Option<Bytes>, ReadActorError>> {
    let (reply_tx, reply_rx) = oneshot::channel();
    tx.try_send(ReadCmd {
        key: Bytes::copy_from_slice(key),
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
    sm.expect_get()
        .withf(|k| k == b"k1")
        .returning(|_| Ok(Some(Bytes::from_static(b"v1"))));

    let lease = valid_lease();
    let (tx, rx) = mpsc::channel(8);

    let handle = tokio::spawn(run_read_actor(rx, lease, Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k1", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let result = reply_rx.await.expect("reply sent");
    assert_eq!(result.unwrap(), Some(Bytes::from_static(b"v1")));
    handle.await.unwrap();
}

/// Eventual read returns None for missing key
#[tokio::test]
async fn test_read_actor_eventual_read_missing_key_returns_none() {
    let mut sm = make_sm_running();
    sm.expect_get().returning(|_| Ok(None));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"missing", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    assert!(reply_rx.await.unwrap().unwrap().is_none());
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
    sm.expect_get().returning(|_| Ok(Some(Bytes::from_static(b"val"))));

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
    lease.renew(1, now_ms().saturating_sub(1)); // deadline already past

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

    drop(tx); // close sender → ReadActor loop exits

    handle.await.unwrap();

    // After ReadActor exits, Arc<SM> count should be 0 (sole owner was ReadActor)
    assert!(
        sm_weak.upgrade().is_none(),
        "Arc<SM> should be dropped when ReadActor exits"
    );
}

/// sm.get() returning Err maps to ReadActorError::SmError (not swallowed)
#[tokio::test]
async fn test_read_actor_sm_get_error_returns_sm_error() {
    let mut sm = make_sm_running();
    sm.expect_get().returning(|_| Err(crate::Error::Fatal("disk failure".into())));

    let (tx, rx) = mpsc::channel(8);
    let handle = tokio::spawn(run_read_actor(rx, valid_lease(), Arc::new(sm), 64));

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::EventualConsistency);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::SmError(_)));
    handle.await.unwrap();
}

/// LinearizableRead must never reach ReadActor — defensive guard returns LeaseInvalid.
/// If get() were called, mockall would panic (no expectation set) — implicit safety net.
#[tokio::test]
async fn test_read_actor_linearizable_read_returns_lease_invalid() {
    let sm = make_sm_running(); // no expect_get() — any get() call panics

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

    lease_clone.revoke(); // revoke before read is processed

    let reply_rx = send_read(&tx, b"k", ReadConsistencyPolicy::LeaseRead);
    drop(tx);

    let err = reply_rx.await.unwrap().unwrap_err();
    assert!(matches!(err, ReadActorError::LeaseInvalid));
    handle.await.unwrap();
}
