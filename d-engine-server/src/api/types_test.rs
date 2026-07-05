use bytes::Bytes;
use d_engine_core::BatchOp;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::client::write_command::Operation;

use super::WriteOperation;

/// Verify Insert with TTL converts correctly to proto WriteCommand.
#[test]
fn test_write_operation_insert_with_ttl() {
    let op = WriteOperation::Insert {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
        ttl_secs: Some(60),
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::Insert(ins)) => {
            assert_eq!(ins.key, Bytes::from("k"));
            assert_eq!(ins.value, Bytes::from("v"));
            assert_eq!(ins.ttl_secs, 60);
        }
        other => panic!("expected Insert, got {:?}", other),
    }
}

/// Verify Insert with no TTL maps ttl_secs=None → proto ttl_secs=0.
#[test]
fn test_write_operation_insert_no_ttl_maps_to_zero() {
    let op = WriteOperation::Insert {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
        ttl_secs: None,
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::Insert(ins)) => assert_eq!(ins.ttl_secs, 0),
        other => panic!("expected Insert, got {:?}", other),
    }
}

/// Verify Delete converts correctly to proto WriteCommand.
#[test]
fn test_write_operation_delete() {
    let op = WriteOperation::Delete {
        key: Bytes::from("del-key"),
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::Delete(del)) => assert_eq!(del.key, Bytes::from("del-key")),
        other => panic!("expected Delete, got {:?}", other),
    }
}

/// Verify CAS with expected value converts correctly.
#[test]
fn test_write_operation_cas_with_expected() {
    let op = WriteOperation::CompareAndSwap {
        key: Bytes::from("k"),
        expected: Some(Bytes::from("old")),
        new_value: Bytes::from("new"),
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::CompareAndSwap(cas)) => {
            assert_eq!(cas.key, Bytes::from("k"));
            assert_eq!(cas.expected_value, Some(Bytes::from("old")));
            assert_eq!(cas.new_value, Bytes::from("new"));
        }
        other => panic!("expected CompareAndSwap, got {:?}", other),
    }
}

/// CAS with expected=None maps to expected_value=None (key-must-not-exist semantics).
#[test]
fn test_write_operation_cas_key_must_not_exist() {
    let op = WriteOperation::CompareAndSwap {
        key: Bytes::from("k"),
        expected: None,
        new_value: Bytes::from("new"),
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::CompareAndSwap(cas)) => assert!(cas.expected_value.is_none()),
        other => panic!("expected CompareAndSwap, got {:?}", other),
    }
}

/// Verify Batch with mixed Insert/Delete ops converts correctly to proto WriteCommand.
#[test]
fn test_write_operation_batch_mixed_ops() {
    use d_engine_proto::client::write_command::batch_op;

    let op = WriteOperation::Batch {
        ops: vec![
            BatchOp::Insert {
                key: Bytes::from("k1"),
                value: Bytes::from("v1"),
            },
            BatchOp::Delete {
                key: Bytes::from("k2"),
            },
        ],
    };
    let cmd = WriteCommand::from(op);
    match cmd.operation {
        Some(Operation::Batch(b)) => {
            assert_eq!(b.ops.len(), 2);
            match &b.ops[0].op {
                Some(batch_op::Op::Insert(ins)) => {
                    assert_eq!(ins.key, Bytes::from("k1"));
                    assert_eq!(ins.value, Bytes::from("v1"));
                    assert_eq!(ins.ttl_secs, 0);
                }
                other => panic!("expected Insert, got {:?}", other),
            }
            match &b.ops[1].op {
                Some(batch_op::Op::Delete(del)) => {
                    assert_eq!(del.key, Bytes::from("k2"));
                }
                other => panic!("expected Delete, got {:?}", other),
            }
        }
        other => panic!("expected Batch, got {:?}", other),
    }
}
