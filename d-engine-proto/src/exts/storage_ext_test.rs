use crate::server::storage::SnapshotMetadata;
use bytes::Bytes;

#[test]
fn test_snapshot_metadata_checksum_array_valid() {
    let checksum = [1u8; 32];
    let metadata = SnapshotMetadata {
        checksum: Bytes::copy_from_slice(&checksum),
        ..Default::default()
    };

    let result = metadata.checksum_array();
    assert!(result.is_ok());
    let array = result.unwrap();
    assert_eq!(array, checksum);
}

#[test]
fn test_snapshot_metadata_checksum_array_invalid_length() {
    let metadata = SnapshotMetadata {
        checksum: Bytes::from(vec![1u8; 16]), // Wrong length
        ..Default::default()
    };

    let result = metadata.checksum_array();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid checksum length: 16"
    );
}

#[test]
fn test_snapshot_metadata_checksum_array_empty() {
    let metadata = SnapshotMetadata {
        checksum: Bytes::new(),
        ..Default::default()
    };

    let result = metadata.checksum_array();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid checksum length: 0"
    );
}

#[test]
fn test_snapshot_metadata_set_checksum_array() {
    let mut metadata = SnapshotMetadata::default();
    let checksum = [42u8; 32];

    metadata.set_checksum_array(checksum);

    assert_eq!(metadata.checksum.as_ref(), &checksum);
}

#[test]
fn test_snapshot_metadata_round_trip() {
    let mut metadata = SnapshotMetadata::default();
    let original_checksum = [123u8; 32];

    metadata.set_checksum_array(original_checksum);
    let retrieved = metadata.checksum_array().unwrap();

    assert_eq!(retrieved, original_checksum);
}

#[test]
fn test_snapshot_metadata_checksum_too_long() {
    let metadata = SnapshotMetadata {
        checksum: Bytes::from(vec![1u8; 64]), // Too long
        ..Default::default()
    };

    let result = metadata.checksum_array();
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().to_string(),
        "Invalid checksum length: 64"
    );
}

#[test]
fn test_snapshot_metadata_checksum_with_different_values() {
    let mut metadata = SnapshotMetadata::default();
    let checksum: [u8; 32] = std::array::from_fn(|i| (i as u8).wrapping_mul(7));

    metadata.set_checksum_array(checksum);
    let retrieved = metadata.checksum_array().unwrap();

    assert_eq!(retrieved, checksum);
    for (i, &value) in retrieved.iter().enumerate() {
        assert_eq!(value, (i as u8).wrapping_mul(7));
    }
}
