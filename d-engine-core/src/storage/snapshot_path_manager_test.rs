use std::path::PathBuf;

use d_engine_proto::common::LogId;

use super::SnapshotPathManager;

#[test]
fn test_final_snapshot_path() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());
    let log_id = LogId {
        index: 100,
        term: 5,
    };

    let path = manager.final_snapshot_path(&log_id);
    assert_eq!(path, PathBuf::from("/data/snapshot-100-5.tar.gz"));
}

#[test]
fn test_temp_work_path() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());
    let log_id = LogId {
        index: 100,
        term: 5,
    };

    let path = manager.temp_work_path(&log_id);
    assert_eq!(path, PathBuf::from("/data/temp-100-5"));
}

#[test]
fn test_temp_assembly_file() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());

    let path = manager.temp_assembly_file();
    assert_eq!(path, PathBuf::from("/data/temp-snapshot.part.tar.gz"));
}

#[test]
fn test_parse_final_snapshot_filename() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());

    let result = manager.parse_snapshot_filename("snapshot-100-5.tar.gz");
    assert_eq!(result, Some((100, 5)));
}

#[test]
fn test_parse_temp_snapshot_filename() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());

    let result = manager.parse_snapshot_filename("temp-100-5");
    assert_eq!(result, Some((100, 5)));
}

#[test]
fn test_parse_invalid_filename() {
    let manager = SnapshotPathManager::new(PathBuf::from("/data"), "snapshot-".to_string());

    let result = manager.parse_snapshot_filename("invalid-file.txt");
    assert_eq!(result, None);
}

#[test]
fn test_custom_prefixes() {
    let manager = SnapshotPathManager::with_prefixes(
        PathBuf::from("/data"),
        "snap-".to_string(),
        "work-".to_string(),
    );

    let log_id = LogId {
        index: 100,
        term: 5,
    };
    assert_eq!(
        manager.final_snapshot_path(&log_id),
        PathBuf::from("/data/snap-100-5.tar.gz")
    );
    assert_eq!(
        manager.temp_work_path(&log_id),
        PathBuf::from("/data/work-100-5")
    );
}
