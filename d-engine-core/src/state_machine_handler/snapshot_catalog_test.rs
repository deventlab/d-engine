use std::fs;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_proto::common::LogId;
use d_engine_proto::server::storage::SnapshotMetadata;
use prost::Message;
use tempfile::tempdir;
use tracing_test::traced_test;

use super::SnapshotCatalog;
use crate::SnapshotPathManager;

fn metadata(
    index: u64,
    term: u64,
) -> SnapshotMetadata {
    SnapshotMetadata {
        last_included: Some(LogId { index, term }),
        checksum: Bytes::from(vec![index as u8; 32]),
    }
}

fn catalog_over(dir: &Path) -> SnapshotCatalog {
    SnapshotCatalog::open(Arc::new(SnapshotPathManager::new(
        dir.to_path_buf(),
        "snapshot-".to_string(),
    )))
}

/// An archive and the record the engine wrote for it, as a finished snapshot leaves them.
fn write_snapshot(
    dir: &Path,
    index: u64,
    term: u64,
) {
    fs::write(
        dir.join(format!("snapshot-{index}-{term}.tar.gz")),
        b"archive",
    )
    .unwrap();
    fs::write(
        dir.join(format!("snapshot-{index}-{term}.meta")),
        metadata(index, term).encode_to_vec(),
    )
    .unwrap();
}

/// Purpose: with several snapshots on disk, the newest one is the node's latest.
#[test]
#[traced_test]
fn test_open_picks_the_newest_snapshot() {
    let dir = tempdir().unwrap();
    write_snapshot(dir.path(), 10, 1);
    write_snapshot(dir.path(), 30, 2);
    write_snapshot(dir.path(), 20, 2);

    assert_eq!(catalog_over(dir.path()).latest(), Some(metadata(30, 2)));
}

/// A record whose archive was deleted (cleanup crashed halfway) describes a snapshot the node no
/// longer has.
///
/// Purpose: it is ignored, and the newest snapshot that still has its archive is used.
#[test]
#[traced_test]
fn test_open_ignores_a_record_whose_archive_is_gone() {
    let dir = tempdir().unwrap();
    write_snapshot(dir.path(), 10, 1);
    write_snapshot(dir.path(), 20, 1);
    fs::remove_file(dir.path().join("snapshot-20-1.tar.gz")).unwrap();

    assert_eq!(catalog_over(dir.path()).latest(), Some(metadata(10, 1)));
}

/// A record that cannot be decoded (a damaged disk) must not stop the node from starting.
///
/// Purpose: it is ignored and reported, and the other snapshots are still found.
#[test]
#[traced_test]
fn test_open_ignores_an_undecodable_record() {
    let dir = tempdir().unwrap();
    write_snapshot(dir.path(), 10, 1);
    fs::write(dir.path().join("snapshot-20-1.tar.gz"), b"archive").unwrap();
    fs::write(dir.path().join("snapshot-20-1.meta"), b"not a record").unwrap();

    assert_eq!(catalog_over(dir.path()).latest(), Some(metadata(10, 1)));
    assert!(logs_contain("ignoring undecodable snapshot record"));
}

/// A record whose file name says one boundary while its content says another.
///
/// Purpose: it is not trusted: the archive is looked up by the boundary in the name.
#[test]
#[traced_test]
fn test_open_ignores_a_record_whose_name_disagrees_with_its_boundary() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("snapshot-20-1.tar.gz"), b"archive").unwrap();
    fs::write(
        dir.path().join("snapshot-5-1.meta"),
        metadata(20, 1).encode_to_vec(),
    )
    .unwrap();

    assert_eq!(catalog_over(dir.path()).latest(), None);
}

/// Purpose: a directory that does not exist yet holds no snapshots, and is not an error.
#[test]
#[traced_test]
fn test_open_without_a_snapshots_directory_has_no_snapshot() {
    let dir = tempdir().unwrap();

    assert_eq!(catalog_over(&dir.path().join("missing")).latest(), None);
}

/// A late, older snapshot (for example one received after a newer one was built) is recorded but
/// must not replace the newer one as the latest.
///
/// Purpose: the latest snapshot never moves backwards.
#[tokio::test]
#[traced_test]
async fn test_publish_does_not_replace_a_newer_snapshot_with_an_older_one() {
    let dir = tempdir().unwrap();
    let catalog = catalog_over(dir.path());
    fs::write(dir.path().join("snapshot-30-2.tar.gz"), b"archive").unwrap();
    fs::write(dir.path().join("snapshot-10-1.tar.gz"), b"archive").unwrap();

    catalog.publish(&metadata(30, 2)).await.unwrap();
    catalog.publish(&metadata(10, 1)).await.unwrap();

    assert_eq!(catalog.latest(), Some(metadata(30, 2)));
    assert!(
        dir.path().join("snapshot-10-1.meta").exists(),
        "the older snapshot is still recorded"
    );
}

/// Purpose: a record written by `publish` is found again by a node that starts later.
#[tokio::test]
#[traced_test]
async fn test_published_record_survives_a_restart() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("snapshot-10-1.tar.gz"), b"archive").unwrap();
    catalog_over(dir.path()).publish(&metadata(10, 1)).await.unwrap();

    assert_eq!(catalog_over(dir.path()).latest(), Some(metadata(10, 1)));
}
