//! Limits on unpacking a snapshot archive received from another node.
//!
//! The archive comes from a peer, so a small file must not be able to fill the disk or exhaust the
//! inodes of the node that unpacks it. The limits are settings because only the application knows
//! how large its own snapshots legitimately are.

use crate::RaftConfig;
use crate::SnapshotConfig;

const GIB: u64 = 1024 * 1024 * 1024;

fn load_snapshot_toml(text: &str) -> SnapshotConfig {
    config::Config::builder()
        .add_source(config::File::from_str(text, config::FileFormat::Toml))
        .build()
        .expect("toml must parse")
        .try_deserialize()
        .expect("config must deserialize")
}

fn config_with(change: impl FnOnce(&mut SnapshotConfig)) -> RaftConfig {
    let mut config = RaftConfig::default();
    change(&mut config.snapshot);
    config
}

#[test]
fn test_unpack_limits_defaults_are_far_above_a_normal_snapshot() {
    let snapshot = SnapshotConfig::default();

    assert_eq!(snapshot.max_unpack_total_bytes, 256 * GIB);
    assert_eq!(snapshot.max_unpack_file_bytes, 64 * GIB);
    assert_eq!(snapshot.max_unpack_entries, 100_000);
}

/// A config file written before these settings existed must keep loading, with the defaults.
#[test]
fn test_unpack_limits_missing_keys_parse_to_defaults() {
    let snapshot = load_snapshot_toml("chunk_size = 2048\n");

    assert_eq!(snapshot.chunk_size, 2048);
    assert_eq!(snapshot.max_unpack_total_bytes, 256 * GIB);
    assert_eq!(snapshot.max_unpack_file_bytes, 64 * GIB);
    assert_eq!(snapshot.max_unpack_entries, 100_000);
}

#[test]
fn test_unpack_limits_explicit_values_are_honored() {
    let snapshot = load_snapshot_toml(
        "max_unpack_total_bytes = 2000\nmax_unpack_file_bytes = 1000\nmax_unpack_entries = 7\n",
    );

    assert_eq!(snapshot.max_unpack_total_bytes, 2000);
    assert_eq!(snapshot.max_unpack_file_bytes, 1000);
    assert_eq!(snapshot.max_unpack_entries, 7);
}

#[test]
fn test_unpack_limits_defaults_pass_validation() {
    assert!(RaftConfig::default().validate().is_ok());
}

/// A limit of 0 would reject every snapshot, so a node could never catch up: startup must fail.
#[test]
fn test_unpack_limit_of_zero_is_rejected() {
    let total = config_with(|s| s.max_unpack_total_bytes = 0);
    let file = config_with(|s| s.max_unpack_file_bytes = 0);
    let entries = config_with(|s| s.max_unpack_entries = 0);

    let total_err = total.validate().expect_err("zero total must be rejected").to_string();
    let file_err = file.validate().expect_err("zero file limit must be rejected").to_string();
    let entries_err = entries.validate().expect_err("zero entries must be rejected").to_string();

    assert!(total_err.contains("max_unpack_total_bytes"), "{total_err}");
    assert!(file_err.contains("max_unpack_file_bytes"), "{file_err}");
    assert!(entries_err.contains("max_unpack_entries"), "{entries_err}");
}

/// One file cannot be larger than all files together, so such a pair means the single-file limit
/// can never take effect. That is a mistake in the config, and it is reported at startup.
#[test]
fn test_unpack_file_limit_above_total_limit_is_rejected() {
    let config = config_with(|s| {
        s.max_unpack_total_bytes = 1000;
        s.max_unpack_file_bytes = 1001;
    });

    let err = config
        .validate()
        .expect_err("file limit above total must be rejected")
        .to_string();

    assert!(err.contains("max_unpack_file_bytes"), "{err}");
    assert!(err.contains("max_unpack_total_bytes"), "{err}");
}

#[test]
fn test_unpack_file_limit_equal_to_total_limit_is_accepted() {
    let config = config_with(|s| {
        s.max_unpack_total_bytes = 1000;
        s.max_unpack_file_bytes = 1000;
    });

    assert!(config.validate().is_ok());
}
