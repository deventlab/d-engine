//! Packing a snapshot directory into one `.tar.gz` file, and unpacking one received from a peer.
//!
//! The receiver of an archive does not choose what is in it: the sender picks the paths, the entry
//! types and the sizes. So unpacking accepts only what a snapshot can contain (regular files and
//! directories, inside the target directory, within the configured limits) and refuses the rest
//! instead of repairing it. Every refusal below has its own test, and each of them must fail when
//! the matching check is removed.
//!
//! On error the contents of the target directory are unspecified: the caller unpacks into a scratch
//! directory and discards it when unpacking fails.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use flate2::Compression;
use flate2::write::GzEncoder;
use tar::EntryType;
use tar::Header;

use crate::utils::snapshot_archive::UnpackLimits;
use crate::utils::snapshot_archive::pack;
use crate::utils::snapshot_archive::unpack;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

fn generous() -> UnpackLimits {
    UnpackLimits {
        max_total_bytes: 1 << 30,
        max_file_bytes: 1 << 30,
        max_entries: 10_000,
    }
}

fn limits(
    max_total_bytes: u64,
    max_file_bytes: u64,
    max_entries: u64,
) -> UnpackLimits {
    UnpackLimits {
        max_total_bytes,
        max_file_bytes,
        max_entries,
    }
}

/// A header written byte by byte. The `tar` crate refuses to write `..` or an absolute path through
/// its own API, and the whole point here is to build what a hostile sender can build.
fn raw_header(
    name: &str,
    kind: EntryType,
    size: u64,
    link: Option<&str>,
) -> Header {
    let mut header = Header::new_gnu();
    {
        let gnu = header.as_gnu_mut().unwrap();
        gnu.name[..name.len()].copy_from_slice(name.as_bytes());
        if let Some(link) = link {
            gnu.linkname[..link.len()].copy_from_slice(link.as_bytes());
        }
    }
    header.set_entry_type(kind);
    header.set_size(size);
    header.set_mode(0o644);
    header.set_cksum();
    header
}

fn file(
    name: &str,
    data: &[u8],
) -> (Header, Vec<u8>) {
    (
        raw_header(name, EntryType::Regular, data.len() as u64, None),
        data.to_vec(),
    )
}

fn entry(
    name: &str,
    kind: EntryType,
    link: Option<&str>,
) -> (Header, Vec<u8>) {
    (raw_header(name, kind, 0, link), Vec::new())
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn tar_bytes(entries: &[(Header, Vec<u8>)]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (header, data) in entries {
        builder.append(header, data.as_slice()).unwrap();
    }
    builder.into_inner().unwrap()
}

/// Writes `entries` as a `.tar.gz` file in `dir` and returns its path.
fn write_archive(
    dir: &Path,
    entries: &[(Header, Vec<u8>)],
) -> PathBuf {
    write_raw(dir, &gzip(&tar_bytes(entries)))
}

fn write_raw(
    dir: &Path,
    bytes: &[u8],
) -> PathBuf {
    let path = dir.join("snapshot.tar.gz");
    fs::write(&path, bytes).unwrap();
    path
}

/// Every file and directory under `root`, by relative path; a directory maps to `None`.
fn tree_of(root: &Path) -> BTreeMap<String, Option<Vec<u8>>> {
    fn walk(
        root: &Path,
        dir: &Path,
        out: &mut BTreeMap<String, Option<Vec<u8>>>,
    ) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap().to_string_lossy().to_string();
            if path.is_dir() {
                out.insert(relative, None);
                walk(root, &path, out);
            } else {
                out.insert(relative, Some(fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

/// Deterministic bytes that do not compress well, so a "large file" is large on disk too.
fn noise(len: usize) -> Vec<u8> {
    let mut state: u32 = 0x9E37_79B9;
    (0..len)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (state >> 24) as u8
        })
        .collect()
}

fn unpack_error(
    archive: &Path,
    target: &Path,
    limits: &UnpackLimits,
) -> String {
    let err = unpack(archive, target, limits).expect_err("the archive must be refused");
    format!("{err:?}")
}

// ---------------------------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------------------------

/// What a snapshot directory holds comes out of the archive unchanged: nested directories, an empty
/// file, an empty directory and a file of several megabytes.
#[test]
fn test_pack_then_unpack_restores_the_directory_exactly() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    fs::create_dir_all(source.join("nested/deeper")).unwrap();
    fs::create_dir_all(source.join("empty_dir")).unwrap();
    fs::write(source.join("a.txt"), b"hello").unwrap();
    fs::write(source.join("empty_file"), b"").unwrap();
    fs::write(source.join("nested/c.txt"), b"nested").unwrap();
    fs::write(source.join("nested/deeper/big.bin"), noise(3 * 1024 * 1024)).unwrap();

    let archive = work.path().join("snapshot.tar.gz");
    pack(&source, &archive).unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    unpack(&archive, &target, &generous()).unwrap();

    assert_eq!(tree_of(&target), tree_of(&source));
}

/// A RocksDB checkpoint contains hard links to its table files. The archive stores them as ordinary
/// files, so unpacking, which refuses hard links, must still accept what `pack` wrote.
#[cfg(unix)]
#[test]
fn test_pack_stores_hard_linked_source_files_as_regular_files() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("000001.sst"), b"table data").unwrap();
    fs::hard_link(source.join("000001.sst"), source.join("000001_copy.sst")).unwrap();

    let archive = work.path().join("snapshot.tar.gz");
    pack(&source, &archive).unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    unpack(&archive, &target, &generous()).unwrap();

    assert_eq!(tree_of(&target), tree_of(&source));
}

/// A symbolic link in the source is not something a snapshot contains, and the receiver would
/// refuse it. Failing when packing finds the mistake on the node that made it, not on its peers.
#[cfg(unix)]
#[test]
fn test_pack_rejects_a_symlink_in_the_source_directory() {
    let work = tempfile::tempdir().unwrap();
    let source = work.path().join("source");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("real.txt"), b"data").unwrap();
    std::os::unix::fs::symlink("/etc", source.join("link")).unwrap();

    let result = pack(&source, &work.path().join("snapshot.tar.gz"));

    assert!(result.is_err(), "a symlink must not be packed");
}

/// Archives made by other tools usually start with an entry for the root directory, `./`, and name
/// their files `./name`. The root entry is the target directory itself, so it is accepted and
/// creates nothing; a `./` prefix on a path changes nothing.
#[test]
fn test_root_directory_entry_and_dot_prefixed_paths_are_accepted() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(
        work.path(),
        &[
            entry("./", EntryType::Directory, None),
            file("./meta.txt", b"meta"),
            file("./sub/a.txt", b"a"),
        ],
    );

    unpack(&archive, &target, &generous()).unwrap();

    let expected = BTreeMap::from([
        ("meta.txt".to_string(), Some(b"meta".to_vec())),
        ("sub".to_string(), None),
        ("sub/a.txt".to_string(), Some(b"a".to_vec())),
    ]);
    assert_eq!(tree_of(&target), expected);
}

/// A directory entry that forbids writing, followed by a file inside it, must unpack: the entry
/// only describes the directory, and the engine creates files in it before it applies the mode.
#[test]
fn test_directory_entry_without_write_permission_does_not_block_its_files() {
    let work = tempfile::tempdir().unwrap();
    let mut dir_header = raw_header("ro/", EntryType::Directory, 0, None);
    dir_header.set_mode(0o555);
    dir_header.set_cksum();
    let archive = write_archive(
        work.path(),
        &[(dir_header, Vec::new()), file("ro/x.txt", b"inside")],
    );
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();

    unpack(&archive, &target, &generous()).unwrap();

    assert_eq!(fs::read(target.join("ro/x.txt")).unwrap(), b"inside");
    // Let the temporary directory be deleted afterwards.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(target.join("ro"), fs::Permissions::from_mode(0o755)).unwrap();
    }
}

// ---------------------------------------------------------------------------------------------
// What a snapshot cannot contain
// ---------------------------------------------------------------------------------------------

/// `..` would leave the target directory.
#[test]
fn test_entry_with_parent_directory_component_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(work.path(), &[file("../escaped.txt", b"x")]);

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("escaped.txt"), "{message}");
    assert!(!work.path().join("escaped.txt").exists());
}

/// An absolute path names a place of the sender's choosing, so it is refused, not re-rooted into
/// the target directory.
#[test]
fn test_entry_with_absolute_path_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let outside = work.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let absolute = format!("{}/abs.txt", outside.display());
    let archive = write_archive(work.path(), &[file(&absolute, b"x")]);

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("abs.txt"), "{message}");
    assert!(!outside.join("abs.txt").exists());
    assert!(
        !target.join("abs.txt").exists(),
        "an absolute path must not be quietly re-rooted either"
    );
}

/// A symbolic link is refused. A link followed by a file written through it is the attack:
/// the file would land wherever the link points.
#[cfg(unix)]
#[test]
fn test_symlink_entry_is_rejected_and_nothing_is_written_through_it() {
    let work = tempfile::tempdir().unwrap();
    let outside = work.path().join("outside");
    fs::create_dir_all(&outside).unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(
        work.path(),
        &[
            entry("link", EntryType::Symlink, Some(outside.to_str().unwrap())),
            file("link/pwned.txt", b"x"),
        ],
    );

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("link"), "{message}");
    assert!(!outside.join("pwned.txt").exists());
    assert!(
        fs::symlink_metadata(target.join("link")).is_err(),
        "the link itself must not be created"
    );
}

#[cfg(unix)]
#[test]
fn test_hard_link_entry_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(
        work.path(),
        &[
            file("ok.txt", b"data"),
            entry("hard", EntryType::Link, Some("ok.txt")),
        ],
    );

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("hard"), "{message}");
    assert!(!target.join("hard").exists());
}

#[test]
fn test_device_entry_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(work.path(), &[entry("dev", EntryType::Char, None)]);

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("dev"), "{message}");
    assert!(!target.join("dev").exists());
}

#[test]
fn test_fifo_entry_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(work.path(), &[entry("pipe", EntryType::Fifo, None)]);

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("pipe"), "{message}");
    assert!(!target.join("pipe").exists());
}

/// Two entries with one path: the second would silently replace the first. A snapshot written by
/// the engine never does that, so it is refused.
#[test]
fn test_duplicate_path_is_rejected() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(
        work.path(),
        &[file("dup.txt", b"first"), file("dup.txt", b"second")],
    );

    let message = unpack_error(&archive, &target, &generous());

    assert!(message.contains("dup.txt"), "{message}");
}

// ---------------------------------------------------------------------------------------------
// Limits: a small archive must not be able to fill the disk
// ---------------------------------------------------------------------------------------------

/// The total is checked at its limit: equal passes, one byte more fails, and the message names the
/// setting that raises the limit.
#[test]
fn test_total_size_limit_is_enforced_at_the_boundary() {
    let work = tempfile::tempdir().unwrap();
    let archive = write_archive(work.path(), &[file("a", &[1u8; 60]), file("b", &[2u8; 60])]);

    let at_limit = work.path().join("at_limit");
    fs::create_dir_all(&at_limit).unwrap();
    unpack(&archive, &at_limit, &limits(120, 120, 10)).unwrap();

    let over = work.path().join("over");
    fs::create_dir_all(&over).unwrap();
    let message = unpack_error(&archive, &over, &limits(119, 119, 10));
    assert!(message.contains("max_unpack_total_bytes"), "{message}");
}

#[test]
fn test_single_file_limit_is_enforced_at_the_boundary() {
    let work = tempfile::tempdir().unwrap();
    let archive = write_archive(work.path(), &[file("a", &[1u8; 50])]);

    let at_limit = work.path().join("at_limit");
    fs::create_dir_all(&at_limit).unwrap();
    unpack(&archive, &at_limit, &limits(1000, 50, 10)).unwrap();

    let over = work.path().join("over");
    fs::create_dir_all(&over).unwrap();
    let message = unpack_error(&archive, &over, &limits(1000, 49, 10));
    assert!(message.contains("max_unpack_file_bytes"), "{message}");
}

#[test]
fn test_entry_count_limit_is_enforced_at_the_boundary() {
    let work = tempfile::tempdir().unwrap();
    let archive = write_archive(
        work.path(),
        &[file("a", b"1"), file("b", b"2"), file("c", b"3")],
    );

    let at_limit = work.path().join("at_limit");
    fs::create_dir_all(&at_limit).unwrap();
    unpack(&archive, &at_limit, &limits(1000, 1000, 3)).unwrap();

    let over = work.path().join("over");
    fs::create_dir_all(&over).unwrap();
    let message = unpack_error(&archive, &over, &limits(1000, 1000, 2));
    assert!(message.contains("max_unpack_entries"), "{message}");
}

/// The size a header declares is checked before any of the file is written, so an archive that
/// announces a huge file is refused at once and leaves no part of that file behind.
#[test]
fn test_file_that_declares_more_than_the_limit_is_refused_before_it_is_written() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let header = raw_header("huge.bin", EntryType::Regular, 1 << 40, None);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(header.as_bytes());
    bytes.extend_from_slice(&[0u8; 1024]);
    let archive = write_raw(work.path(), &gzip(&bytes));

    let message = unpack_error(&archive, &target, &limits(1 << 20, 1 << 20, 10));

    assert!(message.contains("max_unpack_file_bytes"), "{message}");
    assert!(!target.join("huge.bin").exists());
}

/// A file of zeros compresses to almost nothing: a few kilobytes on the wire become megabytes on
/// disk. The limit is on what comes out, not on what was received.
#[test]
fn test_highly_compressible_file_is_stopped_by_the_size_limit() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_archive(
        work.path(),
        &[file("zeros.bin", &vec![0u8; 8 * 1024 * 1024])],
    );
    assert!(
        fs::metadata(&archive).unwrap().len() < 64 * 1024,
        "the test needs an archive that is small compared with its content"
    );

    let message = unpack_error(&archive, &target, &limits(1 << 20, 1 << 20, 10));

    assert!(message.contains("max_unpack_file_bytes"), "{message}");
}

/// `tar` reads a long-name entry into memory at the size its header declares. An archive that
/// declares megabytes of "name" made of zeros costs the sender almost nothing and the receiver
/// that much memory, however large the declared size is. The unpacker stops reading well before.
#[test]
fn test_long_name_entry_with_a_huge_declared_size_is_stopped() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let name_len: u64 = 8 * 1024 * 1024;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        raw_header("././@LongLink", EntryType::GNULongName, name_len, None).as_bytes(),
    );
    bytes.extend_from_slice(&vec![b'a'; name_len as usize]);
    bytes.extend_from_slice(raw_header("x.txt", EntryType::Regular, 0, None).as_bytes());
    bytes.extend_from_slice(&[0u8; 1024]);
    let archive = write_raw(work.path(), &gzip(&bytes));

    let message = unpack_error(&archive, &target, &generous());

    assert!(
        message.contains("more data than its entries declare"),
        "{message}"
    );
}

// ---------------------------------------------------------------------------------------------
// A damaged archive
// ---------------------------------------------------------------------------------------------

/// The compressed stream ends in the middle of the content.
#[test]
fn test_archive_cut_in_the_middle_fails_to_unpack() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let full = gzip(&tar_bytes(&[file("a.bin", &noise(256 * 1024))]));
    let archive = write_raw(work.path(), &full[..full.len() / 2]);

    assert!(unpack(&archive, &target, &generous()).is_err());
}

/// Only the 8 byte gzip trailer (checksum and length) is missing. The tar data is complete and
/// ends properly, so only reading the compressed stream to its end notices that it was cut.
#[test]
fn test_archive_without_the_gzip_trailer_fails_to_unpack() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let full = gzip(&tar_bytes(&[file("a.txt", b"complete content")]));
    let archive = write_raw(work.path(), &full[..full.len() - 8]);

    assert!(unpack(&archive, &target, &generous()).is_err());
}

#[test]
fn test_file_that_is_not_gzip_fails_to_unpack() {
    let work = tempfile::tempdir().unwrap();
    let target = work.path().join("target");
    fs::create_dir_all(&target).unwrap();
    let archive = write_raw(work.path(), b"this is not a compressed archive at all");

    assert!(unpack(&archive, &target, &generous()).is_err());
}
