//! Packs a snapshot directory into one `.tar.gz` file, and unpacks one received from a peer.
//!
//! The receiver of an archive does not choose what is in it: the sender picks the paths, the
//! entry types and the sizes. `unpack` therefore accepts only what a snapshot can contain
//! (regular files and directories, inside the target directory, within the configured limits)
//! and refuses everything else instead of repairing it.
//!
//! `tar` and `flate2` are used here and nowhere else, so replacing either one means changing
//! this file only.
//!
//! Both functions block. Call them from `tokio::task::spawn_blocking`.
//!
//! On error the contents of the target directory are unspecified. The caller unpacks into a
//! scratch directory and discards it when unpacking fails.

use std::cell::Cell;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use tar::Archive;
use tar::Builder;
use tar::EntryType;

use crate::Result;
use crate::SnapshotError;

/// Bytes the archive may hold beyond the data of the entries accepted so far: the next header and
/// the long-name or extended-attribute entries that `tar` reads into memory before it hands the
/// entry over. A legitimate path or attribute block is a few kilobytes.
const EXTENSION_ALLOWANCE_BYTES: u64 = 1024 * 1024;

const TAR_BLOCK_BYTES: u64 = 512;

/// What a received archive may unpack to. The names match the `raft.snapshot` settings that fill
/// them, because the errors tell the operator which setting to raise.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UnpackLimits {
    /// Sum of the sizes of all files.
    pub(crate) max_total_bytes: u64,
    /// Size of any single file.
    pub(crate) max_file_bytes: u64,
    /// Number of entries, files and directories together.
    pub(crate) max_entries: u64,
}

/// Packs the regular files and directories under `source_dir` into `dest` as a gzip-compressed
/// tar archive, and syncs `dest` to disk before it returns.
///
/// Anything else under `source_dir` (a symbolic link, a device, a socket) fails the call: a
/// snapshot never contains it, and the receiver would refuse the archive.
pub(crate) fn pack(
    source_dir: &Path,
    dest: &Path,
) -> Result<()> {
    pack_inner(source_dir, dest)
        .map_err(|e| SnapshotError::OperationFailed(format!("Failed to pack snapshot: {e}")).into())
}

fn pack_inner(
    source_dir: &Path,
    dest: &Path,
) -> io::Result<()> {
    let file = File::create(dest)?;
    let encoder = GzEncoder::new(BufWriter::new(file), Compression::default());
    let mut builder = Builder::new(encoder);
    builder.follow_symlinks(false);
    append_tree(&mut builder, source_dir, source_dir)?;

    // `into_inner` writes the end of the tar archive; `finish` writes the gzip trailer.
    let writer = builder.into_inner()?.finish()?;
    let file = writer.into_inner().map_err(io::IntoInnerError::into_error)?;
    file.sync_all()
}

fn append_tree<W: io::Write>(
    builder: &mut Builder<W>,
    root: &Path,
    dir: &Path,
) -> io::Result<()> {
    let mut children = fs::read_dir(dir)?.collect::<io::Result<Vec<_>>>()?;
    // The same directory always packs to the same archive.
    children.sort_by_key(|child| child.file_name());

    for child in children {
        let path = child.path();
        let relative = path.strip_prefix(root).map_err(io::Error::other)?;
        let file_type = fs::symlink_metadata(&path)?.file_type();
        if file_type.is_dir() {
            builder.append_dir(relative, &path)?;
            append_tree(builder, root, &path)?;
        } else if file_type.is_file() {
            builder.append_path_with_name(&path, relative)?;
        } else {
            return Err(io::Error::other(format!(
                "{} is neither a regular file nor a directory",
                path.display()
            )));
        }
    }
    Ok(())
}

/// Unpacks the `.tar.gz` file `archive_path` into the existing directory `target_dir`.
///
/// Fails, naming the entry or the setting involved, when the archive:
/// - holds anything but regular files and directories,
/// - names a path that is absolute, contains `..`, or appears twice,
/// - exceeds one of `limits`,
/// - is damaged, including an archive whose compressed stream was cut short.
pub(crate) fn unpack(
    archive_path: &Path,
    target_dir: &Path,
    limits: &UnpackLimits,
) -> Result<()> {
    unpack_inner(archive_path, target_dir, limits).map_err(|e| {
        SnapshotError::OperationFailed(format!("Failed to unpack snapshot: {e}")).into()
    })
}

fn refuse(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn unpack_inner(
    archive_path: &Path,
    target_dir: &Path,
    limits: &UnpackLimits,
) -> io::Result<()> {
    let file = File::open(archive_path)?;
    let allowed = Rc::new(Cell::new(EXTENSION_ALLOWANCE_BYTES + TAR_BLOCK_BYTES));
    let reader = BoundedReader {
        inner: GzDecoder::new(BufReader::new(file)),
        position: 0,
        allowed: Rc::clone(&allowed),
    };
    let mut archive = Archive::new(reader);
    // A second file with a name that only differs by case on a case-insensitive file system would
    // pass the duplicate check below; creating it must fail instead of replacing the first.
    archive.set_overwrite(false);

    let mut seen = HashSet::new();
    let mut entries = 0u64;
    let mut total_bytes = 0u64;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let kind = entry.header().entry_type();

        entries += 1;
        if entries > limits.max_entries {
            return Err(refuse(format!(
                "archive has more than {} entries (max_unpack_entries); raise \
                 raft.snapshot.max_unpack_entries if this snapshot is legitimate",
                limits.max_entries
            )));
        }

        if kind != EntryType::Regular && kind != EntryType::Directory {
            return Err(refuse(format!(
                "entry {path:?} has type {kind:?}; only regular files and directories are accepted"
            )));
        }

        let relative = relative_path(&path)?;
        if !relative.as_os_str().is_empty() && !seen.insert(relative.clone()) {
            return Err(refuse(format!("entry {path:?} appears more than once")));
        }

        let size = entry.size();
        if size > limits.max_file_bytes {
            return Err(refuse(format!(
                "entry {path:?} is {size} bytes, over max_unpack_file_bytes ({}); raise \
                 raft.snapshot.max_unpack_file_bytes if this snapshot is legitimate",
                limits.max_file_bytes
            )));
        }
        total_bytes = total_bytes.saturating_add(size);
        if total_bytes > limits.max_total_bytes {
            return Err(refuse(format!(
                "archive unpacks to more than {} bytes (max_unpack_total_bytes); raise \
                 raft.snapshot.max_unpack_total_bytes if this snapshot is legitimate",
                limits.max_total_bytes
            )));
        }

        // The data of this entry, then room for the headers of the next one.
        let data_end = entry
            .raw_file_position()
            .saturating_add(size.div_ceil(TAR_BLOCK_BYTES).saturating_mul(TAR_BLOCK_BYTES));
        allowed.set(data_end.saturating_add(EXTENSION_ALLOWANCE_BYTES));

        if kind == EntryType::Directory {
            // Directories are created with the default mode: the mode stored in the archive is
            // not applied, so a read-only directory entry cannot block the files inside it.
            if !relative.as_os_str().is_empty() {
                fs::create_dir_all(target_dir.join(&relative))?;
            }
            continue;
        }

        if relative.as_os_str().is_empty() {
            return Err(refuse(format!("file entry {path:?} has no file name")));
        }
        if !entry.unpack_in(target_dir)? {
            return Err(refuse(format!(
                "entry {path:?} was refused by the unpacker"
            )));
        }
    }

    // `tar` stops at its end marker without looking at the rest of the compressed stream, and the
    // gzip checksum and length sit at the very end of it. Read on, so that a cut-off stream fails.
    let mut reader = archive.into_inner();
    let position = reader.position;
    allowed.set(position.saturating_add(EXTENSION_ALLOWANCE_BYTES));
    io::copy(&mut reader, &mut io::sink())
        .map_err(|e| io::Error::new(e.kind(), format!("the archive does not end cleanly: {e}")))?;
    Ok(())
}

/// The path as a sequence of plain names, without `.` components. Fails on `..` and on a path
/// that starts at the root or at a drive.
fn relative_path(path: &Path) -> io::Result<PathBuf> {
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => relative.push(name),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(refuse(format!("entry {path:?} contains a `..` component")));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(refuse(format!("entry {path:?} is an absolute path")));
            }
        }
    }
    Ok(relative)
}

/// Passes on what the compressed stream decompresses to, but never reads past the position that
/// the entries accepted so far allow. `tar` reads long-name and extended-attribute entries into
/// memory at whatever size their header declares; this stops a header that declares far more than
/// it holds from making the receiver read without end.
struct BoundedReader<R> {
    inner: R,
    position: u64,
    allowed: Rc<Cell<u64>>,
}

impl<R: Read> Read for BoundedReader<R> {
    fn read(
        &mut self,
        buf: &mut [u8],
    ) -> io::Result<usize> {
        let room = self.allowed.get().saturating_sub(self.position);
        if room == 0 {
            return Err(refuse(
                "the archive holds more data than its entries declare".to_string(),
            ));
        }
        let wanted = buf.len().min(usize::try_from(room).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..wanted])?;
        self.position += n as u64;
        Ok(n)
    }
}
