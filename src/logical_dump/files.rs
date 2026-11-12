// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Staging, hashing and publication of dump files and import targets.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use hawdb_integrity::IntegrityHasher;

use super::model::{FileEntry, FileKind};
use super::value::malformed;
use crate::{HawDBError, Result};

/// Largest JSON Lines record a reader accepts. A record is one row, node or
/// relationship; this bounds reader memory against a damaged dump.
pub(super) const MAX_RECORD_BYTES: u64 = 256 * 1024 * 1024;
/// Largest `manifest.json` or `schema.json` a reader accepts.
pub(super) const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;

pub(super) fn io_error(action: &str, path: &Path, error: std::io::Error) -> HawDBError {
    HawDBError::Storage(format!(
        "logical dump: {action} {}: {error}",
        path.display()
    ))
}

/// A destination that must not exist yet, with a unique sibling staging
/// directory created for this call only. Publication moves the staging
/// directory onto the destination with a no-replace rename; dropping an
/// unpublished stage removes the directory this call created and nothing
/// else.
pub(super) struct Stage {
    staging: PathBuf,
    destination: PathBuf,
    published: bool,
}

impl Stage {
    pub(super) fn create(destination: &Path, purpose: &str) -> Result<Self> {
        if fs::symlink_metadata(destination).is_ok() {
            return Err(HawDBError::Storage(format!(
                "logical dump: destination {} already exists; {purpose} writes only to a new path",
                destination.display()
            )));
        }
        let name = destination.file_name().ok_or_else(|| {
            HawDBError::Storage(format!(
                "logical dump: destination {} has no final path component",
                destination.display()
            ))
        })?;
        let parent = match destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        if !parent.is_dir() {
            return Err(HawDBError::Storage(format!(
                "logical dump: parent directory {} does not exist",
                parent.display()
            )));
        }
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let staging = parent.join(format!(
            ".{}.hawdb-{purpose}-{}-{nonce}-{}",
            name.to_string_lossy(),
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        // `create_dir` (not `create_dir_all`) refuses an existing path, so the
        // stage is always a directory this call created.
        fs::create_dir(&staging).map_err(|error| io_error("create", &staging, error))?;
        Ok(Self {
            staging,
            destination: destination.to_path_buf(),
            published: false,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.staging
    }

    /// Atomically moves the complete stage onto the destination and makes the
    /// move durable.
    ///
    /// The move is one no-replace filesystem operation
    /// (`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)` on
    /// macOS, `MoveFileExW` without `MOVEFILE_REPLACE_EXISTING` on Windows):
    /// a file, directory or symlink that appears at the destination at any
    /// point before the move is never replaced, and the publication refuses
    /// with nothing published. Platforms or filesystems without such a
    /// primitive refuse rather than fall back to check-then-rename.
    ///
    /// Once the move may have happened, every failure is
    /// [`HawDBError::PublishOutcomeUnknown`]: the destination then holds this
    /// call's complete result, but its name may not survive a crash. The
    /// stage is never removed after that point.
    pub(super) fn publish(mut self) -> Result<()> {
        sync_directory(&self.staging)?;
        #[cfg(test)]
        hooks::before_publish(&self.destination);
        if let Err(error) =
            hawdb_storage::file_io::rename_directory_new(&self.staging, &self.destination)
        {
            // An error may follow a completed move. A stage that is no longer
            // at its own name was moved: the destination is ours.
            if fs::symlink_metadata(&self.staging).is_ok() {
                let reason = if error.kind() == std::io::ErrorKind::AlreadyExists {
                    "appeared during the operation".to_string()
                } else {
                    format!("could not be published ({error})")
                };
                return Err(HawDBError::Storage(format!(
                    "logical dump: destination {} {reason}; nothing was published",
                    self.destination.display()
                )));
            }
            self.published = true;
            return Err(self.outcome_unknown(format!("the move reported {error}")));
        }
        self.published = true;
        let parent = match self.destination.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        sync_published_parent(parent).map_err(|error| {
            self.outcome_unknown(format!(
                "the parent directory sync after the move failed: {error}"
            ))
        })
    }

    fn outcome_unknown(&self, message: String) -> HawDBError {
        HawDBError::PublishOutcomeUnknown {
            destination: self.destination.clone(),
            message: format!("logical dump: {message}"),
        }
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_dir_all(&self.staging);
        }
    }
}

fn sync_published_parent(parent: &Path) -> Result<()> {
    #[cfg(test)]
    hooks::parent_sync_failure()?;
    sync_directory(parent)
}

/// Completes the durability barrier of a publication that returned
/// [`HawDBError::PublishOutcomeUnknown`], or confirms one that succeeded:
/// checks that `destination` is a directory (not a symlink) and synchronizes
/// it and its parent directory. Idempotent and read-only for the namespace.
pub(super) fn confirm_publication(destination: impl AsRef<Path>) -> Result<()> {
    let destination = destination.as_ref();
    let metadata = fs::symlink_metadata(destination)
        .map_err(|error| io_error("inspect", destination, error))?;
    if !metadata.is_dir() {
        return Err(HawDBError::Storage(format!(
            "logical dump: {} is not a directory, so it is not a published dump or import",
            destination.display()
        )));
    }
    sync_directory(destination)?;
    let parent = match destination.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    sync_published_parent(parent)
}

/// Test-only fault points around publication. Thread-local, so a test only
/// affects publications on its own thread.
#[cfg(test)]
pub(super) mod hooks {
    use std::cell::{Cell, RefCell};
    use std::path::Path;

    use crate::{HawDBError, Result};

    type BeforePublish = Box<dyn FnOnce(&Path)>;

    thread_local! {
        static BEFORE_PUBLISH: RefCell<Option<BeforePublish>> = const { RefCell::new(None) };
        static FAIL_PARENT_SYNC: Cell<u32> = const { Cell::new(0) };
    }

    /// Runs `hook` with the destination immediately before the no-replace
    /// move of the next publication on this thread.
    pub(crate) fn set_before_publish(hook: impl FnOnce(&Path) + 'static) {
        BEFORE_PUBLISH.with(|slot| *slot.borrow_mut() = Some(Box::new(hook)));
    }

    /// Fails the next `count` post-move parent-directory syncs on this thread.
    pub(crate) fn fail_parent_sync(count: u32) {
        FAIL_PARENT_SYNC.with(|slot| slot.set(count));
    }

    pub(super) fn before_publish(destination: &Path) {
        let hook = BEFORE_PUBLISH.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook(destination);
        }
    }

    pub(super) fn parent_sync_failure() -> Result<()> {
        FAIL_PARENT_SYNC.with(|slot| match slot.get() {
            0 => Ok(()),
            remaining => {
                slot.set(remaining - 1);
                Err(HawDBError::Storage(
                    "injected parent directory sync failure".to_string(),
                ))
            }
        })
    }
}

pub(super) fn sync_directory(path: &Path) -> Result<()> {
    crate::sync_bootstrap_directory(path)
}

/// A JSON Lines (or single JSON document) file being written into a stage,
/// hashed as it is written.
pub(super) struct HashedWriter {
    relative: String,
    path: PathBuf,
    writer: BufWriter<File>,
    hasher: IntegrityHasher,
    bytes: u64,
    records: u64,
}

impl HashedWriter {
    pub(super) fn create(root: &Path, relative: String) -> Result<Self> {
        let path = root.join(&relative);
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| io_error("create", &path, error))?;
        Ok(Self {
            relative,
            path,
            writer: BufWriter::with_capacity(1 << 16, file),
            hasher: IntegrityHasher::new(),
            bytes: 0,
            records: 0,
        })
    }

    pub(super) fn records(&self) -> u64 {
        self.records
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer
            .write_all(bytes)
            .map_err(|error| io_error("write", &self.path, error))?;
        self.hasher.update(bytes);
        self.bytes += bytes.len() as u64;
        Ok(())
    }

    pub(super) fn write_record(&mut self, record: &serde_json::Value) -> Result<()> {
        let mut line = serde_json::to_vec(record).map_err(|error| {
            HawDBError::Storage(format!("logical dump: encode record: {error}"))
        })?;
        if line.len() as u64 >= MAX_RECORD_BYTES {
            return Err(HawDBError::Storage(format!(
                "logical dump: a record in {} encodes to {} bytes, above the {MAX_RECORD_BYTES}-byte record limit",
                self.relative,
                line.len()
            )));
        }
        line.push(b'\n');
        self.write_bytes(&line)?;
        self.records += 1;
        Ok(())
    }

    pub(super) fn write_document(&mut self, document: &impl serde::Serialize) -> Result<()> {
        let mut bytes = serde_json::to_vec_pretty(document).map_err(|error| {
            HawDBError::Storage(format!("logical dump: encode {}: {error}", self.relative))
        })?;
        bytes.push(b'\n');
        self.write_bytes(&bytes)
    }

    /// Flushes and synchronizes the file and returns its manifest entry.
    pub(super) fn finish(self, kind: FileKind, table: Option<String>) -> Result<FileEntry> {
        let file = self
            .writer
            .into_inner()
            .map_err(|error| io_error("flush", &self.path, error.into_error()))?;
        file.sync_all()
            .map_err(|error| io_error("synchronize", &self.path, error))?;
        Ok(FileEntry {
            path: self.relative,
            kind,
            table,
            records: if kind == FileKind::Schema {
                0
            } else {
                self.records
            },
            bytes: self.bytes,
            sha256: self.hasher.finish().sha256.to_string(),
        })
    }
}

/// A dump-relative path from the manifest, accepted only in the shapes this
/// format writes: `schema.json` or `data/<name>` with a plain file name.
pub(super) fn resolve_entry_path(root: &Path, relative: &str) -> Result<PathBuf> {
    let valid_name = |name: &str| {
        !name.is_empty()
            && name != "."
            && name != ".."
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    };
    let valid = match relative.split_once('/') {
        None => valid_name(relative),
        Some((super::model::DATA_DIR, name)) => valid_name(name),
        Some(_) => false,
    };
    if !valid {
        return Err(malformed(format!(
            "manifest file path {relative:?} is not a dump-relative file name"
        )));
    }
    Ok(root.join(relative))
}

/// Reads a whole metadata document, refusing oversized files.
pub(super) fn read_metadata(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(|error| io_error("open", path, error))?;
    let mut bytes = Vec::new();
    file.take(MAX_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| io_error("read", path, error))?;
    if bytes.len() as u64 > MAX_METADATA_BYTES {
        return Err(malformed(format!(
            "{} exceeds the {MAX_METADATA_BYTES}-byte metadata limit",
            path.display()
        )));
    }
    Ok(bytes)
}

/// Verifies a whole-file entry (length and SHA-256) before it is used.
pub(super) fn verify_bytes(entry: &FileEntry, bytes: &[u8]) -> Result<()> {
    let mut hasher = IntegrityHasher::new();
    hasher.update(bytes);
    let sha256 = hasher.finish().sha256.to_string();
    if bytes.len() as u64 != entry.bytes || sha256 != entry.sha256 {
        return Err(checksum_mismatch(entry));
    }
    Ok(())
}

fn checksum_mismatch(entry: &FileEntry) -> HawDBError {
    malformed(format!(
        "{} does not match its manifest length and SHA-256; the dump is damaged or incomplete",
        entry.path
    ))
}

/// Streams the records of one data file. The caller must call
/// [`RecordReader::finish`] after the last record: the file's length, record
/// count and SHA-256 are only known to match at the end, and the import is
/// published only after every file finished.
pub(super) struct RecordReader<'a> {
    entry: &'a FileEntry,
    path: PathBuf,
    reader: BufReader<File>,
    hasher: IntegrityHasher,
    bytes: u64,
    records: u64,
    line: Vec<u8>,
}

impl<'a> RecordReader<'a> {
    pub(super) fn open(root: &Path, entry: &'a FileEntry) -> Result<Self> {
        let path = resolve_entry_path(root, &entry.path)?;
        let file = File::open(&path).map_err(|error| io_error("open", &path, error))?;
        Ok(Self {
            entry,
            path,
            reader: BufReader::with_capacity(1 << 16, file),
            hasher: IntegrityHasher::new(),
            bytes: 0,
            records: 0,
            line: Vec::new(),
        })
    }

    pub(super) fn next_record(&mut self) -> Result<Option<serde_json::Value>> {
        self.line.clear();
        let read = (&mut self.reader)
            .take(MAX_RECORD_BYTES + 1)
            .read_until(b'\n', &mut self.line)
            .map_err(|error| io_error("read", &self.path, error))?;
        if read == 0 {
            return Ok(None);
        }
        self.hasher.update(&self.line);
        self.bytes += read as u64;
        if self.bytes > self.entry.bytes {
            return Err(checksum_mismatch(self.entry));
        }
        if self.line.last() != Some(&b'\n') {
            return Err(malformed(format!(
                "{} ends with an incomplete record",
                self.entry.path
            )));
        }
        self.records += 1;
        serde_json::from_slice(&self.line[..self.line.len() - 1])
            .map(Some)
            .map_err(|error| {
                malformed(format!(
                    "{} record {} is not valid JSON: {error}",
                    self.entry.path, self.records
                ))
            })
    }

    pub(super) fn finish(self) -> Result<()> {
        let sha256 = self.hasher.finish().sha256.to_string();
        if self.bytes != self.entry.bytes || sha256 != self.entry.sha256 {
            return Err(checksum_mismatch(self.entry));
        }
        if self.records != self.entry.records {
            return Err(malformed(format!(
                "{} holds {} records; the manifest records {}",
                self.entry.path, self.records, self.entry.records
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
