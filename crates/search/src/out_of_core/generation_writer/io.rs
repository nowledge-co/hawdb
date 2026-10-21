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

//! One publication's paths, native scratch and bounded file transfer buffers.

use super::artifact_name::Name;
use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::{
    checked_add, path::OwnedPath, reserved::native_path, BuildMemory, SPOOL_BUFFER_BYTES,
};
use crate::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use hawdb_integrity::Crc32cHasher;
use hawdb_storage::durability::durable_replace_file;
use hawdb_storage::file_io::{self as fs, File, OpenOptions};
use hawdb_storage::immutable_object::{ObjectKind, ObjectReference, ObjectReferenceWriter};
#[cfg(test)]
use std::cell::Cell;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

#[cfg(test)]
thread_local! {
    static FAIL_REPLACE_AT: Cell<u64> = const { Cell::new(0) };
    static ABORT_REPLACE_AT: Cell<Option<u64>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(super) fn fail_replace_at(replace_number: u64) {
    FAIL_REPLACE_AT.with(|remaining| remaining.set(replace_number));
}

#[cfg(test)]
fn should_fail_replace() -> bool {
    FAIL_REPLACE_AT.with(|remaining| {
        let current = remaining.get();
        if current == 0 {
            false
        } else if current == 1 {
            remaining.set(0);
            true
        } else {
            remaining.set(current - 1);
            false
        }
    })
}

#[cfg(test)]
fn should_abort_replace() -> bool {
    ABORT_REPLACE_AT.with(|remaining| {
        let current = remaining.get().unwrap_or_else(|| {
            std::env::var("HAWDB_TEST_ABORT_REPLACE_AT")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0)
        });
        if current == 0 {
            remaining.set(Some(0));
            false
        } else if current == 1 {
            remaining.set(Some(0));
            true
        } else {
            remaining.set(Some(current - 1));
            false
        }
    })
}

pub(in crate::out_of_core) struct GenerationIo<'a> {
    memory: &'a BuildMemory,
    task: &'a RuntimeTaskContext,
}

// A producer-captured full object reference. It describes the bytes but
// establishes no source authority or durability/activation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::out_of_core) struct ArtifactSeal {
    pub(in crate::out_of_core) length: u64,
    pub(in crate::out_of_core) reference: ObjectReference,
}

impl<'a> GenerationIo<'a> {
    pub(in crate::out_of_core) fn new(
        memory: &'a BuildMemory,
        task: &'a RuntimeTaskContext,
    ) -> Self {
        Self { memory, task }
    }

    pub(super) fn path(&self, root: &Path, name: &Path) -> Result<OwnedPath> {
        OwnedPath::join(root, name, self.memory, self.task)
    }

    pub(super) fn native<T>(&self, paths: &[&Path], work: impl FnOnce() -> T) -> Result<T> {
        checkpoint(self.task)?;
        let bytes = paths.iter().try_fold(0, |bytes, path| {
            checked_add(bytes, native_path::bytes(path)?)
        })?;
        let _scratch = self.memory.spool.reserve(bytes)?;
        // A caller returning allocated data must admit that payload separately.
        // No checkpoint follows a syscall: a successful active-manifest replace
        // is the commit fence, even if cancellation arrives during its sync.
        Ok(work())
    }

    pub(super) fn read(&self, path: &Path, max_bytes: u64) -> Result<AdmittedBytes> {
        let mut file = self.native(&[path], || File::open(path))??;
        let length = file.metadata()?.len();
        if length > max_bytes {
            return Err(HawDBError::Storage(format!(
                "search generation file requires {length} bytes, exceeding {max_bytes}"
            )));
        }
        let length = usize::try_from(length).map_err(|_| {
            HawDBError::Storage("search generation file length exceeds usize".into())
        })?;
        let memory = self.memory.spool.reserve(length)?;
        let _scratch = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut output = AdmittedBytes {
            bytes: Vec::new(),
            _memory: memory,
        };
        output.bytes.try_reserve_exact(length).map_err(|error| {
            HawDBError::Execution(format!("search generation file allocation failed: {error}"))
        })?;
        if output.bytes.capacity() > length {
            return Err(HawDBError::Execution(
                "search generation file exceeds admission".into(),
            ));
        }
        let mut buffer = [0u8; SPOOL_BUFFER_BYTES];
        loop {
            checkpoint(self.task)?;
            let read = read_chunk(&mut file, &mut buffer, self.task)?;
            if read == 0 {
                break;
            }
            if read > length - output.bytes.len() {
                return Err(HawDBError::Storage(
                    "search generation file grew during read".into(),
                ));
            }
            output.bytes.extend_from_slice(&buffer[..read]);
        }
        checkpoint(self.task)?;
        if output.bytes.len() != length {
            return Err(HawDBError::Storage(
                "search generation file shrank during read".into(),
            ));
        }
        Ok(output)
    }

    pub(super) fn checksum(&self, path: &Path) -> Result<(u64, u64)> {
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut file = self.native(&[path], || File::open(path))??;
        let expected = file.metadata()?.len();
        let (length, checksum) = checksum_reader(&mut file, self.task)?;
        if length != expected {
            return Err(HawDBError::Storage(format!(
                "search generation artifact {} changed while checksumming",
                path.display()
            )));
        }
        Ok((length, checksum))
    }

    /// Read one held file to EOF under admitted bounded scratch. The producer
    /// seals staged bytes before installation; verification must not recompute
    /// its expectation from an installed pathname or an alias inventory.
    pub(in crate::out_of_core) fn seal(&self, path: &Path) -> Result<ArtifactSeal> {
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut file = self.native(&[path], || File::open(path))??;
        self.seal_file_admitted(&mut file)
    }

    /// Capture the staged checksum and producer reference from the same read.
    pub(super) fn seal_with_checksum(&self, path: &Path) -> Result<(ArtifactSeal, u64)> {
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut file = self.native(&[path], || File::open(path))??;
        let expected = file.metadata()?.len();
        let mut checksum = Crc32cHasher::new();
        let seal = seal_reader(&mut file, expected, self.task, Some(&mut checksum))?;
        Ok((seal, checksum.finish()))
    }

    /// Seal the producer's still-held candidate, before namespace installation.
    /// The caller retains exclusive write custody; the cursor finishes at EOF.
    pub(in crate::out_of_core) fn seal_file(&self, file: &mut File) -> Result<ArtifactSeal> {
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        self.seal_file_admitted(file)
    }

    fn seal_file_admitted(&self, file: &mut File) -> Result<ArtifactSeal> {
        file.seek(SeekFrom::Start(0))?;
        let expected = file.metadata()?.len();
        seal_reader(file, expected, self.task, None)
    }

    pub(in crate::out_of_core) fn verify_object(
        &self,
        path: &Path,
        object: super::super::ArtifactObject,
    ) -> Result<()> {
        if self.seal(path)?.reference != object.reference() {
            return Err(HawDBError::StorageIntegrity(
                "installed Search artifact differs from its producer reference".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn verify_seal(
        &self,
        path: &Path,
        expected: ArtifactSeal,
        name: &str,
    ) -> Result<()> {
        if self.seal(path)? != expected {
            return Err(HawDBError::StorageIntegrity(format!(
                "published search generation {name} does not match its staged byte seal"
            )));
        }
        Ok(())
    }

    /// Check both identities in one bounded read of the same installed file.
    /// Keep the producer reference independent of the installed bytes: native
    /// identity alone cannot detect in-place damage or support copy fallback.
    pub(super) fn verify_artifact(
        &self,
        path: &Path,
        expected_len: u64,
        object: super::super::ArtifactObject,
        expected_checksum: Option<u64>,
        name: &str,
    ) -> Result<()> {
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut file = self.native(&[path], || File::open(path))??;
        let length = file.metadata()?.len();
        if length != expected_len {
            return Err(HawDBError::Storage(format!(
                "published search generation {name} does not match its staged artifact"
            )));
        }
        let mut checksum = expected_checksum.map(|_| Crc32cHasher::new());
        let seal = seal_reader(&mut file, length, self.task, checksum.as_mut())?;
        if checksum.map(|digest| digest.finish()) != expected_checksum {
            return Err(HawDBError::Storage(format!(
                "published search generation {name} does not match its staged artifact"
            )));
        }
        if seal.reference != object.reference() {
            return Err(HawDBError::StorageIntegrity(
                "installed Search artifact differs from its producer reference".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn link(&self, source: &Path, target: &Path) -> Result<()> {
        self.link_with(source, target, |source, temporary| {
            fs::hard_link(source, temporary)
        })
    }

    fn link_with(
        &self,
        source: &Path,
        target: &Path,
        hard_link: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
    ) -> Result<()> {
        let mut temporary = Temporary::new(target, self.memory, self.task)?;
        if self
            .native(&[source, &temporary.path], || {
                hard_link(source, &temporary.path)
            })?
            .is_err()
        {
            self.copy(source, &temporary.path)?;
        }
        self.replace(&mut temporary, target)
    }

    fn copy(&self, source: &Path, target: &Path) -> Result<()> {
        // Admit the transfer buffer before opening or truncating its output.
        let _buffer = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut source = self.native(&[source], || File::open(source))??;
        let metadata = source.metadata()?;
        if !metadata.is_file() {
            return Err(HawDBError::Storage(
                "search publication source is not a file".into(),
            ));
        }
        let mut output = self.native(&[target], || {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(target)
        })??;
        let copied = copy_reader(&mut source, &mut output, self.task)?;
        if copied != metadata.len() {
            return Err(HawDBError::Storage(
                "search publication source length changed during copy".into(),
            ));
        }
        checkpoint(self.task)?;
        output.set_permissions(metadata.permissions())?;
        output.sync_all()?;
        Ok(())
    }

    pub(super) fn write(&self, target: &Path, bytes: &[u8]) -> Result<()> {
        let mut temporary = Temporary::new(target, self.memory, self.task)?;
        {
            let mut file = self.native(&[&temporary.path], || File::create(&temporary.path))??;
            CheckedWriter::new(&mut file, Some(self.task)).write_all(bytes)?;
            checkpoint(self.task)?;
            file.sync_all()?;
        }
        self.replace(&mut temporary, target)
    }

    fn replace(&self, temporary: &mut Temporary, target: &Path) -> Result<()> {
        #[cfg(test)]
        if should_abort_replace() {
            std::process::abort();
        }
        #[cfg(test)]
        if should_fail_replace() {
            return Err(crate::HawDBError::Storage(
                "injected publication replace failure".into(),
            ));
        }
        self.replace_with(temporary, target, durable_replace_file)
    }

    fn replace_with(
        &self,
        temporary: &mut Temporary,
        target: &Path,
        replace: impl FnOnce(&Path, &Path) -> std::io::Result<()>,
    ) -> Result<()> {
        self.native(&[&temporary.path, target], || {
            replace(&temporary.path, target)
        })??;
        temporary.armed = false;
        Ok(())
    }
}

pub(super) struct AdmittedBytes {
    pub(super) bytes: Vec<u8>,
    _memory: QueryMemoryLease,
}

struct Temporary {
    path: OwnedPath,
    // Cleanup must not need a new admission after another operation exhausts
    // the root, or while unwinding from a cancellation or I/O failure.
    _cleanup_scratch: QueryMemoryLease,
    armed: bool,
}

impl Temporary {
    fn new(target: &Path, memory: &BuildMemory, task: &RuntimeTaskContext) -> Result<Self> {
        let sequence = crate::out_of_core::CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let extension = Name::temporary_extension(sequence, memory, task)?;
        let path = OwnedPath::with_extension(target, &extension, memory, task)?;
        let cleanup_scratch = memory.spool.reserve(native_path::bytes(&path)?)?;
        Ok(Self {
            path,
            _cleanup_scratch: cleanup_scratch,
            armed: true,
        })
    }
}

impl Drop for Temporary {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn seal_reader(
    reader: &mut impl Read,
    expected: u64,
    task: &RuntimeTaskContext,
    mut checksum: Option<&mut Crc32cHasher>,
) -> Result<ArtifactSeal> {
    let mut hasher = ObjectReferenceWriter::new(ObjectKind::CheckpointArtifact, 1, expected)
        .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?;
    let mut buffer = [0u8; SPOOL_BUFFER_BYTES];
    let mut length = 0u64;
    loop {
        let count = read_chunk(reader, &mut buffer, task)?;
        if count == 0 {
            break;
        }
        length = length
            .checked_add(count as u64)
            .filter(|length| *length <= expected)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("search artifact grew while sealing".into())
            })?;
        hasher.write_all(&buffer[..count])?;
        if let Some(checksum) = &mut checksum {
            checksum.update(&buffer[..count]);
        }
    }
    checkpoint(task)?;
    if length != expected {
        return Err(HawDBError::StorageIntegrity(
            "search artifact shrank while sealing".into(),
        ));
    }
    Ok(ArtifactSeal {
        length,
        reference: hasher
            .finish()
            .map_err(|error| HawDBError::StorageIntegrity(error.to_string()))?,
    })
}

fn checksum_reader(reader: &mut impl Read, task: &RuntimeTaskContext) -> Result<(u64, u64)> {
    let mut checksum = Crc32cHasher::new();
    let mut length = 0u64;
    let mut buffer = [0u8; SPOOL_BUFFER_BYTES];
    loop {
        checkpoint(task)?;
        let read = read_chunk(reader, &mut buffer, task)?;
        if read == 0 {
            break;
        }
        length = length.checked_add(read as u64).ok_or_else(|| {
            HawDBError::Storage("search artifact checksum length overflow".into())
        })?;
        checksum.update(&buffer[..read]);
    }
    checkpoint(task)?;
    Ok((length, checksum.finish()))
}

fn copy_reader(
    reader: &mut impl Read,
    writer: &mut impl Write,
    task: &RuntimeTaskContext,
) -> Result<u64> {
    let mut buffer = [0u8; SPOOL_BUFFER_BYTES];
    let mut writer = CheckedWriter::new(writer, Some(task));
    let mut length = 0u64;
    loop {
        checkpoint(task)?;
        let read = read_chunk(reader, &mut buffer, task)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| HawDBError::Storage("search artifact copy length overflow".into()))?;
    }
    checkpoint(task)?;
    Ok(length)
}

fn read_chunk(
    reader: &mut impl Read,
    buffer: &mut [u8],
    task: &RuntimeTaskContext,
) -> Result<usize> {
    loop {
        checkpoint(task)?;
        match reader.read(buffer) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return Ok(result?),
        }
    }
}

#[cfg(test)]
mod tests;
