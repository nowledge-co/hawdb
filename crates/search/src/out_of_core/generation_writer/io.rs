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
use hawdb_storage::durable_replace_file;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering;

pub(super) struct GenerationIo<'a> {
    memory: &'a BuildMemory,
    task: &'a RuntimeTaskContext,
}

impl<'a> GenerationIo<'a> {
    pub(super) fn new(memory: &'a BuildMemory, task: &'a RuntimeTaskContext) -> Self {
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

    pub(super) fn length(&self, path: &Path) -> Result<u64> {
        Ok(self.native(&[path], || fs::metadata(path))??.len())
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

    pub(super) fn verify(
        &self,
        path: &Path,
        expected_len: u64,
        expected_checksum: Option<u64>,
        name: &str,
    ) -> Result<()> {
        let actual_len = self.length(path)?;
        let checksum_matches = match expected_checksum {
            Some(expected) => self.checksum(path)?.1 == expected,
            None => true,
        };
        if actual_len != expected_len || !checksum_matches {
            return Err(HawDBError::Storage(format!(
                "published search generation {name} does not match its staged artifact"
            )));
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
