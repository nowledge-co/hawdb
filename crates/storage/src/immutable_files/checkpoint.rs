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

use super::*;
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};

#[derive(Debug)]
pub(crate) enum CheckpointImmutableFileError {
    Io(io::Error),
    Work(CheckpointWorkError),
}

impl From<CheckpointWorkError> for CheckpointImmutableFileError {
    fn from(error: CheckpointWorkError) -> Self {
        Self::Work(error)
    }
}

impl CheckpointImmutableFileError {
    fn io(error: io::Error, work: &CheckpointWorkContext) -> Self {
        match hawdb_core::error::file_descriptor_error(&error) {
            Some(error) => {
                Self::Work(work.record_failure(CheckpointWorkError::FileDescriptors(error)))
            }
            None => Self::Io(error),
        }
    }
}

impl ImmutableFileHandles {
    fn try_checkpoint_opening(
        &self,
        work: &CheckpointWorkContext,
    ) -> Result<std::sync::MutexGuard<'_, ()>, CheckpointImmutableFileError> {
        match self.opening.try_lock() {
            Ok(opening) => Ok(opening),
            Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => Err(work
                .record_failure(CheckpointWorkError::Contended("immutable file validation"))
                .into()),
        }
    }

    /// A cold captured read validates the entire object in bounded waves. A
    /// failed or stopped validation never publishes a partially verified handle.
    /// Cache/registration allocations still need separate lifetime admission.
    pub(crate) fn checkpoint_get(
        &self,
        binding: &ImmutableFileBinding,
        context: &FileOpenContext,
        work: &CheckpointWorkContext,
    ) -> Result<Arc<File>, CheckpointImmutableFileError> {
        {
            let unit = work.start_unit()?;
            let cached = self.cached(binding.reference);
            unit.finish();
            work.checkpoint()?;
            if let Some(file) = cached {
                self.state.record_cache_hit();
                return Ok(file);
            }
        }
        // Protect validation from retirement, but release the opening lock
        // before scratch admission, native reads and hashes. Foreground reads
        // can open other objects while this task yields between bounded units.
        let _validation = {
            let unit = work.start_unit()?;
            let opening = self.try_checkpoint_opening(work)?;
            let validation = match self.checkpoint_validation.try_read() {
                Ok(validation) => validation,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    return Err(work
                        .record_failure(CheckpointWorkError::Contended("immutable file retirement"))
                        .into());
                }
            };
            let cached = self.cached(binding.reference);
            drop(opening);
            unit.finish();
            work.checkpoint()?;
            if let Some(file) = cached {
                self.state.record_cache_hit();
                return Ok(file);
            }
            validation
        };
        self.state.record_cache_miss();
        // Admit the hash scratch before opening a native descriptor. Include the
        // final EOF probe even for an empty immutable object.
        let scratch_length = binding.reference.byte_length.clamp(1, 64 * 1024) as usize;
        let mut scratch = CheckpointBytes::zeroed(scratch_length, work)?;
        let mut file = {
            let unit = work.start_unit()?;
            let _wave = work.io_wave()?;
            let file = OpenOptions::new()
                .read(true)
                .descriptor_kind(DescriptorKind::ImmutableCache)
                .open_with_context(&binding.object_path, context)
                .map_err(|error| CheckpointImmutableFileError::io(error, work))?;
            let metadata = file
                .metadata()
                .map_err(|error| CheckpointImmutableFileError::io(error, work))?;
            if !metadata.is_file() || metadata.len() != binding.reference.byte_length {
                return Err(CheckpointImmutableFileError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "immutable handle identity length mismatch",
                )));
            }
            unit.finish();
            file
        };
        let mut hasher = crate::immutable_object::identity_hasher(
            binding.reference.kind,
            binding.reference.format_version,
            binding.reference.byte_length,
        );
        let mut read_bytes = 0_u64;
        loop {
            let read = {
                let unit = work.start_unit()?;
                let _wave = work.io_wave()?;
                let read = file
                    .read(scratch.as_mut_slice())
                    .map_err(|error| CheckpointImmutableFileError::io(error, work))?;
                unit.finish();
                read
            };
            if read == 0 {
                break;
            }
            let unit = work.start_unit()?;
            read_bytes = read_bytes.checked_add(read as u64).ok_or_else(|| {
                CheckpointImmutableFileError::Io(io::Error::other("immutable file length overflow"))
            })?;
            if read_bytes > binding.reference.byte_length {
                return Err(CheckpointImmutableFileError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "immutable handle grew during validation",
                )));
            }
            hasher.update(&scratch[..read]);
            unit.finish();
        }
        {
            let unit = work.start_unit()?;
            if read_bytes != binding.reference.byte_length
                || hasher.finish().sha256 != binding.reference.sha256
            {
                return Err(CheckpointImmutableFileError::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "immutable handle identity digest mismatch",
                )));
            }
            unit.finish();
        }
        work.checkpoint()?;
        drop(scratch);
        let unit = work.start_unit()?;
        let mut candidate = Some(file);
        let file = {
            let _opening = self.try_checkpoint_opening(work)?;
            let mut handles = self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(handle) = handles.get_mut(&binding.reference) {
                handle.last_used = self.next_tick();
                handle.file.clone()
            } else {
                let file = Arc::new(candidate.take().expect("validated candidate is present"));
                handles.insert(
                    binding.reference,
                    CachedHandle {
                        file: file.clone(),
                        last_used: self.next_tick(),
                    },
                );
                file
            }
        };
        // A foreground reader may have published the same verified identity.
        // Close the redundant native file after releasing both cache locks.
        drop(candidate);
        unit.finish();
        work.checkpoint()?;
        Ok(file)
    }
}
