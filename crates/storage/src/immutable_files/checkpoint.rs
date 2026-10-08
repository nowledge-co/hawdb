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
            let cached = self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&binding.reference)
                .cloned();
            unit.finish();
            work.checkpoint()?;
            if let Some(file) = cached {
                self.state.record_cache_hit();
                return Ok(file);
            }
        }
        // Do not block a checkpoint task behind another object's validation.
        // Retain the existing opening/retirement serialization while validating;
        // cancellation releases it. Foreground lock latency remains a separate
        // qualification gate until opening ownership is shared incrementally.
        let _opening = {
            let unit = work.start_unit()?;
            let opening = match self.opening.try_lock() {
                Ok(opening) => opening,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => {
                    return Err(work
                        .record_failure(CheckpointWorkError::Contended("immutable file validation"))
                        .into());
                }
            };
            let cached = self
                .handles
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&binding.reference)
                .cloned();
            unit.finish();
            work.checkpoint()?;
            if let Some(file) = cached {
                self.state.record_cache_hit();
                return Ok(file);
            }
            opening
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
        let file = Arc::new(file);
        self.handles
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(binding.reference, file.clone());
        unit.finish();
        work.checkpoint()?;
        Ok(file)
    }
}
