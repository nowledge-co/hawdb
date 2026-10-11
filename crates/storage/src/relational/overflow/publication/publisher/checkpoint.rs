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

//! Fixed primitive/file blocks for the admitted checkpoint publisher. Input
//! retention, final-owner destruction and cleanup debt need separate hard
//! accounting; these controls do not qualify complete candidate resources.

use super::*;
use crate::background::CheckpointWorkError;
use crate::file_io::PublicationLockGuard;

pub(in crate::relational::overflow::publication) const BLOCK_BYTES: usize = 64 * 1024;

fn work_error(error: CheckpointWorkError) -> RelationalOverflowPublicationError {
    RelationalOverflowPublicationError::Admission(error.to_string())
}

pub(in crate::relational::overflow::publication) fn check(
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalOverflowPublicationError> {
    work.map_or(Ok(()), |work| work.checkpoint().map_err(work_error))
}

// Callers may use these only for a bounded primitive, never a traversal or
// complete variable-width artifact. I/O waves end before nested builders.
pub(in crate::relational::overflow::publication) fn cpu<T>(
    work: Option<&CheckpointWorkContext>,
    operation: impl FnOnce() -> Result<T, RelationalOverflowPublicationError>,
) -> Result<T, RelationalOverflowPublicationError> {
    let Some(work) = work else {
        return operation();
    };
    let unit = work.start_unit().map_err(work_error)?;
    let result = operation()?;
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

pub(in crate::relational::overflow::publication) fn io<T>(
    work: Option<&CheckpointWorkContext>,
    operation: impl FnOnce() -> Result<T, RelationalOverflowPublicationError>,
) -> Result<T, RelationalOverflowPublicationError> {
    let Some(work) = work else {
        return operation();
    };
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    work.checkpoint().map_err(work_error)?;
    let result = operation()?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

pub(in crate::relational::overflow::publication) fn integrity(
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<hawdb_integrity::IntegrityDigest, RelationalOverflowPublicationError> {
    match work {
        Some(work) => work.integrity(bytes).map_err(work_error),
        None => Ok(integrity_digest(bytes)),
    }
}

pub(in crate::relational::overflow::publication) fn write(
    file: &mut File,
    bytes: &[u8],
    context: &'static str,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalOverflowPublicationError> {
    if work.is_none() {
        return file.write_all(bytes).map_err(durability(context));
    }
    for block in bytes.chunks(BLOCK_BYTES) {
        io(work, || file.write_all(block).map_err(durability(context)))?;
    }
    check(work)
}

pub(in crate::relational::overflow::publication) fn update(
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalOverflowPublicationError> {
    if work.is_none() {
        hasher.update(bytes);
        return Ok(());
    }
    for block in bytes.chunks(BLOCK_BYTES) {
        cpu(work, || {
            hasher.update(block);
            Ok(())
        })?;
    }
    check(work)
}

pub(in crate::relational::overflow::publication) fn sort(
    inputs: &mut [RelationalOverflowExtentInput],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalOverflowPublicationError> {
    let Some(work) = work else {
        inputs.sort_by_key(|input| input.reference().digest);
        return Ok(());
    };
    // In-place heapsort needs no full-sized output buffer. Each unit compares
    // at most three fixed-size digests and swaps one fixed-size input record.
    for parent in (0..inputs.len() / 2).rev() {
        sift(inputs, parent, inputs.len(), work)?;
    }
    for end in (1..inputs.len()).rev() {
        cpu(Some(work), || {
            inputs.swap(0, end);
            Ok(())
        })?;
        sift(inputs, 0, end, work)?;
    }
    check(Some(work))
}

fn sift(
    inputs: &mut [RelationalOverflowExtentInput],
    mut parent: usize,
    end: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalOverflowPublicationError> {
    while parent < end / 2 {
        let next = cpu(Some(work), || {
            let left = parent * 2 + 1;
            let right = left + 1;
            let child = if right < end
                && inputs[right].reference().digest > inputs[left].reference().digest
            {
                right
            } else {
                left
            };
            if inputs[parent].reference().digest >= inputs[child].reference().digest {
                return Ok(None);
            }
            inputs.swap(parent, child);
            Ok(Some(child))
        })?;
        let Some(child) = next else {
            break;
        };
        parent = child;
    }
    check(Some(work))
}

pub(in crate::relational::overflow::publication) fn descriptor(
    base: &RelationalOverflowRootReader,
    file: &mut File,
    ordinal: u64,
    work: Option<&CheckpointWorkContext>,
) -> Result<RelationalOverflowExtentDescriptor, RelationalOverflowPublicationError> {
    // The ordinary reader reads and verifies exactly one 120-byte descriptor.
    io(work, || base.read_descriptor_from(file, ordinal))
}

pub(in crate::relational::overflow::publication) fn publish(
    source: &Path,
    destination: &Path,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalOverflowPublicationError> {
    io(work, || durable_publish_immutable(source, destination))
}

pub(in crate::relational::overflow::publication) fn lock(
    directory: &Path,
    work: Option<&CheckpointWorkContext>,
) -> Result<PublicationLockGuard, RelationalOverflowPublicationError> {
    if work.is_none() {
        return acquire_publication_lock(directory).map(PublicationLockGuard::new);
    }
    io(work, || {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE))
            .map_err(durability("open overflow publication lock"))?;
        match lock.try_lock() {
            Ok(()) => Ok(PublicationLockGuard::new(lock)),
            Err(std::fs::TryLockError::WouldBlock) => {
                Err(RelationalOverflowPublicationError::Admission(
                    "overflow publication lock is busy".into(),
                ))
            }
            Err(std::fs::TryLockError::Error(error)) => {
                Err(durability("lock overflow publication")(error))
            }
        }
    })
}
