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

//! Actual file/publication operations for an admitted checkpoint task.
//! A completed rename remains recovery evidence when its reply is lost.
//! Retained memory, disk/FD ownership and cleanup debt need a shared ledger.

use super::*;
use crate::background::CheckpointWorkContext;

pub(super) fn cpu<T>(
    work: Option<&CheckpointWorkContext>,
    operation: impl FnOnce() -> Result<T, RelationalRowPagePublicationError>,
) -> Result<T, RelationalRowPagePublicationError> {
    let Some(work) = work else {
        return operation();
    };
    let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
    let result = operation()?;
    unit.finish();
    work.checkpoint().map_err(root::checkpoint::work_error)?;
    Ok(result)
}

pub(in crate::relational::row_page::publication) fn sort_dirty_pages(
    pages: &mut [PreparedDirtyPage],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    let Some(work) = work else {
        pages.sort_by(|left, right| {
            left.descriptor
                .lower_bound
                .cmp(&right.descriptor.lower_bound)
        });
        return Ok(());
    };
    // Valid dirty pages have distinct non-overlapping bounds. Equal bounds
    // remain an error after sorting, so stable tie order cannot select data.
    for parent in (0..pages.len() / 2).rev() {
        sift(pages, parent, pages.len(), work)?;
    }
    for end in (1..pages.len()).rev() {
        cpu(Some(work), || {
            pages.swap(0, end);
            Ok(())
        })?;
        sift(pages, 0, end, work)?;
    }
    work.checkpoint().map_err(root::checkpoint::work_error)
}

fn sift(
    pages: &mut [PreparedDirtyPage],
    mut parent: usize,
    end: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    while parent < end / 2 {
        let left = parent * 2 + 1;
        let right = left + 1;
        let child = if right < end
            && root::checkpoint::compare(
                &pages[right].descriptor.lower_bound,
                &pages[left].descriptor.lower_bound,
                work,
            )?
            .is_gt()
        {
            right
        } else {
            left
        };
        if !root::checkpoint::compare(
            &pages[parent].descriptor.lower_bound,
            &pages[child].descriptor.lower_bound,
            work,
        )?
        .is_lt()
        {
            break;
        }
        cpu(Some(work), || {
            pages.swap(parent, child);
            Ok(())
        })?;
        parent = child;
    }
    work.checkpoint().map_err(root::checkpoint::work_error)
}

pub(super) fn io<T>(
    work: Option<&CheckpointWorkContext>,
    operation: impl FnOnce() -> Result<T, RelationalRowPagePublicationError>,
) -> Result<T, RelationalRowPagePublicationError> {
    let Some(work) = work else {
        return operation();
    };
    let unit = work.start_unit().map_err(root::checkpoint::work_error)?;
    let wave = work.io_wave().map_err(root::checkpoint::work_error)?;
    work.checkpoint().map_err(root::checkpoint::work_error)?;
    let result = operation()?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(root::checkpoint::work_error)?;
    Ok(result)
}

pub(super) fn lock(
    directory: &Path,
    work: Option<&CheckpointWorkContext>,
) -> Result<File, RelationalRowPagePublicationError> {
    if work.is_none() {
        return acquire_publication_lock(directory);
    }
    io(work, || {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join(RELATIONAL_ROW_PAGE_PUBLICATION_LOCK_FILE))
            .map_err(durability("open row-page publication lock"))?;
        match lock.try_lock() {
            Ok(()) => Ok(lock),
            Err(std::fs::TryLockError::WouldBlock) => {
                Err(RelationalRowPagePublicationError::Admission(
                    "row-page publication lock is busy".into(),
                ))
            }
            Err(std::fs::TryLockError::Error(error)) => {
                Err(durability("lock row-page publication")(error))
            }
        }
    })
}

pub(super) fn create(
    temporary: &mut OwnedTemporaryArtifacts,
    path: &Path,
    context: &'static str,
    work: Option<&CheckpointWorkContext>,
) -> Result<File, RelationalRowPagePublicationError> {
    io(work, || temporary.create(path, context))
}

pub(super) fn write_synced(
    temporary: &mut OwnedTemporaryArtifacts,
    path: &Path,
    bytes: &[u8],
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    let Some(work) = work else {
        return super::write_synced(temporary, path, bytes);
    };
    let mut file = create(temporary, path, "create row-page candidate", Some(work))?;
    for block in bytes.chunks(64 * 1024) {
        io(Some(work), || {
            file.write_all(block)
                .map_err(durability("write row-page candidate"))
        })?;
    }
    io(Some(work), || {
        file.sync_all()
            .map_err(durability("sync row-page candidate"))
    })
}

pub(super) fn publish(
    source: &Path,
    destination: &Path,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    // Once publication starts, complete its existing synchronization barrier.
    // Cancellation afterward can lose a reply but cannot undo its rename.
    io(work, || durable_publish_immutable(source, destination))
}

pub(super) fn select(
    source: &Path,
    destination: &Path,
    work: Option<&CheckpointWorkContext>,
) -> Result<(), RelationalRowPagePublicationError> {
    io(work, || {
        durable_replace_file(source, destination)
            .map_err(durability("publish latest row-page manifest"))
    })
}
