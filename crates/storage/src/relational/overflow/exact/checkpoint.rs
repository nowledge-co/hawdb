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

//! Controlled iteration over an already sorted reference set. Sorting/spill
//! construction, actual capacities, FD retention and final-owner cleanup still
//! need hard candidate accounting; this does not qualify complete sort work.

use super::*;
use crate::background::CheckpointWorkContext;

type Head = (RelationalOverflowRef, usize);
type Visitor<'a> =
    dyn FnMut(RelationalOverflowRef) -> Result<bool, RelationalOverflowPublicationError> + 'a;

fn work_error(error: crate::background::CheckpointWorkError) -> RelationalOverflowPublicationError {
    RelationalOverflowPublicationError::Admission(error.to_string())
}

fn cpu<T>(
    work: &CheckpointWorkContext,
    operation: impl FnOnce() -> Result<T, RelationalOverflowPublicationError>,
) -> Result<T, RelationalOverflowPublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let result = operation()?;
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

fn io<T>(
    work: &CheckpointWorkContext,
    operation: impl FnOnce() -> Result<T, RelationalOverflowPublicationError>,
) -> Result<T, RelationalOverflowPublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    let result = operation()?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

pub(super) fn visit(
    set: &RelationalOverflowReferenceSet,
    visitor: &mut Visitor<'_>,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalOverflowPublicationError> {
    work.checkpoint().map_err(work_error)?;
    match &set.source {
        ReferenceSetSource::Memory(references) => {
            for reference in references {
                let reference = cpu(work, || Ok(*reference))?;
                // Visitors can run nested builders; retain no CPU/I/O permit.
                if !visitor(reference)? {
                    work.checkpoint().map_err(work_error)?;
                    return Ok(());
                }
                work.checkpoint().map_err(work_error)?;
            }
        }
        ReferenceSetSource::Spilled { paths, config } => {
            let buffer_bytes = run_reader_buffer_bytes(*config);
            let (mut readers, mut heap) = cpu(work, || {
                Ok((
                    Vec::with_capacity(paths.len()),
                    Vec::with_capacity(paths.len()),
                ))
            })?;
            for path in paths {
                let reader = io(work, || RunReader::open(path, buffer_bytes))?;
                cpu(work, || {
                    readers.push(reader);
                    Ok(())
                })?;
            }
            for (run, reader) in readers.iter_mut().enumerate() {
                // One fixed 60-byte record can refill the capped 8 KiB reader
                // at most twice. The parser performs fixed-width validation.
                if let Some(reference) = io(work, || reader.next_reference())? {
                    push(&mut heap, (reference, run), work)?;
                }
            }
            let mut previous: Option<RelationalOverflowRef> = None;
            while let Some((reference, run)) = pop(&mut heap, work)? {
                let duplicate = cpu(work, || match previous {
                    Some(previous)
                        if previous.digest == reference.digest && previous != reference =>
                    {
                        Err(RelationalOverflowPublicationError::Corrupt(format!(
                            "overflow digest {} has conflicting reference metadata",
                            reference.digest
                        )))
                    }
                    Some(previous) if previous == reference => Ok(true),
                    _ => Ok(false),
                })?;
                if !duplicate {
                    if !visitor(reference)? {
                        work.checkpoint().map_err(work_error)?;
                        return Ok(());
                    }
                    work.checkpoint().map_err(work_error)?;
                    previous = Some(reference);
                }
                if let Some(next) = io(work, || readers[run].next_reference())? {
                    push(&mut heap, (next, run), work)?;
                }
            }
        }
    }
    work.checkpoint().map_err(work_error)
}

fn push(
    heap: &mut Vec<Head>,
    head: Head,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalOverflowPublicationError> {
    let mut child = cpu(work, || {
        heap.push(head);
        Ok(heap.len() - 1)
    })?;
    while child > 0 {
        let parent = (child - 1) / 2;
        let swapped = cpu(work, || {
            if heap[parent] <= heap[child] {
                return Ok(false);
            }
            heap.swap(parent, child);
            Ok(true)
        })?;
        if !swapped {
            break;
        }
        child = parent;
    }
    Ok(())
}

fn pop(
    heap: &mut Vec<Head>,
    work: &CheckpointWorkContext,
) -> Result<Option<Head>, RelationalOverflowPublicationError> {
    let head = cpu(work, || {
        if heap.is_empty() {
            return Ok(None);
        }
        let last = heap.len() - 1;
        heap.swap(0, last);
        Ok(heap.pop())
    })?;
    let mut parent = 0;
    while parent < heap.len() / 2 {
        let next = cpu(work, || {
            let left = parent * 2 + 1;
            let right = left + 1;
            let child = if right < heap.len() && heap[right] < heap[left] {
                right
            } else {
                left
            };
            if heap[parent] <= heap[child] {
                return Ok(None);
            }
            heap.swap(parent, child);
            Ok(Some(child))
        })?;
        let Some(child) = next else { break };
        parent = child;
    }
    Ok(head)
}

#[cfg(test)]
mod tests;
