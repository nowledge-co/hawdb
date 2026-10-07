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

//! Cooperative collection for ordinary and metadata-only checkpoint inputs.
//! Reference and input capacity is admitted before allocation. Reference sorting
//! uses fixed-size comparison/swap units without allocating map nodes or scratch.
//! Source retention and allocator/destruction latency remain separate bounds.

use super::*;
use crate::background::{CheckpointValues, CheckpointWorkContext, CheckpointWorkError};
use crate::relational::{
    RelationalOverflowExtentInput, RelationalOverflowInputs, RelationalRowPageTableDelta,
};

fn work_error(error: CheckpointWorkError) -> RelationalError {
    RelationalError::Admission(error.to_string())
}

fn cpu<T>(
    work: &CheckpointWorkContext,
    operation: impl FnOnce() -> Result<T, RelationalError>,
) -> Result<T, RelationalError> {
    let unit = work.start_unit().map_err(work_error)?;
    let result = operation()?;
    unit.finish();
    work.checkpoint().map_err(work_error)?;
    Ok(result)
}

struct References {
    values: CheckpointValues<RelationalOverflowRef>,
    distinct: usize,
}

impl References {
    fn new(capacity: usize, work: &CheckpointWorkContext) -> Result<Self, RelationalError> {
        Ok(Self {
            values: CheckpointValues::new(capacity, work).map_err(work_error)?,
            distinct: 0,
        })
    }

    fn sorted_distinct(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> Result<&[RelationalOverflowRef], RelationalError> {
        let values = self.values.as_mut_slice();
        let length = values.len();
        // In-place heap sort requires no second database-sized allocation.
        // Every sift step compares at most three fixed-size references and
        // swaps at most one pair; no unit represents the entire heap or sort.
        for root in (0..length / 2).rev() {
            sift_down(values, root, length, work)?;
        }
        for end in (1..values.len()).rev() {
            cpu(work, || {
                values.swap(0, end);
                Ok(())
            })?;
            sift_down(values, 0, end, work)?;
        }
        let mut distinct = 0;
        for index in 0..values.len() {
            cpu(work, || {
                let reference = values[index];
                if distinct != 0 && values[distinct - 1].digest == reference.digest {
                    if values[distinct - 1] != reference {
                        return Err(RelationalError::Corruption(format!(
                            "overflow digest {} has conflicting reference metadata",
                            reference.digest
                        )));
                    }
                } else {
                    values[distinct] = reference;
                    distinct += 1;
                }
                Ok(())
            })?;
        }
        self.distinct = distinct;
        work.checkpoint().map_err(work_error)?;
        Ok(&values[..distinct])
    }

    fn as_slice(&self) -> &[RelationalOverflowRef] {
        &self.values.as_slice()[..self.distinct]
    }
}

fn sift_down(
    values: &mut [RelationalOverflowRef],
    mut root: usize,
    end: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    while let Some(left) = root
        .checked_mul(2)
        .and_then(|index| index.checked_add(1))
        .filter(|index| *index < end)
    {
        let next = cpu(work, || {
            let right = left + 1;
            let child = if right < end && values[left].digest < values[right].digest {
                right
            } else {
                left
            };
            if values[root].digest < values[child].digest {
                values.swap(root, child);
                Ok(Some(child))
            } else {
                Ok(None)
            }
        })?;
        let Some(next) = next else { break };
        root = next;
    }
    Ok(())
}

fn count_row(
    row: &RelationalRow,
    capacity: &mut usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    cpu(work, || Ok(()))?;
    for value in row.values.iter() {
        cpu(work, || {
            if matches!(value, RelationalValue::Overflow(_)) {
                *capacity = capacity.checked_add(1).ok_or_else(|| {
                    RelationalError::Admission(
                        "checkpoint overflow reference count overflows usize".into(),
                    )
                })?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

fn collect_row(
    row: &RelationalRow,
    references: &mut References,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    // Include empty rows and non-overflow values in the admitted traversal.
    cpu(work, || Ok(()))?;
    for value in row.values.iter() {
        if let RelationalValue::Overflow(reference) = value {
            references
                .values
                .push(*reference, work)
                .map_err(work_error)?;
        } else {
            cpu(work, || Ok(()))?;
        }
    }
    Ok(())
}

pub(in crate::relational) fn generation_inputs(
    state: &RelationalState,
    has_base_generation: bool,
    max_materialized_bytes: usize,
    work: &CheckpointWorkContext,
) -> Result<RelationalOverflowInputs, RelationalError> {
    cpu(work, || {
        state.require_materialized_rows("relational overflow checkpoint")
    })?;
    let mut capacity = 0;
    for segment in state.segments.values() {
        cpu(work, || Ok(()))?;
        for row in segment.rows.values() {
            count_row(row, &mut capacity, work)?;
        }
    }
    let mut references = References::new(capacity, work)?;
    for segment in state.segments.values() {
        cpu(work, || Ok(()))?;
        for row in segment.rows.values() {
            collect_row(row, &mut references, work)?;
        }
    }
    let sorted = references.sorted_distinct(work)?;
    cpu(work, || {
        if sorted.len() != state.overflow_segments.len() {
            return Err(RelationalError::Corruption(
                "relational overflow segments do not match the reachable row closure".into(),
            ));
        }
        Ok(())
    })?;
    for reference in sorted {
        cpu(work, || {
            if !state.overflow_segments.contains_key(&reference.digest) {
                return Err(RelationalError::Corruption(
                    "relational overflow segments do not match the reachable row closure".into(),
                ));
            }
            Ok(())
        })?;
    }
    let mut inputs =
        CheckpointValues::new(references.as_slice().len(), work).map_err(work_error)?;
    let mut materialized_bytes = 0usize;
    for &reference in references.as_slice() {
        let digest = reference.digest;
        let segment = cpu(work, || {
            state.overflow_segments.get(&digest).ok_or_else(|| {
                RelationalError::Corruption(format!(
                    "missing overflow segment for reachable digest {digest}"
                ))
            })
        })?;
        let input = match segment {
            RelationalOverflowSegment::Inline(encoded) => cpu(work, || {
                Ok(RelationalOverflowExtentInput::Write {
                    reference,
                    encoded: Arc::clone(encoded),
                })
            })?,
            RelationalOverflowSegment::FileRange { .. } if has_base_generation => {
                cpu(work, || Ok(RelationalOverflowExtentInput::Reuse(reference)))?
            }
            RelationalOverflowSegment::FileRange { reader, range } => {
                let encoded = reader
                    .checkpoint_range_with_work_context(range, work)
                    .map_err(|error| match error {
                        crate::scan::CheckpointRangeReadError::Read(error) => {
                            RelationalError::Corruption(format!(
                                "failed to read file-backed overflow segment: {error}"
                            ))
                        }
                        crate::scan::CheckpointRangeReadError::Work(error) => work_error(error),
                    })?;
                // Preserve the ordinary read-error and accumulated-byte order.
                cpu(work, || {
                    materialized_bytes =
                        materialized_bytes
                            .checked_add(encoded.len())
                            .ok_or_else(|| {
                                RelationalError::Admission(
                                    "checkpoint overflow materialization byte count overflow"
                                        .into(),
                                )
                            })?;
                    if materialized_bytes > max_materialized_bytes {
                        return Err(RelationalError::Admission(format!(
                            "checkpoint overflow materialization uses {materialized_bytes} bytes, exceeding limit {max_materialized_bytes}"
                        )));
                    }
                    Ok(())
                })?;
                let encoded = work.arc_bytes(&encoded).map_err(work_error)?;
                RelationalOverflowExtentInput::CheckpointWrite { reference, encoded }
            }
        };
        inputs.push(input, work).map_err(work_error)?;
    }
    drop(references);
    work.checkpoint().map_err(work_error)?;
    inputs
        .share(work)
        .map(RelationalOverflowInputs::checkpoint)
        .map_err(work_error)
}

pub(in crate::relational) fn delta_inputs(
    state: &RelationalState,
    deltas: &[RelationalRowPageTableDelta],
    work: &CheckpointWorkContext,
) -> Result<RelationalOverflowInputs, RelationalError> {
    cpu(work, || {
        state.require_sparse_workspace_source("relational overflow delta checkpoint")
    })?;
    let mut capacity = 0;
    for delta in deltas {
        cpu(work, || Ok(()))?;
        for page in &delta.dirty_pages {
            cpu(work, || Ok(()))?;
            for row in &page.rows {
                count_row(&row.row, &mut capacity, work)?;
            }
        }
    }
    let mut references = References::new(capacity, work)?;
    for delta in deltas {
        cpu(work, || Ok(()))?;
        for page in &delta.dirty_pages {
            cpu(work, || Ok(()))?;
            for row in &page.rows {
                collect_row(&row.row, &mut references, work)?;
            }
        }
    }
    let sorted = references.sorted_distinct(work)?;
    let mut inputs = CheckpointValues::new(sorted.len(), work).map_err(work_error)?;
    for &reference in sorted {
        let digest = reference.digest;
        let input = cpu(work, || {
            Ok(match state.overflow_segments.get(&digest) {
                Some(RelationalOverflowSegment::Inline(encoded)) => {
                    RelationalOverflowExtentInput::Write {
                        reference,
                        encoded: Arc::clone(encoded),
                    }
                }
                Some(RelationalOverflowSegment::FileRange { .. }) | None => {
                    RelationalOverflowExtentInput::Reuse(reference)
                }
            })
        })?;
        inputs.push(input, work).map_err(work_error)?;
    }
    drop(references);
    work.checkpoint().map_err(work_error)?;
    inputs
        .share(work)
        .map(RelationalOverflowInputs::checkpoint)
        .map_err(work_error)
}

#[cfg(test)]
mod tests;
