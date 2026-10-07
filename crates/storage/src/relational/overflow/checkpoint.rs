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
//! Map insertion/lookup, capacity allocation and final-owner destruction still
//! need hard resource/time bounds; a field boundary is not a complete bound.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use crate::relational::{RelationalOverflowExtentInput, RelationalRowPageTableDelta};
use std::collections::BTreeMap;

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

type References = BTreeMap<Sha256Digest, RelationalOverflowRef>;

fn collect_row(
    row: &RelationalRow,
    references: &mut References,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    // Include empty rows and non-overflow values in the admitted traversal.
    cpu(work, || Ok(()))?;
    for value in row.values.iter() {
        cpu(work, || {
            if let RelationalValue::Overflow(reference) = value
                && let Some(previous) = references.insert(reference.digest, *reference)
                && previous != *reference
            {
                return Err(RelationalError::Corruption(format!(
                    "overflow digest {} has conflicting reference metadata",
                    reference.digest
                )));
            }
            Ok(())
        })?;
    }
    Ok(())
}

pub(in crate::relational) fn generation_inputs(
    state: &RelationalState,
    has_base_generation: bool,
    max_materialized_bytes: usize,
    work: &CheckpointWorkContext,
) -> Result<Vec<RelationalOverflowExtentInput>, RelationalError> {
    cpu(work, || {
        state.require_materialized_rows("relational overflow checkpoint")
    })?;
    let mut references = References::new();
    for segment in state.segments.values() {
        cpu(work, || Ok(()))?;
        for row in segment.rows.values() {
            collect_row(row, &mut references, work)?;
        }
    }
    cpu(work, || {
        if references.len() != state.overflow_segments.len() {
            return Err(RelationalError::Corruption(
                "relational overflow segments do not match the reachable row closure".into(),
            ));
        }
        Ok(())
    })?;
    for digest in references.keys() {
        cpu(work, || {
            if !state.overflow_segments.contains_key(digest) {
                return Err(RelationalError::Corruption(
                    "relational overflow segments do not match the reachable row closure".into(),
                ));
            }
            Ok(())
        })?;
    }
    let mut inputs = cpu(work, || Ok(Vec::with_capacity(references.len())))?;
    let mut materialized_bytes = 0usize;
    for (digest, reference) in references {
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
                RelationalOverflowExtentInput::Write { reference, encoded }
            }
        };
        cpu(work, || {
            inputs.push(input);
            Ok(())
        })?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(inputs)
}

pub(in crate::relational) fn delta_inputs(
    state: &RelationalState,
    deltas: &[RelationalRowPageTableDelta],
    work: &CheckpointWorkContext,
) -> Result<Vec<RelationalOverflowExtentInput>, RelationalError> {
    cpu(work, || {
        state.require_sparse_workspace_source("relational overflow delta checkpoint")
    })?;
    let mut references = References::new();
    for delta in deltas {
        cpu(work, || Ok(()))?;
        for page in &delta.dirty_pages {
            cpu(work, || Ok(()))?;
            for row in &page.rows {
                collect_row(&row.row, &mut references, work)?;
            }
        }
    }
    let mut inputs = cpu(work, || Ok(Vec::with_capacity(references.len())))?;
    for (digest, reference) in references {
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
        cpu(work, || {
            inputs.push(input);
            Ok(())
        })?;
    }
    work.checkpoint().map_err(work_error)?;
    Ok(inputs)
}

#[cfg(test)]
mod tests;
