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

//! Cooperative hooks for the shared row-page encoder. Hard allocation and
//! resource qualification remain separate from the common wire/error grammar.

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};
use crate::relational::ordered_key::{
    encode_ordered_relational_key_with_work_context, CheckpointKeyEncodeError,
};

pub(super) fn work_error(error: CheckpointWorkError) -> RelationalRowPageError {
    RelationalRowPageError::Admission(error.to_string())
}

pub(super) fn ordered_key(
    key: &RelationalKey,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPageError> {
    encode_ordered_relational_key_with_work_context(key, work).map_err(|error| match error {
        CheckpointKeyEncodeError::Key(crate::relational::RelationalError::Corruption(message)) => {
            RelationalRowPageError::Admission(message)
        }
        CheckpointKeyEncodeError::Key(error) => {
            RelationalRowPageError::Admission(error.to_string())
        }
        CheckpointKeyEncodeError::Work(error) => work_error(error),
    })
}

pub(super) fn compare(
    left: &[u8],
    right: &[u8],
    work: &CheckpointWorkContext,
) -> Result<std::cmp::Ordering, RelationalRowPageError> {
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        let unit = work.start_unit().map_err(work_error)?;
        let result = left.cmp(right);
        unit.finish();
        if result != std::cmp::Ordering::Equal {
            return Ok(result);
        }
    }
    let unit = work.start_unit().map_err(work_error)?;
    let result = left.len().cmp(&right.len());
    unit.finish();
    Ok(result)
}

pub(super) fn append(
    output: &mut Vec<u8>,
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPageError> {
    // Reserve before copies so block writes never recopy a growing complete
    // buffer. The allocator and actual capacities still require hard admission.
    let unit = work.start_unit().map_err(work_error)?;
    output.reserve(bytes.len());
    unit.finish();
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        output.extend_from_slice(block);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn encode(
    page: &ImmutableRelationalRowPage,
    limits: RelationalRowPageLimits,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPageError> {
    super::encoding::encode(page, limits, &super::encoding::ControlledWork(work))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod golden_tests;
