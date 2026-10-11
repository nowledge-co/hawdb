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

//! Validate borrowed property keys without a source-sized uncharged tree or
//! unbounded variable-width comparisons. Digest collisions remain exact checks.

use super::{CanonicalSegmentError, CheckpointWorkContext, Sha256Digest};
use crate::background::CheckpointValues;

const INLINE_KEYS: usize = 32;
const SORT_KEYS: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Key {
    digest: Sha256Digest,
    index: usize,
}

const EMPTY: Key = Key {
    digest: Sha256Digest::from_bytes([0; 32]),
    index: 0,
};

pub(super) fn validate(
    keys: &[String],
    work: &CheckpointWorkContext,
) -> Result<(), CanonicalSegmentError> {
    if keys.len() <= INLINE_KEYS {
        let mut values = [EMPTY; INLINE_KEYS];
        for (index, key) in keys.iter().enumerate() {
            let digest = work.integrity(key.as_bytes())?.sha256;
            let unit = work.start_unit()?;
            values[index] = Key { digest, index };
            unit.finish();
        }
        let values = &mut values[..keys.len()];
        let unit = work.start_unit()?;
        values.sort_unstable();
        unit.finish();
        return validate_sorted(keys, values, work);
    }

    let mut values = CheckpointValues::new(keys.len(), work)?;
    for (index, key) in keys.iter().enumerate() {
        let digest = work.integrity(key.as_bytes())?.sha256;
        values.push(Key { digest, index }, work)?;
    }
    for chunk in values.as_mut_slice().chunks_mut(SORT_KEYS) {
        let unit = work.start_unit()?;
        // Only fixed-width digest/index keys are compared. The chunk count
        // bounds both the sort's CPU and its allocation-free recursion stack.
        chunk.sort_unstable();
        unit.finish();
        work.checkpoint()?;
    }

    if keys.len() > SORT_KEYS {
        let mut scratch = CheckpointValues::new(keys.len(), work)?;
        for _ in 0..keys.len() {
            scratch.push(EMPTY, work)?;
        }
        let mut width = SORT_KEYS;
        while width < keys.len() {
            let stride = width.saturating_mul(2);
            for start in (0..keys.len()).step_by(stride) {
                let middle = start.saturating_add(width).min(keys.len());
                let end = start.saturating_add(stride).min(keys.len());
                let (mut left, mut right) = (start, middle);
                let input = values.as_slice();
                for chunk in scratch.as_mut_slice()[start..end].chunks_mut(SORT_KEYS) {
                    let unit = work.start_unit()?;
                    for output in chunk {
                        if right == end || (left < middle && input[left] <= input[right]) {
                            *output = input[left];
                            left += 1;
                        } else {
                            *output = input[right];
                            right += 1;
                        }
                    }
                    unit.finish();
                    work.checkpoint()?;
                }
                debug_assert_eq!(left, middle);
                debug_assert_eq!(right, end);
            }
            std::mem::swap(&mut values, &mut scratch);
            width = stride;
        }
        // Scratch is destroyed, then its exact-capacity inventory is released.
        drop(scratch);
    }
    validate_sorted(keys, values.as_slice(), work)
}

fn validate_sorted(
    keys: &[String],
    values: &[Key],
    work: &CheckpointWorkContext,
) -> Result<(), CanonicalSegmentError> {
    let mut start = 0;
    while start < values.len() {
        let mut end = start + 1;
        while end < values.len() {
            let unit = work.start_unit()?;
            let same = values[start].digest == values[end].digest;
            unit.finish();
            work.checkpoint()?;
            if !same {
                break;
            }
            end += 1;
        }
        // A digest is a sorting key, never evidence of equality. Check every
        // preceding key in the collision group, including nonadjacent repeats.
        for right in start + 1..end {
            for left in start..right {
                if equal(
                    keys[values[left].index].as_bytes(),
                    keys[values[right].index].as_bytes(),
                    work,
                )? {
                    return Err(CanonicalSegmentError::Corrupt(
                        "canonical manifest property keys are not unique".into(),
                    ));
                }
            }
        }
        start = end;
    }
    work.checkpoint()?;
    Ok(())
}

fn equal(
    left: &[u8],
    right: &[u8],
    work: &CheckpointWorkContext,
) -> Result<bool, CanonicalSegmentError> {
    let unit = work.start_unit()?;
    let same_len = left.len() == right.len();
    unit.finish();
    work.checkpoint()?;
    if !same_len {
        return Ok(false);
    }
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        let unit = work.start_unit()?;
        let same = left == right;
        unit.finish();
        work.checkpoint()?;
        if !same {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
