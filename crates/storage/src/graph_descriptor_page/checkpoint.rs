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

//! Checkpoint pages retain admitted bytes and entry ranges. Validation borrows
//! every field instead of constructing and caching a second decoded page.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointBytes, CheckpointValues, CheckpointWorkContext,
    CheckpointWorkError,
};
use crate::graph_descriptor_tree::GraphDescriptorTreeError;
use std::ops::Range;

pub(crate) struct CheckpointDescriptorPage {
    bytes: CheckpointBytes,
    entries: CheckpointValues<Range<usize>>,
    leaf: bool,
}

pub(crate) struct CheckpointDescriptorChild {
    pub(crate) reference: GraphDescriptorPageRef,
    _memory: CheckpointAllocationOwner,
}

impl CheckpointDescriptorPage {
    pub(crate) fn decode_bound(
        reference: &GraphDescriptorPageRef,
        kind: GraphDescriptorKind,
        epoch: u64,
        bytes: CheckpointBytes,
        limits: GraphDescriptorPageLimits,
        work: &CheckpointWorkContext,
    ) -> Result<Self, GraphDescriptorTreeError> {
        let unit = work.start_unit()?;
        validate_page_ref(reference, limits, ErrorClass::Corrupt)?;
        if bytes.len() < PAGE_HEADER_BYTES
            || bytes.len() as u64 != reference.length.get()
            || &bytes[..8] != PAGE_MAGIC
            || read_u16(&bytes[8..10]) != PAGE_VERSION
            || bytes[11] != kind.tag()
            || read_u64(&bytes[12..20]) != reference.physical_generation
            || read_u64(&bytes[20..28]) != epoch
            || read_u64(&bytes[28..36]) != reference.page_id.get()
            || read_u64(&bytes[40..48]) != (bytes.len() - PAGE_HEADER_BYTES) as u64
            || read_u32(&bytes[48..52]) != reference.content_crc32c.get()
            || &bytes[52..84] != reference.content_sha256.as_bytes()
        {
            return Err(corrupt(
                "checkpoint descriptor page does not match its reference",
            ));
        }
        let leaf = match GraphDescriptorPageKind::from_tag(bytes[10])? {
            GraphDescriptorPageKind::Leaf => true,
            GraphDescriptorPageKind::Interior => false,
        };
        let count = read_u32(&bytes[36..40]) as usize;
        if count == 0 {
            return Err(corrupt("checkpoint descriptor page must not be empty"));
        }
        validate_entry_count(count, limits, ErrorClass::Admission)?;
        // Even the shortest valid entry needs two nonempty byte strings.
        let minimum = if leaf { 16 } else { 92 };
        if count > (bytes.len() - PAGE_HEADER_BYTES) / minimum {
            return Err(corrupt(
                "descriptor page count exceeds its complete field framing",
            ));
        }
        unit.finish();
        let mut hasher = IntegrityHasher::new();
        for range in [&bytes[..48], &bytes[PAGE_HEADER_BYTES..]] {
            for block in range.chunks(64 * 1024) {
                let unit = work.start_unit()?;
                hasher.update(block);
                unit.finish();
            }
        }
        let digest = hasher.finish();
        if digest.crc32c != reference.content_crc32c || digest.sha256 != reference.content_sha256 {
            return Err(corrupt("checkpoint descriptor page checksum mismatch"));
        }
        let mut entries = CheckpointValues::new(count, work)?;
        let mut fields = Fields::new(&bytes[PAGE_HEADER_BYTES..]);
        let mut offset = PAGE_HEADER_BYTES;
        let mut previous: Option<&[u8]> = None;
        let mut lower = None;
        loop {
            let unit = work.start_unit()?;
            let Some(field) = fields.next() else {
                unit.finish();
                break;
            };
            let (tag, payload) = field?;
            let start = offset + FIELD_HEADER_BYTES;
            offset = start + payload.len();
            if tag
                != if leaf {
                    LEAF_ENTRY_FIELD
                } else {
                    INTERIOR_ENTRY_FIELD
                }
            {
                unit.finish();
                continue;
            }
            if leaf {
                let (key, value) = leaf_entry(payload, limits)?;
                validate_key(key, limits, ErrorClass::Corrupt, "leaf key")?;
                if value.is_empty() || previous.is_some_and(|previous| previous >= key) {
                    return Err(corrupt(
                        "checkpoint leaf descriptors are empty or unordered",
                    ));
                }
                lower.get_or_insert(key);
                previous = Some(key);
            } else {
                let child = BorrowedReference::decode(payload, limits)?;
                if child.physical_generation > reference.physical_generation
                    || previous.is_some_and(|previous| previous >= child.lower)
                {
                    return Err(corrupt("checkpoint interior child ranges are unordered or from a future generation"));
                }
                lower.get_or_insert(child.lower);
                previous = Some(child.upper);
            }
            ensure_not_overdeclared(count, entries.as_slice().len() + 1)?;
            unit.finish();
            entries.push(start..offset, work)?;
        }
        ensure_entry_count(count, entries.as_slice().len())?;
        if lower != Some(reference.lower_bound.as_slice())
            || previous != Some(reference.upper_bound.as_slice())
        {
            return Err(corrupt(
                "checkpoint descriptor page bounds do not match its reference",
            ));
        }
        work.checkpoint()?;
        Ok(Self {
            bytes,
            entries,
            leaf,
        })
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.leaf
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.as_slice().len()
    }

    pub(crate) fn lower_bound(
        &self,
        target: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<(usize, u64), GraphDescriptorTreeError> {
        let mut low = 0;
        let mut high = self.len();
        let mut comparisons = 0;
        while low < high {
            let unit = work.start_unit()?;
            let middle = low + (high - low) / 2;
            let key = if self.leaf {
                self.leaf_entry(middle)?.0
            } else {
                self.child_bounds(middle)?.1
            };
            if key < target {
                low = middle + 1;
            } else {
                high = middle;
            }
            comparisons += 1;
            unit.finish();
        }
        work.checkpoint()?;
        Ok((low, comparisons))
    }

    pub(crate) fn leaf_entry(
        &self,
        index: usize,
    ) -> Result<(&[u8], &[u8]), GraphDescriptorTreeError> {
        leaf_entry(
            self.entry(index)?,
            GraphDescriptorPageLimits {
                max_key_bytes: NonZeroUsize::MAX,
                max_value_bytes: NonZeroUsize::MAX,
                ..Default::default()
            },
        )
        .map_err(Into::into)
    }

    pub(crate) fn child_bounds(
        &self,
        index: usize,
    ) -> Result<(&[u8], &[u8]), GraphDescriptorTreeError> {
        let entry = self.entry(index)?;
        let (lower, offset) = decode_bytes(entry, 76, usize::MAX, "child lower bound")?;
        let (upper, _) = decode_bytes(entry, offset, usize::MAX, "child upper bound")?;
        Ok((lower, upper))
    }

    pub(crate) fn child(
        &self,
        index: usize,
        limits: GraphDescriptorPageLimits,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointDescriptorChild, GraphDescriptorTreeError> {
        let unit = work.start_unit()?;
        let child = BorrowedReference::decode(self.entry(index)?, limits)?;
        unit.finish();
        let mut memory = CheckpointAllocationOwner::default();
        let lower_bound = copy_bound(child.lower, &mut memory, work)?;
        let upper_bound = copy_bound(child.upper, &mut memory, work)?;
        work.checkpoint()?;
        Ok(CheckpointDescriptorChild {
            reference: GraphDescriptorPageRef {
                artifact_id: child.artifact_id,
                physical_generation: child.physical_generation,
                page_id: child.page_id,
                offset: child.offset,
                length: child.length,
                content_crc32c: child.content_crc32c,
                content_sha256: child.content_sha256,
                lower_bound,
                upper_bound,
            },
            _memory: memory,
        })
    }

    fn entry(&self, index: usize) -> Result<&[u8], GraphDescriptorTreeError> {
        let range = self
            .entries
            .as_slice()
            .get(index)
            .ok_or_else(|| corrupt("checkpoint descriptor index out of bounds"))?;
        Ok(&self.bytes[range.clone()])
    }
}

fn leaf_entry(
    bytes: &[u8],
    limits: GraphDescriptorPageLimits,
) -> Result<(&[u8], &[u8]), GraphDescriptorPageError> {
    let (key, offset) = decode_bytes(bytes, 0, limits.max_key_bytes.get(), "leaf key")?;
    let (value, end) = decode_bytes(
        bytes,
        offset,
        limits.max_value_bytes.get(),
        "descriptor value",
    )?;
    if end != bytes.len() {
        return Err(GraphDescriptorPageError::Corrupt(
            "leaf entry contains trailing bytes".into(),
        ));
    }
    Ok((key, value))
}

struct BorrowedReference<'a> {
    artifact_id: u64,
    physical_generation: u64,
    page_id: GraphDescriptorPageId,
    offset: u64,
    length: NonZeroU64,
    content_crc32c: Crc32c,
    content_sha256: Sha256Digest,
    lower: &'a [u8],
    upper: &'a [u8],
}

impl<'a> BorrowedReference<'a> {
    fn decode(
        bytes: &'a [u8],
        limits: GraphDescriptorPageLimits,
    ) -> Result<Self, GraphDescriptorTreeError> {
        if bytes.len() < 76 {
            return Err(corrupt("truncated checkpoint descriptor child reference"));
        }
        let artifact_id = read_u64(&bytes[..8]);
        let physical_generation = read_u64(&bytes[8..16]);
        let page_id = page_id(read_u64(&bytes[16..24]), "child page id")?;
        let offset = read_u64(&bytes[24..32]);
        let length = NonZeroU64::new(read_u64(&bytes[32..40]))
            .ok_or_else(|| corrupt("child page length must be nonzero"))?;
        let (lower, next) =
            decode_bytes(bytes, 76, limits.max_key_bytes.get(), "child lower bound")?;
        let (upper, end) =
            decode_bytes(bytes, next, limits.max_key_bytes.get(), "child upper bound")?;
        validate_key(lower, limits, ErrorClass::Corrupt, "child lower bound")?;
        validate_key(upper, limits, ErrorClass::Corrupt, "child upper bound")?;
        if artifact_id == 0
            || physical_generation == 0
            || end != bytes.len()
            || length.get() < PAGE_HEADER_BYTES as u64
            || length.get() > limits.max_page_bytes.get() as u64
            || offset.checked_add(length.get()).is_none()
            || lower > upper
        {
            return Err(corrupt("invalid checkpoint descriptor child reference"));
        }
        Ok(Self {
            artifact_id,
            physical_generation,
            page_id,
            offset,
            length,
            content_crc32c: Crc32c::new(read_u32(&bytes[40..44])),
            content_sha256: Sha256Digest::from_bytes(
                bytes[44..76].try_into().expect("fixed child digest"),
            ),
            lower,
            upper,
        })
    }
}

fn copy_bound(
    bytes: &[u8],
    memory: &mut CheckpointAllocationOwner,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, CheckpointWorkError> {
    let unit = work.start_unit()?;
    memory.reserve(bytes.len(), work)?;
    let mut output = Vec::new();
    output.try_reserve_exact(bytes.len()).map_err(|error| {
        work.record_failure(CheckpointWorkError::Allocation {
            bytes: bytes.len() as u64,
            reason: error.to_string(),
        })
    })?;
    if output.capacity() != bytes.len() {
        return Err(work.record_failure(CheckpointWorkError::Allocation {
            bytes: bytes.len() as u64,
            reason: "descriptor bound allocation exceeded admitted capacity".into(),
        }));
    }
    unit.finish();
    for chunk in bytes.chunks(64 * 1024) {
        let unit = work.start_unit()?;
        output.extend_from_slice(chunk);
        unit.finish();
    }
    work.checkpoint()?;
    Ok(output)
}

fn corrupt(message: &str) -> GraphDescriptorTreeError {
    GraphDescriptorTreeError::Page(GraphDescriptorPageError::Corrupt(message.into()))
}

#[cfg(test)]
mod tests;
