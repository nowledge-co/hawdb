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

//! Private, memory-only representation of a fully verified descriptor page.
//! The original image, entry offsets and codec-limit maxima share one cache
//! charge. Only strongly bound `SegmentCache::get_verified` leases may enter
//! the warm constructor; ordinary cache insertion cannot establish this proof.

use super::*;
use crate::cache::SegmentBytes;

const OFFSET_BYTES: usize = 8;
const MAXIMA_BYTES: usize = 24;

pub(crate) fn encode_verified_view(
    reference: &GraphDescriptorPageRef,
    kind: GraphDescriptorKind,
    epoch: u64,
    mut encoded: Vec<u8>,
    limits: GraphDescriptorPageLimits,
) -> Result<Vec<u8>, GraphDescriptorPageError> {
    let page =
        ImmutableGraphDescriptorPage::decode_bound(reference, kind, epoch, &encoded, limits)?;
    let (tag, max_key, max_value, max_child_bytes) = match &page.body {
        ImmutableGraphDescriptorPageBody::Leaf(entries) => (
            LEAF_ENTRY_FIELD,
            entries
                .iter()
                .map(|entry| entry.key.len())
                .max()
                .unwrap_or(0),
            entries
                .iter()
                .map(|entry| entry.value.len())
                .max()
                .unwrap_or(0),
            0,
        ),
        ImmutableGraphDescriptorPageBody::Interior(entries) => (
            INTERIOR_ENTRY_FIELD,
            entries
                .iter()
                .map(|entry| {
                    entry
                        .child
                        .lower_bound
                        .len()
                        .max(entry.child.upper_bound.len())
                })
                .max()
                .unwrap_or(0),
            0,
            entries
                .iter()
                .map(|entry| entry.child.length.get())
                .max()
                .unwrap_or(0),
        ),
    };
    let mut offsets = Vec::new();
    let mut offset = PAGE_HEADER_BYTES;
    for field in Fields::new(&encoded[PAGE_HEADER_BYTES..]) {
        let (field_tag, payload) = field?;
        if field_tag == tag {
            offsets.extend_from_slice(&(offset as u64).to_le_bytes());
        }
        offset += FIELD_HEADER_BYTES + payload.len();
    }
    // Keep opaque extension fields in the authenticated image, but index only
    // the declared entries, just like the full page codec.
    encoded.extend_from_slice(&offsets);
    encoded.extend_from_slice(&(max_key as u64).to_le_bytes());
    encoded.extend_from_slice(&(max_value as u64).to_le_bytes());
    encoded.extend_from_slice(&max_child_bytes.to_le_bytes());
    Ok(encoded)
}

pub(crate) struct GraphDescriptorPageView {
    bytes: SegmentBytes,
    physical_len: usize,
    count: usize,
    leaf: bool,
}

impl GraphDescriptorPageView {
    /// The immutable bytes and offset table were produced only after full
    /// checksum, identity, framing, ordering and bounds validation. Warm reads
    /// recheck the selecting reference and current codec limits without hashing
    /// or allocating all the other entries again.
    pub(crate) fn from_verified_cache(
        bytes: SegmentBytes,
        reference: &GraphDescriptorPageRef,
        kind: GraphDescriptorKind,
        epoch: u64,
        limits: GraphDescriptorPageLimits,
    ) -> Result<Self, GraphDescriptorPageError> {
        reference.validate(limits)?;
        let physical_len = usize::try_from(reference.length.get()).map_err(|_| {
            GraphDescriptorPageError::Admission(
                "descriptor page length exceeds address space".into(),
            )
        })?;
        if bytes.len() < PAGE_HEADER_BYTES || physical_len > bytes.len() {
            return Err(corrupt("truncated verified descriptor page"));
        }
        let count = read_u32(&bytes[36..40]) as usize;
        validate_entry_count(count, limits, ErrorClass::Admission)?;
        let expected_len = count
            .checked_mul(OFFSET_BYTES)
            .and_then(|length| length.checked_add(MAXIMA_BYTES))
            .and_then(|length| length.checked_add(physical_len))
            .ok_or_else(|| corrupt("verified descriptor page length overflow"))?;
        if bytes.len() != expected_len
            || &bytes[..8] != PAGE_MAGIC
            || read_u16(&bytes[8..10]) != PAGE_VERSION
            || bytes[11] != kind.tag()
            || read_u64(&bytes[12..20]) != reference.physical_generation
            || read_u64(&bytes[20..28]) != epoch
            || read_u64(&bytes[28..36]) != reference.page_id.get()
            || read_u64(&bytes[40..48]) != (physical_len - PAGE_HEADER_BYTES) as u64
            || read_u32(&bytes[48..52]) != reference.content_crc32c.get()
            || &bytes[52..84] != reference.content_sha256.as_bytes()
        {
            return Err(corrupt(
                "verified descriptor page does not match its reference",
            ));
        }
        let maxima = &bytes[expected_len - MAXIMA_BYTES..];
        if read_u64(&maxima[..8]) > limits.max_key_bytes.get() as u64
            || read_u64(&maxima[8..16]) > limits.max_value_bytes.get() as u64
            || read_u64(&maxima[16..24]) > limits.max_page_bytes.get() as u64
        {
            return Err(GraphDescriptorPageError::Admission(
                "verified descriptor page exceeds current codec limits".into(),
            ));
        }
        let leaf = match GraphDescriptorPageKind::from_tag(bytes[10])? {
            GraphDescriptorPageKind::Leaf => true,
            GraphDescriptorPageKind::Interior => false,
        };
        let page = Self {
            bytes,
            physical_len,
            count,
            leaf,
        };
        let (lower, upper) = if leaf {
            (page.leaf_entry(0)?.0, page.leaf_entry(count - 1)?.0)
        } else {
            (page.child_bounds(0)?.0, page.child_bounds(count - 1)?.1)
        };
        if lower != reference.lower_bound || upper != reference.upper_bound {
            return Err(corrupt(
                "verified descriptor page key range does not match its reference",
            ));
        }
        Ok(page)
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.leaf
    }
    pub(crate) fn len(&self) -> usize {
        self.count
    }

    pub(crate) fn lower_bound(
        &self,
        target: &[u8],
    ) -> Result<(usize, u64), GraphDescriptorPageError> {
        let mut low = 0;
        let mut high = self.count;
        let mut comparisons = 0;
        while low < high {
            comparisons += 1;
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
        }
        Ok((low, comparisons))
    }

    pub(crate) fn leaf_entry(
        &self,
        index: usize,
    ) -> Result<(&[u8], &[u8]), GraphDescriptorPageError> {
        let entry = self.entry(index)?;
        let (key, offset) = decode_bytes(entry, 0, usize::MAX, "verified leaf key")?;
        let (value, _) = decode_bytes(entry, offset, usize::MAX, "verified descriptor value")?;
        Ok((key, value))
    }

    pub(crate) fn child_bounds(
        &self,
        index: usize,
    ) -> Result<(&[u8], &[u8]), GraphDescriptorPageError> {
        let entry = self.entry(index)?;
        let (lower, offset) = decode_bytes(entry, 76, usize::MAX, "verified child lower bound")?;
        let (upper, _) = decode_bytes(entry, offset, usize::MAX, "verified child upper bound")?;
        Ok((lower, upper))
    }

    pub(crate) fn child(
        &self,
        index: usize,
        limits: GraphDescriptorPageLimits,
    ) -> Result<GraphDescriptorPageRef, GraphDescriptorPageError> {
        decode_page_ref(self.entry(index)?, limits)
    }

    fn entry(&self, index: usize) -> Result<&[u8], GraphDescriptorPageError> {
        if index >= self.count {
            return Err(corrupt("verified descriptor entry index out of bounds"));
        }
        let slot = self.physical_len + index * OFFSET_BYTES;
        let offset = usize::try_from(read_u64(&self.bytes[slot..slot + OFFSET_BYTES]))
            .map_err(|_| corrupt("verified descriptor entry offset exceeds address space"))?;
        let physical = &self.bytes[..self.physical_len];
        let header = physical
            .get(offset..offset.saturating_add(FIELD_HEADER_BYTES))
            .ok_or_else(|| corrupt("truncated verified descriptor field header"))?;
        let start = offset + FIELD_HEADER_BYTES;
        let end = start
            .checked_add(read_u32(&header[2..6]) as usize)
            .ok_or_else(|| corrupt("verified descriptor field length overflow"))?;
        physical
            .get(start..end)
            .ok_or_else(|| corrupt("truncated verified descriptor field"))
    }
}

fn corrupt(message: &str) -> GraphDescriptorPageError {
    GraphDescriptorPageError::Corrupt(message.into())
}
