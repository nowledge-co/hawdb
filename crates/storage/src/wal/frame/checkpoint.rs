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

//! Admit complete framed output before allocation, with one-block work units.
//! Payload/decoder/runtime ownership require their own allocation accounting.

use super::*;
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};

#[cfg(test)]
mod tests;

pub(crate) fn frame_binary_wal_record_with_work_context(
    generation: u64,
    payload: &[u8],
    position: u64,
    work: &CheckpointWorkContext,
) -> std::result::Result<CheckpointBytes, CheckpointWorkError> {
    let block_pos = (position % WAL_BLOCK_BYTES as u64) as usize;
    let remaining_block = WAL_BLOCK_BYTES - block_pos;
    let padding = if remaining_block < WAL_FRAGMENT_HEADER_BYTES {
        remaining_block
    } else {
        0
    };
    let first_capacity = if padding != 0 {
        WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES
    } else {
        remaining_block - WAL_FRAGMENT_HEADER_BYTES
    };
    let remainder = payload.len().saturating_sub(first_capacity);
    // Avoid a dataset-sized counting pass and addition in ceil division.
    let more_fragments = if remainder == 0 {
        0
    } else {
        (remainder - 1) / (WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES) + 1
    };
    let capacity = more_fragments
        .checked_add(1)
        .and_then(|fragments| fragments.checked_mul(WAL_FRAGMENT_HEADER_BYTES))
        .and_then(|headers| headers.checked_add(payload.len()))
        .and_then(|bytes| bytes.checked_add(padding))
        .ok_or_else(|| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "framed WAL byte capacity overflows usize".into(),
            })
        })?;
    let mut output = CheckpointBytes::new(capacity, work)?;
    if padding != 0 {
        output.append(&[0; WAL_FRAGMENT_HEADER_BYTES][..padding], work)?;
    }
    let mut available = first_capacity;
    let mut remaining = payload;
    let mut first = true;
    loop {
        let take = available.min(remaining.len());
        let (chunk, rest) = remaining.split_at(take);
        let fragment_type = match (first, rest.is_empty()) {
            (true, true) => FRAGMENT_FULL,
            (true, false) => FRAGMENT_FIRST,
            (false, true) => FRAGMENT_LAST,
            (false, false) => FRAGMENT_MIDDLE,
        };
        let unit = work.start_unit()?;
        // CRC covers at most one physical block; the ordinary encoder remains
        // the independent complete-byte reference, including empty fragments.
        let crc = masked_fragment_crc(fragment_type, generation, chunk);
        let mut header = [0; WAL_FRAGMENT_HEADER_BYTES];
        header[..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(take as u16).to_le_bytes());
        header[6] = fragment_type;
        header[7..].copy_from_slice(&generation.to_le_bytes());
        unit.finish();
        // Append owns separate units. Never nest a second local QoS permit.
        output.append(&header, work)?;
        output.append(chunk, work)?;
        if rest.is_empty() {
            work.checkpoint()?;
            debug_assert_eq!(output.len(), capacity);
            return Ok(output);
        }
        remaining = rest;
        first = false;
        available = WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES;
    }
}
