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

//! Reframe borrowed payloads in physical-block units without a complete output
//! allocation. The reader retains payload/decoder ownership separately.

use super::*;
use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};

mod reader;
pub(crate) use reader::{CheckpointBinaryWalReader, CheckpointWalReadEvent};

#[cfg(test)]
mod stream_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod writer_tests;

pub(crate) struct CheckpointWalFrameStream<'a> {
    generation: u64,
    remaining: &'a [u8],
    first_capacity: usize,
    padding: usize,
    first: bool,
    finished: bool,
    encoded_len: usize,
    work: &'a CheckpointWorkContext,
}

pub(crate) struct CheckpointWalFragment<'a> {
    header: [u8; WAL_FRAGMENT_HEADER_BYTES],
    body: &'a [u8],
    padding: usize,
}

impl CheckpointWalFragment<'_> {
    pub(crate) fn parts(&self) -> [&[u8]; 3] {
        [
            &[0; WAL_FRAGMENT_HEADER_BYTES][..self.padding],
            &self.header,
            self.body,
        ]
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.padding + self.header.len() + self.body.len()
    }

    /// Write the bounded physical fragment without allocating a combined
    /// buffer. Complete short writes before the caller advances replay state.
    pub(crate) fn write_to<W: std::io::Write>(&self, output: &mut W) -> std::io::Result<()> {
        let mut slices = self.parts().map(std::io::IoSlice::new);
        let mut remaining = &mut slices[..];
        std::io::IoSlice::advance_slices(&mut remaining, 0);
        while !remaining.is_empty() {
            match output.write_vectored(remaining) {
                Ok(0) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "checkpoint WAL fragment write made no progress",
                    ));
                }
                Ok(written) => std::io::IoSlice::advance_slices(&mut remaining, written),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl<'a> CheckpointWalFrameStream<'a> {
    pub(crate) fn new(
        generation: u64,
        payload: &'a [u8],
        position: u64,
        work: &'a CheckpointWorkContext,
    ) -> std::result::Result<Self, CheckpointWorkError> {
        let remaining_block = WAL_BLOCK_BYTES - (position % WAL_BLOCK_BYTES as u64) as usize;
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
        let more_fragments = if remainder == 0 {
            0
        } else {
            (remainder - 1) / (WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES) + 1
        };
        let encoded_len = more_fragments
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
        Ok(Self {
            generation,
            remaining: payload,
            first_capacity,
            padding,
            first: true,
            finished: false,
            encoded_len,
            work,
        })
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    pub(crate) fn next(
        &mut self,
    ) -> std::result::Result<Option<CheckpointWalFragment<'a>>, CheckpointWorkError> {
        self.work.checkpoint()?;
        if self.finished {
            return Ok(None);
        }
        let capacity = if self.first {
            self.first_capacity
        } else {
            WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES
        };
        let (chunk, rest) = self.remaining.split_at(capacity.min(self.remaining.len()));
        let fragment_type = match (self.first, rest.is_empty()) {
            (true, true) => FRAGMENT_FULL,
            (true, false) => FRAGMENT_FIRST,
            (false, true) => FRAGMENT_LAST,
            (false, false) => FRAGMENT_MIDDLE,
        };
        let unit = self.work.start_unit()?;
        let crc = masked_fragment_crc(fragment_type, self.generation, chunk);
        let mut header = [0; WAL_FRAGMENT_HEADER_BYTES];
        header[..4].copy_from_slice(&crc.to_le_bytes());
        header[4..6].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
        header[6] = fragment_type;
        header[7..].copy_from_slice(&self.generation.to_le_bytes());
        unit.finish();
        self.work.checkpoint()?;
        let fragment = CheckpointWalFragment {
            header,
            body: chunk,
            padding: if self.first { self.padding } else { 0 },
        };
        self.first = false;
        self.finished = rest.is_empty();
        self.remaining = rest;
        Ok(Some(fragment))
    }
}

// Retain the original admitted-buffer fixtures as independent regression
// references; production catch-up streams fragments from the captured payload.
#[cfg(test)]
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
