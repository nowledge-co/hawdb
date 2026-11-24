// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Zstd frame boundaries of indexed blocks, checked without decoding.
//!
//! A run of blocks is inflated through one streaming decoder, which accepts
//! any split of a valid frame sequence. A sparse read decodes one block's
//! compressed span alone. For both to accept exactly the same indexes, every
//! span must hold exactly one complete frame that declares the block's
//! uncompressed length; zstd then refuses a frame whose output differs from
//! its declared content size, so each frame inflates to exactly its block.

use super::invalid;
use crate::Result;

const FRAME_MAGIC: u32 = 0xfd2f_b528;
const HEADER_MAX_BYTES: usize = 18;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Collecting the frame header (`wanted` bytes once the descriptor is known).
    Header {
        wanted: usize,
    },
    BlockHeader,
    Payload {
        remaining: u64,
        last: bool,
    },
    Checksum,
    Done,
}

/// Walks one block's compressed span, byte stream by byte stream.
pub(super) struct FrameWalk {
    stage: Stage,
    bytes: [u8; HEADER_MAX_BYTES],
    filled: usize,
    checksum: bool,
    expected_content: u64,
}

impl FrameWalk {
    /// Starts a span whose frame must declare `content_len` output bytes.
    pub(super) fn new(content_len: u64) -> Self {
        Self {
            stage: Stage::Header { wanted: 5 },
            bytes: [0; HEADER_MAX_BYTES],
            filled: 0,
            checksum: false,
            expected_content: content_len,
        }
    }

    fn header(&mut self) -> Result<Stage> {
        if u32::from_le_bytes(self.bytes[..4].try_into().expect("four bytes")) != FRAME_MAGIC {
            return Err(invalid("block is not one zstd frame"));
        }
        let descriptor = self.bytes[4];
        let single = descriptor & 0x20 != 0;
        if descriptor & 0x08 != 0 {
            return Err(invalid("block frame sets a reserved bit"));
        }
        if descriptor & 0x03 != 0 {
            return Err(invalid("block frame names a dictionary"));
        }
        let content_bytes = [usize::from(single), 2, 4, 8][usize::from(descriptor >> 6)];
        if content_bytes == 0 {
            return Err(invalid("block frame does not declare its content size"));
        }
        let wanted = 5 + usize::from(!single) + content_bytes;
        if self.filled < wanted {
            return Ok(Stage::Header { wanted });
        }
        let field = &self.bytes[5 + usize::from(!single)..wanted];
        let mut content = [0u8; 8];
        content[..field.len()].copy_from_slice(field);
        let mut content = u64::from_le_bytes(content);
        if content_bytes == 2 {
            content += 256;
        }
        if content != self.expected_content {
            return Err(invalid("block frame content size disagrees with its index"));
        }
        self.checksum = descriptor & 0x04 != 0;
        self.filled = 0;
        Ok(Stage::BlockHeader)
    }

    /// Consumes the next bytes of the span.
    pub(super) fn feed(&mut self, mut input: &[u8]) -> Result<()> {
        while !input.is_empty() {
            match self.stage {
                Stage::Header { wanted } => {
                    let take = (wanted - self.filled).min(input.len());
                    self.bytes[self.filled..self.filled + take].copy_from_slice(&input[..take]);
                    self.filled += take;
                    input = &input[take..];
                    if self.filled == wanted {
                        self.stage = self.header()?;
                    }
                }
                Stage::BlockHeader | Stage::Checksum => {
                    let wanted = if self.stage == Stage::Checksum { 4 } else { 3 };
                    let take = (wanted - self.filled).min(input.len());
                    self.bytes[self.filled..self.filled + take].copy_from_slice(&input[..take]);
                    self.filled += take;
                    input = &input[take..];
                    if self.filled < wanted {
                        continue;
                    }
                    self.filled = 0;
                    if self.stage == Stage::Checksum {
                        self.stage = Stage::Done;
                        continue;
                    }
                    let header = u32::from(self.bytes[0])
                        | (u32::from(self.bytes[1]) << 8)
                        | (u32::from(self.bytes[2]) << 16);
                    let last = header & 1 != 0;
                    let size = u64::from(header >> 3);
                    let remaining = match (header >> 1) & 3 {
                        0 | 2 => size,
                        1 => 1,
                        _ => return Err(invalid("block frame uses a reserved block type")),
                    };
                    self.stage = Stage::Payload { remaining, last };
                    self.end_payload_if_empty();
                }
                Stage::Payload { remaining, last } => {
                    let take = remaining.min(input.len() as u64);
                    input = &input[take as usize..];
                    self.stage = Stage::Payload {
                        remaining: remaining - take,
                        last,
                    };
                    self.end_payload_if_empty();
                }
                Stage::Done => return Err(invalid("block has bytes after its frame")),
            }
        }
        Ok(())
    }

    fn end_payload_if_empty(&mut self) {
        if let Stage::Payload { remaining: 0, last } = self.stage {
            self.stage = match (last, self.checksum) {
                (false, _) => Stage::BlockHeader,
                (true, true) => Stage::Checksum,
                (true, false) => Stage::Done,
            };
        }
    }

    /// Whether the span ended exactly at the end of its frame.
    pub(super) fn finish(&self) -> Result<()> {
        if self.stage == Stage::Done {
            Ok(())
        } else {
            Err(invalid("block frame crosses its indexed boundary"))
        }
    }
}

/// Checks one complete compressed span.
pub(super) fn check_span(span: &[u8], content_len: u64) -> Result<()> {
    let mut walk = FrameWalk::new(content_len);
    walk.feed(span)?;
    walk.finish()
}
