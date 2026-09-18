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

//! Block-oriented fragment framing for the binary WAL (spec §3.4.1/§3.4.2).
//!
//! # File layout
//!
//! A binary WAL file starts with a fixed 28-byte file header:
//!
//! ```text
//! magic "SKWALB01" (8B) | generation (8B LE) | start LSN (8B LE) | crc32c (4B LE)
//! ```
//!
//! where the checksum covers the first 24 bytes. The byte stream after the
//! file header is divided into fixed 32 KiB blocks. A record is written as
//! one or more fragments, each carrying a 15-byte header:
//!
//! ```text
//! crc32c (4B LE) | length (2B LE) | type (1B) | generation (8B LE)
//! ```
//!
//! Fragment types are FULL (1), FIRST (2), MIDDLE (3), LAST (4); type 0 is
//! reserved so an unwritten (zero) region can never alias a valid fragment.
//! The checksum covers the type byte, the generation, and the payload, and
//! is then masked (RocksDB-style rotation plus constant), so a fragment
//! whose type or generation byte flips fails its checksum. Binding the WAL
//! generation into every fragment header realizes the recyclable-log
//! discipline required by spec §3.4.1: a well-formed fragment carrying a
//! stale generation reads as end of log. The spec sketches a 7-byte header
//! with the generation mixed into the mask; this implementation chooses the
//! explicit-generation-header variant (15 bytes, CRC-covered) so a stale
//! generation is distinguishable from corruption.
//!
//! A block tail shorter than one fragment header is zero-filled. Fragment
//! payloads never span blocks; large records fragment across blocks, so
//! framing safety requires no bound on record size.
//!
//! # Recovery policy (spec §3.4.2, stricter than RocksDB defaults)
//!
//! Block-aligned resynchronization locates damage but never skips it:
//!
//! - A structurally complete fragment (its declared payload fits in the
//!   file) whose checksum fails, a sequence-invalid fragment outside a
//!   pending chain, or a non-zero block trailer is corruption and fails
//!   closed.
//! - Physically missing data at end of file — a truncated fragment header,
//!   a payload cut short by EOF, or a chain still awaiting MIDDLE/LAST —
//!   is a torn tail, repairable through the audited doctor protocol (explicitly
//!   or on a writable open with automatic tail repair enabled).
//!   The torn tail begins at the first byte of the incomplete chain.
//! - A well-formed fragment carrying a stale generation, or an all-zero
//!   (unwritten) fragment header, marks end of log. If any block boundary
//!   after such a marker (or after a sequence violation inside a pending
//!   chain) still holds a valid current-generation fragment, the log was
//!   damaged in place and the reader fails closed instead.

use hawdb_core::{HawDBError, Result};
use std::io::Read;

pub const WAL_BINARY_MAGIC: &[u8; 8] = b"SKWALB01";
pub const WAL_BLOCK_BYTES: usize = 32 * 1024;
pub const WAL_FRAGMENT_HEADER_BYTES: usize = 15;
pub const WAL_BINARY_FILE_HEADER_BYTES: usize = 28;

const FRAGMENT_FULL: u8 = 1;
const FRAGMENT_FIRST: u8 = 2;
const FRAGMENT_MIDDLE: u8 = 3;
const FRAGMENT_LAST: u8 = 4;

const CRC_MASK_DELTA: u32 = 0xa282_ead8;

fn masked_fragment_crc(fragment_type: u8, generation: u64, payload: &[u8]) -> u32 {
    let mut hasher = hawdb_integrity::Crc32cHasher::new();
    hasher.update(&[fragment_type]);
    hasher.update(&generation.to_le_bytes());
    hasher.update(payload);
    hasher
        .finish_u32()
        .rotate_right(15)
        .wrapping_add(CRC_MASK_DELTA)
}

pub fn encode_binary_wal_header(generation: u64, start_lsn: u64) -> Vec<u8> {
    let mut header = Vec::with_capacity(WAL_BINARY_FILE_HEADER_BYTES);
    header.extend_from_slice(WAL_BINARY_MAGIC);
    header.extend_from_slice(&generation.to_le_bytes());
    header.extend_from_slice(&start_lsn.to_le_bytes());
    let checksum = hawdb_integrity::crc32c(&header).get();
    header.extend_from_slice(&checksum.to_le_bytes());
    header
}

pub fn decode_binary_wal_header(bytes: &[u8]) -> Result<(u64, u64)> {
    if bytes.len() < WAL_BINARY_FILE_HEADER_BYTES {
        return Err(HawDBError::Storage(
            "binary WAL file header is truncated".to_string(),
        ));
    }
    let header = &bytes[..WAL_BINARY_FILE_HEADER_BYTES];
    if &header[..8] != WAL_BINARY_MAGIC {
        return Err(HawDBError::Storage(
            "WAL is missing a supported generation header".to_string(),
        ));
    }
    let expected = u32::from_le_bytes(header[24..28].try_into().expect("4-byte checksum"));
    let actual = hawdb_integrity::crc32c(&header[..24]).get();
    if expected != actual {
        return Err(HawDBError::Storage(format!(
            "WAL header checksum mismatch: expected {expected}, got {actual}"
        )));
    }
    let generation = u64::from_le_bytes(header[8..16].try_into().expect("8-byte generation"));
    let start_lsn = u64::from_le_bytes(header[16..24].try_into().expect("8-byte start LSN"));
    Ok((generation, start_lsn))
}

/// Frames one record payload into fragments, starting at `position` bytes
/// past the file header (i.e. `file_len - 28` for an append). Returns the
/// bytes to append, including any zero-filled trailer needed to reach the
/// next block boundary first.
pub fn frame_binary_wal_record(generation: u64, payload: &[u8], position: u64) -> Vec<u8> {
    let mut block_pos = (position % WAL_BLOCK_BYTES as u64) as usize;
    let mut out = Vec::with_capacity(
        payload.len() + 2 * WAL_FRAGMENT_HEADER_BYTES + WAL_FRAGMENT_HEADER_BYTES,
    );
    if WAL_BLOCK_BYTES - block_pos < WAL_FRAGMENT_HEADER_BYTES {
        out.resize(out.len() + (WAL_BLOCK_BYTES - block_pos), 0);
        block_pos = 0;
    }
    let mut remaining = payload;
    let mut first = true;
    loop {
        let available = WAL_BLOCK_BYTES - block_pos - WAL_FRAGMENT_HEADER_BYTES;
        let take = available.min(remaining.len());
        let (chunk, rest) = remaining.split_at(take);
        let fragment_type = match (first, rest.is_empty()) {
            (true, true) => FRAGMENT_FULL,
            (true, false) => FRAGMENT_FIRST,
            (false, true) => FRAGMENT_LAST,
            (false, false) => FRAGMENT_MIDDLE,
        };
        let crc = masked_fragment_crc(fragment_type, generation, chunk);
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&(take as u16).to_le_bytes());
        out.push(fragment_type);
        out.extend_from_slice(&generation.to_le_bytes());
        out.extend_from_slice(chunk);
        if rest.is_empty() {
            return out;
        }
        remaining = rest;
        first = false;
        block_pos = 0;
    }
}

/// One event from the binary fragment reader. Offsets are absolute file
/// offsets (the 28-byte file header included).
#[derive(Debug)]
pub enum BinaryWalReadEvent {
    Record {
        payload: Vec<u8>,
        start_offset: u64,
        end_offset: u64,
    },
    TornTail {
        valid_prefix_len: u64,
        reason: String,
    },
    Corrupt {
        offset: u64,
        reason: String,
    },
    Eof,
}

struct PendingChain {
    start_offset: u64,
    payload: Vec<u8>,
}

/// Streams fragment chains out of the block layout after the file header.
pub struct BinaryWalReader<R: Read> {
    reader: R,
    generation: u64,
    max_record_bytes: Option<usize>,
    block: Vec<u8>,
    block_len: usize,
    block_pos: usize,
    /// Absolute file offset of `block[0]`.
    block_offset: u64,
    /// The stream ended inside the current block.
    stream_ended: bool,
    block_loaded: bool,
    chain: Option<PendingChain>,
    finished: bool,
}

enum FragmentParse {
    /// A valid current-generation fragment (type, payload slice range).
    Fragment {
        fragment_type: u8,
        payload_end: usize,
    },
    /// End-of-log marker: stale generation or all-zero (unwritten) header.
    EndOfLog { reason: String },
    /// Structurally complete but invalid: fails closed.
    Corrupt { reason: String },
    /// Data physically missing at end of file.
    Torn { reason: String },
    /// Block exhausted; advance to the next block.
    NeedNextBlock,
    /// Clean end of the byte stream.
    EndOfStream,
}

impl<R: Read> BinaryWalReader<R> {
    pub fn new(reader: R, generation: u64, max_record_bytes: Option<usize>) -> Self {
        Self {
            reader,
            generation,
            max_record_bytes,
            block: vec![0u8; WAL_BLOCK_BYTES],
            block_len: 0,
            block_pos: 0,
            block_offset: WAL_BINARY_FILE_HEADER_BYTES as u64,
            stream_ended: false,
            block_loaded: false,
            chain: None,
            finished: false,
        }
    }

    fn load_next_block(&mut self) -> Result<()> {
        self.block_offset += self.block_len as u64;
        self.block_len = 0;
        self.block_pos = 0;
        while self.block_len < WAL_BLOCK_BYTES {
            let read = self.reader.read(&mut self.block[self.block_len..])?;
            if read == 0 {
                self.stream_ended = true;
                break;
            }
            self.block_len += read;
        }
        self.block_loaded = true;
        Ok(())
    }

    fn parse_fragment_at_cursor(&mut self) -> Result<FragmentParse> {
        if !self.block_loaded || (self.block_pos == self.block_len && !self.stream_ended) {
            self.load_next_block()?;
        }
        let remaining = self.block_len - self.block_pos;
        if remaining == 0 {
            return Ok(if self.stream_ended {
                FragmentParse::EndOfStream
            } else {
                FragmentParse::NeedNextBlock
            });
        }
        if WAL_BLOCK_BYTES - self.block_pos < WAL_FRAGMENT_HEADER_BYTES {
            // Trailer region of a block: the writer zero-fills it.
            let trailer = &self.block[self.block_pos..self.block_len];
            if trailer.iter().any(|byte| *byte != 0) {
                return Ok(FragmentParse::Corrupt {
                    reason: "block trailer is not zero-filled".to_string(),
                });
            }
            if self.block_len < WAL_BLOCK_BYTES {
                // The trailer itself was cut short by EOF; nothing but a
                // record append writes trailer zeros, so one was in flight.
                return Ok(FragmentParse::Torn {
                    reason: "block trailer is cut short at end of file".to_string(),
                });
            }
            self.block_pos = self.block_len;
            return Ok(FragmentParse::NeedNextBlock);
        }
        if remaining < WAL_FRAGMENT_HEADER_BYTES {
            return Ok(FragmentParse::Torn {
                reason: "fragment header is cut short at end of file".to_string(),
            });
        }
        let header_start = self.block_pos;
        let header = &self.block[header_start..header_start + WAL_FRAGMENT_HEADER_BYTES];
        let stored_crc = u32::from_le_bytes(header[0..4].try_into().expect("4-byte crc"));
        let declared_len = u16::from_le_bytes(header[4..6].try_into().expect("2-byte length"));
        let fragment_type = header[6];
        let fragment_generation =
            u64::from_le_bytes(header[7..15].try_into().expect("8-byte generation"));
        if stored_crc == 0 && declared_len == 0 && fragment_type == 0 && fragment_generation == 0 {
            return Ok(FragmentParse::EndOfLog {
                reason: "unwritten (all-zero) fragment header".to_string(),
            });
        }
        let payload_start = header_start + WAL_FRAGMENT_HEADER_BYTES;
        let payload_end = payload_start + declared_len as usize;
        if payload_end > WAL_BLOCK_BYTES {
            return Ok(FragmentParse::Corrupt {
                reason: format!("fragment length {declared_len} overflows its 32 KiB block"),
            });
        }
        if payload_end > self.block_len {
            if self.stream_ended {
                return Ok(FragmentParse::Torn {
                    reason: format!(
                        "fragment payload of {declared_len} bytes is cut short at end of file"
                    ),
                });
            }
            return Ok(FragmentParse::Corrupt {
                reason: "fragment payload crosses a block boundary".to_string(),
            });
        }
        let payload = &self.block[payload_start..payload_end];
        let expected_crc = masked_fragment_crc(fragment_type, fragment_generation, payload);
        if stored_crc != expected_crc {
            return Ok(FragmentParse::Corrupt {
                reason: format!(
                    "fragment checksum mismatch: expected {expected_crc}, got {stored_crc}"
                ),
            });
        }
        if !(FRAGMENT_FULL..=FRAGMENT_LAST).contains(&fragment_type) {
            return Ok(FragmentParse::Corrupt {
                reason: format!("invalid fragment type {fragment_type}"),
            });
        }
        if fragment_generation != self.generation {
            return Ok(FragmentParse::EndOfLog {
                reason: format!(
                    "fragment carries stale WAL generation {fragment_generation}, expected {}",
                    self.generation
                ),
            });
        }
        Ok(FragmentParse::Fragment {
            fragment_type,
            payload_end,
        })
    }

    /// Block-aligned resynchronization after an end-of-log marker or a
    /// sequence violation inside a pending chain: it locates live data (so
    /// damage inside the durable prefix fails closed) but never skips it.
    fn live_fragment_after(&mut self) -> Result<bool> {
        loop {
            self.load_next_block()?;
            if self.block_len == 0 {
                return Ok(false);
            }
            if self.block_len >= WAL_FRAGMENT_HEADER_BYTES {
                let header = &self.block[..WAL_FRAGMENT_HEADER_BYTES];
                let stored_crc = u32::from_le_bytes(header[0..4].try_into().expect("crc"));
                let declared_len =
                    u16::from_le_bytes(header[4..6].try_into().expect("length")) as usize;
                let fragment_type = header[6];
                let fragment_generation =
                    u64::from_le_bytes(header[7..15].try_into().expect("generation"));
                let payload_end = WAL_FRAGMENT_HEADER_BYTES + declared_len;
                if (FRAGMENT_FULL..=FRAGMENT_LAST).contains(&fragment_type)
                    && fragment_generation == self.generation
                    && payload_end <= self.block_len
                    && stored_crc
                        == masked_fragment_crc(
                            fragment_type,
                            fragment_generation,
                            &self.block[WAL_FRAGMENT_HEADER_BYTES..payload_end],
                        )
                {
                    return Ok(true);
                }
            }
            if self.stream_ended {
                return Ok(false);
            }
        }
    }

    fn end_of_log(&mut self, offset: u64, marker_reason: String) -> Result<BinaryWalReadEvent> {
        self.finished = true;
        if self.live_fragment_after()? {
            return Ok(BinaryWalReadEvent::Corrupt {
                offset,
                reason: format!("valid fragments follow an end-of-log marker ({marker_reason})"),
            });
        }
        match self.chain.take() {
            Some(chain) => Ok(BinaryWalReadEvent::TornTail {
                valid_prefix_len: chain.start_offset,
                reason: format!("fragment chain is incomplete at end of log ({marker_reason})"),
            }),
            None => Ok(BinaryWalReadEvent::Eof),
        }
    }

    fn chain_violation(&mut self, offset: u64, reason: String) -> Result<BinaryWalReadEvent> {
        self.finished = true;
        if self.chain.is_some() {
            // Skip past the violating fragment, then resynchronize on block
            // boundaries: live data after it means mid-log damage.
            if self.live_fragment_after()? {
                return Ok(BinaryWalReadEvent::Corrupt { offset, reason });
            }
            let chain = self.chain.take().expect("pending chain");
            return Ok(BinaryWalReadEvent::TornTail {
                valid_prefix_len: chain.start_offset,
                reason: format!("fragment chain is incomplete at end of file ({reason})"),
            });
        }
        Ok(BinaryWalReadEvent::Corrupt { offset, reason })
    }

    pub fn next_event(&mut self) -> Result<BinaryWalReadEvent> {
        if self.finished {
            return Ok(BinaryWalReadEvent::Eof);
        }
        loop {
            let fragment_offset = self.block_offset + self.block_pos as u64;
            match self.parse_fragment_at_cursor()? {
                FragmentParse::NeedNextBlock => continue,
                FragmentParse::EndOfStream => {
                    self.finished = true;
                    return match self.chain.take() {
                        Some(chain) => Ok(BinaryWalReadEvent::TornTail {
                            valid_prefix_len: chain.start_offset,
                            reason: "fragment chain is incomplete at end of file".to_string(),
                        }),
                        None => Ok(BinaryWalReadEvent::Eof),
                    };
                }
                FragmentParse::Torn { reason } => {
                    self.finished = true;
                    let valid_prefix_len = match self.chain.take() {
                        Some(chain) => chain.start_offset,
                        None => fragment_offset,
                    };
                    return Ok(BinaryWalReadEvent::TornTail {
                        valid_prefix_len,
                        reason,
                    });
                }
                FragmentParse::Corrupt { reason } => {
                    self.finished = true;
                    return Ok(BinaryWalReadEvent::Corrupt {
                        offset: fragment_offset,
                        reason,
                    });
                }
                FragmentParse::EndOfLog { reason } => {
                    return self.end_of_log(fragment_offset, reason);
                }
                FragmentParse::Fragment {
                    fragment_type,
                    payload_end,
                } => {
                    let payload_start = self.block_pos + WAL_FRAGMENT_HEADER_BYTES;
                    self.block_pos = payload_end;
                    match fragment_type {
                        FRAGMENT_FULL => {
                            if self.chain.is_some() {
                                self.block_pos = payload_start - WAL_FRAGMENT_HEADER_BYTES;
                                return self.chain_violation(
                                    fragment_offset,
                                    "FULL fragment interrupts an open fragment chain".to_string(),
                                );
                            }
                            let payload = self.block[payload_start..payload_end].to_vec();
                            self.enforce_record_limit(payload.len())?;
                            return Ok(BinaryWalReadEvent::Record {
                                payload,
                                start_offset: fragment_offset,
                                end_offset: self.block_offset + self.block_pos as u64,
                            });
                        }
                        FRAGMENT_FIRST => {
                            if self.chain.is_some() {
                                self.block_pos = payload_start - WAL_FRAGMENT_HEADER_BYTES;
                                return self.chain_violation(
                                    fragment_offset,
                                    "FIRST fragment interrupts an open fragment chain".to_string(),
                                );
                            }
                            let payload = self.block[payload_start..payload_end].to_vec();
                            self.enforce_record_limit(payload.len())?;
                            self.chain = Some(PendingChain {
                                start_offset: fragment_offset,
                                payload,
                            });
                        }
                        FRAGMENT_MIDDLE | FRAGMENT_LAST => {
                            let Some(chain) = self.chain.as_mut() else {
                                self.finished = true;
                                return Ok(BinaryWalReadEvent::Corrupt {
                                    offset: fragment_offset,
                                    reason: "continuation fragment without a FIRST fragment"
                                        .to_string(),
                                });
                            };
                            chain
                                .payload
                                .extend_from_slice(&self.block[payload_start..payload_end]);
                            let payload_len = chain.payload.len();
                            self.enforce_record_limit(payload_len)?;
                            if fragment_type == FRAGMENT_LAST {
                                let chain = self.chain.take().expect("pending chain");
                                return Ok(BinaryWalReadEvent::Record {
                                    payload: chain.payload,
                                    start_offset: chain.start_offset,
                                    end_offset: self.block_offset + self.block_pos as u64,
                                });
                            }
                        }
                        _ => unreachable!("fragment type validated during parse"),
                    }
                }
            }
        }
    }

    fn enforce_record_limit(&self, payload_len: usize) -> Result<()> {
        if self
            .max_record_bytes
            .is_some_and(|limit| payload_len > limit)
        {
            return Err(HawDBError::Storage(format!(
                "WAL record byte limit exceeded: max_wal_record_bytes={}",
                self.max_record_bytes.unwrap_or_default()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed_file(generation: u64, start_lsn: u64, payloads: &[Vec<u8>]) -> Vec<u8> {
        let mut file = encode_binary_wal_header(generation, start_lsn);
        for payload in payloads {
            let position = file.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
            let framed = frame_binary_wal_record(generation, payload, position);
            file.extend_from_slice(&framed);
        }
        file
    }

    fn read_all(file: &[u8], generation: u64) -> Vec<BinaryWalReadEvent> {
        let mut reader =
            BinaryWalReader::new(&file[WAL_BINARY_FILE_HEADER_BYTES..], generation, None);
        let mut events = Vec::new();
        loop {
            let event = reader.next_event().unwrap();
            let terminal = !matches!(event, BinaryWalReadEvent::Record { .. });
            events.push(event);
            if terminal {
                return events;
            }
        }
    }

    fn record_payloads(events: &[BinaryWalReadEvent]) -> Vec<&[u8]> {
        events
            .iter()
            .filter_map(|event| match event {
                BinaryWalReadEvent::Record { payload, .. } => Some(payload.as_slice()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn file_header_round_trips_and_rejects_damage() {
        let header = encode_binary_wal_header(7, 42);
        assert_eq!(header.len(), WAL_BINARY_FILE_HEADER_BYTES);
        assert_eq!(decode_binary_wal_header(&header).unwrap(), (7, 42));
        let mut flipped = header.clone();
        flipped[9] ^= 0x01;
        assert!(decode_binary_wal_header(&flipped)
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
        assert!(decode_binary_wal_header(&header[..20])
            .unwrap_err()
            .to_string()
            .contains("truncated"));
        let mut wrong_magic = header;
        wrong_magic[0] = b'X';
        assert!(decode_binary_wal_header(&wrong_magic)
            .unwrap_err()
            .to_string()
            .contains("missing a supported generation header"));
    }

    #[test]
    fn small_records_round_trip_as_full_fragments() {
        let payloads = vec![b"alpha".to_vec(), b"beta".to_vec(), Vec::new()];
        let file = framed_file(3, 10, &payloads);
        let events = read_all(&file, 3);
        assert_eq!(
            record_payloads(&events),
            vec![b"alpha".as_slice(), b"beta", b""]
        );
        assert!(matches!(events.last(), Some(BinaryWalReadEvent::Eof)));
    }

    #[test]
    fn large_records_span_multiple_blocks() {
        // > 2 blocks, exercising FIRST/MIDDLE/LAST assembly.
        let big = (0..90_000u32).map(|value| value as u8).collect::<Vec<_>>();
        let payloads = vec![b"lead".to_vec(), big.clone(), b"tail".to_vec()];
        let file = framed_file(1, 1, &payloads);
        assert!(file.len() > 2 * WAL_BLOCK_BYTES);
        let events = read_all(&file, 1);
        assert_eq!(
            record_payloads(&events),
            vec![b"lead".as_slice(), big.as_slice(), b"tail"]
        );
        assert!(matches!(events.last(), Some(BinaryWalReadEvent::Eof)));
    }

    #[test]
    fn exact_block_boundary_fit_and_zero_length_first_fragment() {
        // First record leaves exactly one header of space in the block; the
        // next record must start with a zero-payload FIRST fragment.
        let fill = WAL_BLOCK_BYTES - 2 * WAL_FRAGMENT_HEADER_BYTES - WAL_FRAGMENT_HEADER_BYTES;
        let first = vec![0x5au8; fill];
        let second = vec![0xa5u8; 64];
        let file = framed_file(2, 5, &[first.clone(), second.clone()]);
        let events = read_all(&file, 2);
        assert_eq!(
            record_payloads(&events),
            vec![first.as_slice(), second.as_slice()]
        );
        // And a record that exactly fills a block ends flush on the boundary.
        let exact = vec![0x11u8; WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES];
        let file = framed_file(2, 5, std::slice::from_ref(&exact));
        assert_eq!(
            (file.len() - WAL_BINARY_FILE_HEADER_BYTES) % WAL_BLOCK_BYTES,
            0
        );
        let events = read_all(&file, 2);
        assert_eq!(record_payloads(&events), vec![exact.as_slice()]);
    }

    #[test]
    fn block_tail_shorter_than_a_header_is_zero_filled_and_skipped() {
        // Leave fewer than 15 bytes before the block boundary.
        let fill = WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES - 10;
        let first = vec![0x33u8; fill];
        let second = b"after-trailer".to_vec();
        let file = framed_file(4, 9, &[first.clone(), second.clone()]);
        let trailer_start = WAL_BINARY_FILE_HEADER_BYTES + WAL_FRAGMENT_HEADER_BYTES + fill;
        assert!(file[trailer_start..trailer_start + 10]
            .iter()
            .all(|b| *b == 0));
        let events = read_all(&file, 4);
        assert_eq!(
            record_payloads(&events),
            vec![first.as_slice(), second.as_slice()]
        );
        // A non-zero trailer byte fails closed.
        let mut damaged = file.clone();
        damaged[trailer_start + 3] = 0xff;
        let events = read_all(&damaged, 4);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("trailer is not zero-filled")
        ));
    }

    #[test]
    fn masked_crc_rejects_type_flipped_and_generation_flipped_fragments() {
        let file = framed_file(6, 2, &[b"payload-under-test".to_vec()]);
        let type_offset = WAL_BINARY_FILE_HEADER_BYTES + 6;
        let mut type_flipped = file.clone();
        type_flipped[type_offset] = FRAGMENT_LAST;
        let events = read_all(&type_flipped, 6);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("checksum mismatch")
        ));
        let mut generation_flipped = file.clone();
        generation_flipped[type_offset + 1] ^= 0x01;
        let events = read_all(&generation_flipped, 6);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("checksum mismatch")
        ));
    }

    #[test]
    fn stale_generation_fragment_reads_as_clean_end_of_log() {
        let mut file = framed_file(9, 1, &[b"current".to_vec()]);
        let position = file.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
        let stale = frame_binary_wal_record(8, b"recycled", position);
        file.extend_from_slice(&stale);
        let events = read_all(&file, 9);
        assert_eq!(record_payloads(&events), vec![b"current".as_slice()]);
        assert!(matches!(events.last(), Some(BinaryWalReadEvent::Eof)));
    }

    #[test]
    fn live_fragments_after_a_stale_marker_fail_closed() {
        // Damage disguised as recycling: a stale fragment followed by valid
        // current-generation data on a later block boundary.
        let mut file = framed_file(9, 1, &[b"current".to_vec()]);
        let position = file.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
        file.extend_from_slice(&frame_binary_wal_record(8, b"recycled", position));
        // Zero-fill to the next block boundary, then a valid record.
        let content_len = file.len() - WAL_BINARY_FILE_HEADER_BYTES;
        let fill = WAL_BLOCK_BYTES - (content_len % WAL_BLOCK_BYTES);
        file.resize(file.len() + fill, 0);
        let position = file.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
        file.extend_from_slice(&frame_binary_wal_record(9, b"live-after-damage", position));
        let events = read_all(&file, 9);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("end-of-log marker")
        ));
    }

    #[test]
    fn torn_tail_matrix_cut_mid_fragment_between_fragments_and_after_chain() {
        let big = vec![0x77u8; WAL_BLOCK_BYTES + 500];
        let file = framed_file(5, 3, &[b"keep".to_vec(), big]);
        let keep_framed_len = WAL_FRAGMENT_HEADER_BYTES + 4;
        let chain_start = (WAL_BINARY_FILE_HEADER_BYTES + keep_framed_len) as u64;

        // Cut inside the FIRST fragment header.
        let events = read_all(&file[..chain_start as usize + 7], 5);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::TornTail { valid_prefix_len, .. })
                if *valid_prefix_len == chain_start
        ));
        // Cut inside the FIRST fragment payload.
        let events = read_all(&file[..chain_start as usize + 200], 5);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::TornTail { valid_prefix_len, .. })
                if *valid_prefix_len == chain_start
        ));
        // Cut exactly between FIRST and LAST (block boundary): the chain is
        // still open, so the tail is torn back to the chain start.
        let boundary = WAL_BINARY_FILE_HEADER_BYTES + WAL_BLOCK_BYTES;
        let events = read_all(&file[..boundary], 5);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::TornTail { valid_prefix_len, reason })
                if *valid_prefix_len == chain_start && reason.contains("incomplete")
        ));
        // Cut inside the LAST fragment payload.
        let events = read_all(&file[..file.len() - 100], 5);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::TornTail { valid_prefix_len, .. })
                if *valid_prefix_len == chain_start
        ));
        // Cut exactly after the complete chain: clean EOF, nothing torn.
        let events = read_all(&file, 5);
        assert_eq!(record_payloads(&events).len(), 2);
        assert!(matches!(events.last(), Some(BinaryWalReadEvent::Eof)));
    }

    #[test]
    fn mid_log_corruption_fails_closed_even_with_valid_records_after() {
        let payloads = vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
        let mut file = framed_file(1, 1, &payloads);
        // Flip one payload byte of the middle record (a complete chain).
        let second_start = WAL_BINARY_FILE_HEADER_BYTES
            + (WAL_FRAGMENT_HEADER_BYTES + 3)
            + WAL_FRAGMENT_HEADER_BYTES;
        file[second_start] ^= 0xff;
        let events = read_all(&file, 1);
        assert_eq!(record_payloads(&events), vec![b"one".as_slice()]);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("checksum mismatch")
        ));
    }

    #[test]
    fn sequence_violations_fail_closed_mid_log() {
        // An orphan continuation fragment (LAST without FIRST) is corruption.
        let mut file = encode_binary_wal_header(1, 1);
        let chunk = b"orphan";
        let crc = masked_fragment_crc(FRAGMENT_LAST, 1, chunk);
        file.extend_from_slice(&crc.to_le_bytes());
        file.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
        file.push(FRAGMENT_LAST);
        file.extend_from_slice(&1u64.to_le_bytes());
        file.extend_from_slice(chunk);
        let events = read_all(&file, 1);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::Corrupt { reason, .. })
                if reason.contains("without a FIRST fragment")
        ));
    }

    #[test]
    fn chain_interrupted_by_full_fragment_at_eof_is_a_torn_tail() {
        // FIRST (of an unfinished chain) followed by a FULL fragment at end
        // of file: the chain is incomplete, so the tail is torn from the
        // chain start; nothing after it may be silently replayed.
        let mut file = encode_binary_wal_header(1, 1);
        let chain_start = file.len() as u64;
        let chunk = b"unfinished";
        let crc = masked_fragment_crc(FRAGMENT_FIRST, 1, chunk);
        file.extend_from_slice(&crc.to_le_bytes());
        file.extend_from_slice(&(chunk.len() as u16).to_le_bytes());
        file.push(FRAGMENT_FIRST);
        file.extend_from_slice(&1u64.to_le_bytes());
        file.extend_from_slice(chunk);
        let position = file.len() as u64 - WAL_BINARY_FILE_HEADER_BYTES as u64;
        file.extend_from_slice(&frame_binary_wal_record(1, b"full-after-first", position));
        let events = read_all(&file, 1);
        assert!(matches!(
            events.last(),
            Some(BinaryWalReadEvent::TornTail { valid_prefix_len, .. })
                if *valid_prefix_len == chain_start
        ));
    }

    #[test]
    fn record_limit_is_enforced_on_assembled_payloads() {
        let big = vec![0x42u8; WAL_BLOCK_BYTES * 2];
        let file = framed_file(1, 1, &[big]);
        let mut reader = BinaryWalReader::new(
            &file[WAL_BINARY_FILE_HEADER_BYTES..],
            1,
            Some(WAL_BLOCK_BYTES),
        );
        assert!(reader
            .next_event()
            .unwrap_err()
            .to_string()
            .contains("WAL record byte limit exceeded"));
    }
}
