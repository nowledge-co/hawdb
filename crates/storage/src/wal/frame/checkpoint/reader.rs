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

//! Account physical read buffers and fragment-chain growth under the admitted task.
//! Ordinary framing and recovery remain independent event/byte references.

use super::*;

#[cfg(test)]
mod tests;

/// One event from the binary fragment reader. Offsets are absolute file
/// offsets (the 28-byte file header included).
#[derive(Debug)]
pub(crate) enum CheckpointWalReadEvent {
    Record {
        payload: CheckpointBytes,
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
    payload: CheckpointBytes,
}

pub(crate) struct CheckpointBinaryWalReader<R: Read> {
    reader: R,
    work: CheckpointWorkContext,
    generation: u64,
    max_record_bytes: Option<usize>,
    block: CheckpointBytes,
    block_len: usize,
    block_pos: usize,
    /// Absolute file offset of `block[0]`.
    block_offset: u64,
    /// The stream ended inside the current block.
    stream_ended: bool,
    block_loaded: bool,
    chain: Option<PendingChain>,
    finished: bool,
    // A checkpoint captures a complete byte boundary while owning the writer.
    // Later appends must not extend this reader's captured interval.
    end_offset: Option<u64>,
}

impl<R: Read + std::io::Seek> CheckpointBinaryWalReader<R> {
    pub(crate) fn range(
        reader: R,
        generation: u64,
        max_record_bytes: Option<usize>,
        from_offset: u64,
        to_offset: u64,
        work: &CheckpointWorkContext,
    ) -> Result<Self> {
        let header_bytes = WAL_BINARY_FILE_HEADER_BYTES as u64;
        if from_offset < header_bytes || from_offset > to_offset {
            return Err(HawDBError::Storage(
                "invalid captured WAL byte interval".into(),
            ));
        }
        let data_offset = from_offset - header_bytes;
        let block_start =
            header_bytes + data_offset / WAL_BLOCK_BYTES as u64 * WAL_BLOCK_BYTES as u64;
        let mut cursor = Self::new(reader, generation, max_record_bytes, work)?;
        let wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        cursor.reader.seek(std::io::SeekFrom::Start(block_start))?;
        drop(wave);
        cursor.block_offset = block_start;
        cursor.end_offset = Some(to_offset);
        if from_offset == to_offset {
            cursor.finished = true;
            return Ok(cursor);
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        cursor.load_next_block()?;
        unit.finish();
        let skip = usize::try_from(from_offset - block_start).map_err(|_| {
            HawDBError::Storage("captured WAL offset exceeds platform limits".into())
        })?;
        if skip > cursor.block_len {
            return Err(HawDBError::Storage(
                "captured WAL interval starts beyond the available bytes".into(),
            ));
        }
        cursor.block_pos = skip;
        Ok(cursor)
    }
}

impl<R: Read> CheckpointBinaryWalReader<R> {
    pub(crate) fn new(
        reader: R,
        generation: u64,
        max_record_bytes: Option<usize>,
        work: &CheckpointWorkContext,
    ) -> Result<Self> {
        Ok(Self {
            reader,
            work: work.clone(),
            generation,
            max_record_bytes,
            block: CheckpointBytes::zeroed(WAL_BLOCK_BYTES, work)
                .map_err(HawDBError::from_storage_error)?,
            block_len: 0,
            block_pos: 0,
            block_offset: WAL_BINARY_FILE_HEADER_BYTES as u64,
            stream_ended: false,
            block_loaded: false,
            chain: None,
            finished: false,
            end_offset: None,
        })
    }

    fn load_next_block(&mut self) -> Result<()> {
        self.block_offset += self.block_len as u64;
        self.block_len = 0;
        self.block_pos = 0;
        while self.block_len < WAL_BLOCK_BYTES {
            let remaining = self
                .end_offset
                .map_or(WAL_BLOCK_BYTES - self.block_len, |end| {
                    usize::try_from(end.saturating_sub(self.block_offset + self.block_len as u64))
                        .unwrap_or(usize::MAX)
                        .min(WAL_BLOCK_BYTES - self.block_len)
                });
            if remaining == 0 {
                self.stream_ended = true;
                break;
            }
            let wave = self
                .work
                .io_wave()
                .map_err(HawDBError::from_storage_error)?;
            let read = self
                .reader
                .read(&mut self.block.as_mut_slice()[self.block_len..self.block_len + remaining])?;
            drop(wave);
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
        let unit = self
            .work
            .start_unit()
            .map_err(HawDBError::from_storage_error)?;
        if !self.block_loaded || (self.block_pos == self.block_len && !self.stream_ended) {
            self.load_next_block()?;
        }
        let result = self.parse_loaded_fragment();
        unit.finish();
        // A physically observed corruption retains priority over a concurrent
        // stop. Valid fragments reach another work checkpoint before copying.
        result
    }

    fn parse_loaded_fragment(&mut self) -> Result<FragmentParse> {
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
            let unit = self
                .work
                .start_unit()
                .map_err(HawDBError::from_storage_error)?;
            self.load_next_block()?;
            let mut live = false;
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
                    live = true;
                }
            }
            unit.finish();
            if live {
                return Ok(true);
            }
            self.work
                .checkpoint()
                .map_err(HawDBError::from_storage_error)?;
            if self.block_len == 0 || self.stream_ended {
                return Ok(false);
            }
        }
    }

    fn end_of_log(&mut self, offset: u64, marker_reason: String) -> Result<CheckpointWalReadEvent> {
        self.finished = true;
        if self.live_fragment_after()? {
            return Ok(CheckpointWalReadEvent::Corrupt {
                offset,
                reason: format!("valid fragments follow an end-of-log marker ({marker_reason})"),
            });
        }
        match self.chain.take() {
            Some(chain) => Ok(CheckpointWalReadEvent::TornTail {
                valid_prefix_len: chain.start_offset,
                reason: format!("fragment chain is incomplete at end of log ({marker_reason})"),
            }),
            None => Ok(CheckpointWalReadEvent::Eof),
        }
    }

    fn chain_violation(&mut self, offset: u64, reason: String) -> Result<CheckpointWalReadEvent> {
        self.finished = true;
        if self.chain.is_some() {
            // Skip past the violating fragment, then resynchronize on block
            // boundaries: live data after it means mid-log damage.
            if self.live_fragment_after()? {
                return Ok(CheckpointWalReadEvent::Corrupt { offset, reason });
            }
            let chain = self.chain.take().expect("pending chain");
            return Ok(CheckpointWalReadEvent::TornTail {
                valid_prefix_len: chain.start_offset,
                reason: format!("fragment chain is incomplete at end of file ({reason})"),
            });
        }
        Ok(CheckpointWalReadEvent::Corrupt { offset, reason })
    }

    pub fn next_event(&mut self) -> Result<CheckpointWalReadEvent> {
        if self.finished {
            return Ok(CheckpointWalReadEvent::Eof);
        }
        loop {
            let fragment_offset = self.block_offset + self.block_pos as u64;
            match self.parse_fragment_at_cursor()? {
                FragmentParse::NeedNextBlock => continue,
                FragmentParse::EndOfStream => {
                    self.finished = true;
                    return match self.chain.take() {
                        Some(chain) => Ok(CheckpointWalReadEvent::TornTail {
                            valid_prefix_len: chain.start_offset,
                            reason: "fragment chain is incomplete at end of file".to_string(),
                        }),
                        None => Ok(CheckpointWalReadEvent::Eof),
                    };
                }
                FragmentParse::Torn { reason } => {
                    self.finished = true;
                    let valid_prefix_len = match self.chain.take() {
                        Some(chain) => chain.start_offset,
                        None => fragment_offset,
                    };
                    return Ok(CheckpointWalReadEvent::TornTail {
                        valid_prefix_len,
                        reason,
                    });
                }
                FragmentParse::Corrupt { reason } => {
                    self.finished = true;
                    return Ok(CheckpointWalReadEvent::Corrupt {
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
                            let length = payload_end - payload_start;
                            self.enforce_record_limit(length)?;
                            let mut payload = CheckpointBytes::new(length, &self.work)
                                .map_err(HawDBError::from_storage_error)?;
                            payload
                                .append(&self.block[payload_start..payload_end], &self.work)
                                .map_err(HawDBError::from_storage_error)?;
                            return Ok(CheckpointWalReadEvent::Record {
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
                            let length = payload_end - payload_start;
                            self.enforce_record_limit(length)?;
                            let mut payload = CheckpointBytes::new(length, &self.work)
                                .map_err(HawDBError::from_storage_error)?;
                            payload
                                .append(&self.block[payload_start..payload_end], &self.work)
                                .map_err(HawDBError::from_storage_error)?;
                            self.chain = Some(PendingChain {
                                start_offset: fragment_offset,
                                payload,
                            });
                        }
                        FRAGMENT_MIDDLE | FRAGMENT_LAST => {
                            let Some(chain) = self.chain.as_mut() else {
                                self.finished = true;
                                return Ok(CheckpointWalReadEvent::Corrupt {
                                    offset: fragment_offset,
                                    reason: "continuation fragment without a FIRST fragment"
                                        .to_string(),
                                });
                            };
                            let length = chain
                                .payload
                                .len()
                                .checked_add(payload_end - payload_start)
                                .ok_or_else(|| {
                                    HawDBError::Storage(
                                        "WAL fragment chain byte count overflows usize".into(),
                                    )
                                })?;
                            if self.max_record_bytes.is_some_and(|limit| length > limit) {
                                return Err(HawDBError::Storage(format!(
                                    "WAL record byte limit exceeded: max_wal_record_bytes={}",
                                    self.max_record_bytes.unwrap_or_default()
                                )));
                            }
                            chain
                                .payload
                                .append_growing(
                                    &self.block[payload_start..payload_end],
                                    self.max_record_bytes,
                                    &self.work,
                                )
                                .map_err(HawDBError::from_storage_error)?;
                            if fragment_type == FRAGMENT_LAST {
                                let chain = self.chain.take().expect("pending chain");
                                return Ok(CheckpointWalReadEvent::Record {
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
