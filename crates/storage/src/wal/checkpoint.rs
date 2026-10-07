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

//! Read only the captured suffix with admitted input ownership and block units.
//! Decoded WalOp/runtime allocation ownership requires separate qualification.

use super::*;
use crate::background::CheckpointWorkContext;

#[cfg(test)]
mod tests;

pub(crate) struct CheckpointWalRecordCursor {
    reader: frame::CheckpointBinaryWalReader<File>,
    work: CheckpointWorkContext,
}

impl CheckpointWalRecordCursor {
    pub(crate) fn open_range(
        path: &Path,
        max_record_bytes: Option<usize>,
        generation: u64,
        start_lsn: u64,
        from_offset: u64,
        to_offset: u64,
        work: &CheckpointWorkContext,
    ) -> Result<Self> {
        let wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        let mut file = File::open(path)?;
        let mut header = [0; frame::WAL_BINARY_FILE_HEADER_BYTES];
        let mut filled = 0;
        while filled < header.len() {
            let read = file.read(&mut header[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled != header.len() {
            return Err(HawDBError::StorageIntegrity(
                "captured WAL has an invalid header".into(),
            ));
        }
        let identity = frame::decode_binary_wal_header(&header).map_err(|_| {
            HawDBError::StorageIntegrity("captured WAL has an invalid header".into())
        })?;
        if identity != (generation, start_lsn) {
            return Err(HawDBError::StorageIntegrity(
                "captured WAL header identity changed".into(),
            ));
        }
        if file.metadata()?.len() < to_offset {
            return Err(HawDBError::StorageIntegrity(
                "captured WAL interval lost previously committed bytes".into(),
            ));
        }
        drop(wave);
        // Reuse the validated handle. No BufReader introduces an unaccounted
        // buffer; the physical reader admits its complete 32 KiB allocation.
        Ok(Self {
            reader: frame::CheckpointBinaryWalReader::range(
                file,
                generation,
                max_record_bytes,
                from_offset,
                to_offset,
                work,
            )?,
            work: work.clone(),
        })
    }

    pub(crate) fn next(&mut self) -> Result<WalCursorEvent> {
        match self.reader.next_event()? {
            frame::CheckpointWalReadEvent::Record {
                payload,
                start_offset,
                end_offset,
            } => {
                let payload_len = payload.len() as u64;
                let payload_sha256 = self
                    .work
                    .integrity(&payload)
                    .map_err(HawDBError::from_storage_error)?
                    .sha256;
                let decoded =
                    binary::decode_binary_wal_record_with_work_context(&payload, &self.work);
                match decoded? {
                    binary::BinaryWalRecordDecode::Entry { entry, .. } => {
                        self.work
                            .checkpoint()
                            .map_err(HawDBError::from_storage_error)?;
                        Ok(WalCursorEvent::Entry {
                            entry,
                            start_offset,
                            encoded_len: end_offset - start_offset,
                            payload_len,
                            payload_sha256,
                        })
                    }
                    binary::BinaryWalRecordDecode::Corrupt(reason) => Ok(WalCursorEvent::Corrupt {
                        offset: start_offset,
                        reason,
                    }),
                }
            }
            frame::CheckpointWalReadEvent::TornTail {
                valid_prefix_len,
                reason,
            } => Ok(WalCursorEvent::TornTail {
                valid_prefix_len,
                reason,
            }),
            frame::CheckpointWalReadEvent::Corrupt { offset, reason } => {
                Ok(WalCursorEvent::Corrupt { offset, reason })
            }
            frame::CheckpointWalReadEvent::Eof => Ok(WalCursorEvent::Eof),
        }
    }
}
