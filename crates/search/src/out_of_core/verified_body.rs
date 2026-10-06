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

//! Verified body transfer retains both the source reader and its private output.

use super::*;
use crate::build_control::checkpoint;
use crate::build_memory::{AdmittedHeader, BuildMemory};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use hawdb_integrity::Crc32cHasher;
use std::io::{self, Read, Seek, SeekFrom};

/// Independent limits for private body staging and retained document headers.
#[derive(Debug, Clone, Copy)]
pub struct SearchBodyReadOptions {
    pub max_body_bytes: NonZeroU64,
    pub max_header_bytes: NonZeroUsize,
}

impl Default for SearchBodyReadOptions {
    fn default() -> Self {
        Self {
            max_body_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_header_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        }
    }
}

/// A privately staged body admitted only after complete source-segment validation.
///
/// Observe successful EOF and `is_complete()` before accepting a transfer. A
/// later cancellation, file error or short consumer read is not completion.
/// The anonymous file, its disk occupancy, and this generation pin remain owned
/// until drop. The initial anonymous-file implementation requires Linux and a
/// filesystem supporting `O_TMPFILE`; unsupported platforms fail before output.
#[derive(Debug)]
pub struct SearchVerifiedBody<'reader> {
    file: File,
    header: AdmittedHeader,
    body_bytes: u64,
    body_checksum: u64,
    read_bytes: u64,
    digest: Crc32cHasher,
    complete: bool,
    failed: bool,
    task: RuntimeTaskContext,
    _pin: &'reader SearchOutOfCoreReader,
    _memory: QueryMemoryLease,
    pub(super) _admission: Option<SearchGenerationAdmission>,
}

impl SearchVerifiedBody<'_> {
    pub fn header(&self) -> &crate::SearchDocumentHeader {
        &self.header
    }
    pub fn body_bytes(&self) -> u64 {
        self.body_bytes
    }
    pub fn body_checksum(&self) -> u64 {
        self.body_checksum
    }
    pub fn is_complete(&self) -> bool {
        self.complete
    }
}

impl Read for SearchVerifiedBody<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.failed {
            return Err(io::Error::other("verified body transfer previously failed"));
        }
        let result = (|| {
            checkpoint(&self.task).map_err(io::Error::other)?;
            let limit = output.len().min(8192);
            let count = self.file.read(&mut output[..limit])?;
            self.digest.update(&output[..count]);
            self.read_bytes = self
                .read_bytes
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::other("verified transfer length overflow"))?;
            if self.read_bytes > self.body_bytes {
                return Err(io::Error::other("verified body grew after admission"));
            }
            if count == 0 {
                if self.read_bytes != self.body_bytes || self.digest.finish() != self.body_checksum
                {
                    return Err(io::Error::other(
                        "verified body transfer integrity mismatch",
                    ));
                }
                self.complete = true;
            }
            Ok(count)
        })();
        if result.is_err() {
            self.failed = true;
            self.complete = false;
        }
        result
    }
}

impl SearchOutOfCoreReader {
    /// Validates the candidate's exact visible version and complete required
    /// segment before returning a privately staged body. No source prefix escapes
    /// on late corruption or admission failure. This does not rerank candidates.
    pub fn open_verified_body(
        &self,
        candidate: &SearchOutOfCoreCandidate,
        options: SearchBodyReadOptions,
        task: RuntimeTaskContext,
    ) -> Result<SearchVerifiedBody<'_>> {
        checkpoint(&task)?;
        if candidate.generation != self.generation() {
            return Err(HawDBError::Storage(
                "body candidate belongs to another generation".into(),
            ));
        }
        let route = self
            .segment_for_document(&candidate.scores.id)?
            .ok_or_else(|| HawDBError::Storage("body candidate is not visible".into()))?;
        let artifact = &self.segments[route.artifact_index];
        if candidate.content_segment_id != artifact.content_segment_id {
            return Err(HawDBError::Storage(
                "body candidate content version mismatch".into(),
            ));
        }
        let range = route
            .segment
            .payload_range
            .ok_or_else(|| HawDBError::Storage("body segment has no payload range".into()))?;
        let memory = BuildMemory::new(&task)?;
        let retained = memory
            .retained
            .reserve(std::mem::size_of::<SearchVerifiedBody<'_>>())?;
        let mut file = File::anonymous(&self.root)?;
        let admission = hydration::ReadAdmission {
            memory: &memory,
            task: &task,
            max_header_bytes: options.max_header_bytes.get(),
        };
        let receipt = hydration::read_validated(
            hydration::RangeReader {
                file: &artifact.payload,
                offset: range.offset,
                remaining: range.length,
            },
            range.length,
            range.checksum,
            self.config.max_uncompressed_segment_bytes.get(),
            Some(admission),
            |text| {
                hydration::selected_body::select(
                    text,
                    route.segment,
                    &candidate.scores.id,
                    &mut file,
                    options.max_body_bytes.get(),
                    admission,
                )
            },
        )?;
        file.seek(SeekFrom::Start(0))?;
        checkpoint(&task)?;
        Ok(SearchVerifiedBody {
            file,
            header: receipt.header,
            body_bytes: receipt.body_bytes,
            body_checksum: receipt.body_checksum,
            read_bytes: 0,
            digest: Crc32cHasher::new(),
            complete: false,
            failed: false,
            task,
            _pin: self,
            _memory: retained,
            _admission: None,
        })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
