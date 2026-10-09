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

use crate::build_control::{checkpoint, CheckedWriter};
#[cfg(test)]
use crate::build_memory::AdmittedDocument;
use crate::build_memory::{BuildMemory, SPOOL_BUFFER_BYTES};
#[cfg(test)]
use crate::checksum_bytes;
use crate::document_encoding::DocumentEncoding;
use crate::error::{HawDBError, Result};
use crate::lexical_projection::DocumentsDigest;
use crate::SearchDocument;
use hawdb_core::RuntimeTaskContext;
use hawdb_integrity::Crc32cHasher;
use hawdb_storage::file_io::{self as fs, File};
#[cfg(test)]
use std::io::BufReader;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) const SPOOL_HEADER: &[u8; 8] = b"SKNSPOL1";
pub(super) const SPOOL_FRAME_HEADER_BYTES: u64 = 16;
static GENERATION_WRITER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct FrameDigests {
    record: Crc32cHasher,
}

impl Write for FrameDigests {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.record.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Append an already admitted frame without retaining its encoded payload.
#[cfg(test)]
pub(super) fn write_frame(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut DocumentsDigest,
) -> Result<()> {
    write_frame_inner(output, encoding, documents_digest, None)
}

pub(super) fn write_frame_with_context(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut DocumentsDigest,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    let _scratch_memory = memory
        .spool
        .reserve(crate::document_encoding::HEX_BUFFER_BYTES)?;
    write_frame_inner(output, encoding, documents_digest, Some(task))
}

fn write_frame_inner(
    output: &mut impl Write,
    encoding: &DocumentEncoding<'_>,
    documents_digest: &mut DocumentsDigest,
    task: Option<&RuntimeTaskContext>,
) -> Result<()> {
    // The existing format places the checksum before the payload. A bounded
    // prepass keeps writes sequential without per-document seek/flush. The
    // borrowed source cannot change between the two encoding passes.
    let mut digests = FrameDigests {
        record: Crc32cHasher::new(),
    };
    encoding.write_to(&mut CheckedWriter::new(&mut digests, task))?;
    let mut output = CheckedWriter::new(output, task);
    output.write_all(&(encoding.len() as u64).to_le_bytes())?;
    output.write_all(&digests.record.finish().to_le_bytes())?;
    encoding.write_to(&mut output)?;
    // Failed or partial writes must not commit a new logical stream identity.
    documents_digest.add_record(digests.record.finish(), encoding.len() as u64);
    Ok(())
}

#[cfg(test)]
mod write_tests;

mod decoding;
mod records;
pub(super) use records::{SpoolCursor, SpoolRecord};

#[cfg(test)]
pub(super) fn decode_line_admitted(
    line: &[u8],
    ordinal: usize,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<AdmittedDocument> {
    let mut digest = Crc32cHasher::new();
    digest.update(line);
    let mut input = line;
    decoding::read_frame_admitted(
        &mut input,
        line.len(),
        digest.finish(),
        ordinal,
        memory,
        usize::MAX,
        task,
    )
}

pub(super) struct SpoolSource<'a> {
    pub(super) path: &'a Path,
    pub(super) document_count: usize,
    pub(super) max_record_bytes: u64,
    pub(super) max_metadata_fields: usize,
    pub(super) memory: BuildMemory,
}

impl SpoolSource<'_> {
    #[cfg(test)]
    pub(super) fn scan(
        &self,
        consumer: &mut dyn FnMut(u64, SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_with_context(&RuntimeTaskContext::default(), consumer)
    }

    #[cfg(test)]
    pub(super) fn scan_with_context(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(u64, SearchDocument) -> Result<()>,
    ) -> Result<()> {
        self.scan_admitted(task_context, &mut |ordinal, document| {
            let (document, _lease) = document.into_parts();
            consumer(ordinal, document)
        })
    }

    #[cfg(test)]
    pub(super) fn scan_admitted(
        &self,
        task_context: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(u64, AdmittedDocument) -> Result<()>,
    ) -> Result<()> {
        checkpoint(task_context)?;
        let _buffer_memory = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let file = super::io::GenerationIo::new(&self.memory, task_context)
            .native(&[self.path], || File::open(self.path))??;
        #[cfg(test)]
        let file = read_evidence::track(file);
        let mut reader = BufReader::with_capacity(SPOOL_BUFFER_BYTES, file);
        let mut header = [0u8; SPOOL_HEADER.len()];
        reader.read_exact(&mut header)?;
        if &header != SPOOL_HEADER {
            return Err(HawDBError::Storage(
                "search generation spool header is invalid".to_string(),
            ));
        }
        let mut _previous_id_memory = None;
        let mut previous_id = None::<String>;
        for ordinal in 0..self.document_count {
            checkpoint(task_context)?;
            let mut raw_length = [0u8; 8];
            let mut raw_checksum = [0u8; 8];
            reader.read_exact(&mut raw_length).map_err(|error| {
                HawDBError::Storage(format!(
                    "search generation spool record {ordinal} has a truncated length: {error}"
                ))
            })?;
            reader.read_exact(&mut raw_checksum).map_err(|error| {
                HawDBError::Storage(format!(
                    "search generation spool record {ordinal} has a truncated checksum: {error}"
                ))
            })?;
            let length = u64::from_le_bytes(raw_length);
            if length == 0 || length > self.max_record_bytes {
                return Err(HawDBError::Storage(format!(
                    "search generation spool record {ordinal} length {length} is outside its admission"
                )));
            }
            let length = usize::try_from(length).map_err(|_| {
                HawDBError::Storage(format!(
                    "search generation spool record {ordinal} length exceeds usize"
                ))
            })?;
            let expected_checksum = u64::from_le_bytes(raw_checksum);
            let document = decoding::read_frame_admitted(
                &mut reader,
                length,
                expected_checksum,
                ordinal,
                &self.memory,
                self.max_metadata_fields,
                task_context,
            )?;
            checkpoint(task_context)?;
            if previous_id
                .as_ref()
                .is_some_and(|previous| previous >= &document.id)
            {
                return Err(HawDBError::Storage(format!(
                    "search generation spool record {ordinal} is not strictly ordered"
                )));
            }
            let previous_id_memory = self.memory.retained.reserve(document.id.len())?;
            previous_id = Some(document.id.clone());
            _previous_id_memory = Some(previous_id_memory);
            let document_ordinal = u64::try_from(ordinal).map_err(|_| {
                HawDBError::Storage("search document ordinal exceeds u64".to_string())
            })?;
            consumer(document_ordinal, document)?;
            checkpoint(task_context)?;
        }
        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err(HawDBError::Storage(
                "search generation spool has trailing records".to_string(),
            ));
        }
        Ok(())
    }
}

mod stage;
#[cfg(test)]
pub(super) use stage::evidence::fail_unlink;
pub(in crate::out_of_core) use stage::retry_staging_cleanup;
pub use stage::SearchStagingCleanupReport;
pub(in crate::out_of_core) use stage::StageDirectory;

#[cfg(test)]
pub(crate) use crate::build_control::read_observation as read_evidence;

#[cfg(test)]
mod stage_tests;
