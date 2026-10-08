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

//! Validated record references keep a counted immutable spool handle alive.

use super::*;
use crate::analyzer_stream::{Control, TokenOccurrence};
use crate::build_memory::{shared::Shared, AdmittedHeader};
use crate::build_term::Term;
use crate::document_encoding::HeaderSource;
use crate::lexical_projection::{DocumentSource, LexicalProjectionConfig};
use crate::out_of_core::hydration::RangeReader;
use crate::SearchAnalyzerLexicon;
use hawdb_executor::QueryMemoryLease;
use std::mem::size_of;

struct SourceFile {
    file: File,
    _memory: QueryMemoryLease,
}

pub(in super::super) struct SpoolRecord {
    pub(in super::super) header: AdmittedHeader,
    source: Shared<SourceFile>,
    offset: u64,
    pub(in super::super) encoded_bytes: u64,
    checksum: u64,
    body_offset: u64,
    body_bytes: u64,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    _memory: QueryMemoryLease,
}

impl SpoolRecord {
    pub(in super::super) fn add_digest(&self, digest: &mut DocumentsDigest) {
        digest.add_record(self.checksum, self.encoded_bytes);
    }

    pub(in super::super) fn write_to(
        &self,
        output: &mut impl Write,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<()> {
        let _scratch = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut buffer = [0; SPOOL_BUFFER_BYTES];
        let mut input = RangeReader {
            file: &self.source.file,
            offset: self.offset,
            remaining: self.encoded_bytes,
        };
        let mut digest = Crc32cHasher::new();
        loop {
            checkpoint(task)?;
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count])?;
            digest.update(&buffer[..count]);
        }
        if digest.finish() != self.checksum {
            return Err(HawDBError::Storage(
                "immutable spool record checksum changed".into(),
            ));
        }
        Ok(())
    }

    fn body(&self) -> Result<RangeReader<'_>> {
        Ok(RangeReader {
            file: &self.source.file,
            offset: self.body_offset,
            remaining: self
                .body_bytes
                .checked_mul(2)
                .ok_or_else(|| HawDBError::Storage("spool body extent overflow".into()))?,
        })
    }
}

impl DocumentSource for SpoolRecord {
    fn id(&self) -> &str {
        &self.header.id
    }

    fn admit_source(&self, config: LexicalProjectionConfig) -> Result<()> {
        crate::lexical_projection::source::admit_streamed_source(
            self.header(),
            self.body_bytes,
            config,
        )
    }

    fn visit_tokens(
        &self,
        analyzer: &SearchAnalyzerLexicon,
        control: Control<'_>,
        emit: &mut dyn FnMut(u8, usize, Term, TokenOccurrence) -> Result<()>,
    ) -> Result<()> {
        let memory = control.memory.ok_or_else(|| {
            HawDBError::Execution("streamed analysis requires memory admission".into())
        })?;
        let scratch = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let mut input = HexReader {
            input: self.body()?,
            buffer: [0; SPOOL_BUFFER_BYTES],
            position: 0,
            filled: 0,
            _memory: scratch,
        };
        crate::lexical_projection::source::visit_streamed_source(
            self.header(),
            &mut input,
            self.body_bytes,
            analyzer,
            control,
            emit,
        )
    }
}

struct HexReader<R> {
    input: R,
    buffer: [u8; SPOOL_BUFFER_BYTES],
    position: usize,
    filled: usize,
    _memory: QueryMemoryLease,
}

impl<R: Read> HexReader<R> {
    fn next(&mut self) -> io::Result<Option<u8>> {
        if self.position == self.filled {
            self.filled = self.input.read(&mut self.buffer)?;
            self.position = 0;
            if self.filled == 0 {
                return Ok(None);
            }
        }
        let value = self.buffer[self.position];
        self.position += 1;
        Ok(Some(value))
    }
}

impl<R: Read> Read for HexReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        for (index, slot) in output.iter_mut().enumerate() {
            let Some(high) = self.next()? else {
                return Ok(index);
            };
            let low = self.next()?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "odd spool body hex length")
            })?;
            let pair = [high, low];
            *slot = std::str::from_utf8(&pair)
                .ok()
                .and_then(|raw| u8::from_str_radix(raw, 16).ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid spool body hex")
                })?;
        }
        Ok(output.len())
    }
}

struct BodyRange {
    source: Shared<SourceFile>,
    offset: u64,
    remaining: u64,
}

impl Read for BodyRange {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let mut range = RangeReader {
            file: &self.source.file,
            offset: self.offset,
            remaining: self.remaining,
        };
        let count = range.read(output)?;
        self.offset = range.offset;
        self.remaining = range.remaining;
        Ok(count)
    }
}

impl SpoolRecord {
    pub(in super::super) fn into_body(self) -> Result<(AdmittedHeader, impl Read, u64)> {
        let scratch = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let remaining = self
            .body_bytes
            .checked_mul(2)
            .ok_or_else(|| HawDBError::Storage("spool body extent overflow".into()))?;
        Ok((
            self.header,
            HexReader {
                input: BodyRange {
                    source: self.source,
                    offset: self.body_offset,
                    remaining,
                },
                buffer: [0; SPOOL_BUFFER_BYTES],
                position: 0,
                filled: 0,
                _memory: scratch,
            },
            self.body_bytes,
        ))
    }
}

/// One admitted header per source, with bodies retained only in private files.
pub(in super::super) struct SpoolCursor {
    source: Shared<SourceFile>,
    offset: u64,
    length: u64,
    ordinal: usize,
    document_count: usize,
    max_record_bytes: u64,
    max_metadata_fields: usize,
    previous: Option<(String, QueryMemoryLease)>,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    _memory: QueryMemoryLease,
}

impl SpoolCursor {
    pub(in super::super) fn position(&self) -> u64 {
        self.offset
    }

    pub(in super::super) fn next(&mut self) -> Result<Option<SpoolRecord>> {
        checkpoint(&self.task)?;
        if self.ordinal == self.document_count {
            if self.offset != self.length {
                return Err(HawDBError::Storage("spool has trailing records".into()));
            }
            return Ok(None);
        }
        let input = RangeReader {
            file: &self.source.file,
            offset: self.offset,
            remaining: self.length.saturating_sub(self.offset),
        };
        #[cfg(test)]
        let mut input = read_evidence::track_reads(input);
        #[cfg(not(test))]
        let mut input = input;
        let mut frame = [0; 16];
        input.read_exact(&mut frame)?;
        let length = u64::from_le_bytes(frame[..8].try_into().unwrap());
        let checksum = u64::from_le_bytes(frame[8..].try_into().unwrap());
        if length == 0 || length > self.max_record_bytes {
            return Err(HawDBError::Storage("spool record exceeds admission".into()));
        }
        let size = usize::try_from(length)
            .map_err(|_| HawDBError::Storage("spool record length exceeds usize".into()))?;
        let offset = self
            .offset
            .checked_add(SPOOL_FRAME_HEADER_BYTES)
            .ok_or_else(|| HawDBError::Storage("spool offset overflow".into()))?;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= self.length)
            .ok_or_else(|| {
                HawDBError::Storage("spool record is truncated or its extent overflows".into())
            })?;
        let (header, body_offset, body_bytes) = decoding::read_record_admitted(
            &mut input,
            size,
            checksum,
            self.ordinal,
            &self.memory,
            self.max_metadata_fields,
            &self.task,
        )?;
        if self
            .previous
            .as_ref()
            .is_some_and(|previous| previous.0 >= header.id)
        {
            return Err(HawDBError::Storage(
                "spool documents are not strictly ordered".into(),
            ));
        }
        let id_memory = self.memory.retained.reserve(header.id.len())?;
        self.previous = Some((header.id.clone(), id_memory));
        let record_memory = self.memory.retained.reserve(size_of::<SpoolRecord>())?;
        let body_offset = offset
            .checked_add(body_offset)
            .ok_or_else(|| HawDBError::Storage("spool body offset overflow".into()))?;
        self.offset = end;
        self.ordinal += 1;
        Ok(Some(SpoolRecord {
            header,
            source: self.source.clone(),
            offset,
            encoded_bytes: length,
            checksum,
            body_offset,
            body_bytes,
            memory: self.memory.clone(),
            task: self.task.clone(),
            _memory: record_memory,
        }))
    }
}

impl SpoolSource<'_> {
    pub(in super::super) fn range_cursor(
        &self,
        start: u64,
        end: u64,
        documents: usize,
        task: &RuntimeTaskContext,
    ) -> Result<SpoolCursor> {
        let mut cursor = self.cursor(task)?;
        if start < SPOOL_HEADER.len() as u64 || end < start || end > cursor.length {
            return Err(HawDBError::Storage("invalid initial spool range".into()));
        }
        cursor.offset = start;
        cursor.length = end;
        cursor.document_count = documents;
        Ok(cursor)
    }

    pub(in super::super) fn cursor(&self, task: &RuntimeTaskContext) -> Result<SpoolCursor> {
        checkpoint(task)?;
        let cursor_memory = self.memory.spool.reserve(size_of::<SpoolCursor>())?;
        let shared_memory = self
            .memory
            .spool
            .reserve(size_of::<SourceFile>() + 2 * size_of::<usize>())?;
        let file = super::super::io::GenerationIo::new(&self.memory, task)
            .native(&[self.path], || File::open(self.path))??;
        let length = file.metadata()?.len();
        let input = RangeReader {
            file: &file,
            offset: 0,
            remaining: length,
        };
        #[cfg(test)]
        let mut input = read_evidence::track(input);
        #[cfg(not(test))]
        let mut input = input;
        let mut header = [0; 8];
        input.read_exact(&mut header)?;
        if &header != SPOOL_HEADER {
            return Err(HawDBError::Storage("invalid search spool header".into()));
        }
        Ok(SpoolCursor {
            source: Shared::new(SourceFile {
                file,
                _memory: shared_memory,
            }),
            offset: SPOOL_HEADER.len() as u64,
            length,
            ordinal: 0,
            document_count: self.document_count,
            max_record_bytes: self.max_record_bytes,
            max_metadata_fields: self.max_metadata_fields,
            previous: None,
            memory: self.memory.clone(),
            task: task.clone(),
            _memory: cursor_memory,
        })
    }
}

impl SpoolSource<'_> {
    pub(in super::super) fn scan_records(
        &self,
        task: &RuntimeTaskContext,
        consumer: &mut dyn FnMut(u64, SpoolRecord) -> Result<()>,
    ) -> Result<()> {
        let mut cursor = self.cursor(task)?;
        let mut ordinal = 0u64;
        while let Some(record) = cursor.next()? {
            consumer(ordinal, record)?;
            ordinal += 1;
        }
        Ok(())
    }
}

impl crate::document_encoding::HeaderSource for SpoolRecord {
    fn header(&self) -> crate::document_encoding::Header<'_> {
        self.header.header()
    }
}

impl crate::document_encoding::RecordSource for SpoolRecord {
    fn encoded_len(&self, _: Option<&RuntimeTaskContext>) -> Result<usize> {
        usize::try_from(self.encoded_bytes)
            .map_err(|_| HawDBError::Storage("spool record exceeds usize".into()))
    }
    fn write_encoded(&self, output: &mut impl Write) -> io::Result<()> {
        self.write_to(output, &self.memory, &self.task)
            .map_err(io::Error::other)
    }
}
