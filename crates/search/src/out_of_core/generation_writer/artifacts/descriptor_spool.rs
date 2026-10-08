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

//! Private descriptor entries are streamed so completed segments release their metadata.

use super::*;
use crate::build_control::CheckedWriter;
use crate::build_memory::path::OwnedPath;
use crate::document_encoding::write_segment_to;
use hawdb_integrity::Crc32cHasher;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};

pub(super) struct DescriptorSpool {
    writer: BufWriter<File>,
    bytes: u64,
    checksum: Crc32cHasher,
    pub(super) segment_count: u64,
    _path: OwnedPath,
    _buffer_memory: QueryMemoryLease,
}

impl DescriptorSpool {
    pub(super) fn new(
        stage: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let path = OwnedPath::join(
            stage,
            Path::new("search-segment-descriptors.spool.hawdb"),
            memory,
            task,
        )?;
        let buffer_memory = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::with_capacity(SPOOL_BUFFER_BYTES, file),
            bytes: 0,
            checksum: Crc32cHasher::new(),
            segment_count: 0,
            _path: path,
            _buffer_memory: buffer_memory,
        })
    }

    pub(super) fn push(
        &mut self,
        descriptor: &SearchSegmentDescriptorEntry,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        limit: u64,
    ) -> Result<()> {
        checkpoint(task)?;
        if descriptor.segment_id != self.segment_count {
            return Err(HawDBError::Storage(
                "descriptor spool segment order changed".into(),
            ));
        }
        let _scratch = memory.spool.reserve(HEX_BUFFER_BYTES)?;
        let mut output = DigestOutput {
            writer: &mut self.writer,
            checksum: &mut self.checksum,
            bytes: &mut self.bytes,
            limit,
        };
        write_segment_to(&mut output, descriptor, task)?;
        self.segment_count = self
            .segment_count
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("descriptor spool segment count overflow".into()))?;
        Ok(())
    }

    pub(super) fn write_descriptor(
        &mut self,
        output: &mut impl Write,
        document_count: usize,
        limit: u64,
        task: &RuntimeTaskContext,
    ) -> Result<u64> {
        checkpoint(task)?;
        self.writer.flush()?;
        let input = self.writer.get_mut();
        if input.metadata()?.len() != self.bytes {
            return Err(HawDBError::Storage(
                "descriptor spool length changed".into(),
            ));
        }
        input.seek(SeekFrom::Start(0))?;
        let mut bytes = 0;
        let mut checksum = Crc32cHasher::new();
        let mut output = DigestOutput {
            writer: output,
            checksum: &mut checksum,
            bytes: &mut bytes,
            limit,
        };
        let mut checked = CheckedWriter::new(&mut output, Some(task));
        writeln!(checked, "HAWDB_SEARCH_SEGMENTS_V3")?;
        writeln!(
            checked,
            "target_documents\t{}",
            SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS
        )?;
        writeln!(checked, "document_count\t{document_count}")?;
        let mut remaining = self.bytes;
        let mut spool_checksum = Crc32cHasher::new();
        let mut buffer = [0; SPOOL_BUFFER_BYTES];
        while remaining != 0 {
            checkpoint(task)?;
            let count = remaining.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..count])?;
            spool_checksum.update(&buffer[..count]);
            checked.write_all(&buffer[..count])?;
            remaining -= count as u64;
        }
        if spool_checksum.finish() != self.checksum.finish() {
            return Err(HawDBError::Storage(
                "descriptor spool checksum changed".into(),
            ));
        }
        let body_checksum = output.checksum.finish();
        writeln!(
            CheckedWriter::new(&mut output, Some(task)),
            "checksum\t{body_checksum}"
        )?;
        Ok(*output.bytes)
    }
}

struct DigestOutput<'a, W> {
    writer: &'a mut W,
    checksum: &'a mut Crc32cHasher,
    bytes: &'a mut u64,
    limit: u64,
}

impl<W: Write> Write for DigestOutput<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let projected = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("search descriptor encoded size overflow"))?;
        if projected > self.limit {
            return Err(io::Error::other(format!(
                "search generation descriptor requires at least {projected} bytes, exceeding {}",
                self.limit,
            )));
        }
        self.writer.write_all(bytes)?;
        self.checksum.update(bytes);
        *self.bytes = projected;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;
