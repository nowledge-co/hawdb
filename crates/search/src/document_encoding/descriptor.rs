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

use super::*;
use crate::build_control::{checkpoint, CheckedWriter};
use crate::{SearchSegmentDescriptor, SearchSegmentPayloadRange};
use hawdb_core::RuntimeTaskContext;
use hawdb_integrity::Crc32cHasher;
use std::io::Write as _;

pub(crate) struct DescriptorEncoding<'a> {
    descriptor: &'a SearchSegmentDescriptor,
    body_bytes: usize,
    footer: [u8; 32],
    footer_len: usize,
    task: Option<&'a RuntimeTaskContext>,
    bytes: usize,
}

impl<'a> DescriptorEncoding<'a> {
    pub(crate) fn new(descriptor: &'a SearchSegmentDescriptor, max_bytes: u64) -> Result<Self> {
        Self::new_with_context(descriptor, max_bytes, None)
    }

    pub(crate) fn new_with_context(
        descriptor: &'a SearchSegmentDescriptor,
        max_bytes: u64,
        task: Option<&'a RuntimeTaskContext>,
    ) -> Result<Self> {
        task.map_or(Ok(()), checkpoint)?;
        let mut length = EncodedLength::default();
        write_body(
            &mut CheckedSink {
                sink: &mut length,
                task,
            },
            descriptor,
        )
        .map_err(|_| {
            task.and_then(|task| checkpoint(task).err())
                .unwrap_or_else(size_overflow)
        })?;
        let mut encoding = Self {
            descriptor,
            body_bytes: length.0,
            footer: [0; 32],
            footer_len: 0,
            task,
            bytes: 0,
        };
        let mut digest = DigestWriter(Crc32cHasher::new());
        encoding.write_body_to(&mut digest)?;
        // A u64 checksum and the fixed grammar fit without a heap footer.
        let mut footer = io::Cursor::new(&mut encoding.footer[..]);
        writeln!(footer, "checksum\t{}", digest.0.finish())?;
        encoding.footer_len = footer.position() as usize;
        encoding.bytes = encoding
            .body_bytes
            .checked_add(encoding.footer_len)
            .ok_or_else(size_overflow)?;
        if encoding.bytes as u64 > max_bytes {
            return Err(HawDBError::Storage(format!(
                "search generation descriptor requires {} bytes, exceeding {max_bytes}",
                encoding.bytes,
            )));
        }
        Ok(encoding)
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes
    }

    pub(crate) fn write_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        self.write_body_to(writer)?;
        CheckedWriter::new(writer, self.task).write_all(&self.footer[..self.footer_len])
    }

    fn write_body_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        let mut output = CheckedWriter::new(writer, self.task);
        IoSink {
            writer: &mut output,
            error: None,
            remaining: self.body_bytes,
        }
        .write_checked(|sink| write_body(sink, self.descriptor))
    }
}

fn size_overflow() -> HawDBError {
    HawDBError::Storage("search segment descriptor encoded size overflow".into())
}

struct DigestWriter(Crc32cHasher);

impl io::Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        #[cfg(test)]
        tests::record_digest(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn write_body(sink: &mut impl DocumentSink, descriptor: &SearchSegmentDescriptor) -> fmt::Result {
    sink.write_str("HAWDB_SEARCH_SEGMENTS_V3\n")?;
    writeln!(sink, "target_documents\t{}", descriptor.target_documents)?;
    writeln!(sink, "document_count\t{}", descriptor.document_count)?;
    for segment in &descriptor.segments {
        let range = segment.payload_range.unwrap_or(SearchSegmentPayloadRange {
            artifact_id: 0,
            offset: 0,
            length: 0,
            checksum: 0,
        });
        write!(sink, "segment\t{}\t", segment.segment_id)?;
        sink.write_hex(&segment.first_document_id)?;
        sink.write_char('\t')?;
        sink.write_hex(&segment.last_document_id)?;
        writeln!(
            sink,
            "\t{}\t{}\t{}\t{}\t{}",
            segment.document_count, range.artifact_id, range.offset, range.length, range.checksum
        )?;
        for (field, summary) in &segment.metadata {
            sink.write_str("field\t")?;
            sink.write_hex(field)?;
            write!(sink, "\t{}\t", summary.present_count)?;
            for (index, value) in summary.values.iter().enumerate() {
                if index != 0 {
                    sink.write_char(',')?;
                }
                sink.write_hex(value)?;
            }
            sink.write_char('\t')?;
            if let Some(range) = summary.numeric_range {
                write!(sink, "{}\t{}", range.min, range.max)?;
            } else {
                sink.write_char('\t')?;
            }
            sink.write_char('\t')?;
            if let Some(range) = summary.timestamp_range {
                write!(
                    sink,
                    "{}\t{}",
                    range.min_epoch_millis, range.max_epoch_millis
                )?;
            } else {
                sink.write_char('\t')?;
            }
            sink.write_char('\n')?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
