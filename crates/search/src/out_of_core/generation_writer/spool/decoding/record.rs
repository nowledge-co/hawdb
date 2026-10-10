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

//! Validate an immutable frame while retaining only its bounded header.

use super::*;
use crate::analyzer_stream::{reader::utf8, Control};
use crate::build_memory::AdmittedHeader;
use crate::SearchDocumentHeader;

pub(in super::super) fn read_record_admitted(
    reader: &mut impl Read,
    length: usize,
    expected_checksum: u64,
    ordinal: usize,
    memory: &BuildMemory,
    max_metadata_fields: usize,
    task: &RuntimeTaskContext,
) -> Result<(AdmittedHeader, u64, u64)> {
    let admission = Admission::new(memory, max_metadata_fields, task)?;
    let _scratch = memory.spool.reserve(INPUT_BYTES)?;
    let mut frame = FrameReader {
        reader,
        unread: length,
        buffer: [0; INPUT_BYTES],
        position: 0,
        filled: 0,
        digest: Crc32cHasher::new(),
        ordinal,
        admission: Some(admission),
        consumed: 0,
    };
    let result = read_header(&mut frame, memory, task, length as u64 / 2);
    frame.position = frame.filled;
    while frame.fill()? {
        frame.position = frame.filled;
    }
    if frame.digest.finish() != expected_checksum {
        return Err(frame.invalid("checksum mismatch"));
    }
    let (header, offset, bytes) = result?;
    let admission = frame.admission.take().expect("admitted frame");
    // The existing admission owns every header string, vector and map entry.
    Ok((
        AdmittedHeader {
            header,
            _memory: admission.into_lease(),
        },
        offset,
        bytes,
    ))
}

fn read_header<R: Read>(
    frame: &mut FrameReader<'_, R>,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    max_body_bytes: u64,
) -> Result<(SearchDocumentHeader, u64, u64)> {
    for byte in b"doc\t" {
        if frame.next()? != Some(*byte) {
            return Err(frame.invalid("invalid document prefix"));
        }
    }
    let id = frame.string_column()?;
    let title = frame.string_column()?;
    let body_offset = frame.consumed;
    let mut body = Body {
        frame,
        ended: false,
    };
    let body_bytes = utf8::visit(
        &mut body,
        Control {
            memory: Some(crate::analyzer_memory::Memory::Build(memory)),
            task: Some(task),
            ..Control::default()
        },
        max_body_bytes,
        |_| Ok(()),
    )?;
    let embedding = frame.embedding()?;
    let metadata = frame.metadata()?;
    Ok((
        SearchDocumentHeader {
            id,
            title,
            embedding,
            metadata,
        },
        body_offset,
        body_bytes,
    ))
}

struct Body<'a, 'frame, R> {
    frame: &'a mut FrameReader<'frame, R>,
    ended: bool,
}

impl<R: Read> Read for Body<'_, '_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.ended {
            return Ok(0);
        }
        for (index, slot) in output.iter_mut().enumerate() {
            let next = self.frame.next().map_err(io::Error::other)?;
            if next == Some(b'\t') {
                self.ended = true;
                return Ok(index);
            }
            let high = next.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "missing body separator")
            })?;
            let low = self
                .frame
                .next()
                .map_err(io::Error::other)?
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "incomplete body hex pair")
                })?;
            let pair = [high, low];
            *slot = std::str::from_utf8(&pair)
                .ok()
                .and_then(|raw| u8::from_str_radix(raw, 16).ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid body hex field")
                })?;
        }
        Ok(output.len())
    }
}
