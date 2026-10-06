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

//! Bounded selected-body capture behind the complete-envelope integrity fence.

use super::*;
use crate::analyzer_stream::{reader::utf8, Control};
use crate::build_memory::{checked_add, checked_mul, reserve_capacity, MAP_ENTRY_BYTES};
use hawdb_executor::QueryMemoryLease;
use std::io::Write;

type Header = crate::build_memory::AdmittedHeader;

pub(in crate::out_of_core) struct Receipt {
    pub(in crate::out_of_core) header: Header,
    pub(in crate::out_of_core) body_bytes: u64,
    pub(in crate::out_of_core) body_checksum: u64,
    pub(in crate::out_of_core) needs_chinese: bool,
}

struct Columns<'a> {
    bytes: Vec<u8>,
    memory: QueryMemoryLease,
    admission: ReadAdmission<'a>,
}

impl<'a> Columns<'a> {
    fn new(admission: ReadAdmission<'a>) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            memory: admission.memory.input.reserve(0)?,
            admission,
        })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        let required = checked_add(self.bytes.len(), bytes.len())?;
        if required > self.admission.max_header_bytes {
            return Err(invalid("document header exceeds admission"));
        }
        let capacity = required.max(
            self.bytes
                .capacity()
                .saturating_mul(2)
                .min(self.admission.max_header_bytes),
        );
        reserve_capacity(&mut self.bytes, capacity, &mut self.memory)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn column(&mut self, text: &mut dyn BufRead, separators: &[u8]) -> Result<Option<u8>> {
        loop {
            crate::build_control::checkpoint(self.admission.task)?;
            let available = match text.fill_buf() {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if available.is_empty() {
                return Ok(None);
            }
            let end = available.iter().position(|byte| separators.contains(byte));
            let count = end.unwrap_or(available.len());
            let separator = end.map(|index| available[index]);
            self.append(&available[..count])?;
            text.consume(count + usize::from(separator.is_some()));
            if separator.is_some() {
                return Ok(separator);
            }
        }
    }

    fn tab(&mut self, text: &mut dyn BufRead) -> Result<()> {
        if self.column(text, b"\t\n")? != Some(b'\t') {
            return Err(invalid("missing document field"));
        }
        self.append(b"\t")
    }

    fn decode(&self) -> Result<Header> {
        let raw = std::str::from_utf8(&self.bytes).map_err(|_| invalid("header is not UTF-8"))?;
        let mut fields = raw.split('\t');
        let mut next = || {
            fields
                .next()
                .ok_or_else(|| invalid("missing document field"))
        };
        let id = next()?;
        let title = next()?;
        let embedding = next()?;
        let metadata = next()?;
        if fields.next().is_some() {
            return Err(invalid("extra document field"));
        }
        // Reserve decoding overlap, vector growth and every potential B-tree
        // entry before the existing scalar/metadata parsers allocate anything.
        let entries = checked_add(metadata.bytes().filter(|byte| *byte == b';').count(), 2)?;
        let required = checked_add(
            std::mem::size_of::<Header>(),
            checked_add(
                checked_mul(self.bytes.len(), 8)?,
                checked_mul(entries, MAP_ENTRY_BYTES)?,
            )?,
        )?;
        let memory = self.admission.memory.input.reserve(required)?;
        Ok(Header {
            header: crate::SearchDocumentHeader {
                id: crate::decode_string(id).map_err(|_| invalid("invalid document id"))?,
                title: crate::decode_string(title).map_err(|_| invalid("invalid title"))?,
                embedding: crate::decode_embedding(embedding)
                    .map_err(|_| invalid("invalid embedding"))?,
                metadata: crate::decode_metadata(metadata)
                    .map_err(|_| invalid("invalid metadata"))?,
            },
            _memory: memory,
        })
    }
}

/// Decode a single hex column without reading into the following column.
struct HexBody<'a> {
    text: &'a mut dyn BufRead,
    done: bool,
}

impl Read for HexBody<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.done || output.is_empty() {
            return Ok(0);
        }
        let mut written = 0;
        let mut high = None;
        while written < output.len() {
            let available = match self.text.fill_buf() {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if available.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unterminated body field",
                ));
            }
            let mut consumed = 0;
            for &byte in available {
                consumed += 1;
                if byte == b'\t' {
                    if high.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "odd body hex length",
                        ));
                    }
                    self.done = true;
                    break;
                }
                if let Some(first) = high.take() {
                    let pair = [first, byte];
                    let value = std::str::from_utf8(&pair)
                        .ok()
                        .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid body hex")
                        })?;
                    output[written] = value;
                    written += 1;
                    if written == output.len() {
                        break;
                    }
                } else {
                    high = Some(byte);
                }
            }
            self.text.consume(consumed);
            if self.done {
                break;
            }
        }
        Ok(written)
    }
}

pub(in crate::out_of_core) fn select(
    text: &mut dyn BufRead,
    segment: &SearchSegmentDescriptorEntry,
    requested: &str,
    output: &mut impl Write,
    max_body_bytes: u64,
    admission: ReadAdmission<'_>,
) -> Result<Receipt> {
    struct Selected<'a, W> {
        id: &'a str,
        output: &'a mut W,
        receipt: Option<Receipt>,
    }
    impl<W: Write> Consumer for Selected<'_, W> {
        fn start(&mut self, id: &str) -> Result<bool> {
            Ok(id == self.id)
        }
        fn body(&mut self, bytes: &[u8]) -> Result<()> {
            self.output.write_all(bytes)?;
            Ok(())
        }
        fn finish(&mut self, receipt: Receipt) -> Result<()> {
            if receipt.header.id == self.id {
                self.receipt = Some(receipt);
            }
            Ok(())
        }
    }
    let mut consumer = Selected {
        id: requested,
        output,
        receipt: None,
    };
    visit(text, segment, &mut consumer, max_body_bytes, admission)?;
    consumer.output.flush()?;
    consumer
        .receipt
        .ok_or_else(|| invalid("selected document was not found"))
}

/// Consumers may only write private state until `read_validated` succeeds.
pub(in crate::out_of_core) trait Consumer {
    fn start(&mut self, id: &str) -> Result<bool>;
    fn body(&mut self, bytes: &[u8]) -> Result<()>;
    fn finish(&mut self, receipt: Receipt) -> Result<()>;
}

pub(in crate::out_of_core) fn visit(
    text: &mut dyn BufRead,
    segment: &SearchSegmentDescriptorEntry,
    consumer: &mut impl Consumer,
    max_body_bytes: u64,
    admission: ReadAdmission<'_>,
) -> Result<()> {
    let mut previous = None::<(String, QueryMemoryLease)>;
    let mut count = 0usize;
    let mut columns = Columns::new(admission)?;
    loop {
        columns.bytes.clear();
        let end = columns.column(text, b"\t\n")?;
        if end != Some(b'\t') {
            let line = columns.bytes.strip_suffix(b"\r").unwrap_or(&columns.bytes);
            if !line.is_empty() && line != b"HAWDB_SEARCH_SEGMENT_V1" {
                return Err(invalid("invalid document prefix"));
            }
            if end.is_none() {
                break;
            }
            continue;
        }
        if columns.bytes != b"doc" {
            return Err(invalid("invalid document prefix"));
        }
        columns.bytes.clear();
        columns.tab(text)?;
        let id_end = columns.bytes.len() - 1;
        columns.tab(text)?;
        let id_memory = admission.memory.input.reserve(id_end)?;
        let id = std::str::from_utf8(&columns.bytes[..id_end])
            .ok()
            .and_then(|id| crate::decode_string(id).ok())
            .ok_or_else(|| invalid("invalid document id"))?;
        let wanted = consumer.start(&id)?;
        if count == 0 && id != segment.first_document_id
            || previous.as_ref().is_some_and(|previous| previous.0 >= id)
        {
            return Err(invalid("document bounds or order mismatch"));
        }
        drop(id);
        drop(id_memory);
        let mut digest = Crc32cHasher::new();
        let mut needs_chinese = false;
        let body_bytes = utf8::visit(
            &mut HexBody { text, done: false },
            Control {
                memory: Some(admission.memory),
                task: Some(admission.task),
                workspace: None,
                checkpoint_throttle: None,
            },
            if wanted { max_body_bytes } else { u64::MAX },
            |part| {
                if wanted {
                    consumer.body(part.as_bytes())?;
                }
                digest.update(part.as_bytes());
                needs_chinese |= part.chars().any(crate::cjk_tokenizer::is_han_search_char);
                Ok(())
            },
        )?;
        columns.tab(text)?;
        let end = columns.column(text, b"\n")?;
        if end.is_some() && columns.bytes.last() == Some(&b'\r') {
            columns.bytes.pop();
        }
        let header = columns.decode()?;
        count = count
            .checked_add(1)
            .filter(|count| *count <= segment.document_count)
            .ok_or_else(|| invalid("document count exceeds descriptor"))?;
        let memory = admission.memory.input.reserve(header.id.len())?;
        let previous_id = header.id.clone();
        previous = Some((previous_id, memory));
        if wanted {
            consumer.finish(Receipt {
                header,
                body_bytes,
                body_checksum: digest.finish(),
                needs_chinese,
            })?;
        }
        if end.is_none() {
            break;
        }
    }
    if count != segment.document_count
        || previous.as_ref().map(|previous| previous.0.as_str())
            != Some(segment.last_document_id.as_str())
    {
        return Err(invalid("document count or bounds mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
