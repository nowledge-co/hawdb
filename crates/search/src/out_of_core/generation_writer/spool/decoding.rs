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
use hawdb_executor::QueryMemoryLease;
use std::collections::BTreeMap;

mod admission;
use admission::Admission;

const INPUT_BYTES: usize = 8192;

/// Decode an admitted record without retaining its complete encoded payload.
#[cfg(test)]
pub(super) fn read_frame(
    reader: &mut impl Read,
    length: usize,
    expected_checksum: u64,
    ordinal: usize,
) -> Result<SearchDocument> {
    decode_frame(reader, length, expected_checksum, ordinal, None).map(|(document, _)| document)
}

pub(super) fn read_frame_admitted(
    reader: &mut impl Read,
    length: usize,
    expected_checksum: u64,
    ordinal: usize,
    memory: &BuildMemory,
    max_metadata_fields: usize,
    task: &RuntimeTaskContext,
) -> Result<AdmittedDocument> {
    let admission = Admission::new(memory, max_metadata_fields, task)?;
    let _scratch_memory = memory.spool.reserve(INPUT_BYTES)?;
    let (document, lease) =
        decode_frame(reader, length, expected_checksum, ordinal, Some(admission))?;
    Ok(AdmittedDocument::from_admitted_parts(
        document,
        lease.expect("admitted decoding retains its originating lease"),
    ))
}

fn decode_frame(
    reader: &mut impl Read,
    length: usize,
    expected_checksum: u64,
    ordinal: usize,
    admission: Option<Admission<'_>>,
) -> Result<(SearchDocument, Option<QueryMemoryLease>)> {
    let mut frame = FrameReader {
        reader,
        unread: length,
        buffer: [0; INPUT_BYTES],
        position: 0,
        filled: 0,
        digest: Crc32cHasher::new(),
        ordinal,
        admission,
    };
    let document = frame.document();
    // As with the original decoder, validate the whole frame before exposing
    // either a document or a syntax error. A corrupt prefix cannot bypass the
    // checksum or hide a later I/O failure.
    frame.position = frame.filled;
    while frame.fill()? {
        frame.position = frame.filled;
    }
    if frame.digest.finish() != expected_checksum {
        return Err(frame.invalid("checksum mismatch"));
    }
    let document = document?;
    if let Some(admission) = &mut frame.admission {
        admission.finish(&document)?;
    }
    let lease = frame.admission.take().map(Admission::into_lease);
    Ok((document, lease))
}

struct FrameReader<'a, R> {
    reader: &'a mut R,
    unread: usize,
    buffer: [u8; INPUT_BYTES],
    position: usize,
    filled: usize,
    digest: Crc32cHasher,
    ordinal: usize,
    admission: Option<Admission<'a>>,
}

impl<R: Read> FrameReader<'_, R> {
    fn invalid(&self, reason: impl std::fmt::Display) -> HawDBError {
        HawDBError::Storage(format!(
            "search generation spool record {} {reason}",
            self.ordinal
        ))
    }

    fn fill(&mut self) -> Result<bool> {
        if self.position < self.filled {
            return Ok(true);
        }
        if self.unread == 0 {
            return Ok(false);
        }
        if let Some(admission) = &self.admission {
            admission.checkpoint()?;
        }
        let limit = self.unread.min(INPUT_BYTES);
        let count = loop {
            match self.reader.read(&mut self.buffer[..limit]) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        }
        .map_err(|error| self.invalid(format_args!("is truncated: {error}")))?;
        if count == 0 {
            return Err(self.invalid("is truncated: unexpected end of file"));
        }
        self.digest.update(&self.buffer[..count]);
        self.unread -= count;
        self.position = 0;
        self.filled = count;
        Ok(true)
    }

    fn peek(&mut self) -> Result<Option<u8>> {
        Ok(self.fill()?.then(|| self.buffer[self.position]))
    }

    fn next(&mut self) -> Result<Option<u8>> {
        let byte = self.peek()?;
        self.position += usize::from(byte.is_some());
        Ok(byte)
    }

    fn document(&mut self) -> Result<SearchDocument> {
        for expected in b"doc\t" {
            if self.next()? != Some(*expected) {
                return Err(self.invalid("has an invalid document prefix"));
            }
        }
        let id = self.string_column()?;
        let title = self.string_column()?;
        let content = self.string_column()?;
        let embedding = self.embedding()?;
        let metadata = self.metadata()?;
        Ok(SearchDocument {
            id,
            title,
            content,
            embedding,
            metadata,
        })
    }

    fn string_column(&mut self) -> Result<String> {
        let (value, separator) = self.hex(b"\t")?;
        if separator != Some(b'\t') {
            return Err(self.invalid("is missing a document field"));
        }
        Ok(value)
    }

    fn hex(&mut self, separators: &[u8]) -> Result<(String, Option<u8>)> {
        let mut decoded = Vec::new();
        let mut first = None;
        let separator = loop {
            let Some(byte) = self.next()? else {
                break None;
            };
            if separators.contains(&byte) {
                break Some(byte);
            }
            if !byte.is_ascii() {
                return Err(self.invalid("has a non-ASCII hex field"));
            }
            if let Some(high) = first.take() {
                let pair = [high, byte];
                // Reuse the old radix semantics, including uppercase digits
                // and accepted leading-plus pairs, without slicing UTF-8.
                let pair = std::str::from_utf8(&pair).expect("ASCII hex pair");
                let value = u8::from_str_radix(pair, 16)
                    .map_err(|_| self.invalid("has an invalid hex field"))?;
                self.reserve(&mut decoded)?;
                decoded.push(value);
            } else {
                first = Some(byte);
            }
        };
        if first.is_some() {
            return Err(self.invalid("has an odd hex field length"));
        }
        let decoded = String::from_utf8(decoded)
            .map_err(|_| self.invalid("has a decoded field that is not UTF-8"))?;
        Ok((decoded, separator))
    }

    fn embedding(&mut self) -> Result<Option<Vec<f32>>> {
        if self.peek()? == Some(b'\t') {
            self.next()?;
            return Ok(None);
        }
        let mut values = Vec::new();
        // A valid noncanonical float can have an arbitrarily long spelling.
        // Its temporary token remains an explicit resident unit admitted by
        // the frame length; do not silently introduce a numeric-token cap.
        let mut token = Vec::new();
        loop {
            let separator = loop {
                match self.next()? {
                    Some(byte @ (b',' | b'\t')) => break byte,
                    Some(byte) => {
                        self.reserve(&mut token)?;
                        token.push(byte);
                    }
                    None => return Err(self.invalid("is missing its metadata field")),
                }
            };
            let value = std::str::from_utf8(&token)
                .ok()
                .and_then(|raw| raw.parse::<f32>().ok())
                .ok_or_else(|| self.invalid("has an invalid embedding value"))?;
            self.reserve(&mut values)?;
            values.push(value);
            token.clear();
            if separator == b'\t' {
                let token_bytes = token.capacity();
                drop(token);
                if let Some(admission) = &mut self.admission {
                    admission.release(token_bytes);
                }
                return Ok(Some(values));
            }
        }
    }

    fn metadata(&mut self) -> Result<BTreeMap<String, String>> {
        let mut metadata = BTreeMap::new();
        match self.peek()? {
            None => return Ok(metadata),
            Some(b'\n') => {
                self.next()?;
                self.end()?;
                return Ok(metadata);
            }
            _ => {}
        }
        let mut field_count = 0usize;
        loop {
            if let Some(admission) = &self.admission {
                admission.admit_field(field_count)?;
            }
            field_count += 1;
            let (key, separator) = self.hex(b"=")?;
            if separator != Some(b'=') {
                return Err(self.invalid("has an invalid metadata pair"));
            }
            let (value, separator) = self.hex(b";\n")?;
            let key_bytes = key.capacity();
            if metadata.contains_key(&key) {
                let previous = metadata.insert(key, value).expect("existing metadata key");
                let released = key_bytes + previous.capacity();
                drop(previous);
                if let Some(admission) = &mut self.admission {
                    admission.release(released);
                }
            } else {
                if !metadata.is_empty()
                    && let Some(admission) = &mut self.admission
                {
                    admission.reserve_entry()?;
                }
                metadata.insert(key, value);
            }
            match separator {
                Some(b';') => {}
                Some(b'\n') => {
                    self.end()?;
                    return Ok(metadata);
                }
                None => return Ok(metadata),
                _ => unreachable!("hex parser returns only its declared separators"),
            }
        }
    }

    fn end(&mut self) -> Result<()> {
        if self.next()?.is_some() {
            return Err(self.invalid("has trailing bytes after its document line"));
        }
        Ok(())
    }

    fn reserve<T>(&mut self, values: &mut Vec<T>) -> Result<()> {
        if let Some(admission) = &mut self.admission {
            admission.reserve(values)
        } else {
            values.try_reserve(1).map_err(|error| {
                self.invalid(format_args!("cannot allocate a decoded field: {error}"))
            })
        }
    }
}

#[cfg(test)]
mod tests;
