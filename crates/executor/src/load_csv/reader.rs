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

//! A streaming RFC 4180 record reader with a fixed-size input buffer.

use super::CsvImportLimits;
use hawdb_core::{HawDBError, Result};
use std::io::Read;

/// Bytes read from the source per refill; the only buffered file contents.
pub const CSV_READ_BUFFER_BYTES: usize = 64 * 1024;

const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// One field of a record. An empty unquoted field is `None`, so callers can
/// distinguish it from the quoted empty string `""`.
pub type CsvField = Option<String>;

/// Reads records one at a time. Each record is bounded by
/// [`CsvImportLimits::max_record_bytes`] and
/// [`CsvImportLimits::max_fields_per_record`], and the whole input by
/// [`CsvImportLimits::max_file_bytes`].
pub struct CsvRecordReader<R> {
    input: R,
    buffer: Box<[u8]>,
    position: usize,
    filled: usize,
    eof: bool,
    started: bool,
    terminator: u8,
    limits: CsvImportLimits,
    bytes_read: u64,
    /// One-based physical line of the next unread byte.
    line: u64,
    records: u64,
}

impl<R: Read> CsvRecordReader<R> {
    pub fn new(input: R, terminator: u8, limits: CsvImportLimits) -> Result<Self> {
        if !terminator.is_ascii() || matches!(terminator, b'"' | b'\r' | b'\n') {
            return Err(HawDBError::Semantic(
                "FIELDTERMINATOR must be one ASCII character other than a double quote or line break"
                    .to_string(),
            ));
        }
        Ok(Self {
            input,
            buffer: vec![0; CSV_READ_BUFFER_BYTES].into_boxed_slice(),
            position: 0,
            filled: 0,
            eof: false,
            started: false,
            terminator,
            limits,
            bytes_read: 0,
            line: 1,
            records: 0,
        })
    }

    /// Records returned so far, including a header record.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Returns the next record, skipping lines that contain no bytes at all.
    pub fn next_record(&mut self) -> Result<Option<Vec<CsvField>>> {
        if !self.started {
            self.started = true;
            self.skip_bom()?;
        }
        loop {
            let Some(first) = self.peek()? else {
                return Ok(None);
            };
            if matches!(first, b'\n' | b'\r') {
                self.consume_line_break(first)?;
                continue;
            }
            let start_line = self.line;
            let record = self.read_record(start_line)?;
            self.records += 1;
            return Ok(Some(record));
        }
    }

    fn read_record(&mut self, start_line: u64) -> Result<Vec<CsvField>> {
        let mut fields = Vec::new();
        let mut record_bytes = 0usize;
        loop {
            if fields.len() == self.limits.max_fields_per_record {
                return Err(self.error(
                    start_line,
                    format!(
                        "has more than max_fields_per_record {} fields",
                        self.limits.max_fields_per_record
                    ),
                ));
            }
            let mut field = Vec::new();
            let quoted = self.peek()? == Some(b'"');
            let end = if quoted {
                self.bump();
                self.read_quoted(&mut field, &mut record_bytes, start_line)?
            } else {
                self.read_unquoted(&mut field, &mut record_bytes, start_line)?
            };
            let text = String::from_utf8(field).map_err(|_| {
                self.error(
                    start_line,
                    format!("field {} is not valid UTF-8", fields.len() + 1),
                )
            })?;
            fields.push((quoted || !text.is_empty()).then_some(text));
            if end == FieldEnd::Record {
                return Ok(fields);
            }
        }
    }

    fn read_unquoted(
        &mut self,
        field: &mut Vec<u8>,
        record_bytes: &mut usize,
        start_line: u64,
    ) -> Result<FieldEnd> {
        loop {
            let Some(byte) = self.peek()? else {
                return Ok(FieldEnd::Record);
            };
            self.bump();
            match byte {
                byte if byte == self.terminator => return Ok(FieldEnd::Field),
                b'\n' | b'\r' => {
                    self.finish_line_break(byte)?;
                    return Ok(FieldEnd::Record);
                }
                // A quote inside an unquoted field is kept literally.
                byte => self.push(field, byte, record_bytes, start_line)?,
            }
        }
    }

    fn read_quoted(
        &mut self,
        field: &mut Vec<u8>,
        record_bytes: &mut usize,
        start_line: u64,
    ) -> Result<FieldEnd> {
        loop {
            let Some(byte) = self.peek()? else {
                return Err(self.error(start_line, "has an unterminated quoted field".to_string()));
            };
            self.bump();
            match byte {
                b'"' if self.peek()? == Some(b'"') => {
                    self.bump();
                    self.push(field, b'"', record_bytes, start_line)?;
                }
                b'"' => break,
                b'\n' => {
                    self.line += 1;
                    self.push(field, byte, record_bytes, start_line)?;
                }
                b'\r' => {
                    if self.peek()? != Some(b'\n') {
                        self.line += 1;
                    }
                    self.push(field, byte, record_bytes, start_line)?;
                }
                byte => self.push(field, byte, record_bytes, start_line)?,
            }
        }
        match self.peek()? {
            None => Ok(FieldEnd::Record),
            Some(byte) if byte == self.terminator => {
                self.bump();
                Ok(FieldEnd::Field)
            }
            Some(byte @ (b'\n' | b'\r')) => {
                self.consume_line_break(byte)?;
                Ok(FieldEnd::Record)
            }
            Some(_) => Err(self.error(
                start_line,
                "has a character after a closing quote; quote the whole field and double embedded quotes"
                    .to_string(),
            )),
        }
    }

    fn push(
        &self,
        field: &mut Vec<u8>,
        byte: u8,
        record_bytes: &mut usize,
        start_line: u64,
    ) -> Result<()> {
        *record_bytes += 1;
        if *record_bytes > self.limits.max_record_bytes {
            return Err(self.error(
                start_line,
                format!("exceeds max_record_bytes {}", self.limits.max_record_bytes),
            ));
        }
        field.push(byte);
        Ok(())
    }

    fn consume_line_break(&mut self, byte: u8) -> Result<()> {
        self.bump();
        self.finish_line_break(byte)
    }

    /// Completes a line break whose first byte was consumed: CRLF, LF or CR.
    fn finish_line_break(&mut self, byte: u8) -> Result<()> {
        if byte == b'\r' && self.peek()? == Some(b'\n') {
            self.bump();
        }
        self.line += 1;
        Ok(())
    }

    fn skip_bom(&mut self) -> Result<()> {
        while self.filled - self.position < UTF8_BOM.len() && !self.eof {
            self.fill()?;
        }
        if self.buffer[self.position..self.filled].starts_with(&UTF8_BOM) {
            self.position += UTF8_BOM.len();
        }
        Ok(())
    }

    fn peek(&mut self) -> Result<Option<u8>> {
        if self.position == self.filled {
            if self.eof {
                return Ok(None);
            }
            self.position = 0;
            self.filled = 0;
            self.fill()?;
            if self.filled == 0 {
                return Ok(None);
            }
        }
        Ok(Some(self.buffer[self.position]))
    }

    fn bump(&mut self) {
        self.position += 1;
    }

    /// Appends input after any unread bytes; sets `eof` on a zero-byte read.
    fn fill(&mut self) -> Result<()> {
        if self.position > 0 {
            self.buffer.copy_within(self.position..self.filled, 0);
            self.filled -= self.position;
            self.position = 0;
        }
        let read = loop {
            match self.input.read(&mut self.buffer[self.filled..]) {
                Ok(read) => break read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(HawDBError::Execution(format!(
                        "LOAD CSV failed to read the source: {error}"
                    )))
                }
            }
        };
        if read == 0 {
            self.eof = true;
            return Ok(());
        }
        self.bytes_read = self.bytes_read.saturating_add(read as u64);
        if self.bytes_read > self.limits.max_file_bytes {
            return Err(HawDBError::Execution(format!(
                "LOAD CSV source exceeds max_file_bytes {}",
                self.limits.max_file_bytes
            )));
        }
        self.filled += read;
        Ok(())
    }

    fn error(&self, start_line: u64, detail: String) -> HawDBError {
        HawDBError::Execution(format!(
            "LOAD CSV record {} starting at line {start_line} {detail}",
            self.records + 1
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldEnd {
    Field,
    Record,
}
