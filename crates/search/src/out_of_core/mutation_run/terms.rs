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

//! Terms retain immutable file ranges; decoding holds at most one scalar.

use super::*;
use crate::build_memory::{shared::Shared, BuildMemory};
use hawdb_executor::QueryMemoryLease;
use hawdb_integrity::Crc32cHasher;
use hawdb_storage::file_io::File;
use serde::ser::SerializeSeq;
use std::io::{BufRead, BufReader, Read, Seek, Write};

#[derive(Debug)]
pub(crate) struct TermFile {
    pub(super) file: File,
    _memory: Option<QueryMemoryLease>,
}
impl TermFile {
    pub(crate) fn new(file: File, memory: Option<&BuildMemory>) -> Result<Shared<Self>> {
        let lease = memory
            .map(|memory| {
                memory
                    .retained
                    .reserve(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            })
            .transpose()?;
        Ok(Shared::new(Self {
            file,
            _memory: lease,
        }))
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Terms {
    Owned(Vec<String>),
    Stored {
        file: Shared<TermFile>,
        offset: u64,
        bytes: u64,
        count: usize,
        scalar_bytes: usize,
        checksum: u64,
        memory: Option<BuildMemory>,
    },
}
impl From<Vec<String>> for Terms {
    fn from(value: Vec<String>) -> Self {
        Self::Owned(value)
    }
}
impl Terms {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Owned(terms) => terms.len(),
            Self::Stored { count, .. } => *count,
        }
    }
    pub(crate) fn retained_bytes(&self) -> Result<u64> {
        match self {
            Self::Owned(terms) => terms.iter().try_fold(
                multiply(terms.capacity() as u64, std::mem::size_of::<String>())?,
                |bytes, term| add(bytes, term.capacity() as u64),
            ),
            Self::Stored { .. } => Ok(0),
        }
    }
    pub(crate) fn workspace_bytes(&self) -> Result<u64> {
        match self {
            Self::Owned(terms) => Ok(terms.iter().map(String::len).max().unwrap_or(0) as u64),
            Self::Stored { scalar_bytes, .. } => add(8192, multiply(*scalar_bytes as u64, 8)?),
        }
    }
    pub(in crate::out_of_core) fn cursor(&self) -> Result<Cursor<'_>> {
        match self {
            Self::Owned(terms) => Ok(Cursor::Owned(terms.iter())),
            Self::Stored {
                file,
                offset,
                bytes,
                count,
                scalar_bytes,
                checksum,
                memory,
            } => {
                let lease = memory
                    .as_ref()
                    .map(|memory| {
                        let bytes = usize::try_from(self.workspace_bytes()?)
                            .map_err(|_| size_overflow())?;
                        memory.spool.reserve(bytes)
                    })
                    .transpose()?;
                let mut reader = BufReader::with_capacity(
                    8192,
                    super::super::hydration::CheckedReader::new(
                        super::super::hydration::RangeReader {
                            file: &file.file,
                            offset: *offset,
                            remaining: *bytes,
                        },
                    ),
                );
                expect(&mut reader, b'[')?;
                Ok(Cursor::Stored {
                    reader,
                    remaining: *count,
                    scalar_bytes: *scalar_bytes,
                    checksum: *checksum,
                    first: true,
                    ended: false,
                    _memory: lease,
                })
            }
        }
    }
    pub(crate) fn visit(&self, mut emit: impl FnMut(&str) -> Result<()>) -> Result<()> {
        let mut cursor = self.cursor()?;
        while let Some(term) = cursor.next()? {
            emit(&term)?;
        }
        Ok(())
    }
    pub(crate) fn validate(&self) -> Result<()> {
        let mut previous = None;
        let mut cursor = self.cursor()?;
        while let Some(term) = cursor.next()? {
            if term.is_empty()
                || previous
                    .as_ref()
                    .is_some_and(|previous: &String| previous >= &term)
            {
                return Err(invalid_terms());
            }
            previous = Some(term);
        }
        Ok(())
    }
}

pub(in crate::out_of_core) enum Cursor<'a> {
    Owned(std::slice::Iter<'a, String>),
    Stored {
        reader: BufReader<
            super::super::hydration::CheckedReader<super::super::hydration::RangeReader<'a>>,
        >,
        remaining: usize,
        scalar_bytes: usize,
        checksum: u64,
        first: bool,
        ended: bool,
        _memory: Option<QueryMemoryLease>,
    },
}
impl Cursor<'_> {
    pub(crate) fn next(&mut self) -> Result<Option<String>> {
        match self {
            Self::Owned(terms) => Ok(terms.next().cloned()),
            Self::Stored {
                reader,
                remaining,
                scalar_bytes,
                checksum,
                first,
                ended,
                ..
            } => {
                if *ended {
                    return Ok(None);
                }
                if *remaining == 0 {
                    expect(reader, b']')?;
                    let mut trailing = [0];
                    while reader.read(&mut trailing)? != 0 {
                        if !trailing[0].is_ascii_whitespace() {
                            return Err(invalid_terms());
                        }
                    }
                    if reader.get_ref().digest.finish() != *checksum {
                        return Err(HawDBError::Storage(
                            "mutation term range checksum mismatch".into(),
                        ));
                    }
                    *ended = true;
                    return Ok(None);
                }
                if !*first {
                    expect(reader, b',')?;
                }
                *first = false;
                skip_space(reader)?;
                let mut limited = reader.take(*scalar_bytes as u64);
                let term =
                    String::deserialize(&mut serde_json::Deserializer::from_reader(&mut limited))
                        .map_err(|error| {
                        HawDBError::Storage(format!("invalid mutation term: {error}"))
                    })?;
                *remaining -= 1;
                Ok(Some(term))
            }
        }
    }
}
fn skip_space(reader: &mut impl BufRead) -> Result<()> {
    loop {
        let bytes = reader.fill_buf()?;
        let count = bytes
            .iter()
            .take_while(|byte| byte.is_ascii_whitespace())
            .count();
        if count == 0 {
            return Ok(());
        }
        reader.consume(count);
    }
}
fn expect(reader: &mut impl BufRead, expected: u8) -> Result<()> {
    skip_space(reader)?;
    let mut byte = [0];
    reader.read_exact(&mut byte)?;
    if byte[0] != expected {
        return Err(invalid_terms());
    }
    Ok(())
}
fn invalid_terms() -> HawDBError {
    HawDBError::Storage(
        "search mutation-run retraction terms are not strictly ordered or valid".into(),
    )
}
impl Serialize for Terms {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;
        let mut cursor = self.cursor().map_err(serde::ser::Error::custom)?;
        while let Some(term) = cursor.next().map_err(serde::ser::Error::custom)? {
            sequence.serialize_element(&term)?;
        }
        sequence.end()
    }
}
impl<'de> Deserialize<'de> for Terms {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Vec::<String>::deserialize(deserializer).map(Self::Owned)
    }
}
#[cfg(test)]
impl PartialEq for Terms {
    fn eq(&self, other: &Self) -> bool {
        let collect = |terms: &Self| {
            let mut result = Vec::new();
            terms
                .visit(|term| {
                    result.push(term.to_owned());
                    Ok(())
                })
                .unwrap();
            result
        };
        collect(self) == collect(other)
    }
}
#[cfg(test)]
impl Eq for Terms {}

pub(crate) struct Writer {
    pub(crate) file: Shared<TermFile>,
    start: u64,
    count: usize,
    scalar_bytes: usize,
    memory: BuildMemory,
    max_bytes: u64,
    digest: Crc32cHasher,
}
impl Writer {
    pub(crate) fn new(
        file: Shared<TermFile>,
        memory: &BuildMemory,
        max_bytes: u64,
    ) -> Result<Self> {
        let mut output = &file.file;
        let start = output.stream_position()?;
        if start.checked_add(2).ok_or_else(size_overflow)? > max_bytes {
            return Err(HawDBError::Storage(
                "mutation term spill exceeds byte admission".into(),
            ));
        }
        output.write_all(b"[")?;
        let mut digest = Crc32cHasher::new();
        digest.update(b"[");
        Ok(Self {
            file,
            start,
            count: 0,
            scalar_bytes: 2,
            memory: memory.clone(),
            max_bytes,
            digest,
        })
    }
    pub(crate) fn push(&mut self, term: &str) -> Result<()> {
        let bound = term
            .len()
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(size_overflow)?;
        let mut output = &self.file.file;
        // Check before serialization; count the whole private term file, not just this array.
        if output
            .stream_position()?
            .checked_add(bound as u64 + 2)
            .ok_or_else(size_overflow)?
            > self.max_bytes
        {
            return Err(HawDBError::Storage(
                "mutation term spill exceeds byte admission".into(),
            ));
        }
        if self.count != 0 {
            output.write_all(b",")?;
            self.digest.update(b",");
        }
        serde_json::to_writer(
            Hashed {
                output: &mut output,
                digest: &mut self.digest,
            },
            term,
        )
        .map_err(|error| HawDBError::Storage(format!("cannot write mutation term: {error}")))?;
        self.count = self.count.checked_add(1).ok_or_else(size_overflow)?;
        self.scalar_bytes = self.scalar_bytes.max(bound);
        Ok(())
    }
    pub(crate) fn finish(mut self) -> Result<Terms> {
        let mut output = &self.file.file;
        output.write_all(b"]")?;
        self.digest.update(b"]");
        let bytes = output
            .stream_position()?
            .checked_sub(self.start)
            .ok_or_else(size_overflow)?;
        Ok(Terms::Stored {
            file: self.file,
            offset: self.start,
            bytes,
            count: self.count,
            scalar_bytes: self.scalar_bytes,
            checksum: self.digest.finish(),
            memory: Some(self.memory),
        })
    }
}

struct Hashed<'a, W> {
    output: &'a mut W,
    digest: &'a mut Crc32cHasher,
}
impl<W: Write> Write for Hashed<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.output.write(bytes)?;
        self.digest.update(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.output.flush()
    }
}

pub(super) fn range_checksum(file: &File, offset: u64, bytes: u64) -> Result<u64> {
    let mut input = super::super::hydration::RangeReader {
        file,
        offset,
        remaining: bytes,
    };
    let mut buffer = [0; 8192];
    let mut digest = Crc32cHasher::new();
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(digest.finish())
}

#[cfg(test)]
impl Terms {
    pub(in crate::out_of_core) fn owned_mut(&mut self) -> &mut Vec<String> {
        match self {
            Self::Owned(terms) => terms,
            _ => panic!("fixture mutation requires owned terms"),
        }
    }
    pub(in crate::out_of_core) fn to_vec(&self) -> Vec<String> {
        let mut terms = Vec::new();
        self.visit(|term| {
            terms.push(term.to_owned());
            Ok(())
        })
        .unwrap();
        terms
    }
}
#[cfg(test)]
impl FromIterator<String> for Terms {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self::Owned(iter.into_iter().collect())
    }
}
