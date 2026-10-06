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

//! Decode bounded headers while terms keep their original JSON array ranges.

use super::*;
use crate::build_memory::shared::Shared;
use hawdb_integrity::Crc32cHasher;
use hawdb_storage::file_io::File;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::cell::Cell;
use std::fmt;
use std::io::{self, BufReader, Read};

#[derive(Default)]
struct ScalarScan {
    quoted: bool,
    escaped: bool,
    current: usize,
    largest: usize,
}
impl ScalarScan {
    fn byte(&mut self, byte: u8) -> Result<()> {
        if self.quoted {
            self.current = self.current.checked_add(1).ok_or_else(size_overflow)?;
            if self.escaped {
                self.escaped = false;
            } else if byte == b'\\' {
                self.escaped = true;
            } else if byte == b'"' {
                self.quoted = false;
            }
        } else if byte == b'"' {
            self.quoted = true;
            self.current = 1;
        } else if byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E') {
            self.current = self.current.checked_add(1).ok_or_else(size_overflow)?;
        } else {
            self.current = 0;
        }
        self.largest = self.largest.max(self.current);
        Ok(())
    }
}

pub(super) fn preflight(
    file: &File,
    manifest: &SearchOutOfCoreMutationRunManifest,
) -> Result<usize> {
    if file.metadata()?.len() != manifest.len {
        return Err(HawDBError::Storage(
            "search mutation-run artifact length or checksum mismatch".into(),
        ));
    }
    let mut input = super::super::hydration::RangeReader {
        file,
        offset: 0,
        remaining: manifest.len,
    };
    let mut digest = Crc32cHasher::new();
    let mut scan = ScalarScan::default();
    let mut buffer = [0; 8192];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        for &byte in &buffer[..count] {
            scan.byte(byte)?;
        }
    }
    if digest.finish() != manifest.checksum {
        return Err(HawDBError::Storage(
            "search mutation-run artifact length or checksum mismatch".into(),
        ));
    }
    Ok(scan.largest.max(8))
}

pub(super) fn working_bytes(entries: usize, scalar: usize) -> Result<u64> {
    add(
        add(
            multiply(
                entries as u64,
                std::mem::size_of::<SearchMutationRunEntry>()
                    + crate::build_memory::SET_ENTRY_BYTES,
            )?,
            multiply(
                scalar as u64,
                entries.checked_add(9).ok_or_else(size_overflow)?,
            )?,
        )?,
        16384 + (std::mem::size_of::<terms::TermFile>() + 2 * std::mem::size_of::<usize>()) as u64,
    )
}

struct CountRead<'a, R> {
    inner: R,
    offset: &'a Cell<u64>,
    scalar: ScalarScan,
    max_scalar: usize,
}
impl<R: Read> Read for CountRead<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        for &byte in &bytes[..count] {
            self.scalar.byte(byte).map_err(io::Error::other)?;
            if self.scalar.current > self.max_scalar {
                return Err(io::Error::other("mutation scalar grew after admission"));
            }
        }
        self.offset.set(
            self.offset
                .get()
                .checked_add(count as u64)
                .ok_or_else(|| io::Error::other("mutation offset overflow"))?,
        );
        Ok(count)
    }
}

#[derive(Clone, Copy)]
struct Context<'a> {
    file: &'a Shared<terms::TermFile>,
    offset: &'a Cell<u64>,
    scalar: usize,
    entries: usize,
}

pub(super) fn decode(
    file: Shared<terms::TermFile>,
    manifest: &SearchOutOfCoreMutationRunManifest,
    scalar: usize,
) -> Result<SearchMutationRunEnvelope> {
    let offset = Cell::new(0);
    let context = Context {
        file: &file,
        offset: &offset,
        scalar,
        entries: manifest.entry_count,
    };
    let input = CountRead {
        inner: BufReader::with_capacity(
            8192,
            super::super::hydration::RangeReader {
                file: &file.file,
                offset: 0,
                remaining: manifest.len,
            },
        ),
        offset: &offset,
        scalar: ScalarScan::default(),
        max_scalar: scalar,
    };
    let mut decoder = serde_json::Deserializer::from_reader(input);
    let value = EnvelopeSeed(context)
        .deserialize(&mut decoder)
        .map_err(|error| invalid(&error.to_string()))?;
    decoder.end().map_err(|error| invalid(&error.to_string()))?;
    Ok(value)
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::Storage(format!("invalid search mutation-run artifact: {message}"))
}

struct EnvelopeSeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for EnvelopeSeed<'_> {
    type Value = SearchMutationRunEnvelope;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for EnvelopeSeed<'_> {
    type Value = SearchMutationRunEnvelope;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a mutation envelope object")
    }
    fn visit_map<M: MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut body: Option<SearchMutationRunBody> = None;
        let mut checksum: Option<u64> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "body" => {
                    if body.is_some() {
                        return Err(de::Error::duplicate_field("body"));
                    }
                    body = Some(map.next_value_seed(BodySeed(self.0))?);
                }
                "checksum" => {
                    if checksum.is_some() {
                        return Err(de::Error::duplicate_field("checksum"));
                    }
                    checksum = Some(map.next_value()?);
                }
                _ => return Err(de::Error::custom("unknown mutation field")),
            }
        }
        Ok(SearchMutationRunEnvelope {
            body: body.ok_or_else(|| de::Error::missing_field("body"))?,
            checksum: checksum.ok_or_else(|| de::Error::missing_field("checksum"))?,
        })
    }
}

struct BodySeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for BodySeed<'_> {
    type Value = SearchMutationRunBody;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for BodySeed<'_> {
    type Value = SearchMutationRunBody;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a mutation body object")
    }
    fn visit_map<M: MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut format: Option<String> = None;
        let mut generation: Option<u64> = None;
        let mut analyzer_digest: Option<u64> = None;
        let mut entries: Option<Vec<SearchMutationRunEntry>> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "format" => {
                    if format.is_some() {
                        return Err(de::Error::duplicate_field("format"));
                    }
                    format = Some(map.next_value()?);
                }
                "generation" => {
                    if generation.is_some() {
                        return Err(de::Error::duplicate_field("generation"));
                    }
                    generation = Some(map.next_value()?);
                }
                "analyzer_digest" => {
                    if analyzer_digest.is_some() {
                        return Err(de::Error::duplicate_field("analyzer_digest"));
                    }
                    analyzer_digest = Some(map.next_value()?);
                }
                "entries" => {
                    if entries.is_some() {
                        return Err(de::Error::duplicate_field("entries"));
                    }
                    entries = Some(map.next_value_seed(EntriesSeed(self.0))?);
                }
                _ => return Err(de::Error::custom("unknown mutation field")),
            }
        }
        Ok(SearchMutationRunBody {
            format: format.ok_or_else(|| de::Error::missing_field("format"))?,
            generation: generation.ok_or_else(|| de::Error::missing_field("generation"))?,
            analyzer_digest: analyzer_digest
                .ok_or_else(|| de::Error::missing_field("analyzer_digest"))?,
            entries: entries.ok_or_else(|| de::Error::missing_field("entries"))?,
        })
    }
}

struct EntrySeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for EntrySeed<'_> {
    type Value = SearchMutationRunEntry;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for EntrySeed<'_> {
    type Value = SearchMutationRunEntry;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a mutation entry object")
    }
    fn visit_map<M: MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut document_id: Option<String> = None;
        let mut target_segment_id: Option<u64> = None;
        let mut operation: Option<SearchMutationOperation> = None;
        let mut retraction: Option<SearchMutationRetraction> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "document_id" => {
                    if document_id.is_some() {
                        return Err(de::Error::duplicate_field("document_id"));
                    }
                    document_id = Some(map.next_value()?);
                }
                "target_segment_id" => {
                    if target_segment_id.is_some() {
                        return Err(de::Error::duplicate_field("target_segment_id"));
                    }
                    target_segment_id = Some(map.next_value()?);
                }
                "operation" => {
                    if operation.is_some() {
                        return Err(de::Error::duplicate_field("operation"));
                    }
                    operation = Some(map.next_value()?);
                }
                "retraction" => {
                    if retraction.is_some() {
                        return Err(de::Error::duplicate_field("retraction"));
                    }
                    retraction = Some(map.next_value_seed(RetractionSeed(self.0))?);
                }
                _ => return Err(de::Error::custom("unknown mutation field")),
            }
        }
        Ok(SearchMutationRunEntry {
            document_id: document_id.ok_or_else(|| de::Error::missing_field("document_id"))?,
            target_segment_id: target_segment_id
                .ok_or_else(|| de::Error::missing_field("target_segment_id"))?,
            operation: operation.ok_or_else(|| de::Error::missing_field("operation"))?,
            retraction: retraction.ok_or_else(|| de::Error::missing_field("retraction"))?,
        })
    }
}

struct RetractionSeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for RetractionSeed<'_> {
    type Value = SearchMutationRetraction;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for RetractionSeed<'_> {
    type Value = SearchMutationRetraction;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a mutation retraction object")
    }
    fn visit_map<M: MapAccess<'de>>(
        self,
        mut map: M,
    ) -> std::result::Result<Self::Value, M::Error> {
        let mut documents_digest: Option<u64> = None;
        let mut lexical_document_len: Option<u64> = None;
        let mut unique_terms: Option<terms::Terms> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "documents_digest" => {
                    if documents_digest.is_some() {
                        return Err(de::Error::duplicate_field("documents_digest"));
                    }
                    documents_digest = Some(map.next_value()?);
                }
                "lexical_document_len" => {
                    if lexical_document_len.is_some() {
                        return Err(de::Error::duplicate_field("lexical_document_len"));
                    }
                    lexical_document_len = Some(map.next_value()?);
                }
                "unique_terms" => {
                    if unique_terms.is_some() {
                        return Err(de::Error::duplicate_field("unique_terms"));
                    }
                    unique_terms = Some(map.next_value_seed(TermsSeed(self.0))?);
                }
                _ => return Err(de::Error::custom("unknown mutation field")),
            }
        }
        Ok(SearchMutationRetraction {
            documents_digest: documents_digest
                .ok_or_else(|| de::Error::missing_field("documents_digest"))?,
            lexical_document_len: lexical_document_len
                .ok_or_else(|| de::Error::missing_field("lexical_document_len"))?,
            unique_terms: unique_terms.ok_or_else(|| de::Error::missing_field("unique_terms"))?,
        })
    }
}

struct EntriesSeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for EntriesSeed<'_> {
    type Value = Vec<SearchMutationRunEntry>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for EntriesSeed<'_> {
    type Value = Vec<SearchMutationRunEntry>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("mutation entries")
    }
    fn visit_seq<S: SeqAccess<'de>>(
        self,
        mut sequence: S,
    ) -> std::result::Result<Self::Value, S::Error> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.0.entries)
            .map_err(de::Error::custom)?;
        if entries.capacity() > self.0.entries {
            return Err(de::Error::custom(
                "mutation entry capacity exceeds admission",
            ));
        }
        for _ in 0..self.0.entries {
            entries.push(
                sequence
                    .next_element_seed(EntrySeed(self.0))?
                    .ok_or_else(|| de::Error::custom("mutation entry count shrank"))?,
            );
        }
        if sequence.next_element::<de::IgnoredAny>()?.is_some() {
            return Err(de::Error::custom("mutation entry count grew"));
        }
        Ok(entries)
    }
}

struct TermsSeed<'a>(Context<'a>);
impl<'de> DeserializeSeed<'de> for TermsSeed<'_> {
    type Value = terms::Terms;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for TermsSeed<'_> {
    type Value = terms::Terms;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ordered mutation terms")
    }
    fn visit_seq<S: SeqAccess<'de>>(
        self,
        mut sequence: S,
    ) -> std::result::Result<Self::Value, S::Error> {
        let start = self
            .0
            .offset
            .get()
            .checked_sub(1)
            .ok_or_else(|| de::Error::custom("missing term array"))?;
        let mut previous: Option<String> = None;
        let mut count = 0usize;
        while let Some(term) = sequence.next_element::<String>()? {
            if term.is_empty() || previous.as_ref().is_some_and(|previous| previous >= &term) {
                return Err(de::Error::custom("unordered mutation terms"));
            }
            previous = Some(term);
            count = count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("mutation term count overflow"))?;
        }
        let bytes = self
            .0
            .offset
            .get()
            .checked_sub(start)
            .ok_or_else(|| de::Error::custom("mutation term range overflow"))?;
        Ok(terms::Terms::Stored {
            file: self.0.file.clone(),
            offset: start,
            bytes,
            count,
            scalar_bytes: self.0.scalar,
            checksum: terms::range_checksum(&self.0.file.file, start, bytes)
                .map_err(de::Error::custom)?,
            memory: None,
        })
    }
}

pub(super) fn entries_scalar(entries: &[SearchMutationRunEntry]) -> Result<usize> {
    struct ScanWriter(ScalarScan);
    impl io::Write for ScanWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            for &byte in bytes {
                self.0.byte(byte).map_err(io::Error::other)?;
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = ScanWriter(ScalarScan::default());
    serde_json::to_writer(&mut writer, entries).map_err(|error| invalid(&error.to_string()))?;
    Ok(writer.0.largest.max(MUTATION_RUN_FORMAT.len() + 2).max(32))
}
