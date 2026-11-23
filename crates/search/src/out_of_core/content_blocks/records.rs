// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Binary document records inside `blocks1` content blocks (layout 2, #1917).
//!
//! ```text
//! record := id_len u32 | title_len u32 | content_len u32 | dimension u32
//!           | metadata_count u32 | id | title | content
//!           | embedding (dimension x f32) | metadata_count x entry
//! entry  := key_len u32 | value_len u32 | key | value
//! ```
//!
//! Integers and vector components are little-endian; strings are raw UTF-8
//! bytes rather than hex, and the vector is its `f32` bits rather than
//! decimal text. A record decodes to exactly the document its text `doc` line
//! decodes to: a zero dimension is no embedding, and metadata entries follow
//! the stored map's key order. Selective readers parse records in place and
//! skip unselected ones by their lengths; [`TextRecords`] re-encodes records
//! as the canonical `doc` lines for whole-segment consumers.

use super::invalid;
use crate::document_encoding::{Header as DocumentHeader, RecordSource};
use crate::{Result, SearchDocument};
use std::collections::BTreeMap;
use std::io::{self, BufRead, Read, Write};

pub(in crate::out_of_core) const PREFIX_BYTES: usize = 20;
const ENTRY_PREFIX_BYTES: usize = 8;
/// Input taken per step when re-encoding a string field as hex.
const HEX_INPUT_BYTES: usize = 4096;

fn field_len(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| invalid("record field exceeds u32"))
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

/// The binary length of a record with this header and `content_len` content
/// bytes.
pub(in crate::out_of_core) fn record_len(
    header: DocumentHeader<'_>,
    content_len: u64,
) -> Result<u64> {
    let dimension = header.embedding.map_or(0, <[f32]>::len);
    field_len(header.id.len())?;
    field_len(header.title.len())?;
    field_len(usize::try_from(content_len).map_err(|_| invalid("record content overflow"))?)?;
    field_len(dimension)?;
    field_len(header.metadata.len())?;
    let mut bytes = (PREFIX_BYTES as u64)
        .checked_add(header.id.len() as u64)
        .and_then(|bytes| bytes.checked_add(header.title.len() as u64))
        .and_then(|bytes| bytes.checked_add(content_len))
        .and_then(|bytes| bytes.checked_add((dimension as u64).checked_mul(4)?))
        .ok_or_else(|| invalid("record length overflow"))?;
    for (key, value) in header.metadata {
        field_len(key.len())?;
        field_len(value.len())?;
        bytes = bytes
            .checked_add((ENTRY_PREFIX_BYTES + key.len()) as u64)
            .and_then(|bytes| bytes.checked_add(value.len() as u64))
            .ok_or_else(|| invalid("record length overflow"))?;
    }
    Ok(bytes)
}

/// The content length of a record source, derived from its text length.
pub(in crate::out_of_core) fn content_len(
    source: &impl RecordSource,
    task: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<u64> {
    let text = source.encoded_len(task)? as u64;
    let without = crate::document_encoding::streamed::record_len(source.header(), 0)?;
    text.checked_sub(without)
        .filter(|hex| hex % 2 == 0)
        .map(|hex| hex / 2)
        .ok_or_else(|| invalid("record text length disagrees with its header"))
}

/// Writes one binary record. The content is taken from the source's text
/// encoding, whose fourth field is the hex-encoded content; every other
/// field comes from its header.
pub(in crate::out_of_core) fn write_record(
    output: &mut impl Write,
    source: &impl RecordSource,
    content_len: u64,
) -> io::Result<()> {
    let header = source.header();
    let embedding = header.embedding.unwrap_or_default();
    let too_long = |_| io::Error::other(invalid("record field exceeds u32"));
    for value in [
        u32::try_from(header.id.len()).map_err(too_long)?,
        u32::try_from(header.title.len()).map_err(too_long)?,
        u32::try_from(content_len).map_err(too_long)?,
        u32::try_from(embedding.len()).map_err(too_long)?,
        u32::try_from(header.metadata.len()).map_err(too_long)?,
    ] {
        output.write_all(&value.to_le_bytes())?;
    }
    output.write_all(header.id.as_bytes())?;
    output.write_all(header.title.as_bytes())?;
    let mut content = ContentField {
        output: &mut *output,
        tabs: 0,
        high: None,
        written: 0,
        buffer: [0; 512],
        buffered: 0,
    };
    source.write_encoded(&mut content)?;
    content.flush_buffer()?;
    if content.tabs < 4 || content.high.is_some() || content.written != content_len {
        return Err(io::Error::other(invalid(
            "record content disagrees with its admitted length",
        )));
    }
    let mut buffer = [0u8; 1024];
    for values in embedding.chunks(buffer.len() / 4) {
        for (value, output) in values.iter().zip(buffer.as_chunks_mut::<4>().0) {
            *output = value.to_le_bytes();
        }
        output.write_all(&buffer[..values.len() * 4])?;
    }
    for (key, value) in header.metadata {
        output.write_all(&u32::try_from(key.len()).map_err(too_long)?.to_le_bytes())?;
        output.write_all(&u32::try_from(value.len()).map_err(too_long)?.to_le_bytes())?;
        output.write_all(key.as_bytes())?;
        output.write_all(value.as_bytes())?;
    }
    Ok(())
}

/// Decodes the hex content field of a text record stream and discards the
/// rest. Hex digits never include a tab, so tabs delimit the fields.
struct ContentField<'w, W> {
    output: &'w mut W,
    tabs: usize,
    high: Option<u8>,
    written: u64,
    buffer: [u8; 512],
    buffered: usize,
}

impl<W: Write> ContentField<'_, W> {
    fn flush_buffer(&mut self) -> io::Result<()> {
        self.output.write_all(&self.buffer[..self.buffered])?;
        self.written += self.buffered as u64;
        self.buffered = 0;
        Ok(())
    }
}

impl<W: Write> Write for ContentField<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for &byte in bytes {
            if byte == b'\t' {
                self.tabs += 1;
                continue;
            }
            if self.tabs != 3 {
                continue;
            }
            let nibble = match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(io::Error::other(invalid("record content is not hex"))),
            };
            match self.high.take() {
                None => self.high = Some(nibble),
                Some(high) => {
                    if self.buffered == self.buffer.len() {
                        self.flush_buffer()?;
                    }
                    self.buffer[self.buffered] = (high << 4) | nibble;
                    self.buffered += 1;
                }
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Cursor<'b> {
    body: &'b [u8],
    at: usize,
}

impl<'b> Cursor<'b> {
    fn take(&mut self, length: usize) -> Result<&'b [u8]> {
        let end = self
            .at
            .checked_add(length)
            .filter(|end| *end <= self.body.len())
            .ok_or_else(|| invalid("record is truncated"))?;
        let bytes = &self.body[self.at..end];
        self.at = end;
        Ok(bytes)
    }
}

/// One record parsed in place. Only `id` is validated as UTF-8 until the
/// record is decoded.
pub(in crate::out_of_core) struct Record<'b> {
    pub(in crate::out_of_core) id: &'b str,
    title: &'b [u8],
    content: &'b [u8],
    embedding: &'b [u8],
    metadata: &'b [u8],
    metadata_count: usize,
}

impl<'b> Record<'b> {
    /// Parses the record at the start of `body` and returns it with its
    /// length. Every length is checked against `body`.
    pub(in crate::out_of_core) fn parse(body: &'b [u8]) -> Result<(Self, usize)> {
        if body.len() < PREFIX_BYTES {
            return Err(invalid("record is truncated"));
        }
        let field = |index: usize| u32_at(body, 4 * index) as usize;
        let (id_len, title_len, content_len) = (field(0), field(1), field(2));
        let (dimension, metadata_count) = (field(3), field(4));
        let mut cursor = Cursor {
            body,
            at: PREFIX_BYTES,
        };
        let id = std::str::from_utf8(cursor.take(id_len)?)
            .map_err(|_| invalid("record id is not UTF-8"))?;
        let title = cursor.take(title_len)?;
        let content = cursor.take(content_len)?;
        let embedding = cursor.take(
            dimension
                .checked_mul(4)
                .ok_or_else(|| invalid("record is truncated"))?,
        )?;
        if metadata_count > (body.len() - cursor.at) / ENTRY_PREFIX_BYTES {
            return Err(invalid("record is truncated"));
        }
        let metadata_start = cursor.at;
        for _ in 0..metadata_count {
            let prefix = cursor.take(ENTRY_PREFIX_BYTES)?;
            let key = u32_at(prefix, 0) as usize;
            let value = u32_at(prefix, 4) as usize;
            cursor.take(
                key.checked_add(value)
                    .ok_or_else(|| invalid("record is truncated"))?,
            )?;
        }
        Ok((
            Self {
                id,
                title,
                content,
                embedding,
                metadata: &body[metadata_start..cursor.at],
                metadata_count,
            },
            cursor.at,
        ))
    }

    /// The bytes its decoded document holds (`search_document_bytes`).
    pub(in crate::out_of_core) fn document_bytes(&self) -> u64 {
        (self.id.len() + self.title.len() + self.content.len() + self.embedding.len()) as u64
            + (self.metadata.len() - ENTRY_PREFIX_BYTES * self.metadata_count) as u64
    }

    /// The vector dimension it would decode to.
    pub(in crate::out_of_core) fn dimension(&self) -> usize {
        self.embedding.len() / 4
    }

    pub(in crate::out_of_core) fn metadata_count(&self) -> usize {
        self.metadata_count
    }

    fn metadata(&self) -> impl Iterator<Item = (&'b [u8], &'b [u8])> {
        let mut at = 0;
        let metadata = self.metadata;
        (0..self.metadata_count).map(move |_| {
            let key = u32_at(metadata, at) as usize;
            let value = u32_at(metadata, at + 4) as usize;
            let key_start = at + ENTRY_PREFIX_BYTES;
            at = key_start + key + value;
            (
                &metadata[key_start..key_start + key],
                &metadata[key_start + key..at],
            )
        })
    }

    /// Decodes the document; without `embeddings` its vector is left
    /// undecoded and the document carries none, as for text records.
    pub(in crate::out_of_core) fn decode(&self, embeddings: bool) -> Result<SearchDocument> {
        let text = |bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| invalid("record text is not UTF-8"))
        };
        let mut metadata = BTreeMap::new();
        for (key, value) in self.metadata() {
            metadata.insert(text(key)?, text(value)?);
        }
        Ok(SearchDocument {
            id: self.id.to_owned(),
            title: text(self.title)?,
            content: text(self.content)?,
            embedding: (embeddings && !self.embedding.is_empty()).then(|| {
                self.embedding
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|bytes| f32::from_le_bytes(*bytes))
                    .collect()
            }),
            metadata,
        })
    }
}

/// Visits every record of a block body in order, after checking that ids
/// strictly increase and that the count and first and last ids match the
/// block's index entry.
pub(in crate::out_of_core) fn scan<'b>(
    body: &'b [u8],
    bounds: crate::out_of_core::hydration::Bounds<'_>,
    mut visit: impl FnMut(&Record<'b>) -> Result<()>,
) -> Result<()> {
    let mut at = 0;
    let mut count = 0usize;
    let mut previous = None::<&str>;
    while at < body.len() {
        let (record, length) = Record::parse(&body[at..])?;
        if count == 0 && record.id != bounds.first {
            return Err(invalid("block records disagree with its index"));
        }
        if previous.is_some_and(|previous| previous >= record.id) {
            return Err(invalid("block records are not strictly ordered"));
        }
        count += 1;
        if count > bounds.count {
            return Err(invalid("block records disagree with its index"));
        }
        visit(&record)?;
        previous = Some(record.id);
        at += length;
    }
    if count != bounds.count || previous != Some(bounds.last) {
        return Err(invalid("block records disagree with its index"));
    }
    Ok(())
}

/// Counts the records streaming through one block and captures its first and
/// last ids, across arbitrary read boundaries.
#[derive(Default)]
pub(super) struct RecordWalk {
    records: u64,
    prefix: [u8; PREFIX_BYTES],
    filled: usize,
    stage: Stage,
    id: Vec<u8>,
    last: Vec<u8>,
    first_ok: bool,
    /// The current record's id exceeded `limit` and was truncated.
    overflow: bool,
    /// The previous record's (`last`) id was truncated, so it can match no
    /// indexed id: a truncated id may equal a shorter indexed id that is its
    /// prefix, which the sparse scan rejects.
    last_overflow: bool,
    limit: usize,
}

#[derive(Default, Clone, Copy)]
enum Stage {
    /// Collecting a record prefix (or, with `entries`, an entry prefix).
    #[default]
    Prefix,
    Id {
        remaining: u64,
        skip: u64,
        entries: u64,
    },
    Skip {
        remaining: u64,
        entries: u64,
    },
    Entry {
        entries: u64,
    },
}

impl RecordWalk {
    pub(super) fn start(&mut self, limit: usize) {
        *self = Self {
            id: std::mem::take(&mut self.id),
            last: std::mem::take(&mut self.last),
            limit,
            ..Self::default()
        };
        self.id.clear();
        self.last.clear();
    }

    fn finish_record(&mut self, first: &str) {
        if self.records == 0 {
            self.first_ok = !self.overflow && self.id == first.as_bytes();
        }
        self.records += 1;
        std::mem::swap(&mut self.last, &mut self.id);
        self.id.clear();
        self.last_overflow = std::mem::take(&mut self.overflow);
        self.stage = Stage::Prefix;
        self.filled = 0;
    }

    pub(super) fn feed(&mut self, mut bytes: &[u8], first: &str) {
        loop {
            match self.stage {
                Stage::Prefix | Stage::Entry { .. } => {
                    let wanted = match self.stage {
                        Stage::Prefix => PREFIX_BYTES,
                        _ => ENTRY_PREFIX_BYTES,
                    };
                    if bytes.is_empty() {
                        return;
                    }
                    let take = (wanted - self.filled).min(bytes.len());
                    self.prefix[self.filled..self.filled + take].copy_from_slice(&bytes[..take]);
                    self.filled += take;
                    bytes = &bytes[take..];
                    if self.filled < wanted {
                        return;
                    }
                    self.filled = 0;
                    let field = |index: usize| u64::from(u32_at(&self.prefix, 4 * index));
                    self.stage = match self.stage {
                        Stage::Prefix => Stage::Id {
                            remaining: field(0),
                            skip: field(1) + field(2) + 4 * field(3),
                            entries: field(4),
                        },
                        Stage::Entry { entries } => Stage::Skip {
                            remaining: field(0) + field(1),
                            entries: entries - 1,
                        },
                        _ => unreachable!(),
                    };
                }
                Stage::Id {
                    remaining,
                    skip,
                    entries,
                } => {
                    let take = remaining.min(bytes.len() as u64) as usize;
                    let room = self.limit.saturating_sub(self.id.len()).min(take);
                    self.id.extend_from_slice(&bytes[..room]);
                    self.overflow |= room < take;
                    bytes = &bytes[take..];
                    if remaining > take as u64 {
                        self.stage = Stage::Id {
                            remaining: remaining - take as u64,
                            skip,
                            entries,
                        };
                        return;
                    }
                    self.stage = Stage::Skip {
                        remaining: skip,
                        entries,
                    };
                }
                Stage::Skip { remaining, entries } => {
                    let take = remaining.min(bytes.len() as u64);
                    bytes = &bytes[take as usize..];
                    if remaining > take {
                        self.stage = Stage::Skip {
                            remaining: remaining - take,
                            entries,
                        };
                        return;
                    }
                    if entries == 0 {
                        self.finish_record(first);
                    } else {
                        self.stage = Stage::Entry { entries };
                    }
                }
            }
        }
    }

    /// Whether the block ended on a record boundary with the indexed count
    /// and first and last ids.
    pub(super) fn finish(&self, entry: &super::Entry, last: &str) -> bool {
        matches!(self.stage, Stage::Prefix)
            && self.filled == 0
            && self.records == u64::from(entry.document_count)
            && self.first_ok
            && !self.last_overflow
            && self.last == last.as_bytes()
    }
}

/// Re-encodes a stream of binary records as the canonical text `doc` lines,
/// holding one bounded output buffer and no whole record.
pub(super) struct TextRecords<R> {
    inner: R,
    output: Vec<u8>,
    position: usize,
    state: Text,
}

#[derive(Clone, Copy)]
enum Text {
    Record,
    Hex {
        field: u8,
        remaining: u64,
        lengths: [u64; 2],
        dimension: u64,
        entries: u64,
    },
    Vector {
        remaining: u64,
        first: bool,
        entries: u64,
    },
    Entries {
        remaining: u64,
        first: bool,
    },
    Key {
        remaining: u64,
        value: u64,
        entries: u64,
    },
    Value {
        remaining: u64,
        entries: u64,
    },
}

/// Output gathered before it is handed to the reader. One step adds at most
/// `2 * HEX_INPUT_BYTES` hex digits or one formatted component.
pub(super) const TEXT_OUTPUT_BYTES: usize = 16 * 1024;
const TEXT_BUFFER_BYTES: usize = TEXT_OUTPUT_BYTES + 2 * HEX_INPUT_BYTES + 64;

impl<R: BufRead> TextRecords<R> {
    pub(super) fn new(inner: R) -> Self {
        Self {
            inner,
            output: Vec::with_capacity(TEXT_BUFFER_BYTES),
            position: 0,
            state: Text::Record,
        }
    }

    /// Reads exactly `output.len()` bytes; `Ok(false)` only at a clean end
    /// before the first byte.
    fn read_exact_or_end(&mut self, output: &mut [u8]) -> io::Result<bool> {
        let mut filled = 0;
        while filled < output.len() {
            match self.inner.read(&mut output[filled..]) {
                Ok(0) if filled == 0 => return Ok(false),
                Ok(0) => return Err(io::Error::other(invalid("record is truncated"))),
                Ok(count) => filled += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    fn read_exact(&mut self, output: &mut [u8]) -> io::Result<()> {
        if self.read_exact_or_end(output)? {
            Ok(())
        } else {
            Err(io::Error::other(invalid("record is truncated")))
        }
    }

    /// Hex-encodes up to `remaining` input bytes; returns how many it took.
    fn hex(&mut self, remaining: u64) -> io::Result<u64> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let available = self.inner.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::other(invalid("record is truncated")));
        }
        let take = (remaining.min(available.len() as u64) as usize).min(HEX_INPUT_BYTES);
        for byte in &available[..take] {
            self.output.push(HEX[usize::from(byte >> 4)]);
            self.output.push(HEX[usize::from(byte & 15)]);
        }
        self.inner.consume(take);
        Ok(take as u64)
    }

    /// Advances until the output holds a chunk or the stream ends.
    fn fill(&mut self) -> io::Result<()> {
        self.output.clear();
        self.position = 0;
        while self.output.len() < TEXT_OUTPUT_BYTES {
            match self.state {
                Text::Record => {
                    let mut prefix = [0; PREFIX_BYTES];
                    if !self.read_exact_or_end(&mut prefix)? {
                        return Ok(());
                    }
                    let field = |index: usize| u64::from(u32_at(&prefix, 4 * index));
                    self.output.extend_from_slice(b"doc\t");
                    self.state = Text::Hex {
                        field: 0,
                        remaining: field(0),
                        lengths: [field(1), field(2)],
                        dimension: field(3),
                        entries: field(4),
                    };
                }
                Text::Hex {
                    field,
                    remaining,
                    lengths,
                    dimension,
                    entries,
                } => {
                    if remaining > 0 {
                        let taken = self.hex(remaining)?;
                        self.state = Text::Hex {
                            field,
                            remaining: remaining - taken,
                            lengths,
                            dimension,
                            entries,
                        };
                        continue;
                    }
                    self.output.push(b'\t');
                    self.state = match field {
                        0 | 1 => Text::Hex {
                            field: field + 1,
                            remaining: lengths[usize::from(field)],
                            lengths,
                            dimension,
                            entries,
                        },
                        _ => Text::Vector {
                            remaining: dimension,
                            first: true,
                            entries,
                        },
                    };
                }
                Text::Vector {
                    remaining,
                    first,
                    entries,
                } => {
                    if remaining == 0 {
                        self.output.push(b'\t');
                        self.state = Text::Entries {
                            remaining: entries,
                            first: true,
                        };
                        continue;
                    }
                    let mut value = [0; 4];
                    self.read_exact(&mut value)?;
                    if !first {
                        self.output.push(b',');
                    }
                    write!(self.output, "{}", f32::from_le_bytes(value))?;
                    self.state = Text::Vector {
                        remaining: remaining - 1,
                        first: false,
                        entries,
                    };
                }
                Text::Entries { remaining, first } => {
                    if remaining == 0 {
                        self.output.push(b'\n');
                        self.state = Text::Record;
                        continue;
                    }
                    if !first {
                        self.output.push(b';');
                    }
                    let mut prefix = [0; ENTRY_PREFIX_BYTES];
                    self.read_exact(&mut prefix)?;
                    self.state = Text::Key {
                        remaining: u64::from(u32_at(&prefix, 0)),
                        value: u64::from(u32_at(&prefix, 4)),
                        entries: remaining - 1,
                    };
                }
                Text::Key {
                    remaining,
                    value,
                    entries,
                } => {
                    if remaining > 0 {
                        let taken = self.hex(remaining)?;
                        self.state = Text::Key {
                            remaining: remaining - taken,
                            value,
                            entries,
                        };
                        continue;
                    }
                    self.output.push(b'=');
                    self.state = Text::Value {
                        remaining: value,
                        entries,
                    };
                }
                Text::Value { remaining, entries } => {
                    if remaining > 0 {
                        let taken = self.hex(remaining)?;
                        self.state = Text::Value {
                            remaining: remaining - taken,
                            entries,
                        };
                        continue;
                    }
                    self.state = Text::Entries {
                        remaining: entries,
                        first: false,
                    };
                }
            }
        }
        Ok(())
    }
}

impl<R: BufRead> Read for TextRecords<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.position == self.output.len() {
            self.fill()?;
        }
        let count = (self.output.len() - self.position).min(output.len());
        output[..count].copy_from_slice(&self.output[self.position..self.position + count]);
        self.position += count;
        Ok(count)
    }
}

impl<R: BufRead> BufRead for TextRecords<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.position == self.output.len() {
            self.fill()?;
        }
        Ok(&self.output[self.position..])
    }

    fn consume(&mut self, amount: usize) {
        self.position = (self.position + amount).min(self.output.len());
    }
}

/// The memory a [`TextRecords`] holds besides its input reader.
pub(super) const TEXT_RECORDS_BYTES: usize = TEXT_BUFFER_BYTES;

#[cfg(test)]
mod tests;
