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

use super::{
    encode_block_header, posting_codec, write_string, BlockDescriptor, BlockKind, Digest,
    HawDBError, Posting, Result, SPILL_IO_BUFFER_BYTES,
};
use crate::build_control::{checkpoint, CheckedWriter};
use fst::MapBuilder;
use hawdb_core::RuntimeTaskContext;
use std::io::{self, Write};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
pub(super) enum Entries<'a> {
    Documents(&'a [(String, u32)]),
    Postings(&'a [Posting]),
}

impl<'a> Entries<'a> {
    fn kind(self) -> BlockKind {
        match self {
            Self::Documents(_) => BlockKind::Documents,
            Self::Postings(_) => BlockKind::Postings,
        }
    }

    fn len(self) -> usize {
        match self {
            Self::Documents(entries) => entries.len(),
            Self::Postings(entries) => entries.len(),
        }
    }

    pub(super) fn bounds(self) -> Result<(&'a str, &'a str)> {
        let bounds = match self {
            Self::Documents(entries) => entries
                .first()
                .zip(entries.last())
                .map(|(first, last)| (first.0.as_str(), last.0.as_str())),
            Self::Postings(entries) => entries
                .first()
                .zip(entries.last())
                .map(|(first, last)| (first.term.as_str(), last.term.as_str())),
        };
        bounds.ok_or_else(|| HawDBError::Storage("cannot encode an empty lexical block".into()))
    }

    fn encode(self, writer: &mut impl Write, generation: u64, block_id: u64) -> Result<()> {
        encode_block_header(writer, generation, block_id, self.kind(), self.len())?;
        match self {
            Self::Documents(entries) => {
                for (id, length) in entries {
                    write_string(writer, id)?;
                    writer.write_all(&length.to_le_bytes())?;
                }
            }
            Self::Postings(entries) => encode_postings(entries, writer)?,
        }
        Ok(())
    }
}

fn encode_postings(entries: &[Posting], writer: &mut impl Write) -> Result<()> {
    let mut dictionary = MapBuilder::memory();
    let mut frames = Vec::new();
    let mut start = 0usize;
    let mut previous_term: Option<&str> = None;
    while start < entries.len() {
        let term = entries[start].term.as_str();
        if term.is_empty() || previous_term.is_some_and(|previous| previous >= term) {
            return Err(HawDBError::Storage(
                "lexical posting terms are invalid or unordered".into(),
            ));
        }
        let mut end = start + 1;
        while end < entries.len() && entries[end].term.as_str() == term {
            end += 1;
        }
        let frame = posting_codec::encode_by(end - start, |index| {
            let posting = &entries[start + index];
            posting_codec::Posting {
                ordinal: posting.ordinal,
                tf: posting.term_frequency,
            }
        })
        .map_err(|error| HawDBError::Storage(format!("invalid lexical posting frame: {error}")))?;
        let offset = u32::try_from(frames.len())
            .map_err(|_| HawDBError::Storage("lexical posting payload exceeds u32".into()))?;
        let document_frequency = u32::try_from(end - start).map_err(|_| {
            HawDBError::Storage("lexical posting document frequency exceeds u32".into())
        })?;
        dictionary
            .insert(
                term,
                (u64::from(offset) << 32) | u64::from(document_frequency),
            )
            .map_err(|error| {
                HawDBError::Storage(format!("invalid lexical term dictionary: {error}"))
            })?;
        frames.extend_from_slice(&frame);
        previous_term = Some(term);
        start = end;
    }
    let dictionary = dictionary.into_inner().map_err(|error| {
        HawDBError::Storage(format!("invalid lexical term dictionary: {error}"))
    })?;
    writer.write_all(
        &u32::try_from(dictionary.len())
            .map_err(|_| HawDBError::Storage("lexical term dictionary exceeds u32".into()))?
            .to_le_bytes(),
    )?;
    writer.write_all(&dictionary)?;
    writer.write_all(&frames)?;
    Ok(())
}

#[cfg(test)]
pub(super) fn write_block(
    writer: &mut impl Write,
    generation: u64,
    block_id: u64,
    offset: u64,
    max_bytes: u64,
    entries: Entries<'_>,
) -> Result<BlockDescriptor> {
    write_block_with_context(
        writer, generation, block_id, offset, max_bytes, entries, None,
    )
}

pub(super) fn write_block_with_context(
    writer: &mut impl Write,
    generation: u64,
    block_id: u64,
    offset: u64,
    max_bytes: u64,
    entries: Entries<'_>,
    task: Option<&RuntimeTaskContext>,
) -> Result<BlockDescriptor> {
    task.map_or(Ok(()), checkpoint)?;
    let (min_key, max_key) = entries.bounds()?;
    // The sizing pass follows the wire grammar without copying or hashing data.
    // It also validates representable string lengths and counts before any I/O.
    let mut counter = CountingWriter::default();
    entries.encode(
        &mut CheckedWriter::new(&mut counter, task),
        generation,
        block_id,
    )?;
    let length = counter.0;
    if length > max_bytes {
        return Err(HawDBError::Storage(format!(
            "lexical build produced a {length} byte block, exceeding {max_bytes}"
        )));
    }
    offset
        .checked_add(length)
        .ok_or_else(|| HawDBError::Storage("lexical block offset exceeds u64".into()))?;
    block_id
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("lexical block identity exceeds u64".into()))?;

    task.map_or(Ok(()), checkpoint)?;
    // The builder admits directory slots and both key copies before this call.
    let min_key = min_key.to_owned();
    let max_key = max_key.to_owned();
    let mut checked = CheckedWriter::new(writer, task);
    let mut output = DigestWriter {
        writer: &mut checked,
        digest: Digest::new(),
    };
    entries.encode(&mut output, generation, block_id)?;
    task.map_or(Ok(()), checkpoint)?;
    Ok(BlockDescriptor {
        block_id,
        kind: entries.kind(),
        min_key,
        max_key,
        offset,
        length,
        checksum: output.digest.finish(),
        // The encoding pass above checked the same borrowed entry count.
        entry_count: entries.len() as u32,
        ordinal_start: 0,
    })
}

#[derive(Default)]
struct CountingWriter(u64);

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("lexical block length exceeds u64"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DigestWriter<'a, W> {
    writer: &'a mut W,
    digest: Digest,
}

impl<W: Write> Write for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = bytes.len().min(SPILL_IO_BUFFER_BYTES);
        let written = self.writer.write(&bytes[..length])?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
