use super::{
    encode_block_header, encode_posting, write_string, BlockDescriptor, BlockKind, Digest, Posting,
    Result, SkeinError, SPILL_IO_BUFFER_BYTES,
};
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

    fn bounds(self) -> Result<(&'a str, &'a str)> {
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
        bounds.ok_or_else(|| SkeinError::Storage("cannot encode an empty lexical block".into()))
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
            Self::Postings(entries) => {
                for posting in entries {
                    encode_posting(&mut *writer, posting)?;
                }
            }
        }
        Ok(())
    }
}

pub(super) fn write_block(
    writer: &mut impl Write,
    generation: u64,
    block_id: u64,
    offset: u64,
    max_bytes: u64,
    entries: Entries<'_>,
) -> Result<BlockDescriptor> {
    let (min_key, max_key) = entries.bounds()?;
    // The sizing pass follows the wire grammar without copying or hashing data.
    // It also validates representable string lengths and counts before any I/O.
    let mut counter = CountingWriter::default();
    entries.encode(&mut counter, generation, block_id)?;
    let length = counter.0;
    if length > max_bytes {
        return Err(SkeinError::Storage(format!(
            "lexical build produced a {length} byte block, exceeding {max_bytes}"
        )));
    }
    offset
        .checked_add(length)
        .ok_or_else(|| SkeinError::Storage("lexical block offset exceeds u64".into()))?;
    block_id
        .checked_add(1)
        .ok_or_else(|| SkeinError::Storage("lexical block identity exceeds u64".into()))?;

    let mut output = DigestWriter {
        writer,
        digest: Digest::new(),
    };
    entries.encode(&mut output, generation, block_id)?;
    Ok(BlockDescriptor {
        block_id,
        kind: entries.kind(),
        min_key: min_key.to_owned(),
        max_key: max_key.to_owned(),
        offset,
        length,
        checksum: output.digest.finish(),
        // The encoding pass above checked the same borrowed entry count.
        entry_count: entries.len() as u32,
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
