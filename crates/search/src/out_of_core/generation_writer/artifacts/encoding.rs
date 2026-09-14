use crate::document_encoding::SegmentEncoding;
use crate::{search_snapshot_compression_header, Result, SkeinError, SEARCH_COMPRESSION_LEVEL};
use skein_integrity::Crc32cHasher;
use std::io::{self, Write};

pub(super) fn encode_segment_payload(
    encoding: &SegmentEncoding<'_>,
    segment_id: u64,
    name: &str,
    max_uncompressed_bytes: u64,
    max_compressed_bytes: u64,
) -> Result<Vec<u8>> {
    if encoding.len() as u64 > max_uncompressed_bytes {
        return Err(SkeinError::Storage(format!(
            "search generation {name} segment {segment_id} requires {} bytes, exceeding {max_uncompressed_bytes}",
            encoding.len(),
        )));
    }
    let result = (|| {
        let buffer = CompressedBuffer {
            bytes: Vec::new(),
            limit: usize::try_from(max_compressed_bytes).unwrap_or(usize::MAX),
            digest: Crc32cHasher::new(),
        };
        let mut encoder = zstd::stream::write::Encoder::new(buffer, SEARCH_COMPRESSION_LEVEL)?;
        let mut digest = Crc32cHasher::new();
        encoding.write_to(&mut DigestWriter {
            writer: &mut encoder,
            digest: &mut digest,
        })?;
        let mut compressed = encoder.finish()?;
        let header = search_snapshot_compression_header(
            digest.finish(),
            compressed.digest.finish(),
            encoding.len(),
            compressed.bytes.len(),
        );
        // Grow and move in place: retaining a second compressed payload would
        // defeat admission even after eliminating the uncompressed text copy.
        compressed.reserve(header.len())?;
        let payload_len = compressed.bytes.len();
        compressed.bytes.resize(payload_len + header.len(), 0);
        compressed.bytes.copy_within(..payload_len, header.len());
        compressed.bytes[..header.len()].copy_from_slice(header.as_bytes());
        Ok(compressed.bytes)
    })();
    result.map_err(|error: io::Error| {
        SkeinError::Storage(format!(
            "search generation {name} segment {segment_id} encoding failed: {error}"
        ))
    })
}

struct CompressedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    digest: Crc32cHasher,
}

impl CompressedBuffer {
    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        let required = self.bytes.len().checked_add(additional).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "compressed size overflow")
        })?;
        if required > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "requires {required} compressed bytes, exceeding {}",
                    self.limit
                ),
            ));
        }
        if required <= self.bytes.capacity() {
            return Ok(());
        }
        // Bound geometric growth by the admitted limit. Exact growth on every
        // zstd block can repeatedly copy an otherwise admitted large payload.
        let capacity = required.max(self.bytes.capacity().saturating_mul(2).min(self.limit));
        self.bytes
            .try_reserve_exact(capacity - self.bytes.len())
            .map_err(|error| {
                io::Error::other(format!(
                    "cannot allocate compressed search segment: {error}"
                ))
            })
    }
}

impl Write for CompressedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.reserve(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        self.digest.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct DigestWriter<'a, W> {
    writer: &'a mut W,
    digest: &'a mut Crc32cHasher,
}

impl<W: Write> Write for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;
