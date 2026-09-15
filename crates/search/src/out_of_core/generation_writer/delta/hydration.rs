//! Ordered update hydration, retaining one admitted encoded/decoded document.

use crate::build_control::checkpoint;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, reserve_capacity, AdmittedDocument, BuildMemory,
    SET_ENTRY_BYTES,
};
use crate::out_of_core::hydration::{CheckedReader, RangeReader};
use crate::out_of_core::search_document_bytes;
use crate::{
    parse_snapshot_header, Result, SearchOutOfCoreMetrics, SearchOutOfCoreReader,
    SearchSegmentDescriptorEntry, SkeinError,
};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::io::{self, BufRead, BufReader, Read};

mod decoder;

const INPUT_BYTES: usize = 8192;

pub(super) fn visit(
    reader: &SearchOutOfCoreReader,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    consumer: &mut dyn FnMut(AdmittedDocument) -> Result<()>,
) -> Result<SearchOutOfCoreMetrics> {
    let mut metrics = SearchOutOfCoreMetrics::default();
    for segment in &reader.descriptor.segments {
        checkpoint(task)?;
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        let input = RangeReader {
            file: &reader.payload,
            offset: range.offset,
            remaining: range.length,
        };
        let peak = read_segment(
            input,
            range.length,
            range.checksum,
            segment,
            reader.config.max_uncompressed_segment_bytes.get(),
            memory,
            task,
            consumer,
        )?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.hydration_segment_bytes_read = metrics
            .hydration_segment_bytes_read
            .saturating_add(range.length);
        metrics.peak_segment_document_bytes = metrics.peak_segment_document_bytes.max(peak);
    }
    Ok(metrics)
}

#[allow(clippy::too_many_arguments)]
fn read_segment(
    input: impl Read,
    length: u64,
    checksum: u64,
    segment: &SearchSegmentDescriptorEntry,
    max_uncompressed: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    consumer: &mut dyn FnMut(AdmittedDocument) -> Result<()>,
) -> Result<u64> {
    let _scratch = memory.spool.reserve(4 * INPUT_BYTES)?;
    let mut range = CheckedReader::new(Controlled {
        inner: input.take(length),
        task,
    });
    let mut buffered = BufReader::with_capacity(INPUT_BYTES, &mut range);
    let documents = read_envelope(
        &mut buffered,
        length,
        segment,
        max_uncompressed,
        memory,
        task,
        consumer,
    );
    // A malformed prefix cannot hide a later range read/checksum failure. The
    // consumer writes only to an unpublished stage, which any error discards.
    io::copy(&mut buffered, &mut io::sink())?;
    drop(buffered);
    if range.count != length {
        return Err(invalid("payload length mismatch"));
    }
    if range.digest.finish() != checksum {
        return Err(invalid("payload checksum mismatch"));
    }
    documents
}

fn read_envelope(
    input: &mut impl BufRead,
    length: u64,
    segment: &SearchSegmentDescriptorEntry,
    max_uncompressed: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    consumer: &mut dyn FnMut(AdmittedDocument) -> Result<()>,
) -> Result<u64> {
    let mut header = Buffer::new(memory)?;
    while !header.bytes.ends_with(b"\n\n") {
        if !header.append_line(input, task)? {
            return Err(invalid("compressed envelope missing header terminator"));
        }
    }
    let compressed_len = length
        .checked_sub(header.bytes.len() as u64)
        .ok_or_else(|| invalid("compressed length underflow"))?;
    let text = std::str::from_utf8(&header.bytes[..header.bytes.len() - 2])
        .map_err(|_| invalid("compressed envelope header is not UTF-8"))?;
    // The existing parser borrows fields but allocates a split-vector and a
    // small seen-field tree. Admit their worst growth before calling it.
    let columns = text
        .lines()
        .map(|line| line.bytes().filter(|byte| *byte == b'\t').count() + 1)
        .max()
        .unwrap_or(1);
    let parser = memory.spool.reserve(add(
        mul(mul(columns.max(1), 4)?, std::mem::size_of::<&str>())?,
        6 * SET_ENTRY_BYTES,
    )?)?;
    let parsed = parse_snapshot_header(text)?;
    drop(parser);
    drop(header);
    let expected_compressed_len = parsed
        .compressed_len
        .ok_or_else(|| invalid("missing compressed_len"))? as u64;
    let expected_compressed_checksum = parsed
        .compressed_checksum
        .ok_or_else(|| invalid("missing compressed_checksum"))?;
    let expected_len = parsed
        .uncompressed_len
        .ok_or_else(|| invalid("missing uncompressed_len"))? as u64;
    let expected_checksum = parsed
        .uncompressed_checksum
        .ok_or_else(|| invalid("missing uncompressed_checksum"))?;
    if compressed_len != expected_compressed_len {
        return Err(invalid("compressed length mismatch"));
    }
    if expected_len > max_uncompressed {
        return Err(invalid(format_args!(
            "uncompressed payload requires {expected_len} bytes, exceeding {max_uncompressed}"
        )));
    }
    let mut compressed = CheckedReader::new(input);
    let decoder = decoder::Decoder::new(
        BufReader::with_capacity(INPUT_BYTES, &mut compressed),
        memory,
        task,
    )?;
    let mut inflated = CheckedReader::new(decoder.take(expected_len.saturating_add(1)));
    let mut text = BufReader::with_capacity(INPUT_BYTES, &mut inflated);
    let documents = read_documents(&mut text, segment, memory, task, consumer);
    let drained = io::copy(&mut text, &mut io::sink());
    drop(text);
    let count = inflated.count;
    let digest = inflated.digest.finish();
    drop(inflated);
    io::copy(&mut compressed, &mut io::sink())?;
    if compressed.count != expected_compressed_len
        || compressed.digest.finish() != expected_compressed_checksum
    {
        return Err(invalid("compressed checksum or length mismatch"));
    }
    if let Err(error) = drained {
        return documents.and(Err(error.into()));
    }
    if count != expected_len {
        return Err(invalid("uncompressed length mismatch"));
    }
    if digest != expected_checksum {
        return Err(invalid("uncompressed checksum mismatch"));
    }
    documents
}

fn read_documents(
    text: &mut impl BufRead,
    segment: &SearchSegmentDescriptorEntry,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    consumer: &mut dyn FnMut(AdmittedDocument) -> Result<()>,
) -> Result<u64> {
    let mut previous = None::<PreviousId>;
    let mut count = 0usize;
    let mut peak = 0;
    let mut line = Buffer::new(memory)?;
    loop {
        checkpoint(task)?;
        line.bytes.clear();
        if !line.append_line(text, task)? {
            break;
        }
        let bytes = match line.bytes.strip_suffix(b"\n") {
            Some(bytes) => bytes.strip_suffix(b"\r").unwrap_or(bytes),
            None => &line.bytes,
        };
        if bytes.is_empty() || bytes == b"SKEIN_SEARCH_SEGMENT_V1" {
            continue;
        }
        std::str::from_utf8(bytes).map_err(|_| invalid("document line is not UTF-8"))?;
        let document = super::super::spool::decode_line_admitted(bytes, count, memory, task)?;
        if count == 0 && document.id != segment.first_document_id {
            return Err(invalid("document bounds do not match its descriptor"));
        }
        if previous
            .as_ref()
            .is_some_and(|previous| previous.value >= document.id)
        {
            return Err(invalid("documents are not strictly ordered"));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid("document count overflow"))?;
        if count > segment.document_count {
            return Err(invalid("document count exceeds its descriptor"));
        }
        let lease = memory.retained.reserve(document.id.len())?;
        previous = Some(PreviousId {
            value: document.id.clone(),
            _memory: lease,
        });
        peak = peak.max(search_document_bytes(&document));
        consumer(document)?;
    }
    if count != segment.document_count
        || previous.as_ref().map(|id| id.value.as_str()) != Some(segment.last_document_id.as_str())
    {
        return Err(invalid(
            "document count or bounds do not match its descriptor",
        ));
    }
    Ok(peak)
}

struct PreviousId {
    value: String,
    _memory: QueryMemoryLease,
}

struct Buffer {
    bytes: Vec<u8>,
    memory: QueryMemoryLease,
}

impl Buffer {
    fn new(memory: &BuildMemory) -> Result<Self> {
        Ok(Self {
            bytes: Vec::new(),
            memory: memory.spool.reserve(0)?,
        })
    }

    fn append_line(&mut self, input: &mut impl BufRead, task: &RuntimeTaskContext) -> Result<bool> {
        let initial = self.bytes.len();
        loop {
            checkpoint(task)?;
            let bytes = input.fill_buf()?;
            if bytes.is_empty() {
                return Ok(self.bytes.len() != initial);
            }
            let newline = bytes.iter().position(|byte| *byte == b'\n');
            let count = newline.map_or(bytes.len(), |position| position + 1);
            let required = add(self.bytes.len(), count)?;
            if required > self.bytes.capacity() {
                let capacity = required.max(mul(self.bytes.capacity().max(4), 2)?);
                reserve_capacity(&mut self.bytes, capacity, &mut self.memory)?;
            }
            self.bytes.extend_from_slice(&bytes[..count]);
            input.consume(count);
            if newline.is_some() {
                return Ok(true);
            }
        }
    }
}

struct Controlled<'a, R> {
    inner: R,
    task: &'a RuntimeTaskContext,
}

impl<R: Read> Read for Controlled<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            checkpoint(self.task).map_err(io::Error::other)?;
            match self.inner.read(bytes) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => return result,
            }
        }
    }
}

fn invalid(reason: impl std::fmt::Display) -> SkeinError {
    SkeinError::Storage(format!("search hydration {reason}"))
}

#[cfg(test)]
mod tests;
