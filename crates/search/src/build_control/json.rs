//! Borrowed JSON envelopes with admission before output allocation.

use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::BuildMemory;
use crate::build_memory::{checked_add as add, checked_mul as mul};
use crate::error::{Result, SkeinError};
use serde::Serialize;
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher;
use std::io::{self, Write};

#[derive(Serialize)]
struct Envelope<'a, T> {
    body: &'a T,
    checksum: u64,
}

#[cfg(test)]
pub(crate) fn encode(body: &impl Serialize, max_bytes: u64, name: &'static str) -> Result<Vec<u8>> {
    Ok(encode_inner(body, max_bytes, None, None, name)?.bytes)
}

#[derive(Debug)]
pub(crate) struct EncodedManifest {
    pub(crate) bytes: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}

pub(crate) fn encode_with_context(
    body: &impl Serialize,
    max_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    name: &'static str,
) -> Result<EncodedManifest> {
    encode_inner(body, max_bytes, Some(memory), Some(task), name)
}

fn encode_inner(
    body: &impl Serialize,
    max_bytes: u64,
    memory: Option<&BuildMemory>,
    task: Option<&RuntimeTaskContext>,
    name: &'static str,
) -> Result<EncodedManifest> {
    prepare(body, max_bytes, task, name)?.encode_inner(memory, task)
}

pub(crate) struct PreparedEnvelope<'a, T> {
    envelope: Envelope<'a, T>,
    length: usize,
    checksum: u64,
    name: &'static str,
}

pub(crate) fn prepare<'a, T: Serialize>(
    body: &'a T,
    max_bytes: u64,
    task: Option<&RuntimeTaskContext>,
    name: &'static str,
) -> Result<PreparedEnvelope<'a, T>> {
    task.map_or(Ok(()), checkpoint)?;
    let envelope = Envelope {
        body,
        checksum: checksum_with_context(body, task)?,
    };
    let mut measure = JsonMeasure {
        digest: Some(Crc32cHasher::new()),
        ..JsonMeasure::default()
    };
    serde_json::to_writer(CheckedWriter::new(&mut measure, task), &envelope).map_err(json_error)?;
    task.map_or(Ok(()), checkpoint)?;
    let length = measure.length;
    if length > max_bytes {
        return Err(SkeinError::Storage(format!(
            "{name} requires {length} bytes, exceeding {max_bytes}"
        )));
    }
    let length = usize::try_from(length)
        .map_err(|_| SkeinError::Storage(format!("{name} length exceeds usize")))?;
    Ok(PreparedEnvelope {
        envelope,
        length,
        checksum: measure
            .digest
            .expect("envelope measure has a digest")
            .finish(),
        name,
    })
}

impl<T: Serialize> PreparedEnvelope<'_, T> {
    pub(crate) fn len(&self) -> usize {
        self.length
    }

    pub(crate) fn checksum(&self) -> u64 {
        self.checksum
    }

    pub(crate) fn encode(
        self,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<EncodedManifest> {
        self.encode_inner(Some(memory), Some(task))
    }

    fn encode_inner(
        self,
        memory: Option<&BuildMemory>,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<EncodedManifest> {
        task.map_or(Ok(()), checkpoint)?;
        let Self {
            envelope,
            length,
            name,
            ..
        } = self;
        let lease = memory
            .map(|memory| memory.retained.reserve(length))
            .transpose()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|error| SkeinError::Storage(format!("{name} allocation failed: {error}")))?;
        // Both passes borrow the same immutable body. The fixed output boundary
        // also rejects serializers that change their length after preparation.
        if bytes.capacity() > length {
            return Err(SkeinError::Execution(format!(
                "{name} capacity exceeded admission"
            )));
        }
        let mut output = AdmittedOutput {
            bytes: &mut bytes,
            length,
            name,
        };
        serde_json::to_writer(CheckedWriter::new(&mut output, task), &envelope)
            .map_err(json_error)?;
        task.map_or(Ok(()), checkpoint)?;
        if bytes.len() != length {
            return Err(SkeinError::Storage(format!(
                "{name} length changed after admission"
            )));
        }
        Ok(EncodedManifest {
            bytes,
            _memory: lease,
        })
    }
}

pub(crate) fn checksum_with_context(
    body: &impl Serialize,
    task: Option<&RuntimeTaskContext>,
) -> Result<u64> {
    task.map_or(Ok(()), checkpoint)?;
    let mut measure = JsonMeasure {
        digest: Some(Crc32cHasher::new()),
        ..JsonMeasure::default()
    };
    serde_json::to_writer(CheckedWriter::new(&mut measure, task), body).map_err(json_error)?;
    task.map_or(Ok(()), checkpoint)?;
    Ok(measure
        .digest
        .expect("checksum writer has a digest")
        .finish())
}

/// Preflight the fixed manifest schemas' owned strings, vectors and serde scratch.
/// Callers supply their trusted largest vector element and number of vectors.
pub(crate) fn decode_capacity(
    bytes: &[u8],
    element_bytes: usize,
    vector_count: usize,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let mut containers = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    let mut token_bytes = 0usize;
    let mut largest = 8usize;
    for chunk in bytes.chunks(8192) {
        checkpoint(task)?;
        for &byte in chunk {
            if quoted {
                token_bytes += 1;
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    quoted = false;
                    largest = largest.max(token_bytes);
                    token_bytes = 0;
                }
            } else if byte == b'"' {
                largest = largest.max(token_bytes);
                token_bytes = 0;
                quoted = true;
            } else if byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E') {
                token_bytes += 1;
            } else {
                largest = largest.max(token_bytes);
                token_bytes = 0;
                if matches!(byte, b'{' | b'[') {
                    containers += 1;
                }
            }
        }
    }
    checkpoint(task)?;
    largest = largest.max(token_bytes);
    // Each successfully decoded record opens a map or sequence; malformed input
    // cannot append a partially decoded record. Include each vector's four-slot
    // minimum and old/replacement capacity overlap under pinned serde/Rust.
    let slots = add(containers, mul(vector_count, 4)?)?;
    let vectors = mul(mul(slots, 3)?, element_bytes)?;
    // Decoded strings cannot exceed their complete source bytes. Escaped strings
    // and number parsing share reusable scratch with geometric growth/overlap.
    // Error diagnostics may quote an unknown field and the fixed schema fields.
    add(add(add(bytes.len(), vectors)?, mul(largest, 8)?)?, 4096)
}

fn json_error(error: serde_json::Error) -> SkeinError {
    SkeinError::Storage(error.to_string())
}

#[derive(Default)]
struct JsonMeasure {
    length: u64,
    digest: Option<Crc32cHasher>,
}

impl Write for JsonMeasure {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.length = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("search manifest length exceeds u64"))?;
        if let Some(digest) = &mut self.digest {
            digest.update(bytes);
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct AdmittedOutput<'a> {
    bytes: &'a mut Vec<u8>,
    length: usize,
    name: &'static str,
}

impl Write for AdmittedOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other(format!("{} length exceeds usize", self.name)))?;
        if length > self.length || length > self.bytes.capacity() {
            return Err(io::Error::other(format!(
                "{} exceeded its admitted output buffer",
                self.name
            )));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_output_rejects_growth_without_changing_bytes_or_capacity() {
        let mut bytes = Vec::with_capacity(8);
        let capacity = bytes.capacity();
        let mut output = AdmittedOutput {
            bytes: &mut bytes,
            length: 4,
            name: "test manifest",
        };
        output.write_all(b"abcd").unwrap();
        assert!(output.write_all(b"x").is_err());
        assert_eq!(bytes, b"abcd");
        assert_eq!(bytes.capacity(), capacity);
    }

    #[test]
    fn counting_overflow_rejects_without_advancing() {
        let mut measure = JsonMeasure {
            length: u64::MAX,
            digest: None,
        };
        assert!(measure.write(b"x").is_err());
        assert_eq!(measure.length, u64::MAX);
    }
}
