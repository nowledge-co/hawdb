use crate::build_control::{checkpoint, CheckedWriter};
use crate::build_memory::BuildMemory;
use crate::error::{Result, SkeinError};
use serde::Serialize;
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher;
use std::io::{self, Write};

#[cfg(test)]
pub(super) mod tests;

#[derive(Serialize)]
struct Envelope<'a, T> {
    body: &'a T,
    checksum: u64,
}

#[cfg(test)]
pub(super) fn encode(body: &impl Serialize, max_bytes: u64) -> Result<Vec<u8>> {
    Ok(encode_inner(body, max_bytes, None, None)?.bytes)
}

#[derive(Debug)]
pub(super) struct EncodedManifest {
    pub(super) bytes: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}

pub(super) fn encode_with_context(
    body: &impl Serialize,
    max_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<EncodedManifest> {
    encode_inner(body, max_bytes, Some(memory), Some(task))
}

fn encode_inner(
    body: &impl Serialize,
    max_bytes: u64,
    memory: Option<&BuildMemory>,
    task: Option<&RuntimeTaskContext>,
) -> Result<EncodedManifest> {
    task.map_or(Ok(()), checkpoint)?;
    let envelope = Envelope {
        body,
        checksum: checksum_with_context(body, task)?,
    };
    let mut measure = JsonMeasure::default();
    serde_json::to_writer(CheckedWriter::new(&mut measure, task), &envelope).map_err(json_error)?;
    task.map_or(Ok(()), checkpoint)?;
    let length = measure.length;
    if length > max_bytes {
        return Err(SkeinError::Storage(format!(
            "lexical projection manifest requires {length} bytes, exceeding {max_bytes}"
        )));
    }
    let length = usize::try_from(length).map_err(|_| {
        SkeinError::Storage("lexical projection manifest length exceeds usize".into())
    })?;
    let lease = memory
        .map(|memory| memory.retained.reserve(length))
        .transpose()?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|error| {
        SkeinError::Storage(format!(
            "lexical projection manifest allocation failed: {error}"
        ))
    })?;
    // Serialization borrows the same immutable body for both passes. A fixed
    // output boundary also prevents future serializers from growing the buffer
    // if their output unexpectedly differs from the admitted sizing pass.
    if bytes.capacity() > length {
        return Err(SkeinError::Execution(
            "lexical manifest capacity exceeded admission".into(),
        ));
    }
    let mut output = AdmittedOutput {
        bytes: &mut bytes,
        length,
    };
    serde_json::to_writer(CheckedWriter::new(&mut output, task), &envelope).map_err(json_error)?;
    task.map_or(Ok(()), checkpoint)?;
    if bytes.len() != length {
        return Err(SkeinError::Storage(
            "lexical projection manifest length changed after admission".into(),
        ));
    }
    Ok(EncodedManifest {
        bytes,
        _memory: lease,
    })
}

pub(super) fn checksum_with_context(
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
            .ok_or_else(|| io::Error::other("lexical projection manifest length exceeds u64"))?;
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
}

impl Write for AdmittedOutput<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length =
            self.bytes.len().checked_add(bytes.len()).ok_or_else(|| {
                io::Error::other("lexical projection manifest length exceeds usize")
            })?;
        if length > self.length || length > self.bytes.capacity() {
            return Err(io::Error::other(
                "lexical projection manifest exceeded its admitted output buffer",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
