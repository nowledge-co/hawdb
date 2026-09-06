//! Fixed-capacity, operation-owned output after an allocation-free sizing pass.

use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::error::{Result, SkeinError};
use serde::Serialize;
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher;
use std::fmt;
use std::io::{self, Write};

pub(crate) struct Buffer {
    bytes: Vec<u8>,
    limit: usize,
    task: RuntimeTaskContext,
    _memory: QueryMemoryLease,
}

impl Buffer {
    pub(crate) fn new(
        bytes: usize,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        evidence::allocation();
        let mut output = Vec::new();
        output.try_reserve_exact(bytes).map_err(|error| {
            SkeinError::Execution(format!("search build output allocation failed: {error}"))
        })?;
        Ok(Self {
            bytes: output,
            limit: bytes,
            task: task.clone(),
            _memory: lease,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        checkpoint(&self.task).map_err(io::Error::other)?;
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other(
                "search build output exceeds admitted capacity",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        checkpoint(&self.task).map_err(io::Error::other)
    }
}

impl fmt::Write for Buffer {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.write_all(text.as_bytes()).map_err(|_| fmt::Error)
    }
}

struct Measure<'a> {
    bytes: usize,
    limit: u64,
    checksum: Crc32cHasher,
    task: &'a RuntimeTaskContext,
}

impl<'a> Measure<'a> {
    fn new(limit: u64, task: &'a RuntimeTaskContext) -> Self {
        Self {
            bytes: 0,
            limit,
            checksum: Crc32cHasher::new(),
            task,
        }
    }
}

impl Write for Measure<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        checkpoint(self.task).map_err(io::Error::other)?;
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|&size| size as u64 <= self.limit)
            .ok_or_else(|| io::Error::other("search build encoded byte budget exceeded"))?;
        self.checksum.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl fmt::Write for Measure<'_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.write_all(text.as_bytes()).map_err(|_| fmt::Error)
    }
}

fn encoding_error(task: &RuntimeTaskContext) -> SkeinError {
    checkpoint(task).err().unwrap_or_else(|| {
        SkeinError::Storage(
            "search build encoded byte budget exceeded or preflight mismatch".to_owned(),
        )
    })
}

pub(crate) fn formatted(
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    emit: impl Fn(&mut dyn fmt::Write) -> fmt::Result,
) -> Result<Buffer> {
    let mut measure = Measure::new(limit, task);
    emit(&mut measure).map_err(|_| encoding_error(task))?;
    let mut output = Buffer::new(measure.bytes, memory, task)?;
    emit(&mut output).map_err(|_| encoding_error(task))?;
    if output.len() != measure.bytes {
        return Err(encoding_error(task));
    }
    checkpoint(task)?;
    Ok(output)
}

pub(crate) fn formatted_with_checksum(
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    emit: impl Fn(&mut dyn fmt::Write) -> fmt::Result,
) -> Result<Buffer> {
    let mut measure = Measure::new(limit, task);
    emit(&mut measure).map_err(|_| encoding_error(task))?;
    let checksum = measure.checksum.finish();
    formatted(limit, memory, task, |output| {
        emit(output)?;
        writeln!(output, "checksum\t{checksum}")
    })
}

pub(crate) fn json_envelope<T: Serialize>(
    body: &T,
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Buffer> {
    let mut measure = Measure::new(limit, task);
    serde_json::to_writer(&mut measure, body).map_err(|_| encoding_error(task))?;
    #[derive(Serialize)]
    struct Envelope<'a, T> {
        body: &'a T,
        checksum: u64,
    }
    let envelope = Envelope {
        body,
        checksum: measure.checksum.finish(),
    };
    let mut measure = Measure::new(limit, task);
    serde_json::to_writer(&mut measure, &envelope).map_err(|_| encoding_error(task))?;
    let mut output = Buffer::new(measure.bytes, memory, task)?;
    serde_json::to_writer(&mut output, &envelope).map_err(|_| encoding_error(task))?;
    if output.len() != measure.bytes {
        return Err(encoding_error(task));
    }
    checkpoint(task)?;
    Ok(output)
}

#[cfg(test)]
pub(crate) mod evidence {
    use std::cell::Cell;
    thread_local! { static ALLOCATIONS: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn allocation() {
        ALLOCATIONS.with(|value| value.set(value.get() + 1));
    }
    pub(crate) fn take() -> usize {
        ALLOCATIONS.with(|value| value.replace(0))
    }
}
