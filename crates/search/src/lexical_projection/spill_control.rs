//! Check one record at entry, then every bounded chunk of its output.

use super::{checkpoint, RuntimeTaskContext, SPILL_IO_BUFFER_BYTES};
use std::io::{self, Write};

pub(super) struct RecordWriter<'a, W> {
    writer: &'a mut W,
    task: Option<&'a RuntimeTaskContext>,
    remaining: usize,
}

impl<'a, W> RecordWriter<'a, W> {
    pub(super) fn new(writer: &'a mut W, task: Option<&'a RuntimeTaskContext>) -> io::Result<Self> {
        task.map_or(Ok(()), checkpoint).map_err(io::Error::other)?;
        Ok(Self {
            writer,
            task,
            remaining: SPILL_IO_BUFFER_BYTES,
        })
    }

    fn check(&mut self) -> io::Result<()> {
        self.task
            .map_or(Ok(()), checkpoint)
            .map_err(io::Error::other)?;
        self.remaining = SPILL_IO_BUFFER_BYTES;
        Ok(())
    }
}

impl<W: Write> Write for RecordWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            self.check()?;
        }
        let result = self.writer.write(&bytes[..bytes.len().min(self.remaining)]);
        match result {
            Ok(written) => self.remaining -= written,
            // Repeated interrupted writes must still observe cancellation.
            Err(_) => self.remaining = 0,
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.check()?;
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;
