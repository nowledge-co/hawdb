//! Cooperative controls for one mutable projection build, never its readers.

use crate::error::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_integrity::Crc32cHasher;
use std::io::{self, Write};

pub(crate) fn checkpoint(context: &RuntimeTaskContext) -> Result<()> {
    context
        .checkpoint()
        .map_err(|reason| SkeinError::Execution(format!("search generation build {reason}")))
}

pub(crate) fn write_checksummed(
    writer: &mut impl Write,
    bytes: &[u8],
    context: Option<&RuntimeTaskContext>,
) -> io::Result<u64> {
    let mut writer = CheckedWriter::new(writer, context);
    let mut digest = Crc32cHasher::new();
    for chunk in bytes.chunks(8192) {
        writer.write_all(chunk)?;
        digest.update(chunk);
    }
    writer.checkpoint()?;
    Ok(digest.finish())
}

pub(crate) struct CheckedWriter<'a, W> {
    writer: &'a mut W,
    context: Option<&'a RuntimeTaskContext>,
}

impl<'a, W> CheckedWriter<'a, W> {
    pub(crate) fn new(writer: &'a mut W, context: Option<&'a RuntimeTaskContext>) -> Self {
        Self { writer, context }
    }

    fn checkpoint(&self) -> io::Result<()> {
        self.context
            .map_or(Ok(()), checkpoint)
            .map_err(io::Error::other)
    }
}

impl<W: Write> Write for CheckedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.checkpoint()?;
        self.writer.write(&bytes[..bytes.len().min(8192)])
    }

    fn flush(&mut self) -> io::Result<()> {
        self.checkpoint()?;
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests;
