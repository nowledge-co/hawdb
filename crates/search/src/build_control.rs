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

//! Cooperative controls for one mutable projection build, never its readers.

use crate::error::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_integrity::Crc32cHasher;
use std::io::{self, Write};

pub(crate) mod json;
pub(crate) mod temporary;

pub(crate) fn checkpoint(context: &RuntimeTaskContext) -> Result<()> {
    #[cfg(test)]
    observation::record();
    context
        .checkpoint()
        .map_err(|reason| HawDBError::Execution(format!("search generation build {reason}")))
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

#[cfg(test)]
pub(crate) mod observation;
