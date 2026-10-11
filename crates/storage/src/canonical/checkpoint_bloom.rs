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

//! Hash the existing canonical encoding through its streaming writer instead
//! of materializing a variable-width key/value buffer. Controlled hashing owns
//! each bounded CPU unit; it does not perform physical I/O.

use super::{u32_len, write_value_streaming, CanonicalSegmentError, CheckpointWorkContext};
use crate::background::CheckpointWorkError;
use hawdb_core::{LabelId, Value};
use hawdb_integrity::Crc32cHasher;
use std::io::{self, Write};

pub(super) fn key(
    label: LabelId,
    property: &str,
    value: &Value,
    work: Option<&CheckpointWorkContext>,
) -> Result<u64, CanonicalSegmentError> {
    let mut output = Hash {
        crc: Crc32cHasher::new(),
        work,
        failure: None,
    };
    let result = (|| {
        output.write_all(&label.0.to_le_bytes())?;
        output.write_all(&u32_len(property.len(), "string")?.to_le_bytes())?;
        output.write_all(property.as_bytes())?;
        write_value_streaming(&mut output, value, 0)
    })();
    // The existing streaming writer returns I/O errors. Preserve their typed
    // work cause without string matching or allocating a diagnostic wrapper.
    if let Some(error) = output.failure {
        return Err(CanonicalSegmentError::Work(error));
    }
    result?;
    if let Some(work) = work {
        work.checkpoint()?;
    }
    Ok(output.crc.finish())
}

struct Hash<'a> {
    crc: Crc32cHasher,
    work: Option<&'a CheckpointWorkContext>,
    failure: Option<CheckpointWorkError>,
}

impl Hash<'_> {
    fn stopped(&mut self, error: CheckpointWorkError) -> io::Error {
        self.failure = Some(error);
        // A simple Other error allocates nothing and write_all does not retry
        // it. Interrupted would retry forever after cancellation.
        io::ErrorKind::Other.into()
    }
}

impl Write for Hash<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for block in bytes.chunks(64 * 1024) {
            let unit = match self.work.map(CheckpointWorkContext::start_unit).transpose() {
                Ok(unit) => unit,
                Err(error) => return Err(self.stopped(error)),
            };
            self.crc.update(block);
            if let Some(unit) = unit {
                unit.finish();
            }
            if let Some(work) = self.work
                && let Err(error) = work.checkpoint()
            {
                return Err(self.stopped(error));
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
