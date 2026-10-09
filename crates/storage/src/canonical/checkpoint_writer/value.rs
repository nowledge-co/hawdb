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

//! Count the established canonical value encoding without a complete buffer,
//! then admit its exact capacity and retain ownership through its final use.

use super::*;
use crate::background::CheckpointBytes;
use std::io::{self, Write};

pub(crate) fn encode(
    value: &Value,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes, CanonicalSegmentError> {
    let mut counter = Counter {
        length: 0,
        work,
        failure: None,
    };
    let result = write_value_streaming(&mut counter, value, 1);
    if let Some(error) = counter.failure {
        return Err(CanonicalSegmentError::Work(error));
    }
    result?;
    work.checkpoint()?;
    let mut output = Output {
        bytes: CheckpointBytes::new(counter.length, work)?,
        work,
        failure: None,
    };
    let result = write_value_streaming(&mut output, value, 1);
    if let Some(error) = output.failure {
        return Err(CanonicalSegmentError::Work(error));
    }
    result?;
    work.checkpoint()?;
    Ok(output.bytes)
}

struct Counter<'a> {
    length: usize,
    work: &'a CheckpointWorkContext,
    failure: Option<CheckpointWorkError>,
}

impl Counter<'_> {
    fn stop(&mut self, error: CheckpointWorkError) -> io::Error {
        self.failure = Some(error);
        io::ErrorKind::Other.into()
    }
}

impl Write for Counter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for block in bytes.chunks(64 * 1024) {
            let unit = match self.work.start_unit() {
                Ok(unit) => unit,
                Err(error) => return Err(self.stop(error)),
            };
            self.length = match self.length.checked_add(block.len()) {
                Some(length) => length,
                None => {
                    return Err(self.stop(self.work.record_failure(
                        CheckpointWorkError::Allocation {
                            bytes: u64::MAX,
                            reason: "canonical value wire length overflows usize".into(),
                        },
                    )))
                }
            };
            unit.finish();
            if let Err(error) = self.work.checkpoint() {
                return Err(self.stop(error));
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct Output<'a> {
    bytes: CheckpointBytes,
    work: &'a CheckpointWorkContext,
    failure: Option<CheckpointWorkError>,
}

impl Write for Output<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.bytes.append(bytes, self.work) {
            self.failure = Some(error);
            return Err(io::ErrorKind::Other.into());
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
