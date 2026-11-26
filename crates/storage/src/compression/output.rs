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

//! A fixed admitted output for one unflushed Zstd frame.

use crate::background::{CheckpointBytes, CheckpointWorkContext, CheckpointWorkError};
use std::io::{self, Write};

pub(crate) struct AdmittedCompressionOutput {
    bytes: CheckpointBytes,
    used: usize,
}

impl AdmittedCompressionOutput {
    pub(crate) fn new(
        input_len: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        // Include the existing streaming/header slack above compressBound.
        // Exceeding this fixed capacity fails before authority publication.
        let capacity = super::backend::compress_bound(input_len)
            .map_err(|error| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: input_len as u64,
                    reason: error.to_string(),
                })
            })?
            .checked_add(input_len / (64 * 1024) * 32)
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or_else(|| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: u64::MAX,
                    reason: "compressed output capacity overflows usize".into(),
                })
            })?;
        Ok(Self {
            bytes: CheckpointBytes::zeroed(capacity, work)?,
            used: 0,
        })
    }

    pub(crate) fn into_bytes(mut self) -> CheckpointBytes {
        self.bytes.truncate(self.used);
        self.bytes
    }
}

impl Write for AdmittedCompressionOutput {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .used
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("compressed output length overflows usize"))?;
        let destination = self
            .bytes
            .as_mut_slice()
            .get_mut(self.used..end)
            .ok_or_else(|| {
                io::Error::other("compressed stream exceeds admitted output capacity")
            })?;
        // The encoder calls this inside a bounded compression unit. Its
        // fixed admitted staging buffer bounds each copy without a nested unit.
        destination.copy_from_slice(bytes);
        self.used = end;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
