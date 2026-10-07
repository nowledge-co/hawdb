// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Keep actual byte capacity and its admitted ownership together.
//! Replacement reserves both buffers before copying; the old allocation is
//! destroyed before its lease. Allocator metadata/latency are platform assumptions.

use super::*;

#[derive(Debug)]
pub(crate) struct CheckpointBytes {
    // Fields drop in declaration order: bytes must die before their admission.
    bytes: Vec<u8>,
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
}

/// Immutable checkpoint bytes whose clones share the original allocation lease.
/// There is no conversion that detaches the bytes from admitted ownership.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct CheckpointSharedBytes(Arc<CheckpointBytes>);

impl std::ops::Deref for CheckpointSharedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for CheckpointSharedBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq for CheckpointSharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for CheckpointSharedBytes {}

impl CheckpointSharedBytes {
    pub(super) fn copy(
        source: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        // The pinned standard Arc layout owns two usize reference counters.
        // Include the concrete shared cell as well as its exact Vec capacity.
        // Allocator metadata/fragmentation remain platform assumptions.
        let ownership_bytes = std::mem::size_of::<CheckpointBytes>()
            + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
        let mut bytes = CheckpointBytes::new_with_ownership(source.len(), ownership_bytes, work)?;
        bytes.append(source, work)?;
        let unit = work.start_unit()?;
        let output = Self(Arc::new(bytes));
        unit.finish();
        work.checkpoint()?;
        Ok(output)
    }
}

struct Append<'a> {
    bytes: &'a mut Vec<u8>,
    previous_len: usize,
    committed: bool,
}

impl Drop for Append<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.bytes.truncate(self.previous_len);
        }
    }
}

#[cfg(test)]
mod tests;

impl std::ops::Deref for CheckpointBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl CheckpointBytes {
    pub(crate) fn new(
        capacity: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        Self::new_with_ownership(capacity, 0, work)
    }

    pub(crate) fn zeroed(
        length: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        let mut output = Self::new(length, work)?;
        for start in (0..length).step_by(64 * 1024) {
            let unit = work.start_unit()?;
            output
                .bytes
                .resize(start.saturating_add(64 * 1024).min(length), 0);
            unit.finish();
        }
        work.checkpoint()?;
        Ok(output)
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    fn new_with_ownership(
        capacity: usize,
        ownership_bytes: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        let unit = work.start_unit()?;
        let reservation = capacity.checked_add(ownership_bytes).ok_or_else(|| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "byte buffer ownership size overflows usize".into(),
            })
        })?;
        let memory = work.reserve_memory(reservation)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).map_err(|error| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: error.to_string(),
            })
        })?;
        if bytes.capacity() != capacity {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: format!(
                    "allocator granted {} bytes beyond admitted capacity {capacity}",
                    bytes.capacity()
                ),
            }));
        }
        let output = Self {
            bytes,
            _memory: memory,
        };
        unit.finish();
        work.checkpoint()?;
        Ok(output)
    }

    pub(crate) fn append(
        &mut self,
        bytes: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        let capacity = self.bytes.len().checked_add(bytes.len()).ok_or_else(|| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "byte buffer length overflows usize".into(),
            })
        })?;
        if capacity > self.bytes.capacity() {
            let mut replacement = Self::new(capacity, work)?;
            for block in self.bytes.chunks(64 * 1024) {
                let unit = work.start_unit()?;
                replacement.bytes.extend_from_slice(block);
                unit.finish();
            }
            std::mem::swap(self, &mut replacement);
            drop(replacement);
        }
        let previous_len = self.bytes.len();
        let mut append = Append {
            bytes: &mut self.bytes,
            previous_len,
            committed: false,
        };
        for block in bytes.chunks(64 * 1024) {
            let unit = work.start_unit()?;
            append.bytes.extend_from_slice(block);
            unit.finish();
        }
        work.checkpoint()?;
        append.committed = true;
        Ok(())
    }
}
