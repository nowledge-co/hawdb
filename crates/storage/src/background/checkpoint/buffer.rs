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
        Self::build(source.len(), work, |bytes| bytes.append(source, work))
    }

    pub(crate) fn build(
        capacity: usize,
        work: &CheckpointWorkContext,
        write: impl FnOnce(&mut CheckpointBytes) -> Result<(), CheckpointWorkError>,
    ) -> Result<Self, CheckpointWorkError> {
        Self::build_with_error(capacity, work, std::convert::identity, write)
    }

    pub(crate) fn build_with_error<E>(
        capacity: usize,
        work: &CheckpointWorkContext,
        map_work: impl Fn(CheckpointWorkError) -> E,
        write: impl FnOnce(&mut CheckpointBytes) -> Result<(), E>,
    ) -> Result<Self, E> {
        // The pinned standard Arc layout owns two usize reference counters.
        // Include the concrete shared cell as well as its exact Vec capacity.
        // Allocator metadata/fragmentation remain platform assumptions.
        let ownership_bytes = std::mem::size_of::<CheckpointBytes>()
            + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>();
        let mut bytes = CheckpointBytes::new_with_ownership(capacity, ownership_bytes, work)
            .map_err(&map_work)?;
        write(&mut bytes)?;
        let unit = work.start_unit().map_err(&map_work)?;
        let output = Self(Arc::new(bytes));
        unit.finish();
        work.checkpoint().map_err(map_work)?;
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
    pub(crate) fn truncate(&mut self, length: usize) {
        self.bytes.truncate(length);
    }

    #[cfg(test)]
    pub(crate) fn into_unadmitted(self) -> Vec<u8> {
        assert!(
            self._memory.is_none(),
            "admitted bytes cannot detach their lease"
        );
        self.bytes
    }

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

    /// Fragment chains grow geometrically. Admit simultaneous old/new
    /// capacities before copying, without extending beyond the record limit.
    pub(crate) fn append_growing(
        &mut self,
        bytes: &[u8],
        limit: Option<usize>,
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        let required = self.bytes.len().checked_add(bytes.len()).ok_or_else(|| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "fragment chain length overflows usize".into(),
            })
        })?;
        if required > self.bytes.capacity() {
            let capacity = required
                .max(self.bytes.capacity().saturating_mul(2))
                .min(limit.unwrap_or(usize::MAX));
            if capacity < required {
                return Err(work.record_failure(CheckpointWorkError::Allocation {
                    bytes: required as u64,
                    reason: "fragment chain exceeds its checked record limit".into(),
                }));
            }
            let mut replacement = Self::new(capacity, work)?;
            replacement.append(&self.bytes, work)?;
            std::mem::swap(self, &mut replacement);
            drop(replacement);
        }
        self.append(bytes, work)
    }

    /// Install this buffer in retained state before observing the next stop.
    pub(crate) fn new_retained(
        capacity: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        Self::allocate_capacity(capacity, 0, work)
    }

    pub(crate) fn extend_zeroes_to(
        &mut self,
        length: usize,
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        if length > self.bytes.capacity() {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: length as u64,
                reason: "zero initialization exceeds admitted byte capacity".into(),
            }));
        }
        while self.bytes.len() < length {
            let unit = work.start_unit()?;
            self.bytes
                .resize(self.bytes.len().saturating_add(64 * 1024).min(length), 0);
            unit.finish();
        }
        work.checkpoint()
    }

    /// Append one admitted physical block, retaining it if the unit cancels.
    pub(crate) fn append_block_retained(
        &mut self,
        block: &[u8],
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        let required = self.bytes.len().checked_add(block.len()).ok_or_else(|| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: u64::MAX,
                reason: "retained byte length overflows usize".into(),
            })
        })?;
        if block.len() > 64 * 1024 || required > self.bytes.capacity() {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: required as u64,
                reason: "retained byte block exceeds its admission".into(),
            }));
        }
        let unit = work.start_unit()?;
        self.bytes.extend_from_slice(block);
        unit.finish();
        Ok(())
    }

    fn new_with_ownership(
        capacity: usize,
        ownership_bytes: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        let output = Self::allocate_capacity(capacity, ownership_bytes, work)?;
        work.checkpoint()?;
        Ok(output)
    }

    fn allocate_capacity(
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
