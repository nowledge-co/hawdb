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

//! Admit fixed value capacity before allocation and retain it through sharing.
//! Element allocations need their own ownership. Arc layout/allocator metadata
//! and allocation/destruction latency remain platform assumptions.

use super::*;

#[derive(Debug)]
pub(crate) struct CheckpointValues<T> {
    // Drop elements and their backing allocation before the capacity lease.
    values: Vec<T>,
    _memory: Option<Box<dyn RuntimeMemoryPermit>>,
    capacity: usize,
}

#[derive(Debug)]
pub(crate) struct CheckpointSharedValues<T>(Arc<CheckpointValues<T>>);

impl<T> Clone for CheckpointSharedValues<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> std::ops::Deref for CheckpointSharedValues<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.0.values
    }
}

impl<T> CheckpointValues<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.values
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }

    pub(crate) fn new(
        capacity: usize,
        work: &CheckpointWorkContext,
    ) -> Result<Self, CheckpointWorkError> {
        let unit = work.start_unit()?;
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .and_then(|bytes| {
                bytes.checked_add(2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>())
            })
            .ok_or_else(|| {
                work.record_failure(CheckpointWorkError::Allocation {
                    bytes: u64::MAX,
                    reason: "checkpoint value capacity overflows usize".into(),
                })
            })?;
        let memory = work.reserve_memory(bytes)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|error| {
            work.record_failure(CheckpointWorkError::Allocation {
                bytes: bytes as u64,
                reason: error.to_string(),
            })
        })?;
        // A zero-sized Vec has no backing allocation and reports usize::MAX.
        if std::mem::size_of::<T>() != 0 && values.capacity() != capacity {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: bytes as u64,
                reason: format!(
                    "allocator granted {} values beyond admitted capacity {capacity}",
                    values.capacity()
                ),
            }));
        }
        let output = Self {
            values,
            _memory: memory,
            capacity,
        };
        unit.finish();
        work.checkpoint()?;
        Ok(output)
    }

    pub(crate) fn push(
        &mut self,
        value: T,
        work: &CheckpointWorkContext,
    ) -> Result<(), CheckpointWorkError> {
        let unit = work.start_unit()?;
        if self.values.len() == self.capacity {
            return Err(work.record_failure(CheckpointWorkError::Allocation {
                bytes: self
                    .values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<T>()) as u64,
                reason: "checkpoint value push would exceed admitted capacity".into(),
            }));
        }
        self.values.push(value);
        unit.finish();
        work.checkpoint()
    }

    pub(crate) fn share(
        self,
        work: &CheckpointWorkContext,
    ) -> Result<CheckpointSharedValues<T>, CheckpointWorkError> {
        let unit = work.start_unit()?;
        let output = CheckpointSharedValues(Arc::new(self));
        unit.finish();
        work.checkpoint()?;
        Ok(output)
    }
}
