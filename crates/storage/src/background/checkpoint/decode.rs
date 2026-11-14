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

//! Allocation inventory shared by checkpoint codecs. Values and every
//! replacement capacity are admitted before allocation; callers move the
//! inventory with retained data rather than returning detached buffers.

use super::*;
use hawdb_core::{HawDBError, Result};

pub(crate) struct CheckpointDecodeContext {
    pub(crate) work: CheckpointWorkContext,
    pub(crate) memory: std::cell::RefCell<crate::background::CheckpointAllocationOwner>,
}

impl std::ops::Deref for CheckpointDecodeContext {
    type Target = CheckpointWorkContext;
    fn deref(&self) -> &Self::Target {
        &self.work
    }
}

impl CheckpointDecodeContext {
    pub(crate) fn reserve(
        &self,
        bytes: usize,
    ) -> Result<crate::background::CheckpointAllocationToken> {
        self.memory
            .borrow_mut()
            .reserve(bytes, self)
            .map_err(HawDBError::from_storage_error)
    }
    pub(crate) fn find(
        &self,
        address: usize,
    ) -> Result<crate::background::CheckpointAllocationToken> {
        self.memory
            .borrow()
            .find(address, self)
            .map_err(HawDBError::from_storage_error)
    }
    pub(crate) fn find_retained(
        &self,
        address: usize,
        pending: &mut Option<crate::background::CheckpointAllocationLookup>,
    ) -> Result<crate::background::CheckpointAllocationToken> {
        self.memory
            .borrow()
            .find_retained(address, pending, self)
            .map_err(HawDBError::from_storage_error)
    }
}

pub(crate) fn allocation(
    error: impl std::fmt::Display,
    bytes: usize,
    work: &CheckpointDecodeContext,
) -> HawDBError {
    HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
        bytes: bytes as u64,
        reason: error.to_string(),
    }))
}

/// Own the incoming item and both vector backings while a bounded move pauses.
/// The caller must retain the associated decode inventory until this state dies.
pub(crate) struct CheckpointVectorPush<T> {
    value: Option<T>,
    growth: Option<VectorGrowth<T>>,
    previous: Option<crate::background::CheckpointAllocationToken>,
    located_previous: bool,
    lookup: Option<crate::background::CheckpointAllocationLookup>,
}

struct VectorGrowth<T> {
    old: std::vec::IntoIter<T>,
    replacement: Vec<T>,
    previous: Option<crate::background::CheckpointAllocationToken>,
}

impl<T> CheckpointVectorPush<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            growth: None,
            previous: None,
            located_previous: false,
            lookup: None,
        }
    }
}

impl CheckpointDecodeContext {
    pub(crate) fn vector_capacity<T>(&self, capacity: usize) -> Result<Vec<T>> {
        let bytes = capacity
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| {
                allocation("decoded vector capacity overflows usize", usize::MAX, self)
            })?;
        let token = self.reserve(bytes)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(capacity)
            .map_err(|error| allocation(error, bytes, self))?;
        if std::mem::size_of::<T>() != 0 && output.capacity() != capacity {
            return Err(allocation(
                "decoded vector capacity exceeds admission",
                bytes,
                self,
            ));
        }
        token.address(output.as_ptr() as usize);
        Ok(output)
    }

    /// Copy one schema string after admitting its exact backing capacity.
    /// The caller keeps this inventory until the copied string is destroyed.
    pub(crate) fn string(&self, value: &str) -> Result<String> {
        let mut output = self
            .string_capacity(value.len())
            .map_err(HawDBError::from_storage_error)?;
        let mut offset = 0;
        while offset < value.len() {
            let end = value.floor_char_boundary(offset.saturating_add(64 * 1024));
            let unit = self.start_unit().map_err(HawDBError::from_storage_error)?;
            output.push_str(&value[offset..end]);
            offset = end;
            unit.finish();
        }
        self.checkpoint().map_err(HawDBError::from_storage_error)?;
        Ok(output)
    }

    pub(crate) fn string_capacity(
        &self,
        capacity: usize,
    ) -> std::result::Result<String, CheckpointWorkError> {
        let token = self.memory.borrow_mut().reserve(capacity, self)?;
        let mut output = String::new();
        output.try_reserve_exact(capacity).map_err(|error| {
            self.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: error.to_string(),
            })
        })?;
        if output.capacity() != capacity {
            return Err(self.record_failure(CheckpointWorkError::Allocation {
                bytes: capacity as u64,
                reason: "string capacity differs from admitted capacity".into(),
            }));
        }
        token.address(output.as_ptr() as usize);
        Ok(output)
    }

    pub(crate) fn push<T>(&self, values: &mut Vec<T>, value: T) -> Result<()> {
        let mut pending = Some(CheckpointVectorPush::new(value));
        self.resume_push(values, &mut pending)
    }

    /// Resume only with the same destination and inventory, excluding reads or
    /// other mutations until the pending item has been appended.
    pub(crate) fn resume_push<T>(
        &self,
        values: &mut Vec<T>,
        pending: &mut Option<CheckpointVectorPush<T>>,
    ) -> Result<()> {
        let Some(state) = pending.as_mut() else {
            return Ok(());
        };
        if state.growth.is_none() && values.len() == values.capacity() {
            if !state.located_previous {
                state.previous = if values.capacity() == 0 {
                    None
                } else {
                    Some(self.find_retained(values.as_ptr() as usize, &mut state.lookup)?)
                };
                state.located_previous = true;
            }
            let capacity = values
                .capacity()
                .saturating_mul(2)
                .max(values.len().saturating_add(1));
            let unit = self.start_unit().map_err(HawDBError::from_storage_error)?;
            let replacement = self.vector_capacity(capacity)?;
            state.growth = Some(VectorGrowth {
                old: std::mem::take(values).into_iter(),
                replacement,
                previous: state.previous.take(),
            });
            unit.finish();
        }
        if let Some(growth) = &mut state.growth {
            let count = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
            while !growth.old.as_slice().is_empty() {
                let unit = self.start_unit().map_err(HawDBError::from_storage_error)?;
                growth.replacement.extend(growth.old.by_ref().take(count));
                unit.finish();
            }
            let growth = state.growth.take().expect("retained vector growth");
            drop(growth.old);
            if let Some(previous) = growth.previous {
                previous.release_buffer();
            }
            *values = growth.replacement;
        }
        let unit = self.start_unit().map_err(HawDBError::from_storage_error)?;
        values.push(state.value.take().expect("pending vector item"));
        *pending = None;
        unit.finish();
        self.checkpoint().map_err(HawDBError::from_storage_error)
    }
}
