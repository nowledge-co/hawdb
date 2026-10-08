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

impl CheckpointDecodeContext {
    pub(crate) fn push<T>(&self, values: &mut Vec<T>, value: T) -> Result<()> {
        let work = self;
        if values.len() == values.capacity() {
            let previous = if values.capacity() == 0 {
                None
            } else {
                Some(work.find(values.as_ptr() as usize)?)
            };
            let capacity = values
                .capacity()
                .saturating_mul(2)
                .max(values.len().saturating_add(1));
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    allocation("WAL vector capacity overflows usize", usize::MAX, work)
                })?;
            let token = work.reserve(bytes)?;
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(capacity).map_err(|error| {
                allocation(
                    error,
                    capacity.saturating_mul(std::mem::size_of::<T>()),
                    work,
                )
            })?;
            if replacement.capacity() != capacity {
                return Err(allocation(
                    "vector capacity differs from admitted capacity",
                    bytes,
                    work,
                ));
            }
            token.address(replacement.as_ptr() as usize);
            unit.finish();
            let mut old = std::mem::take(values).into_iter();
            let count = (64 * 1024 / std::mem::size_of::<T>().max(1)).max(1);
            while !old.as_slice().is_empty() {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                replacement.extend(old.by_ref().take(count));
                unit.finish();
            }
            drop(old);
            if let Some(previous) = previous {
                previous.release_buffer();
            }
            *values = replacement;
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        values.push(value);
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)
    }
}
