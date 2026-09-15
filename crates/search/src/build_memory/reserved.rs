//! Subdivide one admitted reservation without releasing its root ownership.

use super::checked_add;
use super::shared::Shared;
use crate::{Result, SkeinError};
use skein_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::mem::size_of;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug, Clone)]
pub(crate) struct ReservedMemory(Shared<Inner>);

#[derive(Debug)]
struct Inner {
    state: Mutex<State>,
    scratch: Mutex<()>,
    scratch_bytes: usize,
}

#[derive(Debug)]
struct State {
    capacity: usize,
    used: usize,
    memory: QueryMemoryLease,
}

impl ReservedMemory {
    #[cfg(test)]
    pub(crate) fn new(account: &QueryMemoryAccount, capacity: usize) -> Result<Self> {
        Self::with_scratch_capacity(account, capacity, 0)
    }

    pub(crate) fn with_scratch_capacity(
        account: &QueryMemoryAccount,
        capacity: usize,
        scratch_bytes: usize,
    ) -> Result<Self> {
        let memory = account.reserve(checked_add(
            checked_add(capacity, scratch_bytes)?,
            Self::metadata_bytes(),
        )?)?;
        Ok(Self(Shared::new(Inner {
            state: Mutex::new(State {
                capacity,
                used: 0,
                memory,
            }),
            scratch: Mutex::new(()),
            scratch_bytes,
        })))
    }

    pub(crate) const fn metadata_bytes() -> usize {
        size_of::<Inner>() + 2 * size_of::<usize>()
    }

    pub(crate) fn ensure_capacity(&self, capacity: usize) -> Result<()> {
        let mut state = self.state();
        if capacity > state.capacity {
            let additional = capacity - state.capacity;
            state.memory.grow(additional)?;
            state.capacity = capacity;
        }
        Ok(())
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<Grant> {
        self.charge(bytes)?;
        Ok(Grant {
            owner: self.clone(),
            bytes,
        })
    }

    fn charge(&self, bytes: usize) -> Result<()> {
        let mut state = self.state();
        let required = checked_add(state.used, bytes)?;
        if required > state.capacity {
            return Err(SkeinError::Execution(format!(
                "search spill progress would use {required} bytes, exceeding its {}-byte reservation",
                state.capacity,
            )));
        }
        state.used = required;
        Ok(())
    }

    pub(crate) fn with_scratch<T>(
        &self,
        bytes: usize,
        work: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        if bytes > self.0.scratch_bytes {
            return Err(SkeinError::Execution(format!(
                "search spill needs {bytes} native path scratch bytes, exceeding its {}-byte reservation",
                self.0.scratch_bytes,
            )));
        }
        // This capacity is never granted to retained payloads. Serialize native
        // path conversions; returned data must have its own separate admission.
        let _exclusive = self
            .0
            .scratch
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        work()
    }

    fn release(&self, bytes: usize) {
        let mut state = self.state();
        state.used -= bytes;
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // No user code runs under this lock. Fallible checks precede counter
        // changes, including when the underlying root rejects capacity growth.
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

#[derive(Debug)]
pub(crate) struct Grant {
    owner: ReservedMemory,
    bytes: usize,
}

impl Grant {
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn grow(&mut self, bytes: usize) -> Result<()> {
        let required = checked_add(self.bytes, bytes)?;
        self.owner.charge(bytes)?;
        self.bytes = required;
        Ok(())
    }

    pub(crate) fn shrink(&mut self, bytes: usize) {
        let released = bytes.min(self.bytes);
        self.owner.release(released);
        self.bytes -= released;
    }

    pub(crate) fn with_scratch<T>(
        &self,
        bytes: usize,
        work: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.owner.with_scratch(bytes, work)
    }
}

impl Drop for Grant {
    fn drop(&mut self) {
        self.owner.release(self.bytes);
    }
}

#[cfg(test)]
mod tests;

pub(crate) mod native_path;
