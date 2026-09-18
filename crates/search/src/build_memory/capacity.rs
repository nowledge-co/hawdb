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

//! Own vector replacement admission until the candidate is accepted or dropped.

use super::{checked_mul, reserved::Grant};
use crate::{HawDBError, Result};
use hawdb_executor::QueryMemoryLease;
use std::collections::TryReserveError;
use std::mem::size_of;

pub(crate) enum Memory<'a> {
    Lease(&'a mut QueryMemoryLease),
    Grant(Option<&'a mut Grant>),
}

impl Memory<'_> {
    fn grow(&mut self, bytes: usize) -> Result<()> {
        match self {
            Self::Lease(lease) => lease.grow(bytes),
            Self::Grant(grant) => grant.as_mut().map_or(Ok(()), |grant| grant.grow(bytes)),
        }
    }

    fn shrink(&mut self, bytes: usize) {
        match self {
            Self::Lease(lease) => lease.shrink(bytes),
            Self::Grant(Some(grant)) => grant.shrink(bytes),
            Self::Grant(None) => {}
        }
    }
}

struct Growth<'a> {
    memory: Memory<'a>,
    additional: usize,
}

impl Growth<'_> {
    fn commit(mut self, old_bytes: usize) {
        self.memory.shrink(old_bytes);
        self.additional = 0;
    }
}

impl Drop for Growth<'_> {
    fn drop(&mut self) {
        if self.additional != 0 {
            self.memory.shrink(self.additional);
        }
    }
}

pub(crate) fn reserve<T>(
    values: &mut Vec<T>,
    capacity: usize,
    memory: Memory<'_>,
    description: &'static str,
) -> Result<()> {
    reserve_with(
        values,
        capacity,
        memory,
        description,
        Vec::try_reserve_exact,
    )
}

fn reserve_with<T>(
    values: &mut Vec<T>,
    capacity: usize,
    mut memory: Memory<'_>,
    description: &'static str,
    allocate: impl FnOnce(&mut Vec<T>, usize) -> std::result::Result<(), TryReserveError>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let old_bytes = checked_mul(values.capacity(), size_of::<T>())?;
    let replacement_bytes = checked_mul(capacity, size_of::<T>())?;
    memory.grow(replacement_bytes)?;
    let growth = Growth {
        memory,
        additional: replacement_bytes,
    };
    // Declared after the guard: failure or unwind destroys the candidate before
    // returning its admission. The original allocation and elements stay owned.
    let mut replacement = Vec::new();
    allocate(&mut replacement, capacity).map_err(|error| {
        HawDBError::Execution(format!("{description} allocation failed: {error}"))
    })?;
    if replacement.capacity() != capacity {
        return Err(HawDBError::Execution(format!(
            "{description} exceeded admission"
        )));
    }
    // No allocation is needed: capacity exceeds the original capacity and len.
    // Drop the emptied old buffer before releasing its share of the admission.
    replacement.append(values);
    *values = replacement;
    growth.commit(old_bytes);
    Ok(())
}

#[cfg(test)]
mod tests;
