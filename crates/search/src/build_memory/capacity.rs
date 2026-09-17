//! Own vector replacement admission until the candidate is accepted or dropped.

use super::{checked_mul, reserved::Grant};
use crate::{Result, SkeinError};
use skein_executor::QueryMemoryLease;
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
        SkeinError::Execution(format!("{description} allocation failed: {error}"))
    })?;
    if replacement.capacity() != capacity {
        return Err(SkeinError::Execution(format!(
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
