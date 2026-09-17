use super::*;
use crate::build_memory::{checked_add, checked_mul, BuildMemory};
use crate::build_term::Term;
use crate::{RuntimeTaskContext, SkeinError};
use skein_executor::QueryMemoryLease;
use std::collections::hash_map::Entry;
use std::mem::size_of;

#[derive(Clone, Copy, Default)]
pub(crate) struct Control<'a> {
    pub(crate) memory: Option<&'a BuildMemory>,
    pub(crate) task: Option<&'a RuntimeTaskContext>,
    pub(crate) workspace: Option<&'a crate::analyzer_workspace::Workspace>,
}

impl Control<'_> {
    pub(super) fn check(self) -> Result<()> {
        self.task.map_or(Ok(()), crate::build_control::checkpoint)
    }

    pub(super) fn copy(self, text: &str) -> Result<Term> {
        self.check()?;
        Term::copy(text, self.memory)
    }

    pub(super) fn build(self, capacity: usize, build: impl FnOnce() -> String) -> Result<Term> {
        self.check()?;
        let term = Term::build(capacity, self.memory, build)?;
        self.check()?;
        Ok(term)
    }
}

pub(super) struct Dedup<'a> {
    terms: HashMap<Text<'a>, ()>,
    _memory: Option<QueryMemoryLease>,
}

impl<'a> Dedup<'a> {
    pub(super) fn new() -> Self {
        Self {
            terms: HashMap::new(),
            _memory: None,
        }
    }

    pub(super) fn insert(&mut self, text: Text<'a>, control: Control<'_>) -> Result<Option<Term>> {
        if self.terms.len() == self.terms.capacity() {
            // A duplicate must not need a replacement admission when full.
            if self.terms.contains_key(text.as_str()) {
                return Ok(None);
            }
            let mut replacement = Self::with_capacity(checked_add(self.terms.len(), 1)?, control)?;
            replacement.terms.extend(self.terms.drain());
            // Release the old allocation and lease before copying a new term,
            // preserving the existing peak-admission boundary. Growth failures
            // before this commit leave the original owner unchanged.
            *self = replacement;
        }
        match self.terms.entry(text) {
            Entry::Occupied(_) => Ok(None),
            Entry::Vacant(entry) => {
                let emitted = entry.key().materialize(control)?;
                entry.insert(());
                Ok(Some(emitted))
            }
        }
    }

    fn with_capacity(capacity: usize, control: Control<'_>) -> Result<Self> {
        let bytes = table_bytes::<(Text<'_>, ())>(capacity)?;
        let memory = control
            .memory
            .map(|memory| memory.retained.reserve(bytes))
            .transpose()?;
        // Field order keeps this candidate's lease alive through allocation
        // failure and capacity rejection, without a manual grow/shrink rollback.
        let mut candidate = Self {
            terms: HashMap::new(),
            _memory: memory,
        };
        candidate.terms.try_reserve(capacity).map_err(|error| {
            SkeinError::Execution(format!("reserve search identifier deduplication: {error}"))
        })?;
        if table_bytes::<(Text<'_>, ())>(candidate.terms.capacity())? > bytes {
            return Err(SkeinError::Execution(
                "search identifier deduplication exceeded admitted capacity".into(),
            ));
        }
        Ok(candidate)
    }
}

fn table_bytes<T>(capacity: usize) -> Result<usize> {
    if capacity == 0 {
        return Ok(0);
    }
    // Pinned Rust HashMap load factor: three usable slots in four buckets,
    // otherwise at most seven eighths full. Allow either SIMD control width.
    let buckets = if capacity < 8 {
        if capacity <= 3 {
            4
        } else {
            8
        }
    } else {
        checked_mul(capacity, 8)?
            .checked_div(7)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| SkeinError::Execution("search dedup capacity overflow".into()))?
    };
    checked_add(checked_mul(buckets, checked_add(size_of::<T>(), 1)?)?, 32)
}

#[cfg(test)]
mod tests;
