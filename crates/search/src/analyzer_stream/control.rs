use super::*;
use crate::build_memory::{checked_add, checked_mul, BuildMemory};
use crate::build_term::Term;
use crate::{RuntimeTaskContext, SkeinError};
use skein_executor::QueryMemoryLease;
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
    memory: Option<QueryMemoryLease>,
}

impl<'a> Dedup<'a> {
    pub(super) fn new() -> Self {
        Self {
            terms: HashMap::new(),
            memory: None,
        }
    }

    pub(super) fn insert(&mut self, text: Text<'a>, control: Control<'_>) -> Result<Option<Term>> {
        if self.terms.contains_key(text.as_str()) {
            return Ok(None);
        }
        if self.terms.len() == self.terms.capacity() {
            let old = table_bytes::<(Text<'_>, ())>(self.terms.capacity())?;
            let next = table_bytes::<(Text<'_>, ())>(checked_add(self.terms.len(), 1)?)?;
            if let Some(memory) = control.memory {
                match self.memory.as_mut() {
                    Some(lease) => lease.grow(next)?,
                    None => self.memory = Some(memory.retained.reserve(next)?),
                }
            }
            self.terms.try_reserve(1).map_err(|error| {
                SkeinError::Execution(format!("reserve search identifier deduplication: {error}"))
            })?;
            if table_bytes::<(Text<'_>, ())>(self.terms.capacity())? > next {
                return Err(SkeinError::Execution(
                    "search identifier deduplication exceeded admitted capacity".into(),
                ));
            }
            if let Some(memory) = self.memory.as_mut() {
                memory.shrink(old);
            }
        }
        let emitted = text.materialize(control)?;
        self.terms.insert(text, ());
        Ok(Some(emitted))
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
