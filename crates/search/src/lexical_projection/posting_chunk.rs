use super::{Posting, SpillRuns};
use crate::build_memory::{checked_mul, BuildMemory};
use crate::error::{Result, SkeinError};
use skein_executor::QueryMemoryLease;
use std::mem::size_of;

pub(super) struct PostingChunk {
    postings: Vec<Posting>,
    pub(super) bytes: u64,
    memory: BuildMemory,
    slots: QueryMemoryLease,
    terms: QueryMemoryLease,
}

impl PostingChunk {
    pub(super) fn new(memory: BuildMemory) -> Result<Self> {
        Ok(Self {
            postings: Vec::new(),
            bytes: 0,
            slots: memory.retained.reserve(0)?,
            terms: memory.retained.reserve(0)?,
            memory,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    pub(super) fn push(&mut self, posting: Posting) -> Result<()> {
        if self.postings.len() == self.postings.capacity() {
            let capacity = checked_mul(self.postings.capacity().max(2), 2)?;
            // The old allocation remains charged during replacement allocation.
            let next_slots = self
                .memory
                .retained
                .reserve(checked_mul(capacity, size_of::<Posting>())?)?;
            self.postings
                .try_reserve_exact(capacity - self.postings.len())
                .map_err(|error| {
                    SkeinError::Execution(format!(
                        "lexical posting chunk allocation failed: {error}"
                    ))
                })?;
            self.slots = next_slots;
        }
        self.terms.grow(posting.term.capacity())?;
        self.bytes = self
            .bytes
            .checked_add(posting.resident_bytes())
            .ok_or_else(|| {
                SkeinError::Execution("lexical posting chunk byte count overflow".to_string())
            })?;
        self.postings.push(posting);
        Ok(())
    }

    pub(super) fn spill(&mut self, runs: &mut SpillRuns) -> Result<()> {
        runs.spill(&mut self.postings)?;
        debug_assert!(self.postings.is_empty());
        self.terms.shrink(self.terms.bytes());
        self.bytes = 0;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn retained_bytes(&self) -> usize {
        self.slots.bytes() + self.terms.bytes()
    }
}
