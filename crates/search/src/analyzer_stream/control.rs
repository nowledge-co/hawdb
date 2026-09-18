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

use super::*;
use crate::build_memory::{checked_add, checked_mul, BuildMemory};
use crate::build_term::Term;
use crate::{HawDBError, RuntimeTaskContext};
use hawdb_executor::QueryMemoryLease;
use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::mem::size_of;

const CHECKPOINT_STRIDE: usize = 1024;

#[derive(Clone, Copy, Default)]
pub(crate) struct Control<'a> {
    pub(crate) memory: Option<&'a BuildMemory>,
    pub(crate) task: Option<&'a RuntimeTaskContext>,
    pub(crate) workspace: Option<&'a crate::analyzer_workspace::Workspace>,
    pub(crate) checkpoint_throttle: Option<&'a CheckpointThrottle>,
}

pub(crate) struct CheckpointThrottle {
    remaining: Cell<usize>,
}

impl CheckpointThrottle {
    pub(crate) fn new() -> Self {
        Self {
            remaining: Cell::new(0),
        }
    }

    fn check(&self, task: &RuntimeTaskContext) -> Result<()> {
        let remaining = self.remaining.get();
        if remaining == 0 {
            crate::build_control::checkpoint(task)?;
            self.remaining.set(CHECKPOINT_STRIDE - 1);
        } else {
            self.remaining.set(remaining - 1);
        }
        Ok(())
    }
}

impl<'a> Control<'a> {
    pub(super) fn with_checkpoint_throttle(self, throttle: &'a CheckpointThrottle) -> Self {
        Self {
            checkpoint_throttle: Some(throttle),
            ..self
        }
    }

    pub(super) fn check(self) -> Result<()> {
        let Some(task) = self.task else {
            return Ok(());
        };
        // Cancellation remains immediate at each tokenizer work point. The
        // deadline path retains bounded latency without calling Instant::now
        // for every token, suffix and alias.
        if task.cancellation().is_cancelled() {
            return crate::build_control::checkpoint(task);
        }
        self.checkpoint_throttle.map_or_else(
            || crate::build_control::checkpoint(task),
            |throttle| throttle.check(task),
        )
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

    pub(super) fn reset(&mut self) {
        self.terms.clear();
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
            HawDBError::Execution(format!("reserve search identifier deduplication: {error}"))
        })?;
        if table_bytes::<(Text<'_>, ())>(candidate.terms.capacity())? > bytes {
            return Err(HawDBError::Execution(
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
            .ok_or_else(|| HawDBError::Execution("search dedup capacity overflow".into()))?
    };
    checked_add(checked_mul(buckets, checked_add(size_of::<T>(), 1)?)?, 32)
}

#[cfg(test)]
mod tests;
