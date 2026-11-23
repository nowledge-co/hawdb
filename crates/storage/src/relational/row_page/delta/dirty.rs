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

//! Bounded sorted dirty entries with admitted backing and explicit COW.

use super::{RelationalRowDeltaError, RowDeltaKey, RowDeltaValue};
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointWorkContext,
};
use std::sync::Arc;

type Entry = (RowDeltaKey, RowDeltaValue);

#[derive(Debug)]
struct Backing {
    entries: Vec<Entry>,
    _memory: CheckpointAllocationOwner,
}

#[derive(Debug, Clone, Default)]
pub(super) struct DirtyEntries {
    backing: Option<Arc<Backing>>,
}

impl DirtyEntries {
    fn as_slice(&self) -> &[Entry] {
        self.backing
            .as_ref()
            .map_or(&[], |backing| &backing.entries)
    }
    pub(super) fn len(&self) -> usize {
        self.as_slice().len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
    pub(super) fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&RowDeltaKey, &RowDeltaValue)> + ExactSizeIterator {
        self.as_slice().iter().map(|(key, value)| (key, value))
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &RowDeltaValue> {
        self.as_slice().iter().map(|(_, value)| value)
    }
    pub(super) fn first_key_value(&self) -> Option<(&RowDeltaKey, &RowDeltaValue)> {
        self.as_slice().first().map(|(key, value)| (key, value))
    }
    pub(super) fn last_key_value(&self) -> Option<(&RowDeltaKey, &RowDeltaValue)> {
        self.as_slice().last().map(|(key, value)| (key, value))
    }
    fn position(&self, key: &RowDeltaKey) -> Result<usize, usize> {
        self.as_slice()
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
    }
    pub(super) fn get(&self, key: &RowDeltaKey) -> Option<&RowDeltaValue> {
        self.position(key)
            .ok()
            .map(|position| &self.as_slice()[position].1)
    }
    pub(super) fn clear(&mut self) {
        self.backing = None;
    }

    fn ensure_capacity(
        &mut self,
        needed: usize,
        work: &CheckpointWorkContext,
    ) -> Result<(), RelationalRowDeltaError> {
        if self.backing.as_ref().is_some_and(|backing| {
            Arc::strong_count(backing) == 1 && backing.entries.capacity() >= needed
        }) {
            return Ok(());
        }
        let old_capacity = self
            .backing
            .as_ref()
            .map_or(0, |backing| backing.entries.capacity());
        let capacity = if old_capacity >= needed {
            old_capacity
        } else {
            old_capacity.saturating_mul(2).max(needed).max(1)
        };
        let allocations = CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut entries = allocations
            .vector_capacity(capacity)
            .map_err(RelationalRowDeltaError::from_admission)?;
        allocations
            .reserve(std::mem::size_of::<Backing>() + 2 * std::mem::size_of::<usize>())
            .map_err(RelationalRowDeltaError::from_admission)?;
        for entry in self.as_slice() {
            let unit = work
                .start_unit()
                .map_err(RelationalRowDeltaError::from_work)?;
            entries.push(entry.clone());
            unit.finish();
        }
        work.checkpoint()
            .map_err(RelationalRowDeltaError::from_work)?;
        let replacement = Arc::new(Backing {
            entries,
            _memory: allocations.memory.into_inner(),
        });
        self.backing = Some(replacement);
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        key: RowDeltaKey,
        mut value: RowDeltaValue,
        work: &CheckpointWorkContext,
    ) -> Result<Option<RowDeltaValue>, RelationalRowDeltaError> {
        let position = self.position(&key);
        let needed = self
            .len()
            .checked_add(usize::from(position.is_err()))
            .ok_or_else(|| {
                RelationalRowDeltaError::Admission("dirty entry capacity overflow".into())
            })?;
        self.ensure_capacity(needed, work)?;
        let entries = &mut Arc::get_mut(self.backing.as_mut().expect("admitted dirty backing"))
            .expect("private dirty backing")
            .entries;
        match position {
            Ok(position) => {
                let existing = &mut entries[position].1;
                let incoming_key_memory = std::mem::replace(
                    &mut value._key_memory,
                    std::mem::take(&mut existing._key_memory),
                );
                drop(key);
                drop(incoming_key_memory);
                Ok(Some(std::mem::replace(existing, value)))
            }
            Err(position) => {
                entries.push((key, value));
                for next in (position..entries.len() - 1).rev() {
                    let unit = match work.start_unit() {
                        Ok(unit) => unit,
                        Err(error) => {
                            // Undo completed moves without further admission: a denied
                            // insertion must leave the existing sorted entries intact.
                            for moved in next + 1..entries.len() - 1 {
                                entries.swap(moved, moved + 1);
                            }
                            entries.pop();
                            return Err(RelationalRowDeltaError::from_work(error));
                        }
                    };
                    entries.swap(next, next + 1);
                    unit.finish();
                }
                Ok(None)
            }
        }
    }
}
