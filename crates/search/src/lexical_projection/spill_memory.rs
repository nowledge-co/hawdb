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

//! Physical spill ownership, separate from the existing logical build limits.

use super::*;
use crate::build_memory::reserved::{native_path, Grant, ReservedMemory};
use crate::build_memory::{checked_add as add, checked_mul as mul, grow_slots};

pub(super) const RUN_NAME_BYTES: usize = 80;

pub(super) fn path_bytes(root: &Path) -> Result<usize> {
    let parent = root.as_os_str().as_encoded_bytes().len();
    let verbatim = matches!(root.components().next(), Some(std::path::Component::Prefix(prefix)) if prefix.kind().is_verbatim());
    // Fixed-name formatting, native join growth, and the cleanup copy coexist.
    add(
        crate::build_memory::path::join_bytes(parent, RUN_NAME_BYTES, verbatim)?,
        add(parent, 4 * RUN_NAME_BYTES)?,
    )
}

impl SpillRuns {
    pub(super) fn with_context(
        root: &Path,
        generation: u64,
        config: LexicalProjectionConfig,
        memory: BuildMemory,
        task: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task)?;
        let root_memory = memory.retained.reserve(root.as_os_str().len())?;
        let progress = ReservedMemory::with_scratch_capacity(
            &memory.spool,
            0,
            native_path::child_bytes(root, RUN_NAME_BYTES)?,
        )?;
        let mut result =
            Self::new_with_control(root, generation, config, SpillControl::new(progress, task));
        result.path_slots = result.control.reserve(0)?;
        result._root_memory = Some(root_memory);
        result.prepare(0, 0)?;
        Ok(result)
    }

    pub(super) fn check(&self) -> Result<()> {
        self.control.check()
    }

    pub(super) fn prepare(&mut self, term: usize, id: usize) -> Result<()> {
        self.check()?;
        let Some(progress) = self.control.progress_for_growth() else {
            return Ok(());
        };
        let term = self.max_term_bytes.max(term);
        let id = self.max_id_bytes.max(id);
        if term == self.max_term_bytes
            && id == self.max_id_bytes
            && self.prepared_paths == Some(self.paths.len())
        {
            return Ok(());
        }
        let fan_in = self
            .config
            .max_merge_fan_in
            .get()
            .min(add(self.paths.len(), 1)?)
            .max(2);
        let frequency = add(
            mul(3, SPILL_IO_BUFFER_BYTES)?,
            mul(4, Term::reserved_bytes(term)?)?,
        )?;
        let corpus = add(
            mul(add(fan_in, 1)?, SPILL_IO_BUFFER_BYTES)?,
            add(
                mul(add(fan_in, 2)?, add(Term::reserved_bytes(term)?, id)?)?,
                mul(
                    fan_in,
                    add(
                        std::mem::size_of::<RunReader>(),
                        std::mem::size_of::<Reverse<(RunPosting, usize)>>(),
                    )?,
                )?,
            )?,
        )?;
        // Frequency binary carries retain at most one path per level. Corpus
        // compaction keeps a full source level and its destinations until unlink
        // succeeds. Include one future run and registry replacement overlap.
        let levels = (usize::BITS - self.config.max_spill_runs.get().leading_zeros()) as usize;
        let paths = add(mul(add(self.paths.len(), 1)?, 2)?, add(levels, 3)?)?;
        let registry = mul(
            mul(add(self.paths.len(), 2)?, 6)?,
            std::mem::size_of::<RemoveOnDrop>(),
        )?;
        progress.ensure_capacity(add(
            frequency.max(corpus),
            add(mul(paths, path_bytes(&self.root)?)?, registry)?,
        )?)?;
        self.max_term_bytes = term;
        self.max_id_bytes = id;
        self.prepared_paths = Some(self.paths.len());
        Ok(())
    }

    pub(super) fn next_guard(&mut self) -> Result<RemoveOnDrop> {
        // Ingestion preadmitted a full merge level plus its output paths. Do not
        // grow that reservation merely because a level temporarily retains both.
        self.check()?;
        let memory = self.control.reserve(path_bytes(&self.root)?)?;
        let path = self.next_path()?;
        Ok(RemoveOnDrop {
            path,
            armed: true,
            _memory: memory,
        })
    }

    pub(super) fn register(&mut self, mut guard: RemoveOnDrop) -> Result<()> {
        if self.paths.len() == self.paths.capacity() {
            let capacity = mul(self.paths.capacity().max(1), 2)?;
            reserve_slots(&mut self.paths, capacity, self.path_slots.as_mut())?;
        }
        if let Some(memory) = &mut guard._memory {
            if guard.path.capacity() > memory.bytes() {
                return Err(HawDBError::Execution(
                    "search spill path allocation exceeded its admitted capacity".into(),
                ));
            }
            memory.shrink(memory.bytes() - guard.path.capacity());
        }
        self.paths.push(guard);
        Ok(())
    }
}

pub(super) fn reserve_slots<T>(
    values: &mut Vec<T>,
    capacity: usize,
    memory: Option<&mut Grant>,
) -> Result<()> {
    crate::build_memory::capacity::reserve(
        values,
        capacity,
        crate::build_memory::capacity::Memory::Grant(memory),
        "search spill slots",
    )
}

pub(super) struct PendingPostings {
    pub(super) values: Vec<Posting>,
    pub(super) bytes: u64,
    slots: Option<QueryMemoryLease>,
    strings: Option<QueryMemoryLease>,
}

impl PendingPostings {
    pub(super) fn new(memory: Option<&BuildMemory>) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            bytes: 0,
            slots: memory
                .map(|memory| memory.retained.reserve(0))
                .transpose()?,
            strings: memory
                .map(|memory| memory.retained.reserve(0))
                .transpose()?,
        })
    }

    pub(super) fn push(&mut self, term: Term, id: &str, frequency: u32, length: u32) -> Result<()> {
        if let Some(slots) = &mut self.slots {
            grow_slots(&mut self.values, slots)?;
        }
        if let Some(strings) = &mut self.strings {
            strings.grow(id.len())?;
        }
        self.bytes = self
            .bytes
            .saturating_add(Posting::resident_bytes(&term, id));
        self.values.push(Posting {
            term,
            document_id: id.to_owned(),
            term_frequency: frequency,
            document_len: length,
        });
        Ok(())
    }

    pub(super) fn flush(&mut self, pool: &mut SpillRuns) -> Result<()> {
        if !self.values.is_empty() {
            pool.spill(&mut self.values)?;
        }
        self.values = Vec::new();
        self.bytes = 0;
        if let Some(slots) = &mut self.slots {
            slots.reset();
        }
        if let Some(strings) = &mut self.strings {
            strings.reset();
        }
        Ok(())
    }
}

/// The decoded ID dies before its grant. Terms retain their own shared grant.
#[derive(Debug)]
pub(super) struct RunPosting {
    pub(super) posting: Posting,
    pub(super) _id_memory: Option<Grant>,
}

impl std::ops::Deref for RunPosting {
    type Target = Posting;
    fn deref(&self) -> &Posting {
        &self.posting
    }
}
impl PartialEq for RunPosting {
    fn eq(&self, other: &Self) -> bool {
        self.posting == other.posting
    }
}
impl Eq for RunPosting {}
impl PartialOrd for RunPosting {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RunPosting {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.posting.cmp(&other.posting)
    }
}

pub(super) fn read_text(
    reader: &mut impl Read,
    length: usize,
    control: &SpillControl,
) -> Result<String> {
    // Both readers check at record entry. Only large fields need another check
    // before their allocation; their subsequent I/O remains bounded to 8 KiB.
    if length > SPILL_IO_BUFFER_BYTES {
        control.check()?;
    }
    let mut bytes = vec![0; length];
    for (index, chunk) in bytes.chunks_mut(SPILL_IO_BUFFER_BYTES).enumerate() {
        if index != 0 {
            control.check()?;
        }
        reader.read_exact(chunk)?;
    }
    String::from_utf8(bytes)
        .map_err(|error| HawDBError::Storage(format!("invalid lexical spill utf-8: {error}")))
}

impl AsRef<Path> for RemoveOnDrop {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests;
