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

//! Operation-owned opaque analyzer state; leases survive native TLS destruction.

use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, BuildMemory};
use crate::{HawDBError, Result, RuntimeTaskContext};
use hawdb_executor::QueryMemoryLease;
use std::cell::RefCell;
use std::mem::size_of;

mod bounds;

const STACK_BYTES: usize = 2 * 1024 * 1024;
// Fixed std thread/packet/parking bookkeeping, separate from the captured closure
// and result sizes. This allowance does not model OS metadata or process RSS.
const THREAD_BOOKKEEPING_BYTES: usize = 4096;
const WARMUP: &str = "\u{9f98}\u{9750}";

pub(crate) fn document_needs_workspace(document: &crate::SearchDocument) -> bool {
    crate::analyzer_stream::document_token_fields(document)
        .any(|(text, _)| text.chars().any(crate::cjk_tokenizer::is_han_search_char))
}

pub(crate) struct Workspace {
    memory: BuildMemory,
    task: RuntimeTaskContext,
    retained: RefCell<Retained>,
    _header: QueryMemoryLease,
}

struct Retained {
    characters: usize,
    memory: QueryMemoryLease,
}

impl Workspace {
    fn new(memory: BuildMemory, task: RuntimeTaskContext) -> Result<Self> {
        checkpoint(&task)?;
        let header = memory.retained.reserve(size_of::<Self>())?;
        let retained = memory
            .retained
            .reserve(required(bounds::regex_retained())?)?;
        Ok(Self {
            memory,
            task,
            retained: RefCell::new(Retained {
                characters: 0,
                memory: retained,
            }),
            _header: header,
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        checkpoint(&self.task)
    }

    pub(crate) fn admit(&self, text: &str) -> Result<QueryMemoryLease> {
        checkpoint(&self.task)?;
        let mut characters = 0usize;
        for _ in text.chars() {
            characters = checked_add(characters, 1)?;
            if characters.is_multiple_of(1024) {
                checkpoint(&self.task)?;
            }
        }
        let transient = self
            .memory
            .spool
            .reserve(required(bounds::invocation(text.len(), characters))?)?;
        let mut retained = self.retained.borrow_mut();
        let next_characters = retained.characters.max(characters);
        let next_bytes = checked_add(
            required(bounds::regex_retained())?,
            required(bounds::hmm_retained(next_characters))?,
        )?;
        let growth = next_bytes.saturating_sub(retained.memory.bytes());
        retained.memory.grow(growth)?;
        retained.characters = next_characters;
        checkpoint(&self.task)?;
        Ok(transient)
    }

    fn warm_up(&self) -> Result<()> {
        checkpoint(&self.task)?;
        let _constructor = self
            .memory
            .spool
            .reserve(required(bounds::regex_construction())?)?;
        let _scratch = self.admit(WARMUP)?;
        crate::cjk_tokenizer::prime_workspace(WARMUP)?;
        checkpoint(&self.task)
    }
}

pub(crate) fn run<T, F>(memory: &BuildMemory, task: &RuntimeTaskContext, work: F) -> Result<T>
where
    T: Send,
    F: FnOnce(&Workspace) -> Result<T> + Send,
{
    checkpoint(task)?;
    // One owned worker is within either nonzero execution ceiling. The caller
    // waits for it; this does not create a second concurrently running operator.
    let thread_bytes = checked_add(
        checked_add(STACK_BYTES, THREAD_BOOKKEEPING_BYTES)?,
        checked_add(size_of::<F>(), size_of::<Result<T>>())?,
    )?;
    let _thread_memory = memory.retained.reserve(thread_bytes)?;
    let mut workspace = Workspace::new(memory.clone(), task.clone())?;
    #[cfg(test)]
    let observation = entrypoint_tests::capture(memory);
    #[cfg(test)]
    let read_evidence = crate::out_of_core::analyzer_read_evidence::capture();
    std::thread::scope(|scope| {
        // Only the worker may access the workspace. The parent owns both leases
        // until native join completes, including when the worker panics.
        let worker_workspace = &mut workspace;
        let handle = std::thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn_scoped(scope, move || {
                #[cfg(test)]
                entrypoint_tests::install(observation);
                #[cfg(test)]
                let _read_evidence = read_evidence.install();
                worker_workspace.warm_up()?;
                work(worker_workspace)
            })
            .map_err(|error| {
                HawDBError::Execution(format!("search analyzer worker creation failed: {error}"))
            })?;
        match handle.join() {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    })
}

fn required(bytes: Option<usize>) -> Result<usize> {
    bytes.ok_or_else(|| HawDBError::Execution("search analyzer capacity overflow".into()))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod entrypoint_tests;
