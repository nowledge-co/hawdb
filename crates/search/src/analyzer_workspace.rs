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

use crate::analyzer_memory::Memory;
use crate::build_memory::{checked_add, BuildMemory};
use crate::{HawDBError, Result, RuntimeTaskContext};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use std::cell::RefCell;
use std::mem::size_of;

pub(crate) mod bounds;

mod stack;
use stack::STACK_BYTES;
// Fixed std thread/packet/parking bookkeeping, separate from the captured closure
// and result sizes. This allowance does not model OS metadata or process RSS.
const THREAD_BOOKKEEPING_BYTES: usize = 4096;
const WARMUP: &str = "\u{9f98}\u{9750}";

pub(crate) fn document_needs_workspace(document: &crate::SearchDocument) -> bool {
    crate::analyzer_stream::document_token_fields(document)
        .any(|(text, _)| text.chars().any(crate::cjk_tokenizer::is_han_search_char))
}

pub(crate) struct Workspace {
    memory: OwnedMemory,
    task: RuntimeTaskContext,
    retained: RefCell<Retained>,
    _header: QueryMemoryLease,
}

enum OwnedMemory {
    Build(BuildMemory),
    Query(QueryMemoryAccount),
}

impl OwnedMemory {
    fn view(&self) -> Memory<'_> {
        match self {
            Self::Build(memory) => Memory::Build(memory),
            Self::Query(memory) => Memory::Query(memory),
        }
    }
}

struct Retained {
    characters: usize,
    memory: QueryMemoryLease,
}

impl Workspace {
    fn new(memory: OwnedMemory, task: RuntimeTaskContext) -> Result<Self> {
        memory.view().checkpoint(&task)?;
        let header = memory.view().retained().reserve(size_of::<Self>())?;
        let retained = memory
            .view()
            .retained()
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
        self.memory.view().checkpoint(&self.task)
    }

    pub(crate) fn admit(&self, text: &str) -> Result<QueryMemoryLease> {
        self.checkpoint()?;
        let mut characters = 0usize;
        for _ in text.chars() {
            characters = checked_add(characters, 1)?;
            if characters.is_multiple_of(1024) {
                self.checkpoint()?;
            }
        }
        let transient = self
            .memory
            .view()
            .spool()
            .reserve(required(bounds::invocation(text.len(), characters))?)?;
        let mut retained = self.retained.borrow_mut();
        let next_characters = retained.characters.max(characters);
        let next_bytes = checked_add(
            required(bounds::regex_retained())?,
            required(bounds::scratch_retained(next_characters))?,
        )?;
        let growth = next_bytes.saturating_sub(retained.memory.bytes());
        retained.memory.grow(growth)?;
        retained.characters = next_characters;
        self.checkpoint()?;
        Ok(transient)
    }

    fn warm_up(&self) -> Result<()> {
        self.checkpoint()?;
        let _constructor = self
            .memory
            .view()
            .spool()
            .reserve(required(bounds::regex_construction())?)?;
        let _scratch = self.admit(WARMUP)?;
        crate::cjk_tokenizer::prime_workspace(WARMUP)?;
        self.checkpoint()
    }
}

pub(crate) fn run<T, F>(memory: &BuildMemory, task: &RuntimeTaskContext, work: F) -> Result<T>
where
    T: Send,
    F: FnOnce(&Workspace) -> Result<T> + Send,
{
    run_owned(OwnedMemory::Build(memory.clone()), task, work)
}

pub(crate) fn run_query<T, F>(
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
    work: F,
) -> Result<T>
where
    T: Send,
    F: FnOnce(&Workspace) -> Result<T> + Send,
{
    let output = run_owned(OwnedMemory::Query(memory.clone()), task, work)?;
    // Native TLS destruction can itself request cancellation. Own returned
    // payloads before the final check, after joining and releasing workspace.
    crate::query_control::checkpoint(task)?;
    Ok(output)
}

fn run_owned<T, F>(memory: OwnedMemory, task: &RuntimeTaskContext, work: F) -> Result<T>
where
    T: Send,
    F: FnOnce(&Workspace) -> Result<T> + Send,
{
    memory.view().checkpoint(task)?;
    // One owned worker is within either nonzero execution ceiling. The caller
    // waits for it; this does not create a second concurrently running operator.
    let thread_bytes = checked_add(
        checked_add(STACK_BYTES, THREAD_BOOKKEEPING_BYTES)?,
        checked_add(size_of::<F>(), size_of::<Result<T>>())?,
    )?;
    let _thread_memory = memory.view().retained().reserve(thread_bytes)?;
    let mut workspace = Workspace::new(memory, task.clone())?;
    #[cfg(test)]
    let observation = match &workspace.memory {
        OwnedMemory::Build(memory) => observation::capture(&memory.ledger),
        OwnedMemory::Query(_) => None,
    };
    #[cfg(test)]
    let read_evidence = matches!(workspace.memory, OwnedMemory::Build(_))
        .then(crate::build_control::read_observation::capture);
    std::thread::scope(|scope| {
        // Only the worker may access the workspace. The parent owns both leases
        // until native join completes, including when the worker panics.
        let worker_workspace = &mut workspace;
        let handle = std::thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn_scoped(scope, move || {
                #[cfg(test)]
                observation::install(observation);
                #[cfg(test)]
                let _read_evidence = read_evidence.map(|evidence| evidence.install());
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

#[cfg(test)]
mod observation;
