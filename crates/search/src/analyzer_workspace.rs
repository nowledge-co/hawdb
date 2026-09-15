//! Operation-owned opaque analyzer state; leases survive native TLS destruction.

use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, BuildMemory};
use crate::{Result, RuntimeTaskContext, SkeinError};
use skein_executor::QueryMemoryLease;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use std::thread::ScopedJoinHandle;

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
    retained: Mutex<Retained>,
    _header: QueryMemoryLease,
}

struct Retained {
    characters: usize,
    memory: QueryMemoryLease,
}

impl Workspace {
    fn new(memory: BuildMemory, task: RuntimeTaskContext) -> Result<Self> {
        checkpoint(&task)?;
        let header = memory
            .retained
            .reserve(checked_add(size_of::<Self>(), 2 * size_of::<usize>())?)?;
        let retained = memory
            .retained
            .reserve(required(bounds::regex_retained())?)?;
        Ok(Self {
            memory,
            task,
            retained: Mutex::new(Retained {
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
            if characters.is_multiple_of(1024) {
                checkpoint(&self.task)?;
            }
            characters = checked_add(characters, 1)?;
        }
        let transient = self
            .memory
            .spool
            .reserve(required(bounds::invocation(text.len(), characters))?)?;
        let mut retained = self
            .retained
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
    F: FnOnce(Arc<Workspace>) -> Result<T> + Send,
{
    checkpoint(task)?;
    // One owned worker is within either nonzero execution ceiling. The caller
    // waits for it; this does not create a second concurrently running operator.
    let thread_bytes = checked_add(
        checked_add(STACK_BYTES, THREAD_BOOKKEEPING_BYTES)?,
        checked_add(size_of::<F>(), size_of::<Result<T>>())?,
    )?;
    let thread_memory = memory.retained.reserve(thread_bytes)?;
    let workspace = Arc::new(Workspace::new(memory.clone(), task.clone())?);
    #[cfg(test)]
    let observation = entrypoint_tests::capture(memory);
    std::thread::scope(|scope| {
        let worker_workspace = Arc::clone(&workspace);
        let handle = std::thread::Builder::new()
            .stack_size(STACK_BYTES)
            .spawn_scoped(scope, move || {
                #[cfg(test)]
                entrypoint_tests::install(observation);
                worker_workspace.warm_up()?;
                work(worker_workspace)
            })
            .map_err(|error| {
                SkeinError::Execution(format!("search analyzer worker creation failed: {error}"))
            })?;
        let worker = JoinedWorker {
            handle: Some(handle),
            _workspace: workspace,
            _thread_memory: thread_memory,
        };
        match worker.join() {
            Ok(result) => result,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    })
}

struct JoinedWorker<'scope, T> {
    handle: Option<ScopedJoinHandle<'scope, T>>,
    _workspace: Arc<Workspace>,
    _thread_memory: QueryMemoryLease,
}

impl<T> JoinedWorker<'_, T> {
    fn join(mut self) -> std::thread::Result<T> {
        self.handle.take().expect("unjoined analyzer worker").join()
    }
}

impl<T> Drop for JoinedWorker<'_, T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            // Parent unwind must join before either capacity owner is dropped.
            // Preserve the parent's panic if the worker also panicked.
            let _ = handle.join();
        }
    }
}

fn required(bytes: Option<usize>) -> Result<usize> {
    bytes.ok_or_else(|| SkeinError::Execution("search analyzer capacity overflow".into()))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod entrypoint_tests;
