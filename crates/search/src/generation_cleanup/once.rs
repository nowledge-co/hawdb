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

//! One writer needs cleanup counts, not an immediately discarded retry queue.

use super::{CleanupCandidate, SearchProjectionCleanupOptions, SearchProjectionGenerations};
use crate::build_control::checkpoint;
use crate::build_memory::{directory, BuildMemory};
use crate::Result;
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::path::Path;
use std::{fs, io};

pub(crate) struct PreparedCleanup {
    // Reserve before publication; the complete synchronous pass uses no fresh
    // admission even if another owner fills the root after the commit fence.
    memory: Option<QueryMemoryLease>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct CleanupResult {
    pub(crate) deleted_files: usize,
    pub(crate) pending_files: usize,
    pub(crate) retry_required: bool,
}

impl PreparedCleanup {
    pub(crate) fn prepare(
        root: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        #[cfg(test)]
        evidence::run(evidence::Point::BeforeAdmission, memory);
        let bytes = directory::scan_bytes(root)?;
        // Cleanup is optional. A denied pass is observable as a scan requiring
        // retry; it must not convert an otherwise successful commit into failure.
        Ok(Self {
            memory: memory.spool.reserve(bytes).ok(),
        })
    }

    pub(crate) fn run(
        self,
        root: &Path,
        generations: SearchProjectionGenerations,
        options: SearchProjectionCleanupOptions,
        task: &RuntimeTaskContext,
    ) -> CleanupResult {
        self.run_with_remover(root, generations, options, task, |path| {
            fs::remove_file(path)
        })
    }

    fn run_with_remover(
        self,
        root: &Path,
        generations: SearchProjectionGenerations,
        options: SearchProjectionCleanupOptions,
        task: &RuntimeTaskContext,
        mut remove: impl FnMut(&Path) -> io::Result<()>,
    ) -> CleanupResult {
        let mut result = CleanupResult {
            retry_required: generations.out_of_core_discovery_failed,
            ..Default::default()
        };
        if self.memory.is_none() || task.checkpoint().is_err() {
            result.retry_required = true;
            return result;
        }
        let mut entries = match fs::read_dir(root) {
            Ok(entries) => entries,
            Err(_) => {
                result.retry_required = true;
                return result;
            }
        };
        let mut attempts = 0;
        loop {
            if task.checkpoint().is_err() {
                result.retry_required = true;
                break;
            }
            let Some(entry) = entries.next() else {
                break;
            };
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    result.retry_required = true;
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(candidate) = CleanupCandidate::parse(name) else {
                continue;
            };
            if !candidate.is_obsolete(generations) {
                continue;
            }
            if attempts == options.max_delete_attempts.get() {
                result.defer(options);
                continue;
            }
            attempts += 1;
            let path = root.join(name);
            match remove(&path) {
                Ok(()) => result.deleted_files = result.deleted_files.saturating_add(1),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => result.defer(options),
            }
        }
        // A cancellation arriving during the last successful unlink still
        // reports a retry, without undoing its completed deletion count.
        if task.checkpoint().is_err() {
            result.retry_required = true;
        }
        result
    }
}

impl CleanupResult {
    fn defer(&mut self, options: SearchProjectionCleanupOptions) {
        self.pending_files = self
            .pending_files
            .saturating_add(1)
            .min(options.max_pending_files.get());
        self.retry_required = true;
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod evidence {
    use crate::build_memory::BuildMemory;
    use std::cell::RefCell;
    type Callback = Box<dyn FnOnce(&BuildMemory)>;
    #[derive(Clone, Copy)]
    pub(crate) enum Point {
        BeforeAdmission,
        AfterCommit,
    }
    thread_local! { static CALLBACKS: RefCell<[Option<Callback>; 2]> = RefCell::new([None, None]); }
    pub(crate) struct Guard {
        point: Point,
        previous: Option<Callback>,
    }
    impl Drop for Guard {
        fn drop(&mut self) {
            CALLBACKS.with(|slots| slots.borrow_mut()[self.point as usize] = self.previous.take());
        }
    }
    pub(crate) fn at(point: Point, callback: impl FnOnce(&BuildMemory) + 'static) -> Guard {
        let previous =
            CALLBACKS.with(|slots| slots.borrow_mut()[point as usize].replace(Box::new(callback)));
        Guard { point, previous }
    }
    pub(crate) fn run(point: Point, memory: &BuildMemory) {
        let callback = CALLBACKS.with(|slots| slots.borrow_mut()[point as usize].take());
        if let Some(callback) = callback {
            callback(memory);
        }
    }
}
