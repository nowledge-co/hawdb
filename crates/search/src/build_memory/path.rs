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

//! Native build paths retain their admission for the full allocation lifetime.

use super::{checked_add as add, checked_mul as mul, BuildMemory};
use crate::build_control::checkpoint;
use crate::{HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::mem::size_of;
use std::ops::Deref;
use std::path::{Component, Path, PathBuf};

#[derive(Debug)]
pub(crate) struct OwnedPath {
    value: PathBuf,
    _memory: QueryMemoryLease,
}

impl OwnedPath {
    pub(crate) fn retained_bytes(&self) -> usize {
        self._memory.bytes()
    }

    pub(crate) fn canonicalize(
        path: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::resolve(path, memory, task, |path| {
            hawdb_storage::file_io::canonicalize(path)
        })
    }

    pub(crate) fn absolute(
        path: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::resolve(path, memory, task, |path| std::path::absolute(path))
    }

    fn resolve(
        path: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        resolve: impl FnOnce(&Path) -> std::io::Result<PathBuf>,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory.retained.reserve(Self::resolution_bytes(path)?)?;
        let value = resolve(path)?;
        Self::finish(value, lease, task)
    }

    pub(crate) fn resolution_bytes(path: &Path) -> Result<usize> {
        // Cover the native extended Windows path representation, its UTF-8
        // result and overlapping conversion buffers before resolving aliases.
        add(4 * 32_768 * 3, super::reserved::native_path::bytes(path)?)
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.value.capacity()
    }

    pub(crate) fn copy(
        path: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let bytes = path.as_os_str().as_encoded_bytes().len();
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        evidence::record();
        Self::finish(path.to_path_buf(), lease, task)
    }

    pub(crate) fn join(
        parent: &Path,
        name: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::join_with_task(parent, name, memory, Some(task))
    }

    fn join_with_task(
        parent: &Path,
        name: &Path,
        memory: &BuildMemory,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        task.map_or(Ok(()), checkpoint)?;
        let verbatim = matches!(parent.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim());
        let bytes = join_bytes(
            parent.as_os_str().as_encoded_bytes().len(),
            name.as_os_str().as_encoded_bytes().len(),
            verbatim,
        )?;
        let lease = memory.retained.reserve(bytes)?;
        #[cfg(test)]
        evidence::record();
        Self::finish_with_task(parent.join(name), lease, task)
    }

    fn finish(value: PathBuf, lease: QueryMemoryLease, task: &RuntimeTaskContext) -> Result<Self> {
        Self::finish_with_task(value, lease, Some(task))
    }

    pub(crate) fn with_extension(
        path: &Path,
        extension: &str,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // Path::_with_extension reserves the result up front. Include the old
        // extension in this bound while preserving native path semantics.
        let bytes = add(
            path.as_os_str().as_encoded_bytes().len(),
            add(extension.len(), 1)?,
        )?;
        let lease = memory.retained.reserve(bytes)?;
        Self::finish(path.with_extension(extension), lease, task)
    }

    fn finish_with_task(
        value: PathBuf,
        lease: QueryMemoryLease,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        let mut owned = Self {
            value,
            _memory: lease,
        };
        task.map_or(Ok(()), checkpoint)?;
        if owned.value.capacity() > owned._memory.bytes() {
            return Err(HawDBError::Execution(
                "search writer path exceeds preflight capacity".into(),
            ));
        }
        owned
            ._memory
            .shrink(owned._memory.bytes() - owned.value.capacity());
        Ok(owned)
    }
}

impl Deref for OwnedPath {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl AsRef<Path> for OwnedPath {
    fn as_ref(&self) -> &Path {
        &self.value
    }
}

pub(crate) fn join_bytes(parent: usize, name: usize, verbatim: bool) -> Result<usize> {
    let length = add(parent, add(name, 1)?)?;
    // Rust 1.97.1 PathBuf::_push uses amortized OsString growth. Verbatim
    // prefixes additionally collect components and reconstruct another buffer.
    // Preserve the standard library's platform-specific normalization semantics.
    if verbatim {
        add(
            retained_join_bytes(parent, name, verbatim)?,
            mul(mul(length.max(4), 4)?, size_of::<Component<'_>>())?,
        )
    } else {
        mul(length.max(8), 3)
    }
}

/// Capacity retained by a joined path after temporary normalization is freed.
pub(crate) fn retained_join_bytes(parent: usize, name: usize, verbatim: bool) -> Result<usize> {
    let length = add(parent, add(name, 1)?)?;
    if verbatim {
        mul(length.max(8), 4)
    } else {
        // Appending the separator and name can grow the root buffer twice.
        Ok(length.max(mul(parent, 4)?).max(16))
    }
}

#[cfg(test)]
pub(crate) mod evidence {
    use hawdb_core::RuntimeCancellationToken;
    use std::cell::{Cell, RefCell};

    thread_local! {
        static PATHS: Cell<usize> = const { Cell::new(0) };
        static CANCEL_ARMED: Cell<bool> = const { Cell::new(false) };
        static CANCEL: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
    }

    pub(super) fn record() {
        PATHS.set(PATHS.get() + 1);
        // Keep inactive fault injection from initializing droppable TLS during
        // allocation measurements. Its native destructor bookkeeping outlives
        // the measured path and is absent from production builds.
        if CANCEL_ARMED.replace(false) {
            CANCEL.with_borrow_mut(|value| {
                if let Some(token) = value.take() {
                    token.cancel();
                }
            });
        }
    }

    pub(crate) fn take() -> usize {
        PATHS.replace(0)
    }

    pub(crate) fn cancel_next(token: RuntimeCancellationToken) {
        CANCEL.with_borrow_mut(|value| *value = Some(token));
        CANCEL_ARMED.set(true);
    }
}
