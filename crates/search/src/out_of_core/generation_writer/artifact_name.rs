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

//! Generated artifact names retain their capacity through publication.

use crate::{build_control::checkpoint, build_memory::BuildMemory, HawDBError, Result};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::ops::Deref;
use std::path::Path;

#[derive(Debug)]
pub(in super::super) struct Name {
    value: String,
    _memory: QueryMemoryLease,
}

impl Name {
    #[cfg(feature = "vector-search")]
    pub(super) fn rabitq(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::generated("search_rabitq.", generation, memory, task)
    }

    pub(super) fn generated(
        prefix: &'static str,
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        if prefix.len() > 128 - 20 - ".hawdb".len() {
            return Err(HawDBError::Execution(
                "search artifact prefix exceeds preflight capacity".into(),
            ));
        }
        Self::formatted(|| format!("{prefix}{generation}.hawdb"), memory, task)
    }

    pub(super) fn temporary_extension(
        sequence: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::formatted(
            || format!("tmp.{}.{}", std::process::id(), sequence),
            memory,
            task,
        )
    }

    fn formatted(
        format: impl FnOnce() -> String,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // The fixed file grammar and full-width u64 fit 128 bytes. Admit
        // formatter growth before allocation, then retain the actual capacity.
        let lease = memory.retained.reserve(3 * 128)?;
        let mut name = Self {
            value: format(),
            _memory: lease,
        };
        checkpoint(task)?;
        if name.value.capacity() > 128 {
            return Err(HawDBError::Execution(
                "search artifact name exceeded preflight capacity".into(),
            ));
        }
        name._memory
            .shrink(name._memory.bytes() - name.value.capacity());
        Ok(name)
    }
}

impl Deref for Name {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl AsRef<Path> for Name {
    fn as_ref(&self) -> &Path {
        Path::new(&self.value)
    }
}
