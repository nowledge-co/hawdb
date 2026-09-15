//! Generated artifact names retain their capacity through publication.

use crate::{build_control::checkpoint, build_memory::BuildMemory, Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
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
        if prefix.len() > 128 - 20 - ".skein".len() {
            return Err(SkeinError::Execution(
                "search artifact prefix exceeds preflight capacity".into(),
            ));
        }
        Self::formatted(|| format!("{prefix}{generation}.skein"), memory, task)
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
            return Err(SkeinError::Execution(
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
