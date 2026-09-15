//! Generated artifact names retain their capacity through publication.

#[cfg(feature = "vector-search")]
use crate::{build_control::checkpoint, build_memory::BuildMemory, Result, SkeinError};
#[cfg(feature = "vector-search")]
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::ops::Deref;
use std::path::Path;

#[derive(Debug)]
pub(in super::super) struct Name {
    value: String,
    _memory: QueryMemoryLease,
}

#[cfg(feature = "vector-search")]
impl Name {
    pub(super) fn rabitq(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // The fixed file grammar and full-width u64 fit 128 bytes. Admit
        // formatter growth before allocation, then retain the actual capacity.
        let lease = memory.retained.reserve(3 * 128)?;
        let mut name = Self {
            value: crate::rabitq_artifact_file(generation),
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
