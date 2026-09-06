//! Private artifact-builder names and native paths on the generation root.

use super::context_memory::OwnedPath;
use super::{LEXICAL_MANIFEST_FILE, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::{Result, SkeinError, SEARCH_SEGMENT_DESCRIPTOR_FILE, SEARCH_SEGMENT_PAYLOAD_FILE};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::path::Path;

#[derive(Debug)]
pub(super) struct Name {
    value: String,
    _memory: QueryMemoryLease,
}

impl Name {
    pub(super) fn lexical(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::new(generation, super::lexical_artifact_file, memory, task)
    }

    #[cfg(any(test, feature = "vector-search"))]
    pub(super) fn rabitq(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        Self::new(generation, crate::rabitq_artifact_file, memory, task)
    }

    fn new(
        generation: u64,
        format: fn(u64) -> String,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // The fixed formats and a full-width u64 fit 128 bytes. Include
        // overlapping formatter buffers before allocating the original name.
        let lease = memory.retained.reserve(3 * 128)?;
        #[cfg(test)]
        tests::record_name();
        let mut name = Self {
            value: format(generation),
            _memory: lease,
        };
        checkpoint(task)?;
        if name.value.capacity() > 128 {
            return Err(SkeinError::Execution(
                "search artifact name exceeds preflight capacity".into(),
            ));
        }
        name._memory
            .shrink(name._memory.bytes() - name.value.capacity());
        Ok(name)
    }
}

impl std::ops::Deref for Name {
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

pub(super) struct Segment {
    pub(super) document: OwnedPath,
    pub(super) metadata: OwnedPath,
    pub(super) vector: OwnedPath,
    pub(super) descriptor: OwnedPath,
    pub(super) temporary: OwnedPath,
}

impl Segment {
    pub(super) fn new(
        stage: &Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let document =
            OwnedPath::join(stage, Path::new(SEARCH_SEGMENT_PAYLOAD_FILE), memory, task)?;
        let metadata = OwnedPath::join(stage, Path::new(STAGE_METADATA_FILE), memory, task)?;
        let vector = OwnedPath::join(stage, Path::new(STAGE_VECTOR_FILE), memory, task)?;
        let descriptor = OwnedPath::join(
            stage,
            Path::new(SEARCH_SEGMENT_DESCRIPTOR_FILE),
            memory,
            task,
        )?;
        let temporary = OwnedPath::with_extension(&descriptor, "skein.tmp", memory, task)?;
        Ok(Self {
            document,
            metadata,
            vector,
            descriptor,
            temporary,
        })
    }
}

pub(super) struct Lexical {
    pub(super) name: Name,
    pub(super) artifact: OwnedPath,
    pub(super) manifest: OwnedPath,
}

impl Lexical {
    pub(super) fn new(
        stage: &Path,
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let name = Name::lexical(generation, memory, task)?;
        let artifact = OwnedPath::join(stage, name.as_ref(), memory, task)?;
        let manifest = OwnedPath::join(stage, Path::new(LEXICAL_MANIFEST_FILE), memory, task)?;
        Ok(Self {
            name,
            artifact,
            manifest,
        })
    }
}

#[cfg(test)]
mod tests;
