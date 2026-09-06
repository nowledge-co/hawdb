//! Complete path ownership prepared before the manifest-last commit gate.

use super::{PublishGenerationInput, LEXICAL_MANIFEST_FILE};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, BuildMemory};
use crate::out_of_core::generation_writer::context_memory::OwnedPath;
use crate::out_of_core::generation_writer::{STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::out_of_core::{
    publish_generation_link_at, write_generation_artifact_at, CANDIDATE_SEQUENCE,
    OUT_OF_CORE_MANIFEST_FILE,
};
use crate::{Result, SkeinError, SEARCH_SEGMENT_DESCRIPTOR_FILE, SEARCH_SEGMENT_PAYLOAD_FILE};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::path::Path;
use std::sync::atomic::Ordering;

pub(super) struct Names {
    value: NameStrings,
    _memory: QueryMemoryLease,
}

pub(super) struct NameStrings {
    pub(super) descriptor: String,
    pub(super) payload: String,
    pub(super) metadata: String,
    pub(super) vector: String,
    pub(super) layout: String,
    pub(super) lexical_manifest: String,
}

impl std::ops::Deref for Names {
    type Target = NameStrings;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl Names {
    pub(super) fn new(
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        // Each fixed format and a full u64 fit in 128 bytes. Include overlapping
        // formatter growth before constructing any of the original names.
        let lease = memory.retained.reserve(6 * 3 * 128)?;
        #[cfg(test)]
        super::tests::record_names();
        let mut names = Self {
            value: NameStrings {
                descriptor: format!("search_projection_segments.{generation}.skein"),
                payload: format!("search_projection_segment_payloads.{generation}.skein"),
                metadata: format!("search_projection_metadata_payloads.{generation}.skein"),
                vector: format!("search_projection_vector_payloads.{generation}.skein"),
                layout: format!("search_projection_out_of_core_layout.{generation}.skein"),
                lexical_manifest: format!("search_lexical.manifest.{generation}.skein"),
            },
            _memory: lease,
        };
        checkpoint(task)?;
        let mut bytes = 0;
        for name in names.all() {
            if name.capacity() > 128 {
                return Err(SkeinError::Execution(
                    "search publication name exceeds preflight capacity".into(),
                ));
            }
            bytes = checked_add(bytes, name.capacity())?;
        }
        names._memory.shrink(names._memory.bytes() - bytes);
        Ok(names)
    }

    pub(super) fn all(&self) -> [&String; 6] {
        [
            &self.descriptor,
            &self.payload,
            &self.metadata,
            &self.vector,
            &self.layout,
            &self.lexical_manifest,
        ]
    }
}

pub(super) struct Target {
    pub(super) path: OwnedPath,
    pub(super) temporary: OwnedPath,
}

impl Target {
    pub(super) fn new(
        root: &Path,
        name: &str,
        sequence: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let path = OwnedPath::join(root, Path::new(name), memory, task)?;
        let _extension_memory = memory.retained.reserve(3 * 128)?;
        let extension = format!("tmp.{}.{}", std::process::id(), sequence);
        if extension.capacity() > 128 {
            return Err(SkeinError::Execution(
                "search publication temporary name exceeds preflight capacity".into(),
            ));
        }
        let temporary = OwnedPath::with_extension(&path, &extension, memory, task)?;
        Ok(Self { path, temporary })
    }

    pub(super) fn write(&self, bytes: &[u8]) -> Result<()> {
        write_generation_artifact_at(&self.path, &self.temporary, bytes)
    }
}

pub(super) struct Transfer {
    pub(super) source: OwnedPath,
    pub(super) target: Target,
}

impl Transfer {
    fn new(
        input: &PublishGenerationInput<'_>,
        source: &str,
        destination: &str,
        sequence: u64,
    ) -> Result<Self> {
        Ok(Self {
            source: OwnedPath::join(
                input.stage,
                Path::new(source),
                input.memory,
                input.task_context,
            )?,
            target: Target::new(
                input.root,
                destination,
                sequence,
                input.memory,
                input.task_context,
            )?,
        })
    }

    pub(super) fn publish(&self) -> Result<()> {
        publish_generation_link_at(&self.source, &self.target.path, &self.target.temporary)
    }
}

pub(super) struct Paths {
    pub(super) descriptor: Transfer,
    pub(super) payload: Transfer,
    pub(super) metadata: Transfer,
    pub(super) vector: Transfer,
    pub(super) lexical: Transfer,
    pub(super) lexical_manifest: Transfer,
    pub(super) rabitq: Option<Transfer>,
    pub(super) layout: Target,
    pub(super) manifest: Target,
}

impl Paths {
    pub(super) fn new(input: &PublishGenerationInput<'_>, names: &Names) -> Result<Self> {
        Self::prepare(input, names, &mut || {
            CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        })
    }

    // Deterministic sequence values keep exact-budget tests independent of
    // concurrent publishers. Production uses the existing shared counter.
    pub(super) fn prepare(
        input: &PublishGenerationInput<'_>,
        names: &Names,
        next: &mut impl FnMut() -> u64,
    ) -> Result<Self> {
        Ok(Self {
            descriptor: Transfer::new(
                input,
                SEARCH_SEGMENT_DESCRIPTOR_FILE,
                &names.descriptor,
                next(),
            )?,
            payload: Transfer::new(input, SEARCH_SEGMENT_PAYLOAD_FILE, &names.payload, next())?,
            metadata: Transfer::new(input, STAGE_METADATA_FILE, &names.metadata, next())?,
            vector: Transfer::new(input, STAGE_VECTOR_FILE, &names.vector, next())?,
            lexical: Transfer::new(
                input,
                input.lexical_artifact_name,
                input.lexical_artifact_name,
                next(),
            )?,
            lexical_manifest: Transfer::new(
                input,
                LEXICAL_MANIFEST_FILE,
                &names.lexical_manifest,
                next(),
            )?,
            rabitq: input
                .rabitq
                .map(|artifact| {
                    Transfer::new(input, &artifact.file_name, &artifact.file_name, next())
                })
                .transpose()?,
            layout: Target::new(
                input.root,
                &names.layout,
                next(),
                input.memory,
                input.task_context,
            )?,
            manifest: Target::new(
                input.root,
                OUT_OF_CORE_MANIFEST_FILE,
                next(),
                input.memory,
                input.task_context,
            )?,
        })
    }
}
