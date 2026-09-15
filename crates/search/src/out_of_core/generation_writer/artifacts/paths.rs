use super::*;
use crate::build_memory::path::OwnedPath;
use crate::SEARCH_SEGMENT_DESCRIPTOR_FILE;
use skein_core::RuntimeTaskContext;

pub(super) struct Paths {
    pub(super) document: OwnedPath,
    pub(super) metadata: OwnedPath,
    pub(super) vector: OwnedPath,
    pub(super) descriptor: OwnedPath,
    pub(super) temporary: OwnedPath,
}

impl Paths {
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
