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

use super::*;
use crate::build_memory::path::OwnedPath;
use crate::SEARCH_SEGMENT_DESCRIPTOR_FILE;
use hawdb_core::RuntimeTaskContext;

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
        let temporary = OwnedPath::with_extension(&descriptor, "hawdb.tmp", memory, task)?;
        Ok(Self {
            document,
            metadata,
            vector,
            descriptor,
            temporary,
        })
    }
}
