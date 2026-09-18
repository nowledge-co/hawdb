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

//! Owned admission around the embedded projection writer's complete lifecycle.

use super::SearchOutOfCoreGenerationBuildOptions;
use crate::build_memory::{checked_add, checked_mul, BuildMemory};
use crate::error::Result;
use hawdb_executor::QueryMemoryLease;
use hawdb_vector_projection::{ProjectionBuildAdmission, RaBitQBitWidth, SegmentDescriptor};
use std::mem::size_of;
use std::path::Path;

pub(super) struct Admission {
    pub(super) memory: BuildMemory,
    state: QueryMemoryLease,
    directory: QueryMemoryLease,
    identity_bytes: usize,
    quantization_bytes: usize,
    pub(super) segment_rows: usize,
}

impl Admission {
    // Reserve identity copies before constructing a config or calling its
    // resource_admission(), which itself clones the validated identity.
    pub(super) fn new(
        options: &SearchOutOfCoreGenerationBuildOptions,
        path: &Path,
        memory: BuildMemory,
    ) -> Result<Self> {
        let identity_bytes = options
            .embedding_manifest
            .as_ref()
            .map_or(Ok(0), |identity| {
                checked_add(
                    identity.model.len(),
                    identity.version.as_ref().map_or(0, String::len),
                )
            })?;
        // Core create owns target/temporary paths and a formatted extension;
        // finish reopens another target path before the original writer drops.
        let path_bytes = checked_add(
            checked_mul(path.as_os_str().as_encoded_bytes().len(), 3)?,
            65 + 3 * 64,
        )?;
        let state = memory
            .retained
            .reserve(checked_add(checked_mul(identity_bytes, 2)?, path_bytes)?)?;
        let directory = memory.retained.reserve(0)?;
        Ok(Self {
            memory,
            state,
            directory,
            identity_bytes,
            quantization_bytes: 0,
            segment_rows: 0,
        })
    }

    pub(super) fn admit_state(
        &mut self,
        admission: ProjectionBuildAdmission,
        dimension: usize,
        bits: RaBitQBitWidth,
    ) -> Result<()> {
        // The component model covers the pending rows, transform and packed
        // buffers. The scalar quantizer's temporary arrays are additional.
        self.quantization_bytes = checked_mul(dimension, size_of::<f64>())?;
        if bits == RaBitQBitWidth::Four {
            // codes: usize per coordinate. RescaleEvent is { f64, usize } on
            // supported 32/64-bit targets. Heap length never exceeds dimension;
            // 3x includes old/new doubling overlap and its initial four slots.
            self.quantization_bytes = checked_add(
                self.quantization_bytes,
                checked_add(
                    checked_mul(dimension, size_of::<usize>())?,
                    checked_mul(dimension.max(4), 3 * 16)?,
                )?,
            )?;
        }
        self.state.grow(admission.peak_working_bytes)?;
        self.segment_rows = admission.admitted_segment_rows;
        Ok(())
    }

    pub(super) fn quantization(&self) -> Result<QueryMemoryLease> {
        self.memory.retained.reserve(self.quantization_bytes)
    }

    pub(super) fn admit_directory(&mut self, segments: usize) -> Result<()> {
        if segments == 0 {
            return Ok(());
        }
        // ProjectionWriter grows its descriptor Vec geometrically. Keep the
        // complete old/replacement bound admitted between calls, not just len.
        let bytes = directory_bytes(segments)?;
        if bytes > self.directory.bytes() {
            self.directory.grow(bytes - self.directory.bytes())?;
        }
        Ok(())
    }

    pub(super) fn finalize(&self, segments: usize) -> Result<QueryMemoryLease> {
        self.memory
            .retained
            .reserve(finalize_bytes(self.identity_bytes, segments)?)
    }

    #[cfg(test)]
    pub(super) fn used_bytes(&self) -> usize {
        self.memory.ledger.snapshot().used_bytes
    }
}

pub(super) fn directory_bytes(segments: usize) -> Result<usize> {
    checked_mul(segments.max(4), 3 * size_of::<SegmentDescriptor>())
}

pub(super) fn json_bytes(identity_bytes: usize, segments: usize) -> Result<usize> {
    // ProjectionManifest's fixed fields fit in 4 KiB; each descriptor has six
    // integers (<=20 digits) plus fixed keys/separators, fitting in 256 bytes.
    // serde JSON string escaping expands each source byte by at most six.
    checked_add(
        4096,
        checked_add(checked_mul(identity_bytes, 6)?, checked_mul(segments, 256)?)?,
    )
}

pub(super) fn finalize_bytes(identity_bytes: usize, segments: usize) -> Result<usize> {
    let json = json_bytes(identity_bytes, segments)?;
    // The writer retains its state and old manifest while reopening. Include
    // serde's old/new output growth (3x), the reopened exact raw JSON (1x),
    // parsed descriptor growth, string/parser scratch and Arc<Mmap> metadata.
    // Payload mappings/page-cache residency are not heap capacity or RSS proof.
    checked_add(
        checked_mul(json, 4)?,
        checked_add(
            directory_bytes(segments)?,
            checked_add(checked_mul(identity_bytes.max(128), 4)?, 1024)?,
        )?,
    )
}
