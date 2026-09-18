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

//! Incremental RaBitQ sink for the shared generation scan.

use super::{RaBitQGenerationArtifact, SearchOutOfCoreGenerationWriter};
use crate::build_control::checkpoint;
use crate::error::Result;
use crate::SearchDocument;
use hawdb_core::RuntimeTaskContext;

#[cfg(feature = "vector-search")]
use super::rabitq_memory::Admission;
#[cfg(feature = "vector-search")]
use super::{artifact_name::Name, context_memory::OwnedPath, HawDBError};

#[cfg(feature = "vector-search")]
pub(super) struct RaBitQArtifactBuilder {
    writer: Option<hawdb_vector_projection::ProjectionWriter>,
    file_name: Name,
    path: OwnedPath,
    expected_documents: usize,
    vector_ordinal: u64,
    task_context: RuntimeTaskContext,
    failed: bool,
    // Data fields drop before these leases, including on partial construction.
    admission: Option<Admission>,
}

#[cfg(feature = "vector-search")]
impl RaBitQArtifactBuilder {
    pub(super) fn new(input: &SearchOutOfCoreGenerationWriter, generation: u64) -> Result<Self> {
        checkpoint(&input.task_context)?;
        let file_name = Name::rabitq(generation, &input.memory, &input.task_context)?;
        let path = OwnedPath::join(
            &input.stage.path,
            file_name.as_ref(),
            &input.memory,
            &input.task_context,
        )?;
        let mut admission = None;
        let writer = if input.vector_document_count == 0 {
            None
        } else {
            let dimension = input.embedding_dimension.ok_or_else(|| {
                HawDBError::Storage(
                    "search generation has vector documents without an embedding dimension"
                        .to_string(),
                )
            })?;
            let mut memory = Admission::new(&input.options, &path, input.memory.clone())?;
            let identity = hawdb_vector_projection::ProjectionIdentity {
                generation,
                source_epoch: input.options.source_graph_commit_epoch,
                embedding_model: input
                    .options
                    .embedding_manifest
                    .as_ref()
                    .map(|manifest| manifest.model.clone()),
                embedding_version: input
                    .options
                    .embedding_manifest
                    .as_ref()
                    .and_then(|manifest| manifest.version.clone()),
            };
            let config = hawdb_vector_projection::ProjectionBuildConfig::new(dimension, identity)
                .with_bit_width(input.options.rabitq_bit_width)
                .with_segment_rows(input.options.rabitq_segment_rows.get())
                .with_max_working_bytes(input.options.rabitq_build_memory_bytes.get())
                .with_transform_seed(input.options.rabitq_transform_seed);
            memory.admit_state(
                config.resource_admission().map_err(rabitq_error)?,
                dimension,
                input.options.rabitq_bit_width,
            )?;
            admission = Some(memory);
            #[cfg(test)]
            evidence::create();
            let writer = hawdb_vector_projection::ProjectionWriter::create(&path, config)
                .map_err(rabitq_error)?;
            #[cfg(test)]
            evidence::completed(0);
            checkpoint(&input.task_context)?;
            Some(writer)
        };
        Ok(Self {
            writer,
            file_name,
            path,
            expected_documents: input.vector_document_count,
            vector_ordinal: 0,
            task_context: input.task_context.clone(),
            failed: false,
            admission,
        })
    }

    pub(super) fn push(&mut self, document: &SearchDocument) -> Result<()> {
        if self.failed {
            return Err(HawDBError::Storage(
                "search RaBitQ writer already failed".to_owned(),
            ));
        }
        let result = self.push_inner(document);
        self.failed = result.is_err();
        result
    }

    fn push_inner(&mut self, document: &SearchDocument) -> Result<()> {
        checkpoint(&self.task_context)?;
        let Some(embedding) = document.embedding.as_deref() else {
            return Ok(());
        };
        if self.vector_ordinal >= self.expected_documents as u64 {
            return Err(HawDBError::Storage(
                "search RaBitQ exceeded the expected vector document count".to_owned(),
            ));
        }
        let next = self
            .vector_ordinal
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("search vector ordinal overflow".to_owned()))?;
        let memory = self.admission.as_mut().ok_or_else(|| {
            HawDBError::Storage("search RaBitQ has no admitted writer".to_owned())
        })?;
        memory.admit_directory(next as usize / memory.segment_rows)?;
        let _quantization = memory.quantization()?;
        let writer = self.writer.as_mut().ok_or_else(|| {
            HawDBError::Storage(
                "search generation contains unexpected vector documents".to_string(),
            )
        })?;
        #[cfg(test)]
        evidence::push();
        writer
            .push(self.vector_ordinal, embedding)
            .map_err(rabitq_error)?;
        #[cfg(test)]
        evidence::completed(1);
        checkpoint(&self.task_context)?;
        self.vector_ordinal = next;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Option<RaBitQGenerationArtifact>> {
        checkpoint(&self.task_context)?;
        if self.failed {
            return Err(HawDBError::Storage(
                "search RaBitQ writer already failed".to_owned(),
            ));
        }
        if self.vector_ordinal != self.expected_documents as u64 {
            return Err(HawDBError::Storage(
                "search RaBitQ build did not consume the expected vector document count"
                    .to_string(),
            ));
        }
        let Some(writer) = self.writer else {
            return Ok(None);
        };
        let memory = self
            .admission
            .as_mut()
            .expect("a vector writer owns admission");
        let segments = self.expected_documents.div_ceil(memory.segment_rows);
        memory.admit_directory(segments)?;
        let _finalize = memory.finalize(segments)?;
        #[cfg(test)]
        evidence::finish();
        let projection = writer.finish().map_err(rabitq_error)?;
        #[cfg(test)]
        evidence::reopened(memory.used_bytes());
        #[cfg(test)]
        evidence::completed(2);
        checkpoint(&self.task_context)?;
        let manifest = projection.manifest();
        let (artifact_bytes, artifact_checksum) =
            super::publication::file_len_checksum_with_context(
                &self.path,
                &memory.memory,
                &self.task_context,
            )?;
        Ok(Some(RaBitQGenerationArtifact {
            file_name: self.file_name,
            artifact_bytes,
            artifact_checksum,
            source_digest: manifest.source_digest,
            document_count: manifest.document_count,
            payload_checksum: manifest.payload_checksum,
            peak_build_working_bytes: manifest.peak_build_working_bytes,
        }))
    }
}

#[cfg(feature = "vector-search")]
fn rabitq_error(error: hawdb_vector_projection::ProjectionError) -> HawDBError {
    HawDBError::Storage(format!("search RaBitQ projection: {error}"))
}

#[cfg(all(test, feature = "vector-search"))]
pub(super) mod evidence {
    use hawdb_core::RuntimeCancellationToken;
    use std::cell::{Cell, RefCell};
    thread_local! { static CALLS: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) }; }
    thread_local! { static REOPENED_BYTES: Cell<usize> = const { Cell::new(0) }; }
    thread_local! { static CANCEL: RefCell<Option<(usize, RuntimeCancellationToken)>> = const { RefCell::new(None) }; }
    pub(super) fn completed(call: usize) {
        CANCEL.with_borrow_mut(|pending| {
            if let Some((stage, token)) = pending
                && *stage == call
            {
                token.cancel();
                *pending = None;
            }
        });
    }
    pub(in super::super) fn cancel_after(call: usize, token: RuntimeCancellationToken) {
        CANCEL.with_borrow_mut(|pending| *pending = Some((call, token)));
    }

    pub(super) fn create() {
        CALLS.with(|c| {
            let (a, b, d) = c.get();
            c.set((a + 1, b, d));
        });
    }
    pub(super) fn push() {
        CALLS.with(|c| {
            let (a, b, d) = c.get();
            c.set((a, b + 1, d));
        });
    }
    pub(super) fn finish() {
        CALLS.with(|c| {
            let (a, b, d) = c.get();
            c.set((a, b, d + 1));
        });
    }
    pub(in super::super) fn take() -> (usize, usize, usize) {
        CALLS.with(|c| c.replace((0, 0, 0)))
    }
    pub(super) fn reopened(bytes: usize) {
        REOPENED_BYTES.with(|value| value.set(bytes));
    }
    pub(in super::super) fn take_reopened_bytes() -> usize {
        REOPENED_BYTES.with(|value| value.replace(0))
    }
}

#[cfg(not(feature = "vector-search"))]
pub(super) struct RaBitQArtifactBuilder(RuntimeTaskContext);

#[cfg(not(feature = "vector-search"))]
impl RaBitQArtifactBuilder {
    pub(super) fn new(input: &SearchOutOfCoreGenerationWriter, _: u64) -> Result<Self> {
        checkpoint(&input.task_context)?;
        Ok(Self(input.task_context.clone()))
    }
    pub(super) fn push(&mut self, _: &SearchDocument) -> Result<()> {
        checkpoint(&self.0)
    }
    pub(super) fn finish(self) -> Result<Option<RaBitQGenerationArtifact>> {
        checkpoint(&self.0)?;
        Ok(None)
    }
}

#[cfg(all(test, feature = "vector-search"))]
mod tests;
