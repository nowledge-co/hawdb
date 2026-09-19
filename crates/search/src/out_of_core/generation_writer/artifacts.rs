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

use super::super::{
    append_sidecar_payload_with_context, SearchOutOfCoreLayoutBody, SearchOutOfCoreSegmentLayout,
    OUT_OF_CORE_LAYOUT_FORMAT,
};
#[cfg(test)]
use super::spool::SpoolSource;
use super::{SearchOutOfCoreGenerationBuildOptions, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::build_control::{checkpoint, temporary::RemoveOnDrop, write_checksummed};
use crate::build_memory::path::OwnedPath;
use crate::build_memory::{
    checked_mul, grow_slots, AdmittedDocument, BuildMemory, SPOOL_BUFFER_BYTES,
};
use crate::document_encoding::{
    DescriptorEncoding, DocumentEncoding, SegmentEncoding, SegmentKind, HEX_BUFFER_BYTES,
};
use crate::error::{HawDBError, Result};
use crate::{
    SearchDocument, SearchSegmentDescriptor, SearchSegmentDescriptorEntry,
    SearchSegmentPayloadRange, SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
    SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID, SEARCH_SEGMENT_PAYLOAD_FILE,
};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

mod descriptor;
pub(super) mod encoding;
mod paths;

#[cfg(test)]
mod tests;

pub(super) struct SegmentArtifactBuilder<'a> {
    descriptor_path: OwnedPath,
    descriptor_temporary: OwnedPath,
    generation: u64,
    options: &'a SearchOutOfCoreGenerationBuildOptions,
    fields: &'a BTreeSet<String>,
    document_file: File,
    metadata_file: File,
    vector_file: File,
    documents: Vec<AdmittedDocument>,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    segment_encoded_bytes: u64,
    descriptor: SearchSegmentDescriptor,
    layouts: Vec<SearchOutOfCoreSegmentLayout>,
    document_offset: u64,
    metadata_offset: u64,
    vector_offset: u64,
    next_vector_ordinal: u64,
    next_document_ordinal: u64,
    descriptor_working_bytes: u64,
    peak_segment_document_count: usize,
    peak_segment_encoded_bytes: u64,
    _documents_memory: QueryMemoryLease,
    descriptor_memory: QueryMemoryLease,
    layout_memory: QueryMemoryLease,
    failed: bool,
}

pub(super) struct SegmentArtifactOutput {
    pub(super) layout: SearchOutOfCoreLayoutBody,
    pub(super) descriptor_working_bytes: u64,
    pub(super) descriptor_bytes: u64,
    pub(super) document_payload_bytes: u64,
    pub(super) metadata_payload_bytes: u64,
    pub(super) vector_payload_bytes: u64,
    pub(super) peak_segment_document_count: usize,
    pub(super) peak_segment_encoded_bytes: u64,
    _layout_memory: QueryMemoryLease,
}

impl<'a> SegmentArtifactBuilder<'a> {
    #[cfg(test)]
    pub(super) fn new(
        stage: &Path,
        generation: u64,
        fields: &'a BTreeSet<String>,
        options: &'a SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<Self> {
        Self::new_with_memory(
            stage,
            generation,
            fields,
            options,
            BuildMemory::new(&hawdb_core::RuntimeTaskContext::default())?,
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_memory(
        stage: &Path,
        generation: u64,
        fields: &'a BTreeSet<String>,
        options: &'a SearchOutOfCoreGenerationBuildOptions,
        memory: BuildMemory,
    ) -> Result<Self> {
        Self::new_with_context(
            stage,
            generation,
            fields,
            options,
            memory,
            RuntimeTaskContext::default(),
        )
    }

    pub(super) fn new_with_context(
        stage: &Path,
        generation: u64,
        fields: &'a BTreeSet<String>,
        options: &'a SearchOutOfCoreGenerationBuildOptions,
        memory: BuildMemory,
        task: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task)?;
        let documents_memory = memory.retained.reserve(checked_mul(
            SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
            std::mem::size_of::<AdmittedDocument>(),
        )?)?;
        let mut documents = Vec::new();
        documents
            .try_reserve_exact(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS)
            .map_err(|error| {
                HawDBError::Execution(format!(
                    "search segment document allocation failed: {error}"
                ))
            })?;
        if documents.capacity() > SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
            return Err(HawDBError::Execution(
                "search segment document capacity exceeded admission".into(),
            ));
        }
        let descriptor_memory = memory.retained.reserve(0)?;
        let layout_memory = memory.retained.reserve(OUT_OF_CORE_LAYOUT_FORMAT.len())?;
        let paths = paths::Paths::new(stage, &memory, &task)?;
        Ok(Self {
            descriptor_path: paths.descriptor,
            descriptor_temporary: paths.temporary,
            generation,
            options,
            fields,
            document_file: File::create(&paths.document)?,
            metadata_file: File::create(&paths.metadata)?,
            vector_file: File::create(&paths.vector)?,
            documents,
            memory,
            task,
            segment_encoded_bytes: 0,
            descriptor: SearchSegmentDescriptor {
                target_documents: SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
                document_count: 0,
                segments: Vec::new(),
            },
            layouts: Vec::new(),
            document_offset: 0,
            metadata_offset: 0,
            vector_offset: 0,
            next_vector_ordinal: 0,
            next_document_ordinal: 0,
            descriptor_working_bytes: 0,
            peak_segment_document_count: 0,
            peak_segment_encoded_bytes: 0,
            _documents_memory: documents_memory,
            descriptor_memory,
            layout_memory,
            failed: false,
        })
    }

    #[cfg(test)]
    pub(super) fn build(mut self, source: &SpoolSource) -> Result<SegmentArtifactOutput> {
        source.scan(&mut |ordinal, document| self.push(ordinal, document))?;
        self.finish(source.document_count)
    }

    pub(super) fn finish(mut self, document_count: usize) -> Result<SegmentArtifactOutput> {
        if u64::try_from(document_count).ok() != Some(self.next_document_ordinal) {
            return Err(HawDBError::Storage(
                "search segment document count disagrees with source ordinals".to_string(),
            ));
        }
        self.flush_segment()?;
        for file in [&self.document_file, &self.metadata_file, &self.vector_file] {
            checkpoint(&self.task)?;
            file.sync_all()?;
        }
        self.descriptor.document_count = document_count;
        let descriptor_bytes = self.write_descriptor()?;
        Ok(SegmentArtifactOutput {
            layout: SearchOutOfCoreLayoutBody {
                format: OUT_OF_CORE_LAYOUT_FORMAT.to_string(),
                generation: self.generation,
                document_count,
                segments: self.layouts,
            },
            descriptor_working_bytes: self.descriptor_working_bytes,
            descriptor_bytes,
            document_payload_bytes: self.document_offset,
            metadata_payload_bytes: self.metadata_offset,
            vector_payload_bytes: self.vector_offset,
            peak_segment_document_count: self.peak_segment_document_count,
            peak_segment_encoded_bytes: self.peak_segment_encoded_bytes,
            _layout_memory: self.layout_memory,
        })
    }

    #[cfg(test)]
    pub(super) fn push(&mut self, ordinal: u64, document: SearchDocument) -> Result<()> {
        let document = self.memory.admit_document(document)?;
        self.push_admitted(ordinal, document)
    }

    pub(super) fn push_admitted(&mut self, ordinal: u64, document: AdmittedDocument) -> Result<()> {
        self.check_healthy()?;
        let result = self.push_inner(ordinal, document);
        self.failed = result.is_err();
        result
    }

    fn push_inner(&mut self, ordinal: u64, document: AdmittedDocument) -> Result<()> {
        checkpoint(&self.task)?;
        if ordinal != self.next_document_ordinal {
            return Err(HawDBError::Storage(format!(
                "search segment document ordinal {ordinal} does not follow {}",
                self.next_document_ordinal
            )));
        }
        let next_document_ordinal = ordinal.checked_add(1).ok_or_else(|| {
            HawDBError::Storage("search segment document ordinal overflow".to_string())
        })?;
        let encoded_bytes =
            DocumentEncoding::new_with_context(&document, Some(&self.task))?.len() as u64;
        let projected = self.segment_encoded_bytes.saturating_add(encoded_bytes);
        if !self.documents.is_empty()
            && (self.documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS
                || projected > self.options.max_segment_uncompressed_bytes.get())
        {
            self.flush_segment()?;
        }
        if encoded_bytes.saturating_add(64) > self.options.max_segment_uncompressed_bytes.get() {
            return Err(HawDBError::Storage(format!(
                "search generation document {} cannot fit the admitted segment buffer",
                document.id
            )));
        }
        self.segment_encoded_bytes = self.segment_encoded_bytes.saturating_add(encoded_bytes);
        self.documents.push(document);
        self.next_document_ordinal = next_document_ordinal;
        Ok(())
    }

    fn flush_segment(&mut self) -> Result<()> {
        self.check_healthy()?;
        let result = self.flush_segment_inner();
        self.failed = result.is_err();
        result
    }

    fn check_healthy(&self) -> Result<()> {
        if self.failed {
            return Err(HawDBError::Storage(
                "search segment writer already failed".into(),
            ));
        }
        checkpoint(&self.task)
    }

    fn flush_segment_inner(&mut self) -> Result<()> {
        if self.documents.is_empty() {
            return Ok(());
        }
        grow_slots(&mut self.descriptor.segments, &mut self.descriptor_memory)?;
        grow_slots(&mut self.layouts, &mut self.layout_memory)?;
        let segment_id = self.descriptor.segments.len() as u64;
        let (mut descriptor, projected_descriptor_bytes) = descriptor::build_with_context(
            segment_id,
            &self.documents,
            self.fields,
            self.descriptor_working_bytes,
            self.options.max_descriptor_working_bytes.get(),
            descriptor::Admission {
                memory: &self.memory,
                task: &self.task,
                retained: &mut self.descriptor_memory,
            },
        )?;

        let document_payload = self.encode_segment_payload(segment_id, SegmentKind::Documents)?;
        let document_length = document_payload.len() as u64;
        let checksum = write_checksummed(
            &mut self.document_file,
            document_payload.as_ref(),
            Some(&self.task),
        )?;
        descriptor.payload_range = Some(SearchSegmentPayloadRange {
            artifact_id: SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            offset: self.document_offset,
            length: document_length,
            checksum,
        });
        self.document_offset = self
            .document_offset
            .checked_add(document_length)
            .ok_or_else(|| HawDBError::Storage("search generation payload overflow".to_string()))?;
        drop(document_payload);

        let vector_ordinal_base = self.next_vector_ordinal;
        let metadata_payload = self.encode_segment_payload(
            segment_id,
            SegmentKind::Metadata {
                vector_ordinal_base,
            },
        )?;
        let metadata = append_sidecar_payload_with_context(
            &mut self.metadata_file,
            &mut self.metadata_offset,
            metadata_payload.as_ref(),
            self.documents.len(),
            Some(&self.task),
        )?;
        drop(metadata_payload);

        let vector_count = self
            .documents
            .iter()
            .filter(|document| document.embedding.is_some())
            .count();
        self.next_vector_ordinal = vector_ordinal_base.saturating_add(vector_count as u64);
        let vector_payload = self.encode_segment_payload(
            segment_id,
            SegmentKind::Vectors {
                vector_ordinal_base,
            },
        )?;
        let vectors = append_sidecar_payload_with_context(
            &mut self.vector_file,
            &mut self.vector_offset,
            vector_payload.as_ref(),
            vector_count,
            Some(&self.task),
        )?;
        self.descriptor_working_bytes = projected_descriptor_bytes;
        self.descriptor.segments.push(descriptor);
        self.layouts.push(SearchOutOfCoreSegmentLayout {
            segment_id,
            vector_ordinal_base,
            metadata,
            vectors,
        });
        self.peak_segment_document_count =
            self.peak_segment_document_count.max(self.documents.len());
        self.peak_segment_encoded_bytes = self
            .peak_segment_encoded_bytes
            .max(self.segment_encoded_bytes);
        self.documents.clear();
        self.segment_encoded_bytes = 0;
        Ok(())
    }

    fn write_descriptor(&self) -> Result<u64> {
        checkpoint(&self.task)?;
        let _io_memory = self
            .memory
            .spool
            .reserve(SPOOL_BUFFER_BYTES + HEX_BUFFER_BYTES + 32)?;
        let encoding = DescriptorEncoding::new_with_context(
            &self.descriptor,
            self.options.max_descriptor_working_bytes.get(),
            Some(&self.task),
        )?;
        {
            let mut file = BufWriter::with_capacity(
                SPOOL_BUFFER_BYTES,
                File::create(&self.descriptor_temporary)?,
            );
            encoding.write_to(&mut file)?;
            file.flush()?;
            file.get_ref().sync_all()?;
        }
        let mut temporary_guard = RemoveOnDrop::new(&self.descriptor_temporary);
        checkpoint(&self.task)?;
        crate::durable_replace_file(&self.descriptor_temporary, &self.descriptor_path)?;
        temporary_guard.disarm();
        checkpoint(&self.task)?;
        Ok(encoding.len() as u64)
    }

    fn encode_segment_payload(
        &self,
        segment_id: u64,
        kind: SegmentKind,
    ) -> Result<encoding::CompressedBuffer> {
        let encoding = SegmentEncoding::new_with_context(&self.documents, kind, Some(&self.task))?;
        encoding::encode_segment_payload_with_context(
            &encoding,
            segment_id,
            kind.name(),
            self.options.max_segment_uncompressed_bytes.get(),
            self.options.max_segment_compressed_bytes.get(),
            &self.memory,
            &self.task,
        )
    }
}

#[cfg(test)]
fn descriptor_working_bytes(descriptor: &SearchSegmentDescriptorEntry) -> u64 {
    let mut bytes = 256u64
        .saturating_add(descriptor.first_document_id.len() as u64)
        .saturating_add(descriptor.last_document_id.len() as u64);
    for (field, summary) in &descriptor.metadata {
        bytes = bytes.saturating_add(192).saturating_add(field.len() as u64);
        for value in &summary.values {
            bytes = bytes.saturating_add(32).saturating_add(value.len() as u64);
        }
    }
    bytes
}
