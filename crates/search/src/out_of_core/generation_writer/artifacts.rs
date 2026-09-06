use super::super::{
    append_sidecar_payload, SearchOutOfCoreLayoutBody, SearchOutOfCoreSegmentLayout,
    OUT_OF_CORE_LAYOUT_FORMAT,
};
use super::segment_io::{self, Kind};
use super::segment_memory;
#[cfg(test)]
use super::spool::SpoolSource;
use super::{SearchOutOfCoreGenerationBuildOptions, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_mul, AdmittedDocument, BuildMemory};
use crate::error::{Result, SkeinError};
#[cfg(test)]
use crate::SearchDocument;
use crate::{
    checksum_bytes, SearchSegmentDescriptor, SearchSegmentDescriptorEntry,
    SearchSegmentPayloadRange, SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
    SEARCH_SEGMENT_DESCRIPTOR_FILE, SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) struct SegmentArtifactBuilder<'a> {
    stage: PathBuf,
    generation: u64,
    options: &'a SearchOutOfCoreGenerationBuildOptions,
    fields: &'a BTreeSet<String>,
    document_file: File,
    metadata_file: File,
    vector_file: File,
    documents: Vec<AdmittedDocument>,
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
    task_context: RuntimeTaskContext,
    memory: BuildMemory,
    _document_slots: QueryMemoryLease,
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
            BuildMemory::new(&RuntimeTaskContext::default())?,
        )
    }

    pub(super) fn new_with_memory(
        stage: &Path,
        generation: u64,
        fields: &'a BTreeSet<String>,
        options: &'a SearchOutOfCoreGenerationBuildOptions,
        memory: BuildMemory,
    ) -> Result<Self> {
        let document_slots = memory.retained.reserve(checked_mul(
            SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
            std::mem::size_of::<AdmittedDocument>(),
        )?)?;
        let descriptor_memory = memory.retained.reserve(0)?;
        let layout_memory = memory.retained.reserve(OUT_OF_CORE_LAYOUT_FORMAT.len())?;
        Ok(Self {
            stage: stage.to_path_buf(),
            generation,
            options,
            fields,
            document_file: File::create(stage.join(SEARCH_SEGMENT_PAYLOAD_FILE))?,
            metadata_file: File::create(stage.join(STAGE_METADATA_FILE))?,
            vector_file: File::create(stage.join(STAGE_VECTOR_FILE))?,
            documents: Vec::with_capacity(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS),
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
            task_context: RuntimeTaskContext::default(),
            memory,
            _document_slots: document_slots,
            descriptor_memory,
            layout_memory,
            failed: false,
        })
    }

    pub(super) fn with_context(mut self, task_context: RuntimeTaskContext) -> Self {
        self.task_context = task_context;
        self
    }

    #[cfg(test)]
    pub(super) fn build(mut self, source: &SpoolSource) -> Result<SegmentArtifactOutput> {
        source.scan(&mut |ordinal, document| self.push(ordinal, document))?;
        self.finish(source.document_count)
    }

    pub(super) fn finish(mut self, document_count: usize) -> Result<SegmentArtifactOutput> {
        checkpoint(&self.task_context)?;
        if u64::try_from(document_count).ok() != Some(self.next_document_ordinal) {
            return Err(SkeinError::Storage(
                "search segment document count disagrees with source ordinals".to_string(),
            ));
        }
        self.flush_segment()?;
        self.document_file.sync_all()?;
        self.metadata_file.sync_all()?;
        self.vector_file.sync_all()?;
        self.descriptor.document_count = document_count;
        let encoded = segment_memory::encode_descriptor(
            &self.descriptor,
            self.options.max_descriptor_working_bytes.get(),
            &self.memory,
            &self.task_context,
        )?;
        let descriptor_path = self.stage.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
        let temporary = descriptor_path.with_extension("skein.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(encoded.as_ref())?;
        file.sync_all()?;
        drop(file);
        skein_storage::durable_replace_file(&temporary, &descriptor_path)?;
        drop(encoded);
        checkpoint(&self.task_context)?;
        let descriptor_bytes = fs::metadata(self.stage.join(SEARCH_SEGMENT_DESCRIPTOR_FILE))?.len();
        if descriptor_bytes > self.options.max_descriptor_working_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation descriptor requires {descriptor_bytes} bytes, exceeding {}",
                self.options.max_descriptor_working_bytes
            )));
        }
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
        checkpoint(&self.task_context)?;
        if self.failed {
            return Err(SkeinError::Storage(
                "search segment writer already failed".to_owned(),
            ));
        }
        if ordinal != self.next_document_ordinal {
            return Err(SkeinError::Storage(format!(
                "search segment document ordinal {ordinal} does not follow {}",
                self.next_document_ordinal
            )));
        }
        let next_document_ordinal = ordinal.checked_add(1).ok_or_else(|| {
            SkeinError::Storage("search segment document ordinal overflow".to_string())
        })?;
        let encoded_bytes = crate::document_codec::encoded_len(
            &document,
            self.options.max_record_bytes.get(),
            Some(&self.task_context),
        )? as u64;
        let projected = self.segment_encoded_bytes.saturating_add(encoded_bytes);
        if !self.documents.is_empty()
            && (self.documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS
                || projected > self.options.max_segment_uncompressed_bytes.get())
        {
            self.flush_segment()?;
        }
        if encoded_bytes.saturating_add(64) > self.options.max_segment_uncompressed_bytes.get() {
            return Err(SkeinError::Storage(format!(
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
        if self.failed {
            return Err(SkeinError::Storage(
                "search segment writer already failed".to_owned(),
            ));
        }
        let result = self.flush_segment_inner();
        self.failed = result.is_err();
        result
    }

    fn flush_segment_inner(&mut self) -> Result<()> {
        checkpoint(&self.task_context)?;
        if self.documents.is_empty() {
            return Ok(());
        }
        let segment_id = self.descriptor.segments.len() as u64;
        let mut descriptor = segment_memory::descriptor(
            segment_id,
            &self.documents,
            self.fields,
            &self.memory,
            &mut self.descriptor_memory,
            &self.task_context,
        )?;

        let document_payload = self.encode_segment_payload(Kind::Document)?;
        let document_length = document_payload.len() as u64;
        self.document_file.write_all(document_payload.as_ref())?;
        descriptor.payload_range = Some(SearchSegmentPayloadRange {
            artifact_id: SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            offset: self.document_offset,
            length: document_length,
            checksum: checksum_bytes(document_payload.as_ref()),
        });
        self.document_offset = self
            .document_offset
            .checked_add(document_length)
            .ok_or_else(|| SkeinError::Storage("search generation payload overflow".to_string()))?;
        drop(document_payload);

        let vector_ordinal_base = self.next_vector_ordinal;
        let metadata_payload = self.encode_segment_payload(Kind::Metadata)?;
        let metadata = append_sidecar_payload(
            &mut self.metadata_file,
            &mut self.metadata_offset,
            metadata_payload.as_ref(),
            self.documents.len(),
        )?;
        drop(metadata_payload);

        let vector_count = self
            .documents
            .iter()
            .filter(|document| document.embedding.is_some())
            .count();
        let vector_payload = self.encode_segment_payload(Kind::Vector)?;
        self.next_vector_ordinal = vector_ordinal_base
            .checked_add(vector_count as u64)
            .ok_or_else(|| {
                SkeinError::Storage("search vector ordinal range overflow".to_owned())
            })?;
        let vectors = append_sidecar_payload(
            &mut self.vector_file,
            &mut self.vector_offset,
            vector_payload.as_ref(),
            vector_count,
        )?;
        let entry_bytes = descriptor_working_bytes(&descriptor);
        let projected_descriptor_bytes = self
            .descriptor_working_bytes
            .saturating_add(entry_bytes)
            .saturating_add(96);
        if projected_descriptor_bytes > self.options.max_descriptor_working_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation descriptor working set requires {projected_descriptor_bytes} bytes, exceeding {}",
                self.options.max_descriptor_working_bytes
            )));
        }
        self.descriptor_working_bytes = projected_descriptor_bytes;
        segment_memory::grow_slots(&mut self.descriptor.segments, &mut self.descriptor_memory)?;
        self.descriptor.segments.push(descriptor);
        segment_memory::grow_slots(&mut self.layouts, &mut self.layout_memory)?;
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

    fn encode_segment_payload(&self, kind: Kind) -> Result<crate::build_io::Buffer> {
        let body = segment_io::body(
            kind,
            &self.documents,
            self.next_vector_ordinal,
            self.options.max_segment_uncompressed_bytes.get(),
            &self.memory,
            &self.task_context,
        )?;
        segment_io::compress(
            body.as_ref(),
            self.options.max_segment_compressed_bytes.get(),
            &self.memory,
            &self.task_context,
        )
    }
}

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
