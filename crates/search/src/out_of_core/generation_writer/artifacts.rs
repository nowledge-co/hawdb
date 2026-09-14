use super::super::{
    append_sidecar_payload, SearchOutOfCoreLayoutBody, SearchOutOfCoreSegmentLayout,
    OUT_OF_CORE_LAYOUT_FORMAT,
};
#[cfg(test)]
use super::spool::SpoolSource;
use super::{SearchOutOfCoreGenerationBuildOptions, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::document_encoding::{DocumentEncoding, SegmentEncoding, SegmentKind};
use crate::error::{Result, SkeinError};
use crate::{
    checksum_bytes, write_search_segment_descriptor_bounded, SearchDocument,
    SearchSegmentDescriptor, SearchSegmentDescriptorEntry, SearchSegmentPayloadRange,
    SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS, SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

mod descriptor;
mod encoding;

pub(super) struct SegmentArtifactBuilder<'a> {
    stage: PathBuf,
    generation: u64,
    options: &'a SearchOutOfCoreGenerationBuildOptions,
    fields: &'a BTreeSet<String>,
    document_file: File,
    metadata_file: File,
    vector_file: File,
    documents: Vec<SearchDocument>,
    segment_encoded_bytes: u64,
    descriptor: SearchSegmentDescriptor,
    layouts: Vec<SearchOutOfCoreSegmentLayout>,
    document_offset: u64,
    metadata_offset: u64,
    vector_offset: u64,
    next_vector_ordinal: u64,
    descriptor_working_bytes: u64,
    peak_segment_document_count: usize,
    peak_segment_encoded_bytes: u64,
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
}

impl<'a> SegmentArtifactBuilder<'a> {
    pub(super) fn new(
        stage: &Path,
        generation: u64,
        fields: &'a BTreeSet<String>,
        options: &'a SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<Self> {
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
            descriptor_working_bytes: 0,
            peak_segment_document_count: 0,
            peak_segment_encoded_bytes: 0,
        })
    }

    #[cfg(test)]
    pub(super) fn build(mut self, source: &SpoolSource) -> Result<SegmentArtifactOutput> {
        source.scan(&mut |document| self.push(document))?;
        self.finish(source.document_count)
    }

    pub(super) fn finish(mut self, document_count: usize) -> Result<SegmentArtifactOutput> {
        self.flush_segment()?;
        self.document_file.sync_all()?;
        self.metadata_file.sync_all()?;
        self.vector_file.sync_all()?;
        self.descriptor.document_count = document_count;
        let descriptor_bytes = write_search_segment_descriptor_bounded(
            &self.stage,
            &self.descriptor,
            self.options.max_descriptor_working_bytes.get(),
        )?;
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
        })
    }

    pub(super) fn push(&mut self, document: SearchDocument) -> Result<()> {
        let encoded_bytes = DocumentEncoding::new(&document)?.len() as u64;
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
        Ok(())
    }

    fn flush_segment(&mut self) -> Result<()> {
        if self.documents.is_empty() {
            return Ok(());
        }
        let segment_id = self.descriptor.segments.len() as u64;
        let (mut descriptor, projected_descriptor_bytes) = descriptor::build(
            segment_id,
            &self.documents,
            self.fields,
            self.descriptor_working_bytes,
            self.options.max_descriptor_working_bytes.get(),
        )?;

        let document_payload = self.encode_segment_payload(segment_id, SegmentKind::Documents)?;
        let document_length = document_payload.len() as u64;
        self.document_file.write_all(&document_payload)?;
        descriptor.payload_range = Some(SearchSegmentPayloadRange {
            artifact_id: SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            offset: self.document_offset,
            length: document_length,
            checksum: checksum_bytes(&document_payload),
        });
        self.document_offset = self
            .document_offset
            .checked_add(document_length)
            .ok_or_else(|| SkeinError::Storage("search generation payload overflow".to_string()))?;
        drop(document_payload);

        let vector_ordinal_base = self.next_vector_ordinal;
        let metadata_payload = self.encode_segment_payload(
            segment_id,
            SegmentKind::Metadata {
                vector_ordinal_base,
            },
        )?;
        let metadata = append_sidecar_payload(
            &mut self.metadata_file,
            &mut self.metadata_offset,
            &metadata_payload,
            self.documents.len(),
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
        let vectors = append_sidecar_payload(
            &mut self.vector_file,
            &mut self.vector_offset,
            &vector_payload,
            vector_count,
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

    fn encode_segment_payload(&self, segment_id: u64, kind: SegmentKind) -> Result<Vec<u8>> {
        let encoding = SegmentEncoding::new(&self.documents, kind)?;
        encoding::encode_segment_payload(
            &encoding,
            segment_id,
            kind.name(),
            self.options.max_segment_uncompressed_bytes.get(),
            self.options.max_segment_compressed_bytes.get(),
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
