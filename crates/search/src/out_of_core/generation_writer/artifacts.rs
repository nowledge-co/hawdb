use super::super::{
    append_sidecar_payload, SearchOutOfCoreLayoutBody, SearchOutOfCoreSegmentLayout,
    OUT_OF_CORE_LAYOUT_FORMAT,
};
#[cfg(test)]
use super::spool::SpoolSource;
use super::{SearchOutOfCoreGenerationBuildOptions, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::document_encoding::DocumentEncoding;
use crate::error::{Result, SkeinError};
use crate::{
    checksum_bytes, encode_embedding, encode_metadata, encode_search_document_line,
    encode_search_snapshot_text, encode_string, write_search_segment_descriptor, SearchDocument,
    SearchSegmentDescriptor, SearchSegmentDescriptorEntry, SearchSegmentPayloadRange,
    SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS, SEARCH_SEGMENT_DESCRIPTOR_FILE,
    SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID, SEARCH_SEGMENT_PAYLOAD_FILE,
};
use std::collections::BTreeSet;
use std::fmt::Write as FmtWrite;
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
        write_search_segment_descriptor(&self.stage, &self.descriptor)?;
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
        let references = self.documents.iter().collect::<Vec<_>>();
        let mut descriptor =
            SearchSegmentDescriptorEntry::from_documents(segment_id, &references, self.fields);

        let mut document_body =
            String::with_capacity(usize::try_from(self.segment_encoded_bytes).unwrap_or_default());
        document_body.push_str("SKEIN_SEARCH_SEGMENT_V1\n");
        for document in &self.documents {
            document_body.push_str(&encode_search_document_line(document));
        }
        let document_payload =
            self.encode_segment_payload(segment_id, "document", document_body)?;
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

        let mut metadata_body = String::from("SKEIN_SEARCH_METADATA_SEGMENT_V1\n");
        let vector_ordinal_base = self.next_vector_ordinal;
        let mut next_vector_ordinal = vector_ordinal_base;
        for document in &self.documents {
            let vector_ordinal = document.embedding.as_ref().map(|_| {
                let ordinal = next_vector_ordinal;
                next_vector_ordinal = next_vector_ordinal.saturating_add(1);
                ordinal
            });
            writeln!(
                metadata_body,
                "meta\t{}\t{}\t{}",
                encode_string(&document.id),
                vector_ordinal
                    .map(|ordinal| ordinal.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                encode_metadata(&document.metadata)
            )
            .map_err(|_| SkeinError::Storage("search metadata encoding failed".to_string()))?;
        }
        let metadata_payload =
            self.encode_segment_payload(segment_id, "metadata", metadata_body)?;
        let metadata = append_sidecar_payload(
            &mut self.metadata_file,
            &mut self.metadata_offset,
            &metadata_payload,
            self.documents.len(),
        )?;
        drop(metadata_payload);

        let mut vector_body = String::from("SKEIN_SEARCH_VECTOR_SEGMENT_V1\n");
        let mut vector_count = 0usize;
        for document in &self.documents {
            if let Some(embedding) = document.embedding.as_deref() {
                writeln!(
                    vector_body,
                    "vector\t{}\t{}\t{}",
                    vector_ordinal_base.saturating_add(vector_count as u64),
                    encode_string(&document.id),
                    encode_embedding(Some(embedding))
                )
                .map_err(|_| SkeinError::Storage("search vector encoding failed".to_string()))?;
                vector_count = vector_count.saturating_add(1);
            }
        }
        self.next_vector_ordinal = next_vector_ordinal;
        let vector_payload = self.encode_segment_payload(segment_id, "vector", vector_body)?;
        let vectors = append_sidecar_payload(
            &mut self.vector_file,
            &mut self.vector_offset,
            &vector_payload,
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

    fn encode_segment_payload(&self, segment_id: u64, name: &str, body: String) -> Result<Vec<u8>> {
        if body.len() as u64 > self.options.max_segment_uncompressed_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation {name} segment {segment_id} requires {} bytes, exceeding {}",
                body.len(),
                self.options.max_segment_uncompressed_bytes
            )));
        }
        let payload = encode_search_snapshot_text(&body)?;
        if payload.len() as u64 > self.options.max_segment_compressed_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation {name} segment {segment_id} requires {} compressed bytes, exceeding {}",
                payload.len(), self.options.max_segment_compressed_bytes
            )));
        }
        Ok(payload)
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
