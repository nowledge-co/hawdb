use super::next_generation;
use crate::build_control::checkpoint;
use crate::build_memory::{
    checked_add, checked_mul, BuildMemory, SET_ENTRY_BYTES, SPOOL_BUFFER_BYTES,
};
#[cfg(test)]
use crate::encode_search_document_line;
use crate::error::{Result, SkeinError};
use crate::generation_cleanup::{
    SearchProjectionCleanupOptions, SearchProjectionCleanupState, SearchProjectionGenerations,
};
use crate::lexical_projection::{
    analyzer_digest as lexical_analyzer_digest, artifact_file as lexical_artifact_file,
    LexicalProjectionConfig, LexicalProjectionWriter, MANIFEST_FILE as LEXICAL_MANIFEST_FILE,
};
use crate::{
    checksum_bytes, SearchAnalyzerLexicon, SearchDocument, SearchEmbeddingManifest,
    NOWLEDGE_MEMORY_MATERIALIZED_METADATA_PATHS, NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
    SEARCH_DOCUMENT_ID_FIELD,
};
use artifacts::SegmentArtifactBuilder;
#[cfg(test)]
use publication::file_len_checksum;
use publication::{publish_generation, PublishGenerationInput};
use rabitq::RaBitQArtifactBuilder;
use serde::Serialize;
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher;
use spool::{SpoolSource, StageDirectory, SPOOL_FRAME_HEADER_BYTES, SPOOL_HEADER};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

mod artifact_paths;
mod artifacts;
mod context_memory;
mod delta;
mod delta_memory;
mod publication;
mod rabitq;
#[cfg(feature = "vector-search")]
mod rabitq_memory;
mod segment_io;
mod segment_memory;
mod spool;
#[cfg(test)]
mod tests;

pub use delta::SearchOutOfCoreGenerationUpdate;

const STAGE_METADATA_FILE: &str = "search_projection_metadata_payloads.stage.skein";
const STAGE_VECTOR_FILE: &str = "search_projection_vector_payloads.stage.skein";

/// Explicit admission limits and immutable identity for a streaming out-of-core build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutOfCoreGenerationBuildOptions {
    pub max_documents: NonZeroUsize,
    pub max_logical_document_bytes: NonZeroU64,
    pub max_spool_bytes: NonZeroU64,
    pub max_record_bytes: NonZeroU64,
    pub max_delta_operations: NonZeroUsize,
    pub max_delta_working_bytes: NonZeroU64,
    pub max_segment_uncompressed_bytes: NonZeroU64,
    pub max_segment_compressed_bytes: NonZeroU64,
    pub max_generation_bytes: NonZeroU64,
    pub max_descriptor_working_bytes: NonZeroU64,
    pub max_metadata_fields: NonZeroUsize,
    pub max_metadata_field_bytes: NonZeroU64,
    pub lexical_build_memory_bytes: NonZeroU64,
    pub lexical_dictionary_build_memory_bytes: NonZeroU64,
    pub lexical_dictionary_validation_bytes: NonZeroU64,
    pub lexical_max_directory_bytes: NonZeroU64,
    pub lexical_max_spill_bytes: NonZeroU64,
    pub lexical_max_spill_runs: NonZeroUsize,
    pub lexical_max_merge_fan_in: NonZeroUsize,
    pub lexical_max_document_source_bytes: NonZeroU64,
    pub rabitq_segment_rows: NonZeroUsize,
    pub rabitq_build_memory_bytes: NonZeroUsize,
    pub rabitq_transform_seed: u64,
    #[cfg(feature = "vector-search")]
    pub rabitq_bit_width: skein_vector_projection::RaBitQBitWidth,
    pub source_graph_commit_epoch: Option<u64>,
    pub import_source_graph_commit_epoch: Option<u64>,
    pub embedding_manifest: Option<SearchEmbeddingManifest>,
    pub analyzer_lexicon: SearchAnalyzerLexicon,
    pub cleanup_options: SearchProjectionCleanupOptions,
}

impl Default for SearchOutOfCoreGenerationBuildOptions {
    fn default() -> Self {
        Self {
            max_documents: NonZeroUsize::new(100_000_000).unwrap(),
            max_logical_document_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
            max_spool_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
            max_record_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            max_delta_operations: NonZeroUsize::new(1_000_000).unwrap(),
            max_delta_working_bytes: NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
            max_segment_uncompressed_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            max_segment_compressed_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_generation_bytes: NonZeroU64::new(8 * 1024 * 1024 * 1024 * 1024).unwrap(),
            max_descriptor_working_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            max_metadata_fields: NonZeroUsize::new(256).unwrap(),
            max_metadata_field_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            lexical_build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            lexical_dictionary_build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            lexical_dictionary_validation_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            lexical_max_directory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            lexical_max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
            lexical_max_spill_runs: NonZeroUsize::new(4_096).unwrap(),
            lexical_max_merge_fan_in: NonZeroUsize::new(32).unwrap(),
            lexical_max_document_source_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            rabitq_segment_rows: NonZeroUsize::new(1_024).unwrap(),
            rabitq_build_memory_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            rabitq_transform_seed: 0x534b_4549_4e56_5134,
            #[cfg(feature = "vector-search")]
            rabitq_bit_width: skein_vector_projection::RaBitQBitWidth::default(),
            source_graph_commit_epoch: None,
            import_source_graph_commit_epoch: None,
            embedding_manifest: None,
            analyzer_lexicon: SearchAnalyzerLexicon::default(),
            cleanup_options: SearchProjectionCleanupOptions::default(),
        }
    }
}

/// Resource and identity evidence emitted by a streaming out-of-core generation build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SearchOutOfCoreGenerationBuildReport {
    pub generation: u64,
    pub lexical_generation: u64,
    pub document_count: usize,
    pub vector_document_count: usize,
    pub documents_digest: u64,
    pub logical_document_bytes: u64,
    pub spool_bytes: u64,
    pub peak_record_bytes: u64,
    pub peak_segment_document_count: usize,
    pub peak_segment_encoded_bytes: u64,
    pub descriptor_working_bytes: u64,
    pub descriptor_bytes: u64,
    pub document_payload_bytes: u64,
    pub metadata_payload_bytes: u64,
    pub vector_payload_bytes: u64,
    pub lexical_artifact_bytes: u64,
    pub lexical_manifest_bytes: u64,
    pub lexical_byte_counters: crate::SearchLexicalArtifactBytes,
    pub rabitq_artifact_bytes: u64,
    pub rabitq_source_digest: Option<u64>,
    pub rabitq_peak_build_working_bytes: usize,
    pub manifest_bytes: u64,
    pub generation_bytes: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub embedding_dimension: Option<usize>,
    pub resident_document_count: usize,
    pub active_manifest_published_last: bool,
    pub cleanup_deleted_files: usize,
    pub cleanup_pending_files: usize,
    pub cleanup_retry_required: bool,
}

/// Builds an immutable search generation without retaining the full document corpus.
///
/// Documents must be pushed in strictly increasing UTF-8 ID order. The writer
/// keeps only one encoded input record while spooling and at most one descriptor
/// segment while producing document and sidecar payloads. The descriptor itself
/// remains bounded by `max_descriptor_working_bytes` because the serving reader
/// must retain that range index.
pub struct SearchOutOfCoreGenerationWriter {
    root: context_memory::OwnedPath,
    spool_path: context_memory::OwnedPath,
    // Close the spool before stage cleanup, including on Windows.
    spool: Option<BufWriter<File>>,
    stage: StageDirectory,
    options: context_memory::Options,
    last_document_id: Option<String>,
    document_count: usize,
    vector_document_count: usize,
    logical_document_bytes: u64,
    spool_bytes: u64,
    peak_record_bytes: u64,
    embedding_dimension: Option<usize>,
    documents_digest: Crc32cHasher,
    metadata_fields: BTreeSet<String>,
    metadata_field_bytes: u64,
    expected_active_generation: Option<u64>,
    poisoned: bool,
    task_context: RuntimeTaskContext,
    memory: BuildMemory,
    // Keep charges after the payload fields so data drops before its leases.
    spool_memory: Option<QueryMemoryLease>,
    metadata_memory: QueryMemoryLease,
    last_id_memory: Option<QueryMemoryLease>,
}

impl std::fmt::Debug for SearchOutOfCoreGenerationWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SearchOutOfCoreGenerationWriter")
            .field("root", &self.root)
            .field("stage", &self.stage.path)
            .field("document_count", &self.document_count)
            .field("logical_document_bytes", &self.logical_document_bytes)
            .field("spool_bytes", &self.spool_bytes)
            .field(
                "expected_active_generation",
                &self.expected_active_generation,
            )
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl SearchOutOfCoreGenerationWriter {
    pub fn create(
        root: impl AsRef<Path>,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<Self> {
        Self::create_with_context(root, options, RuntimeTaskContext::default())
    }

    /// Builds with cooperative cancellation/deadline checks through spooling and
    /// artifact preparation. Cancellation is checked before final publication;
    /// once that commit section starts, it completes without observing late
    /// cancellation. Individual filesystem and bounded codec calls are not
    /// interruptible. The caller retains ownership of its governor admission.
    /// An optional task memory reservation is shared by owned input, spool
    /// buffers, decoded/segment documents, lexical frequency sets and posting
    /// chunks. Component caps still apply. Identifier/Jieba scratch, codecs,
    /// descriptors and vector working sets are not yet charged to this ledger;
    /// this is not total build admission.
    pub fn create_with_context(
        root: impl AsRef<Path>,
        options: SearchOutOfCoreGenerationBuildOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task_context)?;
        validate_options(&options)?;
        let memory = BuildMemory::new(&task_context)?;
        let options = context_memory::Options::new(options, &memory, &task_context)?;
        Self::create_with_memory(root, options, task_context, memory)
    }

    // Delta admission creates the same root before converting input rows.
    fn create_with_memory(
        root: impl AsRef<Path>,
        options: context_memory::Options,
        task_context: RuntimeTaskContext,
        memory: BuildMemory,
    ) -> Result<Self> {
        checkpoint(&task_context)?;
        validate_options(&options)?;
        let metadata_bytes = required_descriptor_field_names().try_fold(0, |bytes, field| {
            checked_add(bytes, checked_add(SET_ENTRY_BYTES, field.len())?)
        })?;
        let metadata_memory = memory.retained.reserve(metadata_bytes)?;
        let spool_memory = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let root = context_memory::OwnedPath::copy(root.as_ref(), &memory, &task_context)?;
        fs::create_dir_all(&root)?;
        let stage = StageDirectory::create(&root, &memory, &task_context)?;
        let spool_path = context_memory::OwnedPath::join(
            &stage.path,
            Path::new("documents.spool.skein"),
            &memory,
            &task_context,
        )?;
        let mut spool = BufWriter::with_capacity(
            SPOOL_BUFFER_BYTES,
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&spool_path)?,
        );
        spool.write_all(SPOOL_HEADER)?;
        let metadata_fields = required_descriptor_fields();
        let metadata_field_bytes = metadata_fields.iter().map(|field| field.len() as u64).sum();
        if metadata_fields.len() > options.max_metadata_fields.get()
            || metadata_field_bytes > options.max_metadata_field_bytes.get()
        {
            return Err(SkeinError::Storage(
                "search generation descriptor field admission is smaller than the required field set"
                    .to_string(),
            ));
        }
        let embedding_dimension = options
            .embedding_manifest
            .as_ref()
            .map(|manifest| manifest.dimension);
        Ok(Self {
            root,
            spool_path,
            spool: Some(spool),
            stage,
            options,
            last_document_id: None,
            document_count: 0,
            vector_document_count: 0,
            logical_document_bytes: 0,
            spool_bytes: SPOOL_HEADER.len() as u64,
            peak_record_bytes: 0,
            embedding_dimension,
            documents_digest: Crc32cHasher::new(),
            metadata_fields,
            metadata_field_bytes,
            expected_active_generation: None,
            poisoned: false,
            task_context,
            memory,
            spool_memory: Some(spool_memory),
            metadata_memory,
            last_id_memory: None,
        })
    }

    pub fn push(&mut self, document: SearchDocument) -> Result<()> {
        if self.poisoned {
            return Err(SkeinError::Storage(
                "search generation writer is poisoned after an earlier input failure".to_string(),
            ));
        }
        let result = self.push_inner(document);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub fn prepare_delta(
        reader: &super::SearchOutOfCoreReader,
        delta: crate::SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<SearchOutOfCoreGenerationUpdate> {
        SearchOutOfCoreGenerationUpdate::prepare(reader, delta, options)
    }

    /// Keeps the same build context through delta scanning, spooling and finish.
    pub fn prepare_delta_with_context(
        reader: &super::SearchOutOfCoreReader,
        delta: crate::SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreGenerationUpdate> {
        SearchOutOfCoreGenerationUpdate::prepare_with_context(reader, delta, options, task_context)
    }

    pub fn finish(self) -> Result<SearchOutOfCoreGenerationBuildReport> {
        self.finish_with_artifacts(Self::build_artifacts)
    }

    fn finish_with_artifacts(
        mut self,
        build: impl FnOnce(&Self, &SpoolSource<'_>, u64) -> Result<GenerationArtifacts>,
    ) -> Result<SearchOutOfCoreGenerationBuildReport> {
        checkpoint(&self.task_context)?;
        if self.poisoned {
            return Err(SkeinError::Storage(
                "cannot finish a poisoned search generation writer".to_string(),
            ));
        }
        let mut spool = self.spool.take().ok_or_else(|| {
            SkeinError::Storage("search generation spool is already closed".to_string())
        })?;
        spool.flush()?;
        spool.get_ref().sync_all()?;
        drop(spool);
        self.spool_memory.take();
        checkpoint(&self.task_context)?;
        let actual_spool_bytes = fs::metadata(&self.spool_path)?.len();
        if actual_spool_bytes != self.spool_bytes {
            return Err(SkeinError::Storage(format!(
                "search generation spool length changed: expected {}, got {actual_spool_bytes}",
                self.spool_bytes
            )));
        }

        let _publish_lease = super::SearchProjectionPublishLease::acquire(&self.root)?;
        checkpoint(&self.task_context)?;
        if let Some(expected) = self.expected_active_generation {
            let actual = super::active_manifest_generation(&self.root)?;
            if actual != Some(expected) {
                return Err(SkeinError::Storage(format!(
                    "search generation update base changed before publication: expected {expected}, got {actual:?}"
                )));
            }
        }

        let source = SpoolSource {
            path: &self.spool_path,
            document_count: self.document_count,
            max_record_bytes: self.options.max_record_bytes.get(),
            max_metadata_fields: self.options.max_metadata_fields.get(),
            memory: self.memory.clone(),
        };
        let generation = next_generation(&self.root)?;
        let lexical_generation = generation;
        let GenerationArtifacts {
            segment: segment_output,
            lexical_artifact_name,
            lexical_artifact_bytes,
            lexical_manifest_bytes,
            lexical_byte_counters,
            rabitq,
        } = build(&self, &source, generation)?;

        let published = publish_generation(PublishGenerationInput {
            task_context: &self.task_context,
            memory: &self.memory,
            root: &self.root,
            stage: &self.stage.path,
            generation,
            document_count: self.document_count,
            documents_digest: self.documents_digest.finish(),
            source_graph_commit_epoch: self.options.source_graph_commit_epoch,
            import_source_graph_commit_epoch: self.options.import_source_graph_commit_epoch,
            embedding_manifest: self.options.embedding_manifest.as_ref(),
            embedding_dimension: self.embedding_dimension,
            layout: &segment_output.layout,
            lexical_artifact_name: &lexical_artifact_name,
            rabitq: rabitq.as_ref(),
            payload_bytes: segment_output.document_payload_bytes,
            metadata_payload_bytes: segment_output.metadata_payload_bytes,
            vector_payload_bytes: segment_output.vector_payload_bytes,
            max_generation_bytes: self.options.max_generation_bytes.get(),
        })?;

        let mut cleanup_state = SearchProjectionCleanupState::default();
        let cleanup = cleanup_state.run(
            &self.root,
            SearchProjectionGenerations {
                lexical: Some(lexical_generation),
                out_of_core: Some(generation),
                rabitq: rabitq.as_ref().map(|_| generation),
                rabitq_remove_all: rabitq.is_none(),
                out_of_core_discovery_failed: false,
            },
            self.options.cleanup_options,
        );

        Ok(SearchOutOfCoreGenerationBuildReport {
            generation,
            lexical_generation,
            document_count: self.document_count,
            vector_document_count: self.vector_document_count,
            documents_digest: self.documents_digest.finish(),
            logical_document_bytes: self.logical_document_bytes,
            spool_bytes: self.spool_bytes,
            peak_record_bytes: self.peak_record_bytes,
            peak_segment_document_count: segment_output.peak_segment_document_count,
            peak_segment_encoded_bytes: segment_output.peak_segment_encoded_bytes,
            descriptor_working_bytes: segment_output.descriptor_working_bytes,
            descriptor_bytes: segment_output.descriptor_bytes,
            document_payload_bytes: segment_output.document_payload_bytes,
            metadata_payload_bytes: segment_output.metadata_payload_bytes,
            vector_payload_bytes: segment_output.vector_payload_bytes,
            lexical_artifact_bytes,
            lexical_manifest_bytes,
            lexical_byte_counters,
            rabitq_artifact_bytes: rabitq
                .as_ref()
                .map_or(0, |artifact| artifact.artifact_bytes),
            rabitq_source_digest: rabitq.as_ref().map(|artifact| artifact.source_digest),
            rabitq_peak_build_working_bytes: rabitq
                .as_ref()
                .map_or(0, |artifact| artifact.peak_build_working_bytes),
            manifest_bytes: published.manifest_bytes,
            generation_bytes: published.generation_bytes,
            source_graph_commit_epoch: self.options.source_graph_commit_epoch,
            embedding_dimension: self.embedding_dimension,
            resident_document_count: 0,
            active_manifest_published_last: true,
            cleanup_deleted_files: cleanup.deleted_files,
            cleanup_pending_files: cleanup.pending_after,
            cleanup_retry_required: cleanup.retry_required,
        })
    }

    fn build_artifacts(
        &self,
        source: &SpoolSource<'_>,
        generation: u64,
    ) -> Result<GenerationArtifacts> {
        let mut segments = SegmentArtifactBuilder::new_with_context(
            &self.stage.path,
            generation,
            &self.metadata_fields,
            &self.options,
            self.memory.clone(),
            self.task_context.clone(),
        )?;
        let mut vectors = RaBitQArtifactBuilder::new(self, generation)?;
        let mut completed = None;
        let lexical_config = LexicalProjectionConfig {
            build_memory_bytes: self.options.lexical_build_memory_bytes,
            dictionary_build_memory_bytes: self.options.lexical_dictionary_build_memory_bytes,
            dictionary_validation_bytes: self.options.lexical_dictionary_validation_bytes,
            max_directory_bytes: self.options.lexical_max_directory_bytes,
            max_spill_bytes: self.options.lexical_max_spill_bytes,
            max_spill_runs: self.options.lexical_max_spill_runs,
            max_merge_fan_in: self.options.lexical_max_merge_fan_in,
            max_document_source_bytes: self.options.lexical_max_document_source_bytes,
            ..LexicalProjectionConfig::default()
        };
        let lexical = LexicalProjectionWriter::new(lexical_config)
            .with_context(self.task_context.clone())
            .with_memory(self.memory.clone())
            .write_scanned(
                &self.stage.path,
                generation,
                self.options.source_graph_commit_epoch,
                lexical_analyzer_digest(&self.options.analyzer_lexicon),
                self.documents_digest.finish(),
                |consume| {
                    source.scan_admitted(&self.task_context, &mut |ordinal, document| {
                        consume(ordinal, &document)?;
                        vectors.push(&document)?;
                        segments.push_admitted(ordinal, document)
                    })?;
                    // Drop both writers' buffers before lexical external merge.
                    // All artifacts remain private to the stage until publication.
                    completed = Some((segments.finish(source.document_count)?, vectors.finish()?));
                    Ok(())
                },
                &self.options.analyzer_lexicon,
            )?;
        let lexical_byte_counters = lexical.artifact_bytes();
        drop(lexical);
        checkpoint(&self.task_context)?;
        let (segment, rabitq) = completed.expect("lexical build completed its input scan");
        let lexical_paths = artifact_paths::Lexical::new(
            &self.stage.path,
            generation,
            &self.memory,
            &self.task_context,
        )?;
        let (lexical_artifact_bytes, _) = publication::file_len_checksum_with_context(
            &lexical_paths.artifact,
            &self.task_context,
        )?;
        let (lexical_manifest_bytes, _) = publication::file_len_checksum_with_context(
            &lexical_paths.manifest,
            &self.task_context,
        )?;
        Ok(GenerationArtifacts {
            segment,
            lexical_artifact_name: lexical_paths.name,
            lexical_artifact_bytes,
            lexical_manifest_bytes,
            lexical_byte_counters,
            rabitq,
        })
    }

    fn push_inner(&mut self, document: SearchDocument) -> Result<()> {
        checkpoint(&self.task_context)?;
        let document = self.memory.admit_document(document)?;
        if document.id.is_empty() {
            return Err(SkeinError::Storage(
                "search generation document id must not be empty".to_string(),
            ));
        }
        if self
            .last_document_id
            .as_ref()
            .is_some_and(|previous| previous >= &document.id)
        {
            return Err(SkeinError::Storage(format!(
                "search generation document ids must be strictly increasing: previous {:?}, next {:?}",
                self.last_document_id.as_deref().unwrap_or_default(),
                document.id
            )));
        }
        if self.document_count >= self.options.max_documents.get() {
            return Err(SkeinError::Storage(format!(
                "search generation exceeds the admitted {} documents",
                self.options.max_documents
            )));
        }
        let next_dimension = validate_embedding(
            &document,
            self.embedding_dimension,
            self.options.embedding_manifest.as_ref(),
        )?;
        let new_fields = document
            .metadata
            .keys()
            .filter(|field| !self.metadata_fields.contains(*field));
        let (added_fields, added_field_bytes) =
            new_fields.fold((0usize, 0u64), |(count, bytes), field| {
                (
                    count.saturating_add(1),
                    bytes.saturating_add(field.len() as u64),
                )
            });
        let next_field_count = self.metadata_fields.len().saturating_add(added_fields);
        let next_field_bytes = self.metadata_field_bytes.saturating_add(added_field_bytes);
        if next_field_count > self.options.max_metadata_fields.get()
            || next_field_bytes > self.options.max_metadata_field_bytes.get()
        {
            return Err(SkeinError::Storage(format!(
                "search generation metadata fields require {next_field_count} fields and {next_field_bytes} bytes, exceeding {} fields or {} bytes",
                self.options.max_metadata_fields, self.options.max_metadata_field_bytes
            )));
        }
        let record_limit = self
            .options
            .max_record_bytes
            .get()
            .min(
                self.options
                    .max_logical_document_bytes
                    .get()
                    .saturating_sub(self.logical_document_bytes),
            )
            .min(
                self.options
                    .max_spool_bytes
                    .get()
                    .saturating_sub(self.spool_bytes)
                    .saturating_sub(SPOOL_FRAME_HEADER_BYTES),
            );
        let record_len =
            crate::document_codec::encoded_len(&document, record_limit, Some(&self.task_context))?;
        let _record_memory = self.memory.spool.reserve(record_len)?;
        let record = crate::document_codec::encode_admitted(
            &document,
            record_len,
            Some(&self.task_context),
        )?;
        checkpoint(&self.task_context)?;
        let record_bytes = record.len() as u64;
        let logical_document_bytes = self
            .logical_document_bytes
            .checked_add(record_bytes)
            .ok_or_else(|| {
                SkeinError::Storage("search generation byte count overflow".to_string())
            })?;
        if logical_document_bytes > self.options.max_logical_document_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation requires {logical_document_bytes} logical bytes, exceeding {}",
                self.options.max_logical_document_bytes
            )));
        }
        let spool_bytes = self
            .spool_bytes
            .checked_add(SPOOL_FRAME_HEADER_BYTES)
            .and_then(|bytes| bytes.checked_add(record_bytes))
            .ok_or_else(|| {
                SkeinError::Storage("search generation spool size overflow".to_string())
            })?;
        if spool_bytes > self.options.max_spool_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "search generation spool requires {spool_bytes} bytes, exceeding {}",
                self.options.max_spool_bytes
            )));
        }
        let last_id_memory = self.memory.retained.reserve(document.id.capacity())?;
        self.metadata_memory.grow(checked_add(
            checked_mul(added_fields, SET_ENTRY_BYTES)?,
            usize::try_from(added_field_bytes).map_err(|_| {
                SkeinError::Execution("metadata field capacity exceeds usize".to_string())
            })?,
        )?)?;
        let spool = self.spool.as_mut().ok_or_else(|| {
            SkeinError::Storage("search generation spool is already closed".to_string())
        })?;
        spool.write_all(&record_bytes.to_le_bytes())?;
        spool.write_all(&checksum_bytes(record.as_bytes()).to_le_bytes())?;
        spool.write_all(record.as_bytes())?;
        checkpoint(&self.task_context)?;

        self.documents_digest.update(record.as_bytes());
        self.document_count = self.document_count.saturating_add(1);
        if document.embedding.is_some() {
            self.vector_document_count = self.vector_document_count.saturating_add(1);
        }
        self.logical_document_bytes = logical_document_bytes;
        self.spool_bytes = spool_bytes;
        self.peak_record_bytes = self.peak_record_bytes.max(record_bytes);
        self.embedding_dimension = next_dimension;
        for field in document.metadata.keys() {
            if !self.metadata_fields.contains(field) {
                self.metadata_fields.insert(field.clone());
            }
        }
        self.last_document_id = Some(document.document.id);
        self.last_id_memory = Some(last_id_memory);
        self.metadata_field_bytes = next_field_bytes;
        Ok(())
    }
}

struct GenerationArtifacts {
    segment: artifacts::SegmentArtifactOutput,
    lexical_artifact_name: artifact_paths::Name,
    lexical_artifact_bytes: u64,
    lexical_manifest_bytes: u64,
    lexical_byte_counters: crate::SearchLexicalArtifactBytes,
    rabitq: Option<RaBitQGenerationArtifact>,
}

#[derive(Debug)]
pub(super) struct RaBitQGenerationArtifact {
    file_name: artifact_paths::Name,
    pub(super) artifact_bytes: u64,
    pub(super) artifact_checksum: u64,
    pub(super) source_digest: u64,
    pub(super) document_count: usize,
    pub(super) payload_checksum: u32,
    pub(super) peak_build_working_bytes: usize,
}

#[cfg(all(test, feature = "vector-search"))]
fn build_rabitq_artifact(
    source: &SpoolSource<'_>,
    stage: &Path,
    generation: u64,
    vector_document_count: usize,
    embedding_dimension: Option<usize>,
    embedding_manifest: Option<&SearchEmbeddingManifest>,
    options: &SearchOutOfCoreGenerationBuildOptions,
) -> Result<Option<RaBitQGenerationArtifact>> {
    if vector_document_count == 0 {
        return Ok(None);
    }
    let dimension = embedding_dimension.ok_or_else(|| {
        SkeinError::Storage(
            "search generation has vector documents without an embedding dimension".to_string(),
        )
    })?;
    let file_name =
        artifact_paths::Name::rabitq(generation, &source.memory, &RuntimeTaskContext::default())?;
    let path = stage.join(&file_name);
    let identity = skein_vector_projection::ProjectionIdentity {
        generation,
        source_epoch: options.source_graph_commit_epoch,
        embedding_model: embedding_manifest.map(|manifest| manifest.model.clone()),
        embedding_version: embedding_manifest.and_then(|manifest| manifest.version.clone()),
    };
    let config = skein_vector_projection::ProjectionBuildConfig::new(dimension, identity)
        .with_bit_width(options.rabitq_bit_width)
        .with_segment_rows(options.rabitq_segment_rows.get())
        .with_max_working_bytes(options.rabitq_build_memory_bytes.get())
        .with_transform_seed(options.rabitq_transform_seed);
    let mut writer =
        skein_vector_projection::ProjectionWriter::create(&path, config).map_err(rabitq_error)?;
    let mut vector_ordinal = 0u64;
    source.scan(&mut |_, document| {
        let Some(embedding) = document.embedding.as_deref() else {
            return Ok(());
        };
        writer
            .push(vector_ordinal, embedding)
            .map_err(rabitq_error)?;
        vector_ordinal = vector_ordinal
            .checked_add(1)
            .ok_or_else(|| SkeinError::Storage("search vector ordinal overflow".to_string()))?;
        Ok(())
    })?;
    if vector_ordinal != vector_document_count as u64 {
        return Err(SkeinError::Storage(
            "search RaBitQ build did not consume the expected vector document count".to_string(),
        ));
    }
    let projection = writer.finish().map_err(rabitq_error)?;
    let manifest = projection.manifest();
    let (artifact_bytes, artifact_checksum) = file_len_checksum(&path)?;
    Ok(Some(RaBitQGenerationArtifact {
        file_name,
        artifact_bytes,
        artifact_checksum,
        source_digest: manifest.source_digest,
        document_count: manifest.document_count,
        payload_checksum: manifest.payload_checksum,
        peak_build_working_bytes: manifest.peak_build_working_bytes,
    }))
}

#[cfg(all(test, not(feature = "vector-search")))]
fn build_rabitq_artifact(
    _source: &SpoolSource<'_>,
    _stage: &Path,
    _generation: u64,
    _vector_document_count: usize,
    _embedding_dimension: Option<usize>,
    _embedding_manifest: Option<&SearchEmbeddingManifest>,
    _options: &SearchOutOfCoreGenerationBuildOptions,
) -> Result<Option<RaBitQGenerationArtifact>> {
    Ok(None)
}

#[cfg(all(test, feature = "vector-search"))]
fn rabitq_error(error: skein_vector_projection::ProjectionError) -> SkeinError {
    SkeinError::Storage(format!("search RaBitQ projection: {error}"))
}

fn validate_options(options: &SearchOutOfCoreGenerationBuildOptions) -> Result<()> {
    if options.max_record_bytes.get() > options.max_segment_uncompressed_bytes.get() {
        return Err(SkeinError::Storage(
            "search generation max_record_bytes exceeds max_segment_uncompressed_bytes".to_string(),
        ));
    }
    if options.lexical_max_merge_fan_in.get() < 2 {
        return Err(SkeinError::Storage(
            "search generation lexical merge fan-in must be at least two".to_string(),
        ));
    }
    if let Some(manifest) = &options.embedding_manifest
        && (manifest.model.trim().is_empty() || manifest.dimension == 0)
    {
        return Err(SkeinError::Storage(
            "search generation embedding manifest requires a model and non-zero dimension"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_embedding(
    document: &SearchDocument,
    current_dimension: Option<usize>,
    manifest: Option<&SearchEmbeddingManifest>,
) -> Result<Option<usize>> {
    let Some(embedding) = document.embedding.as_deref() else {
        return Ok(current_dimension);
    };
    if embedding.is_empty() || !embedding.iter().all(|value| value.is_finite()) {
        return Err(SkeinError::Storage(format!(
            "search generation document {} has an empty or non-finite embedding",
            document.id
        )));
    }
    if let Some(manifest) = manifest
        && manifest.dimension != embedding.len()
    {
        return Err(SkeinError::Storage(format!(
            "search generation embedding manifest expects dimension {}, document {} has {}",
            manifest.dimension,
            document.id,
            embedding.len()
        )));
    }
    if let Some(dimension) = current_dimension
        && dimension != embedding.len()
    {
        return Err(SkeinError::Storage(format!(
            "search generation embedding dimension mismatch: expected {dimension}, document {} has {}",
            document.id,
            embedding.len()
        )));
    }
    Ok(Some(embedding.len()))
}

fn required_descriptor_fields() -> BTreeSet<String> {
    required_descriptor_field_names()
        .map(str::to_string)
        .collect()
}

fn required_descriptor_field_names() -> impl Iterator<Item = &'static str> {
    std::iter::once(SEARCH_DOCUMENT_ID_FIELD)
        .chain(
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
                .iter()
                .copied(),
        )
        .chain(NOWLEDGE_MEMORY_MATERIALIZED_METADATA_PATHS.iter().copied())
}
