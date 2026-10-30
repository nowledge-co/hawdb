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

use super::lexical_projection::{
    manifest_generation as lexical_manifest_generation, LexicalCorpusStatistics,
    LexicalProjectionConfig, LexicalProjectionReader, DEFAULT_MAX_MANIFEST_BYTES, MANIFEST_FILE,
};
use super::{
    checksum_bytes, decode_embedding, decode_metadata, decode_search_snapshot_text_bounded,
    decode_string, encode_embedding, encode_metadata, encode_search_snapshot_text, encode_string,
    lexical_analyzer_digest, lexical_documents_digest, matched_query_spans_bounded,
    matched_query_terms, rrf_child_score, search_empty_reasons, top_ranked_candidates,
    top_ranked_ids, weighted_rrf_score, CompressedVectorSearchMode, CosineQuery,
    SearchAccessControlContext, SearchAnalyzerLexicon, SearchCandidateSetReport, SearchDocument,
    SearchEmbeddingManifest, SearchFallbackReasonCode, SearchFieldPruningAccumulator, SearchHit,
    SearchIndex, SearchMode, SearchPageWindow, SearchPredicatePushdownReport,
    SearchProjectionFreshness, SearchQueryOptions, SearchResultSet, SearchRetrieverReport,
    SearchScoredCandidate, SearchSegmentDescriptor, SearchSegmentDescriptorEntry,
    SearchTruncationReasonCode, VectorSearchExecutionOptions, FULL_REINDEX_MARKER,
    METADATA_REPAIR_MARKER, SEARCH_SEGMENT_DESCRIPTOR_FILE, SEARCH_SEGMENT_PAYLOAD_FILE,
};
use crate::bounded_file::{read_bound_file, read_bounded_file};
use crate::error::{HawDBError, Result};
use crate::resident_documents::DocumentView;
#[cfg(test)]
use crate::{decode_search_segment_documents_bounded, validate_search_segment_documents};
use crate::{
    RuntimeCapabilities, RuntimeCapability, SearchLexicalSourcePolicy, SearchLexicalTermPolicy,
};
use hawdb_core::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope};
use hawdb_storage::durability::durable_replace_file;
use hawdb_storage::file_io::{self as fs, File, OpenOptions};
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod admitted_root;
mod candidate_set;
mod query_memory;
mod query_output;
pub use query_output::{SearchQueryError, SearchQueryResult, SearchRetainedOutput};
mod query_ranking;
mod score_collector;
mod vector_query;
use candidate_set::CandidateSet;
use score_collector::BoundedScoreCollector;

pub(crate) struct CandidateQuery<'a> {
    pub(crate) text: &'a str,
    pub(crate) embedding: Option<&'a [f32]>,
    pub(crate) mode: SearchMode,
    pub(crate) options: SearchQueryOptions,
    pub(crate) task: &'a hawdb_core::RuntimeTaskContext,
    pub(crate) permit: &'a hawdb_qos::RuntimePermit,
}

/// A complete native candidate reader retained privately until a separate
/// source-E fence and durable selector can grant serving authority.
#[derive(Clone)]
pub(crate) struct CandidateView(Arc<SharedCandidateView>);

struct SharedCandidateView {
    opened: admitted_root::OpenedRoot,
    // Close the reader and native pins before refunding the shared allocation.
    _memory: hawdb_qos::RuntimeRetainedMemory,
}

impl std::fmt::Debug for CandidateView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CandidateView")
            .field("document_count", &self.document_count())
            .field("source_epoch", &self.source_epoch())
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}

impl CandidateView {
    pub(crate) fn write_vector_successor(
        &self,
        index: &SearchIndex,
        previous: &crate::consumer::ConsumerProjection,
        target: &SearchProjectionPublishLease,
        task: hawdb_core::RuntimeTaskContext,
        generation_bytes: NonZeroU64,
        permit: Arc<hawdb_qos::RuntimePermit>,
    ) -> Result<crate::snapshot_writer::SearchSnapshotWriteReport> {
        if !self.shares_governor_with(&permit) {
            return Err(HawDBError::Execution(
                "vector successor requires its original governor".into(),
            ));
        }
        self.0.opened.write_vector_successor(
            index,
            previous,
            target,
            task,
            generation_bytes,
            permit,
        )
    }

    pub(crate) fn vector_index_query(
        &self,
        request: crate::VectorIndexQuery<'_>,
        publication_generation: u64,
        permit: &hawdb_qos::RuntimePermit,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> SearchQueryResult<crate::VectorIndexRetainedOutput> {
        self.0
            .opened
            .vector_index_query(request, publication_generation, permit, task)
    }

    pub(crate) fn prepare_consumer_update(
        &self,
        permit: Arc<hawdb_qos::RuntimePermit>,
        options: SearchOutOfCoreGenerationBuildOptions,
        limits: SearchUnorderedMutationLimits,
        task: hawdb_core::RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreUnorderedMutationWriter<'_>> {
        self.0
            .opened
            .prepare_consumer_update(permit, options, limits, task)
    }

    pub(crate) fn shares_governor_with(&self, permit: &hawdb_qos::RuntimePermit) -> bool {
        self.0._memory.shares_governor_with(permit) && self.0.opened.shares_governor_with(permit)
    }

    pub(crate) fn probe_query(
        &self,
        query: CandidateQuery<'_>,
    ) -> SearchQueryResult<SearchRetainedOutput> {
        self.0.opened.probe_query(query)
    }

    pub(crate) fn document_count(&self) -> usize {
        self.0.opened.document_count()
    }

    pub(crate) fn source_epoch(&self) -> Option<u64> {
        self.0.opened.source_epoch()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.0.opened.generation()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum StationaryRoot<'a> {
    Born(&'a hawdb_storage::consumer_allocation::PendingConsumerGeneration),
    Reopened(&'a hawdb_storage::consumer_allocation::ReopenedConsumerGeneration),
}

impl<'a> StationaryRoot<'a> {
    fn generation_name(self) -> &'a str {
        match self {
            Self::Born(root) => root.generation_name(),
            Self::Reopened(root) => root.generation_name(),
        }
    }
    fn directory(
        self,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<&'a hawdb_storage::file_io::SourceDirectory> {
        match self {
            Self::Born(root) => root.born_directory(),
            Self::Reopened(root) => root.directory(task),
        }
    }
}

pub(crate) struct StationaryCandidateOpenContext<'a> {
    pub(crate) stationary: StationaryRoot<'a>,
    pub(crate) publication: &'a mut SearchProjectionPublishLease,
    pub(crate) memory: &'a crate::build_memory::BuildMemory,
    pub(crate) task: &'a hawdb_core::RuntimeTaskContext,
    pub(crate) permit: &'a hawdb_qos::RuntimePermit,
    pub(crate) expected_manifest: hawdb_storage::immutable_object::ObjectReference,
    pub(crate) config: SearchOutOfCoreConfig,
    pub(crate) max_artifact_read_bytes: NonZeroU64,
}

pub(crate) fn open_stationary_candidate(
    context: StationaryCandidateOpenContext<'_>,
) -> Result<CandidateView> {
    let StationaryCandidateOpenContext {
        stationary,
        publication,
        memory,
        task,
        permit,
        expected_manifest,
        config,
        max_artifact_read_bytes,
    } = context;
    let project = publication.with_captured_directory(|directory| {
        hawdb_storage::file_descriptors::ProjectFileDescriptors::from_open_context(
            &directory.operation_context(),
        )
    });
    let admission = crate::build_memory::read::ReadAdmission {
        memory,
        task,
        permit: crate::build_memory::read::ReadPermit::Governed(permit),
    };
    let root = Path::new(stationary.generation_name());
    let opened = admitted_root::open(admitted_root::OpenContext {
        root,
        publication: Some(publication),
        stationary: Some(stationary),
        expected_stationary_manifest: Some(expected_manifest),
        config,
        analyzer_lexicon: SearchAnalyzerLexicon::default(),
        lexical_term_policy: SearchLexicalTermPolicy::default(),
        lexical_source_policy: SearchLexicalSourcePolicy::default(),
        admission: Some(&admission),
        project: Some(&project),
        max_artifact_read_bytes: max_artifact_read_bytes.get(),
    })?;
    let shared_bytes = std::mem::size_of::<SharedCandidateView>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .ok_or_else(|| HawDBError::Execution("shared candidate allocation overflows".into()))?;
    let memory = permit
        .reserve_retained_memory(shared_bytes as u64)
        .map_err(|error| HawDBError::Execution(error.to_string()))?;
    Ok(CandidateView(Arc::new(SharedCandidateView {
        opened,
        _memory: memory,
    })))
}
pub(crate) mod artifact_key;
mod generation_writer;
mod hydration;
mod metadata_query;
mod projection_scan;
mod verified_body;
pub use verified_body::{SearchBodyReadOptions, SearchVerifiedBody};
pub(crate) mod mutation_run;
mod publish_lease;
mod reuse;
mod root_registry;
mod text_seed;
#[cfg(feature = "vector-search")]
mod vector_qualification;
#[cfg(feature = "vector-search")]
mod vector_reader;
#[cfg(test)]
mod vector_selection_tests;
mod vector_serving;
pub use generation_writer::{
    GovernedSearchGenerationUpdate, GovernedSearchGenerationWriter, PreparedSearchPublication,
    ScheduledSearchOutOfCoreSegmentCompactionReport, SearchAdmittedConsumerDocuments,
    SearchBaselineComparisonLimits, SearchBaselineComparisonReport,
    SearchComparedConsumerDocuments, SearchGenerationAdmission, SearchMatchedBaseline,
    SearchMutationInspectionInput, SearchMutationInspectionStep, SearchMutationSortStep,
    SearchMutationSortStepLimits, SearchMutationSortStepUsage, SearchOutOfCoreBaselineComparison,
    SearchOutOfCoreDeltaComparison, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreGenerationUpdate,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreMutationWriter,
    SearchOutOfCorePreparedGeneration, SearchOutOfCorePreparedUpdate,
    SearchOutOfCoreSegmentCompaction, SearchOutOfCoreSegmentCompactionPolicy,
    SearchOutOfCoreSegmentCompactionReport, SearchOutOfCoreSegmentCompactionStopReason,
    SearchOutOfCoreUnorderedMutationWriter, SearchOutOfCoreUnorderedReplacementWriter,
    SearchStagingCleanupReport, SearchUnorderedMutationLimits, SearchUnorderedMutationReport,
};
pub(super) use publish_lease::SearchProjectionPublishLease;
pub use text_seed::{SearchTextSeedOutput, SearchTextSeedReport};
#[cfg(feature = "vector-search")]
use vector_serving::vector_projection_error;
use vector_serving::VectorScoreScan;

pub(crate) const OUT_OF_CORE_MANIFEST_FILE: &str = "search_projection.out_of_core.manifest.hawdb";
const OUT_OF_CORE_FORMAT: &str = "HAWDB_SEARCH_OUT_OF_CORE_V4";
const OUT_OF_CORE_LAYOUT_FORMAT: &str = "HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V1";
pub(crate) const MAX_OUT_OF_CORE_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_MARKER_BYTES: u64 = 64 * 1024;
const CANDIDATE_FILE_HEADER: &[u8; 8] = b"SKNCAND1";
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutOfCoreConfig {
    pub max_compressed_segment_bytes: NonZeroU64,
    pub max_uncompressed_segment_bytes: NonZeroU64,
    pub max_descriptor_bytes: NonZeroU64,
    /// Caller-selected encoded size bound for one mutation-run artifact.
    pub max_mutation_run_bytes: NonZeroU64,
    /// Aggregate mutation-run buffers, decoded ownership and validation indexes.
    /// Content artifacts and one-target hydration/analysis use their own limits.
    pub max_mutation_working_bytes: NonZeroU64,
    /// Total transient memory available for exact old-version reanalysis.
    pub max_reanalysis_working_bytes: NonZeroU64,
    /// External frequency-run bytes admitted during old-version reanalysis.
    pub max_reanalysis_spill_bytes: NonZeroU64,
    /// Bounded resident document header during streaming body operations.
    pub max_document_header_bytes: NonZeroUsize,
    /// Caller-selected encoded lexical manifest limit, including private decoding.
    /// Prepared generation updates inherit this limit and require it to fit `isize`.
    pub max_lexical_manifest_bytes: NonZeroU64,
    pub max_candidate_spill_bytes: NonZeroU64,
    pub max_candidate_block_bytes: NonZeroU64,
    pub max_score_entries: NonZeroUsize,
    pub max_vector_candidates: NonZeroUsize,
    pub max_vector_search_working_bytes: NonZeroUsize,
    pub max_vector_search_parallelism: NonZeroUsize,
    pub max_hydrated_documents: NonZeroUsize,
    pub max_hydrated_bytes: NonZeroU64,
    pub max_matched_spans: NonZeroUsize,
    pub max_matched_span_bytes: NonZeroU64,
    pub spill_directory: PathBuf,
}

impl Default for SearchOutOfCoreConfig {
    fn default() -> Self {
        Self {
            max_compressed_segment_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_uncompressed_segment_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            max_descriptor_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            max_mutation_run_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_mutation_working_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            max_reanalysis_working_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            max_reanalysis_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024).unwrap(),
            max_document_header_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
            max_lexical_manifest_bytes: NonZeroU64::new(DEFAULT_MAX_MANIFEST_BYTES).unwrap(),
            max_candidate_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024).unwrap(),
            max_candidate_block_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
            max_score_entries: NonZeroUsize::new(1_000_000).unwrap(),
            max_vector_candidates: NonZeroUsize::new(1_024).unwrap(),
            max_vector_search_working_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
            max_vector_search_parallelism: NonZeroUsize::MIN,
            max_hydrated_documents: NonZeroUsize::new(1024).unwrap(),
            max_hydrated_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
            max_matched_spans: NonZeroUsize::new(4096).unwrap(),
            max_matched_span_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
            spill_directory: std::env::temp_dir().join("hawdb-search"),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchOutOfCoreMetrics {
    pub segment_range_reads: u64,
    pub segment_bytes_read: u64,
    pub lexical_document_block_reads: u64,
    pub lexical_document_bytes_read: u64,
    pub metadata_segment_bytes_read: u64,
    pub vector_segment_bytes_read: u64,
    pub hydration_segment_bytes_read: u64,
    pub peak_segment_document_bytes: u64,
    pub peak_metadata_segment_bytes: u64,
    pub peak_vector_segment_bytes: u64,
    pub candidate_spill_bytes: u64,
    pub candidate_block_reads: u64,
    pub candidate_bytes_read: u64,
    pub vector_bytes_read: u64,
    pub rabitq_payload_bytes_read: u64,
    /// Bodies processed through bounded staging without owned materialization.
    pub streamed_documents: usize,
    pub streamed_body_bytes: u64,
    pub hydrated_documents: usize,
    pub hydrated_bytes: u64,
}

struct SearchDocumentSegmentRoute<'a> {
    artifact_index: usize,
    segment: &'a SearchSegmentDescriptorEntry,
    lexical_document_bytes_read: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutOfCoreOutput<H = SearchHit> {
    pub result: SearchResultSet<H>,
    pub metrics: SearchOutOfCoreMetrics,
}

/// A ranked hit bound to this reader's immutable content version.
///
/// No body or matched spans are loaded. Keep the originating reader alive for
/// subsequent version-specific reads; this value alone does not retain a pin.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutOfCoreCandidate {
    pub scores: SearchScoredCandidate,
    pub generation: u64,
    pub content_segment_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutOfCoreHydrationOutput {
    pub documents: Vec<SearchDocument>,
    pub metrics: SearchOutOfCoreMetrics,
}

/// A current document together with the immutable content segment that owns it.
///
/// Mutation preparation uses this identity to rewrite the affected content
/// segment instead of applying a generation-wide tombstone.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SearchOutOfCoreMutationTarget {
    pub(crate) content_segment_id: u64,
    pub(crate) document: SearchDocument,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SearchOutOfCoreMutationTargetOutput {
    pub(crate) targets: BTreeMap<String, SearchOutOfCoreMutationTarget>,
    pub(crate) metrics: SearchOutOfCoreMetrics,
}

#[derive(Debug)]
pub struct SearchOutOfCoreReader {
    root: PathBuf,
    _project_files: hawdb_storage::file_descriptors::ProjectFileDescriptors,
    config: SearchOutOfCoreConfig,
    analyzer_lexicon: SearchAnalyzerLexicon,
    manifest: SearchOutOfCoreManifestBody,
    segments: Vec<SearchOutOfCoreSegmentReader>,
    visibility: mutation_run::MutationVisibility,
    lexical_source_policy: SearchLexicalSourcePolicy,
    lexical_term_policy: SearchLexicalTermPolicy,
    runtime_capabilities: RuntimeCapabilities,
}

/// One immutable group of search artifacts selected by an out-of-core manifest.
///
/// The current manifest selects one group. Keeping the artifact closure separate
/// from reader-wide identity and admission state is the boundary needed for
/// incremental segment publication.
#[derive(Debug, Clone)]
pub(super) struct SearchOutOfCoreSegmentReader(
    crate::build_memory::shared::Shared<SegmentReaderContents>,
);

// Share one complete immutable payload. A future retained admission can live
// beside these values and follow every snapshot/reuse clone, rather than being
// detached from independently cloned descriptor/layout/lexical metadata.
#[derive(Debug)]
pub(super) struct SegmentReaderContents {
    content_segment_id: u64,
    descriptor: crate::document_encoding::DecodedDescriptor,
    payload: File,
    payload_length: u64,
    metadata_payload: File,
    metadata_payload_length: u64,
    vector_payload: File,
    vector_payload_length: u64,
    layout: SearchOutOfCoreLayoutBody,
    lexical_projection: LexicalProjectionReader,
    #[cfg(feature = "vector-search")]
    rabitq_projection: Option<vector_reader::Projection>,
    _source_pins: [Option<root_registry::ArtifactPin>; 3],
    _layout_memory: Option<crate::build_memory::read::ReaderMemory>,
    _reader_memory: Option<crate::build_memory::read::ReaderMemory>,
}

struct SegmentOpenContext<'a> {
    cleanup_source: Option<&'a captured_cleanup::Source<'a>>,
    capture: Option<&'a root_registry::PendingCapture<'a, 'a>>,
    root: &'a Path,
    config: &'a SearchOutOfCoreConfig,
    analyzer_lexicon: &'a SearchAnalyzerLexicon,
    lexical_source_policy: SearchLexicalSourcePolicy,
    lexical_term_policy: SearchLexicalTermPolicy,
    active_manifest: &'a SearchOutOfCoreManifestBody,
    admission: Option<&'a crate::build_memory::read::ReadAdmission<'a>>,
    max_artifact_read_bytes: u64,
}

impl std::ops::Deref for SearchOutOfCoreSegmentReader {
    type Target = SegmentReaderContents;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PublishedOutOfCoreProjection {
    pub(super) generation: u64,
    pub(super) bytes_written: u64,
}

mod artifact_reference;
mod captured_cleanup;
mod scratch_directory;
use artifact_reference::{ArtifactObject, ResidentSnapshot, SegmentObjects};
pub(crate) use scratch_directory::{ScratchDirectory, SpillFile};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreManifestBody<S = String> {
    format: S,
    generation: u64,
    resident_snapshot: Option<ResidentSnapshot>,
    resident_snapshot_absent: bool,
    segments: Vec<SearchOutOfCoreSegmentManifest<S>>,
    #[serde(default)]
    mutation_runs: Vec<SearchOutOfCoreMutationRunManifest<S>>,
    document_count: usize,
    documents_digest: u64,
    source_graph_commit_epoch: Option<u64>,
    import_source_graph_commit_epoch: Option<u64>,
    embedding_model: Option<S>,
    embedding_version: Option<S>,
    embedding_dimension: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    vector_index_binding: Option<S>,
}

/// One immutable artifact closure selected by an out-of-core manifest.
///
/// Segment-local identity stays with the files it validates. The active
/// manifest owns the logical projection identity and will eventually select
/// multiple entries as an incremental segment set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "S: Deserialize<'de>"))]
struct SearchOutOfCoreSegmentManifest<S = String> {
    segment_id: u64,
    level: u32,
    generation: u64,
    objects: SegmentObjects,
    descriptor_file: S,
    descriptor_len: u64,
    descriptor_checksum: u64,
    payload_file: S,
    payload_len: u64,
    metadata_payload_file: S,
    metadata_payload_len: u64,
    vector_payload_file: S,
    vector_payload_len: u64,
    layout_file: S,
    layout_len: u64,
    layout_checksum: u64,
    lexical_manifest_file: S,
    lexical_manifest_len: u64,
    lexical_manifest_checksum: u64,
    #[serde(default)]
    rabitq_artifact_file: Option<S>,
    // The existing artifact slot and filename grammar carry either backend.
    // This digest selects the graph format; readers also check the definition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    diskann_source_digest: Option<[u8; 32]>,
    #[serde(default)]
    rabitq_artifact_len: Option<u64>,
    #[serde(default)]
    rabitq_artifact_checksum: Option<u64>,
    #[serde(default)]
    rabitq_source_digest: Option<u64>,
    #[serde(default)]
    rabitq_vector_document_count: Option<usize>,
    #[serde(default)]
    rabitq_payload_checksum: Option<u32>,
    #[serde(default)]
    rabitq_peak_build_working_bytes: Option<usize>,
    document_count: usize,
    documents_digest: u64,
    source_graph_commit_epoch: Option<u64>,
}

/// One immutable mutation artifact selected by the active manifest.
///
/// The artifact carries the target-specific retractions that will later drive
/// shared visibility and live corpus statistics. Its identity remains separate
/// from content segments because a delete may publish no replacement content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound(deserialize = "S: Deserialize<'de>"))]
struct SearchOutOfCoreMutationRunManifest<S = String> {
    generation: u64,
    object: ArtifactObject,
    file: S,
    len: u64,
    checksum: u64,
    entry_count: usize,
    analyzer_digest: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreManifestEnvelope {
    body: SearchOutOfCoreManifestBody,
    checksum: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreLayoutEnvelope {
    body: SearchOutOfCoreLayoutBody,
    checksum: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreLayoutBody {
    format: String,
    generation: u64,
    document_count: usize,
    segments: Vec<SearchOutOfCoreSegmentLayout>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreSegmentLayout {
    segment_id: u64,
    vector_ordinal_base: u64,
    metadata: SearchOutOfCoreRange,
    vectors: SearchOutOfCoreRange,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreRange {
    offset: u64,
    length: u64,
    checksum: u64,
    entry_count: usize,
}

#[derive(Debug)]
struct SearchMetadataDocument {
    id: String,
    vector_ordinal: Option<u64>,
    metadata: BTreeMap<String, String>,
}

#[derive(Debug)]
struct SearchVectorDocument {
    id: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Copy)]
pub struct SearchOutOfCoreExecutionContext<'a> {
    pub access_control: Option<&'a SearchAccessControlContext>,
    pub compressed_vector_search_mode: CompressedVectorSearchMode,
    pub vector_execution_options: VectorSearchExecutionOptions<'a>,
}

impl Default for SearchOutOfCoreExecutionContext<'_> {
    fn default() -> Self {
        Self::scalar()
    }
}

impl SearchOutOfCoreExecutionContext<'_> {
    fn scalar() -> Self {
        Self {
            access_control: None,
            compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
            vector_execution_options: VectorSearchExecutionOptions::default(),
        }
    }
}

impl SearchOutOfCoreManifestBody {
    /// Reserve before serde can grow either vector, including invalid inputs
    /// rejected only after decoding. Each record opens a JSON map or sequence.
    /// Include validation's ID sets as well as the largest owned record type.
    fn decode_capacity(bytes: &[u8], task: &hawdb_core::RuntimeTaskContext) -> Result<usize> {
        use crate::build_memory::{checked_add, SET_ENTRY_BYTES};
        let element = std::mem::size_of::<SearchOutOfCoreSegmentManifest>()
            .max(std::mem::size_of::<SearchOutOfCoreMutationRunManifest>());
        checked_add(
            crate::build_control::json::decode_capacity(
                bytes,
                checked_add(element, SET_ENTRY_BYTES)?,
                2,
                task,
            )?,
            3 * 128,
        )
    }

    fn encode(&self) -> Result<Vec<u8>> {
        self.validate_names()?;
        let body = serde_json::to_vec(self).map_err(|error| {
            HawDBError::Storage(format!(
                "failed to encode search out-of-core manifest: {error}"
            ))
        })?;
        serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
            body: self.clone(),
            checksum: checksum_bytes(&body),
        })
        .map_err(|error| {
            HawDBError::Storage(format!(
                "failed to encode search out-of-core manifest envelope: {error}"
            ))
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_context(bytes, None)
    }

    fn decode_with_context(
        bytes: &[u8],
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Self> {
        let envelope = serde_json::from_slice::<SearchOutOfCoreManifestEnvelope>(bytes);
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        let envelope = envelope.map_err(|error| {
            HawDBError::Storage(format!("invalid search out-of-core manifest: {error}"))
        })?;
        if crate::build_control::json::checksum_with_context(&envelope.body, task)?
            != envelope.checksum
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate_names_with_context(task)?;
        Ok(envelope.body)
    }
}

/// Check the producer's declared artifact closure against the Consumer's
/// immutable references. The caller owns admission for `decode_capacity`, the
/// manifest bytes and the artifact list before invoking this validator.
pub(crate) fn validate_consumer_manifest_artifacts(
    bytes: &[u8],
    source_epoch: u64,
    artifacts: &[crate::consumer::generation_metadata::ArtifactReference<'_>],
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<bool> {
    let manifest = SearchOutOfCoreManifestBody::decode_with_context(bytes, Some(task))?;
    if manifest.source_graph_commit_epoch != Some(source_epoch)
        || (manifest.resident_snapshot.is_none() && !manifest.resident_snapshot_absent)
    {
        return Err(HawDBError::StorageIntegrity(
            "consumer candidate manifest lacks the requested source epoch or resident absence proof"
                .into(),
        ));
    }
    let require = |name: &str, object: ArtifactObject| -> Result<()> {
        let index = artifacts
            .binary_search_by(|artifact| artifact.name.cmp(name))
            .map_err(|_| {
                HawDBError::StorageIntegrity(
                    "consumer candidate omits an artifact declared by its Search manifest".into(),
                )
            })?;
        if artifacts[index].reference != object.reference() {
            return Err(HawDBError::StorageIntegrity(
                "consumer candidate artifact reference differs from its Search manifest".into(),
            ));
        }
        Ok(())
    };
    if let Some(snapshot) = manifest.resident_snapshot {
        require(crate::SEARCH_SNAPSHOT_FILE, snapshot.object)?;
    }
    for segment in &manifest.segments {
        crate::build_control::checkpoint(task)?;
        for (name, object) in [
            (segment.descriptor_file.as_str(), segment.objects.descriptor),
            (segment.payload_file.as_str(), segment.objects.payload),
            (
                segment.metadata_payload_file.as_str(),
                segment.objects.metadata,
            ),
            (segment.vector_payload_file.as_str(), segment.objects.vector),
            (segment.layout_file.as_str(), segment.objects.layout),
            (
                segment.lexical_manifest_file.as_str(),
                segment.objects.lexical_manifest,
            ),
        ] {
            require(name, object)?;
        }
        require(
            &crate::lexical_projection::artifact_file(segment.objects.lexical_generation),
            segment.objects.lexical_payload,
        )?;
        if let Some(name) = &segment.rabitq_artifact_file {
            require(
                name,
                segment.objects.rabitq.ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "consumer candidate manifest has an unbound RaBitQ artifact".into(),
                    )
                })?,
            )?;
        }
    }
    for run in &manifest.mutation_runs {
        crate::build_control::checkpoint(task)?;
        require(&run.file, run.object)?;
    }
    for artifact in artifacts {
        crate::build_control::checkpoint(task)?;
        let declared = (artifact.name == crate::SEARCH_SNAPSHOT_FILE
            && manifest.resident_snapshot.is_some())
            || manifest
                .segments
                .iter()
                .any(|segment| segment.object_for(artifact.name).is_some())
            || manifest
                .mutation_runs
                .iter()
                .any(|run| run.file == artifact.name);
        if !declared {
            return Err(HawDBError::StorageIntegrity(
                "consumer candidate contains an artifact absent from its Search manifest".into(),
            ));
        }
    }
    Ok(manifest.resident_snapshot_absent)
}

/// Recover the producer's complete declared closure through its retained root.
pub(crate) struct ConsumerProducerReferences {
    pub manifest: hawdb_storage::immutable_object::ObjectReference,
    pub artifacts: Vec<(String, hawdb_storage::immutable_object::ObjectReference)>,
}

pub(crate) fn consumer_producer_references(
    publication: &SearchProjectionPublishLease,
) -> Result<ConsumerProducerReferences> {
    use hawdb_storage::immutable_object::{ObjectKind, ObjectReference};
    let bytes = crate::bounded_file::read_bounded_open_file(
        publication.open_captured_child(OUT_OF_CORE_MANIFEST_FILE)?,
        OUT_OF_CORE_MANIFEST_FILE,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
    )?;
    let manifest = SearchOutOfCoreManifestBody::decode(&bytes)?;
    let mut artifacts = BTreeMap::new();
    if let Some(snapshot) = manifest.resident_snapshot {
        artifacts.insert(
            crate::SEARCH_SNAPSHOT_FILE.to_owned(),
            snapshot.object.reference(),
        );
    }
    for segment in &manifest.segments {
        for (name, object) in [
            (&segment.descriptor_file, segment.objects.descriptor),
            (&segment.payload_file, segment.objects.payload),
            (&segment.metadata_payload_file, segment.objects.metadata),
            (&segment.vector_payload_file, segment.objects.vector),
            (&segment.layout_file, segment.objects.layout),
            (
                &segment.lexical_manifest_file,
                segment.objects.lexical_manifest,
            ),
        ] {
            artifacts.insert(name.clone(), object.reference());
        }
        artifacts.insert(
            crate::lexical_projection::artifact_file(segment.objects.lexical_generation),
            segment.objects.lexical_payload.reference(),
        );
        if let Some(name) = &segment.rabitq_artifact_file {
            artifacts.insert(
                name.clone(),
                segment
                    .objects
                    .rabitq
                    .ok_or_else(|| {
                        HawDBError::StorageIntegrity("missing producer vector reference".into())
                    })?
                    .reference(),
            );
        }
    }
    for run in &manifest.mutation_runs {
        artifacts.insert(run.file.clone(), run.object.reference());
    }
    Ok(ConsumerProducerReferences {
        manifest: ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes),
        artifacts: artifacts.into_iter().collect(),
    })
}

pub(crate) fn consumer_manifest_decode_capacity(
    bytes: &[u8],
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<usize> {
    SearchOutOfCoreManifestBody::decode_capacity(bytes, task)
}

impl<S: AsRef<str>> SearchOutOfCoreManifestBody<S> {
    fn validate_names(&self) -> Result<()> {
        self.validate_names_with_context(None)
    }
    fn validate_names_with_context(
        &self,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<()> {
        task.map_or(Ok(()), crate::build_control::checkpoint)?;

        if self.format.as_ref() != OUT_OF_CORE_FORMAT
            || self.generation == 0
            || self.segments.is_empty()
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest header or segment set is invalid".to_string(),
            ));
        }
        if self.resident_snapshot_absent && self.resident_snapshot.is_some() {
            return Err(HawDBError::StorageIntegrity(
                "conflicting resident snapshot ownership states".into(),
            ));
        }
        if self
            .resident_snapshot
            .is_some_and(|snapshot| snapshot.metadata_only)
            && self.vector_index_binding.is_none()
        {
            return Err(HawDBError::StorageIntegrity(
                "metadata-only snapshot requires a vector index binding".into(),
            ));
        }
        if let Some(encoded) = &self.vector_index_binding {
            let descriptor = crate::vector_index::decode(encoded.as_ref())?;
            if self.embedding_dimension != Some(descriptor.dimension().get() as usize) {
                return Err(HawDBError::StorageIntegrity(
                    "vector index manifest dimension mismatch".into(),
                ));
            }
        }
        let mut ids = BTreeSet::new();
        let mut content_document_count = 0usize;
        for segment in &self.segments {
            task.map_or(Ok(()), crate::build_control::checkpoint)?;
            if !ids.insert(segment.segment_id) {
                return Err(HawDBError::Storage(
                    "search out-of-core manifest has duplicate segment ids".to_string(),
                ));
            }
            content_document_count = content_document_count
                .checked_add(segment.document_count)
                .ok_or_else(|| {
                    HawDBError::Storage(
                        "search out-of-core manifest document count overflows".to_string(),
                    )
                })?;
            segment.validate_names()?;
        }
        let mut mutation_generations = BTreeSet::new();
        for run in &self.mutation_runs {
            task.map_or(Ok(()), crate::build_control::checkpoint)?;
            if run.generation > self.generation || !mutation_generations.insert(run.generation) {
                return Err(HawDBError::Storage(
                    "search out-of-core manifest has an invalid mutation-run generation"
                        .to_string(),
                ));
            }
            run.validate_names()?;
        }
        if self.document_count > content_document_count
            || (self.mutation_runs.is_empty() && content_document_count != self.document_count)
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest document count does not match its artifact closure"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl<S: AsRef<str>> SearchOutOfCoreMutationRunManifest<S> {
    fn validate_names(&self) -> Result<()> {
        if self.generation == 0
            || self.len == 0
            || self.entry_count == 0
            || self.object.byte_length != self.len
        {
            return Err(HawDBError::Storage(
                "search out-of-core mutation-run manifest metadata is invalid".to_string(),
            ));
        }
        let name = self.file.as_ref();
        if Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
            || name != mutation_run::artifact_file(self.generation)
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest contains an invalid mutation-run artifact name"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl<S: AsRef<str>> SearchOutOfCoreSegmentManifest<S> {
    fn validate_names(&self) -> Result<()> {
        self.objects.validate(self)?;
        if self.generation == 0 {
            return Err(HawDBError::Storage(
                "search out-of-core segment generation is invalid".to_string(),
            ));
        }
        for name in [
            self.descriptor_file.as_ref(),
            self.payload_file.as_ref(),
            self.metadata_payload_file.as_ref(),
            self.vector_payload_file.as_ref(),
            self.layout_file.as_ref(),
            self.lexical_manifest_file.as_ref(),
        ] {
            if Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name) {
                return Err(HawDBError::Storage(
                    "search out-of-core manifest contains an invalid artifact name".to_string(),
                ));
            }
        }
        if let Some(name) = self.rabitq_artifact_file.as_ref().map(AsRef::as_ref)
            && Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest contains an invalid RaBitQ artifact name".to_string(),
            ));
        }
        if self.descriptor_file.as_ref()
            != format!("search_projection_segments.{}.hawdb", self.generation)
            || self.payload_file.as_ref()
                != format!(
                    "search_projection_segment_payloads.{}.hawdb",
                    self.generation
                )
            || self.metadata_payload_file.as_ref()
                != format!(
                    "search_projection_metadata_payloads.{}.hawdb",
                    self.generation
                )
            || self.vector_payload_file.as_ref()
                != format!(
                    "search_projection_vector_payloads.{}.hawdb",
                    self.generation
                )
            || self.layout_file.as_ref()
                != format!(
                    "search_projection_out_of_core_layout.{}.hawdb",
                    self.generation
                )
            || self.lexical_manifest_file.as_ref()
                != format!("search_lexical.manifest.{}.hawdb", self.generation)
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest artifact names do not match its generation"
                    .to_string(),
            ));
        }
        let rabitq_fields = [
            self.rabitq_artifact_file.is_some(),
            self.rabitq_artifact_len.is_some(),
            self.rabitq_artifact_checksum.is_some(),
            self.rabitq_source_digest.is_some(),
            self.rabitq_vector_document_count.is_some(),
            self.rabitq_payload_checksum.is_some(),
            self.rabitq_peak_build_working_bytes.is_some(),
        ];
        if rabitq_fields.iter().any(|present| *present)
            && rabitq_fields.iter().any(|present| !*present)
        {
            return Err(HawDBError::Storage(
                "search out-of-core manifest has an incomplete RaBitQ identity".to_string(),
            ));
        }
        if self.diskann_source_digest.is_some() && self.rabitq_artifact_file.is_none() {
            return Err(HawDBError::StorageIntegrity(
                "DiskANN identity has no candidate artifact".into(),
            ));
        }
        if let Some(name) = self.rabitq_artifact_file.as_ref().map(AsRef::as_ref)
            && name != format!("search_rabitq.{}.hawdb", self.generation)
        {
            return Err(HawDBError::Storage(
                "search out-of-core RaBitQ artifact does not match its generation".to_string(),
            ));
        }
        Ok(())
    }
}

impl SearchOutOfCoreLayoutBody {
    fn encode(&self) -> Result<Vec<u8>> {
        let body = serde_json::to_vec(self).map_err(|error| {
            HawDBError::Storage(format!(
                "failed to encode search out-of-core layout: {error}"
            ))
        })?;
        serde_json::to_vec(&SearchOutOfCoreLayoutEnvelope {
            body: self.clone(),
            checksum: checksum_bytes(&body),
        })
        .map_err(|error| {
            HawDBError::Storage(format!(
                "failed to encode search out-of-core layout envelope: {error}"
            ))
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_context(bytes, None)
    }

    fn decode_with_context(
        bytes: &[u8],
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Self> {
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        let envelope = serde_json::from_slice::<SearchOutOfCoreLayoutEnvelope>(bytes);
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        let envelope = envelope.map_err(|error| {
            HawDBError::Storage(format!("invalid search out-of-core layout: {error}"))
        })?;
        let checksum = crate::build_control::json::checksum_with_context(&envelope.body, task);
        // CheckedWriter reports cancellation through serde's I/O error. Keep
        // that execution outcome from being classified as corrupt storage.
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        if checksum? != envelope.checksum {
            return Err(HawDBError::Storage(
                "search out-of-core layout checksum mismatch".into(),
            ));
        }
        Ok(envelope.body)
    }

    fn validate(
        &self,
        manifest: &SearchOutOfCoreSegmentManifest,
        descriptor: &SearchSegmentDescriptor,
        max_compressed_segment_bytes: u64,
    ) -> Result<()> {
        if self.format != OUT_OF_CORE_LAYOUT_FORMAT
            || self.generation != manifest.generation
            || self.document_count != manifest.document_count
            || self.segments.len() != descriptor.segments.len()
        {
            return Err(HawDBError::Storage(
                "search out-of-core layout header does not match its manifest or descriptor"
                    .to_string(),
            ));
        }
        let mut metadata_end = 0u64;
        let mut vector_end = 0u64;
        let mut vector_ordinal_base = 0u64;
        for (layout, segment) in self.segments.iter().zip(&descriptor.segments) {
            if layout.segment_id != segment.segment_id
                || layout.vector_ordinal_base != vector_ordinal_base
                || layout.metadata.entry_count != segment.document_count
                || layout.vectors.entry_count > segment.document_count
            {
                return Err(HawDBError::Storage(format!(
                    "search out-of-core layout does not match segment {}",
                    segment.segment_id
                )));
            }
            validate_out_of_core_range(
                layout.metadata,
                metadata_end,
                manifest.metadata_payload_len,
                max_compressed_segment_bytes,
                segment.segment_id,
                "metadata",
            )?;
            validate_out_of_core_range(
                layout.vectors,
                vector_end,
                manifest.vector_payload_len,
                max_compressed_segment_bytes,
                segment.segment_id,
                "vector",
            )?;
            metadata_end = layout.metadata.offset + layout.metadata.length;
            vector_end = layout.vectors.offset + layout.vectors.length;
            vector_ordinal_base = vector_ordinal_base
                .checked_add(layout.vectors.entry_count as u64)
                .ok_or_else(|| {
                    HawDBError::Storage("search vector ordinal range overflow".to_string())
                })?;
        }
        if metadata_end != manifest.metadata_payload_len
            || vector_end != manifest.vector_payload_len
        {
            return Err(HawDBError::Storage(
                "search out-of-core sidecar layout does not cover its payload files".to_string(),
            ));
        }
        Ok(())
    }
}

impl SearchOutOfCoreSegmentReader {
    fn validate_pinned_paths(
        &self,
        root: &Path,
        manifest: &SearchOutOfCoreSegmentManifest,
    ) -> Result<()> {
        let lexical_file =
            crate::lexical_projection::artifact_file(self.lexical_projection.generation());
        for (file, expected_len) in [
            (manifest.descriptor_file.as_str(), manifest.descriptor_len),
            (manifest.payload_file.as_str(), manifest.payload_len),
            (
                manifest.metadata_payload_file.as_str(),
                manifest.metadata_payload_len,
            ),
            (
                manifest.vector_payload_file.as_str(),
                manifest.vector_payload_len,
            ),
            (manifest.layout_file.as_str(), manifest.layout_len),
            (
                manifest.lexical_manifest_file.as_str(),
                manifest.lexical_manifest_len,
            ),
            (
                lexical_file.as_str(),
                self.lexical_projection.artifact_len(),
            ),
        ] {
            if fs::metadata(root.join(file))?.len() != expected_len {
                return Err(HawDBError::Storage(
                    "pinned search artifact length changed".into(),
                ));
            }
        }
        if let Some((file, expected_len)) = manifest
            .rabitq_artifact_file
            .as_deref()
            .zip(manifest.rabitq_artifact_len)
            && fs::metadata(root.join(file))?.len() != expected_len
        {
            return Err(HawDBError::Storage(
                "pinned RaBitQ artifact length changed".into(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn open(
        root: &Path,
        config: &SearchOutOfCoreConfig,
        analyzer_lexicon: &SearchAnalyzerLexicon,
        lexical_source_policy: SearchLexicalSourcePolicy,
        lexical_term_policy: SearchLexicalTermPolicy,
        active_manifest: &SearchOutOfCoreManifestBody,
        manifest: &SearchOutOfCoreSegmentManifest,
    ) -> Result<Self> {
        Self::open_with_read_admission(
            SegmentOpenContext {
                cleanup_source: None,
                capture: None,
                root,
                config,
                analyzer_lexicon,
                lexical_source_policy,
                lexical_term_policy,
                active_manifest,
                admission: None,
                max_artifact_read_bytes: u64::MAX,
            },
            manifest,
        )
    }

    fn open_with_read_admission(
        context: SegmentOpenContext<'_>,
        manifest: &SearchOutOfCoreSegmentManifest,
    ) -> Result<Self> {
        let SegmentOpenContext {
            cleanup_source,
            capture,
            root,
            config,
            analyzer_lexicon,
            lexical_source_policy,
            lexical_term_policy,
            active_manifest,
            admission,
            max_artifact_read_bytes,
        } = context;
        if cleanup_source.is_some()
            && (capture.is_some() || admission.is_some_and(|admission| !admission.is_synchronous()))
        {
            return Err(HawDBError::Execution(
                "incompatible captured cleanup/read admission modes".into(),
            ));
        }
        if admission.is_some_and(|admission| admission.is_synchronous()) && cleanup_source.is_none()
        {
            return Err(HawDBError::Execution(
                "synchronous metadata requires captured publication custody".into(),
            ));
        }
        let reader_memory = admission
            .map(|context| {
                use crate::build_memory::{
                    checked_add as add, checked_mul as mul, reserved::native_path,
                };
                let native = mul(add(native_path::bytes(root)?, 4096)?, 4)?;
                context.retained(add(
                    add(
                        std::mem::size_of::<SegmentReaderContents>(),
                        2 * std::mem::size_of::<usize>(),
                    )?,
                    native,
                )?)
            })
            .transpose()?;
        let source_pins = match capture {
            Some(capture) => capture.pin_segment(manifest)?,
            None => [None, None, None],
        };
        #[cfg(not(feature = "vector-search"))]
        let _ = active_manifest;
        let read_captured = |name: &str, object: ArtifactObject, checksum: u64, maximum: u64| {
            let capture = capture.expect("captured branch owns source custody");
            let admission = admission.expect("captured branch owns read admission");
            admission.require_read_limit(object.byte_length, max_artifact_read_bytes)?;
            let file = capture.open_artifact(name, object.reference())?;
            admission.read_file_range(
                &file,
                object.byte_length,
                maximum,
                crate::build_memory::read::FileRange {
                    offset: 0,
                    length: object.byte_length,
                    checksum,
                },
            )
        };
        let descriptor_path = match admission {
            Some(context) => Some(crate::build_memory::path::OwnedPath::join(
                root,
                Path::new(&manifest.descriptor_file),
                context.memory,
                context.task,
            )?),
            None => None,
        };
        let descriptor_bytes = match admission {
            Some(_) if capture.is_some() => read_captured(
                &manifest.descriptor_file,
                manifest.objects.descriptor,
                manifest.descriptor_checksum,
                config.max_descriptor_bytes.get(),
            )?,
            Some(context) if cleanup_source.is_some() => cleanup_source.unwrap().read_admitted(
                &manifest.descriptor_file,
                manifest.objects.descriptor,
                manifest.descriptor_checksum,
                config.max_descriptor_bytes.get(),
                context,
            )?,
            Some(context) => {
                context.require_read_limit(manifest.descriptor_len, max_artifact_read_bytes)?;
                context.read_bound(
                    descriptor_path.as_ref().expect("admitted path"),
                    manifest.descriptor_len,
                    manifest.descriptor_checksum,
                    config.max_descriptor_bytes.get(),
                )?
            }

            None if cleanup_source.is_some() => {
                crate::build_memory::read::ReadBytes::unaccounted(cleanup_source.unwrap().read(
                    &manifest.descriptor_file,
                    manifest.objects.descriptor,
                    manifest.descriptor_checksum,
                    config.max_descriptor_bytes.get(),
                )?)
            }
            None => crate::build_memory::read::ReadBytes::unaccounted(read_bound_artifact(
                &root.join(&manifest.descriptor_file),
                manifest.descriptor_len,
                manifest.descriptor_checksum,
                config.max_descriptor_bytes.get(),
                "search segment descriptor",
            )?),
        };
        let descriptor_text = std::str::from_utf8(&descriptor_bytes).map_err(|error| {
            HawDBError::Storage(format!("search segment descriptor is not UTF-8: {error}"))
        })?;
        let descriptor =
            crate::document_encoding::DecodedDescriptor::decode(descriptor_text, admission)?;
        drop(descriptor_bytes);
        drop(descriptor_path);
        if descriptor.document_count != manifest.document_count {
            return Err(HawDBError::Storage(format!(
                "search out-of-core manifest expects {} documents, descriptor has {}",
                manifest.document_count, descriptor.document_count
            )));
        }
        for segment in &descriptor.segments {
            let range = segment.payload_range.ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search segment {} has no physical payload range",
                    segment.segment_id
                ))
            })?;
            if range.length > config.max_compressed_segment_bytes.get() {
                return Err(HawDBError::Storage(format!(
                    "search segment {} requires {} compressed bytes, exceeding {}",
                    segment.segment_id, range.length, config.max_compressed_segment_bytes
                )));
            }
            if range.offset.saturating_add(range.length) > manifest.payload_len {
                return Err(HawDBError::Storage(format!(
                    "search segment {} exceeds the published payload length",
                    segment.segment_id
                )));
            }
        }

        let payload = match admission {
            Some(context) if capture.is_some() => {
                context.require_read_limit(manifest.payload_len, max_artifact_read_bytes)?;
                capture
                    .unwrap()
                    .open_artifact(&manifest.payload_file, manifest.objects.payload.reference())?
            }
            Some(context) if cleanup_source.is_some() => cleanup_source.unwrap().open_admitted(
                &manifest.payload_file,
                manifest.objects.payload,
                context,
            )?,
            Some(context) => {
                let path = crate::build_memory::path::OwnedPath::join(
                    root,
                    Path::new(&manifest.payload_file),
                    context.memory,
                    context.task,
                )?;
                context.open_bound_length(&path, manifest.payload_len, max_artifact_read_bytes)?
            }
            None if cleanup_source.is_some() => cleanup_source
                .unwrap()
                .open(&manifest.payload_file, manifest.objects.payload)?,
            None => open_exact_length_artifact(
                &root.join(&manifest.payload_file),
                manifest.payload_len,
                "search out-of-core payload",
            )?,
        };

        let layout_path = match admission {
            Some(context) => Some(crate::build_memory::path::OwnedPath::join(
                root,
                Path::new(&manifest.layout_file),
                context.memory,
                context.task,
            )?),
            None => None,
        };
        let layout_bytes = match admission {
            Some(_) if capture.is_some() => read_captured(
                &manifest.layout_file,
                manifest.objects.layout,
                manifest.layout_checksum,
                config.max_descriptor_bytes.get(),
            )?,
            Some(context) if cleanup_source.is_some() => cleanup_source.unwrap().read_admitted(
                &manifest.layout_file,
                manifest.objects.layout,
                manifest.layout_checksum,
                config.max_descriptor_bytes.get(),
                context,
            )?,
            Some(context) => {
                context.require_read_limit(manifest.layout_len, max_artifact_read_bytes)?;
                context.read_bound(
                    layout_path.as_ref().expect("admitted layout path"),
                    manifest.layout_len,
                    manifest.layout_checksum,
                    config.max_descriptor_bytes.get(),
                )?
            }

            None if cleanup_source.is_some() => {
                crate::build_memory::read::ReadBytes::unaccounted(cleanup_source.unwrap().read(
                    &manifest.layout_file,
                    manifest.objects.layout,
                    manifest.layout_checksum,
                    config.max_descriptor_bytes.get(),
                )?)
            }
            None => crate::build_memory::read::ReadBytes::unaccounted(read_bound_artifact(
                &root.join(&manifest.layout_file),
                manifest.layout_len,
                manifest.layout_checksum,
                config.max_descriptor_bytes.get(),
                "search out-of-core layout",
            )?),
        };
        let layout_memory = admission
            .map(|context| {
                let bytes = crate::build_control::json::decode_capacity(
                    &layout_bytes,
                    std::mem::size_of::<SearchOutOfCoreSegmentLayout>(),
                    1,
                    context.task,
                )?;
                context.retained(bytes)
            })
            .transpose()?;
        let layout = match admission {
            Some(context) => {
                SearchOutOfCoreLayoutBody::decode_with_context(&layout_bytes, Some(context.task))?
            }
            None => SearchOutOfCoreLayoutBody::decode(&layout_bytes)?,
        };
        drop(layout_bytes);
        drop(layout_path);
        layout.validate(
            manifest,
            &descriptor,
            config.max_compressed_segment_bytes.get(),
        )?;
        let vector_count = manifest
            .rabitq_artifact_file
            .as_ref()
            .map(|_| {
                layout.segments.iter().try_fold(0usize, |count, segment| {
                    if let Some(context) = admission {
                        crate::build_control::checkpoint(context.task)?;
                    }
                    count
                        .checked_add(segment.vectors.entry_count)
                        .ok_or_else(|| {
                            HawDBError::Storage("search vector count exceeds address space".into())
                        })
                })
            })
            .transpose()?;
        if manifest.rabitq_vector_document_count != vector_count {
            return Err(HawDBError::Storage(
                "search out-of-core RaBitQ vector count does not match the sidecar layout"
                    .to_string(),
            ));
        }

        let open_payload = |name: &str, object: ArtifactObject, label: &str| -> Result<File> {
            let len = object.byte_length;
            match admission {
                Some(context) if capture.is_some() => {
                    context.require_read_limit(len, max_artifact_read_bytes)?;
                    capture.unwrap().open_artifact(name, object.reference())
                }
                Some(context) if cleanup_source.is_some() => {
                    cleanup_source.unwrap().open_admitted(name, object, context)
                }
                Some(context) => {
                    let path = crate::build_memory::path::OwnedPath::join(
                        root,
                        Path::new(name),
                        context.memory,
                        context.task,
                    )?;
                    context.open_bound_length(&path, len, max_artifact_read_bytes)
                }
                None if cleanup_source.is_some() => cleanup_source.unwrap().open(name, object),
                None => open_exact_length_artifact(&root.join(name), len, label),
            }
        };
        let metadata_payload = open_payload(
            &manifest.metadata_payload_file,
            manifest.objects.metadata,
            "search out-of-core metadata payload",
        )?;
        let vector_payload = open_payload(
            &manifest.vector_payload_file,
            manifest.objects.vector,
            "search out-of-core vector payload",
        )?;
        let lexical_path = admission
            .map(|context| {
                crate::build_memory::path::OwnedPath::join(
                    root,
                    Path::new(&manifest.lexical_manifest_file),
                    context.memory,
                    context.task,
                )
            })
            .transpose()?;
        let lexical_manifest_bytes = match admission {
            Some(_) if capture.is_some() => read_captured(
                &manifest.lexical_manifest_file,
                manifest.objects.lexical_manifest,
                manifest.lexical_manifest_checksum,
                config.max_lexical_manifest_bytes.get(),
            )?,
            Some(context) if cleanup_source.is_some() => cleanup_source.unwrap().read_admitted(
                &manifest.lexical_manifest_file,
                manifest.objects.lexical_manifest,
                manifest.lexical_manifest_checksum,
                config.max_lexical_manifest_bytes.get(),
                context,
            )?,
            Some(context) => {
                context
                    .require_read_limit(manifest.lexical_manifest_len, max_artifact_read_bytes)?;
                context.read_bound(
                    lexical_path.as_ref().expect("admitted lexical path"),
                    manifest.lexical_manifest_len,
                    manifest.lexical_manifest_checksum,
                    config.max_lexical_manifest_bytes.get(),
                )?
            }

            None if cleanup_source.is_some() => {
                crate::build_memory::read::ReadBytes::unaccounted(cleanup_source.unwrap().read(
                    &manifest.lexical_manifest_file,
                    manifest.objects.lexical_manifest,
                    manifest.lexical_manifest_checksum,
                    config.max_lexical_manifest_bytes.get(),
                )?)
            }
            None => crate::build_memory::read::ReadBytes::unaccounted(read_bound_artifact(
                &root.join(&manifest.lexical_manifest_file),
                manifest.lexical_manifest_len,
                manifest.lexical_manifest_checksum,
                config.max_lexical_manifest_bytes.get(),
                "search lexical manifest",
            )?),
        };
        let lexical_config = LexicalProjectionConfig {
            max_manifest_bytes: config.max_lexical_manifest_bytes,
            max_term_bytes: lexical_term_policy.max_term_bytes(),
            max_document_source_bytes: lexical_source_policy.max_document_source_bytes(),
            max_document_tokens: lexical_source_policy.max_document_tokens(),
            max_spill_bytes: config.max_reanalysis_spill_bytes,
            max_query_score_entries: config.max_score_entries,
            ..LexicalProjectionConfig::default()
        };
        let lexical_projection = match admission {
            Some(context) => LexicalProjectionReader::load_admitted_manifest_bytes(
                crate::lexical_projection::ReaderOpenContext {
                    captured_file: if let Some(source) = cleanup_source {
                        let _name = context.scratch(3 * 128)?;
                        let name = crate::lexical_projection::artifact_file(
                            manifest.objects.lexical_generation,
                        );
                        Some((
                            manifest.objects.lexical_generation,
                            source.open_admitted(
                                &name,
                                manifest.objects.lexical_payload,
                                context,
                            )?,
                        ))
                    } else {
                        capture
                            .map(|capture| -> Result<_> {
                                let _name_memory = context.scratch(3 * 128)?;
                                let name = crate::lexical_projection::artifact_file(
                                    manifest.objects.lexical_generation,
                                );
                                if name.capacity() > 128 {
                                    return Err(HawDBError::Execution(
                                        "lexical name exceeds admission".into(),
                                    ));
                                }
                                Ok((
                                    manifest.objects.lexical_generation,
                                    capture.open_artifact(
                                        &name,
                                        manifest.objects.lexical_payload.reference(),
                                    )?,
                                ))
                            })
                            .transpose()?
                    },
                    root,
                    identity: crate::lexical_projection::ReaderIdentity {
                        source_epoch: manifest.source_graph_commit_epoch,
                        analyzer_digest: crate::lexical_projection::analyzer_digest_with_context(
                            analyzer_lexicon,
                            context.task,
                        )?,
                        documents_digest: manifest.documents_digest,
                    },
                    config: lexical_config,
                    admission: context,
                    max_artifact_read_bytes,
                },
                &lexical_manifest_bytes,
            )?,
            None if cleanup_source.is_some() => {
                LexicalProjectionReader::load_captured_manifest_bytes(
                    &lexical_manifest_bytes,
                    manifest.source_graph_commit_epoch,
                    lexical_analyzer_digest(analyzer_lexicon),
                    manifest.documents_digest,
                    lexical_config,
                    cleanup_source.unwrap().open(
                        &crate::lexical_projection::artifact_file(
                            manifest.objects.lexical_generation,
                        ),
                        manifest.objects.lexical_payload,
                    )?,
                    manifest.objects.lexical_generation,
                )?
                .map(|reader| {
                    Arc::into_inner(reader).ok_or_else(|| {
                        HawDBError::Storage("captured lexical reader unexpectedly shared".into())
                    })
                })
                .transpose()?
            }
            None => LexicalProjectionReader::load_manifest_bytes(
                root,
                &lexical_manifest_bytes,
                manifest.source_graph_commit_epoch,
                lexical_analyzer_digest(analyzer_lexicon),
                manifest.documents_digest,
                lexical_config,
            )?
            .map(|reader| {
                Arc::into_inner(reader).ok_or_else(|| {
                    HawDBError::Storage("new lexical segment reader was unexpectedly shared".into())
                })
            })
            .transpose()?,
        }
        .ok_or_else(|| {
            HawDBError::Storage(
                "published search lexical projection does not match the out-of-core manifest"
                    .into(),
            )
        })?;
        if lexical_projection.generation() != manifest.objects.lexical_generation
            || lexical_projection.artifact_len() != manifest.objects.lexical_payload.byte_length
        {
            return Err(HawDBError::StorageIntegrity(
                "lexical data differs from its producer reference".into(),
            ));
        }
        drop(lexical_manifest_bytes);
        drop(lexical_path);

        #[cfg(feature = "vector-search")]
        let rabitq_projection = vector_reader::open(
            root,
            active_manifest,
            manifest,
            config.max_vector_search_working_bytes.get(),
            admission,
            max_artifact_read_bytes,
            match (
                capture,
                cleanup_source,
                manifest.rabitq_artifact_file.as_deref(),
            ) {
                (Some(capture), _, Some(name)) => Some(
                    capture.open_artifact(
                        name,
                        manifest
                            .objects
                            .rabitq
                            .expect("validated vector reference")
                            .reference(),
                    )?,
                ),
                (_, Some(source), Some(name)) => Some(match admission {
                    Some(admission) => source.open_admitted(
                        name,
                        manifest.objects.rabitq.expect("validated vector reference"),
                        admission,
                    )?,
                    None => source.open(
                        name,
                        manifest.objects.rabitq.expect("validated vector reference"),
                    )?,
                }),
                _ => None,
            },
        )?;
        #[cfg(not(feature = "vector-search"))]
        if let Some(context) = admission {
            if let Some(name) = manifest.rabitq_artifact_file.as_deref() {
                let path = crate::build_memory::path::OwnedPath::join(
                    root,
                    Path::new(name),
                    context.memory,
                    context.task,
                )?;
                let length = manifest
                    .rabitq_artifact_len
                    .expect("validated vector length");
                let checksum = manifest
                    .rabitq_artifact_checksum
                    .expect("validated vector checksum");
                let file = match (capture, cleanup_source) {
                    (Some(capture), _) => context.visit_file_bound(
                        capture.open_artifact(
                            name,
                            manifest
                                .objects
                                .rabitq
                                .expect("validated vector reference")
                                .reference(),
                        )?,
                        length,
                        checksum,
                        max_artifact_read_bytes,
                        |_| Ok(()),
                    )?,
                    (None, Some(source)) => context.visit_file_bound(
                        source.open_admitted(
                            name,
                            manifest.objects.rabitq.expect("validated vector reference"),
                            context,
                        )?,
                        length,
                        checksum,
                        max_artifact_read_bytes,
                        |_| Ok(()),
                    )?,
                    (None, None) => context.visit_bound(
                        &path,
                        length,
                        checksum,
                        max_artifact_read_bytes,
                        |_| Ok(()),
                    )?,
                };
                drop(file);
            }
        } else if let Some(source) = cleanup_source {
            if let Some(name) = manifest.rabitq_artifact_file.as_deref() {
                drop(source.open(
                    name,
                    manifest.objects.rabitq.expect("validated vector reference"),
                )?);
            }
        } else {
            verify_rabitq_artifact(root, manifest)?;
        }

        Ok(Self(crate::build_memory::shared::Shared::new(
            SegmentReaderContents {
                _source_pins: source_pins,
                content_segment_id: manifest.segment_id,
                descriptor,
                payload,
                payload_length: manifest.payload_len,
                metadata_payload,
                metadata_payload_length: manifest.metadata_payload_len,
                vector_payload,
                vector_payload_length: manifest.vector_payload_len,
                layout,
                lexical_projection,
                #[cfg(feature = "vector-search")]
                rabitq_projection,
                _layout_memory: layout_memory,
                _reader_memory: reader_memory,
            },
        )))
    }
}

impl SearchOutOfCoreReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config_and_analyzer(
            path,
            SearchOutOfCoreConfig::default(),
            SearchAnalyzerLexicon::default(),
        )
    }

    pub fn open_with_config(path: impl AsRef<Path>, config: SearchOutOfCoreConfig) -> Result<Self> {
        Self::open_with_config_and_analyzer(path, config, SearchAnalyzerLexicon::default())
    }

    pub fn open_with_config_and_analyzer(
        path: impl AsRef<Path>,
        config: SearchOutOfCoreConfig,
        analyzer_lexicon: SearchAnalyzerLexicon,
    ) -> Result<Self> {
        Self::open_with_term_policy(
            path,
            config,
            analyzer_lexicon,
            SearchLexicalTermPolicy::default(),
        )
    }

    /// Opens a generation under host-selected resource and lexical term limits.
    ///
    /// An insufficient term policy fails before returning a usable reader. The
    /// actual dictionary requirements are checked, not a writer-declared cap.
    pub fn open_with_term_policy(
        path: impl AsRef<Path>,
        config: SearchOutOfCoreConfig,
        analyzer_lexicon: SearchAnalyzerLexicon,
        lexical_term_policy: SearchLexicalTermPolicy,
    ) -> Result<Self> {
        Self::open_with_lexical_policies(
            path,
            config,
            analyzer_lexicon,
            lexical_term_policy,
            SearchLexicalSourcePolicy::default(),
        )
    }

    /// Opens with explicit source admission for target reanalysis and later updates.
    pub fn open_with_source_policy(
        path: impl AsRef<Path>,
        config: SearchOutOfCoreConfig,
        analyzer_lexicon: SearchAnalyzerLexicon,
        lexical_source_policy: SearchLexicalSourcePolicy,
    ) -> Result<Self> {
        Self::open_with_lexical_policies(
            path,
            config,
            analyzer_lexicon,
            SearchLexicalTermPolicy::default(),
            lexical_source_policy,
        )
    }

    /// Opens a generation with explicit host-selected lexical policies.
    ///
    /// Source admission governs old-version reanalysis, compaction and subsequent
    /// writes. It is never widened from an artifact declaration.
    pub fn open_with_lexical_policies(
        path: impl AsRef<Path>,
        config: SearchOutOfCoreConfig,
        analyzer_lexicon: SearchAnalyzerLexicon,
        lexical_term_policy: SearchLexicalTermPolicy,
        lexical_source_policy: SearchLexicalSourcePolicy,
    ) -> Result<Self> {
        admitted_root::open(admitted_root::OpenContext {
            root: path.as_ref(),
            publication: None,
            stationary: None,
            expected_stationary_manifest: None,
            config,
            analyzer_lexicon,
            lexical_term_policy,
            lexical_source_policy,
            admission: None,
            project: None,
            max_artifact_read_bytes: u64::MAX,
        })?
        .into_ordinary()
    }

    pub(super) fn primary_segment(&self) -> &SearchOutOfCoreSegmentReader {
        // Logical-manifest metadata remains shared. Any data-plane operation
        // must iterate the complete artifact set instead of using this helper.
        self.segments
            .first()
            .expect("validated search manifest has at least one segment")
    }

    pub fn lexical_term_policy(&self) -> SearchLexicalTermPolicy {
        self.lexical_term_policy
    }

    pub fn lexical_source_policy(&self) -> SearchLexicalSourcePolicy {
        self.lexical_source_policy
    }

    /// Changes admission for subsequent queries and prepared updates.
    ///
    /// Lowering below the open generation's actual term requirement fails and
    /// leaves the previous policy intact. Exclusive access prevents changes
    /// during a query; already prepared updates keep their own snapshot.
    pub fn set_lexical_term_policy(&mut self, policy: SearchLexicalTermPolicy) -> Result<()> {
        for artifact in &self.segments {
            artifact
                .lexical_projection
                .validate_term_limit(policy.max_term_bytes())?;
        }
        self.lexical_term_policy = policy;
        Ok(())
    }

    /// Changes source admission for later generation updates.
    ///
    /// This does not affect reading an already published generation because
    /// source admission is a host-owned write policy.
    pub fn set_lexical_source_policy(&mut self, policy: SearchLexicalSourcePolicy) {
        self.lexical_source_policy = policy;
    }

    pub fn document_count(&self) -> usize {
        self.manifest.document_count
    }

    pub fn projection_payload_bytes(&self) -> u64 {
        self.manifest.segments.iter().fold(0u64, |total, segment| {
            total
                .saturating_add(segment.descriptor_len)
                .saturating_add(segment.payload_len)
                .saturating_add(segment.metadata_payload_len)
                .saturating_add(segment.vector_payload_len)
                .saturating_add(segment.layout_len)
                .saturating_add(segment.lexical_manifest_len)
                .saturating_add(segment.rabitq_artifact_len.unwrap_or_default())
        })
    }

    #[doc(hidden)]
    pub fn config(&self) -> &SearchOutOfCoreConfig {
        &self.config
    }

    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    /// Attaches a newer durable head, sharing unchanged immutable content handles.
    ///
    /// Retained content paths and lengths are checked before their handles are
    /// reused. Changed heads decode and checksum every mutation run; target
    /// reanalysis is reused only for the same run and exact content references,
    /// with an unchanged global embedding identity. Fresh opens validate the
    /// complete closure. Published artifacts must remain immutable while readers
    /// exist. On failure this reader retains its previously pinned generation.
    pub fn refresh(&mut self) -> Result<SearchOutOfCoreRefreshReport> {
        let (closure, report) = self.prepare_refresh()?;
        if let Some(closure) = closure {
            self.manifest = closure.manifest;
            self.segments = closure.segments;
            self.visibility = closure.visibility;
        }
        Ok(report)
    }

    /// Publishes a refreshed reader without changing existing snapshot owners.
    /// Unchanged content retains its native artifact pins and shared allocations;
    /// no reader or document corpus is cloned on snapshot acquisition.
    pub fn refresh_shared(reader: &mut Arc<Self>) -> Result<SearchOutOfCoreRefreshReport> {
        let (closure, report) = reader.prepare_refresh()?;
        if let Some(closure) = closure {
            let next = Self {
                root: reader.root.clone(),
                _project_files: reader._project_files.clone(),
                config: reader.config.clone(),
                analyzer_lexicon: reader.analyzer_lexicon.clone(),
                manifest: closure.manifest,
                segments: closure.segments,
                visibility: closure.visibility,
                lexical_source_policy: reader.lexical_source_policy,
                lexical_term_policy: reader.lexical_term_policy,
                runtime_capabilities: reader.runtime_capabilities,
            };
            *reader = Arc::new(next);
        }
        Ok(report)
    }

    fn prepare_refresh(
        &self,
    ) -> Result<(Option<LoadedArtifactClosure>, SearchOutOfCoreRefreshReport)> {
        let bytes = read_bounded_file(
            &self.root.join(OUT_OF_CORE_MANIFEST_FILE),
            MAX_OUT_OF_CORE_MANIFEST_BYTES,
        )?;
        let manifest = SearchOutOfCoreManifestBody::decode(&bytes)?;
        if manifest == self.manifest {
            for (segment, reference) in self.segments.iter().zip(&manifest.segments) {
                segment.validate_pinned_paths(&self.root, reference)?;
            }
            for run in &manifest.mutation_runs {
                if fs::metadata(self.root.join(&run.file))?.len() != run.len {
                    return Err(HawDBError::Storage(
                        "pinned mutation-run artifact length changed".into(),
                    ));
                }
            }
            return Ok((
                None,
                SearchOutOfCoreRefreshReport {
                    generation: manifest.generation,
                    generation_changed: false,
                    validated_retractions: 0,
                    reused_retractions: self.visibility.retractions().count(),
                    opened_content_segments: 0,
                    reused_content_segments: self.segments.len(),
                },
            ));
        }
        if manifest.generation <= self.manifest.generation {
            return Err(HawDBError::Storage(
                "search durable head regressed or changed an existing generation".into(),
            ));
        }
        let closure = load_artifact_closure_for_manifest(
            &self.root,
            &self.config,
            &self.analyzer_lexicon,
            self.lexical_term_policy,
            self.lexical_source_policy,
            manifest,
            Some(reuse::View::from_reader(self)),
        )?;
        let report = closure.refresh;
        Ok((Some(closure), report))
    }

    #[cfg(all(test, feature = "full-text-search"))]
    pub(crate) fn artifact_count(&self) -> usize {
        self.manifest.segments.len()
    }

    pub fn source_graph_commit_epoch(&self) -> Option<u64> {
        self.manifest.source_graph_commit_epoch
    }

    pub(super) fn can_append_after(
        &self,
        first_document_id: &str,
        memory: &crate::build_memory::BuildMemory,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<bool> {
        let mut generations = Vec::new();
        let mut generation_memory = memory.retained.reserve(0)?;
        crate::build_memory::reserve_capacity(
            &mut generations,
            self.manifest.segments.len(),
            &mut generation_memory,
        )?;
        for segment in &self.manifest.segments {
            crate::build_control::checkpoint(task)?;
            generations.push(segment.generation);
        }
        generations.sort_unstable();
        if generations.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(HawDBError::Storage(
                "search generation contains duplicate document content artifacts".into(),
            ));
        }
        let mut previous_last_document_id = None;
        let mut ordered = true;
        for artifact in &self.segments {
            crate::build_control::checkpoint(task)?;
            let descriptor_first = artifact.descriptor.segments.first();
            let descriptor_last = artifact.descriptor.segments.last();
            let lexical_bounds = artifact.lexical_projection.document_id_bounds();
            match (descriptor_first, descriptor_last, lexical_bounds) {
                (None, None, None) => continue,
                (Some(first), Some(last), Some((lexical_first, lexical_last)))
                    if first.first_document_id == lexical_first
                        && last.last_document_id == lexical_last => {}
                _ => {
                    return Err(HawDBError::Storage(
                        "search generation update requires descriptor and lexical document ranges to agree"
                            .to_string(),
                    ));
                }
            }
            for segment in &artifact.descriptor.segments {
                if previous_last_document_id
                    .is_some_and(|previous| previous >= segment.first_document_id.as_str())
                {
                    ordered = false;
                }
                previous_last_document_id = Some(segment.last_document_id.as_str());
            }
        }
        Ok(
            ordered
                && previous_last_document_id.is_none_or(|previous| previous < first_document_id),
        )
    }

    pub fn import_source_graph_commit_epoch(&self) -> Option<u64> {
        self.manifest.import_source_graph_commit_epoch
    }

    pub fn embedding_manifest(&self) -> Option<SearchEmbeddingManifest> {
        self.manifest
            .embedding_model
            .as_ref()
            .zip(self.manifest.embedding_dimension)
            .map(|(model, dimension)| SearchEmbeddingManifest {
                model: model.clone(),
                version: self.manifest.embedding_version.clone(),
                dimension,
            })
    }

    /// Decode the field binding sealed into this immutable projection.
    pub fn vector_index_descriptor(&self) -> Result<Option<hawdb_core::VectorIndexDescriptor>> {
        self.manifest
            .vector_index_binding
            .as_deref()
            .map(crate::vector_index::decode)
            .transpose()
    }

    pub fn analyzer_lexicon(&self) -> &SearchAnalyzerLexicon {
        &self.analyzer_lexicon
    }

    pub fn production_qualification_identity(
        &self,
    ) -> super::SearchProjectionQualificationIdentity {
        super::SearchProjectionQualificationIdentity {
            projection_generation: self.manifest.generation,
            source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            document_count: self.manifest.document_count,
            documents_digest: self.manifest.documents_digest,
            analyzer_digest: lexical_analyzer_digest(&self.analyzer_lexicon),
            embedding_model: self.manifest.embedding_model.clone(),
            embedding_version: self.manifest.embedding_version.clone(),
            embedding_dimension: self.manifest.embedding_dimension,
        }
    }

    pub fn vector_projection_qualification_identity(
        &self,
    ) -> Option<super::VectorProjectionQualificationIdentity> {
        #[cfg(feature = "vector-search")]
        {
            if self.has_diskann_projection() {
                return self.diskann_qualification().map(|(identity, _)| identity);
            }
            let projection = self.primary_segment().rabitq_projection.as_ref()?;
            let manifest = projection.manifest();
            Some(super::VectorProjectionQualificationIdentity {
                projection_generation: manifest.identity.generation,
                source_graph_commit_epoch: manifest.identity.source_epoch,
                document_count: manifest.document_count,
                source_digest: manifest.source_digest,
                payload_bytes: manifest.payload_bytes,
                payload_checksum: manifest.payload_checksum,
                format_version: manifest.format_version,
                algorithm: manifest.algorithm.to_owned(),
                bit_width: manifest.bit_width,
                dimension: manifest.dimension,
                transform_seed: manifest.transform_seed,
                embedding_model: manifest.identity.embedding_model.clone(),
                embedding_version: manifest.identity.embedding_version.clone(),
                file_backed: true,
            })
        }
        #[cfg(not(feature = "vector-search"))]
        {
            None
        }
    }

    pub fn vector_projection_resource_evidence(
        &self,
    ) -> Option<super::VectorProjectionResourceEvidence> {
        #[cfg(feature = "vector-search")]
        {
            if self.has_diskann_projection() {
                return self.diskann_qualification().map(|(_, resources)| resources);
            }
            let projection = self.primary_segment().rabitq_projection.as_ref()?;
            let manifest = projection.manifest();
            let raw_vector_bytes = (manifest.document_count as u64)
                .saturating_mul(manifest.dimension as u64)
                .saturating_mul(std::mem::size_of::<f32>() as u64);
            Some(super::VectorProjectionResourceEvidence {
                segment_count: manifest.segment_count,
                requested_segment_rows: manifest.requested_segment_rows,
                admitted_segment_rows: manifest.admitted_segment_rows,
                configured_build_working_bytes: manifest.configured_build_working_bytes,
                peak_build_working_bytes: manifest.peak_build_working_bytes,
                raw_vector_bytes,
                projection_payload_bytes: manifest.payload_bytes,
                build_write_amplification_per_million: manifest
                    .payload_bytes
                    .saturating_mul(1_000_000)
                    .checked_div(raw_vector_bytes.max(1))
                    .unwrap_or(u64::MAX),
            })
        }
        #[cfg(not(feature = "vector-search"))]
        {
            None
        }
    }

    pub fn resident_document_count(&self) -> usize {
        0
    }

    pub fn set_runtime_capabilities(&mut self, capabilities: RuntimeCapabilities) {
        self.runtime_capabilities =
            crate::compiled_capabilities::effective_runtime_capabilities(capabilities);
    }

    pub fn runtime_capabilities(&self) -> RuntimeCapabilities {
        self.runtime_capabilities
    }

    pub fn projection_freshness(&self) -> SearchProjectionFreshness {
        let read_marker = |name| {
            read_marker_lines_bounded(&self.root.join(name), MAX_MARKER_BYTES)
                .unwrap_or_else(|error| vec![format!("failed to read marker {name}: {error}")])
        };
        let full_reindex_reasons = read_marker(FULL_REINDEX_MARKER);
        let metadata_repair_reasons = read_marker(METADATA_REPAIR_MARKER);
        self.freshness_with_markers(full_reindex_reasons, metadata_repair_reasons)
    }

    fn freshness_with_markers(
        &self,
        full_reindex_reasons: Vec<String>,
        metadata_repair_reasons: Vec<String>,
    ) -> SearchProjectionFreshness {
        SearchProjectionFreshness {
            document_count: self.manifest.document_count,
            import_source_graph_commit_epoch: self.manifest.import_source_graph_commit_epoch,
            source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            durable_source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            has_uncheckpointed_changes: false,
            full_reindex_needed: !full_reindex_reasons.is_empty(),
            full_reindex_reasons,
            metadata_repair_needed: !metadata_repair_reasons.is_empty(),
            metadata_repair_reasons,
            embedding_model: self.manifest.embedding_model.clone(),
            embedding_version: self.manifest.embedding_version.clone(),
            embedding_dimension: self.manifest.embedding_dimension,
        }
    }

    pub fn search_with_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext::scalar(),
        )
    }

    /// Scores every eligible raw vector, bypassing approximate graph traversal.
    pub fn search_with_options_exact(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                vector_execution_options: VectorSearchExecutionOptions::default()
                    .with_exact_search(),
                ..SearchOutOfCoreExecutionContext::scalar()
            },
        )
    }

    pub fn search_with_options_compressed_vector_projection_mode(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: None,
                compressed_vector_search_mode,
                vector_execution_options: VectorSearchExecutionOptions::default(),
            },
        )
    }

    pub fn search_with_options_compressed_vector_projection_context(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
        task_context: &crate::RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: None,
                compressed_vector_search_mode,
                vector_execution_options: VectorSearchExecutionOptions::bounded(
                    NonZeroUsize::new(
                        self.config
                            .max_vector_search_parallelism
                            .get()
                            .min(task_context.admitted_parallelism().get()),
                    )
                    .expect("out-of-core vector parallelism is non-zero"),
                    self.config.max_vector_search_working_bytes.get(),
                    Some(task_context),
                ),
            },
        )
    }

    pub fn search_with_options_compressed_vector_projection_execution_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
        vector_execution_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: None,
                compressed_vector_search_mode,
                vector_execution_options,
            },
        )
    }

    pub fn search_with_options_access_control(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        access_control: SearchAccessControlContext,
    ) -> Result<SearchOutOfCoreOutput> {
        self.search_with_options_internal(
            query_text,
            query_embedding,
            mode,
            options,
            SearchOutOfCoreExecutionContext {
                access_control: Some(&access_control),
                compressed_vector_search_mode: CompressedVectorSearchMode::Disabled,
                vector_execution_options: VectorSearchExecutionOptions::default(),
            },
        )
    }

    pub fn hydrate_documents(
        &self,
        document_ids: &[String],
    ) -> Result<SearchOutOfCoreHydrationOutput> {
        let requested = document_ids.iter().cloned().collect::<BTreeSet<_>>();
        if requested.len() != document_ids.len() {
            return Err(HawDBError::Storage(
                "search hydration document ids must be unique".to_string(),
            ));
        }
        let targets = self.resolve_mutation_targets(&requested)?;
        let mut metrics = targets.metrics;
        let mut hydrated = targets
            .targets
            .into_iter()
            .map(|(id, target)| (id, target.document))
            .collect::<BTreeMap<_, _>>();
        let documents = document_ids
            .iter()
            .map(|id| {
                hydrated.remove(id).ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search document {id} was not found during bounded hydration"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        metrics.hydrated_documents = documents.len();
        Ok(SearchOutOfCoreHydrationOutput { documents, metrics })
    }

    /// Resolves the current content segment for each requested document.
    ///
    /// The input is a `BTreeSet` so mutation preparation receives a unique,
    /// deterministic document-id order. Resolution performs the same bounded
    /// physical hydration as serving, while retaining the manifest segment
    /// identity required for a segment-local replacement or delete.
    pub(crate) fn resolve_mutation_targets(
        &self,
        document_ids: &BTreeSet<String>,
    ) -> Result<SearchOutOfCoreMutationTargetOutput> {
        let mut metrics = SearchOutOfCoreMetrics::default();
        let targets = self.load_mutation_targets(document_ids, &mut metrics)?;
        metrics.hydrated_documents = targets.len();
        Ok(SearchOutOfCoreMutationTargetOutput { targets, metrics })
    }

    /// Resolves the content segment that currently owns one document.
    ///
    /// This reads the lexical document-id index but does not hydrate the
    /// document payload. A caller that rebuilds the selected segment therefore
    /// pays for the segment stream exactly once.
    pub(crate) fn resolve_mutation_segment(&self, document_id: &str) -> Result<Option<u64>> {
        let Some(route) = self.segment_for_document(document_id)? else {
            return Ok(None);
        };
        let artifact = self.segments.get(route.artifact_index).ok_or_else(|| {
            HawDBError::Storage(format!(
                "search mutation target references unknown layer {}",
                route.artifact_index
            ))
        })?;
        Ok(Some(artifact.content_segment_id))
    }

    #[cfg(test)]
    fn visit_documents_in_order(
        &self,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<SearchOutOfCoreMetrics> {
        let mut metrics = SearchOutOfCoreMetrics::default();
        for artifact in &self.segments {
            for segment in &artifact.descriptor.segments {
                for document in self.read_hydration_segment(artifact, segment, &mut metrics)? {
                    consumer(document)?;
                }
            }
        }
        Ok(metrics)
    }

    /// Scores and filters through the same path as full hits, without body hydration.
    pub fn search_candidates_with_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchOutOfCoreOutput<SearchOutOfCoreCandidate>> {
        self.search_candidates_with_context(
            query_text,
            query_embedding,
            mode,
            options,
            Default::default(),
        )
    }

    /// Selects candidate output with the same ACL and vector execution controls.
    pub fn search_candidates_with_context(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        execution: SearchOutOfCoreExecutionContext<'_>,
    ) -> Result<SearchOutOfCoreOutput<SearchOutOfCoreCandidate>> {
        self.search_with_materializer(
            query_text,
            query_embedding,
            mode,
            options,
            execution,
            |candidates, _context, metrics| {
                candidates
                    .iter()
                    .map(|candidate| {
                        let route = self.segment_for_document(&candidate.id)?.ok_or_else(|| {
                            HawDBError::Storage(
                                "ranked candidate has no visible content version".into(),
                            )
                        })?;
                        metrics.lexical_document_bytes_read = metrics
                            .lexical_document_bytes_read
                            .saturating_add(route.lexical_document_bytes_read);
                        metrics.lexical_document_block_reads =
                            metrics.lexical_document_block_reads.saturating_add(1);
                        Ok(SearchOutOfCoreCandidate {
                            scores: candidate.clone(),
                            generation: self.manifest.generation,
                            content_segment_id: self.segments[route.artifact_index]
                                .content_segment_id,
                        })
                    })
                    .collect()
            },
        )
    }

    fn search_with_options_internal<'execution>(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        execution: impl Into<query_memory::Execution<'execution>>,
    ) -> Result<SearchOutOfCoreOutput> {
        if options.limit > self.config.max_hydrated_documents.get() {
            return Err(HawDBError::Storage(format!(
                "search hydration requested {} documents, exceeding {}",
                options.limit, self.config.max_hydrated_documents
            )));
        }
        self.search_with_materializer(
            query_text,
            query_embedding,
            mode,
            options,
            execution,
            |candidates, context, metrics| self.hydrate_hits(candidates, context, metrics),
        )
    }

    fn search_with_materializer<'execution, H>(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        execution: impl Into<query_memory::Execution<'execution>>,
        materialize: impl FnOnce(
            &[SearchScoredCandidate],
            HitHydrationContext<'_>,
            &mut SearchOutOfCoreMetrics,
        ) -> Result<Vec<H>>,
    ) -> Result<SearchOutOfCoreOutput<H>> {
        let query_memory::Execution {
            base: execution,
            query,
            immutable_freshness,
            scratch,
            output,
        } = execution.into();
        let output_memory = query_output::Memory(output);
        if output.is_some() != query.is_some() {
            return Err(HawDBError::Execution(
                "native query requires paired working and result admission".into(),
            ));
        }
        let SearchOutOfCoreExecutionContext {
            access_control,
            compressed_vector_search_mode,
            vector_execution_options,
        } = execution;
        let task_context = vector_execution_options.task_context;
        if let Some(task_context) = task_context {
            task_context.checkpoint().map_err(|reason| {
                HawDBError::Execution(format!("search out-of-core task {reason}"))
            })?;
        }
        if access_control.is_some() {
            self.runtime_capabilities
                .require(RuntimeCapability::AccessControl)?;
        }
        self.require_search_capabilities(mode)?;
        let page_score_limit = options
            .offset
            .checked_add(options.limit)
            .ok_or_else(|| HawDBError::Storage("search offset and limit overflow".to_string()))?;
        if page_score_limit > self.config.max_score_entries.get() {
            return Err(HawDBError::Storage(format!(
                "search page window requires {page_score_limit} score entries, exceeding {}",
                self.config.max_score_entries
            )));
        }
        if options
            .rank_window
            .is_some_and(|window| window > self.config.max_score_entries.get())
        {
            return Err(HawDBError::Storage(format!(
                "search rank window exceeds the admitted {} score entries",
                self.config.max_score_entries
            )));
        }

        let metadata_filters = match access_control {
            Some(access_control) => {
                if let Some(policy_epoch) = options.policy_epoch
                    && policy_epoch != access_control.policy_epoch
                {
                    return Err(HawDBError::Storage(format!(
                        "search options policy epoch {policy_epoch} does not match access control policy epoch {}",
                        access_control.policy_epoch
                    )));
                }
                access_control.effective_metadata_filters(&options.metadata_filters)?
            }
            None => output_memory.map(&options.metadata_filters)?,
        };
        let policy_epoch = access_control
            .map(|access_control| access_control.policy_epoch)
            .or(options.policy_epoch);
        let normalized_terms = self.query_terms(query_text, query)?;
        let query_terms = &normalized_terms.values;
        let mut predicate_pushdown = query_memory::Predicates::new(&metadata_filters, query)?;
        predicate_pushdown.report.parse_error =
            output_memory.optional(predicate_pushdown.report.parse_error.as_deref())?;
        let mut metrics = SearchOutOfCoreMetrics::default();
        let candidate_set = self.build_candidate_set(
            candidate_set::CandidateFilter {
                predicates: &predicate_pushdown.predicates,
                eligible: None,
            },
            &mut predicate_pushdown.report,
            &mut metrics,
            query,
            scratch,
            output_memory,
        )?;
        let filtered_document_count = candidate_set.cardinality();
        let candidate_report = SearchCandidateSetReport {
            id_space: output_memory.text("search_projection_document_id")?,
            representation: output_memory.text(candidate_set.representation())?,
            cardinality: filtered_document_count,
            exact: true,
            snapshot_source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            policy_epoch,
            filtered_out_count: self
                .manifest
                .document_count
                .saturating_sub(filtered_document_count),
            metadata_filters: output_memory.map(&options.metadata_filters)?,
            metadata_predicate_pushdown: predicate_pushdown.report,
        };

        let text_available = !query_terms.is_empty();
        let (text_fallback_reason_codes, text_fallback_reasons) =
            if !text_available && mode != SearchMode::Vector {
                (
                    output_memory.codes(&[SearchFallbackReasonCode::TextQueryEmpty])?,
                    output_memory.one_text("query text produced no searchable terms")?,
                )
            } else {
                (Vec::new(), Vec::new())
            };

        let mut vector_fallback_reason_codes = Vec::new();
        let mut vector_fallback_reasons = Vec::new();
        let vector_available = match (query_embedding, self.manifest.embedding_dimension) {
            (Some(vector), Some(dimension)) if vector.len() == dimension => true,
            (Some(vector), Some(dimension)) => {
                output_memory.push(
                    &mut vector_fallback_reason_codes,
                    SearchFallbackReasonCode::VectorDimensionMismatch,
                )?;
                let reason = output_memory.format(format_args!(
                    "query embedding dimension {} does not match index dimension {dimension}",
                    vector.len()
                ))?;
                output_memory.push(&mut vector_fallback_reasons, reason)?;
                false
            }
            (Some(_), None) => {
                output_memory.push(
                    &mut vector_fallback_reason_codes,
                    SearchFallbackReasonCode::VectorIndexEmpty,
                )?;
                let reason = output_memory.text("index has no vector rows")?;
                output_memory.push(&mut vector_fallback_reasons, reason)?;
                false
            }
            (None, _) => {
                if mode != SearchMode::Text {
                    output_memory.push(
                        &mut vector_fallback_reason_codes,
                        SearchFallbackReasonCode::QueryEmbeddingMissing,
                    )?;
                    let reason = output_memory.text("query embedding not provided")?;
                    output_memory.push(&mut vector_fallback_reasons, reason)?;
                }
                false
            }
        };

        let retained_text_limit = match mode {
            SearchMode::Text => Some(page_score_limit),
            SearchMode::Hybrid => options.rank_window,
            SearchMode::Vector => Some(0),
        };
        let text = if text_available && mode != SearchMode::Vector {
            Some(self.query_text_scores(
                query_terms,
                retained_text_limit,
                mode == SearchMode::Hybrid,
                &candidate_set,
                &mut metrics,
                query,
            )?)
        } else {
            None
        };
        let empty_scores = BTreeMap::new();
        let (
            text_scores,
            text_matching_count,
            lexical_postings_visited,
            lexical_bytes_read,
            lexical_blocks_skipped,
        ) = match &text {
            Some(scores) => (
                &scores.values,
                scores.matching_count,
                scores.postings_visited,
                scores.bytes_read,
                scores.blocks_skipped,
            ),
            None => (&empty_scores, 0, 0, 0, 0),
        };

        let retained_vector_limit = match mode {
            SearchMode::Vector => Some(page_score_limit),
            SearchMode::Hybrid => options.rank_window,
            SearchMode::Text => Some(0),
        };
        let vector_scan = if vector_available && mode != SearchMode::Text {
            self.scan_vector_scores(
                query_embedding.expect("vector availability requires an embedding"),
                &candidate_set,
                retained_vector_limit,
                vector_serving::ScanOptions {
                    compressed: compressed_vector_search_mode,
                    execution: vector_execution_options,
                    #[cfg(feature = "vector-search")]
                    candidate_window: options.rank_window,
                    query,
                },
                &mut metrics,
            )?
        } else {
            VectorScoreScan::default()
        };
        let vector_fallback_reason_codes = output_memory.combine(
            &vector_fallback_reason_codes,
            &vector_scan.fallback_reason_codes,
        )?;
        let vector_fallback_reasons = output_memory
            .combine_strings(&vector_fallback_reasons, &vector_scan.fallback_reasons)?;
        let vector_scores = &vector_scan.scores;

        let vector_ranks = query_ranking::Ranks::new(vector_scores, query)?;
        let text_ranks = query_ranking::Ranks::new(text_scores, query)?;
        let vector_window_ranks = vector_ranks.window(options.rank_window, query)?;
        let text_window_ranks = text_ranks.window(options.rank_window, query)?;
        let fallback_reason_codes =
            output_memory.combine(&vector_fallback_reason_codes, &text_fallback_reason_codes)?;
        let fallback_reasons =
            output_memory.combine_strings(&vector_fallback_reasons, &text_fallback_reasons)?;

        let mut retrievers = output_memory.vec(2)?;
        retrievers.push(SearchRetrieverReport {
            name: output_memory.text("vector")?,
            backend: output_memory.text(&vector_scan.backend)?,
            backend_selection_reason: None,
            estimated_raw_vector_bytes: self.manifest.embedding_dimension.map(|dimension| {
                (dimension as u64)
                    .saturating_mul(std::mem::size_of::<f32>() as u64)
                    .saturating_mul(filtered_document_count as u64)
            }),
            filter_selectivity_per_million: None,
            available: vector_available && mode != SearchMode::Text,
            input_candidate_set: output_memory.candidate(&candidate_report)?,
            candidate_score_source: output_memory.text(
                if vector_available && mode != SearchMode::Text {
                    vector_scan.candidate_score_source.as_str()
                } else {
                    "none"
                },
            )?,
            final_score_source: output_memory.text(
                if vector_available && mode != SearchMode::Text {
                    "raw_vector"
                } else {
                    "none"
                },
            )?,
            generated_candidate_count: vector_scan.generated_candidate_count,
            candidate_scan_rounds: vector_scan.segment_scan_count,
            descriptor_pruned_count: candidate_report
                .metadata_predicate_pushdown
                .segment_pruned_document_count,
            scalar_filtered_count: candidate_report.filtered_out_count.saturating_sub(
                candidate_report
                    .metadata_predicate_pushdown
                    .segment_pruned_document_count,
            ),
            residual_filtered_count: 0,
            reranked_candidate_count: vector_scan.reranked_candidate_count,
            raw_vector_bytes_read: metrics.vector_bytes_read,
            candidate_scan_kernel: output_memory
                .optional(vector_scan.candidate_scan_kernel.as_deref())?,
            candidate_scan_worker_count: vector_scan.candidate_scan_worker_count,
            candidate_scan_segment_count: vector_scan.candidate_scan_segment_count,
            candidate_scan_scanned_segment_count: vector_scan.candidate_scan_scanned_segment_count,
            candidate_scan_scored_document_count: vector_scan.candidate_scan_scored_document_count,
            candidate_scan_filtered_document_count: vector_scan
                .candidate_scan_filtered_document_count,
            candidate_scan_scanned_block_count: vector_scan.candidate_scan_scanned_block_count,
            candidate_scan_skipped_block_count: vector_scan.candidate_scan_skipped_block_count,
            candidate_scan_adaptive_exact_scan_count: vector_scan
                .candidate_scan_adaptive_exact_scan_count,
            candidate_scan_payload_bytes_read: vector_scan.candidate_scan_payload_bytes_read,
            candidate_scan_admitted_working_bytes: vector_scan
                .candidate_scan_admitted_working_bytes,
            posting_bytes_read: 0,
            candidate_postings_visited: 0,
            candidate_blocks_skipped: 0,
            segmented_lexical_projection_used: false,
            index_covered_document_count: vector_scan.vector_document_count,
            index_candidate_document_count: vector_scan.vector_document_count,
            index_coverage_complete: true,
            candidate_count: vector_scan.matching_count,
            candidate_set: output_memory.retriever_set(
                vector_window_ranks.len(),
                self.manifest.source_graph_commit_epoch,
                policy_epoch,
            )?,
            fallback_reason_codes: output_memory.codes(&vector_fallback_reason_codes)?,
            fallback_reasons: output_memory.strings(&vector_fallback_reasons)?,
            candidate_top_ids: Vec::new(),
            top_hit_ids: output_memory.top_ids(&vector_window_ranks, options.limit)?,
            top_candidates: output_memory.top_candidates(
                &vector_window_ranks,
                vector_scores,
                options.limit,
            )?,
        });
        retrievers.push(SearchRetrieverReport {
            name: output_memory.text("text")?,
            backend: output_memory.text("segmented_bm25_text")?,
            backend_selection_reason: None,
            estimated_raw_vector_bytes: None,
            filter_selectivity_per_million: None,
            available: text_available && mode != SearchMode::Vector,
            input_candidate_set: output_memory.candidate(&candidate_report)?,
            candidate_score_source: output_memory.text(
                if text_available && mode != SearchMode::Vector {
                    "segmented_bm25"
                } else {
                    "none"
                },
            )?,
            final_score_source: output_memory.text(
                if text_available && mode != SearchMode::Vector {
                    "segmented_bm25"
                } else {
                    "none"
                },
            )?,
            generated_candidate_count: text_matching_count,
            candidate_scan_rounds: 0,
            descriptor_pruned_count: candidate_report
                .metadata_predicate_pushdown
                .segment_pruned_document_count,
            scalar_filtered_count: candidate_report.filtered_out_count.saturating_sub(
                candidate_report
                    .metadata_predicate_pushdown
                    .segment_pruned_document_count,
            ),
            residual_filtered_count: 0,
            reranked_candidate_count: 0,
            raw_vector_bytes_read: 0,
            candidate_scan_kernel: None,
            candidate_scan_worker_count: 0,
            candidate_scan_segment_count: 0,
            candidate_scan_scanned_segment_count: 0,
            candidate_scan_scored_document_count: 0,
            candidate_scan_filtered_document_count: 0,
            candidate_scan_scanned_block_count: 0,
            candidate_scan_skipped_block_count: 0,
            candidate_scan_adaptive_exact_scan_count: 0,
            candidate_scan_payload_bytes_read: 0,
            candidate_scan_admitted_working_bytes: 0,
            posting_bytes_read: lexical_bytes_read,
            candidate_postings_visited: lexical_postings_visited,
            candidate_blocks_skipped: lexical_blocks_skipped,
            segmented_lexical_projection_used: true,
            index_covered_document_count: 0,
            index_candidate_document_count: 0,
            index_coverage_complete: true,
            candidate_count: text_matching_count,
            candidate_set: output_memory.retriever_set(
                text_window_ranks.len(),
                self.manifest.source_graph_commit_epoch,
                policy_epoch,
            )?,
            fallback_reason_codes: text_fallback_reason_codes,
            fallback_reasons: text_fallback_reasons,
            candidate_top_ids: Vec::new(),
            top_hit_ids: output_memory.top_ids(&text_window_ranks, options.limit)?,
            top_candidates: output_memory.top_candidates(
                &text_window_ranks,
                text_scores,
                options.limit,
            )?,
        });

        let mut scored_candidates = query_ranking::Candidates::new(
            query_ranking::Inputs {
                vector_scores,
                text_scores,
                vector_ranks: &vector_ranks,
                text_ranks: &text_ranks,
                vector_window: &vector_window_ranks,
                text_window: &text_window_ranks,
                mode,
                weights: options.fusion_weights,
            },
            query,
        )?;
        let total_hits = match mode {
            SearchMode::Text => text_matching_count,
            SearchMode::Vector => vector_scan.matching_count,
            SearchMode::Hybrid => scored_candidates.values.len(),
        };
        let page_end = options.offset.saturating_add(options.limit);
        let truncated = total_hits > page_end;
        scored_candidates.page(options.offset, options.limit)?;
        let page_candidates = &scored_candidates.values;
        let projection_freshness = if immutable_freshness {
            // Born construction checked the actual native directory. Saved
            // paths and later unrelated marker files cannot redirect this view.
            output_memory.immutable_freshness(self)?
        } else if output.is_some() {
            output_memory.freshness(&self.projection_freshness())?
        } else {
            self.projection_freshness()
        };
        let hydration = HitHydrationContext {
            query_terms,
            query_embedding,
            mode,
            fallback_reason_codes: &fallback_reason_codes,
            fallback_reasons: &fallback_reasons,
            projection_freshness: &projection_freshness,
            query,
            output_memory,
        };
        let hits = materialize(page_candidates, hydration, &mut metrics)?;

        let truncation_reasons = if truncated && options.offset > 0 {
            let reason = output_memory.format(format_args!(
                "offset {} limit {} returned from {total_hits} matching hits",
                options.offset, options.limit
            ))?;
            output_memory.one_owned(reason)?
        } else if truncated {
            let reason = output_memory.format(format_args!(
                "limit {} returned from {total_hits} matching hits",
                options.limit
            ))?;
            output_memory.one_owned(reason)?
        } else {
            Vec::new()
        };
        let truncation_reason_codes = if truncated {
            output_memory.codes(&[SearchTruncationReasonCode::LimitExceeded])?
        } else {
            Vec::new()
        };
        let empty_state = query_output::report::Empty {
            empty: hits.is_empty(),
            documents: self.manifest.document_count,
            filtered: filtered_document_count,
            total: total_hits,
            page: SearchPageWindow {
                offset: options.offset,
                limit: options.limit,
            },
        };
        let empty_reasons =
            output_memory.empty_reasons(empty_state, &truncation_reasons, &fallback_reasons)?;
        let empty_reason_codes = output_memory.empty_codes(empty_state)?;
        if let Some(query) = query {
            query.checkpoint()?;
        }
        Ok(SearchOutOfCoreOutput {
            result: SearchResultSet {
                hits,
                total_hits,
                limit: options.limit,
                offset: options.offset,
                truncated,
                truncation_reason_codes,
                truncation_reasons,
                empty_reason_codes,
                empty_reasons,
                fallback_reason_codes,
                fallback_reasons,
                retrievers,
                candidate_set: candidate_report,
                rank_window: options.rank_window,
                fusion_weights: options.fusion_weights,
                document_count: self.manifest.document_count,
                filtered_document_count,
                projection_freshness,
            },
            metrics,
        })
    }

    fn hydrate_hits(
        &self,
        candidates: &[SearchScoredCandidate],
        context: HitHydrationContext<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchHit>> {
        if candidates.len() > self.config.max_hydrated_documents.get() {
            return Err(HawDBError::Storage(format!(
                "search hydration requires {} documents, exceeding {}",
                candidates.len(),
                self.config.max_hydrated_documents
            )));
        }
        let mut hydrated = hydration::query::Loaded::new(self, candidates, metrics, context.query)?;
        let query_scorer = if context.mode != SearchMode::Text && !candidates.is_empty() {
            context
                .query_embedding
                .map(|query| {
                    CosineQuery::with_work(query, || {
                        context.query.map_or(Ok(()), |query| query.checkpoint())
                    })
                })
                .transpose()?
        } else {
            None
        };
        let mut hits = context.output_memory.vec(candidates.len())?;
        let mut matched_span_count = 0usize;
        let mut matched_span_bytes = 0u64;
        for candidate in candidates {
            let document = hydrated.remove(&candidate.id).ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search candidate {} was not found during late hydration",
                    candidate.id
                ))
            })?;
            let vector_score = if context.mode != SearchMode::Text {
                context
                    .query_embedding
                    .zip(document.embedding.as_deref())
                    .filter(|(query, embedding)| {
                        !embedding.is_empty() && query.len() == embedding.len()
                    })
                    .and_then(|(_, embedding)| query_scorer.as_ref()?.score(embedding))
                    .filter(|score| *score > 0.0)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let matched = context.output_memory.matches(
                context.query_terms,
                &document,
                &self.analyzer_lexicon,
                query_output::matches::Limits {
                    count: self
                        .config
                        .max_matched_spans
                        .get()
                        .saturating_sub(matched_span_count),
                    bytes: self.config.max_matched_span_bytes.get(),
                    used: matched_span_bytes,
                },
            )?;
            let matched_spans = matched.spans;
            matched_span_count = matched_span_count.saturating_add(matched_spans.len());
            matched_span_bytes = matched_span_bytes.saturating_add(
                matched_spans
                    .iter()
                    .map(matched_span_bytes_for)
                    .sum::<u64>(),
            );
            hits.push(SearchHit {
                id: context.output_memory.text(&candidate.id)?,
                score: candidate.score,
                vector_score,
                text_score: candidate.text_score,
                rrf_score: candidate.rrf_score,
                vector_rrf_score: candidate.vector_rrf_score,
                text_rrf_score: candidate.text_rrf_score,
                vector_rank: candidate.vector_rank,
                text_rank: candidate.text_rank,
                kind: context
                    .output_memory
                    .optional(document.metadata.get("kind").map(String::as_str))?,
                external_id: context
                    .output_memory
                    .optional(document.metadata.get("external_id").map(String::as_str))?,
                source_id: context
                    .output_memory
                    .optional(document.metadata.get("source_id").map(String::as_str))?,
                matched_terms: matched.terms,
                matched_spans,
                fallback_reason_codes: context
                    .output_memory
                    .codes(context.fallback_reason_codes)?,
                fallback_reasons: context.output_memory.strings(context.fallback_reasons)?,
                projection_freshness: context
                    .output_memory
                    .freshness(context.projection_freshness)?,
            });
        }
        metrics.hydrated_documents = hits.len();
        Ok(hits)
    }

    fn load_documents(
        &self,
        document_ids: &BTreeSet<String>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<BTreeMap<String, SearchDocument>> {
        Ok(self
            .load_document_targets(document_ids, metrics, "hydration")?
            .into_iter()
            .map(|(id, target)| (id, target.document))
            .collect())
    }

    fn load_mutation_targets(
        &self,
        document_ids: &BTreeSet<String>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<BTreeMap<String, SearchOutOfCoreMutationTarget>> {
        self.load_document_targets(document_ids, metrics, "mutation target resolution")
    }

    fn load_document_targets(
        &self,
        document_ids: &BTreeSet<String>,
        metrics: &mut SearchOutOfCoreMetrics,
        operation: &str,
    ) -> Result<BTreeMap<String, SearchOutOfCoreMutationTarget>> {
        if document_ids.len() > self.config.max_hydrated_documents.get() {
            return Err(HawDBError::Storage(format!(
                "search {operation} requires {} documents, exceeding {}",
                document_ids.len(),
                self.config.max_hydrated_documents
            )));
        }
        let mut segment_documents = BTreeMap::<(usize, u64), BTreeSet<String>>::new();
        for id in document_ids {
            let route = self.segment_for_document(id)?.ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search document {id} is outside the published document ranges"
                ))
            })?;
            if route.lexical_document_bytes_read > 0 {
                metrics.lexical_document_block_reads =
                    metrics.lexical_document_block_reads.saturating_add(1);
                metrics.lexical_document_bytes_read = metrics
                    .lexical_document_bytes_read
                    .saturating_add(route.lexical_document_bytes_read);
            }
            segment_documents
                .entry((route.artifact_index, route.segment.segment_id))
                .or_default()
                .insert(id.clone());
        }
        let mut targets = BTreeMap::<String, SearchOutOfCoreMutationTarget>::new();
        let mut hydrated_bytes = 0u64;
        for ((layer, segment_id), ids) in segment_documents {
            let artifact = self.segments.get(layer).ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search {operation} references unknown layer {layer}"
                ))
            })?;
            let segment = self
                .segments
                .get(layer)
                .expect("layer was checked above")
                .descriptor
                .segments
                .get(segment_id as usize)
                .filter(|segment| segment.segment_id == segment_id)
                .ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search {operation} references unknown segment {segment_id}"
                    ))
                })?;
            for document in artifact.read_selected_hydration_segment(
                &self.config,
                segment,
                &ids,
                hydrated_bytes,
                metrics,
            )? {
                hydrated_bytes = hydrated_bytes
                    .checked_add(search_document_bytes(&document))
                    .ok_or_else(|| {
                        HawDBError::Storage(format!("search {operation} byte count overflow"))
                    })?;
                // Selected decoding already enforces this cap; retain the
                // aggregate check as a consistency guard for that accounting.
                if hydrated_bytes > self.config.max_hydrated_bytes.get() {
                    return Err(HawDBError::Storage(format!(
                        "search {operation} requires {hydrated_bytes} bytes, exceeding {}",
                        self.config.max_hydrated_bytes
                    )));
                }
                let id = document.id.clone();
                if targets
                    .insert(
                        id.clone(),
                        SearchOutOfCoreMutationTarget {
                            content_segment_id: artifact.content_segment_id,
                            document,
                        },
                    )
                    .is_some()
                {
                    return Err(HawDBError::Storage(format!(
                        "search {operation} found duplicate document {id}"
                    )));
                }
            }
        }
        if targets.len() != document_ids.len() {
            return Err(HawDBError::Storage(format!(
                "search {operation} found {} of {} requested documents",
                targets.len(),
                document_ids.len()
            )));
        }
        metrics.hydrated_bytes = hydrated_bytes;
        Ok(targets)
    }

    fn segment_for_document(&self, id: &str) -> Result<Option<SearchDocumentSegmentRoute<'_>>> {
        self.segment_for_document_with_context(id, None)
    }

    fn segment_for_document_with_context(
        &self,
        id: &str,
        context: Option<crate::lexical_projection::QueryContext<'_>>,
    ) -> Result<Option<SearchDocumentSegmentRoute<'_>>> {
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let mut route = None;
        let mut lexical_document_bytes_read = 0u64;
        for (artifact_index, artifact) in self.segments.iter().enumerate() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if !self.visibility.is_visible(artifact.content_segment_id, id) {
                continue;
            }
            let Ok(index) = artifact.descriptor.segments.binary_search_by(|segment| {
                if segment.last_document_id.as_str() < id {
                    CmpOrdering::Less
                } else if segment.first_document_id.as_str() > id {
                    CmpOrdering::Greater
                } else {
                    CmpOrdering::Equal
                }
            }) else {
                continue;
            };
            let segment = artifact
                .descriptor
                .segments
                .get(index)
                .expect("binary search returned an existing descriptor segment");
            let probe = match context {
                Some(context) => artifact
                    .lexical_projection
                    .probe_document_id_with_query(id, context)?,
                None => artifact.lexical_projection.probe_document_id(id)?,
            };
            lexical_document_bytes_read =
                lexical_document_bytes_read.saturating_add(probe.bytes_read);
            if !probe.present {
                continue;
            }
            if route.replace((artifact_index, segment)).is_some() {
                return Err(HawDBError::Storage(format!(
                    "search out-of-core segment set has duplicate document {id}"
                )));
            }
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        Ok(
            route.map(|(artifact_index, segment)| SearchDocumentSegmentRoute {
                artifact_index,
                segment,
                lexical_document_bytes_read,
            }),
        )
    }

    fn require_search_capabilities(&self, mode: SearchMode) -> Result<()> {
        match mode {
            SearchMode::Hybrid => {
                self.runtime_capabilities
                    .require(RuntimeCapability::VectorSearch)?;
                self.runtime_capabilities
                    .require(RuntimeCapability::FullTextSearch)
            }
            SearchMode::Vector => self
                .runtime_capabilities
                .require(RuntimeCapability::VectorSearch),
            SearchMode::Text => self
                .runtime_capabilities
                .require(RuntimeCapability::FullTextSearch),
        }
    }

    #[cfg(test)]
    fn read_hydration_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchDocument>> {
        let range = segment.payload_range.ok_or_else(|| {
            HawDBError::Storage(format!(
                "search segment {} has no payload range",
                segment.segment_id
            ))
        })?;
        let payload = read_out_of_core_payload_range(
            &artifact.payload,
            SearchOutOfCoreRange {
                offset: range.offset,
                length: range.length,
                checksum: range.checksum,
                entry_count: segment.document_count,
            },
            segment.segment_id,
            "hydration",
            metrics,
        )?;
        metrics.hydration_segment_bytes_read = metrics
            .hydration_segment_bytes_read
            .saturating_add(range.length);
        let documents = decode_search_segment_documents_bounded(
            &payload,
            self.config.max_uncompressed_segment_bytes.get(),
        )?;
        validate_search_segment_documents(segment, &documents)?;
        metrics.peak_segment_document_bytes = metrics
            .peak_segment_document_bytes
            .max(documents.iter().map(search_document_bytes).sum());
        Ok(documents)
    }

    fn read_metadata_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchMetadataDocument>> {
        self.read_metadata_segment_with(
            artifact,
            segment,
            metrics,
            decode_search_snapshot_text_bounded,
        )
    }

    fn read_metadata_segment_admitted(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        memory: &hawdb_executor::QueryMemoryAccount,
        task: &crate::RuntimeTaskContext,
    ) -> Result<Vec<SearchMetadataDocument>> {
        self.read_metadata_segment_with(artifact, segment, metrics, |payload, limit| {
            crate::decode_search_snapshot_text_admitted(payload, limit, memory, task)
        })
    }

    fn read_metadata_segment_with(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        decode: impl FnOnce(&[u8], u64) -> Result<String>,
    ) -> Result<Vec<SearchMetadataDocument>> {
        let range = Self::artifact_layout_range(artifact, segment.segment_id)?.metadata;
        let payload = read_out_of_core_payload_range(
            &artifact.metadata_payload,
            range,
            segment.segment_id,
            "metadata",
            metrics,
        )?;
        metrics.metadata_segment_bytes_read = metrics
            .metadata_segment_bytes_read
            .saturating_add(range.length);
        let text = decode(&payload, self.config.max_uncompressed_segment_bytes.get())?;
        metrics.peak_metadata_segment_bytes =
            metrics.peak_metadata_segment_bytes.max(text.len() as u64);
        let documents = decode_metadata_segment(&text, segment, range.entry_count)?;
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let ordinals = documents
            .iter()
            .filter_map(|document| document.vector_ordinal)
            .collect::<Vec<_>>();
        if ordinals.len() != layout.vectors.entry_count
            || ordinals.iter().enumerate().any(|(offset, ordinal)| {
                *ordinal != layout.vector_ordinal_base.saturating_add(offset as u64)
            })
        {
            return Err(HawDBError::Storage(format!(
                "search segment {} metadata vector ordinals do not match its layout",
                segment.segment_id
            )));
        }
        Ok(documents)
    }

    fn read_vector_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchVectorDocument>> {
        self.read_vector_segment_selected(artifact, segment, metrics, usize::MAX, |_| true)
    }

    fn read_vector_segment_selected(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        selected_capacity: usize,
        selected: impl FnMut(u64) -> bool,
    ) -> Result<Vec<SearchVectorDocument>> {
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let range = layout.vectors;
        let payload = read_out_of_core_payload_range(
            &artifact.vector_payload,
            range,
            segment.segment_id,
            "vector",
            metrics,
        )?;
        metrics.vector_segment_bytes_read = metrics
            .vector_segment_bytes_read
            .saturating_add(range.length);
        let text = decode_search_snapshot_text_bounded(
            &payload,
            self.config.max_uncompressed_segment_bytes.get(),
        )?;
        metrics.peak_vector_segment_bytes =
            metrics.peak_vector_segment_bytes.max(text.len() as u64);
        decode_vector_segment(
            &text,
            segment,
            range.entry_count,
            layout.vector_ordinal_base,
            self.manifest.embedding_dimension,
            selected_capacity,
            selected,
        )
    }

    fn artifact_layout_range(
        artifact: &SearchOutOfCoreSegmentReader,
        segment_id: u64,
    ) -> Result<&SearchOutOfCoreSegmentLayout> {
        artifact
            .layout
            .segments
            .get(segment_id as usize)
            .filter(|layout| layout.segment_id == segment_id)
            .ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layout has no segment {segment_id}"
                ))
            })
    }
}

fn verify_rabitq_artifact(root: &Path, manifest: &SearchOutOfCoreSegmentManifest) -> Result<()> {
    let Some(file_name) = manifest.rabitq_artifact_file.as_deref() else {
        return Ok(());
    };
    let expected_len = manifest
        .rabitq_artifact_len
        .expect("validated RaBitQ identity has a length");
    let expected_checksum = manifest
        .rabitq_artifact_checksum
        .expect("validated RaBitQ identity has a checksum");
    let (actual_len, actual_checksum) = file_len_checksum_streaming(&root.join(file_name))?;
    if actual_len != expected_len || actual_checksum != expected_checksum {
        return Err(HawDBError::Storage(
            "search out-of-core RaBitQ artifact does not match its manifest".to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "vector-search")]
fn open_rabitq_projection(
    root: &Path,
    active_manifest: &SearchOutOfCoreManifestBody,
    manifest: &SearchOutOfCoreSegmentManifest,
    max_working_bytes: usize,
    captured_file: Option<File>,
) -> Result<Option<hawdb_vector_projection::FileProjection>> {
    if captured_file.is_none() {
        verify_rabitq_artifact(root, manifest)?;
    }
    let Some(file_name) = manifest.rabitq_artifact_file.as_deref() else {
        return Ok(None);
    };
    let peak_build_working_bytes = manifest
        .rabitq_peak_build_working_bytes
        .expect("validated RaBitQ identity has a build memory bound");
    if peak_build_working_bytes > max_working_bytes {
        return Err(HawDBError::Storage(format!(
            "search RaBitQ artifact requires {peak_build_working_bytes} build bytes, exceeding the serving admission {max_working_bytes}"
        )));
    }
    let projection = match captured_file {
        Some(mut file) => {
            let (length, checksum) = file_len_checksum_held(&mut file)?;
            if Some(length) != manifest.rabitq_artifact_len
                || Some(checksum) != manifest.rabitq_artifact_checksum
            {
                return Err(HawDBError::StorageIntegrity(
                    "captured vector CRC mismatch".into(),
                ));
            }
            hawdb_vector_projection::FileProjection::open_file(file, &root.join(file_name))
        }
        None => hawdb_vector_projection::FileProjection::open(root.join(file_name)),
    }
    .map_err(vector_projection_error)?;
    let artifact_manifest = projection.manifest();
    if let Some(encoded) = &active_manifest.vector_index_binding {
        let definition = crate::vector_index::decode(encoded)?;
        crate::vector_index::validate_projection(&definition, artifact_manifest)?;
    }
    let expected_identity = hawdb_vector_projection::ProjectionIdentity {
        generation: manifest.generation,
        source_epoch: manifest.source_graph_commit_epoch,
        embedding_model: active_manifest.embedding_model.clone(),
        embedding_version: active_manifest.embedding_version.clone(),
    };
    if artifact_manifest.identity != expected_identity
        || Some(artifact_manifest.dimension) != active_manifest.embedding_dimension
        || Some(artifact_manifest.document_count) != manifest.rabitq_vector_document_count
        || Some(artifact_manifest.source_digest) != manifest.rabitq_source_digest
        || Some(artifact_manifest.payload_checksum) != manifest.rabitq_payload_checksum
    {
        return Err(HawDBError::Storage(
            "search out-of-core RaBitQ identity does not match its generation".to_string(),
        ));
    }
    Ok(Some(projection))
}

fn file_len_checksum_streaming(path: &Path) -> Result<(u64, u64)> {
    let mut file = File::open(path)?;
    file_len_checksum_held(&mut file)
}

fn file_len_checksum_held(file: &mut File) -> Result<(u64, u64)> {
    file.seek(SeekFrom::Start(0))?;
    let expected_len = file.metadata()?.len();
    let mut hasher = hawdb_integrity::Crc32cHasher::new();
    let mut actual_len = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        actual_len = actual_len.saturating_add(read as u64);
    }
    if actual_len != expected_len {
        return Err(HawDBError::Storage(
            "held search artifact changed while checksumming".into(),
        ));
    }
    file.seek(SeekFrom::Start(0))?;
    Ok((actual_len, hasher.finish()))
}

// Cleanup needs the validated artifact closure even when its format is not
// ready for serving. This helper cannot construct a public query reader.
struct LoadedArtifactClosure {
    manifest: SearchOutOfCoreManifestBody,
    segments: Vec<SearchOutOfCoreSegmentReader>,
    visibility: mutation_run::MutationVisibility,
    refresh: SearchOutOfCoreRefreshReport,
    collections_memory: Option<crate::build_memory::read::ReaderMemory>,
}

struct ClosureLoadContext<'a, 'execution> {
    validation_source: Option<&'a captured_cleanup::Source<'a>>,
    cleanup_source: Option<&'a captured_cleanup::Source<'a>>,
    capture: Option<&'a root_registry::PendingCapture<'a, 'execution>>,
    root: &'a Path,
    config: &'a SearchOutOfCoreConfig,
    analyzer_lexicon: &'a SearchAnalyzerLexicon,
    lexical_term_policy: SearchLexicalTermPolicy,
    lexical_source_policy: SearchLexicalSourcePolicy,
    previous: Option<reuse::View<'a>>,
    admission: Option<&'a crate::build_memory::read::ReadAdmission<'execution>>,
    max_artifact_read_bytes: u64,
}

/// Target-validation work performed while attaching a durable search head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchOutOfCoreRefreshReport {
    pub generation: u64,
    pub generation_changed: bool,
    pub validated_retractions: usize,
    pub reused_retractions: usize,
    pub opened_content_segments: usize,
    pub reused_content_segments: usize,
}

fn load_artifact_closure_for_manifest(
    root: &Path,
    config: &SearchOutOfCoreConfig,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    lexical_term_policy: SearchLexicalTermPolicy,
    lexical_source_policy: SearchLexicalSourcePolicy,
    manifest: SearchOutOfCoreManifestBody,
    previous: Option<reuse::View<'_>>,
) -> Result<LoadedArtifactClosure> {
    load_artifact_closure_with_context(
        ClosureLoadContext {
            cleanup_source: None,
            validation_source: None,
            capture: None,
            root,
            config,
            analyzer_lexicon,
            lexical_term_policy,
            lexical_source_policy,
            previous,
            admission: None,
            max_artifact_read_bytes: u64::MAX,
        },
        manifest,
    )
}

fn load_artifact_closure_with_context(
    context: ClosureLoadContext<'_, '_>,
    manifest: SearchOutOfCoreManifestBody,
) -> Result<LoadedArtifactClosure> {
    let ClosureLoadContext {
        cleanup_source,
        validation_source,
        capture,
        root,
        config,
        analyzer_lexicon,
        lexical_term_policy,
        lexical_source_policy,
        previous,
        admission,
        max_artifact_read_bytes,
    } = context;
    if let (Some(source), Some(capture)) = (validation_source, capture) {
        source.validate_capture(capture)?;
    }
    if cleanup_source.is_some()
        && (capture.is_some()
            || previous.is_some()
            || admission.is_some_and(|admission| !admission.is_synchronous()))
    {
        return Err(HawDBError::Execution(
            "captured cleanup cannot use path-based reader reuse".into(),
        ));
    }
    if admission.is_some_and(|admission| admission.is_synchronous()) && cleanup_source.is_none() {
        return Err(HawDBError::Execution(
            "synchronous closure requires captured publication custody".into(),
        ));
    }
    let checkpoint = || {
        admission.map_or(Ok(()), |admission| {
            crate::build_control::checkpoint(admission.task)
        })
    };
    checkpoint()?;
    if admission.is_some() && previous.is_some() {
        return Err(HawDBError::Execution(
            "governed reuse requires source-root custody".into(),
        ));
    }
    let collections_memory = admission
        .map(|admission| {
            use crate::build_memory::{checked_add, checked_mul};
            admission.retained(checked_add(
                checked_mul(
                    manifest.segments.len(),
                    std::mem::size_of::<SearchOutOfCoreSegmentReader>(),
                )?,
                checked_mul(
                    manifest.mutation_runs.len(),
                    std::mem::size_of::<mutation_run::SearchMutationRun>(),
                )?,
            )?)
        })
        .transpose()?;
    let previous = previous.filter(|reader| {
        reader.manifest.embedding_dimension == manifest.embedding_dimension
            && reader.manifest.embedding_model == manifest.embedding_model
            && reader.manifest.embedding_version == manifest.embedding_version
    });
    let mut refresh = SearchOutOfCoreRefreshReport {
        generation: manifest.generation,
        generation_changed: true,
        validated_retractions: 0,
        reused_retractions: 0,
        opened_content_segments: 0,
        reused_content_segments: 0,
    };
    let opened_segments = manifest.segments.iter().map(|segment| {
        checkpoint()?;
        if let Some(previous) = previous
            && let Some(position) = previous
                .manifest
                .segments
                .iter()
                .position(|reference| reference == segment)
        {
            previous.segments[position].validate_pinned_paths(root, segment)?;
            refresh.reused_content_segments += 1;
            // Share immutable descriptor/layout metadata as well as file
            // handles, avoiding a corpus-sized ID-summary copy per refresh.
            return Ok(previous.segments[position].clone());
        }
        refresh.opened_content_segments += 1;
        match admission {
            Some(_) => SearchOutOfCoreSegmentReader::open_with_read_admission(
                SegmentOpenContext {
                    cleanup_source,
                    capture,
                    root,
                    config,
                    analyzer_lexicon,
                    lexical_source_policy,
                    lexical_term_policy,
                    active_manifest: &manifest,
                    admission,
                    max_artifact_read_bytes,
                },
                segment,
            ),
            None => SearchOutOfCoreSegmentReader::open_with_read_admission(
                SegmentOpenContext {
                    cleanup_source,
                    capture: None,
                    root,
                    config,
                    analyzer_lexicon,
                    lexical_source_policy,
                    lexical_term_policy,
                    active_manifest: &manifest,
                    admission: None,
                    max_artifact_read_bytes,
                },
                segment,
            ),
        }
    });
    let segments = if admission.is_some() {
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(manifest.segments.len())
            .map_err(|error| {
                HawDBError::Execution(format!("reader segment list allocation failed: {error}"))
            })?;
        if segments.capacity() > manifest.segments.len() {
            return Err(HawDBError::Execution(
                "reader segment list exceeded admitted capacity".into(),
            ));
        }
        for segment in opened_segments {
            segments.push(segment?);
        }
        segments
    } else {
        opened_segments.collect::<Result<Vec<_>>>()?
    };
    let mut mutation_budget = mutation_run::MutationRunBudget::new(
        config.max_mutation_working_bytes.get(),
        manifest.mutation_runs.len(),
        manifest.segments.len(),
    )
    .map_err(|error| match (admission.is_some(), error) {
        (true, HawDBError::Storage(message)) => HawDBError::Execution(message),
        (_, error) => error,
    })?;
    let allocation_error = |message| {
        if admission.is_some() {
            HawDBError::Execution(message)
        } else {
            HawDBError::Storage(message)
        }
    };
    let mut mutation_runs = Vec::new();
    mutation_runs
        .try_reserve_exact(manifest.mutation_runs.len())
        .map_err(|error| {
            allocation_error(format!(
                "search mutation-run list allocation failed: {error}"
            ))
        })?;
    if mutation_runs.capacity() > manifest.mutation_runs.len() {
        return Err(allocation_error(
            "search mutation-run list exceeded admitted capacity".into(),
        ));
    }
    let analyzer_digest = if manifest.mutation_runs.is_empty() {
        0
    } else {
        match admission {
            Some(admission) => crate::lexical_projection::analyzer_digest_with_context(
                analyzer_lexicon,
                admission.task,
            )?,
            None => lexical_analyzer_digest(analyzer_lexicon),
        }
    };
    for run in &manifest.mutation_runs {
        checkpoint()?;
        let opened = match (admission, capture) {
            (Some(_), Some(capture)) => mutation_run::SearchMutationRun::open_for_capture(
                root,
                run,
                config.max_mutation_run_bytes.get(),
                analyzer_digest,
                &mut mutation_budget,
                capture,
                max_artifact_read_bytes,
            ),
            (Some(admission), None) if cleanup_source.is_some() => {
                mutation_run::SearchMutationRun::open_for_cleanup(
                    root,
                    run,
                    &mut mutation_budget,
                    cleanup_source.unwrap(),
                    admission,
                    (config.max_mutation_run_bytes.get(), max_artifact_read_bytes),
                    analyzer_digest,
                )
            }
            (Some(_), None) => mutation_run::SearchMutationRun::open_with_read_admission(
                root,
                run,
                config.max_mutation_run_bytes.get(),
                analyzer_digest,
                &mut mutation_budget,
                admission,
                max_artifact_read_bytes,
            ),
            (None, _) if cleanup_source.is_some() => {
                if run.len > config.max_mutation_run_bytes.get() {
                    return Err(HawDBError::Execution(
                        "captured cleanup mutation exceeds read ceiling".into(),
                    ));
                }
                mutation_run::SearchMutationRun::open_ordinary_file(
                    cleanup_source.unwrap().open(&run.file, run.object)?,
                    run,
                    config.max_mutation_run_bytes.get(),
                    analyzer_digest,
                    &mut mutation_budget,
                )
            }
            (None, _) => mutation_run::SearchMutationRun::open(
                root,
                run,
                config.max_mutation_run_bytes.get(),
                analyzer_digest,
                &mut mutation_budget,
            ),
        };
        mutation_runs.push(opened?);
    }
    let _validation_memory = admission
        .map(|admission| {
            use crate::build_memory::{checked_add, checked_mul, SET_ENTRY_BYTES};
            let entries = manifest
                .mutation_runs
                .iter()
                .try_fold(0usize, |count, run| {
                    checkpoint()?;
                    checked_add(count, run.entry_count)
                })?;
            admission.scratch(checked_mul(
                checked_add(manifest.segments.len(), entries)?,
                SET_ENTRY_BYTES,
            )?)
        })
        .transpose()?;
    match admission {
        Some(admission) => mutation_run::validate_closure_with_context(
            &manifest,
            &mutation_runs,
            Some(admission.task),
        )?,
        None => mutation_run::validate_closure(&manifest, &mutation_runs)?,
    }
    let targets_to_validate = mutation_runs
        .iter()
        .zip(&manifest.mutation_runs)
        .filter_map(|(run, reference)| {
            let reusable = previous
                .and_then(|reader| {
                    let index = reader
                        .manifest
                        .mutation_runs
                        .iter()
                        .position(|candidate| candidate == reference)?;
                    if run.entries().iter().all(|entry| {
                        let target = manifest
                            .segments
                            .iter()
                            .find(|segment| segment.segment_id == entry.target_segment_id);
                        target.is_some_and(|target| reader.manifest.segments.contains(target))
                    }) {
                        reader.visibility.vector_retraction_count(index)
                    } else {
                        None
                    }
                })
                .is_some_and(|count| run.record_vector_retraction_count(count).is_ok());
            if reusable {
                refresh.reused_retractions += run.entries().len();
                None
            } else {
                refresh.validated_retractions += run.entries().len();
                Some(run)
            }
        });
    mutation_run::validate_targets_with_admission(
        root,
        &segments,
        targets_to_validate,
        config,
        analyzer_lexicon,
        lexical_source_policy,
        mutation_run::TargetValidationContext {
            admission: admission.map(|admission| (admission, max_artifact_read_bytes)),
            source: validation_source.or(cleanup_source),
            capture,
        },
    )?;
    checkpoint()?;
    Ok(LoadedArtifactClosure {
        manifest,
        segments,
        visibility: mutation_run::MutationVisibility::from_validated_runs(mutation_runs),
        refresh,
        collections_memory,
    })
}

pub(super) struct PublishedArtifactGenerations {
    pub(super) active_generation: u64,
    pub(super) lexical_generations: BTreeSet<u64>,
    pub(super) out_of_core_generations: BTreeSet<u64>,
    pub(super) rabitq_generations: BTreeSet<u64>,
}

pub(super) fn validate_resident_snapshot_identity(index: &SearchIndex, root: &Path) -> Result<()> {
    let path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !fs::try_exists(&path)? {
        return Ok(());
    }
    let bytes = read_bounded_file(&path, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
    validate_resident_snapshot_manifest(index, &bytes)
}

pub(super) fn validate_captured_resident_snapshot_identity(
    index: &SearchIndex,
    publication: &SearchProjectionPublishLease,
) -> Result<()> {
    let file = match publication.open_captured_child(OUT_OF_CORE_MANIFEST_FILE) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let bytes = crate::bounded_file::read_bounded_open_file(
        file,
        OUT_OF_CORE_MANIFEST_FILE,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
    )?;
    validate_resident_snapshot_manifest(index, &bytes)
}

pub(super) fn validate_registered_artifact_closure(
    index: &SearchIndex,
    publication: &SearchProjectionPublishLease,
) -> Result<()> {
    let task = hawdb_core::RuntimeTaskContext::default();
    let source = captured_cleanup::Source::new(
        publication,
        &task,
        captured_cleanup::Limits {
            artifact_bytes: u64::MAX,
            total_bytes: u64::MAX,
            files: usize::MAX,
        },
    );
    let bytes = source.head()?.ok_or_else(|| {
        HawDBError::StorageIntegrity("missing registered out-of-core manifest".into())
    })?;
    let manifest = SearchOutOfCoreManifestBody::decode(&bytes)?;
    let config = SearchOutOfCoreConfig::default();
    let closure = load_artifact_closure_with_context(
        ClosureLoadContext {
            cleanup_source: Some(&source),
            validation_source: Some(&source),
            capture: None,
            root: index
                .path
                .as_deref()
                .expect("persistent registered projection"),
            config: &config,
            analyzer_lexicon: &index.analyzer_lexicon,
            lexical_term_policy: Default::default(),
            lexical_source_policy: Default::default(),
            previous: None,
            admission: None,
            max_artifact_read_bytes: u64::MAX,
        },
        manifest,
    )?;
    if closure.manifest.source_graph_commit_epoch != index.source_graph_commit_epoch
        || closure.manifest.document_count != index.documents.len()
    {
        return Err(HawDBError::StorageIntegrity(
            "registered generation does not match its snapshot".into(),
        ));
    }
    Ok(())
}

fn validate_resident_snapshot_manifest(index: &SearchIndex, bytes: &[u8]) -> Result<()> {
    let manifest = SearchOutOfCoreManifestBody::decode(bytes)?;
    let embedding = index.embedding_manifest.as_ref();
    let summary = index.vector_document_summary;
    let digest = summary.map_or_else(
        || lexical_documents_digest(&index.documents),
        |summary| summary.documents_digest,
    );
    let snapshot_mismatch = summary.is_some_and(|summary| {
        !manifest.resident_snapshot.is_some_and(|snapshot| {
            snapshot.metadata_only
                && snapshot.vector_document_count == summary.vector_document_count
        })
    }) || manifest
        .resident_snapshot
        .is_some_and(|snapshot| snapshot.metadata_only != summary.is_some());
    if snapshot_mismatch
        || manifest.document_count != index.document_count()
        || manifest.documents_digest != digest
        || manifest.source_graph_commit_epoch != index.source_graph_commit_epoch
        || manifest.import_source_graph_commit_epoch != index.import_source_graph_commit_epoch
        || manifest.embedding_dimension != index.embedding_dimension
        || manifest.vector_index_binding
            != index
                .vector_index_descriptor
                .as_ref()
                .map(crate::vector_index::encode)
        || manifest.embedding_model.as_deref() != embedding.map(|identity| identity.model.as_str())
        || manifest.embedding_version.as_deref()
            != embedding.and_then(|identity| identity.version.as_deref())
    {
        return Err(HawDBError::Storage(
            "resident search snapshot does not match the published immutable projection; use incremental maintenance or rebuild from the authoritative source".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn published_artifact_generations(
    root: &Path,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Result<Option<PublishedArtifactGenerations>> {
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !fs::try_exists(&manifest_path)? {
        return Ok(None);
    }
    let bytes = read_bounded_file(&manifest_path, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
    let manifest = SearchOutOfCoreManifestBody::decode(&bytes)?;
    let closure = load_artifact_closure_for_manifest(
        root,
        &SearchOutOfCoreConfig::default(),
        analyzer_lexicon,
        Default::default(),
        Default::default(),
        manifest,
        None,
    )?;
    Ok(Some(artifact_generations(&closure)))
}

pub(super) struct CapturedGenerations {
    pub(super) generations: Option<PublishedArtifactGenerations>,
    pub(super) memory: Option<crate::build_memory::read::ReaderMemory>,
    pub(super) resident_vector_absence_verified: bool,
    pub(super) resident_vector_receipt_verified: bool,
}

pub(super) fn captured_artifact_generations(
    root_label: &Path,
    publication: &SearchProjectionPublishLease,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    task: &hawdb_core::RuntimeTaskContext,
    options: crate::SearchProjectionCleanupOptions,
    admission: Option<&crate::build_memory::read::ReadAdmission<'_>>,
) -> Result<CapturedGenerations> {
    let task = admission.map_or(task, |admission| admission.task);
    let limits = captured_cleanup::Limits {
        artifact_bytes: options.max_verification_bytes.get(),
        total_bytes: options.max_verification_bytes.get(),
        files: options.max_verification_files.get(),
    };
    let source = captured_cleanup::Source::new(publication, task, limits);
    let config = SearchOutOfCoreConfig::default();
    match admission {
        Some(admission) => {
            let capture = if admission.is_synchronous() {
                None
            } else {
                Some(publication.cleanup_capture(admission)?)
            };
            let Some(file) = source.head_file(Some(admission))? else {
                source.prove_no_projection_files(root_label, Some(admission))?;
                return Ok(CapturedGenerations {
                    generations: None,
                    memory: None,
                    resident_vector_absence_verified: true,
                    resident_vector_receipt_verified: false,
                });
            };
            let bytes = admission.read_unbound_file(
                &file,
                MAX_OUT_OF_CORE_MANIFEST_BYTES.min(limits.total_bytes),
            )?;
            let _decode =
                admission.retained(SearchOutOfCoreManifestBody::decode_capacity(&bytes, task)?)?;
            let manifest = SearchOutOfCoreManifestBody::decode_with_context(&bytes, Some(task))?;
            // Fresh relative names must match the selected references even if
            // the shared FD cache already holds an older unlinked native file.
            if !admission.is_synchronous() {
                source.verify_manifest(&manifest, Some(admission))?;
            }
            let resident_vector_absence_verified = source.verify_resident_vector_absence(
                manifest.resident_snapshot,
                manifest.resident_snapshot_absent,
                Some(admission),
            )?;
            let resident_vector_receipt_verified = manifest.resident_snapshot.is_some();
            let records = manifest
                .segments
                .len()
                .checked_mul(4)
                .and_then(|n| n.checked_add(manifest.mutation_runs.len()))
                .ok_or_else(|| HawDBError::Execution("cleanup retention size overflow".into()))?;
            let memory = admission.retained(crate::build_memory::checked_mul(
                records,
                crate::build_memory::SET_ENTRY_BYTES,
            )?)?;
            let closure = load_artifact_closure_with_context(
                ClosureLoadContext {
                    cleanup_source: admission.is_synchronous().then_some(&source),
                    validation_source: Some(&source),
                    capture: capture.as_ref(),
                    root: root_label,
                    config: &config,
                    analyzer_lexicon,
                    lexical_term_policy: Default::default(),
                    lexical_source_policy: Default::default(),
                    previous: None,
                    admission: Some(admission),
                    max_artifact_read_bytes: limits.artifact_bytes,
                },
                manifest,
            )?;
            Ok(CapturedGenerations {
                generations: Some(artifact_generations(&closure)),
                memory: Some(memory),
                resident_vector_absence_verified,
                resident_vector_receipt_verified,
            })
        }
        None => {
            if task.memory_reservation().is_some() {
                return Err(HawDBError::Execution(
                    "bound cleanup requires the originating admission".into(),
                ));
            }
            let _io = task
                .acquire_io_wave(std::num::NonZeroUsize::MIN)
                .map_err(|error| HawDBError::Execution(error.to_string()))?;
            let Some(bytes) = source.head()? else {
                source.prove_no_projection_files(root_label, None)?;
                return Ok(CapturedGenerations {
                    generations: None,
                    memory: None,
                    resident_vector_absence_verified: true,
                    resident_vector_receipt_verified: false,
                });
            };
            let manifest = SearchOutOfCoreManifestBody::decode_with_context(&bytes, Some(task))?;
            let resident_vector_absence_verified = source.verify_resident_vector_absence(
                manifest.resident_snapshot,
                manifest.resident_snapshot_absent,
                None,
            )?;
            let resident_vector_receipt_verified = manifest.resident_snapshot.is_some();
            let closure = load_artifact_closure_with_context(
                ClosureLoadContext {
                    cleanup_source: Some(&source),
                    validation_source: Some(&source),
                    capture: None,
                    root: root_label,
                    config: &config,
                    analyzer_lexicon,
                    lexical_term_policy: Default::default(),
                    lexical_source_policy: Default::default(),
                    previous: None,
                    admission: None,
                    max_artifact_read_bytes: limits.artifact_bytes,
                },
                manifest,
            )?;
            Ok(CapturedGenerations {
                generations: Some(artifact_generations(&closure)),
                memory: None,
                resident_vector_absence_verified,
                resident_vector_receipt_verified,
            })
        }
    }
}

fn artifact_generations(closure: &LoadedArtifactClosure) -> PublishedArtifactGenerations {
    let manifest = &closure.manifest;
    let mut lexical_generations = BTreeSet::new();
    let mut out_of_core_generations = BTreeSet::new();
    let mut rabitq_generations = BTreeSet::new();
    for artifact in &manifest.segments {
        lexical_generations.insert(artifact.generation);
        lexical_generations.insert(artifact.objects.lexical_generation);
        out_of_core_generations.insert(artifact.generation);
        if artifact.rabitq_artifact_file.is_some() {
            rabitq_generations.insert(artifact.generation);
        }
    }
    out_of_core_generations.extend(manifest.mutation_runs.iter().map(|run| run.generation));
    PublishedArtifactGenerations {
        active_generation: manifest.generation,
        lexical_generations,
        out_of_core_generations,
        rabitq_generations,
    }
}

struct ResidentProducerReferences {
    descriptor: ArtifactObject,
    payload: ArtifactObject,
    lexical_generation: u64,
    lexical: crate::lexical_projection::ProducerReferences,
}

#[cfg(feature = "vector-search")]
pub(super) struct ResidentRaBitQArtifact {
    pub(super) reference: hawdb_storage::immutable_object::ObjectReference,
    pub(super) checksum: u64,
    pub(super) source_digest: u64,
    pub(super) vector_document_count: usize,
    pub(super) payload_checksum: u32,
    pub(super) peak_build_working_bytes: usize,
    pub(super) diskann_source_digest: Option<[u8; 32]>,
}

pub(super) struct ResidentPublicationPlan {
    pub(super) generation: u64,
    #[cfg(feature = "vector-search")]
    pub(super) rabitq: Option<ResidentRaBitQArtifact>,
}

pub(super) fn write_resident_artifacts(
    index: &SearchIndex,
    root: &Path,
    snapshot: &crate::snapshot_writer::SearchSnapshotWriteReport,
    plan: ResidentPublicationPlan,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<PublishedOutOfCoreProjection> {
    let generation = plan.generation;
    let memory = crate::build_memory::BuildMemory::new(task)?;
    let io = generation_writer::io::GenerationIo::new(&memory, task);
    let mut descriptor = SearchSegmentDescriptor::build(&index.documents);
    let payload =
        super::write_search_segment_payloads(root, &index.documents, &mut descriptor, |file| {
            ArtifactObject::from_reference(io.seal_file(file)?.reference)
        })?;
    let (descriptor_bytes, descriptor_object) =
        super::write_search_segment_descriptor_output(root, &descriptor, u64::MAX, |file| {
            ArtifactObject::from_reference(io.seal_file(file)?.reference)
        })?;
    let lexical_generation = generation;
    let projection = super::LexicalProjectionWriter::new(index.lexical_config)
        .with_context(memory.clone(), task.clone())
        .write(
            root,
            lexical_generation,
            index.source_graph_commit_epoch,
            lexical_analyzer_digest(&index.analyzer_lexicon),
            lexical_documents_digest(&index.documents),
            index.documents.values(),
            &index.analyzer_lexicon,
        )?;
    let lexical = projection.producer_references().ok_or_else(|| {
        HawDBError::StorageIntegrity(
            "resident checkpoint requires lexical producer references".into(),
        )
    })?;
    let producer_bytes = descriptor_bytes
        .saturating_add(payload.byte_length)
        .saturating_add(lexical.manifest.byte_length)
        .saturating_add(lexical.artifact.byte_length);
    index.replace_lexical_projection(Some(projection));
    let mut published = publish_out_of_core_projection(
        index,
        root,
        snapshot,
        &descriptor,
        plan,
        ResidentProducerReferences {
            descriptor: descriptor_object,
            payload,
            lexical_generation,
            lexical,
        },
        &io,
    )?;
    published.bytes_written = published.bytes_written.saturating_add(producer_bytes);
    Ok(published)
}

fn publish_out_of_core_projection(
    index: &SearchIndex,
    root: &Path,
    snapshot: &crate::snapshot_writer::SearchSnapshotWriteReport,
    descriptor: &SearchSegmentDescriptor,
    plan: ResidentPublicationPlan,
    producer: ResidentProducerReferences,
    io: &generation_writer::io::GenerationIo<'_>,
) -> Result<PublishedOutOfCoreProjection> {
    let generation = plan.generation;
    #[cfg(feature = "vector-search")]
    let rabitq = plan.rabitq;
    if !descriptor.matches_documents(&index.documents) {
        return Err(HawDBError::Storage(
            "search segment descriptor does not match checkpoint documents".to_string(),
        ));
    }
    let descriptor_file = format!("search_projection_segments.{generation}.hawdb");
    let payload_file = format!("search_projection_segment_payloads.{generation}.hawdb");
    let metadata_payload_file = format!("search_projection_metadata_payloads.{generation}.hawdb");
    let vector_payload_file = format!("search_projection_vector_payloads.{generation}.hawdb");
    let layout_file = format!("search_projection_out_of_core_layout.{generation}.hawdb");
    let lexical_manifest_file = format!("search_lexical.manifest.{generation}.hawdb");
    let mut bytes_written = publish_generation_link(
        &root.join(SEARCH_SEGMENT_DESCRIPTOR_FILE),
        &root.join(&descriptor_file),
    )?;
    bytes_written = bytes_written.saturating_add(publish_generation_link(
        &root.join(SEARCH_SEGMENT_PAYLOAD_FILE),
        &root.join(&payload_file),
    )?);
    bytes_written = bytes_written.saturating_add(publish_generation_link(
        &root.join(MANIFEST_FILE),
        &root.join(&lexical_manifest_file),
    )?);

    let (layout, metadata_object, vector_object) = write_out_of_core_sidecars(
        index,
        descriptor,
        generation,
        &root.join(&metadata_payload_file),
        &root.join(&vector_payload_file),
        io,
    )?;
    let metadata_payload_len = metadata_object.byte_length;
    let vector_payload_len = vector_object.byte_length;
    let layout_bytes = layout.encode()?;
    let layout_object = ArtifactObject::for_bytes(&layout_bytes);
    bytes_written = bytes_written.saturating_add(write_generation_artifact(
        &root.join(&layout_file),
        &layout_bytes,
    )?);
    bytes_written = bytes_written
        .saturating_add(metadata_payload_len)
        .saturating_add(vector_payload_len);

    let descriptor_bytes = fs::read(root.join(&descriptor_file))?;
    let lexical_manifest_bytes = fs::read(root.join(&lexical_manifest_file))?;
    let lexical_payload_file =
        crate::lexical_projection::artifact_file(producer.lexical_generation);
    let lexical_manifest_object = ArtifactObject::from_reference(producer.lexical.manifest)?;
    let lexical_payload_object = ArtifactObject::from_reference(producer.lexical.artifact)?;
    #[cfg(test)]
    AFTER_RESIDENT_SIDECARS.with(|hook| {
        if let Some(index) = hook.take() {
            let name = [
                &metadata_payload_file,
                &vector_payload_file,
                &layout_file,
                &descriptor_file,
                &payload_file,
                &lexical_manifest_file,
                &lexical_payload_file,
            ][index];
            let path = root.join(name);
            let mut bytes = fs::read(&path).unwrap();
            bytes[0] ^= 1;
            fs::write(&path, bytes).unwrap();
        }
    });
    for (name, object) in [
        (&metadata_payload_file, metadata_object),
        (&vector_payload_file, vector_object),
        (&layout_file, layout_object),
        (&descriptor_file, producer.descriptor),
        (&payload_file, producer.payload),
        (&lexical_manifest_file, lexical_manifest_object),
        (&lexical_payload_file, lexical_payload_object),
    ] {
        io.verify_object(&root.join(name), object)?;
    }
    #[cfg(feature = "vector-search")]
    if let Some(artifact) = rabitq.as_ref() {
        io.verify_object(
            &root.join(format!("search_rabitq.{generation}.hawdb")),
            ArtifactObject::from_reference(artifact.reference)?,
        )?;
    }
    let objects = SegmentObjects {
        lexical_generation: producer.lexical_generation,
        descriptor: producer.descriptor,
        payload: producer.payload,
        metadata: metadata_object,
        vector: vector_object,
        layout: layout_object,
        lexical_manifest: lexical_manifest_object,
        lexical_payload: lexical_payload_object,
        #[cfg(feature = "vector-search")]
        rabitq: rabitq
            .as_ref()
            .map(|artifact| ArtifactObject::from_reference(artifact.reference))
            .transpose()?,
        #[cfg(not(feature = "vector-search"))]
        rabitq: None,
    };
    let embedding = index.embedding_manifest.as_ref();
    let manifest = SearchOutOfCoreManifestBody {
        format: OUT_OF_CORE_FORMAT.to_string(),
        generation,
        resident_snapshot: Some(ResidentSnapshot {
            object: ArtifactObject::from_reference(snapshot.encoded_object)?,
            vector_document_count: snapshot.vector_document_count,
            metadata_only: false,
        }),
        resident_snapshot_absent: false,
        segments: vec![SearchOutOfCoreSegmentManifest {
            #[cfg(feature = "vector-search")]
            diskann_source_digest: rabitq
                .as_ref()
                .and_then(|artifact| artifact.diskann_source_digest),
            #[cfg(not(feature = "vector-search"))]
            diskann_source_digest: None,
            objects,
            segment_id: 0,
            level: 0,
            generation,
            descriptor_file,
            descriptor_len: descriptor_bytes.len() as u64,
            descriptor_checksum: checksum_bytes(&descriptor_bytes),
            payload_file,
            payload_len: fs::metadata(root.join(format!(
                "search_projection_segment_payloads.{generation}.hawdb"
            )))?
            .len(),
            metadata_payload_file,
            metadata_payload_len,
            vector_payload_file,
            vector_payload_len,
            layout_file,
            layout_len: layout_bytes.len() as u64,
            layout_checksum: checksum_bytes(&layout_bytes),
            lexical_manifest_file,
            lexical_manifest_len: lexical_manifest_bytes.len() as u64,
            lexical_manifest_checksum: checksum_bytes(&lexical_manifest_bytes),
            #[cfg(feature = "vector-search")]
            rabitq_artifact_file: rabitq
                .as_ref()
                .map(|_| format!("search_rabitq.{generation}.hawdb")),
            #[cfg(not(feature = "vector-search"))]
            rabitq_artifact_file: None,
            #[cfg(feature = "vector-search")]
            rabitq_artifact_len: rabitq
                .as_ref()
                .map(|artifact| artifact.reference.byte_length),
            #[cfg(not(feature = "vector-search"))]
            rabitq_artifact_len: None,
            #[cfg(feature = "vector-search")]
            rabitq_artifact_checksum: rabitq.as_ref().map(|artifact| artifact.checksum),
            #[cfg(not(feature = "vector-search"))]
            rabitq_artifact_checksum: None,
            #[cfg(feature = "vector-search")]
            rabitq_source_digest: rabitq.as_ref().map(|artifact| artifact.source_digest),
            #[cfg(not(feature = "vector-search"))]
            rabitq_source_digest: None,
            #[cfg(feature = "vector-search")]
            rabitq_vector_document_count: rabitq
                .as_ref()
                .map(|artifact| artifact.vector_document_count),
            #[cfg(not(feature = "vector-search"))]
            rabitq_vector_document_count: None,
            #[cfg(feature = "vector-search")]
            rabitq_payload_checksum: rabitq.as_ref().map(|artifact| artifact.payload_checksum),
            #[cfg(not(feature = "vector-search"))]
            rabitq_payload_checksum: None,
            #[cfg(feature = "vector-search")]
            rabitq_peak_build_working_bytes: rabitq
                .as_ref()
                .map(|artifact| artifact.peak_build_working_bytes),
            #[cfg(not(feature = "vector-search"))]
            rabitq_peak_build_working_bytes: None,
            document_count: index.documents.len(),
            documents_digest: lexical_documents_digest(&index.documents),
            source_graph_commit_epoch: index.source_graph_commit_epoch,
        }],
        mutation_runs: Vec::new(),
        document_count: index.documents.len(),
        documents_digest: lexical_documents_digest(&index.documents),
        source_graph_commit_epoch: index.source_graph_commit_epoch,
        import_source_graph_commit_epoch: index.import_source_graph_commit_epoch,
        embedding_model: embedding.map(|manifest| manifest.model.clone()),
        embedding_version: embedding.and_then(|manifest| manifest.version.clone()),
        embedding_dimension: embedding
            .map(|manifest| manifest.dimension)
            .or(index.embedding_dimension),
        vector_index_binding: index
            .vector_index_descriptor
            .as_ref()
            .map(crate::vector_index::encode),
    };
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    let tmp_path = manifest_path.with_extension(format!("tmp.{generation}"));
    let manifest_bytes = manifest.encode()?;
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&manifest_bytes)?;
        file.sync_all()?;
    }
    durable_replace_file(&tmp_path, &manifest_path)?;
    Ok(PublishedOutOfCoreProjection {
        generation,
        bytes_written: bytes_written.saturating_add(manifest_bytes.len() as u64),
    })
}

#[cfg(test)]
thread_local! {
    static AFTER_RESIDENT_SIDECARS: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

fn write_out_of_core_sidecars(
    index: &SearchIndex,
    descriptor: &SearchSegmentDescriptor,
    generation: u64,
    metadata_path: &Path,
    vector_path: &Path,
    io: &generation_writer::io::GenerationIo<'_>,
) -> Result<(SearchOutOfCoreLayoutBody, ArtifactObject, ArtifactObject)> {
    let metadata_tmp = temporary_artifact_path(metadata_path);
    let vector_tmp = temporary_artifact_path(vector_path);
    let mut metadata_guard = CandidateFileGuard::new(metadata_tmp.clone());
    let mut vector_guard = CandidateFileGuard::new(vector_tmp.clone());
    let mut metadata_offset = 0u64;
    let mut vector_offset = 0u64;
    let mut vector_ordinal = 0u64;
    let mut layouts = Vec::with_capacity(descriptor.segments.len());
    let metadata_object;
    let vector_object;
    {
        let mut metadata_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&metadata_tmp)?;
        let mut vector_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&vector_tmp)?;

        for segment in &descriptor.segments {
            let documents = index
                .documents
                .range_documents(&segment.first_document_id, &segment.last_document_id)
                .map(|(_, document)| document)
                .collect::<Vec<_>>();
            if documents.len() != segment.document_count {
                return Err(HawDBError::Storage(format!(
                    "search sidecar segment {} has {} documents, expected {}",
                    segment.segment_id,
                    documents.len(),
                    segment.document_count
                )));
            }

            let mut metadata_body = String::from("HAWDB_SEARCH_METADATA_SEGMENT_V1\n");
            let mut vector_body = String::from("HAWDB_SEARCH_VECTOR_SEGMENT_V1\n");
            let mut vector_count = 0usize;
            for document in documents {
                let document_vector_ordinal = document.embedding.as_ref().map(|_| vector_ordinal);
                metadata_body.push_str(&format!(
                    "meta\t{}\t{}\t{}\n",
                    encode_string(&document.id),
                    document_vector_ordinal
                        .map(|ordinal| ordinal.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    encode_metadata(&document.metadata)
                ));
                if let Some(embedding) = document.embedding.as_deref() {
                    if embedding.is_empty() {
                        return Err(HawDBError::Storage(format!(
                            "search document {} has an empty embedding",
                            document.id
                        )));
                    }
                    vector_body.push_str(&format!(
                        "vector\t{}\t{}\t{}\n",
                        vector_ordinal,
                        encode_string(&document.id),
                        encode_embedding(Some(embedding))
                    ));
                    vector_count = vector_count.saturating_add(1);
                    vector_ordinal = vector_ordinal.checked_add(1).ok_or_else(|| {
                        HawDBError::Storage("search vector ordinal overflow".to_string())
                    })?;
                }
            }

            let metadata_payload = encode_search_snapshot_text(&metadata_body)?;
            let vector_payload = encode_search_snapshot_text(&vector_body)?;
            let metadata = append_sidecar_payload(
                &mut metadata_file,
                &mut metadata_offset,
                &metadata_payload,
                segment.document_count,
            )?;
            let vectors = append_sidecar_payload(
                &mut vector_file,
                &mut vector_offset,
                &vector_payload,
                vector_count,
            )?;
            layouts.push(SearchOutOfCoreSegmentLayout {
                segment_id: segment.segment_id,
                vector_ordinal_base: vector_ordinal.saturating_sub(vector_count as u64),
                metadata,
                vectors,
            });
        }

        metadata_file.sync_all()?;
        vector_file.sync_all()?;
        metadata_object =
            ArtifactObject::from_reference(io.seal_file(&mut metadata_file)?.reference)?;
        vector_object = ArtifactObject::from_reference(io.seal_file(&mut vector_file)?.reference)?;
    }
    durable_replace_file(&metadata_tmp, metadata_path)?;
    metadata_guard.disarm();
    durable_replace_file(&vector_tmp, vector_path)?;
    vector_guard.disarm();
    Ok((
        SearchOutOfCoreLayoutBody {
            format: OUT_OF_CORE_LAYOUT_FORMAT.to_string(),
            generation,
            document_count: descriptor.document_count,
            segments: layouts,
        },
        metadata_object,
        vector_object,
    ))
}

fn append_sidecar_payload(
    file: &mut impl Write,
    offset: &mut u64,
    payload: &[u8],
    entry_count: usize,
) -> Result<SearchOutOfCoreRange> {
    append_sidecar_payload_with_context(file, offset, payload, entry_count, None)
}

fn append_sidecar_payload_with_context(
    file: &mut impl Write,
    offset: &mut u64,
    payload: &[u8],
    entry_count: usize,
    task: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<SearchOutOfCoreRange> {
    let length = u64::try_from(payload.len()).map_err(|_| {
        HawDBError::Storage("search sidecar payload length exceeds u64".to_string())
    })?;
    let checksum = crate::build_control::write_checksummed(file, payload, task)?;
    let range = SearchOutOfCoreRange {
        offset: *offset,
        length,
        checksum,
        entry_count,
    };
    *offset = offset
        .checked_add(length)
        .ok_or_else(|| HawDBError::Storage("search sidecar payload offset overflow".to_string()))?;
    Ok(range)
}

struct HitHydrationContext<'a> {
    query_terms: &'a BTreeSet<String>,
    query_embedding: Option<&'a [f32]>,
    mode: SearchMode,
    fallback_reason_codes: &'a [SearchFallbackReasonCode],
    fallback_reasons: &'a [String],
    projection_freshness: &'a SearchProjectionFreshness,
    query: Option<crate::lexical_projection::QueryContext<'a>>,
    output_memory: query_output::Memory<'a>,
}

struct CandidateFileGuard {
    path: PathBuf,
    armed: bool,
}

impl CandidateFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CandidateFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn search_document_bytes(document: &SearchDocument) -> u64 {
    let scalar = document
        .id
        .len()
        .saturating_add(document.title.len())
        .saturating_add(document.content.len())
        .saturating_add(
            document
                .metadata
                .iter()
                .fold(0usize, |bytes, (key, value)| {
                    bytes.saturating_add(key.len()).saturating_add(value.len())
                }),
        );
    let vector = document
        .embedding
        .as_ref()
        .map(|embedding| embedding.len().saturating_mul(std::mem::size_of::<f32>()))
        .unwrap_or(0);
    scalar.saturating_add(vector) as u64
}

fn validate_out_of_core_range(
    range: SearchOutOfCoreRange,
    expected_offset: u64,
    artifact_len: u64,
    max_compressed_segment_bytes: u64,
    segment_id: u64,
    kind: &str,
) -> Result<()> {
    if range.offset != expected_offset || range.length == 0 {
        return Err(HawDBError::Storage(format!(
            "search segment {segment_id} has an invalid {kind} sidecar range"
        )));
    }
    if range.length > max_compressed_segment_bytes {
        return Err(HawDBError::Storage(format!(
            "search segment {segment_id} {kind} sidecar requires {} compressed bytes, exceeding {max_compressed_segment_bytes}",
            range.length
        )));
    }
    let end = range.offset.checked_add(range.length).ok_or_else(|| {
        HawDBError::Storage(format!(
            "search segment {segment_id} {kind} sidecar range overflows"
        ))
    })?;
    if end > artifact_len {
        return Err(HawDBError::Storage(format!(
            "search segment {segment_id} {kind} sidecar exceeds its payload length"
        )));
    }
    Ok(())
}

fn read_out_of_core_payload_range(
    file: &File,
    range: SearchOutOfCoreRange,
    segment_id: u64,
    kind: &str,
    metrics: &mut SearchOutOfCoreMetrics,
) -> Result<Vec<u8>> {
    let length = usize::try_from(range.length).map_err(|_| {
        HawDBError::Storage(format!(
            "search segment {segment_id} {kind} payload length exceeds the platform address space"
        ))
    })?;
    let mut payload = vec![0u8; length];
    read_search_range(file, range.offset, &mut payload)?;
    metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
    metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
    let actual_checksum = checksum_bytes(&payload);
    if actual_checksum != range.checksum {
        return Err(HawDBError::Storage(format!(
            "search segment {segment_id} {kind} payload checksum mismatch: expected {}, got {actual_checksum}",
            range.checksum
        )));
    }
    Ok(payload)
}

fn decode_metadata_segment(
    text: &str,
    segment: &SearchSegmentDescriptorEntry,
    expected_count: usize,
) -> Result<Vec<SearchMetadataDocument>> {
    let mut lines = text.lines();
    if lines.next() != Some("HAWDB_SEARCH_METADATA_SEGMENT_V1") {
        return Err(HawDBError::Storage(format!(
            "search segment {} metadata sidecar has an invalid header",
            segment.segment_id
        )));
    }
    let mut documents = Vec::with_capacity(expected_count);
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["meta", raw_id, raw_vector_ordinal, raw_metadata] => {
                documents.push(SearchMetadataDocument {
                    id: decode_string(raw_id)?,
                    vector_ordinal: decode_optional_vector_ordinal(raw_vector_ordinal)?,
                    metadata: decode_metadata(raw_metadata)?,
                })
            }
            _ => {
                return Err(HawDBError::Storage(format!(
                    "search segment {} has an invalid metadata sidecar line",
                    segment.segment_id
                )));
            }
        }
    }
    if documents.len() != expected_count
        || documents.first().map(|document| document.id.as_str())
            != Some(segment.first_document_id.as_str())
        || documents.last().map(|document| document.id.as_str())
            != Some(segment.last_document_id.as_str())
        || documents.windows(2).any(|pair| pair[0].id >= pair[1].id)
    {
        return Err(HawDBError::Storage(format!(
            "search segment {} metadata sidecar count, bounds, or ordering mismatch",
            segment.segment_id
        )));
    }
    Ok(documents)
}

fn decode_vector_segment(
    text: &str,
    segment: &SearchSegmentDescriptorEntry,
    expected_count: usize,
    expected_ordinal_base: u64,
    expected_dimension: Option<usize>,
    selected_capacity: usize,
    mut selected: impl FnMut(u64) -> bool,
) -> Result<Vec<SearchVectorDocument>> {
    let mut lines = text.lines();
    if lines.next() != Some("HAWDB_SEARCH_VECTOR_SEGMENT_V1") {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar has an invalid header",
            segment.segment_id
        )));
    }
    let mut documents: Vec<SearchVectorDocument> =
        Vec::with_capacity(expected_count.min(selected_capacity));
    let mut count = 0usize;
    let mut previous_unselected_id = None;
    let mut invalid_layout = false;
    for line in lines {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["vector", raw_ordinal, raw_id, raw_embedding] => {
                let vector_ordinal = raw_ordinal.parse::<u64>().map_err(|_| {
                    HawDBError::Storage(format!(
                        "search segment {} vector sidecar has an invalid ordinal",
                        segment.segment_id
                    ))
                })?;
                let embedding = if selected(vector_ordinal) {
                    decode_embedding(raw_embedding)?
                } else {
                    None
                };
                let (dimension, finite) = match &embedding {
                    Some(values) => (values.len(), values.iter().all(|value| value.is_finite())),
                    None => validate_unselected_embedding(raw_embedding)?,
                };
                if dimension == 0 {
                    return Err(HawDBError::Storage(format!(
                        "search segment {} vector sidecar contains an empty embedding",
                        segment.segment_id
                    )));
                }
                if !finite || expected_dimension.is_none_or(|expected| dimension != expected) {
                    return Err(HawDBError::Storage(format!(
                        "search segment {} vector sidecar has an invalid embedding",
                        segment.segment_id
                    )));
                }
                let id = decode_string(raw_id)?;
                // Retained IDs remain in the output; only an unselected predecessor
                // needs separate ownership for the ordering check.
                let previous_id = previous_unselected_id
                    .as_deref()
                    .or_else(|| documents.last().map(|document| document.id.as_str()));
                invalid_layout |= vector_ordinal
                    != expected_ordinal_base.saturating_add(count as u64)
                    || previous_id.is_some_and(|previous| previous >= id.as_str())
                    || id < segment.first_document_id
                    || id > segment.last_document_id;
                count = count.saturating_add(1);
                if let Some(embedding) = embedding {
                    previous_unselected_id = None;
                    documents.push(SearchVectorDocument { id, embedding });
                } else {
                    previous_unselected_id = Some(id);
                }
            }
            _ => {
                return Err(HawDBError::Storage(format!(
                    "search segment {} has an invalid vector sidecar line",
                    segment.segment_id
                )));
            }
        }
    }
    // Keep layout errors after row parsing, including errors in unselected rows.
    if count != expected_count || invalid_layout {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar count, bounds, or ordering mismatch",
            segment.segment_id
        )));
    }
    Ok(documents)
}

fn validate_unselected_embedding(input: &str) -> Result<(usize, bool)> {
    if input.is_empty() {
        return Ok((0, true));
    }
    let mut dimension = 0usize;
    let mut finite = true;
    let mut remaining = input;
    loop {
        // Locate the delimiter while validating canonical finite decimals. Do
        // not scan every common coordinate twice through split + digit checks.
        let (raw, rest, common) = match common_finite_decimal_end(remaining.as_bytes()) {
            Some(end) => (
                &remaining[..end],
                (end < remaining.len()).then(|| &remaining[end + 1..]),
                true,
            ),
            None => match remaining.split_once(',') {
                Some((raw, rest)) => (raw, Some(rest), false),
                None => (remaining, None, false),
            },
        };
        if !common {
            let value = raw
                .parse::<f32>()
                .map_err(|_| HawDBError::Storage(format!("invalid embedding value: {raw}")))?;
            // A later parse error must still take precedence over a non-finite value.
            finite &= value.is_finite();
        }
        dimension += 1;
        match rest {
            Some(rest) => remaining = rest,
            None => return Ok((dimension, finite)),
        }
    }
}

/// Ends only at a comma or EOF after an ASCII decimal known to be finite.
/// Other forms use the original f32 parser for just that coordinate.
fn common_finite_decimal_end(value: &[u8]) -> Option<usize> {
    let mut end = usize::from(value.first() == Some(&b'-'));
    let integer = *value.get(end)?;
    if integer != b'0' && integer != b'1' {
        return None;
    }
    end += 1;
    if integer == b'0' && value.get(end) == Some(&b'.') {
        end += 1;
        let fraction = end;
        while value.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
        if end == fraction {
            return None;
        }
    }
    match value.get(end) {
        None | Some(b',') => Some(end),
        _ => None,
    }
}

// The manual selected-materialization prototype retains its original guard.
#[cfg(test)]
fn is_common_finite_decimal(value: &str) -> bool {
    // These normalized decimal forms are finite without conversion. All other
    // representations retain the same parser, including exponent and overflow cases.
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    matches!(unsigned, "0" | "1")
        || unsigned.strip_prefix("0.").is_some_and(|fraction| {
            !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn decode_optional_vector_ordinal(raw: &str) -> Result<Option<u64>> {
    if raw == "-" {
        return Ok(None);
    }
    raw.parse::<u64>().map(Some).map_err(|_| {
        HawDBError::Storage("search metadata sidecar has an invalid vector ordinal".to_string())
    })
}

fn matched_span_bytes_for(span: &super::SearchMatchedSpan) -> u64 {
    (span.field.len() as u64)
        .saturating_add(span.text.len() as u64)
        .saturating_add(span.term.len() as u64)
        .saturating_add(std::mem::size_of::<super::SearchMatchedSpan>() as u64)
}

fn read_bound_artifact(
    path: &Path,
    expected_len: u64,
    expected_checksum: u64,
    max_bytes: u64,
    name: &str,
) -> Result<Vec<u8>> {
    if expected_len > max_bytes {
        return Err(HawDBError::Storage(format!(
            "{name} exceeds the manifest read budget"
        )));
    }
    let bytes = read_bound_file(path, expected_len, name)?;
    if bytes.len() as u64 != expected_len || checksum_bytes(&bytes) != expected_checksum {
        return Err(HawDBError::Storage(format!(
            "{name} length or checksum mismatch"
        )));
    }
    Ok(bytes)
}

fn open_exact_length_artifact(path: &Path, expected_len: u64, name: &str) -> Result<File> {
    let file = File::open(path)?;
    let actual_len = file.metadata()?.len();
    if actual_len != expected_len {
        return Err(HawDBError::Storage(format!(
            "{name} length mismatch: expected {expected_len}, got {actual_len}"
        )));
    }
    Ok(file)
}

fn read_marker_lines_bounded(path: &Path, max_bytes: u64) -> Result<Vec<String>> {
    // Only a missing entry means no marker. Do not hide lookup errors or
    // treat an existing but unreadable symlink as proof of freshness.
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    }
    let bytes = read_bounded_file(path, max_bytes)?;
    let content = std::str::from_utf8(&bytes)
        .map_err(|error| HawDBError::Storage(format!("search marker is not UTF-8: {error}")))?;
    Ok(content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

pub(super) fn next_generation(root: &Path, max_lexical_manifest_bytes: u64) -> Result<u64> {
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !fs::try_exists(&manifest_path)? {
        return Ok(1);
    }
    let active_generation = match read_bounded_file(&manifest_path, MAX_OUT_OF_CORE_MANIFEST_BYTES)
        .and_then(|bytes| SearchOutOfCoreManifestBody::decode(&bytes))
        .map(|manifest| manifest.generation)
    {
        Ok(generation) => generation,
        Err(error @ HawDBError::FileDescriptors(_)) => return Err(error),
        Err(_) => latest_recoverable_lexical_generation(root, max_lexical_manifest_bytes)?,
    };
    active_generation
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("search out-of-core generation overflow".to_string()))
}

fn latest_recoverable_lexical_generation(root: &Path, max_manifest_bytes: u64) -> Result<u64> {
    let mut latest = 0u64;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(generation) = name
            .strip_prefix("search_lexical.manifest.")
            .and_then(|value| value.strip_suffix(".hawdb"))
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if lexical_manifest_generation(&entry.path(), max_manifest_bytes)? == Some(generation) {
            latest = latest.max(generation);
        }
    }
    Ok(latest)
}

fn publish_generation_link(source: &Path, target: &Path) -> Result<u64> {
    let tmp = temporary_artifact_path(target);
    let mut guard = CandidateFileGuard::new(tmp.clone());
    let bytes_written = match fs::hard_link(source, &tmp) {
        Ok(()) => 0,
        Err(_) => {
            let copied = fs::copy(source, &tmp)?;
            File::open(&tmp)?.sync_all()?;
            copied
        }
    };
    durable_replace_file(&tmp, target)?;
    guard.disarm();
    Ok(bytes_written)
}

fn write_generation_artifact(target: &Path, bytes: &[u8]) -> Result<u64> {
    let tmp = temporary_artifact_path(target);
    let mut guard = CandidateFileGuard::new(tmp.clone());
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    durable_replace_file(&tmp, target)?;
    guard.disarm();
    Ok(bytes.len() as u64)
}

fn temporary_artifact_path(target: &Path) -> PathBuf {
    target.with_extension(format!(
        "tmp.{}.{}",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn read_search_range(file: &File, offset: u64, bytes: &mut [u8]) -> Result<()> {
    #[cfg(any(unix, windows))]
    {
        match hawdb_storage::io::read_exact_at(file, bytes, offset) {
            Ok(()) => Ok(()),
            #[cfg(windows)]
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Err(
                HawDBError::Storage("search range read reached an unexpected EOF".to_string()),
            ),
            Err(error) => Err(error.into()),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, offset, bytes);
        Err(HawDBError::Storage(
            "search out-of-core range reads are unsupported on this platform".to_string(),
        ))
    }
}

#[cfg(feature = "vector-search")]
pub(super) fn seal_resident_candidate(
    file: &mut File,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<(hawdb_storage::immutable_object::ObjectReference, u64)> {
    let memory = crate::build_memory::BuildMemory::new(task)?;
    let (seal, checksum) =
        generation_writer::io::GenerationIo::new(&memory, task).seal_file_with_checksum(file)?;
    Ok((seal.reference, checksum))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "full-text-search")]
    mod bm25_windows;
    #[cfg(feature = "full-text-search")]
    mod candidate_budget;
    mod reader_admission;
    #[cfg(feature = "full-text-search")]
    mod span_budget;
    mod target_admission;
    #[cfg(feature = "full-text-search")]
    mod text_seed;

    use super::*;
    use crate::{
        SearchFusionWeights, SearchLexicalFeasibilityCoverage, SearchLexicalFeasibilityMetrics,
        SearchLexicalProductionQualificationReport, SearchProjectionCleanupOptions,
    };
    use std::io::{Seek, SeekFrom};

    fn load_artifact_closure(
        root: &Path,
        config: &SearchOutOfCoreConfig,
        analyzer_lexicon: &SearchAnalyzerLexicon,
        lexical_term_policy: SearchLexicalTermPolicy,
        lexical_source_policy: SearchLexicalSourcePolicy,
    ) -> Result<(
        SearchOutOfCoreManifestBody,
        Vec<SearchOutOfCoreSegmentReader>,
        mutation_run::MutationVisibility,
    )> {
        let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
        let manifest_bytes = read_bounded_file(&manifest_path, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
        let manifest = SearchOutOfCoreManifestBody::decode(&manifest_bytes)?;
        let closure = load_artifact_closure_for_manifest(
            root,
            config,
            analyzer_lexicon,
            lexical_term_policy,
            lexical_source_policy,
            manifest,
            None,
        )?;
        Ok((closure.manifest, closure.segments, closure.visibility))
    }

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    thread_local! {
        // Each libtest thread owns its fresh fixture domains through cleanup.
        // Existing unknown roots on Windows require conservative alias probes;
        // fixtures must not borrow another concurrent test's exhausted domain.
        static TEST_PROJECTS: std::cell::RefCell<Vec<hawdb_storage::file_descriptors::ProjectFileDescriptors>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    pub(super) fn test_dir(name: &str) -> PathBuf {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = crate::test_temp_dir().join(format!(
            "hawdb-search-out-of-core-{name}-{}-{sequence}",
            std::process::id()
        ));
        let files = hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire(
            &path,
            hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
        )
        .unwrap();
        TEST_PROJECTS.with_borrow_mut(|owners| owners.push(files));
        path
    }

    pub(super) fn options(limit: usize, rank_window: Option<usize>) -> SearchQueryOptions {
        SearchQueryOptions {
            limit,
            offset: 0,
            rank_window,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        }
    }

    #[test]
    fn resident_sidecar_producer_references_reject_installed_same_length_changes() {
        check_resident_producer_changes(0..3);
    }

    #[test]
    fn resident_segment_and_lexical_producer_references_reject_installed_same_length_changes() {
        check_resident_producer_changes(3..7);
    }

    fn check_resident_producer_changes(artifacts: std::ops::Range<usize>) {
        for artifact in artifacts {
            let path = test_dir("resident-sidecar-producer-reference");
            let mut index = SearchIndex::open(&path).unwrap();
            index.upsert(document(0, "team")).unwrap();
            index.checkpoint().unwrap();
            let selected = fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
            AFTER_RESIDENT_SIDECARS.with(|hook| hook.set(Some(artifact)));
            let error = index.checkpoint().unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("differs from its producer reference"),
                "artifact {artifact}: {error}"
            );
            assert_eq!(
                fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                selected
            );
            assert_eq!(
                SearchOutOfCoreReader::open(&path).unwrap().document_count(),
                1
            );
            index.checkpoint().unwrap();
            let bytes = fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
            let manifest = SearchOutOfCoreManifestBody::decode(&bytes).unwrap();
            let segment = &manifest.segments[0];
            let lexical_payload =
                crate::lexical_projection::artifact_file(segment.objects.lexical_generation);
            for (name, object) in [
                (&segment.descriptor_file, segment.objects.descriptor),
                (&segment.payload_file, segment.objects.payload),
                (
                    &segment.lexical_manifest_file,
                    segment.objects.lexical_manifest,
                ),
                (&lexical_payload, segment.objects.lexical_payload),
                (&segment.metadata_payload_file, segment.objects.metadata),
                (&segment.vector_payload_file, segment.objects.vector),
                (&segment.layout_file, segment.objects.layout),
            ] {
                assert_eq!(
                    object,
                    ArtifactObject::for_bytes(&fs::read(path.join(name)).unwrap())
                );
            }
            assert_eq!(
                SearchOutOfCoreReader::open(&path).unwrap().document_count(),
                1
            );
            drop(index);
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn bound_artifact_read_retains_one_encoded_buffer() {
        let _serial = crate::test_allocation::serial();
        let root = test_dir("bound-artifact-allocation");
        let path = root.join("artifact");
        // Cross the final geometric growth boundary of a large descriptor.
        let data = vec![b'x'; 17 * 1024 * 1024 + 4096];
        fs::write(&path, &data).unwrap();
        let checksum = checksum_bytes(&data);
        // Shared descriptor state and droppable test hooks are fixture owners,
        // so initialize them before tracking this read's encoded buffer.
        drop(
            read_bound_artifact(
                &path,
                data.len() as u64,
                checksum,
                64 * 1024 * 1024,
                "allocation fixture",
            )
            .unwrap(),
        );
        let baseline = crate::test_allocation::live();
        let (bytes, peak) = crate::test_allocation::measure(|| {
            read_bound_artifact(
                &path,
                data.len() as u64,
                checksum,
                64 * 1024 * 1024,
                "allocation fixture",
            )
            .unwrap()
        });
        assert_eq!(bytes, data);
        let peak = peak.saturating_sub(baseline);
        assert!(
            peak <= data.len() + 64 * 1024,
            "bound artifact retained {peak} allocated bytes for {} encoded bytes",
            data.len()
        );
        drop(bytes);
        assert_eq!(crate::test_allocation::live(), baseline);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    #[cfg(any(unix, windows))]
    fn search_range_adapter_preserves_exact_reads_and_eof_errors() {
        let directory = test_dir("positioned-read-adapter");
        let path = directory.join("range.hawdb");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(b"abcdefgh").unwrap();
        let mut bytes = [0; 4];
        read_search_range(&file, 2, &mut bytes).unwrap();
        assert_eq!(&bytes, b"cdef");
        let error = read_search_range(&file, 6, &mut bytes).unwrap_err();
        #[cfg(unix)]
        assert_eq!(
            error,
            HawDBError::Storage("failed to fill whole buffer".to_string())
        );
        #[cfg(windows)]
        assert_eq!(
            error,
            HawDBError::Storage("search range read reached an unexpected EOF".to_string())
        );
        assert_eq!(
            read_search_range(&file, u64::MAX, &mut bytes).unwrap_err(),
            HawDBError::Storage("read range end overflows u64".to_string())
        );
        drop(file);
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    fn document(number: usize, space: &str) -> SearchDocument {
        SearchDocument {
            id: format!("memory:{number:03}"),
            title: std::iter::repeat_n("graph", number % 3 + 1)
                .collect::<Vec<_>>()
                .join(" "),
            content: format!("storage memory document {number}"),
            embedding: Some(vec![number as f32 + 1.0, (16 - number) as f32]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), number.to_string()),
                ("space_id".to_string(), space.to_string()),
            ]),
        }
    }

    fn production_identity() -> crate::ProductionQualificationIdentity {
        crate::ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: vec!["full-text-search".to_string(), "vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: 42,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        }
    }

    fn assert_search_parity(expected: &SearchResultSet, actual: &SearchResultSet) {
        assert_eq!(actual.total_hits, expected.total_hits);
        assert_eq!(
            actual.filtered_document_count,
            expected.filtered_document_count
        );
        assert_eq!(actual.hits.len(), expected.hits.len());
        for (actual, expected) in actual.hits.iter().zip(&expected.hits) {
            assert_eq!(actual.id, expected.id);
            assert_eq!(actual.vector_rank, expected.vector_rank);
            assert_eq!(actual.text_rank, expected.text_rank);
            assert!((actual.score - expected.score).abs() < 1e-12);
            assert!((actual.vector_score - expected.vector_score).abs() < 1e-12);
            assert!((actual.text_score - expected.text_score).abs() < 1e-12);
            assert_eq!(actual.matched_terms, expected.matched_terms);
            assert_eq!(actual.matched_spans, expected.matched_spans);
        }
    }

    #[test]
    fn out_of_core_text_vector_and_hybrid_match_resident_search() {
        let path = test_dir("parity");
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..12 {
            index.upsert(document(number, "team")).unwrap();
        }
        index.checkpoint().unwrap();
        let resident = SearchIndex::open(&path).unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reader.document_count(), 12);
        assert_eq!(reader.resident_document_count(), 0);
        let hydrated = reader
            .hydrate_documents(&["memory:003".to_string(), "memory:001".to_string()])
            .unwrap();
        assert_eq!(
            hydrated
                .documents
                .iter()
                .map(|document| document.id.as_str())
                .collect::<Vec<_>>(),
            vec!["memory:003", "memory:001"]
        );
        assert_eq!(hydrated.metrics.hydrated_documents, 2);

        let query_embedding = [1.0, 0.5];
        let modes: &[(SearchMode, Option<usize>)] = &[
            #[cfg(feature = "full-text-search")]
            (SearchMode::Text, None),
            #[cfg(feature = "vector-search")]
            (SearchMode::Vector, None),
            #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
            (SearchMode::Hybrid, Some(6)),
        ];
        for &(mode, rank_window) in modes {
            let options = options(5, rank_window);
            let expected = resident
                .try_search_with_options(
                    "graph storage",
                    Some(&query_embedding),
                    mode,
                    options.clone(),
                )
                .unwrap();
            let actual = reader
                .search_with_options("graph storage", Some(&query_embedding), mode, options)
                .unwrap();
            assert_search_parity(&expected, &actual.result);
            assert!(actual.metrics.hydrated_documents <= 5);
        }
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_text_search_uses_chinese_word_segmentation() {
        let path = test_dir("chinese-tokenization");
        let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:cn".to_string(),
                title: "\u{73b0}\u{4ee3}\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}".to_string(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();

        let output = reader
            .search_with_options(term, None, SearchMode::Text, options(10, None))
            .unwrap();

        assert_eq!(output.result.hits[0].id, "memory:cn");
        assert!(output.result.hits[0]
            .matched_terms
            .iter()
            .any(|token| token == term));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_candidate_spill_applies_metadata_before_ranking() {
        let path = test_dir("metadata");
        let spill = path.join("spill");
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..10 {
            let space = if number < 4 { "team" } else { "private" };
            index.upsert(document(number, space)).unwrap();
        }
        index.checkpoint().unwrap();
        let config = SearchOutOfCoreConfig {
            spill_directory: spill.clone(),
            ..SearchOutOfCoreConfig::default()
        };
        let reader = SearchOutOfCoreReader::open_with_config(&path, config).unwrap();
        let mut options = options(10, None);
        options
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());
        let output = reader
            .search_with_options("graph", None, SearchMode::Text, options)
            .unwrap();
        assert_eq!(output.result.filtered_document_count, 4);
        assert_eq!(output.result.total_hits, 4);
        assert_eq!(
            output.result.candidate_set.representation,
            "spilled_sorted_document_ids"
        );
        assert!(output.metrics.candidate_spill_bytes > 0);
        assert!(fs::read_dir(spill).unwrap().next().is_none());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn out_of_core_filter_and_vector_scoring_do_not_hydrate_full_documents() {
        let path = test_dir("sidecar-read-paths");
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..4 {
            index.upsert(document(number, "team")).unwrap();
        }
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();

        let mut filtered = options(0, None);
        filtered
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());
        let text = reader
            .search_with_options("graph", None, SearchMode::Text, filtered)
            .unwrap();
        assert_eq!(text.result.total_hits, 4);
        assert!(text.metrics.metadata_segment_bytes_read > 0);
        assert_eq!(text.metrics.vector_segment_bytes_read, 0);
        assert_eq!(text.metrics.hydration_segment_bytes_read, 0);
        assert_eq!(text.metrics.peak_segment_document_bytes, 0);

        let vector = reader
            .search_with_options("", Some(&[1.0, 0.5]), SearchMode::Vector, options(0, None))
            .unwrap();
        assert_eq!(vector.result.total_hits, 4);
        assert_eq!(vector.metrics.metadata_segment_bytes_read, 0);
        assert!(vector.metrics.vector_segment_bytes_read > 0);
        assert_eq!(vector.metrics.hydration_segment_bytes_read, 0);
        assert_eq!(vector.metrics.peak_segment_document_bytes, 0);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn out_of_core_vector_execution_options_enforce_memory_and_cancellation() {
        for diskann in [false, true] {
            check_vector_execution_options(diskann);
        }
    }

    #[cfg(feature = "vector-search")]
    fn check_vector_execution_options(diskann: bool) {
        let path = test_dir("vector-execution-options");
        let mut index = SearchIndex::open(&path).unwrap();
        if !diskann {
            index.set_rabitq_projection_build_options(Default::default());
        }
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();

        let memory_error = reader
            .search_with_options_compressed_vector_projection_execution_options(
                "",
                Some(&[1.0, 0.5]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Disabled,
                VectorSearchExecutionOptions::bounded(NonZeroUsize::MIN, 64, None),
            )
            .unwrap_err();
        if diskann {
            assert!(
                matches!(&memory_error, HawDBError::ResourceBudgetExceeded(details)
                    if details.resource == hawdb_core::ResourceBudgetKind::MemoryBytes),
                "{memory_error:?}"
            );
        } else {
            assert!(memory_error
                .to_string()
                .contains("uniqueness check exceeds the admitted working memory"));
        }

        let cancellation = crate::RuntimeCancellationToken::new();
        let task_context = crate::RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let cancellation_error = reader
            .search_with_options_compressed_vector_projection_execution_options(
                "",
                Some(&[1.0, 0.5]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Disabled,
                VectorSearchExecutionOptions::bounded(NonZeroUsize::MIN, 1024, Some(&task_context)),
            )
            .unwrap_err();
        assert!(cancellation_error.to_string().contains("cancelled"));
        fs::remove_dir_all(path).unwrap();
    }

    #[cfg(all(feature = "acl", feature = "full-text-search"))]
    #[test]
    fn out_of_core_acl_excludes_hidden_documents_from_bm25_candidates() {
        let path = test_dir("acl");
        let mut reference = SearchIndex::in_memory();
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..2 {
            let allowed = document(number, "team");
            reference.upsert(allowed.clone()).unwrap();
            index.upsert(allowed).unwrap();
        }
        let mut hidden = document(9, "private");
        hidden.title = "graph graph graph graph graph graph".to_string();
        index.upsert(hidden).unwrap();
        index.checkpoint().unwrap();
        let expected = reference
            .try_search_with_options("graph", None, SearchMode::Text, options(10, None))
            .unwrap();
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        reader.set_runtime_capabilities(
            RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, true),
        );
        let actual = reader
            .search_with_options_access_control(
                "graph",
                None,
                SearchMode::Text,
                options(10, None),
                SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]),
            )
            .unwrap();
        assert_eq!(actual.result.total_hits, expected.total_hits);
        assert_eq!(actual.result.hits.len(), expected.hits.len());
        for (actual, expected) in actual.result.hits.iter().zip(&expected.hits) {
            assert_eq!(actual.id, expected.id);
            assert_eq!(actual.text_rank, expected.text_rank);
            assert_eq!(actual.matched_terms, expected.matched_terms);
            assert_eq!(actual.matched_spans, expected.matched_spans);
        }
        assert_eq!(actual.result.candidate_set.filtered_out_count, 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_hydration_and_candidate_budgets_fail_closed() {
        let path = test_dir("budgets");
        let mut index = SearchIndex::open(&path).unwrap();
        let mut large = document(0, "team");
        large.content = "graph ".repeat(256);
        index.upsert(large).unwrap();
        index.checkpoint().unwrap();

        let hydration_config = SearchOutOfCoreConfig {
            max_hydrated_bytes: NonZeroU64::new(32).unwrap(),
            ..SearchOutOfCoreConfig::default()
        };
        let hydration_reader =
            SearchOutOfCoreReader::open_with_config(&path, hydration_config).unwrap();
        let error = hydration_reader
            .search_with_options("graph", None, SearchMode::Text, options(1, None))
            .unwrap_err();
        assert!(error.to_string().contains("search hydration requires"));
        assert!(matches!(
            error,
            HawDBError::ResourceBudgetExceeded(details)
                if details.budget_option == "SearchOutOfCoreConfig.max_hydrated_bytes"
        ));

        let span_config = SearchOutOfCoreConfig {
            max_matched_spans: NonZeroUsize::new(1).unwrap(),
            ..SearchOutOfCoreConfig::default()
        };
        let span_reader = SearchOutOfCoreReader::open_with_config(&path, span_config).unwrap();
        let error = span_reader
            .search_with_options("graph", None, SearchMode::Text, options(1, None))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("matched-span hydration exceeded"));

        let spill_config = SearchOutOfCoreConfig {
            spill_directory: path.join("spill-budget"),
            max_candidate_spill_bytes: NonZeroU64::new(1).unwrap(),
            ..SearchOutOfCoreConfig::default()
        };
        let spill_reader = SearchOutOfCoreReader::open_with_config(&path, spill_config).unwrap();
        let mut filtered = options(1, None);
        filtered
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());
        let error = spill_reader
            .search_with_options("graph", None, SearchMode::Text, filtered)
            .unwrap_err();
        assert!(error.to_string().contains("candidate spill requires"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_segment_corruption_fails_during_late_hydration() {
        let path = test_dir("corruption");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let payload_path = path.join(
            &reader
                .manifest
                .segments
                .first()
                .expect("manifest has a segment")
                .payload_file,
        );
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(payload_path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        #[cfg(feature = "full-text-search")]
        {
            let unhydrated = reader
                .search_with_options("graph", None, SearchMode::Text, options(0, None))
                .unwrap();
            assert_eq!(unhydrated.result.total_hits, 1);
            assert_eq!(unhydrated.metrics.hydration_segment_bytes_read, 0);
            let error = reader
                .search_with_options("graph", None, SearchMode::Text, options(1, None))
                .unwrap_err();
            assert!(error.to_string().contains("payload checksum mismatch"));
        }
        let error = reader
            .hydrate_documents(&[document(0, "team").id])
            .unwrap_err();
        assert!(error.to_string().contains("payload checksum mismatch"));
        drop(file);
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_reader_rejects_duplicate_manifest_segment_ids() {
        let path = test_dir("multiple-manifest-segments");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();

        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let envelope: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut body = envelope.body;
        let duplicate = body.segments[0].clone();
        body.segments.push(duplicate);
        let body_bytes = serde_json::to_vec(&body).unwrap();
        let bytes = serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
            body,
            checksum: checksum_bytes(&body_bytes),
        })
        .unwrap();
        fs::write(&manifest_path, bytes).unwrap();

        let error = SearchOutOfCoreReader::open(&path).unwrap_err();
        assert!(error.to_string().contains("duplicate segment ids"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_reader_and_generation_update_reject_duplicate_manifest_documents() {
        let path = test_dir("duplicate-segment-documents");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();

        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let envelope: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut body = envelope.body;
        let mut duplicate = body.segments[0].clone();
        duplicate.segment_id = 1;
        body.segments.push(duplicate);
        body.document_count = 2;
        let body_bytes = serde_json::to_vec(&body).unwrap();
        let bytes = serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
            body,
            checksum: checksum_bytes(&body_bytes),
        })
        .unwrap();
        fs::write(&manifest_path, &bytes).unwrap();

        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let error = reader
            .search_with_options("graph", None, SearchMode::Text, options(1, None))
            .unwrap_err();
        assert!(error.to_string().contains("duplicate document"));
        let error = reader
            .hydrate_documents(&[document(0, "team").id])
            .unwrap_err();
        assert!(error.to_string().contains("duplicate document"));
        #[cfg(feature = "vector-search")]
        {
            let error = reader
                .search_with_options("", Some(&[1.0, 16.0]), SearchMode::Vector, options(1, None))
                .unwrap_err();
            assert!(error.to_string().contains("duplicate document"));
        }
        let error = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta::default(),
            Default::default(),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("duplicate document content artifacts"));
        assert_eq!(fs::read(&manifest_path).unwrap(), bytes);
        fs::remove_dir_all(path).unwrap();
    }

    #[cfg(feature = "full-text-search")]
    fn publish_two_artifact_manifest(
        path: &Path,
        initial_document: SearchDocument,
        next_document: SearchDocument,
    ) {
        publish_two_artifact_manifest_with_options(
            path,
            initial_document,
            Default::default(),
            next_document,
            Default::default(),
        );
    }

    #[cfg(feature = "full-text-search")]
    fn publish_two_artifact_manifest_with_options(
        path: &Path,
        initial_document: SearchDocument,
        initial_options: SearchOutOfCoreGenerationBuildOptions,
        next_document: SearchDocument,
        next_options: SearchOutOfCoreGenerationBuildOptions,
    ) {
        publish_two_artifact_manifest_with_initial_documents(
            path,
            vec![initial_document],
            initial_options,
            next_document,
            next_options,
        );
    }

    #[cfg(feature = "full-text-search")]
    fn publish_two_artifact_manifest_with_initial_documents(
        path: &Path,
        initial_documents: Vec<SearchDocument>,
        mut initial_options: SearchOutOfCoreGenerationBuildOptions,
        next_document: SearchDocument,
        mut next_options: SearchOutOfCoreGenerationBuildOptions,
    ) {
        // This family qualifies legacy layered RaBitQ, including mixed
        // quantization and budget fallback. Default graphs have their own CRUD
        // and pin/reopen family in generation_writer::tests::diskann.
        initial_options.vector_backend = None;
        next_options.vector_backend = None;
        let mut initial = SearchOutOfCoreGenerationWriter::create(path, initial_options).unwrap();
        for document in initial_documents {
            initial.push(document).unwrap();
        }
        initial.finish().unwrap();

        // Ordinary readers do not register GC pins. Retain an admitted private
        // capture until the combined manifest selects the first closure again.
        let initial_capture = {
            use crate::build_memory::read::{ReadAdmission, ReadPermit};
            let reader = SearchOutOfCoreReader::open(path).unwrap();
            let governor = crate::build_memory::read::tests::governor();
            let permit = governor
                .try_admit(
                    hawdb_qos::RuntimeWorkRequest::background_maintenance(8 << 20)
                        .with_io_wave_slots(1),
                )
                .unwrap();
            let task = permit.bind_task_context(Default::default());
            let memory = crate::build_memory::BuildMemory::new(&task).unwrap();
            let admission = ReadAdmission {
                memory: &memory,
                task: &task,
                permit: ReadPermit::Governed(&permit),
            };
            admitted_root::open(admitted_root::OpenContext {
                root: path,
                config: reader.config.clone(),
                analyzer_lexicon: reader.analyzer_lexicon.clone(),
                lexical_term_policy: reader.lexical_term_policy,
                lexical_source_policy: reader.lexical_source_policy,
                admission: Some(&admission),
                project: Some(&reader._project_files),
                max_artifact_read_bytes: 32 << 20,
                publication: None,
                stationary: None,
                expected_stationary_manifest: None,
            })
            .unwrap()
        };
        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let initial_manifest: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let initial_segment = initial_manifest
            .body
            .segments
            .into_iter()
            .next()
            .expect("initial manifest has one segment");

        let mut next = SearchOutOfCoreGenerationWriter::create(path, next_options).unwrap();
        next.push(next_document).unwrap();
        next.finish().unwrap();
        if let Some(name) = &initial_segment.rabitq_artifact_file {
            assert!(
                path.join(name).is_file(),
                "the captured first segment must survive replacement cleanup"
            );
        }

        let next_manifest: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut body = next_manifest.body;
        let mut next_segment = body.segments.pop().expect("next manifest has one segment");
        next_segment.segment_id = 1;
        body.segments = vec![initial_segment, next_segment];
        body.document_count = body
            .segments
            .iter()
            .map(|segment| segment.document_count)
            .sum();
        body.documents_digest = body.segments.iter().fold(0, |digest, segment| {
            crate::lexical_projection::DocumentsDigest::combine(digest, segment.documents_digest)
        });
        let body_bytes = serde_json::to_vec(&body).unwrap();
        fs::write(
            &manifest_path,
            serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
                body,
                checksum: checksum_bytes(&body_bytes),
            })
            .unwrap(),
        )
        .unwrap();
        drop(initial_capture);
    }

    fn install_delete_mutation_run(
        path: &Path,
        document: &SearchDocument,
        target_segment_id: u64,
        run_generation: u64,
    ) {
        install_edited_mutation_run(path, document, target_segment_id, run_generation, |_| {});
    }

    #[cfg(feature = "full-text-search")]
    fn install_delete_mutation_runs(
        path: &Path,
        targets: &[(&SearchDocument, u64)],
        run_generation: u64,
    ) {
        let reader = SearchOutOfCoreReader::open(path).unwrap();
        let entries = targets
            .iter()
            .map(|(document, target_segment_id)| {
                Ok(mutation_run::SearchMutationRunEntry {
                    document_id: document.id.clone(),
                    target_segment_id: *target_segment_id,
                    operation: mutation_run::SearchMutationOperation::Delete,
                    retraction: mutation_run::SearchMutationRetraction::from_document(
                        document,
                        &reader.segments[0].lexical_projection,
                        &SearchAnalyzerLexicon::default(),
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let envelope: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut manifest = envelope.body;
        let mut documents_digest = manifest.segments.iter().fold(0, |digest, segment| {
            crate::lexical_projection::DocumentsDigest::combine(digest, segment.documents_digest)
        });
        for entry in &entries {
            documents_digest = crate::lexical_projection::DocumentsDigest::replace(
                documents_digest,
                entry.retraction.documents_digest,
                0,
            );
        }
        let analyzer_digest = lexical_analyzer_digest(&SearchAnalyzerLexicon::default());
        let run_body =
            mutation_run::SearchMutationRunBody::new(manifest.generation, analyzer_digest, entries)
                .unwrap();
        let run_bytes = run_body.encode().unwrap();
        let run_file = mutation_run::artifact_file(run_generation);
        fs::write(path.join(&run_file), &run_bytes).unwrap();
        manifest.mutation_runs = vec![SearchOutOfCoreMutationRunManifest {
            object: ArtifactObject::for_bytes(&run_bytes),
            generation: manifest.generation,
            file: run_file,
            len: run_bytes.len() as u64,
            checksum: checksum_bytes(&run_bytes),
            entry_count: targets.len(),
            analyzer_digest,
        }];
        manifest.document_count = manifest.document_count.checked_sub(targets.len()).unwrap();
        manifest.documents_digest = documents_digest;
        let body_bytes = serde_json::to_vec(&manifest).unwrap();
        fs::write(
            manifest_path,
            serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
                body: manifest,
                checksum: checksum_bytes(&body_bytes),
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn install_edited_mutation_run(
        path: &Path,
        document: &SearchDocument,
        target_segment_id: u64,
        run_generation: u64,
        edit: impl FnOnce(&mut mutation_run::SearchMutationRunEntry),
    ) {
        let reader = SearchOutOfCoreReader::open(path).unwrap();
        let mut entry = mutation_run::SearchMutationRunEntry {
            document_id: document.id.clone(),
            target_segment_id,
            operation: mutation_run::SearchMutationOperation::Delete,
            retraction: mutation_run::SearchMutationRetraction::from_document(
                document,
                &reader.segments[0].lexical_projection,
                &SearchAnalyzerLexicon::default(),
            )
            .unwrap(),
        };
        edit(&mut entry);
        // Recompute both envelope checksums and the manifest aggregate identity.
        // Invalid fixtures must reach target verification, not fail a checksum.
        let document_digest = entry.retraction.documents_digest;
        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let envelope: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let mut manifest = envelope.body;
        manifest.generation = run_generation;
        let analyzer_digest = lexical_analyzer_digest(&SearchAnalyzerLexicon::default());
        let run_body = mutation_run::SearchMutationRunBody::new(
            manifest.generation,
            analyzer_digest,
            vec![entry],
        )
        .unwrap();
        let run_bytes = run_body.encode().unwrap();
        let run_file = mutation_run::artifact_file(manifest.generation);
        fs::write(path.join(&run_file), &run_bytes).unwrap();
        manifest.mutation_runs = vec![SearchOutOfCoreMutationRunManifest {
            object: ArtifactObject::for_bytes(&run_bytes),
            generation: manifest.generation,
            file: run_file,
            len: run_bytes.len() as u64,
            checksum: checksum_bytes(&run_bytes),
            entry_count: 1,
            analyzer_digest,
        }];
        manifest.document_count = manifest.document_count.checked_sub(1).unwrap();
        manifest.documents_digest = crate::lexical_projection::DocumentsDigest::replace(
            manifest.segments.iter().fold(0, |digest, segment| {
                crate::lexical_projection::DocumentsDigest::combine(
                    digest,
                    segment.documents_digest,
                )
            }),
            document_digest,
            0,
        );
        let body_bytes = serde_json::to_vec(&manifest).unwrap();
        fs::write(
            manifest_path,
            serde_json::to_vec(&SearchOutOfCoreManifestEnvelope {
                body: manifest,
                checksum: checksum_bytes(&body_bytes),
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn out_of_core_reader_validates_mutation_run_artifact_closure() {
        let path = test_dir("mutation-run-artifact-closure");
        let document = document(0, "team");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document.clone()).unwrap();
        index.checkpoint().unwrap();
        let old_reader = SearchOutOfCoreReader::open(&path).unwrap();
        install_delete_mutation_run(&path, &document, 0, 2);

        // Integrity/cleanup and the public reader must agree on the complete
        // closure: the deleted physical version is retained for target-bound
        // validation but is not visible through hydration or search.
        let manifest: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap())
                .unwrap();
        let limited = SearchOutOfCoreReader::open_with_config(
            &path,
            SearchOutOfCoreConfig {
                max_mutation_run_bytes: NonZeroU64::new(manifest.body.mutation_runs[0].len - 1)
                    .unwrap(),
                ..SearchOutOfCoreConfig::default()
            },
        )
        .unwrap_err();
        assert!(limited
            .to_string()
            .contains("mutation-run artifact exceeds the configured read budget"));
        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reopened.document_count(), 0);
        assert!(reopened
            .hydrate_documents(std::slice::from_ref(&document.id))
            .is_err());
        let (validated, _, _) = load_artifact_closure(
            &path,
            &SearchOutOfCoreConfig::default(),
            &SearchAnalyzerLexicon::default(),
            SearchLexicalTermPolicy::default(),
            SearchLexicalSourcePolicy::default(),
        )
        .unwrap();
        assert_eq!(validated.document_count, 0);
        let generations = published_artifact_generations(&path, &SearchAnalyzerLexicon::default())
            .unwrap()
            .unwrap();
        assert_eq!(generations.active_generation, 2);
        assert_eq!(generations.lexical_generations, BTreeSet::from([1]));
        assert_eq!(generations.out_of_core_generations, BTreeSet::from([1, 2]));

        let cleanup = index.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
        assert_eq!(cleanup.generation_discovery_failures, 0);
        assert!(!cleanup.retry_required, "{cleanup:?}");
        assert!(path.join(&manifest.body.mutation_runs[0].file).is_file());
        assert_eq!(
            old_reader
                .hydrate_documents(std::slice::from_ref(&document.id))
                .unwrap()
                .documents,
            vec![document.clone()]
        );

        fs::write(
            path.join(&manifest.body.mutation_runs[0].file),
            b"corrupt mutation run",
        )
        .unwrap();
        let error = SearchOutOfCoreReader::open(&path).unwrap_err();
        assert!(error
            .to_string()
            .contains("mutation-run artifact length or checksum mismatch"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn mutation_publication_recovery_ignores_orphaned_run_after_manifest_failure() {
        let path = test_dir("mutation-publication-recovery");
        let document = document(0, "team");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document.clone()).unwrap();
        index.checkpoint().unwrap();

        install_delete_mutation_run(&path, &document, 0, 2);
        let active_manifest = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let published = fs::read(&active_manifest).unwrap();
        let envelope: SearchOutOfCoreManifestEnvelope = serde_json::from_slice(&published).unwrap();
        let mutation_run = envelope.body.mutation_runs[0].file.clone();
        fs::copy(
            path.join(&mutation_run),
            path.join(mutation_run::artifact_file(3)),
        )
        .unwrap();

        // A process crash after the run file is durable but before the
        // manifest publish must leave the previous committed generation
        // selectable. Recovery must not infer a mutation closure from the
        // orphaned artifact alone.
        fs::write(&active_manifest, b"torn manifest tail").unwrap();
        drop(index);
        let mut retry = SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        retry.push(document.clone()).unwrap();
        let report = retry.finish().unwrap();
        assert_eq!(report.generation, 2);
        let recovered = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(recovered.generation(), 2);
        assert_eq!(recovered.document_count(), 1);
        assert_eq!(
            recovered
                .hydrate_documents(std::slice::from_ref(&document.id))
                .unwrap()
                .documents,
            vec![document]
        );
        drop(recovered);

        assert!(path.join(mutation_run::artifact_file(3)).exists());
        let active: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(&fs::read(&active_manifest).unwrap()).unwrap();
        assert!(active
            .body
            .mutation_runs
            .iter()
            .all(|run| run.generation != 3));
        fs::remove_dir_all(path).unwrap();
    }

    // Test the shared read implementation through the public constructor. The
    // mutation writer and compaction lifecycle remain covered separately.
    #[cfg(feature = "full-text-search")]
    fn mutation_reader_for_test(path: &Path) -> SearchOutOfCoreReader {
        SearchOutOfCoreReader::open(path).unwrap()
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn reader_refresh_validates_only_new_retractions_without_compaction() {
        let path = test_dir("refresh-uncompacted-history");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        writer.push(document(0, "team")).unwrap();
        writer.push(document(1, "team")).unwrap();
        writer.finish().unwrap();
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let original = reader.primary_segment().clone();
        for round in 0..24 {
            let row = crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: "000".into(),
                title: format!("graph revision {round}"),
                body: "graph refresh history ".repeat(32),
                embedding: Some(vec![1.0, round as f32]),
                source_id: None,
                metadata: BTreeMap::new(),
            };
            SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                crate::SearchProjectionDelta {
                    upserts: vec![row.clone()],
                    ..Default::default()
                },
                Default::default(),
            )
            .unwrap()
            .finish()
            .unwrap();
            let report = reader.refresh().unwrap();
            assert!(report.generation_changed);
            assert_eq!(report.validated_retractions, 1);
            assert_eq!(report.reused_retractions, round);
            assert_eq!(report.opened_content_segments, 1);
            assert_eq!(report.reused_content_segments, round + 1);
            assert!(std::ptr::eq(
                &original.payload,
                &reader.primary_segment().payload
            ));
            assert!(std::ptr::eq(
                &original.descriptor,
                &reader.primary_segment().descriptor
            ));
            assert!(std::ptr::eq(
                &original.layout,
                &reader.primary_segment().layout
            ));
            assert_eq!(reader.manifest.mutation_runs.len(), round + 1);
            assert_eq!(reader.document_count(), 2);
            assert_eq!(
                reader
                    .hydrate_documents(&["memory:000".into()])
                    .unwrap()
                    .documents,
                vec![row.into_document()]
            );
            let fresh = SearchOutOfCoreReader::open(&path).unwrap();
            assert_search_parity(
                &fresh
                    .search_with_options("graph", None, SearchMode::Text, options(10, None))
                    .unwrap()
                    .result,
                &reader
                    .search_with_options("graph", None, SearchMode::Text, options(10, None))
                    .unwrap()
                    .result,
            );
            let unchanged = reader.refresh().unwrap();
            assert!(!unchanged.generation_changed);
            assert_eq!(unchanged.validated_retractions, 0);
            assert_eq!(unchanged.reused_retractions, round + 1);
            assert_eq!(unchanged.opened_content_segments, 0);
            assert_eq!(unchanged.reused_content_segments, round + 2);
        }
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn reader_refresh_rejects_fabricated_new_retractions_and_retains_its_pin() {
        let path = test_dir("refresh-fabricated-target");
        let old = document(0, "team");
        publish_two_artifact_manifest(&path, old.clone(), document(1, "team"));
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let generation = reader.generation();
        let expected = reader
            .search_with_options("graph", None, SearchMode::Text, options(10, None))
            .unwrap()
            .result;
        install_edited_mutation_run(&path, &old, 0, generation + 1, |entry| {
            entry.document_id = "memory:missing".into();
        });
        let error = reader.refresh().unwrap_err();
        assert!(
            error.to_string().contains("target document is absent"),
            "{error}"
        );
        assert_eq!(reader.generation(), generation);
        assert_search_parity(
            &expected,
            &reader
                .search_with_options("graph", None, SearchMode::Text, options(10, None))
                .unwrap()
                .result,
        );
        assert!(SearchOutOfCoreReader::open(&path).is_err());
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn reader_refresh_revalidates_a_changed_run_at_a_known_run_generation() {
        let path = test_dir("refresh-changed-run-identity");
        let old = document(0, "team");
        publish_two_artifact_manifest(&path, old.clone(), document(1, "team"));
        install_delete_mutation_run(&path, &old, 0, 3);
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let generation = reader.generation();
        let mut manifest = reader.manifest.clone();
        let reference = &mut manifest.mutation_runs[0];
        let mut entry = reader.visibility.retractions().next().unwrap().clone();
        entry.retraction.lexical_document_len += 1;
        let bytes = mutation_run::SearchMutationRunBody::new(
            reference.generation,
            reference.analyzer_digest,
            vec![entry],
        )
        .unwrap()
        .encode()
        .unwrap();
        // Keep the run generation, target and aggregate document identity. Only
        // the immutable run fingerprint changes in this corruption fixture.
        fs::write(path.join(&reference.file), &bytes).unwrap();
        reference.len = bytes.len() as u64;
        reference.object = ArtifactObject::for_bytes(&bytes);
        reference.checksum = checksum_bytes(&bytes);
        manifest.generation += 1;
        fs::write(
            path.join(OUT_OF_CORE_MANIFEST_FILE),
            manifest.encode().unwrap(),
        )
        .unwrap();
        let error = reader.refresh().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match its target document"),
            "{error}"
        );
        assert_eq!(reader.generation(), generation);
        assert_eq!(reader.document_count(), 1);
        assert!(SearchOutOfCoreReader::open(&path).is_err());
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reader_refresh_revalidates_changed_content_references() {
        let path = test_dir("refresh-changed-content-reference");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        writer.push(document(0, "team")).unwrap();
        writer.finish().unwrap();
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let generation = reader.generation();
        let mut manifest = reader.manifest.clone();
        manifest.generation += 1;
        manifest.segments[0].payload_len += 1;
        manifest.segments[0].objects.payload.byte_length += 1;
        fs::write(
            path.join(OUT_OF_CORE_MANIFEST_FILE),
            manifest.encode().unwrap(),
        )
        .unwrap();
        assert!(reader.refresh().is_err());
        assert_eq!(reader.generation(), generation);
        assert!(SearchOutOfCoreReader::open(&path).is_err());
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reader_refresh_rejects_missing_pinned_payload_and_lexical_paths() {
        let path = test_dir("refresh-missing-pinned-paths");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        writer.push(document(0, "team")).unwrap();
        writer.finish().unwrap();
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let generation = reader.generation();
        let files = [
            reader.manifest.segments[0].payload_file.clone(),
            crate::lexical_projection::artifact_file(
                reader.primary_segment().lexical_projection.generation(),
            ),
        ];
        for file in files {
            let original = path.join(file);
            let retained = path.join("retained-missing-artifact");
            fs::rename(&original, &retained).unwrap();
            assert!(reader.refresh().is_err());
            let mut manifest = reader.manifest.clone();
            manifest.generation += 1;
            fs::write(
                path.join(OUT_OF_CORE_MANIFEST_FILE),
                manifest.encode().unwrap(),
            )
            .unwrap();
            assert!(reader.refresh().is_err());
            assert_eq!(reader.generation(), generation);
            fs::rename(retained, original).unwrap();
            fs::write(
                path.join(OUT_OF_CORE_MANIFEST_FILE),
                reader.manifest.encode().unwrap(),
            )
            .unwrap();
        }
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn reader_refresh_revalidates_changed_global_embedding_identity() {
        let path = test_dir("refresh-embedding-identity");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        writer.push(document(0, "team")).unwrap();
        let report = writer.finish().unwrap();
        assert!(report.rabitq_artifact_bytes > 0);
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        let generation = reader.generation();
        for field in 0..3 {
            let mut manifest = reader.manifest.clone();
            manifest.generation += 1;
            match field {
                0 => manifest.embedding_model = Some("changed-model".into()),
                1 => manifest.embedding_version = Some("changed-version".into()),
                2 => manifest.embedding_dimension = Some(3),
                _ => unreachable!(),
            }
            fs::write(
                path.join(OUT_OF_CORE_MANIFEST_FILE),
                manifest.encode().unwrap(),
            )
            .unwrap();
            let error = reader.refresh().unwrap_err();
            assert!(error.to_string().contains("identity"), "{error}");
            assert_eq!(reader.generation(), generation);
            assert!(SearchOutOfCoreReader::open(&path).is_err());
        }
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn reader_refresh_rejects_regressed_and_reused_head_generations() {
        let path = test_dir("refresh-head-identity");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        writer.push(document(0, "team")).unwrap();
        writer.finish().unwrap();
        let manifest_path = path.join(OUT_OF_CORE_MANIFEST_FILE);
        let old_bytes = fs::read(&manifest_path).unwrap();
        let mut reader = SearchOutOfCoreReader::open(&path).unwrap();
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                deletes: vec![document(0, "team").id],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        reader.refresh().unwrap();
        let generation = reader.generation();
        let current_bytes = fs::read(&manifest_path).unwrap();
        fs::write(&manifest_path, old_bytes).unwrap();
        let error = reader.refresh().unwrap_err();
        assert!(
            error.to_string().contains("durable head regressed"),
            "{error}"
        );
        assert_eq!(reader.generation(), generation);
        let mut changed = SearchOutOfCoreManifestBody::decode(&current_bytes).unwrap();
        changed.source_graph_commit_epoch = Some(123);
        fs::write(&manifest_path, changed.encode().unwrap()).unwrap();
        let error = reader.refresh().unwrap_err();
        assert!(
            error.to_string().contains("changed an existing generation"),
            "{error}"
        );
        assert_eq!(reader.generation(), generation);
        fs::write(&manifest_path, current_bytes).unwrap();
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_publication_replacements_match_rebuild_and_can_delete_to_empty() {
        let path = test_dir("mutation-replace-publication");
        let merged_path = test_dir("mutation-replace-publication-merged");
        let old = document(0, "team");
        publish_two_artifact_manifest_with_initial_documents(
            &path,
            vec![old.clone(), document(1, "team")],
            Default::default(),
            document(2, "team"),
            Default::default(),
        );
        install_delete_mutation_run(&path, &old, 0, 3);
        let reader = mutation_reader_for_test(&path);
        let row = |id: &str, body: &str| crate::SearchProjectionRow {
            kind: crate::SearchProjectionKind::Memory,
            external_id: id.into(),
            title: "graph replacement".into(),
            body: body.into(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: None,
            metadata: BTreeMap::new(),
        };
        let revived = row("000", "new graph version");
        let replacement = row("001", "updated graph memory");
        let (delta, _, _) = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                upserts: vec![replacement.clone(), revived.clone()],
                deletes: vec!["memory:002".into()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        assert_eq!(delta.before_document_count, 2);
        assert_eq!(delta.after_document_count, 2);
        assert_eq!(delta.deleted_documents, 1);
        let current = mutation_reader_for_test(&path);
        let mut merged =
            SearchOutOfCoreGenerationWriter::create(&merged_path, Default::default()).unwrap();
        merged.push(revived.clone().into_document()).unwrap();
        merged.push(replacement.into_document()).unwrap();
        merged.finish().unwrap();
        let merged = SearchOutOfCoreReader::open(&merged_path).unwrap();
        let expected = merged
            .search_with_options("graph", None, SearchMode::Text, options(10, None))
            .unwrap();
        let actual = current
            .search_with_options("graph", None, SearchMode::Text, options(10, None))
            .unwrap();
        assert_search_parity(&expected.result, &actual.result);
        let latest = row("001", "latest graph revision");
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &current,
            crate::SearchProjectionDelta {
                upserts: vec![latest.clone()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let replaced = mutation_reader_for_test(&path);
        assert_eq!(
            replaced
                .hydrate_documents(&["memory:001".into()])
                .unwrap()
                .documents,
            vec![latest.into_document()]
        );
        assert_eq!(replaced.manifest.mutation_runs.len(), 3);
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &replaced,
            crate::SearchProjectionDelta {
                deletes: vec!["memory:000".into(), "memory:001".into()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let empty = mutation_reader_for_test(&path);
        assert_eq!(empty.document_count(), 0);
        assert_eq!(empty.manifest.documents_digest, 0);
        assert!(empty
            .search_with_options("graph", None, SearchMode::Text, options(10, None))
            .unwrap()
            .result
            .hits
            .is_empty());
        drop(empty);
        drop(replaced);
        drop(current);
        drop(reader);
        drop(merged);
        fs::remove_dir_all(path).unwrap();
        fs::remove_dir_all(merged_path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_publication_budget_and_cancel_leave_active_manifest_unchanged() {
        let path = test_dir("mutation-publication-rejection");
        let old = document(0, "team");
        publish_two_artifact_manifest(&path, old.clone(), document(1, "team"));
        install_delete_mutation_run(&path, &old, 0, 3);
        let before = fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let delta = || crate::SearchProjectionDelta {
            deletes: vec!["memory:001".into()],
            ..Default::default()
        };
        let mut reader = mutation_reader_for_test(&path);
        reader.config.max_mutation_run_bytes = NonZeroU64::MIN;
        assert!(SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            delta(),
            Default::default()
        )
        .unwrap_err()
        .to_string()
        .contains("mutation term spill exceeds byte admission"));
        assert_eq!(
            fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        reader.config.max_mutation_run_bytes =
            SearchOutOfCoreConfig::default().max_mutation_run_bytes;
        reader.config.max_mutation_working_bytes = NonZeroU64::MIN;
        assert!(SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            delta(),
            Default::default()
        )
        .unwrap_err()
        .to_string()
        .contains("mutation-run working set"));
        assert_eq!(
            fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        reader.config.max_mutation_working_bytes =
            SearchOutOfCoreConfig::default().max_mutation_working_bytes;
        let token = crate::RuntimeCancellationToken::new();
        let task = crate::RuntimeTaskContext::without_deadline(token.clone());
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            delta(),
            Default::default(),
            task,
        )
        .unwrap();
        assert!(token.cancel());
        assert!(update
            .finish()
            .unwrap_err()
            .to_string()
            .contains("cancelled"));
        assert_eq!(
            fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        drop(reader);
        let reopened = mutation_reader_for_test(&path);
        assert_eq!(reopened.document_count(), 1);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_delete_publication_reuses_content_and_repeated_delete_is_a_noop() {
        let path = test_dir("mutation-delete-publication");
        let old = document(0, "team");
        let deleted = document(1, "team");
        let live = document(2, "team");
        publish_two_artifact_manifest_with_initial_documents(
            &path,
            vec![old.clone(), deleted.clone()],
            Default::default(),
            live.clone(),
            Default::default(),
        );
        install_delete_mutation_run(&path, &old, 0, 3);
        let reader = mutation_reader_for_test(&path);
        let physical = serde_json::to_vec(&reader.manifest.segments).unwrap();
        let corpus_artifact_bytes: u64 = reader
            .manifest
            .segments
            .iter()
            .map(|segment| {
                segment
                    .descriptor_len
                    .saturating_add(segment.payload_len)
                    .saturating_add(segment.metadata_payload_len)
                    .saturating_add(segment.vector_payload_len)
                    .saturating_add(segment.layout_len)
                    .saturating_add(segment.lexical_manifest_len)
                    .saturating_add(segment.rabitq_artifact_len.unwrap_or_default())
            })
            .sum();
        let before_payloads: Vec<_> = reader
            .manifest
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.payload_file.clone(),
                    fs::read(path.join(&segment.payload_file)).unwrap(),
                )
            })
            .collect();
        let prepare = |reader: &SearchOutOfCoreReader| {
            SearchOutOfCoreGenerationWriter::prepare_delta(
                reader,
                crate::SearchProjectionDelta {
                    deletes: vec![deleted.id.clone(), "memory:missing".into()],
                    ..Default::default()
                },
                Default::default(),
            )
            .unwrap()
        };
        let update = prepare(&reader);
        let stale = prepare(&reader);
        assert_eq!(update.delta_report().deleted_documents, 1);
        assert_eq!(update.source_read_metrics().hydrated_documents, 0);
        assert_eq!(update.source_read_metrics().streamed_documents, 1);
        let (_, build, _) = update.finish().unwrap();
        assert_eq!(build.document_count, 1);
        assert_eq!(build.document_payload_bytes, 0);
        assert_eq!(build.lexical_artifact_bytes, 0);
        assert!(
            build.generation_bytes < corpus_artifact_bytes,
            "delete-only checkpoint rewrote the corpus: {} >= {}",
            build.generation_bytes,
            corpus_artifact_bytes
        );
        assert!(stale
            .finish()
            .unwrap_err()
            .to_string()
            .contains("base changed"));
        let reopened = mutation_reader_for_test(&path);
        assert_eq!(
            serde_json::to_vec(&reopened.manifest.segments).unwrap(),
            physical
        );
        assert_eq!(reopened.manifest.mutation_runs.len(), 2);
        assert_eq!(
            build.generation_bytes,
            build.manifest_bytes + reopened.manifest.mutation_runs[1].len
        );
        assert_eq!(
            reopened
                .hydrate_documents(std::slice::from_ref(&live.id))
                .unwrap()
                .documents,
            vec![live.clone()]
        );
        assert!(reopened
            .hydrate_documents(std::slice::from_ref(&deleted.id))
            .is_err());
        for (name, bytes) in before_payloads {
            assert_eq!(fs::read(path.join(name)).unwrap(), bytes);
        }
        let repeated = prepare(&reopened);
        assert_eq!(repeated.delta_report().deleted_documents, 0);
        assert_eq!(repeated.source_read_metrics().hydrated_documents, 0);
        let (_, repeated_build, _) = repeated.finish().unwrap();
        let final_reader = mutation_reader_for_test(&path);
        assert_eq!(final_reader.manifest.mutation_runs.len(), 2);
        assert_eq!(
            repeated_build.generation_bytes,
            repeated_build.manifest_bytes
        );
        assert_eq!(final_reader.document_count(), 1);
        assert_eq!(
            serde_json::to_vec(&final_reader.manifest.segments).unwrap(),
            physical
        );
        drop(final_reader);
        drop(reopened);
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_append_preserves_retractions_and_logical_identity() {
        let path = test_dir("mutation-append-preservation");
        let old = document(0, "team");
        let live = document(1, "team");
        publish_two_artifact_manifest(&path, old.clone(), live.clone());
        install_delete_mutation_run(&path, &old, 0, 3);
        let reader = mutation_reader_for_test(&path);
        let run = reader.manifest.mutation_runs[0].clone();
        let run_bytes = fs::read(path.join(&run.file)).unwrap();
        let appended = crate::SearchProjectionRow {
            kind: crate::SearchProjectionKind::Memory,
            external_id: "999".into(),
            title: "appended".into(),
            body: "graph memory".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        };
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                upserts: vec![appended.clone()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
        assert_eq!(update.source_read_metrics().hydrated_documents, 0);
        let (_, build, _) = update.finish().unwrap();
        assert_eq!(build.document_count, 2);
        let reopened = mutation_reader_for_test(&path);
        assert_eq!(reopened.manifest.mutation_runs.len(), 1);
        assert_eq!(reopened.manifest.mutation_runs[0].file, run.file);
        assert_eq!(fs::read(path.join(&run.file)).unwrap(), run_bytes);
        assert_eq!(reopened.manifest.embedding_dimension, Some(2));
        assert!(reopened
            .hydrate_documents(std::slice::from_ref(&old.id))
            .is_err());
        let ids = [live.id.clone(), "memory:999".into()];
        let output = reopened.hydrate_documents(&ids).unwrap();
        assert_eq!(output.documents, vec![live, appended.into_document()]);
        assert_eq!(reopened.document_count(), 2);
        assert!(!reopened.visibility.is_visible(0, &old.id));
        drop(reopened);
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn mutation_rabitq_budget_fallback_is_exact_observable_and_failure_specific() {
        let path = test_dir("mutation-rabitq-budget-fallback");
        let old = document(0, "team");
        let unchanged = document(1, "team");
        let mut replacement = old.clone();
        replacement.content = "updated graph memory".into();
        replacement.embedding = Some(vec![16.0, 1.0]);
        publish_two_artifact_manifest_with_initial_documents(
            &path,
            vec![old.clone(), unchanged],
            Default::default(),
            replacement,
            Default::default(),
        );
        install_edited_mutation_run(&path, &old, 0, 3, |entry| {
            entry.operation = mutation_run::SearchMutationOperation::Replace
        });
        let mut reader = mutation_reader_for_test(&path);
        reader.config.max_vector_candidates = NonZeroUsize::new(4).unwrap();
        // Enough for the retained headers and scalar scoring of two live IDs,
        // but only one byte remains for the ordinal allowlist plus its block.
        let budget = vector_serving::candidate_working_bytes(4).unwrap() + 1;
        reader.config.max_vector_search_working_bytes = NonZeroUsize::new(budget).unwrap();
        let search = |reader: &SearchOutOfCoreReader, mode| {
            reader.search_with_options_compressed_vector_projection_mode(
                "",
                Some(&[1.0, 16.0]),
                SearchMode::Vector,
                options(1, None),
                mode,
            )
        };
        let expected = search(&reader, CompressedVectorSearchMode::Disabled).unwrap();
        let error = search(&reader, CompressedVectorSearchMode::Required).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("candidate allowlist and block require"),
            "{error}"
        );
        let actual = search(&reader, CompressedVectorSearchMode::Preferred).unwrap();
        assert_search_parity(&expected.result, &actual.result);
        assert!(actual
            .result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorBudgetExceeded));

        let cancellation = crate::RuntimeCancellationToken::new();
        let task = crate::RuntimeTaskContext::without_deadline(cancellation.clone());
        assert!(cancellation.cancel());
        let error = reader
            .search_with_options_compressed_vector_projection_execution_options(
                "",
                Some(&[1.0, 16.0]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Preferred,
                VectorSearchExecutionOptions::bounded(NonZeroUsize::MIN, budget, Some(&task)),
            )
            .unwrap_err();
        assert!(error.to_string().contains("cancelled"), "{error}");

        reader.config.max_vector_search_working_bytes =
            SearchOutOfCoreConfig::default().max_vector_search_working_bytes;
        let mismatch = reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&[1.0]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Preferred,
            )
            .unwrap();
        assert!(mismatch
            .result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
        assert!(!mismatch
            .result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorBudgetExceeded));

        // Mapped artifacts are immutable for the reader's lifetime. Drop the
        // mapping before corrupting files, then verify open rejects corruption.
        let segments = reader.manifest.segments.clone();
        drop(reader);
        for segment in &segments {
            let artifact = segment.rabitq_artifact_file.as_ref().unwrap();
            fs::write(path.join(artifact), b"truncated projection").unwrap();
        }
        let error = SearchOutOfCoreReader::open(&path).unwrap_err();
        assert!(error.to_string().contains("RaBitQ artifact"), "{error}");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn mutation_rabitq_equal_scores_match_merged_logical_id_order() {
        let path = test_dir("mutation-rabitq-ties");
        let merged_path = test_dir("mutation-rabitq-ties-merged");
        let mut old = document(0, "team");
        old.embedding = Some(vec![1.0, 1.0]);
        let mut replacement = old.clone();
        replacement.content = "updated graph memory".into();
        let mut unchanged = document(1, "team");
        unchanged.embedding = replacement.embedding.clone();
        publish_two_artifact_manifest_with_initial_documents(
            &path,
            vec![old.clone(), unchanged.clone()],
            Default::default(),
            replacement.clone(),
            Default::default(),
        );
        install_edited_mutation_run(&path, &old, 0, 3, |entry| {
            entry.operation = mutation_run::SearchMutationOperation::Replace
        });
        let mut reader = mutation_reader_for_test(&path);
        reader.config.max_vector_candidates = NonZeroUsize::new(1).unwrap();
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &merged_path,
            SearchOutOfCoreGenerationBuildOptions {
                vector_backend: None,
                ..Default::default()
            },
        )
        .unwrap();
        writer.push(replacement).unwrap();
        writer.push(unchanged).unwrap();
        writer.finish().unwrap();
        let mut merged = SearchOutOfCoreReader::open(&merged_path).unwrap();
        merged.config.max_vector_candidates = NonZeroUsize::new(1).unwrap();
        let search = |reader: &SearchOutOfCoreReader| {
            reader
                .search_with_options_compressed_vector_projection_mode(
                    "",
                    Some(&[1.0, 1.0]),
                    SearchMode::Vector,
                    options(1, None),
                    CompressedVectorSearchMode::Required,
                )
                .unwrap()
        };
        let expected = search(&merged);
        let actual = search(&reader);
        assert_eq!(expected.result.hits[0].id, "memory:000");
        assert_search_parity(&expected.result, &actual.result);
        fs::remove_dir_all(path).unwrap();
        fs::remove_dir_all(merged_path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_visibility_matches_merged_reads_for_delete_and_replace() {
        for replace in [false, true] {
            let path = test_dir(&format!("mutation-visible-{replace}"));
            let merged_path = test_dir(&format!("mutation-merged-{replace}"));
            let old = document(0, "old-space");
            let mut live = if replace {
                old.clone()
            } else {
                document(1, "live-space")
            };
            live.title = "graph replacement".into();
            live.content = "memory current version".into();
            live.metadata.insert("space_id".into(), "live-space".into());
            live.embedding = Some(vec![16.0, 1.0]);
            publish_two_artifact_manifest(&path, old.clone(), live.clone());
            install_edited_mutation_run(&path, &old, 0, 3, |entry| {
                if replace {
                    entry.operation = mutation_run::SearchMutationOperation::Replace;
                }
            });
            let reader = SearchOutOfCoreReader::open(&path).unwrap();
            let mut merged = SearchOutOfCoreGenerationWriter::create(
                &merged_path,
                SearchOutOfCoreGenerationBuildOptions {
                    vector_backend: None,
                    ..Default::default()
                },
            )
            .unwrap();
            merged.push(live.clone()).unwrap();
            merged.finish().unwrap();
            let merged = SearchOutOfCoreReader::open(&merged_path).unwrap();

            assert_eq!(reader.document_count(), 1);
            assert_eq!(
                reader
                    .hydrate_documents(std::slice::from_ref(&live.id))
                    .unwrap()
                    .documents,
                vec![live.clone()]
            );
            if !replace {
                assert!(reader
                    .hydrate_documents(std::slice::from_ref(&old.id))
                    .is_err());
            }
            let mut modes = vec![SearchMode::Text];
            #[cfg(feature = "vector-search")]
            modes.extend([SearchMode::Vector, SearchMode::Hybrid]);
            for mode in modes.drain(..) {
                for filter in [None, Some("old-space"), Some("live-space")] {
                    let mut options = options(10, None);
                    if let Some(space) = filter {
                        options
                            .metadata_filters
                            .insert("space_id".into(), space.into());
                    }
                    let expected = merged
                        .search_with_options("graph", Some(&[1.0, 16.0]), mode, options.clone())
                        .unwrap();
                    let actual = reader
                        .search_with_options("graph", Some(&[1.0, 16.0]), mode, options)
                        .unwrap();
                    assert_search_parity(&expected.result, &actual.result);
                }
            }
            assert_eq!(
                reader
                    .search_with_options("storage", None, SearchMode::Text, options(10, None))
                    .unwrap()
                    .result
                    .total_hits,
                0
            );
            #[cfg(feature = "vector-search")]
            {
                let actual = reader
                    .search_with_options_compressed_vector_projection_mode(
                        "",
                        Some(&[1.0, 16.0]),
                        SearchMode::Vector,
                        options(10, None),
                        CompressedVectorSearchMode::Required,
                    )
                    .unwrap();
                let expected = merged
                    .search_with_options_compressed_vector_projection_mode(
                        "",
                        Some(&[1.0, 16.0]),
                        SearchMode::Vector,
                        options(10, None),
                        CompressedVectorSearchMode::Required,
                    )
                    .unwrap();
                assert_search_parity(&expected.result, &actual.result);
            }
            fs::remove_dir_all(path).unwrap();
            fs::remove_dir_all(merged_path).unwrap();
        }
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_compaction_absorbs_a_complete_target_closure() {
        let path = test_dir("mutation-compaction-guard");
        let old = document(0, "old-space");
        let live = document(1, "live-space");
        publish_two_artifact_manifest(&path, old.clone(), live.clone());
        install_delete_mutation_run(&path, &old, 0, 3);
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let policy =
            crate::out_of_core::generation_writer::SearchOutOfCoreSegmentCompactionPolicy::default(
            );
        assert!(
            SearchOutOfCoreGenerationWriter::segment_compaction_work_plan(
                &reader,
                policy,
                hawdb_qos::BackgroundWorkHint::default(),
            )
            .unwrap()
            .is_some()
        );
        let report =
            SearchOutOfCoreGenerationWriter::compact_segments(&reader, policy, Default::default())
                .unwrap()
                .unwrap();
        assert_eq!(report.build().document_count, 1);
        drop(reader);
        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert!(reopened.manifest.mutation_runs.is_empty());
        assert_eq!(reopened.manifest.segments.len(), 1);
        assert_eq!(
            reopened
                .hydrate_documents(std::slice::from_ref(&live.id))
                .unwrap()
                .documents,
            vec![live]
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_compaction_rewrites_entries_outside_the_selected_range() {
        let path = test_dir("mutation-compaction-partial-closure");
        let first = document(0, "first-space");
        let live = document(1, "live-space");
        let outside = document(2, "outside-space");
        publish_two_artifact_manifest(&path, first.clone(), live.clone());

        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let outside_row = crate::SearchProjectionRow {
            kind: crate::SearchProjectionKind::Memory,
            external_id: "002".into(),
            title: outside.title.clone(),
            body: outside.content.clone(),
            embedding: outside.embedding.clone(),
            source_id: None,
            metadata: outside.metadata.clone(),
        };
        let outside = outside_row.clone().into_document();
        SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                upserts: vec![outside_row],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap()
        .finish()
        .unwrap();
        drop(reader);

        install_delete_mutation_runs(&path, &[(&first, 0), (&outside, 2)], 3);
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let policy =
            crate::out_of_core::generation_writer::SearchOutOfCoreSegmentCompactionPolicy::default(
            );
        let report =
            SearchOutOfCoreGenerationWriter::compact_segments(&reader, policy, Default::default())
                .unwrap()
                .unwrap();
        assert_eq!(report.build().document_count, 1);
        drop(reader);

        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reopened.document_count(), 1);
        assert_eq!(reopened.manifest.mutation_runs.len(), 1);
        assert_eq!(reopened.manifest.mutation_runs[0].entry_count, 1);
        assert!(reopened
            .hydrate_documents(std::slice::from_ref(&live.id))
            .unwrap()
            .documents
            .contains(&live));
        assert!(reopened
            .hydrate_documents(std::slice::from_ref(&outside.id))
            .is_err());
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_compaction_publication_failure_preserves_the_active_closure() {
        let path = test_dir("mutation-compaction-publication-failure");
        let old = document(0, "old-space");
        let live = document(1, "live-space");
        publish_two_artifact_manifest(&path, old.clone(), live.clone());
        install_delete_mutation_run(&path, &old, 0, 3);
        let before = fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let error = SearchOutOfCoreGenerationWriter::compact_segments(
            &reader,
            SearchOutOfCoreSegmentCompactionPolicy::default(),
            SearchOutOfCoreGenerationBuildOptions {
                max_generation_bytes: NonZeroU64::MIN,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("published bytes"), "{error}");
        assert_eq!(
            fs::read(path.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        drop(reader);
        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reopened.document_count(), 1);
        assert!(reopened.manifest.mutation_runs.len() == 1);
        assert!(fs::read_dir(&path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".search-generation.")));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn mutation_sustained_replacements_append_and_compaction_reopen_each_round() {
        let path = test_dir("mutation-sustained-rounds");
        let mut initial = SearchOutOfCoreGenerationWriter::create(
            &path,
            SearchOutOfCoreGenerationBuildOptions {
                ..Default::default()
            },
        )
        .unwrap();
        initial.push(document(0, "team")).unwrap();
        initial.finish().unwrap();
        for number in 1..32 {
            let reader = SearchOutOfCoreReader::open(&path).unwrap();
            let mut appended = document(number % 16, "team");
            appended.id = format!("memory:{number:03}");
            appended
                .metadata
                .insert("external_id".to_string(), number.to_string());
            let row = crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: format!("{number:03}"),
                title: appended.title.clone(),
                body: appended.content.clone(),
                embedding: appended.embedding.clone(),
                source_id: None,
                metadata: appended.metadata.clone(),
            };
            SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                crate::SearchProjectionDelta {
                    upserts: vec![row],
                    ..Default::default()
                },
                Default::default(),
            )
            .unwrap()
            .finish()
            .unwrap();
        }
        let initial_reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert!(
            initial_reader.manifest.segments.len() >= 2,
            "the sustained fixture must contain multiple immutable segments"
        );
        drop(initial_reader);

        let policy = SearchOutOfCoreSegmentCompactionPolicy::default()
            .with_level_zero_target_bytes(NonZeroU64::new(8 * 1024).unwrap())
            .unwrap();
        let mut compaction_count = 0;
        for round in 0..4 {
            let reader = SearchOutOfCoreReader::open(&path).unwrap();
            let mut replaced = document(round, "replacement-space");
            replaced
                .metadata
                .insert("external_id".to_string(), format!("{round:03}"));
            let mut appended = document(0, "append-space");
            appended.id = format!("memory:{:03}", 100 + round);
            appended
                .metadata
                .insert("external_id".to_string(), (100 + round).to_string());
            let replacement_row = crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: format!("{round:03}"),
                title: replaced.title.clone(),
                body: replaced.content.clone(),
                embedding: replaced.embedding.clone(),
                source_id: None,
                metadata: replaced.metadata.clone(),
            };
            let appended_row = crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: format!("{:03}", 100 + round),
                title: appended.title.clone(),
                body: appended.content.clone(),
                embedding: appended.embedding.clone(),
                source_id: None,
                metadata: appended.metadata.clone(),
            };
            SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                crate::SearchProjectionDelta {
                    upserts: vec![replacement_row, appended_row],
                    ..Default::default()
                },
                Default::default(),
            )
            .unwrap()
            .finish()
            .unwrap();
            drop(reader);

            let reader = SearchOutOfCoreReader::open(&path).unwrap();
            let compaction = SearchOutOfCoreGenerationWriter::compact_segments(
                &reader,
                policy,
                Default::default(),
            )
            .unwrap();
            if let Some(report) = &compaction {
                compaction_count += 1;
                assert!(report.source_segment_count() >= 2);
                assert!(report.source_bytes() > 0);
                assert!(report.build().document_count > 0);
            }
            drop(reader);

            let reopened = SearchOutOfCoreReader::open(&path).unwrap();
            assert_eq!(reopened.document_count(), 33 + round);
            assert_eq!(
                reopened
                    .hydrate_documents(&[replaced.id.clone()])
                    .unwrap()
                    .documents,
                vec![replaced]
            );
            assert_eq!(
                reopened
                    .hydrate_documents(&[appended.id.clone()])
                    .unwrap()
                    .documents,
                vec![appended]
            );
            drop(reopened);
        }
        assert!(
            compaction_count > 0,
            "sustained fixture must compact at least once"
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_mutation_retractions_must_match_physical_targets() {
        for corruption in [
            "missing_id",
            "id_gap",
            "other_id",
            "digest",
            "length",
            "missing_term",
            "extra_term",
        ] {
            let path = test_dir(&format!("mutation-retraction-{corruption}"));
            let previous = document(0, "team");
            let mut index = SearchIndex::open(&path).unwrap();
            index.upsert(previous.clone()).unwrap();
            index.upsert(document(2, "team")).unwrap();
            index.checkpoint().unwrap();
            install_edited_mutation_run(&path, &previous, 0, 2, |entry| match corruption {
                "missing_id" => entry.document_id = "memory:missing".into(),
                "id_gap" => entry.document_id = "memory:001".into(),
                "other_id" => entry.document_id = "memory:002".into(),
                "digest" => entry.retraction.documents_digest ^= 1,
                "length" => entry.retraction.lexical_document_len += 1,
                "missing_term" => {
                    entry.retraction.unique_terms.owned_mut().pop().unwrap();
                }
                "extra_term" => {
                    entry
                        .retraction
                        .unique_terms
                        .owned_mut()
                        .push("zzzzzz".into());
                }
                _ => unreachable!(),
            });
            let error = load_artifact_closure(
                &path,
                &SearchOutOfCoreConfig::default(),
                &SearchAnalyzerLexicon::default(),
                SearchLexicalTermPolicy::default(),
                SearchLexicalSourcePolicy::default(),
            )
            .unwrap_err();
            let expected = if matches!(corruption, "missing_id" | "id_gap") {
                "target document is absent"
            } else {
                "retraction does not match its target document"
            };
            assert!(
                error.to_string().contains(expected),
                "{corruption}: {error}"
            );
            let cleanup = index.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
            assert!(cleanup.retry_required, "{corruption}");
            assert!(cleanup.generation_discovery_failures > 0, "{corruption}");
            assert!(path.join(mutation_run::artifact_file(2)).is_file());
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_mutation_retraction_binds_same_id_to_exact_content_version() {
        for target in [0, 1] {
            let path = test_dir(&format!("mutation-target-version-{target}"));
            let old = document(0, "team");
            let mut replacement = old.clone();
            replacement.content = "a different replacement version".into();
            publish_two_artifact_manifest(&path, old.clone(), replacement);
            install_delete_mutation_run(&path, &old, target, 3);
            let result = load_artifact_closure(
                &path,
                &SearchOutOfCoreConfig::default(),
                &SearchAnalyzerLexicon::default(),
                SearchLexicalTermPolicy::default(),
                SearchLexicalSourcePolicy::default(),
            );
            if target == 0 {
                assert_eq!(result.unwrap().0.document_count, 1);
            } else {
                let error = result.unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("retraction does not match its target document"),
                    "{error}"
                );
            }
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn out_of_core_mutation_retraction_uses_weighted_analyzer_contributions() {
        for stopwords in [false, true] {
            let path = test_dir(&format!("mutation-analyzer-contribution-{stopwords}"));
            let analyzer = if stopwords {
                SearchAnalyzerLexicon::empty().with_stopwords(["memory"])
            } else {
                SearchAnalyzerLexicon::empty()
            };
            let document = SearchDocument {
                id: "memory:contribution".into(),
                title: "graph graph".into(),
                content: "memory graph".into(),
                embedding: None,
                metadata: BTreeMap::new(),
            };
            let mut writer = SearchOutOfCoreGenerationWriter::create(
                &path,
                SearchOutOfCoreGenerationBuildOptions {
                    analyzer_lexicon: analyzer.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            writer.push(document.clone()).unwrap();
            writer.finish().unwrap();
            let (_, segments, _) = load_artifact_closure(
                &path,
                &SearchOutOfCoreConfig::default(),
                &analyzer,
                SearchLexicalTermPolicy::default(),
                SearchLexicalSourcePolicy::default(),
            )
            .unwrap();
            let retraction = mutation_run::SearchMutationRetraction::from_document(
                &document,
                &segments[0].lexical_projection,
                &analyzer,
            )
            .unwrap();
            // Title: graph twice plus graph_graph, each weighted by two.
            // Body: memory, graph and memory_graph, each weighted by one.
            // Removing memory leaves the distinct memory_graph phrase intact.
            assert_eq!(
                retraction.lexical_document_len,
                if stopwords { 8 } else { 9 }
            );
            assert_eq!(
                retraction.unique_terms.to_vec(),
                if stopwords {
                    vec!["graph", "graph_graph", "memory_graph"]
                } else {
                    vec!["graph", "graph_graph", "memory", "memory_graph"]
                }
            );
            assert_eq!(
                retraction.documents_digest,
                lexical_documents_digest(&BTreeMap::from([(document.id.clone(), document)]))
            );
            fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn out_of_core_mutation_working_budget_is_enforced_only_for_mutation_closures() {
        let path = test_dir("mutation-working-budget");
        let document = document(0, "team");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document.clone()).unwrap();
        index.checkpoint().unwrap();
        let config = SearchOutOfCoreConfig {
            max_mutation_working_bytes: NonZeroU64::MIN,
            ..Default::default()
        };
        SearchOutOfCoreReader::open_with_config(&path, config.clone()).unwrap();
        install_delete_mutation_run(&path, &document, 0, 2);
        let error = SearchOutOfCoreReader::open_with_config(&path, config).unwrap_err();
        assert!(
            error.to_string().contains("mutation-run working set"),
            "{error}"
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_mutation_target_validation_uses_reanalysis_budget() {
        let path = test_dir("mutation-target-hydration-budget");
        let document = document(0, "team");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document.clone()).unwrap();
        index.checkpoint().unwrap();
        install_delete_mutation_run(&path, &document, 0, 2);
        SearchOutOfCoreReader::open_with_config(
            &path,
            SearchOutOfCoreConfig {
                max_hydrated_bytes: NonZeroU64::MIN,
                ..Default::default()
            },
        )
        .unwrap();
        let error = load_artifact_closure(
            &path,
            &SearchOutOfCoreConfig {
                max_reanalysis_working_bytes: NonZeroU64::MIN,
                ..SearchOutOfCoreConfig::default()
            },
            &SearchAnalyzerLexicon::default(),
            SearchLexicalTermPolicy::default(),
            SearchLexicalSourcePolicy::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("memory"), "{error}");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_reader_rejects_mutation_runs_targeting_inactive_segments() {
        let path = test_dir("mutation-run-inactive-target");
        let document = document(0, "team");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document.clone()).unwrap();
        index.checkpoint().unwrap();
        install_delete_mutation_run(&path, &document, 99, 2);

        let error = SearchOutOfCoreReader::open(&path).unwrap_err();
        assert!(error
            .to_string()
            .contains("targets a segment outside the active manifest"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn published_artifact_generations_track_every_manifest_artifact() {
        let path = test_dir("published-artifact-generations");
        publish_two_artifact_manifest(&path, document(0, "team"), document(1, "team"));

        let generations = published_artifact_generations(&path, &SearchAnalyzerLexicon::default())
            .unwrap()
            .unwrap();
        assert_eq!(generations.active_generation, 2);
        assert_eq!(generations.lexical_generations, BTreeSet::from([1, 2]));
        assert_eq!(generations.out_of_core_generations, BTreeSet::from([1, 2]));
        #[cfg(feature = "vector-search")]
        assert_eq!(generations.rabitq_generations, BTreeSet::from([1, 2]));
        #[cfg(not(feature = "vector-search"))]
        assert!(generations.rabitq_generations.is_empty());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_generation_update_retains_every_ordered_manifest_artifact() {
        let path = test_dir("multi-segment-generation-update");
        publish_two_artifact_manifest(&path, document(0, "team"), document(1, "team"));
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let appended = crate::SearchProjectionRow {
            kind: crate::SearchProjectionKind::Memory,
            external_id: "002".to_string(),
            title: "added graph document".to_string(),
            body: "storage memory document 2".to_string(),
            embedding: Some(vec![3.0, 14.0]),
            source_id: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        };
        let expected_appended = appended.clone().into_document();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                upserts: vec![appended],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
        assert_eq!(update.delta_report().before_document_count, 2);
        assert_eq!(update.delta_report().action, "incremental_segment_append");
        assert_eq!(update.source_read_metrics().hydrated_documents, 0);
        assert_eq!(update.source_read_metrics().segment_range_reads, 0);
        let (_, build, _) = update.finish().unwrap();
        assert_eq!(build.document_count, 3);

        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reader.manifest.segments.len(), 3);
        assert_eq!(
            reader
                .hydrate_documents(&[
                    "memory:000".to_string(),
                    "memory:001".to_string(),
                    "memory:002".to_string(),
                ])
                .unwrap()
                .documents,
            vec![document(0, "team"), document(1, "team"), expected_appended],
        );
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_multi_segment_metadata_filter_reads_each_artifact() {
        let path = test_dir("multi-segment-metadata-filter");
        publish_two_artifact_manifest(&path, document(0, "team"), document(1, "team"));

        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let mut filtered = options(10, None);
        filtered
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());
        let output = reader
            .search_with_options("graph", None, SearchMode::Text, filtered)
            .unwrap();
        assert_eq!(output.result.filtered_document_count, 2);
        assert_eq!(output.result.total_hits, 2);
        assert_eq!(
            output
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["memory:000", "memory:001"]),
        );
        assert_eq!(
            output
                .result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(output.metrics.candidate_block_reads, 2);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn out_of_core_multi_segment_vector_and_hybrid_query_all_artifacts() {
        let path = test_dir("multi-segment-vector");
        publish_two_artifact_manifest(&path, document(0, "team"), document(1, "team"));
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let query_embedding = [1.0, 16.0];
        let mut filtered = options(10, None);
        filtered
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());

        let vector = reader
            .search_with_options(
                "",
                Some(&query_embedding),
                SearchMode::Vector,
                filtered.clone(),
            )
            .unwrap();
        assert_eq!(vector.result.filtered_document_count, 2);
        assert_eq!(vector.result.total_hits, 2);
        assert_eq!(
            vector
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["memory:000", "memory:001"]),
        );
        assert_eq!(vector.metrics.candidate_block_reads, 2);
        assert!(vector.metrics.vector_segment_bytes_read > 0);

        let limited = reader
            .search_with_options("", Some(&[16.0, 1.0]), SearchMode::Vector, options(1, None))
            .unwrap();
        assert_eq!(limited.result.total_hits, 2);
        assert_eq!(limited.result.hits.len(), 1);
        assert_eq!(limited.result.hits[0].id, "memory:001");

        let hybrid = reader
            .search_with_options(
                "graph",
                Some(&query_embedding),
                SearchMode::Hybrid,
                filtered.clone(),
            )
            .unwrap();
        assert_eq!(hybrid.result.filtered_document_count, 2);
        assert_eq!(hybrid.result.total_hits, 2);
        assert!(hybrid.metrics.vector_segment_bytes_read > 0);

        let preferred = reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&query_embedding),
                SearchMode::Vector,
                filtered.clone(),
                CompressedVectorSearchMode::Preferred,
            )
            .unwrap();
        assert_eq!(preferred.result.total_hits, 2);
        assert_eq!(
            preferred.result.retrievers[0].backend,
            "hawdb_rabitq_out_of_core_candidate_projection"
        );
        assert!(!preferred
            .result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorProjectionUnavailable));
        assert_eq!(preferred.metrics.candidate_block_reads, 2);
        assert!(preferred.metrics.rabitq_payload_bytes_read > 0);
        let required = reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&query_embedding),
                SearchMode::Vector,
                filtered,
                CompressedVectorSearchMode::Required,
            )
            .unwrap();
        assert_eq!(required.result.total_hits, 2);
        assert_eq!(
            required
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["memory:000", "memory:001"]),
        );
        assert_eq!(required.metrics.candidate_block_reads, 2);
        assert!(required.metrics.rabitq_payload_bytes_read > 0);

        let limited_config = SearchOutOfCoreConfig {
            max_vector_candidates: std::num::NonZeroUsize::new(1).unwrap(),
            ..SearchOutOfCoreConfig::default()
        };
        let limited_reader =
            SearchOutOfCoreReader::open_with_config(&path, limited_config).unwrap();
        let limited = limited_reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&[16.0, 1.0]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Required,
            )
            .unwrap();
        assert_eq!(limited.result.total_hits, 1);
        assert_eq!(limited.result.hits.len(), 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn out_of_core_multi_segment_rabitq_rejects_mixed_quantization() {
        let path = test_dir("multi-segment-rabitq-mixed-quantization");
        publish_two_artifact_manifest_with_options(
            &path,
            document(0, "team"),
            SearchOutOfCoreGenerationBuildOptions {
                rabitq_transform_seed: 1,
                ..Default::default()
            },
            document(1, "team"),
            SearchOutOfCoreGenerationBuildOptions {
                rabitq_transform_seed: 2,
                ..Default::default()
            },
        );
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let error = reader
            .search_with_options_compressed_vector_projection_mode(
                "",
                Some(&[1.0, 16.0]),
                SearchMode::Vector,
                options(1, None),
                CompressedVectorSearchMode::Required,
            )
            .unwrap_err();
        assert!(error.to_string().contains("matching backend parameters"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn out_of_core_sidecar_corruption_fails_closed_in_the_consuming_stage() {
        let metadata_path = test_dir("metadata-corruption");
        let mut metadata_index = SearchIndex::open(&metadata_path).unwrap();
        metadata_index.upsert(document(0, "team")).unwrap();
        metadata_index.checkpoint().unwrap();
        let metadata_reader = SearchOutOfCoreReader::open(&metadata_path).unwrap();
        corrupt_first_byte(
            &metadata_path.join(
                &metadata_reader
                    .manifest
                    .segments
                    .first()
                    .expect("manifest has a segment")
                    .metadata_payload_file,
            ),
        );
        let mut filtered = options(0, None);
        filtered
            .metadata_filters
            .insert("space_id".to_string(), "team".to_string());
        let error = metadata_reader
            .search_with_options("graph", None, SearchMode::Text, filtered)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("metadata payload checksum mismatch"));
        fs::remove_dir_all(metadata_path).unwrap();

        let vector_path = test_dir("vector-corruption");
        let mut vector_index = SearchIndex::open(&vector_path).unwrap();
        vector_index.upsert(document(0, "team")).unwrap();
        vector_index.checkpoint().unwrap();
        let vector_reader = SearchOutOfCoreReader::open(&vector_path).unwrap();
        corrupt_first_byte(
            &vector_path.join(
                &vector_reader
                    .manifest
                    .segments
                    .first()
                    .expect("manifest has a segment")
                    .vector_payload_file,
            ),
        );
        let error = vector_reader
            .search_with_options("", Some(&[1.0, 0.5]), SearchMode::Vector, options(0, None))
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("vector payload checksum mismatch"));
        fs::remove_dir_all(vector_path).unwrap();
    }

    #[test]
    fn out_of_core_reader_pins_immutable_generation_across_checkpoint() {
        let path = test_dir("generation");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let old_reader = SearchOutOfCoreReader::open(&path).unwrap();

        index.upsert(document(1, "team")).unwrap();
        index.checkpoint().unwrap();
        let new_reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(old_reader.document_count(), 1);
        assert_eq!(new_reader.document_count(), 2);
        assert_eq!(
            old_reader
                .hydrate_documents(&[document(0, "team").id])
                .unwrap()
                .documents,
            vec![document(0, "team")]
        );
        assert!(old_reader
            .hydrate_documents(&[document(1, "team").id])
            .is_err());
        assert_eq!(
            new_reader
                .hydrate_documents(&[document(1, "team").id])
                .unwrap()
                .documents,
            vec![document(1, "team")]
        );
        #[cfg(feature = "full-text-search")]
        {
            let old = old_reader
                .search_with_options("graph", None, SearchMode::Text, options(10, None))
                .unwrap();
            let new = new_reader
                .search_with_options("graph", None, SearchMode::Text, options(10, None))
                .unwrap();
            assert_eq!(old.result.total_hits, 1);
            assert_eq!(new.result.total_hits, 2);
        }
        drop(old_reader);
        drop(new_reader);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn out_of_core_reader_survives_generation_retention_cleanup() {
        let path = test_dir("generation-retention");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let oldest_reader = SearchOutOfCoreReader::open(&path).unwrap();

        index.upsert(document(1, "team")).unwrap();
        index.checkpoint().unwrap();
        index.upsert(document(2, "team")).unwrap();
        index.checkpoint().unwrap();

        assert_eq!(oldest_reader.document_count(), 1);
        assert_eq!(
            oldest_reader
                .hydrate_documents(&[document(0, "team").id])
                .unwrap()
                .documents,
            vec![document(0, "team")]
        );
        assert!(oldest_reader
            .hydrate_documents(&[document(2, "team").id])
            .is_err());
        let newest_reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(newest_reader.document_count(), 3);
        assert_eq!(
            newest_reader
                .hydrate_documents(&[document(2, "team").id])
                .unwrap()
                .documents,
            vec![document(2, "team")]
        );
        drop(newest_reader);
        #[cfg(feature = "full-text-search")]
        {
            let oldest = oldest_reader
                .search_with_options("graph", None, SearchMode::Text, options(10, None))
                .unwrap();
            let newest = SearchOutOfCoreReader::open(&path)
                .unwrap()
                .search_with_options("graph", None, SearchMode::Text, options(10, None))
                .unwrap();
            assert_eq!(oldest.result.total_hits, 1);
            assert_eq!(newest.result.total_hits, 3);
        }
        drop(oldest_reader);
        let cleanup = index.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
        assert!(!cleanup.retry_required, "{cleanup:?}");
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn out_of_core_large_payload_only_retains_one_segment_and_final_page() {
        let path = test_dir("large-payload");
        let mut index = SearchIndex::open(&path).unwrap();
        let content = format!("graph {}", "x ".repeat(64 * 1024));
        for number in 0..8 {
            let mut document = document(number, "team");
            document.content = content.clone();
            index.upsert(document).unwrap();
        }
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let output = reader
            .search_with_options("graph", None, SearchMode::Text, options(1, None))
            .unwrap();
        assert_eq!(reader.resident_document_count(), 0);
        assert_eq!(output.metrics.hydrated_documents, 1);
        assert!(output.metrics.hydrated_bytes < 256 * 1024);
        assert!(output.metrics.peak_segment_document_bytes < 512 * 1024);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn nowledge_out_of_core_facade_uses_the_bounded_reader() {
        let path = test_dir("nowledge-facade");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let projection = SearchOutOfCoreReader::open(&path).unwrap();
        let output = projection
            .search_with_options("graph", None, SearchMode::Text, options(1, None))
            .unwrap();
        assert_eq!(output.result.hits[0].id, "memory:000");
        assert_eq!(output.metrics.hydrated_documents, 1);
        assert_eq!(projection.resident_document_count(), 0);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn production_facade_rejects_unqualified_or_stale_lexical_evidence() {
        let path = test_dir("nowledge-production-qualification");
        let mut index = SearchIndex::open(&path).unwrap();
        index.upsert(document(0, "team")).unwrap();
        index.checkpoint().unwrap();
        let qualification = SearchLexicalProductionQualificationReport::evaluate(
            999,
            Some(999),
            100_000,
            true,
            SearchLexicalFeasibilityCoverage::default(),
            SearchLexicalFeasibilityMetrics::default(),
        );

        let projection = SearchOutOfCoreReader::open(&path).unwrap();
        let error = qualification
            .validate_for_projection_and_release(
                &projection.production_qualification_identity(),
                &production_identity(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("projection_identity_mismatch"));
        assert!(error.to_string().contains("workload_coverage_incomplete"));
        fs::remove_dir_all(path).unwrap();
    }

    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn corrupt_first_byte(path: &Path) {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
    }
}
