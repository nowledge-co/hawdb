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

mod projection_scan;
pub use projection_scan::{SearchProjectionScanBudget, SearchProjectionScanReport};
mod projection_id_page;
pub use projection_id_page::{
    SearchProjectionIdBatch, SearchProjectionIdPage, SearchProjectionPageGeneration,
};
#[doc(hidden)]
pub mod candidate_evidence;
#[doc(hidden)]
pub mod candidate_evidence_cli;
use hawdb_core::{
    Catalog, HawDBError, ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, Result,
    RuntimeCapabilities, RuntimeCapability, Value,
};
pub use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
pub use hawdb_evidence::{
    ProductionEvidenceBinding, ProductionQualificationIdentity,
    PRODUCTION_QUALIFICATION_POLICY_VERSION,
};
use hawdb_integrity::checksum_u64;
use hawdb_optimizer_predicate::{
    normalize_search_enum_value, search_field_is_enum_like, SearchPredicate, SearchPredicateOp,
    SearchPredicateSet, SearchScalarValue,
};
use hawdb_optimizer_vector::{
    select_adaptive_vector_backend, AdaptiveVectorBackend, AdaptiveVectorBackendDecision,
    AdaptiveVectorBackendInput, AdaptiveVectorBackendPolicy, VectorCompressionPreference,
};
use hawdb_plan_core::{VectorBackendSelectionReason, VectorCandidateSource};
use hawdb_qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    WorkClass, WorkRequest,
};
use hawdb_storage::file_io::{self as fs, File};
use hawdb_storage::{
    durability::durable_replace_file,
    scan::{
        EnumDictionaryStats, FieldSummary, RangeBound, ScanPredicate, SegmentPruner,
        SegmentReadRange, SegmentSummary,
    },
};
use hawdb_storage::{NodeId, NodeRecord};
use hawdb_telemetry::{KernelTelemetry, KernelTelemetryOperation, TelemetrySink};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Cursor, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

mod analyzer_lexicon;
mod analyzer_memory;
mod analyzer_stream;
mod analyzer_workspace;
mod bounded_file;
mod build_control;
mod build_memory;
mod build_term;
mod cjk_dictionary;
mod cjk_tokenizer;
#[cfg(test)]
mod compression_tests;
/// Internal assembly support for the embedded database facade.
#[doc(hidden)]
pub mod consumer;
#[cfg(feature = "vector-search")]
mod diskann_identity;
#[cfg(test)]
mod document_decoding_tests;
mod document_encoding;
mod document_set_digest;
pub use document_set_digest::SearchDocumentSetDigest;
mod generation_cleanup;
mod identifier;
#[doc(hidden)]
pub mod knowledge_retrieval_pipeline;
mod lexical_projection;
#[cfg(test)]
mod resident_checkpoint_atomicity_tests;
pub use lexical_projection::LexicalQueryTermPolicy;
mod lexical_readiness;
#[cfg(all(test, feature = "full-text-search"))]
mod lexical_snapshot_test_gate;
mod lexical_source_policy;
mod lexical_term_policy;
mod out_of_core;
mod projection_catch_up;
mod projection_delta_contracts;
#[doc(hidden)]
pub mod projection_evidence;
#[doc(hidden)]
pub mod projection_evidence_cli;
pub mod projection_ownership;
mod query_control;
mod query_matches;
mod resident_bm25;
#[cfg(feature = "vector-search")]
mod resident_diskann;
mod resident_text_seed;

#[doc(hidden)]
pub mod projection_consumer;
#[cfg(feature = "vector-search")]
pub mod rabitq_projection;
mod range_io;
mod resident_documents;
mod resident_snapshot;
use resident_documents::{DocumentView, ResidentDocuments};
pub use resident_snapshot::{
    SearchIndexPublishedSnapshot, SearchIndexReadLease, SearchIndexReadSnapshot,
};
mod recall_validation;
mod snapshot_writer;
#[cfg(test)]
#[path = "../tests/support/live_allocation.rs"]
mod test_allocation;
mod vector_execution;
mod vector_index;
mod vector_scoring;
pub use vector_index::{
    VectorIndexHit, VectorIndexQuery, VectorIndexQueryReport, VectorIndexRetainedOutput,
};
pub use vector_scoring::SearchVectorScoring;

use document_encoding::encode_search_document_line;

mod error {
    pub use hawdb_core::{HawDBError, Result};
}

/// Storage-neutral source for graph-derived search projection maintenance.
///
/// The search kernel owns projection semantics; embedding code owns graph
/// layout, scan, recovery, and relationship access. This is deliberately
/// analogous to `hawdb_analytics::ProjectionSource`.
pub trait SearchProjectionSource {
    fn source_graph_commit_epoch(&self) -> u64;

    fn estimated_projection_node_count(&self) -> usize;

    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> Result<()>,
    ) -> Result<()>;

    fn projection_business_labels(
        &self,
        catalog: &Catalog,
        node: &NodeRecord,
    ) -> Result<Vec<String>>;

    /// Collects sorted, deduplicated business labels keyed by graph node.
    ///
    /// Sources with bulk relationship access should override this method so a
    /// rebuild does not repeat the same relationship scan for every node.
    fn projection_business_labels_by_node(
        &self,
        catalog: &Catalog,
    ) -> Result<HashMap<NodeId, Vec<String>>> {
        let mut labels_by_node = HashMap::new();
        self.visit_projection_nodes(&mut |node| {
            if projection_row_from_node(catalog, &node).is_none() {
                return Ok(());
            }
            let mut labels = self.projection_business_labels(catalog, &node)?;
            labels.sort();
            labels.dedup();
            if !labels.is_empty() {
                labels_by_node.insert(node.id, labels);
            }
            Ok(())
        })?;
        Ok(labels_by_node)
    }
}

pub const fn compiled_runtime_capabilities() -> RuntimeCapabilities {
    RuntimeCapabilities {
        access_control: cfg!(feature = "acl"),
        full_text_search: cfg!(feature = "full-text-search"),
        search_projection_publication: cfg!(any(unix, windows)),
        vector_search: cfg!(feature = "vector-search"),
        graph_analytics: cfg!(feature = "graph-analytics"),
        background_maintenance: cfg!(feature = "background-maintenance"),
    }
}

mod compiled_capabilities {
    use hawdb_core::RuntimeCapabilities;

    pub(crate) const fn effective_runtime_capabilities(
        requested: RuntimeCapabilities,
    ) -> RuntimeCapabilities {
        requested.intersection(super::compiled_runtime_capabilities())
    }
}

#[cfg(test)]
mod fallible_wrapper_proof;
use analyzer_lexicon::{CORE_SEMANTIC_ALIAS_RULES, NOWLEDGE_MEMORY_SEMANTIC_ALIAS_RULES};
use analyzer_stream::{document_token_fields, visit_token_list, TokenOccurrence};
#[cfg(test)]
use cjk_tokenizer::is_cjk_search_char;
use generation_cleanup::SearchProjectionCleanupState;
pub use generation_cleanup::{
    SearchProjectionCleanupOptions, SearchProjectionCleanupOutcome, SearchProjectionCleanupReport,
    SEARCH_PROJECTION_CLEANUP_PROTOCOL,
};
pub use knowledge_retrieval_pipeline::{KnowledgeRetrievalPipelineReport, KnowledgeRetrievalStage};
use lexical_projection::{
    analyzer_digest as lexical_analyzer_digest, documents_digest as lexical_documents_digest,
    LexicalMiniDelta, LexicalProjectionConfig, LexicalProjectionReader, LexicalProjectionWriter,
};
pub use lexical_readiness::{
    SearchLexicalFeasibilityCoverage, SearchLexicalFeasibilityMetrics,
    SearchLexicalProductionQualificationReport, SearchProjectionQualificationIdentity,
    SearchTopKScoreParity, SEARCH_LEXICAL_QUALIFICATION_PROTOCOL,
    SEARCH_LEXICAL_QUALIFICATION_PROTOCOL_VERSION,
};
pub use lexical_source_policy::SearchLexicalSourcePolicy;
pub use lexical_term_policy::SearchLexicalTermPolicy;
pub use out_of_core::{
    GovernedSearchGenerationUpdate, GovernedSearchGenerationWriter, PreparedSearchPublication,
    ScheduledSearchOutOfCoreSegmentCompactionReport, SearchAdmittedConsumerDocuments,
    SearchBaselineComparisonLimits, SearchBaselineComparisonReport, SearchBodyReadOptions,
    SearchComparedConsumerDocuments, SearchContentVerificationReport, SearchGenerationAdmission,
    SearchMatchedBaseline, SearchMutationInspectionInput, SearchMutationInspectionStep,
    SearchMutationSortStep, SearchMutationSortStepLimits, SearchMutationSortStepUsage,
    SearchOutOfCoreBaselineComparison, SearchOutOfCoreCandidate, SearchOutOfCoreConfig,
    SearchOutOfCoreDeltaComparison, SearchOutOfCoreDocumentLookup, SearchOutOfCoreExecutionContext,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationBuildReport,
    SearchOutOfCoreGenerationUpdate, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreHydrationOutput, SearchOutOfCoreMetrics, SearchOutOfCoreMutationWriter,
    SearchOutOfCoreOutput, SearchOutOfCorePreparedGeneration, SearchOutOfCorePreparedUpdate,
    SearchOutOfCoreReader, SearchOutOfCoreRefreshReport, SearchOutOfCoreSegmentCompaction,
    SearchOutOfCoreSegmentCompactionPolicy, SearchOutOfCoreSegmentCompactionReport,
    SearchOutOfCoreSegmentCompactionStopReason, SearchOutOfCoreUnorderedMutationWriter,
    SearchOutOfCoreUnorderedReplacementWriter, SearchProjectionEpochAdvanceReport,
    SearchQueryError, SearchQueryResult, SearchRetainedOutput, SearchStagingCleanupReport,
    SearchTextSeedOutput, SearchTextSeedReport, SearchUnorderedMutationLimits,
    SearchUnorderedMutationReport, SearchVerifiedBody,
};
pub use resident_text_seed::SearchResidentTextSeedOutput;
// These are internal ownership seams. Hosts continue to use the embedded facade.
#[doc(hidden)]
pub use projection_catch_up::{
    run_scheduled_search_projection_catch_up, run_search_projection_catch_up,
    scheduled_search_projection_catch_up_report, search_projection_catch_up_report,
    search_projection_durable_epoch, start_search_projection_background_work,
    validate_search_projection_catch_up_request,
};
pub use projection_catch_up::{
    ScheduledSearchProjectionCatchUpReport, SearchProjectionCatchUpReport,
    SearchProjectionCatchUpStopReason,
};
#[doc(hidden)]
pub use projection_delta_contracts::{
    SearchProjectionChangeBatch, SearchProjectionGraphDeltaRequest, SearchProjectionRelationalDelta,
};
pub use range_io::SearchRangeReadConfig;
use recall_validation::{sample_positions, VectorRecallValidationAccumulator};
pub use recall_validation::{
    VectorProjectionQualificationIdentity, VectorProjectionResourceEvidence,
    VectorRecallProductionQualificationReport, VectorRecallValidationBlocker,
    VectorRecallValidationOptions, VectorRecallValidationReport,
    MAX_VECTOR_RECALL_VALIDATION_CANDIDATE_LIMIT, MAX_VECTOR_RECALL_VALIDATION_SAMPLES,
    MAX_VECTOR_RECALL_VALIDATION_TOP_K, MINIMUM_VECTOR_QUALIFICATION_DOCUMENT_COUNT,
    VECTOR_RECALL_PRODUCTION_QUALIFICATION_PROTOCOL, VECTOR_RECALL_VALIDATION_PROTOCOL,
};
pub use snapshot_writer::SearchCheckpointReport;
use vector_execution::{execute_search_vector_plan, SearchVectorExecutionRequest};

#[cfg(feature = "vector-search")]
use rabitq_projection::{RaBitQCandidateProjection, RaBitQCandidateProjectionBuildOptions};

const SEARCH_SNAPSHOT_FILE: &str = "search_projection.hawdb";
const SEARCH_SEGMENT_DESCRIPTOR_FILE: &str = "search_projection_segments.hawdb";
const SEARCH_SEGMENT_PAYLOAD_FILE: &str = "search_projection_segment_payloads.hawdb";
const SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID: u64 = 1;

/// Generation-named resident snapshot. An unregistered resident checkpoint
/// writes this before its selector; `search_projection.hawdb` is then a hard
/// link to the generation the out-of-core manifest selects.
fn resident_snapshot_file(generation: u64) -> String {
    format!("search_projection.{generation}.hawdb")
}

const RABITQ_CANDIDATE_BACKEND: &str = "hawdb_rabitq_candidate_projection";
const DISKANN_CANDIDATE_BACKEND: &str = "hawdb_diskann_candidate_projection";

#[derive(Clone, Copy)]
enum VectorRecallProbeBackend {
    Default,
    Compressed,
}

impl VectorRecallProbeBackend {
    fn name(self) -> &'static str {
        match self {
            Self::Default => DISKANN_CANDIDATE_BACKEND,
            Self::Compressed => RABITQ_CANDIDATE_BACKEND,
        }
    }
}
static QUARANTINE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "vector-search")]
const SEARCH_RABITQ_PROJECTION_PREFIX: &str = "search_rabitq.";
#[cfg(feature = "vector-search")]
const SEARCH_RABITQ_PROJECTION_SUFFIX: &str = ".hawdb";
pub const FULL_REINDEX_MARKER: &str = ".reindex_needed";
pub const METADATA_REPAIR_MARKER: &str = ".projection_metadata_repair_needed";
/// Records the projection's externally owned kinds (#1814). Kept beside the
/// selector instead of inside it so earlier readers, which refuse unknown
/// manifest fields, still open the projection. See [`projection_ownership`].
pub const EXTERNALLY_OWNED_KINDS_MARKER: &str = projection_ownership::MARKER;
/// Records whether the host must resupply externally owned rows (#1814).
pub const EXTERNALLY_OWNED_ROWS_MARKER: &str = projection_ownership::ROWS_MARKER;
/// The BM25 constants shared with the SQL search index access path.
use hawdb_executor::bm25::{BM25_B, BM25_K1};
const TITLE_TERM_FREQUENCY_WEIGHT: usize = 2;
const RRF_K: f64 = 60.0;
const SEARCH_COMPRESSION_HEADER: &str = "HAWDB_COMPRESSED_V1";
const SEARCH_COMPRESSION_LEVEL: i32 = 3;
const SEARCH_DOCUMENT_ID_FIELD: &str = "document_id";
pub use hawdb_evidence::replacement_contract::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS;

/// Nested Memory metadata paths with stable scalar/list semantics that are
/// materialized into the search projection. Unknown paths remain residual
/// predicates instead of expanding every segment descriptor for arbitrary
/// application metadata.
pub const NOWLEDGE_MEMORY_MATERIALIZED_METADATA_PATHS: &[&str] = &[
    "state",
    "topic",
    "customer.tier",
    "purpose",
    "project_id",
    "agent_id",
    "host_agent_id",
    "source_app",
];
#[cfg(not(test))]
const SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS: usize = 128;
#[cfg(test)]
const SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchDocument {
    pub id: String,
    pub title: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: BTreeMap<String, String>,
}

/// Bounded resident fields of a document whose body has separate ownership.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchDocumentHeader {
    pub id: String,
    pub title: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: BTreeMap<String, String>,
}

/// Identity checks for a one-shot UTF-8 body supplied by the host.
///
/// The host must pin its canonical source snapshot through capture. CRC32c
/// detects accidental changes; it is not cryptographic source authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchDocumentBody {
    pub bytes: u64,
    pub expected_checksum: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SearchProjectionKind {
    Memory,
    Message,
    Entity,
    Source,
    SourceChunk,
    Community,
}

impl SearchProjectionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchProjectionKind::Memory => "memory",
            SearchProjectionKind::Message => "message",
            SearchProjectionKind::Entity => "entity",
            SearchProjectionKind::Source => "source",
            SearchProjectionKind::SourceChunk => "source_chunk",
            SearchProjectionKind::Community => "community",
        }
    }

    /// Parses a name produced by [`Self::as_str`].
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name {
            "memory" => SearchProjectionKind::Memory,
            "message" => SearchProjectionKind::Message,
            "entity" => SearchProjectionKind::Entity,
            "source" => SearchProjectionKind::Source,
            "source_chunk" => SearchProjectionKind::SourceChunk,
            "community" => SearchProjectionKind::Community,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchProjectionRow {
    pub kind: SearchProjectionKind,
    pub external_id: String,
    pub title: String,
    pub body: String,
    pub embedding: Option<Vec<f32>>,
    pub source_id: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchEmbeddingManifest {
    pub model: String,
    pub version: Option<String>,
    pub dimension: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionFreshness {
    pub document_count: usize,
    /// Immutable provenance of a completed external bootstrap import. This is
    /// not a local changefeed cursor and must never drive local catch-up.
    pub import_source_graph_commit_epoch: Option<u64>,
    pub source_graph_commit_epoch: Option<u64>,
    pub durable_source_graph_commit_epoch: Option<u64>,
    pub has_uncheckpointed_changes: bool,
    pub full_reindex_needed: bool,
    pub full_reindex_reasons: Vec<String>,
    pub metadata_repair_needed: bool,
    pub metadata_repair_reasons: Vec<String>,
    pub embedding_model: Option<String>,
    pub embedding_version: Option<String>,
    pub embedding_dimension: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchProjectionProbeOptions {
    pub active_embedding_model: Option<String>,
    pub active_embedding_dimension: Option<usize>,
}

impl SearchProjectionRow {
    pub fn into_document(self) -> SearchDocument {
        let kind = self.kind.as_str();
        let mut metadata = self.metadata;
        metadata.insert("kind".to_string(), kind.to_string());
        metadata.insert("external_id".to_string(), self.external_id.clone());
        if let Some(source_id) = self.source_id {
            metadata.insert("source_id".to_string(), source_id);
        }
        SearchDocument {
            id: format!("{kind}:{}", self.external_id),
            title: self.title,
            content: self.body,
            embedding: self.embedding,
            metadata,
        }
    }
}

pub fn search_projection_document_id_for_node(
    catalog: &Catalog,
    node: &NodeRecord,
) -> Option<String> {
    hawdb_storage::projection::projection_document_id_for_node(catalog, node)
}

pub fn search_projection_document_id_for_label_and_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    node_id: NodeId,
) -> Option<String> {
    hawdb_storage::projection::projection_document_id_for_label_and_properties(
        label, properties, node_id,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Hybrid,
    Vector,
    Text,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    pub id: String,
    pub score: f64,
    /// Vector relevance in `[0, 1]`, higher is better. The scale depends on
    /// the projection's metric: positive cosine similarity for cosine, and
    /// `1 / (1 + l2_distance)` for Euclidean, which is often far below 1 for
    /// unnormalized embeddings. Compare scores only within one metric; a
    /// relevance floor tuned on cosine does not carry over to Euclidean.
    /// `SearchVectorScoring::LinearSquaredL2` instead exposes
    /// `max(0, 1 - squared_l2 / 2)` after selecting neighbors by distance.
    pub vector_score: f64,
    pub text_score: f64,
    pub rrf_score: f64,
    pub vector_rrf_score: f64,
    pub text_rrf_score: f64,
    pub vector_rank: Option<usize>,
    pub text_rank: Option<usize>,
    pub kind: Option<String>,
    pub external_id: Option<String>,
    pub source_id: Option<String>,
    pub matched_terms: Vec<String>,
    pub matched_spans: Vec<SearchMatchedSpan>,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub projection_freshness: SearchProjectionFreshness,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMatchedSpan {
    pub field: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub text: String,
    pub term: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResultSet<H = SearchHit> {
    pub hits: Vec<H>,
    pub total_hits: usize,
    pub limit: usize,
    pub offset: usize,
    pub truncated: bool,
    pub truncation_reason_codes: Vec<SearchTruncationReasonCode>,
    pub truncation_reasons: Vec<String>,
    pub empty_reason_codes: Vec<SearchEmptyReasonCode>,
    pub empty_reasons: Vec<String>,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub retrievers: Vec<SearchRetrieverReport>,
    pub candidate_set: SearchCandidateSetReport,
    pub rank_window: Option<usize>,
    pub fusion_weights: SearchFusionWeights,
    pub document_count: usize,
    pub filtered_document_count: usize,
    pub projection_freshness: SearchProjectionFreshness,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchScoredCandidate {
    pub id: String,
    pub score: f64,
    pub vector_score: f64,
    pub text_score: f64,
    pub rrf_score: f64,
    pub vector_rrf_score: f64,
    pub text_rrf_score: f64,
    pub vector_rank: Option<usize>,
    pub text_rank: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchTruncationReasonCode {
    LimitExceeded,
}

impl SearchTruncationReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchTruncationReasonCode::LimitExceeded => "limit_exceeded",
        }
    }
}

impl FromStr for SearchTruncationReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "limit_exceeded" => Ok(SearchTruncationReasonCode::LimitExceeded),
            _ => Err("unknown search truncation reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchFallbackReasonCode {
    VectorDimensionMismatch,
    VectorIndexEmpty,
    CompressedVectorProjectionUnavailable,
    CompressedVectorBudgetExceeded,
    DiskAnnCacheBudgetExceeded,
    QueryEmbeddingMissing,
    TextQueryEmpty,
    /// The analyzed text query exceeded the scored-term cap; only its most
    /// selective (lowest document frequency) terms were scored.
    TextQueryTermsReduced,
}

impl SearchFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchFallbackReasonCode::VectorDimensionMismatch => "vector_dimension_mismatch",
            SearchFallbackReasonCode::VectorIndexEmpty => "vector_index_empty",
            SearchFallbackReasonCode::CompressedVectorProjectionUnavailable => {
                "compressed_vector_projection_unavailable"
            }
            SearchFallbackReasonCode::CompressedVectorBudgetExceeded => {
                "compressed_vector_budget_exceeded"
            }
            SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded => "diskann_cache_budget_exceeded",
            SearchFallbackReasonCode::QueryEmbeddingMissing => "query_embedding_missing",
            SearchFallbackReasonCode::TextQueryEmpty => "text_query_empty",
            SearchFallbackReasonCode::TextQueryTermsReduced => "text_query_terms_reduced",
        }
    }
}

impl FromStr for SearchFallbackReasonCode {
    type Err = &'static str;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "vector_dimension_mismatch" => Ok(SearchFallbackReasonCode::VectorDimensionMismatch),
            "vector_index_empty" => Ok(SearchFallbackReasonCode::VectorIndexEmpty),
            "compressed_vector_projection_unavailable" => {
                Ok(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable)
            }
            "compressed_vector_budget_exceeded" => {
                Ok(SearchFallbackReasonCode::CompressedVectorBudgetExceeded)
            }
            "diskann_cache_budget_exceeded" => {
                Ok(SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded)
            }
            "query_embedding_missing" => Ok(SearchFallbackReasonCode::QueryEmbeddingMissing),
            "text_query_empty" => Ok(SearchFallbackReasonCode::TextQueryEmpty),
            "text_query_terms_reduced" => Ok(SearchFallbackReasonCode::TextQueryTermsReduced),
            _ => Err("unknown search fallback reason code"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchEmptyReasonCode {
    ProjectionEmpty,
    MetadataFilterEmpty,
    RetrieverNoHits,
    LimitExcludedAllHits,
}

impl SearchEmptyReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProjectionEmpty => "projection_empty",
            Self::MetadataFilterEmpty => "metadata_filter_empty",
            Self::RetrieverNoHits => "retriever_no_hits",
            Self::LimitExcludedAllHits => "limit_excluded_all_hits",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchCandidateSetReport {
    pub id_space: String,
    pub representation: String,
    pub cardinality: usize,
    pub exact: bool,
    pub snapshot_source_graph_commit_epoch: Option<u64>,
    pub policy_epoch: Option<u64>,
    pub filtered_out_count: usize,
    pub metadata_filters: BTreeMap<String, String>,
    pub metadata_predicate_pushdown: SearchPredicatePushdownReport,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchPredicatePushdownReport {
    pub input_predicate_count: usize,
    pub pushed_predicate_count: usize,
    pub residual_predicate_count: usize,
    pub unsatisfiable: bool,
    pub parse_error: Option<String>,
    pub segment_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub segment_pruning_candidate_document_count: usize,
    pub segment_pruned_document_count: usize,
    pub segment_scanned_document_count: usize,
    pub persisted_segment_descriptor_used: bool,
    pub physical_range_read_count: usize,
    pub physical_bytes_read: u64,
    /// Scanned segments whose candidates came from metadata value postings
    /// instead of decoding their metadata records.
    pub postings_segment_count: usize,
    /// Equality/IN predicates compiled into the segment postings plan.
    pub postings_predicate_count: usize,
    /// Predicates left for metadata evaluation after a postings prefilter.
    pub postings_residual_predicate_count: usize,
    /// Scanned segments where postings narrowed the metadata records evaluated
    /// by residual predicates.
    pub postings_prefiltered_segment_count: usize,
    pub field_summaries: Vec<SearchPredicateFieldPruningReport>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchPredicateFieldPruningReport {
    pub field: String,
    pub value_kind: String,
    pub operation_kinds: Vec<String>,
    pub segment_count: usize,
    pub pruned_segment_count: usize,
    pub scanned_segment_count: usize,
    pub numeric_range_summary_used: bool,
    pub timestamp_range_summary_used: bool,
    pub value_summary_used: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRetrieverReport {
    pub name: String,
    pub backend: String,
    pub backend_selection_reason: Option<VectorBackendSelectionReason>,
    pub estimated_raw_vector_bytes: Option<u64>,
    pub filter_selectivity_per_million: Option<u32>,
    pub available: bool,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_score_source: String,
    pub final_score_source: String,
    pub generated_candidate_count: usize,
    pub candidate_scan_rounds: usize,
    pub descriptor_pruned_count: usize,
    pub scalar_filtered_count: usize,
    pub residual_filtered_count: usize,
    pub reranked_candidate_count: usize,
    pub raw_vector_bytes_read: u64,
    pub candidate_scan_kernel: Option<String>,
    pub candidate_scan_worker_count: usize,
    pub candidate_scan_segment_count: usize,
    pub candidate_scan_scanned_segment_count: usize,
    pub candidate_scan_scored_document_count: usize,
    pub candidate_scan_filtered_document_count: usize,
    pub candidate_scan_scanned_block_count: usize,
    pub candidate_scan_skipped_block_count: usize,
    pub candidate_scan_adaptive_exact_scan_count: usize,
    pub candidate_scan_payload_bytes_read: u64,
    pub candidate_scan_admitted_working_bytes: usize,
    pub posting_bytes_read: u64,
    pub candidate_postings_visited: u64,
    /// Posting blocks whose frame-header bound could not reach the retained
    /// floor, so their payloads were never decoded.
    pub candidate_blocks_skipped: u64,
    pub segmented_lexical_projection_used: bool,
    pub index_covered_document_count: usize,
    pub index_candidate_document_count: usize,
    pub index_coverage_complete: bool,
    pub candidate_count: usize,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
    pub candidate_top_ids: Vec<String>,
    pub top_hit_ids: Vec<String>,
    pub top_candidates: Vec<SearchRetrieverCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRetrieverCandidateSetReport {
    pub id_space: String,
    pub representation: String,
    pub cardinality: usize,
    pub exact: bool,
    pub snapshot_source_graph_commit_epoch: Option<u64>,
    pub policy_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRetrieverCandidate {
    pub id: String,
    pub rank: usize,
    pub score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchFusionWeights {
    pub vector_weight: f64,
    pub text_weight: f64,
}

impl Default for SearchFusionWeights {
    fn default() -> Self {
        Self {
            vector_weight: 1.0,
            text_weight: 1.0,
        }
    }
}

/// How much of each ranked out-of-core hit is materialized after scoring.
///
/// Ranking, filtering, totals and reports are identical for every level.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SearchHitHydration {
    /// Hydrate the metadata fields, matched terms/spans and the vector score
    /// recomputed from the stored embedding. Vector-only requests without
    /// query terms need no matched spans, so they read the metadata and vector
    /// sidecars instead of the full document payload; the hits are identical.
    #[default]
    Documents,
    /// Hydrate the metadata fields and the vector score recomputed from the
    /// stored embedding, without matched terms/spans. Hits read only their
    /// metadata sidecar segments and their own binary vector rows, never the
    /// document payload, for every search mode. The hits equal `Documents`
    /// hits except that `matched_terms` and `matched_spans` are empty.
    Metadata,
    /// Return identifiers and ranking scores without reading any document,
    /// metadata or vector payload. `kind`, `external_id` and `source_id` are
    /// `None`, matched terms/spans are empty, and `vector_score` is the vector
    /// retriever's ranking score (0 for hits it did not rank).
    ScoresOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchQueryOptions {
    pub limit: usize,
    pub offset: usize,
    pub rank_window: Option<usize>,
    pub fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub policy_epoch: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAccessControlContext {
    pub policy_epoch: u64,
    pub visibility_metadata_field: String,
    pub allowed_visibility_values: BTreeSet<String>,
}

impl SearchAccessControlContext {
    pub fn visibility_scopes(
        policy_epoch: u64,
        visibility_metadata_field: impl Into<String>,
        allowed_visibility_values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            policy_epoch,
            visibility_metadata_field: visibility_metadata_field.into(),
            allowed_visibility_values: allowed_visibility_values
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }

    /// Refuses a context the caller built incorrectly. These are caller
    /// mistakes, not storage faults, so they are `Semantic` like the core
    /// `QueryAccessControlContext::validate` (#1767).
    fn validate(&self) -> Result<()> {
        if self.policy_epoch == 0 {
            return Err(HawDBError::Semantic(
                "access control context requires a non-zero policy epoch".to_string(),
            ));
        }
        if self.visibility_metadata_field.trim().is_empty() {
            return Err(HawDBError::Semantic(
                "access control context requires a visibility metadata field".to_string(),
            ));
        }
        if self.allowed_visibility_values.is_empty() {
            return Err(HawDBError::Semantic(
                "access control context requires at least one visibility value".to_string(),
            ));
        }
        if self
            .allowed_visibility_values
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(HawDBError::Semantic(
                "access control context visibility values must be non-empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Refuses search options whose own policy epoch disagrees with this
    /// context. Both the resident and the out-of-core reader call this.
    fn check_options_policy_epoch(&self, policy_epoch: Option<u64>) -> Result<()> {
        match policy_epoch {
            Some(policy_epoch) if policy_epoch != self.policy_epoch => {
                Err(HawDBError::Semantic(format!(
                    "search options policy epoch {policy_epoch} does not match access control policy epoch {}",
                    self.policy_epoch
                )))
            }
            _ => Ok(()),
        }
    }

    /// Requests each caller check refuses: `(options policy epoch, context)`.
    /// Shared by the resident and out-of-core reader tests (#1767).
    #[cfg(all(test, feature = "acl", feature = "full-text-search"))]
    pub(crate) fn invalid_requests_for_test() -> Vec<(Option<u64>, Self)> {
        vec![
            (Some(8), Self::visibility_scopes(7, "space_id", ["team"])),
            (None, Self::visibility_scopes(0, "space_id", ["team"])),
            (None, Self::visibility_scopes(7, " ", ["team"])),
            (
                None,
                Self::visibility_scopes(7, "space_id", Vec::<String>::new()),
            ),
            (None, Self::visibility_scopes(7, "space_id", ["team", " "])),
        ]
    }

    /// The metadata filters a query runs with under this context: `options`'
    /// filters plus the visibility predicate, after both caller checks.
    pub(crate) fn effective_query_filters(
        &self,
        options: &SearchQueryOptions,
    ) -> Result<BTreeMap<String, String>> {
        self.check_options_policy_epoch(options.policy_epoch)?;
        self.effective_metadata_filters(&options.metadata_filters)
    }

    pub fn effective_metadata_filters(
        &self,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        self.validate()?;
        let mut filters = metadata_filters.clone();
        if self.allowed_visibility_values.len() == 1 {
            filters.insert(
                self.visibility_metadata_field.clone(),
                self.allowed_visibility_values
                    .iter()
                    .next()
                    .expect("single visibility value")
                    .clone(),
            );
        } else {
            filters.insert(
                format!("{}__in", self.visibility_metadata_field),
                serde_json::to_string(
                    &self
                        .allowed_visibility_values
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| {
                    HawDBError::or_stop(&error, || {
                        HawDBError::Storage(format!(
                            "failed to encode access control visibility predicate: {error}"
                        ))
                    })
                })?,
            );
        }
        Ok(filters)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressedVectorSearchMode {
    Disabled,
    Preferred,
    Required,
}

impl CompressedVectorSearchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Preferred => "preferred",
            Self::Required => "required",
        }
    }
}

pub const DEFAULT_VECTOR_SEARCH_WORKING_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VectorSearchKernelPreference {
    #[default]
    Auto,
    Scalar,
    Avx2,
    Neon,
}

impl VectorSearchKernelPreference {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Neon => "neon",
        }
    }

    #[cfg(feature = "vector-search")]
    pub(crate) fn projection_preference(self) -> hawdb_vector_projection::KernelPreference {
        match self {
            Self::Auto => hawdb_vector_projection::KernelPreference::Auto,
            Self::Scalar => hawdb_vector_projection::KernelPreference::Scalar,
            Self::Avx2 => hawdb_vector_projection::KernelPreference::Avx2,
            Self::Neon => hawdb_vector_projection::KernelPreference::Neon,
        }
    }
}

/// Default DiskANN navigation beam for resident and canonical index serving.
pub const DEFAULT_NAVIGATION_BEAM: usize = 1024;

/// Default beam for ordinary out-of-core Search queries. Canonical index
/// serving keeps `DEFAULT_NAVIGATION_BEAM` and its single-graph recall budget.
pub const DEFAULT_OUT_OF_CORE_NAVIGATION_BEAM: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct VectorSearchExecutionOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: VectorSearchKernelPreference,
    pub task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    capture_candidate_ids: bool,
    exact: bool,
    navigation_beam: Option<NonZeroUsize>,
    candidate_window: Option<NonZeroUsize>,
}

impl<'a> VectorSearchExecutionOptions<'a> {
    pub fn bounded(
        max_parallelism: NonZeroUsize,
        max_working_bytes: usize,
        task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            max_parallelism,
            max_working_bytes,
            kernel: VectorSearchKernelPreference::Auto,
            task_context,
            capture_candidate_ids: false,
            exact: false,
            navigation_beam: None,
            candidate_window: None,
        }
    }

    pub fn admitted(
        max_working_bytes: usize,
        task_context: &'a hawdb_core::RuntimeTaskContext,
    ) -> Self {
        Self::bounded(
            task_context.admitted_parallelism(),
            max_working_bytes,
            Some(task_context),
        )
    }

    pub fn with_kernel(mut self, kernel: VectorSearchKernelPreference) -> Self {
        self.kernel = kernel;
        self
    }

    /// Bypass approximate graph candidates and score the complete eligible set.
    pub fn with_exact_search(mut self) -> Self {
        self.exact = true;
        self
    }

    pub fn capture_candidates_for_validation(mut self) -> Self {
        self.capture_candidate_ids = true;
        self
    }

    /// DiskANN navigation beam for this query, never below the
    /// requested result count. A smaller beam trades recall for speed. The
    /// traversal stays bounded by `max_working_bytes`, and a beam whose
    /// workspace does not fit is refused, not truncated.
    /// Ordinary out-of-core Search inherits its reader's default (256);
    /// resident and canonical index serving retain the generic default (1024).
    pub fn with_navigation_beam(mut self, beam: NonZeroUsize) -> Self {
        self.navigation_beam = Some(beam);
        self
    }

    /// Approximate candidates generated and reranked for this query: the
    /// RaBitQ rerank depth and DiskANN candidate count. Without it, the window
    /// is `max(limit, rank_window, 128)` (out of core, at most
    /// `max_vector_candidates`). The window never goes below the limit or rank
    /// window. A window below the default 128 does less work but can lower
    /// recall, because fewer approximate candidates reach the exact rerank.
    /// Out of core, a window above the reader's `max_vector_candidates` is
    /// refused; resident search stays bounded by its working memory.
    pub fn with_candidate_window(mut self, window: NonZeroUsize) -> Self {
        self.candidate_window = Some(window);
        self
    }

    #[cfg(feature = "vector-search")]
    pub(crate) fn navigation_beam(&self, top_k: usize) -> usize {
        top_k.max(self.navigation_beam.map_or(
            crate::vector_execution::DISKANN_NAVIGATION_BEAM,
            NonZeroUsize::get,
        ))
    }

    #[cfg(feature = "vector-search")]
    pub(crate) fn candidate_window(&self) -> Option<usize> {
        self.candidate_window.map(NonZeroUsize::get)
    }

    /// The resident DiskANN candidate floor: the per-query window, or the
    /// default navigation window.
    #[cfg(feature = "vector-search")]
    pub(crate) fn resident_candidate_window(&self) -> usize {
        self.candidate_window()
            .unwrap_or(hawdb_vector_projection::diskann::DiskAnnSearchOptions::default().beam_width)
    }
}

impl Default for VectorSearchExecutionOptions<'_> {
    fn default() -> Self {
        Self {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_VECTOR_SEARCH_WORKING_BYTES,
            kernel: VectorSearchKernelPreference::Auto,
            task_context: None,
            capture_candidate_ids: false,
            exact: false,
            navigation_beam: None,
            candidate_window: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveVectorSearchOptions {
    pub compression_mode: CompressedVectorSearchMode,
    pub backend_policy: AdaptiveVectorBackendPolicy,
    pub recall_validation_probe: bool,
}

impl AdaptiveVectorSearchOptions {
    pub fn new(compression_mode: CompressedVectorSearchMode) -> Self {
        Self {
            compression_mode,
            backend_policy: AdaptiveVectorBackendPolicy::default(),
            recall_validation_probe: false,
        }
    }

    pub fn with_backend_policy(mut self, policy: AdaptiveVectorBackendPolicy) -> Self {
        self.backend_policy = policy;
        self
    }

    pub fn as_recall_validation_probe(mut self) -> Self {
        self.recall_validation_probe = true;
        self
    }
}

fn vector_compression_preference(mode: CompressedVectorSearchMode) -> VectorCompressionPreference {
    match mode {
        CompressedVectorSearchMode::Disabled => VectorCompressionPreference::Disabled,
        CompressedVectorSearchMode::Preferred => VectorCompressionPreference::Preferred,
        CompressedVectorSearchMode::Required => VectorCompressionPreference::Required,
    }
}

fn adaptive_vector_fallback_reason(decision: AdaptiveVectorBackendDecision) -> Option<String> {
    match decision.reason {
        VectorBackendSelectionReason::QuantizedProjectionUnavailable => {
            Some(if decision.backend == AdaptiveVectorBackend::RequiredProjectionUnavailable {
                "compressed vector projection required but unavailable"
            } else {
                "compressed vector projection unavailable; fell back to scalar vector scan"
            }
            .to_string())
        }
        VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete => {
            Some(if decision.backend == AdaptiveVectorBackend::RequiredProjectionUnavailable {
                "compressed vector projection required but candidate coverage is incomplete"
            } else {
                "compressed vector projection candidate coverage is incomplete; fell back to scalar vector scan"
            }
            .to_string())
        }
        _ => None,
    }
}

fn recall_validation_hit_ids(
    result: &SearchResultSet,
    sampled_document_id: &str,
    top_k: usize,
) -> Vec<String> {
    result
        .hits
        .iter()
        .filter(|hit| hit.id != sampled_document_id)
        .take(top_k)
        .map(|hit| hit.id.clone())
        .collect()
}

fn recall_validation_candidate_ids(
    candidate_ids: &[String],
    sampled_document_id: &str,
    candidate_limit: usize,
) -> Vec<String> {
    candidate_ids
        .iter()
        .filter(|id| id.as_str() != sampled_document_id)
        .take(candidate_limit)
        .cloned()
        .collect()
}

#[derive(Clone, Copy)]
enum VectorSearchBackend<'a> {
    Scalar,
    #[cfg(feature = "vector-search")]
    DiskAnn {
        index: &'a SearchIndex,
    },
    CompressedRequiredUnavailable,
    #[cfg(not(feature = "vector-search"))]
    _Lifetime(std::marker::PhantomData<&'a ()>),
    #[cfg(feature = "vector-search")]
    RaBitQ {
        projection: &'a RaBitQCandidateProjection,
        required: bool,
    },
    #[cfg(feature = "qualification")]
    ExternalValidation {
        candidates: &'a [(String, f64)],
        indexed_document_ids: &'a BTreeSet<String>,
    },
}

#[derive(Clone, Copy)]
enum SearchPayloadAccess {
    InMemory,
    PrunedRanges,
}

struct SearchExecutionStrategy<'a> {
    vector_backend: VectorSearchBackendRequest<'a>,
    vector_backend_fallback_reason: Option<String>,
    payload_access: SearchPayloadAccess,
    capture_candidate_ids: bool,
    vector_execution_options: VectorSearchExecutionOptions<'a>,
}

#[derive(Clone, Copy)]
struct AdaptiveVectorExecutionControls<'a> {
    use_physical_range_reads: bool,
    capture_candidate_ids: bool,
    vector_execution_options: VectorSearchExecutionOptions<'a>,
}

impl<'a> AdaptiveVectorExecutionControls<'a> {
    const IN_MEMORY: Self = Self {
        use_physical_range_reads: false,
        capture_candidate_ids: false,
        vector_execution_options: VectorSearchExecutionOptions {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_VECTOR_SEARCH_WORKING_BYTES,
            kernel: VectorSearchKernelPreference::Auto,
            task_context: None,
            capture_candidate_ids: false,
            exact: false,
            navigation_beam: None,
            candidate_window: None,
        },
    };
    const PERSISTED: Self = Self {
        use_physical_range_reads: true,
        capture_candidate_ids: false,
        vector_execution_options: VectorSearchExecutionOptions {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_VECTOR_SEARCH_WORKING_BYTES,
            kernel: VectorSearchKernelPreference::Auto,
            task_context: None,
            capture_candidate_ids: false,
            exact: false,
            navigation_beam: None,
            candidate_window: None,
        },
    };
    const RECALL_CANDIDATES: Self = Self {
        use_physical_range_reads: false,
        capture_candidate_ids: true,
        vector_execution_options: VectorSearchExecutionOptions {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_VECTOR_SEARCH_WORKING_BYTES,
            kernel: VectorSearchKernelPreference::Auto,
            task_context: None,
            capture_candidate_ids: true,
            exact: false,
            navigation_beam: None,
            candidate_window: None,
        },
    };

    fn persisted(vector_execution_options: VectorSearchExecutionOptions<'a>) -> Self {
        Self {
            use_physical_range_reads: true,
            capture_candidate_ids: vector_execution_options.capture_candidate_ids,
            vector_execution_options,
        }
    }
}

#[derive(Clone, Copy)]
enum VectorSearchBackendRequest<'a> {
    Default,
    Fixed(VectorSearchBackend<'a>),
    Adaptive(AdaptiveVectorSearchRequest),
}

#[derive(Clone, Copy)]
struct AdaptiveVectorSearchRequest {
    compression_preference: VectorCompressionPreference,
    policy: AdaptiveVectorBackendPolicy,
    recall_validation_probe: bool,
}

struct PreparedAdaptiveVectorBackend {
    decision: AdaptiveVectorBackendDecision,
    fallback_reason: Option<String>,
    #[cfg(feature = "vector-search")]
    projection: Option<Arc<RaBitQCandidateProjection>>,
}

impl<'a> SearchExecutionStrategy<'a> {
    fn default_vector(
        use_physical_range_reads: bool,
        options: VectorSearchExecutionOptions<'a>,
    ) -> Self {
        let mut strategy = Self::fixed(VectorSearchBackend::Scalar, None, use_physical_range_reads);
        strategy.vector_backend = VectorSearchBackendRequest::Default;
        strategy.capture_candidate_ids = options.capture_candidate_ids;
        strategy.vector_execution_options = options;
        strategy
    }

    fn fixed(
        vector_backend: VectorSearchBackend<'a>,
        vector_backend_fallback_reason: Option<String>,
        use_physical_range_reads: bool,
    ) -> Self {
        Self {
            vector_backend: VectorSearchBackendRequest::Fixed(vector_backend),
            vector_backend_fallback_reason,
            payload_access: if use_physical_range_reads {
                SearchPayloadAccess::PrunedRanges
            } else {
                SearchPayloadAccess::InMemory
            },
            capture_candidate_ids: false,
            vector_execution_options: VectorSearchExecutionOptions::default(),
        }
    }

    fn adaptive(
        compression_preference: VectorCompressionPreference,
        policy: AdaptiveVectorBackendPolicy,
        recall_validation_probe: bool,
        controls: AdaptiveVectorExecutionControls<'a>,
    ) -> Self {
        Self {
            vector_backend: VectorSearchBackendRequest::Adaptive(AdaptiveVectorSearchRequest {
                compression_preference,
                policy,
                recall_validation_probe,
            }),
            vector_backend_fallback_reason: None,
            payload_access: if controls.use_physical_range_reads {
                SearchPayloadAccess::PrunedRanges
            } else {
                SearchPayloadAccess::InMemory
            },
            capture_candidate_ids: controls.capture_candidate_ids,
            vector_execution_options: controls.vector_execution_options,
        }
    }
}

impl PreparedAdaptiveVectorBackend {
    fn backend(&self) -> VectorSearchBackend<'_> {
        match self.decision.backend {
            AdaptiveVectorBackend::ScalarFlat => VectorSearchBackend::Scalar,
            AdaptiveVectorBackend::RequiredProjectionUnavailable => {
                VectorSearchBackend::CompressedRequiredUnavailable
            }
            AdaptiveVectorBackend::QuantizedProjection => {
                #[cfg(feature = "vector-search")]
                {
                    VectorSearchBackend::RaBitQ {
                        projection: self
                            .projection
                            .as_ref()
                            .expect("quantized backend requires a loaded projection"),
                        required: self.decision.reason
                            == VectorBackendSelectionReason::QuantizedRequired,
                    }
                }
                #[cfg(not(feature = "vector-search"))]
                {
                    unreachable!("quantized backend is unavailable without vector-search")
                }
            }
        }
    }
}

impl VectorSearchBackend<'_> {
    fn report_name(self) -> &'static str {
        match self {
            Self::Scalar => "scalar_vector_scan",
            #[cfg(feature = "vector-search")]
            Self::DiskAnn { .. } => DISKANN_CANDIDATE_BACKEND,
            Self::CompressedRequiredUnavailable => "compressed_vector_projection_required",
            #[cfg(not(feature = "vector-search"))]
            Self::_Lifetime(_) => "scalar_vector_scan",
            #[cfg(feature = "vector-search")]
            Self::RaBitQ { .. } => RABITQ_CANDIDATE_BACKEND,
            #[cfg(feature = "qualification")]
            Self::ExternalValidation { .. } => "external_validation_candidate_projection",
        }
    }

    fn candidate_source(self) -> VectorCandidateSource {
        match self {
            Self::Scalar | Self::CompressedRequiredUnavailable => VectorCandidateSource::Scalar,
            #[cfg(feature = "vector-search")]
            Self::DiskAnn { .. } => VectorCandidateSource::Ann,
            #[cfg(not(feature = "vector-search"))]
            Self::_Lifetime(_) => VectorCandidateSource::Scalar,
            #[cfg(feature = "vector-search")]
            Self::RaBitQ { .. } => VectorCandidateSource::Quantized,
            #[cfg(feature = "qualification")]
            Self::ExternalValidation { .. } => VectorCandidateSource::Quantized,
        }
    }

    fn index_coverage(self, documents: &[&SearchDocument]) -> (usize, usize) {
        let candidate_count = documents
            .iter()
            .filter(|document| document.embedding.is_some())
            .count();
        let covered_count = match self {
            Self::Scalar => candidate_count,
            #[cfg(feature = "vector-search")]
            Self::DiskAnn { .. } => candidate_count,
            Self::CompressedRequiredUnavailable => 0,
            #[cfg(not(feature = "vector-search"))]
            Self::_Lifetime(_) => candidate_count,
            #[cfg(feature = "vector-search")]
            Self::RaBitQ { projection, .. } => documents
                .iter()
                .filter(|document| {
                    document.embedding.is_some()
                        && projection.contains_document_id(document.id.as_str())
                })
                .count(),
            #[cfg(feature = "qualification")]
            Self::ExternalValidation {
                indexed_document_ids,
                ..
            } => documents
                .iter()
                .filter(|document| {
                    document.embedding.is_some()
                        && indexed_document_ids.contains(document.id.as_str())
                })
                .count(),
        };
        (covered_count, candidate_count)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAnalyzerLexicon {
    alias_rules: Vec<SearchAnalyzerAliasRule>,
    stopwords: BTreeSet<String>,
}

impl SearchAnalyzerLexicon {
    /// Distinct analyzed terms `text` produces as a lexical query.
    ///
    /// Hosts can compare this with the analyzed-term cap (default 1024) before
    /// submitting a query. Unlike query admission, it analyzes all of `text`
    /// without that cap, so its cost grows with the input.
    pub fn analyzed_query_term_count(&self, text: &str) -> usize {
        tokenize(text, self).len()
    }

    pub fn empty() -> Self {
        Self {
            alias_rules: Vec::new(),
            stopwords: BTreeSet::new(),
        }
    }

    pub fn with_alias_rule<I, A, S, T>(mut self, inputs: I, aliases: A) -> Self
    where
        I: IntoIterator<Item = S>,
        A: IntoIterator<Item = T>,
        S: Into<String>,
        T: Into<String>,
    {
        let inputs = inputs
            .into_iter()
            .map(Into::into)
            .filter(|input: &String| !input.is_empty())
            .collect::<Vec<_>>();
        let aliases = aliases
            .into_iter()
            .map(Into::into)
            .filter(|alias: &String| !alias.is_empty())
            .collect::<Vec<_>>();
        if !inputs.is_empty() && !aliases.is_empty() {
            self.alias_rules
                .push(SearchAnalyzerAliasRule { inputs, aliases });
        }
        self
    }

    pub fn with_normalized_alias_rule<I, A, S, T>(self, inputs: I, aliases: A) -> Self
    where
        I: IntoIterator<Item = S>,
        A: IntoIterator<Item = T>,
        S: AsRef<str>,
        T: AsRef<str>,
    {
        self.with_alias_rule(
            inputs
                .into_iter()
                .flat_map(|input| normalized_alias_rule_terms(input.as_ref())),
            aliases
                .into_iter()
                .flat_map(|alias| normalized_alias_rule_terms(alias.as_ref())),
        )
    }

    pub fn with_stopwords<I, S>(mut self, stopwords: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.stopwords.extend(
            stopwords
                .into_iter()
                .flat_map(|stopword| normalized_stopword_terms(stopword.as_ref())),
        );
        self
    }

    pub fn nowledge_memory() -> Self {
        Self::default().with_nowledge_memory_aliases()
    }

    pub fn with_nowledge_memory_aliases(self) -> Self {
        NOWLEDGE_MEMORY_SEMANTIC_ALIAS_RULES
            .iter()
            .fold(self, |lexicon, (inputs, aliases)| {
                lexicon.with_normalized_alias_rule(inputs.iter().copied(), aliases.iter().copied())
            })
    }

    fn is_stopword(&self, token: &str) -> bool {
        is_core_search_stopword(token) || self.stopwords.contains(token)
    }

    #[cfg(test)]
    fn semantic_aliases(&self, token: &str) -> Vec<String> {
        self.alias_rules
            .iter()
            .filter(|rule| rule.inputs.iter().any(|input| input == token))
            .flat_map(|rule| rule.aliases.iter().cloned())
            .collect()
    }

    fn semantic_alias_slices<'a>(&'a self, token: &str) -> impl Iterator<Item = &'a str> {
        self.alias_rules
            .iter()
            .filter(move |rule| rule.inputs.iter().any(|input| input == token))
            .flat_map(|rule| rule.aliases.iter().map(String::as_str))
    }
}

impl Default for SearchAnalyzerLexicon {
    fn default() -> Self {
        CORE_SEMANTIC_ALIAS_RULES
            .iter()
            .fold(Self::empty(), |lexicon, (inputs, aliases)| {
                lexicon.with_alias_rule(inputs.iter().copied(), aliases.iter().copied())
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchAnalyzerAliasRule {
    inputs: Vec<String>,
    aliases: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchRebuildOptions {
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRebuildSummary {
    pub scanned_nodes: usize,
    pub indexed_documents: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetadataRepairOptions {
    pub max_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRepairSummary {
    pub scanned_nodes: usize,
    pub repaired_documents: usize,
    pub missing_documents: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDerivedArtifactReport {
    pub artifact_type: String,
    pub name: String,
    pub action: String,
    pub before_document_count: usize,
    pub after_document_count: usize,
    pub scanned_nodes: usize,
    pub indexed_documents: usize,
    pub full_reindex_needed: bool,
    pub full_reindex_reasons: Vec<String>,
    pub metadata_repair_needed: bool,
    pub metadata_repair_reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchProjectionDelta {
    pub upserts: Vec<SearchProjectionRow>,
    pub deletes: Vec<String>,
    pub max_operations: Option<usize>,
    pub source_graph_commit_epoch: Option<u64>,
}

impl SearchProjectionDelta {
    pub fn operation_count(&self) -> usize {
        self.upserts.len() + self.deletes.len()
    }

    pub fn background_work_request(&self) -> WorkRequest {
        WorkRequest::background(WorkClass::Projection, self.operation_count())
    }

    pub fn background_work_plan(&self, hint: BackgroundWorkHint) -> Option<BackgroundWorkPlan> {
        let operation_count = self.operation_count();
        if operation_count == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            operation_count,
            hint,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionDeltaReport {
    pub artifact_type: String,
    pub name: String,
    pub action: String,
    pub before_document_count: usize,
    pub after_document_count: usize,
    pub upserted_documents: usize,
    pub deleted_documents: usize,
    pub operation_count: usize,
    pub source_graph_commit_epoch_before: Option<u64>,
    pub source_graph_commit_epoch_after: Option<u64>,
    pub source_graph_commit_epoch_updated: bool,
    /// Generation cleanup after the publication that made this delta
    /// durable; `None` when the call published nothing.
    pub projection_cleanup: Option<SearchProjectionCleanupOutcome>,
}

#[derive(Debug)]
pub struct SearchIndex {
    read_generation: Mutex<std::sync::Weak<resident_snapshot::ReadGeneration>>,
    read_freshness: Option<Arc<SearchProjectionFreshness>>,
    documents: ResidentDocuments,
    vector_index_descriptor: Option<hawdb_core::VectorIndexDescriptor>,
    vector_scoring: SearchVectorScoring,
    vector_document_summary: Option<vector_index::VectorDocumentSummary>,
    canonical_vector_source_epoch: Option<u64>,
    canonical_vector_delta: Option<vector_index::CanonicalVectorDelta>,
    consumer_binding: Option<consumer::ConsumerBinding>,
    consumer_binding_valid: bool,
    consumer_owner: std::sync::Weak<consumer::MutationOwner>,
    consumer_mutation_generation: Option<u64>,
    consumer_receipt: Mutex<Option<consumer::CheckpointReceipt>>,
    path: Option<PathBuf>,
    _project_files: Option<hawdb_storage::file_descriptors::ProjectFileDescriptors>,
    embedding_dimension: Option<usize>,
    rabitq_checkpoint: Option<(u8, usize, u64)>,
    embedding_manifest: Option<SearchEmbeddingManifest>,
    import_source_graph_commit_epoch: Option<u64>,
    source_graph_commit_epoch: Option<u64>,
    durable_source_graph_commit_epoch: Mutex<Option<u64>>,
    /// The epoch the loaded snapshot, lexical and vector artifacts are stamped
    /// with, when a verified metadata-only advance (#1756) moved
    /// `source_graph_commit_epoch` past it. Cleared by the next checkpoint,
    /// which restamps every artifact.
    artifact_source_graph_commit_epoch: Mutex<Option<u64>>,
    marker_lines: Mutex<BTreeMap<String, Vec<String>>>,
    analyzer_lexicon: Arc<SearchAnalyzerLexicon>,
    lexical_projection: Mutex<Option<Arc<LexicalProjectionReader>>>,
    lexical_delta: Mutex<Arc<LexicalMiniDelta>>,
    lexical_config: LexicalProjectionConfig,
    #[cfg(feature = "vector-search")]
    rabitq_projection: Mutex<Option<Arc<RaBitQCandidateProjection>>>,
    #[cfg(feature = "vector-search")]
    rabitq_build_options: RaBitQCandidateProjectionBuildOptions,
    #[cfg(feature = "vector-search")]
    resident_diskann: resident_diskann::ResidentDiskAnn,
    segment_descriptor: Option<Arc<SearchSegmentDescriptor>>,
    segment_payload_reader: Option<Arc<hawdb_storage::scan::FileSegmentRangeReader>>,
    range_read_config: SearchRangeReadConfig,
    cleanup_state: Mutex<SearchProjectionCleanupState>,
    cleanup_options: SearchProjectionCleanupOptions,
    runtime_capabilities: RuntimeCapabilities,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    /// Kinds whose documents only the host writes (#1814). Runtime policy,
    /// never persisted here; see [`Self::set_externally_owned_kinds`].
    externally_owned_kinds: BTreeSet<SearchProjectionKind>,
}

impl SearchIndex {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let project_files =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component(
                path.as_ref(),
                true,
            )?;
        fs::create_dir_all(path.as_ref())?;
        let _lease = out_of_core::SearchProjectionPublishLease::acquire(path.as_ref())?;
        let mut index = Self::open_under_lease(path.as_ref(), false, &_lease)?;
        index._project_files = Some(project_files);
        Ok(index)
    }

    fn open_under_lease(
        path: &Path,
        registered: bool,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> Result<Self> {
        let mut index = Self {
            path: Some(path.to_path_buf()),
            _project_files: Some(if registered {
                publication.with_captured_directory(|directory| {
                    hawdb_storage::file_descriptors::ProjectFileDescriptors::from_open_context(
                        &directory.operation_context(),
                    )
                })
            } else {
                hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component(
                    path, false,
                )?
            }),
            ..Self::default()
        };
        let loaded = if registered {
            index.load_snapshot_from_publication(publication)
        } else {
            out_of_core::recover_resident_aliases(path).and_then(|()| index.load_snapshot())
        };
        loaded.map_err(|error| {
            if registered && !matches!(error, HawDBError::FileDescriptors(_)) {
                HawDBError::StorageIntegrity(error.to_string())
            } else {
                error
            }
        })?;
        if index.vector_document_summary.is_some() {
            return Err(HawDBError::Storage(
                "metadata-only vector Consumers require registered native reopen".into(),
            ));
        }
        let advanced_source_epoch = if registered {
            out_of_core::validate_captured_resident_snapshot_identity(&index, publication)?
        } else {
            out_of_core::validate_resident_snapshot_identity(&index, path)?
        };
        if index.consumer_binding.is_some() != registered {
            return Err(HawDBError::Storage(
                "projection consumer lifecycle does not match snapshot binding".into(),
            ));
        }
        if registered {
            index
                .validate_registered_artifacts(publication)
                .map_err(|error| match error {
                    HawDBError::FileDescriptors(_) => error,
                    _ => HawDBError::StorageIntegrity(error.to_string()),
                })?;
        } else {
            index.load_or_rebuild_segment_descriptor()?;
            index.load_lexical_projection()?;
        }
        #[cfg(feature = "vector-search")]
        if registered {
            index.load_registered_rabitq_projection(publication)?;
        } else {
            index.load_rabitq_projection()?;
        }
        // Every artifact above was validated against its content stamp. Only
        // now report the epoch the selector advanced to (#1756).
        if let Some(epoch) = advanced_source_epoch {
            index.apply_published_epoch_advance(epoch);
        }
        index.capture_segment_payload_reader(publication)?;
        for marker in [FULL_REINDEX_MARKER, METADATA_REPAIR_MARKER] {
            let lines = index.read_marker_lines(marker)?;
            index
                .marker_lines
                .get_mut()
                .unwrap_or_else(|error| error.into_inner())
                .insert(marker.into(), lines);
        }
        index.retry_projection_cleanup_under_publication(
            SearchProjectionCleanupOptions::default(),
            publication,
        );
        Ok(index)
    }

    /// What a lexical query above the scored-term cap does on this resident
    /// index; [`LexicalQueryTermPolicy::Exact`] by default. Under `Exact`,
    /// projection-backed search and text seeds refuse such a query, while
    /// snapshot-free search scores every analyzed term. Not persisted.
    pub fn with_lexical_query_term_policy(mut self, policy: LexicalQueryTermPolicy) -> Self {
        self.lexical_config.query_term_policy = policy;
        self
    }

    /// Sets the resident policy in place. Like [`Self::with_lexical_query_term_policy`]
    /// it is not persisted: a reopened index starts at `Exact`.
    pub fn set_lexical_query_term_policy(&mut self, policy: LexicalQueryTermPolicy) {
        self.lexical_config.query_term_policy = policy;
    }

    pub fn with_analyzer_lexicon(mut self, analyzer_lexicon: SearchAnalyzerLexicon) -> Self {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.analyzer_lexicon = Arc::new(analyzer_lexicon);
        self.invalidate_lexical_projection();
        self
    }

    pub fn set_analyzer_lexicon(&mut self, analyzer_lexicon: SearchAnalyzerLexicon) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.analyzer_lexicon = Arc::new(analyzer_lexicon);
        self.invalidate_lexical_projection();
    }

    fn lexical_snapshot(&self) -> Option<(Arc<LexicalProjectionReader>, Arc<LexicalMiniDelta>)> {
        let projection = self
            .lexical_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reader = projection.as_ref()?;
        // Keep the reader locked until its delta is captured: checkpoint replaces both.
        let delta = self
            .lexical_delta
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        Some((Arc::clone(reader), delta))
    }

    fn replace_lexical_projection(&self, projection: Option<Arc<LexicalProjectionReader>>) {
        // Match snapshot acquisition order and publish the reader and reset together.
        let mut current = self
            .lexical_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut delta = self
            .lexical_delta
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = projection;
        *delta = Arc::default();
    }

    fn invalidate_lexical_projection(&self) {
        self.replace_lexical_projection(None);
    }

    fn load_lexical_projection(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let projection = LexicalProjectionReader::load(
            path,
            self.source_graph_commit_epoch,
            lexical_analyzer_digest(&self.analyzer_lexicon),
            lexical_documents_digest(&self.documents),
            self.lexical_config,
        )?;
        self.replace_lexical_projection(projection);
        Ok(())
    }

    fn load_lexical_projection_under_publication(
        &self,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> Result<()> {
        let projection = LexicalProjectionReader::load_under_publication(
            publication,
            self.source_graph_commit_epoch,
            lexical_analyzer_digest(&self.analyzer_lexicon),
            lexical_documents_digest(&self.documents),
            self.lexical_config,
        )?;
        self.replace_lexical_projection(projection);
        Ok(())
    }

    #[cfg(feature = "vector-search")]
    pub fn set_rabitq_projection_build_options(
        &mut self,
        options: RaBitQCandidateProjectionBuildOptions,
    ) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.rabitq_checkpoint = Some((
            options.bit_width.bits(),
            options.segment_rows,
            options.transform_seed,
        ));
        self.rabitq_build_options = options;
        self.invalidate_rabitq_projection();
    }

    #[cfg(feature = "vector-search")]
    fn invalidate_rabitq_projection(&self) {
        *self
            .rabitq_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    #[cfg(feature = "vector-search")]
    fn load_rabitq_projection(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let artifacts = match rabitq_artifacts_descending(path) {
            Ok(artifacts) => artifacts,
            Err(error @ HawDBError::FileDescriptors(_)) => return Err(error),
            // An optional resident projection can be rebuilt or served by the
            // existing exact path. Keep its historical non-resource fallback.
            Err(_) => return Ok(()),
        };
        for (generation, artifact_path) in artifacts {
            match resident_diskann::artifact::validate_file(self, &artifact_path, generation) {
                Ok(resident_diskann::artifact::Restore::Ready) => return Ok(()),
                Ok(resident_diskann::artifact::Restore::Skip) => continue,
                Ok(resident_diskann::artifact::Restore::Other) => {}
                Err(HawDBError::StorageIntegrity(_)) => {
                    if let Some(name) = artifact_path.file_name().and_then(|name| name.to_str()) {
                        quarantine_rebuildable_artifact(path, name);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
            let identity = self.rabitq_projection_identity(generation);
            match RaBitQCandidateProjection::load_from_path_classified(
                &artifact_path,
                &self.documents,
                &identity,
            ) {
                Ok(projection) => {
                    if projection.manifest().metric != self.vector_metric().into() {
                        return Err(HawDBError::StorageIntegrity(
                            "RaBitQ artifact metric differs from its projection".into(),
                        ));
                    }
                    if let Some(binding) = &self.vector_index_descriptor {
                        vector_index::validate_projection(binding, projection.manifest())?;
                    }
                    *self
                        .rabitq_projection
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(Arc::new(projection));
                    return Ok(());
                }
                Err(error) if error.should_quarantine() => {
                    if let Some(name) = artifact_path.file_name().and_then(|name| name.to_str()) {
                        quarantine_rebuildable_artifact(path, name);
                    }
                }
                Err(rabitq_projection::RaBitQCandidateProjectionLoadError::Resource(error)) => {
                    return Err(error);
                }
                Err(_) => {}
            }
        }
        Ok(())
    }

    #[cfg(feature = "vector-search")]
    fn rabitq_projection_identity(
        &self,
        generation: u64,
    ) -> hawdb_vector_projection::ProjectionIdentity {
        hawdb_vector_projection::ProjectionIdentity {
            generation,
            source_epoch: self.source_graph_commit_epoch,
            embedding_model: self
                .embedding_manifest
                .as_ref()
                .map(|manifest| manifest.model.clone()),
            embedding_version: self
                .embedding_manifest
                .as_ref()
                .and_then(|manifest| manifest.version.clone()),
        }
    }

    #[cfg(feature = "vector-search")]
    fn rabitq_projection(&self) -> Option<Arc<RaBitQCandidateProjection>> {
        self.rabitq_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    #[cfg(feature = "vector-search")]
    fn rabitq_backend_selected(&self) -> bool {
        self.vector_index_descriptor.as_ref().map_or_else(
            || self.rabitq_checkpoint.is_some(),
            |definition| {
                matches!(
                    definition.backend(),
                    hawdb_core::VectorIndexBackend::RaBitQ { .. }
                )
            },
        )
    }

    #[cfg(feature = "vector-search")]
    fn build_in_memory_rabitq_projection(&self) -> Result<Option<Arc<RaBitQCandidateProjection>>> {
        self.build_in_memory_rabitq_projection_with_context(None, None)
    }

    #[cfg(feature = "vector-search")]
    fn lock_rabitq_projection(
        &self,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<std::sync::MutexGuard<'_, Option<Arc<RaBitQCandidateProjection>>>> {
        if task.is_none() {
            return Ok(self
                .rabitq_projection
                .lock()
                .unwrap_or_else(|error| error.into_inner()));
        }
        loop {
            task.map_or(Ok(()), crate::build_control::checkpoint)?;
            match self.rabitq_projection.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    }

    #[cfg(feature = "vector-search")]
    fn build_in_memory_rabitq_projection_with_context(
        &self,
        task: Option<&RuntimeTaskContext>,
        max_working_bytes: Option<usize>,
    ) -> Result<Option<Arc<RaBitQCandidateProjection>>> {
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        self.vector_scoring
            .validate(self.vector_index_descriptor.is_some())?;
        // Serialize the first build within a generation. Captured generations
        // reserve its complete build/cache capacity before a governed lease is
        // returned; concurrent readers must not allocate duplicate projections.
        let mut cached = self.lock_rabitq_projection(task)?;
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        if let Some(projection) = cached.as_ref() {
            return Ok(Some(Arc::clone(projection)));
        }
        if let Some(available) = max_working_bytes {
            let required = RaBitQCandidateProjection::in_memory_capacity_bound_with_context(
                &self.documents,
                self.rabitq_projection_identity(0),
                self.rabitq_build_options,
                task,
            )?;
            task.map_or(Ok(()), crate::build_control::checkpoint)?;
            if required > available {
                return Err(rabitq_projection::map_projection_error(
                    hawdb_vector_projection::ProjectionError::ResourceBudgetExceeded {
                        required,
                        available,
                    },
                    "resident RaBitQ build/cache admission",
                ));
            }
        }
        let projection = RaBitQCandidateProjection::build_from_view(
            &self.documents,
            self.rabitq_projection_identity(0),
            self.rabitq_build_options,
            self.vector_metric(),
            task,
        )?
        .map(Arc::new);
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        *cached = projection.clone();
        Ok(projection)
    }

    fn record_lexical_upsert(&self, document: &SearchDocument) {
        if self
            .lexical_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none()
        {
            return;
        }
        let result = {
            let mut delta = self
                .lexical_delta
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            delta.upsert(
                document,
                self.documents.get(&document.id),
                &self.analyzer_lexicon,
                self.lexical_config,
            )
        };
        if result.is_err() {
            self.invalidate_lexical_projection();
        }
    }

    fn record_lexical_delete(&self, document_id: &str) {
        if self
            .lexical_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_none()
        {
            return;
        }
        let admitted = {
            let mut delta = self
                .lexical_delta
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            delta.delete(
                document_id,
                self.documents.get(document_id),
                &self.analyzer_lexicon,
                self.lexical_config,
            )
        };
        if !matches!(admitted, Ok(true)) {
            self.invalidate_lexical_projection();
        }
    }

    pub fn set_telemetry_sink(&mut self, telemetry: Option<Arc<dyn TelemetrySink>>) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.telemetry = telemetry;
    }

    pub fn telemetry_sink_configured(&self) -> bool {
        self.telemetry.is_some()
    }

    pub fn set_range_read_config(&mut self, config: SearchRangeReadConfig) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.range_read_config = config;
    }

    pub fn range_read_config(&self) -> SearchRangeReadConfig {
        self.range_read_config
    }

    pub fn set_runtime_capabilities(&mut self, capabilities: RuntimeCapabilities) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        self.runtime_capabilities =
            crate::compiled_capabilities::effective_runtime_capabilities(capabilities);
    }

    pub fn runtime_capabilities(&self) -> RuntimeCapabilities {
        self.runtime_capabilities
    }

    /// Declares kinds whose documents the host supplies itself (#1814).
    ///
    /// Graph changefeed catch-up applied to this index then derives no rows
    /// for nodes of these kinds: their documents are written only by host
    /// deltas, and are removed only when the graph node is deleted (or the
    /// host deletes them). Graph-derived rows of other kinds and every host
    /// delta are unaffected. A full rebuild from the graph carries the current
    /// document of every live node of these kinds over unchanged, dropping
    /// documents whose node no longer exists, and metadata repair leaves them
    /// untouched. Both raise the persisted rows-required fence of a
    /// persistent index, so the host re-pushes its rows. The set is not
    /// persisted by the index; an embedded store records it with
    /// `EmbeddedStoreOpenOptions::with_externally_owned_search_kinds`.
    pub fn set_externally_owned_kinds(&mut self, kinds: BTreeSet<SearchProjectionKind>) {
        self.externally_owned_kinds = kinds;
    }

    pub fn externally_owned_kinds(&self) -> &BTreeSet<SearchProjectionKind> {
        &self.externally_owned_kinds
    }

    /// Raises the persisted rows-required fence before a rebuild or metadata
    /// repair of a projection with externally owned kinds (#1814).
    ///
    /// Carry-over keeps the documents the index holds, but host writes that
    /// have not reached this index (for example to another directory during
    /// an out-of-core rebuild) cannot be proven present, so the host always
    /// re-pushes after maintenance. Raising first keeps a failed rebuild
    /// fail-safe. An unreadable state fails closed; if a valid state disappears
    /// after open, the raise creates a fresh incarnation that rejects retained
    /// acknowledgements from the removed state.
    fn raise_externally_owned_rows_required(&self) -> Result<()> {
        if self.externally_owned_kinds.is_empty() {
            return Ok(());
        }
        let Some(path) = self.path.as_deref() else {
            return Ok(());
        };
        projection_ownership::raise_rows_required(path).map(|_| ())
    }

    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        if let Some(embedding) = &document.embedding {
            vector_index::validate_embedding(
                self.vector_index_descriptor.as_ref(),
                embedding,
                || Ok(()),
            )?;
            self.validate_or_set_dimension(embedding.len())?;
        }
        #[cfg(feature = "vector-search")]
        self.resident_diskann.record_change(&document.id);
        self.record_lexical_upsert(&document);
        self.documents.insert(document.id.clone(), document);
        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();
        self.segment_descriptor = None;
        self.segment_payload_reader = None;
        Ok(())
    }

    pub fn upsert_projection_row(&mut self, row: SearchProjectionRow) -> Result<()> {
        self.upsert(row.into_document())
    }

    pub fn delete(&mut self, id: &str) {
        let _consumer_mutation = self.mark_untracked_consumer_mutation();
        #[cfg(feature = "vector-search")]
        self.resident_diskann.record_change(id);
        self.record_lexical_delete(id);
        self.documents.remove(id);
        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();
        self.segment_descriptor = None;
        self.segment_payload_reader = None;
    }

    pub fn apply_projection_delta(
        &mut self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        let operation_count = delta.operation_count();
        if let Some(limit) = delta.max_operations
            && operation_count > limit
        {
            // A per-batch budget refusal, not storage damage (#1924).
            return Err(HawDBError::SearchProjectionBatchBudgetExceeded {
                budget: hawdb_core::SearchProjectionBatchBudget::ProjectionDelta,
                commit_epoch: None,
                required_operations: operation_count,
                limit,
            });
        }

        let mut next_embedding_dimension = self.embedding_dimension;
        for row in &delta.upserts {
            let Some(embedding) = &row.embedding else {
                continue;
            };
            let dimension = embedding.len();
            vector_index::validate_embedding(
                self.vector_index_descriptor.as_ref(),
                embedding,
                || Ok(()),
            )?;
            if let Some(manifest) = &self.embedding_manifest
                && manifest.dimension != dimension
            {
                return Err(HawDBError::Storage(format!(
                    "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                    manifest.dimension
                )));
            }
            match next_embedding_dimension {
                Some(existing) if existing != dimension => {
                    return Err(HawDBError::Storage(format!(
                        "embedding dimension mismatch: index has {existing}, row has {dimension}"
                    )));
                }
                Some(_) => {}
                None => next_embedding_dimension = Some(dimension),
            }
        }

        let SearchProjectionDelta {
            upserts,
            deletes,
            max_operations: _,
            source_graph_commit_epoch,
        } = delta;
        let before_document_count = self.documents.len();
        let source_graph_commit_epoch_before = self.source_graph_commit_epoch;
        let mut deleted_documents = 0;
        for id in deletes {
            #[cfg(feature = "vector-search")]
            self.resident_diskann.record_change(&id);
            self.record_lexical_delete(&id);
            if self.documents.remove(&id).is_some() {
                deleted_documents += 1;
            }
        }
        let upserted_documents = upserts.len();
        for row in upserts {
            let document = row.into_document();
            #[cfg(feature = "vector-search")]
            self.resident_diskann.record_change(&document.id);
            self.record_lexical_upsert(&document);
            self.documents.insert(document.id.clone(), document);
        }

        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();

        self.segment_descriptor = None;
        self.segment_payload_reader = None;
        self.embedding_dimension = next_embedding_dimension;
        let source_graph_commit_epoch_updated = source_graph_commit_epoch.is_some();
        if let Some(epoch) = source_graph_commit_epoch {
            self.source_graph_commit_epoch = Some(epoch);
        }
        let source_graph_commit_epoch_after = self.source_graph_commit_epoch;
        Ok(SearchProjectionDeltaReport {
            artifact_type: "search_projection".to_string(),
            name: "search_projection".to_string(),
            action: "incremental_update".to_string(),
            before_document_count,
            after_document_count: self.documents.len(),
            upserted_documents,
            deleted_documents,
            operation_count,
            source_graph_commit_epoch_before,
            source_graph_commit_epoch_after,
            source_graph_commit_epoch_updated,
            projection_cleanup: None,
        })
    }

    pub fn apply_background_projection_delta(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        policy.admit_or_refuse(state, &delta.background_work_request())?;
        self.apply_projection_delta(delta)
    }

    pub fn apply_scheduled_background_projection_delta(
        &mut self,
        scheduler: &LocalQosScheduler,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let permit = scheduler.try_start_or_refuse(delta.background_work_request())?;

        let result = self.apply_projection_delta(delta);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn document(&self, id: &str) -> Option<&SearchDocument> {
        self.documents.get(id)
    }

    pub fn document_count(&self) -> usize {
        self.vector_document_summary
            .map_or(self.documents.len(), |summary| summary.document_count)
    }

    /// The largest eligible-set size an exact vector-only read of this
    /// resident index under `working_bytes` is guaranteed to serve, for
    /// eligible document IDs of at most `max_document_id_bytes` bytes. Raw
    /// candidates retain a geometrically grown vector (at most two slots per
    /// candidate) plus their IDs; a raw rerank also keeps a lookup over every
    /// indexed document and two ID copies per candidate. A larger read may be
    /// refused; it is never truncated.
    pub fn exact_vector_search_capacity(
        &self,
        working_bytes: usize,
        max_document_id_bytes: usize,
    ) -> usize {
        resident_exact_vector_search_capacity(
            self.document_count(),
            working_bytes,
            max_document_id_bytes,
        )
    }

    /// Returns vector-bearing document IDs accepted by HawDB's metadata
    /// predicate implementation. This validation surface keeps differential
    /// oracles aligned with serving filter semantics.
    #[cfg(feature = "qualification")]
    #[doc(hidden)]
    pub fn vector_document_ids_matching_filters_for_validation(
        &self,
        filters: &BTreeMap<String, String>,
    ) -> BTreeSet<String> {
        let predicate_pushdown = search_metadata_predicate_pushdown(filters);
        self.documents
            .values()
            .filter(|document| {
                document.embedding.is_some()
                    && search_document_matches_predicates(document, &predicate_pushdown.predicates)
            })
            .map(|document| document.id.clone())
            .collect()
    }

    pub fn embedding_manifest(&self) -> Option<&SearchEmbeddingManifest> {
        self.embedding_manifest.as_ref()
    }

    pub fn projection_freshness(&self) -> SearchProjectionFreshness {
        if let Some(freshness) = &self.read_freshness {
            return freshness.as_ref().clone();
        }
        let full_reindex_reasons = self
            .read_marker_lines(FULL_REINDEX_MARKER)
            .unwrap_or_default();
        let metadata_repair_reasons = self
            .read_marker_lines(METADATA_REPAIR_MARKER)
            .unwrap_or_default();
        self.freshness_with_reasons(full_reindex_reasons, metadata_repair_reasons)
    }

    fn freshness_with_reasons(
        &self,
        full_reindex_reasons: Vec<String>,
        metadata_repair_reasons: Vec<String>,
    ) -> SearchProjectionFreshness {
        let durable_source_graph_commit_epoch = *self
            .durable_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        SearchProjectionFreshness {
            document_count: self.document_count(),
            import_source_graph_commit_epoch: self.import_source_graph_commit_epoch,
            source_graph_commit_epoch: self.source_graph_commit_epoch,
            durable_source_graph_commit_epoch,
            has_uncheckpointed_changes: self.source_graph_commit_epoch
                != durable_source_graph_commit_epoch,
            full_reindex_needed: !full_reindex_reasons.is_empty(),
            full_reindex_reasons,
            metadata_repair_needed: !metadata_repair_reasons.is_empty(),
            metadata_repair_reasons,
            embedding_model: self
                .embedding_manifest
                .as_ref()
                .map(|manifest| manifest.model.clone()),
            embedding_version: self
                .embedding_manifest
                .as_ref()
                .and_then(|manifest| manifest.version.clone()),
            embedding_dimension: self
                .embedding_manifest
                .as_ref()
                .map(|manifest| manifest.dimension)
                .or(self.embedding_dimension),
        }
    }

    /// Records immutable provenance for an external graph bootstrap. The
    /// mutable local projection cursor remains independent.
    pub fn record_import_source_graph_commit_epoch(&mut self, epoch: u64) -> Result<()> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        self.validate_import_source_graph_commit_epoch(epoch)?;
        self.import_source_graph_commit_epoch = Some(epoch);
        Ok(())
    }

    /// Verifies that an import retry still belongs to the same frozen legacy
    /// source without changing the in-memory projection state.
    pub fn validate_import_source_graph_commit_epoch(&self, epoch: u64) -> Result<()> {
        match self.import_source_graph_commit_epoch {
            Some(existing) if existing != epoch => Err(HawDBError::Storage(
                "search projection import provenance conflicts with existing source".to_string(),
            )),
            Some(_) | None => Ok(()),
        }
    }

    pub fn nowledge_search_projection_probe_json(
        &self,
        options: SearchProjectionProbeOptions,
    ) -> serde_json::Value {
        let freshness = self.projection_freshness();
        let table_reports =
            search_projection_probe_table_reports(&self.documents, freshness.embedding_dimension);
        let manifest = self.embedding_manifest.as_ref();
        let model = manifest.map(|manifest| manifest.model.as_str());
        let dimension = manifest
            .map(|manifest| manifest.dimension)
            .or(freshness.embedding_dimension);
        let active_model = options.active_embedding_model.as_deref().or(model);
        let active_dimension = options.active_embedding_dimension.or(dimension);
        let model_matches = model.is_some() && model == active_model;
        let dimension_matches = dimension.is_some() && dimension == active_dimension;
        let has_documents = !self.documents.is_empty();
        let has_text = self
            .documents
            .values()
            .any(|document| !document.title.is_empty() || !document.content.is_empty());
        let has_vector = self
            .documents
            .values()
            .any(|document| document.embedding.is_some());
        let predicate_pushdown = search_projection_probe_predicate_pushdown_report(self);
        let production_filter_pruning =
            search_projection_probe_production_filter_pruning_report(self);
        let compressed_vector_projection =
            search_projection_probe_compressed_vector_projection_report(self);
        let generation_cleanup = self.projection_cleanup_report();
        let mut blocker_codes = search_projection_probe_blocker_codes(
            has_documents,
            has_text,
            has_vector,
            manifest.is_some(),
            model_matches,
            dimension_matches,
            &freshness,
        );
        if generation_cleanup.retry_required {
            blocker_codes.push("projection_generation_cleanup_pending".to_string());
            blocker_codes.sort();
        }

        serde_json::json!({
            "protocol": "hawdb-nowledge-search-projection-probe",
            "derived_projection": true,
            "document_count": self.documents.len(),
            "document_identity": search_projection_probe_document_identity_report(&self.documents),
            "tables": table_reports,
            "embedding_manifest": {
                "model": model,
                "version": manifest.and_then(|manifest| manifest.version.as_deref()),
                "dimension": dimension,
                "active_model": active_model,
                "active_dimension": active_dimension,
                "model_matches": model_matches,
                "dimension_matches": dimension_matches,
            },
            "fail_soft": {
                "fts_to_vector_ready": has_vector,
                "vector_to_fts_ready": has_text,
                "no_500_on_leg_failure": has_documents,
            },
            "lifecycle": {
                "rebuild_marker_ready": !freshness.full_reindex_needed,
                "metadata_repair_marker_ready": !freshness.metadata_repair_needed,
                "generation_cleanup_ready": !generation_cleanup.retry_required,
                "full_reindex_needed": freshness.full_reindex_needed,
                "full_reindex_reasons": freshness.full_reindex_reasons,
                "metadata_repair_needed": freshness.metadata_repair_needed,
                "metadata_repair_reasons": freshness.metadata_repair_reasons,
                "source_graph_commit_epoch": freshness.source_graph_commit_epoch,
            },
            "generation_cleanup": generation_cleanup.json(),
            "incremental_update": {
                "ready": has_documents && freshness.durable_source_graph_commit_epoch.is_some(),
                "upsert_ready": has_documents,
                "delete_ready": has_documents,
                "watermark_ready": freshness.durable_source_graph_commit_epoch.is_some(),
                "source_graph_commit_epoch": freshness.source_graph_commit_epoch,
                "durable_source_graph_commit_epoch": freshness.durable_source_graph_commit_epoch,
                "has_uncheckpointed_changes": freshness.has_uncheckpointed_changes,
            },
            "compressed_vector_projection": compressed_vector_projection,
            "predicate_pushdown": predicate_pushdown,
            "production_filter_pruning": production_filter_pruning,
            "blocker_codes": blocker_codes,
        })
    }

    pub fn apply_embedding_manifest(&mut self, manifest: SearchEmbeddingManifest) -> Result<()> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        if let Some(existing) = &self.embedding_manifest {
            if existing == &manifest {
                return Ok(());
            }
            self.mark_full_reindex_needed(&format!(
                "embedding manifest changed from {} to {}",
                format_embedding_manifest(existing),
                format_embedding_manifest(&manifest)
            ))?;
        } else if !self.documents.is_empty() {
            self.mark_full_reindex_needed(&format!(
                "embedding manifest initialized as {} for existing projection",
                format_embedding_manifest(&manifest)
            ))?;
        }
        self.embedding_dimension = Some(manifest.dimension);
        self.embedding_manifest = Some(manifest);
        #[cfg(feature = "vector-search")]
        self.resident_diskann.reset();
        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();
        Ok(())
    }

    pub fn rebuild_from_graph<S: SearchProjectionSource + ?Sized>(
        &mut self,
        catalog: &Catalog,
        store: &S,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        self.raise_externally_owned_rows_required()?;
        let started = hawdb_core::time::Instant::now();
        let mut next_documents = BTreeMap::new();
        let mut scanned_nodes = 0;

        let result = (|| {
            let business_labels_by_node = store.projection_business_labels_by_node(catalog)?;
            store.visit_projection_nodes(&mut |node| {
                scanned_nodes += 1;
                let Some(row) = projection_row_from_node_with_business_labels(
                    catalog,
                    &node,
                    business_labels_by_node
                        .get(&node.id)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                ) else {
                    return Ok(());
                };
                // The host is the only writer of owned kinds: carry its
                // current document over for a live node instead of deriving
                // one. Documents of nodes that no longer exist are dropped.
                let owned = self.externally_owned_kinds.contains(&row.kind);
                let carried = owned
                    .then(|| {
                        self.documents
                            .get(&format!("{}:{}", row.kind.as_str(), row.external_id))
                            .cloned()
                    })
                    .flatten();
                if owned && carried.is_none() {
                    return Ok(());
                }
                if options
                    .max_rows
                    .map(|limit| next_documents.len() >= limit)
                    .unwrap_or(false)
                {
                    self.mark_full_reindex_needed("full rebuild exceeded configured row limit")?;
                    return Err(HawDBError::Storage(format!(
                        "full rebuild exceeded configured row limit after {} documents",
                        next_documents.len()
                    )));
                }
                let document = carried.unwrap_or_else(|| row.into_document());
                next_documents.insert(document.id.clone(), document);
                Ok(())
            })?;

            self.documents = next_documents.into();
            self.segment_descriptor = None;
            self.segment_payload_reader = None;
            #[cfg(feature = "vector-search")]
            self.resident_diskann.reset();
            self.invalidate_lexical_projection();
            #[cfg(feature = "vector-search")]
            self.invalidate_rabitq_projection();
            self.source_graph_commit_epoch = Some(store.source_graph_commit_epoch());
            self.embedding_dimension = self
                .embedding_manifest
                .as_ref()
                .map(|manifest| manifest.dimension);
            self.clear_marker(FULL_REINDEX_MARKER)?;
            self.clear_marker(METADATA_REPAIR_MARKER)?;
            Ok(SearchRebuildSummary {
                scanned_nodes,
                indexed_documents: self.documents.len(),
            })
        })();
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::IndexMaintenance,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: scanned_nodes,
                byte_count: 0,
                fsync_micros: 0,
                generation: None,
            });
        }
        result
    }

    pub fn rebuild_derived_artifacts<S: SearchProjectionSource + ?Sized>(
        &mut self,
        catalog: &Catalog,
        store: &S,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        let before_document_count = self.documents.len();
        let summary = self.rebuild_from_graph(catalog, store, options)?;
        let freshness = self.projection_freshness();
        Ok(SearchDerivedArtifactReport {
            artifact_type: "search_projection".to_string(),
            name: "search_projection".to_string(),
            action: "rebuilt".to_string(),
            before_document_count,
            after_document_count: self.documents.len(),
            scanned_nodes: summary.scanned_nodes,
            indexed_documents: summary.indexed_documents,
            full_reindex_needed: freshness.full_reindex_needed,
            full_reindex_reasons: freshness.full_reindex_reasons,
            metadata_repair_needed: freshness.metadata_repair_needed,
            metadata_repair_reasons: freshness.metadata_repair_reasons,
        })
    }

    pub fn rebuild_background_work_plan<S: SearchProjectionSource + ?Sized>(
        &self,
        store: &S,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = self.rebuild_estimated_operations(store);
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            estimated_operations,
            hint,
        ))
    }

    pub fn rebuild_background_derived_artifacts<S: SearchProjectionSource + ?Sized>(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        catalog: &Catalog,
        store: &S,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(
            WorkClass::Projection,
            self.rebuild_estimated_operations(store),
        );
        policy.admit_or_refuse(state, &request)?;
        self.rebuild_derived_artifacts(catalog, store, options)
    }

    pub fn rebuild_scheduled_background_derived_artifacts<S: SearchProjectionSource + ?Sized>(
        &mut self,
        scheduler: &LocalQosScheduler,
        catalog: &Catalog,
        store: &S,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(
            WorkClass::Projection,
            self.rebuild_estimated_operations(store),
        );
        let permit = scheduler.try_start_or_refuse(request)?;

        let result = self.rebuild_derived_artifacts(catalog, store, options);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn repair_metadata_from_graph<S: SearchProjectionSource + ?Sized>(
        &mut self,
        catalog: &Catalog,
        store: &S,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        let _consumer_mutation = self.require_owned_mutation_or_unregistered()?;
        self.raise_externally_owned_rows_required()?;
        let started = hawdb_core::time::Instant::now();
        let mut repairs = Vec::new();
        let mut scanned_nodes = 0;
        let mut missing_documents = 0;

        let result = (|| {
            let business_labels_by_node = store.projection_business_labels_by_node(catalog)?;
            store.visit_projection_nodes(&mut |node| {
                scanned_nodes += 1;
                let Some(row) = projection_row_from_node_with_business_labels(
                    catalog,
                    &node,
                    business_labels_by_node
                        .get(&node.id)
                        .map(Vec::as_slice)
                        .unwrap_or_default(),
                ) else {
                    return Ok(());
                };
                // The host owns these documents' metadata as well.
                if self.externally_owned_kinds.contains(&row.kind) {
                    return Ok(());
                }
                let document = row.into_document();
                if !self.documents.contains_key(&document.id) {
                    missing_documents += 1;
                    return Ok(());
                }
                if options
                    .max_rows
                    .map(|limit| repairs.len() >= limit)
                    .unwrap_or(false)
                {
                    self.mark_metadata_repair_needed(
                        "metadata repair exceeded configured row limit",
                    )?;
                    return Err(HawDBError::Storage(format!(
                        "metadata repair exceeded configured row limit after {} documents",
                        repairs.len()
                    )));
                }
                repairs.push((document.id, document.metadata));
                Ok(())
            })?;

            let repaired_documents = repairs.len();
            let mut repaired_projection_documents = Vec::with_capacity(repaired_documents);
            for (id, metadata) in repairs {
                if let Some(existing) = self.documents.get(&id) {
                    let mut repaired = existing.clone();
                    repaired.metadata = metadata;
                    repaired_projection_documents.push(repaired);
                }
            }
            for document in &repaired_projection_documents {
                self.record_lexical_upsert(document);
            }
            for document in repaired_projection_documents {
                self.documents.insert(document.id.clone(), document);
            }
            self.segment_descriptor = None;
            self.segment_payload_reader = None;
            if missing_documents > 0 {
                self.mark_full_reindex_needed("metadata repair found missing projection rows")?;
            }
            self.clear_marker(METADATA_REPAIR_MARKER)?;
            Ok(MetadataRepairSummary {
                scanned_nodes,
                repaired_documents,
                missing_documents,
            })
        })();
        if let Some(telemetry) = &self.telemetry {
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::IndexMaintenance,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: scanned_nodes,
                byte_count: 0,
                fsync_micros: 0,
                generation: None,
            });
        }
        result
    }

    pub fn metadata_repair_background_work_plan<S: SearchProjectionSource + ?Sized>(
        &self,
        store: &S,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = store.estimated_projection_node_count();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            estimated_operations,
            hint,
        ))
    }

    pub fn repair_background_metadata_from_graph<S: SearchProjectionSource + ?Sized>(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        catalog: &Catalog,
        store: &S,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(WorkClass::Projection, estimated_operations);
        policy.admit_or_refuse(state, &request)?;
        self.repair_metadata_from_graph(catalog, store, options)
    }

    pub fn repair_scheduled_background_metadata_from_graph<S: SearchProjectionSource + ?Sized>(
        &mut self,
        scheduler: &LocalQosScheduler,
        catalog: &Catalog,
        store: &S,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let request = WorkRequest::background(WorkClass::Projection, estimated_operations);
        let permit = scheduler.try_start_or_refuse(request)?;

        let result = self.repair_metadata_from_graph(catalog, store, options);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    fn rebuild_estimated_operations<S: SearchProjectionSource + ?Sized>(&self, store: &S) -> usize {
        store
            .estimated_projection_node_count()
            .max(self.documents.len())
    }

    /// Limits for the generation cleanup that runs after every checkpoint.
    /// Not persisted: a reopened index starts at the defaults.
    pub fn set_projection_cleanup_options(&mut self, options: SearchProjectionCleanupOptions) {
        self.cleanup_options = options;
    }

    pub fn projection_cleanup_options(&self) -> SearchProjectionCleanupOptions {
        self.cleanup_options
    }

    pub fn projection_cleanup_report(&self) -> SearchProjectionCleanupReport {
        self.cleanup_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .report()
    }

    pub fn retry_projection_cleanup(
        &self,
        options: SearchProjectionCleanupOptions,
    ) -> SearchProjectionCleanupReport {
        let Some(path) = &self.path else {
            return self.projection_cleanup_report();
        };
        if self.consumer_binding_valid
            && self.consumer_binding.is_some()
            && let Ok(consumer_mutation) = self.borrow_consumer_mutation()
        {
            return self.retry_projection_cleanup_under_publication(
                options,
                consumer_mutation.publication(),
            );
        }
        let Ok(_publication) = out_of_core::SearchProjectionPublishLease::acquire_for_cleanup(path)
        else {
            return self
                .cleanup_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .defer_for_publication(options);
        };
        self.retry_projection_cleanup_under_publication(options, &_publication)
    }

    // Caller retains the same root's publication authority through discovery
    // and sweep. Checkpoint already owns that authority and must not reacquire.
    fn retry_projection_cleanup_under_publication(
        &self,
        options: SearchProjectionCleanupOptions,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> SearchProjectionCleanupReport {
        let Some(path) = &self.path else {
            return self.projection_cleanup_report();
        };
        self.cleanup_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .run_under_publication(path, options, publication, &self.analyzer_lexicon, None)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.checkpoint_with_report().map(|_| ())
    }

    pub fn checkpoint_with_report(&self) -> Result<SearchCheckpointReport> {
        self.require_unregistered_publication()?;
        self.checkpoint_with_lease(None)
    }

    fn checkpoint_with_lease(
        &self,
        publication: Option<&out_of_core::SearchProjectionPublishLease>,
    ) -> Result<SearchCheckpointReport> {
        self.checkpoint_with_task(publication, &RuntimeTaskContext::default())
    }

    fn checkpoint_with_task(
        &self,
        publication: Option<&out_of_core::SearchProjectionPublishLease>,
        task: &RuntimeTaskContext,
    ) -> Result<SearchCheckpointReport> {
        self.vector_scoring
            .validate(self.vector_index_descriptor.is_some())?;
        let Some(path) = &self.path else {
            return Ok(SearchCheckpointReport::in_memory(self.documents.len()));
        };
        task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)?;
        let _io = task
            .acquire_io_wave(NonZeroUsize::MIN)
            .map_err(HawDBError::from_execution_error)?;
        let memory = build_memory::BuildMemory::new(task)?;
        let started = hawdb_core::time::Instant::now();
        let result = (|| {
            let owned_publication = if publication.is_some() {
                None
            } else {
                Some(out_of_core::SearchProjectionPublishLease::acquire(path)?)
            };
            let publication_lease = publication
                .or(owned_publication.as_ref())
                .expect("checkpoint retains publication authority");
            // Unregistered resident checkpoints write every artifact under a
            // generation name and commit with the manifest rename. Registered
            // Consumers keep their legacy order; their own publication record
            // selects the producer closure.
            let deferred_aliases = self.consumer_binding.is_none();
            if deferred_aliases {
                // `open` may have created this directory without persisting
                // its entry; a durable selector inside it must not vanish.
                hawdb_storage::durability::sync_directory_ancestors(path)?;
            }
            let write_snapshot = |target: &Path| {
                snapshot_writer::write_search_snapshot_with_context(
                    target,
                    snapshot_writer::SearchSnapshotMetadata {
                        source_graph_commit_epoch: self.source_graph_commit_epoch,
                        import_source_graph_commit_epoch: self.import_source_graph_commit_epoch,
                        embedding_manifest: self.embedding_manifest.as_ref(),
                        embedding_dimension: self.embedding_dimension,
                        consumer_binding: self.consumer_binding.as_ref(),
                        vector_index_descriptor: self.vector_index_descriptor.as_ref(),
                        vector_scoring: self.vector_scoring,
                        vector_document_summary: None,
                        canonical_vector_source_epoch: None,
                        rabitq_checkpoint: self.rabitq_checkpoint,
                    },
                    self.documents.values(),
                    &memory,
                    task,
                )
            };
            let legacy_snapshot = if deferred_aliases {
                None
            } else {
                Some(write_snapshot(&path.join(SEARCH_SNAPSHOT_FILE))?)
            };
            let generation =
                out_of_core::next_generation(path, self.lexical_config.max_manifest_bytes.get())?;
            #[cfg(feature = "vector-search")]
            let (generation, rabitq_bytes_written, rabitq) =
                self.write_rabitq_projection(path, generation, task, publication_lease)?;
            #[cfg(not(feature = "vector-search"))]
            let rabitq_bytes_written = 0;
            #[cfg(test)]
            out_of_core::resident_checkpoint_fault(out_of_core::ResidentCheckpointStep::RaBitQ)?;
            let snapshot = match legacy_snapshot {
                Some(snapshot) => snapshot,
                None => write_snapshot(&path.join(resident_snapshot_file(generation)))?,
            };
            #[cfg(test)]
            out_of_core::resident_checkpoint_fault(out_of_core::ResidentCheckpointStep::Snapshot)?;
            let publication = out_of_core::write_resident_artifacts(
                self,
                path,
                &snapshot,
                out_of_core::ResidentPublicationPlan {
                    generation,
                    deferred_aliases,
                    #[cfg(feature = "vector-search")]
                    rabitq,
                },
                task,
            )?;
            let projection_bytes_written = publication
                .bytes_written
                .saturating_add(rabitq_bytes_written);
            *self
                .durable_source_graph_commit_epoch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = self.source_graph_commit_epoch;
            // Every published artifact now carries the current epoch.
            *self
                .artifact_source_graph_commit_epoch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            #[cfg(test)]
            out_of_core::resident_checkpoint_fault(out_of_core::ResidentCheckpointStep::Cleanup)?;
            let cleanup = self.retry_projection_cleanup_under_publication(
                self.cleanup_options,
                publication_lease,
            );
            if let Some(binding) = &self.consumer_binding {
                *self
                    .consumer_receipt
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) =
                    Some(consumer::CheckpointReceipt {
                        binding: binding.clone(),
                        source_epoch: self.source_graph_commit_epoch.ok_or_else(|| {
                            HawDBError::Storage("registered checkpoint missing source epoch".into())
                        })?,
                        encoded_len: snapshot.encoded_len,
                        sha256: snapshot.encoded_sha256.to_string(),
                    });
            }
            Ok(snapshot.finish(
                publication.generation,
                projection_bytes_written,
                Some(SearchProjectionCleanupOutcome::from(&cleanup)),
            ))
        })();
        if let Some(telemetry) = &self.telemetry {
            let (byte_count, generation) = result
                .as_ref()
                .map(|report| {
                    (
                        report
                            .snapshot_compressed_bytes
                            .saturating_add(report.projection_bytes_written),
                        Some(report.projection_generation),
                    )
                })
                .unwrap_or((0, None));
            telemetry.record_kernel(KernelTelemetry {
                operation: KernelTelemetryOperation::SearchCheckpoint,
                success: result.is_ok(),
                elapsed_micros: elapsed_micros(started),
                item_count: self.documents.len(),
                byte_count,
                fsync_micros: 0,
                generation,
            });
        }
        result
    }

    #[cfg(feature = "vector-search")]
    fn write_rabitq_projection(
        &self,
        path: &Path,
        minimum_generation: u64,
        task: &RuntimeTaskContext,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> Result<(u64, u64, Option<out_of_core::ResidentRaBitQArtifact>)> {
        if self
            .documents
            .values()
            .all(|document| document.embedding.is_none())
        {
            self.invalidate_rabitq_projection();
            return Ok((minimum_generation, 0, None));
        }
        let loaded_generation = self
            .rabitq_projection()
            .map(|projection| projection.manifest().identity.generation)
            .unwrap_or(0);
        let artifact_generation = latest_rabitq_artifact(path)?
            .map(|(generation, _)| generation)
            .unwrap_or(0);
        let generation = minimum_generation
            .max(loaded_generation.saturating_add(1))
            .max(artifact_generation.saturating_add(1));
        let artifact_path = path.join(rabitq_artifact_file(generation));
        if let Some(artifact) =
            resident_diskann::artifact::write(self, &artifact_path, generation, task, publication)?
        {
            self.invalidate_rabitq_projection();
            return Ok((generation, artifact.reference.byte_length, Some(artifact)));
        }
        self.vector_scoring
            .validate(self.vector_index_descriptor.is_some())?;
        let projection = RaBitQCandidateProjection::write_from_view(
            &artifact_path,
            &self.documents,
            self.rabitq_projection_identity(generation),
            self.rabitq_build_options,
            self.vector_metric(),
        )?
        .ok_or_else(|| {
            HawDBError::Storage(
                "HawDB RaBitQ projection build produced no artifact for vector documents"
                    .to_string(),
            )
        })?;
        let bytes_written = fs::metadata(&artifact_path)?.len();
        let (reference, checksum) = projection.file_artifact_reference().ok_or_else(|| {
            HawDBError::StorageIntegrity(
                "resident RaBitQ producer did not return an artifact reference".into(),
            )
        })?;
        let artifact = out_of_core::ResidentRaBitQArtifact {
            reference,
            checksum,
            source_digest: projection.manifest().source_digest,
            vector_document_count: projection.manifest().document_count,
            payload_checksum: projection.manifest().payload_checksum,
            peak_build_working_bytes: projection.manifest().peak_build_working_bytes,
            diskann_source_digest: None,
        };
        *self
            .rabitq_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(projection));
        Ok((generation, bytes_written, Some(artifact)))
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// Sources exceeding the optional graph cache use scalar search; use
    /// [`Self::search_with_report`] for the fallback diagnostic.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    pub fn search(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        limit: usize,
    ) -> Result<Vec<SearchHit>> {
        self.search_with_report(query_text, query_embedding, mode, limit)
            .map(|result| result.hits)
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    pub fn search_with_report(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        limit: usize,
    ) -> Result<SearchResultSet> {
        self.search_with_options(
            query_text,
            query_embedding,
            mode,
            SearchQueryOptions {
                limit,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    /// Sources exceeding the optional graph cache use scalar search and report
    /// [`SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded`].
    pub fn search_with_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::default_vector(false, VectorSearchExecutionOptions::default()),
            None,
        )
    }

    /// Exhaustively score the complete eligible resident vector domain.
    pub fn search_with_options_exact(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::default_vector(
                false,
                VectorSearchExecutionOptions::default().with_exact_search(),
            ),
            None,
        )
    }

    /// Default candidates under the caller's memory and cancellation limits.
    /// Explicit RaBitQ selection refuses build/cache or query budget failures.
    /// The ordinary graph may use scalar search with a reported cache fallback.
    pub fn try_search_with_options_context(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        vector_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::default_vector(true, vector_options),
            None,
        )
    }

    pub fn try_search_with_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::default_vector(true, VectorSearchExecutionOptions::default()),
            None,
        )
    }

    pub fn try_search_with_options_access_control(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        access_control: SearchAccessControlContext,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::default_vector(true, VectorSearchExecutionOptions::default()),
            Some(&access_control),
        )
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    pub fn search_with_options_prefer_compressed_vector_projection(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchResultSet> {
        self.search_with_options_compressed_vector_projection_mode(
            query_text,
            query_embedding,
            mode,
            options,
            CompressedVectorSearchMode::Preferred,
        )
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    pub fn search_with_options_compressed_vector_projection_mode(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            AdaptiveVectorSearchOptions::new(compressed_vector_search_mode),
            AdaptiveVectorExecutionControls::IN_MEMORY,
        )
    }

    pub fn try_search_with_options_compressed_vector_projection_mode(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            AdaptiveVectorSearchOptions::new(compressed_vector_search_mode),
            AdaptiveVectorExecutionControls::PERSISTED,
        )
    }

    pub fn try_search_with_options_adaptive_vector_projection(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            adaptive_options,
            AdaptiveVectorExecutionControls::PERSISTED,
        )
    }

    pub fn try_search_with_options_adaptive_vector_projection_context(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
        vector_execution_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchResultSet> {
        if let Some(task_context) = vector_execution_options.task_context {
            task_context
                .checkpoint()
                .map_err(HawDBError::RuntimeTaskStopped)?;
        }
        let result = self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            adaptive_options,
            AdaptiveVectorExecutionControls::persisted(vector_execution_options),
        )?;
        if let Some(task_context) = vector_execution_options.task_context {
            task_context
                .checkpoint()
                .map_err(HawDBError::RuntimeTaskStopped)?;
        }
        Ok(result)
    }

    /// Searches resident documents, returning unavailable-capability and execution errors.
    /// This entrypoint preserves in-memory payload access; `try_*` methods may read persisted ranges.
    pub fn search_with_options_adaptive_vector_projection(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            adaptive_options,
            AdaptiveVectorExecutionControls::IN_MEMORY,
        )
    }

    pub fn validate_sampled_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> VectorRecallValidationReport {
        self.validate_sampled_vector_recall_with_controls(
            options,
            AdaptiveVectorExecutionControls::IN_MEMORY,
            AdaptiveVectorExecutionControls::RECALL_CANDIDATES,
        )
    }

    /// Validate the explicit compressed candidate path rather than the default engine.
    pub fn validate_sampled_compressed_vector_recall(
        &self,
        options: VectorRecallValidationOptions,
    ) -> VectorRecallValidationReport {
        self.validate_sampled_vector_recall_with_backend(
            options,
            AdaptiveVectorExecutionControls::IN_MEMORY,
            AdaptiveVectorExecutionControls::RECALL_CANDIDATES,
            VectorRecallProbeBackend::Compressed,
        )
    }

    fn validate_sampled_vector_recall_with_controls(
        &self,
        options: VectorRecallValidationOptions,
        exact_controls: AdaptiveVectorExecutionControls<'_>,
        approximate_controls: AdaptiveVectorExecutionControls<'_>,
    ) -> VectorRecallValidationReport {
        self.validate_sampled_vector_recall_with_backend(
            options,
            exact_controls,
            approximate_controls,
            VectorRecallProbeBackend::Default,
        )
    }

    fn validate_sampled_vector_recall_with_backend(
        &self,
        options: VectorRecallValidationOptions,
        exact_controls: AdaptiveVectorExecutionControls<'_>,
        approximate_controls: AdaptiveVectorExecutionControls<'_>,
        backend: VectorRecallProbeBackend,
    ) -> VectorRecallValidationReport {
        let predicate_pushdown = search_metadata_predicate_pushdown(&options.metadata_filters);
        let eligible = || {
            self.documents.values().filter(|document| {
                document.embedding.is_some()
                    && search_document_matches_predicates(document, &predicate_pushdown.predicates)
            })
        };
        let sample_candidate_count = eligible().count();
        let mut accumulator =
            VectorRecallValidationAccumulator::new(sample_candidate_count, &options)
                .with_backend(backend.name());
        if predicate_pushdown.report.parse_error.is_some() {
            accumulator.mark_metadata_filter_invalid();
            return accumulator.finish();
        }
        if accumulator.requested_sample_count() == 0 || accumulator.top_k() == 0 {
            return accumulator.finish();
        }
        let sample_positions =
            sample_positions(sample_candidate_count, accumulator.requested_sample_count())
                .into_iter()
                .collect::<BTreeSet<_>>();
        let sample_documents = eligible()
            .enumerate()
            .filter(|(index, _)| sample_positions.contains(index))
            .map(|(_, document)| document)
            .collect::<Vec<_>>();
        let query_limit = accumulator.top_k().saturating_add(1);

        for document in sample_documents {
            let embedding = document
                .embedding
                .as_deref()
                .expect("sampled vector document has an embedding");
            let exact_query_options = SearchQueryOptions {
                limit: query_limit,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: options.metadata_filters.clone(),
                policy_epoch: None,
            };
            let Ok(exact) = self
                .try_search_with_options_compressed_vector_projection_mode_internal(
                    "",
                    Some(embedding),
                    SearchMode::Vector,
                    exact_query_options.clone(),
                    AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Disabled)
                        .as_recall_validation_probe(),
                    exact_controls,
                )
            else {
                accumulator.mark_probe_execution_failed();
                break;
            };
            let approximate_query_options = SearchQueryOptions {
                rank_window: Some(accumulator.candidate_limit().saturating_add(1)),
                ..exact_query_options
            };
            let approximate = match backend {
                VectorRecallProbeBackend::Default => self
                    .try_search_with_options_using_vector_backend(
                        "",
                        Some(embedding),
                        SearchMode::Vector,
                        approximate_query_options,
                        SearchExecutionStrategy::default_vector(
                            approximate_controls.use_physical_range_reads,
                            approximate_controls.vector_execution_options,
                        ),
                        None,
                    ),
                VectorRecallProbeBackend::Compressed => self
                    .try_search_with_options_compressed_vector_projection_mode_internal(
                        "",
                        Some(embedding),
                        SearchMode::Vector,
                        approximate_query_options,
                        AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Required),
                        approximate_controls,
                    ),
            };
            let Ok(approximate) = approximate else {
                accumulator.mark_probe_execution_failed();
                break;
            };
            let exact_ids =
                recall_validation_hit_ids(&exact, document.id.as_str(), accumulator.top_k());
            let approximate_ids =
                recall_validation_hit_ids(&approximate, document.id.as_str(), accumulator.top_k());
            let approximate_retriever = approximate
                .retrievers
                .iter()
                .find(|retriever| retriever.name == "vector")
                .expect("vector search always reports the vector retriever");
            let candidate_ids = recall_validation_candidate_ids(
                &approximate_retriever.candidate_top_ids,
                document.id.as_str(),
                accumulator.candidate_limit(),
            );
            accumulator.record(
                &exact_ids,
                &candidate_ids,
                &approximate_ids,
                approximate_retriever,
            );
        }

        accumulator.finish()
    }

    pub fn validate_sampled_vector_recall_access_control(
        &self,
        options: VectorRecallValidationOptions,
        access_control: &SearchAccessControlContext,
    ) -> VectorRecallValidationReport {
        self.validate_sampled_vector_recall_access_control_backend(
            options,
            access_control,
            VectorRecallProbeBackend::Default,
        )
    }

    pub fn validate_sampled_compressed_vector_recall_access_control(
        &self,
        options: VectorRecallValidationOptions,
        access_control: &SearchAccessControlContext,
    ) -> VectorRecallValidationReport {
        self.validate_sampled_vector_recall_access_control_backend(
            options,
            access_control,
            VectorRecallProbeBackend::Compressed,
        )
    }

    fn validate_sampled_vector_recall_access_control_backend(
        &self,
        mut options: VectorRecallValidationOptions,
        access_control: &SearchAccessControlContext,
        backend: VectorRecallProbeBackend,
    ) -> VectorRecallValidationReport {
        match access_control.effective_metadata_filters(&options.metadata_filters) {
            Ok(filters) => options.metadata_filters = filters,
            Err(_) => {
                let mut accumulator = VectorRecallValidationAccumulator::new(0, &options)
                    .with_backend(backend.name());
                accumulator.mark_metadata_filter_invalid();
                return accumulator.finish();
            }
        }
        self.validate_sampled_vector_recall_with_backend(
            options,
            AdaptiveVectorExecutionControls::IN_MEMORY,
            AdaptiveVectorExecutionControls::RECALL_CANDIDATES,
            backend,
        )
    }

    pub fn vector_projection_qualification_identity(
        &self,
    ) -> Option<VectorProjectionQualificationIdentity> {
        #[cfg(feature = "vector-search")]
        {
            if !self.rabitq_backend_selected() {
                return self.resident_diskann.qualification_identity(self);
            }
            let projection = self.rabitq_projection()?;
            let manifest = projection.manifest();
            Some(VectorProjectionQualificationIdentity {
                projection_generation: manifest.identity.generation,
                source_graph_commit_epoch: manifest.identity.source_epoch,
                document_count: manifest.document_count,
                source_digest: manifest.source_digest,
                payload_bytes: manifest.payload_bytes,
                payload_checksum: manifest.payload_checksum,
                format_version: manifest.format_version,
                algorithm: manifest.algorithm.clone(),
                bit_width: manifest.bit_width,
                dimension: manifest.dimension,
                transform_seed: manifest.transform_seed,
                embedding_model: manifest.identity.embedding_model.clone(),
                embedding_version: manifest.identity.embedding_version.clone(),
                file_backed: projection.is_file_backed(),
            })
        }
        #[cfg(not(feature = "vector-search"))]
        {
            None
        }
    }

    pub fn vector_projection_resource_evidence(&self) -> Option<VectorProjectionResourceEvidence> {
        #[cfg(feature = "vector-search")]
        {
            // Resident graph snapshots do not persist construction measurements.
            // Never manufacture their resource evidence from an unrelated codec.
            if !self.rabitq_backend_selected() {
                return None;
            }
            let projection = self.rabitq_projection()?;
            let manifest = projection.manifest();
            let raw_vector_bytes = u64::try_from(manifest.document_count)
                .unwrap_or(u64::MAX)
                .saturating_mul(u64::try_from(manifest.dimension).unwrap_or(u64::MAX))
                .saturating_mul(std::mem::size_of::<f32>() as u64);
            let build_write_amplification_per_million = if raw_vector_bytes == 0 {
                0
            } else {
                manifest
                    .payload_bytes
                    .saturating_mul(1_000_000)
                    .checked_div(raw_vector_bytes)
                    .unwrap_or(u64::MAX)
            };
            Some(VectorProjectionResourceEvidence {
                segment_count: manifest.segments.len(),
                requested_segment_rows: manifest.requested_segment_rows,
                admitted_segment_rows: manifest.admitted_segment_rows,
                configured_build_working_bytes: manifest.configured_build_working_bytes,
                peak_build_working_bytes: manifest.peak_build_working_bytes,
                raw_vector_bytes,
                projection_payload_bytes: manifest.payload_bytes,
                build_write_amplification_per_million,
            })
        }
        #[cfg(not(feature = "vector-search"))]
        {
            None
        }
    }

    pub fn qualify_sampled_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> VectorRecallProductionQualificationReport {
        let recall = self.validate_sampled_vector_recall(options);
        VectorRecallProductionQualificationReport::evaluate(
            recall,
            self.vector_projection_qualification_identity()
                .unwrap_or_default(),
            evidence_binding,
            expected_identity,
        )
    }

    pub fn qualify_sampled_compressed_vector_recall_for_production(
        &self,
        options: VectorRecallValidationOptions,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> VectorRecallProductionQualificationReport {
        let recall = self.validate_sampled_compressed_vector_recall(options);
        VectorRecallProductionQualificationReport::evaluate(
            recall,
            self.vector_projection_qualification_identity()
                .unwrap_or_default(),
            evidence_binding,
            expected_identity,
        )
    }

    fn try_search_with_options_compressed_vector_projection_mode_internal(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
        controls: AdaptiveVectorExecutionControls,
    ) -> Result<SearchResultSet> {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::adaptive(
                vector_compression_preference(adaptive_options.compression_mode),
                adaptive_options.backend_policy,
                adaptive_options.recall_validation_probe,
                controls,
            ),
            None,
        )
    }

    fn prepare_adaptive_vector_backend(
        &self,
        request: AdaptiveVectorSearchRequest,
        filtered_documents: &[&SearchDocument],
        embedding_dimension: usize,
        execution: VectorSearchExecutionOptions<'_>,
    ) -> Result<PreparedAdaptiveVectorBackend> {
        let task = execution.task_context;
        task.map_or(Ok(()), crate::build_control::checkpoint)?;
        let document_count = self
            .documents
            .values()
            .filter(|document| document.embedding.is_some())
            .count();
        let filtered_document_count = filtered_documents
            .iter()
            .filter(|document| document.embedding.is_some())
            .count();
        let decision_input =
            |quantized_projection_available, quantized_projection_covered_document_count| {
                AdaptiveVectorBackendInput {
                    compression_preference: request.compression_preference,
                    document_count,
                    filtered_document_count,
                    embedding_dimension,
                    recall_validation_probe: request.recall_validation_probe,
                    quantized_projection_available,
                    quantized_projection_covered_document_count,
                }
            };
        let preliminary = select_adaptive_vector_backend(
            decision_input(true, filtered_document_count),
            request.policy,
        );
        if preliminary.backend == AdaptiveVectorBackend::ScalarFlat {
            return Ok(PreparedAdaptiveVectorBackend {
                decision: preliminary,
                fallback_reason: None,
                #[cfg(feature = "vector-search")]
                projection: None,
            });
        }

        #[cfg(feature = "vector-search")]
        {
            let cached = self.lock_rabitq_projection(task)?.clone();
            let projection = cached.or_else(|| {
                self.path
                    .is_none()
                    .then(|| {
                        self.build_in_memory_rabitq_projection_with_context(
                            task,
                            Some(execution.max_working_bytes),
                        )
                        .ok()
                        .flatten()
                    })
                    .flatten()
            });
            task.map_or(Ok(()), crate::build_control::checkpoint)?;
            let covered_document_count = projection
                .as_ref()
                .map(|projection| {
                    filtered_documents
                        .iter()
                        .filter(|document| {
                            document.embedding.is_some()
                                && projection.contains_document_id(document.id.as_str())
                        })
                        .count()
                })
                .unwrap_or(0);
            let decision = select_adaptive_vector_backend(
                decision_input(projection.is_some(), covered_document_count),
                request.policy,
            );
            let fallback_reason = adaptive_vector_fallback_reason(decision);
            Ok(PreparedAdaptiveVectorBackend {
                projection: (decision.backend == AdaptiveVectorBackend::QuantizedProjection)
                    .then_some(projection)
                    .flatten(),
                decision,
                fallback_reason,
            })
        }

        #[cfg(not(feature = "vector-search"))]
        {
            let decision = select_adaptive_vector_backend(decision_input(false, 0), request.policy);
            Ok(PreparedAdaptiveVectorBackend {
                decision,
                fallback_reason: adaptive_vector_fallback_reason(decision),
            })
        }
    }

    /// Executes externally generated candidates through HawDB's normal
    /// metadata filtering and raw-vector rerank path. This validation surface
    /// is not a production vector backend contract.
    #[cfg(feature = "qualification")]
    #[doc(hidden)]
    pub fn search_with_external_vector_candidates_for_validation(
        &self,
        candidates: &[(String, f64)],
        indexed_document_ids: &BTreeSet<String>,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> Result<SearchResultSet> {
        let mut strategy = SearchExecutionStrategy::fixed(
            VectorSearchBackend::ExternalValidation {
                candidates,
                indexed_document_ids,
            },
            None,
            false,
        );
        strategy.capture_candidate_ids = true;
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            strategy,
            None,
        )
    }

    fn try_search_with_options_using_vector_backend(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        strategy: SearchExecutionStrategy<'_>,
        access_control: Option<&SearchAccessControlContext>,
    ) -> Result<SearchResultSet> {
        if access_control.is_some() {
            self.runtime_capabilities
                .require(RuntimeCapability::AccessControl)?;
        }
        self.require_search_capabilities(mode)?;
        let SearchExecutionStrategy {
            vector_backend: vector_backend_request,
            vector_backend_fallback_reason,
            payload_access,
            capture_candidate_ids,
            vector_execution_options,
        } = strategy;
        let query_can_generate_candidates = vector_execution::query_can_generate_candidates(
            query_embedding,
            self.embedding_dimension,
            self.vector_metric(),
            vector_execution_options,
        )?;
        // Text-scoring modes stop analyzing once the analyzed-term cap is passed.
        let mut query_terms = if mode == SearchMode::Vector {
            tokenize(query_text, &self.analyzer_lexicon)
        } else {
            tokenize_query_bounded(query_text, &self.analyzer_lexicon, &self.lexical_config)?
        };
        let mut vector_fallback_reason_codes = Vec::new();
        let mut vector_fallback_reasons = Vec::new();
        if let Some(reason) = vector_backend_fallback_reason {
            vector_fallback_reason_codes
                .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
            vector_fallback_reasons.push(reason);
        }
        let limit = options.limit;
        let document_count = self.documents.len();
        let metadata_filters = match access_control {
            Some(access_control) => access_control.effective_query_filters(&options)?,
            None => options.metadata_filters.clone(),
        };
        let policy_epoch = access_control
            .map(|access_control| access_control.policy_epoch)
            .or(options.policy_epoch);
        let mut predicate_pushdown = search_metadata_predicate_pushdown(&metadata_filters);
        let range_read = if matches!(payload_access, SearchPayloadAccess::PrunedRanges) {
            self.read_pruned_search_segments(&predicate_pushdown.predicates)?
        } else {
            None
        };
        let filtered = match &range_read {
            Some(range_read) => filter_search_documents_with_persisted_segments(
                &range_read.documents,
                &predicate_pushdown.predicates,
                self.segment_descriptor
                    .as_ref()
                    .expect("range reads require a persisted segment descriptor"),
            ),
            None => filter_search_documents_with_segment_pruning(
                &self.documents,
                &predicate_pushdown.predicates,
                self.segment_descriptor.as_deref(),
            ),
        };
        predicate_pushdown.report.segment_count = filtered.segment_count;
        predicate_pushdown.report.pruned_segment_count = filtered.pruned_segment_count;
        predicate_pushdown.report.scanned_segment_count = filtered.scanned_segment_count;
        predicate_pushdown
            .report
            .segment_pruning_candidate_document_count =
            filtered.segment_pruning_candidate_document_count;
        predicate_pushdown.report.segment_pruned_document_count =
            filtered.segment_pruned_document_count;
        predicate_pushdown.report.segment_scanned_document_count =
            filtered.segment_scanned_document_count;
        predicate_pushdown.report.persisted_segment_descriptor_used =
            filtered.persisted_segment_descriptor_used;
        if let Some(range_read) = &range_read {
            predicate_pushdown.report.physical_range_read_count = range_read.range_count;
            predicate_pushdown.report.physical_bytes_read = range_read.bytes_read;
        }
        predicate_pushdown.report.field_summaries = filtered.field_summaries;
        let mut filtered_documents = filtered.documents;
        filtered_documents.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        let filtered_document_count = filtered_documents.len();
        let mut prepared_adaptive_backend = None;
        self.vector_scoring
            .validate(self.vector_index_descriptor.is_some())?;
        #[cfg(feature = "vector-search")]
        let default_without_candidates = mode != SearchMode::Text
            && matches!(vector_backend_request, VectorSearchBackendRequest::Default)
            && (limit == 0
                || !filtered_documents
                    .iter()
                    .any(|document| document.embedding.is_some()))
            && if self.rabitq_backend_selected() {
                self.lock_rabitq_projection(vector_execution_options.task_context)?
                    .is_none()
            } else {
                !self
                    .resident_diskann
                    .has_cached_projection(vector_execution_options.task_context)?
            };
        #[cfg(not(feature = "vector-search"))]
        let default_without_candidates = false;
        let vector_backend_request = if vector_execution_options.exact || default_without_candidates
        {
            VectorSearchBackendRequest::Fixed(VectorSearchBackend::Scalar)
        } else {
            vector_backend_request
        };
        #[cfg(feature = "vector-search")]
        let default_rabitq =
            if matches!(vector_backend_request, VectorSearchBackendRequest::Default)
                && self.rabitq_backend_selected()
                && mode != SearchMode::Text
                && query_can_generate_candidates
            {
                self.build_in_memory_rabitq_projection_with_context(
                    vector_execution_options.task_context,
                    Some(vector_execution_options.max_working_bytes),
                )?
            } else {
                None
            };
        let vector_backend = match vector_backend_request {
            VectorSearchBackendRequest::Default => {
                #[cfg(feature = "vector-search")]
                {
                    if let Some(projection) = default_rabitq.as_deref() {
                        VectorSearchBackend::RaBitQ {
                            projection,
                            required: true,
                        }
                    } else {
                        if !vector_execution_options.exact
                            && mode != SearchMode::Text
                            && query_can_generate_candidates
                        {
                            let candidate_limit = options
                                .offset
                                .saturating_add(limit)
                                .max(options.rank_window.unwrap_or(0))
                                .max(vector_execution_options.resident_candidate_window());
                            if self.resident_diskann.admits_default_search(
                                self,
                                &filtered_documents,
                                query_embedding.expect("valid query").len(),
                                candidate_limit,
                                vector_execution_options,
                            )? {
                                VectorSearchBackend::DiskAnn { index: self }
                            } else {
                                vector_fallback_reason_codes
                                    .push(SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded);
                                vector_fallback_reasons.push("resident DiskANN cache exceeds its memory envelope; using scalar vector scan".into());
                                VectorSearchBackend::Scalar
                            }
                        } else {
                            VectorSearchBackend::Scalar
                        }
                    }
                }
                #[cfg(not(feature = "vector-search"))]
                {
                    VectorSearchBackend::Scalar
                }
            }
            VectorSearchBackendRequest::Fixed(backend) => backend,
            VectorSearchBackendRequest::Adaptive(request)
                if mode != SearchMode::Text && query_can_generate_candidates =>
            {
                prepared_adaptive_backend = Some(
                    self.prepare_adaptive_vector_backend(
                        request,
                        &filtered_documents,
                        self.embedding_dimension
                            .unwrap_or_else(|| query_embedding.map_or(0, <[f32]>::len)),
                        vector_execution_options,
                    )?,
                );
                prepared_adaptive_backend
                    .as_ref()
                    .expect("adaptive backend was prepared")
                    .backend()
            }
            VectorSearchBackendRequest::Adaptive(_) => VectorSearchBackend::Scalar,
        };
        if let Some(reason) = prepared_adaptive_backend
            .as_ref()
            .and_then(|backend| backend.fallback_reason.as_ref())
        {
            vector_fallback_reason_codes
                .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
            vector_fallback_reasons.push(reason.clone());
        }
        let vector_backend_decision = prepared_adaptive_backend
            .as_ref()
            .map(|backend| backend.decision);
        let vector_filter_fields = predicate_pushdown
            .predicates
            .predicates()
            .iter()
            .map(|predicate| predicate.field().name().to_string())
            .collect::<Vec<_>>();
        let candidate_set = SearchCandidateSetReport {
            id_space: "search_projection_document_id".to_string(),
            representation: "sorted_document_ids".to_string(),
            cardinality: filtered_document_count,
            exact: true,
            snapshot_source_graph_commit_epoch: self.source_graph_commit_epoch,
            policy_epoch,
            filtered_out_count: document_count.saturating_sub(filtered_document_count),
            metadata_filters: options.metadata_filters.clone(),
            metadata_predicate_pushdown: predicate_pushdown.report,
        };
        let vector_available = match (query_embedding, self.embedding_dimension) {
            (Some(vector), Some(dimension)) if vector.len() == dimension => true,
            (Some(vector), Some(dimension)) => {
                vector_fallback_reason_codes
                    .push(SearchFallbackReasonCode::VectorDimensionMismatch);
                vector_fallback_reasons.push(format!(
                    "query embedding dimension {} does not match index dimension {dimension}",
                    vector.len()
                ));
                false
            }
            (Some(_), None) => {
                vector_fallback_reason_codes.push(SearchFallbackReasonCode::VectorIndexEmpty);
                vector_fallback_reasons.push("index has no vector rows".to_string());
                false
            }
            (None, _) => {
                if mode != SearchMode::Text {
                    vector_fallback_reason_codes
                        .push(SearchFallbackReasonCode::QueryEmbeddingMissing);
                    vector_fallback_reasons.push("query embedding not provided".to_string());
                }
                false
            }
        };
        let text_available = !query_terms.is_empty();
        let (mut text_fallback_reason_codes, mut text_fallback_reasons) =
            if !text_available && mode != SearchMode::Vector {
                (
                    vec![SearchFallbackReasonCode::TextQueryEmpty],
                    vec!["query text produced no searchable terms".to_string()],
                )
            } else {
                (Vec::new(), Vec::new())
            };
        let lexical_snapshot = if text_available
            && mode != SearchMode::Vector
            && matches!(payload_access, SearchPayloadAccess::PrunedRanges)
        {
            self.lexical_snapshot()
        } else {
            None
        };
        #[cfg(all(test, feature = "full-text-search"))]
        if lexical_snapshot.is_some() {
            lexical_snapshot_test_gate::pause_after_capture();
        }
        // The segmented projection bounds scored terms; long queries keep
        // their most selective terms instead of failing the whole search.
        if let Some((projection, delta)) = &lexical_snapshot {
            let selection = projection.fit_query_terms(
                &mut query_terms,
                delta,
                self.lexical_config.max_term_bytes,
                self.lexical_config.query_term_policy,
                None,
            )?;
            if selection.reduced() {
                text_fallback_reason_codes.push(SearchFallbackReasonCode::TextQueryTermsReduced);
                text_fallback_reasons.push(selection.to_string());
            }
        }
        // Without a projection, resident BM25 applies the same caps and the
        // same selection as the resident text seed over the filtered corpus.
        if text_available && mode != SearchMode::Vector && lexical_snapshot.is_none() {
            let selection = resident_bm25::select_query_terms(
                &mut query_terms,
                filtered_documents.iter().copied(),
                &self.analyzer_lexicon,
                &self.lexical_config,
            )?;
            if selection.reduced() {
                text_fallback_reason_codes.push(SearchFallbackReasonCode::TextQueryTermsReduced);
                text_fallback_reasons.push(selection.to_string());
            }
        }
        let mut text_corpus =
            if text_available && mode != SearchMode::Vector && lexical_snapshot.is_none() {
                Some(resident_bm25::Scorer::from_documents(
                    &query_terms,
                    filtered_documents.iter().copied(),
                    &self.analyzer_lexicon,
                )?)
            } else {
                None
            };
        let projection_freshness = self.projection_freshness();
        let (vector_index_covered_document_count, vector_index_candidate_document_count) =
            vector_backend.index_coverage(&filtered_documents);

        let mut vector_execution = if vector_available && mode != SearchMode::Text {
            Some(execute_search_vector_plan(SearchVectorExecutionRequest {
                query_embedding: query_embedding
                    .expect("vector_available requires query embedding"),
                metric: self.vector_metric(),
                scoring: self.vector_scoring,
                documents: &filtered_documents,
                backend: vector_backend,
                filter_fields: vector_filter_fields,
                limit: options.offset.saturating_add(limit),
                rank_window: options.rank_window,
                capture_candidate_ids,
                vector_execution_options,
                fallback_reason_codes: &mut vector_fallback_reason_codes,
                fallback_reasons: &mut vector_fallback_reasons,
            })?)
        } else {
            None
        };
        let vector_scores = vector_execution
            .as_ref()
            .map(|execution| execution.scores.clone())
            .unwrap_or_default();
        let retained_text_score_limit = match mode {
            SearchMode::Text => Some(options.offset.saturating_add(limit)),
            SearchMode::Hybrid => options.rank_window,
            SearchMode::Vector => Some(0),
        };
        let lexical_report = lexical_snapshot
            .as_ref()
            .map(|(projection, delta)| {
                projection.score_with_stable_filter(
                    &query_terms,
                    delta,
                    retained_text_score_limit,
                    |id| {
                        Ok(filtered_documents
                            .binary_search_by(|document| document.id.as_str().cmp(id))
                            .is_ok())
                    },
                )
            })
            .transpose()?;
        let (
            mut text_scores,
            lexical_matching_document_count,
            lexical_postings_visited,
            lexical_bytes_read,
            lexical_blocks_skipped,
        ) = if let Some(report) = lexical_report {
            (
                report.scores,
                report.matching_document_count,
                report.postings_visited,
                report.bytes_read,
                report.blocks_skipped,
            )
        } else {
            (BTreeMap::new(), 0, 0, 0, 0)
        };
        if let Some(corpus) = &mut text_corpus {
            for document in &filtered_documents {
                let text_score = corpus.score(document, &self.analyzer_lexicon)?;
                if text_score > 0.0 {
                    text_scores.insert(document.id.clone(), text_score);
                }
            }
        }
        let segmented_lexical_projection_used = lexical_snapshot.is_some();
        let text_candidate_count = if segmented_lexical_projection_used {
            lexical_matching_document_count
        } else {
            text_scores.len()
        };
        let mut fallback_reason_codes = vector_fallback_reason_codes.clone();
        fallback_reason_codes.extend(text_fallback_reason_codes.iter().copied());
        let mut fallback_reasons = vector_fallback_reasons.clone();
        fallback_reasons.extend(text_fallback_reasons.iter().cloned());

        let vector_ranks = ranked_scores(&vector_scores);
        let text_ranks = ranked_scores(&text_scores);
        let vector_window_ranks = window_ranks(&vector_ranks, options.rank_window);
        let text_window_ranks = window_ranks(&text_ranks, options.rank_window);
        let mut retrievers = vec![
            SearchRetrieverReport {
                name: "vector".to_string(),
                backend: vector_backend.report_name().to_string(),
                backend_selection_reason: vector_backend_decision.map(|decision| decision.reason),
                estimated_raw_vector_bytes: vector_backend_decision
                    .map(|decision| decision.estimated_raw_vector_bytes),
                filter_selectivity_per_million: vector_backend_decision
                    .map(|decision| decision.filter_selectivity_per_million),
                available: vector_available && mode != SearchMode::Text,
                input_candidate_set: candidate_set.clone(),
                candidate_score_source: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.candidate_score_source.as_str())
                    .unwrap_or("none")
                    .to_string(),
                final_score_source: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.final_score_source.as_str())
                    .unwrap_or("none")
                    .to_string(),
                generated_candidate_count: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.generated_candidate_count)
                    .unwrap_or(0),
                candidate_scan_rounds: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.candidate_scan_rounds)
                    .unwrap_or(0),
                descriptor_pruned_count: candidate_set
                    .metadata_predicate_pushdown
                    .segment_pruned_document_count,
                scalar_filtered_count: candidate_set
                    .filtered_out_count
                    .saturating_sub(
                        candidate_set
                            .metadata_predicate_pushdown
                            .segment_pruned_document_count,
                    )
                    .saturating_add(
                        vector_execution
                            .as_ref()
                            .map(|execution| execution.report.residual_filtered_count)
                            .unwrap_or(0),
                    ),
                residual_filtered_count: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.residual_filtered_count)
                    .unwrap_or(0),
                reranked_candidate_count: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.reranked_candidate_count)
                    .unwrap_or(0),
                raw_vector_bytes_read: vector_execution
                    .as_ref()
                    .map(|execution| execution.report.raw_vector_bytes_read)
                    .unwrap_or(0),
                candidate_scan_kernel: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.kernel.clone()),
                candidate_scan_worker_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.worker_count)
                    .unwrap_or(0),
                candidate_scan_segment_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.segment_count)
                    .unwrap_or(0),
                candidate_scan_scanned_segment_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.scanned_segment_count)
                    .unwrap_or(0),
                candidate_scan_scored_document_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.scored_document_count)
                    .unwrap_or(0),
                candidate_scan_filtered_document_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.filtered_document_count)
                    .unwrap_or(0),
                candidate_scan_scanned_block_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.scanned_block_count)
                    .unwrap_or(0),
                candidate_scan_skipped_block_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.skipped_block_count)
                    .unwrap_or(0),
                candidate_scan_adaptive_exact_scan_count: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.adaptive_exact_scan_count)
                    .unwrap_or(0),
                candidate_scan_payload_bytes_read: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.payload_bytes_read)
                    .unwrap_or(0),
                candidate_scan_admitted_working_bytes: vector_execution
                    .as_ref()
                    .and_then(|execution| execution.report.candidate_scan_metrics.as_ref())
                    .map(|metrics| metrics.admitted_working_bytes)
                    .unwrap_or(0),
                posting_bytes_read: 0,
                candidate_postings_visited: 0,
                candidate_blocks_skipped: 0,
                segmented_lexical_projection_used: false,
                index_covered_document_count: vector_index_covered_document_count,
                index_candidate_document_count: vector_index_candidate_document_count,
                index_coverage_complete: vector_index_covered_document_count
                    == vector_index_candidate_document_count,
                candidate_count: vector_scores.len(),
                candidate_set: retriever_candidate_set_report(
                    vector_window_ranks.len(),
                    self.source_graph_commit_epoch,
                    options.policy_epoch,
                    vector_execution.as_ref().is_some_and(|execution| {
                        execution.report.candidate_score_source
                            == hawdb_executor::VectorScoreSource::RawVector
                    }),
                ),
                fallback_reason_codes: vector_fallback_reason_codes,
                fallback_reasons: vector_fallback_reasons,
                candidate_top_ids: vector_execution
                    .as_mut()
                    .map(|execution| std::mem::take(&mut execution.candidate_ids))
                    .unwrap_or_default(),
                top_hit_ids: top_ranked_ids(&vector_window_ranks, limit),
                top_candidates: top_ranked_candidates(&vector_window_ranks, &vector_scores, limit),
            },
            SearchRetrieverReport {
                name: "text".to_string(),
                backend: if segmented_lexical_projection_used {
                    "segmented_bm25_text"
                } else {
                    "bm25_text"
                }
                .to_string(),
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                available: text_available && mode != SearchMode::Vector,
                input_candidate_set: candidate_set.clone(),
                candidate_score_source: if text_available && mode != SearchMode::Vector {
                    if segmented_lexical_projection_used {
                        "segmented_bm25"
                    } else {
                        "bm25"
                    }
                } else {
                    "none"
                }
                .to_string(),
                final_score_source: if text_available && mode != SearchMode::Vector {
                    if segmented_lexical_projection_used {
                        "segmented_bm25"
                    } else {
                        "bm25"
                    }
                } else {
                    "none"
                }
                .to_string(),
                generated_candidate_count: text_candidate_count,
                candidate_scan_rounds: 0,
                descriptor_pruned_count: candidate_set
                    .metadata_predicate_pushdown
                    .segment_pruned_document_count,
                scalar_filtered_count: candidate_set.filtered_out_count.saturating_sub(
                    candidate_set
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
                segmented_lexical_projection_used,
                index_covered_document_count: 0,
                index_candidate_document_count: 0,
                index_coverage_complete: true,
                candidate_count: text_candidate_count,
                candidate_set: retriever_candidate_set_report(
                    text_window_ranks.len(),
                    self.source_graph_commit_epoch,
                    options.policy_epoch,
                    true,
                ),
                fallback_reason_codes: text_fallback_reason_codes,
                fallback_reasons: text_fallback_reasons,
                candidate_top_ids: Vec::new(),
                top_hit_ids: top_ranked_ids(&text_window_ranks, limit),
                top_candidates: top_ranked_candidates(&text_window_ranks, &text_scores, limit),
            },
        ];
        for retriever in &mut retrievers {
            if retriever.name == "vector" {
                for candidate in &mut retriever.top_candidates {
                    candidate.score = self.vector_scoring.exposed(candidate.score);
                }
            }
        }
        let mut scored_candidates = vector_scores
            .keys()
            .chain(text_scores.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|id| {
                let vector_score = vector_scores.get(&id).copied().unwrap_or(0.0);
                let text_score = text_scores.get(&id).copied().unwrap_or(0.0);
                let vector_rank = match mode {
                    SearchMode::Hybrid => vector_window_ranks.get(&id).copied(),
                    SearchMode::Vector | SearchMode::Text => vector_ranks.get(&id).copied(),
                };
                let text_rank = match mode {
                    SearchMode::Hybrid => text_window_ranks.get(&id).copied(),
                    SearchMode::Vector | SearchMode::Text => text_ranks.get(&id).copied(),
                };
                let vector_rrf_score = rrf_child_score(vector_rank);
                let text_rrf_score = rrf_child_score(text_rank);
                let rrf_score =
                    weighted_rrf_score(vector_rrf_score, text_rrf_score, options.fusion_weights);
                let score = match mode {
                    SearchMode::Hybrid => rrf_score,
                    SearchMode::Vector => vector_score,
                    SearchMode::Text => text_score,
                };
                (score > 0.0).then_some(SearchScoredCandidate {
                    id,
                    score,
                    vector_score,
                    text_score,
                    rrf_score,
                    vector_rrf_score,
                    text_rrf_score,
                    vector_rank,
                    text_rank,
                })
            })
            .collect::<Vec<_>>();
        scored_candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        let total_hits = if mode == SearchMode::Text && segmented_lexical_projection_used {
            lexical_matching_document_count
        } else {
            scored_candidates.len()
        };
        let page_end = options.offset.saturating_add(limit);
        let truncated = total_hits > page_end;
        let mut hits = Vec::with_capacity(limit.min(scored_candidates.len()));
        for candidate in scored_candidates
            .into_iter()
            .skip(options.offset)
            .take(limit)
        {
            let document = filtered_documents
                .binary_search_by(|document| document.id.cmp(&candidate.id))
                .ok()
                .and_then(|index| filtered_documents.get(index).copied())
                .ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search candidate {} is missing from the filtered document set",
                        candidate.id
                    ))
                })?;
            let (matched_terms, matched_spans) = query_matches_bounded(
                &query_terms,
                document,
                &self.analyzer_lexicon,
                usize::MAX,
                u64::MAX,
                0,
            )
            .expect("unbounded matched-span collection cannot exhaust its admission limit");
            hits.push(SearchHit {
                id: candidate.id,
                score: if mode == SearchMode::Vector {
                    self.vector_scoring.exposed(candidate.score)
                } else {
                    candidate.score
                },
                vector_score: self.vector_scoring.exposed(candidate.vector_score),
                text_score: candidate.text_score,
                rrf_score: candidate.rrf_score,
                vector_rrf_score: candidate.vector_rrf_score,
                text_rrf_score: candidate.text_rrf_score,
                vector_rank: candidate.vector_rank,
                text_rank: candidate.text_rank,
                kind: document.metadata.get("kind").cloned(),
                external_id: document.metadata.get("external_id").cloned(),
                source_id: document.metadata.get("source_id").cloned(),
                matched_terms,
                matched_spans,
                fallback_reason_codes: fallback_reason_codes.clone(),
                fallback_reasons: fallback_reasons.clone(),
                projection_freshness: projection_freshness.clone(),
            });
        }
        let truncation_reasons = if truncated && options.offset > 0 {
            vec![format!(
                "offset {} limit {limit} returned from {total_hits} matching hits",
                options.offset
            )]
        } else if truncated {
            vec![format!(
                "limit {limit} returned from {total_hits} matching hits"
            )]
        } else {
            Vec::new()
        };
        let truncation_reason_codes = if truncated {
            vec![SearchTruncationReasonCode::LimitExceeded]
        } else {
            Vec::new()
        };
        let empty_reasons = search_empty_reasons(
            hits.is_empty(),
            document_count,
            filtered_document_count,
            total_hits,
            SearchPageWindow {
                offset: options.offset,
                limit,
            },
            &truncation_reasons,
            &fallback_reasons,
        );
        let empty_reason_codes = search_empty_reason_codes(
            hits.is_empty(),
            document_count,
            filtered_document_count,
            total_hits,
        );
        Ok(SearchResultSet {
            hits,
            total_hits,
            limit,
            offset: options.offset,
            truncated,
            truncation_reason_codes,
            truncation_reasons,
            empty_reason_codes,
            empty_reasons,
            fallback_reason_codes,
            fallback_reasons,
            retrievers,
            candidate_set,
            rank_window: options.rank_window,
            fusion_weights: options.fusion_weights,
            document_count,
            filtered_document_count,
            projection_freshness,
        })
    }

    fn require_search_capabilities(&self, mode: SearchMode) -> Result<()> {
        if matches!(mode, SearchMode::Text | SearchMode::Hybrid) {
            self.runtime_capabilities
                .require(RuntimeCapability::FullTextSearch)?;
        }
        if matches!(mode, SearchMode::Vector | SearchMode::Hybrid) {
            self.runtime_capabilities
                .require(RuntimeCapability::VectorSearch)?;
        }
        Ok(())
    }

    pub fn mark_full_reindex_needed(&self, reason: &str) -> Result<()> {
        self.append_marker(FULL_REINDEX_MARKER, reason)
    }

    pub fn full_reindex_needed(&self) -> bool {
        self.read_marker_lines(FULL_REINDEX_MARKER)
            .map(|lines| !lines.is_empty())
            .unwrap_or(false)
    }

    pub fn mark_metadata_repair_needed(&self, reason: &str) -> Result<()> {
        self.write_marker(METADATA_REPAIR_MARKER, reason)
    }

    pub fn metadata_repair_needed(&self) -> bool {
        self.read_marker_lines(METADATA_REPAIR_MARKER)
            .map(|lines| !lines.is_empty())
            .unwrap_or(false)
    }

    fn validate_or_set_dimension(&mut self, dimension: usize) -> Result<()> {
        if let Some(manifest) = &self.embedding_manifest
            && manifest.dimension != dimension
        {
            return Err(HawDBError::Storage(format!(
                "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                manifest.dimension
            )));
        }
        match self.embedding_dimension {
            Some(existing) if existing != dimension => Err(HawDBError::Storage(format!(
                "embedding dimension mismatch: index has {existing}, row has {dimension}"
            ))),
            Some(_) => Ok(()),
            None => {
                self.embedding_dimension = Some(dimension);
                Ok(())
            }
        }
    }

    fn load_snapshot(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot_path = path.join(SEARCH_SNAPSHOT_FILE);
        if !fs::try_exists(&snapshot_path)? {
            return Ok(());
        }
        let text = read_search_snapshot_text(&snapshot_path)?;
        self.parse_snapshot_text(&text)
    }

    /// Read the registered snapshot through the original native directory.
    /// A saved pathname may now designate another projection namespace.
    fn load_snapshot_from_publication(
        &mut self,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> Result<()> {
        let mut file = publication.open_captured_child(SEARCH_SNAPSHOT_FILE)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let text = decode_search_snapshot_envelope(&bytes)?;
        self.parse_snapshot_text(&text)
    }

    fn parse_snapshot_text(&mut self, text: &str) -> Result<()> {
        let (body, checksum) = split_checksum(text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(HawDBError::Storage(format!(
                "search projection checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        for line in body.lines() {
            if line == "HAWDB_SEARCH_PROJECTION_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["canonical_vector_source_epoch", epoch] => {
                    if self.canonical_vector_source_epoch.is_some() {
                        return Err(vector_index::invalid("duplicate canonical vector source"));
                    }
                    self.canonical_vector_source_epoch =
                        Some(parse_u64(epoch, "canonical vector source epoch")?);
                }
                ["vector_document_summary", documents, vectors, digest] => {
                    if self.vector_document_summary.is_some() || !self.documents.is_empty() {
                        return Err(vector_index::invalid(
                            "duplicate or misplaced vector summary",
                        ));
                    }
                    let summary = vector_index::VectorDocumentSummary {
                        document_count: parse_usize(documents, "logical vector documents")?,
                        vector_document_count: parse_usize(vectors, "vector-bearing documents")?,
                        documents_digest: parse_u64(digest, "vector documents digest")?,
                    };
                    summary.validate()?;
                    self.vector_document_summary = Some(summary);
                }
                ["vector_scoring", "linear_squared_l2"] => {
                    if !self.vector_scoring.is_default() || !self.documents.is_empty() {
                        return Err(vector_index::invalid(
                            "duplicate or misplaced vector scoring",
                        ));
                    }
                    self.vector_scoring = SearchVectorScoring::LinearSquaredL2;
                }
                ["vector_index_binding", encoded] => {
                    if self.vector_index_descriptor.is_some() || !self.documents.is_empty() {
                        return Err(HawDBError::StorageIntegrity(
                            "duplicate or misplaced vector index binding".into(),
                        ));
                    }
                    self.restore_vector_index(vector_index::decode(encoded)?);
                }
                ["projection_consumer_binding", database, projection, id, registration, checkpoint] =>
                {
                    if body.lines().nth(1) != Some(line) {
                        return Err(HawDBError::Storage(
                            "projection consumer binding must follow the snapshot header".into(),
                        ));
                    }
                    if self.consumer_binding.is_some() {
                        return Err(HawDBError::Storage(
                            "duplicate projection consumer binding".into(),
                        ));
                    }
                    self.consumer_binding = Some(consumer::ConsumerBinding::parse(
                        database,
                        projection,
                        id,
                        registration,
                        checkpoint,
                    )?);
                }
                ["source_graph_commit_epoch", raw] => {
                    if self.source_graph_commit_epoch.is_some() {
                        return Err(HawDBError::Storage(
                            "duplicate projection source epoch".into(),
                        ));
                    }
                    let epoch = parse_u64(raw, "source graph commit epoch")?;
                    self.source_graph_commit_epoch = Some(epoch);
                    *self
                        .durable_source_graph_commit_epoch
                        .get_mut()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(epoch);
                }
                ["import_source_graph_commit_epoch", raw] => {
                    self.import_source_graph_commit_epoch =
                        Some(parse_u64(raw, "import source graph commit epoch")?);
                }
                ["embedding_manifest", raw_model, raw_version, raw_dimension] => {
                    let version = decode_string(raw_version)?;
                    let manifest = SearchEmbeddingManifest {
                        model: decode_string(raw_model)?,
                        version: if version.is_empty() {
                            None
                        } else {
                            Some(version)
                        },
                        dimension: parse_usize(raw_dimension, "embedding manifest dimension")?,
                    };
                    self.embedding_dimension = Some(manifest.dimension);
                    self.embedding_manifest = Some(manifest);
                    #[cfg(feature = "vector-search")]
                    self.resident_diskann.reset();
                }
                ["rabitq_checkpoint", bits, rows, seed] => {
                    let bits = parse_u64(bits, "RaBitQ bit width")?;
                    let rows = parse_usize(rows, "RaBitQ segment rows")?;
                    let seed = parse_u64(seed, "RaBitQ seed")?;
                    if self.rabitq_checkpoint.is_some()
                        || !matches!(bits, 1 | 4)
                        || rows == 0
                        || rows > u32::MAX as usize
                    {
                        return Err(HawDBError::StorageIntegrity(
                            "invalid explicit RaBitQ checkpoint settings".into(),
                        ));
                    }
                    self.rabitq_checkpoint = Some((bits as u8, rows, seed));
                    #[cfg(feature = "vector-search")]
                    {
                        self.rabitq_build_options.bit_width = if bits == 1 {
                            hawdb_vector_projection::RaBitQBitWidth::One
                        } else {
                            hawdb_vector_projection::RaBitQBitWidth::Four
                        };
                        self.rabitq_build_options.segment_rows = rows;
                        self.rabitq_build_options.transform_seed = seed;
                    }
                }
                ["embedding_dimension", raw] => {
                    let dimension = parse_usize(raw, "embedding dimension")?;
                    if let Some(manifest) = &self.embedding_manifest
                        && manifest.dimension != dimension
                    {
                        return Err(HawDBError::Storage(format!(
                                "embedding manifest dimension {} does not match snapshot dimension {dimension}",
                                manifest.dimension
                            )));
                    }
                    self.embedding_dimension = Some(dimension);
                }
                ["doc", raw_id, raw_title, raw_content, raw_embedding, raw_metadata] => {
                    if self.vector_document_summary.is_some() {
                        return Err(vector_index::invalid(
                            "vector summary cannot contain resident rows",
                        ));
                    }
                    let embedding = decode_embedding(raw_embedding)?;
                    if let Some(embedding) = &embedding {
                        vector_index::validate_embedding(
                            self.vector_index_descriptor.as_ref(),
                            embedding,
                            || Ok(()),
                        )?;
                        self.validate_or_set_dimension(embedding.len())?;
                    }
                    let document = SearchDocument {
                        id: decode_string(raw_id)?,
                        title: decode_string(raw_title)?,
                        content: decode_string(raw_content)?,
                        embedding,
                        metadata: decode_metadata(raw_metadata)?,
                    };
                    self.documents.insert(document.id.clone(), document);
                }
                [""] => {}
                _ => {
                    return Err(HawDBError::Storage(format!(
                        "invalid search projection line: {line}"
                    )));
                }
            }
        }
        if self.vector_document_summary.is_some()
            && (self.vector_index_descriptor.is_none() || self.consumer_binding.is_none())
        {
            return Err(vector_index::invalid(
                "vector summary requires a bound Consumer",
            ));
        }
        if self.canonical_vector_source_epoch.is_some()
            && (self.canonical_vector_source_epoch != self.source_graph_commit_epoch
                || !self
                    .vector_document_summary
                    .is_some_and(|summary| summary.document_count == summary.vector_document_count))
        {
            return Err(vector_index::invalid(
                "canonical vector source requires a complete vector-only summary at the same epoch",
            ));
        }
        if self
            .vector_index_descriptor
            .as_ref()
            .is_some_and(|binding| {
                self.embedding_dimension != Some(binding.dimension().get() as usize)
            })
        {
            return Err(HawDBError::StorageIntegrity(
                "vector index snapshot dimension mismatch".into(),
            ));
        }
        if self.consumer_binding.is_some()
            && (self.source_graph_commit_epoch.is_none()
                || self
                    .import_source_graph_commit_epoch
                    .zip(self.source_graph_commit_epoch)
                    .is_some_and(|(import, source)| import > source))
        {
            return Err(HawDBError::Storage(
                "invalid registered projection epochs".into(),
            ));
        }
        self.vector_scoring
            .validate(self.vector_index_descriptor.is_some())?;
        Ok(())
    }

    fn load_or_rebuild_segment_descriptor(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        self.segment_descriptor = (match read_search_segment_descriptor(path) {
            Ok(Some(descriptor))
                if descriptor.matches_documents(&self.documents)
                    && descriptor.payload_artifact_is_available(path)? =>
            {
                Some(descriptor)
            }
            Ok(None) => Some(SearchSegmentDescriptor::build(&self.documents)),
            Err(error @ HawDBError::FileDescriptors(_)) => return Err(error),
            Ok(Some(_)) | Err(_) => {
                quarantine_rebuildable_artifact(path, SEARCH_SEGMENT_DESCRIPTOR_FILE);
                let descriptor = SearchSegmentDescriptor::build(&self.documents);
                let _ = write_search_segment_descriptor(path, &descriptor);
                Some(descriptor)
            }
        })
        .map(Arc::new);
        Ok(())
    }

    fn marker_path(&self, name: &str) -> Option<PathBuf> {
        self.path.as_ref().map(|path| path.join(name))
    }

    fn append_marker(&self, name: &str, reason: &str) -> Result<()> {
        let _marker_authority = self.require_marker_mutation()?;
        {
            let mut marker_lines = self
                .marker_lines
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let lines = marker_lines.entry(name.to_string()).or_default();
            if !lines.iter().any(|line| line == reason) {
                lines.push(reason.to_string());
            }
        }
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        let existing = fs::read_to_string(&path).unwrap_or_default();
        if existing.lines().any(|line| line == reason) {
            return Ok(());
        }
        let next = if existing.trim().is_empty() {
            reason.to_string()
        } else {
            format!("{}\n{reason}", existing.trim())
        };
        fs::write(path, next)?;
        Ok(())
    }

    fn write_marker(&self, name: &str, reason: &str) -> Result<()> {
        let _marker_authority = self.require_marker_mutation()?;
        self.marker_lines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(name.to_string(), vec![reason.to_string()]);
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        fs::write(path, reason)?;
        Ok(())
    }

    fn clear_marker(&self, name: &str) -> Result<()> {
        let _marker_authority = self.require_marker_mutation()?;
        self.marker_lines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(name);
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn read_marker_lines(&self, name: &str) -> Result<Vec<String>> {
        let Some(path) = self.marker_path(name) else {
            return Ok(self
                .marker_lines
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(name)
                .cloned()
                .unwrap_or_default());
        };
        let content = fs::read_to_string(path).unwrap_or_default();
        Ok(content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self {
            read_generation: Mutex::default(),
            read_freshness: None,
            documents: ResidentDocuments::default(),
            consumer_binding: None,
            consumer_binding_valid: true,
            consumer_owner: std::sync::Weak::new(),
            consumer_mutation_generation: None,
            consumer_receipt: Mutex::new(None),
            path: None,
            _project_files: None,
            embedding_dimension: None,
            rabitq_checkpoint: None,
            embedding_manifest: None,
            vector_index_descriptor: None,
            vector_scoring: SearchVectorScoring::default(),
            vector_document_summary: None,
            canonical_vector_source_epoch: None,
            canonical_vector_delta: None,
            import_source_graph_commit_epoch: None,
            source_graph_commit_epoch: None,
            durable_source_graph_commit_epoch: Mutex::new(None),
            artifact_source_graph_commit_epoch: Mutex::new(None),
            marker_lines: Mutex::new(BTreeMap::new()),
            analyzer_lexicon: Arc::default(),
            lexical_projection: Mutex::new(None),
            lexical_delta: Mutex::new(Arc::default()),
            lexical_config: LexicalProjectionConfig::default(),
            #[cfg(feature = "vector-search")]
            rabitq_projection: Mutex::new(None),
            #[cfg(feature = "vector-search")]
            resident_diskann: Default::default(),
            #[cfg(feature = "vector-search")]
            rabitq_build_options: RaBitQCandidateProjectionBuildOptions::default(),
            segment_descriptor: None,
            segment_payload_reader: None,
            range_read_config: SearchRangeReadConfig::default(),
            cleanup_state: Mutex::new(SearchProjectionCleanupState::default()),
            cleanup_options: SearchProjectionCleanupOptions::default(),
            runtime_capabilities: crate::compiled_runtime_capabilities(),
            telemetry: None,
            externally_owned_kinds: BTreeSet::new(),
        }
    }
}

fn quarantine_rebuildable_artifact(parent: &Path, name: &str) {
    let source = parent.join(name);
    if !source.exists() {
        return;
    }
    let sequence = QUARANTINE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let quarantine_name = format!("{name}.corrupt.{}.{}", std::process::id(), sequence);
    let _ = fs::rename(source, parent.join(quarantine_name));
}

#[cfg(feature = "vector-search")]
fn rabitq_artifact_file(generation: u64) -> String {
    format!("{SEARCH_RABITQ_PROJECTION_PREFIX}{generation}{SEARCH_RABITQ_PROJECTION_SUFFIX}")
}

#[cfg(feature = "vector-search")]
fn rabitq_artifact_generation(name: &str) -> Option<u64> {
    name.strip_prefix(SEARCH_RABITQ_PROJECTION_PREFIX)
        .and_then(|value| value.strip_suffix(SEARCH_RABITQ_PROJECTION_SUFFIX))
        .and_then(|value| value.parse().ok())
}

#[cfg(feature = "vector-search")]
fn latest_rabitq_artifact(path: &Path) -> Result<Option<(u64, PathBuf)>> {
    Ok(rabitq_artifacts_descending(path)?.into_iter().next())
}

#[cfg(feature = "vector-search")]
fn rabitq_artifacts_descending(path: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut artifacts = Vec::new();
    for entry in entries {
        let entry = entry?;
        if let Some(generation) = entry
            .file_name()
            .to_str()
            .and_then(rabitq_artifact_generation)
        {
            artifacts.push((generation, entry.path()));
        }
    }
    artifacts.sort_unstable_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    Ok(artifacts)
}

#[cfg(feature = "vector-search")]
fn rabitq_artifacts_descending_under_publication(
    publication: &out_of_core::SearchProjectionPublishLease,
) -> Result<Vec<(u64, String)>> {
    const MAX_REGISTERED_ARTIFACT_NAMES: usize = 4096;
    let mut artifacts = Vec::new();
    publication.with_captured_directory(|directory| {
        directory.visit_children(
            &directory.operation_context(),
            MAX_REGISTERED_ARTIFACT_NAMES,
            |name| {
                if let Some(generation) = rabitq_artifact_generation(name) {
                    artifacts.push((generation, name.to_owned()));
                }
                Ok(())
            },
        )
    })?;
    artifacts.sort_unstable_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    Ok(artifacts)
}

const NOWLEDGE_SEARCH_PROJECTION_TABLES: &[(&str, &str, bool)] = &[
    ("memories_index", "memory", true),
    ("messages_index", "message", false),
    ("communities_index", "community", true),
    ("entities_index", "entity", true),
    ("sources_index", "source", true),
    ("source_chunks_index", "source_chunk", true),
];

fn search_projection_probe_table_reports(
    documents: &impl DocumentView,
    embedding_dimension: Option<usize>,
) -> Vec<serde_json::Value> {
    NOWLEDGE_SEARCH_PROJECTION_TABLES
        .iter()
        .map(|(table_name, kind, requires_vector)| {
            let table_documents = documents
                .values()
                .filter(|document| {
                    document
                        .metadata
                        .get("kind")
                        .is_some_and(|value| value == kind)
                })
                .collect::<Vec<_>>();
            let present = !table_documents.is_empty();
            let has_text = table_documents
                .iter()
                .any(|document| !document.title.is_empty() || !document.content.is_empty());
            let vectors_match = !*requires_vector
                || table_documents.iter().all(|document| {
                    document.embedding.as_ref().is_some_and(|embedding| {
                        embedding_dimension.is_none_or(|dimension| embedding.len() == dimension)
                    })
                });
            let vector_ready = !*requires_vector || (present && vectors_match);
            let mut blocker_codes = Vec::new();
            if !present {
                blocker_codes.push("missing_table_rows");
            }
            if !has_text {
                blocker_codes.push("missing_searchable_text");
            }
            if *requires_vector && !vector_ready {
                blocker_codes.push("missing_or_mismatched_vectors");
            }
            serde_json::json!({
                "name": table_name,
                "kind": kind,
                "present": present,
                "fts_ready": present && has_text,
                "vector_ready": vector_ready,
                "row_count": table_documents.len(),
                "requires_vector": requires_vector,
                "blocker_codes": blocker_codes,
            })
        })
        .collect()
}

fn search_projection_probe_document_identity_report(
    documents: &impl DocumentView,
) -> serde_json::Value {
    let mut body = String::new();
    for document_id in documents.keys() {
        body.push_str(document_id);
        body.push('\n');
    }
    serde_json::json!({
        "ready": true,
        "id_space": "search_projection_document_id",
        "representation": "sorted_document_ids",
        "document_count": documents.len(),
        "checksum": checksum_bytes(body.as_bytes()),
    })
}

fn search_projection_probe_predicate_pushdown_report(index: &SearchIndex) -> serde_json::Value {
    let segment_descriptor_ready = index
        .segment_descriptor
        .as_deref()
        .is_some_and(|descriptor| descriptor.matches_documents(&index.documents));
    let transient_descriptor;
    let descriptor = match index
        .segment_descriptor
        .as_deref()
        .filter(|descriptor| descriptor.matches_documents(&index.documents))
    {
        Some(descriptor) => Some(descriptor),
        None if !index.documents.is_empty() => {
            transient_descriptor = SearchSegmentDescriptor::build(&index.documents);
            Some(&transient_descriptor)
        }
        None => None,
    };
    let segment_descriptor_field_summaries = descriptor
        .map(search_projection_probe_segment_descriptor_field_summaries)
        .unwrap_or_default();
    let segment_document_pruning = descriptor
        .map(search_projection_probe_segment_document_pruning_report)
        .unwrap_or_default();
    let physical_segment_range_count = descriptor
        .map(|descriptor| descriptor.physical_read_ranges().len())
        .unwrap_or_default();
    let physical_segment_ranges_ready = descriptor.is_some_and(|descriptor| {
        physical_segment_range_count == descriptor.segments.len()
            && index.path.as_ref().is_some_and(|path| {
                descriptor
                    .payload_artifact_is_available(path)
                    .unwrap_or(false)
            })
    });
    serde_json::json!({
        "ready": true,
        "equality_ready": true,
        "in_list_ready": true,
        "not_in_list_ready": true,
        "range_ready": true,
        "row_filter_ready": true,
        "segment_pruning_ready": true,
        "numeric_min_max_ready": true,
        "timestamp_min_max_ready": true,
        "persisted_segment_descriptor_ready": segment_descriptor_ready,
        "physical_segment_ranges_ready": physical_segment_ranges_ready,
        "physical_segment_range_count": physical_segment_range_count,
        "supported_ops": ["eq", "in", "not_in", "gt", "gte", "lt", "lte"],
        "scan_filter_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
        "segment_descriptor_field_count": segment_descriptor_field_summaries.len(),
        "segment_descriptor_field_summaries": segment_descriptor_field_summaries,
        "segment_document_pruning_ready": segment_document_pruning.ready,
        "segment_pruning_candidate_document_count": segment_document_pruning.candidate_document_count,
        "segment_pruned_document_count": segment_document_pruning.pruned_document_count,
        "segment_scanned_document_count": segment_document_pruning.scanned_document_count,
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct SearchProjectionProbeSegmentDocumentPruningReport {
    ready: bool,
    candidate_document_count: usize,
    pruned_document_count: usize,
    scanned_document_count: usize,
}

fn search_projection_probe_segment_document_pruning_report(
    descriptor: &SearchSegmentDescriptor,
) -> SearchProjectionProbeSegmentDocumentPruningReport {
    let Some(first_segment) = descriptor.segments.first() else {
        return SearchProjectionProbeSegmentDocumentPruningReport::default();
    };
    if first_segment.first_document_id.is_empty() {
        return SearchProjectionProbeSegmentDocumentPruningReport::default();
    }
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::eq(
        SEARCH_DOCUMENT_ID_FIELD,
        first_segment.first_document_id.clone(),
    )]);
    let mut pruned_document_count = 0;
    let mut scanned_document_count = 0;
    for segment in &descriptor.segments {
        if segment.may_match_predicates(&predicates) {
            scanned_document_count += segment.document_count;
        } else {
            pruned_document_count += segment.document_count;
        }
    }
    let ready = descriptor.document_count > 0
        && pruned_document_count > 0
        && scanned_document_count > 0
        && pruned_document_count + scanned_document_count == descriptor.document_count;
    SearchProjectionProbeSegmentDocumentPruningReport {
        ready,
        candidate_document_count: descriptor.document_count,
        pruned_document_count,
        scanned_document_count,
    }
}

fn search_projection_probe_production_filter_pruning_report(
    index: &SearchIndex,
) -> serde_json::Value {
    let Some(descriptor) = index
        .segment_descriptor
        .as_deref()
        .filter(|descriptor| descriptor.matches_documents(&index.documents))
    else {
        return serde_json::json!({
            "ready": false,
            "persisted_segment_descriptor_used": false,
            "payload_read_avoidance_ready": false,
            "sample_count": 0,
            "ready_field_count": 0,
            "required_field_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
            "missing_fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS,
            "samples": [],
        });
    };

    let mut samples = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| search_projection_probe_production_filter_sample(descriptor, field))
        .collect::<Vec<_>>();
    let missing_fields = samples
        .iter()
        .filter(|sample| !sample.ready)
        .map(|sample| sample.field)
        .collect::<Vec<_>>();
    let ready_field_count = NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        .len()
        .saturating_sub(missing_fields.len());
    samples.push(search_projection_probe_production_filter_sample(
        descriptor,
        SEARCH_DOCUMENT_ID_FIELD,
    ));
    let payload_read_avoidance_ready = samples
        .iter()
        .any(|sample| sample.segment_pruned_document_count > 0);
    let explain_analyze_ready = samples.iter().all(|sample| sample.explain_analyze_ready);
    let ready = missing_fields.is_empty() && payload_read_avoidance_ready && explain_analyze_ready;
    serde_json::json!({
        "ready": ready,
        "persisted_segment_descriptor_used": true,
        "payload_read_avoidance_ready": payload_read_avoidance_ready,
        "explain_analyze_ready": explain_analyze_ready,
        "sample_count": samples.len(),
        "ready_field_count": ready_field_count,
        "required_field_count": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len(),
        "missing_fields": missing_fields,
        "samples": samples
            .into_iter()
            .map(SearchProjectionProbeProductionFilterSample::json)
            .collect::<Vec<_>>(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SearchProjectionProbeProductionFilterSample<'a> {
    field: &'a str,
    operation: &'static str,
    operation_family: &'static str,
    value_kind: &'static str,
    ready: bool,
    capability_ready: bool,
    persisted_segment_descriptor_used: bool,
    segment_count: usize,
    scanned_segment_count: usize,
    pruned_segment_count: usize,
    segment_pruning_candidate_document_count: usize,
    segment_scanned_document_count: usize,
    segment_pruned_document_count: usize,
    field_report_count: usize,
    value_summary_used: bool,
    numeric_range_summary_used: bool,
    timestamp_range_summary_used: bool,
    normalized_default_equality: bool,
    unique_key_lookup: bool,
    explain_analyze_ready: bool,
}

impl SearchProjectionProbeProductionFilterSample<'_> {
    fn json(self) -> serde_json::Value {
        serde_json::json!({
            "field": self.field,
            "operation": self.operation,
            "operation_family": self.operation_family,
            "value_kind": self.value_kind,
            "ready": self.ready,
            "capability_ready": self.capability_ready,
            "persisted_segment_descriptor_used": self.persisted_segment_descriptor_used,
            "segment_count": self.segment_count,
            "scanned_segment_count": self.scanned_segment_count,
            "pruned_segment_count": self.pruned_segment_count,
            "segment_pruning_candidate_document_count": self.segment_pruning_candidate_document_count,
            "segment_scanned_document_count": self.segment_scanned_document_count,
            "segment_pruned_document_count": self.segment_pruned_document_count,
            "field_report_count": self.field_report_count,
            "value_summary_used": self.value_summary_used,
            "numeric_range_summary_used": self.numeric_range_summary_used,
            "timestamp_range_summary_used": self.timestamp_range_summary_used,
            "normalized_default_equality": self.normalized_default_equality,
            "unique_key_lookup": self.unique_key_lookup,
            "explain_analyze": {
                "ready": self.explain_analyze_ready,
                "operator": "search_projection_segment_scan",
                "segment_count": self.segment_count,
                "scanned_segment_count": self.scanned_segment_count,
                "pruned_segment_count": self.pruned_segment_count,
                "candidate_document_count": self.segment_pruning_candidate_document_count,
                "scanned_document_count": self.segment_scanned_document_count,
                "pruned_document_count": self.segment_pruned_document_count,
                "payload_read_avoidance": self.segment_pruned_document_count > 0,
            },
        })
    }
}

fn search_projection_probe_production_filter_sample<'a>(
    descriptor: &SearchSegmentDescriptor,
    field: &'a str,
) -> SearchProjectionProbeProductionFilterSample<'a> {
    let value_kind = search_projection_probe_production_filter_value_kind(field);
    let operation_family = search_projection_probe_production_filter_operation_family(field);
    let (operation, predicate) =
        search_projection_probe_production_filter_predicate(descriptor, field);
    let predicates = SearchPredicateSet::new(vec![predicate]);
    let pruning = explain_search_segments_with_persisted_descriptor(&predicates, descriptor);
    let field_reports = pruning
        .field_summaries
        .iter()
        .filter(|summary| summary.field == field)
        .collect::<Vec<_>>();
    let value_summary_used = field_reports
        .iter()
        .any(|summary| summary.value_summary_used);
    let numeric_range_summary_used = field_reports
        .iter()
        .any(|summary| summary.numeric_range_summary_used);
    let timestamp_range_summary_used = field_reports
        .iter()
        .any(|summary| summary.timestamp_range_summary_used);
    let unique_key_lookup = field == SEARCH_DOCUMENT_ID_FIELD
        && descriptor.segments.iter().all(|segment| {
            segment.metadata.get(field).is_some_and(|summary| {
                summary.present_count == segment.document_count
                    && summary.values.len() == segment.document_count
            })
        });
    let capability_ready = match operation_family {
        "numeric_range" => numeric_range_summary_used,
        "timestamp_range" => timestamp_range_summary_used || numeric_range_summary_used,
        "unique_key" => unique_key_lookup,
        _ => value_summary_used,
    };
    let ready = pruning.persisted_segment_descriptor_used
        && pruning.segment_count > 0
        && pruning.segment_pruning_candidate_document_count == descriptor.document_count
        && field_reports.len() == 1
        && capability_ready;
    let explain_analyze_ready = pruning.persisted_segment_descriptor_used
        && pruning.segment_count > 0
        && pruning.segment_pruning_candidate_document_count == descriptor.document_count
        && pruning.segment_pruned_document_count + pruning.segment_scanned_document_count
            == descriptor.document_count;
    SearchProjectionProbeProductionFilterSample {
        field,
        operation,
        operation_family,
        value_kind,
        ready,
        capability_ready,
        persisted_segment_descriptor_used: pruning.persisted_segment_descriptor_used,
        segment_count: pruning.segment_count,
        scanned_segment_count: pruning.scanned_segment_count,
        pruned_segment_count: pruning.pruned_segment_count,
        segment_pruning_candidate_document_count: pruning.segment_pruning_candidate_document_count,
        segment_scanned_document_count: pruning.segment_scanned_document_count,
        segment_pruned_document_count: pruning.segment_pruned_document_count,
        field_report_count: field_reports.len(),
        value_summary_used,
        numeric_range_summary_used,
        timestamp_range_summary_used,
        normalized_default_equality: operation_family == "normalized_default_equality",
        unique_key_lookup,
        explain_analyze_ready,
    }
}

fn explain_search_segments_with_persisted_descriptor(
    predicates: &SearchPredicateSet,
    descriptor: &SearchSegmentDescriptor,
) -> FilteredSearchDocuments<'static> {
    let mut pruned_segment_count = 0;
    let mut scanned_segment_count = 0;
    let mut pruned_document_count = 0;
    let mut scanned_document_count = 0;
    let mut field_pruning = SearchFieldPruningAccumulator::new(predicates);

    for segment in &descriptor.segments {
        field_pruning.observe_persisted_segment(segment, predicates);
        if !segment.may_match_predicates(predicates) {
            pruned_segment_count += 1;
            pruned_document_count += segment.document_count;
            continue;
        }
        scanned_segment_count += 1;
        scanned_document_count += segment.document_count;
    }

    FilteredSearchDocuments {
        documents: Vec::new(),
        segment_count: descriptor.segments.len(),
        pruned_segment_count,
        scanned_segment_count,
        segment_pruning_candidate_document_count: descriptor.document_count,
        segment_pruned_document_count: pruned_document_count,
        segment_scanned_document_count: scanned_document_count,
        persisted_segment_descriptor_used: true,
        field_summaries: field_pruning.into_reports(),
    }
}

fn search_projection_probe_production_filter_predicate(
    descriptor: &SearchSegmentDescriptor,
    field: &str,
) -> (&'static str, SearchPredicate) {
    let value = descriptor
        .segments
        .iter()
        .filter_map(|segment| segment.metadata.get(field))
        .find_map(|summary| {
            summary
                .values
                .iter()
                .next()
                .cloned()
                .or_else(|| summary.numeric_range.map(|range| range.min.to_string()))
                .or_else(|| {
                    summary
                        .timestamp_range
                        .map(|range| range.min_epoch_millis.to_string())
                })
        })
        .unwrap_or_default();
    match search_projection_probe_production_filter_operation_family(field) {
        "numeric_range" | "timestamp_range" => ("gte", SearchPredicate::gte(field, value)),
        "enum_in_list" => (
            "in",
            SearchPredicate::in_list(field, std::iter::once(value)),
        ),
        _ => ("eq", SearchPredicate::eq(field, value)),
    }
}

fn search_projection_probe_production_filter_operation_family(field: &str) -> &'static str {
    match field {
        SEARCH_DOCUMENT_ID_FIELD => "unique_key",
        "unit_type" | "lifecycle_state" | "temporal_context" => "enum_in_list",
        "importance" | "confidence" => "numeric_range",
        "created_at" | "updated_at" | "event_start" | "event_end" => "timestamp_range",
        "is_latest" => "normalized_default_equality",
        _ => "equality",
    }
}

fn search_projection_probe_production_filter_value_kind(field: &str) -> &'static str {
    match field {
        "importance" | "confidence" => "numeric_range",
        "created_at" | "updated_at" | "event_start" | "event_end" => "timestamp_range",
        _ => "value",
    }
}

fn search_projection_probe_segment_descriptor_field_summaries(
    descriptor: &SearchSegmentDescriptor,
) -> Vec<serde_json::Value> {
    let mut fields = BTreeMap::<String, SearchProjectionProbeFieldSummary>::new();
    for segment in &descriptor.segments {
        for (field, summary) in &segment.metadata {
            let entry = fields.entry(field.clone()).or_default();
            entry.segment_count += 1;
            entry.present_document_count += summary.present_count;
            if !summary.values.is_empty() {
                entry.value_summary_segment_count += 1;
            }
            if summary.numeric_range.is_some() {
                entry.numeric_range_segment_count += 1;
            }
            if summary.timestamp_range.is_some() {
                entry.timestamp_range_segment_count += 1;
            }
            if field == SEARCH_DOCUMENT_ID_FIELD
                && summary.present_count == segment.document_count
                && summary.values.len() == segment.document_count
            {
                entry.unique_key_summary_segment_count += 1;
            }
        }
    }
    fields
        .into_iter()
        .map(|(field, summary)| {
            serde_json::json!({
                "field": field,
                "segment_count": summary.segment_count,
                "present_document_count": summary.present_document_count,
                "value_summary_used": summary.value_summary_segment_count > 0,
                "value_summary_segment_count": summary.value_summary_segment_count,
                "numeric_range_summary_used": summary.numeric_range_segment_count > 0,
                "numeric_range_segment_count": summary.numeric_range_segment_count,
                "timestamp_range_summary_used": summary.timestamp_range_segment_count > 0,
                "timestamp_range_segment_count": summary.timestamp_range_segment_count,
                "unique_key_summary_used": summary.unique_key_summary_segment_count > 0,
                "unique_key_summary_segment_count": summary.unique_key_summary_segment_count,
            })
        })
        .collect()
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SearchProjectionProbeFieldSummary {
    segment_count: usize,
    present_document_count: usize,
    value_summary_segment_count: usize,
    numeric_range_segment_count: usize,
    timestamp_range_segment_count: usize,
    unique_key_summary_segment_count: usize,
}

#[cfg(feature = "vector-search")]
fn search_projection_probe_compressed_vector_projection_report(
    index: &SearchIndex,
) -> serde_json::Value {
    // This capability describes compiled dispatch support; per-query candidate
    // metrics report the actual selected kernel, including scalar fallback.
    let supports_runtime_simd_dispatch = cfg!(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "aarch64"
    ));
    let projection = index.rabitq_projection().or_else(|| {
        index
            .path
            .is_none()
            .then(|| index.build_in_memory_rabitq_projection().ok().flatten())
            .flatten()
    });
    match projection {
        Some(projection) => serde_json::json!({
            "engine": "hawdb_rabitq_scan",
            "compiled": true,
            "ready": true,
            "format_version": projection.manifest().format_version,
            "algorithm": projection.manifest().algorithm,
            "bit_width": projection.manifest().bit_width,
            "dimension": projection.manifest().dimension,
            "document_count": projection.manifest().document_count,
            "generation": projection.manifest().identity.generation,
            "source_epoch": projection.manifest().identity.source_epoch,
            "embedding_model": projection.manifest().identity.embedding_model,
            "embedding_version": projection.manifest().identity.embedding_version,
            "transform": projection.manifest().transform,
            "quantizer": projection.manifest().quantizer,
            "calibration": projection.manifest().calibration,
            "segment_count": projection.manifest().segments.len(),
            "payload_bytes": projection.manifest().payload_bytes,
            "peak_build_working_bytes": projection.build_report().peak_working_bytes,
            "supports_allowlist": true,
            "supports_filter_bitmap": true,
            "supports_scalar_reference": true,
            "supports_runtime_simd_dispatch": supports_runtime_simd_dispatch,
            "supports_governed_parallelism": true,
            "raw_rerank_required": true,
            "persisted_artifact_used": projection.is_file_backed(),
            "artifact_rebuilt_from_snapshot": !projection.is_file_backed(),
            "blocker_codes": [],
        }),
        None => serde_json::json!({
            "engine": "hawdb_rabitq_scan",
            "compiled": true,
            "ready": false,
            "algorithm": hawdb_vector_projection::PROJECTION_ALGORITHM,
            "bit_width": hawdb_vector_projection::PROJECTION_BIT_WIDTH,
            "dimension": serde_json::Value::Null,
            "document_count": 0,
            "quantizer": hawdb_vector_projection::PROJECTION_QUANTIZER,
            "calibration": hawdb_vector_projection::PROJECTION_CALIBRATION,
            "supports_allowlist": true,
            "supports_filter_bitmap": true,
            "supports_scalar_reference": true,
            "supports_runtime_simd_dispatch": supports_runtime_simd_dispatch,
            "supports_governed_parallelism": true,
            "raw_rerank_required": true,
            "persisted_artifact_used": false,
            "artifact_rebuilt_from_snapshot": false,
            "blocker_codes": ["rabitq_projection_unavailable"],
        }),
    }
}

#[cfg(not(feature = "vector-search"))]
fn search_projection_probe_compressed_vector_projection_report(
    _index: &SearchIndex,
) -> serde_json::Value {
    serde_json::json!({
        "engine": "hawdb_rabitq_scan",
        "compiled": false,
        "ready": false,
        "algorithm": "rabitq",
        "bit_width": serde_json::Value::Null,
        "dimension": serde_json::Value::Null,
        "document_count": 0,
        "supports_allowlist": false,
        "supports_filter_bitmap": false,
        "supports_scalar_reference": false,
        "supports_runtime_simd_dispatch": false,
        "supports_governed_parallelism": false,
        "raw_rerank_required": true,
        "persisted_artifact_used": false,
        "artifact_rebuilt_from_snapshot": false,
        "blocker_codes": ["vector_search_feature_disabled"],
    })
}

fn search_projection_probe_blocker_codes(
    has_documents: bool,
    has_text: bool,
    has_vector: bool,
    has_manifest: bool,
    model_matches: bool,
    dimension_matches: bool,
    freshness: &SearchProjectionFreshness,
) -> Vec<String> {
    let mut blockers = BTreeSet::new();
    if !has_documents {
        blockers.insert("empty_search_projection".to_string());
    }
    if !has_text {
        blockers.insert("missing_text_leg".to_string());
    }
    if !has_vector {
        blockers.insert("missing_vector_leg".to_string());
    }
    if !has_manifest {
        blockers.insert("missing_embedding_manifest".to_string());
    }
    if !model_matches {
        blockers.insert("embedding_model_mismatch".to_string());
    }
    if !dimension_matches {
        blockers.insert("embedding_dimension_mismatch".to_string());
    }
    if freshness.full_reindex_needed {
        blockers.insert("full_reindex_needed".to_string());
    }
    if freshness.metadata_repair_needed {
        blockers.insert("metadata_repair_needed".to_string());
    }
    blockers.into_iter().collect()
}

pub(crate) fn projection_row_from_node(
    catalog: &Catalog,
    node: &NodeRecord,
) -> Option<SearchProjectionRow> {
    let kind = node.labels.iter().find_map(|label_id| {
        catalog
            .label_name(*label_id)
            .and_then(search_projection_kind_from_label)
    })?;
    let external_id = string_property(node, "id")
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| node.id.0.to_string());
    let title = first_string_property(node, &["title", "name", "summary", "id"])
        .unwrap_or_else(|| external_id.clone());
    let body = first_string_property(
        node,
        &["content", "body", "text", "summary", "title", "name"],
    )
    .unwrap_or_else(|| title.clone());
    let source_id = first_non_empty_string_property(node, &["source_id", "thread_id", "source"]);
    let mut metadata = BTreeMap::new();
    for (key, value) in &node.properties {
        if matches!(key.as_str(), "kind" | "external_id" | "source_id") {
            continue;
        }
        metadata.insert(key.clone(), projection_metadata_value(key, value));
    }
    metadata
        .entry("space_id".to_string())
        .or_insert_with(|| DEFAULT_SPACE_ID.to_string());
    if metadata
        .get("space_id")
        .is_some_and(|space_id| space_id.is_empty())
    {
        metadata.insert("space_id".to_string(), DEFAULT_SPACE_ID.to_string());
    }
    if kind == SearchProjectionKind::Memory {
        materialize_memory_metadata_paths(node, &mut metadata);
    }
    Some(SearchProjectionRow {
        kind,
        external_id,
        title,
        body,
        embedding: None,
        source_id,
        metadata,
    })
}

pub fn projection_row_from_node_with_graph_metadata<S: SearchProjectionSource + ?Sized>(
    catalog: &Catalog,
    store: &S,
    node: &NodeRecord,
) -> Result<Option<SearchProjectionRow>> {
    let Some(mut row) = projection_row_from_node(catalog, node) else {
        return Ok(None);
    };
    let labels = projection_business_labels_for_node(catalog, store, node)?;
    apply_projection_business_labels(&mut row, &labels);
    Ok(Some(row))
}

/// Shapes one canonical node with already collected business-label values.
/// The caller must establish label completeness and source validity separately.
#[doc(hidden)]
pub fn projection_row_from_node_with_business_labels(
    catalog: &Catalog,
    node: &NodeRecord,
    labels: &[String],
) -> Option<SearchProjectionRow> {
    let mut row = projection_row_from_node(catalog, node)?;
    apply_projection_business_labels(&mut row, labels);
    Some(row)
}

fn apply_projection_business_labels(row: &mut SearchProjectionRow, labels: &[String]) {
    if !labels.is_empty() {
        row.metadata.insert(
            "labels".to_string(),
            serde_json::to_string(labels).expect("label metadata serializes as a string array"),
        );
    }
}

fn projection_business_labels_for_node<S: SearchProjectionSource + ?Sized>(
    catalog: &Catalog,
    store: &S,
    node: &NodeRecord,
) -> Result<Vec<String>> {
    store.projection_business_labels(catalog, node)
}

#[doc(hidden)]
pub fn projection_business_label_value(label: &NodeRecord) -> Option<String> {
    projection_business_label_value_from_properties(&label.properties)
}

/// Applies the canonical business-label precedence to a decoded query node.
#[doc(hidden)]
pub fn projection_business_label_value_from_properties(
    properties: &BTreeMap<String, Value>,
) -> Option<String> {
    ["canonical_name", "name", "id"]
        .into_iter()
        .filter_map(|key| properties.get(key).map(value_to_projection_string))
        .find(|value| !value.is_empty())
}

fn search_projection_kind_from_label(label: &str) -> Option<SearchProjectionKind> {
    match label {
        "Memory" | "memory" => Some(SearchProjectionKind::Memory),
        "Message" | "message" => Some(SearchProjectionKind::Message),
        "Entity" | "entity" => Some(SearchProjectionKind::Entity),
        "Source" | "source" => Some(SearchProjectionKind::Source),
        "SourceChunk" | "source_chunk" | "chunk" => Some(SearchProjectionKind::SourceChunk),
        "Community" | "community" => Some(SearchProjectionKind::Community),
        _ => None,
    }
}

fn first_string_property(node: &NodeRecord, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| string_property(node, key))
}

fn first_non_empty_string_property(node: &NodeRecord, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| string_property(node, key))
        .find(|value| !value.is_empty())
}

fn string_property(node: &NodeRecord, key: &str) -> Option<String> {
    node.properties.get(key).map(value_to_projection_string)
}

const DEFAULT_SPACE_ID: &str = "default";

fn projection_metadata_value(key: &str, value: &Value) -> String {
    let text = value_to_projection_string(value);
    if key == "space_id" && text.is_empty() {
        DEFAULT_SPACE_ID.to_string()
    } else {
        text
    }
}

fn materialize_memory_metadata_paths(node: &NodeRecord, metadata: &mut BTreeMap<String, String>) {
    let Some(raw) = node
        .properties
        .get("metadata")
        .and_then(|value| match value {
            Value::String(raw) => Some(raw),
            _ => None,
        })
    else {
        return;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return;
    };
    for path in NOWLEDGE_MEMORY_MATERIALIZED_METADATA_PATHS {
        let values = metadata_json_values_at_path(&value, path);
        if values.is_empty() {
            continue;
        }
        let key = format!("metadata.{path}");
        let value = if values.len() == 1 {
            values.into_iter().next().unwrap_or_default()
        } else {
            serde_json::to_string(&values).unwrap_or_default()
        };
        metadata.insert(key, value);
    }
}

fn metadata_json_values_at_path(value: &serde_json::Value, path: &str) -> Vec<String> {
    let mut values = vec![value];
    for segment in path.split('.') {
        values = values
            .into_iter()
            .filter_map(|value| value.as_object()?.get(segment))
            .collect();
        if values.is_empty() {
            return Vec::new();
        }
    }
    values
        .into_iter()
        .flat_map(|value| match value {
            serde_json::Value::Array(values) => values.iter().collect::<Vec<_>>(),
            value => vec![value],
        })
        .filter_map(metadata_json_scalar)
        .collect()
}

fn metadata_json_scalar(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

fn value_to_projection_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Decimal(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Binary(value) => format!(
            "\\x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::Uuid(value) => value.to_string(),
        Value::Vector(_)
        | Value::Date(_)
        | Value::LocalTime(_)
        | Value::LocalDateTime(_)
        | Value::DateTime(_)
        | Value::Duration(_) => value.to_string(),
        Value::List(values) => values
            .iter()
            .map(value_to_projection_string)
            .collect::<Vec<_>>()
            .join(","),
        Value::Map(values) => values
            .iter()
            .map(|(key, value)| format!("{key}:{}", value_to_projection_string(value)))
            .collect::<Vec<_>>()
            .join(","),
    }
}

fn ranked_scores(scores: &BTreeMap<String, f64>) -> BTreeMap<String, usize> {
    let mut ranked = scores
        .iter()
        .map(|(id, score)| (id.clone(), *score))
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .partial_cmp(left_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left_id.cmp(right_id))
    });
    ranked
        .into_iter()
        .enumerate()
        .map(|(index, (id, _score))| (id, index + 1))
        .collect()
}

fn search_empty_reason_codes(
    returned_empty: bool,
    document_count: usize,
    filtered_document_count: usize,
    total_hits: usize,
) -> Vec<SearchEmptyReasonCode> {
    if !returned_empty {
        return Vec::new();
    }
    if document_count == 0 {
        return vec![SearchEmptyReasonCode::ProjectionEmpty];
    }
    if filtered_document_count == 0 {
        return vec![SearchEmptyReasonCode::MetadataFilterEmpty];
    }
    if total_hits == 0 {
        return vec![SearchEmptyReasonCode::RetrieverNoHits];
    }
    vec![SearchEmptyReasonCode::LimitExcludedAllHits]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchPageWindow {
    offset: usize,
    limit: usize,
}

fn search_empty_reasons(
    returned_empty: bool,
    document_count: usize,
    filtered_document_count: usize,
    total_hits: usize,
    page_window: SearchPageWindow,
    truncation_reasons: &[String],
    fallback_reasons: &[String],
) -> Vec<String> {
    if !returned_empty {
        return Vec::new();
    }
    if document_count == 0 {
        return vec!["search projection has no documents".to_string()];
    }
    if filtered_document_count == 0 {
        return vec!["metadata filters matched no search documents".to_string()];
    }
    if total_hits == 0 {
        let mut reasons =
            vec!["search retrievers returned no hits inside filtered scope".to_string()];
        reasons.extend(fallback_reasons.iter().cloned());
        return reasons;
    }
    if page_window.offset > 0 {
        return vec![format!(
            "offset {} limit {} returned no hits from {total_hits} matching hits",
            page_window.offset, page_window.limit
        )];
    }
    truncation_reasons.to_vec()
}

fn window_ranks(
    ranks: &BTreeMap<String, usize>,
    rank_window: Option<usize>,
) -> BTreeMap<String, usize> {
    let Some(rank_window) = rank_window else {
        return ranks.clone();
    };
    ranks
        .iter()
        .filter_map(|(id, rank)| (*rank <= rank_window).then_some((id.clone(), *rank)))
        .collect()
}

fn top_ranked_ids<K: AsRef<str> + Ord>(ranks: &BTreeMap<K, usize>, limit: usize) -> Vec<String> {
    let mut ranked = ranks
        .iter()
        .map(|(id, rank)| (id.as_ref().to_owned(), *rank))
        .collect::<Vec<_>>();
    ranked.sort_by(|(left_id, left_rank), (right_id, right_rank)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left_id.cmp(right_id))
    });
    ranked
        .into_iter()
        .take(limit)
        .map(|(id, _rank)| id)
        .collect()
}

fn top_ranked_candidates<K: AsRef<str> + Ord>(
    ranks: &BTreeMap<K, usize>,
    scores: &BTreeMap<String, f64>,
    limit: usize,
) -> Vec<SearchRetrieverCandidate> {
    let mut ranked = ranks
        .iter()
        .filter_map(|(id, rank)| {
            scores
                .get(id.as_ref())
                .map(|score| SearchRetrieverCandidate {
                    id: id.as_ref().to_owned(),
                    rank: *rank,
                    score: *score,
                })
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.rank
            .cmp(&right.rank)
            .then_with(|| left.id.cmp(&right.id))
    });
    ranked.truncate(limit);
    ranked
}

fn retriever_candidate_set_report(
    cardinality: usize,
    snapshot_source_graph_commit_epoch: Option<u64>,
    policy_epoch: Option<u64>,
    exact: bool,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "search_projection_document_id".to_string(),
        representation: "ranked_document_ids".to_string(),
        cardinality,
        exact,
        snapshot_source_graph_commit_epoch,
        policy_epoch,
    }
}

fn rrf_child_score(rank: Option<usize>) -> f64 {
    rank.map(|rank| 1.0 / (RRF_K + rank as f64)).unwrap_or(0.0)
}

fn weighted_rrf_score(
    vector_rrf_score: f64,
    text_rrf_score: f64,
    weights: SearchFusionWeights,
) -> f64 {
    vector_rrf_score * weights.vector_weight + text_rrf_score * weights.text_weight
}

fn format_embedding_manifest(manifest: &SearchEmbeddingManifest) -> String {
    match &manifest.version {
        Some(version) => format!("{}@{}:{}", manifest.model, version, manifest.dimension),
        None => format!("{}:{}", manifest.model, manifest.dimension),
    }
}

#[doc(hidden)]
pub struct SearchMetadataPredicatePushdown {
    pub predicates: SearchPredicateSet,
    pub report: SearchPredicatePushdownReport,
}

#[doc(hidden)]
pub fn search_metadata_predicate_pushdown(
    filters: &BTreeMap<String, String>,
) -> SearchMetadataPredicatePushdown {
    let (predicates, parse_error) = match SearchPredicateSet::from_metadata_filters(filters) {
        Ok(predicates) => (predicates, None),
        Err(error) => (SearchPredicateSet::unsatisfiable(), Some(error.to_string())),
    };
    metadata_predicate_pushdown(filters.len(), predicates, parse_error)
}

fn metadata_predicate_pushdown(
    input_predicate_count: usize,
    predicates: SearchPredicateSet,
    parse_error: Option<String>,
) -> SearchMetadataPredicatePushdown {
    // The metadata scan implements every SearchPredicateOp. Move its complete
    // set rather than cloning it twice through an all-supported optimizer
    // partition whose RuleEvents are immediately discarded.
    let report = SearchPredicatePushdownReport {
        input_predicate_count,
        pushed_predicate_count: predicates.predicates().len(),
        residual_predicate_count: 0,
        unsatisfiable: predicates.is_unsatisfiable(),
        parse_error,
        segment_count: 0,
        pruned_segment_count: 0,
        scanned_segment_count: 0,
        segment_pruning_candidate_document_count: 0,
        segment_pruned_document_count: 0,
        segment_scanned_document_count: 0,
        persisted_segment_descriptor_used: false,
        physical_range_read_count: 0,
        physical_bytes_read: 0,
        postings_segment_count: 0,
        postings_predicate_count: 0,
        postings_residual_predicate_count: 0,
        postings_prefiltered_segment_count: 0,
        field_summaries: Vec::new(),
    };
    SearchMetadataPredicatePushdown { predicates, report }
}

fn search_document_matches_predicates(
    document: &SearchDocument,
    predicates: &SearchPredicateSet,
) -> bool {
    search_metadata_matches_predicates(&document.id, &document.metadata, predicates)
}

fn search_metadata_matches_predicate(
    id: &str,
    metadata: &BTreeMap<String, String>,
    predicate: &SearchPredicate,
) -> bool {
    let header = crate::document_encoding::Header {
        id,
        title: "",
        embedding: None,
        metadata,
    };
    search_header_matches_predicate(header, predicate)
}

fn search_metadata_matches_predicates(
    id: &str,
    metadata: &BTreeMap<String, String>,
    predicates: &SearchPredicateSet,
) -> bool {
    if predicates.is_unsatisfiable() {
        return false;
    }
    predicates
        .predicates()
        .iter()
        .all(|predicate| search_metadata_matches_predicate(id, metadata, predicate))
}

struct FilteredSearchDocuments<'a> {
    documents: Vec<&'a SearchDocument>,
    segment_count: usize,
    pruned_segment_count: usize,
    scanned_segment_count: usize,
    segment_pruning_candidate_document_count: usize,
    segment_pruned_document_count: usize,
    segment_scanned_document_count: usize,
    persisted_segment_descriptor_used: bool,
    field_summaries: Vec<SearchPredicateFieldPruningReport>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SearchNumericRange {
    min: f64,
    max: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchTimestampRange {
    min_epoch_millis: i64,
    max_epoch_millis: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchFilterSegmentSummary {
    document_count: usize,
    present_counts: BTreeMap<String, usize>,
    values: BTreeMap<String, BTreeSet<String>>,
    numeric_ranges: BTreeMap<String, SearchNumericRange>,
    timestamp_ranges: BTreeMap<String, SearchTimestampRange>,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchSegmentDescriptor {
    target_documents: usize,
    document_count: usize,
    segments: Vec<SearchSegmentDescriptorEntry>,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchSegmentDescriptorEntry {
    segment_id: u64,
    first_document_id: String,
    last_document_id: String,
    document_count: usize,
    payload_range: Option<SearchSegmentPayloadRange>,
    metadata: BTreeMap<String, SearchSegmentFieldSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchSegmentPayloadRange {
    artifact_id: u64,
    offset: u64,
    length: u64,
    checksum: u64,
}

#[derive(Debug, Default)]
struct SearchPhysicalRangeRead {
    documents: BTreeMap<String, SearchDocument>,
    range_count: usize,
    bytes_read: u64,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct SearchSegmentFieldSummary {
    present_count: usize,
    /// The complete set of distinct normalized values, used to skip a segment
    /// for `Eq`, `In` and `NotIn`. Empty when `values_omitted` is set.
    values: BTreeSet<String>,
    /// The value set exceeded the segment value-summary bounds, so it is not
    /// kept and value predicates on this field cannot skip the segment (#1746).
    values_omitted: bool,
    numeric_range: Option<SearchNumericRange>,
    timestamp_range: Option<SearchTimestampRange>,
}

/// Largest normalized value an out-of-core segment value summary keeps (#1746).
/// Longer values, such as document bodies copied into metadata, omit the
/// field's value summary for the segment.
const SEARCH_SEGMENT_VALUE_SUMMARY_MAX_VALUE_BYTES: usize = 256;

/// Largest sum of distinct normalized value bytes one out-of-core segment
/// keeps per field (#1746). With 128-document segments this is at most 64
/// bytes per document per field, independent of document size.
const SEARCH_SEGMENT_VALUE_SUMMARY_MAX_FIELD_BYTES: usize = 8 * 1024;

fn filter_search_documents_with_segment_pruning<'a>(
    documents: &'a impl DocumentView,
    predicates: &SearchPredicateSet,
    descriptor: Option<&'a SearchSegmentDescriptor>,
) -> FilteredSearchDocuments<'a> {
    if predicates.is_empty() {
        return FilteredSearchDocuments {
            documents: documents.values().collect(),
            segment_count: 0,
            pruned_segment_count: 0,
            scanned_segment_count: 0,
            segment_pruning_candidate_document_count: 0,
            segment_pruned_document_count: 0,
            segment_scanned_document_count: 0,
            persisted_segment_descriptor_used: false,
            field_summaries: Vec::new(),
        };
    }

    if let Some(descriptor) =
        descriptor.filter(|descriptor| descriptor.matches_documents(documents))
    {
        return filter_search_documents_with_persisted_segments(documents, predicates, descriptor);
    }

    let predicate_fields = search_predicate_fields(predicates);
    let mut filtered_documents = Vec::new();
    let mut segment_documents = Vec::with_capacity(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
    let mut pruning = SearchSegmentPruningState::new(predicates);

    for document in documents.values() {
        segment_documents.push(document);
        if segment_documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
            filter_search_document_segment(
                &mut filtered_documents,
                &mut pruning,
                &segment_documents,
                &predicate_fields,
                predicates,
            );
            segment_documents.clear();
        }
    }

    if !segment_documents.is_empty() {
        filter_search_document_segment(
            &mut filtered_documents,
            &mut pruning,
            &segment_documents,
            &predicate_fields,
            predicates,
        );
    }

    FilteredSearchDocuments {
        documents: filtered_documents,
        segment_count: pruning.segment_count,
        pruned_segment_count: pruning.pruned_segment_count,
        scanned_segment_count: pruning.scanned_segment_count,
        segment_pruning_candidate_document_count: pruning.segment_pruning_candidate_document_count,
        segment_pruned_document_count: pruning.segment_pruned_document_count,
        segment_scanned_document_count: pruning.segment_scanned_document_count,
        persisted_segment_descriptor_used: false,
        field_summaries: pruning.field_pruning.into_reports(),
    }
}

fn filter_search_documents_with_persisted_segments<'a>(
    documents: &'a impl DocumentView,
    predicates: &SearchPredicateSet,
    descriptor: &'a SearchSegmentDescriptor,
) -> FilteredSearchDocuments<'a> {
    let mut filtered_documents = Vec::new();
    let mut pruned_segment_count = 0;
    let mut scanned_segment_count = 0;
    let mut pruned_document_count = 0;
    let mut scanned_document_count = 0;
    let mut field_pruning = SearchFieldPruningAccumulator::new(predicates);

    for segment in &descriptor.segments {
        field_pruning.observe_persisted_segment(segment, predicates);
        if !segment.may_match_predicates(predicates) {
            pruned_segment_count += 1;
            pruned_document_count += segment.document_count;
            continue;
        }
        scanned_segment_count += 1;
        scanned_document_count += segment.document_count;
        filtered_documents.extend(
            documents
                .range_documents(&segment.first_document_id, &segment.last_document_id)
                .map(|(_, document)| document)
                .filter(|document| search_document_matches_predicates(document, predicates)),
        );
    }

    FilteredSearchDocuments {
        documents: filtered_documents,
        segment_count: descriptor.segments.len(),
        pruned_segment_count,
        scanned_segment_count,
        segment_pruning_candidate_document_count: descriptor.document_count,
        segment_pruned_document_count: pruned_document_count,
        segment_scanned_document_count: scanned_document_count,
        persisted_segment_descriptor_used: true,
        field_summaries: field_pruning.into_reports(),
    }
}

fn filter_search_document_segment<'a>(
    output: &mut Vec<&'a SearchDocument>,
    pruning: &mut SearchSegmentPruningState,
    segment_documents: &[&'a SearchDocument],
    predicate_fields: &BTreeSet<String>,
    predicates: &SearchPredicateSet,
) {
    pruning.segment_count += 1;
    let summary = SearchFilterSegmentSummary::from_documents(segment_documents, predicate_fields);
    pruning.segment_pruning_candidate_document_count += summary.document_count;
    pruning
        .field_pruning
        .observe_in_memory_segment(&summary, predicates);
    if !summary.may_match_predicates(predicates) {
        pruning.pruned_segment_count += 1;
        pruning.segment_pruned_document_count += summary.document_count;
        return;
    }
    pruning.scanned_segment_count += 1;
    pruning.segment_scanned_document_count += summary.document_count;
    output.extend(
        segment_documents
            .iter()
            .copied()
            .filter(|document| search_document_matches_predicates(document, predicates)),
    );
}

fn search_predicate_fields(predicates: &SearchPredicateSet) -> BTreeSet<String> {
    predicates
        .predicates()
        .iter()
        .map(|predicate| predicate.field().name().to_string())
        .collect()
}

#[derive(Debug, Clone)]
struct SearchSegmentPruningState {
    segment_count: usize,
    pruned_segment_count: usize,
    scanned_segment_count: usize,
    segment_pruning_candidate_document_count: usize,
    segment_pruned_document_count: usize,
    segment_scanned_document_count: usize,
    field_pruning: SearchFieldPruningAccumulator,
}

impl SearchSegmentPruningState {
    fn new(predicates: &SearchPredicateSet) -> Self {
        Self {
            segment_count: 0,
            pruned_segment_count: 0,
            scanned_segment_count: 0,
            segment_pruning_candidate_document_count: 0,
            segment_pruned_document_count: 0,
            segment_scanned_document_count: 0,
            field_pruning: SearchFieldPruningAccumulator::new(predicates),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SearchFieldPruningAccumulator {
    fields: BTreeMap<String, SearchFieldPruningStats>,
}

#[derive(Debug, Clone, Default)]
struct SearchFieldPruningStats {
    value_kinds: BTreeSet<String>,
    operation_kinds: BTreeSet<String>,
    segment_count: usize,
    pruned_segment_count: usize,
    scanned_segment_count: usize,
    numeric_range_summary_used: bool,
    timestamp_range_summary_used: bool,
    value_summary_used: bool,
}

impl SearchFieldPruningAccumulator {
    fn new(predicates: &SearchPredicateSet) -> Self {
        let mut fields = BTreeMap::<String, SearchFieldPruningStats>::new();
        for predicate in predicates.predicates() {
            let stats = fields
                .entry(predicate.field().name().to_string())
                .or_default();
            stats
                .operation_kinds
                .insert(search_predicate_op_kind(predicate.op()).to_string());
            stats
                .value_kinds
                .insert(search_predicate_value_kind(predicate).to_string());
            match predicate.op() {
                SearchPredicateOp::Eq(_)
                | SearchPredicateOp::In(_)
                | SearchPredicateOp::NotIn(_) => stats.value_summary_used = true,
                SearchPredicateOp::Gt(_)
                | SearchPredicateOp::Gte(_)
                | SearchPredicateOp::Lt(_)
                | SearchPredicateOp::Lte(_) => {
                    stats.numeric_range_summary_used = true;
                    if range_predicate_expects_timestamp(predicate) {
                        stats.timestamp_range_summary_used = true;
                    }
                }
                SearchPredicateOp::Exists | SearchPredicateOp::IsMissing => {
                    stats.value_summary_used = true;
                }
            }
        }
        Self { fields }
    }

    fn observe_in_memory_segment(
        &mut self,
        summary: &SearchFilterSegmentSummary,
        predicates: &SearchPredicateSet,
    ) {
        self.observe_segment(predicates, |field_predicates| {
            field_predicates
                .iter()
                .all(|predicate| summary.may_match_predicate(predicate))
        });
    }

    fn observe_persisted_segment(
        &mut self,
        segment: &SearchSegmentDescriptorEntry,
        predicates: &SearchPredicateSet,
    ) {
        self.observe_segment(predicates, |field_predicates| {
            field_predicates
                .iter()
                .all(|predicate| segment.may_match_predicate(predicate))
        });
    }

    fn observe_segment(
        &mut self,
        predicates: &SearchPredicateSet,
        mut may_match: impl FnMut(&[&SearchPredicate]) -> bool,
    ) {
        let mut predicates_by_field = BTreeMap::<String, Vec<&SearchPredicate>>::new();
        for predicate in predicates.predicates() {
            predicates_by_field
                .entry(predicate.field().name().to_string())
                .or_default()
                .push(predicate);
        }
        for (field, field_predicates) in predicates_by_field {
            let stats = self.fields.entry(field).or_default();
            stats.segment_count += 1;
            if may_match(&field_predicates) {
                stats.scanned_segment_count += 1;
            } else {
                stats.pruned_segment_count += 1;
            }
        }
    }

    fn into_reports(self) -> Vec<SearchPredicateFieldPruningReport> {
        self.fields
            .into_iter()
            .map(|(field, stats)| SearchPredicateFieldPruningReport {
                field,
                value_kind: search_pruning_value_kind(&stats.value_kinds),
                operation_kinds: stats.operation_kinds.into_iter().collect(),
                segment_count: stats.segment_count,
                pruned_segment_count: stats.pruned_segment_count,
                scanned_segment_count: stats.scanned_segment_count,
                numeric_range_summary_used: stats.numeric_range_summary_used,
                timestamp_range_summary_used: stats.timestamp_range_summary_used,
                value_summary_used: stats.value_summary_used,
            })
            .collect()
    }
}

fn search_pruning_value_kind(value_kinds: &BTreeSet<String>) -> String {
    match value_kinds.len() {
        0 => "unknown".to_string(),
        1 => value_kinds
            .first()
            .cloned()
            .unwrap_or_else(|| "unknown".to_string()),
        _ => "mixed".to_string(),
    }
}

fn search_predicate_value_kind(predicate: &SearchPredicate) -> &'static str {
    match predicate.op() {
        SearchPredicateOp::Eq(value)
        | SearchPredicateOp::Gt(value)
        | SearchPredicateOp::Gte(value)
        | SearchPredicateOp::Lt(value)
        | SearchPredicateOp::Lte(value) => search_scalar_report_kind(value),
        SearchPredicateOp::In(values) | SearchPredicateOp::NotIn(values) => values
            .iter()
            .next()
            .map(search_scalar_report_kind)
            .unwrap_or("unknown"),
        SearchPredicateOp::Exists | SearchPredicateOp::IsMissing => "presence",
    }
}

fn search_scalar_report_kind(value: &SearchScalarValue) -> &'static str {
    match value {
        SearchScalarValue::Enum(_) => "enum",
        SearchScalarValue::String(_) => "numeric_or_string",
    }
}

fn search_predicate_op_kind(op: &SearchPredicateOp) -> &'static str {
    match op {
        SearchPredicateOp::Eq(_) => "eq",
        SearchPredicateOp::In(_) => "in",
        SearchPredicateOp::NotIn(_) => "not_in",
        SearchPredicateOp::Gt(_) => "gt",
        SearchPredicateOp::Gte(_) => "gte",
        SearchPredicateOp::Lt(_) => "lt",
        SearchPredicateOp::Lte(_) => "lte",
        SearchPredicateOp::Exists => "exists",
        SearchPredicateOp::IsMissing => "missing",
    }
}

fn range_predicate_expects_timestamp(predicate: &SearchPredicate) -> bool {
    match predicate.op() {
        SearchPredicateOp::Gt(value)
        | SearchPredicateOp::Gte(value)
        | SearchPredicateOp::Lt(value)
        | SearchPredicateOp::Lte(value) => metadata_timestamp_value(value.as_str()).is_some(),
        _ => false,
    }
}

impl SearchFilterSegmentSummary {
    fn from_documents(documents: &[&SearchDocument], fields: &BTreeSet<String>) -> Self {
        let mut present_counts = BTreeMap::new();
        let mut values = BTreeMap::<String, BTreeSet<String>>::new();
        let mut numeric_ranges = BTreeMap::<String, SearchNumericRange>::new();
        let mut timestamp_ranges = BTreeMap::<String, SearchTimestampRange>::new();
        for document in documents {
            for field in fields {
                let actual_values = search_document_field_values(document, field);
                if actual_values.is_empty() {
                    continue;
                }
                *present_counts.entry(field.clone()).or_insert(0) += 1;
                for value in actual_values {
                    values
                        .entry(field.clone())
                        .or_default()
                        .insert(search_segment_summary_value(field, value.as_ref()));
                    if let Some(number) = metadata_numeric_value(value.as_ref()) {
                        numeric_ranges
                            .entry(field.clone())
                            .and_modify(|range| *range = range.with_value(number))
                            .or_insert_with(|| SearchNumericRange::point(number));
                    }
                    if let Some(timestamp) = metadata_timestamp_value(value.as_ref()) {
                        timestamp_ranges
                            .entry(field.clone())
                            .and_modify(|range| *range = range.with_value(timestamp))
                            .or_insert_with(|| SearchTimestampRange::point(timestamp));
                    }
                }
            }
        }
        Self {
            document_count: documents.len(),
            present_counts,
            values,
            numeric_ranges,
            timestamp_ranges,
        }
    }

    fn may_match_predicates(&self, predicates: &SearchPredicateSet) -> bool {
        if predicates.is_unsatisfiable() {
            return false;
        }
        // Each predicate is pruned on its own field, so only those fields
        // are summarized (#1613).
        let summary = self.storage_summary_for(predicates);
        predicates.predicates().iter().all(|predicate| {
            if !self.may_match_predicate(predicate) {
                return false;
            }
            search_storage_scan_predicate(predicate).map_or_else(
                || true,
                |predicate| {
                    SegmentPruner::new(&summary)
                        .evaluate(&predicate)
                        .should_open_payload()
                },
            )
        })
    }

    fn storage_summary_for(&self, predicates: &SearchPredicateSet) -> SegmentSummary {
        let fields = predicates
            .predicates()
            .iter()
            .map(|predicate| predicate.field().name())
            .collect::<BTreeSet<_>>();
        self.storage_summary_fields(|field| fields.contains(field))
    }

    fn storage_summary_fields(&self, include: impl Fn(&str) -> bool) -> SegmentSummary {
        let mut segment = SegmentSummary::new(0, self.document_count as u64);
        for field in self
            .present_counts
            .keys()
            .chain(self.values.keys())
            .chain(self.numeric_ranges.keys())
            .chain(self.timestamp_ranges.keys())
            .filter(|field| include(field))
        {
            if segment.field(field).is_some() {
                continue;
            }
            let present_count = self.present_counts.get(field).copied().unwrap_or_default() as u64;
            let mut summary = FieldSummary::new(self.document_count as u64)
                .with_presence_counts(present_count, 0, self.document_count as u64 - present_count)
                .expect("search summary counts must match segment rows");
            if let Some(range) = self.numeric_ranges.get(field) {
                summary = summary
                    .with_numeric_min_max(range.min, range.max)
                    .expect("search numeric summary must be finite and ordered");
            }
            if let Some(range) = self.timestamp_ranges.get(field) {
                summary = summary
                    .with_datetime_min_max(range.min_epoch_millis, range.max_epoch_millis)
                    .expect("search timestamp summary must be ordered");
            }
            if let Some(values) = self.values.get(field) {
                summary = summary.with_enum_dictionary(EnumDictionaryStats::complete(
                    values
                        .iter()
                        .map(|value| search_segment_summary_value(field, value))
                        .map(Value::String),
                ));
            }
            segment.insert_field(field.clone(), summary);
        }
        segment
    }

    fn may_match_predicate(&self, predicate: &SearchPredicate) -> bool {
        match predicate.op() {
            SearchPredicateOp::Eq(expected) => self
                .values_may_match_any(predicate.field().name(), std::iter::once(expected.as_str())),
            SearchPredicateOp::In(expected_values) => self.values_may_match_any(
                predicate.field().name(),
                expected_values.iter().map(|value| value.as_str()),
            ),
            SearchPredicateOp::NotIn(excluded_values) => {
                self.values_may_match_not_in(predicate.field().name(), excluded_values)
            }
            SearchPredicateOp::Gt(expected)
            | SearchPredicateOp::Gte(expected)
            | SearchPredicateOp::Lt(expected)
            | SearchPredicateOp::Lte(expected) => self.numeric_range_may_match(
                predicate.field().name(),
                predicate.op(),
                expected.as_str(),
            ),
            SearchPredicateOp::Exists => self.field_may_exist(predicate.field().name()),
            SearchPredicateOp::IsMissing => self.field_may_be_missing(predicate.field().name()),
        }
    }

    fn field_may_exist(&self, field: &str) -> bool {
        self.present_counts.get(field).copied().unwrap_or_default() > 0
    }

    fn field_may_be_missing(&self, field: &str) -> bool {
        self.present_counts.get(field).copied().unwrap_or_default() < self.document_count
    }

    fn values_may_match_any<'a>(
        &self,
        field: &str,
        expected_values: impl Iterator<Item = &'a str>,
    ) -> bool {
        let Some(actual_values) = self.values.get(field) else {
            return false;
        };
        expected_values.into_iter().any(|expected| {
            actual_values
                .iter()
                .any(|actual| metadata_value_matches(field, actual, expected))
        })
    }

    fn values_may_match_not_in(
        &self,
        field: &str,
        excluded_values: &BTreeSet<hawdb_optimizer_predicate::SearchScalarValue>,
    ) -> bool {
        let present_count = self.present_counts.get(field).copied().unwrap_or_default();
        if present_count < self.document_count {
            return true;
        }
        let Some(actual_values) = self.values.get(field) else {
            return true;
        };
        actual_values.iter().any(|actual| {
            excluded_values
                .iter()
                .all(|excluded| !metadata_value_matches(field, actual, excluded.as_str()))
        })
    }

    fn numeric_range_may_match(&self, field: &str, op: &SearchPredicateOp, expected: &str) -> bool {
        metadata_range_may_match(
            self.numeric_ranges.get(field).copied(),
            self.timestamp_ranges.get(field).copied(),
            op,
            expected,
        )
    }
}

impl SearchNumericRange {
    fn point(value: f64) -> Self {
        Self {
            min: value,
            max: value,
        }
    }

    fn with_value(self, value: f64) -> Self {
        Self {
            min: self.min.min(value),
            max: self.max.max(value),
        }
    }
}

impl SearchTimestampRange {
    fn point(value: i64) -> Self {
        Self {
            min_epoch_millis: value,
            max_epoch_millis: value,
        }
    }

    fn with_value(self, value: i64) -> Self {
        Self {
            min_epoch_millis: self.min_epoch_millis.min(value),
            max_epoch_millis: self.max_epoch_millis.max(value),
        }
    }
}

impl SearchSegmentDescriptor {
    /// Whether any segment omits a value set, which requires the V4 grammar.
    fn omits_value_summaries(&self) -> bool {
        self.segments
            .iter()
            .any(SearchSegmentDescriptorEntry::omits_value_summaries)
    }

    fn build(documents: &impl DocumentView) -> Self {
        let fields = search_segment_descriptor_fields(documents);
        let mut segments = Vec::new();
        let mut segment_documents = Vec::with_capacity(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
        for document in documents.values() {
            segment_documents.push(document);
            if segment_documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
                segments.push(SearchSegmentDescriptorEntry::from_documents(
                    segments.len() as u64,
                    &segment_documents,
                    &fields,
                ));
                segment_documents.clear();
            }
        }
        if !segment_documents.is_empty() {
            segments.push(SearchSegmentDescriptorEntry::from_documents(
                segments.len() as u64,
                &segment_documents,
                &fields,
            ));
        }
        Self {
            target_documents: SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
            document_count: documents.len(),
            segments,
        }
    }

    fn matches_documents(&self, documents: &impl DocumentView) -> bool {
        self.target_documents == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS
            && self.document_count == documents.len()
            && self
                .segments
                .first()
                .map(|segment| segment.first_document_id.as_str())
                == documents.keys().next().map(String::as_str)
            && self
                .segments
                .last()
                .map(|segment| segment.last_document_id.as_str())
                == documents.keys().next_back().map(String::as_str)
    }

    fn payload_artifact_is_available(&self, path: &Path) -> Result<bool> {
        let Some(last_range) = self
            .segments
            .iter()
            .filter_map(|segment| segment.payload_range)
            .next_back()
        else {
            return Ok(true);
        };
        match fs::metadata(path.join(SEARCH_SEGMENT_PAYLOAD_FILE)) {
            Ok(metadata) => {
                Ok(metadata.len() >= last_range.offset.saturating_add(last_range.length))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn payload_artifact_is_available_under_publication(
        &self,
        publication: &out_of_core::SearchProjectionPublishLease,
    ) -> Result<bool> {
        let Some(last_range) = self
            .segments
            .iter()
            .filter_map(|segment| segment.payload_range)
            .next_back()
        else {
            return Ok(true);
        };
        match publication.open_captured_child(SEARCH_SEGMENT_PAYLOAD_FILE) {
            Ok(file) => {
                Ok(file.metadata()?.len() >= last_range.offset.saturating_add(last_range.length))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn physical_read_ranges(&self) -> Vec<SegmentReadRange> {
        self.segments
            .iter()
            .filter_map(|segment| {
                let range = segment.payload_range?;
                Some(SegmentReadRange::new(
                    range.artifact_id,
                    segment.segment_id,
                    range.offset,
                    std::num::NonZeroU64::new(range.length)?,
                ))
            })
            .collect()
    }
}

impl SearchSegmentDescriptorEntry {
    /// Whether any field omits its value set, which requires the V4 grammar.
    fn omits_value_summaries(&self) -> bool {
        self.metadata.values().any(|summary| summary.values_omitted)
    }

    fn from_documents(
        segment_id: u64,
        documents: &[&SearchDocument],
        fields: &BTreeSet<String>,
    ) -> Self {
        let first_document_id = documents
            .first()
            .map(|document| document.id.clone())
            .unwrap_or_default();
        let last_document_id = documents
            .last()
            .map(|document| document.id.clone())
            .unwrap_or_default();
        let mut metadata = fields
            .iter()
            .map(|field| (field.clone(), SearchSegmentFieldSummary::default()))
            .collect::<BTreeMap<_, _>>();
        for document in documents {
            for field in fields {
                let values = search_document_field_values(document, field);
                if values.is_empty() {
                    continue;
                }
                let summary = metadata.entry(field.clone()).or_default();
                summary.present_count += 1;
                for value in values {
                    summary
                        .values
                        .insert(search_segment_summary_value(field, value.as_ref()));
                    summary.update_range_summaries(value.as_ref());
                }
            }
        }
        Self {
            segment_id,
            first_document_id,
            last_document_id,
            document_count: documents.len(),
            payload_range: None,
            metadata,
        }
    }

    fn may_match_predicates(&self, predicates: &SearchPredicateSet) -> bool {
        if predicates.is_unsatisfiable() {
            return false;
        }
        // Each predicate is pruned on its own field, so only those fields
        // are summarized (#1613).
        let summary = self.storage_summary_for(predicates);
        predicates.predicates().iter().all(|predicate| {
            if !self.may_match_predicate(predicate) {
                return false;
            }
            search_storage_scan_predicate(predicate).map_or_else(
                || true,
                |predicate| {
                    SegmentPruner::new(&summary)
                        .evaluate(&predicate)
                        .should_open_payload()
                },
            )
        })
    }

    fn storage_summary_for(&self, predicates: &SearchPredicateSet) -> SegmentSummary {
        let fields = predicates
            .predicates()
            .iter()
            .map(|predicate| predicate.field().name())
            .collect::<BTreeSet<_>>();
        let mut segment = SegmentSummary::new(0, self.document_count as u64);
        for (field, persisted) in self
            .metadata
            .iter()
            .filter(|(field, _)| fields.contains(field.as_str()))
        {
            let mut summary = FieldSummary::new(self.document_count as u64)
                .with_presence_counts(
                    persisted.present_count as u64,
                    0,
                    self.document_count as u64 - persisted.present_count as u64,
                )
                .expect("persisted search summary counts must match segment rows");
            if let Some(range) = persisted.numeric_range {
                summary = summary
                    .with_numeric_min_max(range.min, range.max)
                    .expect("persisted numeric summary must be finite and ordered");
            }
            if let Some(range) = persisted.timestamp_range {
                summary = summary
                    .with_datetime_min_max(range.min_epoch_millis, range.max_epoch_millis)
                    .expect("persisted timestamp summary must be ordered");
            }
            // An omitted value set proves nothing, so it adds no dictionary.
            if !persisted.values_omitted {
                summary = summary.with_enum_dictionary(EnumDictionaryStats::complete(
                    persisted
                        .values
                        .iter()
                        .map(|value| search_segment_summary_value(field, value))
                        .map(Value::String),
                ));
            }
            segment.insert_field(field.clone(), summary);
        }
        segment
    }

    fn may_match_predicate(&self, predicate: &SearchPredicate) -> bool {
        match predicate.op() {
            SearchPredicateOp::Eq(expected) => self
                .values_may_match_any(predicate.field().name(), std::iter::once(expected.as_str())),
            SearchPredicateOp::In(expected_values) => self.values_may_match_any(
                predicate.field().name(),
                expected_values.iter().map(|value| value.as_str()),
            ),
            SearchPredicateOp::NotIn(excluded_values) => {
                self.values_may_match_not_in(predicate.field().name(), excluded_values)
            }
            SearchPredicateOp::Gt(expected)
            | SearchPredicateOp::Gte(expected)
            | SearchPredicateOp::Lt(expected)
            | SearchPredicateOp::Lte(expected) => self.numeric_range_may_match(
                predicate.field().name(),
                predicate.op(),
                expected.as_str(),
            ),
            SearchPredicateOp::Exists => self.field_may_exist(predicate.field().name()),
            SearchPredicateOp::IsMissing => self.field_may_be_missing(predicate.field().name()),
        }
    }

    fn field_may_exist(&self, field: &str) -> bool {
        self.metadata
            .get(field)
            .is_some_and(|summary| summary.present_count > 0)
    }

    fn field_may_be_missing(&self, field: &str) -> bool {
        self.metadata
            .get(field)
            .is_none_or(|summary| summary.present_count < self.document_count)
    }

    fn values_may_match_any<'a>(
        &self,
        field: &str,
        expected_values: impl Iterator<Item = &'a str>,
    ) -> bool {
        let Some(summary) = self.metadata.get(field) else {
            return false;
        };
        if summary.values_omitted {
            return summary.present_count > 0;
        }
        expected_values.into_iter().any(|expected| {
            summary
                .values
                .iter()
                .any(|actual| metadata_value_matches(field, actual, expected))
        })
    }

    fn values_may_match_not_in(
        &self,
        field: &str,
        excluded_values: &BTreeSet<hawdb_optimizer_predicate::SearchScalarValue>,
    ) -> bool {
        let Some(summary) = self.metadata.get(field) else {
            return true;
        };
        if summary.present_count < self.document_count || summary.values_omitted {
            return true;
        }
        summary.values.iter().any(|actual| {
            excluded_values
                .iter()
                .all(|excluded| !metadata_value_matches(field, actual, excluded.as_str()))
        })
    }

    fn numeric_range_may_match(&self, field: &str, op: &SearchPredicateOp, expected: &str) -> bool {
        metadata_range_may_match(
            self.metadata
                .get(field)
                .and_then(|summary| summary.numeric_range),
            self.metadata
                .get(field)
                .and_then(|summary| summary.timestamp_range),
            op,
            expected,
        )
    }
}

fn search_storage_scan_predicate(predicate: &SearchPredicate) -> Option<ScanPredicate> {
    let property = predicate.field().name().to_string();
    let summary_value = |value: &SearchScalarValue| {
        Value::String(search_segment_summary_value(
            predicate.field().name(),
            value.as_str(),
        ))
    };
    match predicate.op() {
        SearchPredicateOp::Eq(value) => Some(ScanPredicate::Eq {
            property,
            value: summary_value(value),
        }),
        SearchPredicateOp::In(values) => Some(ScanPredicate::In {
            property,
            values: values.iter().map(summary_value).collect(),
        }),
        SearchPredicateOp::NotIn(_) => None,
        SearchPredicateOp::Gt(value) => Some(ScanPredicate::Range {
            property,
            lower: Some(RangeBound::exclusive(search_range_summary_value(value))),
            upper: None,
        }),
        SearchPredicateOp::Gte(value) => Some(ScanPredicate::Range {
            property,
            lower: Some(RangeBound::inclusive(search_range_summary_value(value))),
            upper: None,
        }),
        SearchPredicateOp::Lt(value) => Some(ScanPredicate::Range {
            property,
            lower: None,
            upper: Some(RangeBound::exclusive(search_range_summary_value(value))),
        }),
        SearchPredicateOp::Lte(value) => Some(ScanPredicate::Range {
            property,
            lower: None,
            upper: Some(RangeBound::inclusive(search_range_summary_value(value))),
        }),
        SearchPredicateOp::Exists => Some(ScanPredicate::Exists { property }),
        SearchPredicateOp::IsMissing => Some(ScanPredicate::IsMissing { property }),
    }
}

fn search_range_summary_value(value: &SearchScalarValue) -> Value {
    metadata_numeric_value(value.as_str())
        .map(Value::Float)
        .unwrap_or_else(|| Value::String(value.as_str().to_string()))
}

impl SearchSegmentFieldSummary {
    /// Drop the value set; value predicates can no longer skip the segment.
    fn omit_values(&mut self) {
        self.values = BTreeSet::new();
        self.values_omitted = true;
    }

    fn update_range_summaries(&mut self, value: &str) {
        if let Some(number) = metadata_numeric_value(value) {
            self.numeric_range = Some(match self.numeric_range {
                Some(range) => range.with_value(number),
                None => SearchNumericRange::point(number),
            });
        }
        if let Some(timestamp) = metadata_timestamp_value(value) {
            self.timestamp_range = Some(match self.timestamp_range {
                Some(range) => range.with_value(timestamp),
                None => SearchTimestampRange::point(timestamp),
            });
        }
    }
}

fn search_segment_descriptor_fields(documents: &impl DocumentView) -> BTreeSet<String> {
    let mut fields = documents
        .values()
        .flat_map(|document| document.metadata.keys().cloned())
        .collect::<BTreeSet<_>>();
    fields.insert(SEARCH_DOCUMENT_ID_FIELD.to_string());
    fields.extend(
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
            .iter()
            .map(|field| field.to_string()),
    );
    fields
}

fn search_header_matches_predicate(
    header: crate::document_encoding::Header<'_>,
    predicate: &SearchPredicate,
) -> bool {
    let actual_values = search_header_field_values(header, predicate.field().name());
    match predicate.op() {
        SearchPredicateOp::Eq(expected) => actual_values.iter().any(|actual| {
            metadata_value_matches(predicate.field().name(), actual.as_ref(), expected.as_str())
        }),
        SearchPredicateOp::In(expected_values) => actual_values.iter().any(|actual| {
            expected_values.iter().any(|expected| {
                metadata_value_matches(predicate.field().name(), actual.as_ref(), expected.as_str())
            })
        }),
        SearchPredicateOp::NotIn(excluded_values) => actual_values.iter().all(|actual| {
            excluded_values.iter().all(|excluded| {
                !metadata_value_matches(
                    predicate.field().name(),
                    actual.as_ref(),
                    excluded.as_str(),
                )
            })
        }),
        SearchPredicateOp::Gt(expected) => actual_values
            .iter()
            .any(|actual| metadata_range_gt(actual.as_ref(), expected.as_str())),
        SearchPredicateOp::Gte(expected) => actual_values
            .iter()
            .any(|actual| metadata_range_gte(actual.as_ref(), expected.as_str())),
        SearchPredicateOp::Lt(expected) => actual_values
            .iter()
            .any(|actual| metadata_range_lt(actual.as_ref(), expected.as_str())),
        SearchPredicateOp::Lte(expected) => actual_values
            .iter()
            .any(|actual| metadata_range_lte(actual.as_ref(), expected.as_str())),
        SearchPredicateOp::Exists => !actual_values.is_empty(),
        SearchPredicateOp::IsMissing => actual_values.is_empty(),
    }
}

fn search_document_field_values<'a>(document: &'a SearchDocument, key: &str) -> Vec<Cow<'a, str>> {
    use crate::document_encoding::HeaderSource;
    search_header_field_values(document.header(), key)
}

fn search_header_field_values<'a>(
    header: crate::document_encoding::Header<'a>,
    key: &str,
) -> Vec<Cow<'a, str>> {
    if key == "labels" || key.starts_with("metadata.") {
        return header
            .metadata
            .get(key)
            .map(|value| parse_label_metadata_values(value))
            .unwrap_or_default();
    }
    header.field(key).map(Cow::Borrowed).into_iter().collect()
}

fn parse_label_metadata_values(value: &str) -> Vec<Cow<'_, str>> {
    if let Ok(values) = serde_json::from_str::<Vec<String>>(value) {
        return values
            .into_iter()
            .map(|value| Cow::<str>::Owned(value.trim().to_string()))
            .filter(|value| !value.is_empty())
            .collect();
    }
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Cow::Borrowed)
        .collect()
}

fn metadata_value_matches(key: &str, actual: &str, expected: &str) -> bool {
    normalize_metadata_posting_value(key, actual) == normalize_metadata_posting_value(key, expected)
}

/// Canonical value shared by metadata matching and persistent postings.
fn normalize_metadata_posting_value(key: &str, value: &str) -> String {
    match key {
        "kind" => normalized_projection_kind(value)
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string()),
        _ => normalize_metadata_filter_value(value),
    }
}

fn search_segment_summary_value(key: &str, value: &str) -> String {
    match key {
        "kind" => normalized_projection_kind(value)
            .map(str::to_string)
            .unwrap_or_else(|| normalize_search_enum_value(value)),
        key if search_field_is_enum_like(key) => normalize_search_enum_value(value),
        _ => normalize_metadata_filter_value(value),
    }
}

fn normalize_metadata_filter_value(value: &str) -> String {
    value.trim().to_lowercase()
}

fn metadata_numeric_value(value: &str) -> Option<f64> {
    let number = value.trim().parse::<f64>().ok()?;
    number.is_finite().then_some(number)
}

fn metadata_timestamp_value(value: &str) -> Option<i64> {
    hawdb_core::temporal::parse_metadata_millis(value)
}

fn metadata_range_gt(actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual > expected)
        || metadata_timestamp_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual > expected)
}

fn metadata_range_gte(actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual >= expected)
        || metadata_timestamp_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual >= expected)
}

fn metadata_range_lt(actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual < expected)
        || metadata_timestamp_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual < expected)
}

fn metadata_range_lte(actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual <= expected)
        || metadata_timestamp_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual <= expected)
}

fn metadata_numeric_pair(actual: &str, expected: &str) -> Option<(f64, f64)> {
    Some((
        metadata_numeric_value(actual)?,
        metadata_numeric_value(expected)?,
    ))
}

fn metadata_timestamp_pair(actual: &str, expected: &str) -> Option<(i64, i64)> {
    Some((
        metadata_timestamp_value(actual)?,
        metadata_timestamp_value(expected)?,
    ))
}

fn metadata_range_may_match(
    numeric_range: Option<SearchNumericRange>,
    timestamp_range: Option<SearchTimestampRange>,
    op: &SearchPredicateOp,
    expected: &str,
) -> bool {
    if let Some(expected) = metadata_numeric_value(expected) {
        return numeric_range
            .map(|range| metadata_numeric_range_may_match(range, op, expected))
            .unwrap_or(false);
    }
    if let Some(expected) = metadata_timestamp_value(expected) {
        return timestamp_range
            .map(|range| metadata_timestamp_range_may_match(range, op, expected))
            .unwrap_or(false);
    }
    false
}

fn metadata_numeric_range_may_match(
    range: SearchNumericRange,
    op: &SearchPredicateOp,
    expected: f64,
) -> bool {
    match op {
        SearchPredicateOp::Gt(_) => range.max > expected,
        SearchPredicateOp::Gte(_) => range.max >= expected,
        SearchPredicateOp::Lt(_) => range.min < expected,
        SearchPredicateOp::Lte(_) => range.min <= expected,
        SearchPredicateOp::Eq(_)
        | SearchPredicateOp::In(_)
        | SearchPredicateOp::NotIn(_)
        | SearchPredicateOp::Exists
        | SearchPredicateOp::IsMissing => true,
    }
}

fn metadata_timestamp_range_may_match(
    range: SearchTimestampRange,
    op: &SearchPredicateOp,
    expected: i64,
) -> bool {
    match op {
        SearchPredicateOp::Gt(_) => range.max_epoch_millis > expected,
        SearchPredicateOp::Gte(_) => range.max_epoch_millis >= expected,
        SearchPredicateOp::Lt(_) => range.min_epoch_millis < expected,
        SearchPredicateOp::Lte(_) => range.min_epoch_millis <= expected,
        SearchPredicateOp::Eq(_)
        | SearchPredicateOp::In(_)
        | SearchPredicateOp::NotIn(_)
        | SearchPredicateOp::Exists
        | SearchPredicateOp::IsMissing => true,
    }
}

fn normalized_projection_kind(value: &str) -> Option<&'static str> {
    match value {
        "Memory" | "memory" => Some("memory"),
        "Message" | "message" => Some("message"),
        "Entity" | "entity" => Some("entity"),
        "Source" | "source" => Some("source"),
        "SourceChunk" | "source_chunk" | "sourcechunk" | "chunk" => Some("source_chunk"),
        "Community" | "community" => Some("community"),
        _ => None,
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
struct TextCorpusStats {
    document_count: usize,
    average_document_len: f64,
    document_frequency: BTreeMap<String, usize>,
}

#[cfg(test)]
impl TextCorpusStats {
    fn from_documents<'a>(
        documents: impl Iterator<Item = &'a SearchDocument>,
        analyzer_lexicon: &SearchAnalyzerLexicon,
    ) -> Self {
        let mut document_count = 0;
        let mut total_len = 0;
        let mut document_frequency = BTreeMap::new();
        for document in documents {
            document_count += 1;
            let tokens = document_tokens(document, analyzer_lexicon);
            total_len += tokens.len();
            for term in tokens.into_iter().collect::<BTreeSet<_>>() {
                *document_frequency.entry(term).or_insert(0) += 1;
            }
        }
        Self {
            document_count,
            average_document_len: if document_count == 0 {
                0.0
            } else {
                total_len as f64 / document_count as f64
            },
            document_frequency,
        }
    }
}

#[cfg(test)]
fn bm25_score(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    corpus: &TextCorpusStats,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> f64 {
    if query_terms.is_empty() || corpus.document_count == 0 {
        return 0.0;
    }
    let tokens = document_tokens(document, analyzer_lexicon);
    if tokens.is_empty() {
        return 0.0;
    }
    let term_frequency = token_frequencies(tokens.iter().cloned());
    let document_len = tokens.len() as f64;
    let average_document_len = corpus.average_document_len.max(1.0);
    let mut score = 0.0;
    for term in query_terms {
        let Some(frequency) = term_frequency.get(term).copied() else {
            continue;
        };
        let document_frequency = corpus
            .document_frequency
            .get(term)
            .copied()
            .unwrap_or_default();
        if document_frequency == 0 {
            continue;
        }
        let idf = (1.0
            + (corpus.document_count as f64 - document_frequency as f64 + 0.5)
                / (document_frequency as f64 + 0.5))
            .ln();
        let frequency = frequency as f64;
        let denominator =
            frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * document_len / average_document_len);
        score += idf * (frequency * (BM25_K1 + 1.0)) / denominator;
    }
    score
}

/// Reference: query terms among the whole document's tokens.
#[cfg(test)]
fn matched_query_terms(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    if query_terms.is_empty() {
        return Vec::new();
    }
    let document_terms = document_tokens(document, analyzer_lexicon)
        .into_iter()
        .collect::<BTreeSet<_>>();
    query_terms
        .iter()
        .filter(|term| document_terms.contains(*term))
        .cloned()
        .collect()
}

#[cfg(test)]
fn matched_query_spans(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<SearchMatchedSpan> {
    matched_query_spans_bounded(
        query_terms,
        document,
        analyzer_lexicon,
        usize::MAX,
        u64::MAX,
        0,
    )
    .expect("unbounded matched-span collection cannot exhaust its admission limit")
}

/// Matched terms and spans of one hit without query memory accounting. The
/// span count and byte limits behave like the reference collector's.
fn query_matches_bounded(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    max_spans: usize,
    max_bytes: u64,
    used_bytes: u64,
) -> Result<(Vec<String>, Vec<SearchMatchedSpan>)> {
    let mut terms = BTreeSet::new();
    let mut spans = Vec::new();
    let mut span_bytes = 0u64;
    query_matches::visit(
        query_terms,
        document,
        analyzer_lexicon,
        analyzer_stream::Control::default(),
        |term| {
            terms.insert(term);
            Ok(())
        },
        || Ok(()),
        |span| {
            if spans.len() >= max_spans {
                return Err(HawDBError::Storage(format!(
                    "search matched-span hydration exceeded {max_spans} spans"
                )));
            }
            let required = (span.field.len() as u64)
                .saturating_add(span.text.len() as u64)
                .saturating_add(span.term.len() as u64)
                .saturating_add(std::mem::size_of::<SearchMatchedSpan>() as u64);
            let previous_bytes = span_bytes;
            span_bytes = span_bytes.saturating_add(required);
            let remaining_bytes = max_bytes.saturating_sub(used_bytes);
            if span_bytes > remaining_bytes {
                return Err(ResourceBudgetError {
                    resource: ResourceBudgetKind::MemoryBytes,
                    scope: ResourceBudgetScope::Operator,
                    owner: "search matched-span hydration".to_string(),
                    memory_class: None,
                    requested: required,
                    used: used_bytes.saturating_add(previous_bytes),
                    budget: max_bytes,
                    budget_option: "SearchOutOfCoreConfig.max_matched_span_bytes",
                    message: format!(
                        "search matched-span hydration requires {span_bytes} bytes, exceeding {remaining_bytes}"
                    ),
                }
                .into());
            }
            spans.push(SearchMatchedSpan {
                field: span.field.to_string(),
                start_byte: span.start,
                end_byte: span.end,
                text: span.text.to_string(),
                term: span.term.to_string(),
            });
            Ok(())
        },
    )?;
    Ok((terms.into_iter().map(str::to_string).collect(), spans))
}

/// Reference: every word of title and content analyzed on its own.
#[cfg(test)]
fn matched_query_spans_bounded(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    max_spans: usize,
    max_bytes: u64,
    used_bytes: u64,
) -> Result<Vec<SearchMatchedSpan>> {
    if query_terms.is_empty() {
        return Ok(Vec::new());
    }
    let mut collector = MatchedSpanCollector {
        query_terms,
        analyzer_lexicon,
        spans: Vec::new(),
        span_bytes: 0,
        max_spans,
        max_bytes,
        used_bytes,
    };
    collector.collect("title", &document.title)?;
    collector.collect("content", &document.content)?;
    Ok(collector.spans)
}

#[cfg(test)]
struct MatchedSpanCollector<'a> {
    query_terms: &'a BTreeSet<String>,
    analyzer_lexicon: &'a SearchAnalyzerLexicon,
    spans: Vec<SearchMatchedSpan>,
    span_bytes: u64,
    max_spans: usize,
    max_bytes: u64,
    used_bytes: u64,
}

#[cfg(test)]
impl MatchedSpanCollector<'_> {
    fn collect(&mut self, field: &str, text: &str) -> Result<()> {
        let mut run_start = None::<usize>;
        for (index, ch) in text.char_indices() {
            if ch.is_alphanumeric() || ch == '_' {
                run_start.get_or_insert(index);
            } else if let Some(start) = run_start.take() {
                self.push(field, text, start, index)?;
            }
        }
        if let Some(start) = run_start {
            self.push(field, text, start, text.len())?;
        }
        Ok(())
    }

    fn push(&mut self, field: &str, text: &str, start_byte: usize, end_byte: usize) -> Result<()> {
        let raw = &text[start_byte..end_byte];
        let matching_terms = identifier_tokens(raw, self.analyzer_lexicon)
            .into_iter()
            .filter(|term| self.query_terms.contains(term))
            .collect::<BTreeSet<_>>();
        for term in matching_terms {
            if self.spans.len() >= self.max_spans {
                return Err(HawDBError::Storage(format!(
                    "search matched-span hydration exceeded {} spans",
                    self.max_spans
                )));
            }
            let required = (field.len() as u64)
                .saturating_add(raw.len() as u64)
                .saturating_add(term.len() as u64)
                .saturating_add(std::mem::size_of::<SearchMatchedSpan>() as u64);
            let previous_bytes = self.span_bytes;
            self.span_bytes = self.span_bytes.saturating_add(required);
            let remaining_bytes = self.max_bytes.saturating_sub(self.used_bytes);
            if self.span_bytes > remaining_bytes {
                return Err(ResourceBudgetError {
                    resource: ResourceBudgetKind::MemoryBytes,
                    scope: ResourceBudgetScope::Operator,
                    owner: "search matched-span hydration".to_string(),
                    memory_class: None,
                    requested: required,
                    used: self.used_bytes.saturating_add(previous_bytes),
                    budget: self.max_bytes,
                    budget_option: "SearchOutOfCoreConfig.max_matched_span_bytes",
                    message: format!(
                        "search matched-span hydration requires {} bytes, exceeding {remaining_bytes}",
                        self.span_bytes
                    ),
                }
                .into());
            }
            self.spans.push(SearchMatchedSpan {
                field: field.to_string(),
                start_byte,
                end_byte,
                text: raw.to_string(),
                term,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
fn document_tokens(
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    let mut tokens = Vec::new();
    for (text, weight) in document_token_fields(document) {
        let field_tokens = tokenize_list(text, analyzer_lexicon);
        for _ in 1..weight {
            tokens.extend(field_tokens.iter().cloned());
        }
        tokens.extend(field_tokens);
    }
    tokens
}

#[cfg(test)]
fn token_frequencies(tokens: impl Iterator<Item = String>) -> BTreeMap<String, usize> {
    let mut frequencies = BTreeMap::new();
    for token in tokens {
        *frequencies.entry(token).or_insert(0) += 1;
    }
    frequencies
}

fn tokenize(text: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> BTreeSet<String> {
    tokenize_list(text, analyzer_lexicon).into_iter().collect()
}

/// Distinct query terms, refusing as soon as they would exceed the analyzed
/// cap. Like the out-of-core normalizer, refusal costs O(cap) analysis rather
/// than tokenizing the remainder of an oversized query.
fn tokenize_query_bounded(
    text: &str,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    config: &LexicalProjectionConfig,
) -> Result<BTreeSet<String>> {
    let limit = config.analyzed_query_term_limit();
    let mut terms = BTreeSet::new();
    analyzer_stream::visit_token_list(text, analyzer_lexicon, |term, _| {
        if !terms.contains(&term) {
            if terms.len() >= limit {
                return Err(config.analyzed_query_term_error());
            }
            terms.insert(term);
        }
        Ok(())
    })?;
    Ok(terms)
}

fn tokenize_list(text: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    analyzer_stream::collect_token_list(text, analyzer_lexicon)
}

fn normalized_alias_rule_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

fn normalized_stopword_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

#[cfg(test)]
fn identifier_tokens(raw: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    analyzer_stream::identifier_tokens(raw, analyzer_lexicon)
}

// Keep the eager expansion helpers as an independent test reference.
#[cfg(test)]
fn push_cjk_ngram_tokens(
    tokens: &mut TokenSequence,
    raw: &str,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    let mut run = Vec::new();
    for ch in raw.chars() {
        if is_cjk_search_char(ch) {
            run.push(ch);
        } else {
            push_cjk_ngram_run_tokens(tokens, &run, analyzer_lexicon);
            run.clear();
        }
    }
    push_cjk_ngram_run_tokens(tokens, &run, analyzer_lexicon);
}

#[cfg(test)]
fn push_cjk_ngram_run_tokens(
    tokens: &mut TokenSequence,
    run: &[char],
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    for width in [2_usize, 3] {
        if run.len() < width {
            continue;
        }
        for window in run.windows(width) {
            push_unique_token(tokens, window.iter().collect(), analyzer_lexicon);
        }
    }
}

#[cfg(test)]
fn push_unique_token(
    tokens: &mut TokenSequence,
    token: String,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    if !token.is_empty() && !analyzer_lexicon.is_stopword(&token) {
        tokens.push_unique(token);
    }
}

#[cfg(test)]
fn push_analyzed_token(
    tokens: &mut TokenSequence,
    token: String,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    push_unique_token(tokens, token.clone(), analyzer_lexicon);
    for normalized in normalize_english_suffixes(&token) {
        push_unique_token(tokens, normalized, analyzer_lexicon);
    }
    for alias in analyzer_lexicon.semantic_aliases(&token) {
        push_unique_token(tokens, alias, analyzer_lexicon);
    }
}

#[derive(Debug, Default)]
struct TokenSequence {
    token_ids: HashMap<String, usize>,
    order: Vec<usize>,
}

impl TokenSequence {
    #[cfg(test)]
    fn push_unique(&mut self, token: String) {
        let next_id = self.token_ids.len();
        if let std::collections::hash_map::Entry::Vacant(entry) = self.token_ids.entry(token) {
            entry.insert(next_id);
            self.order.push(next_id);
        }
    }

    #[cfg(test)]
    fn push(&mut self, token: String) {
        let next_id = self.token_ids.len();
        let token_id = *self.token_ids.entry(token).or_insert(next_id);
        self.order.push(token_id);
    }

    #[cfg(test)]
    fn extend(&mut self, tokens: impl IntoIterator<Item = String>) {
        for token in tokens {
            self.push(token);
        }
    }

    fn into_vec(self) -> Vec<String> {
        let mut tokens_by_id = (0..self.token_ids.len())
            .map(|_| None)
            .collect::<Vec<Option<String>>>();
        for (token, token_id) in self.token_ids {
            tokens_by_id[token_id] = Some(token);
        }

        let mut remaining = vec![0_usize; tokens_by_id.len()];
        for token_id in &self.order {
            remaining[*token_id] += 1;
        }

        self.order
            .into_iter()
            .map(|token_id| {
                remaining[token_id] -= 1;
                if remaining[token_id] == 0 {
                    tokens_by_id[token_id]
                        .take()
                        .expect("token sequence IDs must resolve")
                } else {
                    tokens_by_id[token_id]
                        .as_ref()
                        .expect("token sequence IDs must resolve")
                        .clone()
                }
            })
            .collect()
    }
}

#[cfg(test)]
fn normalize_english_suffixes(token: &str) -> Vec<String> {
    if token.len() <= 4 || token.contains('_') || token.chars().any(|ch| ch.is_ascii_digit()) {
        return Vec::new();
    }
    if let Some(stem) = token.strip_suffix("ies")
        && stem.len() >= 2
    {
        return vec![format!("{stem}y")];
    }
    if let Some(stem) = token.strip_suffix("ing")
        && stem.len() >= 3
    {
        return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
    }
    if let Some(stem) = token.strip_suffix("ed")
        && stem.len() >= 3
    {
        return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
    }
    if let Some(stem) = token.strip_suffix('s')
        && stem.len() >= 3
        && !stem.ends_with('s')
    {
        return vec![stem.to_string()];
    }
    Vec::new()
}

#[cfg(test)]
fn suffix_stem_variants(stem: &str) -> Vec<String> {
    let mut variants = vec![stem.to_string()];
    if matches!(stem.chars().last(), Some('c' | 'v' | 'z')) {
        variants.push(format!("{stem}e"));
    }
    variants
}

fn english_suffix_parts(token: &str) -> [Option<(&str, &str)>; 2] {
    if token.len() <= 4 || token.contains('_') || token.chars().any(|ch| ch.is_ascii_digit()) {
        return [None, None];
    }
    if let Some(stem) = token.strip_suffix("ies")
        && stem.len() >= 2
    {
        return [Some((stem, "y")), None];
    }
    let stem = token
        .strip_suffix("ing")
        .or_else(|| token.strip_suffix("ed"));
    if let Some(stem) = stem.filter(|stem| stem.len() >= 3) {
        let stem = trim_doubled_suffix_consonant(stem);
        let tail = matches!(stem.chars().last(), Some('c' | 'v' | 'z')).then_some((stem, "e"));
        return [Some((stem, "")), tail];
    }
    if let Some(stem) = token.strip_suffix('s')
        && stem.len() >= 3
        && !stem.ends_with('s')
    {
        return [Some((stem, "")), None];
    }
    [None, None]
}

fn is_core_search_stopword(token: &str) -> bool {
    matches!(
        token,
        "a" | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "by"
            | "for"
            | "from"
            | "how"
            | "in"
            | "is"
            | "it"
            | "of"
            | "on"
            | "or"
            | "that"
            | "the"
            | "this"
            | "to"
            | "was"
            | "with"
    )
}

fn trim_doubled_suffix_consonant(stem: &str) -> &str {
    let mut chars = stem.char_indices().rev();
    let Some((last_index, last)) = chars.next() else {
        return stem;
    };
    let Some((_, previous)) = chars.next() else {
        return stem;
    };
    if last == previous && is_ascii_consonant(last) {
        &stem[..last_index]
    } else {
        stem
    }
}

fn is_ascii_consonant(ch: char) -> bool {
    ch.is_ascii_alphabetic() && !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u')
}

#[cfg(test)]
mod raw_vector_math_tests;

#[cfg(all(test, feature = "vector-search"))]
mod vector_metric_tests;

// Final facade vector relevance is positive-only in [0, 1], higher is better,
// and is always computed with the projection's own distance metric:
// - cosine: positive cosine similarity. Nonpositive exact similarities cannot
//   contribute a vector hit (text may still match);
// - Euclidean: `1 / (1 + l2_distance)`. It is positive for every finite pair
//   and strictly decreasing in distance, so descending relevance is ascending
//   L2 distance and no candidate is dropped for its direction.
// Signed RaBitQ estimates and graph distances are candidate-ranking hints, not
// final scores. Schema-bound vector index queries expose raw distances instead.
/// One prepared query shared by a scoring phase. Keep legacy nonfinite behavior
/// at this private boundary; public projection distances validate finite inputs.
#[derive(Clone, Copy)]
struct VectorRelevanceQuery<'a> {
    query: &'a [f32],
    metric: hawdb_core::VectorDistance,
    scoring: SearchVectorScoring,
    prepared: Option<hawdb_core::vector::PreparedVectorQuery<'a>>,
}

impl<'a> VectorRelevanceQuery<'a> {
    #[cfg(test)]
    fn new(query: &'a [f32], metric: hawdb_core::VectorDistance) -> Self {
        Self {
            query,
            metric,
            scoring: SearchVectorScoring::default(),
            prepared: (query.len() >= 16)
                .then(|| hawdb_core::vector::PreparedVectorQuery::new(query).ok())
                .flatten(),
        }
    }

    fn with_work(
        query: &'a [f32],
        metric: hawdb_core::VectorDistance,
        mut checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        if query.len() < 16 {
            return Ok(Self {
                query,
                metric,
                scoring: SearchVectorScoring::default(),
                prepared: None,
            });
        }
        let mut failure = None;
        let prepared = hawdb_core::vector::PreparedVectorQuery::with_work(query, || {
            checkpoint().map_err(|error| {
                failure = Some(error);
                hawdb_core::HawDBError::ProjectionBusy
            })
        });
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(Self {
            query,
            metric,
            scoring: SearchVectorScoring::default(),
            prepared: prepared.ok(),
        })
    }

    /// A query whose scoring phase has no candidates needs no prepared norm.
    #[cfg(feature = "vector-search")]
    fn unprepared(query: &'a [f32], metric: hawdb_core::VectorDistance) -> Self {
        Self {
            query,
            metric,
            scoring: SearchVectorScoring::default(),
            prepared: None,
        }
    }

    fn with_scoring(mut self, scoring: SearchVectorScoring) -> Self {
        self.scoring = scoring;
        self
    }

    /// Exact mathematical distance under this query's metric.
    #[cfg(feature = "vector-search")]
    fn distance_with_work(
        &self,
        embedding: &[f32],
        checkpoint: impl FnMut() -> Result<()>,
    ) -> Result<f64> {
        match &self.prepared {
            Some(prepared) => {
                prepared.distance_slices_with_work(self.metric, embedding, checkpoint)
            }
            None => self
                .metric
                .distance_slices_with_work(self.query, embedding, checkpoint),
        }
    }

    /// Facade relevance in [0, 1]; `None` when the pair has no defined score.
    fn score(&self, embedding: &[f32]) -> Option<f64> {
        if self.scoring == SearchVectorScoring::LinearSquaredL2 {
            return self.scoring.ranking_score(self.query, embedding);
        }
        match self.metric {
            hawdb_core::VectorDistance::Cosine => {
                if let Some(prepared) = &self.prepared
                    && let Ok(score) = prepared.positive_cosine_similarity(embedding)
                {
                    return score;
                }
                scalar_cosine_similarity(self.query, embedding)
            }
            hawdb_core::VectorDistance::Euclidean => {
                if self.query.len() != embedding.len() || embedding.is_empty() {
                    return None;
                }
                let distance = match &self.prepared {
                    Some(prepared) => prepared.distance_slices_with_work(
                        hawdb_core::VectorDistance::Euclidean,
                        embedding,
                        || Ok(()),
                    ),
                    None => hawdb_core::VectorDistance::Euclidean.distance_slices_with_work(
                        self.query,
                        embedding,
                        || Ok(()),
                    ),
                }
                .ok()?;
                euclidean_relevance(distance)
            }
        }
    }
}

/// Maps a finite L2 distance onto the facade's higher-is-better (0, 1] scale.
fn euclidean_relevance(distance: f64) -> Option<f64> {
    (distance.is_finite() && distance >= 0.0).then(|| 1.0 / (1.0 + distance))
}

#[cfg(test)]
fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    VectorRelevanceQuery::new(left, hawdb_core::VectorDistance::Cosine).score(right)
}

fn scalar_cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let dot = dot_product(left, right)?;
    let left_norm_squared = dot_product(left, left)?;
    let right_norm_squared = dot_product(right, right)?;
    if left_norm_squared == 0.0 || right_norm_squared == 0.0 {
        return None;
    }
    Some((dot / (left_norm_squared.sqrt() * right_norm_squared.sqrt())).clamp(0.0, 1.0))
}

fn dot_product(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }

    // HawDB owns this exact raw-vector score. It is independent from RaBitQ's
    // candidate index, persistence, and branch state. Keep a single scalar
    // implementation here so a future platform-SIMD kernel can be added without
    // changing the `Option<f64>` search contract.
    Some(left.iter().zip(right).fold(0.0, |sum, (left, right)| {
        sum + f64::from(*left) * f64::from(*right)
    }))
}

fn decode_embedding(input: &str) -> Result<Option<Vec<f32>>> {
    if input.is_empty() {
        return Ok(None);
    }
    input
        .split(',')
        .map(|raw| {
            raw.parse::<f32>()
                .map_err(|_| HawDBError::Storage(format!("invalid embedding value: {raw}")))
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn encode_metadata(metadata: &BTreeMap<String, String>) -> String {
    metadata
        .iter()
        .map(|(key, value)| format!("{}={}", encode_string(key), encode_string(value)))
        .collect::<Vec<_>>()
        .join(";")
}

/// The metadata keys a candidate filter reads, hex-encoded as stored, so a
/// record's other pairs are skipped without being decoded (#1613).
#[derive(Debug, Clone, Default)]
pub(crate) struct MetadataKeys(BTreeSet<String>);

impl MetadataKeys {
    pub(crate) fn contains(&self, encoded_key: &str) -> bool {
        self.0.contains(encoded_key)
    }

    /// Every key `predicates` read: a predicate reads the metadata entry of
    /// its field's name (see [`search_header_field_values`]).
    pub(crate) fn for_predicates(predicates: &SearchPredicateSet) -> Self {
        Self::for_predicate_iter(predicates.predicates())
    }

    pub(crate) fn for_predicate_iter<'a>(
        predicates: impl IntoIterator<Item = &'a SearchPredicate>,
    ) -> Self {
        Self(
            predicates
                .into_iter()
                .map(|predicate| encode_string(predicate.field().name()))
                .collect(),
        )
    }
}

/// [`decode_metadata`] restricted to `keys`. Every pair must still be a
/// `key=value` pair; pairs with other keys are not hex-decoded.
pub(crate) fn decode_metadata_keys(
    input: &str,
    keys: Option<&MetadataKeys>,
) -> Result<BTreeMap<String, String>> {
    let Some(keys) = keys else {
        return decode_metadata(input);
    };
    let mut metadata = BTreeMap::new();
    if input.is_empty() {
        return Ok(metadata);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(HawDBError::Storage(format!(
                "invalid metadata pair: {pair}"
            )));
        };
        if keys.0.contains(key) {
            metadata.insert(decode_string(key)?, decode_string(value)?);
        }
    }
    Ok(metadata)
}

fn decode_metadata(input: &str) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    if input.is_empty() {
        return Ok(metadata);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(HawDBError::Storage(format!(
                "invalid metadata pair: {pair}"
            )));
        };
        metadata.insert(decode_string(key)?, decode_string(value)?);
    }
    Ok(metadata)
}

fn decode_search_document_line(line: &str) -> Result<SearchDocument> {
    decode_search_document_line_with(line, true)
}

/// Decodes one document line; without `embeddings` its decimal-text vector
/// is left unparsed and the document carries none (#1917 text-only hits).
fn decode_search_document_line_with(line: &str, embeddings: bool) -> Result<SearchDocument> {
    let line = line.strip_suffix('\n').unwrap_or(line);
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["doc", raw_id, raw_title, raw_content, raw_embedding, raw_metadata] => {
            Ok(SearchDocument {
                id: decode_string(raw_id)?,
                title: decode_string(raw_title)?,
                content: decode_string(raw_content)?,
                embedding: if embeddings {
                    decode_embedding(raw_embedding)?
                } else {
                    None
                },
                metadata: decode_metadata(raw_metadata)?,
            })
        }
        _ => Err(HawDBError::Storage(format!(
            "invalid search document line: {line}"
        ))),
    }
}

fn write_search_segment_payloads<T>(
    path: &Path,
    file_name: &str,
    documents: &impl DocumentView,
    descriptor: &mut SearchSegmentDescriptor,
    finish: impl FnOnce(&mut File) -> Result<T>,
) -> Result<T> {
    let artifact_path = path.join(file_name);
    let tmp_path = artifact_path.with_extension("hawdb.tmp");
    let mut offset = 0u64;
    let reference;
    {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        for segment in &mut descriptor.segments {
            let mut body = String::from("HAWDB_SEARCH_SEGMENT_V1\n");
            for document in documents
                .range_documents(&segment.first_document_id, &segment.last_document_id)
                .map(|(_, document)| document)
            {
                body.push_str(&encode_search_document_line(document));
            }
            let payload = encode_search_snapshot_text(&body)?;
            let length = u64::try_from(payload.len()).map_err(|_| {
                HawDBError::Storage(format!(
                    "search segment {} payload exceeds the supported range length",
                    segment.segment_id
                ))
            })?;
            segment.payload_range = Some(SearchSegmentPayloadRange {
                artifact_id: SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
                offset,
                length,
                checksum: checksum_bytes(&payload),
            });
            file.write_all(&payload)?;
            offset = offset.checked_add(length).ok_or_else(|| {
                HawDBError::Storage("search segment payload artifact length overflow".to_string())
            })?;
        }
        file.sync_all()?;
        reference = finish(&mut file)?;
    }
    durable_replace_file(&tmp_path, &artifact_path)?;
    Ok(reference)
}

fn write_search_segment_descriptor(
    path: &Path,
    descriptor: &SearchSegmentDescriptor,
) -> Result<()> {
    write_search_segment_descriptor_bounded(path, descriptor, u64::MAX).map(|_| ())
}

fn write_search_segment_descriptor_bounded(
    path: &Path,
    descriptor: &SearchSegmentDescriptor,
    max_bytes: u64,
) -> Result<u64> {
    write_search_segment_descriptor_output(
        path,
        SEARCH_SEGMENT_DESCRIPTOR_FILE,
        descriptor,
        max_bytes,
        |_| Ok(()),
    )
    .map(|(length, ())| length)
}

fn write_search_segment_descriptor_output<T>(
    path: &Path,
    file_name: &str,
    descriptor: &SearchSegmentDescriptor,
    max_bytes: u64,
    finish: impl FnOnce(&mut File) -> Result<T>,
) -> Result<(u64, T)> {
    let encoding = document_encoding::DescriptorEncoding::new(descriptor, max_bytes)?;
    let descriptor_path = path.join(file_name);
    let tmp_path = descriptor_path.with_extension("hawdb.tmp");
    let reference;
    {
        let mut file = std::io::BufWriter::new(
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp_path)?,
        );
        encoding.write_to(&mut file)?;
        file.flush()?;
        file.get_ref().sync_all()?;
        reference = finish(file.get_mut())?;
    }
    durable_replace_file(&tmp_path, &descriptor_path)?;
    Ok((encoding.len() as u64, reference))
}

fn read_search_segment_descriptor(path: &Path) -> Result<Option<SearchSegmentDescriptor>> {
    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    if !fs::try_exists(&descriptor_path)? {
        return Ok(None);
    }
    let text = fs::read_to_string(&descriptor_path)?;
    decode_search_segment_descriptor_text(&text).map(Some)
}

fn read_captured_search_segment_descriptor(
    publication: &out_of_core::SearchProjectionPublishLease,
) -> Result<Option<SearchSegmentDescriptor>> {
    let mut file = match publication.open_captured_child(SEARCH_SEGMENT_DESCRIPTOR_FILE) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    decode_search_segment_descriptor_text(&text).map(Some)
}

#[cfg(test)]
fn encode_search_segment_descriptor_body(descriptor: &SearchSegmentDescriptor) -> String {
    let mut body = String::new();
    body.push_str(if descriptor.omits_value_summaries() {
        "HAWDB_SEARCH_SEGMENTS_V4\n"
    } else {
        "HAWDB_SEARCH_SEGMENTS_V3\n"
    });
    body.push_str(&format!(
        "target_documents\t{}\n",
        descriptor.target_documents
    ));
    body.push_str(&format!("document_count\t{}\n", descriptor.document_count));
    for segment in &descriptor.segments {
        let payload_range = segment.payload_range.unwrap_or(SearchSegmentPayloadRange {
            artifact_id: 0,
            offset: 0,
            length: 0,
            checksum: 0,
        });
        body.push_str(&format!(
            "segment\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            segment.segment_id,
            encode_string(&segment.first_document_id),
            encode_string(&segment.last_document_id),
            segment.document_count,
            payload_range.artifact_id,
            payload_range.offset,
            payload_range.length,
            payload_range.checksum,
        ));
        for (field, summary) in &segment.metadata {
            let (numeric_min, numeric_max) = encode_search_numeric_range(summary.numeric_range);
            let (timestamp_min, timestamp_max) =
                encode_search_timestamp_range(summary.timestamp_range);
            body.push_str(&format!(
                "field\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                encode_string(field),
                summary.present_count,
                if summary.values_omitted {
                    SEARCH_SEGMENT_OMITTED_VALUES.to_string()
                } else {
                    encode_segment_values(&summary.values)
                },
                numeric_min,
                numeric_max,
                timestamp_min,
                timestamp_max
            ));
        }
    }
    body
}

fn decode_search_segment_descriptor_text(text: &str) -> Result<SearchSegmentDescriptor> {
    decode_search_segment_descriptor_with_context(text, None, false)
}

/// Decode a segment descriptor. With `bound_value_summaries`, a value set
/// above the segment value-summary bounds is omitted while it is decoded, so
/// a descriptor written before #1746 does not keep document bodies resident.
fn decode_search_segment_descriptor_with_context(
    text: &str,
    task: Option<&hawdb_core::RuntimeTaskContext>,
    bound_value_summaries: bool,
) -> Result<SearchSegmentDescriptor> {
    let (body, checksum) = split_checksum(text)?;
    let mut hasher = hawdb_integrity::Crc32cHasher::new();
    for chunk in body.as_bytes().chunks(8192) {
        task.map_or(Ok(()), build_control::checkpoint)?;
        hasher.update(chunk);
    }
    let actual = hasher.finish();
    if checksum != actual {
        return Err(HawDBError::Storage(format!(
            "search segment descriptor checksum mismatch: expected {checksum}, got {actual}"
        )));
    }

    let mut target_documents = None;
    let mut document_count = None;
    let mut segments = Vec::new();
    let mut current_segment = None::<SearchSegmentDescriptorEntry>;
    // Only a V4 descriptor may mark a value set as omitted.
    let mut omitted_values_allowed = false;

    for line in body.lines() {
        task.map_or(Ok(()), build_control::checkpoint)?;
        if line == "HAWDB_SEARCH_SEGMENTS_V1"
            || line == "HAWDB_SEARCH_SEGMENTS_V2"
            || line == "HAWDB_SEARCH_SEGMENTS_V3"
        {
            continue;
        }
        if line == SEARCH_SEGMENT_DESCRIPTOR_V4_HEADER {
            omitted_values_allowed = true;
            continue;
        }
        // The longest descriptor production has nine fields. Keep the field
        // table on the stack: rejected input may contain a tab per byte, and
        // collecting those borrowed slices would amplify it into a large Vec.
        let mut fields = [""; 9];
        let mut count = 0;
        for field in line.split('\t') {
            let Some(slot) = fields.get_mut(count) else {
                return Err(HawDBError::Storage(format!(
                    "invalid search segment descriptor line: {line}"
                )));
            };
            *slot = field;
            count += 1;
        }
        match &fields[..count] {
            ["target_documents", raw] => {
                target_documents = Some(parse_usize(raw, "search segment target documents")?);
            }
            ["document_count", raw] => {
                document_count = Some(parse_usize(raw, "search segment document count")?);
            }
            ["segment", raw_first, raw_last, raw_count] => {
                if let Some(segment) = current_segment.take() {
                    segments.push(segment);
                }
                current_segment = Some(SearchSegmentDescriptorEntry {
                    segment_id: segments.len() as u64,
                    first_document_id: decode_string(raw_first)?,
                    last_document_id: decode_string(raw_last)?,
                    document_count: parse_usize(raw_count, "search segment document count")?,
                    payload_range: None,
                    metadata: BTreeMap::new(),
                });
            }
            ["segment", raw_segment_id, raw_first, raw_last, raw_count, raw_artifact_id, raw_offset, raw_length, raw_checksum] =>
            {
                if let Some(segment) = current_segment.take() {
                    segments.push(segment);
                }
                let length = parse_u64(raw_length, "search segment payload length")?;
                current_segment = Some(SearchSegmentDescriptorEntry {
                    segment_id: parse_u64(raw_segment_id, "search segment id")?,
                    first_document_id: decode_string(raw_first)?,
                    last_document_id: decode_string(raw_last)?,
                    document_count: parse_usize(raw_count, "search segment document count")?,
                    payload_range: (length > 0).then_some(SearchSegmentPayloadRange {
                        artifact_id: parse_u64(
                            raw_artifact_id,
                            "search segment payload artifact id",
                        )?,
                        offset: parse_u64(raw_offset, "search segment payload offset")?,
                        length,
                        checksum: parse_u64(raw_checksum, "search segment payload checksum")?,
                    }),
                    metadata: BTreeMap::new(),
                });
            }
            ["field", raw_field, raw_present_count, raw_values, ranges @ ..]
                if matches!(ranges.len(), 0 | 2 | 4) =>
            {
                let Some(segment) = current_segment.as_mut() else {
                    return Err(HawDBError::Storage(
                        "search segment descriptor field appeared before segment".to_string(),
                    ));
                };
                let (values, values_omitted) = decode_segment_value_summary(
                    raw_values,
                    omitted_values_allowed,
                    bound_value_summaries,
                )?;
                segment.metadata.insert(
                    decode_string(raw_field)?,
                    SearchSegmentFieldSummary {
                        present_count: parse_usize(
                            raw_present_count,
                            "search segment field present count",
                        )?,
                        values,
                        values_omitted,
                        numeric_range: match ranges {
                            [raw_numeric_min, raw_numeric_max, ..] => {
                                decode_search_numeric_range(raw_numeric_min, raw_numeric_max)?
                            }
                            _ => None,
                        },
                        timestamp_range: match ranges {
                            [_, _, raw_timestamp_min, raw_timestamp_max] => {
                                decode_search_timestamp_range(raw_timestamp_min, raw_timestamp_max)?
                            }
                            _ => None,
                        },
                    },
                );
            }
            [""] => {}
            _ => {
                return Err(HawDBError::Storage(format!(
                    "invalid search segment descriptor line: {line}"
                )));
            }
        }
    }
    if let Some(segment) = current_segment.take() {
        segments.push(segment);
    }
    validate_search_segment_payload_ranges(&segments)?;

    Ok(SearchSegmentDescriptor {
        target_documents: target_documents.ok_or_else(|| {
            HawDBError::Storage("search segment descriptor missing target_documents".to_string())
        })?,
        document_count: document_count.ok_or_else(|| {
            HawDBError::Storage("search segment descriptor missing document_count".to_string())
        })?,
        segments,
    })
}

fn validate_search_segment_payload_ranges(segments: &[SearchSegmentDescriptorEntry]) -> Result<()> {
    let physical_range_count = segments
        .iter()
        .filter(|segment| segment.payload_range.is_some())
        .count();
    if physical_range_count != 0 && physical_range_count != segments.len() {
        return Err(HawDBError::Storage(
            "search segment descriptor has incomplete physical payload ranges".to_string(),
        ));
    }
    let mut previous_end = 0u64;
    for (expected_id, segment) in segments.iter().enumerate() {
        if segment.segment_id != expected_id as u64 {
            return Err(HawDBError::Storage(format!(
                "search segment descriptor expected segment id {expected_id}, got {}",
                segment.segment_id
            )));
        }
        let Some(range) = segment.payload_range else {
            continue;
        };
        if range.artifact_id != SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID {
            return Err(HawDBError::Storage(format!(
                "search segment {} references unsupported payload artifact {}",
                segment.segment_id, range.artifact_id
            )));
        }
        if range.offset < previous_end {
            return Err(HawDBError::Storage(format!(
                "search segment {} payload range overlaps the previous segment",
                segment.segment_id
            )));
        }
        previous_end = range.offset.checked_add(range.length).ok_or_else(|| {
            HawDBError::Storage(format!(
                "search segment {} payload range overflows",
                segment.segment_id
            ))
        })?;
    }
    Ok(())
}

fn decode_search_segment_documents(payload: &[u8]) -> Result<Vec<SearchDocument>> {
    decode_search_segment_documents_bounded(payload, u64::MAX)
}

fn decode_search_segment_documents_bounded(
    payload: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<Vec<SearchDocument>> {
    let text = decode_search_snapshot_text_bounded(payload, max_uncompressed_bytes)?;
    let mut documents = Vec::new();
    for line in text.lines() {
        if line == "HAWDB_SEARCH_SEGMENT_V1" {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["doc", ..] => documents.push(decode_search_document_line(line)?),
            [""] => {}
            _ => {
                return Err(HawDBError::Storage(format!(
                    "invalid search segment payload line: {line}"
                )));
            }
        }
    }
    Ok(documents)
}

fn validate_search_segment_documents(
    segment: &SearchSegmentDescriptorEntry,
    documents: &[SearchDocument],
) -> Result<()> {
    if documents.len() != segment.document_count {
        return Err(HawDBError::Storage(format!(
            "search segment {} decoded {} documents, expected {}",
            segment.segment_id,
            documents.len(),
            segment.document_count
        )));
    }
    let first = documents.first().map(|document| document.id.as_str());
    let last = documents.last().map(|document| document.id.as_str());
    if first != Some(segment.first_document_id.as_str())
        || last != Some(segment.last_document_id.as_str())
    {
        return Err(HawDBError::Storage(format!(
            "search segment {} document bounds do not match its descriptor",
            segment.segment_id
        )));
    }
    if documents.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err(HawDBError::Storage(format!(
            "search segment {} documents are not strictly ordered",
            segment.segment_id
        )));
    }
    Ok(())
}

#[cfg(test)]
fn encode_segment_values(values: &BTreeSet<String>) -> String {
    values
        .iter()
        .map(|value| encode_string(value))
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_segment_values(input: &str) -> Result<BTreeSet<String>> {
    if input.is_empty() {
        return Ok(BTreeSet::new());
    }
    input.split(',').map(decode_string).collect()
}

/// The value column of a V4 descriptor field whose value set is omitted.
const SEARCH_SEGMENT_OMITTED_VALUES: &str = "~";

/// Header of a descriptor that may contain omitted value sets (#1746).
/// Readers before #1746 reject it, so the selector that references it
/// requires reader version 4 (`docs/SEARCH_PROJECTION_MANIFEST_VERSION.md`).
const SEARCH_SEGMENT_DESCRIPTOR_V4_HEADER: &str = "HAWDB_SEARCH_SEGMENTS_V4";

fn decode_segment_value_summary(
    input: &str,
    omitted_values_allowed: bool,
    bound_value_summaries: bool,
) -> Result<(BTreeSet<String>, bool)> {
    if input == SEARCH_SEGMENT_OMITTED_VALUES {
        if !omitted_values_allowed {
            return Err(HawDBError::Storage(
                "search segment descriptor omits a value summary before V4".to_string(),
            ));
        }
        return Ok((BTreeSet::new(), true));
    }
    if bound_value_summaries && segment_values_exceed_summary_bounds(input) {
        // Each hex pair is one value byte. Validate the column without
        // materializing the values it would have retained.
        for value in input.split(',') {
            if value.len() % 2 != 0 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(HawDBError::Storage(format!(
                    "invalid search segment descriptor hex value of {} bytes",
                    value.len()
                )));
            }
        }
        return Ok((BTreeSet::new(), true));
    }
    Ok((decode_segment_values(input)?, false))
}

/// Whether a hex value column exceeds the segment value-summary bounds. Each
/// encoded value has two hex digits per byte.
fn segment_values_exceed_summary_bounds(input: &str) -> bool {
    if input.is_empty() {
        return false;
    }
    let mut total = 0usize;
    for value in input.split(',') {
        let bytes = value.len() / 2;
        total = total.saturating_add(bytes);
        if bytes > SEARCH_SEGMENT_VALUE_SUMMARY_MAX_VALUE_BYTES
            || total > SEARCH_SEGMENT_VALUE_SUMMARY_MAX_FIELD_BYTES
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
fn encode_search_numeric_range(range: Option<SearchNumericRange>) -> (String, String) {
    range
        .map(|range| (range.min.to_string(), range.max.to_string()))
        .unwrap_or_else(|| (String::new(), String::new()))
}

#[cfg(test)]
fn encode_search_timestamp_range(range: Option<SearchTimestampRange>) -> (String, String) {
    range
        .map(|range| {
            (
                range.min_epoch_millis.to_string(),
                range.max_epoch_millis.to_string(),
            )
        })
        .unwrap_or_else(|| (String::new(), String::new()))
}

fn decode_search_numeric_range(raw_min: &str, raw_max: &str) -> Result<Option<SearchNumericRange>> {
    match (raw_min.is_empty(), raw_max.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => {
            let min = parse_finite_f64(raw_min, "search segment field numeric min")?;
            let max = parse_finite_f64(raw_max, "search segment field numeric max")?;
            if min > max {
                return Err(HawDBError::Storage(
                    "search segment field numeric min is greater than max".to_string(),
                ));
            }
            Ok(Some(SearchNumericRange { min, max }))
        }
        _ => Err(HawDBError::Storage(
            "search segment field numeric range is incomplete".to_string(),
        )),
    }
}

fn decode_search_timestamp_range(
    raw_min: &str,
    raw_max: &str,
) -> Result<Option<SearchTimestampRange>> {
    match (raw_min.is_empty(), raw_max.is_empty()) {
        (true, true) => Ok(None),
        (false, false) => {
            let min_epoch_millis =
                parse_i64(raw_min, "search segment field timestamp min epoch millis")?;
            let max_epoch_millis =
                parse_i64(raw_max, "search segment field timestamp max epoch millis")?;
            if min_epoch_millis > max_epoch_millis {
                return Err(HawDBError::Storage(
                    "search segment field timestamp min is greater than max".to_string(),
                ));
            }
            Ok(Some(SearchTimestampRange {
                min_epoch_millis,
                max_epoch_millis,
            }))
        }
        _ => Err(HawDBError::Storage(
            "search segment field timestamp range is incomplete".to_string(),
        )),
    }
}

fn parse_finite_f64(raw: &str, name: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {raw}")))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(HawDBError::Storage(format!("invalid {name}: {raw}")))
    }
}

fn encode_string(input: &str) -> String {
    input
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_string(input: &str) -> Result<String> {
    if !input.len().is_multiple_of(2) {
        return Err(HawDBError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = input
            .get(offset..offset + 2)
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            .ok_or_else(|| HawDBError::Storage(format!("invalid hex string at byte {offset}")))?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|error| HawDBError::Storage(error.to_string()))
}

fn split_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(HawDBError::Storage(
            "search projection missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "search projection checksum")?;
    Ok((body, checksum))
}

fn encode_search_snapshot_text(text: &str) -> Result<Vec<u8>> {
    let compressed =
        hawdb_storage::compression::encode_all(text.as_bytes(), SEARCH_COMPRESSION_LEVEL).map_err(
            |error| {
                HawDBError::or_stop(&error, || {
                    HawDBError::Storage(format!("zstd compression failed: {error}"))
                })
            },
        )?;
    let compressed_checksum = checksum_bytes(&compressed);
    let uncompressed_checksum = checksum_bytes(text.as_bytes());
    let header = search_snapshot_compression_header(
        uncompressed_checksum,
        compressed_checksum,
        text.len(),
        compressed.len(),
    );
    let mut encoded = header.into_bytes();
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn search_snapshot_compression_header(
    uncompressed_checksum: u64,
    compressed_checksum: u64,
    uncompressed_len: usize,
    compressed_len: usize,
) -> String {
    format!(
        "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
        uncompressed_len,
        compressed_len
    )
}

fn read_search_snapshot_text(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    decode_search_snapshot_envelope(&bytes)
}

fn decode_search_snapshot_envelope(bytes: &[u8]) -> Result<String> {
    if !bytes.starts_with(SEARCH_COMPRESSION_HEADER.as_bytes()) {
        return Err(HawDBError::Storage(
            "search projection is missing the V1 compressed envelope".to_string(),
        ));
    }
    decode_search_snapshot_text(bytes)
}

fn decode_search_snapshot_text(bytes: &[u8]) -> Result<String> {
    decode_search_snapshot_text_bounded(bytes, u64::MAX)
}

struct SnapshotHeader {
    compressed_len: Option<usize>,
    compressed_checksum: Option<u64>,
    uncompressed_len: Option<usize>,
    uncompressed_checksum: Option<u64>,
}

fn parse_snapshot_header(header: &str) -> Result<SnapshotHeader> {
    let mut codec = None;
    let mut compressed_checksum = None;
    let mut uncompressed_checksum = None;
    let mut compressed_len = None;
    let mut uncompressed_len = None;
    let mut seen_fields = BTreeSet::new();
    for line in header.lines() {
        if line == SEARCH_COMPRESSION_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        if !seen_fields.insert(fields[0]) {
            return Err(HawDBError::Storage(format!(
                "search projection compressed envelope has duplicate field: {}",
                fields[0]
            )));
        }
        match fields.as_slice() {
            ["codec", value] => codec = Some(*value),
            ["compressed_checksum", value] => {
                compressed_checksum = Some(parse_u64(value, "compressed checksum")?);
            }
            ["uncompressed_checksum", value] => {
                uncompressed_checksum = Some(parse_u64(value, "uncompressed checksum")?);
            }
            ["compressed_len", value] => {
                compressed_len = Some(parse_usize(value, "compressed length")?);
            }
            ["uncompressed_len", value] => {
                uncompressed_len = Some(parse_usize(value, "uncompressed length")?);
            }
            _ => {
                return Err(HawDBError::Storage(format!(
                    "search projection compressed envelope has invalid header line: {line}"
                )));
            }
        }
    }
    if codec != Some("zstd") {
        return Err(HawDBError::Storage(
            "search projection compressed envelope uses unsupported codec".to_string(),
        ));
    }
    Ok(SnapshotHeader {
        compressed_len,
        compressed_checksum,
        uncompressed_len,
        uncompressed_checksum,
    })
}

fn decode_search_snapshot_text_bounded(
    bytes: &[u8],
    max_uncompressed_bytes: u64,
) -> Result<String> {
    decode_search_snapshot_text_bounded_with(bytes, max_uncompressed_bytes, |payload, expected| {
        let decoder =
            hawdb_storage::compression::Decoder::new(Cursor::new(payload)).map_err(|error| {
                HawDBError::or_stop(&error, || {
                    HawDBError::Storage(format!(
                        "search projection zstd decompression failed: {error}"
                    ))
                })
            })?;
        read_search_snapshot_decoder(decoder, expected)
    })
}

fn decode_search_snapshot_text_admitted(
    bytes: &[u8],
    max_uncompressed_bytes: u64,
    memory: &hawdb_executor::QueryMemoryAccount,
    task: &RuntimeTaskContext,
) -> Result<String> {
    decode_search_snapshot_text_bounded_with(bytes, max_uncompressed_bytes, |payload, expected| {
        let decoder =
            build_memory::decoder::Decoder::with_account(Cursor::new(payload), memory, task)?;
        read_search_snapshot_decoder(decoder, expected)
    })
}

fn decode_search_snapshot_text_bounded_with(
    bytes: &[u8],
    max_uncompressed_bytes: u64,
    decode: impl FnOnce(&[u8], usize) -> Result<Vec<u8>>,
) -> Result<String> {
    let Some(header_end) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(HawDBError::Storage(
            "search projection compressed envelope missing header terminator".to_string(),
        ));
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|error| {
        HawDBError::or_stop(&error, || {
            HawDBError::Storage(format!(
                "search projection compressed envelope header is invalid: {error}"
            ))
        })
    })?;
    let payload = &bytes[header_end + 2..];
    let SnapshotHeader {
        compressed_len,
        compressed_checksum,
        uncompressed_len,
        uncompressed_checksum,
    } = parse_snapshot_header(header)?;
    let expected_compressed_len = compressed_len.ok_or_else(|| {
        HawDBError::Storage(
            "search projection compressed envelope missing compressed_len".to_string(),
        )
    })?;
    if payload.len() != expected_compressed_len {
        return Err(HawDBError::Storage(format!(
            "search projection compressed length mismatch: expected {expected_compressed_len}, got {}",
            payload.len()
        )));
    }
    let expected_compressed_checksum = compressed_checksum.ok_or_else(|| {
        HawDBError::Storage(
            "search projection compressed envelope missing compressed_checksum".to_string(),
        )
    })?;
    let actual_compressed_checksum = checksum_bytes(payload);
    if actual_compressed_checksum != expected_compressed_checksum {
        return Err(HawDBError::Storage(format!(
            "search projection compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_compressed_checksum}"
        )));
    }
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        HawDBError::Storage(
            "search projection compressed envelope missing uncompressed_len".to_string(),
        )
    })?;
    if expected_uncompressed_len as u64 > max_uncompressed_bytes {
        return Err(HawDBError::Storage(format!(
            "search projection uncompressed payload requires {expected_uncompressed_len} bytes, exceeding {max_uncompressed_bytes}"
        )));
    }
    let decoded = decode(payload, expected_uncompressed_len)?;
    #[cfg(test)]
    compression_tests::record_decoded_bytes(decoded.len());
    if decoded.len() as u64 > max_uncompressed_bytes {
        return Err(HawDBError::Storage(format!(
            "search projection decompressed payload exceeded {max_uncompressed_bytes} bytes"
        )));
    }
    if decoded.len() != expected_uncompressed_len {
        return Err(HawDBError::Storage(format!(
            "search projection uncompressed length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        )));
    }
    let expected_uncompressed_checksum = uncompressed_checksum.ok_or_else(|| {
        HawDBError::Storage(
            "search projection compressed envelope missing uncompressed_checksum".to_string(),
        )
    })?;
    let actual_uncompressed_checksum = checksum_bytes(&decoded);
    if actual_uncompressed_checksum != expected_uncompressed_checksum {
        return Err(HawDBError::Storage(format!(
            "search projection uncompressed checksum mismatch: expected {expected_uncompressed_checksum}, got {actual_uncompressed_checksum}"
        )));
    }
    String::from_utf8(decoded).map_err(|error| {
        HawDBError::or_stop(&error, || {
            HawDBError::Storage(format!(
                "search projection decompressed payload is not valid UTF-8: {error}"
            ))
        })
    })
}

fn read_search_snapshot_decoder(decoder: impl Read, expected_len: usize) -> Result<Vec<u8>> {
    let mut decoded = Vec::with_capacity(expected_len.min(1024 * 1024));
    // Probe one byte past the declared size to reject understated lengths.
    decoder
        .take((expected_len as u64).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| {
            HawDBError::or_stop(&error, || {
                HawDBError::Storage(format!(
                    "search projection zstd decompression failed: {error}"
                ))
            })
        })?;
    Ok(decoded)
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    checksum_u64(bytes)
}

fn elapsed_micros(started: hawdb_core::time::Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| HawDBError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
fn test_temp_dir() -> PathBuf {
    // Bazel sandboxes can reuse PIDs while sharing the host TMPDIR.
    // Use the per-action directory to keep fixture names independent.
    std::env::var_os("TEST_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from)
}

#[cfg(test)]
mod tests;

mod graph_store_adapter;

fn resident_exact_vector_search_capacity(
    document_count: usize,
    working_bytes: usize,
    max_document_id_bytes: usize,
) -> usize {
    use hawdb_executor::{VectorCandidate, VectorRawScore};
    let id = max_document_id_bytes;
    let candidates = working_bytes / (2 * size_of::<VectorCandidate>()).saturating_add(id).max(1);
    let lookup = document_count.saturating_mul(3 * size_of::<(&str, &SearchDocument)>());
    let rerank = working_bytes.saturating_sub(lookup)
        / size_of::<VectorCandidate>()
            .saturating_add(size_of::<VectorRawScore>())
            .saturating_add(id.saturating_mul(2))
            .max(1);
    candidates.min(rerank)
}
