use chrono::{DateTime, NaiveDate, NaiveDateTime};
#[doc(hidden)]
pub mod candidate_evidence;
#[cfg(feature = "vector-search")]
use simsimd::SpatialSimilarity;
use skein_core::{Catalog, Result, RuntimeCapabilities, RuntimeCapability, SkeinError, Value};
pub use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
pub use skein_evidence::{
    ProductionEvidenceBinding, ProductionQualificationIdentity,
    PRODUCTION_QUALIFICATION_POLICY_VERSION,
};
use skein_integrity::checksum_u64;
use skein_optimizer::{
    normalize_search_enum_value, push_search_predicates, search_field_is_enum_like,
    select_adaptive_vector_backend, AdaptiveVectorBackend, AdaptiveVectorBackendDecision,
    AdaptiveVectorBackendInput, AdaptiveVectorBackendPolicy, SearchPredicate, SearchPredicateOp,
    SearchPredicateSet, SearchScalarValue, SearchScanPredicateSupport, VectorCompressionPreference,
};
use skein_plan::{VectorBackendSelectionReason, VectorCandidateSource};
use skein_qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    QosAdmission, WorkClass, WorkRequest,
};
use skein_storage::{
    durable_replace_file, EnumDictionaryStats, FieldSummary, RangeBound, ScanPredicate,
    SegmentPruner, SegmentReadRange, SegmentSummary,
};
use skein_storage::{NodeId, NodeRecord};
use skein_telemetry::{KernelTelemetry, KernelTelemetryOperation, TelemetrySink};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{Cursor, Read, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

mod analyzer_lexicon;
mod analyzer_stream;
mod bounded_file;
mod cjk_tokenizer;
#[cfg(test)]
mod compression_tests;
#[cfg(test)]
mod document_decoding_tests;
mod document_encoding;
mod generation_cleanup;
mod identifier;
mod lexical_projection;
mod lexical_readiness;
mod lexical_term_policy;
mod out_of_core;
#[doc(hidden)]
pub mod projection_evidence;
#[doc(hidden)]
pub mod projection_evidence_cli;
#[cfg(feature = "vector-search")]
pub mod rabitq_projection;
mod range_io;
mod recall_validation;
mod snapshot_writer;
mod vector_execution;

use document_encoding::encode_search_document_line;

mod error {
    pub use skein_core::{Result, SkeinError};
}

/// Storage-neutral source for graph-derived search projection maintenance.
///
/// The search kernel owns projection semantics; embedding code owns graph
/// layout, scan, recovery, and relationship access. This is deliberately
/// analogous to `skein_analytics::ProjectionSource`.
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
        vector_search: cfg!(feature = "vector-search"),
        graph_analytics: cfg!(feature = "graph-analytics"),
        background_maintenance: cfg!(feature = "background-maintenance"),
    }
}

mod compiled_capabilities {
    use skein_core::RuntimeCapabilities;

    pub(crate) const fn effective_runtime_capabilities(
        requested: RuntimeCapabilities,
    ) -> RuntimeCapabilities {
        requested.intersection(super::compiled_runtime_capabilities())
    }
}
use analyzer_lexicon::{CORE_SEMANTIC_ALIAS_RULES, NOWLEDGE_MEMORY_SEMANTIC_ALIAS_RULES};
use analyzer_stream::{document_token_fields, visit_token_list, TokenOccurrence};
#[cfg(test)]
use cjk_tokenizer::is_cjk_search_char;
pub use generation_cleanup::{
    SearchProjectionCleanupOptions, SearchProjectionCleanupReport,
    SEARCH_PROJECTION_CLEANUP_PROTOCOL,
};
use generation_cleanup::{SearchProjectionCleanupState, SearchProjectionGenerations};
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
pub use lexical_term_policy::SearchLexicalTermPolicy;
pub use out_of_core::{
    SearchOutOfCoreConfig, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreGenerationUpdate,
    SearchOutOfCoreGenerationWriter, SearchOutOfCoreHydrationOutput, SearchOutOfCoreMetrics,
    SearchOutOfCoreOutput, SearchOutOfCoreReader,
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
use snapshot_writer::write_search_snapshot;
pub use snapshot_writer::SearchCheckpointReport;
use vector_execution::{execute_search_vector_plan, SearchVectorExecutionRequest};

#[cfg(feature = "vector-search")]
use rabitq_projection::{RaBitQCandidateProjection, RaBitQCandidateProjectionBuildOptions};

const SEARCH_SNAPSHOT_FILE: &str = "search_projection.skein";
const SEARCH_SEGMENT_DESCRIPTOR_FILE: &str = "search_projection_segments.skein";
const SEARCH_SEGMENT_PAYLOAD_FILE: &str = "search_projection_segment_payloads.skein";
const SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID: u64 = 1;
const RABITQ_CANDIDATE_BACKEND: &str = "skein_rabitq_candidate_projection";
static QUARANTINE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "vector-search")]
const SEARCH_RABITQ_PROJECTION_PREFIX: &str = "search_rabitq.";
#[cfg(feature = "vector-search")]
const SEARCH_RABITQ_PROJECTION_SUFFIX: &str = ".skein";
pub const FULL_REINDEX_MARKER: &str = ".reindex_needed";
pub const METADATA_REPAIR_MARKER: &str = ".projection_metadata_repair_needed";
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const TITLE_TERM_FREQUENCY_WEIGHT: usize = 2;
const RRF_K: f64 = 60.0;
const SEARCH_COMPRESSION_HEADER: &str = "SKEIN_COMPRESSED_V1";
const SEARCH_COMPRESSION_LEVEL: i32 = 3;
const SEARCH_DOCUMENT_ID_FIELD: &str = "document_id";
pub use skein_evidence::replacement_contract::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS;

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
    skein_storage::projection_document_id_for_node(catalog, node)
}

pub fn search_projection_document_id_for_label_and_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    node_id: NodeId,
) -> Option<String> {
    skein_storage::projection_document_id_for_label_and_properties(label, properties, node_id)
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
pub struct SearchResultSet {
    pub hits: Vec<SearchHit>,
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
struct SearchScoredCandidate {
    id: String,
    score: f64,
    vector_score: f64,
    text_score: f64,
    rrf_score: f64,
    vector_rrf_score: f64,
    text_rrf_score: f64,
    vector_rank: Option<usize>,
    text_rank: Option<usize>,
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
    QueryEmbeddingMissing,
    TextQueryEmpty,
}

impl SearchFallbackReasonCode {
    pub fn as_str(self) -> &'static str {
        match self {
            SearchFallbackReasonCode::VectorDimensionMismatch => "vector_dimension_mismatch",
            SearchFallbackReasonCode::VectorIndexEmpty => "vector_index_empty",
            SearchFallbackReasonCode::CompressedVectorProjectionUnavailable => {
                "compressed_vector_projection_unavailable"
            }
            SearchFallbackReasonCode::QueryEmbeddingMissing => "query_embedding_missing",
            SearchFallbackReasonCode::TextQueryEmpty => "text_query_empty",
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
            "query_embedding_missing" => Ok(SearchFallbackReasonCode::QueryEmbeddingMissing),
            "text_query_empty" => Ok(SearchFallbackReasonCode::TextQueryEmpty),
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
    pub candidate_scan_payload_bytes_read: u64,
    pub candidate_scan_admitted_working_bytes: usize,
    pub posting_bytes_read: u64,
    pub candidate_postings_visited: u64,
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

    fn validate(&self) -> Result<()> {
        if self.policy_epoch == 0 {
            return Err(SkeinError::Storage(
                "access control context requires a non-zero policy epoch".to_string(),
            ));
        }
        if self.visibility_metadata_field.trim().is_empty() {
            return Err(SkeinError::Storage(
                "access control context requires a visibility metadata field".to_string(),
            ));
        }
        if self.allowed_visibility_values.is_empty() {
            return Err(SkeinError::Storage(
                "access control context requires at least one visibility value".to_string(),
            ));
        }
        if self
            .allowed_visibility_values
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(SkeinError::Storage(
                "access control context visibility values must be non-empty".to_string(),
            ));
        }
        Ok(())
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
                    SkeinError::Storage(format!(
                        "failed to encode access control visibility predicate: {error}"
                    ))
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
    pub(crate) fn projection_preference(self) -> skein_vector_projection::KernelPreference {
        match self {
            Self::Auto => skein_vector_projection::KernelPreference::Auto,
            Self::Scalar => skein_vector_projection::KernelPreference::Scalar,
            Self::Avx2 => skein_vector_projection::KernelPreference::Avx2,
            Self::Neon => skein_vector_projection::KernelPreference::Neon,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VectorSearchExecutionOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: VectorSearchKernelPreference,
    pub task_context: Option<&'a skein_core::RuntimeTaskContext>,
    capture_candidate_ids: bool,
}

impl<'a> VectorSearchExecutionOptions<'a> {
    pub fn bounded(
        max_parallelism: NonZeroUsize,
        max_working_bytes: usize,
        task_context: Option<&'a skein_core::RuntimeTaskContext>,
    ) -> Self {
        Self {
            max_parallelism,
            max_working_bytes,
            kernel: VectorSearchKernelPreference::Auto,
            task_context,
            capture_candidate_ids: false,
        }
    }

    pub fn admitted(
        max_working_bytes: usize,
        task_context: &'a skein_core::RuntimeTaskContext,
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

    pub fn capture_candidates_for_validation(mut self) -> Self {
        self.capture_candidate_ids = true;
        self
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

    fn semantic_aliases(&self, token: &str) -> Vec<String> {
        self.alias_rules
            .iter()
            .filter(|rule| rule.inputs.iter().any(|input| input == token))
            .flat_map(|rule| rule.aliases.iter().cloned())
            .collect()
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
}

#[derive(Debug)]
pub struct SearchIndex {
    documents: BTreeMap<String, SearchDocument>,
    path: Option<PathBuf>,
    embedding_dimension: Option<usize>,
    embedding_manifest: Option<SearchEmbeddingManifest>,
    import_source_graph_commit_epoch: Option<u64>,
    source_graph_commit_epoch: Option<u64>,
    durable_source_graph_commit_epoch: Mutex<Option<u64>>,
    marker_lines: Mutex<BTreeMap<String, Vec<String>>>,
    analyzer_lexicon: SearchAnalyzerLexicon,
    lexical_projection: Mutex<Option<Arc<LexicalProjectionReader>>>,
    lexical_delta: Mutex<Arc<LexicalMiniDelta>>,
    lexical_config: LexicalProjectionConfig,
    #[cfg(feature = "vector-search")]
    rabitq_projection: Mutex<Option<Arc<RaBitQCandidateProjection>>>,
    #[cfg(feature = "vector-search")]
    rabitq_build_options: RaBitQCandidateProjectionBuildOptions,
    segment_descriptor: Option<SearchSegmentDescriptor>,
    range_read_config: SearchRangeReadConfig,
    cleanup_state: Mutex<SearchProjectionCleanupState>,
    runtime_capabilities: RuntimeCapabilities,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

impl SearchIndex {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn is_persistent(&self) -> bool {
        self.path.is_some()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(path.as_ref())?;
        let mut index = Self {
            path: Some(path.as_ref().to_path_buf()),
            ..Self::default()
        };
        index.load_snapshot()?;
        index.load_or_rebuild_segment_descriptor()?;
        index.load_lexical_projection()?;
        #[cfg(feature = "vector-search")]
        index.load_rabitq_projection();
        index.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
        Ok(index)
    }

    pub fn with_analyzer_lexicon(mut self, analyzer_lexicon: SearchAnalyzerLexicon) -> Self {
        self.analyzer_lexicon = analyzer_lexicon;
        self.invalidate_lexical_projection();
        self
    }

    pub fn set_analyzer_lexicon(&mut self, analyzer_lexicon: SearchAnalyzerLexicon) {
        self.analyzer_lexicon = analyzer_lexicon;
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

    #[cfg(feature = "vector-search")]
    pub fn set_rabitq_projection_build_options(
        &mut self,
        options: RaBitQCandidateProjectionBuildOptions,
    ) {
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
    fn load_rabitq_projection(&self) {
        let Some(path) = &self.path else {
            return;
        };
        for (generation, artifact_path) in rabitq_artifacts_descending(path) {
            let identity = self.rabitq_projection_identity(generation);
            match RaBitQCandidateProjection::load_from_path_classified(
                &artifact_path,
                &self.documents,
                &identity,
            ) {
                Ok(projection) => {
                    *self
                        .rabitq_projection
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(Arc::new(projection));
                    return;
                }
                Err(error) if error.should_quarantine() => {
                    if let Some(name) = artifact_path.file_name().and_then(|name| name.to_str()) {
                        quarantine_rebuildable_artifact(path, name);
                    }
                }
                Err(_) => {}
            }
        }
    }

    #[cfg(feature = "vector-search")]
    fn rabitq_projection_identity(
        &self,
        generation: u64,
    ) -> skein_vector_projection::ProjectionIdentity {
        skein_vector_projection::ProjectionIdentity {
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
    fn build_in_memory_rabitq_projection(&self) -> Result<Option<Arc<RaBitQCandidateProjection>>> {
        if let Some(projection) = self.rabitq_projection() {
            return Ok(Some(projection));
        }
        let projection = RaBitQCandidateProjection::build_from_documents(
            &self.documents,
            self.rabitq_projection_identity(0),
            self.rabitq_build_options,
        )?
        .map(Arc::new);
        *self
            .rabitq_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = projection.clone();
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
            Arc::make_mut(&mut delta).upsert(
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
            Arc::make_mut(&mut delta).delete(
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
        self.telemetry = telemetry;
    }

    pub fn telemetry_sink_configured(&self) -> bool {
        self.telemetry.is_some()
    }

    pub fn set_range_read_config(&mut self, config: SearchRangeReadConfig) {
        self.range_read_config = config;
    }

    pub fn range_read_config(&self) -> SearchRangeReadConfig {
        self.range_read_config
    }

    pub fn set_runtime_capabilities(&mut self, capabilities: RuntimeCapabilities) {
        self.runtime_capabilities =
            crate::compiled_capabilities::effective_runtime_capabilities(capabilities);
    }

    pub fn runtime_capabilities(&self) -> RuntimeCapabilities {
        self.runtime_capabilities
    }

    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        if let Some(embedding) = &document.embedding {
            self.validate_or_set_dimension(embedding.len())?;
        }
        self.record_lexical_upsert(&document);
        self.documents.insert(document.id.clone(), document);
        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();
        self.segment_descriptor = None;
        Ok(())
    }

    pub fn upsert_projection_row(&mut self, row: SearchProjectionRow) -> Result<()> {
        self.upsert(row.into_document())
    }

    pub fn delete(&mut self, id: &str) {
        self.record_lexical_delete(id);
        self.documents.remove(id);
        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();
        self.segment_descriptor = None;
    }

    pub fn apply_projection_delta(
        &mut self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let operation_count = delta.operation_count();
        if let Some(limit) = delta.max_operations
            && operation_count > limit
        {
            return Err(SkeinError::Storage(format!(
                    "incremental projection update operation count {operation_count} exceeded configured limit {limit}"
                )));
        }

        let mut next_embedding_dimension = self.embedding_dimension;
        for row in &delta.upserts {
            let Some(embedding) = &row.embedding else {
                continue;
            };
            let dimension = embedding.len();
            if let Some(manifest) = &self.embedding_manifest
                && manifest.dimension != dimension
            {
                return Err(SkeinError::Storage(format!(
                    "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                    manifest.dimension
                )));
            }
            match next_embedding_dimension {
                Some(existing) if existing != dimension => {
                    return Err(SkeinError::Storage(format!(
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
            self.record_lexical_delete(&id);
            if self.documents.remove(&id).is_some() {
                deleted_documents += 1;
            }
        }
        let upserted_documents = upserts.len();
        for row in upserts {
            let document = row.into_document();
            self.record_lexical_upsert(&document);
            self.documents.insert(document.id.clone(), document);
        }

        #[cfg(feature = "vector-search")]
        self.invalidate_rabitq_projection();

        self.segment_descriptor = None;
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
        match policy.admit(state, &delta.background_work_request()) {
            QosAdmission::Admit => self.apply_projection_delta(delta),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection delta deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection delta rejected: {reason}"
            ))),
        }
    }

    pub fn apply_scheduled_background_projection_delta(
        &mut self,
        scheduler: &LocalQosScheduler,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        self.runtime_capabilities
            .require(RuntimeCapability::BackgroundMaintenance)?;
        let permit = match scheduler.try_start(delta.background_work_request()) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection delta deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection delta rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.apply_projection_delta(delta);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    pub fn document(&self, id: &str) -> Option<&SearchDocument> {
        self.documents.get(id)
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    /// Returns vector-bearing document IDs accepted by Skein's metadata
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
        let full_reindex_reasons = self
            .read_marker_lines(FULL_REINDEX_MARKER)
            .unwrap_or_default();
        let metadata_repair_reasons = self
            .read_marker_lines(METADATA_REPAIR_MARKER)
            .unwrap_or_default();
        let durable_source_graph_commit_epoch = *self
            .durable_source_graph_commit_epoch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        SearchProjectionFreshness {
            document_count: self.documents.len(),
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
        self.validate_import_source_graph_commit_epoch(epoch)?;
        self.import_source_graph_commit_epoch = Some(epoch);
        Ok(())
    }

    /// Verifies that an import retry still belongs to the same frozen legacy
    /// source without changing the in-memory projection state.
    pub fn validate_import_source_graph_commit_epoch(&self, epoch: u64) -> Result<()> {
        match self.import_source_graph_commit_epoch {
            Some(existing) if existing != epoch => Err(SkeinError::Storage(
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
            "protocol": "skein-nowledge-search-projection-probe",
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
        self.invalidate_rabitq_projection();
        Ok(())
    }

    pub fn rebuild_from_graph<S: SearchProjectionSource + ?Sized>(
        &mut self,
        catalog: &Catalog,
        store: &S,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        let started = std::time::Instant::now();
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
                if options
                    .max_rows
                    .map(|limit| next_documents.len() >= limit)
                    .unwrap_or(false)
                {
                    self.mark_full_reindex_needed("full rebuild exceeded configured row limit")?;
                    return Err(SkeinError::Storage(format!(
                        "full rebuild exceeded configured row limit after {} documents",
                        next_documents.len()
                    )));
                }
                let document = row.into_document();
                next_documents.insert(document.id.clone(), document);
                Ok(())
            })?;

            self.documents = next_documents;
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
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.rebuild_derived_artifacts(catalog, store, options),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection rebuild deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background search projection rebuild rejected: {reason}"
            ))),
        }
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
        let permit = match scheduler.try_start(request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection rebuild deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search projection rebuild rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

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
        let started = std::time::Instant::now();
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
                    return Err(SkeinError::Storage(format!(
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
        match policy.admit(state, &request) {
            QosAdmission::Admit => self.repair_metadata_from_graph(catalog, store, options),
            QosAdmission::Defer { reason, .. } => Err(SkeinError::Storage(format!(
                "background search metadata repair deferred: {reason}"
            ))),
            QosAdmission::Reject { reason, .. } => Err(SkeinError::Storage(format!(
                "background search metadata repair rejected: {reason}"
            ))),
        }
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
        let permit = match scheduler.try_start(request) {
            Ok(permit) => permit,
            Err(QosAdmission::Defer { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search metadata repair deferred: {reason}"
                )));
            }
            Err(QosAdmission::Reject { reason, .. }) => {
                return Err(SkeinError::Storage(format!(
                    "background search metadata repair rejected: {reason}"
                )));
            }
            Err(QosAdmission::Admit) => unreachable!("admitted work returns a permit"),
        };

        let result = self.repair_metadata_from_graph(catalog, store, options);
        permit.finish_with_outcome(result.is_ok());
        result
    }

    fn rebuild_estimated_operations<S: SearchProjectionSource + ?Sized>(&self, store: &S) -> usize {
        store
            .estimated_projection_node_count()
            .max(self.documents.len())
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
        let (out_of_core_generation, out_of_core_discovery_failed) =
            match out_of_core::published_generation(path, &self.analyzer_lexicon) {
                Ok(generation) => (generation, false),
                Err(_) => (None, true),
            };
        let generations = SearchProjectionGenerations {
            lexical: self
                .lexical_projection
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .map(|projection| projection.generation()),
            out_of_core: out_of_core_generation,
            #[cfg(feature = "vector-search")]
            rabitq: self
                .rabitq_projection()
                .map(|projection| projection.manifest().identity.generation),
            #[cfg(not(feature = "vector-search"))]
            rabitq: None,
            rabitq_remove_all: self
                .documents
                .values()
                .all(|document| document.embedding.is_none()),
            out_of_core_discovery_failed,
        };
        self.cleanup_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .run(path, generations, options)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.checkpoint_with_report().map(|_| ())
    }

    pub fn checkpoint_with_report(&self) -> Result<SearchCheckpointReport> {
        let Some(path) = &self.path else {
            return Ok(SearchCheckpointReport::in_memory(self.documents.len()));
        };
        let started = std::time::Instant::now();
        let result = (|| {
            let _publish_lease = out_of_core::SearchProjectionPublishLease::acquire(path)?;
            let snapshot_path = path.join(SEARCH_SNAPSHOT_FILE);
            let snapshot = write_search_snapshot(
                &snapshot_path,
                self.source_graph_commit_epoch,
                self.import_source_graph_commit_epoch,
                self.embedding_manifest.as_ref(),
                self.embedding_dimension,
                self.documents.values(),
            )?;
            self.write_segment_artifacts(path)?;
            self.write_lexical_projection(path)?;
            let projection_generation = out_of_core::publish_out_of_core_projection(self, path)?;
            #[cfg(feature = "vector-search")]
            self.write_rabitq_projection(path)?;
            *self
                .durable_source_graph_commit_epoch
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = self.source_graph_commit_epoch;
            self.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
            Ok(snapshot.finish(projection_generation))
        })();
        if let Some(telemetry) = &self.telemetry {
            let (byte_count, generation) = result
                .as_ref()
                .map(|report| {
                    (
                        report.snapshot_compressed_bytes,
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

    fn write_lexical_projection(&self, path: &Path) -> Result<()> {
        let generation =
            out_of_core::next_generation(path, self.lexical_config.max_manifest_bytes.get())?;
        let projection = LexicalProjectionWriter::new(self.lexical_config).write(
            path,
            generation,
            self.source_graph_commit_epoch,
            lexical_analyzer_digest(&self.analyzer_lexicon),
            lexical_documents_digest(&self.documents),
            self.documents.values(),
            &self.analyzer_lexicon,
        )?;
        self.replace_lexical_projection(Some(projection));
        Ok(())
    }

    #[cfg(feature = "vector-search")]
    fn write_rabitq_projection(&self, path: &Path) -> Result<()> {
        if self
            .documents
            .values()
            .all(|document| document.embedding.is_none())
        {
            self.invalidate_rabitq_projection();
            return Ok(());
        }
        let loaded_generation = self
            .rabitq_projection()
            .map(|projection| projection.manifest().identity.generation)
            .unwrap_or(0);
        let artifact_generation = latest_rabitq_artifact(path)
            .map(|(generation, _)| generation)
            .unwrap_or(0);
        let generation = loaded_generation.max(artifact_generation).saturating_add(1);
        let artifact_path = path.join(rabitq_artifact_file(generation));
        let projection = RaBitQCandidateProjection::write_from_documents(
            &artifact_path,
            &self.documents,
            self.rabitq_projection_identity(generation),
            self.rabitq_build_options,
        )?
        .ok_or_else(|| {
            SkeinError::Storage(
                "Skein RaBitQ projection build produced no artifact for vector documents"
                    .to_string(),
            )
        })?;
        *self
            .rabitq_projection
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(projection));
        Ok(())
    }

    pub fn search(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        limit: usize,
    ) -> Vec<SearchHit> {
        self.search_with_report(query_text, query_embedding, mode, limit)
            .hits
    }

    pub fn search_with_report(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        limit: usize,
    ) -> SearchResultSet {
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

    pub fn search_with_options(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> SearchResultSet {
        self.try_search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            SearchExecutionStrategy::fixed(VectorSearchBackend::Scalar, None, false),
            None,
        )
        .expect("in-memory search path does not perform fallible range I/O")
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
            SearchExecutionStrategy::fixed(VectorSearchBackend::Scalar, None, true),
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
            SearchExecutionStrategy::fixed(VectorSearchBackend::Scalar, None, true),
            Some(&access_control),
        )
    }

    pub fn search_with_options_prefer_compressed_vector_projection(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> SearchResultSet {
        self.search_with_options_compressed_vector_projection_mode(
            query_text,
            query_embedding,
            mode,
            options,
            CompressedVectorSearchMode::Preferred,
        )
    }

    pub fn search_with_options_compressed_vector_projection_mode(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        compressed_vector_search_mode: CompressedVectorSearchMode,
    ) -> SearchResultSet {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            AdaptiveVectorSearchOptions::new(compressed_vector_search_mode),
            AdaptiveVectorExecutionControls::IN_MEMORY,
        )
        .expect("in-memory search path does not perform fallible range I/O")
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
                .map_err(|reason| SkeinError::Execution(format!("vector search task {reason}")))?;
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
                .map_err(|reason| SkeinError::Execution(format!("vector search task {reason}")))?;
        }
        Ok(result)
    }

    pub fn search_with_options_adaptive_vector_projection(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
    ) -> SearchResultSet {
        self.try_search_with_options_compressed_vector_projection_mode_internal(
            query_text,
            query_embedding,
            mode,
            options,
            adaptive_options,
            AdaptiveVectorExecutionControls::IN_MEMORY,
        )
        .expect("in-memory search path does not perform fallible range I/O")
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

    fn validate_sampled_vector_recall_with_controls(
        &self,
        options: VectorRecallValidationOptions,
        exact_controls: AdaptiveVectorExecutionControls<'_>,
        approximate_controls: AdaptiveVectorExecutionControls<'_>,
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
            VectorRecallValidationAccumulator::new(sample_candidate_count, &options);
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
            let Ok(approximate) = self
                .try_search_with_options_compressed_vector_projection_mode_internal(
                    "",
                    Some(embedding),
                    SearchMode::Vector,
                    approximate_query_options,
                    AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Required),
                    approximate_controls,
                )
            else {
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
        mut options: VectorRecallValidationOptions,
        access_control: &SearchAccessControlContext,
    ) -> VectorRecallValidationReport {
        match access_control.effective_metadata_filters(&options.metadata_filters) {
            Ok(filters) => options.metadata_filters = filters,
            Err(_) => {
                let mut accumulator = VectorRecallValidationAccumulator::new(0, &options);
                accumulator.mark_metadata_filter_invalid();
                return accumulator.finish();
            }
        }
        self.validate_sampled_vector_recall(options)
    }

    pub fn vector_projection_qualification_identity(
        &self,
    ) -> Option<VectorProjectionQualificationIdentity> {
        #[cfg(feature = "vector-search")]
        {
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
    ) -> PreparedAdaptiveVectorBackend {
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
            return PreparedAdaptiveVectorBackend {
                decision: preliminary,
                fallback_reason: None,
                #[cfg(feature = "vector-search")]
                projection: None,
            };
        }

        #[cfg(feature = "vector-search")]
        {
            let projection = self.rabitq_projection().or_else(|| {
                self.path
                    .is_none()
                    .then(|| self.build_in_memory_rabitq_projection().ok().flatten())
                    .flatten()
            });
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
            PreparedAdaptiveVectorBackend {
                projection: (decision.backend == AdaptiveVectorBackend::QuantizedProjection)
                    .then_some(projection)
                    .flatten(),
                decision,
                fallback_reason,
            }
        }

        #[cfg(not(feature = "vector-search"))]
        {
            let decision = select_adaptive_vector_backend(decision_input(false, 0), request.policy);
            PreparedAdaptiveVectorBackend {
                decision,
                fallback_reason: adaptive_vector_fallback_reason(decision),
            }
        }
    }

    /// Executes externally generated candidates through Skein's normal
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
        let query_terms = tokenize(query_text, &self.analyzer_lexicon);
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
            Some(access_control) => {
                if let Some(policy_epoch) = options.policy_epoch
                    && policy_epoch != access_control.policy_epoch
                {
                    return Err(SkeinError::Storage(format!(
                        "search options policy epoch {policy_epoch} does not match access control policy epoch {}",
                        access_control.policy_epoch
                    )));
                }
                access_control.effective_metadata_filters(&options.metadata_filters)?
            }
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
                self.segment_descriptor.as_ref(),
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
        let vector_backend = match vector_backend_request {
            VectorSearchBackendRequest::Fixed(backend) => backend,
            VectorSearchBackendRequest::Adaptive(request)
                if mode != SearchMode::Text && query_embedding.is_some() =>
            {
                prepared_adaptive_backend = Some(
                    self.prepare_adaptive_vector_backend(
                        request,
                        &filtered_documents,
                        self.embedding_dimension
                            .unwrap_or_else(|| query_embedding.map_or(0, <[f32]>::len)),
                    ),
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
        let (text_fallback_reason_codes, text_fallback_reasons) =
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
        let text_corpus =
            if text_available && mode != SearchMode::Vector && lexical_snapshot.is_none() {
                Some(TextCorpusStats::from_documents(
                    filtered_documents.iter().copied(),
                    &self.analyzer_lexicon,
                ))
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
                documents: &filtered_documents,
                backend: vector_backend,
                filter_fields: vector_filter_fields,
                limit,
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
                projection.score(&query_terms, delta, retained_text_score_limit, |id| {
                    Ok(filtered_documents
                        .binary_search_by(|document| document.id.as_str().cmp(id))
                        .is_ok())
                })
            })
            .transpose()?;
        let (
            mut text_scores,
            lexical_matching_document_count,
            lexical_postings_visited,
            lexical_bytes_read,
        ) = if let Some(report) = lexical_report {
            (
                report.scores,
                report.matching_document_count,
                report.postings_visited,
                report.bytes_read,
            )
        } else {
            (BTreeMap::new(), 0, 0, 0)
        };
        if lexical_snapshot.is_none() {
            for document in &filtered_documents {
                let text_score = if text_available && mode != SearchMode::Vector {
                    text_corpus
                        .as_ref()
                        .map(|corpus| {
                            bm25_score(&query_terms, document, corpus, &self.analyzer_lexicon)
                        })
                        .unwrap_or(0.0)
                } else {
                    0.0
                };
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
        let retrievers = vec![
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
                            == skein_executor::VectorScoreSource::RawVector
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
                candidate_scan_payload_bytes_read: 0,
                candidate_scan_admitted_working_bytes: 0,
                posting_bytes_read: lexical_bytes_read,
                candidate_postings_visited: lexical_postings_visited,
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
                    SkeinError::Storage(format!(
                        "search candidate {} is missing from the filtered document set",
                        candidate.id
                    ))
                })?;
            hits.push(SearchHit {
                id: candidate.id,
                score: candidate.score,
                vector_score: candidate.vector_score,
                text_score: candidate.text_score,
                rrf_score: candidate.rrf_score,
                vector_rrf_score: candidate.vector_rrf_score,
                text_rrf_score: candidate.text_rrf_score,
                vector_rank: candidate.vector_rank,
                text_rank: candidate.text_rank,
                kind: document.metadata.get("kind").cloned(),
                external_id: document.metadata.get("external_id").cloned(),
                source_id: document.metadata.get("source_id").cloned(),
                matched_terms: matched_query_terms(&query_terms, document, &self.analyzer_lexicon),
                matched_spans: matched_query_spans(&query_terms, document, &self.analyzer_lexicon),
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
            return Err(SkeinError::Storage(format!(
                "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                manifest.dimension
            )));
        }
        match self.embedding_dimension {
            Some(existing) if existing != dimension => Err(SkeinError::Storage(format!(
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
        if !snapshot_path.exists() {
            return Ok(());
        }
        let text = read_search_snapshot_text(&snapshot_path)?;
        let (body, checksum) = split_checksum(&text)?;
        let actual = checksum_bytes(body.as_bytes());
        if checksum != actual {
            return Err(SkeinError::Storage(format!(
                "search projection checksum mismatch: expected {checksum}, got {actual}"
            )));
        }
        for line in body.lines() {
            if line == "SKEIN_SEARCH_PROJECTION_V1" {
                continue;
            }
            let fields = line.split('\t').collect::<Vec<_>>();
            match fields.as_slice() {
                ["source_graph_commit_epoch", raw] => {
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
                }
                ["embedding_dimension", raw] => {
                    let dimension = parse_usize(raw, "embedding dimension")?;
                    if let Some(manifest) = &self.embedding_manifest
                        && manifest.dimension != dimension
                    {
                        return Err(SkeinError::Storage(format!(
                                "embedding manifest dimension {} does not match snapshot dimension {dimension}",
                                manifest.dimension
                            )));
                    }
                    self.embedding_dimension = Some(dimension);
                }
                ["doc", raw_id, raw_title, raw_content, raw_embedding, raw_metadata] => {
                    let embedding = decode_embedding(raw_embedding)?;
                    if let Some(embedding) = &embedding {
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
                    return Err(SkeinError::Storage(format!(
                        "invalid search projection line: {line}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn load_or_rebuild_segment_descriptor(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        self.segment_descriptor = match read_search_segment_descriptor(path) {
            Ok(Some(descriptor))
                if descriptor.matches_documents(&self.documents)
                    && descriptor.payload_artifact_is_available(path) =>
            {
                Some(descriptor)
            }
            Ok(None) => Some(SearchSegmentDescriptor::build(&self.documents)),
            Ok(Some(_)) | Err(_) => {
                quarantine_rebuildable_artifact(path, SEARCH_SEGMENT_DESCRIPTOR_FILE);
                let descriptor = SearchSegmentDescriptor::build(&self.documents);
                let _ = write_search_segment_descriptor(path, &descriptor);
                Some(descriptor)
            }
        };
        Ok(())
    }

    fn write_segment_artifacts(&self, path: &Path) -> Result<()> {
        let mut descriptor = SearchSegmentDescriptor::build(&self.documents);
        write_search_segment_payloads(path, &self.documents, &mut descriptor)?;
        write_search_segment_descriptor(path, &descriptor)
    }

    fn marker_path(&self, name: &str) -> Option<PathBuf> {
        self.path.as_ref().map(|path| path.join(name))
    }

    fn append_marker(&self, name: &str, reason: &str) -> Result<()> {
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
            documents: BTreeMap::new(),
            path: None,
            embedding_dimension: None,
            embedding_manifest: None,
            import_source_graph_commit_epoch: None,
            source_graph_commit_epoch: None,
            durable_source_graph_commit_epoch: Mutex::new(None),
            marker_lines: Mutex::new(BTreeMap::new()),
            analyzer_lexicon: SearchAnalyzerLexicon::default(),
            lexical_projection: Mutex::new(None),
            lexical_delta: Mutex::new(Arc::default()),
            lexical_config: LexicalProjectionConfig::default(),
            #[cfg(feature = "vector-search")]
            rabitq_projection: Mutex::new(None),
            #[cfg(feature = "vector-search")]
            rabitq_build_options: RaBitQCandidateProjectionBuildOptions::default(),
            segment_descriptor: None,
            range_read_config: SearchRangeReadConfig::default(),
            cleanup_state: Mutex::new(SearchProjectionCleanupState::default()),
            runtime_capabilities: crate::compiled_runtime_capabilities(),
            telemetry: None,
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
fn latest_rabitq_artifact(path: &Path) -> Option<(u64, PathBuf)> {
    rabitq_artifacts_descending(path).into_iter().next()
}

#[cfg(feature = "vector-search")]
fn rabitq_artifacts_descending(path: &Path) -> Vec<(u64, PathBuf)> {
    let Ok(entries) = fs::read_dir(path) else {
        return Vec::new();
    };
    let mut artifacts = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let generation = entry
                .file_name()
                .to_str()
                .and_then(rabitq_artifact_generation)?;
            Some((generation, entry.path()))
        })
        .collect::<Vec<_>>();
    artifacts.sort_unstable_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    artifacts
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
    documents: &BTreeMap<String, SearchDocument>,
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
    documents: &BTreeMap<String, SearchDocument>,
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
        .as_ref()
        .is_some_and(|descriptor| descriptor.matches_documents(&index.documents));
    let transient_descriptor;
    let descriptor = match index
        .segment_descriptor
        .as_ref()
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
            && index
                .path
                .as_ref()
                .is_some_and(|path| descriptor.payload_artifact_is_available(path))
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
        .as_ref()
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
    let projection = index.rabitq_projection().or_else(|| {
        index
            .path
            .is_none()
            .then(|| index.build_in_memory_rabitq_projection().ok().flatten())
            .flatten()
    });
    match projection {
        Some(projection) => serde_json::json!({
            "engine": "skein_rabitq_scan",
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
            "supports_runtime_simd_dispatch": false,
            "supports_governed_parallelism": true,
            "raw_rerank_required": true,
            "persisted_artifact_used": projection.is_file_backed(),
            "artifact_rebuilt_from_snapshot": !projection.is_file_backed(),
            "blocker_codes": [],
        }),
        None => serde_json::json!({
            "engine": "skein_rabitq_scan",
            "compiled": true,
            "ready": false,
            "algorithm": skein_vector_projection::PROJECTION_ALGORITHM,
            "bit_width": skein_vector_projection::PROJECTION_BIT_WIDTH,
            "dimension": serde_json::Value::Null,
            "document_count": 0,
            "quantizer": skein_vector_projection::PROJECTION_QUANTIZER,
            "calibration": skein_vector_projection::PROJECTION_CALIBRATION,
            "supports_allowlist": true,
            "supports_filter_bitmap": true,
            "supports_scalar_reference": true,
            "supports_runtime_simd_dispatch": false,
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
        "engine": "skein_rabitq_scan",
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

fn projection_row_from_node_with_business_labels(
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
    first_non_empty_string_property(label, &["canonical_name", "name", "id"])
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
        Value::String(value) => value.clone(),
        Value::Binary(value) => format!(
            "\\x{}",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        Value::Uuid(value) => value.to_string(),
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

fn top_ranked_ids(ranks: &BTreeMap<String, usize>, limit: usize) -> Vec<String> {
    let mut ranked = ranks
        .iter()
        .map(|(id, rank)| (id.clone(), *rank))
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

fn top_ranked_candidates(
    ranks: &BTreeMap<String, usize>,
    scores: &BTreeMap<String, f64>,
    limit: usize,
) -> Vec<SearchRetrieverCandidate> {
    let mut ranked = ranks
        .iter()
        .filter_map(|(id, rank)| {
            scores.get(id).map(|score| SearchRetrieverCandidate {
                id: id.clone(),
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
    let input_predicate_count = filters.len();
    let pushdown = push_search_predicates(&predicates, SearchScanPredicateSupport::default());
    debug_assert!(
        pushdown.residual().is_empty(),
        "default search scan support should push every metadata predicate"
    );
    let report = SearchPredicatePushdownReport {
        input_predicate_count,
        pushed_predicate_count: pushdown.pushed().predicates().len(),
        residual_predicate_count: pushdown.residual().predicates().len(),
        unsatisfiable: pushdown.pushed().is_unsatisfiable(),
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
        field_summaries: Vec::new(),
    };
    SearchMetadataPredicatePushdown {
        predicates: pushdown.pushed().clone(),
        report,
    }
}

fn search_document_matches_predicates(
    document: &SearchDocument,
    predicates: &SearchPredicateSet,
) -> bool {
    if predicates.is_unsatisfiable() {
        return false;
    }
    predicates
        .predicates()
        .iter()
        .all(|predicate| search_document_matches_predicate(document, predicate))
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
    values: BTreeSet<String>,
    numeric_range: Option<SearchNumericRange>,
    timestamp_range: Option<SearchTimestampRange>,
}

fn filter_search_documents_with_segment_pruning<'a>(
    documents: &'a BTreeMap<String, SearchDocument>,
    predicates: &SearchPredicateSet,
    descriptor: Option<&SearchSegmentDescriptor>,
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
    documents: &'a BTreeMap<String, SearchDocument>,
    predicates: &SearchPredicateSet,
    descriptor: &SearchSegmentDescriptor,
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
                .range(segment.first_document_id.clone()..=segment.last_document_id.clone())
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
        let summary = self.storage_summary();
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

    fn storage_summary(&self) -> SegmentSummary {
        let mut segment = SegmentSummary::new(0, self.document_count as u64);
        for field in self
            .present_counts
            .keys()
            .chain(self.values.keys())
            .chain(self.numeric_ranges.keys())
            .chain(self.timestamp_ranges.keys())
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
        excluded_values: &BTreeSet<skein_optimizer::SearchScalarValue>,
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
    fn build(documents: &BTreeMap<String, SearchDocument>) -> Self {
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

    fn matches_documents(&self, documents: &BTreeMap<String, SearchDocument>) -> bool {
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

    fn payload_artifact_is_available(&self, path: &Path) -> bool {
        let Some(last_range) = self
            .segments
            .iter()
            .filter_map(|segment| segment.payload_range)
            .next_back()
        else {
            return true;
        };
        fs::metadata(path.join(SEARCH_SEGMENT_PAYLOAD_FILE))
            .ok()
            .is_some_and(|metadata| {
                metadata.len() >= last_range.offset.saturating_add(last_range.length)
            })
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
        let summary = self.storage_summary();
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

    fn storage_summary(&self) -> SegmentSummary {
        let mut segment = SegmentSummary::new(0, self.document_count as u64);
        for (field, persisted) in &self.metadata {
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
            summary = summary.with_enum_dictionary(EnumDictionaryStats::complete(
                persisted
                    .values
                    .iter()
                    .map(|value| search_segment_summary_value(field, value))
                    .map(Value::String),
            ));
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
        excluded_values: &BTreeSet<skein_optimizer::SearchScalarValue>,
    ) -> bool {
        let Some(summary) = self.metadata.get(field) else {
            return true;
        };
        if summary.present_count < self.document_count {
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

fn search_segment_descriptor_fields(
    documents: &BTreeMap<String, SearchDocument>,
) -> BTreeSet<String> {
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

fn search_document_matches_predicate(
    document: &SearchDocument,
    predicate: &SearchPredicate,
) -> bool {
    let actual_values = search_document_field_values(document, predicate.field().name());
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

fn search_document_field_value<'a>(document: &'a SearchDocument, key: &str) -> Option<&'a str> {
    match key {
        SEARCH_DOCUMENT_ID_FIELD => Some(document.id.as_str()),
        "space_id" => Some(
            document
                .metadata
                .get(key)
                .map(String::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or(DEFAULT_SPACE_ID),
        ),
        _ => document.metadata.get(key).map(String::as_str),
    }
}

fn search_document_field_values<'a>(document: &'a SearchDocument, key: &str) -> Vec<Cow<'a, str>> {
    if key == "labels" || key.starts_with("metadata.") {
        return document
            .metadata
            .get(key)
            .map(|value| parse_label_metadata_values(value))
            .unwrap_or_default();
    }
    search_document_field_value(document, key)
        .map(Cow::Borrowed)
        .into_iter()
        .collect()
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
    match key {
        "kind" => metadata_kind_matches(actual, expected),
        _ => normalize_metadata_filter_value(actual) == normalize_metadata_filter_value(expected),
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
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.timestamp_millis());
    }
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Some(timestamp.and_utc().timestamp_millis());
        }
    }
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|timestamp| timestamp.and_utc().timestamp_millis())
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

fn metadata_kind_matches(actual: &str, expected: &str) -> bool {
    match (
        normalized_projection_kind(actual),
        normalized_projection_kind(expected),
    ) {
        (Some(actual), Some(expected)) => actual == expected,
        _ => actual == expected,
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

#[derive(Debug, Clone, PartialEq)]
struct TextCorpusStats {
    document_count: usize,
    average_document_len: f64,
    document_frequency: BTreeMap<String, usize>,
}

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
    )
    .expect("unbounded matched-span collection cannot exhaust its admission limit")
}

fn matched_query_spans_bounded(
    query_terms: &BTreeSet<String>,
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    max_spans: usize,
    max_bytes: u64,
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
    };
    collector.collect("title", &document.title)?;
    collector.collect("content", &document.content)?;
    Ok(collector.spans)
}

struct MatchedSpanCollector<'a> {
    query_terms: &'a BTreeSet<String>,
    analyzer_lexicon: &'a SearchAnalyzerLexicon,
    spans: Vec<SearchMatchedSpan>,
    span_bytes: u64,
    max_spans: usize,
    max_bytes: u64,
}

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
                return Err(SkeinError::Storage(format!(
                    "search matched-span hydration exceeded {} spans",
                    self.max_spans
                )));
            }
            let required = (field.len() as u64)
                .saturating_add(raw.len() as u64)
                .saturating_add(term.len() as u64)
                .saturating_add(std::mem::size_of::<SearchMatchedSpan>() as u64);
            self.span_bytes = self.span_bytes.saturating_add(required);
            if self.span_bytes > self.max_bytes {
                return Err(SkeinError::Storage(format!(
                    "search matched-span hydration requires {} bytes, exceeding {}",
                    self.span_bytes, self.max_bytes
                )));
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

fn tokenize_list(text: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    analyzer_stream::collect_token_list(text, analyzer_lexicon)
}

fn normalized_alias_rule_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

fn normalized_stopword_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

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

fn suffix_stem_variants(stem: &str) -> Vec<String> {
    let mut variants = vec![stem.to_string()];
    if matches!(stem.chars().last(), Some('c' | 'v' | 'z')) {
        variants.push(format!("{stem}e"));
    }
    variants
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

// Final facade vector relevance is positive-only in [0, 1]. Signed RaBitQ
// estimates are candidate-ranking hints, not final scores; nonpositive exact
// similarities cannot contribute a vector hit (text may still match).
fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
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

#[cfg(feature = "vector-search")]
fn dot_product(left: &[f32], right: &[f32]) -> Option<f64> {
    f32::dot(left, right)
}

#[cfg(not(feature = "vector-search"))]
fn dot_product(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }

    Some(left.iter().zip(right).fold(0.0, |sum, (left, right)| {
        sum + f64::from(*left) * f64::from(*right)
    }))
}

fn encode_embedding(embedding: Option<&[f32]>) -> String {
    embedding
        .map(|values| {
            values
                .iter()
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

fn decode_embedding(input: &str) -> Result<Option<Vec<f32>>> {
    if input.is_empty() {
        return Ok(None);
    }
    input
        .split(',')
        .map(|raw| {
            raw.parse::<f32>()
                .map_err(|_| SkeinError::Storage(format!("invalid embedding value: {raw}")))
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

fn decode_metadata(input: &str) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    if input.is_empty() {
        return Ok(metadata);
    }
    for pair in input.split(';') {
        let Some((key, value)) = pair.split_once('=') else {
            return Err(SkeinError::Storage(format!(
                "invalid metadata pair: {pair}"
            )));
        };
        metadata.insert(decode_string(key)?, decode_string(value)?);
    }
    Ok(metadata)
}

fn decode_search_document_line(line: &str) -> Result<SearchDocument> {
    let line = line.strip_suffix('\n').unwrap_or(line);
    let fields = line.split('\t').collect::<Vec<_>>();
    match fields.as_slice() {
        ["doc", raw_id, raw_title, raw_content, raw_embedding, raw_metadata] => {
            Ok(SearchDocument {
                id: decode_string(raw_id)?,
                title: decode_string(raw_title)?,
                content: decode_string(raw_content)?,
                embedding: decode_embedding(raw_embedding)?,
                metadata: decode_metadata(raw_metadata)?,
            })
        }
        _ => Err(SkeinError::Storage(format!(
            "invalid search document line: {line}"
        ))),
    }
}

fn write_search_segment_payloads(
    path: &Path,
    documents: &BTreeMap<String, SearchDocument>,
    descriptor: &mut SearchSegmentDescriptor,
) -> Result<()> {
    let artifact_path = path.join(SEARCH_SEGMENT_PAYLOAD_FILE);
    let tmp_path = artifact_path.with_extension("skein.tmp");
    let mut offset = 0u64;
    {
        let mut file = File::create(&tmp_path)?;
        for segment in &mut descriptor.segments {
            let mut body = String::from("SKEIN_SEARCH_SEGMENT_V1\n");
            for document in documents
                .range(segment.first_document_id.clone()..=segment.last_document_id.clone())
                .map(|(_, document)| document)
            {
                body.push_str(&encode_search_document_line(document));
            }
            let payload = encode_search_snapshot_text(&body)?;
            let length = u64::try_from(payload.len()).map_err(|_| {
                SkeinError::Storage(format!(
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
                SkeinError::Storage("search segment payload artifact length overflow".to_string())
            })?;
        }
        file.sync_all()?;
    }
    durable_replace_file(&tmp_path, &artifact_path)?;
    Ok(())
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
    let encoding = document_encoding::DescriptorEncoding::new(descriptor, max_bytes)?;
    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let tmp_path = descriptor_path.with_extension("skein.tmp");
    {
        let mut file = std::io::BufWriter::new(File::create(&tmp_path)?);
        encoding.write_to(&mut file)?;
        file.flush()?;
        file.get_ref().sync_all()?;
    }
    durable_replace_file(&tmp_path, &descriptor_path)?;
    Ok(encoding.len() as u64)
}

fn read_search_segment_descriptor(path: &Path) -> Result<Option<SearchSegmentDescriptor>> {
    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    if !descriptor_path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&descriptor_path)?;
    decode_search_segment_descriptor_text(&text).map(Some)
}

#[cfg(test)]
fn encode_search_segment_descriptor_body(descriptor: &SearchSegmentDescriptor) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_SEARCH_SEGMENTS_V3\n");
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
                encode_segment_values(&summary.values),
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
    let (body, checksum) = split_checksum(text)?;
    let actual = checksum_bytes(body.as_bytes());
    if checksum != actual {
        return Err(SkeinError::Storage(format!(
            "search segment descriptor checksum mismatch: expected {checksum}, got {actual}"
        )));
    }

    let mut target_documents = None;
    let mut document_count = None;
    let mut segments = Vec::new();
    let mut current_segment = None::<SearchSegmentDescriptorEntry>;

    for line in body.lines() {
        if line == "SKEIN_SEARCH_SEGMENTS_V1"
            || line == "SKEIN_SEARCH_SEGMENTS_V2"
            || line == "SKEIN_SEARCH_SEGMENTS_V3"
        {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
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
            ["field", raw_field, raw_present_count, raw_values] => {
                let Some(segment) = current_segment.as_mut() else {
                    return Err(SkeinError::Storage(
                        "search segment descriptor field appeared before segment".to_string(),
                    ));
                };
                segment.metadata.insert(
                    decode_string(raw_field)?,
                    SearchSegmentFieldSummary {
                        present_count: parse_usize(
                            raw_present_count,
                            "search segment field present count",
                        )?,
                        values: decode_segment_values(raw_values)?,
                        numeric_range: None,
                        timestamp_range: None,
                    },
                );
            }
            ["field", raw_field, raw_present_count, raw_values, raw_numeric_min, raw_numeric_max] =>
            {
                let Some(segment) = current_segment.as_mut() else {
                    return Err(SkeinError::Storage(
                        "search segment descriptor field appeared before segment".to_string(),
                    ));
                };
                segment.metadata.insert(
                    decode_string(raw_field)?,
                    SearchSegmentFieldSummary {
                        present_count: parse_usize(
                            raw_present_count,
                            "search segment field present count",
                        )?,
                        values: decode_segment_values(raw_values)?,
                        numeric_range: decode_search_numeric_range(
                            raw_numeric_min,
                            raw_numeric_max,
                        )?,
                        timestamp_range: None,
                    },
                );
            }
            ["field", raw_field, raw_present_count, raw_values, raw_numeric_min, raw_numeric_max, raw_timestamp_min, raw_timestamp_max] =>
            {
                let Some(segment) = current_segment.as_mut() else {
                    return Err(SkeinError::Storage(
                        "search segment descriptor field appeared before segment".to_string(),
                    ));
                };
                segment.metadata.insert(
                    decode_string(raw_field)?,
                    SearchSegmentFieldSummary {
                        present_count: parse_usize(
                            raw_present_count,
                            "search segment field present count",
                        )?,
                        values: decode_segment_values(raw_values)?,
                        numeric_range: decode_search_numeric_range(
                            raw_numeric_min,
                            raw_numeric_max,
                        )?,
                        timestamp_range: decode_search_timestamp_range(
                            raw_timestamp_min,
                            raw_timestamp_max,
                        )?,
                    },
                );
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
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
            SkeinError::Storage("search segment descriptor missing target_documents".to_string())
        })?,
        document_count: document_count.ok_or_else(|| {
            SkeinError::Storage("search segment descriptor missing document_count".to_string())
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
        return Err(SkeinError::Storage(
            "search segment descriptor has incomplete physical payload ranges".to_string(),
        ));
    }
    let mut previous_end = 0u64;
    for (expected_id, segment) in segments.iter().enumerate() {
        if segment.segment_id != expected_id as u64 {
            return Err(SkeinError::Storage(format!(
                "search segment descriptor expected segment id {expected_id}, got {}",
                segment.segment_id
            )));
        }
        let Some(range) = segment.payload_range else {
            continue;
        };
        if range.artifact_id != SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID {
            return Err(SkeinError::Storage(format!(
                "search segment {} references unsupported payload artifact {}",
                segment.segment_id, range.artifact_id
            )));
        }
        if range.offset < previous_end {
            return Err(SkeinError::Storage(format!(
                "search segment {} payload range overlaps the previous segment",
                segment.segment_id
            )));
        }
        previous_end = range.offset.checked_add(range.length).ok_or_else(|| {
            SkeinError::Storage(format!(
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
        if line == "SKEIN_SEARCH_SEGMENT_V1" {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["doc", ..] => documents.push(decode_search_document_line(line)?),
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
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
        return Err(SkeinError::Storage(format!(
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
        return Err(SkeinError::Storage(format!(
            "search segment {} document bounds do not match its descriptor",
            segment.segment_id
        )));
    }
    if documents.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err(SkeinError::Storage(format!(
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
                return Err(SkeinError::Storage(
                    "search segment field numeric min is greater than max".to_string(),
                ));
            }
            Ok(Some(SearchNumericRange { min, max }))
        }
        _ => Err(SkeinError::Storage(
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
                return Err(SkeinError::Storage(
                    "search segment field timestamp min is greater than max".to_string(),
                ));
            }
            Ok(Some(SearchTimestampRange {
                min_epoch_millis,
                max_epoch_millis,
            }))
        }
        _ => Err(SkeinError::Storage(
            "search segment field timestamp range is incomplete".to_string(),
        )),
    }
}

fn parse_finite_f64(raw: &str, name: &str) -> Result<f64> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {raw}")))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(SkeinError::Storage(format!("invalid {name}: {raw}")))
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
        return Err(SkeinError::Storage(format!(
            "invalid hex string length: {}",
            input.len()
        )));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for offset in (0..input.len()).step_by(2) {
        let byte = input
            .get(offset..offset + 2)
            .and_then(|pair| u8::from_str_radix(pair, 16).ok())
            .ok_or_else(|| SkeinError::Storage(format!("invalid hex string at byte {offset}")))?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|error| SkeinError::Storage(error.to_string()))
}

fn split_checksum(text: &str) -> Result<(&str, u64)> {
    let Some((body, footer)) = text.rsplit_once("checksum\t") else {
        return Err(SkeinError::Storage(
            "search projection missing checksum footer".to_string(),
        ));
    };
    let checksum = parse_u64(footer.trim(), "search projection checksum")?;
    Ok((body, checksum))
}

fn encode_search_snapshot_text(text: &str) -> Result<Vec<u8>> {
    let compressed = zstd::stream::encode_all(text.as_bytes(), SEARCH_COMPRESSION_LEVEL)
        .map_err(|error| SkeinError::Storage(format!("zstd compression failed: {error}")))?;
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
    if !bytes.starts_with(SEARCH_COMPRESSION_HEADER.as_bytes()) {
        return Err(SkeinError::Storage(
            "search projection is missing the V1 compressed envelope".to_string(),
        ));
    }
    decode_search_snapshot_text(&bytes)
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
            return Err(SkeinError::Storage(format!(
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
                return Err(SkeinError::Storage(format!(
                    "search projection compressed envelope has invalid header line: {line}"
                )));
            }
        }
    }
    if codec != Some("zstd") {
        return Err(SkeinError::Storage(
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
    let Some(header_end) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(SkeinError::Storage(
            "search projection compressed envelope missing header terminator".to_string(),
        ));
    };
    let header = std::str::from_utf8(&bytes[..header_end]).map_err(|error| {
        SkeinError::Storage(format!(
            "search projection compressed envelope header is invalid: {error}"
        ))
    })?;
    let payload = &bytes[header_end + 2..];
    let SnapshotHeader {
        compressed_len,
        compressed_checksum,
        uncompressed_len,
        uncompressed_checksum,
    } = parse_snapshot_header(header)?;
    let expected_compressed_len = compressed_len.ok_or_else(|| {
        SkeinError::Storage(
            "search projection compressed envelope missing compressed_len".to_string(),
        )
    })?;
    if payload.len() != expected_compressed_len {
        return Err(SkeinError::Storage(format!(
            "search projection compressed length mismatch: expected {expected_compressed_len}, got {}",
            payload.len()
        )));
    }
    let expected_compressed_checksum = compressed_checksum.ok_or_else(|| {
        SkeinError::Storage(
            "search projection compressed envelope missing compressed_checksum".to_string(),
        )
    })?;
    let actual_compressed_checksum = checksum_bytes(payload);
    if actual_compressed_checksum != expected_compressed_checksum {
        return Err(SkeinError::Storage(format!(
            "search projection compressed checksum mismatch: expected {expected_compressed_checksum}, got {actual_compressed_checksum}"
        )));
    }
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        SkeinError::Storage(
            "search projection compressed envelope missing uncompressed_len".to_string(),
        )
    })?;
    if expected_uncompressed_len as u64 > max_uncompressed_bytes {
        return Err(SkeinError::Storage(format!(
            "search projection uncompressed payload requires {expected_uncompressed_len} bytes, exceeding {max_uncompressed_bytes}"
        )));
    }
    let decoder = zstd::stream::read::Decoder::new(Cursor::new(payload)).map_err(|error| {
        SkeinError::Storage(format!(
            "search projection zstd decompression failed: {error}"
        ))
    })?;
    let mut decoded = Vec::with_capacity(expected_uncompressed_len.min(1024 * 1024));
    // The declaration has already passed reader admission. Probe one byte past
    // it to reject understated lengths without inflating up to the reader limit.
    decoder
        .take((expected_uncompressed_len as u64).saturating_add(1))
        .read_to_end(&mut decoded)
        .map_err(|error| {
            SkeinError::Storage(format!(
                "search projection zstd decompression failed: {error}"
            ))
        })?;
    #[cfg(test)]
    compression_tests::record_decoded_bytes(decoded.len());
    if decoded.len() as u64 > max_uncompressed_bytes {
        return Err(SkeinError::Storage(format!(
            "search projection decompressed payload exceeded {max_uncompressed_bytes} bytes"
        )));
    }
    if decoded.len() != expected_uncompressed_len {
        return Err(SkeinError::Storage(format!(
            "search projection uncompressed length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        )));
    }
    let expected_uncompressed_checksum = uncompressed_checksum.ok_or_else(|| {
        SkeinError::Storage(
            "search projection compressed envelope missing uncompressed_checksum".to_string(),
        )
    })?;
    let actual_uncompressed_checksum = checksum_bytes(&decoded);
    if actual_uncompressed_checksum != expected_uncompressed_checksum {
        return Err(SkeinError::Storage(format!(
            "search projection uncompressed checksum mismatch: expected {expected_uncompressed_checksum}, got {actual_uncompressed_checksum}"
        )));
    }
    String::from_utf8(decoded).map_err(|error| {
        SkeinError::Storage(format!(
            "search projection decompressed payload is not valid UTF-8: {error}"
        ))
    })
}

fn checksum_bytes(bytes: &[u8]) -> u64 {
    checksum_u64(bytes)
}

fn elapsed_micros(started: std::time::Instant) -> u64 {
    started.elapsed().as_micros().min(u64::MAX as u128) as u64
}

fn parse_u64(input: &str, name: &str) -> Result<u64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_i64(input: &str, name: &str) -> Result<i64> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

fn parse_usize(input: &str, name: &str) -> Result<usize> {
    input
        .parse()
        .map_err(|_| SkeinError::Storage(format!("invalid {name}: {input}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    mod feature_contract;
    #[cfg(feature = "full-text-search")]
    use skein_storage::{FileSegmentRangeReader, SegmentReadExecutor, SegmentReadScheduler};
    use std::cell::Cell;
    #[cfg(feature = "full-text-search")]
    use std::num::{NonZeroU64, NonZeroUsize};

    #[derive(Default)]
    struct TestProjectionSource {
        nodes: Vec<NodeRecord>,
        commit_epoch: u64,
        business_labels: HashMap<NodeId, Vec<String>>,
        business_label_bulk_scans: Cell<usize>,
        business_label_point_lookups: Cell<usize>,
    }

    impl TestProjectionSource {
        fn in_memory() -> Self {
            Self::default()
        }

        fn create_node(
            &mut self,
            catalog: &mut Catalog,
            label: &str,
            properties: BTreeMap<String, Value>,
        ) -> Result<NodeId> {
            let id = NodeId(self.nodes.len() as u64);
            self.nodes.push(NodeRecord {
                id,
                labels: [catalog.get_or_create_label(label)].into_iter().collect(),
                properties,
            });
            self.commit_epoch = self.commit_epoch.saturating_add(1);
            Ok(id)
        }

        fn node(&self, id: NodeId) -> Option<&NodeRecord> {
            self.nodes.get(id.0 as usize)
        }

        #[cfg(feature = "full-text-search")]
        fn commit_epoch(&self) -> u64 {
            self.commit_epoch
        }
    }

    impl SearchProjectionSource for TestProjectionSource {
        fn source_graph_commit_epoch(&self) -> u64 {
            self.commit_epoch
        }

        fn estimated_projection_node_count(&self) -> usize {
            self.nodes.len()
        }

        fn visit_projection_nodes(
            &self,
            visitor: &mut dyn FnMut(NodeRecord) -> Result<()>,
        ) -> Result<()> {
            for node in &self.nodes {
                visitor(node.clone())?;
            }
            Ok(())
        }

        fn projection_business_labels(
            &self,
            _catalog: &Catalog,
            node: &NodeRecord,
        ) -> Result<Vec<String>> {
            self.business_label_point_lookups
                .set(self.business_label_point_lookups.get().saturating_add(1));
            Ok(self
                .business_labels
                .get(&node.id)
                .cloned()
                .unwrap_or_default())
        }

        fn projection_business_labels_by_node(
            &self,
            _catalog: &Catalog,
        ) -> Result<HashMap<NodeId, Vec<String>>> {
            self.business_label_bulk_scans
                .set(self.business_label_bulk_scans.get().saturating_add(1));
            Ok(self.business_labels.clone())
        }
    }

    #[test]
    fn cosine_similarity_preserves_edge_and_numeric_behavior() {
        assert_eq!(cosine_similarity(&[], &[]), None);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), None);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]), None);

        let identical = cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0])
            .expect("identical non-zero vectors have a cosine score");
        assert!((identical - 1.0).abs() < 1e-6);

        let orthogonal = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0])
            .expect("orthogonal non-zero vectors have a cosine score");
        assert!(orthogonal.abs() < 1e-6);

        let observed = cosine_similarity(&[1.0, 2.0, 3.0, 4.0, 5.0], &[5.0, 4.0, 3.0, 2.0, 1.0])
            .expect("non-zero vectors have a cosine score");
        let expected = 35.0_f64 / 55.0_f64;
        assert!((observed - expected).abs() < 1e-6);
    }

    #[cfg(feature = "vector-search")]
    fn force_quantized_policy() -> AdaptiveVectorBackendPolicy {
        AdaptiveVectorBackendPolicy {
            flat_scan_max_documents: 0,
            high_filter_selectivity_per_million: u32::MAX,
            flat_scan_memory_budget_bytes: 0,
        }
    }

    #[test]
    fn final_vector_similarity_is_positive_only_even_for_opposite_vectors() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]), Some(0.0));
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), Some(0.0));
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), Some(1.0));
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn facade_vector_hits_exclude_nonpositive_scores_for_both_backends() {
        let path = unique_test_dir("positive-vector-contract");
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, vector) in [
            ("positive", [1.0, 0.0]),
            ("zero", [0.0, 1.0]),
            ("negative", [-1.0, 0.0]),
        ] {
            index.upsert(doc(id, "vector", "contract", vector)).unwrap();
        }
        index.checkpoint().unwrap();
        let query = [1.0, 0.0];
        let options = SearchQueryOptions {
            limit: 3,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        };
        let resident =
            index.search_with_options("", Some(&query), SearchMode::Vector, options.clone());
        assert_eq!(
            resident
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            vec!["positive"]
        );
        let out_of_core = SearchOutOfCoreReader::open(&path).unwrap();
        let persisted = out_of_core
            .search_with_options("", Some(&query), SearchMode::Vector, options)
            .unwrap();
        assert_eq!(
            persisted
                .result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            vec!["positive"]
        );
        drop(out_of_core);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn memory_projection_materializes_only_registered_nested_metadata_paths() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        let node_id = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("memory-metadata".to_string())),
                    (
                        "metadata".to_string(),
                        Value::String(
                            r#"{"topic":"Graph","customer":{"tier":"Enterprise"},"ignored":{"deep":"value"}}"#
                                .to_string(),
                        ),
                    ),
                ]),
            )
            .unwrap();
        let row = projection_row_from_node(&catalog, store.node(node_id).unwrap()).unwrap();

        assert_eq!(
            row.metadata.get("metadata.topic"),
            Some(&"Graph".to_string())
        );
        assert_eq!(
            row.metadata.get("metadata.customer.tier"),
            Some(&"Enterprise".to_string())
        );
        assert!(!row.metadata.contains_key("metadata.ignored.deep"));
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn hybrid_search_combines_vector_and_text() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        index
            .upsert(doc("b", "Embedding model", "Vector search", [0.0, 1.0]))
            .unwrap();

        let hits = index.search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits[0].id, "a");
        assert!(hits[0].vector_score > 0.99);
        assert!(hits[0].text_score > 0.0);
        assert_eq!(hits[0].vector_rank, Some(1));
        assert_eq!(hits[0].text_rank, Some(1));
        assert_eq!(hits[0].score, hits[0].rrf_score);
        assert!(hits[0].rrf_score > 0.0);
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn hybrid_search_uses_rrf_child_ranks() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "both",
                "Graph retrieval",
                "Hybrid ranking evidence",
                [1.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "vector_only",
                "Embedding",
                "Vector candidate",
                [0.9, 0.1],
            ))
            .unwrap();
        index
            .upsert(doc(
                "text_only",
                "Graph retrieval",
                "Graph graph",
                [0.0, 1.0],
            ))
            .unwrap();

        let hits = index.search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits[0].id, "both");
        assert_eq!(hits[0].vector_rank, Some(1));
        assert_eq!(hits[0].text_rank, Some(2));
        assert!(hits[0].rrf_score > hits[1].rrf_score);
        assert!(hits.iter().any(|hit| hit.vector_rank.is_some()));
        assert!(hits.iter().any(|hit| hit.text_rank.is_some()));
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn search_report_exposes_child_retriever_summary() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("both", "Graph retrieval", "Hybrid", [1.0, 0.0]))
            .unwrap();
        index
            .upsert(doc("vector_only", "Embedding", "Vector", [0.9, 0.1]))
            .unwrap();
        index
            .upsert(doc(
                "text_only",
                "Graph retrieval",
                "Graph graph",
                [0.0, 1.0],
            ))
            .unwrap();

        let result = index.search_with_report("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 2);

        let vector = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "vector")
            .expect("expected vector retriever report");
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert!(vector.available);
        assert!(text.available);
        assert_eq!(vector.backend, "scalar_vector_scan");
        assert_eq!(text.backend, "bm25_text");
        assert_eq!(vector.candidate_score_source, "raw_vector");
        assert_eq!(vector.final_score_source, "raw_vector");
        assert_eq!(vector.generated_candidate_count, 3);
        assert_eq!(vector.candidate_scan_rounds, 1);
        assert_eq!(vector.descriptor_pruned_count, 0);
        assert_eq!(vector.scalar_filtered_count, 0);
        assert_eq!(vector.reranked_candidate_count, 3);
        assert_eq!(vector.raw_vector_bytes_read, 48);
        assert_eq!(vector.index_covered_document_count, 3);
        assert_eq!(vector.index_candidate_document_count, 3);
        assert!(vector.index_coverage_complete);
        assert_eq!(vector.input_candidate_set, result.candidate_set);
        assert_eq!(text.input_candidate_set, result.candidate_set);
        assert_eq!(
            vector.input_candidate_set.representation,
            "sorted_document_ids"
        );
        assert_eq!(vector.input_candidate_set.cardinality, 3);
        assert_eq!(vector.input_candidate_set.filtered_out_count, 0);
        assert_eq!(vector.candidate_count, 2);
        assert_eq!(text.candidate_count, 2);
        assert_eq!(
            vector.candidate_set.id_space,
            "search_projection_document_id"
        );
        assert_eq!(vector.candidate_set.representation, "ranked_document_ids");
        assert_eq!(vector.candidate_set.cardinality, 2);
        assert!(vector.candidate_set.exact);
        assert_eq!(vector.candidate_set.policy_epoch, None);
        assert_eq!(
            vector.candidate_set.snapshot_source_graph_commit_epoch,
            None
        );
        assert_eq!(text.candidate_set, vector.candidate_set);
        assert_eq!(vector.top_hit_ids[0], "both");
        assert_eq!(vector.top_candidates[0].id, "both");
        assert_eq!(vector.top_candidates[0].rank, 1);
        assert!(vector.top_candidates[0].score > 0.99);
        assert_eq!(text.top_hit_ids.len(), 2);
        assert_eq!(text.top_candidates.len(), 2);
        assert_eq!(text.top_candidates[0].rank, 1);
        assert!(text.top_candidates[0].score >= text.top_candidates[1].score);
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn hybrid_search_rank_window_limits_rrf_candidates() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "top_vector",
                "Vector only",
                "Embedding candidate",
                [1.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "top_text",
                "Graph graph",
                "Text only graph",
                [0.0, 1.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "second_text",
                "Graph",
                "Secondary graph evidence",
                [0.0, 0.9],
            ))
            .unwrap();

        let result = index.search_with_options(
            "graph",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: Some(1),
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );

        assert_eq!(result.rank_window, Some(1));
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert_eq!(text.candidate_count, 2);
        assert_eq!(text.candidate_set.cardinality, 1);
        assert_eq!(text.top_candidates.len(), 1);
        assert_eq!(text.top_candidates[0].rank, 1);
        assert!(result
            .hits
            .iter()
            .all(|hit| hit.id != "second_text" || hit.text_rank.is_none()));
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn rank_window_uses_prefiltered_candidate_set() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:hidden_vector".to_string(),
                title: "Hidden vector".to_string(),
                content: "hidden graph".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: BTreeMap::from([("scope".to_string(), "hidden".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:visible_vector".to_string(),
                title: "Visible vector".to_string(),
                content: "visible graph".to_string(),
                embedding: Some(vec![0.9, 0.0]),
                metadata: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:visible_text".to_string(),
                title: "Visible graph".to_string(),
                content: "visible graph graph".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                metadata: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "visible graph",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: Some(1),
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.candidate_set.cardinality, 2);
        assert_eq!(result.candidate_set.filtered_out_count, 1);
        let vector = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "vector")
            .expect("expected vector retriever report");
        assert_eq!(vector.input_candidate_set.cardinality, 2);
        assert_eq!(vector.input_candidate_set.filtered_out_count, 1);
        assert_eq!(
            vector.input_candidate_set.metadata_filters,
            BTreeMap::from([("scope".to_string(), "visible".to_string())])
        );
        assert!(result
            .hits
            .iter()
            .all(|hit| hit.id != "memory:hidden_vector"));
        let visible_vector = result
            .hits
            .iter()
            .find(|hit| hit.id == "memory:visible_vector")
            .expect("visible vector should remain eligible");
        assert_eq!(visible_vector.vector_rank, Some(1));
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn hybrid_search_applies_child_fusion_weights() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "vector_top",
                "Vector only",
                "semantic evidence",
                [1.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "text_top",
                "Graph retrieval",
                "graph retrieval graph retrieval",
                [0.0, 1.0],
            ))
            .unwrap();

        let text_weighted = index.search_with_options(
            "graph retrieval",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights {
                    vector_weight: 1.0,
                    text_weight: 3.0,
                },
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );
        let vector_weighted = index.search_with_options(
            "graph retrieval",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights {
                    vector_weight: 3.0,
                    text_weight: 1.0,
                },
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );

        assert_eq!(
            text_weighted.fusion_weights,
            SearchFusionWeights {
                vector_weight: 1.0,
                text_weight: 3.0
            }
        );
        assert_eq!(text_weighted.hits[0].id, "text_top");
        assert!(text_weighted.hits[0].text_rrf_score > 0.0);
        assert_eq!(text_weighted.hits[0].vector_rrf_score, 0.0);
        assert_eq!(vector_weighted.hits[0].id, "vector_top");
        assert!(vector_weighted.hits[0].vector_rrf_score > 0.0);
        assert_eq!(vector_weighted.hits[0].text_rrf_score, 0.0);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_with_options_applies_metadata_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:thread_1".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("source_id".to_string(), "thread_1".to_string()),
                ]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_0".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("source_id".to_string(), "thread_2".to_string()),
                ]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_2".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("source_id".to_string(), "thread_2".to_string()),
                ]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.document_count, 3);
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(
            result.candidate_set.id_space,
            "search_projection_document_id"
        );
        assert_eq!(result.candidate_set.representation, "sorted_document_ids");
        assert_eq!(result.candidate_set.cardinality, 1);
        assert!(result.candidate_set.exact);
        assert_eq!(
            result.candidate_set.snapshot_source_graph_commit_epoch,
            None
        );
        assert_eq!(result.candidate_set.policy_epoch, None);
        assert_eq!(result.candidate_set.filtered_out_count, 2);
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            1
        );
        assert_eq!(
            result.candidate_set.metadata_filters,
            BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
        );
        assert_eq!(result.hits[0].id, "memory:thread_1");
        assert_eq!(result.hits[0].source_id.as_deref(), Some("thread_1"));
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert_eq!(text.candidate_count, 1);
        assert_eq!(text.candidate_set.cardinality, 1);
        assert_eq!(text.top_hit_ids, vec!["memory:thread_1".to_string()]);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_metadata_filters_match_casefolded_values() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:acme".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("customer".to_string(), " Acme ".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:other".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("customer".to_string(), "Other".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("customer".to_string(), "acme".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:acme");
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.candidate_set.filtered_out_count, 1);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_with_options_applies_metadata_in_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:fact".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:task".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("unit_type".to_string(), "task".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "unit_type__in".to_string(),
                    r#"["fact","learning"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.hits[0].id, "memory:fact");
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .input_predicate_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pushed_predicate_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .residual_predicate_count,
            0
        );
        assert!(
            !result
                .candidate_set
                .metadata_predicate_pushdown
                .unsatisfiable
        );
        assert!(result
            .candidate_set
            .metadata_predicate_pushdown
            .parse_error
            .is_none());
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "unit_type".to_string(),
                value_kind: "enum".to_string(),
                operation_kinds: vec!["in".to_string()],
                segment_count: 1,
                pruned_segment_count: 0,
                scanned_segment_count: 1,
                numeric_range_summary_used: false,
                timestamp_range_summary_used: false,
                value_summary_used: true,
            }]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_with_options_applies_metadata_not_in_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:active".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("lifecycle_state".to_string(), "active".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:deleted".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("lifecycle_state".to_string(), "deleted".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:legacy".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        );
        let hit_ids = result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(result.filtered_document_count, 2);
        assert!(hit_ids.contains("memory:active"));
        assert!(hit_ids.contains("memory:legacy"));
        assert!(!hit_ids.contains("memory:deleted"));
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries[0]
                .value_kind,
            "enum"
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_with_options_applies_metadata_exists_and_missing_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        for (id, source_id) in [
            ("memory:has_source", Some("thread_1")),
            ("memory:missing_source_1", None),
            ("memory:missing_source_2", None),
        ] {
            let mut metadata = BTreeMap::from([("kind".to_string(), "memory".to_string())]);
            if let Some(source_id) = source_id {
                metadata.insert("source_id".to_string(), source_id.to_string());
            }
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "presence predicate retrieval".to_string(),
                    embedding: None,
                    metadata,
                })
                .unwrap();
        }

        let exists = index.search_with_options(
            "presence predicate retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__exists".to_string(),
                    "true".to_string(),
                )]),
                policy_epoch: None,
            },
        );
        let missing = index.search_with_options(
            "presence predicate retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__missing".to_string(),
                    "true".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(exists.total_hits, 1);
        assert_eq!(exists.hits[0].id, "memory:has_source");
        assert_eq!(exists.filtered_document_count, 1);
        assert_eq!(
            exists
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "source_id".to_string(),
                value_kind: "presence".to_string(),
                operation_kinds: vec!["exists".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: false,
                timestamp_range_summary_used: false,
                value_summary_used: true,
            }]
        );

        let missing_ids = missing
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(missing.total_hits, 2);
        assert!(missing_ids.contains("memory:missing_source_1"));
        assert!(missing_ids.contains("memory:missing_source_2"));
        assert!(!missing_ids.contains("memory:has_source"));
        assert_eq!(
            missing
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries[0]
                .operation_kinds,
            vec!["missing".to_string()]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_with_options_applies_numeric_range_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        for (id, created_at) in [
            ("memory:0_old_0", "1"),
            ("memory:0_old_1", "2"),
            ("memory:1_new_0", "10"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "graph projection diagnostics".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("created_at".to_string(), created_at.to_string())]),
                })
                .unwrap();
        }

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("created_at__gt".to_string(), "5".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.hits[0].id, "memory:1_new_0");
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_pruning_candidate_document_count,
            3
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_pruned_document_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_scanned_document_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "created_at".to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["gt".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: true,
                timestamp_range_summary_used: false,
                value_summary_used: false,
            }]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn malformed_numeric_range_filter_fails_closed() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:active".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("created_at".to_string(), "10".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "not-a-number".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 0);
        assert_eq!(result.filtered_document_count, 0);
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            0
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn malformed_metadata_list_filter_fails_closed() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:active".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("lifecycle_state".to_string(), "active".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    "deleted,forgotten".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 0);
        assert_eq!(result.filtered_document_count, 0);
        assert_eq!(result.candidate_set.filtered_out_count, 1);
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .input_predicate_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pushed_predicate_count,
            0
        );
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .unsatisfiable
        );
        assert!(result
            .candidate_set
            .metadata_predicate_pushdown
            .parse_error
            .as_deref()
            .is_some_and(|error| error.contains("expected JSON string array")));
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            0
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_policy_epoch_when_supplied() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:thread_1".to_string(),
                title: "Policy scoped graph".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: Some(42),
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.candidate_set.policy_epoch, Some(42));
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert_eq!(text.candidate_set.policy_epoch, Some(42));
        assert_eq!(
            index
                .search_with_report("graph", None, SearchMode::Text, 10)
                .candidate_set
                .policy_epoch,
            None
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_kind_metadata_filter_accepts_canonical_labels() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Kind scoped graph".to_string(),
                content: "projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "memory".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "entity:entity_1".to_string(),
                title: "Kind scoped graph".to_string(),
                content: "projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "entity".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "projection diagnostics",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("kind".to_string(), "Memory".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.hits[0].id, "memory:mem_1");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_kind_metadata_filter_accepts_source_chunk_variants() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "source_chunk:chunk_1".to_string(),
                title: "Chunk scoped graph".to_string(),
                content: "projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "source_chunk".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "projection diagnostics",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("kind".to_string(), "SourceChunk".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "source_chunk:chunk_1");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_metadata_filters_normalize_default_space() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:missing_space".to_string(),
                title: "Scoped graph".to_string(),
                content: "default space retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:empty_space".to_string(),
                title: "Scoped graph".to_string(),
                content: "default space retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), String::new())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:team_space".to_string(),
                title: "Scoped graph".to_string(),
                content: "default space retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "default space retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    DEFAULT_SPACE_ID.to_string(),
                )]),
                policy_epoch: None,
            },
        );
        let hit_ids = result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(result.filtered_document_count, 2);
        assert!(hit_ids.contains("memory:missing_space"));
        assert!(hit_ids.contains("memory:empty_space"));
        assert!(!hit_ids.contains("memory:team_space"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_metadata_filter_empty_scope() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:thread_1".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "missing_thread".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.document_count, 1);
        assert_eq!(result.filtered_document_count, 0);
        assert_eq!(result.total_hits, 0);
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert_eq!(text.candidate_count, 0);
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn vector_dimension_mismatch_degrades_to_text() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();

        let result =
            index.search_with_report("graph", Some(&[1.0, 0.0, 0.0]), SearchMode::Hybrid, 10);
        let hits = &result.hits;

        assert_eq!(hits[0].id, "a");
        assert_eq!(hits[0].vector_score, 0.0);
        assert!(hits[0].text_score > 0.0);
        assert_eq!(hits[0].vector_rank, None);
        assert_eq!(hits[0].text_rank, Some(1));
        assert!(result.fallback_reasons[0].contains("dimension"));
        assert!(result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
        assert!(hits[0].fallback_reasons[0].contains("dimension"));
        assert!(hits[0]
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
        let vector = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "vector")
            .expect("expected vector retriever report");
        assert!(!vector.available);
        assert!(vector.fallback_reasons[0].contains("dimension"));
        assert!(vector
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert!(text.fallback_reason_codes.is_empty());
        assert!(text.fallback_reasons.is_empty());
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn search_report_exposes_global_fallback_reasons_without_hits() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();

        let result = index.search_with_report("", Some(&[1.0, 0.0, 0.0]), SearchMode::Vector, 10);

        assert!(result.hits.is_empty());
        assert!(result
            .fallback_reasons
            .iter()
            .any(|reason| reason.contains("query embedding dimension 3")));
        assert!(result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
        assert!(result
            .empty_reasons
            .iter()
            .any(|reason| reason.contains("query embedding dimension 3")));
    }

    #[test]
    fn search_fallback_reason_codes_have_stable_string_encodings() {
        let cases = [
            (
                SearchFallbackReasonCode::VectorDimensionMismatch,
                "vector_dimension_mismatch",
            ),
            (
                SearchFallbackReasonCode::VectorIndexEmpty,
                "vector_index_empty",
            ),
            (
                SearchFallbackReasonCode::CompressedVectorProjectionUnavailable,
                "compressed_vector_projection_unavailable",
            ),
            (
                SearchFallbackReasonCode::QueryEmbeddingMissing,
                "query_embedding_missing",
            ),
            (SearchFallbackReasonCode::TextQueryEmpty, "text_query_empty"),
        ];

        for (code, name) in cases {
            assert_eq!(code.as_str(), name);
            assert_eq!(name.parse::<SearchFallbackReasonCode>(), Ok(code));
        }
        assert!("fallback_unknown"
            .parse::<SearchFallbackReasonCode>()
            .is_err());
    }

    #[test]
    fn search_truncation_reason_codes_have_stable_string_encodings() {
        assert_eq!(
            SearchTruncationReasonCode::LimitExceeded.as_str(),
            "limit_exceeded"
        );
        assert_eq!(
            "limit_exceeded".parse::<SearchTruncationReasonCode>(),
            Ok(SearchTruncationReasonCode::LimitExceeded)
        );
        assert!("rank_window_exceeded"
            .parse::<SearchTruncationReasonCode>()
            .is_err());
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn search_report_exposes_missing_query_embedding_reason() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();

        let result = index.search_with_report("graph", None, SearchMode::Vector, 10);

        assert!(result.hits.is_empty());
        assert!(result
            .fallback_reasons
            .iter()
            .any(|reason| reason == "query embedding not provided"));
        assert!(result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::QueryEmbeddingMissing));
        assert!(result
            .empty_reasons
            .iter()
            .any(|reason| reason == "query embedding not provided"));
        let vector = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "vector")
            .expect("expected vector retriever report");
        assert!(!vector.available);
        assert!(vector
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::QueryEmbeddingMissing));
        assert!(vector
            .fallback_reasons
            .iter()
            .any(|reason| reason == "query embedding not provided"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn text_search_uses_bm25_term_frequency_and_length_normalization() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "focused".to_string(),
                title: "Graph graph storage".to_string(),
                content: "graph wal".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "verbose".to_string(),
                title: "Graph storage".to_string(),
                content: "graph runtime parser optimizer storage checkpoint wal manifest"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let hits = index.search("graph storage", None, SearchMode::Text, 10);

        assert_eq!(hits[0].id, "focused");
        assert!(hits[0].text_score > hits[1].text_score);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn text_search_weights_title_terms_twice() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "title_match".to_string(),
                title: "graph".to_string(),
                content: "neutral".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "content_match".to_string(),
                title: "neutral".to_string(),
                content: "graph".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let hits = index.search("graph", None, SearchMode::Text, 10);

        assert_eq!(hits[0].id, "title_match");
        assert_eq!(hits[1].id, "content_match");
        assert!(hits[0].text_score > hits[1].text_score);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_keeps_nowledge_style_identifiers_case_insensitive() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory".to_string(),
                title: "Nowledge_Source".to_string(),
                content: "Graph-storage, graph storage.".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let underscore_hits = index.search("nowledge_source", None, SearchMode::Text, 10);
        let punctuation_hits = index.search("GRAPH storage", None, SearchMode::Text, 10);

        assert_eq!(underscore_hits[0].id, "memory");
        assert_eq!(punctuation_hits[0].id, "memory");
    }

    #[test]
    fn tokenizer_preserves_local_dedup_and_cross_identifier_frequency() {
        let lexicon = SearchAnalyzerLexicon::empty();

        assert_eq!(
            identifier_tokens("Running_running", &lexicon),
            vec!["running_running", "running", "run"]
        );
        assert_eq!(
            tokenize_list("Graph Graph Graph", &lexicon),
            vec!["graph", "graph_graph", "graph", "graph"]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_matches_camel_snake_kebab_and_path_identifiers() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "chunk".to_string(),
                title: "SourceChunk nowledge_source".to_string(),
                content: "artifact-source/chunk parserV2".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let camel_hits = index.search("source chunk", None, SearchMode::Text, 10);
        let snake_hits = index.search("source_chunk", None, SearchMode::Text, 10);
        let kebab_hits = index.search("artifact source", None, SearchMode::Text, 10);
        let version_hits = index.search("parser v2", None, SearchMode::Text, 10);

        assert_eq!(camel_hits[0].id, "chunk");
        assert_eq!(snake_hits[0].id, "chunk");
        assert_eq!(kebab_hits[0].id, "chunk");
        assert_eq!(version_hits[0].id, "chunk");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_matches_cjk_subterms_with_ngrams() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "design".to_string(),
                title: "自研图数据库设计".to_string(),
                content: "稳定逻辑ID和可重建投影".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let graph_hits = index.search_with_report("图数据库", None, SearchMode::Text, 10);
        let projection_hits = index.search("重建投影", None, SearchMode::Text, 10);

        assert_eq!(graph_hits.hits[0].id, "design");
        assert!(graph_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "数据库"));
        assert_eq!(projection_hits[0].id, "design");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_emits_dictionary_backed_chinese_search_terms() {
        let mut index = SearchIndex::in_memory();
        let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
        index
            .upsert(SearchDocument {
                id: "distributed".to_string(),
                title: "\u{73b0}\u{4ee3}\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}".to_string(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let output = index.search_with_report(term, None, SearchMode::Text, 10);

        assert_eq!(output.hits[0].id, "distributed");
        assert!(output.hits[0]
            .matched_terms
            .iter()
            .any(|token| token == term));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_splits_acronym_titlecase_boundaries() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "runtime".to_string(),
                title: "LSMTree HTTPServer GraphQLParser".to_string(),
                content: "WALCheckpoint handles MVCCSnapshot readers".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let lsm_hits = index.search("lsm tree", None, SearchMode::Text, 10);
        let http_hits = index.search("http server", None, SearchMode::Text, 10);
        let graphql_hits = index.search("graphql parser", None, SearchMode::Text, 10);
        let checkpoint_hits = index.search("wal checkpoint", None, SearchMode::Text, 10);
        let mvcc_hits = index.search_with_report("mvcc snapshot", None, SearchMode::Text, 10);

        assert_eq!(lsm_hits[0].id, "runtime");
        assert_eq!(http_hits[0].id, "runtime");
        assert_eq!(graphql_hits[0].id, "runtime");
        assert_eq!(checkpoint_hits[0].id, "runtime");
        assert_eq!(mvcc_hits.hits[0].id, "runtime");
        assert!(mvcc_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "mvcc"));
        assert!(mvcc_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "snapshot"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_normalizes_common_english_suffixes() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory".to_string(),
                title: "Memories linked sources".to_string(),
                content: "threading normalized archived chunks".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let memory_hits = index.search("memory", None, SearchMode::Text, 10);
        let source_hits = index.search("source", None, SearchMode::Text, 10);
        let thread_hits = index.search("thread", None, SearchMode::Text, 10);
        let archive_hits = index.search("archive", None, SearchMode::Text, 10);
        let chunk_hits = index.search("chunk", None, SearchMode::Text, 10);

        assert_eq!(memory_hits[0].id, "memory");
        assert_eq!(source_hits[0].id, "memory");
        assert_eq!(thread_hits[0].id, "memory");
        assert_eq!(archive_hits[0].id, "memory");
        assert_eq!(chunk_hits[0].id, "memory");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_knowledge_retrieval_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "graph-rag".to_string(),
                title: "GraphRAG over knowledge graph evidence".to_string(),
                content: "retrieval augmented generation with bounded graph expansion".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let graph_retrieval_hits = index.search("graph retrieval", None, SearchMode::Text, 10);
        let rag_hits = index.search("rag", None, SearchMode::Text, 10);
        let expanded_hits =
            index.search("retrieval augmented generation", None, SearchMode::Text, 10);
        let kg_hits = index.search("kg", None, SearchMode::Text, 10);
        let knowledge_graph_hits = index.search("knowledge graph", None, SearchMode::Text, 10);

        assert_eq!(graph_retrieval_hits[0].id, "graph-rag");
        assert_eq!(rag_hits[0].id, "graph-rag");
        assert_eq!(expanded_hits[0].id, "graph-rag");
        assert_eq!(kg_hits[0].id, "graph-rag");
        assert_eq!(knowledge_graph_hits[0].id, "graph-rag");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_nowledge_memory_lifecycle_aliases() {
        let mut index = SearchIndex::in_memory()
            .with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
        index
            .upsert(SearchDocument {
                id: "memory-lifecycle".to_string(),
                title: "Crystal memory keeps SYNTHESIZED_FROM evidence".to_string(),
                content: "Episodic provenance preserves raw Thread and SourceChunk records"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let crystallization_hits = index.search("crystallization", None, SearchMode::Text, 10);
        let synthesized_hits = index.search("synthesized memory", None, SearchMode::Text, 10);
        let raw_evidence_hits =
            index.search_with_report("raw evidence", None, SearchMode::Text, 10);

        assert_eq!(crystallization_hits[0].id, "memory-lifecycle");
        assert_eq!(synthesized_hits[0].id, "memory-lifecycle");
        assert_eq!(raw_evidence_hits.hits[0].id, "memory-lifecycle");
        assert!(raw_evidence_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "raw_evidence"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_keeps_nowledge_application_aliases_out_of_default_lexicon() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory-lifecycle".to_string(),
                title: "Crystal memory".to_string(),
                content: "SYNTHESIZED_FROM evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let hits = index.search("crystallization", None, SearchMode::Text, 10);

        assert!(hits.is_empty());
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_nowledge_schema_relationship_aliases() {
        let mut index = SearchIndex::in_memory()
            .with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
        index
            .upsert(SearchDocument {
                id: "schema-relationships".to_string(),
                title: "SOURCED_FROM MENTIONS EVOLVES ai_summary".to_string(),
                content: "Community summaries link source provenance and memory evolution"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let source_hits = index.search("source provenance", None, SearchMode::Text, 10);
        let mention_hits = index.search("entity mention", None, SearchMode::Text, 10);
        let evolution_hits = index.search("memory evolution", None, SearchMode::Text, 10);
        let summary_hits =
            index.search_with_report("community summary", None, SearchMode::Text, 10);

        assert_eq!(source_hits[0].id, "schema-relationships");
        assert_eq!(mention_hits[0].id, "schema-relationships");
        assert_eq!(evolution_hits[0].id, "schema-relationships");
        assert_eq!(summary_hits.hits[0].id, "schema-relationships");
        assert!(summary_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "community_summary"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_nowledge_memory_relation_product_aliases() {
        let mut index = SearchIndex::in_memory()
            .with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
        index
            .upsert(SearchDocument {
                id: "memory-relations".to_string(),
                title: "MEMORY_RELATES_TO relation_type same_topic".to_string(),
                content: "Internal schema rows model semantic relation edges between memories"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let product_hits =
            index.search_with_report("memory-to-memory relationships", None, SearchMode::Text, 10);
        let schema_hits = index.search_with_report("relation_type", None, SearchMode::Text, 10);

        assert_eq!(product_hits.hits[0].id, "memory-relations");
        assert!(product_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "memory_relates_to"));
        assert_eq!(schema_hits.hits[0].id, "memory-relations");
        assert!(schema_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "semantic_relation"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_normalizes_application_alias_rules() {
        let mut index = SearchIndex::in_memory().with_analyzer_lexicon(
            SearchAnalyzerLexicon::default()
                .with_normalized_alias_rule(
                    ["raw evidence", "RawEvidence"],
                    ["episodic provenance"],
                )
                .with_normalized_alias_rule(["community summary"], ["aiSummary"]),
        );
        index
            .upsert(SearchDocument {
                id: "readable-aliases".to_string(),
                title: "Episodic provenance keeps aiSummary auditable".to_string(),
                content: "Application lexicons should not expose analyzer token internals"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let raw_evidence_hits =
            index.search_with_report("raw evidence", None, SearchMode::Text, 10);
        let summary_hits =
            index.search_with_report("community summary", None, SearchMode::Text, 10);

        assert_eq!(raw_evidence_hits.hits[0].id, "readable-aliases");
        assert!(raw_evidence_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "episodic_provenance"));
        assert_eq!(summary_hits.hits[0].id, "readable-aliases");
        assert!(summary_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "ai_summary"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_applies_application_stopword_rules() {
        let mut index = SearchIndex::in_memory().with_analyzer_lexicon(
            SearchAnalyzerLexicon::default().with_stopwords(["memory lifecycle", "thread"]),
        );
        index
            .upsert(SearchDocument {
                id: "specific".to_string(),
                title: "MemoryLifecycle WAL checkpoint".to_string(),
                content: "Thread compaction evidence remains auditable".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let hits =
            index.search_with_report("memory lifecycle checkpoint", None, SearchMode::Text, 10);
        let thread_hits = index.search_with_report("thread evidence", None, SearchMode::Text, 10);
        let noisy_hits = index.search("memory lifecycle thread", None, SearchMode::Text, 10);

        assert_eq!(hits.hits[0].id, "specific");
        assert!(hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "checkpoint"));
        assert!(!hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "memory" || term == "lifecycle" || term == "memory_lifecycle"));
        assert_eq!(thread_hits.hits[0].id, "specific");
        assert!(thread_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "evidence"));
        assert!(!thread_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "thread"));
        assert!(noisy_hits.is_empty());
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_database_system_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "runtime".to_string(),
                title: "LSMTree WALCheckpoint MVCCSnapshot CypherPlanner".to_string(),
                content: "CSR and CSC projections back graph analytics".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let wal_hits = index.search_with_report("write ahead log", None, SearchMode::Text, 10);
        let mvcc_hits = index.search(
            "multi version concurrency control",
            None,
            SearchMode::Text,
            10,
        );
        let lsm_hits = index.search("log structured merge tree", None, SearchMode::Text, 10);
        let csr_hits = index.search("compressed sparse row", None, SearchMode::Text, 10);
        let csc_hits = index.search("compressed sparse column", None, SearchMode::Text, 10);
        let opencypher_hits = index.search("opencypher", None, SearchMode::Text, 10);

        assert_eq!(wal_hits.hits[0].id, "runtime");
        assert!(wal_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "wal"));
        assert_eq!(mvcc_hits[0].id, "runtime");
        assert_eq!(lsm_hits[0].id, "runtime");
        assert_eq!(csr_hits[0].id, "runtime");
        assert_eq!(csc_hits[0].id, "runtime");
        assert_eq!(opencypher_hits[0].id, "runtime");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_graph_stream_and_projection_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "skein-lightning".to_string(),
                title: "SkeinLightning publishes GraphStream and RelationalStream".to_string(),
                content: "Checkpointed snapshots track projection freshness".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let import_hits = index.search("database import", None, SearchMode::Text, 10);
        let export_hits = index.search("graph export", None, SearchMode::Text, 10);
        let value_hits = index.search("value stream", None, SearchMode::Text, 10);
        let checkpoint_hits =
            index.search_with_report("checkpoint freshness", None, SearchMode::Text, 10);
        let staleness_hits = index.search("projection staleness", None, SearchMode::Text, 10);

        assert_eq!(import_hits[0].id, "skein-lightning");
        assert_eq!(export_hits[0].id, "skein-lightning");
        assert_eq!(value_hits[0].id, "skein-lightning");
        assert_eq!(checkpoint_hits.hits[0].id, "skein-lightning");
        assert_eq!(staleness_hits[0].id, "skein-lightning");
        assert!(checkpoint_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "checkpoint"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_migration_projection_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "projection".to_string(),
                title: "PostgreSQL pgvector FTS LanceDB Kuzu".to_string(),
                content: "Local graph projection replaces Ladybug search adapters".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let postgres_hits = index.search("postgres", None, SearchMode::Text, 10);
        let pg_hits = index.search("pg", None, SearchMode::Text, 10);
        let vector_hits = index.search("vector search", None, SearchMode::Text, 10);
        let fts_hits = index.search_with_report("full text search", None, SearchMode::Text, 10);
        let lance_hits = index.search("lance", None, SearchMode::Text, 10);
        let ladybug_hits = index.search("ladybug", None, SearchMode::Text, 10);

        assert_eq!(postgres_hits[0].id, "projection");
        assert_eq!(pg_hits[0].id, "projection");
        assert_eq!(vector_hits[0].id, "projection");
        assert_eq!(fts_hits.hits[0].id, "projection");
        assert_eq!(lance_hits[0].id, "projection");
        assert_eq!(ladybug_hits[0].id, "projection");
        assert!(fts_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "fts"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_expands_retriever_algorithm_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "retrieval".to_string(),
                title: "RRF hybrid ranking over ANN candidates".to_string(),
                content: "reciprocal rank fusion explains vector and text child retrievers"
                    .to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "facade".to_string(),
                title: "HybridRetrieve facade".to_string(),
                content: "retriever DAG merges search hits and graph seed candidates".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let rrf_hits = index.search("reciprocal rank fusion", None, SearchMode::Text, 10);
        let ann_hits = index.search("approximate nearest neighbor", None, SearchMode::Text, 10);
        let abbreviation_hits = index.search("rrf ann", None, SearchMode::Text, 10);
        let retrieval_hits = index.search("hybrid retrieval", None, SearchMode::Text, 10);
        let search_hits = index.search("hybrid search", None, SearchMode::Text, 10);

        assert_eq!(rrf_hits[0].id, "retrieval");
        assert_eq!(ann_hits[0].id, "retrieval");
        assert_eq!(abbreviation_hits[0].id, "retrieval");
        assert_eq!(retrieval_hits[0].id, "facade");
        assert_eq!(search_hits[0].id, "facade");
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn text_search_indexes_selected_projection_metadata_identifiers() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "metadata-only".to_string(),
                title: "Untitled".to_string(),
                content: "No body match".to_string(),
                embedding: None,
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("external_id".to_string(), "mem_graph_alpha".to_string()),
                    ("source_id".to_string(), "thread_projection_1".to_string()),
                    ("space_id".to_string(), "team_archive".to_string()),
                ]),
            })
            .unwrap();

        let external_id_hits = index.search("mem graph alpha", None, SearchMode::Text, 10);
        let source_id_hits =
            index.search_with_report("thread projection 1", None, SearchMode::Text, 10);
        let space_id_hits = index.search_with_report("team archive", None, SearchMode::Text, 10);

        assert_eq!(external_id_hits[0].id, "metadata-only");
        assert_eq!(source_id_hits.hits[0].id, "metadata-only");
        assert_eq!(space_id_hits.hits[0].id, "metadata-only");
        assert!(source_id_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "thread"));
        assert!(space_id_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "archive"));
        assert!(source_id_hits.hits[0].matched_spans.is_empty());
        assert!(space_id_hits.hits[0].matched_spans.is_empty());
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn tokenizer_filters_stopwords_from_text_scoring_and_matched_terms() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "noise".to_string(),
                title: "The of in".to_string(),
                content: "and to with".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "graph".to_string(),
                title: "Graph memory".to_string(),
                content: "indexed retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let result = index.search_with_report("the graph of memory", None, SearchMode::Text, 10);

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "graph");
        assert_eq!(
            result.hits[0].matched_terms,
            vec!["graph".to_string(), "memory".to_string()]
        );
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert_eq!(text.candidate_count, 1);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_hits_expose_projection_provenance_and_matched_terms() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem_1".to_string(),
                title: "GraphRAG retrieval".to_string(),
                body: "Evidence from source chunks".to_string(),
                embedding: None,
                source_id: Some("thread_1".to_string()),
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            })
            .unwrap();

        let hits = index.search("rag source chunk", None, SearchMode::Text, 10);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:mem_1");
        assert_eq!(hits[0].kind.as_deref(), Some("memory"));
        assert_eq!(hits[0].external_id.as_deref(), Some("mem_1"));
        assert_eq!(hits[0].source_id.as_deref(), Some("thread_1"));
        assert!(hits[0].matched_terms.iter().any(|term| term == "rag"));
        assert!(hits[0].matched_terms.iter().any(|term| term == "source"));
        assert!(hits[0].matched_terms.iter().any(|term| term == "chunk"));
        assert!(hits[0]
            .matched_spans
            .iter()
            .any(|span| span.field == "title" && span.text == "GraphRAG" && span.term == "rag"));
        assert!(hits[0]
            .matched_spans
            .iter()
            .any(|span| span.field == "content" && span.text == "source" && span.term == "source"));
        assert!(hits[0]
            .matched_spans
            .iter()
            .any(|span| span.field == "content" && span.text == "chunks" && span.term == "chunk"));
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn search_hits_expose_projection_freshness() {
        let path = unique_test_dir("search_projection_freshness");
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "text-embedding-3-small".to_string(),
                version: Some("2026-07-15".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem_1".to_string(),
                title: "Graph retrieval".to_string(),
                body: "Freshness aware search projection".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("thread_1".to_string()),
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.mark_full_reindex_needed("stale projection").unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();

        let freshness = index.projection_freshness();
        let hits = index.search(
            "freshness projection",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            10,
        );

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].projection_freshness, freshness);
        assert_eq!(hits[0].projection_freshness.document_count, 1);
        assert!(hits[0].projection_freshness.full_reindex_needed);
        assert_eq!(
            hits[0].projection_freshness.full_reindex_reasons,
            vec!["stale projection".to_string()]
        );
        assert!(hits[0].projection_freshness.metadata_repair_needed);
        assert_eq!(
            hits[0].projection_freshness.metadata_repair_reasons,
            vec!["missing metadata".to_string()]
        );
        assert_eq!(
            hits[0].projection_freshness.embedding_model.as_deref(),
            Some("text-embedding-3-small")
        );
        assert_eq!(
            hits[0].projection_freshness.embedding_version.as_deref(),
            Some("2026-07-15")
        );
        assert_eq!(hits[0].projection_freshness.embedding_dimension, Some(2));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_limit_truncation() {
        let mut index = SearchIndex::in_memory();
        for id in ["a", "b", "c"] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph retrieval".to_string(),
                    content: "projection evidence".to_string(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }

        let result = index.search_with_report("graph", None, SearchMode::Text, 2);

        assert_eq!(result.hits.len(), 2);
        assert_eq!(result.total_hits, 3);
        assert_eq!(result.limit, 2);
        assert!(result.truncated);
        assert_eq!(
            result.truncation_reason_codes,
            vec![SearchTruncationReasonCode::LimitExceeded]
        );
        assert_eq!(result.truncation_reasons.len(), 1);
        assert!(result.truncation_reasons[0].contains("limit 2"));
        assert!(result.truncation_reasons[0].contains("3 matching hits"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_empty_projection_reason() {
        let index = SearchIndex::in_memory();

        let result = index.search_with_report("graph", None, SearchMode::Text, 10);

        assert!(result.hits.is_empty());
        assert_eq!(
            result.empty_reason_codes,
            vec![SearchEmptyReasonCode::ProjectionEmpty]
        );
        assert_eq!(result.empty_reason_codes[0].as_str(), "projection_empty");
        assert_eq!(
            result.empty_reasons,
            vec!["search projection has no documents".to_string()]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_metadata_filter_empty_reason() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "a".to_string(),
                title: "Graph retrieval".to_string(),
                content: "projection evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            })
            .unwrap();

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "archive".to_string())]),
                policy_epoch: None,
            },
        );

        assert!(result.hits.is_empty());
        assert_eq!(
            result.empty_reason_codes,
            vec![SearchEmptyReasonCode::MetadataFilterEmpty]
        );
        assert_eq!(
            result.empty_reasons,
            vec!["metadata filters matched no search documents".to_string()]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_no_matching_rows_empty_reason() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "a".to_string(),
                title: "Graph retrieval".to_string(),
                content: "projection evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let result = index.search_with_report("unmatched needle", None, SearchMode::Text, 10);

        assert!(result.hits.is_empty());
        assert_eq!(
            result.empty_reason_codes,
            vec![SearchEmptyReasonCode::RetrieverNoHits]
        );
        assert_eq!(
            result.empty_reasons,
            vec!["search retrievers returned no hits inside filtered scope".to_string()]
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_empty_text_query_reason() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "a".to_string(),
                title: "Graph retrieval".to_string(),
                content: "projection evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let result = index.search_with_report("", None, SearchMode::Text, 10);

        assert!(result.hits.is_empty());
        assert_eq!(
            result.empty_reason_codes,
            vec![SearchEmptyReasonCode::RetrieverNoHits]
        );
        assert!(result
            .fallback_reasons
            .iter()
            .any(|reason| reason == "query text produced no searchable terms"));
        assert!(result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::TextQueryEmpty));
        assert!(result
            .empty_reasons
            .iter()
            .any(|reason| reason == "query text produced no searchable terms"));
        let text = result
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .expect("expected text retriever report");
        assert!(!text.available);
        assert!(text
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::TextQueryEmpty));
        assert!(text
            .fallback_reasons
            .iter()
            .any(|reason| reason == "query text produced no searchable terms"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_report_exposes_limit_zero_empty_reason() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "a".to_string(),
                title: "Graph retrieval".to_string(),
                content: "projection evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let result = index.search_with_report("graph", None, SearchMode::Text, 0);

        assert!(result.hits.is_empty());
        assert_eq!(result.total_hits, 1);
        assert!(result.truncated);
        assert_eq!(
            result.empty_reason_codes,
            vec![SearchEmptyReasonCode::LimitExcludedAllHits]
        );
        assert_eq!(
            result.empty_reasons,
            vec!["limit 0 returned from 1 matching hits".to_string()]
        );
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn projection_snapshot_round_trips() {
        let path = unique_test_dir("search_snapshot");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
                .unwrap();
            let report = index.checkpoint_with_report().unwrap();
            assert_eq!(report.document_count, 1);
            assert!(report.snapshot_streamed);
            assert!(report.snapshot_uncompressed_bytes > 0);
            assert!(report.snapshot_compressed_bytes > 0);
            assert!(report.snapshot_peak_record_bytes > 0);
            assert!(report.projection_generation > 0);
        }
        {
            let index = SearchIndex::open(&path).unwrap();
            let hits = index.search("adjacency", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);
            assert_eq!(hits[0].id, "a");
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn graph_rebuild_records_source_commit_epoch() {
        let path = unique_test_dir("search_source_graph_epoch");
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
                .unwrap();
            assert_eq!(
                index.projection_freshness().source_graph_commit_epoch,
                Some(store.commit_epoch())
            );
            index.checkpoint().unwrap();
        }
        {
            let index = SearchIndex::open(&path).unwrap();
            assert_eq!(
                index.projection_freshness().source_graph_commit_epoch,
                Some(store.commit_epoch())
            );
            let result = index.search_with_options(
                "graph storage",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            );
            assert_eq!(result.total_hits, 1);
            assert_eq!(
                result.candidate_set.snapshot_source_graph_commit_epoch,
                Some(store.commit_epoch())
            );
            assert_eq!(result.candidate_set.policy_epoch, None);
        }

        let snapshot = read_search_snapshot_text(&path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
        assert!(snapshot.contains("source_graph_commit_epoch\t1\n"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_checkpoint_publishes_snapshot_file() {
        let path = unique_test_dir("search_snapshot_publish");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "text-embedding-3-small".to_string(),
                    version: None,
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
                .unwrap();
            index.checkpoint().unwrap();
        }

        let snapshot_bytes = std::fs::read(path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
        assert!(snapshot_bytes.starts_with(SEARCH_COMPRESSION_HEADER.as_bytes()));
        let snapshot = read_search_snapshot_text(&path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
        assert!(snapshot.contains("SKEIN_SEARCH_PROJECTION_V1\n"));
        assert!(snapshot.contains("embedding_manifest\t"));
        assert!(snapshot.contains("doc\t"));
        assert!(snapshot.contains("checksum\t"));

        let index = SearchIndex::open(&path).unwrap();
        assert_eq!(
            index.embedding_manifest(),
            Some(&SearchEmbeddingManifest {
                model: "text-embedding-3-small".to_string(),
                version: None,
                dimension: 2,
            })
        );
        assert!(index.document("a").is_some());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn rabitq_reopen_preserves_stale_valid_generation() {
        let path = unique_test_dir("rabitq_stale_generation");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Vector A",
                    "Original projection",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index.checkpoint().unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Vector A",
                    "Updated projection",
                    [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index.checkpoint().unwrap();
        }

        let stale = path.join(rabitq_artifact_file(1));
        let stale_bytes = std::fs::read(&stale).unwrap();
        std::fs::remove_file(path.join(rabitq_artifact_file(2))).unwrap();

        for _ in 0..2 {
            let index = SearchIndex::open(&path).unwrap();
            assert!(index.rabitq_projection().is_none());
            assert_eq!(std::fs::read(&stale).unwrap(), stale_bytes);
            assert!(!std::fs::read_dir(&path).unwrap().flatten().any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("search_rabitq.1.skein.corrupt.")
            }));
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn rabitq_reopen_falls_back_to_previous_valid_generation() {
        let path = unique_test_dir("rabitq_generation_fallback");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Vector A",
                    "Generation fallback",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index.checkpoint().unwrap();
            index.checkpoint().unwrap();
        }

        let latest = path.join(rabitq_artifact_file(2));
        let mut bytes = std::fs::read(&latest).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&latest, bytes).unwrap();

        let index = SearchIndex::open(&path).unwrap();
        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: None,
            active_embedding_dimension: Some(8),
        });
        assert_eq!(probe["compressed_vector_projection"]["ready"], true);
        assert_eq!(probe["compressed_vector_projection"]["generation"], 1);
        assert!(!latest.exists());
        assert!(!std::fs::read_dir(&path).unwrap().flatten().any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("search_rabitq.2.skein.corrupt.")
        }));
        assert!(index.projection_cleanup_report().deleted_files >= 1);

        let result = index.search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 1,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        );
        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(
            result.retrievers[0].backend,
            "skein_rabitq_candidate_projection"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn compressed_vector_projection_disabled_uses_scalar_even_when_artifact_exists() {
        let path = unique_test_dir("search_rabitq_disabled_uses_scalar");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Vector A",
                    "Compressed projection",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index.checkpoint().unwrap();
        }

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Disabled,
        );

        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(result.retrievers[0].backend, "scalar_vector_scan");
        assert_eq!(
            result.retrievers[0].backend_selection_reason,
            Some(VectorBackendSelectionReason::CompressionDisabled)
        );
        assert!(result.retrievers[0].fallback_reason_codes.is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn compressed_vector_projection_required_uses_rabitq_projection() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:a",
                "Vector A",
                "Compressed projection",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();

        let result = index.search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        );

        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(
            result.retrievers[0].backend,
            "skein_rabitq_candidate_projection"
        );
        assert_eq!(
            result.retrievers[0].candidate_score_source,
            "quantized_approximate"
        );
        assert_eq!(result.retrievers[0].final_score_source, "raw_vector");
        assert!(result.retrievers[0].raw_vector_bytes_read > 0);
        assert!(result.retrievers[0].candidate_scan_kernel.is_some());
        assert_eq!(result.retrievers[0].candidate_scan_scored_document_count, 1);
        assert!(result.retrievers[0].fallback_reason_codes.is_empty());
        assert!(result.fallback_reason_codes.is_empty());
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn adaptive_vector_backend_uses_filtered_candidate_count_and_recall_probe() {
        let mut index = SearchIndex::in_memory();
        for id in 0..8 {
            let mut document = doc(
                &format!("memory:{id}"),
                "Adaptive vector",
                "Filtered candidate",
                [1.0, 0.0],
            );
            document.metadata.insert(
                "space_id".to_string(),
                if id == 0 { "selected" } else { "other" }.to_string(),
            );
            index.upsert(document).unwrap();
        }
        let options = SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("space_id".to_string(), "selected".to_string())]),
            policy_epoch: None,
        };

        let filtered = index.search_with_options_adaptive_vector_projection(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options.clone(),
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred)
                .with_backend_policy(AdaptiveVectorBackendPolicy {
                    flat_scan_max_documents: 2,
                    high_filter_selectivity_per_million: u32::MAX,
                    flat_scan_memory_budget_bytes: 0,
                }),
        );
        let recall_probe = index.search_with_options_adaptive_vector_projection(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options,
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred)
                .with_backend_policy(force_quantized_policy())
                .as_recall_validation_probe(),
        );

        assert_eq!(filtered.hits.len(), 1);
        assert_eq!(
            filtered.retrievers[0].backend_selection_reason,
            Some(VectorBackendSelectionReason::SmallFilteredCandidateSet)
        );
        assert_eq!(filtered.retrievers[0].estimated_raw_vector_bytes, Some(8));
        assert_eq!(
            filtered.retrievers[0].filter_selectivity_per_million,
            Some(875_000)
        );
        assert_eq!(recall_probe.hits.len(), 1);
        assert_eq!(
            recall_probe.retrievers[0].backend_selection_reason,
            Some(VectorBackendSelectionReason::RecallValidationProbe)
        );
        assert_eq!(recall_probe.retrievers[0].backend, "scalar_vector_scan");
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn sampled_vector_recall_validates_filtered_persisted_projection() {
        let path = unique_test_dir("sampled_vector_recall");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, embedding, space_id) in [
                (
                    "memory:a",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "selected",
                ),
                (
                    "memory:b",
                    [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "selected",
                ),
                (
                    "memory:c",
                    [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "other",
                ),
                (
                    "memory:d",
                    [0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "other",
                ),
            ] {
                let mut document = doc(id, "Recall validation", "Bounded sampled query", embedding);
                document
                    .metadata
                    .insert("space_id".to_string(), space_id.to_string());
                index.upsert(document).unwrap();
            }
            index.checkpoint().unwrap();
        }
        let index = SearchIndex::open(&path).unwrap();

        let report = index.validate_sampled_vector_recall(VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: BTreeMap::from([("space_id".to_string(), "selected".to_string())]),
        });

        assert!(report.ready, "{:?}", report.blocker_codes);
        assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
        assert_eq!(report.sample_candidate_count, 2);
        assert_eq!(report.executed_sample_count, 2);
        assert_eq!(report.exact_hit_count, 2);
        assert_eq!(report.candidate_hit_count, 2);
        assert_eq!(report.candidate_overlap_count, 2);
        assert_eq!(report.candidate_recall_at_k_per_million, 1_000_000);
        assert_eq!(report.approximate_hit_count, 2);
        assert_eq!(report.recall_at_k_per_million, 1_000_000);
        assert_eq!(report.overlap_at_k_per_million, 1_000_000);
        assert_eq!(report.average_filter_selectivity_per_million, 500_000);
        assert_eq!(report.fallback_count, 0);
        assert_eq!(report.index_coverage_incomplete_count, 0);
        let json = report.json().to_string();
        assert!(!json.contains("memory:a"));
        assert!(!json.contains("Bounded sampled query"));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sampled_vector_recall_reports_probe_memory_exhaustion() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:a",
                "Recall A",
                "Budgeted recall validation",
                [1.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "memory:b",
                "Recall B",
                "Budgeted recall validation",
                [0.9, 0.1],
            ))
            .unwrap();
        let mut exact_controls = AdaptiveVectorExecutionControls::IN_MEMORY;
        exact_controls.vector_execution_options.max_working_bytes = 0;

        let report = index.validate_sampled_vector_recall_with_controls(
            VectorRecallValidationOptions {
                max_samples: 1,
                top_k: 1,
                candidate_limit: 1,
                minimum_recall_per_million: 1_000_000,
                metadata_filters: BTreeMap::new(),
            },
            exact_controls,
            AdaptiveVectorExecutionControls::RECALL_CANDIDATES,
        );

        assert!(!report.ready);
        assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
        assert_eq!(report.requested_sample_count, 1);
        assert_eq!(report.executed_sample_count, 0);
        assert_eq!(
            report.blocker_codes,
            vec![VectorRecallValidationBlocker::ProbeExecutionFailed]
        );
        assert_eq!(
            report.json()["blocker_codes"],
            serde_json::json!(["probe_execution_failed"])
        );
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn sampled_vector_recall_validates_rabitq_candidate_projection() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:a",
                "Recall A",
                "Missing projection",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "memory:b",
                "Recall B",
                "Missing projection",
                [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();

        let report = index.validate_sampled_vector_recall(VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: BTreeMap::new(),
        });

        assert!(report.ready, "{:?}", report.blocker_codes);
        assert_eq!(report.executed_sample_count, 2);
        assert_eq!(report.exact_hit_count, 2);
        assert_eq!(report.candidate_hit_count, 2);
        assert_eq!(report.candidate_recall_at_k_per_million, 1_000_000);
        assert_eq!(report.approximate_hit_count, 2);
        assert_eq!(report.fallback_count, 0);
        assert!(report.blocker_codes.is_empty());
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn production_recall_identity_is_bound_to_reopened_file_projection() {
        let path = unique_test_dir("production_recall_identity");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "test-embedding".to_string(),
                    version: Some("1".to_string()),
                    dimension: 8,
                })
                .unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Recall A",
                    "Generation-bound projection",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index
                .upsert(doc(
                    "memory:b",
                    "Recall B",
                    "Generation-bound projection",
                    [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: Vec::new(),
                    deletes: Vec::new(),
                    max_operations: Some(1),
                    source_graph_commit_epoch: Some(42),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        let index = SearchIndex::open(&path).unwrap();
        let identity = index.vector_projection_qualification_identity().unwrap();
        assert!(identity.file_backed);
        assert_eq!(identity.projection_generation, 1);
        assert_eq!(identity.source_graph_commit_epoch, Some(42));
        assert_eq!(identity.document_count, 2);
        assert_eq!(identity.embedding_model.as_deref(), Some("test-embedding"));
        assert_eq!(identity.embedding_version.as_deref(), Some("1"));
        assert_eq!(identity.dimension, 8);
        assert!(identity.source_digest > 0);

        let expected = crate::ProductionQualificationIdentity {
            source_revision: "test-revision".to_string(),
            rust_toolchain: "test-toolchain".to_string(),
            target_os: "linux".to_string(),
            target_arch: "x86_64".to_string(),
            enabled_features: vec!["vector-search".to_string()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".to_string(),
            deployment_profile: "production-replica".to_string(),
            dataset_fingerprint: "test-dataset".to_string(),
            canonical_graph_commit_epoch: 42,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let qualification = index.qualify_sampled_vector_recall_for_production(
            VectorRecallValidationOptions {
                max_samples: 2,
                top_k: 1,
                candidate_limit: 1,
                minimum_recall_per_million: 1_000_000,
                metadata_filters: BTreeMap::new(),
            },
            crate::ProductionEvidenceBinding {
                identity: expected.clone(),
                generated_at_unix_seconds: 1,
            },
            expected,
        );
        assert!(!qualification.ready);
        assert_eq!(
            qualification.blocker_codes,
            vec!["dataset_too_small".to_string()]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn sampled_vector_recall_redacts_invalid_metadata_filter() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:a",
                "Recall A",
                "Invalid filter must not leak",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();

        let report = index.validate_sampled_vector_recall(VectorRecallValidationOptions {
            metadata_filters: BTreeMap::from([(
                "lifecycle_state__not_in".to_string(),
                "sensitive malformed filter".to_string(),
            )]),
            ..VectorRecallValidationOptions::default()
        });

        assert!(!report.ready);
        assert!(report
            .blocker_codes
            .contains(&VectorRecallValidationBlocker::MetadataFilterInvalid));
        assert!(!report
            .json()
            .to_string()
            .contains("sensitive malformed filter"));
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn projection_checkpoint_publishes_segment_descriptor() {
        let path = unique_test_dir("search_segment_descriptor_publish");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(SearchDocument {
                    id: "memory:thread_0".to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
                })
                .unwrap();
            index
                .upsert(SearchDocument {
                    id: "memory:thread_1".to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
                })
                .unwrap();
            index
                .upsert(SearchDocument {
                    id: "memory:thread_2".to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }

        let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
        let descriptor = std::fs::read_to_string(&descriptor_path).unwrap();
        assert!(descriptor.contains("SKEIN_SEARCH_SEGMENTS_V3\n"));
        assert!(descriptor.contains("segment\t"));
        assert!(descriptor.contains("field\t"));
        assert!(descriptor.contains("checksum\t"));

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.filtered_document_count, 1);
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_prunes_numeric_range_filters() {
        let path = unique_test_dir("search_segment_descriptor_numeric_range");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, created_at) in [
                ("memory:0_old_0", "1"),
                ("memory:0_old_1", "2"),
                ("memory:1_new_0", "10"),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor range retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([(
                            "created_at".to_string(),
                            created_at.to_string(),
                        )]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        assert!(descriptor.contains("SKEIN_SEARCH_SEGMENTS_V3\n"));
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert_eq!(
            descriptor.segments[0]
                .metadata
                .get("created_at")
                .and_then(|summary| summary.numeric_range),
            Some(SearchNumericRange { min: 1.0, max: 2.0 })
        );
        assert_eq!(
            descriptor.segments[1]
                .metadata
                .get("created_at")
                .and_then(|summary| summary.numeric_range),
            Some(SearchNumericRange {
                min: 10.0,
                max: 10.0
            })
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor range retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "10".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:1_new_0");
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_pruning_candidate_document_count,
            3
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_pruned_document_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_scanned_document_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "created_at".to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["gte".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: true,
                timestamp_range_summary_used: false,
                value_summary_used: false,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_ranges_execute_bounded_physical_reads() {
        let path = unique_test_dir("search_segment_physical_ranges");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, space_id) in [
                ("memory:0", "space-a"),
                ("memory:1", "space-a"),
                ("memory:2", "space-b"),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: format!("Title {id}"),
                        content: "Physical segment range".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([
                            ("unit_type".to_string(), "memory".to_string()),
                            ("space_id".to_string(), space_id.to_string()),
                        ]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor = read_search_segment_descriptor(&path)
            .unwrap()
            .expect("expected persisted segment descriptor");
        assert_eq!(descriptor.segments.len(), 2);
        assert!(descriptor.payload_artifact_is_available(&path));
        let ranges = descriptor.physical_read_ranges();
        assert_eq!(ranges.len(), descriptor.segments.len());
        let scheduled_bytes = ranges.iter().map(|range| range.length.get()).sum::<u64>();
        let schedule =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(1).unwrap())
                .schedule(ranges);
        let mut reader = FileSegmentRangeReader::new();
        reader.register(
            SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            path.join(SEARCH_SEGMENT_PAYLOAD_FILE),
        );
        let mut document_ids = Vec::new();

        let report = SegmentReadExecutor::new(NonZeroU64::new(scheduled_bytes).unwrap())
            .execute(&reader, &schedule, |payload| {
                let segment_id = payload.range.segment_ids[0] as usize;
                let expected = descriptor.segments[segment_id]
                    .payload_range
                    .expect("expected physical payload range");
                assert_eq!(checksum_bytes(&payload.bytes), expected.checksum);
                let documents = decode_search_segment_documents(&payload.bytes)?;
                assert_eq!(
                    documents.len(),
                    descriptor.segments[segment_id].document_count
                );
                document_ids.extend(documents.into_iter().map(|document| document.id));
                Ok::<(), SkeinError>(())
            })
            .unwrap();

        assert_eq!(report.range_count, 2);
        assert_eq!(report.bytes_read, scheduled_bytes);
        assert_eq!(
            document_ids,
            vec![
                "memory:0".to_string(),
                "memory:1".to_string(),
                "memory:2".to_string(),
            ]
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index
            .try_search_with_options(
                "physical segment range",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::from([(
                        "space_id".to_string(),
                        "space-b".to_string(),
                    )]),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(
            result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            vec!["memory:2"]
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_range_read_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_bytes_read,
            descriptor.segments[1].payload_range.unwrap().length
        );

        let mut artifact = std::fs::read(path.join(SEARCH_SEGMENT_PAYLOAD_FILE)).unwrap();
        let corrupt_offset = descriptor.segments[1].payload_range.unwrap().offset as usize;
        artifact[corrupt_offset] ^= 0xff;
        std::fs::write(path.join(SEARCH_SEGMENT_PAYLOAD_FILE), artifact).unwrap();
        let error = index
            .try_search_with_options(
                "physical segment range",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::from([(
                        "space_id".to_string(),
                        "space-b".to_string(),
                    )]),
                    policy_epoch: None,
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("payload checksum mismatch"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_ranges_prune_label_in_filters_before_payload_reads() {
        let path = unique_test_dir("search_segment_label_physical_ranges");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, labels) in [
                ("memory:0", r#"["database"]"#),
                ("memory:1", r#"["database","rust"]"#),
                ("memory:2", r#"["planning"]"#),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: format!("Title {id}"),
                        content: "Physical label segment range".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([("labels".to_string(), labels.to_string())]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor = read_search_segment_descriptor(&path)
            .unwrap()
            .expect("expected persisted segment descriptor");
        assert_eq!(descriptor.segments.len(), 2);
        let index = SearchIndex::open(&path).unwrap();
        let result = index
            .try_search_with_options(
                "physical label segment range",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::from([(
                        "labels__in".to_string(),
                        r#"["Planning"]"#.to_string(),
                    )]),
                    policy_epoch: None,
                },
            )
            .unwrap();

        assert_eq!(
            result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            vec!["memory:2"]
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_range_read_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_bytes_read,
            descriptor.segments[1].payload_range.unwrap().length
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "labels".to_string(),
                value_kind: "enum".to_string(),
                operation_kinds: vec!["in".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: false,
                timestamp_range_summary_used: false,
                value_summary_used: true,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn fallible_search_rejects_pruned_reads_without_physical_ranges() {
        let path = unique_test_dir("search_segment_physical_ranges_missing");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, space_id) in [
                ("memory:0", "space-a"),
                ("memory:1", "space-a"),
                ("memory:2", "space-b"),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: format!("Title {id}"),
                        content: "Physical segment range".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([("space_id".to_string(), space_id.to_string())]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }
        let mut index = SearchIndex::open(&path).unwrap();
        for segment in &mut index
            .segment_descriptor
            .as_mut()
            .expect("persisted segment descriptor")
            .segments
        {
            segment.payload_range = None;
        }

        let error = index
            .try_search_with_options(
                "physical segment range",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::from([(
                        "space_id".to_string(),
                        "space-b".to_string(),
                    )]),
                    policy_epoch: None,
                },
            )
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("physical ranges are unavailable"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_prunes_timestamp_range_filters() {
        let path = unique_test_dir("search_segment_descriptor_timestamp_range");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, created_at) in [
                ("memory:0_old_0", "2026-01-01T00:00:00Z"),
                ("memory:0_old_1", "2026-01-02T00:00:00Z"),
                ("memory:1_new_0", "2026-02-01T12:00:00Z"),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor timestamp retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([(
                            "created_at".to_string(),
                            created_at.to_string(),
                        )]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        assert!(descriptor.contains("SKEIN_SEARCH_SEGMENTS_V3\n"));
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert_eq!(
            descriptor.segments[0]
                .metadata
                .get("created_at")
                .and_then(|summary| summary.timestamp_range),
            Some(SearchTimestampRange {
                min_epoch_millis: metadata_timestamp_value("2026-01-01T00:00:00Z").unwrap(),
                max_epoch_millis: metadata_timestamp_value("2026-01-02T00:00:00Z").unwrap(),
            })
        );
        assert_eq!(
            descriptor.segments[1]
                .metadata
                .get("created_at")
                .and_then(|summary| summary.timestamp_range),
            Some(SearchTimestampRange {
                min_epoch_millis: metadata_timestamp_value("2026-02-01T12:00:00Z").unwrap(),
                max_epoch_millis: metadata_timestamp_value("2026-02-01T12:00:00Z").unwrap(),
            })
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor timestamp retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "2026-02-01T00:00:00Z".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:1_new_0");
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "created_at".to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["gte".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: true,
                timestamp_range_summary_used: true,
                value_summary_used: false,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_preserves_default_space_filter() {
        let path = unique_test_dir("search_segment_descriptor_default_space");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for id in ["memory:default_0", "memory:default_1", "memory:default_2"] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Default scoped graph".to_string(),
                        content: "segment descriptor default space retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::new(),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert!(descriptor
            .segments
            .iter()
            .all(|segment| segment.metadata.contains_key("space_id")));

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor default space retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    DEFAULT_SPACE_ID.to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 3);
        assert_eq!(result.filtered_document_count, 3);
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            0
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_tracks_missing_nowledge_scan_filter_fields() {
        let path = unique_test_dir("search_segment_descriptor_missing_nowledge_fields");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for id in ["memory:0_without_type", "memory:1_without_type"] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor missing field retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::new(),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        for segment in &descriptor.segments {
            for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
                assert!(
                    segment.metadata.contains_key(*field),
                    "missing summary for {field}"
                );
            }
            assert_eq!(
                segment
                    .metadata
                    .get("unit_type")
                    .map(|summary| summary.present_count),
                Some(0)
            );
        }

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor missing field retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 0);
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            0
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_projection_probe_reports_required_scan_filter_descriptor_fields() {
        let path = unique_test_dir("search_projection_probe_scan_filter_fields");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for row in nowledge_probe_rows() {
                index.upsert_projection_row(row).unwrap();
            }
            index.checkpoint().unwrap();
        }

        let index = SearchIndex::open(&path).unwrap();
        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: None,
            active_embedding_dimension: Some(2),
        });
        let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
            .as_array()
            .expect("expected descriptor summaries");
        let fields = summaries
            .iter()
            .filter_map(|summary| summary["field"].as_str())
            .collect::<BTreeSet<_>>();

        assert!(
            probe["predicate_pushdown"]["persisted_segment_descriptor_ready"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_document_pruning_ready"],
            true
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_pruning_candidate_document_count"],
            6
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_pruned_document_count"],
            4
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_scanned_document_count"],
            2
        );
        for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
            assert!(fields.contains(field), "missing descriptor field {field}");
        }
        assert!(fields.contains(SEARCH_DOCUMENT_ID_FIELD));
        for field in ["importance", "confidence"] {
            assert!(
                summaries.iter().any(|summary| summary["field"] == field
                    && summary["numeric_range_summary_used"] == true),
                "missing numeric range summary for {field}"
            );
        }
        for field in ["created_at", "updated_at", "event_start", "event_end"] {
            assert!(
                summaries.iter().any(|summary| summary["field"] == field
                    && summary["timestamp_range_summary_used"] == true),
                "missing timestamp range summary for {field}"
            );
        }
        assert!(summaries
            .iter()
            .any(|summary| summary["field"] == SEARCH_DOCUMENT_ID_FIELD
                && summary["unique_key_summary_used"] == true
                && summary["unique_key_summary_segment_count"] == 3));
        let production_filter_pruning = &probe["production_filter_pruning"];
        assert_eq!(production_filter_pruning["ready"], true);
        assert_eq!(
            production_filter_pruning["persisted_segment_descriptor_used"],
            true
        );
        assert_eq!(
            production_filter_pruning["payload_read_avoidance_ready"],
            true
        );
        assert_eq!(production_filter_pruning["explain_analyze_ready"], true);
        assert_eq!(
            production_filter_pruning["sample_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len() + 1
        );
        assert_eq!(
            production_filter_pruning["ready_field_count"],
            NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
        );
        let samples = production_filter_pruning["samples"]
            .as_array()
            .expect("expected production filter pruning samples");
        for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
            assert!(
                samples
                    .iter()
                    .any(|sample| sample["field"] == *field && sample["ready"] == true),
                "missing production filter pruning sample for {field}"
            );
        }
        assert!(samples.iter().any(|sample| {
            sample["segment_pruned_document_count"]
                .as_u64()
                .unwrap_or(0)
                > 0
        }));
        assert!(samples
            .iter()
            .all(|sample| sample["explain_analyze"]["ready"] == true));
        assert!(samples.iter().all(
            |sample| sample["explain_analyze"]["operator"] == "search_projection_segment_scan"
        ));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_prunes_document_id_filters() {
        let path = unique_test_dir("search_segment_descriptor_document_id");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for id in ["memory:0_old_0", "memory:0_old_1", "memory:1_target"] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor document id retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::new(),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert_eq!(
            descriptor.segments[0]
                .metadata
                .get(SEARCH_DOCUMENT_ID_FIELD)
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from([
                "memory:0_old_0".to_string(),
                "memory:0_old_1".to_string()
            ]))
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor document id retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "document_id__in".to_string(),
                    r#"["memory:1_target"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:1_target");
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .segment_count,
            2
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .scanned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: SEARCH_DOCUMENT_ID_FIELD.to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["in".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: false,
                timestamp_range_summary_used: false,
                value_summary_used: true,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn persisted_segment_descriptor_prunes_enum_not_in_filters() {
        let path = unique_test_dir("search_segment_descriptor_enum_not_in");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, lifecycle_state) in [
                ("memory:0_deleted", "deleted"),
                ("memory:0_forgotten", " forgotten "),
                ("memory:1_active", "ACTIVE"),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor enum retrieval".to_string(),
                        embedding: None,
                        metadata: BTreeMap::from([(
                            "lifecycle_state".to_string(),
                            lifecycle_state.to_string(),
                        )]),
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert_eq!(
            descriptor.segments[0]
                .metadata
                .get("lifecycle_state")
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from([
                "deleted".to_string(),
                "forgotten".to_string()
            ]))
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor enum retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:1_active");
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .pruned_segment_count,
            1
        );
        assert_eq!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "lifecycle_state".to_string(),
                value_kind: "enum".to_string(),
                operation_kinds: vec!["not_in".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                numeric_range_summary_used: false,
                timestamp_range_summary_used: false,
                value_summary_used: true,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn corrupt_segment_descriptor_rebuilds_without_blocking_snapshot_load() {
        let path = unique_test_dir("search_segment_descriptor_rebuild");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(SearchDocument {
                    id: "memory:thread_1".to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor recovery".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }
        std::fs::write(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE), "corrupt").unwrap();

        let index = SearchIndex::open(&path).unwrap();
        assert!(std::fs::read_dir(&path).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("search_projection_segments.skein.corrupt.")
        }));
        let result = index.search_with_options(
            "segment descriptor recovery",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert!(
            result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn embedding_manifest_round_trips_through_snapshot() {
        let path = unique_test_dir("embedding_manifest_snapshot");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "text-embedding-3-small".to_string(),
                    version: Some("2026-07-15".to_string()),
                    dimension: 2,
                })
                .unwrap();
            index
                .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
                .unwrap();
            index.checkpoint().unwrap();
        }
        {
            let index = SearchIndex::open(&path).unwrap();
            assert_eq!(
                index.embedding_manifest(),
                Some(&SearchEmbeddingManifest {
                    model: "text-embedding-3-small".to_string(),
                    version: Some("2026-07-15".to_string()),
                    dimension: 2,
                })
            );
            let hits = index.search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);
            assert_eq!(hits[0].id, "a");
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn embedding_manifest_change_marks_full_reindex_needed() {
        let path = unique_test_dir("embedding_manifest_change");
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "model-a".to_string(),
                version: Some("1".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "model-b".to_string(),
                version: Some("1".to_string()),
                dimension: 2,
            })
            .unwrap();

        assert!(index.full_reindex_needed());
        let marker = std::fs::read_to_string(path.join(FULL_REINDEX_MARKER)).unwrap();
        assert!(marker.contains("embedding manifest changed"));
        assert!(marker.contains("model-a@1:2"));
        assert!(marker.contains("model-b@1:2"));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn embedding_manifest_dimension_rejects_mismatched_rows() {
        let mut index = SearchIndex::in_memory();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "model-a".to_string(),
                version: None,
                dimension: 3,
            })
            .unwrap();

        let error = index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap_err();

        assert!(error.to_string().contains("manifest expects 3"));
    }

    #[test]
    #[cfg(feature = "vector-search")]
    fn nowledge_search_projection_probe_reports_ready_shape() {
        let path = unique_test_dir("nowledge_search_projection_probe_ready");
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: Some("local".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_probe_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(7),
            })
            .unwrap();
        index.checkpoint().unwrap();

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(2),
        });

        assert_eq!(probe["derived_projection"], true);
        assert_eq!(probe["document_count"], 6);
        assert_eq!(probe["lifecycle"]["generation_cleanup_ready"], true);
        assert_eq!(
            probe["generation_cleanup"]["protocol"],
            SEARCH_PROJECTION_CLEANUP_PROTOCOL
        );
        assert_eq!(probe["generation_cleanup"]["retry_required"], false);
        assert_eq!(probe["document_identity"]["ready"], true);
        assert_eq!(
            probe["document_identity"]["id_space"],
            "search_projection_document_id"
        );
        assert_eq!(
            probe["document_identity"]["representation"],
            "sorted_document_ids"
        );
        assert_eq!(probe["document_identity"]["document_count"], 6);
        assert!(probe["document_identity"].get("document_ids").is_none());
        assert_eq!(probe["tables"].as_array().unwrap().len(), 6);
        assert_eq!(
            probe["predicate_pushdown"]["segment_document_pruning_ready"],
            true
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_pruning_candidate_document_count"],
            6
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_pruned_document_count"],
            4
        );
        assert_eq!(
            probe["predicate_pushdown"]["segment_scanned_document_count"],
            2
        );
        assert!(probe["tables"].as_array().unwrap().iter().all(|table| {
            table["present"] == true && table["fts_ready"] == true && table["vector_ready"] == true
        }));
        assert_eq!(probe["embedding_manifest"]["model_matches"], true);
        assert_eq!(probe["embedding_manifest"]["dimension_matches"], true);
        assert_eq!(probe["fail_soft"]["fts_to_vector_ready"], true);
        assert_eq!(probe["fail_soft"]["vector_to_fts_ready"], true);
        assert_eq!(probe["lifecycle"]["rebuild_marker_ready"], true);
        assert_eq!(probe["lifecycle"]["metadata_repair_marker_ready"], true);
        assert_eq!(probe["incremental_update"]["ready"], true);
        assert_eq!(
            probe["compressed_vector_projection"]["engine"],
            "skein_rabitq_scan"
        );
        assert_eq!(probe["compressed_vector_projection"]["compiled"], true);
        assert_eq!(probe["compressed_vector_projection"]["ready"], true);
        assert_eq!(
            probe["compressed_vector_projection"]["bit_width"],
            skein_vector_projection::PROJECTION_BIT_WIDTH
        );
        assert_eq!(probe["compressed_vector_projection"]["dimension"], 2);
        assert_eq!(
            probe["compressed_vector_projection"]["persisted_artifact_used"],
            true
        );
        assert_eq!(probe["blocker_codes"], serde_json::json!([]));
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_projection_probe_blocks_on_invalid_cleanup_generation_identity() {
        let path = unique_test_dir("search_projection_cleanup_invalid_manifest");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "bge-m3".to_string(),
                    version: Some("local".to_string()),
                    dimension: 2,
                })
                .unwrap();
            index
                .apply_projection_delta(SearchProjectionDelta {
                    upserts: nowledge_probe_rows(),
                    deletes: Vec::new(),
                    max_operations: None,
                    source_graph_commit_epoch: Some(7),
                })
                .unwrap();
            index.checkpoint().unwrap();
            index.checkpoint().unwrap();
            index.checkpoint().unwrap();
        }
        let stale_artifact = path.join("search_projection_segments.1.skein");
        std::fs::write(&stale_artifact, b"stale generation").unwrap();
        std::fs::write(
            path.join("search_projection.out_of_core.manifest.skein"),
            b"invalid manifest",
        )
        .unwrap();

        let index = SearchIndex::open(&path).unwrap();
        let report = index.projection_cleanup_report();
        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(2),
        });

        assert_eq!(report.generation_discovery_failures, 1);
        assert!(report.retry_required);
        assert!(stale_artifact.exists());
        assert_eq!(probe["lifecycle"]["generation_cleanup_ready"], false);
        assert!(probe["blocker_codes"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("projection_generation_cleanup_pending")));

        index.checkpoint().unwrap();
        assert!(!index.projection_cleanup_report().retry_required);
        assert!(!stale_artifact.exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn nowledge_search_projection_probe_reports_rabitq_projection_without_vectors() {
        let index = SearchIndex::in_memory();

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: None,
            active_embedding_dimension: None,
        });

        assert_eq!(
            probe["compressed_vector_projection"]["engine"],
            "skein_rabitq_scan"
        );
        assert_eq!(
            probe["compressed_vector_projection"]["compiled"],
            cfg!(feature = "vector-search")
        );
        assert_eq!(probe["compressed_vector_projection"]["ready"], false);
        assert_eq!(
            probe["compressed_vector_projection"]["blocker_codes"],
            serde_json::json!([if cfg!(feature = "vector-search") {
                "rabitq_projection_unavailable"
            } else {
                "vector_search_feature_disabled"
            }])
        );
    }

    #[test]
    #[cfg(feature = "qualification")]
    fn external_vector_candidates_use_skein_filter_and_raw_rerank() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:a".to_string(),
                title: "Vector A".to_string(),
                content: String::new(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:b".to_string(),
                title: "Vector B".to_string(),
                content: String::new(),
                embedding: Some(vec![0.0, 1.0]),
                metadata: BTreeMap::from([("space_id".to_string(), "blocked".to_string())]),
            })
            .unwrap();
        let filters = BTreeMap::from([("space_id".to_string(), "allowed".to_string())]);
        let allowlist = index.vector_document_ids_matching_filters_for_validation(&filters);
        assert_eq!(allowlist, BTreeSet::from(["memory:a".to_string()]));

        let candidates = vec![("memory:b".to_string(), 0.9), ("memory:a".to_string(), 0.8)];
        let result = index
            .search_with_external_vector_candidates_for_validation(
                &candidates,
                &BTreeSet::from(["memory:a".to_string(), "memory:b".to_string()]),
                "",
                Some(&[0.8, 0.6]),
                SearchMode::Vector,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: filters,
                    policy_epoch: Some(42),
                },
            )
            .unwrap();

        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(result.retrievers[0].name, "vector");
        assert_eq!(
            result.retrievers[0].backend,
            "external_validation_candidate_projection"
        );
        assert_eq!(
            result.retrievers[0].candidate_score_source,
            "quantized_approximate"
        );
        assert_eq!(result.retrievers[0].final_score_source, "raw_vector");
        assert_eq!(
            result.retrievers[0].candidate_top_ids,
            vec!["memory:a".to_string()]
        );
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.candidate_set.policy_epoch, Some(42));
    }

    #[test]
    fn nowledge_search_projection_probe_reports_manifest_and_marker_blockers() {
        let path = unique_test_dir("search_projection_probe_blockers");
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "bge-m3".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: nowledge_probe_rows(),
                deletes: Vec::new(),
                max_operations: None,
                source_graph_commit_epoch: Some(7),
            })
            .unwrap();
        index
            .mark_full_reindex_needed("embedding model changed")
            .unwrap();

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("text-embedding-3-small".to_string()),
            active_embedding_dimension: Some(1536),
        });

        assert_eq!(probe["embedding_manifest"]["model_matches"], false);
        assert_eq!(probe["embedding_manifest"]["dimension_matches"], false);
        assert_eq!(probe["lifecycle"]["rebuild_marker_ready"], false);
        assert_eq!(
            probe["blocker_codes"],
            serde_json::json!([
                "embedding_dimension_mismatch",
                "embedding_model_mismatch",
                "full_reindex_needed"
            ])
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_markers_round_trip() {
        let path = unique_test_dir("search_markers");
        let index = SearchIndex::open(&path).unwrap();
        index
            .mark_full_reindex_needed("embedding model changed")
            .unwrap();
        index
            .mark_full_reindex_needed("embedding model changed")
            .unwrap();
        index
            .mark_metadata_repair_needed("missing metadata columns")
            .unwrap();

        assert!(index.full_reindex_needed());
        assert!(index.metadata_repair_needed());
        assert_eq!(
            std::fs::read_to_string(path.join(FULL_REINDEX_MARKER)).unwrap(),
            "embedding model changed"
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    fn nowledge_probe_rows() -> Vec<SearchProjectionRow> {
        vec![
            nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
            nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
            nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
            nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
            nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
            nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
        ]
    }

    fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
        SearchProjectionRow {
            kind,
            external_id: external_id.to_string(),
            title: format!("{external_id} title"),
            body: format!("{external_id} body"),
            embedding: Some(vec![1.0, 0.0]),
            source_id: Some("source_1".to_string()),
            metadata: BTreeMap::from([
                ("space_id".to_string(), "default".to_string()),
                ("unit_type".to_string(), "memory".to_string()),
                ("lifecycle_state".to_string(), "active".to_string()),
                ("importance".to_string(), "0.8".to_string()),
                ("confidence".to_string(), "0.9".to_string()),
                ("created_at".to_string(), "2026-07-01T00:00:00Z".to_string()),
                ("updated_at".to_string(), "2026-07-02T00:00:00Z".to_string()),
                (
                    "event_start".to_string(),
                    "2026-07-01T12:00:00Z".to_string(),
                ),
                ("event_end".to_string(), "2026-07-01T13:00:00Z".to_string()),
                ("is_latest".to_string(), "true".to_string()),
            ]),
        }
    }

    fn nowledge_probe_row_without_embedding(
        kind: SearchProjectionKind,
        external_id: &str,
    ) -> SearchProjectionRow {
        SearchProjectionRow {
            embedding: None,
            ..nowledge_probe_row(kind, external_id)
        }
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn projection_rows_encode_nowledge_shapes() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "mem_1".to_string(),
                title: "Graph storage".to_string(),
                body: "Native adjacency and WAL".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                source_id: Some("thread_1".to_string()),
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            })
            .unwrap();

        let hits = index.search("wal", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:mem_1");
        let document = index.document("memory:mem_1").unwrap();
        assert_eq!(
            document.metadata.get("kind").map(String::as_str),
            Some("memory")
        );
        assert_eq!(
            document.metadata.get("external_id").map(String::as_str),
            Some("mem_1")
        );
        assert_eq!(
            document.metadata.get("source_id").map(String::as_str),
            Some("thread_1")
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn graph_rebuild_falls_back_for_empty_node_ids() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(String::new())),
                    (
                        "title".to_string(),
                        Value::String("Empty id graph".to_string()),
                    ),
                    (
                        "content".to_string(),
                        Value::String("fallback identity projection".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        let hits = index.search("fallback identity projection", None, SearchMode::Text, 10);
        let document = index.document("memory:0").expect("projected document");

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "memory:0");
        assert_eq!(hits[0].external_id.as_deref(), Some("0"));
        assert!(index.document("memory:").is_none());
        assert_eq!(
            document.metadata.get("external_id").map(String::as_str),
            Some("0")
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn graph_rebuild_source_id_fallback_skips_empty_values() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Thread scoped graph".to_string()),
                    ),
                    (
                        "content".to_string(),
                        Value::String("source fallback projection".to_string()),
                    ),
                    ("source_id".to_string(), Value::String(String::new())),
                    (
                        "thread_id".to_string(),
                        Value::String("thread_1".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        let document = index.document("memory:mem_1").expect("projected document");
        let hits = index.search_with_options(
            "source fallback projection",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(
            document.metadata.get("source_id").map(String::as_str),
            Some("thread_1")
        );
        assert_eq!(hits.total_hits, 1);
        assert_eq!(hits.hits[0].source_id.as_deref(), Some("thread_1"));
    }

    #[test]
    fn graph_rebuild_and_metadata_repair_collect_business_labels_once() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        let memory_id = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Label projection".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .business_labels
            .insert(memory_id, vec!["database".to_string(), "rust".to_string()]);

        let mut index = SearchIndex::in_memory();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert_eq!(store.business_label_bulk_scans.get(), 1);
        assert_eq!(store.business_label_point_lookups.get(), 0);
        assert_eq!(
            index
                .document("memory:mem_1")
                .and_then(|document| document.metadata.get("labels"))
                .map(String::as_str),
            Some(r#"["database","rust"]"#)
        );

        index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();

        assert_eq!(store.business_label_bulk_scans.get(), 2);
        assert_eq!(store.business_label_point_lookups.get(), 0);
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn full_rebuild_projects_graph_nodes() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    (
                        "content".to_string(),
                        Value::String("Native adjacency and WAL".to_string()),
                    ),
                    (
                        "source_id".to_string(),
                        Value::String("thread_1".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String("entity_1".to_string())),
                    ("name".to_string(), Value::String("Skein".to_string())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        let summary = index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert_eq!(summary.scanned_nodes, 2);
        assert_eq!(summary.indexed_documents, 2);
        assert!(index.document("memory:mem_1").is_some());
        assert_eq!(
            index
                .document("entity:entity_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("entity")
        );
        let hits = index.search("adjacency", None, SearchMode::Text, 10);
        assert_eq!(hits[0].id, "memory:mem_1");
    }

    #[test]
    fn full_rebuild_limit_failure_keeps_existing_projection() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String("entity_1".to_string())),
                    ("name".to_string(), Value::String("Skein".to_string())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
            .unwrap();
        let error = index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions { max_rows: Some(1) })
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("old").is_some());
        let freshness = index.projection_freshness();
        assert!(freshness.full_reindex_needed);
        assert_eq!(
            freshness.full_reindex_reasons,
            vec!["full rebuild exceeded configured row limit".to_string()]
        );
    }

    #[test]
    fn full_rebuild_clears_projection_markers_on_success() {
        let path = unique_test_dir("search_rebuild_markers");
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::open(&path).unwrap();
        index.mark_full_reindex_needed("stale projection").unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert!(!index.full_reindex_needed());
        assert!(!index.metadata_repair_needed());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_derived_artifact_rebuild_reports_projection_refresh() {
        let path = unique_test_dir("search_derived_artifact_rebuild");
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    (
                        "body".to_string(),
                        Value::String("Native adjacency".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(doc(
                "old",
                "Old projection",
                "Should be replaced",
                [1.0, 0.0],
            ))
            .unwrap();
        index.mark_full_reindex_needed("stale projection").unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();

        let report = index
            .rebuild_derived_artifacts(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert_eq!(report.artifact_type, "search_projection");
        assert_eq!(report.name, "search_projection");
        assert_eq!(report.action, "rebuilt");
        assert_eq!(report.before_document_count, 1);
        assert_eq!(report.after_document_count, 1);
        assert_eq!(report.scanned_nodes, 1);
        assert_eq!(report.indexed_documents, 1);
        assert!(!report.full_reindex_needed);
        assert!(report.full_reindex_reasons.is_empty());
        assert!(!report.metadata_repair_needed);
        assert!(report.metadata_repair_reasons.is_empty());
        assert!(index.document("old").is_none());
        assert!(index.document("memory:mem_1").is_some());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_derived_artifact_rebuild_limit_failure_keeps_existing_projection() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }

        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
            .unwrap();

        let error = index
            .rebuild_derived_artifacts(&catalog, &store, SearchRebuildOptions { max_rows: Some(1) })
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("old").is_some());
    }

    #[test]
    fn search_rebuild_background_work_plan_uses_projection_lane() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }
        let index = SearchIndex::in_memory();

        let plan = index
            .rebuild_background_work_plan(
                &store,
                BackgroundWorkHint {
                    active_topic: true,
                    query_probability_per_million: 200_000,
                    ..BackgroundWorkHint::default()
                },
            )
            .unwrap();

        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(plan.request.estimated_operations, 2);
        let ranked =
            LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
        assert_eq!(ranked[0].index, 0);
        assert!(ranked[0]
            .decision
            .reasons
            .iter()
            .any(|reason| reason == "active topic"));
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn background_search_projection_rebuild_uses_qos_admission() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
            .unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = index
            .rebuild_background_derived_artifacts(
                &policy,
                &LocalQosState::default(),
                &catalog,
                &store,
                SearchRebuildOptions::default(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("old").is_some());
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_search_projection_rebuild_releases_budget_on_rebuild_error() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .rebuild_scheduled_background_derived_artifacts(
                &scheduler,
                &catalog,
                &store,
                SearchRebuildOptions { max_rows: Some(1) },
            )
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(index.document_count(), 1);
        assert!(index.document("old").is_some());
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn projection_delta_incrementally_updates_search_rows() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
            .unwrap();

        let report = index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Incremental FTS".to_string(),
                    body: "Small batches keep embedded search cheap".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            })
            .unwrap();

        assert_eq!(report.action, "incremental_update");
        assert_eq!(report.before_document_count, 1);
        assert_eq!(report.after_document_count, 1);
        assert_eq!(report.upserted_documents, 1);
        assert_eq!(report.deleted_documents, 1);
        assert_eq!(report.operation_count, 2);
        assert_eq!(report.source_graph_commit_epoch_before, None);
        assert_eq!(report.source_graph_commit_epoch_after, None);
        assert!(!report.source_graph_commit_epoch_updated);
        assert!(index.document("memory:old").is_none());
        assert!(index.document("memory:new").is_some());
        let hits = index.search("embedded search", None, SearchMode::Text, 10);
        assert_eq!(hits[0].id, "memory:new");
    }

    #[test]
    fn projection_delta_updates_source_graph_commit_epoch_when_provided() {
        let mut index = SearchIndex::in_memory();
        let report = index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Fresh graph delta".to_string(),
                    body: "Graph derived projection delta advances freshness".to_string(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(7),
            })
            .unwrap();

        assert_eq!(report.source_graph_commit_epoch_before, None);
        assert_eq!(report.source_graph_commit_epoch_after, Some(7));
        assert!(report.source_graph_commit_epoch_updated);
        let freshness = index.projection_freshness();
        assert_eq!(freshness.source_graph_commit_epoch, Some(7));
        assert_eq!(freshness.durable_source_graph_commit_epoch, None);
        assert!(freshness.has_uncheckpointed_changes);
    }

    #[test]
    fn import_provenance_is_independent_from_the_local_projection_cursor() {
        let root = unique_test_dir("import_provenance_is_independent_from_local_cursor");
        let mut index = SearchIndex::open(&root).unwrap();
        index.record_import_source_graph_commit_epoch(7).unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                source_graph_commit_epoch: Some(1),
                ..Default::default()
            })
            .unwrap();
        index.checkpoint().unwrap();
        let reopened = SearchIndex::open(&root).unwrap();
        let freshness = reopened.projection_freshness();
        assert_eq!(freshness.import_source_graph_commit_epoch, Some(7));
        assert_eq!(freshness.source_graph_commit_epoch, Some(1));
        assert_eq!(freshness.durable_source_graph_commit_epoch, Some(1));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn projection_checkpoint_advances_durable_watermark() {
        let path = unique_test_dir("durable_projection_watermark");
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Durable graph delta".to_string(),
                    body: "Checkpoint publishes the durable projection watermark".to_string(),
                    embedding: None,
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(9),
            })
            .unwrap();

        assert_eq!(
            index
                .projection_freshness()
                .durable_source_graph_commit_epoch,
            None
        );
        index.checkpoint().unwrap();
        let freshness = index.projection_freshness();
        assert_eq!(freshness.durable_source_graph_commit_epoch, Some(9));
        assert!(!freshness.has_uncheckpointed_changes);

        let reopened = SearchIndex::open(&path).unwrap();
        assert_eq!(
            reopened
                .projection_freshness()
                .durable_source_graph_commit_epoch,
            Some(9)
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn projection_delta_background_work_plan_is_absent_without_operations() {
        let delta = SearchProjectionDelta::default();

        assert!(delta
            .background_work_plan(BackgroundWorkHint::default())
            .is_none());
    }

    #[test]
    fn projection_delta_background_work_plan_uses_projection_lane() {
        let delta = SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Incremental FTS".to_string(),
                body: "Small batches keep embedded search cheap".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: vec!["memory:old".to_string()],
            max_operations: Some(2),
            source_graph_commit_epoch: None,
        };

        let plan = delta
            .background_work_plan(BackgroundWorkHint {
                recent_delta_operations: 2,
                ..BackgroundWorkHint::default()
            })
            .unwrap();

        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(plan.request.estimated_operations, 2);
        assert_eq!(plan.hint.recent_delta_operations, 2);
    }

    #[test]
    fn projection_delta_budget_failure_keeps_existing_projection() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:old",
                "Old projection",
                "Should stay",
                [1.0, 0.0],
            ))
            .unwrap();

        let error = index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Too much work".to_string(),
                    body: "Should not be applied".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(1),
                source_graph_commit_epoch: None,
            })
            .unwrap_err();

        assert!(error.to_string().contains("operation count 2"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("memory:old").is_some());
        assert!(index.document("memory:new").is_none());
    }

    #[test]
    fn projection_delta_dimension_failure_keeps_existing_projection() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:old",
                "Old projection",
                "Should stay",
                [1.0, 0.0],
            ))
            .unwrap();

        let error = index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Bad dimension".to_string(),
                    body: "Should not be applied".to_string(),
                    embedding: Some(vec![1.0, 0.0, 0.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            })
            .unwrap_err();

        assert!(error.to_string().contains("embedding dimension mismatch"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("memory:old").is_some());
        assert!(index.document("memory:new").is_none());
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn background_projection_delta_uses_qos_admission() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:old",
                "Old projection",
                "Should stay",
                [1.0, 0.0],
            ))
            .unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(1),
            ..LocalQosPolicy::default()
        };

        let error = index
            .apply_background_projection_delta(
                &policy,
                &LocalQosState::default(),
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "new".to_string(),
                        title: "Deferred projection".to_string(),
                        body: "Should not be applied".to_string(),
                        embedding: Some(vec![0.0, 1.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: vec!["memory:old".to_string()],
                    max_operations: Some(2),
                    source_graph_commit_epoch: None,
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(index.document_count(), 1);
        assert!(index.document("memory:old").is_some());
        assert!(index.document("memory:new").is_none());
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn background_projection_delta_applies_when_qos_admits() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
            .unwrap();

        let report = index
            .apply_background_projection_delta(
                &LocalQosPolicy::default(),
                &LocalQosState::default(),
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "new".to_string(),
                        title: "Admitted projection".to_string(),
                        body: "QoS admitted background work".to_string(),
                        embedding: Some(vec![0.0, 1.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: vec!["memory:old".to_string()],
                    max_operations: Some(2),
                    source_graph_commit_epoch: None,
                },
            )
            .unwrap();

        assert_eq!(report.operation_count, 2);
        assert!(index.document("memory:old").is_none());
        assert!(index.document("memory:new").is_some());
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_projection_delta_tracks_running_budget() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(2),
            max_total_background_operations: Some(2),
            ..LocalQosPolicy::default()
        });

        let report = index
            .apply_scheduled_background_projection_delta(
                &scheduler,
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "new".to_string(),
                        title: "Scheduled projection".to_string(),
                        body: "QoS tracked background work".to_string(),
                        embedding: Some(vec![0.0, 1.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: vec!["memory:old".to_string()],
                    max_operations: Some(2),
                    source_graph_commit_epoch: None,
                },
            )
            .unwrap();

        assert_eq!(report.operation_count, 2);
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert!(index.document("memory:old").is_none());
        assert!(index.document("memory:new").is_some());
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_projection_delta_defers_when_scheduler_is_full() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:old",
                "Old projection",
                "Should stay",
                [1.0, 0.0],
            ))
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 3))
            .unwrap();

        let error = index
            .apply_scheduled_background_projection_delta(
                &scheduler,
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "new".to_string(),
                        title: "Deferred projection".to_string(),
                        body: "Should not be applied".to_string(),
                        embedding: Some(vec![0.0, 1.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: vec!["memory:old".to_string()],
                    max_operations: Some(2),
                    source_graph_commit_epoch: None,
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(scheduler.state().running_background_operations, 3);
        assert!(index.document("memory:old").is_some());
        assert!(index.document("memory:new").is_none());

        running.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_projection_delta_releases_budget_on_delta_error() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc(
                "memory:old",
                "Old projection",
                "Should stay",
                [1.0, 0.0],
            ))
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .apply_scheduled_background_projection_delta(
                &scheduler,
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "new".to_string(),
                        title: "Rejected by delta budget".to_string(),
                        body: "Should not be applied".to_string(),
                        embedding: Some(vec![0.0, 1.0]),
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    deletes: vec!["memory:old".to_string()],
                    max_operations: Some(1),
                    source_graph_commit_epoch: None,
                },
            )
            .unwrap_err();

        assert!(error.to_string().contains("exceeded configured limit"));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert!(index.document("memory:old").is_some());
        assert!(index.document("memory:new").is_none());
    }

    #[test]
    fn metadata_repair_updates_only_metadata() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    ("space_id".to_string(), Value::String("default".to_string())),
                    (
                        "source_id".to_string(),
                        Value::String("thread_1".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Old title".to_string(),
                content: "Old body should stay".to_string(),
                embedding: Some(vec![1.0, 0.0]),
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        let summary = index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();

        assert_eq!(summary.repaired_documents, 1);
        let document = index.document("memory:mem_1").unwrap();
        assert_eq!(document.title, "Old title");
        assert_eq!(document.content, "Old body should stay");
        assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
        assert_eq!(
            document.metadata.get("kind").map(String::as_str),
            Some("memory")
        );
        assert_eq!(
            document.metadata.get("source_id").map(String::as_str),
            Some("thread_1")
        );
        assert_eq!(
            document.metadata.get("space_id").map(String::as_str),
            Some("default")
        );
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn metadata_repair_corrects_segmented_lexical_statistics_from_original_documents() {
        let path = unique_test_dir("metadata_repair_lexical_statistics");
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }

        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "First document".to_string(),
                content: "Body".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:mem_2".to_string(),
                title: "Stale document".to_string(),
                content: "Body".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        index.checkpoint().unwrap();

        index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();
        let mut reference = SearchIndex::in_memory();
        for document in index.documents.values().cloned() {
            reference.upsert(document).unwrap();
        }

        let actual = index.search("stale", None, SearchMode::Text, 10);
        let expected = reference.search("stale", None, SearchMode::Text, 10);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].id, "memory:mem_2");
        assert_eq!(actual[0].text_score, expected[0].text_score);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn metadata_repair_background_work_plan_uses_projection_lane() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();
        let index = SearchIndex::in_memory();

        let plan = index
            .metadata_repair_background_work_plan(
                &store,
                BackgroundWorkHint {
                    staleness_millis: 10_000,
                    staleness_ttl_millis: Some(1_000),
                    ..BackgroundWorkHint::default()
                },
            )
            .unwrap();

        assert_eq!(plan.request.class, WorkClass::Projection);
        assert_eq!(plan.request.estimated_operations, 1);
        assert_eq!(plan.hint.staleness_millis, 10_000);
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn background_metadata_repair_uses_qos_admission() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Old title".to_string(),
                content: "Old body should stay".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        let policy = LocalQosPolicy {
            max_background_operations: Some(0),
            ..LocalQosPolicy::default()
        };

        let error = index
            .repair_background_metadata_from_graph(
                &policy,
                &LocalQosState::default(),
                &catalog,
                &store,
                MetadataRepairOptions::default(),
                1,
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("stale")
        );
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_metadata_repair_tracks_running_budget() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                    ("space_id".to_string(), Value::String("default".to_string())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Old title".to_string(),
                content: "Old body should stay".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..LocalQosPolicy::default()
        });

        let summary = index
            .repair_scheduled_background_metadata_from_graph(
                &scheduler,
                &catalog,
                &store,
                MetadataRepairOptions::default(),
                1,
            )
            .unwrap();

        assert_eq!(summary.repaired_documents, 1);
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("memory")
        );
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_metadata_repair_defers_when_scheduler_is_full() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:mem_1".to_string(),
                title: "Old title".to_string(),
                content: "Old body should stay".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 3))
            .unwrap();

        let error = index
            .repair_scheduled_background_metadata_from_graph(
                &scheduler,
                &catalog,
                &store,
                MetadataRepairOptions::default(),
                2,
            )
            .unwrap_err();

        assert!(error.to_string().contains("deferred"));
        assert_eq!(scheduler.state().running_background_operations, 3);
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("stale")
        );

        running.finish();
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    #[cfg(feature = "background-maintenance")]
    fn scheduled_background_metadata_repair_releases_budget_on_repair_error() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }

        let mut index = SearchIndex::in_memory();
        for id in ["mem_1", "mem_2"] {
            index
                .upsert(SearchDocument {
                    id: format!("memory:{id}"),
                    title: id.to_string(),
                    content: id.to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
                })
                .unwrap();
        }
        let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .repair_scheduled_background_metadata_from_graph(
                &scheduler,
                &catalog,
                &store,
                MetadataRepairOptions { max_rows: Some(1) },
                2,
            )
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(scheduler.state().running_background_operations, 0);
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("stale")
        );
    }

    #[test]
    fn graph_projection_normalizes_default_space_metadata() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("missing_space".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Missing space".to_string()),
                    ),
                ]),
            )
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("empty_space".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Empty space".to_string()),
                    ),
                    ("space_id".to_string(), Value::String(String::new())),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::in_memory();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();

        assert_eq!(
            index
                .document("memory:missing_space")
                .and_then(|document| document.metadata.get("space_id"))
                .map(String::as_str),
            Some(DEFAULT_SPACE_ID)
        );
        assert_eq!(
            index
                .document("memory:empty_space")
                .and_then(|document| document.metadata.get("space_id"))
                .map(String::as_str),
            Some(DEFAULT_SPACE_ID)
        );
    }

    #[test]
    fn metadata_repair_limit_failure_keeps_existing_metadata() {
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        for id in ["mem_1", "mem_2"] {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".to_string(), Value::String(id.to_string())),
                        ("title".to_string(), Value::String(id.to_string())),
                    ]),
                )
                .unwrap();
        }

        let mut index = SearchIndex::in_memory();
        for id in ["mem_1", "mem_2"] {
            index
                .upsert(SearchDocument {
                    id: format!("memory:{id}"),
                    title: id.to_string(),
                    content: id.to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
                })
                .unwrap();
        }
        let error = index
            .repair_metadata_from_graph(
                &catalog,
                &store,
                MetadataRepairOptions { max_rows: Some(1) },
            )
            .unwrap_err();

        assert!(error.to_string().contains("row limit"));
        assert_eq!(
            index
                .document("memory:mem_1")
                .unwrap()
                .metadata
                .get("kind")
                .map(String::as_str),
            Some("stale")
        );
        let freshness = index.projection_freshness();
        assert!(freshness.metadata_repair_needed);
        assert_eq!(
            freshness.metadata_repair_reasons,
            vec!["metadata repair exceeded configured row limit".to_string()]
        );
    }

    #[test]
    fn metadata_repair_marks_full_reindex_when_rows_are_missing() {
        let path = unique_test_dir("metadata_repair_missing_rows");
        let mut catalog = Catalog::default();
        let mut store = TestProjectionSource::in_memory();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("mem_1".to_string())),
                    (
                        "title".to_string(),
                        Value::String("Graph storage".to_string()),
                    ),
                ]),
            )
            .unwrap();

        let mut index = SearchIndex::open(&path).unwrap();
        index
            .mark_metadata_repair_needed("missing metadata")
            .unwrap();
        let summary = index
            .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
            .unwrap();

        assert_eq!(summary.missing_documents, 1);
        assert!(index.full_reindex_needed());
        assert!(!index.metadata_repair_needed());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn search_index_mutations_preserve_retained_lexical_delta_snapshots() {
        let path = unique_test_dir("retained_lexical_delta");
        let mut index = SearchIndex::open(&path).unwrap();
        let document = |content: &str| SearchDocument {
            id: "a".into(),
            title: String::new(),
            content: content.into(),
            embedding: None,
            metadata: BTreeMap::new(),
        };
        index.upsert(document("base graph")).unwrap();
        index.checkpoint().unwrap();
        index.upsert(document("alpha")).unwrap();
        let reader = index.lexical_projection.lock().unwrap().clone().unwrap();
        let snapshot = index.lexical_delta.lock().unwrap().clone();
        let terms = BTreeSet::from(["alpha".to_string()]);
        let expected = reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap();
        assert_eq!(expected.matching_document_count, 1);

        for content in [Some("beta"), None, Some("gamma")] {
            if let Some(content) = content {
                index.upsert(document(content)).unwrap();
            } else {
                index.delete("a");
            }
            assert_eq!(
                reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
                expected
            );
            let current = index.lexical_delta.lock().unwrap().clone();
            assert_eq!(
                reader
                    .score(&terms, &current, None, |_| Ok(true))
                    .unwrap()
                    .matching_document_count,
                0
            );
        }
        index.checkpoint().unwrap();
        assert_eq!(
            reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
            expected
        );
        index.set_analyzer_lexicon(SearchAnalyzerLexicon::empty());
        assert_eq!(
            reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
            expected
        );
        drop(reader);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn segmented_lexical_projection_matches_reference_across_delta_and_reopen() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let path = unique_test_dir("segmented_lexical_projection");
        let options = SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: Some(10),
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            policy_epoch: None,
        };
        let mut index = SearchIndex::open(&path).unwrap();
        let documents = [
            SearchDocument {
                id: "a".to_string(),
                title: "Graph Graph".to_string(),
                content: "storage engine".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            },
            SearchDocument {
                id: "b".to_string(),
                title: "Graph memory".to_string(),
                content: "retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            },
            SearchDocument {
                id: "hidden".to_string(),
                title: "Graph Graph Graph".to_string(),
                content: "private".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "private".to_string())]),
            },
        ];
        for document in documents.clone() {
            index.upsert(document).unwrap();
        }
        let reference = index
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        index.checkpoint().unwrap();
        let segmented = index
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        assert_eq!(
            segmented
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>(),
            reference
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>()
        );
        let text = segmented
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .unwrap();
        assert!(text.segmented_lexical_projection_used);
        assert_eq!(text.backend, "segmented_bm25_text");
        assert!(text.posting_bytes_read > 0);
        assert!(text.candidate_postings_visited > 0);

        index
            .upsert(SearchDocument {
                id: "b".to_string(),
                title: "Vector memory".to_string(),
                content: "embedding".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            })
            .unwrap();
        index.delete("a");
        index
            .upsert(SearchDocument {
                id: "c".to_string(),
                title: "Graph query".to_string(),
                content: "optimizer".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            })
            .unwrap();
        let mut delta_reference = SearchIndex::in_memory();
        for document in index.documents.values().cloned() {
            delta_reference.upsert(document).unwrap();
        }
        let expected = delta_reference
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        let actual = index
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>()
        );

        index.checkpoint().unwrap();
        let generation = index
            .lexical_projection
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .generation();
        drop(index);
        let reopened = SearchIndex::open(&path).unwrap();
        let reopened_result = reopened
            .try_search_with_options("graph", None, SearchMode::Text, options)
            .unwrap();
        assert_eq!(
            reopened_result
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>()
        );
        drop(reopened);

        let artifact = path.join(lexical_projection::artifact_file(generation));
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&artifact)
            .unwrap();
        file.seek(SeekFrom::Start(32)).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(32)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
        let error = SearchIndex::open(&path).unwrap_err();
        assert!(error.to_string().contains("artifact checksum mismatch"));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn segmented_lexical_projection_reopens_with_chinese_search_terms() {
        let path = unique_test_dir("segmented_lexical_projection_chinese");
        let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(SearchDocument {
                id: "distributed".to_string(),
                title: "\u{73b0}\u{4ee3}\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}".to_string(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.checkpoint().unwrap();
        drop(index);

        let reopened = SearchIndex::open(&path).unwrap();
        let output = reopened
            .try_search_with_options(
                term,
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        let text = output
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .unwrap();
        assert_eq!(output.hits[0].id, "distributed");
        assert!(output.hits[0]
            .matched_terms
            .iter()
            .any(|token| token == term));
        assert!(text.segmented_lexical_projection_used);
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "full-text-search")]
    fn segmented_lexical_topk_preserves_total_hits_and_page() {
        let path = unique_test_dir("segmented_lexical_topk");
        let mut reference = SearchIndex::in_memory();
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..64 {
            let document = SearchDocument {
                id: format!("doc-{number:03}"),
                title: std::iter::repeat_n("graph", number % 4 + 1)
                    .collect::<Vec<_>>()
                    .join(" "),
                content: format!("storage document {number}"),
                embedding: None,
                metadata: BTreeMap::new(),
            };
            reference.upsert(document.clone()).unwrap();
            index.upsert(document).unwrap();
        }
        let options = SearchQueryOptions {
            limit: 3,
            offset: 2,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        };
        let expected = reference
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        index.checkpoint().unwrap();
        let actual = index
            .try_search_with_options("graph", None, SearchMode::Text, options)
            .unwrap();

        assert_eq!(actual.total_hits, 64);
        assert_eq!(actual.total_hits, expected.total_hits);
        assert_eq!(actual.hits.len(), 3);
        assert!(actual.truncated);
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score))
                .collect::<Vec<_>>()
        );
        let text = actual
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .unwrap();
        assert_eq!(text.generated_candidate_count, 64);
        assert_eq!(text.candidate_count, 64);
        assert_eq!(text.candidate_set.cardinality, 5);
        assert!(text.segmented_lexical_projection_used);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn segmented_lexical_hybrid_rank_window_matches_reference() {
        let path = unique_test_dir("segmented_lexical_hybrid");
        let mut reference = SearchIndex::in_memory();
        let mut index = SearchIndex::open(&path).unwrap();
        for number in 0..12 {
            let document = SearchDocument {
                id: format!("doc-{number:02}"),
                title: std::iter::repeat_n("graph", number % 3 + 1)
                    .collect::<Vec<_>>()
                    .join(" "),
                content: format!("hybrid storage {number}"),
                embedding: Some(vec![number as f32, (12 - number) as f32]),
                metadata: BTreeMap::new(),
            };
            reference.upsert(document.clone()).unwrap();
            index.upsert(document).unwrap();
        }
        let options = SearchQueryOptions {
            limit: 5,
            offset: 0,
            rank_window: Some(3),
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        };
        let query_embedding = [1.0, 0.0];
        let expected = reference
            .try_search_with_options(
                "graph",
                Some(&query_embedding),
                SearchMode::Hybrid,
                options.clone(),
            )
            .unwrap();
        index.checkpoint().unwrap();
        let actual = index
            .try_search_with_options("graph", Some(&query_embedding), SearchMode::Hybrid, options)
            .unwrap();

        assert_eq!(actual.total_hits, expected.total_hits);
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score, hit.vector_rank, hit.text_rank,))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score, hit.vector_rank, hit.text_rank,))
                .collect::<Vec<_>>()
        );
        let text = actual
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .unwrap();
        assert_eq!(text.candidate_count, 12);
        assert_eq!(text.candidate_set.cardinality, 3);
        assert!(text.segmented_lexical_projection_used);
        fs::remove_dir_all(path).unwrap();
    }

    #[cfg(all(feature = "acl", feature = "full-text-search"))]
    #[test]
    fn segmented_lexical_acl_excludes_unauthorized_documents() {
        let path = unique_test_dir("segmented_lexical_acl");
        let mut expected_index = SearchIndex::in_memory();
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_runtime_capabilities(
            RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, true),
        );
        let allowed = [
            SearchDocument {
                id: "allowed-a".to_string(),
                title: "Graph graph".to_string(),
                content: "storage".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            },
            SearchDocument {
                id: "allowed-b".to_string(),
                title: "Graph".to_string(),
                content: "memory".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
            },
        ];
        for document in allowed.clone() {
            expected_index.upsert(document.clone()).unwrap();
            index.upsert(document).unwrap();
        }
        index
            .upsert(SearchDocument {
                id: "hidden".to_string(),
                title: "Graph graph graph graph".to_string(),
                content: "private".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), "private".to_string())]),
            })
            .unwrap();
        let options = SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        };
        let expected = expected_index
            .try_search_with_options("graph", None, SearchMode::Text, options.clone())
            .unwrap();
        index.checkpoint().unwrap();
        let actual = index
            .try_search_with_options_access_control(
                "graph",
                None,
                SearchMode::Text,
                options,
                SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]),
            )
            .unwrap();

        assert_eq!(actual.total_hits, 2);
        assert_eq!(actual.candidate_set.filtered_out_count, 1);
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.text_rank))
                .collect::<Vec<_>>()
        );
        let text = actual
            .retrievers
            .iter()
            .find(|retriever| retriever.name == "text")
            .unwrap();
        assert_eq!(text.candidate_count, 2);
        assert!(text.segmented_lexical_projection_used);
        fs::remove_dir_all(path).unwrap();
    }

    fn doc<const N: usize>(
        id: &str,
        title: &str,
        content: &str,
        embedding: [f32; N],
    ) -> SearchDocument {
        SearchDocument {
            id: id.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            embedding: Some(embedding.to_vec()),
            metadata: BTreeMap::new(),
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("skein_search_{name}_{nanos}"))
    }
}
