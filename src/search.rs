use crate::error::{Result, SkeinError};
use crate::qos::{
    BackgroundWorkHint, BackgroundWorkPlan, LocalQosPolicy, LocalQosScheduler, LocalQosState,
    QosAdmission, WorkClass, WorkRequest,
};
use crate::schema::Catalog;
use crate::store::{GraphStore, NodeId, NodeRecord};
use crate::value::Value;
use skein_optimizer::{
    normalize_search_enum_value, push_search_predicates, search_field_is_enum_like,
    SearchPredicate, SearchPredicateOp, SearchPredicateSet, SearchScalarValue,
    SearchScanPredicateSupport,
};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

mod analyzer_lexicon;
#[cfg(feature = "turbovec")]
pub mod turbovec_projection;
use analyzer_lexicon::{CORE_SEMANTIC_ALIAS_RULES, NOWLEDGE_MEMORY_SEMANTIC_ALIAS_RULES};

const SEARCH_SNAPSHOT_FILE: &str = "search_projection.skein";
const SEARCH_SEGMENT_DESCRIPTOR_FILE: &str = "search_projection_segments.skein";
#[cfg(feature = "turbovec")]
const SEARCH_TURBOVEC_PROJECTION_FILE: &str = "search_projection.tvim";
#[cfg(feature = "turbovec")]
const SEARCH_TURBOVEC_PROJECTION_BIT_WIDTH: usize = 4;
pub const FULL_REINDEX_MARKER: &str = ".reindex_needed";
pub const METADATA_REPAIR_MARKER: &str = ".projection_metadata_repair_needed";
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const RRF_K: f64 = 60.0;
const SEARCH_COMPRESSION_HEADER: &str = "SKEIN_COMPRESSED_V1";
const SEARCH_COMPRESSION_LEVEL: i32 = 3;
const SEARCH_DOCUMENT_ID_FIELD: &str = "document_id";
const DEFAULT_MEMORY_LIFECYCLE_STATE: &str = "active";
const DEFAULT_IS_LATEST: &str = "true";
#[cfg(not(test))]
const SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS: usize = 128;
#[cfg(test)]
const SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS: usize = 2;
pub const NOWLEDGE_SEARCH_SCAN_FILTER_FIELDS: &[&str] = &[
    "id",
    "document_id",
    "kind",
    "external_id",
    "source_id",
    "space_id",
    "unit_type",
    "lifecycle_state",
    "importance",
    "confidence",
    "temporal_context",
    "created_at",
    "updated_at",
    "event_start",
    "event_end",
    "is_latest",
];

#[derive(Debug, Clone, PartialEq)]
pub struct SearchDocument {
    pub id: String,
    pub title: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub source_graph_commit_epoch: Option<u64>,
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
    let kind = node.labels.iter().find_map(|label_id| {
        catalog
            .label_name(*label_id)
            .and_then(search_projection_kind_from_label)
    })?;
    let external_id = string_property(node, "id")
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| node.id.0.to_string());
    Some(format!("{}:{external_id}", kind.as_str()))
}

pub fn search_projection_document_id_for_label_and_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    node_id: NodeId,
) -> Option<String> {
    let kind = search_projection_kind_from_label(label)?;
    let external_id = properties
        .get("id")
        .map(value_to_projection_string)
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| node_id.0.to_string());
    Some(format!("{}:{external_id}", kind.as_str()))
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
    pub pruned_document_count: usize,
    pub scanned_document_count: usize,
    pub persisted_segment_descriptor_used: bool,
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
    pub pruned_document_count: usize,
    pub scanned_document_count: usize,
    pub numeric_range_summary_used: bool,
    pub value_summary_used: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchRetrieverReport {
    pub name: String,
    pub backend: String,
    pub available: bool,
    pub input_candidate_set: SearchCandidateSetReport,
    pub candidate_count: usize,
    pub candidate_set: SearchRetrieverCandidateSetReport,
    pub fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: Vec<String>,
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
    pub rank_window: Option<usize>,
    pub fusion_weights: SearchFusionWeights,
    pub metadata_filters: BTreeMap<String, String>,
    pub policy_epoch: Option<u64>,
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

#[derive(Clone, Copy)]
enum VectorSearchBackend<'a> {
    Scalar,
    CompressedRequiredUnavailable,
    #[cfg(not(feature = "turbovec"))]
    _Lifetime(std::marker::PhantomData<&'a ()>),
    #[cfg(feature = "turbovec")]
    Turbovec(&'a turbovec_projection::TurbovecSearchProjection),
}

impl VectorSearchBackend<'_> {
    fn report_name(self) -> &'static str {
        match self {
            Self::Scalar => "scalar_vector_scan",
            Self::CompressedRequiredUnavailable => "compressed_vector_projection_required",
            #[cfg(not(feature = "turbovec"))]
            Self::_Lifetime(_) => "scalar_vector_scan",
            #[cfg(feature = "turbovec")]
            Self::Turbovec(_) => "turbovec_projection",
        }
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

#[derive(Debug, Default)]
pub struct SearchIndex {
    documents: BTreeMap<String, SearchDocument>,
    path: Option<PathBuf>,
    embedding_dimension: Option<usize>,
    embedding_manifest: Option<SearchEmbeddingManifest>,
    source_graph_commit_epoch: Option<u64>,
    marker_lines: RefCell<BTreeMap<String, Vec<String>>>,
    analyzer_lexicon: SearchAnalyzerLexicon,
    segment_descriptor: Option<SearchSegmentDescriptor>,
}

impl SearchIndex {
    pub fn in_memory() -> Self {
        Self::default()
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        fs::create_dir_all(path.as_ref())?;
        let mut index = Self {
            documents: BTreeMap::new(),
            path: Some(path.as_ref().to_path_buf()),
            embedding_dimension: None,
            embedding_manifest: None,
            source_graph_commit_epoch: None,
            marker_lines: RefCell::new(BTreeMap::new()),
            analyzer_lexicon: SearchAnalyzerLexicon::default(),
            segment_descriptor: None,
        };
        index.load_snapshot()?;
        index.load_or_rebuild_segment_descriptor()?;
        Ok(index)
    }

    pub fn with_analyzer_lexicon(mut self, analyzer_lexicon: SearchAnalyzerLexicon) -> Self {
        self.analyzer_lexicon = analyzer_lexicon;
        self
    }

    pub fn set_analyzer_lexicon(&mut self, analyzer_lexicon: SearchAnalyzerLexicon) {
        self.analyzer_lexicon = analyzer_lexicon;
    }

    pub fn upsert(&mut self, document: SearchDocument) -> Result<()> {
        if let Some(embedding) = &document.embedding {
            self.validate_or_set_dimension(embedding.len())?;
        }
        self.documents.insert(document.id.clone(), document);
        self.segment_descriptor = None;
        Ok(())
    }

    pub fn upsert_projection_row(&mut self, row: SearchProjectionRow) -> Result<()> {
        self.upsert(row.into_document())
    }

    pub fn delete(&mut self, id: &str) {
        self.documents.remove(id);
        self.segment_descriptor = None;
    }

    pub fn apply_projection_delta(
        &mut self,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
        let operation_count = delta.operation_count();
        if let Some(limit) = delta.max_operations {
            if operation_count > limit {
                return Err(SkeinError::Storage(format!(
                    "incremental projection update operation count {operation_count} exceeded configured limit {limit}"
                )));
            }
        }

        let mut next_embedding_dimension = self.embedding_dimension;
        for row in &delta.upserts {
            let Some(embedding) = &row.embedding else {
                continue;
            };
            let dimension = embedding.len();
            if let Some(manifest) = &self.embedding_manifest {
                if manifest.dimension != dimension {
                    return Err(SkeinError::Storage(format!(
                        "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                        manifest.dimension
                    )));
                }
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

        let before_document_count = self.documents.len();
        let source_graph_commit_epoch_before = self.source_graph_commit_epoch;
        let mut next_documents = self.documents.clone();
        let mut deleted_documents = 0;
        for id in delta.deletes {
            if next_documents.remove(&id).is_some() {
                deleted_documents += 1;
            }
        }
        let upserted_documents = delta.upserts.len();
        for row in delta.upserts {
            let document = row.into_document();
            next_documents.insert(document.id.clone(), document);
        }

        self.documents = next_documents;
        self.segment_descriptor = None;
        self.embedding_dimension = next_embedding_dimension;
        let source_graph_commit_epoch_updated = delta.source_graph_commit_epoch.is_some();
        if let Some(epoch) = delta.source_graph_commit_epoch {
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
        scheduler: &mut LocalQosScheduler,
        delta: SearchProjectionDelta,
    ) -> Result<SearchProjectionDeltaReport> {
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
        scheduler.finish(permit);
        result
    }

    pub fn document(&self, id: &str) -> Option<&SearchDocument> {
        self.documents.get(id)
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    #[cfg(feature = "turbovec")]
    pub fn build_turbovec_projection(
        &self,
        bit_width: usize,
    ) -> Result<Option<turbovec_projection::TurbovecSearchProjection>> {
        turbovec_projection::TurbovecSearchProjection::build_from_documents(
            self.documents.values(),
            bit_width,
        )
    }

    #[cfg(feature = "turbovec")]
    pub fn load_turbovec_projection(
        &self,
    ) -> Result<Option<turbovec_projection::TurbovecSearchProjection>> {
        let Some(path) = &self.path else {
            return Ok(None);
        };
        let artifact_path = path.join(SEARCH_TURBOVEC_PROJECTION_FILE);
        if !artifact_path.exists() {
            return Ok(None);
        }
        let projection =
            turbovec_projection::TurbovecSearchProjection::load_from_path(&artifact_path)?;
        self.validate_turbovec_projection_matches_documents(&projection)?;
        Ok(Some(projection))
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
        SearchProjectionFreshness {
            document_count: self.documents.len(),
            source_graph_commit_epoch: self.source_graph_commit_epoch,
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
        let compressed_vector_projection =
            search_projection_probe_compressed_vector_projection_report(self);

        serde_json::json!({
            "protocol": "skein-nowledge-search-projection-probe",
            "derived_projection": true,
            "document_count": self.documents.len(),
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
                "full_reindex_needed": freshness.full_reindex_needed,
                "full_reindex_reasons": freshness.full_reindex_reasons,
                "metadata_repair_needed": freshness.metadata_repair_needed,
                "metadata_repair_reasons": freshness.metadata_repair_reasons,
                "source_graph_commit_epoch": freshness.source_graph_commit_epoch,
            },
            "incremental_update": {
                "ready": has_documents && freshness.source_graph_commit_epoch.is_some(),
                "upsert_ready": has_documents,
                "delete_ready": has_documents,
                "watermark_ready": freshness.source_graph_commit_epoch.is_some(),
                "source_graph_commit_epoch": freshness.source_graph_commit_epoch,
            },
            "compressed_vector_projection": compressed_vector_projection,
            "predicate_pushdown": predicate_pushdown,
            "blocker_codes": search_projection_probe_blocker_codes(
                has_documents,
                has_text,
                has_vector,
                manifest.is_some(),
                model_matches,
                dimension_matches,
                &freshness,
            ),
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
        Ok(())
    }

    pub fn rebuild_from_graph(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
        options: SearchRebuildOptions,
    ) -> Result<SearchRebuildSummary> {
        let mut next_documents = BTreeMap::new();
        let mut scanned_nodes = 0;

        for node in store.scan_nodes(None) {
            scanned_nodes += 1;
            let Some(row) = projection_row_from_node(catalog, node) else {
                continue;
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
        }

        self.documents = next_documents;
        self.source_graph_commit_epoch = Some(store.commit_epoch());
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
    }

    pub fn rebuild_derived_artifacts(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
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

    pub fn rebuild_background_work_plan(
        &self,
        store: &GraphStore,
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

    pub fn rebuild_background_derived_artifacts(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        catalog: &Catalog,
        store: &GraphStore,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
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

    pub fn rebuild_scheduled_background_derived_artifacts(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        catalog: &Catalog,
        store: &GraphStore,
        options: SearchRebuildOptions,
    ) -> Result<SearchDerivedArtifactReport> {
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
        scheduler.finish(permit);
        result
    }

    pub fn repair_metadata_from_graph(
        &mut self,
        catalog: &Catalog,
        store: &GraphStore,
        options: MetadataRepairOptions,
    ) -> Result<MetadataRepairSummary> {
        let mut repairs = Vec::new();
        let mut scanned_nodes = 0;
        let mut missing_documents = 0;

        for node in store.scan_nodes(None) {
            scanned_nodes += 1;
            let Some(row) = projection_row_from_node(catalog, node) else {
                continue;
            };
            let document = row.into_document();
            if !self.documents.contains_key(&document.id) {
                missing_documents += 1;
                continue;
            }
            if options
                .max_rows
                .map(|limit| repairs.len() >= limit)
                .unwrap_or(false)
            {
                self.mark_metadata_repair_needed("metadata repair exceeded configured row limit")?;
                return Err(SkeinError::Storage(format!(
                    "metadata repair exceeded configured row limit after {} documents",
                    repairs.len()
                )));
            }
            repairs.push((document.id, document.metadata));
        }

        let repaired_documents = repairs.len();
        for (id, metadata) in repairs {
            if let Some(existing) = self.documents.get_mut(&id) {
                existing.metadata = metadata;
            }
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
    }

    pub fn metadata_repair_background_work_plan(
        &self,
        store: &GraphStore,
        hint: BackgroundWorkHint,
    ) -> Option<BackgroundWorkPlan> {
        let estimated_operations = store.scan_nodes(None).count();
        if estimated_operations == 0 {
            return None;
        }
        Some(BackgroundWorkPlan::background(
            WorkClass::Projection,
            estimated_operations,
            hint,
        ))
    }

    pub fn repair_background_metadata_from_graph(
        &mut self,
        policy: &LocalQosPolicy,
        state: &LocalQosState,
        catalog: &Catalog,
        store: &GraphStore,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
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

    pub fn repair_scheduled_background_metadata_from_graph(
        &mut self,
        scheduler: &mut LocalQosScheduler,
        catalog: &Catalog,
        store: &GraphStore,
        options: MetadataRepairOptions,
        estimated_operations: usize,
    ) -> Result<MetadataRepairSummary> {
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
        scheduler.finish(permit);
        result
    }

    fn rebuild_estimated_operations(&self, store: &GraphStore) -> usize {
        store.scan_nodes(None).count().max(self.documents.len())
    }

    pub fn checkpoint(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot_path = path.join(SEARCH_SNAPSHOT_FILE);
        let mut body = String::new();
        body.push_str("SKEIN_SEARCH_PROJECTION_V1\n");
        if let Some(epoch) = self.source_graph_commit_epoch {
            body.push_str(&format!("source_graph_commit_epoch\t{epoch}\n"));
        }
        if let Some(manifest) = &self.embedding_manifest {
            body.push_str(&format!(
                "embedding_manifest\t{}\t{}\t{}\n",
                encode_string(&manifest.model),
                encode_string(manifest.version.as_deref().unwrap_or_default()),
                manifest.dimension
            ));
        }
        if let Some(dimension) = self.embedding_dimension {
            body.push_str(&format!("embedding_dimension\t{dimension}\n"));
        }
        for document in self.documents.values() {
            body.push_str(&format!(
                "doc\t{}\t{}\t{}\t{}\t{}\n",
                encode_string(&document.id),
                encode_string(&document.title),
                encode_string(&document.content),
                encode_embedding(document.embedding.as_deref()),
                encode_metadata(&document.metadata),
            ));
        }
        let checksum = checksum_bytes(body.as_bytes());
        let data = format!("{body}checksum\t{checksum}\n");
        let tmp_path = snapshot_path.with_extension("skein.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            let encoded = encode_search_snapshot_text(&data)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
        }
        fs::rename(tmp_path, &snapshot_path)?;
        sync_parent_dir(&snapshot_path)?;
        self.write_segment_descriptor(path)?;
        #[cfg(feature = "turbovec")]
        self.write_turbovec_projection_artifact(path)?;
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
        self.search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            VectorSearchBackend::Scalar,
            None,
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
        if compressed_vector_search_mode == CompressedVectorSearchMode::Disabled {
            return self.search_with_options_using_vector_backend(
                query_text,
                query_embedding,
                mode,
                options,
                VectorSearchBackend::Scalar,
                None,
            );
        }

        #[cfg(feature = "turbovec")]
        {
            if mode != SearchMode::Text && query_embedding.is_some() {
                match self.load_turbovec_projection() {
                    Ok(Some(projection)) => {
                        return self.search_with_options_using_vector_backend(
                            query_text,
                            query_embedding,
                            mode,
                            options,
                            VectorSearchBackend::Turbovec(&projection),
                            None,
                        );
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if compressed_vector_search_mode == CompressedVectorSearchMode::Required {
                            return self.search_with_options_using_vector_backend(
                                query_text,
                                query_embedding,
                                mode,
                                options,
                                VectorSearchBackend::CompressedRequiredUnavailable,
                                Some(format!(
                                    "compressed vector projection required but unavailable: {error}"
                                )),
                            );
                        }
                        return self.search_with_options_using_vector_backend(
                            query_text,
                            query_embedding,
                            mode,
                            options,
                            VectorSearchBackend::Scalar,
                            Some(format!(
                                "compressed vector projection unavailable; fell back to scalar vector scan: {error}"
                            )),
                        );
                    }
                }
            }
        }

        if compressed_vector_search_mode == CompressedVectorSearchMode::Required
            && mode != SearchMode::Text
            && query_embedding.is_some()
        {
            return self.search_with_options_using_vector_backend(
                query_text,
                query_embedding,
                mode,
                options,
                VectorSearchBackend::CompressedRequiredUnavailable,
                None,
            );
        }

        self.search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            VectorSearchBackend::Scalar,
            None,
        )
    }

    #[cfg(feature = "turbovec")]
    pub fn search_with_turbovec_projection(
        &self,
        projection: &turbovec_projection::TurbovecSearchProjection,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
    ) -> SearchResultSet {
        self.search_with_options_using_vector_backend(
            query_text,
            query_embedding,
            mode,
            options,
            VectorSearchBackend::Turbovec(projection),
            None,
        )
    }

    fn search_with_options_using_vector_backend(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        vector_backend: VectorSearchBackend<'_>,
        vector_backend_fallback_reason: Option<String>,
    ) -> SearchResultSet {
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
        let mut predicate_pushdown = search_metadata_predicate_pushdown(&options.metadata_filters);
        let filtered = filter_search_documents_with_segment_pruning(
            &self.documents,
            &predicate_pushdown.predicates,
            self.segment_descriptor.as_ref(),
        );
        predicate_pushdown.report.segment_count = filtered.segment_count;
        predicate_pushdown.report.pruned_segment_count = filtered.pruned_segment_count;
        predicate_pushdown.report.scanned_segment_count = filtered.scanned_segment_count;
        predicate_pushdown.report.pruned_document_count = filtered.pruned_document_count;
        predicate_pushdown.report.scanned_document_count = filtered.scanned_document_count;
        predicate_pushdown.report.persisted_segment_descriptor_used =
            filtered.persisted_segment_descriptor_used;
        predicate_pushdown.report.field_summaries = filtered.field_summaries;
        let filtered_documents = filtered.documents;
        let filtered_document_count = filtered_documents.len();
        let candidate_set = SearchCandidateSetReport {
            id_space: "search_projection_document_id".to_string(),
            representation: "sorted_document_ids".to_string(),
            cardinality: filtered_document_count,
            exact: true,
            snapshot_source_graph_commit_epoch: self.source_graph_commit_epoch,
            policy_epoch: options.policy_epoch,
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
        let text_corpus = if text_available && mode != SearchMode::Vector {
            Some(TextCorpusStats::from_documents(
                filtered_documents.iter().copied(),
                &self.analyzer_lexicon,
            ))
        } else {
            None
        };
        let projection_freshness = self.projection_freshness();

        let vector_scores = if vector_available && mode != SearchMode::Text {
            vector_scores_for_backend(
                query_embedding.expect("vector_available requires query embedding"),
                &filtered_documents,
                vector_backend,
                limit,
                options.rank_window,
                &mut vector_fallback_reason_codes,
                &mut vector_fallback_reasons,
            )
        } else {
            BTreeMap::new()
        };
        let mut text_scores = BTreeMap::new();
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
                available: vector_available && mode != SearchMode::Text,
                input_candidate_set: candidate_set.clone(),
                candidate_count: vector_scores.len(),
                candidate_set: retriever_candidate_set_report(
                    vector_window_ranks.len(),
                    self.source_graph_commit_epoch,
                    options.policy_epoch,
                ),
                fallback_reason_codes: vector_fallback_reason_codes,
                fallback_reasons: vector_fallback_reasons,
                top_hit_ids: top_ranked_ids(&vector_window_ranks, limit),
                top_candidates: top_ranked_candidates(&vector_window_ranks, &vector_scores, limit),
            },
            SearchRetrieverReport {
                name: "text".to_string(),
                backend: "bm25_text".to_string(),
                available: text_available && mode != SearchMode::Vector,
                input_candidate_set: candidate_set.clone(),
                candidate_count: text_scores.len(),
                candidate_set: retriever_candidate_set_report(
                    text_window_ranks.len(),
                    self.source_graph_commit_epoch,
                    options.policy_epoch,
                ),
                fallback_reason_codes: text_fallback_reason_codes,
                fallback_reasons: text_fallback_reasons,
                top_hit_ids: top_ranked_ids(&text_window_ranks, limit),
                top_candidates: top_ranked_candidates(&text_window_ranks, &text_scores, limit),
            },
        ];
        let mut hits = Vec::new();
        for document in filtered_documents {
            let vector_score = vector_scores.get(&document.id).copied().unwrap_or(0.0);
            let text_score = text_scores.get(&document.id).copied().unwrap_or(0.0);
            let vector_rank = match mode {
                SearchMode::Hybrid => vector_window_ranks.get(&document.id).copied(),
                SearchMode::Vector | SearchMode::Text => vector_ranks.get(&document.id).copied(),
            };
            let text_rank = match mode {
                SearchMode::Hybrid => text_window_ranks.get(&document.id).copied(),
                SearchMode::Vector | SearchMode::Text => text_ranks.get(&document.id).copied(),
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
            if score > 0.0 {
                hits.push(SearchHit {
                    id: document.id.clone(),
                    score,
                    vector_score,
                    text_score,
                    rrf_score,
                    vector_rrf_score,
                    text_rrf_score,
                    vector_rank,
                    text_rank,
                    kind: document.metadata.get("kind").cloned().or_else(|| {
                        search_projection_kind_from_document_id(&document.id).map(str::to_string)
                    }),
                    external_id: document.metadata.get("external_id").cloned().or_else(|| {
                        search_projection_external_id_from_document_id(&document.id)
                            .map(str::to_string)
                    }),
                    source_id: document.metadata.get("source_id").cloned(),
                    matched_terms: matched_query_terms(
                        &query_terms,
                        document,
                        &self.analyzer_lexicon,
                    ),
                    matched_spans: matched_query_spans(
                        &query_terms,
                        document,
                        &self.analyzer_lexicon,
                    ),
                    fallback_reason_codes: fallback_reason_codes.clone(),
                    fallback_reasons: fallback_reasons.clone(),
                    projection_freshness: projection_freshness.clone(),
                });
            }
        }
        hits.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        let total_hits = hits.len();
        let truncated = total_hits > limit;
        hits.truncate(limit);
        let truncation_reasons = if truncated {
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
            &truncation_reasons,
            &fallback_reasons,
        );
        let empty_reason_codes = search_empty_reason_codes(
            hits.is_empty(),
            document_count,
            filtered_document_count,
            total_hits,
        );
        SearchResultSet {
            hits,
            total_hits,
            limit,
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
        }
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
        if let Some(manifest) = &self.embedding_manifest {
            if manifest.dimension != dimension {
                return Err(SkeinError::Storage(format!(
                    "embedding dimension mismatch: manifest expects {}, row has {dimension}",
                    manifest.dimension
                )));
            }
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
                    self.source_graph_commit_epoch =
                        Some(parse_u64(raw, "source graph commit epoch")?);
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
                    if let Some(manifest) = &self.embedding_manifest {
                        if manifest.dimension != dimension {
                            return Err(SkeinError::Storage(format!(
                                "embedding manifest dimension {} does not match snapshot dimension {dimension}",
                                manifest.dimension
                            )));
                        }
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
            Ok(Some(descriptor)) if descriptor.matches_documents(&self.documents) => {
                Some(descriptor)
            }
            Ok(_) | Err(_) => Some(SearchSegmentDescriptor::build(&self.documents)),
        };
        Ok(())
    }

    fn write_segment_descriptor(&self, path: &Path) -> Result<()> {
        let descriptor = SearchSegmentDescriptor::build(&self.documents);
        write_search_segment_descriptor(path, &descriptor)
    }

    #[cfg(feature = "turbovec")]
    fn write_turbovec_projection_artifact(&self, path: &Path) -> Result<()> {
        let artifact_path = path.join(SEARCH_TURBOVEC_PROJECTION_FILE);
        match self.build_turbovec_projection(SEARCH_TURBOVEC_PROJECTION_BIT_WIDTH) {
            Ok(Some(projection)) => projection.write_to_path(&artifact_path),
            Ok(None) | Err(_) => {
                remove_turbovec_projection_artifact_files(&artifact_path)?;
                Ok(())
            }
        }
    }

    #[cfg(feature = "turbovec")]
    fn validate_turbovec_projection_matches_documents(
        &self,
        projection: &turbovec_projection::TurbovecSearchProjection,
    ) -> Result<()> {
        if Some(projection.dimension()) != self.embedding_dimension {
            return Err(SkeinError::Storage(format!(
                "turbovec projection dimension {} does not match search projection dimension {:?}",
                projection.dimension(),
                self.embedding_dimension
            )));
        }
        let expected_ids = self.vector_document_ids();
        let actual_ids = projection.mapped_document_ids();
        if actual_ids != expected_ids {
            return Err(SkeinError::Storage(format!(
                "turbovec projection maps {} vector documents but search projection has {} vector documents",
                actual_ids.len(),
                expected_ids.len()
            )));
        }
        Ok(())
    }

    #[cfg(feature = "turbovec")]
    fn vector_document_ids(&self) -> BTreeSet<String> {
        self.documents
            .values()
            .filter(|document| document.embedding.is_some())
            .map(|document| document.id.clone())
            .collect()
    }

    fn marker_path(&self, name: &str) -> Option<PathBuf> {
        self.path.as_ref().map(|path| path.join(name))
    }

    fn append_marker(&self, name: &str, reason: &str) -> Result<()> {
        {
            let mut marker_lines = self.marker_lines.borrow_mut();
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
            .borrow_mut()
            .insert(name.to_string(), vec![reason.to_string()]);
        let Some(path) = self.marker_path(name) else {
            return Ok(());
        };
        fs::write(path, reason)?;
        Ok(())
    }

    fn clear_marker(&self, name: &str) -> Result<()> {
        self.marker_lines.borrow_mut().remove(name);
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
                .borrow()
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

fn search_projection_probe_predicate_pushdown_report(index: &SearchIndex) -> serde_json::Value {
    let segment_descriptor_ready = index
        .segment_descriptor
        .as_ref()
        .is_some_and(|descriptor| descriptor.matches_documents(&index.documents));
    serde_json::json!({
        "ready": true,
        "equality_ready": true,
        "in_list_ready": true,
        "not_in_list_ready": true,
        "range_ready": true,
        "row_filter_ready": true,
        "segment_pruning_ready": true,
        "numeric_min_max_ready": true,
        "persisted_segment_descriptor_ready": segment_descriptor_ready,
        "supported_ops": ["eq", "in", "not_in", "gt", "gte", "lt", "lte"],
        "scan_filter_fields": NOWLEDGE_SEARCH_SCAN_FILTER_FIELDS,
    })
}

#[cfg(feature = "turbovec")]
fn search_projection_probe_compressed_vector_projection_report(
    index: &SearchIndex,
) -> serde_json::Value {
    match index.load_turbovec_projection() {
        Ok(Some(projection)) => {
            return serde_json::json!({
                "engine": "turbovec",
                "compiled": true,
                "ready": true,
                "bit_width": projection.bit_width(),
                "dimension": projection.dimension(),
                "document_count": projection.document_count(),
                "supports_allowlist": true,
                "persisted_artifact_used": true,
                "artifact_rebuilt_from_snapshot": false,
                "blocker_codes": [],
            });
        }
        Ok(None) => {}
        Err(_) => {}
    }

    match index.build_turbovec_projection(SEARCH_TURBOVEC_PROJECTION_BIT_WIDTH) {
        Ok(Some(projection)) => serde_json::json!({
            "engine": "turbovec",
            "compiled": true,
            "ready": true,
            "bit_width": projection.bit_width(),
            "dimension": projection.dimension(),
            "document_count": projection.document_count(),
            "supports_allowlist": true,
            "persisted_artifact_used": false,
            "artifact_rebuilt_from_snapshot": true,
            "blocker_codes": [],
        }),
        Ok(None) => serde_json::json!({
            "engine": "turbovec",
            "compiled": true,
            "ready": false,
            "bit_width": SEARCH_TURBOVEC_PROJECTION_BIT_WIDTH,
            "dimension": serde_json::Value::Null,
            "document_count": 0,
            "supports_allowlist": true,
            "persisted_artifact_used": false,
            "artifact_rebuilt_from_snapshot": false,
            "blocker_codes": ["missing_vector_leg"],
        }),
        Err(error) => serde_json::json!({
            "engine": "turbovec",
            "compiled": true,
            "ready": false,
            "bit_width": SEARCH_TURBOVEC_PROJECTION_BIT_WIDTH,
            "dimension": serde_json::Value::Null,
            "document_count": 0,
            "supports_allowlist": true,
            "persisted_artifact_used": false,
            "artifact_rebuilt_from_snapshot": false,
            "blocker_codes": ["compressed_vector_projection_unavailable"],
            "error": error.to_string(),
        }),
    }
}

#[cfg(not(feature = "turbovec"))]
fn search_projection_probe_compressed_vector_projection_report(
    _index: &SearchIndex,
) -> serde_json::Value {
    serde_json::json!({
        "engine": "turbovec",
        "compiled": false,
        "ready": false,
        "bit_width": serde_json::Value::Null,
        "dimension": serde_json::Value::Null,
        "document_count": 0,
        "supports_allowlist": false,
        "persisted_artifact_used": false,
        "artifact_rebuilt_from_snapshot": false,
        "blocker_codes": ["turbovec_feature_disabled"],
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

fn value_to_projection_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(value) => value.to_string(),
        Value::Int(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::String(value) => value.clone(),
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

fn vector_scores_for_backend(
    query_embedding: &[f32],
    documents: &[&SearchDocument],
    backend: VectorSearchBackend<'_>,
    limit: usize,
    rank_window: Option<usize>,
    fallback_reason_codes: &mut Vec<SearchFallbackReasonCode>,
    fallback_reasons: &mut Vec<String>,
) -> BTreeMap<String, f64> {
    #[cfg(not(feature = "turbovec"))]
    let _ = (limit, rank_window);

    match backend {
        VectorSearchBackend::Scalar => scalar_vector_scores(query_embedding, documents),
        VectorSearchBackend::CompressedRequiredUnavailable => {
            fallback_reason_codes
                .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
            fallback_reasons.push(
                "compressed vector projection required but unavailable; scalar vector scan disabled"
                    .to_string(),
            );
            BTreeMap::new()
        }
        #[cfg(not(feature = "turbovec"))]
        VectorSearchBackend::_Lifetime(_) => unreachable!("lifetime marker is never constructed"),
        #[cfg(feature = "turbovec")]
        VectorSearchBackend::Turbovec(projection) => {
            let allowlist = documents
                .iter()
                .map(|document| document.id.clone())
                .collect::<BTreeSet<_>>();
            let candidate_limit = limit.max(rank_window.unwrap_or(0)).max(1);
            match projection.search(query_embedding, candidate_limit, Some(&allowlist)) {
                Ok(hits) => hits
                    .into_iter()
                    .filter(|hit| hit.score > 0.0)
                    .map(|hit| (hit.id, hit.score))
                    .collect(),
                Err(error) => {
                    fallback_reason_codes.push(SearchFallbackReasonCode::VectorIndexEmpty);
                    fallback_reasons.push(format!(
                        "compressed vector projection unavailable; fell back to scalar vector scan: {error}"
                    ));
                    scalar_vector_scores(query_embedding, documents)
                }
            }
        }
    }
}

fn scalar_vector_scores(
    query_embedding: &[f32],
    documents: &[&SearchDocument],
) -> BTreeMap<String, f64> {
    documents
        .iter()
        .filter_map(|document| {
            document
                .embedding
                .as_deref()
                .and_then(|embedding| cosine_similarity(query_embedding, embedding))
                .filter(|score| *score > 0.0)
                .map(|score| (document.id.clone(), score))
        })
        .collect()
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

fn search_empty_reasons(
    returned_empty: bool,
    document_count: usize,
    filtered_document_count: usize,
    total_hits: usize,
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
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "search_projection_document_id".to_string(),
        representation: "ranked_document_ids".to_string(),
        cardinality,
        exact: true,
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

fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub(crate) struct SearchMetadataPredicatePushdown {
    pub predicates: SearchPredicateSet,
    pub report: SearchPredicatePushdownReport,
}

pub(crate) fn search_metadata_predicate_pushdown(
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
        pruned_document_count: 0,
        scanned_document_count: 0,
        persisted_segment_descriptor_used: false,
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
    pruned_document_count: usize,
    scanned_document_count: usize,
    persisted_segment_descriptor_used: bool,
    field_summaries: Vec<SearchPredicateFieldPruningReport>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SearchNumericRange {
    min: f64,
    max: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchFilterSegmentSummary {
    document_count: usize,
    present_counts: BTreeMap<String, usize>,
    values: BTreeMap<String, BTreeSet<String>>,
    numeric_ranges: BTreeMap<String, SearchNumericRange>,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchSegmentDescriptor {
    target_documents: usize,
    document_count: usize,
    document_fingerprint: Option<u64>,
    segments: Vec<SearchSegmentDescriptorEntry>,
}

#[derive(Debug, Clone, PartialEq)]
struct SearchSegmentDescriptorEntry {
    first_document_id: String,
    last_document_id: String,
    document_count: usize,
    metadata: BTreeMap<String, SearchSegmentFieldSummary>,
}

#[derive(Debug, Clone, Default, PartialEq)]
struct SearchSegmentFieldSummary {
    present_count: usize,
    values: BTreeSet<String>,
    numeric_range: Option<SearchNumericRange>,
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
            pruned_document_count: 0,
            scanned_document_count: 0,
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
    let mut field_pruning = SearchFieldPruningAccumulator::new(predicates);
    let mut filtered_documents = Vec::new();
    let mut segment_documents = Vec::with_capacity(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
    let mut segment_count = 0;
    let mut pruned_segment_count = 0;
    let mut scanned_segment_count = 0;
    let mut pruned_document_count = 0;
    let mut scanned_document_count = 0;

    for document in documents.values() {
        segment_documents.push(document);
        if segment_documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
            filter_search_document_segment(
                &mut filtered_documents,
                &mut segment_count,
                &mut pruned_segment_count,
                &mut scanned_segment_count,
                &mut pruned_document_count,
                &mut scanned_document_count,
                &segment_documents,
                &predicate_fields,
                predicates,
                &mut field_pruning,
            );
            segment_documents.clear();
        }
    }

    if !segment_documents.is_empty() {
        filter_search_document_segment(
            &mut filtered_documents,
            &mut segment_count,
            &mut pruned_segment_count,
            &mut scanned_segment_count,
            &mut pruned_document_count,
            &mut scanned_document_count,
            &segment_documents,
            &predicate_fields,
            predicates,
            &mut field_pruning,
        );
    }

    FilteredSearchDocuments {
        documents: filtered_documents,
        segment_count,
        pruned_segment_count,
        scanned_segment_count,
        pruned_document_count,
        scanned_document_count,
        persisted_segment_descriptor_used: false,
        field_summaries: field_pruning.into_reports(),
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
        pruned_document_count,
        scanned_document_count,
        persisted_segment_descriptor_used: true,
        field_summaries: field_pruning.into_reports(),
    }
}

fn filter_search_document_segment<'a>(
    output: &mut Vec<&'a SearchDocument>,
    segment_count: &mut usize,
    pruned_segment_count: &mut usize,
    scanned_segment_count: &mut usize,
    pruned_document_count: &mut usize,
    scanned_document_count: &mut usize,
    segment_documents: &[&'a SearchDocument],
    predicate_fields: &BTreeSet<String>,
    predicates: &SearchPredicateSet,
    field_pruning: &mut SearchFieldPruningAccumulator,
) {
    *segment_count += 1;
    let summary = SearchFilterSegmentSummary::from_documents(segment_documents, predicate_fields);
    field_pruning.observe_in_memory_segment(&summary, predicates);
    if !summary.may_match_predicates(predicates) {
        *pruned_segment_count += 1;
        *pruned_document_count += segment_documents.len();
        return;
    }
    *scanned_segment_count += 1;
    *scanned_document_count += segment_documents.len();
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
    pruned_document_count: usize,
    scanned_document_count: usize,
    numeric_range_summary_used: bool,
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
                | SearchPredicateOp::Lte(_) => stats.numeric_range_summary_used = true,
            }
        }
        Self { fields }
    }

    fn observe_in_memory_segment(
        &mut self,
        summary: &SearchFilterSegmentSummary,
        predicates: &SearchPredicateSet,
    ) {
        self.observe_segment(predicates, summary.document_count, |field_predicates| {
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
        self.observe_segment(predicates, segment.document_count, |field_predicates| {
            field_predicates
                .iter()
                .all(|predicate| segment.may_match_predicate(predicate))
        });
    }

    fn observe_segment(
        &mut self,
        predicates: &SearchPredicateSet,
        document_count: usize,
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
                stats.scanned_document_count += document_count;
            } else {
                stats.pruned_segment_count += 1;
                stats.pruned_document_count += document_count;
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
                pruned_document_count: stats.pruned_document_count,
                scanned_document_count: stats.scanned_document_count,
                numeric_range_summary_used: stats.numeric_range_summary_used,
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
    }
}

impl SearchFilterSegmentSummary {
    fn from_documents(documents: &[&SearchDocument], fields: &BTreeSet<String>) -> Self {
        let mut present_counts = BTreeMap::new();
        let mut values = BTreeMap::<String, BTreeSet<String>>::new();
        let mut numeric_ranges = BTreeMap::<String, SearchNumericRange>::new();
        for document in documents {
            for field in fields {
                let Some(value) = search_document_field_value(document, field) else {
                    continue;
                };
                *present_counts.entry(field.clone()).or_insert(0) += 1;
                values
                    .entry(field.clone())
                    .or_default()
                    .insert(search_segment_summary_value(field, value));
                if let Some(number) = metadata_numeric_value(value) {
                    numeric_ranges
                        .entry(field.clone())
                        .and_modify(|range| *range = range.with_value(number))
                        .or_insert_with(|| SearchNumericRange::point(number));
                }
            }
        }
        Self {
            document_count: documents.len(),
            present_counts,
            values,
            numeric_ranges,
        }
    }

    fn may_match_predicates(&self, predicates: &SearchPredicateSet) -> bool {
        if predicates.is_unsatisfiable() {
            return false;
        }
        predicates
            .predicates()
            .iter()
            .all(|predicate| self.may_match_predicate(predicate))
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
            | SearchPredicateOp::Lte(expected) => {
                self.range_may_match(predicate.field().name(), predicate.op(), expected.as_str())
            }
        }
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

    fn range_may_match(&self, field: &str, op: &SearchPredicateOp, expected: &str) -> bool {
        if metadata_numeric_range_may_match(self.numeric_ranges.get(field).copied(), op, expected) {
            return true;
        }
        self.values
            .get(field)
            .is_some_and(|values| metadata_string_range_may_match(field, values, op, expected))
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

impl SearchSegmentDescriptor {
    fn build(documents: &BTreeMap<String, SearchDocument>) -> Self {
        let fields = search_segment_descriptor_fields(documents);
        let mut segments = Vec::new();
        let mut segment_documents = Vec::with_capacity(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS);
        for document in documents.values() {
            segment_documents.push(document);
            if segment_documents.len() == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
                segments.push(SearchSegmentDescriptorEntry::from_documents(
                    &segment_documents,
                    &fields,
                ));
                segment_documents.clear();
            }
        }
        if !segment_documents.is_empty() {
            segments.push(SearchSegmentDescriptorEntry::from_documents(
                &segment_documents,
                &fields,
            ));
        }
        Self {
            target_documents: SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS,
            document_count: documents.len(),
            document_fingerprint: Some(search_segment_descriptor_document_fingerprint(documents)),
            segments,
        }
    }

    fn matches_documents(&self, documents: &BTreeMap<String, SearchDocument>) -> bool {
        self.target_documents == SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS
            && self.document_count == documents.len()
            && self.document_fingerprint
                == Some(search_segment_descriptor_document_fingerprint(documents))
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
}

impl SearchSegmentDescriptorEntry {
    fn from_documents(documents: &[&SearchDocument], fields: &BTreeSet<String>) -> Self {
        let first_document_id = documents
            .first()
            .map(|document| document.id.clone())
            .unwrap_or_default();
        let last_document_id = documents
            .last()
            .map(|document| document.id.clone())
            .unwrap_or_default();
        let mut metadata = BTreeMap::<String, SearchSegmentFieldSummary>::new();
        metadata.extend(
            fields
                .iter()
                .map(|field| (field.clone(), SearchSegmentFieldSummary::default())),
        );
        for document in documents {
            for field in fields {
                let Some(value) = search_document_field_value(document, field) else {
                    continue;
                };
                let summary = metadata.entry(field.clone()).or_default();
                summary.present_count += 1;
                summary
                    .values
                    .insert(search_segment_summary_value(field, value));
                summary.update_numeric(value);
            }
        }
        Self {
            first_document_id,
            last_document_id,
            document_count: documents.len(),
            metadata,
        }
    }

    fn may_match_predicates(&self, predicates: &SearchPredicateSet) -> bool {
        if predicates.is_unsatisfiable() {
            return false;
        }
        predicates
            .predicates()
            .iter()
            .all(|predicate| self.may_match_predicate(predicate))
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
            | SearchPredicateOp::Lte(expected) => {
                self.range_may_match(predicate.field().name(), predicate.op(), expected.as_str())
            }
        }
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

    fn range_may_match(&self, field: &str, op: &SearchPredicateOp, expected: &str) -> bool {
        let Some(summary) = self.metadata.get(field) else {
            return false;
        };
        if metadata_numeric_range_may_match(summary.numeric_range, op, expected) {
            return true;
        }
        metadata_string_range_may_match(field, &summary.values, op, expected)
    }
}

impl SearchSegmentFieldSummary {
    fn update_numeric(&mut self, value: &str) {
        let Some(number) = metadata_numeric_value(value) else {
            return;
        };
        self.numeric_range = Some(match self.numeric_range {
            Some(range) => range.with_value(number),
            None => SearchNumericRange::point(number),
        });
    }
}

fn search_segment_descriptor_fields(
    documents: &BTreeMap<String, SearchDocument>,
) -> BTreeSet<String> {
    let mut fields = NOWLEDGE_SEARCH_SCAN_FILTER_FIELDS
        .iter()
        .map(|field| (*field).to_string())
        .collect::<BTreeSet<_>>();
    fields.extend(
        documents
            .values()
            .flat_map(|document| document.metadata.keys().cloned()),
    );
    fields
}

fn search_segment_descriptor_document_fingerprint(
    documents: &BTreeMap<String, SearchDocument>,
) -> u64 {
    let mut body = String::new();
    for document in documents.values() {
        body.push_str(&encode_string(&document.id));
        body.push('\t');
        body.push_str(&encode_metadata(&document.metadata));
        body.push('\n');
    }
    checksum_bytes(body.as_bytes())
}

fn search_document_matches_predicate(
    document: &SearchDocument,
    predicate: &SearchPredicate,
) -> bool {
    let actual = search_document_field_value(document, predicate.field().name());
    match predicate.op() {
        SearchPredicateOp::Eq(expected) => actual.is_some_and(|actual| {
            metadata_value_matches(predicate.field().name(), actual, expected.as_str())
        }),
        SearchPredicateOp::In(expected_values) => actual.is_some_and(|actual| {
            expected_values.iter().any(|expected| {
                metadata_value_matches(predicate.field().name(), actual, expected.as_str())
            })
        }),
        SearchPredicateOp::NotIn(excluded_values) => actual.is_none_or(|actual| {
            excluded_values.iter().all(|excluded| {
                !metadata_value_matches(predicate.field().name(), actual, excluded.as_str())
            })
        }),
        SearchPredicateOp::Gt(expected) => actual.is_some_and(|actual| {
            metadata_range_gt(predicate.field().name(), actual, expected.as_str())
        }),
        SearchPredicateOp::Gte(expected) => actual.is_some_and(|actual| {
            metadata_range_gte(predicate.field().name(), actual, expected.as_str())
        }),
        SearchPredicateOp::Lt(expected) => actual.is_some_and(|actual| {
            metadata_range_lt(predicate.field().name(), actual, expected.as_str())
        }),
        SearchPredicateOp::Lte(expected) => actual.is_some_and(|actual| {
            metadata_range_lte(predicate.field().name(), actual, expected.as_str())
        }),
    }
}

fn search_document_field_value<'a>(document: &'a SearchDocument, key: &str) -> Option<&'a str> {
    match key {
        "id" | SEARCH_DOCUMENT_ID_FIELD => Some(document.id.as_str()),
        "kind" => document
            .metadata
            .get(key)
            .map(String::as_str)
            .or_else(|| search_projection_kind_from_document_id(&document.id)),
        "external_id" => document
            .metadata
            .get(key)
            .map(String::as_str)
            .or_else(|| search_projection_external_id_from_document_id(&document.id)),
        "lifecycle_state" => document
            .metadata
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                (search_document_projection_kind(document) == Some("memory"))
                    .then_some(DEFAULT_MEMORY_LIFECYCLE_STATE)
            }),
        "is_latest" => document
            .metadata
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
            .or(Some(DEFAULT_IS_LATEST)),
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

fn search_document_projection_kind(document: &SearchDocument) -> Option<&'static str> {
    document
        .metadata
        .get("kind")
        .and_then(|value| normalized_projection_kind(value))
        .or_else(|| search_projection_normalized_kind_from_document_id(&document.id))
}

fn search_projection_normalized_kind_from_document_id(document_id: &str) -> Option<&'static str> {
    search_projection_kind_from_document_id(document_id).and_then(normalized_projection_kind)
}

fn search_projection_kind_from_document_id(document_id: &str) -> Option<&str> {
    let (kind, external_id) = document_id.split_once(':')?;
    (!kind.is_empty() && !external_id.is_empty()).then_some(kind)
}

fn search_projection_external_id_from_document_id(document_id: &str) -> Option<&str> {
    let (_kind, external_id) = document_id.split_once(':')?;
    (!external_id.is_empty()).then_some(external_id)
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
        _ => value.to_string(),
    }
}

fn normalize_metadata_filter_value(value: &str) -> String {
    value.trim().to_lowercase()
}

fn metadata_numeric_value(value: &str) -> Option<f64> {
    let number = value.parse::<f64>().ok()?;
    number.is_finite().then_some(number)
}

fn metadata_range_gt(field: &str, actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual > expected)
        || metadata_string_range_pair(field, actual, expected)
            .is_some_and(|(actual, expected)| actual > expected)
}

fn metadata_range_gte(field: &str, actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual >= expected)
        || metadata_string_range_pair(field, actual, expected)
            .is_some_and(|(actual, expected)| actual >= expected)
}

fn metadata_range_lt(field: &str, actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual < expected)
        || metadata_string_range_pair(field, actual, expected)
            .is_some_and(|(actual, expected)| actual < expected)
}

fn metadata_range_lte(field: &str, actual: &str, expected: &str) -> bool {
    metadata_numeric_pair(actual, expected).is_some_and(|(actual, expected)| actual <= expected)
        || metadata_string_range_pair(field, actual, expected)
            .is_some_and(|(actual, expected)| actual <= expected)
}

fn metadata_numeric_pair(actual: &str, expected: &str) -> Option<(f64, f64)> {
    Some((
        metadata_numeric_value(actual)?,
        metadata_numeric_value(expected)?,
    ))
}

fn metadata_string_range_may_match(
    field: &str,
    actual_values: &BTreeSet<String>,
    op: &SearchPredicateOp,
    expected: &str,
) -> bool {
    actual_values.iter().any(|actual| match op {
        SearchPredicateOp::Gt(_) => metadata_range_gt(field, actual, expected),
        SearchPredicateOp::Gte(_) => metadata_range_gte(field, actual, expected),
        SearchPredicateOp::Lt(_) => metadata_range_lt(field, actual, expected),
        SearchPredicateOp::Lte(_) => metadata_range_lte(field, actual, expected),
        SearchPredicateOp::Eq(_) | SearchPredicateOp::In(_) | SearchPredicateOp::NotIn(_) => true,
    })
}

fn metadata_string_range_pair(
    field: &str,
    actual: &str,
    expected: &str,
) -> Option<(String, String)> {
    if !metadata_string_range_field(field) {
        return None;
    }
    Some((
        normalize_metadata_date_for_ordering(actual)?,
        normalize_metadata_date_for_ordering(expected)?,
    ))
}

fn metadata_string_range_field(field: &str) -> bool {
    matches!(field, "event_start" | "event_end")
}

fn normalize_metadata_date_for_ordering(value: &str) -> Option<String> {
    let trimmed = value.trim();
    Some(match trimmed.len() {
        4 => format!("{trimmed}-01-01"),
        7 => format!("{trimmed}-01"),
        _ => trimmed.to_string(),
    })
}

fn metadata_numeric_range_may_match(
    range: Option<SearchNumericRange>,
    op: &SearchPredicateOp,
    expected: &str,
) -> bool {
    let Some(range) = range else {
        return false;
    };
    let Some(expected) = metadata_numeric_value(expected) else {
        return false;
    };
    match op {
        SearchPredicateOp::Gt(_) => range.max > expected,
        SearchPredicateOp::Gte(_) => range.max >= expected,
        SearchPredicateOp::Lt(_) => range.min < expected,
        SearchPredicateOp::Lte(_) => range.min <= expected,
        SearchPredicateOp::Eq(_) | SearchPredicateOp::In(_) | SearchPredicateOp::NotIn(_) => true,
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
    if query_terms.is_empty() {
        return Vec::new();
    }
    let mut spans = Vec::new();
    collect_matched_query_spans(
        "title",
        &document.title,
        query_terms,
        analyzer_lexicon,
        &mut spans,
    );
    collect_matched_query_spans(
        "content",
        &document.content,
        query_terms,
        analyzer_lexicon,
        &mut spans,
    );
    spans
}

fn collect_matched_query_spans(
    field: &str,
    text: &str,
    query_terms: &BTreeSet<String>,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    spans: &mut Vec<SearchMatchedSpan>,
) {
    let mut run_start = None::<usize>;
    for (index, ch) in text.char_indices() {
        if ch.is_alphanumeric() || ch == '_' {
            run_start.get_or_insert(index);
        } else if let Some(start) = run_start.take() {
            push_matched_query_spans(
                field,
                text,
                start,
                index,
                query_terms,
                analyzer_lexicon,
                spans,
            );
        }
    }
    if let Some(start) = run_start {
        push_matched_query_spans(
            field,
            text,
            start,
            text.len(),
            query_terms,
            analyzer_lexicon,
            spans,
        );
    }
}

fn push_matched_query_spans(
    field: &str,
    text: &str,
    start_byte: usize,
    end_byte: usize,
    query_terms: &BTreeSet<String>,
    analyzer_lexicon: &SearchAnalyzerLexicon,
    spans: &mut Vec<SearchMatchedSpan>,
) {
    let raw = &text[start_byte..end_byte];
    let matching_terms = identifier_tokens(raw, analyzer_lexicon)
        .into_iter()
        .filter(|term| query_terms.contains(term))
        .collect::<BTreeSet<_>>();
    for term in matching_terms {
        spans.push(SearchMatchedSpan {
            field: field.to_string(),
            start_byte,
            end_byte,
            text: raw.to_string(),
            term,
        });
    }
}

fn document_tokens(
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    let mut tokens = tokenize_list(&document.title, analyzer_lexicon);
    tokens.extend(tokenize_list(&document.title, analyzer_lexicon));
    tokens.extend(tokenize_list(&document.content, analyzer_lexicon));
    tokens.extend(searchable_metadata_tokens(document, analyzer_lexicon));
    tokens
}

fn searchable_metadata_tokens(
    document: &SearchDocument,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Vec<String> {
    ["kind", "external_id", "source_id", "space_id"]
        .into_iter()
        .filter_map(|key| document.metadata.get(key))
        .flat_map(|value| tokenize_list(value, analyzer_lexicon))
        .collect()
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
    let mut tokens = Vec::new();
    let mut previous_part = None::<String>;
    for raw in text.split(|ch: char| !ch.is_alphanumeric() && ch != '_') {
        let parts = identifier_parts(raw);
        if let (Some(previous), Some(first)) = (previous_part.as_ref(), parts.first()) {
            push_analyzed_token(&mut tokens, format!("{previous}_{first}"), analyzer_lexicon);
        }
        tokens.extend(identifier_tokens(raw, analyzer_lexicon));
        if let Some(last) = parts.last() {
            previous_part = Some(last.clone());
        }
    }
    tokens
}

fn normalized_alias_rule_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

fn normalized_stopword_terms(text: &str) -> Vec<String> {
    tokenize_list(text, &SearchAnalyzerLexicon::empty())
}

fn identifier_tokens(raw: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Vec::new();
    }
    let mut tokens = Vec::new();
    push_unique_token(&mut tokens, raw.to_lowercase(), analyzer_lexicon);
    for token in cjk_ngram_tokens(raw, analyzer_lexicon) {
        push_unique_token(&mut tokens, token, analyzer_lexicon);
    }
    let parts = identifier_parts(raw);
    for part in &parts {
        push_analyzed_token(&mut tokens, part.clone(), analyzer_lexicon);
    }
    for pair in parts.windows(2) {
        push_analyzed_token(&mut tokens, pair.join("_"), analyzer_lexicon);
    }
    tokens
}

fn cjk_ngram_tokens(raw: &str, analyzer_lexicon: &SearchAnalyzerLexicon) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut run = Vec::new();
    for ch in raw.chars() {
        if is_cjk_search_char(ch) {
            run.push(ch);
        } else {
            push_cjk_ngram_tokens(&mut tokens, &run, analyzer_lexicon);
            run.clear();
        }
    }
    push_cjk_ngram_tokens(&mut tokens, &run, analyzer_lexicon);
    tokens
}

fn push_cjk_ngram_tokens(
    tokens: &mut Vec<String>,
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

fn is_cjk_search_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x3040..=0x309F
            | 0x30A0..=0x30FF
            | 0xAC00..=0xD7AF
    )
}

fn identifier_parts(raw: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut previous_kind = IdentifierCharKind::Other;
    let chars = raw.chars().collect::<Vec<_>>();
    for (index, ch) in chars.iter().copied().enumerate() {
        if ch == '_' {
            push_identifier_part(&mut parts, &mut current);
            previous_kind = IdentifierCharKind::Other;
            continue;
        }
        let kind = IdentifierCharKind::from_char(ch);
        let next_kind = chars
            .get(index + 1)
            .copied()
            .map(IdentifierCharKind::from_char);
        if !current.is_empty()
            && ((previous_kind == IdentifierCharKind::Lower && kind == IdentifierCharKind::Upper)
                || (previous_kind == IdentifierCharKind::Upper
                    && kind == IdentifierCharKind::Upper
                    && next_kind == Some(IdentifierCharKind::Lower))
                || (previous_kind != IdentifierCharKind::Digit
                    && kind == IdentifierCharKind::Digit)
                || (previous_kind == IdentifierCharKind::Digit
                    && kind != IdentifierCharKind::Digit))
        {
            push_identifier_part(&mut parts, &mut current);
        }
        current.extend(ch.to_lowercase());
        previous_kind = kind;
    }
    push_identifier_part(&mut parts, &mut current);
    parts
}

fn push_identifier_part(parts: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        parts.push(std::mem::take(current));
    }
}

fn push_unique_token(
    tokens: &mut Vec<String>,
    token: String,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) {
    if !token.is_empty()
        && !analyzer_lexicon.is_stopword(&token)
        && !tokens.iter().any(|existing| existing == &token)
    {
        tokens.push(token);
    }
}

fn push_analyzed_token(
    tokens: &mut Vec<String>,
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

fn normalize_english_suffixes(token: &str) -> Vec<String> {
    if token.len() <= 4 || token.contains('_') || token.chars().any(|ch| ch.is_ascii_digit()) {
        return Vec::new();
    }
    if let Some(stem) = token.strip_suffix("ies") {
        if stem.len() >= 2 {
            return vec![format!("{stem}y")];
        }
    }
    if let Some(stem) = token.strip_suffix("ing") {
        if stem.len() >= 3 {
            return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
        }
    }
    if let Some(stem) = token.strip_suffix("ed") {
        if stem.len() >= 3 {
            return suffix_stem_variants(trim_doubled_suffix_consonant(stem));
        }
    }
    if let Some(stem) = token.strip_suffix('s') {
        if stem.len() >= 3 && !stem.ends_with('s') {
            return vec![stem.to_string()];
        }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IdentifierCharKind {
    Lower,
    Upper,
    Digit,
    Other,
}

impl IdentifierCharKind {
    fn from_char(ch: char) -> Self {
        if ch.is_ascii_lowercase() {
            Self::Lower
        } else if ch.is_ascii_uppercase() {
            Self::Upper
        } else if ch.is_ascii_digit() {
            Self::Digit
        } else {
            Self::Other
        }
    }
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (l, r) in left.iter().zip(right.iter()) {
        let l = f64::from(*l);
        let r = f64::from(*r);
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some((dot / (left_norm.sqrt() * right_norm.sqrt())).max(0.0))
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

fn write_search_segment_descriptor(
    path: &Path,
    descriptor: &SearchSegmentDescriptor,
) -> Result<()> {
    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let tmp_path = descriptor_path.with_extension("skein.tmp");
    let body = encode_search_segment_descriptor_body(descriptor);
    let checksum = checksum_bytes(body.as_bytes());
    let data = format!("{body}checksum\t{checksum}\n");
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(data.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(tmp_path, &descriptor_path)?;
    sync_parent_dir(&descriptor_path)?;
    Ok(())
}

#[cfg(feature = "turbovec")]
fn remove_turbovec_projection_artifact_files(artifact_path: &Path) -> Result<()> {
    remove_file_if_exists(artifact_path)?;
    remove_file_if_exists(
        &turbovec_projection::TurbovecSearchProjection::manifest_path_for(artifact_path),
    )
}

#[cfg(feature = "turbovec")]
fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn read_search_segment_descriptor(path: &Path) -> Result<Option<SearchSegmentDescriptor>> {
    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    if !descriptor_path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&descriptor_path)?;
    decode_search_segment_descriptor_text(&text).map(Some)
}

fn encode_search_segment_descriptor_body(descriptor: &SearchSegmentDescriptor) -> String {
    let mut body = String::new();
    body.push_str("SKEIN_SEARCH_SEGMENTS_V1\n");
    body.push_str(&format!(
        "target_documents\t{}\n",
        descriptor.target_documents
    ));
    body.push_str(&format!("document_count\t{}\n", descriptor.document_count));
    if let Some(fingerprint) = descriptor.document_fingerprint {
        body.push_str(&format!("document_fingerprint\t{fingerprint}\n"));
    }
    for segment in &descriptor.segments {
        body.push_str(&format!(
            "segment\t{}\t{}\t{}\n",
            encode_string(&segment.first_document_id),
            encode_string(&segment.last_document_id),
            segment.document_count
        ));
        for (field, summary) in &segment.metadata {
            let (numeric_min, numeric_max) = encode_search_numeric_range(summary.numeric_range);
            body.push_str(&format!(
                "field\t{}\t{}\t{}\t{}\t{}\n",
                encode_string(field),
                summary.present_count,
                encode_segment_values(&summary.values),
                numeric_min,
                numeric_max
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
    let mut document_fingerprint = None;
    let mut segments = Vec::new();
    let mut current_segment = None::<SearchSegmentDescriptorEntry>;

    for line in body.lines() {
        if line == "SKEIN_SEARCH_SEGMENTS_V1" {
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
            ["document_fingerprint", raw] => {
                document_fingerprint = Some(parse_u64(raw, "search segment document fingerprint")?);
            }
            ["segment", raw_first, raw_last, raw_count] => {
                if let Some(segment) = current_segment.take() {
                    segments.push(segment);
                }
                current_segment = Some(SearchSegmentDescriptorEntry {
                    first_document_id: decode_string(raw_first)?,
                    last_document_id: decode_string(raw_last)?,
                    document_count: parse_usize(raw_count, "search segment document count")?,
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

    Ok(SearchSegmentDescriptor {
        target_documents: target_documents.ok_or_else(|| {
            SkeinError::Storage("search segment descriptor missing target_documents".to_string())
        })?,
        document_count: document_count.ok_or_else(|| {
            SkeinError::Storage("search segment descriptor missing document_count".to_string())
        })?,
        document_fingerprint,
        segments,
    })
}

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

fn encode_search_numeric_range(range: Option<SearchNumericRange>) -> (String, String) {
    range
        .map(|range| (range.min.to_string(), range.max.to_string()))
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
        let byte = u8::from_str_radix(&input[offset..offset + 2], 16)
            .map_err(|_| SkeinError::Storage(format!("invalid hex string: {input}")))?;
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
    let header = format!(
        "{SEARCH_COMPRESSION_HEADER}\ncodec\tzstd\nuncompressed_checksum\t{uncompressed_checksum}\ncompressed_checksum\t{compressed_checksum}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n",
        text.len(),
        compressed.len()
    );
    let mut encoded = header.into_bytes();
    encoded.extend_from_slice(&compressed);
    Ok(encoded)
}

fn read_search_snapshot_text(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    if bytes.starts_with(SEARCH_COMPRESSION_HEADER.as_bytes()) {
        decode_search_snapshot_text(&bytes)
    } else {
        String::from_utf8(bytes).map_err(|error| {
            SkeinError::Storage(format!("search projection is not valid UTF-8: {error}"))
        })
    }
}

fn decode_search_snapshot_text(bytes: &[u8]) -> Result<String> {
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
    let mut codec = None;
    let mut compressed_checksum = None;
    let mut uncompressed_checksum = None;
    let mut compressed_len = None;
    let mut uncompressed_len = None;
    for line in header.lines() {
        if line == SEARCH_COMPRESSION_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
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
    let decoded = zstd::stream::decode_all(Cursor::new(payload)).map_err(|error| {
        SkeinError::Storage(format!(
            "search projection zstd decompression failed: {error}"
        ))
    })?;
    let expected_uncompressed_len = uncompressed_len.ok_or_else(|| {
        SkeinError::Storage(
            "search projection compressed envelope missing uncompressed_len".to_string(),
        )
    })?;
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
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn parse_u64(input: &str, name: &str) -> Result<u64> {
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
    use crate::schema::Catalog;
    use crate::store::GraphStore;

    #[test]
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
                pruned_document_count: 0,
                scanned_document_count: 2,
                numeric_range_summary_used: false,
                value_summary_used: true,
            }]
        );
    }

    #[test]
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
    fn search_with_options_treats_missing_memory_lifecycle_as_active() {
        let mut index = SearchIndex::in_memory();
        for (id, metadata) in [
            (
                "memory:0_deleted",
                BTreeMap::from([("lifecycle_state".to_string(), "deleted".to_string())]),
            ),
            (
                "memory:1_legacy",
                BTreeMap::from([("kind".to_string(), "memory".to_string())]),
            ),
            (
                "memory:2_active",
                BTreeMap::from([("lifecycle_state".to_string(), "ACTIVE".to_string())]),
            ),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "graph projection diagnostics".to_string(),
                    embedding: None,
                    metadata,
                })
                .unwrap();
        }

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state".to_string(),
                    "active".to_string(),
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
        assert!(hit_ids.contains("memory:1_legacy"));
        assert!(hit_ids.contains("memory:2_active"));
        assert!(!hit_ids.contains("memory:0_deleted"));
    }

    #[test]
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
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "created_at".to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["gt".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                pruned_document_count: 2,
                scanned_document_count: 1,
                numeric_range_summary_used: true,
                value_summary_used: false,
            }]
        );
    }

    #[test]
    fn search_with_options_applies_date_string_range_filters_before_ranking() {
        let mut index = SearchIndex::in_memory();
        for (id, event_start) in [
            ("memory:old_0", "2022"),
            ("memory:old_1", "2023-05"),
            ("memory:new", "2024-03-20"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "graph projection diagnostics".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([(
                        "event_start".to_string(),
                        event_start.to_string(),
                    )]),
                })
                .unwrap();
        }

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "event_start__gte".to_string(),
                    "2024-01".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.filtered_document_count, 1);
        assert_eq!(result.hits[0].id, "memory:new");
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
    }

    #[test]
    fn date_string_range_ordering_matches_legacy_partial_padding() {
        assert_eq!(
            normalize_metadata_date_for_ordering("2024"),
            Some("2024-01-01".to_string())
        );
        assert_eq!(
            normalize_metadata_date_for_ordering("2024-03"),
            Some("2024-03-01".to_string())
        );
        assert_eq!(
            normalize_metadata_date_for_ordering("abcd"),
            Some("abcd-01-01".to_string())
        );
        assert_eq!(
            normalize_metadata_date_for_ordering("2024-0x"),
            Some("2024-0x-01".to_string())
        );
        assert_eq!(
            normalize_metadata_date_for_ordering("not-a-date"),
            Some("not-a-date".to_string())
        );
    }

    #[test]
    fn search_ignores_stale_persisted_segment_descriptor_fingerprint() {
        let mut index = SearchIndex::in_memory();
        for (id, source_id) in [
            ("memory:0", "thread_1"),
            ("memory:1", "thread_1"),
            ("memory:2", "thread_2"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "graph projection diagnostics".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), source_id.to_string())]),
                })
                .unwrap();
        }
        let stale_descriptor = SearchSegmentDescriptor::build(&index.documents);
        index
            .upsert(SearchDocument {
                id: "memory:0".to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
            })
            .unwrap();
        index.segment_descriptor = Some(stale_descriptor);

        let result = index.search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_2".to_string(),
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
        assert!(hit_ids.contains("memory:0"));
        assert!(hit_ids.contains("memory:2"));
        assert!(
            !result
                .candidate_set
                .metadata_predicate_pushdown
                .persisted_segment_descriptor_used
        );
    }

    #[test]
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
    fn tokenizer_expands_graph_stream_and_projection_aliases() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "graph-lightning".to_string(),
                title: "GraphLightning publishes GraphStream and ContentStream".to_string(),
                content: "Checkpointed snapshots track projection freshness".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        let import_hits = index.search("bulk graph import", None, SearchMode::Text, 10);
        let export_hits = index.search("graph export", None, SearchMode::Text, 10);
        let value_hits = index.search("value stream", None, SearchMode::Text, 10);
        let checkpoint_hits =
            index.search_with_report("checkpoint freshness", None, SearchMode::Text, 10);
        let staleness_hits = index.search("projection staleness", None, SearchMode::Text, 10);

        assert_eq!(import_hits[0].id, "graph-lightning");
        assert_eq!(export_hits[0].id, "graph-lightning");
        assert_eq!(value_hits[0].id, "graph-lightning");
        assert_eq!(checkpoint_hits.hits[0].id, "graph-lightning");
        assert_eq!(staleness_hits[0].id, "graph-lightning");
        assert!(checkpoint_hits.hits[0]
            .matched_terms
            .iter()
            .any(|term| term == "checkpoint"));
    }

    #[test]
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
    fn projection_snapshot_round_trips() {
        let path = unique_test_dir("search_snapshot");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
                .unwrap();
            index.checkpoint().unwrap();
        }
        {
            let index = SearchIndex::open(&path).unwrap();
            let hits = index.search("adjacency", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10);
            assert_eq!(hits[0].id, "a");
        }
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn graph_rebuild_records_source_commit_epoch() {
        let path = unique_test_dir("search_source_graph_epoch");
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    #[cfg(feature = "turbovec")]
    fn projection_checkpoint_publishes_turbovec_artifact() {
        let path = unique_test_dir("search_turbovec_artifact_publish");
        let artifact_path = path.join(SEARCH_TURBOVEC_PROJECTION_FILE);
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .apply_embedding_manifest(SearchEmbeddingManifest {
                    model: "text-embedding-3-small".to_string(),
                    version: None,
                    dimension: 8,
                })
                .unwrap();
            index
                .upsert(doc(
                    "memory:a",
                    "Vector A",
                    "Compressed projection",
                    [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index
                .upsert(doc(
                    "memory:b",
                    "Vector B",
                    "Compressed projection",
                    [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                ))
                .unwrap();
            index.checkpoint().unwrap();
        }

        assert!(artifact_path.exists());
        assert!(
            turbovec_projection::TurbovecSearchProjection::manifest_path_for(&artifact_path)
                .exists()
        );

        let index = SearchIndex::open(&path).unwrap();
        let projection = index.load_turbovec_projection().unwrap().unwrap();
        let explicit_result = index.search_with_turbovec_projection(
            &projection,
            "",
            Some(&[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );
        let automatic_result = index.search_with_options_prefer_compressed_vector_projection(
            "",
            Some(&[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );
        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("text-embedding-3-small".to_string()),
            active_embedding_dimension: Some(8),
        });

        assert_eq!(explicit_result.hits[0].id, "memory:b");
        assert_eq!(automatic_result.hits[0].id, "memory:b");
        assert_eq!(
            automatic_result.retrievers[0].backend,
            "turbovec_projection"
        );
        assert_eq!(probe["compressed_vector_projection"]["ready"], true);
        assert_eq!(
            probe["compressed_vector_projection"]["persisted_artifact_used"],
            true
        );
        assert_eq!(
            probe["compressed_vector_projection"]["artifact_rebuilt_from_snapshot"],
            false
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn compressed_vector_projection_preference_falls_back_when_artifact_is_corrupt() {
        let path = unique_test_dir("search_turbovec_artifact_corrupt_fallback");
        let artifact_path = path.join(SEARCH_TURBOVEC_PROJECTION_FILE);
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
        std::fs::write(
            turbovec_projection::TurbovecSearchProjection::manifest_path_for(&artifact_path),
            b"not-json",
        )
        .unwrap();

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options_prefer_compressed_vector_projection(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        );

        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(result.retrievers[0].backend, "scalar_vector_scan");
        assert!(result.retrievers[0]
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorProjectionUnavailable));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn compressed_vector_projection_disabled_uses_scalar_even_when_artifact_exists() {
        let path = unique_test_dir("search_turbovec_disabled_uses_scalar");
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
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Disabled,
        );

        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(result.retrievers[0].backend, "scalar_vector_scan");
        assert!(result.retrievers[0].fallback_reason_codes.is_empty());

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn compressed_vector_projection_required_does_not_use_scalar_fallback() {
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
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        );

        assert!(result.hits.is_empty());
        assert_eq!(
            result.retrievers[0].backend,
            "compressed_vector_projection_required"
        );
        assert_eq!(result.retrievers[0].candidate_count, 0);
        assert!(result.retrievers[0]
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorProjectionUnavailable));
        assert!(result
            .fallback_reason_codes
            .contains(&SearchFallbackReasonCode::CompressedVectorProjectionUnavailable));
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn projection_checkpoint_removes_stale_turbovec_artifact_without_vectors() {
        let path = unique_test_dir("search_turbovec_artifact_cleanup");
        let artifact_path = path.join(SEARCH_TURBOVEC_PROJECTION_FILE);
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
            assert!(artifact_path.exists());
        }
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index.delete("memory:a");
            index.checkpoint().unwrap();
        }

        assert!(!artifact_path.exists());
        assert!(
            !turbovec_projection::TurbovecSearchProjection::manifest_path_for(&artifact_path)
                .exists()
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
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
        assert!(descriptor.contains("SKEIN_SEARCH_SEGMENTS_V1\n"));
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
        assert!(descriptor.contains("SKEIN_SEARCH_SEGMENTS_V1\n"));
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
                .field_summaries,
            vec![SearchPredicateFieldPruningReport {
                field: "created_at".to_string(),
                value_kind: "numeric_or_string".to_string(),
                operation_kinds: vec!["gte".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                pruned_document_count: 2,
                scanned_document_count: 1,
                numeric_range_summary_used: true,
                value_summary_used: false,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persisted_segment_descriptor_includes_nowledge_scan_filter_fields() {
        let path = unique_test_dir("search_segment_descriptor_nowledge_fields");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            index
                .upsert(SearchDocument {
                    id: "memory:visible".to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor nowledge fields".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
                })
                .unwrap();
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        let segment = &descriptor.segments[0];

        for field in NOWLEDGE_SEARCH_SCAN_FILTER_FIELDS {
            assert!(
                segment.metadata.contains_key(*field),
                "missing scan field {field}"
            );
        }
        assert_eq!(
            segment
                .metadata
                .get("unit_type")
                .map(|summary| summary.present_count),
            Some(0)
        );
        assert_eq!(
            segment
                .metadata
                .get("lifecycle_state")
                .map(|summary| summary.present_count),
            Some(1)
        );
        assert_eq!(
            segment
                .metadata
                .get("lifecycle_state")
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from(["active".to_string()]))
        );
        assert_eq!(
            segment
                .metadata
                .get("temporal_context")
                .map(|summary| summary.present_count),
            Some(0)
        );
        assert_eq!(
            segment
                .metadata
                .get("source_id")
                .map(|summary| summary.present_count),
            Some(1)
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persisted_segment_descriptor_prunes_document_id_filters() {
        let path = unique_test_dir("search_segment_descriptor_document_id");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for id in ["memory:0_old", "memory:1_old", "memory:2_new"] {
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
                "memory:0_old".to_string(),
                "memory:1_old".to_string()
            ]))
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor document id retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "document_id__in".to_string(),
                    r#"["memory:2_new"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:2_new");
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
                pruned_document_count: 2,
                scanned_document_count: 1,
                numeric_range_summary_used: false,
                value_summary_used: true,
            }]
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persisted_segment_descriptor_prunes_kind_and_external_id_from_document_id() {
        let path = unique_test_dir("search_segment_descriptor_derived_ids");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for id in ["memory:0_old", "memory:1_old", "entity:2_new"] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor derived id retrieval".to_string(),
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
                .get("kind")
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from(["entity".to_string(), "memory".to_string()]))
        );
        assert_eq!(
            descriptor.segments[1]
                .metadata
                .get("external_id")
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from(["1_old".to_string()]))
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor derived id retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "external_id".to_string(),
                    "1_old".to_string(),
                )]),
                policy_epoch: None,
            },
        );

        assert_eq!(result.total_hits, 1);
        assert_eq!(result.hits[0].id, "memory:1_old");
        assert_eq!(result.hits[0].kind.as_deref(), Some("memory"));
        assert_eq!(result.hits[0].external_id.as_deref(), Some("1_old"));
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

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
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
                pruned_document_count: 2,
                scanned_document_count: 1,
                numeric_range_summary_used: false,
                value_summary_used: true,
            }]
        );
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn persisted_segment_descriptor_prunes_active_lifecycle_with_legacy_memory_defaults() {
        let path = unique_test_dir("search_segment_descriptor_lifecycle_defaults");
        {
            let mut index = SearchIndex::open(&path).unwrap();
            for (id, metadata) in [
                (
                    "memory:0_deleted",
                    BTreeMap::from([("lifecycle_state".to_string(), "deleted".to_string())]),
                ),
                (
                    "memory:0_forgotten",
                    BTreeMap::from([("lifecycle_state".to_string(), "forgotten".to_string())]),
                ),
                ("memory:1_legacy", BTreeMap::new()),
                (
                    "memory:1_active",
                    BTreeMap::from([("lifecycle_state".to_string(), "ACTIVE".to_string())]),
                ),
            ] {
                index
                    .upsert(SearchDocument {
                        id: id.to_string(),
                        title: "Graph memory".to_string(),
                        content: "segment descriptor lifecycle defaults".to_string(),
                        embedding: None,
                        metadata,
                    })
                    .unwrap();
            }
            index.checkpoint().unwrap();
        }

        let descriptor =
            std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
        let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
        assert_eq!(
            descriptor.segments[1]
                .metadata
                .get("lifecycle_state")
                .map(|summary| summary.values.clone()),
            Some(BTreeSet::from(["active".to_string()]))
        );

        let index = SearchIndex::open(&path).unwrap();
        let result = index.search_with_options(
            "segment descriptor lifecycle defaults",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state".to_string(),
                    "active".to_string(),
                )]),
                policy_epoch: None,
            },
        );
        let hit_ids = result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<BTreeSet<_>>();

        assert_eq!(result.total_hits, 2);
        assert!(hit_ids.contains("memory:1_legacy"));
        assert!(hit_ids.contains("memory:1_active"));
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
                operation_kinds: vec!["eq".to_string()],
                segment_count: 2,
                pruned_segment_count: 1,
                scanned_segment_count: 1,
                pruned_document_count: 2,
                scanned_document_count: 2,
                numeric_range_summary_used: false,
                value_summary_used: true,
            }]
        );

        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
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
        let result = index.search_with_options(
            "segment descriptor recovery",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
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
    fn nowledge_search_projection_probe_reports_ready_shape() {
        let mut index = SearchIndex::in_memory();
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

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: Some("bge-m3".to_string()),
            active_embedding_dimension: Some(2),
        });

        assert_eq!(probe["derived_projection"], true);
        assert_eq!(probe["document_count"], 6);
        assert_eq!(probe["tables"].as_array().unwrap().len(), 6);
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
        assert_eq!(probe["compressed_vector_projection"]["engine"], "turbovec");
        assert_eq!(probe["blocker_codes"], serde_json::json!([]));
    }

    #[test]
    #[cfg(not(feature = "turbovec"))]
    fn nowledge_search_projection_probe_reports_turbovec_disabled_by_default() {
        let index = SearchIndex::in_memory();

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: None,
            active_embedding_dimension: None,
        });

        assert_eq!(probe["compressed_vector_projection"]["engine"], "turbovec");
        assert_eq!(probe["compressed_vector_projection"]["compiled"], false);
        assert_eq!(probe["compressed_vector_projection"]["ready"], false);
        assert_eq!(
            probe["compressed_vector_projection"]["blocker_codes"],
            serde_json::json!(["turbovec_feature_disabled"])
        );
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn nowledge_search_projection_probe_reports_turbovec_ready_when_feature_enabled() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:turbovec".to_string(),
                title: "Turbovec projection".to_string(),
                content: "Compressed vector backend".to_string(),
                embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                metadata: BTreeMap::from([
                    ("kind".to_string(), "memory".to_string()),
                    ("external_id".to_string(), "turbovec".to_string()),
                ]),
            })
            .unwrap();

        let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
            active_embedding_model: None,
            active_embedding_dimension: Some(8),
        });

        assert_eq!(probe["compressed_vector_projection"]["engine"], "turbovec");
        assert_eq!(probe["compressed_vector_projection"]["compiled"], true);
        assert_eq!(probe["compressed_vector_projection"]["ready"], true);
        assert_eq!(probe["compressed_vector_projection"]["bit_width"], 4);
        assert_eq!(probe["compressed_vector_projection"]["dimension"], 8);
        assert_eq!(probe["compressed_vector_projection"]["document_count"], 1);
        assert_eq!(
            probe["compressed_vector_projection"]["supports_allowlist"],
            true
        );
        assert_eq!(
            probe["compressed_vector_projection"]["blocker_codes"],
            serde_json::json!([])
        );
    }

    #[test]
    #[cfg(feature = "turbovec")]
    fn turbovec_projection_executes_vector_search_with_filter_allowlist() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(SearchDocument {
                id: "memory:a".to_string(),
                title: "Vector A".to_string(),
                content: String::new(),
                embedding: Some(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                metadata: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:b".to_string(),
                title: "Vector B".to_string(),
                content: String::new(),
                embedding: Some(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
                metadata: BTreeMap::from([("space_id".to_string(), "blocked".to_string())]),
            })
            .unwrap();
        let projection = index.build_turbovec_projection(4).unwrap().unwrap();

        let result = index.search_with_turbovec_projection(
            &projection,
            "",
            Some(&[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
                policy_epoch: Some(42),
            },
        );

        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].id, "memory:a");
        assert_eq!(result.retrievers[0].name, "vector");
        assert_eq!(result.retrievers[0].backend, "turbovec_projection");
        assert!(result.retrievers[0].available);
        assert_eq!(result.candidate_set.cardinality, 1);
        assert_eq!(result.candidate_set.filtered_out_count, 1);
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
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
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
    fn graph_rebuild_falls_back_for_empty_node_ids() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    fn graph_rebuild_source_id_fallback_skips_empty_values() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    fn full_rebuild_projects_graph_nodes() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
    fn background_search_projection_rebuild_uses_qos_admission() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    fn scheduled_background_search_projection_rebuild_releases_budget_on_rebuild_error() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .rebuild_scheduled_background_derived_artifacts(
                &mut scheduler,
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
        assert_eq!(
            index.projection_freshness().source_graph_commit_epoch,
            Some(7)
        );
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
    fn scheduled_background_projection_delta_tracks_running_budget() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
            .unwrap();
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(2),
            max_total_background_operations: Some(2),
            ..LocalQosPolicy::default()
        });

        let report = index
            .apply_scheduled_background_projection_delta(
                &mut scheduler,
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 3))
            .unwrap();

        let error = index
            .apply_scheduled_background_projection_delta(
                &mut scheduler,
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

        scheduler.finish(running);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .apply_scheduled_background_projection_delta(
                &mut scheduler,
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
        let mut store = GraphStore::in_memory();
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
    fn metadata_repair_background_work_plan_uses_projection_lane() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    fn background_metadata_repair_uses_qos_admission() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
    fn scheduled_background_metadata_repair_tracks_running_budget() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..LocalQosPolicy::default()
        });

        let summary = index
            .repair_scheduled_background_metadata_from_graph(
                &mut scheduler,
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
    fn scheduled_background_metadata_repair_defers_when_scheduler_is_full() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(4),
            max_total_background_operations: Some(4),
            ..LocalQosPolicy::default()
        });
        let running = scheduler
            .try_start(WorkRequest::background(WorkClass::Analytics, 3))
            .unwrap();

        let error = index
            .repair_scheduled_background_metadata_from_graph(
                &mut scheduler,
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

        scheduler.finish(running);
        assert_eq!(scheduler.state().running_background_operations, 0);
    }

    #[test]
    fn scheduled_background_metadata_repair_releases_budget_on_repair_error() {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::in_memory();
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
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        let error = index
            .repair_scheduled_background_metadata_from_graph(
                &mut scheduler,
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
        let mut store = GraphStore::in_memory();
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
