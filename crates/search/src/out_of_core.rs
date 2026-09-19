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
    manifest_generation as lexical_manifest_generation, LexicalMiniDelta, LexicalProjectionConfig,
    LexicalProjectionReader, DEFAULT_MAX_MANIFEST_BYTES, MANIFEST_FILE,
};
use super::{
    checksum_bytes, cosine_similarity, decode_embedding, decode_metadata,
    decode_search_segment_descriptor_text, decode_search_snapshot_text_bounded, decode_string,
    encode_embedding, encode_metadata, encode_search_snapshot_text, encode_string,
    lexical_analyzer_digest, lexical_documents_digest, matched_query_spans_bounded,
    matched_query_terms, ranked_scores, read_search_segment_descriptor,
    retriever_candidate_set_report, rrf_child_score, search_document_matches_predicates,
    search_empty_reason_codes, search_empty_reasons, search_metadata_predicate_pushdown,
    top_ranked_candidates, top_ranked_ids, weighted_rrf_score, window_ranks,
    CompressedVectorSearchMode, SearchAccessControlContext, SearchAnalyzerLexicon,
    SearchCandidateSetReport, SearchDocument, SearchEmbeddingManifest, SearchFallbackReasonCode,
    SearchFieldPruningAccumulator, SearchHit, SearchIndex, SearchMode, SearchPageWindow,
    SearchPredicatePushdownReport, SearchProjectionFreshness, SearchQueryOptions, SearchResultSet,
    SearchRetrieverReport, SearchScoredCandidate, SearchSegmentDescriptor,
    SearchSegmentDescriptorEntry, SearchTruncationReasonCode, VectorSearchExecutionOptions,
    FULL_REINDEX_MARKER, METADATA_REPAIR_MARKER, SEARCH_SEGMENT_DESCRIPTOR_FILE,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use crate::bounded_file::read_bounded_file;
use crate::error::{HawDBError, Result};
#[cfg(test)]
use crate::{decode_search_segment_documents_bounded, validate_search_segment_documents};
use crate::{RuntimeCapabilities, RuntimeCapability, SearchLexicalTermPolicy};
use hawdb_storage::durable_replace_file;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod generation_writer;
#[cfg(test)]
pub(crate) use generation_writer::analyzer_read_evidence;
mod hydration;
mod publish_lease;
mod vector_serving;
pub use generation_writer::{
    GovernedSearchGenerationUpdate, GovernedSearchGenerationWriter, SearchGenerationAdmission,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationBuildReport,
    SearchOutOfCoreGenerationUpdate, SearchOutOfCoreGenerationWriter,
};
pub(super) use publish_lease::SearchProjectionPublishLease;
#[cfg(feature = "vector-search")]
use vector_serving::vector_projection_error;
use vector_serving::VectorScoreScan;

const OUT_OF_CORE_MANIFEST_FILE: &str = "search_projection.out_of_core.manifest.hawdb";
const OUT_OF_CORE_FORMAT: &str = "HAWDB_SEARCH_OUT_OF_CORE_V1";
const OUT_OF_CORE_LAYOUT_FORMAT: &str = "HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V1";
const MAX_OUT_OF_CORE_MANIFEST_BYTES: u64 = 1024 * 1024;
const MAX_MARKER_BYTES: u64 = 64 * 1024;
const CANDIDATE_FILE_HEADER: &[u8; 8] = b"SKNCAND1";
static CANDIDATE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOutOfCoreConfig {
    pub max_compressed_segment_bytes: NonZeroU64,
    pub max_uncompressed_segment_bytes: NonZeroU64,
    pub max_descriptor_bytes: NonZeroU64,
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
    pub hydrated_documents: usize,
    pub hydrated_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutOfCoreOutput {
    pub result: SearchResultSet,
    pub metrics: SearchOutOfCoreMetrics,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutOfCoreHydrationOutput {
    pub documents: Vec<SearchDocument>,
    pub metrics: SearchOutOfCoreMetrics,
}

#[derive(Debug)]
pub struct SearchOutOfCoreReader {
    root: PathBuf,
    config: SearchOutOfCoreConfig,
    analyzer_lexicon: SearchAnalyzerLexicon,
    manifest: SearchOutOfCoreManifestBody,
    descriptor: SearchSegmentDescriptor,
    payload: Arc<File>,
    metadata_payload: Arc<File>,
    vector_payload: Arc<File>,
    layout: SearchOutOfCoreLayoutBody,
    lexical_projection: Arc<LexicalProjectionReader>,
    lexical_term_policy: SearchLexicalTermPolicy,
    #[cfg(feature = "vector-search")]
    rabitq_projection: Option<Arc<hawdb_vector_projection::FileProjection>>,
    runtime_capabilities: RuntimeCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchOutOfCoreManifestBody<S = String> {
    format: S,
    generation: u64,
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
    import_source_graph_commit_epoch: Option<u64>,
    embedding_model: Option<S>,
    embedding_version: Option<S>,
    embedding_dimension: Option<usize>,
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
    vector_ordinal: u64,
    id: String,
    embedding: Vec<f32>,
}

#[derive(Clone, Copy)]
struct SearchOutOfCoreExecutionContext<'a> {
    access_control: Option<&'a SearchAccessControlContext>,
    compressed_vector_search_mode: CompressedVectorSearchMode,
    vector_execution_options: VectorSearchExecutionOptions<'a>,
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
        let envelope: SearchOutOfCoreManifestEnvelope =
            serde_json::from_slice(bytes).map_err(|error| {
                HawDBError::Storage(format!("invalid search out-of-core manifest: {error}"))
            })?;
        let body = serde_json::to_vec(&envelope.body).map_err(|error| {
            HawDBError::Storage(format!(
                "failed to verify search out-of-core manifest: {error}"
            ))
        })?;
        if checksum_bytes(&body) != envelope.checksum {
            return Err(HawDBError::Storage(
                "search out-of-core manifest checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate_names()?;
        Ok(envelope.body)
    }
}

impl<S: AsRef<str>> SearchOutOfCoreManifestBody<S> {
    fn validate_names(&self) -> Result<()> {
        if self.format.as_ref() != OUT_OF_CORE_FORMAT || self.generation == 0 {
            return Err(HawDBError::Storage(
                "search out-of-core manifest header is invalid".to_string(),
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
        let envelope: SearchOutOfCoreLayoutEnvelope =
            serde_json::from_slice(bytes).map_err(|error| {
                HawDBError::Storage(format!("invalid search out-of-core layout: {error}"))
            })?;
        let body = serde_json::to_vec(&envelope.body).map_err(|error| {
            HawDBError::Storage(format!(
                "failed to verify search out-of-core layout: {error}"
            ))
        })?;
        if checksum_bytes(&body) != envelope.checksum {
            return Err(HawDBError::Storage(
                "search out-of-core layout checksum mismatch".to_string(),
            ));
        }
        Ok(envelope.body)
    }

    fn validate(
        &self,
        manifest: &SearchOutOfCoreManifestBody,
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
        let root = path.as_ref().to_path_buf();
        let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
        let manifest_bytes = read_bounded_file(&manifest_path, MAX_OUT_OF_CORE_MANIFEST_BYTES)?;
        let manifest = SearchOutOfCoreManifestBody::decode(&manifest_bytes)?;

        let descriptor_bytes = read_bound_artifact(
            &root.join(&manifest.descriptor_file),
            manifest.descriptor_len,
            manifest.descriptor_checksum,
            config.max_descriptor_bytes.get(),
            "search segment descriptor",
        )?;
        let descriptor_text = std::str::from_utf8(&descriptor_bytes).map_err(|error| {
            HawDBError::Storage(format!("search segment descriptor is not UTF-8: {error}"))
        })?;
        let descriptor = decode_search_segment_descriptor_text(descriptor_text)?;
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

        let payload_path = root.join(&manifest.payload_file);
        let payload = File::open(&payload_path)?;
        if payload.metadata()?.len() != manifest.payload_len {
            return Err(HawDBError::Storage(
                "search out-of-core payload length mismatch".to_string(),
            ));
        }

        let layout_bytes = read_bound_artifact(
            &root.join(&manifest.layout_file),
            manifest.layout_len,
            manifest.layout_checksum,
            config.max_descriptor_bytes.get(),
            "search out-of-core layout",
        )?;
        let layout = SearchOutOfCoreLayoutBody::decode(&layout_bytes)?;
        layout.validate(
            &manifest,
            &descriptor,
            config.max_compressed_segment_bytes.get(),
        )?;
        if manifest.rabitq_vector_document_count
            != manifest.rabitq_artifact_file.as_ref().map(|_| {
                layout
                    .segments
                    .iter()
                    .map(|segment| segment.vectors.entry_count)
                    .sum()
            })
        {
            return Err(HawDBError::Storage(
                "search out-of-core RaBitQ vector count does not match the sidecar layout"
                    .to_string(),
            ));
        }

        let metadata_payload = open_exact_length_artifact(
            &root.join(&manifest.metadata_payload_file),
            manifest.metadata_payload_len,
            "search out-of-core metadata payload",
        )?;
        let vector_payload = open_exact_length_artifact(
            &root.join(&manifest.vector_payload_file),
            manifest.vector_payload_len,
            "search out-of-core vector payload",
        )?;

        let lexical_manifest_path = root.join(&manifest.lexical_manifest_file);
        let lexical_manifest_bytes = read_bound_artifact(
            &lexical_manifest_path,
            manifest.lexical_manifest_len,
            manifest.lexical_manifest_checksum,
            config.max_lexical_manifest_bytes.get(),
            "search lexical manifest",
        )?;
        let lexical_config = LexicalProjectionConfig {
            max_manifest_bytes: config.max_lexical_manifest_bytes,
            max_term_bytes: lexical_term_policy.max_term_bytes(),
            max_query_score_entries: config.max_score_entries,
            ..LexicalProjectionConfig::default()
        };
        let lexical_projection = LexicalProjectionReader::load_manifest_bytes(
            &root,
            &lexical_manifest_bytes,
            manifest.source_graph_commit_epoch,
            lexical_analyzer_digest(&analyzer_lexicon),
            manifest.documents_digest,
            lexical_config,
        )?
        .ok_or_else(|| {
            HawDBError::Storage(
                "published search lexical projection does not match the out-of-core manifest"
                    .to_string(),
            )
        })?;
        drop(lexical_manifest_bytes);

        #[cfg(feature = "vector-search")]
        let rabitq_projection = open_rabitq_projection(
            &root,
            &manifest,
            config.max_vector_search_working_bytes.get(),
        )?;
        #[cfg(not(feature = "vector-search"))]
        verify_rabitq_artifact(&root, &manifest)?;

        Ok(Self {
            root,
            config,
            analyzer_lexicon,
            manifest,
            descriptor,
            payload: Arc::new(payload),
            metadata_payload: Arc::new(metadata_payload),
            vector_payload: Arc::new(vector_payload),
            layout,
            lexical_projection,
            lexical_term_policy,
            #[cfg(feature = "vector-search")]
            rabitq_projection,
            runtime_capabilities: crate::compiled_runtime_capabilities(),
        })
    }

    pub fn lexical_term_policy(&self) -> SearchLexicalTermPolicy {
        self.lexical_term_policy
    }

    /// Changes admission for subsequent queries and prepared updates.
    ///
    /// Lowering below the open generation's actual term requirement fails and
    /// leaves the previous policy intact. Exclusive access prevents changes
    /// during a query; already prepared updates keep their own snapshot.
    pub fn set_lexical_term_policy(&mut self, policy: SearchLexicalTermPolicy) -> Result<()> {
        self.lexical_projection
            .validate_term_limit(policy.max_term_bytes())?;
        self.lexical_term_policy = policy;
        Ok(())
    }

    pub fn document_count(&self) -> usize {
        self.manifest.document_count
    }

    pub fn projection_payload_bytes(&self) -> u64 {
        self.manifest
            .descriptor_len
            .saturating_add(self.manifest.payload_len)
            .saturating_add(self.manifest.metadata_payload_len)
            .saturating_add(self.manifest.vector_payload_len)
            .saturating_add(self.manifest.layout_len)
            .saturating_add(self.manifest.lexical_manifest_len)
            .saturating_add(self.manifest.rabitq_artifact_len.unwrap_or_default())
    }

    #[doc(hidden)]
    pub fn config(&self) -> &SearchOutOfCoreConfig {
        &self.config
    }

    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub fn source_graph_commit_epoch(&self) -> Option<u64> {
        self.manifest.source_graph_commit_epoch
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
            let projection = self.rabitq_projection.as_ref()?;
            let manifest = projection.manifest();
            Some(super::VectorProjectionQualificationIdentity {
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
            let projection = self.rabitq_projection.as_ref()?;
            let manifest = projection.manifest();
            let raw_vector_bytes = (manifest.document_count as u64)
                .saturating_mul(manifest.dimension as u64)
                .saturating_mul(std::mem::size_of::<f32>() as u64);
            Some(super::VectorProjectionResourceEvidence {
                segment_count: manifest.segments.len(),
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
        let mut metrics = SearchOutOfCoreMetrics::default();
        let mut hydrated = self.load_documents(&requested, &mut metrics)?;
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

    #[cfg(test)]
    fn visit_documents_in_order(
        &self,
        consumer: &mut dyn FnMut(SearchDocument) -> Result<()>,
    ) -> Result<SearchOutOfCoreMetrics> {
        let mut metrics = SearchOutOfCoreMetrics::default();
        for segment in &self.descriptor.segments {
            for document in self.read_hydration_segment(segment, &mut metrics)? {
                consumer(document)?;
            }
        }
        Ok(metrics)
    }

    fn search_with_options_internal(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        execution: SearchOutOfCoreExecutionContext<'_>,
    ) -> Result<SearchOutOfCoreOutput> {
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
        if options.limit > self.config.max_hydrated_documents.get() {
            return Err(HawDBError::Storage(format!(
                "search hydration requested {} documents, exceeding {}",
                options.limit, self.config.max_hydrated_documents
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
            None => options.metadata_filters.clone(),
        };
        let policy_epoch = access_control
            .map(|access_control| access_control.policy_epoch)
            .or(options.policy_epoch);
        let query_terms = self.lexical_projection.tokenize_query(
            query_text,
            &self.analyzer_lexicon,
            self.lexical_term_policy.max_term_bytes(),
        )?;
        let mut predicate_pushdown = search_metadata_predicate_pushdown(&metadata_filters);
        let mut metrics = SearchOutOfCoreMetrics::default();
        let candidate_set = self.build_candidate_set(
            &predicate_pushdown.predicates,
            &mut predicate_pushdown.report,
            &mut metrics,
        )?;
        let filtered_document_count = candidate_set.cardinality();
        let candidate_report = SearchCandidateSetReport {
            id_space: "search_projection_document_id".to_string(),
            representation: candidate_set.representation().to_string(),
            cardinality: filtered_document_count,
            exact: true,
            snapshot_source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            policy_epoch,
            filtered_out_count: self
                .manifest
                .document_count
                .saturating_sub(filtered_document_count),
            metadata_filters: options.metadata_filters.clone(),
            metadata_predicate_pushdown: predicate_pushdown.report,
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

        let mut vector_fallback_reason_codes = Vec::new();
        let mut vector_fallback_reasons = Vec::new();
        let vector_available = match (query_embedding, self.manifest.embedding_dimension) {
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

        let retained_text_limit = match mode {
            SearchMode::Text => Some(page_score_limit),
            SearchMode::Hybrid => options.rank_window,
            SearchMode::Vector => Some(0),
        };
        let lexical_report = if text_available && mode != SearchMode::Vector {
            Some(self.lexical_projection.score_with_term_limit(
                &query_terms,
                &LexicalMiniDelta::default(),
                self.lexical_term_policy.max_term_bytes(),
                retained_text_limit,
                |id| candidate_set.contains(id, &mut metrics),
            )?)
        } else {
            None
        };
        let (text_scores, text_matching_count, lexical_postings_visited, lexical_bytes_read) =
            lexical_report
                .map(|report| {
                    (
                        report.scores,
                        report.matching_document_count,
                        report.postings_visited,
                        report.bytes_read,
                    )
                })
                .unwrap_or_default();

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
                compressed_vector_search_mode,
                vector_execution_options,
                &mut metrics,
            )?
        } else {
            VectorScoreScan::default()
        };
        vector_fallback_reason_codes.extend(vector_scan.fallback_reason_codes.iter().copied());
        vector_fallback_reasons.extend(vector_scan.fallback_reasons.iter().cloned());
        let vector_scores = &vector_scan.scores;

        let vector_ranks = ranked_scores(vector_scores);
        let text_ranks = ranked_scores(&text_scores);
        let vector_window_ranks = window_ranks(&vector_ranks, options.rank_window);
        let text_window_ranks = window_ranks(&text_ranks, options.rank_window);
        let mut fallback_reason_codes = vector_fallback_reason_codes.clone();
        fallback_reason_codes.extend(text_fallback_reason_codes.iter().copied());
        let mut fallback_reasons = vector_fallback_reasons.clone();
        fallback_reasons.extend(text_fallback_reasons.iter().cloned());

        let retrievers = vec![
            SearchRetrieverReport {
                name: "vector".to_string(),
                backend: vector_scan.backend.clone(),
                backend_selection_reason: None,
                estimated_raw_vector_bytes: self.manifest.embedding_dimension.map(|dimension| {
                    (dimension as u64)
                        .saturating_mul(std::mem::size_of::<f32>() as u64)
                        .saturating_mul(filtered_document_count as u64)
                }),
                filter_selectivity_per_million: None,
                available: vector_available && mode != SearchMode::Text,
                input_candidate_set: candidate_report.clone(),
                candidate_score_source: if vector_available && mode != SearchMode::Text {
                    vector_scan.candidate_score_source.as_str()
                } else {
                    "none"
                }
                .to_string(),
                final_score_source: if vector_available && mode != SearchMode::Text {
                    "raw_vector"
                } else {
                    "none"
                }
                .to_string(),
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
                candidate_scan_kernel: vector_scan.candidate_scan_kernel.clone(),
                candidate_scan_worker_count: vector_scan.candidate_scan_worker_count,
                candidate_scan_segment_count: vector_scan.candidate_scan_segment_count,
                candidate_scan_scanned_segment_count: vector_scan
                    .candidate_scan_scanned_segment_count,
                candidate_scan_scored_document_count: vector_scan
                    .candidate_scan_scored_document_count,
                candidate_scan_filtered_document_count: vector_scan
                    .candidate_scan_filtered_document_count,
                candidate_scan_scanned_block_count: vector_scan.candidate_scan_scanned_block_count,
                candidate_scan_skipped_block_count: vector_scan.candidate_scan_skipped_block_count,
                candidate_scan_payload_bytes_read: vector_scan.candidate_scan_payload_bytes_read,
                candidate_scan_admitted_working_bytes: vector_scan
                    .candidate_scan_admitted_working_bytes,
                posting_bytes_read: 0,
                candidate_postings_visited: 0,
                segmented_lexical_projection_used: false,
                index_covered_document_count: vector_scan.vector_document_count,
                index_candidate_document_count: vector_scan.vector_document_count,
                index_coverage_complete: true,
                candidate_count: vector_scan.matching_count,
                candidate_set: retriever_candidate_set_report(
                    vector_window_ranks.len(),
                    self.manifest.source_graph_commit_epoch,
                    policy_epoch,
                    true,
                ),
                fallback_reason_codes: vector_fallback_reason_codes.clone(),
                fallback_reasons: vector_fallback_reasons.clone(),
                candidate_top_ids: Vec::new(),
                top_hit_ids: top_ranked_ids(&vector_window_ranks, options.limit),
                top_candidates: top_ranked_candidates(
                    &vector_window_ranks,
                    vector_scores,
                    options.limit,
                ),
            },
            SearchRetrieverReport {
                name: "text".to_string(),
                backend: "segmented_bm25_text".to_string(),
                backend_selection_reason: None,
                estimated_raw_vector_bytes: None,
                filter_selectivity_per_million: None,
                available: text_available && mode != SearchMode::Vector,
                input_candidate_set: candidate_report.clone(),
                candidate_score_source: if text_available && mode != SearchMode::Vector {
                    "segmented_bm25"
                } else {
                    "none"
                }
                .to_string(),
                final_score_source: if text_available && mode != SearchMode::Vector {
                    "segmented_bm25"
                } else {
                    "none"
                }
                .to_string(),
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
                candidate_scan_payload_bytes_read: 0,
                candidate_scan_admitted_working_bytes: 0,
                posting_bytes_read: lexical_bytes_read,
                candidate_postings_visited: lexical_postings_visited,
                segmented_lexical_projection_used: true,
                index_covered_document_count: 0,
                index_candidate_document_count: 0,
                index_coverage_complete: true,
                candidate_count: text_matching_count,
                candidate_set: retriever_candidate_set_report(
                    text_window_ranks.len(),
                    self.manifest.source_graph_commit_epoch,
                    policy_epoch,
                    true,
                ),
                fallback_reason_codes: text_fallback_reason_codes,
                fallback_reasons: text_fallback_reasons,
                candidate_top_ids: Vec::new(),
                top_hit_ids: top_ranked_ids(&text_window_ranks, options.limit),
                top_candidates: top_ranked_candidates(
                    &text_window_ranks,
                    &text_scores,
                    options.limit,
                ),
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
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        let total_hits = match mode {
            SearchMode::Text => text_matching_count,
            SearchMode::Vector => vector_scan.matching_count,
            SearchMode::Hybrid => scored_candidates.len(),
        };
        let page_end = options.offset.saturating_add(options.limit);
        let truncated = total_hits > page_end;
        let page_candidates = scored_candidates
            .into_iter()
            .skip(options.offset)
            .take(options.limit)
            .collect::<Vec<_>>();
        let projection_freshness = self.projection_freshness();
        let hydration = HitHydrationContext {
            query_terms: &query_terms,
            query_embedding,
            mode,
            fallback_reason_codes: &fallback_reason_codes,
            fallback_reasons: &fallback_reasons,
            projection_freshness: &projection_freshness,
        };
        let hits = self.hydrate_hits(&page_candidates, hydration, &mut metrics)?;

        let truncation_reasons = if truncated && options.offset > 0 {
            vec![format!(
                "offset {} limit {} returned from {total_hits} matching hits",
                options.offset, options.limit
            )]
        } else if truncated {
            vec![format!(
                "limit {} returned from {total_hits} matching hits",
                options.limit
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
            self.manifest.document_count,
            filtered_document_count,
            total_hits,
            SearchPageWindow {
                offset: options.offset,
                limit: options.limit,
            },
            &truncation_reasons,
            &fallback_reasons,
        );
        let empty_reason_codes = search_empty_reason_codes(
            hits.is_empty(),
            self.manifest.document_count,
            filtered_document_count,
            total_hits,
        );
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
        let ids = candidates
            .iter()
            .map(|candidate| candidate.id.clone())
            .collect::<BTreeSet<_>>();
        let mut hydrated = self.load_documents(&ids, metrics)?;
        let mut hits = Vec::with_capacity(candidates.len());
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
                    .and_then(|(query, embedding)| cosine_similarity(query, embedding))
                    .filter(|score| *score > 0.0)
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let matched_spans = matched_query_spans_bounded(
                context.query_terms,
                &document,
                &self.analyzer_lexicon,
                self.config
                    .max_matched_spans
                    .get()
                    .saturating_sub(matched_span_count),
                self.config
                    .max_matched_span_bytes
                    .get()
                    .saturating_sub(matched_span_bytes),
            )?;
            matched_span_count = matched_span_count.saturating_add(matched_spans.len());
            matched_span_bytes = matched_span_bytes.saturating_add(
                matched_spans
                    .iter()
                    .map(matched_span_bytes_for)
                    .sum::<u64>(),
            );
            hits.push(SearchHit {
                id: candidate.id.clone(),
                score: candidate.score,
                vector_score,
                text_score: candidate.text_score,
                rrf_score: candidate.rrf_score,
                vector_rrf_score: candidate.vector_rrf_score,
                text_rrf_score: candidate.text_rrf_score,
                vector_rank: candidate.vector_rank,
                text_rank: candidate.text_rank,
                kind: document.metadata.get("kind").cloned(),
                external_id: document.metadata.get("external_id").cloned(),
                source_id: document.metadata.get("source_id").cloned(),
                matched_terms: matched_query_terms(
                    context.query_terms,
                    &document,
                    &self.analyzer_lexicon,
                ),
                matched_spans,
                fallback_reason_codes: context.fallback_reason_codes.to_vec(),
                fallback_reasons: context.fallback_reasons.to_vec(),
                projection_freshness: context.projection_freshness.clone(),
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
        if document_ids.len() > self.config.max_hydrated_documents.get() {
            return Err(HawDBError::Storage(format!(
                "search hydration requires {} documents, exceeding {}",
                document_ids.len(),
                self.config.max_hydrated_documents
            )));
        }
        let mut segment_documents = BTreeMap::<u64, BTreeSet<String>>::new();
        for id in document_ids {
            let segment = self.segment_for_document(id).ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search document {id} is outside the published document ranges"
                ))
            })?;
            segment_documents
                .entry(segment.segment_id)
                .or_default()
                .insert(id.clone());
        }
        let mut hydrated = BTreeMap::<String, SearchDocument>::new();
        let mut hydrated_bytes = 0u64;
        for (segment_id, ids) in segment_documents {
            let segment = self
                .descriptor
                .segments
                .get(segment_id as usize)
                .filter(|segment| segment.segment_id == segment_id)
                .ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search hydration references unknown segment {segment_id}"
                    ))
                })?;
            for document in self.read_selected_hydration_segment(
                segment,
                &ids,
                self.config.max_hydrated_bytes.get() - hydrated_bytes,
                metrics,
            )? {
                hydrated_bytes = hydrated_bytes
                    .checked_add(search_document_bytes(&document))
                    .ok_or_else(|| {
                        HawDBError::Storage("search hydration byte count overflow".to_string())
                    })?;
                if hydrated_bytes > self.config.max_hydrated_bytes.get() {
                    return Err(HawDBError::Storage(format!(
                        "search hydration requires {hydrated_bytes} bytes, exceeding {}",
                        self.config.max_hydrated_bytes
                    )));
                }
                hydrated.insert(document.id.clone(), document);
            }
        }
        if hydrated.len() != document_ids.len() {
            return Err(HawDBError::Storage(format!(
                "search hydration found {} of {} requested documents",
                hydrated.len(),
                document_ids.len()
            )));
        }
        metrics.hydrated_bytes = hydrated_bytes;
        Ok(hydrated)
    }

    fn segment_for_document(&self, id: &str) -> Option<&SearchSegmentDescriptorEntry> {
        self.descriptor
            .segments
            .binary_search_by(|segment| {
                if segment.last_document_id.as_str() < id {
                    CmpOrdering::Less
                } else if segment.first_document_id.as_str() > id {
                    CmpOrdering::Greater
                } else {
                    CmpOrdering::Equal
                }
            })
            .ok()
            .and_then(|index| self.descriptor.segments.get(index))
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
            &self.payload,
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
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchMetadataDocument>> {
        let range = self.layout_range(segment.segment_id)?.metadata;
        let payload = read_out_of_core_payload_range(
            &self.metadata_payload,
            range,
            segment.segment_id,
            "metadata",
            metrics,
        )?;
        metrics.metadata_segment_bytes_read = metrics
            .metadata_segment_bytes_read
            .saturating_add(range.length);
        let text = decode_search_snapshot_text_bounded(
            &payload,
            self.config.max_uncompressed_segment_bytes.get(),
        )?;
        metrics.peak_metadata_segment_bytes =
            metrics.peak_metadata_segment_bytes.max(text.len() as u64);
        let documents = decode_metadata_segment(&text, segment, range.entry_count)?;
        let layout = self.layout_range(segment.segment_id)?;
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
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<SearchVectorDocument>> {
        let range = self.layout_range(segment.segment_id)?.vectors;
        let payload = read_out_of_core_payload_range(
            &self.vector_payload,
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
            self.layout_range(segment.segment_id)?.vector_ordinal_base,
            self.manifest.embedding_dimension,
        )
    }

    fn layout_range(&self, segment_id: u64) -> Result<&SearchOutOfCoreSegmentLayout> {
        self.layout
            .segments
            .get(segment_id as usize)
            .filter(|layout| layout.segment_id == segment_id)
            .ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layout has no segment {segment_id}"
                ))
            })
    }

    fn build_candidate_set(
        &self,
        predicates: &hawdb_optimizer::SearchPredicateSet,
        report: &mut SearchPredicatePushdownReport,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<CandidateSet> {
        report.segment_count = self.descriptor.segments.len();
        report.segment_pruning_candidate_document_count = self.descriptor.document_count;
        report.persisted_segment_descriptor_used = true;
        let mut field_pruning = SearchFieldPruningAccumulator::new(predicates);

        if predicates.is_empty() {
            report.field_summaries = field_pruning.into_reports();
            return Ok(CandidateSet::All(self.descriptor.document_count));
        }

        fs::create_dir_all(&self.config.spill_directory)?;
        let path = unique_candidate_path(&self.config.spill_directory);
        let mut guard = CandidateFileGuard::new(path.clone());
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        file.write_all(CANDIDATE_FILE_HEADER)?;
        let mut offset = CANDIDATE_FILE_HEADER.len() as u64;
        let mut cardinality = 0usize;
        let mut blocks = Vec::with_capacity(self.descriptor.segments.len());

        for segment in &self.descriptor.segments {
            field_pruning.observe_persisted_segment(segment, predicates);
            if !segment.may_match_predicates(predicates) {
                report.pruned_segment_count = report.pruned_segment_count.saturating_add(1);
                report.segment_pruned_document_count = report
                    .segment_pruned_document_count
                    .saturating_add(segment.document_count);
                blocks.push(CandidateBlock::empty(segment));
                continue;
            }
            report.scanned_segment_count = report.scanned_segment_count.saturating_add(1);
            report.segment_scanned_document_count = report
                .segment_scanned_document_count
                .saturating_add(segment.document_count);
            let documents = self.read_metadata_segment(segment, metrics)?;
            let mut encoded = Vec::new();
            let mut block_cardinality = 0usize;
            for document in documents {
                let candidate = SearchDocument {
                    id: document.id,
                    title: String::new(),
                    content: String::new(),
                    embedding: None,
                    metadata: document.metadata,
                };
                if !search_document_matches_predicates(&candidate, predicates) {
                    continue;
                }
                let id_len = u32::try_from(candidate.id.len()).map_err(|_| {
                    HawDBError::Storage(format!(
                        "search candidate id {} exceeds the supported length",
                        candidate.id
                    ))
                })?;
                encoded.extend_from_slice(&id_len.to_le_bytes());
                encoded.extend_from_slice(candidate.id.as_bytes());
                encoded
                    .extend_from_slice(&document.vector_ordinal.unwrap_or(u64::MAX).to_le_bytes());
                block_cardinality = block_cardinality.saturating_add(1);
            }
            if encoded.len() as u64 > self.config.max_candidate_block_bytes.get() {
                return Err(HawDBError::Storage(format!(
                    "search candidate block for segment {} requires {} bytes, exceeding {}",
                    segment.segment_id,
                    encoded.len(),
                    self.config.max_candidate_block_bytes
                )));
            }
            let next_spill_bytes = offset
                .saturating_add(encoded.len() as u64)
                .saturating_sub(CANDIDATE_FILE_HEADER.len() as u64);
            if next_spill_bytes > self.config.max_candidate_spill_bytes.get() {
                return Err(HawDBError::Storage(format!(
                    "search candidate spill requires {next_spill_bytes} bytes, exceeding {}",
                    self.config.max_candidate_spill_bytes
                )));
            }
            file.write_all(&encoded)?;
            blocks.push(CandidateBlock {
                segment_id: segment.segment_id,
                first_document_id: segment.first_document_id.clone(),
                last_document_id: segment.last_document_id.clone(),
                offset,
                length: encoded.len() as u64,
                cardinality: block_cardinality,
            });
            offset = offset.saturating_add(encoded.len() as u64);
            cardinality = cardinality.saturating_add(block_cardinality);
        }
        file.sync_all()?;
        metrics.candidate_spill_bytes = offset.saturating_sub(CANDIDATE_FILE_HEADER.len() as u64);
        report.physical_range_read_count =
            usize::try_from(metrics.segment_range_reads).unwrap_or(usize::MAX);
        report.physical_bytes_read = metrics.segment_bytes_read;
        report.field_summaries = field_pruning.into_reports();
        guard.disarm();
        Ok(CandidateSet::Spilled(SpilledCandidateSet {
            file: Some(file),
            path,
            blocks,
            cardinality,
            max_block_bytes: self.config.max_candidate_block_bytes.get(),
            cache: Mutex::new(None),
        }))
    }
}

fn verify_rabitq_artifact(root: &Path, manifest: &SearchOutOfCoreManifestBody) -> Result<()> {
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
    manifest: &SearchOutOfCoreManifestBody,
    max_working_bytes: usize,
) -> Result<Option<Arc<hawdb_vector_projection::FileProjection>>> {
    verify_rabitq_artifact(root, manifest)?;
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
    let projection = hawdb_vector_projection::FileProjection::open(root.join(file_name))
        .map_err(vector_projection_error)?;
    let projection_manifest = projection.manifest();
    let expected_identity = hawdb_vector_projection::ProjectionIdentity {
        generation: manifest.generation,
        source_epoch: manifest.source_graph_commit_epoch,
        embedding_model: manifest.embedding_model.clone(),
        embedding_version: manifest.embedding_version.clone(),
    };
    if projection_manifest.identity != expected_identity
        || Some(projection_manifest.dimension) != manifest.embedding_dimension
        || Some(projection_manifest.document_count) != manifest.rabitq_vector_document_count
        || Some(projection_manifest.source_digest) != manifest.rabitq_source_digest
        || Some(projection_manifest.payload_checksum) != manifest.rabitq_payload_checksum
    {
        return Err(HawDBError::Storage(
            "search out-of-core RaBitQ identity does not match its generation".to_string(),
        ));
    }
    Ok(Some(Arc::new(projection)))
}

fn file_len_checksum_streaming(path: &Path) -> Result<(u64, u64)> {
    let mut file = File::open(path)?;
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
        return Err(HawDBError::Storage(format!(
            "search artifact {} changed while checksumming",
            path.display()
        )));
    }
    Ok((actual_len, hasher.finish()))
}

pub(super) fn published_generation(
    root: &Path,
    analyzer_lexicon: &SearchAnalyzerLexicon,
) -> Result<Option<u64>> {
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let reader = SearchOutOfCoreReader::open_with_config_and_analyzer(
        root,
        SearchOutOfCoreConfig::default(),
        analyzer_lexicon.clone(),
    )?;
    Ok(Some(reader.generation()))
}

pub(super) fn publish_out_of_core_projection(index: &SearchIndex, root: &Path) -> Result<u64> {
    let descriptor = read_search_segment_descriptor(root)?.ok_or_else(|| {
        HawDBError::Storage("search segment descriptor is missing after checkpoint".to_string())
    })?;
    if !descriptor.matches_documents(&index.documents) {
        return Err(HawDBError::Storage(
            "search segment descriptor does not match checkpoint documents".to_string(),
        ));
    }
    let generation = next_generation(root, DEFAULT_MAX_MANIFEST_BYTES)?;
    let descriptor_file = format!("search_projection_segments.{generation}.hawdb");
    let payload_file = format!("search_projection_segment_payloads.{generation}.hawdb");
    let metadata_payload_file = format!("search_projection_metadata_payloads.{generation}.hawdb");
    let vector_payload_file = format!("search_projection_vector_payloads.{generation}.hawdb");
    let layout_file = format!("search_projection_out_of_core_layout.{generation}.hawdb");
    let lexical_manifest_file = format!("search_lexical.manifest.{generation}.hawdb");
    publish_generation_link(
        &root.join(SEARCH_SEGMENT_DESCRIPTOR_FILE),
        &root.join(&descriptor_file),
    )?;
    publish_generation_link(
        &root.join(SEARCH_SEGMENT_PAYLOAD_FILE),
        &root.join(&payload_file),
    )?;
    publish_generation_link(
        &root.join(MANIFEST_FILE),
        &root.join(&lexical_manifest_file),
    )?;

    let (layout, metadata_payload_len, vector_payload_len) = write_out_of_core_sidecars(
        index,
        &descriptor,
        generation,
        &root.join(&metadata_payload_file),
        &root.join(&vector_payload_file),
    )?;
    let layout_bytes = layout.encode()?;
    write_generation_artifact(&root.join(&layout_file), &layout_bytes)?;

    let descriptor_bytes = fs::read(root.join(&descriptor_file))?;
    let lexical_manifest_bytes = fs::read(root.join(&lexical_manifest_file))?;
    let embedding = index.embedding_manifest.as_ref();
    let manifest = SearchOutOfCoreManifestBody {
        format: OUT_OF_CORE_FORMAT.to_string(),
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
        rabitq_artifact_file: None,
        rabitq_artifact_len: None,
        rabitq_artifact_checksum: None,
        rabitq_source_digest: None,
        rabitq_vector_document_count: None,
        rabitq_payload_checksum: None,
        rabitq_peak_build_working_bytes: None,
        document_count: index.documents.len(),
        documents_digest: lexical_documents_digest(&index.documents),
        source_graph_commit_epoch: index.source_graph_commit_epoch,
        import_source_graph_commit_epoch: index.import_source_graph_commit_epoch,
        embedding_model: embedding.map(|manifest| manifest.model.clone()),
        embedding_version: embedding.and_then(|manifest| manifest.version.clone()),
        embedding_dimension: embedding
            .map(|manifest| manifest.dimension)
            .or(index.embedding_dimension),
    };
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    let tmp_path = manifest_path.with_extension(format!("tmp.{generation}"));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&manifest.encode()?)?;
        file.sync_all()?;
    }
    durable_replace_file(&tmp_path, &manifest_path)?;
    Ok(generation)
}

fn write_out_of_core_sidecars(
    index: &SearchIndex,
    descriptor: &SearchSegmentDescriptor,
    generation: u64,
    metadata_path: &Path,
    vector_path: &Path,
) -> Result<(SearchOutOfCoreLayoutBody, u64, u64)> {
    let metadata_tmp = temporary_artifact_path(metadata_path);
    let vector_tmp = temporary_artifact_path(vector_path);
    let mut metadata_guard = CandidateFileGuard::new(metadata_tmp.clone());
    let mut vector_guard = CandidateFileGuard::new(vector_tmp.clone());
    let mut metadata_file = File::create(&metadata_tmp)?;
    let mut vector_file = File::create(&vector_tmp)?;
    let mut metadata_offset = 0u64;
    let mut vector_offset = 0u64;
    let mut vector_ordinal = 0u64;
    let mut layouts = Vec::with_capacity(descriptor.segments.len());

    for segment in &descriptor.segments {
        let documents = index
            .documents
            .range(segment.first_document_id.clone()..=segment.last_document_id.clone())
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
    drop(metadata_file);
    drop(vector_file);
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
        metadata_offset,
        vector_offset,
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
}

enum CandidateSet {
    All(usize),
    Spilled(SpilledCandidateSet),
}

impl CandidateSet {
    fn cardinality(&self) -> usize {
        match self {
            Self::All(cardinality) => *cardinality,
            Self::Spilled(set) => set.cardinality,
        }
    }

    fn representation(&self) -> &'static str {
        match self {
            Self::All(_) => "all_documents",
            Self::Spilled(_) => "spilled_sorted_document_ids",
        }
    }

    fn contains(&self, id: &str, metrics: &mut SearchOutOfCoreMetrics) -> Result<bool> {
        match self {
            Self::All(_) => Ok(true),
            Self::Spilled(set) => set.contains(id, metrics),
        }
    }

    fn segment_cardinality(&self, segment_id: u64) -> usize {
        match self {
            Self::All(_) => usize::MAX,
            Self::Spilled(set) => set
                .blocks
                .get(segment_id as usize)
                .filter(|block| block.segment_id == segment_id)
                .map(|block| block.cardinality)
                .unwrap_or(0),
        }
    }

    #[cfg(feature = "vector-search")]
    fn vector_ordinals(
        &self,
        max_bytes: u64,
        task_context: Option<&crate::RuntimeTaskContext>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Option<Vec<u64>>> {
        match self {
            Self::All(_) => Ok(None),
            Self::Spilled(set) => set
                .vector_ordinals(max_bytes, task_context, metrics)
                .map(Some),
        }
    }
}

#[derive(Debug, Clone)]
struct RankedScore {
    id: String,
    score: f64,
}

impl PartialEq for RankedScore {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for RankedScore {}

impl PartialOrd for RankedScore {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedScore {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.id.cmp(&self.id))
    }
}

enum BoundedScoreStorage {
    Full(BTreeMap<String, f64>),
    TopK {
        limit: usize,
        heap: BinaryHeap<Reverse<RankedScore>>,
    },
}

struct BoundedScoreCollector {
    storage: BoundedScoreStorage,
    matching_count: usize,
    max_entries: usize,
}

impl BoundedScoreCollector {
    fn new(retained_limit: Option<usize>, max_entries: usize) -> Result<Self> {
        if retained_limit.is_some_and(|limit| limit > max_entries) {
            return Err(HawDBError::Storage(format!(
                "search rank window exceeds the admitted {max_entries} score entries"
            )));
        }
        Ok(Self {
            storage: match retained_limit {
                Some(limit) => BoundedScoreStorage::TopK {
                    limit,
                    heap: BinaryHeap::with_capacity(limit.saturating_add(1)),
                },
                None => BoundedScoreStorage::Full(BTreeMap::new()),
            },
            matching_count: 0,
            max_entries,
        })
    }

    fn push(&mut self, id: String, score: f64) -> Result<()> {
        self.matching_count = self.matching_count.saturating_add(1);
        match &mut self.storage {
            BoundedScoreStorage::Full(scores) => {
                if scores.len() >= self.max_entries {
                    return Err(HawDBError::Storage(format!(
                        "vector query matched more than {} documents; provide a rank window or narrow the candidate set",
                        self.max_entries
                    )));
                }
                scores.insert(id, score);
            }
            BoundedScoreStorage::TopK { limit, heap } => {
                if *limit == 0 {
                    return Ok(());
                }
                let candidate = RankedScore { id, score };
                if heap.len() < *limit {
                    heap.push(Reverse(candidate));
                } else if heap
                    .peek()
                    .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
                {
                    heap.pop();
                    heap.push(Reverse(candidate));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> BTreeMap<String, f64> {
        match self.storage {
            BoundedScoreStorage::Full(scores) => scores,
            BoundedScoreStorage::TopK { heap, .. } => heap
                .into_iter()
                .map(|Reverse(candidate)| (candidate.id, candidate.score))
                .collect(),
        }
    }
}

#[derive(Debug)]
struct CandidateBlock {
    segment_id: u64,
    first_document_id: String,
    last_document_id: String,
    offset: u64,
    length: u64,
    cardinality: usize,
}

impl CandidateBlock {
    fn empty(segment: &SearchSegmentDescriptorEntry) -> Self {
        Self {
            segment_id: segment.segment_id,
            first_document_id: segment.first_document_id.clone(),
            last_document_id: segment.last_document_id.clone(),
            offset: 0,
            length: 0,
            cardinality: 0,
        }
    }

    fn contains_range(&self, id: &str) -> bool {
        self.first_document_id.as_str() <= id && id <= self.last_document_id.as_str()
    }
}

#[derive(Debug)]
struct CandidateCache {
    segment_id: u64,
    entries: Vec<CandidateEntry>,
}

#[derive(Debug)]
struct CandidateEntry {
    id: String,
}

#[derive(Debug)]
struct SpilledCandidateSet {
    file: Option<File>,
    path: PathBuf,
    blocks: Vec<CandidateBlock>,
    cardinality: usize,
    max_block_bytes: u64,
    cache: Mutex<Option<CandidateCache>>,
}

impl SpilledCandidateSet {
    fn contains(&self, id: &str, metrics: &mut SearchOutOfCoreMetrics) -> Result<bool> {
        let block = self
            .blocks
            .binary_search_by(|block| {
                if id < block.first_document_id.as_str() {
                    std::cmp::Ordering::Greater
                } else if id > block.last_document_id.as_str() {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()
            .and_then(|index| self.blocks.get(index));
        let Some(block) = block.filter(|block| block.contains_range(id)) else {
            return Ok(false);
        };
        if block.cardinality == 0 {
            return Ok(false);
        }
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cache
            .as_ref()
            .is_none_or(|cached| cached.segment_id != block.segment_id)
        {
            if block.length > self.max_block_bytes {
                return Err(HawDBError::Storage(format!(
                    "search candidate block {} exceeds its read budget",
                    block.segment_id
                )));
            }
            let length = usize::try_from(block.length).map_err(|_| {
                HawDBError::Storage("search candidate block length exceeds usize".to_string())
            })?;
            let mut bytes = vec![0u8; length];
            read_search_range(
                self.file.as_ref().ok_or_else(|| {
                    HawDBError::Storage("search candidate spill file is closed".to_string())
                })?,
                block.offset,
                &mut bytes,
            )?;
            metrics.candidate_block_reads = metrics.candidate_block_reads.saturating_add(1);
            metrics.candidate_bytes_read =
                metrics.candidate_bytes_read.saturating_add(block.length);
            *cache = Some(CandidateCache {
                segment_id: block.segment_id,
                entries: decode_candidate_entries(&bytes, block.cardinality)?,
            });
        }
        Ok(cache.as_ref().is_some_and(|cached| {
            cached
                .entries
                .binary_search_by(|value| value.id.as_str().cmp(id))
                .is_ok()
        }))
    }

    #[cfg(feature = "vector-search")]
    fn vector_ordinals(
        &self,
        max_bytes: u64,
        task_context: Option<&crate::RuntimeTaskContext>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<u64>> {
        let ordinal_capacity_bytes = (self.cardinality as u64)
            .checked_mul(std::mem::size_of::<u64>() as u64)
            .ok_or_else(|| {
                HawDBError::Storage("search vector allowlist size overflow".to_string())
            })?;
        let max_block_bytes = self
            .blocks
            .iter()
            .map(|block| block.length)
            .max()
            .unwrap_or_default();
        let required_working_bytes = ordinal_capacity_bytes
            .checked_add(max_block_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("search vector allowlist working set overflow".to_string())
            })?;
        if required_working_bytes > max_bytes {
            return Err(HawDBError::Storage(format!(
                "search vector candidate allowlist and block require {required_working_bytes} bytes, exceeding {max_bytes}"
            )));
        }
        let mut ordinals = Vec::with_capacity(self.cardinality);
        for block in &self.blocks {
            if let Some(task_context) = task_context {
                task_context.checkpoint().map_err(|reason| {
                    HawDBError::Execution(format!("search vector task {reason}"))
                })?;
            }
            if block.cardinality == 0 {
                continue;
            }
            let bytes = self.read_block_bytes(block, metrics)?;
            decode_candidate_ordinals_into(&bytes, block.cardinality, &mut ordinals)?;
        }
        if ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HawDBError::Storage(
                "search vector candidate ordinals are not strictly ordered".to_string(),
            ));
        }
        Ok(ordinals)
    }

    #[cfg(feature = "vector-search")]
    fn read_block_bytes(
        &self,
        block: &CandidateBlock,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Vec<u8>> {
        if block.length > self.max_block_bytes {
            return Err(HawDBError::Storage(format!(
                "search candidate block {} exceeds its read budget",
                block.segment_id
            )));
        }
        let length = usize::try_from(block.length).map_err(|_| {
            HawDBError::Storage("search candidate block length exceeds usize".to_string())
        })?;
        let mut bytes = vec![0u8; length];
        read_search_range(
            self.file.as_ref().ok_or_else(|| {
                HawDBError::Storage("search candidate spill file is closed".to_string())
            })?,
            block.offset,
            &mut bytes,
        )?;
        metrics.candidate_block_reads = metrics.candidate_block_reads.saturating_add(1);
        metrics.candidate_bytes_read = metrics.candidate_bytes_read.saturating_add(block.length);
        Ok(bytes)
    }
}

impl Drop for SpilledCandidateSet {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
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

fn decode_candidate_entries(bytes: &[u8], expected: usize) -> Result<Vec<CandidateEntry>> {
    let mut offset = 0usize;
    let mut entries = Vec::with_capacity(expected);
    while offset < bytes.len() {
        let end = offset.saturating_add(4);
        let length_bytes = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated length".to_string())
        })?;
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
        offset = end;
        let end = offset.checked_add(length).ok_or_else(|| {
            HawDBError::Storage("search candidate id length overflows".to_string())
        })?;
        let raw = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated id".to_string())
        })?;
        let id = String::from_utf8(raw.to_vec()).map_err(|error| {
            HawDBError::Storage(format!("search candidate id is not UTF-8: {error}"))
        })?;
        offset = end;
        let end = offset.saturating_add(std::mem::size_of::<u64>());
        let raw_ordinal = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated vector ordinal".to_string())
        })?;
        let _vector_ordinal = u64::from_le_bytes(raw_ordinal.try_into().unwrap());
        entries.push(CandidateEntry { id });
        offset = end;
    }
    if entries.len() != expected || entries.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err(HawDBError::Storage(
            "search candidate block count or ordering mismatch".to_string(),
        ));
    }
    Ok(entries)
}

#[cfg(feature = "vector-search")]
fn decode_candidate_ordinals_into(
    bytes: &[u8],
    expected: usize,
    ordinals: &mut Vec<u64>,
) -> Result<()> {
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut previous_id = None;
    while offset < bytes.len() {
        let end = offset.saturating_add(std::mem::size_of::<u32>());
        let length_bytes = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated length".to_string())
        })?;
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
        offset = end;
        let end = offset.checked_add(length).ok_or_else(|| {
            HawDBError::Storage("search candidate id length overflows".to_string())
        })?;
        let id = std::str::from_utf8(bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated id".to_string())
        })?)
        .map_err(|error| {
            HawDBError::Storage(format!("search candidate id is not UTF-8: {error}"))
        })?;
        if previous_id.is_some_and(|previous| previous >= id) {
            return Err(HawDBError::Storage(
                "search candidate block ids are not strictly ordered".to_string(),
            ));
        }
        previous_id = Some(id);
        offset = end;
        let end = offset.saturating_add(std::mem::size_of::<u64>());
        let raw_ordinal = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated vector ordinal".to_string())
        })?;
        let vector_ordinal = u64::from_le_bytes(raw_ordinal.try_into().unwrap());
        if vector_ordinal != u64::MAX {
            ordinals.push(vector_ordinal);
        }
        offset = end;
        count = count.saturating_add(1);
    }
    if count != expected {
        return Err(HawDBError::Storage(
            "search candidate block count mismatch".to_string(),
        ));
    }
    Ok(())
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
) -> Result<Vec<SearchVectorDocument>> {
    let mut lines = text.lines();
    if lines.next() != Some("HAWDB_SEARCH_VECTOR_SEGMENT_V1") {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar has an invalid header",
            segment.segment_id
        )));
    }
    let mut documents = Vec::with_capacity(expected_count);
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
                let embedding = decode_embedding(raw_embedding)?.ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search segment {} vector sidecar contains an empty embedding",
                        segment.segment_id
                    ))
                })?;
                if embedding.iter().any(|value| !value.is_finite())
                    || expected_dimension.is_none_or(|dimension| embedding.len() != dimension)
                {
                    return Err(HawDBError::Storage(format!(
                        "search segment {} vector sidecar has an invalid embedding",
                        segment.segment_id
                    )));
                }
                documents.push(SearchVectorDocument {
                    vector_ordinal,
                    id: decode_string(raw_id)?,
                    embedding,
                });
            }
            _ => {
                return Err(HawDBError::Storage(format!(
                    "search segment {} has an invalid vector sidecar line",
                    segment.segment_id
                )));
            }
        }
    }
    if documents.len() != expected_count
        || documents.iter().enumerate().any(|(offset, document)| {
            document.vector_ordinal != expected_ordinal_base.saturating_add(offset as u64)
        })
        || documents.windows(2).any(|pair| pair[0].id >= pair[1].id)
        || documents.iter().any(|document| {
            document.id < segment.first_document_id || document.id > segment.last_document_id
        })
    {
        return Err(HawDBError::Storage(format!(
            "search segment {} vector sidecar count, bounds, or ordering mismatch",
            segment.segment_id
        )));
    }
    Ok(documents)
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
    let bytes = read_bounded_file(path, expected_len)?;
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

fn unique_candidate_path(directory: &Path) -> PathBuf {
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(
        ".hawdb-search-candidates.{}.{}",
        std::process::id(),
        sequence
    ))
}

pub(super) fn next_generation(root: &Path, max_lexical_manifest_bytes: u64) -> Result<u64> {
    let manifest_path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    if !manifest_path.exists() {
        return Ok(1);
    }
    let active_generation = match read_bounded_file(&manifest_path, MAX_OUT_OF_CORE_MANIFEST_BYTES)
        .and_then(|bytes| SearchOutOfCoreManifestBody::decode(&bytes))
        .map(|manifest| manifest.generation)
    {
        Ok(generation) => generation,
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

fn publish_generation_link(source: &Path, target: &Path) -> Result<()> {
    let tmp = temporary_artifact_path(target);
    let mut guard = CandidateFileGuard::new(tmp.clone());
    match fs::hard_link(source, &tmp) {
        Ok(()) => {}
        Err(_) => {
            fs::copy(source, &tmp)?;
            File::open(&tmp)?.sync_all()?;
        }
    }
    durable_replace_file(&tmp, target)?;
    guard.disarm();
    Ok(())
}

fn write_generation_artifact(target: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = temporary_artifact_path(target);
    let mut guard = CandidateFileGuard::new(tmp.clone());
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    durable_replace_file(&tmp, target)?;
    guard.disarm();
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        SearchFusionWeights, SearchLexicalFeasibilityCoverage, SearchLexicalFeasibilityMetrics,
        SearchLexicalProductionQualificationReport, SearchProjectionCleanupOptions,
    };
    use std::io::{Seek, SeekFrom};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    fn test_dir(name: &str) -> PathBuf {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hawdb-search-out-of-core-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn options(limit: usize, rank_window: Option<usize>) -> SearchQueryOptions {
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
        let path = test_dir("vector-execution-options");
        let mut index = SearchIndex::open(&path).unwrap();
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
        assert!(memory_error
            .to_string()
            .contains("admitted 0 score entries"));

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
        let payload_path = path.join(&reader.manifest.payload_file);
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
    #[cfg(all(feature = "full-text-search", feature = "vector-search"))]
    fn out_of_core_sidecar_corruption_fails_closed_in_the_consuming_stage() {
        let metadata_path = test_dir("metadata-corruption");
        let mut metadata_index = SearchIndex::open(&metadata_path).unwrap();
        metadata_index.upsert(document(0, "team")).unwrap();
        metadata_index.checkpoint().unwrap();
        let metadata_reader = SearchOutOfCoreReader::open(&metadata_path).unwrap();
        corrupt_first_byte(&metadata_path.join(&metadata_reader.manifest.metadata_payload_file));
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
        corrupt_first_byte(&vector_path.join(&vector_reader.manifest.vector_payload_file));
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
        assert!(!cleanup.retry_required);
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
