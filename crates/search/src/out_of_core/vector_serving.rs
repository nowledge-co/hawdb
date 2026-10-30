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

use super::{BoundedScoreCollector, CandidateSet, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use crate::build_memory::{checked_add as add, SET_ENTRY_BYTES};
use crate::error::{HawDBError, Result};
use crate::lexical_projection::QueryContext;
use crate::{CompressedVectorSearchMode, CosineQuery, SearchFallbackReasonCode};
use hawdb_executor::QueryMemoryLease;
#[cfg(feature = "vector-search")]
use std::cmp::{Ordering, Reverse};
#[cfg(feature = "vector-search")]
use std::collections::BinaryHeap;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "vector-search")]
use std::num::NonZeroUsize;

const VECTOR_SCORE_ENTRY_WORKING_BYTES: usize = 128;

#[cfg(feature = "vector-search")]
#[derive(Debug)]
enum RabitqScanError {
    Budget(HawDBError),
    Failure(HawDBError),
}

#[cfg(feature = "vector-search")]
impl RabitqScanError {
    fn budget(required: u64, available: u64, message: String) -> Self {
        Self::Budget(
            hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::Operator,
                owner: "RaBitQ candidate scan".into(),
                memory_class: None,
                requested: required,
                used: 0,
                budget: available,
                budget_option: "SearchOutOfCoreConfig.max_vector_search_working_bytes",
                message,
            }
            .into(),
        )
    }

    fn into_error(self) -> HawDBError {
        match self {
            Self::Budget(error) | Self::Failure(error) => error,
        }
    }

    fn projection(error: hawdb_vector_projection::ProjectionError) -> Self {
        match error {
            hawdb_vector_projection::ProjectionError::ResourceBudgetExceeded {
                required,
                available,
            } => Self::budget(required as u64, available as u64, error.to_string()),
            _ => match vector_projection_error(error) {
                error @ HawDBError::FileDescriptors(_) => Self::Budget(error),
                error => Self::Failure(error),
            },
        }
    }
}

#[cfg(feature = "vector-search")]
impl From<HawDBError> for RabitqScanError {
    fn from(error: HawDBError) -> Self {
        Self::Failure(error)
    }
}

#[cfg(feature = "vector-search")]
type RabitqResult<T> = std::result::Result<T, RabitqScanError>;

#[cfg(feature = "vector-search")]
pub(super) fn candidate_working_bytes(limit: usize) -> Result<usize> {
    let entry_bytes = std::mem::size_of::<LayeredProjectionHit>() * 2
        + std::mem::size_of::<hawdb_vector_projection::ProjectionHit>() * 2;
    limit
        .checked_mul(entry_bytes)
        .ok_or_else(|| HawDBError::Storage("search RaBitQ candidate size overflow".into()))
}

fn admitted_score_entries(configured_max_entries: usize, working_bytes: usize) -> usize {
    configured_max_entries.min(working_bytes / VECTOR_SCORE_ENTRY_WORKING_BYTES)
}

#[cfg(feature = "vector-search")]
#[derive(Debug)]
struct LayeredProjectionHit {
    layer: usize,
    ordinal: u64,
    score: f32,
    logical_id: Option<String>,
}

#[cfg(feature = "vector-search")]
impl PartialEq for LayeredProjectionHit {
    fn eq(&self, other: &Self) -> bool {
        self.layer == other.layer
            && self.ordinal == other.ordinal
            && self.score.to_bits() == other.score.to_bits()
            && self.logical_id == other.logical_id
    }
}

#[cfg(feature = "vector-search")]
impl Eq for LayeredProjectionHit {}

#[cfg(feature = "vector-search")]
impl PartialOrd for LayeredProjectionHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(feature = "vector-search")]
impl Ord for LayeredProjectionHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.logical_id.cmp(&self.logical_id))
            .then_with(|| other.layer.cmp(&self.layer))
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

#[cfg(feature = "vector-search")]
struct BoundedLayeredProjectionHits {
    limit: usize,
    heap: BinaryHeap<Reverse<LayeredProjectionHit>>,
    logical_id_bytes: usize,
    max_logical_id_bytes: usize,
    id_memory: Option<QueryMemoryLease>,
}

#[cfg(feature = "vector-search")]
impl BoundedLayeredProjectionHits {
    fn new(limit: usize, max_logical_id_bytes: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            logical_id_bytes: 0,
            max_logical_id_bytes,
            id_memory: None,
        }
    }

    fn push(&mut self, candidate: LayeredProjectionHit) -> RabitqResult<()> {
        if self.limit == 0 {
            return Ok(());
        }
        let evicted_bytes = if self.heap.len() == self.limit {
            let Reverse(worst) = self.heap.peek().expect("a full nonempty heap has a root");
            if candidate.cmp(worst).is_le() {
                return Ok(());
            }
            worst.logical_id.as_ref().map_or(0, String::capacity)
        } else {
            0
        };
        let required = self
            .logical_id_bytes
            .checked_sub(evicted_bytes)
            .and_then(|bytes| {
                bytes.checked_add(candidate.logical_id.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(|| {
                HawDBError::Storage("search RaBitQ logical candidate ID size overflow".into())
            })?;
        if required > self.max_logical_id_bytes {
            return Err(RabitqScanError::budget(
                required as u64,
                self.max_logical_id_bytes as u64,
                "search RaBitQ logical candidate IDs exceed the working budget".into(),
            ));
        }
        let incoming_bytes = candidate.logical_id.as_ref().map_or(0, String::capacity);
        if let Some(memory) = &mut self.id_memory {
            memory.grow(incoming_bytes)?;
        }
        if self.heap.len() == self.limit {
            self.heap.pop();
            if let Some(memory) = &mut self.id_memory {
                memory.shrink(evicted_bytes);
            }
        }
        self.heap.push(Reverse(candidate));
        self.logical_id_bytes = required;
        Ok(())
    }

    fn finish(self) -> (Vec<LayeredProjectionHit>, Option<QueryMemoryLease>) {
        let mut hits = self
            .heap
            .into_iter()
            .map(|Reverse(hit)| hit)
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| right.cmp(left));
        (hits, self.id_memory)
    }
}

#[cfg(feature = "vector-search")]
#[derive(Clone, Copy)]
pub(super) struct RerankPolicy<'a> {
    pub(super) candidate_limit: usize,
    pub(super) distance: Option<hawdb_core::VectorDistance>,
    pub(super) query: Option<QueryContext<'a>>,
}

#[derive(Debug)]
pub(super) struct VectorScoreScan {
    pub(super) scores: BTreeMap<String, f64>,
    pub(super) matching_count: usize,
    pub(super) vector_document_count: usize,
    pub(super) segment_scan_count: usize,
    pub(super) backend: String,
    pub(super) candidate_score_source: String,
    pub(super) generated_candidate_count: usize,
    pub(super) reranked_candidate_count: usize,
    pub(super) candidate_scan_kernel: Option<String>,
    pub(super) candidate_scan_worker_count: usize,
    pub(super) candidate_scan_segment_count: usize,
    pub(super) candidate_scan_scanned_segment_count: usize,
    pub(super) candidate_scan_scored_document_count: usize,
    pub(super) candidate_scan_filtered_document_count: usize,
    pub(super) candidate_scan_scanned_block_count: usize,
    pub(super) candidate_scan_skipped_block_count: usize,
    pub(super) candidate_scan_adaptive_exact_scan_count: usize,
    pub(super) candidate_scan_payload_bytes_read: u64,
    pub(super) candidate_scan_admitted_working_bytes: usize,
    pub(super) fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub(super) fallback_reasons: Vec<String>,
    pub(super) _memory: Option<QueryMemoryLease>,
}

impl Default for VectorScoreScan {
    fn default() -> Self {
        Self {
            scores: BTreeMap::new(),
            matching_count: 0,
            vector_document_count: 0,
            segment_scan_count: 0,
            backend: "scalar_vector_segment_scan".to_string(),
            candidate_score_source: "raw_vector".to_string(),
            generated_candidate_count: 0,
            reranked_candidate_count: 0,
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
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
            _memory: None,
        }
    }
}

pub(super) struct ScanOptions<'a> {
    pub(super) compressed: CompressedVectorSearchMode,
    pub(super) execution: super::VectorSearchExecutionOptions<'a>,
    #[cfg(feature = "vector-search")]
    pub(super) candidate_window: Option<usize>,
    pub(super) query: Option<QueryContext<'a>>,
}

impl SearchOutOfCoreReader {
    #[cfg(feature = "vector-search")]
    pub(super) fn rank_exact_index_domain(
        &self,
        request: crate::VectorIndexQuery<'_>,
        query: QueryContext<'_>,
    ) -> Result<VectorScoreScan> {
        let domain = request.eligible.ok_or_else(|| {
            HawDBError::Semantic("exact vector search requires a complete eligible domain".into())
        })?;
        let mut collector = BoundedScoreCollector::with_context(
            Some(request.top_k.get()),
            self.config.max_score_entries.get(),
            Some(query),
        )?;
        for entry in domain.distances() {
            let (id, distance) = entry?;
            query.checkpoint()?;
            collector.push_borrowed(id, -distance)?;
        }
        let scores = collector.finish()?;
        Ok(VectorScoreScan {
            scores: scores.values,
            _memory: scores.memory,
            generated_candidate_count: domain.report().eligible_vectors,
            reranked_candidate_count: domain.report().scored_vectors,
            ..Default::default()
        })
    }

    #[cfg(feature = "vector-search")]
    pub(super) fn scan_index_distances(
        &self,
        request: crate::VectorIndexQuery<'_>,
        candidates: &CandidateSet<'_>,
        query: QueryContext<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<VectorScoreScan> {
        if self.segments.iter().all(|artifact| {
            artifact
                .layout
                .segments
                .iter()
                .all(|segment| segment.vectors.entry_count == 0)
        }) {
            return Ok(VectorScoreScan::default());
        }
        let working_bytes = self
            .config
            .max_vector_search_working_bytes
            .get()
            .min(query.memory.available_bytes());
        self.scan_rabitq_vector_scores(
            request.embedding,
            candidates,
            Some(request.top_k.get()),
            crate::VectorSearchExecutionOptions::bounded(
                NonZeroUsize::new(
                    self.config
                        .max_vector_search_parallelism
                        .get()
                        .min(query.task.admitted_parallelism().get()),
                )
                .expect("nonzero parallelism"),
                working_bytes,
                Some(query.task),
            ),
            metrics,
            RerankPolicy {
                candidate_limit: request.candidate_limit.get(),
                distance: Some(request.descriptor.distance()),
                query: Some(query),
            },
        )
        .map_err(RabitqScanError::into_error)
    }

    pub(super) fn scan_vector_scores(
        &self,
        query_embedding: &[f32],
        candidate_set: &CandidateSet<'_>,
        retained_limit: Option<usize>,
        options: ScanOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<VectorScoreScan> {
        let vector_execution_options = options.execution;
        let task_context = vector_execution_options.task_context;
        checkpoint_vector_task(task_context)?;
        if vector_execution_options.exact
            && options.compressed != CompressedVectorSearchMode::Disabled
        {
            return Err(HawDBError::Semantic(
                "exact vector search cannot require a compressed candidate projection".into(),
            ));
        }
        #[cfg(feature = "vector-search")]
        if options.compressed == CompressedVectorSearchMode::Disabled
            && !vector_execution_options.exact
            && self.vector_layers_use_diskann()
        {
            let mut nonzero = false;
            for chunk in query_embedding.chunks(4096) {
                checkpoint_vector_task(task_context)?;
                if chunk.iter().any(|coordinate| *coordinate != 0.0) {
                    nonzero = true;
                    break;
                }
            }
            if !nonzero {
                // Ordinary Search historically treats undefined cosine scores
                // as absent. Schema-bound vector queries validate separately.
                return self.scan_scalar_vector_scores(
                    query_embedding,
                    candidate_set,
                    retained_limit,
                    vector_execution_options,
                    metrics,
                    options.query,
                );
            }
            let navigation_window =
                hawdb_vector_projection::diskann::DiskAnnSearchOptions::default()
                    .beam_width
                    .min(self.config.max_vector_candidates.get());
            let candidate_limit = retained_limit
                .unwrap_or(1)
                .max(options.candidate_window.unwrap_or(0))
                .max(navigation_window);
            return self
                .scan_rabitq_vector_scores(
                    query_embedding,
                    candidate_set,
                    retained_limit,
                    vector_execution_options,
                    metrics,
                    RerankPolicy {
                        candidate_limit,
                        distance: None,
                        query: options.query,
                    },
                )
                .map_err(RabitqScanError::into_error);
        }
        match options.compressed {
            CompressedVectorSearchMode::Disabled => self.scan_scalar_vector_scores(
                query_embedding,
                candidate_set,
                retained_limit,
                vector_execution_options,
                metrics,
                options.query,
            ),
            CompressedVectorSearchMode::Preferred => {
                #[cfg(feature = "vector-search")]
                if self.segments.iter().all(|artifact| {
                    !Self::layer_has_vectors(artifact)
                        || artifact
                            .rabitq_projection
                            .as_ref()
                            .is_some_and(|value| !value.is_diskann())
                }) {
                    match self.scan_rabitq_vector_scores(
                        query_embedding,
                        candidate_set,
                        retained_limit,
                        vector_execution_options,
                        metrics,
                        RerankPolicy {
                            candidate_limit: self.config.max_vector_candidates.get(),
                            distance: None,
                            query: options.query,
                        },
                    ) {
                        Ok(scan) => return Ok(scan),
                        Err(RabitqScanError::Failure(error)) => return Err(error),
                        Err(RabitqScanError::Budget(error)) => {
                            let mut scan = self.scan_scalar_vector_scores(
                                query_embedding,
                                candidate_set,
                                retained_limit,
                                vector_execution_options,
                                metrics,
                                options.query,
                            )?;
                            scan.fallback_reason_codes
                                .push(SearchFallbackReasonCode::CompressedVectorBudgetExceeded);
                            scan.fallback_reasons
                                .push(format!("{error}; used exact scalar vector segments"));
                            // Preserve a conservative admission receipt for the
                            // abandoned compressed attempt as well as its I/O.
                            scan.candidate_scan_admitted_working_bytes = self
                                .config
                                .max_vector_search_working_bytes
                                .get()
                                .min(vector_execution_options.max_working_bytes);
                            return Ok(scan);
                        }
                    }
                }
                let mut scan = self.scan_scalar_vector_scores(
                    query_embedding,
                    candidate_set,
                    retained_limit,
                    vector_execution_options,
                    metrics,
                    options.query,
                )?;
                scan.fallback_reason_codes
                    .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
                scan.fallback_reasons.push(
                    "one or more out-of-core artifacts do not attach a RaBitQ projection; used exact scalar vector segments"
                        .to_string(),
                );
                Ok(scan)
            }
            CompressedVectorSearchMode::Required => {
                #[cfg(feature = "vector-search")]
                if self.segments.iter().all(|artifact| {
                    !Self::layer_has_vectors(artifact)
                        || artifact
                            .rabitq_projection
                            .as_ref()
                            .is_some_and(|value| !value.is_diskann())
                }) {
                    return self
                        .scan_rabitq_vector_scores(
                            query_embedding,
                            candidate_set,
                            retained_limit,
                            vector_execution_options,
                            metrics,
                            RerankPolicy {
                                candidate_limit: self.config.max_vector_candidates.get(),
                                distance: None,
                                query: options.query,
                            },
                        )
                        .map_err(RabitqScanError::into_error);
                }
                Err(HawDBError::Storage(
                    "out-of-core RaBitQ projection is required but unavailable for one or more artifacts"
                        .to_string(),
                ))
            }
        }
    }

    fn scan_scalar_vector_scores(
        &self,
        query_embedding: &[f32],
        candidate_set: &CandidateSet<'_>,
        retained_limit: Option<usize>,
        vector_execution_options: super::VectorSearchExecutionOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
    ) -> Result<VectorScoreScan> {
        let task_context = vector_execution_options.task_context;
        let max_scored_documents = admitted_score_entries(
            self.config.max_score_entries.get(),
            vector_execution_options.max_working_bytes,
        ) / 2;
        if max_scored_documents == 0 {
            return Err(HawDBError::Storage(
                "search vector uniqueness check exceeds the admitted working memory".to_string(),
            ));
        }
        let mut collector = match context {
            Some(context) => BoundedScoreCollector::with_context(
                retained_limit,
                max_scored_documents,
                Some(context),
            )?,
            None => BoundedScoreCollector::new(retained_limit, max_scored_documents)?,
        };
        let query_scorer = CosineQuery::with_work(query_embedding, || {
            context.map_or_else(
                || checkpoint_vector_task(task_context),
                |query| query.checkpoint(),
            )
        })?;
        let mut vector_document_count = 0usize;
        let mut segment_scan_count = 0usize;
        let mut id_memory = context
            .map(|context| context.memory.reserve(0))
            .transpose()?;
        let mut scored_document_ids = BTreeSet::new();
        for (layer, artifact) in self.segments.iter().enumerate() {
            for segment in &artifact.descriptor.segments {
                checkpoint_vector_task(task_context)?;
                if candidate_set.segment_cardinality(layer, segment.segment_id) == 0 {
                    continue;
                }
                segment_scan_count = segment_scan_count.saturating_add(1);
                let mut vector_bytes_read = 0u64;
                let mut candidate_metrics = SearchOutOfCoreMetrics::default();
                let mut score = |document: &super::SearchVectorDocument| {
                    if !self
                        .visibility
                        .is_visible(artifact.content_segment_id, &document.id)
                        || !candidate_set.contains(&document.id, &mut candidate_metrics)?
                    {
                        return Ok(());
                    }
                    let embedding = document.embedding.as_slice();
                    if embedding.len() != query_embedding.len() || embedding.is_empty() {
                        return Ok(());
                    }
                    if scored_document_ids.contains(&document.id) {
                        return Err(HawDBError::Storage(format!(
                            "search out-of-core segment set has duplicate document {}",
                            document.id
                        )));
                    }
                    if scored_document_ids.len() == max_scored_documents {
                        return Err(HawDBError::Storage(format!(
                            "search vector uniqueness check requires more than {max_scored_documents} document ids"
                        )));
                    }
                    if let Some(memory) = &mut id_memory {
                        memory.grow(add(SET_ENTRY_BYTES, document.id.len())?)?;
                    }
                    scored_document_ids.insert(document.id.clone());
                    vector_document_count = vector_document_count.saturating_add(1);
                    vector_bytes_read = vector_bytes_read.saturating_add(
                        (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64),
                    );
                    if let Some(score) = query_scorer.score(embedding)
                        && score > 0.0
                    {
                        if context.is_some() {
                            collector.push_borrowed(&document.id, score)?;
                        } else {
                            collector.push(document.id.clone(), score)?;
                        }
                    }
                    Ok(())
                };
                if let Some(context) = context {
                    self.visit_query_vector_segment(artifact, segment, metrics, context, score)?;
                } else {
                    for document in self.read_vector_segment(artifact, segment, metrics)? {
                        score(&document)?;
                    }
                }
                metrics.vector_bytes_read =
                    metrics.vector_bytes_read.saturating_add(vector_bytes_read);
                metrics.candidate_block_reads = metrics
                    .candidate_block_reads
                    .saturating_add(candidate_metrics.candidate_block_reads);
                metrics.candidate_bytes_read = metrics
                    .candidate_bytes_read
                    .saturating_add(candidate_metrics.candidate_bytes_read);
            }
        }
        let matching_count = collector.matching_count;
        let scores = collector.finish()?;
        Ok(VectorScoreScan {
            scores: scores.values,
            _memory: scores.memory,
            matching_count,
            vector_document_count,
            segment_scan_count,
            generated_candidate_count: matching_count,
            candidate_scan_segment_count: segment_scan_count,
            candidate_scan_scanned_segment_count: segment_scan_count,
            candidate_scan_scored_document_count: matching_count,
            candidate_scan_payload_bytes_read: metrics.vector_bytes_read,
            ..VectorScoreScan::default()
        })
    }

    #[cfg(feature = "vector-search")]
    fn scan_rabitq_vector_scores(
        &self,
        query_embedding: &[f32],
        candidate_set: &CandidateSet<'_>,
        retained_limit: Option<usize>,
        vector_execution_options: super::VectorSearchExecutionOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
        policy: RerankPolicy<'_>,
    ) -> RabitqResult<VectorScoreScan> {
        self.require_compatible_rabitq_projections()?;
        let task_context = vector_execution_options.task_context;
        checkpoint_vector_task(task_context)?;
        let minimum_candidates = retained_limit.unwrap_or(1).max(1);
        if minimum_candidates > self.config.max_vector_candidates.get() {
            return Err(RabitqScanError::Budget(
                hawdb_core::ResourceBudgetError {
                    resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
                    scope: hawdb_core::ResourceBudgetScope::Operator,
                    owner: "RaBitQ candidate window".into(),
                    memory_class: None,
                    requested: minimum_candidates as u64,
                    used: 0,
                    budget: self.config.max_vector_candidates.get() as u64,
                    budget_option: "SearchOutOfCoreConfig.max_vector_candidates",
                    message: "vector rank window exceeds the candidate limit".into(),
                }
                .into(),
            ));
        }
        let candidate_limit = policy.candidate_limit;
        if candidate_limit < minimum_candidates
            || candidate_limit > self.config.max_vector_candidates.get()
        {
            return Err(HawDBError::Semantic(
                "vector candidate limit must cover top-k and fit the configured candidate limit"
                    .into(),
            )
            .into());
        }
        let total_working_bytes = self
            .config
            .max_vector_search_working_bytes
            .get()
            .min(vector_execution_options.max_working_bytes);
        // The projection can return one local top-k while the global heap is
        // retained. Sorting the heap also transiently materializes a second
        // layered vector, so reserve both representations before dispatch.
        let selected_candidate_bytes = candidate_working_bytes(candidate_limit)?;
        let _candidate_memory = policy
            .query
            .map(|query| query.memory.reserve(selected_candidate_bytes))
            .transpose()?;
        let scan_working_bytes = total_working_bytes
            .checked_sub(selected_candidate_bytes)
            .ok_or_else(|| {
                RabitqScanError::budget(selected_candidate_bytes as u64, total_working_bytes as u64, format!(
                    "search RaBitQ retained candidates require {selected_candidate_bytes} bytes, exceeding {total_working_bytes}"
                ))
            })?;
        let mut selected = BoundedLayeredProjectionHits::new(candidate_limit, scan_working_bytes);
        selected.id_memory = policy
            .query
            .map(|query| query.memory.reserve(0))
            .transpose()?;
        let mut vector_document_count = 0usize;
        let mut generated_candidate_count = 0usize;
        let mut candidate_scan_worker_count = 0usize;
        let mut candidate_scan_segment_count = 0usize;
        let mut candidate_scan_scanned_segment_count = 0usize;
        let mut candidate_scan_scored_document_count = 0usize;
        let mut candidate_scan_filtered_document_count = 0usize;
        let mut candidate_scan_scanned_block_count = 0usize;
        let mut candidate_scan_skipped_block_count = 0usize;
        let mut candidate_scan_adaptive_exact_scan_count = 0usize;
        let mut candidate_scan_payload_bytes_read = 0u64;
        let mut candidate_scan_admitted_working_bytes = 0usize;
        let mut candidate_scan_kernel = None;

        for (layer, artifact) in self.segments.iter().enumerate() {
            checkpoint_vector_task(task_context)?;
            if !Self::layer_has_vectors(artifact) {
                continue;
            }
            let projection = artifact.rabitq_projection.as_ref().ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layer {layer} has no RaBitQ projection"
                ))
            })?;
            let layer_working_bytes = scan_working_bytes
                .checked_sub(selected.logical_id_bytes)
                .ok_or_else(|| {
                    HawDBError::Storage(
                        "search RaBitQ logical candidate ID budget underflow".into(),
                    )
                })?;
            let allowlist_required = candidate_set.vector_allowlist_working_bytes(layer)?;
            if allowlist_required > layer_working_bytes as u64 {
                return Err(RabitqScanError::budget(allowlist_required, layer_working_bytes as u64, format!("search vector candidate allowlist and block require {allowlist_required} bytes, exceeding {layer_working_bytes}")));
            }
            let _allowlist_memory = policy
                .query
                .map(|query| {
                    query
                        .memory
                        .reserve(usize::try_from(allowlist_required).map_err(|_| {
                            HawDBError::Execution("vector allowlist admission overflows".into())
                        })?)
                })
                .transpose()?;
            let allowlist = candidate_set.vector_ordinals_for_layer(
                layer,
                layer_working_bytes as u64,
                task_context,
                metrics,
            )?;
            let allowlist_bytes = allowlist.as_ref().map_or(0usize, |ids| {
                ids.capacity().saturating_mul(std::mem::size_of::<u64>())
            });
            let projection_working_bytes = layer_working_bytes
                .checked_sub(allowlist_bytes)
                .ok_or_else(|| {
                    RabitqScanError::budget(allowlist_bytes as u64, layer_working_bytes as u64, format!(
                        "search vector layer {layer} allowlist requires {allowlist_bytes} bytes, exceeding {scan_working_bytes}"
                    ))
                })?;
            let projection_working_bytes = policy.query.map_or(projection_working_bytes, |query| {
                projection_working_bytes.min(query.memory.available_bytes())
            });
            if projection_working_bytes == 0 {
                return Err(RabitqScanError::budget(
                    1,
                    0,
                    "search RaBitQ scan has no remaining working budget".into(),
                ));
            }
            let mut search_options = hawdb_vector_projection::ProjectionSearchOptions::new()
                .with_max_parallelism(
                    NonZeroUsize::new(
                        self.config
                            .max_vector_search_parallelism
                            .get()
                            .min(vector_execution_options.max_parallelism.get()),
                    )
                    .expect("out-of-core vector parallelism is non-zero"),
                )
                .with_max_working_bytes(projection_working_bytes)
                .with_kernel(vector_execution_options.kernel.projection_preference());
            if let Some(allowlist) = allowlist.as_deref() {
                search_options = search_options.with_allowed_ids(allowlist);
            }
            if let Some(task_context) = task_context {
                search_options = search_options.with_task_context(task_context);
            }
            let projection_memory = policy
                .query
                .map(|query| query.memory.reserve(projection_working_bytes))
                .transpose()?;
            let output = projection
                .search(
                    query_embedding,
                    candidate_limit.min(projection.manifest().document_count.max(1)),
                    search_options,
                )
                .map_err(RabitqScanError::projection)?;
            // Returned hits are covered by the separately retained candidate
            // envelope; scratch is released before metadata/raw hydration.
            drop(projection_memory);
            let projection_report = output.report;
            metrics.rabitq_payload_bytes_read = metrics
                .rabitq_payload_bytes_read
                .saturating_add(projection_report.payload_bytes_read);
            vector_document_count = vector_document_count.saturating_add(
                allowlist
                    .as_ref()
                    .map_or(projection.manifest().document_count, |ids| ids.len()),
            );
            generated_candidate_count =
                generated_candidate_count.saturating_add(projection_report.candidate_count);
            candidate_scan_worker_count =
                candidate_scan_worker_count.saturating_add(projection_report.worker_count);
            candidate_scan_segment_count =
                candidate_scan_segment_count.saturating_add(projection_report.segment_count);
            candidate_scan_scanned_segment_count = candidate_scan_scanned_segment_count
                .saturating_add(projection_report.scanned_segment_count);
            candidate_scan_scored_document_count = candidate_scan_scored_document_count
                .saturating_add(projection_report.scored_document_count);
            candidate_scan_filtered_document_count = candidate_scan_filtered_document_count
                .saturating_add(projection_report.filtered_document_count);
            candidate_scan_scanned_block_count = candidate_scan_scanned_block_count
                .saturating_add(projection_report.scanned_block_count);
            candidate_scan_skipped_block_count = candidate_scan_skipped_block_count
                .saturating_add(projection_report.skipped_block_count);
            candidate_scan_adaptive_exact_scan_count = candidate_scan_adaptive_exact_scan_count
                .saturating_add(projection_report.adaptive_exact_scan_count);
            candidate_scan_payload_bytes_read = candidate_scan_payload_bytes_read
                .saturating_add(projection_report.payload_bytes_read);
            candidate_scan_admitted_working_bytes = candidate_scan_admitted_working_bytes.max(
                projection_report
                    .admitted_working_bytes
                    .saturating_add(allowlist_bytes)
                    .saturating_add(selected.logical_id_bytes),
            );
            candidate_scan_kernel
                .get_or_insert_with(|| projection_report.kernel.as_str().to_string());
            drop(allowlist);
            if self.visibility.is_empty() {
                for hit in output.hits {
                    selected.push(LayeredProjectionHit {
                        layer,
                        ordinal: hit.id,
                        score: hit.score,
                        logical_id: None,
                    })?;
                }
            } else {
                self.retain_logical_projection_hits(
                    layer,
                    output.hits,
                    &mut selected,
                    task_context,
                    metrics,
                    policy.query,
                )?;
                candidate_scan_admitted_working_bytes =
                    candidate_scan_admitted_working_bytes.max(selected.logical_id_bytes);
            }
        }

        let (mut selected, selected_id_memory) = selected.finish();
        // Logical IDs were needed at the approximate top-k boundary. Release
        // them before raw rerank, whose own collector retains document IDs.
        for hit in &mut selected {
            hit.logical_id.take();
        }
        drop(selected_id_memory);
        selected.sort_by(|left, right| {
            left.layer
                .cmp(&right.layer)
                .then_with(|| left.ordinal.cmp(&right.ordinal))
        });
        if selected
            .windows(2)
            .any(|pair| pair[0].layer == pair[1].layer && pair[0].ordinal == pair[1].ordinal)
        {
            return Err(HawDBError::Storage(
                "search RaBitQ projections returned duplicate layer ordinals".to_string(),
            )
            .into());
        }

        let mut collector = BoundedScoreCollector::with_context(
            retained_limit,
            admitted_score_entries(self.config.max_score_entries.get(), scan_working_bytes),
            policy.query,
        )?;
        let query_scorer = if selected.is_empty() {
            CosineQuery {
                query: query_embedding,
                prepared: None,
            }
        } else {
            CosineQuery::with_work(query_embedding, || {
                policy.query.map_or_else(
                    || checkpoint_vector_task(task_context),
                    |query| query.checkpoint(),
                )
            })?
        };
        let mut raw_segment_scan_count = 0usize;
        let mut reranked_candidate_count = 0usize;
        let mut selected_start = 0usize;
        while selected_start < selected.len() {
            let layer = selected[selected_start].layer;
            let selected_end = selected[selected_start..]
                .partition_point(|candidate| candidate.layer == layer)
                .saturating_add(selected_start);
            let selected_layer = &selected[selected_start..selected_end];
            let artifact = self.segments.get(layer).ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search RaBitQ candidate references unknown layer {layer}"
                ))
            })?;
            for (layout, segment) in artifact
                .layout
                .segments
                .iter()
                .zip(&artifact.descriptor.segments)
            {
                checkpoint_vector_task(task_context)?;
                let end = layout
                    .vector_ordinal_base
                    .saturating_add(layout.vectors.entry_count as u64);
                let start_index = selected_layer
                    .partition_point(|candidate| candidate.ordinal < layout.vector_ordinal_base);
                let end_index = selected_layer.partition_point(|candidate| candidate.ordinal < end);
                if start_index == end_index {
                    continue;
                }
                raw_segment_scan_count = raw_segment_scan_count.saturating_add(1);
                let selected_segment = &selected_layer[start_index..end_index];
                if let Some(query) = policy.query {
                    let mut ordinal = layout.vector_ordinal_base;
                    let mut vector_bytes = 0u64;
                    self.visit_query_vector_segment(
                        artifact,
                        segment,
                        metrics,
                        query,
                        |document| {
                            let selected = selected_segment
                                .binary_search_by_key(&ordinal, |candidate| candidate.ordinal)
                                .is_ok();
                            ordinal = ordinal.checked_add(1).ok_or_else(|| {
                                HawDBError::StorageIntegrity("vector ordinal overflow".into())
                            })?;
                            if selected {
                                reranked_candidate_count += 1;
                                vector_bytes = vector_bytes.saturating_add(
                                    (document.embedding.len() as u64).saturating_mul(4),
                                );
                                if let Some(score) = rerank_score(
                                    &query_scorer,
                                    &document.embedding,
                                    policy.distance,
                                    || query.checkpoint(),
                                )? {
                                    collector.push_borrowed(&document.id, score)?;
                                }
                            }
                            Ok(())
                        },
                    )?;
                    metrics.vector_bytes_read =
                        metrics.vector_bytes_read.saturating_add(vector_bytes);
                    continue;
                }
                let documents = if selected_segment.len() == layout.vectors.entry_count {
                    // Unique ordinals within this range cover the whole segment.
                    self.read_vector_segment(artifact, segment, metrics)?
                } else {
                    self.read_vector_segment_selected(
                        artifact,
                        segment,
                        metrics,
                        selected_segment.len(),
                        |ordinal| {
                            selected_segment
                                .binary_search_by_key(&ordinal, |candidate| candidate.ordinal)
                                .is_ok()
                        },
                    )?
                };
                for document in documents {
                    metrics.vector_bytes_read = metrics.vector_bytes_read.saturating_add(
                        (document.embedding.len() as u64)
                            .saturating_mul(std::mem::size_of::<f32>() as u64),
                    );
                    reranked_candidate_count = reranked_candidate_count.saturating_add(1);
                    if let Some(score) =
                        rerank_score(&query_scorer, &document.embedding, policy.distance, || {
                            checkpoint_vector_task(task_context)
                        })?
                    {
                        collector.push(document.id, score)?;
                    }
                }
            }
            selected_start = selected_end;
        }
        if reranked_candidate_count != selected.len() {
            return Err(HawDBError::Storage(format!(
                "search RaBitQ raw rerank hydrated {reranked_candidate_count} of {} candidates",
                selected.len()
            ))
            .into());
        }
        let diskann = self
            .segments
            .iter()
            .find_map(|artifact| artifact.rabitq_projection.as_ref())
            .is_some_and(|projection| projection.manifest().algorithm == "diskann");
        let matching_count = collector.matching_count;
        let scores = collector.finish()?;
        Ok(VectorScoreScan {
            scores: scores.values,
            _memory: scores.memory,
            matching_count,
            vector_document_count,
            segment_scan_count: raw_segment_scan_count,
            backend: if diskann {
                "hawdb_diskann_out_of_core_candidate_graph"
            } else {
                "hawdb_rabitq_out_of_core_candidate_projection"
            }
            .to_string(),
            candidate_score_source: if diskann {
                "graph_distance"
            } else {
                "quantized_projection"
            }
            .to_string(),
            generated_candidate_count,
            reranked_candidate_count,
            candidate_scan_kernel,
            candidate_scan_worker_count,
            candidate_scan_segment_count,
            candidate_scan_scanned_segment_count,
            candidate_scan_scored_document_count,
            candidate_scan_filtered_document_count,
            candidate_scan_scanned_block_count,
            candidate_scan_skipped_block_count,
            candidate_scan_adaptive_exact_scan_count,
            candidate_scan_payload_bytes_read,
            candidate_scan_admitted_working_bytes: candidate_scan_admitted_working_bytes
                .saturating_add(selected_candidate_bytes),
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
        })
    }

    #[cfg(feature = "vector-search")]
    fn retain_logical_projection_hits(
        &self,
        layer: usize,
        mut hits: Vec<hawdb_vector_projection::ProjectionHit>,
        selected: &mut BoundedLayeredProjectionHits,
        task_context: Option<&crate::RuntimeTaskContext>,
        metrics: &mut SearchOutOfCoreMetrics,
        query: Option<QueryContext<'_>>,
    ) -> RabitqResult<()> {
        hits.sort_by_key(|hit| hit.id);
        if hits.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(HawDBError::Storage(
                "search RaBitQ projection returned duplicate ordinals".into(),
            )
            .into());
        }
        let artifact = &self.segments[layer];
        let mut mapped = 0usize;
        let mut previous = None;
        for (layout, segment) in artifact
            .layout
            .segments
            .iter()
            .zip(&artifact.descriptor.segments)
        {
            checkpoint_vector_task(task_context)?;
            let end = layout
                .vector_ordinal_base
                .checked_add(layout.vectors.entry_count as u64)
                .ok_or_else(|| {
                    HawDBError::Storage("search vector ordinal range overflow".into())
                })?;
            let start_index = hits.partition_point(|hit| hit.id < layout.vector_ordinal_base);
            let end_index = hits.partition_point(|hit| hit.id < end);
            if start_index == end_index {
                continue;
            }
            let candidates = &hits[start_index..end_index];
            let mut visit = |document: &super::SearchMetadataDocument| -> Result<()> {
                let Some(ordinal) = document.vector_ordinal else {
                    return Ok(());
                };
                let Ok(index) = candidates.binary_search_by_key(&ordinal, |hit| hit.id) else {
                    return Ok(());
                };
                if previous.is_some_and(|prior| prior >= ordinal)
                    || !self
                        .visibility
                        .is_visible(artifact.content_segment_id, &document.id)
                {
                    return Err(HawDBError::Storage(
                        "search RaBitQ candidate mapping is duplicated, unordered or hidden".into(),
                    ));
                }
                previous = Some(ordinal);
                mapped += 1;
                let _copy_memory = query
                    .map(|query| query.memory.reserve(document.id.len()))
                    .transpose()?;
                selected
                    .push(LayeredProjectionHit {
                        layer,
                        ordinal,
                        score: candidates[index].score,
                        logical_id: Some(document.id.clone()),
                    })
                    .map_err(RabitqScanError::into_error)?;
                Ok(())
            };
            if let Some(query) = query {
                self.visit_query_metadata_segment(artifact, segment, metrics, query, visit)?;
            } else {
                for document in self.read_metadata_segment(artifact, segment, metrics)? {
                    visit(&document)?;
                }
            }
        }
        if mapped != hits.len() {
            return Err(HawDBError::Storage(format!(
                "search RaBitQ logical mapping resolved {mapped} of {} candidates",
                hits.len()
            ))
            .into());
        }
        Ok(())
    }

    #[cfg(feature = "vector-search")]
    fn layer_has_vectors(artifact: &super::SearchOutOfCoreSegmentReader) -> bool {
        artifact
            .layout
            .segments
            .iter()
            .any(|segment| segment.vectors.entry_count != 0)
    }

    #[cfg(feature = "vector-search")]
    fn vector_layers_use_diskann(&self) -> bool {
        let mut found = false;
        for artifact in &self.segments {
            if Self::layer_has_vectors(artifact) {
                if !artifact
                    .rabitq_projection
                    .as_ref()
                    .is_some_and(|value| value.is_diskann())
                {
                    return false;
                }
                found = true;
            }
        }
        found
    }

    #[cfg(feature = "vector-search")]
    fn require_compatible_rabitq_projections(&self) -> Result<()> {
        let mut reference: Option<&super::vector_reader::Projection> = None;
        for (layer, artifact) in self.segments.iter().enumerate() {
            if !Self::layer_has_vectors(artifact) {
                continue;
            }
            let projection = artifact.rabitq_projection.as_ref().ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layer {layer} has no RaBitQ projection"
                ))
            })?;
            if let Some(reference) = reference
                && !reference.compatible(projection)
            {
                return Err(HawDBError::Storage(
                    "search multi-segment candidate projections require matching backend parameters"
                        .to_string(),
                ));
            }
            reference = Some(projection);
        }
        Ok(())
    }
}

#[cfg(feature = "vector-search")]
fn rerank_score(
    query: &CosineQuery<'_>,
    embedding: &[f32],
    distance: Option<hawdb_core::VectorDistance>,
    checkpoint: impl FnMut() -> Result<()>,
) -> Result<Option<f64>> {
    if let Some(distance) = distance {
        // The shared collector maximizes scores. Only this internal value is
        // negated; the indexed-distance result exposes the mathematical distance.
        return match &query.prepared {
            Some(prepared) => prepared.distance_slices_with_work(distance, embedding, checkpoint),
            None => distance.distance_slices_with_work(query.query, embedding, checkpoint),
        }
        .map(|distance| Some(-distance));
    }
    Ok(query.score(embedding).filter(|score| *score > 0.0))
}

fn checkpoint_vector_task(task_context: Option<&crate::RuntimeTaskContext>) -> Result<()> {
    task_context.map_or(Ok(()), |task_context| {
        task_context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("search vector task {reason}")))
    })
}

#[cfg(feature = "vector-search")]
pub(super) fn vector_projection_error(
    error: hawdb_vector_projection::ProjectionError,
) -> HawDBError {
    crate::rabitq_projection::map_projection_error(error, "search RaBitQ projection")
}

#[cfg(all(test, feature = "vector-search"))]
mod tests {
    use super::{BoundedLayeredProjectionHits, LayeredProjectionHit};

    #[test]
    fn projection_fallback_requires_a_typed_resource_error() {
        use super::RabitqScanError;
        use hawdb_vector_projection::ProjectionError;

        assert!(matches!(
            RabitqScanError::projection(ProjectionError::ResourceBudgetExceeded {
                required: 2,
                available: 1,
            }),
            RabitqScanError::Budget(crate::HawDBError::ResourceBudgetExceeded(error))
                if error.requested == 2 && error.used == 0 && error.budget == 1
        ));
        for resource in [
            hawdb_core::error::FileDescriptorError::BudgetExceeded {
                requested: 1,
                available: 0,
                limit: 4,
            },
            hawdb_core::error::FileDescriptorError::OsLimit {
                requested: 1,
                os_code: None,
                soft: None,
                hard: None,
            },
        ] {
            assert!(matches!(
                RabitqScanError::projection(ProjectionError::Io(std::io::Error::other(
                    resource.clone()
                ))),
                RabitqScanError::Budget(crate::HawDBError::FileDescriptors(actual)) if actual == resource
            ));
        }
        for error in [
            ProjectionError::CorruptArtifact("resource budget exceeded".into()),
            ProjectionError::InvalidVector("resource budget exceeded".into()),
            ProjectionError::UnsupportedKernel("resource budget exceeded"),
            ProjectionError::Io(std::io::Error::other("resource budget exceeded")),
        ] {
            assert!(matches!(
                RabitqScanError::projection(error),
                RabitqScanError::Failure(_)
            ));
        }
    }

    #[test]
    fn layered_projection_hits_evict_the_lower_scored_layer() {
        let mut hits = BoundedLayeredProjectionHits::new(1, 0);
        hits.push(LayeredProjectionHit {
            layer: 0,
            ordinal: 0,
            score: 1.0,
            logical_id: None,
        })
        .unwrap();
        hits.push(LayeredProjectionHit {
            layer: 1,
            ordinal: 0,
            score: 2.0,
            logical_id: None,
        })
        .unwrap();

        let (hits, _) = hits.finish();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].layer, 1);
        assert_eq!(hits[0].ordinal, 0);
    }

    #[test]
    fn logical_candidate_ties_ignore_layer_order_and_reject_over_budget_atomically() {
        let mut hits = BoundedLayeredProjectionHits::new(1, 2);
        let candidate = |layer, id: &str, score| LayeredProjectionHit {
            layer,
            ordinal: 0,
            score,
            logical_id: Some(id.into()),
        };
        hits.push(candidate(0, "z", 1.0)).unwrap();
        hits.push(candidate(1, "a", 1.0)).unwrap();
        assert_eq!(hits.logical_id_bytes, 1);
        assert!(hits.push(candidate(2, "long", 2.0)).is_err());
        assert_eq!(hits.logical_id_bytes, 1);
        hits.push(candidate(2, "long", 0.0)).unwrap();
        let (result, _) = hits.finish();
        assert_eq!(result[0].logical_id.as_deref(), Some("a"));
    }
}
