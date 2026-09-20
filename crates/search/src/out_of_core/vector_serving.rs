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
use crate::error::{HawDBError, Result};
use crate::{cosine_similarity, CompressedVectorSearchMode, SearchFallbackReasonCode};
#[cfg(feature = "vector-search")]
use std::cmp::{Ordering, Reverse};
#[cfg(feature = "vector-search")]
use std::collections::BinaryHeap;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "vector-search")]
use std::num::NonZeroUsize;

const VECTOR_SCORE_ENTRY_WORKING_BYTES: usize = 128;

fn admitted_score_entries(configured_max_entries: usize, working_bytes: usize) -> usize {
    configured_max_entries.min(working_bytes / VECTOR_SCORE_ENTRY_WORKING_BYTES)
}

#[cfg(feature = "vector-search")]
#[derive(Debug)]
struct LayeredProjectionHit {
    layer: usize,
    ordinal: u64,
    score: f32,
}

#[cfg(feature = "vector-search")]
impl PartialEq for LayeredProjectionHit {
    fn eq(&self, other: &Self) -> bool {
        self.layer == other.layer
            && self.ordinal == other.ordinal
            && self.score.to_bits() == other.score.to_bits()
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
            .then_with(|| other.layer.cmp(&self.layer))
            .then_with(|| other.ordinal.cmp(&self.ordinal))
    }
}

#[cfg(feature = "vector-search")]
struct BoundedLayeredProjectionHits {
    limit: usize,
    heap: BinaryHeap<Reverse<LayeredProjectionHit>>,
}

#[cfg(feature = "vector-search")]
impl BoundedLayeredProjectionHits {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    fn push(&mut self, candidate: LayeredProjectionHit) {
        if self.limit == 0 {
            return;
        }
        if self.heap.len() < self.limit {
            self.heap.push(Reverse(candidate));
        } else if self
            .heap
            .peek()
            .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
        {
            self.heap.pop();
            self.heap.push(Reverse(candidate));
        }
    }

    fn finish(self) -> Vec<LayeredProjectionHit> {
        let mut hits = self
            .heap
            .into_iter()
            .map(|Reverse(hit)| hit)
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| right.cmp(left));
        hits
    }
}

#[derive(Debug, Clone)]
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
    pub(super) candidate_scan_payload_bytes_read: u64,
    pub(super) candidate_scan_admitted_working_bytes: usize,
    pub(super) fallback_reason_codes: Vec<SearchFallbackReasonCode>,
    pub(super) fallback_reasons: Vec<String>,
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
            candidate_scan_payload_bytes_read: 0,
            candidate_scan_admitted_working_bytes: 0,
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
        }
    }
}

impl SearchOutOfCoreReader {
    pub(super) fn scan_vector_scores(
        &self,
        query_embedding: &[f32],
        candidate_set: &CandidateSet,
        retained_limit: Option<usize>,
        compressed_vector_search_mode: CompressedVectorSearchMode,
        vector_execution_options: super::VectorSearchExecutionOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<VectorScoreScan> {
        let task_context = vector_execution_options.task_context;
        checkpoint_vector_task(task_context)?;
        match compressed_vector_search_mode {
            CompressedVectorSearchMode::Disabled => self.scan_scalar_vector_scores(
                query_embedding,
                candidate_set,
                retained_limit,
                vector_execution_options,
                metrics,
            ),
            CompressedVectorSearchMode::Preferred => {
                #[cfg(feature = "vector-search")]
                if self
                    .segments
                    .iter()
                    .all(|artifact| artifact.rabitq_projection.is_some())
                {
                    return self.scan_rabitq_vector_scores(
                        query_embedding,
                        candidate_set,
                        retained_limit,
                        vector_execution_options,
                        metrics,
                    );
                }
                let mut scan = self.scan_scalar_vector_scores(
                    query_embedding,
                    candidate_set,
                    retained_limit,
                    vector_execution_options,
                    metrics,
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
                if self
                    .segments
                    .iter()
                    .all(|artifact| artifact.rabitq_projection.is_some())
                {
                    return self.scan_rabitq_vector_scores(
                        query_embedding,
                        candidate_set,
                        retained_limit,
                        vector_execution_options,
                        metrics,
                    );
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
        candidate_set: &CandidateSet,
        retained_limit: Option<usize>,
        vector_execution_options: super::VectorSearchExecutionOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
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
        let mut collector = BoundedScoreCollector::new(retained_limit, max_scored_documents)?;
        let mut vector_document_count = 0usize;
        let mut segment_scan_count = 0usize;
        let mut scored_document_ids = BTreeSet::new();
        for (layer, artifact) in self.segments.iter().enumerate() {
            for segment in &artifact.descriptor.segments {
                checkpoint_vector_task(task_context)?;
                if candidate_set.segment_cardinality(layer, segment.segment_id) == 0 {
                    continue;
                }
                segment_scan_count = segment_scan_count.saturating_add(1);
                let documents = self.read_vector_segment(artifact, segment, metrics)?;
                for document in &documents {
                    if !candidate_set.contains(&document.id, metrics)? {
                        continue;
                    }
                    let embedding = document.embedding.as_slice();
                    if embedding.len() != query_embedding.len() || embedding.is_empty() {
                        continue;
                    }
                    if !scored_document_ids.insert(document.id.clone()) {
                        return Err(HawDBError::Storage(format!(
                            "search out-of-core segment set has duplicate document {}",
                            document.id
                        )));
                    }
                    if scored_document_ids.len() > max_scored_documents {
                        return Err(HawDBError::Storage(format!(
                            "search vector uniqueness check requires more than {max_scored_documents} document ids"
                        )));
                    }
                    vector_document_count = vector_document_count.saturating_add(1);
                    metrics.vector_bytes_read = metrics.vector_bytes_read.saturating_add(
                        (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64),
                    );
                    if let Some(score) = cosine_similarity(query_embedding, embedding)
                        && score > 0.0
                    {
                        collector.push(document.id.clone(), score)?;
                    }
                }
            }
        }
        let matching_count = collector.matching_count;
        Ok(VectorScoreScan {
            scores: collector.finish(),
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
        candidate_set: &CandidateSet,
        retained_limit: Option<usize>,
        vector_execution_options: super::VectorSearchExecutionOptions<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<VectorScoreScan> {
        self.require_compatible_rabitq_projections()?;
        let task_context = vector_execution_options.task_context;
        checkpoint_vector_task(task_context)?;
        let minimum_candidates = retained_limit.unwrap_or(1).max(1);
        if minimum_candidates > self.config.max_vector_candidates.get() {
            return Err(HawDBError::Storage(format!(
                "search vector rank window requires {minimum_candidates} candidates, exceeding {}",
                self.config.max_vector_candidates
            )));
        }
        let candidate_limit = self.config.max_vector_candidates.get();
        let total_working_bytes = self
            .config
            .max_vector_search_working_bytes
            .get()
            .min(vector_execution_options.max_working_bytes);
        // The projection can return one local top-k while the global heap is
        // retained. Sorting the heap also transiently materializes a second
        // layered vector, so reserve both representations before dispatch.
        let selected_candidate_entry_bytes = std::mem::size_of::<LayeredProjectionHit>()
            .saturating_mul(2)
            .saturating_add(
                std::mem::size_of::<hawdb_vector_projection::ProjectionHit>().saturating_mul(2),
            );
        let selected_candidate_bytes = candidate_limit
            .checked_mul(selected_candidate_entry_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("search RaBitQ candidate size overflow".to_string())
            })?;
        let scan_working_bytes = total_working_bytes
            .checked_sub(selected_candidate_bytes)
            .ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search RaBitQ retained candidates require {selected_candidate_bytes} bytes, exceeding {total_working_bytes}"
                ))
            })?;
        let mut selected = BoundedLayeredProjectionHits::new(candidate_limit);
        let mut vector_document_count = 0usize;
        let mut generated_candidate_count = 0usize;
        let mut candidate_scan_worker_count = 0usize;
        let mut candidate_scan_segment_count = 0usize;
        let mut candidate_scan_scanned_segment_count = 0usize;
        let mut candidate_scan_scored_document_count = 0usize;
        let mut candidate_scan_filtered_document_count = 0usize;
        let mut candidate_scan_scanned_block_count = 0usize;
        let mut candidate_scan_skipped_block_count = 0usize;
        let mut candidate_scan_payload_bytes_read = 0u64;
        let mut candidate_scan_admitted_working_bytes = 0usize;
        let mut candidate_scan_kernel = None;

        for (layer, artifact) in self.segments.iter().enumerate() {
            checkpoint_vector_task(task_context)?;
            let projection = artifact.rabitq_projection.as_ref().ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layer {layer} has no RaBitQ projection"
                ))
            })?;
            let allowlist = candidate_set.vector_ordinals_for_layer(
                layer,
                scan_working_bytes as u64,
                task_context,
                metrics,
            )?;
            let allowlist_bytes = allowlist.as_ref().map_or(0usize, |ids| {
                ids.capacity().saturating_mul(std::mem::size_of::<u64>())
            });
            let projection_working_bytes = scan_working_bytes
                .checked_sub(allowlist_bytes)
                .ok_or_else(|| {
                    HawDBError::Storage(format!(
                        "search vector layer {layer} allowlist requires {allowlist_bytes} bytes, exceeding {scan_working_bytes}"
                    ))
                })?;
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
            let output = projection
                .search(
                    query_embedding,
                    candidate_limit.min(projection.manifest().document_count.max(1)),
                    search_options,
                )
                .map_err(vector_projection_error)?;
            let projection_report = output.report;
            metrics.rabitq_payload_bytes_read = metrics
                .rabitq_payload_bytes_read
                .saturating_add(projection_report.payload_bytes_read);
            vector_document_count =
                vector_document_count.saturating_add(projection.manifest().document_count);
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
            candidate_scan_payload_bytes_read = candidate_scan_payload_bytes_read
                .saturating_add(projection_report.payload_bytes_read);
            candidate_scan_admitted_working_bytes = candidate_scan_admitted_working_bytes.max(
                projection_report
                    .admitted_working_bytes
                    .saturating_add(allowlist_bytes),
            );
            candidate_scan_kernel
                .get_or_insert_with(|| projection_report.kernel.as_str().to_string());
            for hit in output.hits {
                selected.push(LayeredProjectionHit {
                    layer,
                    ordinal: hit.id,
                    score: hit.score,
                });
            }
        }

        let mut selected = selected.finish();
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
            ));
        }

        let mut collector = BoundedScoreCollector::new(
            retained_limit,
            admitted_score_entries(self.config.max_score_entries.get(), scan_working_bytes),
        )?;
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
                for document in self.read_vector_segment(artifact, segment, metrics)? {
                    if selected_layer[start_index..end_index]
                        .binary_search_by_key(&document.vector_ordinal, |candidate| {
                            candidate.ordinal
                        })
                        .is_err()
                    {
                        continue;
                    }
                    metrics.vector_bytes_read = metrics.vector_bytes_read.saturating_add(
                        (document.embedding.len() as u64)
                            .saturating_mul(std::mem::size_of::<f32>() as u64),
                    );
                    reranked_candidate_count = reranked_candidate_count.saturating_add(1);
                    if let Some(score) = cosine_similarity(query_embedding, &document.embedding)
                        && score > 0.0
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
            )));
        }
        let matching_count = collector.matching_count;
        Ok(VectorScoreScan {
            scores: collector.finish(),
            matching_count,
            vector_document_count,
            segment_scan_count: raw_segment_scan_count,
            backend: "hawdb_rabitq_out_of_core_candidate_projection".to_string(),
            candidate_score_source: "quantized_projection".to_string(),
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
            candidate_scan_payload_bytes_read,
            candidate_scan_admitted_working_bytes: candidate_scan_admitted_working_bytes
                .saturating_add(selected_candidate_bytes),
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
        })
    }

    #[cfg(feature = "vector-search")]
    fn require_compatible_rabitq_projections(&self) -> Result<()> {
        let mut reference = None;
        for (layer, artifact) in self.segments.iter().enumerate() {
            let projection = artifact.rabitq_projection.as_ref().ok_or_else(|| {
                HawDBError::Storage(format!(
                    "search out-of-core layer {layer} has no RaBitQ projection"
                ))
            })?;
            let manifest = projection.manifest();
            let identity = (manifest.bit_width, manifest.transform_seed);
            if let Some(reference) = reference
                && identity != reference
            {
                return Err(HawDBError::Storage(
                    "search multi-segment RaBitQ projections require matching bit widths and transform seeds"
                        .to_string(),
                ));
            }
            reference = Some(identity);
        }
        Ok(())
    }
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
    HawDBError::Storage(format!("search RaBitQ projection: {error}"))
}

#[cfg(all(test, feature = "vector-search"))]
mod tests {
    use super::{BoundedLayeredProjectionHits, LayeredProjectionHit};

    #[test]
    fn layered_projection_hits_evict_the_lower_scored_layer() {
        let mut hits = BoundedLayeredProjectionHits::new(1);
        hits.push(LayeredProjectionHit {
            layer: 0,
            ordinal: 0,
            score: 1.0,
        });
        hits.push(LayeredProjectionHit {
            layer: 1,
            ordinal: 0,
            score: 2.0,
        });

        let hits = hits.finish();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].layer, 1);
        assert_eq!(hits[0].ordinal, 0);
    }
}
