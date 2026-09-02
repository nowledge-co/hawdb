use super::{BoundedScoreCollector, CandidateSet, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use crate::error::{Result, SkeinError};
use crate::{cosine_similarity, CompressedVectorSearchMode, SearchFallbackReasonCode};
use std::collections::BTreeMap;
#[cfg(feature = "vector-search")]
use std::num::NonZeroUsize;

const VECTOR_SCORE_ENTRY_WORKING_BYTES: usize = 128;

fn admitted_score_entries(configured_max_entries: usize, working_bytes: usize) -> usize {
    configured_max_entries.min(working_bytes / VECTOR_SCORE_ENTRY_WORKING_BYTES)
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
                if self.rabitq_projection.is_some() {
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
                    "out-of-core RaBitQ projection is not attached to this generation; used exact scalar vector segments"
                        .to_string(),
                );
                Ok(scan)
            }
            CompressedVectorSearchMode::Required => {
                #[cfg(feature = "vector-search")]
                if self.rabitq_projection.is_some() {
                    return self.scan_rabitq_vector_scores(
                        query_embedding,
                        candidate_set,
                        retained_limit,
                        vector_execution_options,
                        metrics,
                    );
                }
                Err(SkeinError::Storage(
                    "out-of-core RaBitQ projection is required but unavailable for this generation"
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
        let mut collector = BoundedScoreCollector::new(
            retained_limit,
            admitted_score_entries(
                self.config.max_score_entries.get(),
                vector_execution_options.max_working_bytes,
            ),
        )?;
        let mut vector_document_count = 0usize;
        let mut segment_scan_count = 0usize;
        for segment in &self.descriptor.segments {
            checkpoint_vector_task(task_context)?;
            if candidate_set.segment_cardinality(segment.segment_id) == 0 {
                continue;
            }
            segment_scan_count = segment_scan_count.saturating_add(1);
            let documents = self.read_vector_segment(segment, metrics)?;
            for document in &documents {
                if !candidate_set.contains(&document.id, metrics)? {
                    continue;
                }
                let embedding = document.embedding.as_slice();
                if embedding.len() != query_embedding.len() || embedding.is_empty() {
                    continue;
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
        let task_context = vector_execution_options.task_context;
        let projection = self.rabitq_projection.as_ref().ok_or_else(|| {
            SkeinError::Storage("search out-of-core RaBitQ projection is unavailable".to_string())
        })?;
        checkpoint_vector_task(task_context)?;
        let minimum_candidates = retained_limit.unwrap_or(1).max(1);
        if minimum_candidates > self.config.max_vector_candidates.get() {
            return Err(SkeinError::Storage(format!(
                "search vector rank window requires {minimum_candidates} candidates, exceeding {}",
                self.config.max_vector_candidates
            )));
        }
        let candidate_limit = self
            .config
            .max_vector_candidates
            .get()
            .min(projection.manifest().document_count.max(1));
        let total_working_bytes = self
            .config
            .max_vector_search_working_bytes
            .get()
            .min(vector_execution_options.max_working_bytes);
        let allowlist =
            candidate_set.vector_ordinals(total_working_bytes as u64, task_context, metrics)?;
        let allowlist_bytes = allowlist.as_ref().map_or(0usize, |ids| {
            ids.capacity().saturating_mul(std::mem::size_of::<u64>())
        });
        let scan_working_bytes = total_working_bytes
            .checked_sub(allowlist_bytes)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "search vector allowlist requires {allowlist_bytes} bytes, exceeding {total_working_bytes}"
                ))
            })?;
        let mut search_options = skein_vector_projection::ProjectionSearchOptions::new()
            .with_max_parallelism(
                NonZeroUsize::new(
                    self.config
                        .max_vector_search_parallelism
                        .get()
                        .min(vector_execution_options.max_parallelism.get()),
                )
                .expect("out-of-core vector parallelism is non-zero"),
            )
            .with_max_working_bytes(scan_working_bytes)
            .with_kernel(vector_execution_options.kernel.projection_preference());
        if let Some(allowlist) = allowlist.as_deref() {
            search_options = search_options.with_allowed_ids(allowlist);
        }
        if let Some(task_context) = task_context {
            search_options = search_options.with_task_context(task_context);
        }
        let output = projection
            .search(query_embedding, candidate_limit, search_options)
            .map_err(vector_projection_error)?;
        let projection_report = output.report;
        metrics.rabitq_payload_bytes_read = metrics
            .rabitq_payload_bytes_read
            .saturating_add(projection_report.payload_bytes_read);
        let mut selected_ordinals = output
            .hits
            .into_iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>();
        selected_ordinals.sort_unstable();
        if selected_ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(SkeinError::Storage(
                "search RaBitQ projection returned duplicate candidate ordinals".to_string(),
            ));
        }

        let retained_working_bytes = allowlist_bytes.saturating_add(
            selected_ordinals
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        );
        let score_working_bytes = total_working_bytes
            .checked_sub(retained_working_bytes)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "search vector retained candidates require {retained_working_bytes} bytes, exceeding {total_working_bytes}"
                ))
            })?;
        let mut collector = BoundedScoreCollector::new(
            retained_limit,
            admitted_score_entries(self.config.max_score_entries.get(), score_working_bytes),
        )?;
        let mut raw_segment_scan_count = 0usize;
        let mut reranked_candidate_count = 0usize;
        for (layout, segment) in self.layout.segments.iter().zip(&self.descriptor.segments) {
            checkpoint_vector_task(task_context)?;
            let end = layout
                .vector_ordinal_base
                .saturating_add(layout.vectors.entry_count as u64);
            let start_index =
                selected_ordinals.partition_point(|ordinal| *ordinal < layout.vector_ordinal_base);
            let end_index = selected_ordinals.partition_point(|ordinal| *ordinal < end);
            if start_index == end_index {
                continue;
            }
            raw_segment_scan_count = raw_segment_scan_count.saturating_add(1);
            for document in self.read_vector_segment(segment, metrics)? {
                if selected_ordinals[start_index..end_index]
                    .binary_search(&document.vector_ordinal)
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
        if reranked_candidate_count != selected_ordinals.len() {
            return Err(SkeinError::Storage(format!(
                "search RaBitQ raw rerank hydrated {reranked_candidate_count} of {} candidates",
                selected_ordinals.len()
            )));
        }
        let matching_count = collector.matching_count;
        Ok(VectorScoreScan {
            scores: collector.finish(),
            matching_count,
            vector_document_count: projection.manifest().document_count,
            segment_scan_count: raw_segment_scan_count,
            backend: "skein_rabitq_out_of_core_candidate_projection".to_string(),
            candidate_score_source: "quantized_projection".to_string(),
            generated_candidate_count: projection_report.candidate_count,
            reranked_candidate_count,
            candidate_scan_kernel: Some(projection_report.kernel.as_str().to_string()),
            candidate_scan_worker_count: projection_report.worker_count,
            candidate_scan_segment_count: projection_report.segment_count,
            candidate_scan_scanned_segment_count: projection_report.scanned_segment_count,
            candidate_scan_scored_document_count: projection_report.scored_document_count,
            candidate_scan_filtered_document_count: projection_report.filtered_document_count,
            candidate_scan_scanned_block_count: projection_report.scanned_block_count,
            candidate_scan_skipped_block_count: projection_report.skipped_block_count,
            candidate_scan_payload_bytes_read: projection_report.payload_bytes_read,
            candidate_scan_admitted_working_bytes: projection_report
                .admitted_working_bytes
                .saturating_add(allowlist_bytes),
            fallback_reason_codes: Vec::new(),
            fallback_reasons: Vec::new(),
        })
    }
}

fn checkpoint_vector_task(task_context: Option<&crate::RuntimeTaskContext>) -> Result<()> {
    task_context.map_or(Ok(()), |task_context| {
        task_context
            .checkpoint()
            .map_err(|reason| SkeinError::Execution(format!("search vector task {reason}")))
    })
}

#[cfg(feature = "vector-search")]
pub(super) fn vector_projection_error(
    error: skein_vector_projection::ProjectionError,
) -> SkeinError {
    SkeinError::Storage(format!("search RaBitQ projection: {error}"))
}
