use super::{
    cosine_similarity, SearchDocument, SearchFallbackReasonCode, VectorSearchBackend,
    VectorSearchExecutionOptions,
};
use crate::error::{Result, SkeinError};
use skein_executor::{
    execute_vector_plan, VectorCandidate, VectorCandidateBatch, VectorCandidateScanMetrics,
    VectorCandidateScanRequest, VectorExecutionReport, VectorExecutionSource,
    VectorRawRerankRequest, VectorRawScore, VectorResidualFilterRequest, VectorScoreSource,
};
use skein_optimizer::{
    plan_vector_search, OptimizerContext, QueryFamily, ResourceHints, VectorPrecision,
};
use skein_plan::VectorSearchLogicalPlan;
use std::collections::BTreeMap;
#[cfg(feature = "qualification")]
use std::collections::BTreeSet;

pub(super) struct SearchVectorExecution {
    pub scores: BTreeMap<String, f64>,
    pub candidate_ids: Vec<String>,
    pub report: VectorExecutionReport,
}

pub(super) struct SearchVectorExecutionRequest<'a, 'b> {
    pub query_embedding: &'a [f32],
    pub documents: &'a [&'a SearchDocument],
    pub backend: VectorSearchBackend<'a>,
    pub filter_fields: Vec<String>,
    pub limit: usize,
    pub rank_window: Option<usize>,
    pub capture_candidate_ids: bool,
    pub vector_execution_options: VectorSearchExecutionOptions<'a>,
    pub fallback_reason_codes: &'b mut Vec<SearchFallbackReasonCode>,
    pub fallback_reasons: &'b mut Vec<String>,
}

pub(super) fn execute_search_vector_plan(
    request: SearchVectorExecutionRequest<'_, '_>,
) -> Result<SearchVectorExecution> {
    let SearchVectorExecutionRequest {
        query_embedding,
        documents,
        backend,
        filter_fields,
        limit,
        rank_window,
        capture_candidate_ids,
        vector_execution_options,
        fallback_reason_codes,
        fallback_reasons,
    } = request;
    let retrieval_limit = match backend {
        VectorSearchBackend::Scalar => documents.len().max(1),
        _ => limit.max(rank_window.unwrap_or(0)).max(1),
    };
    let logical = VectorSearchLogicalPlan {
        embedding_dimension: query_embedding.len(),
        filter_fields,
        residual_filter_fields: Vec::new(),
        initial_candidate_limit: retrieval_limit,
        candidate_source: backend.candidate_source(),
        candidate_limit: retrieval_limit,
        top_k: retrieval_limit,
    };
    let context = OptimizerContext::default()
        .with_query_family(QueryFamily::VectorSearch)
        .with_resource_hints(ResourceHints {
            priority: 128,
            max_memory_bytes: Some(
                u64::try_from(vector_execution_options.max_working_bytes).unwrap_or(u64::MAX),
            ),
            max_parallelism: vector_execution_options.max_parallelism.get(),
        });
    let planned = plan_vector_search(&logical, &context)
        .map_err(|error| SkeinError::Storage(format!("vector planning failed: {error}")))?;
    debug_assert_eq!(planned.properties.precision, VectorPrecision::RawReranked);
    let vector_execution_options = VectorSearchExecutionOptions {
        max_parallelism: std::num::NonZeroUsize::new(
            vector_execution_options
                .max_parallelism
                .get()
                .min(planned.properties.max_parallelism.max(1)),
        )
        .expect("planned vector parallelism is non-zero"),
        max_working_bytes: planned
            .properties
            .max_memory_bytes
            .and_then(|bytes| usize::try_from(bytes).ok())
            .map_or(vector_execution_options.max_working_bytes, |bytes| {
                bytes.min(vector_execution_options.max_working_bytes)
            }),
        ..vector_execution_options
    };

    let mut source = SearchVectorSource {
        query_embedding,
        documents,
        backend,
        fallback_reason_codes,
        fallback_reasons,
        raw_vector_bytes_read: 0,
        candidate_scan_metrics: None,
        capture_candidate_ids,
        vector_execution_options,
    };
    let output = execute_vector_plan(&planned.plan, &mut source).map_err(|error| {
        SkeinError::Storage(format!("vector physical execution failed: {error}"))
    })?;
    Ok(SearchVectorExecution {
        candidate_ids: output.candidate_ids,
        scores: output
            .scores
            .into_iter()
            .filter(|score| score.score > 0.0)
            .map(|score| (score.id, score.score))
            .collect(),
        report: output.report,
    })
}

struct SearchVectorSource<'a, 'b> {
    query_embedding: &'a [f32],
    documents: &'a [&'a SearchDocument],
    backend: VectorSearchBackend<'a>,
    fallback_reason_codes: &'b mut Vec<SearchFallbackReasonCode>,
    fallback_reasons: &'b mut Vec<String>,
    raw_vector_bytes_read: u64,
    candidate_scan_metrics: Option<VectorCandidateScanMetrics>,
    capture_candidate_ids: bool,
    #[cfg_attr(not(feature = "vector-search"), allow(dead_code))]
    vector_execution_options: VectorSearchExecutionOptions<'a>,
}

impl VectorExecutionSource for SearchVectorSource<'_, '_> {
    type Error = SkeinError;

    fn scan_candidates(
        &mut self,
        request: VectorCandidateScanRequest<'_>,
    ) -> Result<VectorCandidateBatch> {
        self.checkpoint()?;
        debug_assert_eq!(request.source, self.backend.candidate_source());
        debug_assert_eq!(request.embedding_dimension, self.query_embedding.len());
        match self.backend {
            VectorSearchBackend::Scalar => self.raw_vector_candidates(),
            VectorSearchBackend::CompressedRequiredUnavailable => {
                self.fallback_reason_codes
                    .push(SearchFallbackReasonCode::CompressedVectorProjectionUnavailable);
                self.fallback_reasons.push(
                    "compressed vector projection required but unavailable; scalar vector scan disabled"
                        .to_string(),
                );
                Ok(VectorCandidateBatch {
                    score_source: VectorScoreSource::Unavailable,
                    candidates: Vec::new(),
                })
            }
            #[cfg(not(feature = "vector-search"))]
            VectorSearchBackend::_Lifetime(_) => {
                unreachable!("lifetime marker is never constructed")
            }
            #[cfg(feature = "vector-search")]
            VectorSearchBackend::RaBitQ {
                projection,
                required,
            } => {
                let filtered_vector_count = self
                    .documents
                    .iter()
                    .filter(|document| document.embedding.is_some())
                    .count();
                let allowlist = (filtered_vector_count != projection.manifest().document_count)
                    .then_some(self.documents);
                match projection.search_candidates_for_documents_with_options(
                    self.query_embedding,
                    request.candidate_limit,
                    allowlist,
                    super::rabitq_projection::RaBitQCandidateScanOptions {
                        max_parallelism: self.vector_execution_options.max_parallelism,
                        max_working_bytes: self.vector_execution_options.max_working_bytes,
                        kernel: self.vector_execution_options.kernel.projection_preference(),
                        task_context: self.vector_execution_options.task_context,
                    },
                ) {
                    Ok(output) => {
                        self.record_candidate_scan_metrics(&output.report);
                        Ok(VectorCandidateBatch {
                            score_source: VectorScoreSource::QuantizedApproximate,
                            candidates: output
                                .candidates
                                .into_iter()
                                .map(|hit| VectorCandidate {
                                    id: hit.id,
                                    score: hit.score,
                                })
                                .collect(),
                        })
                    }
                    Err(error) => {
                        if required {
                            self.fallback_reason_codes.push(
                                SearchFallbackReasonCode::CompressedVectorProjectionUnavailable,
                            );
                            self.fallback_reasons.push(format!(
                                "Skein RaBitQ projection is required but candidate scan failed: {error}"
                            ));
                            Ok(VectorCandidateBatch {
                                score_source: VectorScoreSource::Unavailable,
                                candidates: Vec::new(),
                            })
                        } else {
                            self.fallback_reason_codes
                                .push(SearchFallbackReasonCode::VectorIndexEmpty);
                            self.fallback_reasons.push(format!(
                                "Skein RaBitQ projection unavailable; fell back to scalar vector scan: {error}"
                            ));
                            self.raw_vector_candidates()
                        }
                    }
                }
            }
            #[cfg(feature = "qualification")]
            VectorSearchBackend::ExternalValidation { candidates, .. } => {
                let allowlist = self
                    .documents
                    .iter()
                    .map(|document| document.id.as_str())
                    .collect::<BTreeSet<_>>();
                Ok(VectorCandidateBatch {
                    score_source: VectorScoreSource::QuantizedApproximate,
                    candidates: candidates
                        .iter()
                        .filter(|(id, _)| allowlist.contains(id.as_str()))
                        .take(request.candidate_limit)
                        .map(|(id, score)| VectorCandidate {
                            id: id.clone(),
                            score: *score,
                        })
                        .collect(),
                })
            }
        }
    }

    fn rerank_raw(&mut self, request: VectorRawRerankRequest<'_>) -> Result<Vec<VectorRawScore>> {
        self.checkpoint()?;
        debug_assert_eq!(request.embedding_dimension, self.query_embedding.len());
        let candidate_bytes = request.candidates.iter().fold(0usize, |total, candidate| {
            total
                .saturating_add(std::mem::size_of::<VectorCandidate>())
                .saturating_add(std::mem::size_of::<VectorRawScore>())
                .saturating_add(candidate.id.capacity().saturating_mul(2))
        });
        let lookup_bytes = self
            .documents
            .len()
            .saturating_mul(std::mem::size_of::<(&str, &SearchDocument)>().saturating_mul(3));
        let required_working_bytes = candidate_bytes.saturating_add(lookup_bytes);
        if required_working_bytes > self.vector_execution_options.max_working_bytes {
            return Err(SkeinError::Execution(format!(
                "raw vector rerank requires {required_working_bytes} estimated bytes, exceeding admitted working memory {}",
                self.vector_execution_options.max_working_bytes
            )));
        }
        let documents = self
            .documents
            .iter()
            .map(|document| (document.id.as_str(), *document))
            .collect::<BTreeMap<_, _>>();
        let mut scores = Vec::with_capacity(request.candidates.len());
        for candidate in request.candidates {
            self.checkpoint()?;
            let Some(document) = documents.get(candidate.id.as_str()) else {
                continue;
            };
            let Some(embedding) = document.embedding.as_deref() else {
                continue;
            };
            if embedding.len() != self.query_embedding.len() || embedding.is_empty() {
                continue;
            }
            self.raw_vector_bytes_read = self
                .raw_vector_bytes_read
                .saturating_add(vector_bytes(embedding.len()));
            let Some(score) = cosine_similarity(self.query_embedding, embedding) else {
                continue;
            };
            scores.push(VectorRawScore {
                id: candidate.id.clone(),
                score,
            });
        }
        Ok(scores)
    }

    fn filter_residual(
        &mut self,
        request: VectorResidualFilterRequest<'_>,
    ) -> Result<Vec<VectorCandidate>> {
        self.checkpoint()?;
        debug_assert!(
            request.fields.is_empty(),
            "supported Nowledge filters must be descriptor-safe"
        );
        Ok(request.candidates)
    }

    fn raw_vector_bytes_read(&self) -> u64 {
        self.raw_vector_bytes_read
    }

    fn candidate_scan_metrics(&self) -> Option<VectorCandidateScanMetrics> {
        self.candidate_scan_metrics.clone()
    }

    fn capture_candidate_ids(&self) -> bool {
        self.capture_candidate_ids
    }
}

impl SearchVectorSource<'_, '_> {
    fn checkpoint(&self) -> Result<()> {
        match self.vector_execution_options.task_context {
            Some(task_context) => task_context
                .checkpoint()
                .map_err(|reason| SkeinError::Execution(format!("vector search task {reason}"))),
            None => Ok(()),
        }
    }

    #[cfg(feature = "vector-search")]
    fn record_candidate_scan_metrics(
        &mut self,
        report: &skein_vector_projection::ProjectionSearchReport,
    ) {
        let next = VectorCandidateScanMetrics {
            kernel: report.kernel.as_str().to_string(),
            worker_count: report.worker_count,
            segment_count: report.segment_count,
            scanned_segment_count: report.scanned_segment_count,
            scored_document_count: report.scored_document_count,
            filtered_document_count: report.filtered_document_count,
            scanned_block_count: report.scanned_block_count,
            skipped_block_count: report.skipped_block_count,
            payload_bytes_read: report.payload_bytes_read,
            admitted_working_bytes: report.admitted_working_bytes,
        };
        if let Some(existing) = &mut self.candidate_scan_metrics {
            if existing.kernel != next.kernel {
                existing.kernel = "mixed".to_string();
            }
            existing.worker_count = existing.worker_count.max(next.worker_count);
            existing.segment_count = existing.segment_count.max(next.segment_count);
            existing.scanned_segment_count = existing
                .scanned_segment_count
                .saturating_add(next.scanned_segment_count);
            existing.scored_document_count = existing
                .scored_document_count
                .saturating_add(next.scored_document_count);
            existing.filtered_document_count = existing
                .filtered_document_count
                .saturating_add(next.filtered_document_count);
            existing.scanned_block_count = existing
                .scanned_block_count
                .saturating_add(next.scanned_block_count);
            existing.skipped_block_count = existing
                .skipped_block_count
                .saturating_add(next.skipped_block_count);
            existing.payload_bytes_read = existing
                .payload_bytes_read
                .saturating_add(next.payload_bytes_read);
            existing.admitted_working_bytes = existing
                .admitted_working_bytes
                .max(next.admitted_working_bytes);
        } else {
            self.candidate_scan_metrics = Some(next);
        }
    }

    fn raw_vector_candidates(&mut self) -> Result<VectorCandidateBatch> {
        let mut candidates = Vec::new();
        let mut id_bytes = 0usize;
        for document in self.documents {
            self.checkpoint()?;
            let Some(embedding) = document.embedding.as_deref() else {
                continue;
            };
            if embedding.len() != self.query_embedding.len() || embedding.is_empty() {
                continue;
            }
            self.raw_vector_bytes_read = self
                .raw_vector_bytes_read
                .saturating_add(vector_bytes(embedding.len()));
            let Some(score) = cosine_similarity(self.query_embedding, embedding) else {
                continue;
            };
            let id = document.id.clone();
            id_bytes = id_bytes.saturating_add(id.capacity());
            candidates.push(VectorCandidate { id, score });
            let candidate_bytes = candidates
                .capacity()
                .saturating_mul(std::mem::size_of::<VectorCandidate>())
                .saturating_add(id_bytes);
            if candidate_bytes > self.vector_execution_options.max_working_bytes {
                return Err(SkeinError::Execution(format!(
                    "scalar vector candidates require {candidate_bytes} bytes, exceeding admitted working memory {}",
                    self.vector_execution_options.max_working_bytes
                )));
            }
        }
        Ok(VectorCandidateBatch {
            score_source: VectorScoreSource::RawVector,
            candidates,
        })
    }
}

fn vector_bytes(dimension: usize) -> u64 {
    u64::try_from(dimension)
        .unwrap_or(u64::MAX)
        .saturating_mul(std::mem::size_of::<f32>() as u64)
}
