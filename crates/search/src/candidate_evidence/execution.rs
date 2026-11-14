// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bind candidate evidence to the request used by the actual retriever.

use super::{effective_search_candidate_mode, SearchCandidateOutput, SearchCandidateRequest};
use crate::{
    AdaptiveVectorSearchOptions, Result, SearchIndex, SearchOutOfCoreExecutionContext,
    SearchOutOfCoreReader, SearchQueryOptions, VectorSearchExecutionOptions,
};

fn query_options(request: &SearchCandidateRequest) -> SearchQueryOptions {
    SearchQueryOptions {
        limit: request.limit,
        offset: request.offset,
        rank_window: request.rank_window,
        fusion_weights: request.fusion_weights,
        metadata_filters: request.metadata_filters.clone(),
        policy_epoch: None,
    }
}

fn adaptive_options(request: &SearchCandidateRequest) -> AdaptiveVectorSearchOptions {
    AdaptiveVectorSearchOptions {
        compression_mode: effective_search_candidate_mode(request),
        backend_policy: request.adaptive_vector_backend_policy,
        recall_validation_probe: request.recall_validation_probe,
    }
}

impl SearchIndex {
    /// Retrieve resident candidates and bind their evidence before returning them.
    pub fn search_candidates_with_report(
        &self,
        request: &SearchCandidateRequest,
    ) -> Result<SearchCandidateOutput> {
        if request.exact_vector_search {
            return self.try_search_candidates_with_report_context(
                request,
                VectorSearchExecutionOptions::default(),
            );
        }
        let adaptive = adaptive_options(request);
        let result = self.search_with_options_adaptive_vector_projection(
            &request.query_text,
            request.query_embedding.as_deref(),
            request.mode,
            query_options(request),
            adaptive,
        )?;
        SearchCandidateOutput::from_search_result(request, adaptive.compression_mode, result, None)
    }

    /// Preserve the admitted execution limits and persisted-range read path.
    pub fn try_search_candidates_with_report_context(
        &self,
        request: &SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchCandidateOutput> {
        let execution = request.vector_execution_options(execution)?;
        let adaptive = adaptive_options(request);
        let result = self.try_search_with_options_adaptive_vector_projection_context(
            &request.query_text,
            request.query_embedding.as_deref(),
            request.mode,
            query_options(request),
            adaptive,
            execution,
        )?;
        SearchCandidateOutput::from_search_result(request, adaptive.compression_mode, result, None)
    }
}

impl SearchOutOfCoreReader {
    /// Bind out-of-core evidence and metrics to the executed request.
    pub fn search_candidates_with_report_context(
        &self,
        request: &SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchCandidateOutput> {
        self.search_candidates_with_report_execution(request, execution, None)
    }

    /// Bind candidate evidence while charging the caller's admitted working memory.
    ///
    /// See [`Self::search_with_options_admitted`] for the working-memory and
    /// result-budget contract. The execution task context must be bound to `permit`.
    pub fn search_candidates_with_report_admitted(
        &self,
        request: &SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
        permit: &hawdb_qos::RuntimePermit,
    ) -> Result<SearchCandidateOutput> {
        self.search_candidates_with_report_execution(request, execution, Some(permit))
    }

    fn search_candidates_with_report_execution(
        &self,
        request: &SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
        permit: Option<&hawdb_qos::RuntimePermit>,
    ) -> Result<SearchCandidateOutput> {
        let compression = effective_search_candidate_mode(request);
        let execution = request.vector_execution_options(execution)?;
        let output = match permit {
            Some(permit) => self.search_with_options_admitted(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                query_options(request),
                SearchOutOfCoreExecutionContext {
                    access_control: None,
                    compressed_vector_search_mode: compression,
                    vector_execution_options: execution,
                },
                permit,
            )?,
            None => self.search_with_options_compressed_vector_projection_execution_options(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                query_options(request),
                compression,
                execution,
            )?,
        };
        SearchCandidateOutput::from_search_result(
            request,
            compression,
            output.result,
            Some(output.metrics),
        )
    }
}
