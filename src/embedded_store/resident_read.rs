// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::search::{
    SearchIndexReadSnapshot, SearchProjectionScanBudget, SearchProjectionScanReport,
    SearchResidentTextSeedOutput,
};
use hawdb_executor::QueryMemoryAccount;

#[derive(Clone, Copy)]
pub(super) enum ResidentProjectionRef<'a> {
    Live(&'a SearchProjection),
    Snapshot(&'a SearchIndexReadSnapshot),
}

macro_rules! read_projection {
    ($source:expr, $method:ident($($argument:expr),* $(,)?)) => {
        match $source {
            ResidentProjectionRef::Live(projection) => projection.index().$method($($argument),*),
            ResidentProjectionRef::Snapshot(snapshot) => snapshot.$method($($argument),*),
        }
    };
}

impl ResidentProjectionRef<'_> {
    pub(super) fn document(&self, id: &str) -> Option<&SearchDocument> {
        read_projection!(self, document(id))
    }

    pub(super) fn freshness(&self) -> SearchProjectionFreshness {
        match self {
            Self::Live(projection) => projection.freshness(),
            Self::Snapshot(snapshot) => snapshot.projection_freshness().clone(),
        }
    }

    pub(super) fn text_seed_scores_with_context(
        &self,
        query: &str,
        filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<SearchResidentTextSeedOutput> {
        read_projection!(
            self,
            text_seed_scores_with_context(query, filters, max_candidates, memory, task)
        )
    }

    pub(super) fn complete_text_seed_scores_with_context(
        &self,
        query: &str,
        filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<SearchResidentTextSeedOutput> {
        read_projection!(
            self,
            complete_text_seed_scores_with_context(query, filters, max_candidates, memory, task)
        )
    }

    pub(super) fn visit_projection_documents(
        &self,
        budget: SearchProjectionScanBudget,
        task: &RuntimeTaskContext,
        visitor: &mut dyn FnMut(&SearchDocument) -> Result<()>,
    ) -> Result<SearchProjectionScanReport> {
        read_projection!(self, visit_projection_documents(budget, task, visitor))
    }

    pub(super) fn try_search_candidates_with_report_context(
        &self,
        request: &SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchCandidateOutput> {
        let compression = effective_search_candidate_mode(request);
        let execution = request.vector_execution_options(execution)?;
        let result = read_projection!(
            self,
            try_search_with_options_adaptive_vector_projection_context(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                SearchQueryOptions {
                    limit: request.limit,
                    offset: request.offset,
                    rank_window: request.rank_window,
                    fusion_weights: request.fusion_weights,
                    metadata_filters: request.metadata_filters.clone(),
                    policy_epoch: None,
                },
                AdaptiveVectorSearchOptions {
                    compression_mode: compression,
                    backend_policy: request.adaptive_vector_backend_policy,
                    recall_validation_probe: request.recall_validation_probe,
                },
                execution,
            )
        )?;
        SearchCandidateOutput::from_search_result(request, compression, result, None)
    }
}
