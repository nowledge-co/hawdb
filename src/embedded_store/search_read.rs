// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Search helpers share the publication's immutable graph/projection pair.

use super::*;
use crate::search::SearchIndexReadLease;

pub(super) struct AdmittedSearchRead {
    view: Arc<crate::api::PublishedConcurrentRead>,
    mode: GraphMode,
    resident: Option<SearchIndexReadLease>,
    out_of_core: Option<OutOfCoreSearchProjection>,
    advisor: RetrievalProjectionAdvisor,
    task: RuntimeTaskContext,
}

impl PublishedCanonicalRead {
    fn search_admission(&self) -> Result<RuntimeWorkRequest> {
        self.view.snapshot.ensure_usable()?;
        let config = self.view.snapshot.config();
        let result_bytes = config.max_read_result_payload_bytes.ok_or_else(|| {
            HawDBError::Execution(
                "admitted typed search requires max_read_result_payload_bytes".into(),
            )
        })?;
        let result_bytes = u64::try_from(result_bytes).map_err(|_| {
            HawDBError::Execution("admitted typed search result budget exceeds u64".into())
        })?;
        let working_bytes = u64::try_from(config.execution_memory.blocking_operator_bytes.get())
            .unwrap_or(u64::MAX);
        let memory_bytes = match (&self.resident_projection, &self.out_of_core_projection) {
            (Some(_), None) => working_bytes,
            (None, Some(projection)) => {
                projection.runtime_admission_memory_bytes(working_bytes, result_bytes)
            }
            (None, None) => return Err(missing_search_projection_error()),
            (Some(_), Some(_)) => return Err(ambiguous_search_projection_error()),
        };
        Ok(
            RuntimeWorkRequest::foreground_query(memory_bytes, result_bytes)
                .with_io_slots(self.external_seed_io_slots)
                .with_blocking(true),
        )
    }
}

impl EmbeddedStoreHandle {
    pub(super) fn with_admitted_search<T>(
        &self,
        operation: impl FnOnce(&AdmittedSearchRead) -> Result<T>,
    ) -> Result<T> {
        // Admission may invoke host callbacks. Carry metadata only across it,
        // so a writer can replace and retire the previously published source.
        let (identity, config, governor, request, resident_kind) = {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(source_changed)?;
            (
                published.instance_identity.clone(),
                published.view.snapshot.config().clone(),
                published.governor.clone(),
                published.search_admission()?,
                published.resident_projection.is_some(),
            )
        };
        let permit = try_admit_runtime(&governor, request)?;
        let (view, mode, resident, out_of_core, advisor) = {
            let published = self.lock_published_read()?;
            let published = published.as_ref().ok_or_else(source_changed)?;
            if published.instance_identity != identity
                || published.view.snapshot.config() != &config
                || published.resident_projection.is_some() != resident_kind
                || published.search_admission()? != request
            {
                return Err(source_changed());
            }
            (
                Arc::clone(&published.view),
                published.mode,
                published.resident_projection.clone(),
                published
                    .out_of_core_projection
                    .as_ref()
                    .map(|source| source.read_snapshot()),
                published.retrieval_projection_advisor.clone(),
            )
        };
        // Retained capacity can be expensive to compute or refuse. Do this
        // outside publication, dropping all refused source owners before return.
        let resident = resident
            .map(|source| source.retain_with_permit(&permit))
            .transpose()?;
        let read = AdmittedSearchRead {
            view,
            mode,
            resident,
            out_of_core,
            advisor,
            task: permit.bind_task_context(RuntimeTaskContext::default()),
        };
        self.finish_canonical_read(operation(&read))
    }
}

impl AdmittedSearchRead {
    pub(super) fn shadow_evidence_json<I, S>(
        &self,
        request: &SearchCandidateRequest,
        primary_candidate_ids: I,
    ) -> Result<serde_json::Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        // Preserve this diagnostic's resident-projection-only contract.
        if self.resident.is_none() {
            return Err(missing_search_projection_error());
        }
        let output = self.search_candidates(request)?;
        let mut accumulator = SearchCandidateShadowAccumulator::new();
        accumulator.record_search_candidate_output(primary_candidate_ids, &output);
        Ok(accumulator.evidence().json())
    }

    fn execution(&self) -> VectorSearchExecutionOptions<'_> {
        VectorSearchExecutionOptions::admitted(
            self.view
                .snapshot
                .config()
                .execution_memory
                .blocking_operator_bytes
                .get(),
            &self.task,
        )
    }

    pub(super) fn search_candidates(
        &self,
        request: &SearchCandidateRequest,
    ) -> Result<SearchCandidateOutput> {
        match (&self.resident, &self.out_of_core) {
            (Some(lease), None) => ResidentProjectionRef::Snapshot(lease.snapshot())
                .try_search_candidates_with_report_context(request, self.execution()),
            (None, Some(projection)) => {
                projection.search_candidates_with_report_context(request, self.execution())
            }
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }

    pub(super) fn hydrate(&self, document_ids: &[String]) -> Result<SearchHydrationOutput> {
        let max_payload_bytes = self
            .view
            .snapshot
            .config()
            .max_read_result_payload_bytes
            .expect("search admission requires a payload budget");
        match (&self.resident, &self.out_of_core) {
            (Some(lease), None) => {
                let mut documents = Vec::with_capacity(document_ids.len());
                let mut payload_bytes = 0usize;
                for id in document_ids {
                    let Some(document) = lease.snapshot().document(id) else {
                        continue;
                    };
                    payload_bytes =
                        payload_bytes.saturating_add(search_document_payload_bytes(document));
                    check_hydration_budget(
                        u64::try_from(payload_bytes).unwrap_or(u64::MAX),
                        max_payload_bytes,
                    )?;
                    documents.push(document.clone());
                }
                Ok(SearchHydrationOutput {
                    documents,
                    out_of_core_metrics: None,
                })
            }
            (None, Some(projection)) => {
                let output = projection.hydrate_documents(document_ids)?;
                check_hydration_budget(output.metrics.hydrated_bytes, max_payload_bytes)?;
                Ok(SearchHydrationOutput {
                    documents: output.documents,
                    out_of_core_metrics: Some(output.metrics),
                })
            }
            (None, None) => Err(missing_search_projection_error()),
            (Some(_), Some(_)) => Err(ambiguous_search_projection_error()),
        }
    }

    pub(super) fn retrieve(&self, request: &KnowledgeRetrievalRequest) -> Result<RetrievalOutput> {
        let config = self.view.snapshot.config();
        let (search, freshness, out_of_core_search_metrics) =
            match (&self.resident, &self.out_of_core) {
                (Some(lease), None) => {
                    let snapshot = lease.snapshot();
                    let search = snapshot
                        .try_search_with_options_adaptive_vector_projection_context(
                            &request.query_text,
                            request.query_embedding.as_deref(),
                            request.mode,
                            SearchQueryOptions {
                                limit: request.limit,
                                offset: request.offset,
                                rank_window: request.rank_window,
                                fusion_weights: request.search_fusion_weights,
                                metadata_filters: request.metadata_filters.clone(),
                                policy_epoch: None,
                            },
                            AdaptiveVectorSearchOptions::new(config.compressed_vector_search_mode)
                                .with_backend_policy(config.adaptive_vector_backend_policy),
                            self.execution(),
                        )?;
                    (search, snapshot.projection_freshness().clone(), None)
                }
                (None, Some(projection)) => {
                    let output = projection
                        .search_candidates_with_report_context(
                            &retrieval_request(config, &self.advisor, request),
                            self.execution(),
                        )?
                        .into_bounded();
                    (
                        output.result,
                        projection.freshness(),
                        output.out_of_core_metrics,
                    )
                }
                (None, None) => return Err(missing_search_projection_error()),
                (Some(_), Some(_)) => return Err(ambiguous_search_projection_error()),
            };
        let output = self
            .view
            .begin_read_transaction_with_context(&self.task)?
            .try_retrieve_knowledge_from_search(search, freshness, request)?;
        let report = retrieval_report(self.mode, config.compressed_vector_search_mode, &output);
        Ok(RetrievalOutput {
            output,
            report,
            out_of_core_search_metrics,
        })
    }
}

pub(super) fn retrieval_request(
    config: &DatabaseConfig,
    advisor: &RetrievalProjectionAdvisor,
    request: &KnowledgeRetrievalRequest,
) -> SearchCandidateRequest {
    SearchCandidateRequest {
        query_text: request.query_text.clone(),
        query_embedding: request.query_embedding.clone(),
        mode: request.mode,
        limit: request.limit,
        offset: request.offset,
        rank_window: request.rank_window,
        fusion_weights: request.search_fusion_weights,
        metadata_filters: request.metadata_filters.clone(),
        compressed_vector_search_mode: config.compressed_vector_search_mode,
        adaptive_vector_backend_policy: config.adaptive_vector_backend_policy,
        recall_validation_probe: false,
        retrieval_projection_advisor: advisor.clone(),
        exact_vector_search: false,
    }
}

fn check_hydration_budget(payload_bytes: u64, limit: usize) -> Result<()> {
    if payload_bytes > u64::try_from(limit).unwrap_or(u64::MAX) {
        return Err(HawDBError::read_budget_exceeded(
            ReadBudgetResource::PayloadBytes,
            limit,
            format!(
                "search projection document hydration produced {payload_bytes} payload bytes, limit is {limit}"
            ),
        ));
    }
    Ok(())
}

fn source_changed() -> HawDBError {
    HawDBError::Execution("search source changed during admission; retry the query".into())
}
