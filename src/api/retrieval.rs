// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Retrieval implementation behind the embedded facade.

use super::*;

impl Database {
    /// Retrieves from resident search payloads, propagating search and graph/pipeline errors.
    pub fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.runtime.get()?.catalog,
            store: &self.runtime.get()?.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge(search_index, request)
    }

    pub fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.runtime.get()?.catalog,
            store: &self.runtime.get()?.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .try_retrieve_knowledge(search_index, request)
    }

    pub(crate) fn try_retrieve_knowledge_from_search(
        &self,
        search: SearchResultSet,
        projection_freshness: SearchProjectionFreshness,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        KnowledgeRetrievalGraphContext {
            catalog: &self.runtime.get()?.catalog,
            store: &self.runtime.get()?.store,
            compressed_vector_search_mode: self.config.compressed_vector_search_mode,
            adaptive_vector_backend_policy: self.config.adaptive_vector_backend_policy,
            query_memory_budget: self.config.execution_memory.query_memory_bytes,
            result_payload_budget: self
                .config
                .max_read_result_payload_bytes
                .unwrap_or(DEFAULT_MAX_READ_RESULT_PAYLOAD_BYTES),
        }
        .retrieve_knowledge_from_search(search, projection_freshness, request)
    }
}

pub(super) struct KnowledgeRetrievalGraphContext<'a, S = GraphStore> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a S,
    pub(super) compressed_vector_search_mode: CompressedVectorSearchMode,
    pub(super) adaptive_vector_backend_policy: hawdb_optimizer::AdaptiveVectorBackendPolicy,
    pub(super) query_memory_budget: NonZeroUsize,
    pub(super) result_payload_budget: usize,
}

impl<S: crate::executor::ExecutionStore> KnowledgeRetrievalGraphContext<'_, S> {
    pub(super) fn retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        self.retrieve_knowledge_internal(search_index, request, false)
    }

    pub(super) fn try_retrieve_knowledge(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        self.retrieve_knowledge_internal(search_index, request, true)
    }

    pub(super) fn retrieve_knowledge_internal(
        &self,
        search_index: &SearchIndex,
        request: &KnowledgeRetrievalRequest,
        use_physical_range_reads: bool,
    ) -> Result<KnowledgeRetrievalOutput> {
        let search_options = SearchQueryOptions {
            limit: request.limit,
            offset: request.offset,
            rank_window: request.rank_window,
            fusion_weights: request.search_fusion_weights,
            metadata_filters: request.metadata_filters.clone(),
            policy_epoch: None,
        };
        let search = if use_physical_range_reads {
            search_index.try_search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                search_options,
                AdaptiveVectorSearchOptions::new(self.compressed_vector_search_mode)
                    .with_backend_policy(self.adaptive_vector_backend_policy),
            )?
        } else {
            search_index.search_with_options_adaptive_vector_projection(
                &request.query_text,
                request.query_embedding.as_deref(),
                request.mode,
                search_options,
                AdaptiveVectorSearchOptions::new(self.compressed_vector_search_mode)
                    .with_backend_policy(self.adaptive_vector_backend_policy),
            )?
        };
        self.retrieve_knowledge_from_search(search, search_index.projection_freshness(), request)
    }

    pub(super) fn retrieve_knowledge_from_search(
        &self,
        mut search: SearchResultSet,
        projection_freshness: SearchProjectionFreshness,
        request: &KnowledgeRetrievalRequest,
    ) -> Result<KnowledgeRetrievalOutput> {
        let graph_commit_epoch =
            <S as hawdb_storage::graph_engine::GraphReadEngine>::commit_epoch(self.store);
        let mut pipeline =
            hawdb_search::knowledge_retrieval_pipeline::KnowledgeRetrievalPipelineBudget::new_with_budget_option(
                self.query_memory_budget,
                self.result_payload_budget,
                "ExecutionMemoryConfig.query_memory_bytes",
            )?;
        let graph_read_account = pipeline.working_account();
        let graph = KnowledgeRetrievalPipelineContext {
            catalog: self.catalog,
            store: self.store,
            query_memory_budget: self.query_memory_budget,
            account: &graph_read_account,
            owned: knowledge_graph_read::KnowledgeOwnedState::new(&graph_read_account)?,
        };
        pipeline.enter(KnowledgeRetrievalStage::SearchCandidate)?;
        pipeline.enter(KnowledgeRetrievalStage::MetadataFilter)?;
        let canonical_search_nodes =
            graph.canonical_search_nodes(&search, &request.metadata_filters)?;
        let search_hit_count = search.hits.len();
        search
            .hits
            .retain(|hit| canonical_search_nodes.contains_key(&hit.id));
        let canonical_identity_filtered_out_count =
            search_hit_count.saturating_sub(search.hits.len());
        let graph_seed_search = graph.search_knowledge_graph_seeds(
            &request.query_text,
            request.graph_seed_limit,
            &request.metadata_filters,
        )?;
        pipeline.enter(KnowledgeRetrievalStage::AuthorizedGraphExpand)?;
        let graph_context_search = graph.expand_knowledge_context(
            &canonical_search_nodes,
            &graph_seed_search.seeds,
            &request.metadata_filters,
            request.graph_context_limit,
            request.graph_context_max_hops,
        )?;
        let evidence = graph.knowledge_evidence_for_search(
            &search,
            &canonical_search_nodes,
            &graph_context_search.paths,
        );
        pipeline.enter(KnowledgeRetrievalStage::Rerank)?;
        let mut candidates = graph.knowledge_candidates(
            &search,
            &evidence,
            &graph_seed_search.seeds,
            &graph_context_search.paths,
            &request.candidate_scoring,
            scoring_reference_time_millis(),
        )?;
        pipeline.retain_working(knowledge_candidates_memory_bytes(&candidates))?;
        let candidate_total_count = candidates.len();
        pipeline.enter(KnowledgeRetrievalStage::TopK)?;
        let mut candidate_fanout_details = Vec::new();
        if let Some(limit) = request.candidate_limit {
            candidates.truncate(limit);
            if candidate_total_count > limit {
                candidate_fanout_details.push(KnowledgeFanoutReasonDetail::candidate_limit(
                    limit,
                    candidate_total_count,
                ));
            }
        }
        pipeline.enter(KnowledgeRetrievalStage::CanonicalHydration)?;
        let (graph_seeds, graph_context_paths, canonical_hydrated_node_count) = graph
            .hydrate_knowledge_output(
                &mut candidates,
                &graph_seed_search.seeds,
                &graph_context_search.paths,
            )?;
        let required_projection_commit_epoch =
            <S as hawdb_storage::graph_engine::GraphReadEngine>::search_projection_changefeed_status(
                self.store,
            )
            .required_projection_commit_epoch();
        let retrievers = knowledge_retriever_reports(
            &search,
            &evidence,
            &graph_seeds,
            &graph_context_paths,
            &projection_freshness,
            KnowledgeGraphSeedRetrieverInput {
                limit: request.graph_seed_limit,
                input_candidate_count: graph_seed_search.input_candidate_count,
                input_filtered_out_count: graph_seed_search.input_filtered_out_count,
                metadata_filters: request.metadata_filters.clone(),
                candidate_count: graph_seed_search.candidate_count,
                graph_commit_epoch,
            },
        );
        let mut fanout_reason_details = graph_context_search.fanout_reason_details.clone();
        fanout_reason_details.extend(graph_seed_search.fanout_reason_details.clone());
        fanout_reason_details.extend(candidate_fanout_details);
        let fanout_reason_codes = knowledge_fanout_reason_codes(&fanout_reason_details);
        let fanout_reasons = knowledge_fanout_reason_messages(&fanout_reason_details);
        let (result_memory_bytes, result_payload_bytes) =
            knowledge_retrieval_result_resource_bytes(KnowledgeRetrievalResultResources {
                search: &search,
                retrievers: &retrievers,
                candidates: &candidates,
                evidence: &evidence,
                graph_seeds: &graph_seeds,
                graph_context_paths: &graph_context_paths,
                fanout_reason_details: &fanout_reason_details,
                fanout_reasons: &fanout_reasons,
            });
        graph.owned.release_for_result(&graph_read_account)?;
        pipeline.retain_result(result_memory_bytes, result_payload_bytes)?;
        let pipeline_report = pipeline.finish(
            graph_commit_epoch,
            canonical_identity_filtered_out_count,
            canonical_hydrated_node_count,
            candidates
                .iter()
                .filter(|candidate| candidate.entity.is_some())
                .count(),
            true,
        )?;
        let diagnostics = knowledge_retrieval_diagnostics(
            &search,
            request,
            &projection_freshness,
            graph_commit_epoch,
            required_projection_commit_epoch,
            KnowledgeRetrievalDiagnosticsInput {
                graph_seed_input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
                    graph_seed_search.input_candidate_count,
                    graph_commit_epoch,
                    graph_seed_search.input_filtered_out_count,
                    request.metadata_filters.clone(),
                ),
                graph_seed_candidate_set: knowledge_graph_seed_candidate_set_report(
                    graph_seed_search.seeds.len(),
                    graph_commit_epoch,
                ),
                graph_seed_candidate_count: graph_seed_search.candidate_count,
                graph_seed_returned_count: graph_seeds.len(),
                graph_context_input_candidate_set:
                    knowledge_graph_context_input_candidate_set_report(
                        graph_context_search.input_seed_count,
                        graph_commit_epoch,
                    ),
                graph_context_candidate_set: knowledge_graph_context_candidate_set_report(
                    graph_context_search.expanded_relationship_count,
                    graph_commit_epoch,
                ),
                graph_context_path_count: graph_context_paths.len(),
                graph_context_node_count: knowledge_context_path_node_count(&graph_context_paths),
                graph_context_relationship_count: graph_context_paths.len(),
                graph_context_truncation_reasons: graph_context_search.truncation_reasons.clone(),
                fanout_reason_details: fanout_reason_details.clone(),
                candidate_count: candidates.len(),
                candidate_total_count,
                pipeline: pipeline_report,
            },
        );
        Ok(KnowledgeRetrievalOutput {
            graph_commit_epoch,
            projection_freshness,
            search,
            retrievers,
            diagnostics,
            candidates,
            evidence,
            graph_seeds,
            graph_context_paths,
            fanout_reason_codes,
            fanout_reason_details,
            fanout_reasons,
        })
    }
}

/// The graph stages cannot execute without the pipeline's query ledger.
pub(super) struct KnowledgeRetrievalPipelineContext<'a, S> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a S,
    pub(super) query_memory_budget: NonZeroUsize,
    pub(super) account: &'a hawdb_executor::QueryMemoryAccount,
    pub(super) owned: knowledge_graph_read::KnowledgeOwnedState,
}

impl<S: crate::executor::ExecutionStore> KnowledgeRetrievalPipelineContext<'_, S> {
    pub(super) fn expand_knowledge_context(
        &self,
        search_nodes: &BTreeMap<String, NodeId>,
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        metadata_filters: &BTreeMap<String, String>,
        graph_context_limit: usize,
        graph_context_max_hops: usize,
    ) -> Result<KnowledgeGraphContextSearchOutput> {
        let temporary = knowledge_graph_read::KnowledgeOwnedState::new(self.account)?;
        let mut paths = Vec::new();
        let mut fanout_reasons = Vec::new();
        let mut truncation_reasons = Vec::new();
        let mut seen_relationships = BTreeSet::new();
        let mut seen_frontier_nodes = BTreeSet::new();
        let mut reported_dense_groups = BTreeSet::new();
        let mut frontier = VecDeque::new();

        for (hit_id, seed_node) in search_nodes {
            let _lookup = self.account.reserve(hit_id.len())?;
            let key = (hit_id.clone(), seed_node.0);
            if !seen_frontier_nodes.contains(&key) {
                temporary.reserve(hit_id.len().saturating_mul(2).saturating_add(
                    128 + 11 * std::mem::size_of::<(String, u64)>()
                        + 2 * std::mem::size_of::<(String, NodeId, usize)>(),
                ))?;
                seen_frontier_nodes.insert(key);
                frontier.reserve_exact(1);
                frontier.push_back((hit_id.clone(), *seed_node, 0usize));
            }
        }
        for seed in graph_seeds {
            let seed_node = seed.node_id;
            let _lookup = self.account.reserve(seed.id.len())?;
            let key = (seed.id.clone(), seed_node.0);
            if !seen_frontier_nodes.contains(&key) {
                temporary.reserve(seed.id.len().saturating_mul(2).saturating_add(
                    128 + 11 * std::mem::size_of::<(String, u64)>()
                        + 2 * std::mem::size_of::<(String, NodeId, usize)>(),
                ))?;
                let seed_id = graph_seed_candidate_id_internal(seed);
                seen_frontier_nodes.insert(key);
                frontier.reserve_exact(1);
                frontier.push_back((seed_id, seed_node, 0usize));
            }
        }
        let input_seed_count = seen_frontier_nodes.len();

        while let Some((seed_hit_id, current_node, depth)) = frontier.pop_front() {
            if depth >= graph_context_max_hops {
                continue;
            }
            record_dense_adjacency_diagnostics(
                DenseAdjacencyDiagnosticContext {
                    catalog: self.catalog,
                    store: self.store,
                    operation: "graph_context",
                    relationship_type: None,
                },
                current_node,
                &mut reported_dense_groups,
                &mut fanout_reasons,
            );
            let control = knowledge_graph_read::visit_expansion_edges(
                self.store,
                current_node,
                self.query_memory_budget.get(),
                self.account,
                |edge| {
                    if !self.graph_expansion_node_is_authorized(edge.next_node, metadata_filters)? {
                        return Ok(hawdb_executor::store::ScanControl::Continue);
                    }
                    let _lookup = self.account.reserve(seed_hit_id.len())?;
                    let relationship_key = (seed_hit_id.clone(), edge.relationship.id.0);
                    if seen_relationships.contains(&relationship_key) {
                        return Ok(hawdb_executor::store::ScanControl::Continue);
                    }
                    temporary.reserve(
                        seed_hit_id
                            .len()
                            .saturating_add(128 + 11 * std::mem::size_of::<(String, u64)>()),
                    )?;
                    seen_relationships.insert(relationship_key);
                    if paths.len() >= graph_context_limit {
                        let detail = KnowledgeFanoutReasonDetail::graph_context_limit(
                            graph_context_limit,
                            &seed_hit_id,
                        );
                        truncation_reasons.push(detail.message.clone());
                        fanout_reasons.push(detail);
                        return Ok(hawdb_executor::store::ScanControl::Stop);
                    }
                    self.owned.reserve_vec(&mut paths)?;
                    self.owned.reserve(seed_hit_id.len())?;
                    paths.push(KnowledgeGraphContextCandidate {
                        seed_hit_id: seed_hit_id.clone(),
                        hop: depth + 1,
                        direction: edge.direction,
                        relationship_id: edge.relationship.id,
                        source_node_id: edge.relationship.source,
                        target_node_id: edge.relationship.target,
                    });
                    let _lookup = self.account.reserve(seed_hit_id.len())?;
                    let frontier_key = (seed_hit_id.clone(), edge.next_node.0);
                    if !seen_frontier_nodes.contains(&frontier_key) {
                        temporary.reserve(seed_hit_id.len().saturating_mul(2).saturating_add(
                            128 + 11 * std::mem::size_of::<(String, u64)>()
                                + 2 * std::mem::size_of::<(String, NodeId, usize)>(),
                        ))?;
                        seen_frontier_nodes.insert(frontier_key);
                        frontier.reserve_exact(1);
                        frontier.push_back((seed_hit_id.clone(), edge.next_node, depth + 1));
                    }
                    Ok(hawdb_executor::store::ScanControl::Continue)
                },
            )?;
            if control == hawdb_executor::store::ScanControl::Stop {
                let expanded_relationship_count = paths.len();
                return Ok(KnowledgeGraphContextSearchOutput {
                    paths,
                    input_seed_count,
                    expanded_relationship_count,
                    fanout_reason_details: fanout_reasons,
                    truncation_reasons,
                });
            }
        }

        let expanded_relationship_count = paths.len();
        Ok(KnowledgeGraphContextSearchOutput {
            paths,
            input_seed_count,
            expanded_relationship_count,
            fanout_reason_details: fanout_reasons,
            truncation_reasons,
        })
    }

    pub(super) fn graph_expansion_node_is_authorized(
        &self,
        node_id: NodeId,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<bool> {
        let scope_filters = knowledge_graph_expansion_scope_filters(metadata_filters);
        if scope_filters.is_empty() {
            return Ok(true);
        }
        let Some(node) = knowledge_graph_read::node(self.store, node_id, None, self.account)?
        else {
            return Ok(false);
        };
        knowledge_graph_filters::PreparedKnowledgeFilter::new(&scope_filters, self.account)?
            .matches(
                self.catalog,
                self.store,
                &node.node,
                self.account,
                self.query_memory_budget.get(),
            )
    }

    pub(super) fn seed_node_for_hit(
        &self,
        kind: Option<&str>,
        external_id: Option<&str>,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<Option<knowledge_graph_read::KnowledgeReadNode>> {
        let Some(external_id) = external_id else {
            return Ok(None);
        };
        let Some(label) = kind.and_then(search_kind_to_label) else {
            return Ok(None);
        };
        let Some(node) = knowledge_graph_read::seed_by_external_id(
            self.catalog,
            self.store,
            label,
            external_id,
            self.account,
        )?
        else {
            return Ok(None);
        };
        if !knowledge_graph_filters::PreparedKnowledgeFilter::new(metadata_filters, self.account)?
            .matches(
            self.catalog,
            self.store,
            &node.node,
            self.account,
            self.query_memory_budget.get(),
        )? {
            return Ok(None);
        }
        Ok(Some(node))
    }

    pub(super) fn canonical_search_nodes(
        &self,
        search: &SearchResultSet,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, NodeId>> {
        let scope_filters = knowledge_graph_expansion_scope_filters(metadata_filters);
        let mut nodes = BTreeMap::new();
        for hit in &search.hits {
            if let Some(node) = self.seed_node_for_hit(
                hit.kind.as_deref(),
                hit.external_id.as_deref(),
                &scope_filters,
            )? {
                self.owned.reserve(
                    hit.id
                        .len()
                        .saturating_add(128 + 11 * std::mem::size_of::<(String, NodeId)>()),
                )?;
                nodes.insert(hit.id.clone(), node.node.id);
            }
        }
        Ok(nodes)
    }

    pub(super) fn knowledge_entity_from_node(&self, node: &NodeRecord) -> Result<KnowledgeEntity> {
        let labels = node
            .labels
            .iter()
            .filter_map(|id| self.catalog.label_name(*id))
            .fold(0usize, |bytes, label| {
                bytes.saturating_add(label.len() + std::mem::size_of::<String>())
            });
        self.owned.reserve(
            hawdb_core::ids::node_allocation_bytes(node)
                .saturating_mul(2)
                .saturating_add(labels)
                .saturating_add(128),
        )?;
        Ok(knowledge_entity_from_node(self.catalog, node))
    }

    pub(super) fn knowledge_evidence_for_search(
        &self,
        search: &SearchResultSet,
        canonical_search_nodes: &BTreeMap<String, NodeId>,
        graph_context_paths: &[KnowledgeGraphContextCandidate],
    ) -> Vec<KnowledgeEvidence> {
        let mut evidence = Vec::with_capacity(search.hits.len());
        for hit in &search.hits {
            let canonical_node_id = canonical_search_nodes.get(&hit.id).map(|node_id| node_id.0);
            let graph_context_path_count = graph_context_paths
                .iter()
                .filter(|path| path.seed_hit_id == hit.id)
                .count();
            evidence.push(KnowledgeEvidence {
                hit_id: hit.id.clone(),
                kind: hit.kind.clone(),
                external_id: hit.external_id.clone(),
                source_id: hit.source_id.clone(),
                canonical_node_id,
                graph_context_path_count,
                matched_terms: hit.matched_terms.clone(),
                matched_spans: hit.matched_spans.clone(),
                score: hit.score,
                rrf_score: hit.rrf_score,
                vector_rrf_score: hit.vector_rrf_score,
                text_rrf_score: hit.text_rrf_score,
                vector_score: hit.vector_score,
                text_score: hit.text_score,
                vector_rank: hit.vector_rank,
                text_rank: hit.text_rank,
            });
        }
        evidence
    }

    /// Score-breakdown inputs for one rerank candidate. Canonical node records
    /// are only loaded when the policy asks for property features.
    pub(super) fn knowledge_candidate_score_breakdown_for(
        &self,
        scoring: &KnowledgeCandidateScoringPolicy,
        needs_properties: bool,
        search_score: Option<f64>,
        graph_seed_score: Option<f64>,
        node_id: Option<NodeId>,
        reference_time_millis: u64,
    ) -> Result<KnowledgeCandidateScoreBreakdown> {
        let node = if needs_properties {
            node_id
                .map(|node_id| knowledge_graph_read::node(self.store, node_id, None, self.account))
                .transpose()?
                .flatten()
        } else {
            None
        };
        let source = KnowledgeCandidateFeatureSource {
            search_score,
            graph_seed_score,
            // Every candidate is a retrieval seed or a graph seed, so its
            // distance from a seed is zero; expanded nodes only appear as
            // graph context today.
            hop_distance: Some(0),
            node: node.as_ref().map(|node| &node.node),
        };
        Ok(knowledge_candidate_score_breakdown(
            &source,
            scoring,
            reference_time_millis,
        ))
    }

    pub(super) fn knowledge_candidates(
        &self,
        search: &SearchResultSet,
        evidence: &[KnowledgeEvidence],
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        graph_context_paths: &[KnowledgeGraphContextCandidate],
        scoring: &KnowledgeCandidateScoringPolicy,
        reference_time_millis: u64,
    ) -> Result<Vec<KnowledgeCandidate>> {
        let needs_properties = matches!(
            scoring,
            KnowledgeCandidateScoringPolicy::Spec(spec)
                if spec.needs_canonical_node_properties()
        );
        let mut candidates = Vec::with_capacity(search.hits.len());
        for (index, (hit, evidence)) in search.hits.iter().zip(evidence.iter()).enumerate() {
            let score_breakdown = self.knowledge_candidate_score_breakdown_for(
                scoring,
                needs_properties,
                Some(hit.score),
                None,
                evidence.canonical_node_id.map(NodeId),
                reference_time_millis,
            )?;
            candidates.push(KnowledgeCandidate {
                id: hit.id.clone(),
                canonical_node_id: evidence.canonical_node_id,
                source: KnowledgeCandidateSource::SearchHit,
                source_rank: index + 1,
                merged_sources: vec![KnowledgeCandidateSource::SearchHit],
                score: score_breakdown.combined_score,
                score_breakdown,
                entity: None,
                evidence: Some(evidence.clone()),
                matched_properties: Vec::new(),
                graph_context_path_count: evidence.graph_context_path_count,
            });
        }

        for (index, seed) in graph_seeds.iter().enumerate() {
            let seed_candidate_id = graph_seed_candidate_id_internal(seed);
            let seed_graph_context_path_count = graph_context_paths
                .iter()
                .filter(|path| path.seed_hit_id == seed_candidate_id)
                .count();
            if let Some(candidate) = candidates
                .iter_mut()
                .find(|candidate| candidate.canonical_node_id == Some(seed.node_id.0))
            {
                candidate.score_breakdown = self.knowledge_candidate_score_breakdown_for(
                    scoring,
                    needs_properties,
                    candidate.score_breakdown.search_score,
                    Some(seed.score),
                    Some(seed.node_id),
                    reference_time_millis,
                )?;
                candidate.score = candidate.score_breakdown.combined_score;
                if !candidate
                    .merged_sources
                    .contains(&KnowledgeCandidateSource::GraphSeed)
                {
                    candidate
                        .merged_sources
                        .push(KnowledgeCandidateSource::GraphSeed);
                }
                for property in &seed.matched_properties {
                    if !candidate.matched_properties.contains(property) {
                        candidate.matched_properties.push(property.clone());
                    }
                }
                candidate.graph_context_path_count += seed_graph_context_path_count;
                continue;
            }
            let score_breakdown = self.knowledge_candidate_score_breakdown_for(
                scoring,
                needs_properties,
                None,
                Some(seed.score),
                Some(seed.node_id),
                reference_time_millis,
            )?;
            candidates.push(KnowledgeCandidate {
                id: seed_candidate_id,
                canonical_node_id: Some(seed.node_id.0),
                source: KnowledgeCandidateSource::GraphSeed,
                source_rank: index + 1,
                merged_sources: vec![KnowledgeCandidateSource::GraphSeed],
                score: score_breakdown.combined_score,
                score_breakdown,
                entity: None,
                evidence: None,
                matched_properties: seed.matched_properties.clone(),
                graph_context_path_count: seed_graph_context_path_count,
            });
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_rank.cmp(&right.source_rank))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(candidates)
    }

    pub(super) fn hydrate_knowledge_output(
        &self,
        candidates: &mut [KnowledgeCandidate],
        graph_seeds: &[KnowledgeGraphSeedCandidate],
        graph_context_paths: &[KnowledgeGraphContextCandidate],
    ) -> Result<(
        Vec<KnowledgeGraphSeed>,
        Vec<KnowledgeGraphContextPath>,
        usize,
    )> {
        let temporary = knowledge_graph_read::KnowledgeOwnedState::new(self.account)?;
        let mut node_ids = BTreeSet::new();
        for id in candidates
            .iter()
            .filter_map(|candidate| candidate.canonical_node_id.map(NodeId))
            .chain(graph_seeds.iter().map(|seed| seed.node_id))
            .chain(
                graph_context_paths
                    .iter()
                    .flat_map(|path| [path.source_node_id, path.target_node_id]),
            )
        {
            if !node_ids.contains(&id) {
                temporary.reserve(128 + 11 * std::mem::size_of::<NodeId>())?;
                node_ids.insert(id);
            }
        }
        let entities = self.hydrate_knowledge_entities(&node_ids)?;

        for candidate in candidates {
            if let Some(entity) = candidate
                .canonical_node_id
                .and_then(|id| entities.get(&NodeId(id)))
            {
                self.owned
                    .reserve(knowledge_entity_allocation_bytes(entity))?;
                candidate.entity = Some(entity.clone());
            }
        }
        let mut hydrated_seeds = Vec::new();
        for seed in graph_seeds {
            let entity = entities.get(&seed.node_id).ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "knowledge retrieval graph seed references missing canonical node {}",
                    seed.node_id.0,
                ))
            })?;
            self.owned.reserve_vec(&mut hydrated_seeds)?;
            self.owned.reserve(
                knowledge_entity_allocation_bytes(entity)
                    .saturating_add(string_slice_bytes(&seed.matched_properties))
                    .saturating_add(
                        seed.matched_properties
                            .len()
                            .saturating_mul(std::mem::size_of::<String>()),
                    ),
            )?;
            hydrated_seeds.push(KnowledgeGraphSeed {
                entity: entity.clone(),
                score: seed.score,
                matched_properties: seed.matched_properties.clone(),
            });
        }
        let mut hydrated_paths = Vec::new();
        for path in graph_context_paths {
            self.owned.reserve_vec(&mut hydrated_paths)?;
            hydrated_paths.push(self.hydrate_graph_context_path(path, &entities)?);
        }
        Ok((hydrated_seeds, hydrated_paths, entities.len()))
    }

    pub(super) fn hydrate_knowledge_entities(
        &self,
        node_ids: &BTreeSet<NodeId>,
    ) -> Result<BTreeMap<NodeId, KnowledgeEntity>> {
        let mut entities = BTreeMap::new();
        for node_id in node_ids {
            let Some(node) = knowledge_graph_read::node(self.store, *node_id, None, self.account)?
            else {
                return Err(HawDBError::StorageIntegrity(format!(
                    "knowledge retrieval canonical hydration references missing node {}",
                    node_id.0
                )));
            };
            self.owned
                .reserve(128 + 11 * std::mem::size_of::<(NodeId, KnowledgeEntity)>())?;
            entities.insert(*node_id, self.knowledge_entity_from_node(&node.node)?);
        }
        Ok(entities)
    }

    pub(super) fn hydrate_graph_context_path(
        &self,
        path: &KnowledgeGraphContextCandidate,
        entities: &BTreeMap<NodeId, KnowledgeEntity>,
    ) -> Result<KnowledgeGraphContextPath> {
        let relationship =
            knowledge_graph_read::relationship(self.store, path.relationship_id, self.account)?
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(format!(
                        "knowledge retrieval graph context references missing relationship {}",
                        path.relationship_id.0
                    ))
                })?;
        let source = entities.get(&path.source_node_id).ok_or_else(|| {
            HawDBError::StorageIntegrity(format!(
                "knowledge retrieval graph context references missing source node {}",
                path.source_node_id.0
            ))
        })?;
        let target = entities.get(&path.target_node_id).ok_or_else(|| {
            HawDBError::StorageIntegrity(format!(
                "knowledge retrieval graph context references missing target node {}",
                path.target_node_id.0
            ))
        })?;
        let labels =
            string_slice_bytes(&source.labels).saturating_add(string_slice_bytes(&target.labels));
        let label_slots = source
            .labels
            .len()
            .saturating_add(target.labels.len())
            .saturating_mul(std::mem::size_of::<String>());
        self.owned.reserve(
            path.seed_hit_id
                .len()
                .saturating_add(labels)
                .saturating_add(label_slots)
                .saturating_add(option_string_bytes(&source.external_id))
                .saturating_add(option_string_bytes(&target.external_id))
                .saturating_add(
                    self.catalog
                        .rel_type_name(relationship.relationship.rel_type)
                        .unwrap_or("<unknown>")
                        .len(),
                ),
        )?;
        self.owned.retain_source(relationship._allocation)?;
        let relationship = relationship.relationship;
        Ok(KnowledgeGraphContextPath {
            seed_hit_id: path.seed_hit_id.clone(),
            hop: path.hop,
            direction: path.direction,
            relationship_id: path.relationship_id.0,
            relationship_type: self
                .catalog
                .rel_type_name(relationship.rel_type)
                .unwrap_or("<unknown>")
                .to_string(),
            relationship_properties: relationship.properties,
            source_node_id: path.source_node_id.0,
            source_labels: source.labels.clone(),
            source_external_id: source.external_id.clone(),
            target_node_id: path.target_node_id.0,
            target_labels: target.labels.clone(),
            target_external_id: target.external_id.clone(),
        })
    }

    pub(super) fn search_knowledge_graph_seeds(
        &self,
        query_text: &str,
        limit: usize,
        metadata_filters: &BTreeMap<String, String>,
    ) -> Result<KnowledgeGraphSeedSearchOutput> {
        use hawdb_executor::graph_seed::GraphSeedScorer;
        use hawdb_executor::store::{admit_graph_read, ScanControl};
        use hawdb_storage::read_view::{AdmittedVec, GraphReadAdmission};

        let account = self.account;
        if limit == 0 {
            return Ok(KnowledgeGraphSeedSearchOutput::default());
        }
        let scorer = GraphSeedScorer::new(query_text, account, None)?;
        if scorer.is_empty() {
            return Ok(KnowledgeGraphSeedSearchOutput::default());
        }
        let filter =
            knowledge_graph_filters::PreparedKnowledgeFilter::new(metadata_filters, account)?;
        let mut allocate_slots = |bytes| admit_graph_read(account, None, bytes);
        let mut scored = AdmittedVec::<KnowledgeGraphSeedCandidate>::new(
            &GraphReadAdmission::new(&mut allocate_slots),
        )?;
        let mut input_candidate_count = 0usize;
        let mut input_filtered_out_count = 0usize;
        let mut candidate_count = 0usize;
        self.store.visit_nodes_with_allocation(
            None,
            &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                &mut |bytes| admit_graph_read(account, None, bytes).map(Some),
                None,
            ),
            &mut |input| {
                let (node, _input_allocation) = input.into_parts();
                if !filter.matches(
                    self.catalog,
                    self.store,
                    &node,
                    account,
                    self.query_memory_budget.get(),
                )? {
                    input_filtered_out_count = input_filtered_out_count.saturating_add(1);
                    return Ok(ScanControl::Continue);
                }
                input_candidate_count = input_candidate_count.saturating_add(1);
                let relevance = scorer.score(&node)?;
                if relevance.score <= 0.0 {
                    return Ok(ScanControl::Continue);
                }
                candidate_count = candidate_count.saturating_add(1);
                // Select before allocating owned candidate ids and property names.
                let replace = if scored.as_slice().len() < limit {
                    None
                } else {
                    let (index, worst) = scored
                        .as_slice()
                        .iter()
                        .enumerate()
                        .max_by(|(_, left), (_, right)| knowledge_graph_seed_order(left, right))
                        .expect("nonzero seed window is full");
                    if relevance.score < worst.score
                        || (relevance.score == worst.score && node.id >= worst.node_id)
                    {
                        return Ok(ScanControl::Continue);
                    }
                    Some(index)
                };
                let seed = graph_seed_candidate_from_node(self.catalog, &node, relevance, account)?;
                if let Some(index) = replace {
                    scored.as_mut_slice()[index] = seed;
                } else {
                    scored.try_push(seed)?;
                }
                Ok(ScanControl::Continue)
            },
        )?;
        let (mut seeds, slots) = scored.into_parts();
        seeds.sort_unstable_by(knowledge_graph_seed_order);
        // The one bounded diagnostic contains at most two usize renderings.
        let report_allocation = account.reserve(if candidate_count > limit {
            std::mem::size_of::<KnowledgeFanoutReasonDetail>() + 256
        } else {
            0
        })?;
        let fanout_reason_details = if candidate_count > limit {
            vec![KnowledgeFanoutReasonDetail::graph_seed_limit(
                limit,
                candidate_count,
            )]
        } else {
            Vec::new()
        };
        Ok(KnowledgeGraphSeedSearchOutput {
            seeds,
            input_candidate_count,
            input_filtered_out_count,
            candidate_count,
            fanout_reason_details,
            _slots: Some(slots),
            _report_allocation: Some(report_allocation),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct KnowledgeGraphContextSearchOutput {
    pub(super) paths: Vec<KnowledgeGraphContextCandidate>,
    pub(super) input_seed_count: usize,
    pub(super) expanded_relationship_count: usize,
    pub(super) fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub(super) truncation_reasons: Vec<String>,
}

#[derive(Default)]
pub(super) struct KnowledgeGraphSeedSearchOutput {
    pub(super) seeds: Vec<KnowledgeGraphSeedCandidate>,
    pub(super) _slots: Option<Box<dyn hawdb_storage::read_view::GraphReadAllocation>>,
    pub(super) _report_allocation: Option<hawdb_executor::QueryMemoryLease>,
    pub(super) input_candidate_count: usize,
    pub(super) input_filtered_out_count: usize,
    pub(super) candidate_count: usize,
    pub(super) fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
}

#[derive(Debug)]
pub(super) struct KnowledgeGraphSeedCandidate {
    pub(super) _allocation: hawdb_executor::QueryMemoryLease,
    pub(super) id: String,
    pub(super) node_id: NodeId,
    pub(super) score: f64,
    pub(super) matched_properties: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct KnowledgeGraphContextCandidate {
    pub(super) seed_hit_id: String,
    pub(super) hop: usize,
    pub(super) direction: KnowledgeGraphPathDirection,
    pub(super) relationship_id: RelId,
    pub(super) source_node_id: NodeId,
    pub(super) target_node_id: NodeId,
}

pub(super) fn knowledge_graph_seed_order(
    left: &KnowledgeGraphSeedCandidate,
    right: &KnowledgeGraphSeedCandidate,
) -> std::cmp::Ordering {
    right
        .score
        .total_cmp(&left.score)
        .then(left.node_id.cmp(&right.node_id))
}

pub(super) fn knowledge_candidates_memory_bytes(candidates: &[KnowledgeCandidate]) -> usize {
    std::mem::size_of_val(candidates).saturating_add(
        candidates
            .iter()
            .map(knowledge_candidate_resource_bytes)
            .fold(0usize, usize::saturating_add),
    )
}

pub(super) struct KnowledgeRetrievalResultResources<'a> {
    pub(super) search: &'a SearchResultSet,
    pub(super) retrievers: &'a [KnowledgeRetrieverReport],
    pub(super) candidates: &'a [KnowledgeCandidate],
    pub(super) evidence: &'a [KnowledgeEvidence],
    pub(super) graph_seeds: &'a [KnowledgeGraphSeed],
    pub(super) graph_context_paths: &'a [KnowledgeGraphContextPath],
    pub(super) fanout_reason_details: &'a [KnowledgeFanoutReasonDetail],
    pub(super) fanout_reasons: &'a [String],
}

pub(super) fn knowledge_retrieval_result_resource_bytes(
    resources: KnowledgeRetrievalResultResources<'_>,
) -> (usize, usize) {
    let payload_bytes = resources
        .search
        .hits
        .iter()
        .map(search_hit_payload_bytes)
        .chain(
            resources
                .retrievers
                .iter()
                .map(knowledge_retriever_payload_bytes),
        )
        .chain(
            resources
                .candidates
                .iter()
                .map(knowledge_candidate_payload_bytes),
        )
        .chain(
            resources
                .evidence
                .iter()
                .map(knowledge_evidence_payload_bytes),
        )
        .chain(
            resources
                .graph_seeds
                .iter()
                .map(knowledge_graph_seed_payload_bytes),
        )
        .chain(
            resources
                .graph_context_paths
                .iter()
                .map(knowledge_graph_context_path_payload_bytes),
        )
        .chain(
            resources
                .fanout_reason_details
                .iter()
                .map(knowledge_fanout_detail_payload_bytes),
        )
        .fold(
            string_slice_bytes(resources.fanout_reasons),
            usize::saturating_add,
        );
    let item_count = resources
        .search
        .hits
        .len()
        .saturating_add(resources.retrievers.len())
        .saturating_add(resources.candidates.len())
        .saturating_add(resources.evidence.len())
        .saturating_add(resources.graph_seeds.len())
        .saturating_add(resources.graph_context_paths.len())
        .saturating_add(resources.fanout_reason_details.len())
        .saturating_add(resources.fanout_reasons.len());
    let memory_bytes = payload_bytes
        .saturating_add(std::mem::size_of::<KnowledgeRetrievalOutput>())
        .saturating_add(item_count.saturating_mul(std::mem::size_of::<usize>() * 4));
    (memory_bytes, payload_bytes)
}

pub(super) fn string_slice_bytes(values: &[String]) -> usize {
    values.iter().fold(0usize, |bytes, value| {
        bytes
            .saturating_add(std::mem::size_of::<String>())
            .saturating_add(value.len())
    })
}

pub(super) fn option_string_bytes(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, |value| value.len())
}

pub(super) fn matched_span_payload_bytes(span: &SearchMatchedSpan) -> usize {
    span.field
        .len()
        .saturating_add(span.text.len())
        .saturating_add(span.term.len())
}

pub(super) fn search_hit_payload_bytes(hit: &crate::search::SearchHit) -> usize {
    hit.id
        .len()
        .saturating_add(option_string_bytes(&hit.kind))
        .saturating_add(option_string_bytes(&hit.external_id))
        .saturating_add(option_string_bytes(&hit.source_id))
        .saturating_add(string_slice_bytes(&hit.matched_terms))
        .saturating_add(
            hit.matched_spans
                .iter()
                .map(matched_span_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(string_slice_bytes(&hit.fallback_reasons))
}

pub(super) fn knowledge_entity_allocation_bytes(entity: &KnowledgeEntity) -> usize {
    std::mem::size_of::<KnowledgeEntity>()
        .saturating_add(string_slice_bytes(&entity.labels))
        .saturating_add(
            entity
                .labels
                .len()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(option_string_bytes(&entity.external_id))
        .saturating_add(hawdb_executor::binding::map_memory_bytes(
            &entity.properties,
        ))
}

pub(super) fn knowledge_entity_payload_bytes(entity: &KnowledgeEntity) -> usize {
    string_slice_bytes(&entity.labels)
        .saturating_add(option_string_bytes(&entity.external_id))
        .saturating_add(hawdb_executor::binding::map_payload_bytes(
            &entity.properties,
        ))
}

pub(super) fn knowledge_evidence_payload_bytes(evidence: &KnowledgeEvidence) -> usize {
    evidence
        .hit_id
        .len()
        .saturating_add(option_string_bytes(&evidence.kind))
        .saturating_add(option_string_bytes(&evidence.external_id))
        .saturating_add(option_string_bytes(&evidence.source_id))
        .saturating_add(string_slice_bytes(&evidence.matched_terms))
        .saturating_add(
            evidence
                .matched_spans
                .iter()
                .map(matched_span_payload_bytes)
                .fold(0usize, usize::saturating_add),
        )
}

pub(super) fn knowledge_candidate_payload_bytes(candidate: &KnowledgeCandidate) -> usize {
    candidate
        .id
        .len()
        .saturating_add(string_slice_bytes(&candidate.matched_properties))
        .saturating_add(
            candidate
                .entity
                .as_ref()
                .map_or(0, knowledge_entity_payload_bytes),
        )
        .saturating_add(
            candidate
                .evidence
                .as_ref()
                .map_or(0, knowledge_evidence_payload_bytes),
        )
}

pub(super) fn knowledge_candidate_resource_bytes(candidate: &KnowledgeCandidate) -> usize {
    std::mem::size_of::<KnowledgeCandidate>()
        .saturating_add(knowledge_candidate_payload_bytes(candidate))
        .saturating_add(
            candidate
                .merged_sources
                .len()
                .saturating_mul(std::mem::size_of::<KnowledgeCandidateSource>()),
        )
}

pub(super) fn knowledge_graph_seed_payload_bytes(seed: &KnowledgeGraphSeed) -> usize {
    knowledge_entity_payload_bytes(&seed.entity)
        .saturating_add(string_slice_bytes(&seed.matched_properties))
}

pub(super) fn knowledge_graph_context_path_payload_bytes(
    path: &KnowledgeGraphContextPath,
) -> usize {
    path.seed_hit_id
        .len()
        .saturating_add(path.relationship_type.len())
        .saturating_add(hawdb_executor::binding::map_payload_bytes(
            &path.relationship_properties,
        ))
        .saturating_add(string_slice_bytes(&path.source_labels))
        .saturating_add(option_string_bytes(&path.source_external_id))
        .saturating_add(string_slice_bytes(&path.target_labels))
        .saturating_add(option_string_bytes(&path.target_external_id))
}

pub(super) fn knowledge_retriever_payload_bytes(report: &KnowledgeRetrieverReport) -> usize {
    report
        .name
        .len()
        .saturating_add(report.backend.len())
        .saturating_add(string_slice_bytes(&report.fallback_reasons))
        .saturating_add(string_slice_bytes(&report.truncation_reasons))
        .saturating_add(
            report
                .top_candidates
                .iter()
                .fold(0usize, |bytes, candidate| {
                    bytes
                        .saturating_add(candidate.id.len())
                        .saturating_add(option_string_bytes(&candidate.kind))
                        .saturating_add(option_string_bytes(&candidate.external_id))
                        .saturating_add(option_string_bytes(&candidate.source_id))
                        .saturating_add(
                            candidate
                                .matched_spans
                                .iter()
                                .map(matched_span_payload_bytes)
                                .fold(0usize, usize::saturating_add),
                        )
                }),
        )
}

pub(super) fn knowledge_fanout_detail_payload_bytes(detail: &KnowledgeFanoutReasonDetail) -> usize {
    detail
        .message
        .len()
        .saturating_add(option_string_bytes(&detail.operation))
        .saturating_add(option_string_bytes(&detail.seed_hit_id))
        .saturating_add(option_string_bytes(&detail.relationship_type))
        .saturating_add(option_string_bytes(&detail.direction))
}

pub(super) fn knowledge_retriever_reports(
    search: &SearchResultSet,
    evidence: &[KnowledgeEvidence],
    graph_seeds: &[KnowledgeGraphSeed],
    graph_context_paths: &[KnowledgeGraphContextPath],
    projection_freshness: &SearchProjectionFreshness,
    graph_seed_input: KnowledgeGraphSeedRetrieverInput,
) -> Vec<KnowledgeRetrieverReport> {
    let evidence_by_hit = evidence
        .iter()
        .map(|evidence| (evidence.hit_id.as_str(), evidence))
        .collect::<BTreeMap<_, _>>();
    let mut reports = search
        .retrievers
        .iter()
        .map(|report| KnowledgeRetrieverReport {
            name: report.name.clone(),
            backend: report.backend.clone(),
            available: report.available,
            input_candidate_set: report.input_candidate_set.clone(),
            candidate_count: report.candidate_count,
            candidate_set: report.candidate_set.clone(),
            limit: Some(search.limit),
            rank_window: search.rank_window,
            fusion_weight: knowledge_search_retriever_fusion_weight(
                report.name.as_str(),
                search.fusion_weights,
            ),
            fallback_reason_codes: report.fallback_reason_codes.clone(),
            knowledge_fallback_reason_codes: Vec::new(),
            fallback_reasons: report.fallback_reasons.clone(),
            truncated: report.candidate_count > report.top_candidates.len(),
            truncation_reason_codes: knowledge_search_retriever_truncation_reason_codes(
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            truncation_reasons: knowledge_search_retriever_truncation_reasons(
                report.name.as_str(),
                report.candidate_count,
                report.top_candidates.len(),
                search.limit,
                search.rank_window,
            ),
            top_candidates: report
                .top_candidates
                .iter()
                .map(|candidate| {
                    let evidence = evidence_by_hit.get(candidate.id.as_str()).copied();
                    KnowledgeRetrieverCandidate {
                        id: candidate.id.clone(),
                        kind: evidence.and_then(|evidence| evidence.kind.clone()),
                        external_id: evidence.and_then(|evidence| evidence.external_id.clone()),
                        source_id: evidence.and_then(|evidence| evidence.source_id.clone()),
                        canonical_node_id: evidence.and_then(|evidence| evidence.canonical_node_id),
                        rank: candidate.rank,
                        score: candidate.score,
                        matched_spans: evidence
                            .map(|evidence| evidence.matched_spans.clone())
                            .unwrap_or_default(),
                        graph_context_path_count: evidence
                            .map(|evidence| evidence.graph_context_path_count)
                            .unwrap_or_default(),
                        projection_freshness: Some(projection_freshness.clone()),
                    }
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    reports.push(KnowledgeRetrieverReport {
        name: "graph_seed".to_string(),
        backend: "graph_seed_expand".to_string(),
        available: graph_seed_input.limit > 0,
        input_candidate_set: knowledge_graph_seed_input_candidate_set_report(
            graph_seed_input.input_candidate_count,
            graph_seed_input.graph_commit_epoch,
            graph_seed_input.input_filtered_out_count,
            graph_seed_input.metadata_filters,
        ),
        candidate_count: graph_seed_input.candidate_count,
        candidate_set: knowledge_graph_seed_candidate_set_report(
            graph_seeds.len(),
            graph_seed_input.graph_commit_epoch,
        ),
        limit: Some(graph_seed_input.limit),
        rank_window: None,
        fusion_weight: None,
        fallback_reason_codes: Vec::new(),
        knowledge_fallback_reason_codes: knowledge_graph_seed_fallback_reason_codes(
            graph_seed_input.limit,
        ),
        fallback_reasons: knowledge_graph_seed_fallback_reasons(graph_seed_input.limit),
        truncated: graph_seed_input.candidate_count > graph_seeds.len(),
        truncation_reason_codes: knowledge_graph_seed_truncation_reason_codes(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
        ),
        truncation_reasons: knowledge_graph_seed_truncation_reasons(
            graph_seed_input.candidate_count,
            graph_seeds.len(),
            graph_seed_input.limit,
        ),
        top_candidates: graph_seeds
            .iter()
            .enumerate()
            .map(|(index, seed)| {
                let id = graph_seed_candidate_id(seed);
                let graph_context_path_count = graph_context_paths
                    .iter()
                    .filter(|path| path.seed_hit_id == id)
                    .count();
                KnowledgeRetrieverCandidate {
                    id,
                    kind: seed.entity.labels.first().cloned(),
                    external_id: seed.entity.external_id.clone(),
                    source_id: None,
                    canonical_node_id: Some(seed.entity.node_id),
                    rank: index + 1,
                    score: seed.score,
                    matched_spans: Vec::new(),
                    graph_context_path_count,
                    projection_freshness: None,
                }
            })
            .collect(),
    });
    reports
}

#[derive(Debug, Clone)]
pub(super) struct KnowledgeGraphSeedRetrieverInput {
    pub(super) limit: usize,
    pub(super) input_candidate_count: usize,
    pub(super) input_filtered_out_count: usize,
    pub(super) metadata_filters: BTreeMap<String, String>,
    pub(super) candidate_count: usize,
    pub(super) graph_commit_epoch: u64,
}

pub(super) fn knowledge_search_retriever_truncation_reasons(
    name: &str,
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<String> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut reasons = Vec::new();
    if let Some(rank_window) = rank_window
        && candidate_count > rank_window
        && returned_count <= rank_window
    {
        reasons.push(format!(
            "{name} rank_window {rank_window} returned from {candidate_count} candidates"
        ));
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        reasons.push(format!(
            "{name} search_limit {search_limit} returned from {candidate_count} candidates"
        ));
    }
    if reasons.is_empty() {
        reasons.push(format!(
            "{name} returned {returned_count} of {candidate_count} candidates"
        ));
    }
    reasons
}

pub(super) fn knowledge_search_retriever_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
    search_limit: usize,
    rank_window: Option<usize>,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count <= returned_count {
        return Vec::new();
    }
    let mut codes = Vec::new();
    if let Some(rank_window) = rank_window
        && candidate_count > rank_window
        && returned_count <= rank_window
    {
        codes.push(KnowledgeTruncationReasonCode::RankWindowExceeded);
    }
    if returned_count >= search_limit && candidate_count > search_limit {
        codes.push(KnowledgeTruncationReasonCode::SearchLimitExceeded);
    }
    if codes.is_empty() {
        codes.push(KnowledgeTruncationReasonCode::PartialCandidateReturn);
    }
    codes
}

pub(super) fn knowledge_search_retriever_fusion_weight(
    name: &str,
    weights: SearchFusionWeights,
) -> Option<f64> {
    match name {
        "vector" => Some(weights.vector_weight),
        "text" => Some(weights.text_weight),
        _ => None,
    }
}

pub(super) fn knowledge_graph_seed_truncation_reasons(
    candidate_count: usize,
    returned_count: usize,
    graph_seed_limit: usize,
) -> Vec<String> {
    if candidate_count > returned_count {
        vec![format!(
            "graph_seed limit {graph_seed_limit} returned from {candidate_count} candidates"
        )]
    } else {
        Vec::new()
    }
}

pub(super) fn knowledge_graph_seed_truncation_reason_codes(
    candidate_count: usize,
    returned_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_count > returned_count {
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    } else {
        Vec::new()
    }
}

pub(super) fn knowledge_graph_seed_fallback_reasons(graph_seed_limit: usize) -> Vec<String> {
    if graph_seed_limit == 0 {
        vec!["graph seed retriever disabled by limit 0".to_string()]
    } else {
        Vec::new()
    }
}

pub(super) fn knowledge_graph_seed_fallback_reason_codes(
    graph_seed_limit: usize,
) -> Vec<KnowledgeFallbackReasonCode> {
    if graph_seed_limit == 0 {
        vec![KnowledgeFallbackReasonCode::GraphSeedLimitZero]
    } else {
        Vec::new()
    }
}

pub(super) fn knowledge_graph_seed_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "ranked_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

pub(super) fn knowledge_graph_seed_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
    filtered_out_count: usize,
    metadata_filters: BTreeMap<String, String>,
) -> SearchCandidateSetReport {
    let metadata_predicate_pushdown = search_metadata_predicate_pushdown(&metadata_filters).report;
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "filtered_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count,
        metadata_filters,
        metadata_predicate_pushdown,
    }
}

pub(super) fn knowledge_graph_context_input_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchCandidateSetReport {
    SearchCandidateSetReport {
        id_space: "canonical_graph_node_id".to_string(),
        representation: "context_seed_node_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
        filtered_out_count: 0,
        metadata_filters: BTreeMap::new(),
        metadata_predicate_pushdown: SearchPredicatePushdownReport::default(),
    }
}

pub(super) fn knowledge_graph_context_candidate_set_report(
    cardinality: usize,
    graph_commit_epoch: u64,
) -> SearchRetrieverCandidateSetReport {
    SearchRetrieverCandidateSetReport {
        id_space: "canonical_graph_relationship_id".to_string(),
        representation: "expanded_relationship_ids".to_string(),
        cardinality,
        exact: true,
        snapshot_source_graph_commit_epoch: Some(graph_commit_epoch),
        policy_epoch: None,
    }
}

pub(super) fn knowledge_graph_context_fallback_reasons(
    request: &KnowledgeRetrievalRequest,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if request.graph_context_limit == 0 {
        reasons.push("graph context expansion disabled by limit 0".to_string());
    }
    if request.graph_context_max_hops == 0 {
        reasons.push("graph context expansion disabled by max_hops 0".to_string());
    }
    reasons
}

pub(super) fn knowledge_graph_context_fallback_reason_codes(
    request: &KnowledgeRetrievalRequest,
) -> Vec<KnowledgeFallbackReasonCode> {
    let mut codes = Vec::new();
    if request.graph_context_limit == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextLimitZero);
    }
    if request.graph_context_max_hops == 0 {
        codes.push(KnowledgeFallbackReasonCode::GraphContextMaxHopsZero);
    }
    codes
}

#[derive(Debug, Clone)]
pub(super) struct KnowledgeRetrievalDiagnosticsInput {
    pub(super) graph_seed_input_candidate_set: SearchCandidateSetReport,
    pub(super) graph_seed_candidate_set: SearchRetrieverCandidateSetReport,
    pub(super) graph_seed_candidate_count: usize,
    pub(super) graph_seed_returned_count: usize,
    pub(super) graph_context_input_candidate_set: SearchCandidateSetReport,
    pub(super) graph_context_candidate_set: SearchRetrieverCandidateSetReport,
    pub(super) graph_context_path_count: usize,
    pub(super) graph_context_node_count: usize,
    pub(super) graph_context_relationship_count: usize,
    pub(super) graph_context_truncation_reasons: Vec<String>,
    pub(super) fanout_reason_details: Vec<KnowledgeFanoutReasonDetail>,
    pub(super) candidate_count: usize,
    pub(super) candidate_total_count: usize,
    pub(super) pipeline: KnowledgeRetrievalPipelineReport,
}

pub(super) fn knowledge_retrieval_diagnostics(
    search: &SearchResultSet,
    request: &KnowledgeRetrievalRequest,
    projection_freshness: &SearchProjectionFreshness,
    graph_commit_epoch: u64,
    required_projection_commit_epoch: u64,
    input: KnowledgeRetrievalDiagnosticsInput,
) -> KnowledgeRetrievalDiagnostics {
    let mut empty_reasons = Vec::new();
    let mut empty_reason_codes = Vec::new();
    let candidate_truncation_reasons = knowledge_candidate_truncation_reasons(
        input.candidate_total_count,
        input.candidate_count,
        request.candidate_limit,
    );
    let candidate_truncation_reason_codes = knowledge_candidate_truncation_reason_codes(
        input.candidate_total_count,
        input.candidate_count,
    );
    if input.candidate_count == 0 {
        empty_reason_codes.extend(
            search
                .empty_reason_codes
                .iter()
                .map(knowledge_empty_reason_code_from_search),
        );
        empty_reasons.extend(search.empty_reasons.iter().cloned());
        if input.candidate_total_count == 0 {
            if request.graph_seed_limit == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero);
                empty_reasons.push("graph seed retriever disabled by limit 0".to_string());
            } else if input.graph_seed_candidate_count == 0 {
                empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates);
                empty_reasons.push("graph seed retriever returned no candidates".to_string());
            }
        }
        if !candidate_truncation_reasons.is_empty() {
            empty_reason_codes
                .push(KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates);
        }
        empty_reasons.extend(candidate_truncation_reasons.iter().cloned());
        empty_reason_codes.push(KnowledgeRetrievalEmptyReasonCode::NoCandidates);
        empty_reasons.push("retrieval produced no candidates".to_string());
    }
    let graph_seed_truncation_reasons = knowledge_graph_seed_truncation_reasons(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
        request.graph_seed_limit,
    );
    let graph_seed_truncation_reason_codes = knowledge_graph_seed_truncation_reason_codes(
        input.graph_seed_candidate_count,
        input.graph_seed_returned_count,
    );
    let graph_context_truncation_reason_codes =
        knowledge_graph_context_truncation_reason_codes(&input.graph_context_truncation_reasons);
    KnowledgeRetrievalDiagnostics {
        graph_commit_epoch,
        projection_source_graph_commit_epoch: projection_freshness.source_graph_commit_epoch,
        projection_commit_lag: search_projection_freshness_commit_lag(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        projection_stale: search_projection_is_stale(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        projection_full_reindex_needed: projection_freshness.full_reindex_needed,
        projection_full_reindex_reasons: projection_freshness.full_reindex_reasons.clone(),
        projection_metadata_repair_needed: projection_freshness.metadata_repair_needed,
        projection_metadata_repair_reasons: projection_freshness.metadata_repair_reasons.clone(),
        search_document_count: search.document_count,
        search_filtered_document_count: search.filtered_document_count,
        search_total_hits: search.total_hits,
        search_candidate_set: search.candidate_set.clone(),
        search_candidate_filtered_out_count: search.candidate_set.filtered_out_count,
        search_limit: search.limit,
        search_truncated: search.truncated,
        search_truncation_reason_codes: search.truncation_reason_codes.clone(),
        search_truncation_reasons: search.truncation_reasons.clone(),
        search_fallback_reason_codes: search.fallback_reason_codes.clone(),
        search_fallback_reasons: search.fallback_reasons.clone(),
        rank_window: search.rank_window,
        search_fusion_weights: search.fusion_weights,
        graph_seed_input_candidate_set: input.graph_seed_input_candidate_set,
        graph_seed_candidate_set: input.graph_seed_candidate_set,
        graph_seed_candidate_count: input.graph_seed_candidate_count,
        graph_seed_returned_count: input.graph_seed_returned_count,
        graph_seed_limit: request.graph_seed_limit,
        graph_seed_truncated: !graph_seed_truncation_reasons.is_empty(),
        graph_seed_truncation_reason_codes,
        graph_seed_truncation_reasons,
        graph_context_input_candidate_set: input.graph_context_input_candidate_set,
        graph_context_candidate_set: input.graph_context_candidate_set,
        graph_context_path_count: input.graph_context_path_count,
        graph_context_node_count: input.graph_context_node_count,
        graph_context_relationship_count: input.graph_context_relationship_count,
        graph_context_limit: request.graph_context_limit,
        graph_context_max_hops: request.graph_context_max_hops,
        graph_context_truncated: !input.graph_context_truncation_reasons.is_empty(),
        graph_context_truncation_reason_codes,
        graph_context_truncation_reasons: input.graph_context_truncation_reasons,
        graph_context_fallback_reason_codes: knowledge_graph_context_fallback_reason_codes(request),
        graph_context_fallback_reasons: knowledge_graph_context_fallback_reasons(request),
        fanout_reason_count: input.fanout_reason_details.len(),
        fanout_reason_codes: knowledge_fanout_reason_codes(&input.fanout_reason_details),
        fanout_reasons: knowledge_fanout_reason_messages(&input.fanout_reason_details),
        fanout_reason_details: input.fanout_reason_details,
        candidate_count: input.candidate_count,
        candidate_total_count: input.candidate_total_count,
        candidate_limit: request.candidate_limit,
        candidate_truncated: !candidate_truncation_reasons.is_empty(),
        candidate_truncation_reason_codes,
        candidate_truncation_reasons,
        warnings: knowledge_retrieval_warnings(
            projection_freshness,
            required_projection_commit_epoch,
        ),
        empty_reason_codes,
        empty_reasons,
        pipeline: input.pipeline,
    }
}

pub(super) fn knowledge_fanout_reason_codes(
    details: &[KnowledgeFanoutReasonDetail],
) -> Vec<KnowledgeFanoutReasonCode> {
    details.iter().map(|detail| detail.code).collect()
}

pub(super) fn knowledge_fanout_reason_messages(
    details: &[KnowledgeFanoutReasonDetail],
) -> Vec<String> {
    details
        .iter()
        .map(|detail| detail.message.clone())
        .collect()
}

pub(super) fn knowledge_empty_reason_code_from_search(
    code: &SearchEmptyReasonCode,
) -> KnowledgeRetrievalEmptyReasonCode {
    match code {
        SearchEmptyReasonCode::ProjectionEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchProjectionEmpty
        }
        SearchEmptyReasonCode::MetadataFilterEmpty => {
            KnowledgeRetrievalEmptyReasonCode::SearchMetadataFilterEmpty
        }
        SearchEmptyReasonCode::RetrieverNoHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits
        }
        SearchEmptyReasonCode::LimitExcludedAllHits => {
            KnowledgeRetrievalEmptyReasonCode::SearchLimitExcludedAllHits
        }
    }
}

pub(super) fn knowledge_candidate_truncation_reasons(
    candidate_total_count: usize,
    candidate_count: usize,
    candidate_limit: Option<usize>,
) -> Vec<String> {
    match candidate_limit {
        Some(limit) if candidate_total_count > candidate_count => vec![format!(
            "knowledge_candidate_limit {limit} returned from {candidate_total_count} merged candidates"
        )],
        _ => Vec::new(),
    }
}

pub(super) fn knowledge_candidate_truncation_reason_codes(
    candidate_total_count: usize,
    candidate_count: usize,
) -> Vec<KnowledgeTruncationReasonCode> {
    if candidate_total_count > candidate_count {
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    } else {
        Vec::new()
    }
}

pub(super) fn knowledge_graph_context_truncation_reason_codes(
    truncation_reasons: &[String],
) -> Vec<KnowledgeTruncationReasonCode> {
    if truncation_reasons.is_empty() {
        Vec::new()
    } else {
        vec![KnowledgeTruncationReasonCode::GraphContextLimitExceeded]
    }
}

pub(super) fn knowledge_retrieval_warnings(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if search_projection_is_stale(projection_freshness, required_projection_commit_epoch) {
        warnings.push("search projection is older than graph snapshot".to_string());
    }
    if projection_freshness.full_reindex_needed {
        warnings.push("search projection requires full reindex".to_string());
        warnings.extend(
            projection_freshness
                .full_reindex_reasons
                .iter()
                .map(|reason| format!("search projection full reindex reason: {reason}")),
        );
    }
    if projection_freshness.metadata_repair_needed {
        warnings.push("search projection metadata repair is needed".to_string());
        warnings.extend(
            projection_freshness
                .metadata_repair_reasons
                .iter()
                .map(|reason| format!("search projection metadata repair reason: {reason}")),
        );
    }
    warnings
}

pub(super) fn search_projection_is_stale(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> bool {
    projection_freshness
        .source_graph_commit_epoch
        .map(|projection_epoch| projection_epoch < required_projection_commit_epoch)
        .unwrap_or(false)
}

pub(super) fn search_projection_freshness_commit_lag(
    projection_freshness: &SearchProjectionFreshness,
    required_projection_commit_epoch: u64,
) -> u64 {
    required_projection_commit_epoch
        .saturating_sub(projection_freshness.source_graph_commit_epoch.unwrap_or(0))
}

pub(super) fn graph_seed_candidate_id(seed: &KnowledgeGraphSeed) -> String {
    let label = seed
        .entity
        .labels
        .first()
        .map(String::as_str)
        .unwrap_or("node");
    match seed.entity.external_id.as_deref() {
        Some(external_id) if !external_id.is_empty() => format!("{label}:{external_id}"),
        _ => format!("node:{}", seed.entity.node_id),
    }
}

pub(super) fn graph_seed_candidate_id_internal(seed: &KnowledgeGraphSeedCandidate) -> String {
    seed.id.clone()
}

pub(super) fn graph_seed_candidate_from_node(
    catalog: &Catalog,
    node: &NodeRecord,
    relevance: hawdb_executor::graph_seed::GraphSeedRelevance,
    account: &hawdb_executor::QueryMemoryAccount,
) -> Result<KnowledgeGraphSeedCandidate> {
    use hawdb_executor::graph_seed::property_text;
    use std::borrow::Cow;
    let label = node
        .labels
        .iter()
        .find_map(|label_id| catalog.label_name(*label_id))
        .unwrap_or("node");
    let (external_id, _external_allocation) = if let Some(value) = node.properties.get("id") {
        property_text(value, account, None)?
    } else {
        (Cow::Borrowed(""), None)
    };
    // u64 decimal fallback requires at most twenty bytes.
    let _fallback_allocation = account.reserve(if external_id.is_empty() { 20 } else { 0 })?;
    let external_id = if external_id.is_empty() {
        Cow::Owned(node.id.0.to_string())
    } else {
        external_id
    };
    let property_count = relevance.matched_properties().count();
    let property_bytes = relevance.matched_properties().map(str::len).sum::<usize>();
    let bytes = label
        .len()
        .checked_add(1)
        .and_then(|bytes| bytes.checked_add(external_id.len()))
        .and_then(|bytes| bytes.checked_add(property_count * std::mem::size_of::<String>()))
        .and_then(|bytes| bytes.checked_add(property_bytes))
        .ok_or_else(|| HawDBError::Execution("graph seed candidate size overflow".into()))?;
    let allocation = account.reserve(bytes)?;
    let id = format!("{label}:{external_id}");
    let mut matched_properties = Vec::with_capacity(property_count);
    matched_properties.extend(relevance.matched_properties().map(str::to_string));
    Ok(KnowledgeGraphSeedCandidate {
        id,
        node_id: node.id,
        score: relevance.score,
        matched_properties,
        _allocation: allocation,
    })
}

pub(super) fn knowledge_graph_expansion_scope_filters(
    metadata_filters: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    const SCOPE_FIELDS: &[&str] = &["space_id", "tenant_id", "workspace_id", "visibility"];
    metadata_filters
        .iter()
        .filter(|(field, _)| SCOPE_FIELDS.contains(&field.as_str()))
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect()
}

pub(super) fn knowledge_candidate_score_breakdown(
    source: &impl ScoringFeatureSource,
    scoring: &KnowledgeCandidateScoringPolicy,
    reference_time_millis: u64,
) -> KnowledgeCandidateScoreBreakdown {
    let search_score = source.search_score();
    let graph_seed_score = source.graph_seed_score();
    let combined_score = match scoring {
        KnowledgeCandidateScoringPolicy::Max => search_score
            .into_iter()
            .chain(graph_seed_score)
            .fold(0.0, f64::max),
        KnowledgeCandidateScoringPolicy::WeightedSum {
            search_weight,
            graph_seed_weight,
        } => {
            search_score.unwrap_or(0.0) * search_weight
                + graph_seed_score.unwrap_or(0.0) * graph_seed_weight
        }
        KnowledgeCandidateScoringPolicy::Spec(spec) => {
            let evaluation = spec.evaluate(source, reference_time_millis);
            return KnowledgeCandidateScoreBreakdown {
                search_score,
                graph_seed_score,
                combined_score: evaluation.combined_score,
                scoring_spec: Some(evaluation),
            };
        }
    };
    KnowledgeCandidateScoreBreakdown {
        search_score,
        graph_seed_score,
        combined_score,
        scoring_spec: None,
    }
}

/// Canonical feature values for one rerank candidate.
pub(super) struct KnowledgeCandidateFeatureSource<'a> {
    pub(super) search_score: Option<f64>,
    pub(super) graph_seed_score: Option<f64>,
    pub(super) hop_distance: Option<usize>,
    pub(super) node: Option<&'a NodeRecord>,
}

impl ScoringFeatureSource for KnowledgeCandidateFeatureSource<'_> {
    fn search_score(&self) -> Option<f64> {
        self.search_score
    }

    fn graph_seed_score(&self) -> Option<f64> {
        self.graph_seed_score
    }

    fn hop_distance(&self) -> Option<usize> {
        self.hop_distance
    }

    fn numeric_property(&self, property: &str) -> Option<f64> {
        knowledge_graph_seed_filter_numeric_value(self.node?, property)
    }

    /// Canonical timestamps are epoch milliseconds; fractional and string
    /// encodings are reported as missing rather than guessed.
    fn timestamp_millis(&self, property: &str) -> Option<u64> {
        match self.node?.properties.get(property)? {
            Value::Int(millis) => u64::try_from(*millis).ok(),
            _ => None,
        }
    }
}

pub(super) fn scoring_reference_time_millis() -> u64 {
    hawdb_core::time::SystemTime::now()
        .duration_since(hawdb_core::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
pub(super) fn graph_seed_score(
    node: &NodeRecord,
    query_terms: &BTreeSet<String>,
    normalized_query: &str,
) -> (f64, Vec<String>) {
    const GRAPH_SEED_PROPERTIES: &[&str] =
        &["id", "title", "name", "summary", "content", "body", "text"];
    let mut score = 0.0;
    let mut matched_properties = Vec::new();
    for property in GRAPH_SEED_PROPERTIES {
        let Some(value) = node.properties.get(*property) else {
            continue;
        };
        let text = value_to_external_id(value);
        let normalized_text = text.to_ascii_lowercase();
        let property_terms = knowledge_query_terms(&text);
        let matched_term_count = query_terms
            .iter()
            .filter(|term| property_terms.contains(*term))
            .count();
        let exact_match = !normalized_query.is_empty() && normalized_text == normalized_query;
        let contains_query =
            !normalized_query.is_empty() && normalized_text.contains(normalized_query);
        if matched_term_count > 0 || exact_match || contains_query {
            matched_properties.push((*property).to_string());
            score += matched_term_count as f64;
            if contains_query {
                score += 2.0;
            }
            if *property == "id" && exact_match {
                score += 8.0;
            }
        }
    }
    (score, matched_properties)
}

pub(super) fn knowledge_graph_seed_filter_numeric_value(
    node: &NodeRecord,
    key: &str,
) -> Option<f64> {
    hawdb_executor::graph_seed::numeric_property(node, key)
}

pub(super) fn parse_metadata_filter_number(value: &str) -> Option<f64> {
    let number = value.parse::<f64>().ok()?;
    number.is_finite().then_some(number)
}

pub(super) fn knowledge_graph_seed_filter_value_matches(
    key: &str,
    actual: &str,
    expected: &str,
) -> bool {
    if key == "kind" {
        let Some(expected_label) = search_kind_to_label(expected) else {
            return false;
        };
        return search_label_to_kind(expected_label)
            .is_some_and(|expected_kind| actual == expected_kind);
    }
    if key != "space_id" && search_field_is_enum_like(key) {
        return actual.trim().eq_ignore_ascii_case(expected.trim());
    }
    // String conversion preserves contextual Unicode rules, including final
    // sigma. Per-character lowercase is not the same normalization contract.
    actual.trim().to_lowercase() == expected.trim().to_lowercase()
}

#[cfg(test)]
pub(super) fn normalized_node_space_id(node: &NodeRecord) -> String {
    node.properties
        .get("space_id")
        .map(value_to_external_id)
        .filter(|space_id| !space_id.is_empty())
        .unwrap_or_else(|| "default".to_string())
}

#[cfg(test)]
pub(super) fn node_projection_source_id(node: &NodeRecord) -> Option<String> {
    ["source_id", "thread_id", "source"]
        .into_iter()
        .filter_map(|key| node.properties.get(key).map(value_to_external_id))
        .find(|source_id| !source_id.is_empty())
}

#[cfg(test)]
pub(super) fn knowledge_query_terms(text: &str) -> BTreeSet<String> {
    text.split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

pub(super) fn knowledge_context_path_node_count(paths: &[KnowledgeGraphContextPath]) -> usize {
    paths
        .iter()
        .flat_map(|path| [path.source_node_id, path.target_node_id])
        .collect::<BTreeSet<_>>()
        .len()
}

pub(super) struct DenseAdjacencyDiagnosticContext<'a, S = GraphStore> {
    pub(super) catalog: &'a Catalog,
    pub(super) store: &'a S,
    pub(super) operation: &'a str,
    pub(super) relationship_type: Option<crate::schema::RelTypeId>,
}

pub(super) fn record_dense_adjacency_diagnostics<S: crate::executor::ExecutionStore>(
    context: DenseAdjacencyDiagnosticContext<'_, S>,
    node_id: NodeId,
    reported_dense_groups: &mut BTreeSet<String>,
    fanout_reasons: &mut Vec<KnowledgeFanoutReasonDetail>,
) {
    for adjacency_direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
        let stats = match context.relationship_type {
            Some(rel_type) => {
                vec![context
                    .store
                    .adjacency_group_stats(node_id, rel_type, adjacency_direction)]
            }
            None => context
                .store
                .adjacency_group_stats_for_node(node_id, adjacency_direction),
        };
        for stats in stats {
            if stats.layout != AdjacencyLayout::Dense {
                continue;
            }
            let rel_type_name = context
                .catalog
                .rel_type_name(stats.rel_type)
                .unwrap_or("<unknown>");
            let direction = adjacency_direction_name(adjacency_direction);
            let key = format!(
                "{}:{rel_type_name}:{direction}:{}",
                context.operation, node_id.0
            );
            if reported_dense_groups.insert(key) {
                fanout_reasons.push(KnowledgeFanoutReasonDetail::dense_adjacency(
                    context.operation,
                    rel_type_name,
                    direction,
                    node_id.0,
                    stats.degree,
                ));
            }
        }
    }
}

pub(super) fn knowledge_path_direction_for_adjacency(
    adjacency_direction: AdjacencyDirection,
) -> KnowledgeGraphPathDirection {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => KnowledgeGraphPathDirection::Outgoing,
        AdjacencyDirection::Incoming => KnowledgeGraphPathDirection::Incoming,
    }
}

pub(super) fn adjacency_direction_name(adjacency_direction: AdjacencyDirection) -> &'static str {
    match adjacency_direction {
        AdjacencyDirection::Outgoing => "outgoing",
        AdjacencyDirection::Incoming => "incoming",
    }
}
