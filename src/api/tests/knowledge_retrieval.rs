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

use super::*;

#[test]
fn retrieves_knowledge_through_database_facade() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph retrieval', content: 'Projection freshness and truncation diagnostics', source_id: 'thread_1'})-[:MENTIONS {chunk_index: 3, source_id: 'thread_1'}]->(:Entity {id: 'entity_1', name: 'HawDB'})")
            .unwrap();
    db.query(
            "CREATE (:Memory {id: 'mem_2', title: 'Graph retrieval', content: 'Search result diagnostics'})",
        )
        .unwrap();
    let extra_entity = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("entity_2".to_string())),
                ("name".to_string(), Value::String("Graph".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            extra_entity,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    let rebuild = db
        .rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "projection diagnostics".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 2,
                graph_context_limit: 1,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(rebuild.indexed_documents, 4);
    assert_eq!(output.graph_commit_epoch, 4);
    assert_eq!(output.projection_freshness.document_count, 4);
    assert_eq!(
        output.projection_freshness.source_graph_commit_epoch,
        Some(4)
    );
    assert_eq!(output.search.total_hits, 2);
    assert_eq!(output.search.hits.len(), 1);
    assert_eq!(output.diagnostics.graph_commit_epoch, 4);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(4)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 0);
    assert!(!output.diagnostics.projection_stale);
    assert!(!output.diagnostics.projection_full_reindex_needed);
    assert!(!output.diagnostics.projection_metadata_repair_needed);
    assert_eq!(output.diagnostics.search_limit, 1);
    assert!(output.diagnostics.search_truncated);
    assert_eq!(
        output.diagnostics.search_truncation_reason_codes,
        vec![SearchTruncationReasonCode::LimitExceeded]
    );
    assert_eq!(
        output.diagnostics.search_truncation_reasons,
        vec!["limit 1 returned from 2 matching hits".to_string()]
    );
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(output.diagnostics.graph_seed_limit, 2);
    assert_eq!(output.diagnostics.graph_context_limit, 1);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert!(output.diagnostics.graph_context_truncated);
    assert_eq!(
        output.diagnostics.graph_context_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphContextLimitExceeded]
    );
    assert_eq!(
        output.diagnostics.graph_context_truncation_reasons,
        vec!["graph_context_limit 1 reached while expanding hit memory:mem_1".to_string()]
    );
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert_eq!(output.evidence.len(), 1);
    assert_eq!(output.candidates.len(), 2);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::SearchHit
    );
    assert_eq!(output.candidates[0].source_rank, 1);
    assert_eq!(output.candidates[0].id, output.search.hits[0].id);
    assert_eq!(output.candidates[0].canonical_node_id, Some(0));
    assert_eq!(
        output.candidates[0].merged_sources,
        vec![
            KnowledgeCandidateSource::SearchHit,
            KnowledgeCandidateSource::GraphSeed
        ]
    );
    assert_eq!(output.candidates[0].entity.as_ref().unwrap().node_id, 0);
    assert_eq!(output.candidates[0].graph_context_path_count, 1);
    assert_eq!(
        output.candidates[0].evidence.as_ref().unwrap(),
        &output.evidence[0]
    );
    assert!(output.candidates[0]
        .matched_properties
        .contains(&"content".to_string()));
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.candidate_count, 2);
    assert_eq!(text_retriever.backend, "bm25_text");
    assert_eq!(text_retriever.limit, Some(1));
    assert_eq!(text_retriever.rank_window, None);
    assert_eq!(text_retriever.fusion_weight, Some(1.0));
    let search_text_retriever = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text search retriever report");
    assert_eq!(
        text_retriever.candidate_set,
        search_text_retriever.candidate_set
    );
    assert_eq!(text_retriever.backend, search_text_retriever.backend);
    assert_eq!(
        text_retriever.top_candidates[0].kind.as_deref(),
        Some("memory")
    );
    assert_eq!(
        text_retriever.top_candidates[0].external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        text_retriever.top_candidates[0].source_id.as_deref(),
        Some("thread_1")
    );
    assert_eq!(text_retriever.top_candidates[0].canonical_node_id, Some(0));
    assert_eq!(text_retriever.top_candidates[0].graph_context_path_count, 1);
    assert_eq!(
        text_retriever.top_candidates[0].projection_freshness,
        Some(output.projection_freshness.clone())
    );
    assert!(text_retriever.top_candidates[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content" && span.term == "projection"));
    assert!(output.search.truncated);
    assert!(output.search.truncation_reasons[0].contains("limit 1"));
    assert_eq!(
        output.search.hits[0].projection_freshness,
        output.projection_freshness
    );
    assert_eq!(output.search.hits[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(output.graph_context_paths[0].hop, 1);
    assert_eq!(
        output.graph_context_paths[0].direction,
        KnowledgeGraphPathDirection::Outgoing
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "MENTIONS"
    );
    assert_eq!(
        output.graph_context_paths[0]
            .relationship_properties
            .get("chunk_index"),
        Some(&Value::Int(3))
    );
    assert_eq!(
        output.graph_context_paths[0]
            .relationship_properties
            .get("source_id"),
        Some(&Value::String("thread_1".to_string()))
    );
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("entity_1")
    );
    assert_eq!(output.evidence[0].hit_id, output.search.hits[0].id);
    assert_eq!(output.evidence[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.evidence[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.evidence[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(output.evidence[0].canonical_node_id, Some(0));
    assert_eq!(output.evidence[0].graph_context_path_count, 1);
    assert!(output.evidence[0]
        .matched_terms
        .contains(&"projection".to_string()));
    assert!(output.evidence[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content"
            && span.text == "Projection"
            && span.term == "projection"));
    assert!(output.evidence[0].text_score > 0.0);
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector retriever report");
    assert_eq!(
        vector_report.input_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(output.graph_seeds.len(), 2);
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert!(graph_seed_report.available);
    assert_eq!(graph_seed_report.backend, "graph_seed_expand");
    assert_eq!(graph_seed_report.candidate_count, 2);
    assert_eq!(
        graph_seed_report.input_candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        graph_seed_report.input_candidate_set.representation,
        "filtered_node_ids"
    );
    assert_eq!(graph_seed_report.input_candidate_set.cardinality, 4);
    assert_eq!(graph_seed_report.input_candidate_set.filtered_out_count, 0);
    assert_eq!(
        graph_seed_report
            .input_candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert_eq!(graph_seed_report.limit, Some(2));
    assert_eq!(graph_seed_report.rank_window, None);
    assert_eq!(graph_seed_report.fusion_weight, None);
    assert!(graph_seed_report.fallback_reasons.is_empty());
    assert_eq!(
        graph_seed_report.candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        graph_seed_report.candidate_set.representation,
        "ranked_node_ids"
    );
    assert_eq!(graph_seed_report.candidate_set.cardinality, 2);
    assert!(graph_seed_report.candidate_set.exact);
    assert_eq!(
        graph_seed_report
            .candidate_set
            .snapshot_source_graph_commit_epoch,
        Some(output.graph_commit_epoch)
    );
    assert_eq!(graph_seed_report.candidate_set.policy_epoch, None);
    assert_eq!(graph_seed_report.top_candidates.len(), 2);
    assert_eq!(graph_seed_report.top_candidates[0].rank, 1);
    assert_eq!(
        graph_seed_report.top_candidates[0].kind.as_deref(),
        Some("Memory")
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(graph_seed_report.top_candidates[0].source_id, None);
    assert_eq!(
        graph_seed_report.top_candidates[0].canonical_node_id,
        Some(0)
    );
    assert!(graph_seed_report.top_candidates[0].matched_spans.is_empty());
    assert_eq!(
        graph_seed_report.top_candidates[0].graph_context_path_count,
        0
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].projection_freshness,
        None
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].id.as_str(),
        "Memory:mem_1"
    );
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
    assert_eq!(
        output.candidates[1].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(output.candidates[1].source_rank, 2);
    assert_eq!(output.candidates[1].canonical_node_id, Some(2));
    assert!(output.candidates[1].evidence.is_none());
    assert_eq!(
        output.candidates[1].entity.as_ref().unwrap(),
        &output.graph_seeds[1].entity
    );
    assert!(output.graph_seeds[0]
        .matched_properties
        .contains(&"content".to_string()));
    assert!(output.graph_seeds[0].score >= output.graph_seeds[1].score);
    assert!(output.diagnostics.graph_context_path_count >= 1);
    assert_eq!(output.diagnostics.graph_context_node_count, 2);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 1);
    assert_eq!(output.diagnostics.fanout_reason_count, 1);
    assert!(output.diagnostics.warnings.is_empty());
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("graph_context_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::GraphContextLimitReached]
    );
    assert_eq!(output.fanout_reason_details.len(), 1);
    assert_eq!(
        output.fanout_reason_details[0].code,
        KnowledgeFanoutReasonCode::GraphContextLimitReached
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(
        output.fanout_reason_details[0].seed_hit_id.as_deref(),
        Some("memory:mem_1")
    );
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(
        output.diagnostics.fanout_reason_details,
        output.fanout_reason_details
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);
}

#[test]
fn nowledge_deep_retrieval_profile_preserves_visible_limit_with_wide_candidate_window() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Deep retrieval', content: 'deep search graph expansion', source_id: 'thread_1'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Deep entity'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Deep retrieval', content: 'deep search graph expansion', source_id: 'thread_2'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let request = KnowledgeRetrievalRequest::nowledge_deep(
        "deep search graph expansion",
        None,
        SearchMode::Text,
        1,
        0,
        BTreeMap::new(),
    );
    assert_eq!(request.limit, 1);
    assert_eq!(
        request.rank_window,
        Some(NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW)
    );
    assert_eq!(
        request.candidate_limit,
        Some(NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW)
    );
    assert_eq!(
        request.graph_seed_limit,
        NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT
    );
    assert_eq!(
        request.graph_context_limit,
        NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW
    );
    assert_eq!(
        request.graph_context_max_hops,
        NOWLEDGE_DEEP_SEARCH_GRAPH_CONTEXT_MAX_HOPS
    );

    let output = db.retrieve_knowledge(&search_index, &request).unwrap();
    assert_eq!(output.search.hits.len(), 1);
    assert!(output.search.total_hits >= 2);
    assert_eq!(output.diagnostics.search_limit, 1);
    assert_eq!(
        output.diagnostics.rank_window,
        Some(NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW)
    );
    assert_eq!(
        output.diagnostics.candidate_limit,
        Some(NOWLEDGE_DEEP_SEARCH_MIN_RANK_WINDOW)
    );
    assert_eq!(
        output.diagnostics.graph_seed_limit,
        NOWLEDGE_DEEP_SEARCH_MIN_GRAPH_SEED_LIMIT
    );
    assert!(output.diagnostics.graph_context_path_count >= 1);
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.target_external_id.as_deref() == Some("entity_1")));
}

#[test]
fn nowledge_deep_retrieval_profile_uses_filtered_candidate_window() {
    let request = KnowledgeRetrievalRequest::nowledge_deep(
        "deep filtered search",
        None,
        SearchMode::Text,
        5,
        2,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
    );

    assert_eq!(request.limit, 5);
    assert_eq!(request.offset, 2);
    assert_eq!(
        request.rank_window,
        Some(NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW)
    );
    assert_eq!(
        request.candidate_limit,
        Some(NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW)
    );
    assert_eq!(
        request.graph_seed_limit,
        nowledge_deep_search_graph_seed_limit(7)
    );
    assert_eq!(
        request.graph_context_limit,
        NOWLEDGE_DEEP_SEARCH_FILTERED_RANK_WINDOW
    );
    assert_eq!(
        request.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
}
