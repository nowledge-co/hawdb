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
fn knowledge_retrieval_returns_graph_seeds_without_search_hits() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'mem_graph', title: 'Graph note'})-[:MENTIONS]->(:Entity {id: 'graph', name: 'Graph database'})",
        )
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_other', title: 'Graph companion'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 5,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 1,
                graph_context_limit: 2,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert!(output.search.hits.is_empty());
    assert!(output.evidence.is_empty());
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(output.candidates[0].source_rank, 1);
    assert_eq!(output.candidates[0].id, "Entity:graph");
    assert_eq!(output.candidates[0].canonical_node_id, Some(1));
    assert_eq!(
        output.candidates[0].entity.as_ref().unwrap(),
        &output.graph_seeds[0].entity
    );
    assert_eq!(output.candidates[0].graph_context_path_count, 1);
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert_eq!(graph_seed_report.candidate_count, 3);
    assert_eq!(graph_seed_report.top_candidates.len(), 1);
    assert_eq!(graph_seed_report.top_candidates[0].id, "Entity:graph");
    assert_eq!(
        graph_seed_report.top_candidates[0].canonical_node_id,
        Some(1)
    );
    assert!(graph_seed_report.top_candidates[0].matched_spans.is_empty());
    assert_eq!(
        graph_seed_report.top_candidates[0].graph_context_path_count,
        1
    );
    assert_eq!(
        graph_seed_report.top_candidates[0].projection_freshness,
        None
    );
    assert_eq!(graph_seed_report.top_candidates[0].rank, 1);
    assert_eq!(graph_seed_report.limit, Some(1));
    assert_eq!(graph_seed_report.rank_window, None);
    assert_eq!(graph_seed_report.fusion_weight, None);
    assert!(graph_seed_report.truncated);
    assert_eq!(
        graph_seed_report.truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    );
    assert!(output.diagnostics.graph_seed_truncated);
    assert_eq!(
        output.diagnostics.graph_seed_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::GraphSeedLimitExceeded]
    );
    assert_eq!(
        output.diagnostics.graph_seed_truncation_reasons,
        vec!["graph_seed limit 1 returned from 3 candidates".to_string()]
    );
    assert!(output.diagnostics.empty_reasons.is_empty());
    assert!(graph_seed_report
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("graph_seed limit 1")));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("graph")
    );
    assert!(output.graph_seeds[0]
        .matched_properties
        .contains(&"id".to_string()));
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output.graph_context_paths[0].seed_hit_id.as_str(),
        "Entity:graph"
    );
    assert_eq!(
        output.graph_context_paths[0].direction,
        KnowledgeGraphPathDirection::Incoming
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "MENTIONS"
    );
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("mem_graph")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("graph")
    );
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("knowledge_graph_seed_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::GraphSeedLimitReached]
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(output.fanout_reason_details[0].total, Some(3));
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);
}

#[test]
fn knowledge_retrieval_keeps_context_per_retriever_seed() {
    let mut db = Database::new();
    db.query(
            "CREATE (:Memory {id: 'shared', title: 'Shared graph seed', content: 'shared graph seed'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Entity'})",
        )
            .unwrap();
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "shared graph seed".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 1,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    let search_evidence = output
        .evidence
        .iter()
        .find(|evidence| evidence.canonical_node_id == Some(0))
        .expect("search evidence for shared memory");
    assert_eq!(search_evidence.graph_context_path_count, 1);

    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed report");
    let graph_seed_candidate = graph_seed_report
        .top_candidates
        .iter()
        .find(|candidate| candidate.canonical_node_id == Some(0))
        .expect("graph seed candidate for shared memory");
    assert_eq!(graph_seed_candidate.graph_context_path_count, 1);
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == search_evidence.hit_id));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "Memory:shared"));
}

#[test]
fn knowledge_retrieval_applies_candidate_limit_after_merge() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'graph_1', title: 'Graph candidate one'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'graph_2', title: 'Graph candidate two'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'graph_3', title: 'Graph candidate three'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph candidate".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 5,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: Some(1),
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 3,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.graph_seeds.len(), 3);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.diagnostics.search_limit, 5);
    assert_eq!(output.diagnostics.candidate_limit, Some(1));
    assert_eq!(output.diagnostics.candidate_count, 1);
    assert_eq!(output.diagnostics.candidate_total_count, 3);
    assert!(output.diagnostics.candidate_truncated);
    assert_eq!(
        output.diagnostics.candidate_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    );
    assert!(output.diagnostics.empty_reason_codes.is_empty());
    assert_eq!(
        output.diagnostics.candidate_truncation_reasons,
        vec!["knowledge_candidate_limit 1 returned from 3 merged candidates".to_string()]
    );
    assert_eq!(output.diagnostics.graph_seed_limit, 3);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(
        output.candidates[0].source,
        KnowledgeCandidateSource::GraphSeed
    );
    assert_eq!(
        output.candidates[0].merged_sources,
        vec![KnowledgeCandidateSource::GraphSeed]
    );
    assert_eq!(output.fanout_reasons.len(), 1);
    assert!(output.fanout_reasons[0].contains("knowledge_candidate_limit 1"));
    assert_eq!(
        output.fanout_reason_codes,
        vec![KnowledgeFanoutReasonCode::CandidateLimitReached]
    );
    assert_eq!(output.fanout_reason_details[0].limit, Some(1));
    assert_eq!(output.fanout_reason_details[0].total, Some(3));
    assert_eq!(
        output.diagnostics.fanout_reason_codes,
        output.fanout_reason_codes
    );
    assert_eq!(output.diagnostics.fanout_reasons, output.fanout_reasons);

    let empty_by_limit = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph candidate".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 5,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: Some(0),
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 3,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();
    assert!(empty_by_limit.candidates.is_empty());
    assert_eq!(empty_by_limit.diagnostics.candidate_limit, Some(0));
    assert_eq!(empty_by_limit.diagnostics.candidate_count, 0);
    assert_eq!(empty_by_limit.diagnostics.candidate_total_count, 3);
    assert!(empty_by_limit.diagnostics.candidate_truncated);
    assert_eq!(
        empty_by_limit.diagnostics.candidate_truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::CandidateLimitExceeded]
    );
    assert!(empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "knowledge_candidate_limit 0 returned from 3 merged candidates"));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .iter()
        .map(|code| code.as_str())
        .any(|code| code == "candidate_limit_excluded_all_candidates"));
    assert!(empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(!empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates));
    assert!(empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let search_empty_by_limit = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph candidate".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 5,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: Some(0),
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();
    assert!(search_empty_by_limit.search.total_hits > 0);
    assert_eq!(search_empty_by_limit.diagnostics.candidate_count, 0);
    assert!(search_empty_by_limit.diagnostics.candidate_total_count > 0);
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason.starts_with("knowledge_candidate_limit 0")));
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::CandidateLimitExcludedAllCandidates));
    assert!(search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(!search_empty_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(!search_empty_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
}

#[test]
fn knowledge_retrieval_applies_weighted_candidate_scoring() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'weighted', title: 'Weighted graph candidate', content: 'graph graph graph'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "weighted graph".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::WeightedSum {
                    search_weight: 2.0,
                    graph_seed_weight: 0.5,
                },
                graph_seed_limit: 1,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.candidates.len(), 1);
    let candidate = &output.candidates[0];
    assert_eq!(
        candidate.merged_sources,
        vec![
            KnowledgeCandidateSource::SearchHit,
            KnowledgeCandidateSource::GraphSeed
        ]
    );
    let search_score = candidate.score_breakdown.search_score.unwrap();
    let graph_seed_score = candidate.score_breakdown.graph_seed_score.unwrap();
    assert_eq!(candidate.score_breakdown.combined_score, candidate.score);
    assert_eq!(candidate.score, search_score * 2.0 + graph_seed_score * 0.5);
}
