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
fn retrieves_bounded_multi_hop_knowledge_context() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root traversal', content: 'Two hop graph context'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();
    let leaf = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("leaf".to_string())),
                ("name".to_string(), Value::String("Leaf".to_string())),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(1),
            leaf,
            "LINKS",
            BTreeMap::from([("weight".to_string(), Value::Int(2))]),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "root traversal".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 4,
                graph_context_max_hops: 2,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.graph_context_paths.len(), 2);
    assert!(output.fanout_reasons.is_empty());
    assert!(output.graph_context_paths.iter().any(|path| path.hop == 1
        && path.source_external_id.as_deref() == Some("root")
        && path.target_external_id.as_deref() == Some("mid")));
    assert!(output.graph_context_paths.iter().any(|path| path.hop == 2
        && path.source_external_id.as_deref() == Some("mid")
        && path.target_external_id.as_deref() == Some("leaf")
        && path.relationship_properties.get("weight") == Some(&Value::Int(2))));
}

#[test]
fn knowledge_retrieval_reports_graph_context_disabled_by_max_hops() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root traversal', content: 'Zero hop graph context'})-[:LINKS]->(:Entity {id: 'mid', name: 'Mid'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "root traversal".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 1,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 4,
                graph_context_max_hops: 0,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert!(output.graph_context_paths.is_empty());
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.graph_context_node_count, 0);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 0);
    assert_eq!(
        output.diagnostics.graph_context_fallback_reasons,
        vec!["graph context expansion disabled by max_hops 0".to_string()]
    );
    assert_eq!(
        output.diagnostics.graph_context_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphContextMaxHopsZero]
    );
    assert!(output.fanout_reasons.is_empty());
    assert!(!output.diagnostics.graph_context_truncated);
}

#[test]
fn knowledge_retrieval_applies_rank_window_to_hybrid_search() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'top_vector', title: 'Vector seed', content: 'orthogonal text'})",
    )
    .unwrap();
    db.query(
        "CREATE (:Memory {id: 'top_text', title: 'Graph seed', content: 'graph graph retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'second_text', title: 'Fallback', content: 'graph context'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:top_vector".to_string(),
            title: "Vector seed".to_string(),
            content: "orthogonal text".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "top_vector".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:top_text".to_string(),
            title: "Graph seed".to_string(),
            content: "graph graph retrieval".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "top_text".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:second_text".to_string(),
            title: "Fallback".to_string(),
            content: "graph context".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "second_text".to_string()),
            ]),
        })
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "graph".to_string(),
                query_embedding: Some(vec![1.0, 0.0]),
                mode: SearchMode::Hybrid,
                limit: 10,
                offset: 0,
                rank_window: Some(1),
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.rank_window, Some(1));
    assert_eq!(output.diagnostics.search_limit, 10);
    assert_eq!(output.diagnostics.rank_window, Some(1));
    assert_eq!(output.diagnostics.graph_seed_limit, 0);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert_eq!(output.evidence.len(), output.search.hits.len());
    assert!(output
        .evidence
        .iter()
        .all(|evidence| evidence.rrf_score == evidence.score));
    let vector_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector knowledge retriever report");
    assert_eq!(
        vector_retriever.input_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(vector_retriever.limit, Some(10));
    assert_eq!(vector_retriever.rank_window, Some(1));
    assert_eq!(vector_retriever.fusion_weight, Some(1.0));
    let text_report = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text retriever report");
    assert_eq!(text_report.candidate_count, 2);
    assert_eq!(text_report.top_candidates.len(), 1);
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.limit, Some(10));
    assert_eq!(text_retriever.rank_window, Some(1));
    assert_eq!(text_retriever.fusion_weight, Some(1.0));
    assert_eq!(text_retriever.top_candidates[0].canonical_node_id, Some(1));
    assert!(text_retriever.truncated);
    assert_eq!(
        text_retriever.truncation_reason_codes,
        vec![KnowledgeTruncationReasonCode::RankWindowExceeded]
    );
    assert!(text_retriever
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("rank_window 1")));
    let second_text = output
        .search
        .hits
        .iter()
        .find(|hit| hit.id == "memory:second_text");
    assert!(second_text.is_none_or(|hit| hit.text_rank.is_none()));
}

#[test]
fn knowledge_retrieval_applies_search_fusion_weights() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'vector_top', title: 'Vector seed', content: 'semantic evidence'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'text_top', title: 'Graph retrieval', content: 'graph retrieval graph retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:vector_top".to_string(),
            title: "Vector seed".to_string(),
            content: "semantic evidence".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "vector_top".to_string()),
            ]),
        })
        .unwrap();
    search_index
        .upsert(crate::search::SearchDocument {
            id: "memory:text_top".to_string(),
            title: "Graph retrieval".to_string(),
            content: "graph retrieval graph retrieval".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "text_top".to_string()),
            ]),
        })
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "graph retrieval".to_string(),
                query_embedding: Some(vec![1.0, 0.0]),
                mode: SearchMode::Hybrid,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights {
                    vector_weight: 1.0,
                    text_weight: 3.0,
                },
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(
        output.search.fusion_weights,
        SearchFusionWeights {
            vector_weight: 1.0,
            text_weight: 3.0
        }
    );
    assert_eq!(
        output.diagnostics.search_fusion_weights,
        SearchFusionWeights {
            vector_weight: 1.0,
            text_weight: 3.0
        }
    );
    let vector_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("vector knowledge retriever report");
    assert_eq!(vector_retriever.fusion_weight, Some(1.0));
    let text_retriever = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text knowledge retriever report");
    assert_eq!(text_retriever.fusion_weight, Some(3.0));
    assert_eq!(output.search.hits[0].id, "memory:text_top");
    assert_eq!(output.evidence[0].hit_id, "memory:text_top");
    assert!(output.evidence[0].text_rrf_score > 0.0);
    assert_eq!(output.evidence[0].vector_rrf_score, 0.0);
    assert_eq!(output.evidence[0].score, output.evidence[0].rrf_score);
}
