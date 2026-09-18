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
fn knowledge_retrieval_binds_idless_search_hits_to_canonical_nodes() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {title: 'Anonymous graph', content: 'idless projection retrieval'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'HawDB'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "idless projection retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 0,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.evidence[0].canonical_node_id, Some(0));
    assert_eq!(output.evidence[0].graph_context_path_count, 1);
    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output.graph_context_paths[0].source_external_id.as_deref(),
        Some("0")
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("entity_1")
    );
}

#[test]
fn knowledge_retrieval_external_filter_uses_projected_identity_for_idless_nodes() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {title: 'Anonymous graph', content: 'projected identity retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'named', title: 'Named graph', content: 'projected identity retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "projected identity retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("external_id".to_string(), "0".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("0")
    );
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:0");
}

#[test]
fn knowledge_retrieval_external_filter_falls_back_for_empty_projected_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: '', title: 'Empty id graph', content: 'empty projected identity retrieval'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'named', title: 'Named graph', content: 'empty projected identity retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "empty projected identity retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("external_id".to_string(), "0".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.search.hits[0].id, "memory:0");
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("0"));
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("0")
    );
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:0");
}

#[test]
fn knowledge_retrieval_normalizes_default_space_filters() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Scoped graph', content: 'default space retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Scoped graph', content: 'default space retrieval', space_id: ''})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_3', title: 'Scoped graph', content: 'default space retrieval', space_id: 'team'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "default space retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();
    let search_hit_ids = output
        .search
        .hits
        .iter()
        .map(|hit| hit.external_id.as_deref())
        .collect::<BTreeSet<_>>();
    let graph_seed_ids = output
        .graph_seeds
        .iter()
        .map(|seed| seed.entity.external_id.as_deref())
        .collect::<BTreeSet<_>>();

    assert_eq!(output.search.total_hits, 2);
    assert_eq!(output.diagnostics.search_filtered_document_count, 2);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 2);
    assert!(search_hit_ids.contains(&Some("mem_1")));
    assert!(search_hit_ids.contains(&Some("mem_2")));
    assert!(!search_hit_ids.contains(&Some("mem_3")));
    assert!(graph_seed_ids.contains(&Some("mem_1")));
    assert!(graph_seed_ids.contains(&Some("mem_2")));
    assert!(!graph_seed_ids.contains(&Some("mem_3")));
}
