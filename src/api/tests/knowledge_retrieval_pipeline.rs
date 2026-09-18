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

fn retrieval_request(limit: usize, candidate_limit: usize) -> KnowledgeRetrievalRequest {
    KnowledgeRetrievalRequest {
        query_text: "pipeline needle".to_string(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit,
        offset: 0,
        rank_window: Some(limit),
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        candidate_limit: Some(candidate_limit),
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 0,
        graph_context_limit: 0,
        graph_context_max_hops: 0,
    }
}

#[test]
fn knowledge_retrieval_hydrates_canonical_output_after_top_k() {
    let mut config = DatabaseConfig {
        max_read_result_payload_bytes: Some(48 * 1024),
        ..DatabaseConfig::default()
    };
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    let payload = format!("pipeline needle {}", "x".repeat(20 * 1024));
    for id in ["memory-a", "memory-b"] {
        db.store
            .create_node(
                &mut db.catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("content".to_string(), Value::String(payload.clone())),
                ]),
            )
            .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .try_retrieve_knowledge(&search_index, &retrieval_request(2, 1))
        .unwrap();

    assert_eq!(
        output.diagnostics.pipeline.stages,
        vec![
            KnowledgeRetrievalStage::SearchCandidate,
            KnowledgeRetrievalStage::MetadataFilter,
            KnowledgeRetrievalStage::AuthorizedGraphExpand,
            KnowledgeRetrievalStage::Rerank,
            KnowledgeRetrievalStage::TopK,
            KnowledgeRetrievalStage::CanonicalHydration,
        ]
    );
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(
        output
            .diagnostics
            .pipeline
            .canonical_output_hydrated_candidate_count,
        1
    );
    assert_eq!(
        output
            .diagnostics
            .pipeline
            .canonical_output_hydrated_node_count,
        1
    );
    assert!(
        output
            .diagnostics
            .pipeline
            .canonical_output_hydration_after_top_k
    );
    assert!(
        output.diagnostics.pipeline.peak_tracked_memory_bytes
            <= output.diagnostics.pipeline.query_memory_budget_bytes
    );
    assert!(
        output.diagnostics.pipeline.result_payload_bytes
            <= output.diagnostics.pipeline.result_payload_budget_bytes
    );
}

#[test]
fn knowledge_retrieval_result_payload_budget_fails_closed() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_payload_bytes: Some(1024),
        ..DatabaseConfig::default()
    });
    db.store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("memory-a".to_string())),
                (
                    "content".to_string(),
                    Value::String(format!("pipeline needle {}", "x".repeat(4096))),
                ),
            ]),
        )
        .unwrap();
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let error = db
        .try_retrieve_knowledge(&search_index, &retrieval_request(1, 1))
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("exceeding max_read_result_payload_bytes 1024"));
}

#[test]
fn knowledge_retrieval_graph_expansion_preserves_space_scope() {
    let mut db = Database::new();
    let memory = db
        .store
        .create_node(
            &mut db.catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("memory-a".to_string())),
                (
                    "content".to_string(),
                    Value::String("pipeline needle".to_string()),
                ),
                ("space_id".to_string(), Value::String("space-a".to_string())),
            ]),
        )
        .unwrap();
    let allowed = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("allowed".to_string())),
                ("space_id".to_string(), Value::String("space-a".to_string())),
            ]),
        )
        .unwrap();
    let denied = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("denied".to_string())),
                ("space_id".to_string(), Value::String("space-b".to_string())),
            ]),
        )
        .unwrap();
    for target in [allowed, denied] {
        db.store
            .create_relationship(&mut db.catalog, memory, target, "MENTIONS", BTreeMap::new())
            .unwrap();
    }
    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let mut request = retrieval_request(1, 1);
    request
        .metadata_filters
        .insert("space_id".to_string(), "space-a".to_string());
    request.graph_context_limit = 8;
    request.graph_context_max_hops = 1;

    let output = db.try_retrieve_knowledge(&search_index, &request).unwrap();

    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("allowed")
    );
    assert!(
        output
            .diagnostics
            .pipeline
            .metadata_filter_authorized_graph_expansion
    );
}

#[test]
fn knowledge_retrieval_filters_search_hits_without_canonical_identity() {
    let db = Database::new();
    let mut search_index = SearchIndex::in_memory();
    search_index
        .upsert(SearchDocument {
            id: "memory:deleted".to_string(),
            title: "Stale projection".to_string(),
            content: "pipeline needle".to_string(),
            embedding: None,
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "deleted".to_string()),
            ]),
        })
        .unwrap();

    let output = db
        .try_retrieve_knowledge(&search_index, &retrieval_request(1, 1))
        .unwrap();

    assert!(output.search.hits.is_empty());
    assert!(output.evidence.is_empty());
    assert!(output.candidates.is_empty());
    assert_eq!(
        output
            .diagnostics
            .pipeline
            .canonical_identity_filtered_out_count,
        1
    );
}
