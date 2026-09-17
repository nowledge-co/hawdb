use super::*;

#[test]
fn knowledge_retrieval_expands_graph_context_by_ordered_adjacency() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Ordered retrieval context'})")
        .unwrap();
    let lower_neighbor_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("lower-neighbor".to_string()),
                ),
                (
                    "name".to_string(),
                    Value::String("Lower neighbor".to_string()),
                ),
            ]),
        )
        .unwrap();
    let higher_neighbor_id = db
        .store
        .create_node(
            &mut db.catalog,
            "Entity",
            BTreeMap::from([
                (
                    "id".to_string(),
                    Value::String("higher-neighbor".to_string()),
                ),
                (
                    "name".to_string(),
                    Value::String("Higher neighbor".to_string()),
                ),
            ]),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            higher_neighbor_id,
            "MENTIONS",
            BTreeMap::new(),
        )
        .unwrap();
    db.store
        .create_relationship(
            &mut db.catalog,
            NodeId(0),
            lower_neighbor_id,
            "RELATES_TO",
            BTreeMap::new(),
        )
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "ordered retrieval".to_string(),
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
                graph_context_limit: 1,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.graph_context_paths.len(), 1);
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .cardinality,
        1
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.cardinality,
        1
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        output.graph_context_paths[0].target_external_id.as_deref(),
        Some("lower-neighbor")
    );
    assert_eq!(
        output.graph_context_paths[0].relationship_type.as_str(),
        "RELATES_TO"
    );
    assert!(output
        .fanout_reasons
        .iter()
        .any(|reason| reason.contains("graph_context_limit 1")));
}

#[test]
fn knowledge_retrieval_reports_dense_graph_context_without_truncation() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Dense retrieval root'})")
        .unwrap();
    for index in 0..DENSE_ADJACENCY_DEGREE_THRESHOLD {
        let target = db
            .store
            .create_node(
                &mut db.catalog,
                "Entity",
                BTreeMap::from([
                    ("id".to_string(), Value::String(format!("entity-{index}"))),
                    ("name".to_string(), Value::String(format!("Entity {index}"))),
                ]),
            )
            .unwrap();
        db.store
            .create_relationship(
                &mut db.catalog,
                NodeId(0),
                target,
                "MENTIONS",
                BTreeMap::new(),
            )
            .unwrap();
    }

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "dense retrieval".to_string(),
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
                graph_context_limit: DENSE_ADJACENCY_DEGREE_THRESHOLD,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(
        output.graph_context_paths.len(),
        DENSE_ADJACENCY_DEGREE_THRESHOLD
    );
    assert_eq!(output.diagnostics.fanout_reason_count, 1);
    assert!(!output.diagnostics.graph_context_truncated);
    assert!(output
        .diagnostics
        .graph_context_truncation_reasons
        .is_empty());
    assert!(output.fanout_reasons[0]
        .contains("graph_context dense_adjacency MENTIONS outgoing node 0 degree"));
}
