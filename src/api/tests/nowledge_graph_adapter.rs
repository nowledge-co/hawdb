use super::*;

#[test]
fn nowledge_graph_adapter_runs_parameterized_query_explain_and_transaction() {
    let mut db = Database::new();
    let mut adapter = NowledgeGraphAdapter::new(&mut db);
    let transaction = adapter
        .transaction(&[
            NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
                parameters: BTreeMap::from([
                    ("id".to_string(), Value::Int(1)),
                    (
                        "title".to_string(),
                        Value::String("Adapter memory".to_string()),
                    ),
                ]),
            },
            NowledgeGraphStatement {
                cypher: "CREATE (:Memory {id: $id, title: $title})".to_string(),
                parameters: BTreeMap::from([
                    ("id".to_string(), Value::Int(2)),
                    (
                        "title".to_string(),
                        Value::String("Second adapter memory".to_string()),
                    ),
                ]),
            },
        ])
        .unwrap();

    assert_eq!(transaction.statement_outputs.len(), 2);
    assert_eq!(transaction.commit_output.rows.len(), 2);

    let query = adapter
        .query(&NowledgeGraphStatement {
            cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        })
        .unwrap();
    assert_eq!(
        query.rows[0].get("title"),
        Some(&Value::String("Adapter memory".to_string()))
    );

    let explain = adapter
        .explain(&NowledgeGraphStatement {
            cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title".to_string(),
            parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
        })
        .unwrap();
    assert!(explain.plan.contains("NodeProjectionScanExec"));
    assert!(explain
        .trace
        .selected_plan
        .contains("NodeProjectionScanExec"));
    assert_eq!(
        explain.work_request,
        WorkRequest::foreground(WorkClass::Query, 1)
    );

    let hinted_work = adapter
        .query_work_request(&NowledgeGraphStatement {
            cypher: "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
                     system.estimated_operations = 64 MATCH (m:Memory) RETURN m.id AS id"
                .to_string(),
            parameters: BTreeMap::new(),
        })
        .unwrap();
    assert_eq!(
        hinted_work,
        WorkRequest::background(WorkClass::Analytics, 64)
    );
    let hinted_explain = adapter
        .explain(&NowledgeGraphStatement {
            cypher: "CYPHER system.work_priority = 'background' system.work_class = 'analytics' \
                     system.estimated_operations = 64 MATCH (m:Memory) RETURN m.id AS id"
                .to_string(),
            parameters: BTreeMap::new(),
        })
        .unwrap();
    assert_eq!(
        hinted_explain.work_request,
        WorkRequest::background(WorkClass::Analytics, 64)
    );
}

#[test]
fn nowledge_graph_adapter_transaction_rolls_back_on_parameter_error() {
    let mut db = Database::new();
    let error = {
        let mut adapter = NowledgeGraphAdapter::new(&mut db);
        adapter
            .transaction(&[
                NowledgeGraphStatement {
                    cypher: "CREATE (:Memory {id: $id, title: 'Buffered'})".to_string(),
                    parameters: BTreeMap::from([("id".to_string(), Value::Int(1))]),
                },
                NowledgeGraphStatement {
                    cypher: "CREATE (:Memory {id: $missing, title: 'Missing'})".to_string(),
                    parameters: BTreeMap::new(),
                },
            ])
            .unwrap_err()
    };

    assert!(error.to_string().contains("missing parameter"));
    let output = db
        .query("MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title")
        .unwrap();
    assert!(output.rows.is_empty());
}

#[test]
fn nowledge_graph_adapter_retrieves_knowledge_with_external_projection() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'root', title: 'Root retrieval', content: 'Adapter knowledge retrieval'})-[:MENTIONS]->(:Entity {id: 'entity_1', name: 'Skein'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let adapter = NowledgeGraphAdapter::new(&mut db);
    let output = adapter
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "adapter retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 4,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 2,
                graph_context_limit: 4,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 1);
    assert_eq!(output.projection_freshness.document_count, 2);
    assert_eq!(output.diagnostics.search_total_hits, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.id_space,
        "search_projection_document_id"
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.representation,
        "sorted_document_ids"
    );
    assert!(output.diagnostics.search_candidate_set.exact);
    assert_eq!(output.diagnostics.search_limit, 4);
    assert!(!output.diagnostics.search_truncated);
    assert!(output.diagnostics.search_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(
        output.diagnostics.search_fusion_weights,
        SearchFusionWeights::default()
    );
    assert_eq!(output.diagnostics.graph_seed_limit, 2);
    assert_eq!(
        output.diagnostics.graph_seed_input_candidate_set.id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .representation,
        "filtered_node_ids"
    );
    assert_eq!(
        output.diagnostics.graph_seed_candidate_set.representation,
        "ranked_node_ids"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .id_space,
        "canonical_graph_node_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_input_candidate_set
            .representation,
        "context_seed_node_ids"
    );
    assert_eq!(
        output.diagnostics.graph_context_candidate_set.id_space,
        "canonical_graph_relationship_id"
    );
    assert_eq!(
        output
            .diagnostics
            .graph_context_candidate_set
            .representation,
        "expanded_relationship_ids"
    );
    assert!(!output.diagnostics.graph_seed_truncated);
    assert!(output.diagnostics.graph_seed_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.graph_context_limit, 4);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert!(!output.diagnostics.graph_context_truncated);
    assert!(output
        .diagnostics
        .graph_context_truncation_reasons
        .is_empty());
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert_eq!(
        output.diagnostics.candidate_total_count,
        output.diagnostics.candidate_count
    );
    assert!(!output.diagnostics.candidate_truncated);
    assert!(output.diagnostics.candidate_truncation_reasons.is_empty());
    assert_eq!(output.diagnostics.graph_context_path_count, 2);
    assert_eq!(output.graph_context_paths.len(), 2);
    assert!(output
        .evidence
        .iter()
        .any(|evidence| evidence.canonical_node_id == Some(0)));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "memory:root"));
    assert!(output
        .graph_context_paths
        .iter()
        .any(|path| path.seed_hit_id == "Memory:root"));
}
