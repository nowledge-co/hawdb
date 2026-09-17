use super::*;

#[test]
fn knowledge_retrieval_diagnostics_expose_search_fallback_reasons() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Fallback retrieval', content: 'text leg survives vector fallback'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "fallback retrieval".to_string(),
                query_embedding: Some(vec![1.0, 0.0]),
                mode: SearchMode::Hybrid,
                limit: 10,
                offset: 0,
                rank_window: None,
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

    assert!(!output.search.hits.is_empty());
    assert!(output
        .search
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .search
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .search_fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector_report.available);
    assert!(vector_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(vector_report
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
    let text_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("expected text retriever report");
    assert!(text_report.fallback_reasons.is_empty());
    assert!(output.search.hits[0]
        .fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output.search.hits[0]
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorIndexEmpty));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_search_fallback_reasons() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Vector-only fallback', content: 'search fallback should explain empty retrieval'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "".to_string(),
                query_embedding: Some(vec![1.0, 0.0]),
                mode: SearchMode::Vector,
                limit: 10,
                offset: 0,
                rank_window: None,
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

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "index has no vector rows"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_missing_query_embedding() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Missing embedding query', content: 'vector retrieval should explain missing embedding'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "missing embedding query".to_string(),
                query_embedding: None,
                mode: SearchMode::Vector,
                limit: 10,
                offset: 0,
                rank_window: None,
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

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    let vector_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector_report.available);
    assert!(vector_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
}

#[test]
fn knowledge_retrieval_empty_reasons_include_empty_text_query() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Empty text query', content: 'text fallback should explain empty retrieval'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "".to_string(),
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
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert!(output.search.hits.is_empty());
    assert!(output.candidates.is_empty());
    assert!(output
        .diagnostics
        .search_fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchRetrieverNoHits));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    let text_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("expected text retriever report");
    assert!(!text_report.available);
    assert!(text_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
}
