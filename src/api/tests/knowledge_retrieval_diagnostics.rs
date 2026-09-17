use super::*;

#[test]
fn knowledge_retrieval_diagnostics_explain_empty_metadata_scope() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_1'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "metadata scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "missing_thread".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 0);
    assert_eq!(output.graph_seeds.len(), 0);
    assert_eq!(output.candidates.len(), 0);
    assert_eq!(output.diagnostics.search_document_count, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 0);
    assert_eq!(output.diagnostics.search_total_hits, 0);
    assert_eq!(output.diagnostics.search_candidate_filtered_out_count, 1);
    assert_eq!(output.diagnostics.search_limit, 10);
    assert_eq!(output.diagnostics.rank_window, None);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 0);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 0);
    assert_eq!(output.diagnostics.graph_seed_limit, 10);
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.graph_context_node_count, 0);
    assert_eq!(output.diagnostics.graph_context_relationship_count, 0);
    assert_eq!(output.diagnostics.graph_context_limit, 0);
    assert_eq!(output.diagnostics.graph_context_max_hops, 1);
    assert_eq!(
        output.diagnostics.graph_context_fallback_reasons,
        vec!["graph context expansion disabled by limit 0".to_string()]
    );
    assert_eq!(
        output.diagnostics.graph_context_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphContextLimitZero]
    );
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.candidate_count, 0);
    assert_eq!(output.diagnostics.candidate_limit, None);
    assert!(output.diagnostics.warnings.is_empty());
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "metadata filters matched no search documents"));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchMetadataFilterEmpty));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedNoCandidates));
    assert!(output
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever returned no candidates"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    let search_disabled_by_limit = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph candidate".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 0,
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
    assert_eq!(search_disabled_by_limit.search.total_hits, 1);
    assert!(search_disabled_by_limit.search.hits.is_empty());
    assert!(search_disabled_by_limit.diagnostics.search_truncated);
    assert!(search_disabled_by_limit.candidates.is_empty());
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "limit 0 returned from 1 matching hits"));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::SearchLimitExcludedAllHits));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::GraphSeedLimitZero));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reason_codes
        .contains(&KnowledgeRetrievalEmptyReasonCode::NoCandidates));
    assert!(search_disabled_by_limit
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
    let disabled_graph_seed_report = search_disabled_by_limit
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert!(!disabled_graph_seed_report.available);
    assert_eq!(
        disabled_graph_seed_report.knowledge_fallback_reason_codes,
        vec![KnowledgeFallbackReasonCode::GraphSeedLimitZero]
    );
    assert!(disabled_graph_seed_report
        .fallback_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
}

#[test]
fn knowledge_retrieval_diagnostics_explain_disabled_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Graph candidate'})")
        .unwrap();
    let search_index = SearchIndex::in_memory();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "Graph candidate".to_string(),
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
    assert!(output.graph_seeds.is_empty());
    assert!(output.candidates.is_empty());
    assert_eq!(output.diagnostics.graph_seed_limit, 0);
    assert_eq!(output.diagnostics.candidate_count, 0);
    assert_eq!(output.diagnostics.candidate_total_count, 0);
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "search projection has no documents"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "graph seed retriever disabled by limit 0"));
    assert!(output
        .diagnostics
        .empty_reasons
        .iter()
        .any(|reason| reason == "retrieval produced no candidates"));
}

#[test]
fn knowledge_retrieval_diagnostics_report_projection_warnings() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Stale projection', content: 'projection warning retrieval'})")
            .unwrap();

    let path = unique_test_dir("knowledge_retrieval_projection_warnings");
    let mut search_index = SearchIndex::open(&path).unwrap();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    search_index
        .mark_full_reindex_needed("stale projection")
        .unwrap();
    search_index
        .mark_metadata_repair_needed("missing derived metadata")
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "projection warning".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert!(output.projection_freshness.full_reindex_needed);
    assert!(output.projection_freshness.metadata_repair_needed);
    assert!(!output.diagnostics.projection_stale);
    assert!(output.diagnostics.projection_full_reindex_needed);
    assert_eq!(
        output.diagnostics.projection_full_reindex_reasons,
        vec!["stale projection".to_string()]
    );
    assert!(output.diagnostics.projection_metadata_repair_needed);
    assert_eq!(
        output.diagnostics.projection_metadata_repair_reasons,
        vec!["missing derived metadata".to_string()]
    );
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection requires full reindex"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection full reindex reason: stale projection"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection metadata repair is needed"));
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning
            == "search projection metadata repair reason: missing derived metadata"));
}

#[test]
fn knowledge_retrieval_diagnostics_expose_stale_projection_flag() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Stale projection', content: 'projection warning retrieval'})",
    )
    .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    db.query(
        "CREATE (:Memory {id: 'mem_2', title: 'New graph row', content: 'newer than projection'})",
    )
    .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "projection warning".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 1);
    assert!(output.diagnostics.projection_stale);
    assert!(!output.diagnostics.projection_full_reindex_needed);
    assert!(!output.diagnostics.projection_metadata_repair_needed);
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection is older than graph snapshot"));
}

#[test]
fn knowledge_retrieval_diagnostics_report_stale_projection_epoch() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Fresh projection', content: 'projection epoch retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Newer graph', content: 'new graph data'})")
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "projection epoch".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.graph_commit_epoch, 2);
    assert_eq!(
        output.projection_freshness.source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.graph_commit_epoch, 2);
    assert_eq!(
        output.diagnostics.projection_source_graph_commit_epoch,
        Some(1)
    );
    assert_eq!(output.diagnostics.projection_commit_lag, 1);
    assert!(output
        .diagnostics
        .warnings
        .iter()
        .any(|warning| warning == "search projection is older than graph snapshot"));
}
