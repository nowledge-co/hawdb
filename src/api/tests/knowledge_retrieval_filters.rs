use super::*;
use crate::SearchEmptyReasonCode;

#[test]
fn knowledge_retrieval_applies_metadata_filters_to_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Filtered graph', content: 'metadata scoped retrieval', source_id: 'thread_2'})")
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
                    "thread_1".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_document_count, 2);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.search_total_hits, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set,
        output.search.candidate_set
    );
    assert_eq!(output.diagnostics.search_candidate_set.cardinality, 1);
    assert_eq!(
        output.diagnostics.search_candidate_set.filtered_out_count,
        1
    );
    assert_eq!(
        output.diagnostics.search_candidate_set.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 1);
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        1
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .cardinality,
        1
    );
    assert_eq!(output.diagnostics.graph_seed_candidate_set.cardinality, 1);
    assert_eq!(output.diagnostics.graph_context_path_count, 0);
    assert_eq!(output.diagnostics.fanout_reason_count, 0);
    assert_eq!(output.diagnostics.candidate_count, 1);
    assert!(output.diagnostics.empty_reasons.is_empty());
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    let text_report = output
        .search
        .retrievers
        .iter()
        .find(|report| report.name == "text")
        .expect("text retriever report");
    assert_eq!(text_report.candidate_count, 1);
    assert_eq!(output.evidence.len(), 1);
    assert_eq!(output.candidates.len(), 1);
    assert_eq!(output.candidates[0].id, "memory:mem_1");
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
    let graph_seed_report = output
        .retrievers
        .iter()
        .find(|report| report.name == "graph_seed")
        .expect("graph seed retriever report");
    assert_eq!(graph_seed_report.candidate_count, 1);
    assert_eq!(graph_seed_report.input_candidate_set.filtered_out_count, 1);
    assert_eq!(
        graph_seed_report.input_candidate_set.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(graph_seed_report.top_candidates[0].id, "Memory:mem_1");
}

#[test]
fn knowledge_retrieval_filter_preserves_stable_offset_limit_pages() {
    let mut db = Database::new();
    for id in ["page_a", "page_b", "page_c", "skip_other"] {
        let unit_type = if id == "skip_other" { "task" } else { "fact" };
        db.query(&format!(
            "CREATE (:Memory {{id: '{id}', title: 'Paged graph', content: 'paged scoped retrieval', unit_type: '{unit_type}'}})"
        ))
        .unwrap();
    }

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let request = KnowledgeRetrievalRequest {
        query_text: "paged scoped retrieval".to_string(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 1,
        offset: 1,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
        candidate_limit: None,
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 10,
        graph_context_limit: 0,
        graph_context_max_hops: 1,
    };
    let first_page = db.retrieve_knowledge(&search_index, &request).unwrap();
    let repeated_page = db.retrieve_knowledge(&search_index, &request).unwrap();

    assert_eq!(first_page.search.total_hits, 3);
    assert_eq!(first_page.search.limit, 1);
    assert_eq!(first_page.search.offset, 1);
    assert!(first_page.search.truncated);
    assert_eq!(first_page.search.hits.len(), 1);
    assert_eq!(
        first_page.search.hits[0].external_id.as_deref(),
        Some("page_b")
    );
    assert_eq!(first_page.search.hits, repeated_page.search.hits);
    assert_eq!(first_page.diagnostics.search_filtered_document_count, 3);
    assert_eq!(
        first_page
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        1
    );
    assert_eq!(
        first_page
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .residual_predicate_count,
        0
    );
    assert_eq!(
        first_page
            .diagnostics
            .search_candidate_set
            .filtered_out_count,
        1
    );

    let empty_page = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                offset: 10,
                ..request
            },
        )
        .unwrap();

    assert_eq!(empty_page.search.total_hits, 3);
    assert_eq!(empty_page.search.offset, 10);
    assert!(empty_page.search.hits.is_empty());
    assert_eq!(
        empty_page.search.empty_reason_codes,
        vec![SearchEmptyReasonCode::LimitExcludedAllHits]
    );
    assert_eq!(
        empty_page.search.empty_reasons,
        vec!["offset 10 limit 1 returned no hits from 3 matching hits".to_string()]
    );
}

#[test]
fn knowledge_retrieval_kind_filter_accepts_canonical_labels() {
    let mut db = Database::new();
    db.query(
        "CREATE (:Memory {id: 'mem_1', title: 'Filtered graph', content: 'kind scoped retrieval'})",
    )
    .unwrap();
    db.query("CREATE (:Entity {id: 'entity_1', name: 'Filtered graph', summary: 'kind scoped retrieval'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "kind scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("kind".to_string(), "Memory".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].kind.as_deref(), Some("memory"));
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_latest_and_history_filters_canonicalize_to_is_latest() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'latest_mem', title: 'Versioned graph', content: 'version scoped retrieval', is_latest: true})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'history_mem', title: 'Versioned graph', content: 'version scoped retrieval', is_latest: false})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let latest = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "version scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("latest".to_string(), "true".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(latest.search.total_hits, 1);
    assert_eq!(
        latest.search.hits[0].external_id.as_deref(),
        Some("latest_mem")
    );
    assert_eq!(
        latest.diagnostics.search_candidate_set.metadata_filters,
        BTreeMap::from([("latest".to_string(), "true".to_string())])
    );

    let history = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "version scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("history".to_string(), "true".to_string())]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(history.search.total_hits, 1);
    assert_eq!(
        history.search.hits[0].external_id.as_deref(),
        Some("history_mem")
    );
    assert_eq!(history.diagnostics.graph_seed_candidate_count, 1);
}

#[test]
fn knowledge_retrieval_date_alias_filters_lower_to_projection_ranges() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'before_event', title: 'Date graph', content: 'date scoped retrieval', event_start: '2026-06-30T10:00:00Z', event_end: '2026-06-30T11:00:00Z', created_at: '2026-06-15T00:00:00Z'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'overlap_event', title: 'Date graph', content: 'date scoped retrieval', event_start: '2026-07-01T12:00:00Z', event_end: '2026-07-01T13:00:00Z', created_at: '2026-06-20T00:00:00Z'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'after_event', title: 'Date graph', content: 'date scoped retrieval', event_start: '2026-07-03T10:00:00Z', event_end: '2026-07-03T11:00:00Z', created_at: '2026-08-01T00:00:00Z'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let event_window = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "date scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([
                    (
                        "event_date_from".to_string(),
                        "2026-07-01T00:00:00Z".to_string(),
                    ),
                    (
                        "event_date_to".to_string(),
                        "2026-07-02T00:00:00Z".to_string(),
                    ),
                ]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(event_window.search.total_hits, 1);
    assert_eq!(
        event_window.search.hits[0].external_id.as_deref(),
        Some("overlap_event")
    );
    assert_eq!(event_window.diagnostics.search_filtered_document_count, 1);

    let recorded_window = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "date scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([
                    ("recorded_date_from".to_string(), "2026-06-01".to_string()),
                    ("recorded_date_to".to_string(), "2026-06-30".to_string()),
                ]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(recorded_window.search.total_hits, 2);
    assert_eq!(
        recorded_window.diagnostics.search_filtered_document_count,
        2
    );
    assert!(recorded_window
        .search
        .hits
        .iter()
        .all(|hit| hit.external_id.as_deref() != Some("after_event")));
}

#[test]
fn knowledge_retrieval_temporal_context_alias_filters_are_descriptor_safe() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'current_mem', title: 'Temporal graph', content: 'temporal scoped retrieval', temporal_context: 'Current'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'archive_mem', title: 'Temporal graph', content: 'temporal scoped retrieval', temporal_context: 'Archived'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "temporal scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "temporal_contexts__in".to_string(),
                    r#"["current"]"#.to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(
        output.search.hits[0].external_id.as_deref(),
        Some("current_mem")
    );
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
}

#[test]
fn knowledge_retrieval_space_scope_alias_filters_match_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'default_mem', title: 'Space graph', content: 'space scoped retrieval'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'empty_space_mem', title: 'Space graph', content: 'space scoped retrieval', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'team_mem', title: 'Space graph', content: 'space scoped retrieval', space_id: 'team'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "space scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "spaces__in".to_string(),
                    r#"["Default"]"#.to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    let search_ids = output
        .search
        .hits
        .iter()
        .filter_map(|hit| hit.external_id.as_deref())
        .collect::<BTreeSet<_>>();
    let graph_seed_ids = output
        .graph_seeds
        .iter()
        .filter_map(|seed| seed.entity.external_id.as_deref())
        .collect::<BTreeSet<_>>();

    assert_eq!(output.search.total_hits, 2);
    assert_eq!(output.diagnostics.search_filtered_document_count, 2);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 2);
    assert_eq!(
        search_ids,
        BTreeSet::from(["default_mem", "empty_space_mem"])
    );
    assert_eq!(
        graph_seed_ids,
        BTreeSet::from(["default_mem", "empty_space_mem"])
    );
    assert!(!search_ids.contains("team_mem"));
    assert!(!graph_seed_ids.contains("team_mem"));
}

#[test]
fn knowledge_retrieval_label_filters_use_relationship_derived_projection_metadata() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'labeled_mem', title: 'Label graph', content: 'label scoped retrieval'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'unlabeled_mem', title: 'Label graph', content: 'label scoped retrieval'})")
        .unwrap();
    db.query(
        "CREATE (:Label {id: 'label_database', name: 'Database', canonical_name: 'database'})",
    )
    .unwrap();
    db.query("MATCH (m:Memory {id: 'labeled_mem'}), (l:Label {id: 'label_database'}) CREATE (m)-[:HAS_LABEL]->(l)")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "label scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "labels__in".to_string(),
                    r#"["Database"]"#.to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(
        output.search.hits[0].external_id.as_deref(),
        Some("labeled_mem")
    );
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("labeled_mem")
    );
    assert_eq!(
        search_index
            .document("memory:labeled_mem")
            .and_then(|document| document.metadata.get("labels"))
            .map(String::as_str),
        Some(r#"["database"]"#)
    );
}

#[test]
fn knowledge_retrieval_source_filter_uses_projection_fallbacks() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Thread scoped graph', content: 'source fallback retrieval', thread_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Thread scoped graph', content: 'source fallback retrieval', thread_id: 'thread_2'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "source fallback retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_source_filter_skips_empty_source_ids() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Thread scoped graph', content: 'empty source fallback retrieval', source_id: '', thread_id: 'thread_1'})")
            .unwrap();
    db.query("CREATE (:Memory {id: 'mem_2', title: 'Thread scoped graph', content: 'empty source fallback retrieval', source_id: '', thread_id: 'thread_2'})")
            .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "empty source fallback retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(output.search.hits[0].source_id.as_deref(), Some("thread_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("mem_1")
    );
}

#[test]
fn knowledge_retrieval_presence_filters_match_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'with_source', title: 'Presence graph', content: 'presence scoped retrieval', source_id: 'thread_1'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'without_source', title: 'Presence graph', content: 'presence scoped retrieval'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let exists = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "presence scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__exists".to_string(),
                    "true".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();
    let missing = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "presence scoped retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__missing".to_string(),
                    "true".to_string(),
                )]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(exists.search.total_hits, 1);
    assert_eq!(
        exists.search.hits[0].external_id.as_deref(),
        Some("with_source")
    );
    assert_eq!(exists.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(
        exists.graph_seeds[0].entity.external_id.as_deref(),
        Some("with_source")
    );

    assert_eq!(missing.search.total_hits, 1);
    assert_eq!(
        missing.search.hits[0].external_id.as_deref(),
        Some("without_source")
    );
    assert_eq!(missing.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(
        missing.graph_seeds[0].entity.external_id.as_deref(),
        Some("without_source")
    );
}

#[test]
fn knowledge_retrieval_metadata_filters_support_typed_in_and_not_in() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'fact_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'fact', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'task_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'task', lifecycle_state: 'active'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'deleted_1', title: 'Typed filter graph', content: 'typed predicate retrieval', unit_type: 'fact', lifecycle_state: 'deleted'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "typed predicate retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([
                    (
                        "unit_type__in".to_string(),
                        r#"["fact","learning"]"#.to_string(),
                    ),
                    (
                        "lifecycle_state__not_in".to_string(),
                        r#"["deleted","forgotten"]"#.to_string(),
                    ),
                ]),
                candidate_limit: None,
                candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
                graph_seed_limit: 10,
                graph_context_limit: 0,
                graph_context_max_hops: 1,
            },
        )
        .unwrap();

    assert_eq!(output.search.total_hits, 1);
    assert_eq!(output.diagnostics.search_filtered_document_count, 1);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 1);
    assert_eq!(output.graph_seeds.len(), 1);
    assert_eq!(output.search.hits[0].external_id.as_deref(), Some("fact_1"));
    assert_eq!(
        output.graph_seeds[0].entity.external_id.as_deref(),
        Some("fact_1")
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        2
    );
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .residual_predicate_count,
        0
    );
    assert!(output
        .diagnostics
        .graph_seed_input_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .is_none());
}

#[test]
fn malformed_typed_metadata_filter_fails_closed_for_search_and_graph_seeds() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 'mem_1', title: 'Typed filter graph', content: 'malformed predicate retrieval', lifecycle_state: 'active'})")
        .unwrap();

    let mut search_index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut search_index, SearchRebuildOptions::default())
        .unwrap();

    let output = db
        .retrieve_knowledge(
            &search_index,
            &KnowledgeRetrievalRequest {
                query_text: "malformed predicate retrieval".to_string(),
                query_embedding: None,
                mode: SearchMode::Text,
                limit: 10,
                offset: 0,
                rank_window: None,
                search_fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    "deleted,forgotten".to_string(),
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
    assert_eq!(output.diagnostics.search_filtered_document_count, 0);
    assert_eq!(output.diagnostics.graph_seed_candidate_count, 0);
    assert_eq!(output.diagnostics.graph_seed_returned_count, 0);
    assert_eq!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .filtered_out_count,
        1
    );
    assert!(
        output
            .diagnostics
            .search_candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(output
        .diagnostics
        .search_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .as_deref()
        .is_some_and(|error| error.contains("expected JSON string array")));
    assert!(
        output
            .diagnostics
            .graph_seed_input_candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(output
        .diagnostics
        .graph_seed_input_candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .as_deref()
        .is_some_and(|error| error.contains("expected JSON string array")));
    assert!(output.graph_seeds.is_empty());
}
