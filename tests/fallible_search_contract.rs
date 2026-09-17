use skein::*;
use std::collections::BTreeMap;
use std::fmt::Debug;

fn request(mode: SearchMode) -> KnowledgeRetrievalRequest {
    KnowledgeRetrievalRequest::nowledge_deep(
        "graph",
        Some(vec![1.0, 0.0]),
        mode,
        5,
        0,
        BTreeMap::new(),
    )
}

fn disabled_index() -> SearchIndex {
    let mut index = SearchIndex::in_memory();
    index.set_runtime_capabilities(
        RuntimeCapabilities::default()
            .with(RuntimeCapability::FullTextSearch, false)
            .with(RuntimeCapability::VectorSearch, false),
    );
    index
}

fn required(mode: SearchMode) -> RuntimeCapability {
    match mode {
        SearchMode::Text | SearchMode::Hybrid => RuntimeCapability::FullTextSearch,
        SearchMode::Vector => RuntimeCapability::VectorSearch,
    }
}

fn unavailable<T: Debug>(result: Result<T>, mode: SearchMode) {
    assert_eq!(
        result.unwrap_err(),
        SkeinError::CapabilityUnavailable {
            capability: required(mode)
        }
    );
}

#[test]
fn retrieval_facades_return_capability_errors_before_pipeline_admission() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_payload_bytes: Some(0),
        ..DatabaseConfig::default()
    });
    let index = disabled_index();
    let epoch = db.commit_epoch();
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        let request = request(mode);
        unavailable(db.retrieve_knowledge(&index, &request), mode);
        unavailable(
            db.begin_read_transaction()
                .retrieve_knowledge(&index, &request),
            mode,
        );
        unavailable(
            NowledgeGraphAdapter::new(&mut db).retrieve_knowledge(&index, &request),
            mode,
        );
    }
    assert_eq!(db.commit_epoch(), epoch);
}

#[test]
fn projection_candidate_readiness_and_shadow_methods_fail_closed() {
    let projection = NowledgeMemSearchProjection::from_index(disabled_index());
    let readiness =
        NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read();
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        let mut request = NowledgeMemSearchCandidateRequest::text("graph", 5);
        request.mode = mode;
        request.query_embedding = Some(vec![1.0, 0.0]);
        unavailable(projection.search_candidates(&request), mode);
        unavailable(projection.search_candidates_with_report(&request), mode);
        unavailable(
            projection.search_candidate_readiness(&request, &readiness),
            mode,
        );
        unavailable(
            projection.search_candidate_shadow_evidence(&request, ["memory:first"]),
            mode,
        );
        unavailable(
            projection.search_candidate_shadow_evidence_json(&request, ["memory:first"]),
            mode,
        );
    }
}

#[test]
fn embedded_store_and_handle_propagate_errors_and_release_admission() {
    let graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    let store = NowledgeMemEmbeddedStore::new(
        graph,
        Some(NowledgeMemSearchProjection::from_index(disabled_index())),
    );
    let readiness =
        NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read();
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        let mut candidate = NowledgeMemSearchCandidateRequest::text("graph", 5);
        candidate.mode = mode;
        candidate.query_embedding = Some(vec![1.0, 0.0]);
        unavailable(store.search_candidates(&candidate), mode);
        unavailable(
            store.search_candidate_readiness(&candidate, &readiness),
            mode,
        );
        unavailable(
            store.search_candidate_shadow_evidence_json(&candidate, ["memory:first"]),
            mode,
        );
        unavailable(store.retrieve_knowledge(&request(mode)), mode);
        unavailable(store.retrieve_knowledge_with_report(&request(mode)), mode);
    }
    let handle = NowledgeMemEmbeddedStoreHandle::new(store);
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        let mut candidate = NowledgeMemSearchCandidateRequest::text("graph", 5);
        candidate.mode = mode;
        candidate.query_embedding = Some(vec![1.0, 0.0]);
        unavailable(handle.search_candidates(&candidate), mode);
        unavailable(handle.search_candidates_with_report(&candidate), mode);
        unavailable(
            handle.search_candidate_readiness(&candidate, &readiness),
            mode,
        );
        unavailable(
            handle.search_candidate_shadow_evidence_json(&candidate, ["memory:first"]),
            mode,
        );
        unavailable(handle.retrieve_knowledge(&request(mode)), mode);
        unavailable(handle.retrieve_knowledge_with_report(&request(mode)), mode);
    }
    let resources = handle.runtime_governor_snapshot().unwrap();
    assert_eq!(resources.admissions, resources.completions);
    assert_eq!(resources.active_foreground_io_slots, 0);
    assert_eq!(resources.admitted_memory_bytes, 0);
}

#[cfg(feature = "full-text-search")]
fn fixture(config: DatabaseConfig) -> (Database, SearchIndex) {
    let mut db = Database::new_with_config(config);
    db.query("CREATE (:Memory {id: 'first', title: 'Graph', content: 'graph evidence', embedding: [1.0, 0.0]})").unwrap();
    let mut index = SearchIndex::in_memory();
    db.rebuild_search_projection(&mut index, SearchRebuildOptions::default())
        .unwrap();
    (db, index)
}

#[cfg(feature = "full-text-search")]
#[test]
fn retrieval_facades_propagate_pipeline_errors_after_successful_search() {
    let (mut db, index) = fixture(DatabaseConfig {
        max_read_result_payload_bytes: Some(1),
        ..DatabaseConfig::default()
    });
    let request = request(SearchMode::Text);
    assert_eq!(
        index
            .search("graph", None, SearchMode::Text, 5)
            .unwrap()
            .len(),
        1
    );
    let expected = db.try_retrieve_knowledge(&index, &request).unwrap_err();
    assert!(expected
        .to_string()
        .contains("max_read_result_payload_bytes"));
    assert_eq!(
        db.retrieve_knowledge(&index, &request).unwrap_err(),
        expected
    );
    assert_eq!(
        db.begin_read_transaction()
            .retrieve_knowledge(&index, &request)
            .unwrap_err(),
        expected
    );
    assert_eq!(
        NowledgeGraphAdapter::new(&mut db)
            .retrieve_knowledge(&index, &request)
            .unwrap_err(),
        expected
    );
}

#[cfg(feature = "full-text-search")]
#[test]
fn enabled_facades_preserve_complete_results_and_reports() {
    let (mut db, index) = fixture(DatabaseConfig::default());
    let readiness =
        NowledgeMemSearchCandidateReadinessOptions::lancedb_replacement_candidate_read();
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        if mode != SearchMode::Text && !cfg!(feature = "vector-search") {
            continue;
        }
        let request = request(mode);
        let expected = db.try_retrieve_knowledge(&index, &request).unwrap();
        assert_eq!(db.retrieve_knowledge(&index, &request).unwrap(), expected);
        assert_eq!(
            db.begin_read_transaction()
                .retrieve_knowledge(&index, &request)
                .unwrap(),
            expected
        );
        assert_eq!(
            NowledgeGraphAdapter::new(&mut db)
                .retrieve_knowledge(&index, &request)
                .unwrap(),
            expected
        );
    }
    let projection = NowledgeMemSearchProjection::from_index(index);
    let request = NowledgeMemSearchCandidateRequest::text("graph", 5);
    let expected = projection
        .try_search_candidates_with_report(&request)
        .unwrap();
    assert_eq!(
        projection.search_candidates_with_report(&request).unwrap(),
        expected
    );
    assert_eq!(
        projection.search_candidates(&request).unwrap(),
        expected.result
    );
    assert_eq!(
        projection
            .search_candidate_readiness(&request, &readiness)
            .unwrap(),
        expected.readiness_report(&readiness)
    );
    let ids: Vec<_> = expected
        .result
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect();
    let mut accumulator = NowledgeMemSearchCandidateShadowAccumulator::new();
    accumulator.record_search_candidate_output(ids.clone(), &expected);
    let evidence = accumulator.evidence();
    assert_eq!(
        projection
            .search_candidate_shadow_evidence(&request, ids.clone())
            .unwrap(),
        evidence
    );
    assert_eq!(
        projection
            .search_candidate_shadow_evidence_json(&request, ids)
            .unwrap(),
        evidence.json()
    );
}
