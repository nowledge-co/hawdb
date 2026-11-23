// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A missing search projection, including an incremental reader detached by a
//! failed re-attach, is a non-poisoning `Semantic` refusal, not `Storage`
//! damage (#1927).

use super::*;
use crate::{IncrementalSearchMaintenanceOptions, SearchProjectionCleanupOptions};

const NOT_CONFIGURED: &str = "nowledge mem search projection is not configured";

fn assert_not_configured(error: HawDBError) {
    assert!(
        matches!(&error, HawDBError::Semantic(message) if message == NOT_CONFIGURED),
        "{error:?}"
    );
}

fn retrieval_request() -> KnowledgeRetrievalRequest {
    KnowledgeRetrievalRequest {
        query_text: "missing projection".to_string(),
        query_embedding: None,
        mode: SearchMode::Text,
        limit: 4,
        offset: 0,
        rank_window: None,
        search_fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        candidate_limit: None,
        candidate_scoring: KnowledgeCandidateScoringPolicy::Max,
        graph_seed_limit: 4,
        graph_context_limit: 4,
        graph_context_max_hops: 1,
    }
}

/// Every public search read refuses with the same `Semantic` error, and the
/// handle keeps serving graph reads and writes afterwards.
fn assert_search_reads_not_configured(handle: &EmbeddedStoreHandle) {
    let request = SearchCandidateRequest::text("missing projection", 4);
    assert_not_configured(handle.search_candidates(&request).unwrap_err());
    assert_not_configured(handle.retrieve_knowledge(&retrieval_request()).unwrap_err());
    assert_not_configured(
        handle
            .search_projection_documents(&["m1".to_string()], 1)
            .unwrap_err(),
    );
    assert_not_configured(
        handle
            .validate_sampled_vector_recall(VectorRecallValidationOptions::default())
            .unwrap_err(),
    );
    assert!(!handle.storage_handle_poisoned().unwrap());
    handle
        .query_with_report("CREATE (:Memory {id: 'after', title: 'after'})")
        .unwrap();
    let rows = handle
        .query_with_report("MATCH (m:Memory {id: 'after'}) RETURN m.id AS id")
        .unwrap()
        .output
        .rows;
    assert_eq!(rows.len(), 1);
}

#[test]
fn search_reads_without_a_projection_are_semantic_and_do_not_poison() {
    let graph = EmbeddedGraph::from_database(Database::new(), GraphMode::WritableCutover);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, None)).unwrap();
    assert_search_reads_not_configured(&handle);
    assert_not_configured(
        handle
            .retry_search_projection_cleanup(SearchProjectionCleanupOptions::default())
            .unwrap_err(),
    );
    assert!(!handle.storage_handle_poisoned().unwrap());
}

#[test]
fn search_reads_after_a_failed_incremental_reattach_are_semantic_and_do_not_poison() {
    let (root, handle, _) = super::incremental_maintenance_fixture(
        "missing_projection_after_failed_reattach_1927",
        IncrementalSearchMaintenanceOptions::default(),
    );
    assert!(handle
        .search_candidates(&SearchCandidateRequest::text("bootstrap", 4))
        .is_ok());
    handle
        .query_with_report("CREATE (:Memory {id: 'm1', title: 'm1'})")
        .unwrap();
    // Remove the durable selector so the next refresh cannot attach the head;
    // the store detaches its incremental reader rather than serve a stale one.
    std::fs::remove_file(
        root.join("search")
            .join("search_projection.out_of_core.manifest.hawdb"),
    )
    .unwrap();
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 4, |_, _| {
            Ok(crate::SearchProjectionRelationalDelta::default())
        })
        .unwrap_err();
    assert_search_reads_not_configured(&handle);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
