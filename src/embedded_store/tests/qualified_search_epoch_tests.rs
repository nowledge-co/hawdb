// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A qualified out-of-core search open refuses a stale production
//! qualification with a typed, non-poisoning error, never `Storage` (#1932).

use super::*;

/// The production lexical qualification requires at least this many
/// documents in the projection.
const QUALIFIED_DOCUMENT_COUNT: usize = 100_000;

fn row(external_id: String) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        title: external_id.clone(),
        external_id,
        body: "qualified".to_string(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

/// Publishes `rows` at `source_graph_commit_epoch` and checkpoints them.
fn publish(search_path: &std::path::Path, rows: Vec<SearchProjectionRow>, source_epoch: u64) {
    let mut index = SearchIndex::open(search_path).unwrap();
    // A production qualification identity names the embedding model; rows
    // carry no vectors so the 100k-document checkpoint stays a few seconds.
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "test-embedding".to_string(),
            version: Some("v1".to_string()),
            dimension: 2,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: rows,
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(source_epoch),
        })
        .unwrap();
    index.checkpoint().unwrap();
}

fn passing_coverage() -> SearchLexicalFeasibilityCoverage {
    SearchLexicalFeasibilityCoverage {
        selective_identifier: true,
        cjk_text: true,
        common_term: true,
        no_hit: true,
        metadata_filter: true,
        acl_filter: true,
        hybrid_rrf: true,
        bounded_generation_update: true,
        bounded_rabitq_serving: true,
        incremental_upsert_delete: true,
        checkpoint_reopen: true,
        corrupt_artifact: true,
        stale_manifest: true,
        mixed_foreground_background: true,
        larger_than_memory: true,
    }
}

fn passing_metrics() -> SearchLexicalFeasibilityMetrics {
    SearchLexicalFeasibilityMetrics {
        canonical_dataset_bytes: 2_000_000_000,
        storage_memory_budget_bytes: 1_000_000_000,
        steady_resident_bytes: 900_000_000,
        peak_resident_bytes: 1_050_000_000,
        baseline_selective_text_p95_micros: 200,
        segmented_selective_text_p95_micros: 100,
        selective_posting_bytes_read: 4096,
        selective_candidate_postings_visited: 32,
        selective_matching_document_count: 16,
        process_memory_capabilities: hawdb_qos::ProcessMemoryCapabilities {
            resident_memory: true,
            total_page_faults: true,
            split_page_faults: true,
        },
        total_page_faults: Some(20),
        minor_page_faults: Some(20),
        major_page_faults: Some(0),
        metadata_sidecar_bytes_read: 2048,
        vector_sidecar_bytes_read: 4096,
        hydration_bytes: 1024,
        segmented_vector_p50_micros: 50,
        segmented_vector_p95_micros: 110,
        segmented_vector_p99_micros: 160,
        segmented_hybrid_p50_micros: 60,
        segmented_hybrid_p95_micros: 120,
        segmented_hybrid_p99_micros: 180,
        baseline_update_p95_micros: 100,
        segmented_update_p95_micros: 110,
        baseline_checkpoint_p95_micros: 1_000,
        segmented_checkpoint_p95_micros: 1_100,
        ..SearchLexicalFeasibilityMetrics::default()
    }
}

/// Qualifies the projection as it is now for a release whose canonical graph
/// is at `canonical_graph_commit_epoch`.
fn qualify(
    search_path: &std::path::Path,
    spill_directory: std::path::PathBuf,
    canonical_graph_commit_epoch: u64,
) -> QualifiedOutOfCoreSearchOptions {
    let projection_identity = OutOfCoreSearchProjection::open(search_path)
        .unwrap()
        .reader()
        .production_qualification_identity();
    let expected_identity = production_identity(canonical_graph_commit_epoch);
    let qualification = SearchLexicalProductionQualificationReport::evaluate_for_production(
        projection_identity,
        ProductionEvidenceBinding {
            identity: expected_identity.clone(),
            generated_at_unix_seconds: 1,
        },
        expected_identity.clone(),
        SearchTopKScoreParity {
            text: true,
            vector: true,
            hybrid: true,
        },
        passing_coverage(),
        passing_metrics(),
    );
    assert!(qualification.ready, "{:?}", qualification.blocker_codes);
    QualifiedOutOfCoreSearchOptions {
        config: SearchOutOfCoreConfig {
            spill_directory,
            ..SearchOutOfCoreConfig::default()
        },
        qualification,
        expected_identity,
    }
}

fn graph_epoch(handle: &EmbeddedStoreHandle) -> u64 {
    handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .commit_epoch()
        .unwrap()
}

fn qualified_options(
    graph_path: &std::path::Path,
    search_path: &std::path::Path,
    qualified: QualifiedOutOfCoreSearchOptions,
) -> EmbeddedStoreOpenOptions {
    EmbeddedStoreOpenOptions::with_qualified_out_of_core_search_projection(
        graph_path,
        search_path,
        GraphMode::ShadowReadOnly,
        qualified,
    )
}

#[test]
fn qualified_out_of_core_open_refuses_a_graph_written_after_qualification_without_poisoning() {
    let root = unique_embedded_store_test_dir("qualified_stale_graph_epoch");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let qualified_epoch = {
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'qualified'})").unwrap();
        db.commit_epoch().unwrap()
    };
    publish(
        &search_path,
        (0..QUALIFIED_DOCUMENT_COUNT)
            .map(|number| row(format!("memory-{number:06}")))
            .collect(),
        qualified_epoch,
    );
    let qualified = qualify(&search_path, root.join("spill"), qualified_epoch);
    let (handle, report) = EmbeddedStoreHandle::open_with_options(qualified_options(
        &graph_path,
        &search_path,
        qualified.clone(),
    ))
    .unwrap();
    assert!(report.search_production_qualification_bound);
    drop(handle);

    // The graph advances after qualification.
    let advanced_epoch = {
        let (writer, _) = EmbeddedStoreHandle::open_with_options(
            EmbeddedStoreOpenOptions::graph_only(&graph_path, GraphMode::WritableCutover),
        )
        .unwrap();
        writer
            .query_with_report("CREATE (:Memory {id: 'after-qualification'})")
            .unwrap();
        graph_epoch(&writer)
    };
    assert!(advanced_epoch > qualified_epoch);

    let expected = HawDBError::SearchProductionQualificationGraphEpochMismatch {
        expected_canonical_graph_commit_epoch: qualified_epoch,
        opened_graph_commit_epoch: advanced_epoch,
    };
    let message = format!(
        "storage error: qualified out-of-core search expected canonical graph commit epoch {qualified_epoch}, opened graph is at {advanced_epoch}"
    );
    let error = EmbeddedStore::open_with_options(qualified_options(
        &graph_path,
        &search_path,
        qualified.clone(),
    ))
    .unwrap_err();
    assert_eq!(error, expected);
    assert_eq!(error.to_string(), message);
    let error = EmbeddedStoreHandle::open_with_options(qualified_options(
        &graph_path,
        &search_path,
        qualified,
    ))
    .unwrap_err();
    assert_eq!(error, expected);
    assert!(!matches!(
        error,
        HawDBError::Storage(_) | HawDBError::StorageIntegrity(_)
    ));

    // The refusal left nothing behind: the graph reopens writable, unpoisoned
    // and at the advanced epoch.
    {
        let (writer, _) = EmbeddedStoreHandle::open_with_options(
            EmbeddedStoreOpenOptions::graph_only(&graph_path, GraphMode::WritableCutover),
        )
        .unwrap();
        assert!(!writer.storage_handle_poisoned().unwrap());
        assert_eq!(graph_epoch(&writer), advanced_epoch);
    }

    // Catch the projection up and qualify again: the open succeeds.
    publish(
        &search_path,
        vec![row("after-qualification".to_string())],
        advanced_epoch,
    );
    let requalified = qualify(&search_path, root.join("spill"), advanced_epoch);
    let (handle, report) = EmbeddedStoreHandle::open_with_options(qualified_options(
        &graph_path,
        &search_path,
        requalified,
    ))
    .unwrap();
    assert!(report.search_production_qualification_bound);
    assert!(!handle.storage_handle_poisoned().unwrap());
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn qualified_out_of_core_open_refuses_a_projection_advanced_after_qualification() {
    let root = unique_embedded_store_test_dir("qualified_stale_projection");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let epoch = {
        let mut db = Database::open(&graph_path).unwrap();
        db.query("CREATE (:Memory {id: 'qualified'})").unwrap();
        db.commit_epoch().unwrap()
    };
    publish(&search_path, vec![row("first".to_string())], epoch);
    let projection_identity = OutOfCoreSearchProjection::open(&search_path)
        .unwrap()
        .reader()
        .production_qualification_identity();
    let expected_identity = production_identity(epoch);
    let qualification = SearchLexicalProductionQualificationReport::evaluate_for_production(
        projection_identity,
        ProductionEvidenceBinding {
            identity: expected_identity.clone(),
            generated_at_unix_seconds: 1,
        },
        expected_identity.clone(),
        SearchTopKScoreParity {
            text: true,
            vector: true,
            hybrid: true,
        },
        passing_coverage(),
        passing_metrics(),
    );
    // A new generation published after qualification.
    publish(&search_path, vec![row("second".to_string())], epoch);

    let error = EmbeddedStore::open_with_options(qualified_options(
        &graph_path,
        &search_path,
        QualifiedOutOfCoreSearchOptions {
            config: SearchOutOfCoreConfig {
                spill_directory: root.join("spill"),
                ..SearchOutOfCoreConfig::default()
            },
            qualification,
            expected_identity,
        },
    ))
    .unwrap_err();
    assert_eq!(
        error,
        HawDBError::SearchProductionQualificationRefused {
            blocker_codes: vec![
                "dataset_too_small".to_string(),
                "projection_identity_mismatch".to_string(),
                "selective_work_not_posting_proportional".to_string(),
            ],
        }
    );
    assert_eq!(
        error.to_string(),
        "storage error: segmented lexical projection is not qualified for the current production release: dataset_too_small,projection_identity_mismatch,selective_work_not_posting_proportional"
    );
    let (writer, _) = EmbeddedStoreHandle::open_with_options(EmbeddedStoreOpenOptions::graph_only(
        &graph_path,
        GraphMode::WritableCutover,
    ))
    .unwrap();
    assert!(!writer.storage_handle_poisoned().unwrap());
    drop(writer);
    std::fs::remove_dir_all(root).unwrap();
}
