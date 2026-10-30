// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{SearchMode, SearchQueryOptions, SearchResultSet, VectorSearchExecutionOptions};

fn document(id: usize, embedding: Option<Vec<f32>>) -> SearchDocument {
    SearchDocument {
        id: format!("{id:04}"),
        title: String::new(),
        content: String::new(),
        embedding,
        metadata: BTreeMap::from([(
            "group".into(),
            if id.is_multiple_of(2) { "even" } else { "odd" }.into(),
        )]),
    }
}

fn options() -> SearchQueryOptions {
    SearchQueryOptions {
        limit: 8,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::from([("group".into(), "even".into())]),
        policy_epoch: None,
    }
}

#[test]
fn oversized_default_resident_graph_falls_back_and_checkpoints_complete_source() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-resident-graph-over-cap-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut index = SearchIndex::open(&root).unwrap();
    let dimension = 16_384;
    let mut vector = vec![0.0; dimension];
    vector[0] = 1.0;
    for id in 0..1024 {
        let mut row = document(id, Some(vector.clone()));
        row.content = "oversized graph fallback".into();
        index.upsert(row).unwrap();
    }
    let (_, _, required) = Projection::memory_bound(&index.documents, dimension, None).unwrap();
    assert!(
        required > CACHE_BYTES,
        "fixture must exceed the real graph cap"
    );
    let assert_fallback = |result: SearchResultSet| {
        let report = &result.retrievers[0];
        assert_eq!(report.backend, "scalar_vector_scan");
        assert!(report
            .fallback_reason_codes
            .iter()
            .any(|code| code.as_str() == "diskann_cache_budget_exceeded"));
        assert_eq!(result.hits.len(), 8);
        assert!(result.hits.iter().all(|hit| {
            hit.id.parse::<usize>().unwrap().is_multiple_of(2) && hit.vector_score == 1.0
        }));
    };
    for mode in [SearchMode::Vector]
        .into_iter()
        .chain(cfg!(feature = "full-text-search").then_some(SearchMode::Hybrid))
    {
        assert_fallback(
            index
                .search_with_options("fallback", Some(&vector), mode, options())
                .unwrap(),
        );
        assert_fallback(
            index
                .try_search_with_options("fallback", Some(&vector), mode, options())
                .unwrap(),
        );
        #[cfg(feature = "acl")]
        assert_fallback(
            index
                .try_search_with_options_access_control(
                    "fallback",
                    Some(&vector),
                    mode,
                    options(),
                    crate::SearchAccessControlContext::visibility_scopes(1, "group", ["even"]),
                )
                .unwrap(),
        );
    }
    assert!(index.resident_diskann.projection.lock().unwrap().is_none());
    let recall = index.validate_sampled_vector_recall(crate::VectorRecallValidationOptions {
        max_samples: 1,
        top_k: 1,
        candidate_limit: 8,
        minimum_recall_per_million: 950_000,
        metadata_filters: options().metadata_filters,
    });
    assert!(!recall.ready);
    assert_eq!(recall.fallback_count, 1);
    assert!(!recall.validates_default_approximate_backend());
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);
    assert!(index
        .try_search_with_options_context(
            "",
            Some(&vector),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task),
        )
        .is_err());
    index.checkpoint().unwrap();
    drop(index);
    let mut index = SearchIndex::open(&root).unwrap();
    assert_eq!(index.document_count(), 1024);
    assert_fallback(
        index
            .try_search_with_options("", Some(&vector), SearchMode::Vector, options())
            .unwrap(),
    );
    index.delete("0000");
    index.upsert(document(2, None)).unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let index = SearchIndex::open(&root).unwrap();
    assert_eq!(index.document_count(), 1023);
    let actual = index
        .try_search_with_options("", Some(&vector), SearchMode::Vector, options())
        .unwrap();
    assert!(actual
        .hits
        .iter()
        .all(|hit| hit.id != "0000" && hit.id != "0002"));
    assert_fallback(actual);
    drop(index);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn dirty_resident_checkpoint_can_stream_while_a_large_previous_cache_is_pinned() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-resident-graph-pinned-cap-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut index = SearchIndex::open(&root).unwrap();
    // Long IDs make the actual retained ordinal map exceed half the cache,
    // without spending the regression's time constructing a large ANN graph.
    let suffix = "x".repeat(128 << 10);
    for id in 0..300 {
        let mut row = document(id, Some(vec![1.0, id as f32]));
        row.id.push_str(&suffix);
        index.upsert(row).unwrap();
    }
    let graph = index
        .resident_diskann
        .snapshot(&index, 2, Default::default())
        .unwrap();
    assert!(graph.memory_bytes > CACHE_BYTES / 2 && graph.memory_bytes < CACHE_BYTES);
    drop(graph);
    index.upsert(document(10, Some(vec![1.0, 0.0]))).unwrap();
    let retained = index.resident_diskann.capture();
    index.checkpoint().unwrap();
    assert!(retained.projection.lock().unwrap().as_ref().unwrap().dirty);
    drop(index);
    let index = SearchIndex::open(&root).unwrap();
    assert_eq!(index.document_count(), 301);
    assert!(index.documents.contains_key("0010"));
    drop(index);
    drop(retained);
    std::fs::remove_dir_all(root).unwrap();
}

fn query(index: &SearchIndex, exact: bool) -> SearchResultSet {
    if exact {
        index.search_with_options_exact("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
    } else {
        index.search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
    }
    .unwrap()
}

fn assert_default(index: &SearchIndex) {
    let exact = query(index, true);
    let actual = query(index, false);
    assert_eq!(
        actual
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.vector_score))
            .collect::<Vec<_>>(),
        exact
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.vector_score))
            .collect::<Vec<_>>()
    );
    let report = actual
        .retrievers
        .iter()
        .find(|report| report.name == "vector")
        .unwrap();
    assert_eq!(report.backend, "hawdb_diskann_candidate_projection");
    assert_eq!(report.candidate_score_source, "ann_approximate");
    assert_eq!(report.final_score_source, "raw_vector");
    assert_eq!(
        report.candidate_scan_kernel.as_deref(),
        Some(hawdb_core::vector::distance_kernel_name(
            hawdb_core::VectorDistance::Cosine,
            2
        ))
    );
    assert_eq!(exact.retrievers[0].backend, "scalar_vector_scan");
}

fn graph_address(index: &SearchIndex) -> usize {
    let state = index.resident_diskann.projection.lock().unwrap();
    let graph = &state.as_ref().unwrap().graph;
    Arc::as_ptr(graph) as usize
}

#[test]
fn default_graph_recall_probe_measures_the_served_backend() {
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32 / 10.0])))
            .unwrap();
    }
    let report = index.validate_sampled_vector_recall(crate::VectorRecallValidationOptions {
        max_samples: 4,
        top_k: 3,
        candidate_limit: 16,
        minimum_recall_per_million: 1_000_000,
        metadata_filters: BTreeMap::from([("group".into(), "even".into())]),
    });
    assert_eq!(
        report.approximate_backend,
        "hawdb_diskann_candidate_projection"
    );
    assert!(report.ready, "{:?}", report.blocker_codes);
    assert!(report.validates_default_approximate_backend());
    assert!(!report.validates_required_approximate_backend());
    assert_eq!(report.executed_sample_count, 4);
    assert_eq!(report.fallback_count, 0);
    assert_eq!(report.recall_at_k_per_million, 1_000_000);
    assert!(index.rabitq_projection.lock().unwrap().is_none());
    assert!(index.vector_projection_resource_evidence().is_none());
}

#[test]
fn default_graph_navigation_is_funded_independently_of_its_output_window() {
    let mut index = SearchIndex::in_memory();
    for id in 0..512 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32 / 1024.0])))
            .unwrap();
    }
    let options = SearchQueryOptions {
        limit: 10,
        rank_window: Some(128),
        metadata_filters: BTreeMap::new(),
        ..options()
    };
    let search = |bytes| {
        index.try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options.clone(),
            VectorSearchExecutionOptions::bounded(std::num::NonZeroUsize::MIN, bytes, None),
        )
    };
    let actual = search(CACHE_BYTES).unwrap();
    let report = &actual.retrievers[0];
    assert_eq!(report.generated_candidate_count, 128);
    assert_eq!(report.reranked_candidate_count, 128);
    assert_eq!(report.candidate_scan_scored_document_count, 512);
    assert_eq!(actual.hits.len(), 10);
    let original = graph_address(&index);
    let former_envelope = {
        let state = index.resident_diskann.projection.lock().unwrap();
        let projection = state.as_ref().unwrap();
        let retained =
            512 * size_of::<u64>() + 2 * 128 * (size_of::<VectorCandidate>() + "0000".len());
        // Funding the old 128-wide navigation must not admit a larger search
        // or silently shrink its beam to fit the obsolete memory envelope.
        projection.memory_bytes
            + retained
            + DiskAnnSearchOptions::default()
                .working_memory_bound(512, 128)
                .unwrap()
    };
    assert!(matches!(
        search(former_envelope),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_eq!(graph_address(&index), original);
    assert_eq!(search(CACHE_BYTES).unwrap().hits, actual.hits);
}

#[test]
fn default_resident_graph_reuses_immutable_baseline_across_crud_and_nulls() {
    let mut index = SearchIndex::in_memory();
    for id in 1..=32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32 / 10.0])))
            .unwrap();
    }
    assert_default(&index);
    let original = graph_address(&index);
    index.upsert(document(2, Some(vec![0.1, 1.0]))).unwrap();
    index.delete("0004");
    index.upsert(document(6, None)).unwrap();
    index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
    index.upsert(document(34, Some(vec![1.0, 0.0]))).unwrap();
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
    // Replacing or reinserting one original ID never makes its old vector live.
    index.upsert(document(2, Some(vec![1.0, 0.0]))).unwrap();
    index.upsert(document(4, Some(vec![1.0, 0.0]))).unwrap();
    index.upsert(document(0, None)).unwrap();
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
    for id in 0..=34 {
        index.delete(&format!("{id:04}"));
    }
    assert_default(&index);
    index.upsert(document(18, Some(vec![1.0, 0.0]))).unwrap();
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
}

#[test]
fn default_resident_graph_budget_refusal_and_cancel_keep_retryable_source() {
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32])))
            .unwrap();
    }
    let error = index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::bounded(std::num::NonZeroUsize::MIN, 1, None),
        )
        .unwrap_err();
    assert!(matches!(error, HawDBError::ResourceBudgetExceeded(_)));
    assert!(index.resident_diskann.projection.lock().unwrap().is_none());
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);
    assert!(index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task)
        )
        .is_err());
    assert!(index.resident_diskann.projection.lock().unwrap().is_none());
    assert_default(&index);
    let original = graph_address(&index);
    assert!(index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::bounded(std::num::NonZeroUsize::MIN, 1, None)
        )
        .is_err());
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
}

#[test]
fn default_resident_graph_retains_legacy_zero_and_signed_cosine_semantics() {
    let mut index = SearchIndex::in_memory();
    for (id, vector) in [
        vec![0.0, 0.0],
        vec![-1.0, 0.0],
        vec![1.0, 0.0],
        vec![1.0, 0.0],
    ]
    .into_iter()
    .enumerate()
    {
        index.upsert(document(id * 2, Some(vector))).unwrap();
    }
    assert_default(&index);
    assert_eq!(
        query(&index, false)
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        ["0004", "0006"]
    );
    let zero = index
        .search_with_options("", Some(&[0.0, 0.0]), SearchMode::Vector, options())
        .unwrap();
    assert!(zero.hits.is_empty());
    index.upsert(document(2, Some(vec![1.0, 0.0]))).unwrap();
    assert_default(&index);
    assert_eq!(query(&index, false).hits[0].id, "0002");
}

#[test]
fn default_resident_graph_refuses_non_finite_source_before_or_after_construction() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(document(0, Some(vec![f32::NAN, 1.0])))
        .unwrap();
    assert!(index
        .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
        .is_err());
    assert!(index.resident_diskann.projection.lock().unwrap().is_none());
    index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
    assert_default(&index);
    let original = graph_address(&index);
    index
        .upsert(document(0, Some(vec![f32::NAN, 1.0])))
        .unwrap();
    assert!(index
        .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
        .is_err());
    index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
}

#[test]
fn default_resident_graph_pagination_includes_the_complete_page_window() {
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32 / 10.0])))
            .unwrap();
    }
    for (offset, limit) in [(0, 4), (4, 4), (12, 4), (16, 4)] {
        let options = SearchQueryOptions {
            offset,
            limit,
            ..options()
        };
        let expected = index
            .search_with_options_exact("", Some(&[1.0, 0.0]), SearchMode::Vector, options.clone())
            .unwrap();
        let actual = index
            .try_search_with_options_context(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                options.clone(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.vector_score))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.vector_score))
                .collect::<Vec<_>>()
        );
        let exact_context = index
            .try_search_with_options_context(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                options,
                VectorSearchExecutionOptions::default().with_exact_search(),
            )
            .unwrap();
        assert_eq!(exact_context.retrievers[0].backend, "scalar_vector_scan");
        assert_eq!(exact_context.hits, expected.hits);
    }
    let original = graph_address(&index);
    let task = hawdb_core::RuntimeTaskContext::default().with_work_limit(1);
    assert!(matches!(
        index.try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task)
        ),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
}

#[test]
fn default_resident_graph_delta_is_validated_before_any_retraction() {
    let row = |id, embedding| {
        let document = document(id, embedding);
        crate::SearchProjectionRow {
            kind: crate::SearchProjectionKind::Memory,
            external_id: document.id,
            title: document.title,
            body: document.content,
            embedding: document.embedding,
            source_id: None,
            metadata: document.metadata,
        }
    };
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert_projection_row(row(id, Some(vec![1.0, id as f32])))
            .unwrap();
    }
    assert_default(&index);
    let original = graph_address(&index);
    let before = query(&index, false);
    assert!(index
        .apply_projection_delta(crate::SearchProjectionDelta {
            upserts: vec![row(32, Some(vec![1.0, 0.0, 0.0]))],
            deletes: vec!["memory:0000".into()],
            source_graph_commit_epoch: Some(2),
            ..Default::default()
        })
        .is_err());
    assert_eq!(before.hits, query(&index, false).hits);
    index
        .apply_projection_delta(crate::SearchProjectionDelta {
            upserts: vec![
                row(2, None),
                row(0, Some(vec![0.0, 1.0])),
                row(32, Some(vec![1.0, 0.0])),
            ],
            deletes: vec!["memory:0004".into()],
            source_graph_commit_epoch: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_default(&index);
    assert_eq!(graph_address(&index), original);
}

#[test]
fn default_resident_graph_reopens_a_mapped_checkpoint_without_changing_raw_scores() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-resident-diskann-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )));
    {
        let mut index = SearchIndex::open(&directory.0).unwrap();
        index
            .apply_embedding_manifest(crate::SearchEmbeddingManifest {
                model: "diskann-qualification".into(),
                version: Some("1".into()),
                dimension: 2,
            })
            .unwrap();
        for id in 0..32 {
            index
                .upsert(document(id, Some(vec![1.0, id as f32])))
                .unwrap();
        }
        index
            .apply_projection_delta(crate::SearchProjectionDelta {
                upserts: Vec::new(),
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(42),
            })
            .unwrap();
        assert_default(&index);
        index.checkpoint().unwrap();
    }
    {
        let mut index = SearchIndex::open(&directory.0).unwrap();
        let identity = index.vector_projection_qualification_identity().unwrap();
        assert_eq!(identity.algorithm, "diskann");
        assert_eq!(identity.format_version, 2);
        assert_eq!(identity.bit_width, 0);
        assert_eq!(identity.source_graph_commit_epoch, Some(42));
        assert_eq!(identity.document_count, 32);
        assert_eq!(
            identity.embedding_model.as_deref(),
            Some("diskann-qualification")
        );
        assert!(identity.file_backed);
        assert!(identity.payload_bytes > 0);
        let expected = crate::ProductionQualificationIdentity {
            source_revision: "test-revision".into(),
            rust_toolchain: "test-toolchain".into(),
            target_os: "linux".into(),
            target_arch: "x86_64".into(),
            enabled_features: vec!["vector-search".into()],
            durable_format_version: 1,
            schema_version: 1,
            configuration_digest: "test-config".into(),
            deployment_profile: "production-replica".into(),
            dataset_fingerprint: "test-dataset".into(),
            canonical_graph_commit_epoch: 42,
            policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
        };
        let qualification = index.qualify_sampled_vector_recall_for_production(
            crate::VectorRecallValidationOptions {
                max_samples: 4,
                top_k: 3,
                candidate_limit: 16,
                minimum_recall_per_million: 1_000_000,
                metadata_filters: BTreeMap::new(),
            },
            crate::ProductionEvidenceBinding {
                identity: expected.clone(),
                generated_at_unix_seconds: 1,
            },
            expected,
        );
        assert_eq!(qualification.projection_identity, identity);
        assert_eq!(qualification.blocker_codes, vec!["dataset_too_small"]);
        assert!(matches!(
            index
                .resident_diskann
                .projection
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .graph
                .as_ref(),
            Graph::File(_)
        ));
        let original = graph_address(&index);
        assert_default(&index);
        let below_build_envelope =
            DiskAnnIndex::build_memory_bound(32, 2, DiskAnnBuildConfig::default()).unwrap() - 1;
        let admitted = index
            .try_search_with_options_context(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                options(),
                VectorSearchExecutionOptions::bounded(
                    std::num::NonZeroUsize::MIN,
                    below_build_envelope,
                    None,
                ),
            )
            .unwrap();
        assert_eq!(admitted.hits, query(&index, false).hits);
        // A clean successor copies verified records without rebuilding the cache.
        index.checkpoint().unwrap();
        assert_eq!(graph_address(&index), original);
        index.upsert(document(0, None)).unwrap();
        assert!(index.vector_projection_qualification_identity().is_none());
        index.delete("0002");
        index.upsert(document(4, Some(vec![1.0, 0.0]))).unwrap();
        index.upsert(document(32, Some(vec![1.0, 0.0]))).unwrap();
        assert_default(&index);
        assert_eq!(graph_address(&index), original);
        index.checkpoint().unwrap();
    }
    assert_default(&SearchIndex::open(&directory.0).unwrap());
}

#[test]
fn default_resident_graph_retains_allocation_ownership_until_cache_drop() {
    use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    #[derive(Debug)]
    struct Controller(Arc<AtomicU64>);
    #[derive(Debug)]
    struct Permit {
        used: Arc<AtomicU64>,
        bytes: u64,
    }
    impl RuntimeMemoryPermit for Permit {
        fn bytes(&self) -> u64 {
            self.bytes
        }
    }
    impl Drop for Permit {
        fn drop(&mut self) {
            self.used.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
    impl RuntimeMemoryController for Controller {
        fn reserve(
            &self,
            bytes: u64,
            ceiling: u64,
        ) -> std::result::Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
            self.0
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                    used.checked_add(bytes).filter(|total| *total <= ceiling)
                })
                .map_err(|used| RuntimeMemoryError::AdmissionDenied {
                    requested_bytes: bytes,
                    available_bytes: ceiling.saturating_sub(used),
                    retryable: true,
                })?;
            Ok(Box::new(Permit {
                used: self.0.clone(),
                bytes,
            }))
        }
    }
    let used = Arc::new(AtomicU64::new(0));
    let task = hawdb_core::RuntimeTaskContext::default()
        .with_memory_controller(Arc::new(Controller(used.clone())))
        .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(
            CACHE_BYTES as u64,
            CACHE_BYTES as u64,
        ));
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32])))
            .unwrap();
    }
    index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task),
        )
        .unwrap();
    let retained = used.load(Ordering::SeqCst);
    assert!(retained > 0);
    assert!(index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(1, &task)
        )
        .is_err());
    assert_eq!(used.load(Ordering::SeqCst), retained);
    assert_default(&index);
    assert_eq!(used.load(Ordering::SeqCst), retained);
    drop(index);
    assert_eq!(used.load(Ordering::SeqCst), 0);
}

#[test]
#[cfg(feature = "full-text-search")]
fn default_resident_graph_keeps_the_navigation_window_for_hybrid_fusion() {
    let mut index = SearchIndex::in_memory();
    for (id, embedding) in [
        (0, vec![1.0, 0.0]),
        (2, vec![0.99, 0.1]),
        (4, vec![0.8, 0.6]),
    ] {
        let mut row = document(id, Some(embedding));
        if id == 4 {
            row.title = "graph".into();
            row.content = "graph".into();
        }
        index.upsert(row).unwrap();
    }
    let options = SearchQueryOptions {
        limit: 1,
        ..options()
    };
    let expected = index
        .search_with_options_exact(
            "graph",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            options.clone(),
        )
        .unwrap();
    let actual = index
        .search_with_options("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, options)
        .unwrap();
    assert_eq!(expected.hits[0].id, "0004");
    assert_eq!(actual.hits, expected.hits);
    assert_eq!(
        actual
            .retrievers
            .iter()
            .find(|report| report.name == "vector")
            .unwrap()
            .backend,
        "hawdb_diskann_candidate_projection"
    );
}

#[test]
fn resident_graph_queries_release_the_cache_lock_before_execution_admission() {
    use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;
    #[derive(Debug)]
    struct Permit(u64);
    impl RuntimeMemoryPermit for Permit {
        fn bytes(&self) -> u64 {
            self.0
        }
    }
    #[derive(Debug)]
    struct Controller {
        armed: AtomicBool,
        reached: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl RuntimeMemoryController for Controller {
        fn reserve(
            &self,
            bytes: u64,
            _: u64,
        ) -> std::result::Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
            if self.armed.load(Ordering::SeqCst) {
                self.reached.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| RuntimeMemoryError::Closed)?;
            }
            Ok(Box::new(Permit(bytes)))
        }
    }
    let (reached, entered) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let controller = Arc::new(Controller {
        armed: AtomicBool::new(false),
        reached,
        release: Mutex::new(resume),
    });
    let task = hawdb_core::RuntimeTaskContext::default().with_memory_controller(controller.clone());
    let mut index = SearchIndex::in_memory();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32])))
            .unwrap();
    }
    index
        .try_search_with_options_context(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options(),
            VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task),
        )
        .unwrap();
    controller.armed.store(true, Ordering::SeqCst);
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            index.try_search_with_options_context(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                options(),
                VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task),
            )
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let unlocked = index.resident_diskann.projection.try_lock().is_ok();
        // Always release the worker before asserting, including on regression.
        let second = unlocked.then(|| query(&index, false));
        release.send(()).unwrap();
        let first = worker.join().unwrap().unwrap();
        assert!(
            unlocked,
            "execution admission retained the global cache lock"
        );
        assert_eq!(first.hits, second.unwrap().hits);
    });
}

#[test]
fn resident_graph_wait_for_initialization_obeys_the_query_deadline() {
    use std::sync::mpsc;
    use std::time::Duration;
    let mut index = SearchIndex::in_memory();
    index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
    let guard = index.resident_diskann.projection.lock().unwrap();
    let (send, receive) = mpsc::channel();
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let task = hawdb_core::RuntimeTaskContext::with_timeout(Duration::from_millis(100));
            let result = index.try_search_with_options_context(
                "",
                Some(&[1.0, 0.0]),
                SearchMode::Vector,
                options(),
                VectorSearchExecutionOptions::admitted(CACHE_BYTES, &task),
            );
            send.send(result).unwrap();
        });
        let result = receive.recv_timeout(Duration::from_secs(2));
        drop(guard);
        worker.join().unwrap();
        assert!(result
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("deadline"));
    });
    assert_default(&index);
}

#[test]
fn default_checkpoint_persists_a_native_graph_and_retains_old_disk_readers() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-checkpoint-graph-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )));
    let mut index = SearchIndex::open(&directory.0).unwrap();
    for id in 0..32 {
        index
            .upsert(document(id, Some(vec![1.0, id as f32 / 10.0])))
            .unwrap();
    }
    let first = index.checkpoint_with_report().unwrap();
    let original = directory
        .0
        .join(crate::rabitq_artifact_file(first.projection_generation));
    let bytes = std::fs::read(&original).unwrap();
    assert_eq!(&bytes[..8], b"HDANN002");
    let pinned = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
    let old = pinned
        .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
        .unwrap();
    assert_eq!(
        old.result.retrievers[0].backend,
        "hawdb_diskann_out_of_core_candidate_graph"
    );
    assert_eq!(
        old.result.retrievers[0].candidate_scan_kernel.as_deref(),
        Some(hawdb_core::vector::distance_kernel_name(
            hawdb_core::VectorDistance::Cosine,
            2
        ))
    );
    index.upsert(document(0, None)).unwrap();
    index.delete("0002");
    index.upsert(document(32, Some(vec![1.0, 0.0]))).unwrap();
    index.checkpoint().unwrap();
    assert_eq!(std::fs::read(original).unwrap(), bytes);
    assert_eq!(
        old.result.hits,
        pinned
            .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
            .unwrap()
            .result
            .hits
    );
    let current = crate::SearchOutOfCoreReader::open(&directory.0).unwrap();
    let actual = current
        .search_with_options("", Some(&[1.0, 0.0]), SearchMode::Vector, options())
        .unwrap();
    assert_eq!(actual.result.hits[0].id, "0032");
    assert_eq!(
        actual.result.retrievers[0].backend,
        "hawdb_diskann_out_of_core_candidate_graph"
    );
    drop(current);
    drop(pinned);
    drop(index);
    assert_default(&SearchIndex::open(&directory.0).unwrap());
}

#[test]
fn resident_graph_corruption_falls_back_only_to_a_matching_source() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    for stale in [false, true] {
        for truncated in [false, true] {
            let directory = Directory(std::env::temp_dir().join(format!(
                "hawdb-resident-graph-fallback-{}",
                hawdb_core::generate_uuidv7().unwrap()
            )));
            let mut index = SearchIndex::open(&directory.0).unwrap();
            index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
            index.upsert(document(2, Some(vec![0.5, 0.5]))).unwrap();
            index.checkpoint().unwrap();
            let previous = directory.0.join(crate::rabitq_artifact_file(1));
            let previous_bytes = std::fs::read(&previous).unwrap();
            if stale {
                index.upsert(document(0, Some(vec![0.0, 1.0]))).unwrap();
            }
            index.checkpoint().unwrap();
            drop(index);
            let current = directory.0.join(crate::rabitq_artifact_file(2));
            let mut damaged = std::fs::read(&current).unwrap();
            if truncated {
                damaged.truncate(80);
            } else {
                damaged[200] ^= 1;
            }
            std::fs::write(&current, &damaged).unwrap();
            let index = SearchIndex::open(&directory.0).unwrap();
            {
                let cache = index.resident_diskann.projection.lock().unwrap();
                if stale {
                    assert!(cache.is_none());
                } else {
                    assert!(matches!(
                        cache.as_ref().unwrap().graph.as_ref(),
                        Graph::File(_)
                    ));
                }
            }
            assert_default(&index);
            assert_eq!(
                query(&index, false).hits[0].id,
                if stale { "0002" } else { "0000" }
            );
            assert_eq!(std::fs::read(&previous).unwrap(), previous_bytes);
            assert!(!current.exists());
            let quarantined = std::fs::read_dir(&directory.0)
                .unwrap()
                .flatten()
                .find(|entry| {
                    entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("search_rabitq.2.hawdb.corrupt.")
                })
                .unwrap();
            assert_eq!(std::fs::read(quarantined.path()).unwrap(), damaged);
        }
    }
}

#[test]
fn explicit_rabitq_checkpoint_choice_survives_reopen_and_replacement() {
    struct Directory(std::path::PathBuf);
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let directory = Directory(std::env::temp_dir().join(format!(
        "hawdb-explicit-rabitq-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )));
    let mut index = SearchIndex::open(&directory.0).unwrap();
    index.set_rabitq_projection_build_options(crate::RaBitQCandidateProjectionBuildOptions {
        bit_width: hawdb_vector_projection::RaBitQBitWidth::One,
        segment_rows: 7,
        transform_seed: 42,
        ..Default::default()
    });
    index.upsert(document(0, Some(vec![1.0, 0.0]))).unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let mut index = SearchIndex::open(&directory.0).unwrap();
    assert_eq!(index.rabitq_checkpoint, Some((1, 7, 42)));
    index.upsert(document(2, Some(vec![0.5, 0.5]))).unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let index = SearchIndex::open(&directory.0).unwrap();
    let projection = index.rabitq_projection().unwrap();
    assert_eq!(projection.manifest().bit_width, 1);
    assert_eq!(projection.manifest().requested_segment_rows, 7);
    assert_eq!(projection.manifest().transform_seed, 42);
}
