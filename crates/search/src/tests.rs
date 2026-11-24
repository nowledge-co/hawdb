// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn search_roots_borrow_project_limits_and_keep_independent_domains() {
    use hawdb_storage::file_descriptors::{ProjectFileDescriptors, DEFAULT_MAX_OPEN_FILES};

    let sequence = QUARANTINE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let root = crate::test_temp_dir().join(format!(
        "hawdb-search-fd-projects-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir(&root).unwrap();
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&root.join("first"), 4, true).unwrap();
    let search_root = project.root().join("search");
    let first = SearchIndex::open(&search_root).unwrap();
    assert_eq!(first._project_files.as_ref().unwrap().metrics().limit, 4);
    let held = (0..4)
        .map(|number| File::create(project.root().join(format!("held-{number}"))).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(project.metrics().open, 4);
    assert!(matches!(
        SearchIndex::open(&search_root),
        Err(HawDBError::FileDescriptors(_))
    ));

    let second = SearchIndex::open(root.join("second")).unwrap();
    let second_files = second._project_files.as_ref().unwrap();
    assert_eq!(second_files.metrics().limit, DEFAULT_MAX_OPEN_FILES);
    assert_eq!(second_files.metrics().open, 0);
    assert_eq!(project.metrics().open, 4);
    drop(held);
    let retried = SearchIndex::open(&search_root).unwrap();
    assert_eq!(project.metrics().open, 0);
    drop(retried);
    drop(first);
    drop(second);
    drop(project);
    std::fs::remove_dir_all(root).unwrap();
}

mod feature_contract;
#[cfg(feature = "full-text-search")]
use hawdb_storage::scan::{FileSegmentRangeReader, SegmentReadExecutor, SegmentReadScheduler};
use std::cell::Cell;
#[cfg(feature = "full-text-search")]
use std::num::{NonZeroU64, NonZeroUsize};

/// A refused QoS background admission is a typed, non-poisoning refusal
/// (#1926), never a storage error.
#[track_caller]
fn assert_background_refusal(error: &HawDBError, requested: u64, available: u64, retryable: bool) {
    assert_eq!(
        error,
        &HawDBError::RuntimeAdmissionRefused(hawdb_core::RuntimeAdmissionRefusal {
            code: "background_task_saturated",
            requested,
            available,
            retryable,
            waited: None,
        }),
        "{error}"
    );
}

#[derive(Default)]
struct TestProjectionSource {
    nodes: Vec<NodeRecord>,
    commit_epoch: u64,
    business_labels: HashMap<NodeId, Vec<String>>,
    business_label_bulk_scans: Cell<usize>,
    business_label_point_lookups: Cell<usize>,
}

impl TestProjectionSource {
    fn in_memory() -> Self {
        Self::default()
    }

    fn create_node(
        &mut self,
        catalog: &mut Catalog,
        label: &str,
        properties: BTreeMap<String, Value>,
    ) -> Result<NodeId> {
        let id = NodeId(self.nodes.len() as u64);
        self.nodes.push(NodeRecord {
            id,
            labels: [catalog.get_or_create_label(label)].into_iter().collect(),
            properties,
        });
        self.commit_epoch = self.commit_epoch.saturating_add(1);
        Ok(id)
    }

    fn node(&self, id: NodeId) -> Option<&NodeRecord> {
        self.nodes.get(id.0 as usize)
    }

    #[cfg(feature = "full-text-search")]
    fn commit_epoch(&self) -> u64 {
        self.commit_epoch
    }
}

impl SearchProjectionSource for TestProjectionSource {
    fn source_graph_commit_epoch(&self) -> u64 {
        self.commit_epoch
    }

    fn estimated_projection_node_count(&self) -> usize {
        self.nodes.len()
    }

    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> Result<()>,
    ) -> Result<()> {
        for node in &self.nodes {
            visitor(node.clone())?;
        }
        Ok(())
    }

    fn projection_business_labels(
        &self,
        _catalog: &Catalog,
        node: &NodeRecord,
    ) -> Result<Vec<String>> {
        self.business_label_point_lookups
            .set(self.business_label_point_lookups.get().saturating_add(1));
        Ok(self
            .business_labels
            .get(&node.id)
            .cloned()
            .unwrap_or_default())
    }

    fn projection_business_labels_by_node(
        &self,
        _catalog: &Catalog,
    ) -> Result<HashMap<NodeId, Vec<String>>> {
        self.business_label_bulk_scans
            .set(self.business_label_bulk_scans.get().saturating_add(1));
        Ok(self.business_labels.clone())
    }
}

#[test]
fn cosine_similarity_preserves_edge_and_numeric_behavior() {
    assert_eq!(cosine_similarity(&[], &[]), None);
    assert_eq!(cosine_similarity(&[1.0], &[1.0, 2.0]), None);
    assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]), None);

    let identical = cosine_similarity(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0])
        .expect("identical non-zero vectors have a cosine score");
    assert!((identical - 1.0).abs() < 1e-6);

    let orthogonal = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0])
        .expect("orthogonal non-zero vectors have a cosine score");
    assert!(orthogonal.abs() < 1e-6);

    let observed = cosine_similarity(&[1.0, 2.0, 3.0, 4.0, 5.0], &[5.0, 4.0, 3.0, 2.0, 1.0])
        .expect("non-zero vectors have a cosine score");
    let expected = 35.0_f64 / 55.0_f64;
    assert!((observed - expected).abs() < 1e-6);
}

#[cfg(feature = "vector-search")]
fn force_quantized_policy() -> AdaptiveVectorBackendPolicy {
    AdaptiveVectorBackendPolicy {
        flat_scan_max_documents: 0,
        high_filter_selectivity_per_million: u32::MAX,
        flat_scan_memory_budget_bytes: 0,
    }
}

#[test]
fn final_vector_similarity_is_positive_only_even_for_opposite_vectors() {
    assert_eq!(cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]), Some(0.0));
    assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), Some(0.0));
    assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), Some(1.0));
}

#[test]
#[cfg(feature = "vector-search")]
fn facade_vector_hits_exclude_nonpositive_scores_for_both_backends() {
    let path = unique_test_dir("positive-vector-contract");
    let mut index = SearchIndex::open(&path).unwrap();
    for (id, vector) in [
        ("positive", [1.0, 0.0]),
        ("zero", [0.0, 1.0]),
        ("negative", [-1.0, 0.0]),
    ] {
        index.upsert(doc(id, "vector", "contract", vector)).unwrap();
    }
    index.checkpoint().unwrap();
    let query = [1.0, 0.0];
    let options = SearchQueryOptions {
        limit: 3,
        offset: 0,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let resident = index
        .search_with_options("", Some(&query), SearchMode::Vector, options.clone())
        .unwrap();
    assert_eq!(
        resident
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["positive"]
    );
    let out_of_core = SearchOutOfCoreReader::open(&path).unwrap();
    let persisted = out_of_core
        .search_with_options("", Some(&query), SearchMode::Vector, options)
        .unwrap();
    assert_eq!(
        persisted
            .result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["positive"]
    );
    drop(out_of_core);
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn memory_projection_materializes_only_registered_nested_metadata_paths() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    let node_id = store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String("memory-metadata".to_string())),
                    (
                        "metadata".to_string(),
                        Value::String(
                            r#"{"topic":"Graph","customer":{"tier":"Enterprise"},"ignored":{"deep":"value"}}"#
                                .to_string(),
                        ),
                    ),
                ]),
            )
            .unwrap();
    let row = projection_row_from_node(&catalog, store.node(node_id).unwrap()).unwrap();

    assert_eq!(
        row.metadata.get("metadata.topic"),
        Some(&"Graph".to_string())
    );
    assert_eq!(
        row.metadata.get("metadata.customer.tier"),
        Some(&"Enterprise".to_string())
    );
    assert!(!row.metadata.contains_key("metadata.ignored.deep"));
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn hybrid_search_combines_vector_and_text() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap();
    index
        .upsert(doc("b", "Embedding model", "Vector search", [0.0, 1.0]))
        .unwrap();

    let hits = index
        .search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10)
        .unwrap();

    assert_eq!(hits[0].id, "a");
    assert!(hits[0].vector_score > 0.99);
    assert!(hits[0].text_score > 0.0);
    assert_eq!(hits[0].vector_rank, Some(1));
    assert_eq!(hits[0].text_rank, Some(1));
    assert_eq!(hits[0].score, hits[0].rrf_score);
    assert!(hits[0].rrf_score > 0.0);
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn hybrid_search_uses_rrf_child_ranks() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "both",
            "Graph retrieval",
            "Hybrid ranking evidence",
            [1.0, 0.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "vector_only",
            "Embedding",
            "Vector candidate",
            [0.9, 0.1],
        ))
        .unwrap();
    index
        .upsert(doc(
            "text_only",
            "Graph retrieval",
            "Graph graph",
            [0.0, 1.0],
        ))
        .unwrap();

    let hits = index
        .search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10)
        .unwrap();

    assert_eq!(hits[0].id, "both");
    assert_eq!(hits[0].vector_rank, Some(1));
    assert_eq!(hits[0].text_rank, Some(2));
    assert!(hits[0].rrf_score > hits[1].rrf_score);
    assert!(hits.iter().any(|hit| hit.vector_rank.is_some()));
    assert!(hits.iter().any(|hit| hit.text_rank.is_some()));
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn search_report_exposes_child_retriever_summary() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("both", "Graph retrieval", "Hybrid", [1.0, 0.0]))
        .unwrap();
    index
        .upsert(doc("vector_only", "Embedding", "Vector", [0.9, 0.1]))
        .unwrap();
    index
        .upsert(doc(
            "text_only",
            "Graph retrieval",
            "Graph graph",
            [0.0, 1.0],
        ))
        .unwrap();

    let result = index
        .search_with_report("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 2)
        .unwrap();

    let vector = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .expect("expected vector retriever report");
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert!(vector.available);
    assert!(text.available);
    assert_eq!(vector.backend, "hawdb_diskann_candidate_projection");
    assert_eq!(text.backend, "bm25_text");
    assert_eq!(vector.candidate_score_source, "ann_approximate");
    assert_eq!(vector.final_score_source, "raw_vector");
    assert_eq!(vector.generated_candidate_count, 2);
    assert_eq!(vector.candidate_scan_rounds, 1);
    assert_eq!(vector.descriptor_pruned_count, 0);
    assert_eq!(vector.scalar_filtered_count, 0);
    assert_eq!(vector.reranked_candidate_count, 2);
    assert_eq!(vector.raw_vector_bytes_read, 40);
    assert_eq!(vector.index_covered_document_count, 3);
    assert_eq!(vector.index_candidate_document_count, 3);
    assert!(vector.index_coverage_complete);
    assert_eq!(vector.input_candidate_set, result.candidate_set);
    assert_eq!(text.input_candidate_set, result.candidate_set);
    assert_eq!(
        vector.input_candidate_set.representation,
        "sorted_document_ids"
    );
    assert_eq!(vector.input_candidate_set.cardinality, 3);
    assert_eq!(vector.input_candidate_set.filtered_out_count, 0);
    assert_eq!(vector.candidate_count, 2);
    assert_eq!(text.candidate_count, 2);
    assert_eq!(
        vector.candidate_set.id_space,
        "search_projection_document_id"
    );
    assert_eq!(vector.candidate_set.representation, "ranked_document_ids");
    assert_eq!(vector.candidate_set.cardinality, 2);
    assert!(!vector.candidate_set.exact);
    assert_eq!(vector.candidate_set.policy_epoch, None);
    assert_eq!(
        vector.candidate_set.snapshot_source_graph_commit_epoch,
        None
    );
    assert_eq!(
        text.candidate_set,
        SearchRetrieverCandidateSetReport {
            exact: true,
            ..vector.candidate_set.clone()
        }
    );
    assert_eq!(vector.top_hit_ids[0], "both");
    assert_eq!(vector.top_candidates[0].id, "both");
    assert_eq!(vector.top_candidates[0].rank, 1);
    assert!(vector.top_candidates[0].score > 0.99);
    assert_eq!(text.top_hit_ids.len(), 2);
    assert_eq!(text.top_candidates.len(), 2);
    assert_eq!(text.top_candidates[0].rank, 1);
    assert!(text.top_candidates[0].score >= text.top_candidates[1].score);
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn hybrid_search_rank_window_limits_rrf_candidates() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "top_vector",
            "Vector only",
            "Embedding candidate",
            [1.0, 0.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "top_text",
            "Graph graph",
            "Text only graph",
            [0.0, 1.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "second_text",
            "Graph",
            "Secondary graph evidence",
            [0.0, 0.9],
        ))
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: Some(1),
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.rank_window, Some(1));
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert_eq!(text.candidate_count, 2);
    assert_eq!(text.candidate_set.cardinality, 1);
    assert_eq!(text.top_candidates.len(), 1);
    assert_eq!(text.top_candidates[0].rank, 1);
    assert!(result
        .hits
        .iter()
        .all(|hit| hit.id != "second_text" || hit.text_rank.is_none()));
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn rank_window_uses_prefiltered_candidate_set() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:hidden_vector".to_string(),
            title: "Hidden vector".to_string(),
            content: "hidden graph".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("scope".to_string(), "hidden".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:visible_vector".to_string(),
            title: "Visible vector".to_string(),
            content: "visible graph".to_string(),
            embedding: Some(vec![0.9, 0.0]),
            metadata: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:visible_text".to_string(),
            title: "Visible graph".to_string(),
            content: "visible graph graph".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "visible graph",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: Some(1),
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("scope".to_string(), "visible".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.candidate_set.cardinality, 2);
    assert_eq!(result.candidate_set.filtered_out_count, 1);
    let vector = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .expect("expected vector retriever report");
    assert_eq!(vector.input_candidate_set.cardinality, 2);
    assert_eq!(vector.input_candidate_set.filtered_out_count, 1);
    assert_eq!(
        vector.input_candidate_set.metadata_filters,
        BTreeMap::from([("scope".to_string(), "visible".to_string())])
    );
    assert!(result
        .hits
        .iter()
        .all(|hit| hit.id != "memory:hidden_vector"));
    let visible_vector = result
        .hits
        .iter()
        .find(|hit| hit.id == "memory:visible_vector")
        .expect("visible vector should remain eligible");
    assert_eq!(visible_vector.vector_rank, Some(1));
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn hybrid_search_applies_child_fusion_weights() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "vector_top",
            "Vector only",
            "semantic evidence",
            [1.0, 0.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "text_top",
            "Graph retrieval",
            "graph retrieval graph retrieval",
            [0.0, 1.0],
        ))
        .unwrap();

    let text_weighted = index
        .search_with_options(
            "graph retrieval",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights {
                    vector_weight: 1.0,
                    text_weight: 3.0,
                },
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    let vector_weighted = index
        .search_with_options(
            "graph retrieval",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights {
                    vector_weight: 3.0,
                    text_weight: 1.0,
                },
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(
        text_weighted.fusion_weights,
        SearchFusionWeights {
            vector_weight: 1.0,
            text_weight: 3.0
        }
    );
    assert_eq!(text_weighted.hits[0].id, "text_top");
    assert!(text_weighted.hits[0].text_rrf_score > 0.0);
    assert_eq!(text_weighted.hits[0].vector_rrf_score, 0.0);
    assert_eq!(vector_weighted.hits[0].id, "vector_top");
    assert!(vector_weighted.hits[0].vector_rrf_score > 0.0);
    assert_eq!(vector_weighted.hits[0].text_rrf_score, 0.0);
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_with_options_applies_metadata_filters_before_ranking() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:thread_1".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("source_id".to_string(), "thread_1".to_string()),
            ]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:thread_0".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("source_id".to_string(), "thread_2".to_string()),
            ]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:thread_2".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("source_id".to_string(), "thread_2".to_string()),
            ]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.document_count, 3);
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(
        result.candidate_set.id_space,
        "search_projection_document_id"
    );
    assert_eq!(result.candidate_set.representation, "sorted_document_ids");
    assert_eq!(result.candidate_set.cardinality, 1);
    assert!(result.candidate_set.exact);
    assert_eq!(
        result.candidate_set.snapshot_source_graph_commit_epoch,
        None
    );
    assert_eq!(result.candidate_set.policy_epoch, None);
    assert_eq!(result.candidate_set.filtered_out_count, 2);
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        1
    );
    assert_eq!(
        result.candidate_set.metadata_filters,
        BTreeMap::from([("source_id".to_string(), "thread_1".to_string())])
    );
    assert_eq!(result.hits[0].id, "memory:thread_1");
    assert_eq!(result.hits[0].source_id.as_deref(), Some("thread_1"));
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert_eq!(text.candidate_count, 1);
    assert_eq!(text.candidate_set.cardinality, 1);
    assert_eq!(text.top_hit_ids, vec!["memory:thread_1".to_string()]);
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_metadata_filters_match_casefolded_values() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:acme".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("customer".to_string(), " Acme ".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:other".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("customer".to_string(), "Other".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("customer".to_string(), "acme".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "memory:acme");
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(result.candidate_set.filtered_out_count, 1);
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_with_options_applies_metadata_in_filters_before_ranking() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:fact".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:task".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("unit_type".to_string(), "task".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "unit_type__in".to_string(),
                    r#"["fact","learning"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(result.hits[0].id, "memory:fact");
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .residual_predicate_count,
        0
    );
    assert!(
        !result
            .candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(result
        .candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .is_none());
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "unit_type".to_string(),
            value_kind: "enum".to_string(),
            operation_kinds: vec!["in".to_string()],
            segment_count: 1,
            pruned_segment_count: 0,
            scanned_segment_count: 1,
            numeric_range_summary_used: false,
            timestamp_range_summary_used: false,
            value_summary_used: true,
        }]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_with_options_applies_metadata_not_in_filters_before_ranking() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:active".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("lifecycle_state".to_string(), "active".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:deleted".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("lifecycle_state".to_string(), "deleted".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:legacy".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();
    let hit_ids = result
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(result.filtered_document_count, 2);
    assert!(hit_ids.contains("memory:active"));
    assert!(hit_ids.contains("memory:legacy"));
    assert!(!hit_ids.contains("memory:deleted"));
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries[0]
            .value_kind,
        "enum"
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_with_options_applies_metadata_exists_and_missing_filters_before_ranking() {
    let mut index = SearchIndex::in_memory();
    for (id, source_id) in [
        ("memory:has_source", Some("thread_1")),
        ("memory:missing_source_1", None),
        ("memory:missing_source_2", None),
    ] {
        let mut metadata = BTreeMap::from([("kind".to_string(), "memory".to_string())]);
        if let Some(source_id) = source_id {
            metadata.insert("source_id".to_string(), source_id.to_string());
        }
        index
            .upsert(SearchDocument {
                id: id.to_string(),
                title: "Graph memory".to_string(),
                content: "presence predicate retrieval".to_string(),
                embedding: None,
                metadata,
            })
            .unwrap();
    }

    let exists = index
        .search_with_options(
            "presence predicate retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__exists".to_string(),
                    "true".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();
    let missing = index
        .search_with_options(
            "presence predicate retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id__missing".to_string(),
                    "true".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(exists.total_hits, 1);
    assert_eq!(exists.hits[0].id, "memory:has_source");
    assert_eq!(exists.filtered_document_count, 1);
    assert_eq!(
        exists
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "source_id".to_string(),
            value_kind: "presence".to_string(),
            operation_kinds: vec!["exists".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: false,
            timestamp_range_summary_used: false,
            value_summary_used: true,
        }]
    );

    let missing_ids = missing
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(missing.total_hits, 2);
    assert!(missing_ids.contains("memory:missing_source_1"));
    assert!(missing_ids.contains("memory:missing_source_2"));
    assert!(!missing_ids.contains("memory:has_source"));
    assert_eq!(
        missing
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries[0]
            .operation_kinds,
        vec!["missing".to_string()]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_with_options_applies_numeric_range_filters_before_ranking() {
    let mut index = SearchIndex::in_memory();
    for (id, created_at) in [
        ("memory:0_old_0", "1"),
        ("memory:0_old_1", "2"),
        ("memory:1_new_0", "10"),
    ] {
        index
            .upsert(SearchDocument {
                id: id.to_string(),
                title: "Graph memory".to_string(),
                content: "graph projection diagnostics".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("created_at".to_string(), created_at.to_string())]),
            })
            .unwrap();
    }

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("created_at__gt".to_string(), "5".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(result.hits[0].id, "memory:1_new_0");
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_pruning_candidate_document_count,
        3
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_pruned_document_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_scanned_document_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "created_at".to_string(),
            value_kind: "numeric_or_string".to_string(),
            operation_kinds: vec!["gt".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: true,
            timestamp_range_summary_used: false,
            value_summary_used: false,
        }]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn malformed_numeric_range_filter_fails_closed() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:active".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("created_at".to_string(), "10".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "not-a-number".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 0);
    assert_eq!(result.filtered_document_count, 0);
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        0
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn malformed_metadata_list_filter_fails_closed() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:active".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("lifecycle_state".to_string(), "active".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    "deleted,forgotten".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 0);
    assert_eq!(result.filtered_document_count, 0);
    assert_eq!(result.candidate_set.filtered_out_count, 1);
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .input_predicate_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pushed_predicate_count,
        0
    );
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .unsatisfiable
    );
    assert!(result
        .candidate_set
        .metadata_predicate_pushdown
        .parse_error
        .as_deref()
        .is_some_and(|error| error.contains("expected JSON string array")));
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        0
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_policy_epoch_when_supplied() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:thread_1".to_string(),
            title: "Policy scoped graph".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: Some(42),
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.candidate_set.policy_epoch, Some(42));
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert_eq!(text.candidate_set.policy_epoch, Some(42));
    assert_eq!(
        index
            .search_with_report("graph", None, SearchMode::Text, 10)
            .unwrap()
            .candidate_set
            .policy_epoch,
        None
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_kind_metadata_filter_accepts_canonical_labels() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Kind scoped graph".to_string(),
            content: "projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "memory".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "entity:entity_1".to_string(),
            title: "Kind scoped graph".to_string(),
            content: "projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "entity".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "projection diagnostics",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("kind".to_string(), "Memory".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(result.hits[0].id, "memory:mem_1");
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_kind_metadata_filter_accepts_source_chunk_variants() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "source_chunk:chunk_1".to_string(),
            title: "Chunk scoped graph".to_string(),
            content: "projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "source_chunk".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "projection diagnostics",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("kind".to_string(), "SourceChunk".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "source_chunk:chunk_1");
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_metadata_filters_normalize_default_space() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:missing_space".to_string(),
            title: "Scoped graph".to_string(),
            content: "default space retrieval".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:empty_space".to_string(),
            title: "Scoped graph".to_string(),
            content: "default space retrieval".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), String::new())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:team_space".to_string(),
            title: "Scoped graph".to_string(),
            content: "default space retrieval".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "default space retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    DEFAULT_SPACE_ID.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();
    let hit_ids = result
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect::<BTreeSet<_>>();

    assert_eq!(result.filtered_document_count, 2);
    assert!(hit_ids.contains("memory:missing_space"));
    assert!(hit_ids.contains("memory:empty_space"));
    assert!(!hit_ids.contains("memory:team_space"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_metadata_filter_empty_scope() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:thread_1".to_string(),
            title: "Graph memory".to_string(),
            content: "graph projection diagnostics".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "missing_thread".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.document_count, 1);
    assert_eq!(result.filtered_document_count, 0);
    assert_eq!(result.total_hits, 0);
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert_eq!(text.candidate_count, 0);
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn vector_dimension_mismatch_degrades_to_text() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap();

    let result = index
        .search_with_report("graph", Some(&[1.0, 0.0, 0.0]), SearchMode::Hybrid, 10)
        .unwrap();
    let hits = &result.hits;

    assert_eq!(hits[0].id, "a");
    assert_eq!(hits[0].vector_score, 0.0);
    assert!(hits[0].text_score > 0.0);
    assert_eq!(hits[0].vector_rank, None);
    assert_eq!(hits[0].text_rank, Some(1));
    assert!(result.fallback_reasons[0].contains("dimension"));
    assert!(result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
    assert!(hits[0].fallback_reasons[0].contains("dimension"));
    assert!(hits[0]
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
    let vector = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector.available);
    assert!(vector.fallback_reasons[0].contains("dimension"));
    assert!(vector
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert!(text.fallback_reason_codes.is_empty());
    assert!(text.fallback_reasons.is_empty());
}

#[test]
#[cfg(feature = "vector-search")]
fn search_report_exposes_global_fallback_reasons_without_hits() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap();

    let result = index
        .search_with_report("", Some(&[1.0, 0.0, 0.0]), SearchMode::Vector, 10)
        .unwrap();

    assert!(result.hits.is_empty());
    assert!(result
        .fallback_reasons
        .iter()
        .any(|reason| reason.contains("query embedding dimension 3")));
    assert!(result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::VectorDimensionMismatch));
    assert!(result
        .empty_reasons
        .iter()
        .any(|reason| reason.contains("query embedding dimension 3")));
}

#[test]
fn search_fallback_reason_codes_have_stable_string_encodings() {
    let cases = [
        (
            SearchFallbackReasonCode::VectorDimensionMismatch,
            "vector_dimension_mismatch",
        ),
        (
            SearchFallbackReasonCode::VectorIndexEmpty,
            "vector_index_empty",
        ),
        (
            SearchFallbackReasonCode::CompressedVectorProjectionUnavailable,
            "compressed_vector_projection_unavailable",
        ),
        (
            SearchFallbackReasonCode::CompressedVectorBudgetExceeded,
            "compressed_vector_budget_exceeded",
        ),
        (
            SearchFallbackReasonCode::DiskAnnCacheBudgetExceeded,
            "diskann_cache_budget_exceeded",
        ),
        (
            SearchFallbackReasonCode::QueryEmbeddingMissing,
            "query_embedding_missing",
        ),
        (SearchFallbackReasonCode::TextQueryEmpty, "text_query_empty"),
        (
            SearchFallbackReasonCode::TextQueryTermsReduced,
            "text_query_terms_reduced",
        ),
    ];

    for (code, name) in cases {
        assert_eq!(code.as_str(), name);
        assert_eq!(name.parse::<SearchFallbackReasonCode>(), Ok(code));
    }
    assert!("fallback_unknown"
        .parse::<SearchFallbackReasonCode>()
        .is_err());
}

#[test]
fn search_truncation_reason_codes_have_stable_string_encodings() {
    assert_eq!(
        SearchTruncationReasonCode::LimitExceeded.as_str(),
        "limit_exceeded"
    );
    assert_eq!(
        "limit_exceeded".parse::<SearchTruncationReasonCode>(),
        Ok(SearchTruncationReasonCode::LimitExceeded)
    );
    assert!("rank_window_exceeded"
        .parse::<SearchTruncationReasonCode>()
        .is_err());
}

#[test]
#[cfg(feature = "vector-search")]
fn search_report_exposes_missing_query_embedding_reason() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap();

    let result = index
        .search_with_report("graph", None, SearchMode::Vector, 10)
        .unwrap();

    assert!(result.hits.is_empty());
    assert!(result
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    assert!(result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::QueryEmbeddingMissing));
    assert!(result
        .empty_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
    let vector = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .expect("expected vector retriever report");
    assert!(!vector.available);
    assert!(vector
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::QueryEmbeddingMissing));
    assert!(vector
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query embedding not provided"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn text_search_uses_bm25_term_frequency_and_length_normalization() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "focused".to_string(),
            title: "Graph graph storage".to_string(),
            content: "graph wal".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "verbose".to_string(),
            title: "Graph storage".to_string(),
            content: "graph runtime parser optimizer storage checkpoint wal manifest".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index
        .search("graph storage", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(hits[0].id, "focused");
    assert!(hits[0].text_score > hits[1].text_score);
}

#[test]
#[cfg(feature = "full-text-search")]
fn text_search_weights_title_terms_twice() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "title_match".to_string(),
            title: "graph".to_string(),
            content: "neutral".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "content_match".to_string(),
            title: "neutral".to_string(),
            content: "graph".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index.search("graph", None, SearchMode::Text, 10).unwrap();

    assert_eq!(hits[0].id, "title_match");
    assert_eq!(hits[1].id, "content_match");
    assert!(hits[0].text_score > hits[1].text_score);
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_keeps_nowledge_style_identifiers_case_insensitive() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory".to_string(),
            title: "Nowledge_Source".to_string(),
            content: "Graph-storage, graph storage.".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let underscore_hits = index
        .search("nowledge_source", None, SearchMode::Text, 10)
        .unwrap();
    let punctuation_hits = index
        .search("GRAPH storage", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(underscore_hits[0].id, "memory");
    assert_eq!(punctuation_hits[0].id, "memory");
}

#[test]
fn tokenizer_preserves_local_dedup_and_cross_identifier_frequency() {
    let lexicon = SearchAnalyzerLexicon::empty();

    assert_eq!(
        identifier_tokens("Running_running", &lexicon),
        vec!["running_running", "running", "run"]
    );
    assert_eq!(
        tokenize_list("Graph Graph Graph", &lexicon),
        vec!["graph", "graph_graph", "graph", "graph"]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_matches_camel_snake_kebab_and_path_identifiers() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "chunk".to_string(),
            title: "SourceChunk nowledge_source".to_string(),
            content: "artifact-source/chunk parserV2".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let camel_hits = index
        .search("source chunk", None, SearchMode::Text, 10)
        .unwrap();
    let snake_hits = index
        .search("source_chunk", None, SearchMode::Text, 10)
        .unwrap();
    let kebab_hits = index
        .search("artifact source", None, SearchMode::Text, 10)
        .unwrap();
    let version_hits = index
        .search("parser v2", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(camel_hits[0].id, "chunk");
    assert_eq!(snake_hits[0].id, "chunk");
    assert_eq!(kebab_hits[0].id, "chunk");
    assert_eq!(version_hits[0].id, "chunk");
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_matches_cjk_subterms_with_ngrams() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "design".to_string(),
            title: "自研图数据库设计".to_string(),
            content: "稳定逻辑ID和可重建投影".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let graph_hits = index
        .search_with_report("图数据库", None, SearchMode::Text, 10)
        .unwrap();
    let projection_hits = index
        .search("重建投影", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(graph_hits.hits[0].id, "design");
    assert!(graph_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "数据库"));
    assert_eq!(projection_hits[0].id, "design");
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_emits_dictionary_backed_chinese_search_terms() {
    let mut index = SearchIndex::in_memory();
    let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
    index
            .upsert(SearchDocument {
                id: "distributed".to_string(),
                title: "\u{73b0}\u{4ee3}\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}".to_string(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

    let output = index
        .search_with_report(term, None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(output.hits[0].id, "distributed");
    assert!(output.hits[0]
        .matched_terms
        .iter()
        .any(|token| token == term));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_splits_acronym_titlecase_boundaries() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "runtime".to_string(),
            title: "LSMTree HTTPServer GraphQLParser".to_string(),
            content: "WALCheckpoint handles MVCCSnapshot readers".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let lsm_hits = index
        .search("lsm tree", None, SearchMode::Text, 10)
        .unwrap();
    let http_hits = index
        .search("http server", None, SearchMode::Text, 10)
        .unwrap();
    let graphql_hits = index
        .search("graphql parser", None, SearchMode::Text, 10)
        .unwrap();
    let checkpoint_hits = index
        .search("wal checkpoint", None, SearchMode::Text, 10)
        .unwrap();
    let mvcc_hits = index
        .search_with_report("mvcc snapshot", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(lsm_hits[0].id, "runtime");
    assert_eq!(http_hits[0].id, "runtime");
    assert_eq!(graphql_hits[0].id, "runtime");
    assert_eq!(checkpoint_hits[0].id, "runtime");
    assert_eq!(mvcc_hits.hits[0].id, "runtime");
    assert!(mvcc_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "mvcc"));
    assert!(mvcc_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "snapshot"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_normalizes_common_english_suffixes() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory".to_string(),
            title: "Memories linked sources".to_string(),
            content: "threading normalized archived chunks".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let memory_hits = index.search("memory", None, SearchMode::Text, 10).unwrap();
    let source_hits = index.search("source", None, SearchMode::Text, 10).unwrap();
    let thread_hits = index.search("thread", None, SearchMode::Text, 10).unwrap();
    let archive_hits = index.search("archive", None, SearchMode::Text, 10).unwrap();
    let chunk_hits = index.search("chunk", None, SearchMode::Text, 10).unwrap();

    assert_eq!(memory_hits[0].id, "memory");
    assert_eq!(source_hits[0].id, "memory");
    assert_eq!(thread_hits[0].id, "memory");
    assert_eq!(archive_hits[0].id, "memory");
    assert_eq!(chunk_hits[0].id, "memory");
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_knowledge_retrieval_aliases() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "graph-rag".to_string(),
            title: "GraphRAG over knowledge graph evidence".to_string(),
            content: "retrieval augmented generation with bounded graph expansion".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let graph_retrieval_hits = index
        .search("graph retrieval", None, SearchMode::Text, 10)
        .unwrap();
    let rag_hits = index.search("rag", None, SearchMode::Text, 10).unwrap();
    let expanded_hits = index
        .search("retrieval augmented generation", None, SearchMode::Text, 10)
        .unwrap();
    let kg_hits = index.search("kg", None, SearchMode::Text, 10).unwrap();
    let knowledge_graph_hits = index
        .search("knowledge graph", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(graph_retrieval_hits[0].id, "graph-rag");
    assert_eq!(rag_hits[0].id, "graph-rag");
    assert_eq!(expanded_hits[0].id, "graph-rag");
    assert_eq!(kg_hits[0].id, "graph-rag");
    assert_eq!(knowledge_graph_hits[0].id, "graph-rag");
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_nowledge_memory_lifecycle_aliases() {
    let mut index =
        SearchIndex::in_memory().with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
    index
        .upsert(SearchDocument {
            id: "memory-lifecycle".to_string(),
            title: "Crystal memory keeps SYNTHESIZED_FROM evidence".to_string(),
            content: "Episodic provenance preserves raw Thread and SourceChunk records".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let crystallization_hits = index
        .search("crystallization", None, SearchMode::Text, 10)
        .unwrap();
    let synthesized_hits = index
        .search("synthesized memory", None, SearchMode::Text, 10)
        .unwrap();
    let raw_evidence_hits = index
        .search_with_report("raw evidence", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(crystallization_hits[0].id, "memory-lifecycle");
    assert_eq!(synthesized_hits[0].id, "memory-lifecycle");
    assert_eq!(raw_evidence_hits.hits[0].id, "memory-lifecycle");
    assert!(raw_evidence_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "raw_evidence"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_keeps_nowledge_application_aliases_out_of_default_lexicon() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory-lifecycle".to_string(),
            title: "Crystal memory".to_string(),
            content: "SYNTHESIZED_FROM evidence".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index
        .search("crystallization", None, SearchMode::Text, 10)
        .unwrap();

    assert!(hits.is_empty());
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_nowledge_schema_relationship_aliases() {
    let mut index =
        SearchIndex::in_memory().with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
    index
        .upsert(SearchDocument {
            id: "schema-relationships".to_string(),
            title: "SOURCED_FROM MENTIONS EVOLVES ai_summary".to_string(),
            content: "Community summaries link source provenance and memory evolution".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let source_hits = index
        .search("source provenance", None, SearchMode::Text, 10)
        .unwrap();
    let mention_hits = index
        .search("entity mention", None, SearchMode::Text, 10)
        .unwrap();
    let evolution_hits = index
        .search("memory evolution", None, SearchMode::Text, 10)
        .unwrap();
    let summary_hits = index
        .search_with_report("community summary", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(source_hits[0].id, "schema-relationships");
    assert_eq!(mention_hits[0].id, "schema-relationships");
    assert_eq!(evolution_hits[0].id, "schema-relationships");
    assert_eq!(summary_hits.hits[0].id, "schema-relationships");
    assert!(summary_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "community_summary"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_nowledge_memory_relation_product_aliases() {
    let mut index =
        SearchIndex::in_memory().with_analyzer_lexicon(SearchAnalyzerLexicon::nowledge_memory());
    index
        .upsert(SearchDocument {
            id: "memory-relations".to_string(),
            title: "MEMORY_RELATES_TO relation_type same_topic".to_string(),
            content: "Internal schema rows model semantic relation edges between memories"
                .to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let product_hits = index
        .search_with_report("memory-to-memory relationships", None, SearchMode::Text, 10)
        .unwrap();
    let schema_hits = index
        .search_with_report("relation_type", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(product_hits.hits[0].id, "memory-relations");
    assert!(product_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "memory_relates_to"));
    assert_eq!(schema_hits.hits[0].id, "memory-relations");
    assert!(schema_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "semantic_relation"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_normalizes_application_alias_rules() {
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(
        SearchAnalyzerLexicon::default()
            .with_normalized_alias_rule(["raw evidence", "RawEvidence"], ["episodic provenance"])
            .with_normalized_alias_rule(["community summary"], ["aiSummary"]),
    );
    index
        .upsert(SearchDocument {
            id: "readable-aliases".to_string(),
            title: "Episodic provenance keeps aiSummary auditable".to_string(),
            content: "Application lexicons should not expose analyzer token internals".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let raw_evidence_hits = index
        .search_with_report("raw evidence", None, SearchMode::Text, 10)
        .unwrap();
    let summary_hits = index
        .search_with_report("community summary", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(raw_evidence_hits.hits[0].id, "readable-aliases");
    assert!(raw_evidence_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "episodic_provenance"));
    assert_eq!(summary_hits.hits[0].id, "readable-aliases");
    assert!(summary_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "ai_summary"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_applies_application_stopword_rules() {
    let mut index = SearchIndex::in_memory().with_analyzer_lexicon(
        SearchAnalyzerLexicon::default().with_stopwords(["memory lifecycle", "thread"]),
    );
    index
        .upsert(SearchDocument {
            id: "specific".to_string(),
            title: "MemoryLifecycle WAL checkpoint".to_string(),
            content: "Thread compaction evidence remains auditable".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let hits = index
        .search_with_report("memory lifecycle checkpoint", None, SearchMode::Text, 10)
        .unwrap();
    let thread_hits = index
        .search_with_report("thread evidence", None, SearchMode::Text, 10)
        .unwrap();
    let noisy_hits = index
        .search("memory lifecycle thread", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(hits.hits[0].id, "specific");
    assert!(hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "checkpoint"));
    assert!(!hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "memory" || term == "lifecycle" || term == "memory_lifecycle"));
    assert_eq!(thread_hits.hits[0].id, "specific");
    assert!(thread_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "evidence"));
    assert!(!thread_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "thread"));
    assert!(noisy_hits.is_empty());
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_database_system_aliases() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "runtime".to_string(),
            title: "LSMTree WALCheckpoint MVCCSnapshot CypherPlanner".to_string(),
            content: "CSR and CSC projections back graph analytics".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let wal_hits = index
        .search_with_report("write ahead log", None, SearchMode::Text, 10)
        .unwrap();
    let mvcc_hits = index
        .search(
            "multi version concurrency control",
            None,
            SearchMode::Text,
            10,
        )
        .unwrap();
    let lsm_hits = index
        .search("log structured merge tree", None, SearchMode::Text, 10)
        .unwrap();
    let csr_hits = index
        .search("compressed sparse row", None, SearchMode::Text, 10)
        .unwrap();
    let csc_hits = index
        .search("compressed sparse column", None, SearchMode::Text, 10)
        .unwrap();
    let opencypher_hits = index
        .search("opencypher", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(wal_hits.hits[0].id, "runtime");
    assert!(wal_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "wal"));
    assert_eq!(mvcc_hits[0].id, "runtime");
    assert_eq!(lsm_hits[0].id, "runtime");
    assert_eq!(csr_hits[0].id, "runtime");
    assert_eq!(csc_hits[0].id, "runtime");
    assert_eq!(opencypher_hits[0].id, "runtime");
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_graph_stream_and_projection_aliases() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "hawdb-lightning".to_string(),
            title: "HawDBLightning publishes GraphStream and RelationalStream".to_string(),
            content: "Checkpointed snapshots track projection freshness".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let import_hits = index
        .search("database import", None, SearchMode::Text, 10)
        .unwrap();
    let export_hits = index
        .search("graph export", None, SearchMode::Text, 10)
        .unwrap();
    let value_hits = index
        .search("value stream", None, SearchMode::Text, 10)
        .unwrap();
    let checkpoint_hits = index
        .search_with_report("checkpoint freshness", None, SearchMode::Text, 10)
        .unwrap();
    let staleness_hits = index
        .search("projection staleness", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(import_hits[0].id, "hawdb-lightning");
    assert_eq!(export_hits[0].id, "hawdb-lightning");
    assert_eq!(value_hits[0].id, "hawdb-lightning");
    assert_eq!(checkpoint_hits.hits[0].id, "hawdb-lightning");
    assert_eq!(staleness_hits[0].id, "hawdb-lightning");
    assert!(checkpoint_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "checkpoint"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_migration_projection_aliases() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "projection".to_string(),
            title: "PostgreSQL pgvector FTS LanceDB Kuzu".to_string(),
            content: "Local graph projection replaces Ladybug search adapters".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let postgres_hits = index
        .search("postgres", None, SearchMode::Text, 10)
        .unwrap();
    let pg_hits = index.search("pg", None, SearchMode::Text, 10).unwrap();
    let vector_hits = index
        .search("vector search", None, SearchMode::Text, 10)
        .unwrap();
    let fts_hits = index
        .search_with_report("full text search", None, SearchMode::Text, 10)
        .unwrap();
    let lance_hits = index.search("lance", None, SearchMode::Text, 10).unwrap();
    let ladybug_hits = index.search("ladybug", None, SearchMode::Text, 10).unwrap();

    assert_eq!(postgres_hits[0].id, "projection");
    assert_eq!(pg_hits[0].id, "projection");
    assert_eq!(vector_hits[0].id, "projection");
    assert_eq!(fts_hits.hits[0].id, "projection");
    assert_eq!(lance_hits[0].id, "projection");
    assert_eq!(ladybug_hits[0].id, "projection");
    assert!(fts_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "fts"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_expands_retriever_algorithm_aliases() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "retrieval".to_string(),
            title: "RRF hybrid ranking over ANN candidates".to_string(),
            content: "reciprocal rank fusion explains vector and text child retrievers".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "facade".to_string(),
            title: "HybridRetrieve facade".to_string(),
            content: "retriever DAG merges search hits and graph seed candidates".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let rrf_hits = index
        .search("reciprocal rank fusion", None, SearchMode::Text, 10)
        .unwrap();
    let ann_hits = index
        .search("approximate nearest neighbor", None, SearchMode::Text, 10)
        .unwrap();
    let abbreviation_hits = index.search("rrf ann", None, SearchMode::Text, 10).unwrap();
    let retrieval_hits = index
        .search("hybrid retrieval", None, SearchMode::Text, 10)
        .unwrap();
    let search_hits = index
        .search("hybrid search", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(rrf_hits[0].id, "retrieval");
    assert_eq!(ann_hits[0].id, "retrieval");
    assert_eq!(abbreviation_hits[0].id, "retrieval");
    assert_eq!(retrieval_hits[0].id, "facade");
    assert_eq!(search_hits[0].id, "facade");
}

#[test]
#[cfg(feature = "full-text-search")]
fn text_search_indexes_selected_projection_metadata_identifiers() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "metadata-only".to_string(),
            title: "Untitled".to_string(),
            content: "No body match".to_string(),
            embedding: None,
            metadata: BTreeMap::from([
                ("kind".to_string(), "memory".to_string()),
                ("external_id".to_string(), "mem_graph_alpha".to_string()),
                ("source_id".to_string(), "thread_projection_1".to_string()),
                ("space_id".to_string(), "team_archive".to_string()),
            ]),
        })
        .unwrap();

    let external_id_hits = index
        .search("mem graph alpha", None, SearchMode::Text, 10)
        .unwrap();
    let source_id_hits = index
        .search_with_report("thread projection 1", None, SearchMode::Text, 10)
        .unwrap();
    let space_id_hits = index
        .search_with_report("team archive", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(external_id_hits[0].id, "metadata-only");
    assert_eq!(source_id_hits.hits[0].id, "metadata-only");
    assert_eq!(space_id_hits.hits[0].id, "metadata-only");
    assert!(source_id_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "thread"));
    assert!(space_id_hits.hits[0]
        .matched_terms
        .iter()
        .any(|term| term == "archive"));
    assert!(source_id_hits.hits[0].matched_spans.is_empty());
    assert!(space_id_hits.hits[0].matched_spans.is_empty());
}

#[test]
#[cfg(feature = "full-text-search")]
fn tokenizer_filters_stopwords_from_text_scoring_and_matched_terms() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "noise".to_string(),
            title: "The of in".to_string(),
            content: "and to with".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "graph".to_string(),
            title: "Graph memory".to_string(),
            content: "indexed retrieval".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let result = index
        .search_with_report("the graph of memory", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "graph");
    assert_eq!(
        result.hits[0].matched_terms,
        vec!["graph".to_string(), "memory".to_string()]
    );
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert_eq!(text.candidate_count, 1);
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_hits_expose_projection_provenance_and_matched_terms() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "mem_1".to_string(),
            title: "GraphRAG retrieval".to_string(),
            body: "Evidence from source chunks".to_string(),
            embedding: None,
            source_id: Some("thread_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        })
        .unwrap();

    let hits = index
        .search("rag source chunk", None, SearchMode::Text, 10)
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "memory:mem_1");
    assert_eq!(hits[0].kind.as_deref(), Some("memory"));
    assert_eq!(hits[0].external_id.as_deref(), Some("mem_1"));
    assert_eq!(hits[0].source_id.as_deref(), Some("thread_1"));
    assert!(hits[0].matched_terms.iter().any(|term| term == "rag"));
    assert!(hits[0].matched_terms.iter().any(|term| term == "source"));
    assert!(hits[0].matched_terms.iter().any(|term| term == "chunk"));
    assert!(hits[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "title" && span.text == "GraphRAG" && span.term == "rag"));
    assert!(hits[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content" && span.text == "source" && span.term == "source"));
    assert!(hits[0]
        .matched_spans
        .iter()
        .any(|span| span.field == "content" && span.text == "chunks" && span.term == "chunk"));
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn search_hits_expose_projection_freshness() {
    let path = unique_test_dir("search_projection_freshness");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "text-embedding-3-small".to_string(),
            version: Some("2026-07-15".to_string()),
            dimension: 2,
        })
        .unwrap();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "mem_1".to_string(),
            title: "Graph retrieval".to_string(),
            body: "Freshness aware search projection".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: Some("thread_1".to_string()),
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index.mark_full_reindex_needed("stale projection").unwrap();
    index
        .mark_metadata_repair_needed("missing metadata")
        .unwrap();

    let freshness = index.projection_freshness();
    let hits = index
        .search(
            "freshness projection",
            Some(&[1.0, 0.0]),
            SearchMode::Hybrid,
            10,
        )
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].projection_freshness, freshness);
    assert_eq!(hits[0].projection_freshness.document_count, 1);
    assert!(hits[0].projection_freshness.full_reindex_needed);
    assert_eq!(
        hits[0].projection_freshness.full_reindex_reasons,
        vec!["stale projection".to_string()]
    );
    assert!(hits[0].projection_freshness.metadata_repair_needed);
    assert_eq!(
        hits[0].projection_freshness.metadata_repair_reasons,
        vec!["missing metadata".to_string()]
    );
    assert_eq!(
        hits[0].projection_freshness.embedding_model.as_deref(),
        Some("text-embedding-3-small")
    );
    assert_eq!(
        hits[0].projection_freshness.embedding_version.as_deref(),
        Some("2026-07-15")
    );
    assert_eq!(hits[0].projection_freshness.embedding_dimension, Some(2));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_limit_truncation() {
    let mut index = SearchIndex::in_memory();
    for id in ["a", "b", "c"] {
        index
            .upsert(SearchDocument {
                id: id.to_string(),
                title: "Graph retrieval".to_string(),
                content: "projection evidence".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }

    let result = index
        .search_with_report("graph", None, SearchMode::Text, 2)
        .unwrap();

    assert_eq!(result.hits.len(), 2);
    assert_eq!(result.total_hits, 3);
    assert_eq!(result.limit, 2);
    assert!(result.truncated);
    assert_eq!(
        result.truncation_reason_codes,
        vec![SearchTruncationReasonCode::LimitExceeded]
    );
    assert_eq!(result.truncation_reasons.len(), 1);
    assert!(result.truncation_reasons[0].contains("limit 2"));
    assert!(result.truncation_reasons[0].contains("3 matching hits"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_empty_projection_reason() {
    let index = SearchIndex::in_memory();

    let result = index
        .search_with_report("graph", None, SearchMode::Text, 10)
        .unwrap();

    assert!(result.hits.is_empty());
    assert_eq!(
        result.empty_reason_codes,
        vec![SearchEmptyReasonCode::ProjectionEmpty]
    );
    assert_eq!(result.empty_reason_codes[0].as_str(), "projection_empty");
    assert_eq!(
        result.empty_reasons,
        vec!["search projection has no documents".to_string()]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_metadata_filter_empty_reason() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "a".to_string(),
            title: "Graph retrieval".to_string(),
            content: "projection evidence".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();

    let result = index
        .search_with_options(
            "graph",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "archive".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert!(result.hits.is_empty());
    assert_eq!(
        result.empty_reason_codes,
        vec![SearchEmptyReasonCode::MetadataFilterEmpty]
    );
    assert_eq!(
        result.empty_reasons,
        vec!["metadata filters matched no search documents".to_string()]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_no_matching_rows_empty_reason() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "a".to_string(),
            title: "Graph retrieval".to_string(),
            content: "projection evidence".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let result = index
        .search_with_report("unmatched needle", None, SearchMode::Text, 10)
        .unwrap();

    assert!(result.hits.is_empty());
    assert_eq!(
        result.empty_reason_codes,
        vec![SearchEmptyReasonCode::RetrieverNoHits]
    );
    assert_eq!(
        result.empty_reasons,
        vec!["search retrievers returned no hits inside filtered scope".to_string()]
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_empty_text_query_reason() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "a".to_string(),
            title: "Graph retrieval".to_string(),
            content: "projection evidence".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let result = index
        .search_with_report("", None, SearchMode::Text, 10)
        .unwrap();

    assert!(result.hits.is_empty());
    assert_eq!(
        result.empty_reason_codes,
        vec![SearchEmptyReasonCode::RetrieverNoHits]
    );
    assert!(result
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    assert!(result
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryEmpty));
    assert!(result
        .empty_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
    let text = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .expect("expected text retriever report");
    assert!(!text.available);
    assert!(text
        .fallback_reason_codes
        .contains(&SearchFallbackReasonCode::TextQueryEmpty));
    assert!(text
        .fallback_reasons
        .iter()
        .any(|reason| reason == "query text produced no searchable terms"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_report_exposes_limit_zero_empty_reason() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "a".to_string(),
            title: "Graph retrieval".to_string(),
            content: "projection evidence".to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        })
        .unwrap();

    let result = index
        .search_with_report("graph", None, SearchMode::Text, 0)
        .unwrap();

    assert!(result.hits.is_empty());
    assert_eq!(result.total_hits, 1);
    assert!(result.truncated);
    assert_eq!(
        result.empty_reason_codes,
        vec![SearchEmptyReasonCode::LimitExcludedAllHits]
    );
    assert_eq!(
        result.empty_reasons,
        vec!["limit 0 returned from 1 matching hits".to_string()]
    );
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn projection_snapshot_round_trips() {
    let path = unique_test_dir("search_snapshot");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        let report = index.checkpoint_with_report().unwrap();
        assert_eq!(report.document_count, 1);
        assert!(report.snapshot_streamed);
        assert!(report.snapshot_uncompressed_bytes > 0);
        assert!(report.snapshot_compressed_bytes > 0);
        assert!(report.snapshot_peak_record_bytes > 0);
        assert!(report.projection_generation > 0);
        assert!(report.projection_bytes_written > 0);
    }
    {
        let index = SearchIndex::open(&path).unwrap();
        let hits = index
            .search("adjacency", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10)
            .unwrap();
        assert_eq!(hits[0].id, "a");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn graph_rebuild_records_source_commit_epoch() {
    let path = unique_test_dir("search_source_graph_epoch");
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();

    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
            .unwrap();
        assert_eq!(
            index.projection_freshness().source_graph_commit_epoch,
            Some(store.commit_epoch())
        );
        let report = index.checkpoint_with_report().unwrap();
        assert!(report.projection_bytes_written > 0);
    }
    {
        let index = SearchIndex::open(&path).unwrap();
        assert_eq!(
            index.projection_freshness().source_graph_commit_epoch,
            Some(store.commit_epoch())
        );
        let result = index
            .search_with_options(
                "graph storage",
                None,
                SearchMode::Text,
                SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: SearchFusionWeights::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(result.total_hits, 1);
        assert_eq!(
            result.candidate_set.snapshot_source_graph_commit_epoch,
            Some(store.commit_epoch())
        );
        assert_eq!(result.candidate_set.policy_epoch, None);
    }

    let snapshot = read_search_snapshot_text(&path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    assert!(snapshot.contains("source_graph_commit_epoch\t1\n"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn in_memory_checkpoint_reports_no_projection_writes() {
    let index = SearchIndex::in_memory();

    let report = index.checkpoint_with_report().unwrap();

    assert_eq!(report.projection_generation, 0);
    assert_eq!(report.projection_bytes_written, 0);
    assert!(!report.snapshot_streamed);
}

#[test]
fn projection_checkpoint_publishes_snapshot_file() {
    let path = unique_test_dir("search_snapshot_publish");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "text-embedding-3-small".to_string(),
                version: None,
                dimension: 2,
            })
            .unwrap();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        index.checkpoint().unwrap();
    }

    let snapshot_bytes = std::fs::read(path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    assert!(snapshot_bytes.starts_with(SEARCH_COMPRESSION_HEADER.as_bytes()));
    let snapshot = read_search_snapshot_text(&path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    assert!(snapshot.contains("HAWDB_SEARCH_PROJECTION_V1\n"));
    assert!(snapshot.contains("embedding_manifest\t"));
    assert!(snapshot.contains("doc\t"));
    assert!(snapshot.contains("checksum\t"));

    let index = SearchIndex::open(&path).unwrap();
    assert_eq!(
        index.embedding_manifest(),
        Some(&SearchEmbeddingManifest {
            model: "text-embedding-3-small".to_string(),
            version: None,
            dimension: 2,
        })
    );
    assert!(index.document("a").is_some());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn rabitq_reopen_preserves_stale_valid_generation() {
    let path = unique_test_dir("rabitq_stale_generation");
    // Retain this fixture's descriptor domain across index closure. Windows
    // alias discovery for an unregistered existing root may otherwise probe
    // another parallel fixture whose descriptor budget is exhausted.
    let project_files = {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(doc(
                "memory:a",
                "Vector A",
                "Original projection",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index.checkpoint().unwrap();
        index
            .upsert(doc(
                "memory:a",
                "Vector A",
                "Updated projection",
                [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index.checkpoint().unwrap();
        index._project_files.as_ref().unwrap().clone()
    };
    assert_eq!(project_files.metrics().open, 0);

    let stale = path.join(rabitq_artifact_file(1));
    let stale_bytes = std::fs::read(&stale).unwrap();
    std::fs::remove_file(path.join(rabitq_artifact_file(2))).unwrap();

    for _ in 0..2 {
        let index = SearchIndex::open(&path).unwrap();
        assert!(index.rabitq_projection().is_none());
        assert_eq!(std::fs::read(&stale).unwrap(), stale_bytes);
        assert!(!std::fs::read_dir(&path).unwrap().flatten().any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("search_rabitq.1.hawdb.corrupt.")
        }));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn rabitq_reopen_falls_back_to_previous_valid_generation() {
    let path = unique_test_dir("rabitq_generation_fallback");
    // Retain this fixture's descriptor domain across index closure. Windows
    // alias discovery for an unregistered existing root may otherwise probe
    // another parallel fixture whose descriptor budget is exhausted.
    let project_files = {
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_rabitq_projection_build_options(Default::default());
        index
            .upsert(doc(
                "memory:a",
                "Vector A",
                "Generation fallback",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index.checkpoint().unwrap();
        index.checkpoint().unwrap();
        index._project_files.as_ref().unwrap().clone()
    };
    assert_eq!(project_files.metrics().open, 0);

    let latest = path.join(rabitq_artifact_file(2));
    let mut bytes = std::fs::read(&latest).unwrap();
    bytes[0] ^= 0xff;
    std::fs::write(&latest, bytes).unwrap();

    let index = SearchIndex::open(&path).unwrap();
    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: None,
        active_embedding_dimension: Some(8),
    });
    assert_eq!(probe["compressed_vector_projection"]["ready"], true);
    assert_eq!(probe["compressed_vector_projection"]["generation"], 1);
    assert!(!latest.exists());
    assert!(std::fs::read_dir(&path).unwrap().flatten().any(|entry| {
        entry
            .file_name()
            .to_string_lossy()
            .starts_with("search_rabitq.2.hawdb.corrupt.")
    }));
    assert!(index.projection_cleanup_report().retry_required);

    let result = index
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 1,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap();
    assert_eq!(result.hits[0].id, "memory:a");
    assert_eq!(
        result.retrievers[0].backend,
        "hawdb_rabitq_candidate_projection"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn compressed_vector_projection_disabled_uses_scalar_even_when_artifact_exists() {
    let path = unique_test_dir("search_rabitq_disabled_uses_scalar");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(doc(
                "memory:a",
                "Vector A",
                "Compressed projection",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index.checkpoint().unwrap();
    }

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Disabled,
        )
        .unwrap();

    assert_eq!(result.hits[0].id, "memory:a");
    assert_eq!(result.retrievers[0].backend, "scalar_vector_scan");
    assert_eq!(
        result.retrievers[0].backend_selection_reason,
        Some(VectorBackendSelectionReason::CompressionDisabled)
    );
    assert!(result.retrievers[0].fallback_reason_codes.is_empty());

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn compressed_vector_projection_required_uses_rabitq_projection() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:a",
            "Vector A",
            "Compressed projection",
            [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ))
        .unwrap();

    let result = index
        .search_with_options_compressed_vector_projection_mode(
            "",
            Some(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
            CompressedVectorSearchMode::Required,
        )
        .unwrap();

    assert_eq!(result.hits[0].id, "memory:a");
    assert_eq!(
        result.retrievers[0].backend,
        "hawdb_rabitq_candidate_projection"
    );
    assert_eq!(
        result.retrievers[0].candidate_score_source,
        "quantized_approximate"
    );
    assert_eq!(result.retrievers[0].final_score_source, "raw_vector");
    assert!(result.retrievers[0].raw_vector_bytes_read > 0);
    assert!(result.retrievers[0].candidate_scan_kernel.is_some());
    assert_eq!(result.retrievers[0].candidate_scan_scored_document_count, 1);
    assert!(result.retrievers[0].fallback_reason_codes.is_empty());
    assert!(result.fallback_reason_codes.is_empty());
}

#[test]
#[cfg(feature = "vector-search")]
fn adaptive_vector_backend_uses_filtered_candidate_count_and_recall_probe() {
    let mut index = SearchIndex::in_memory();
    for id in 0..8 {
        let mut document = doc(
            &format!("memory:{id}"),
            "Adaptive vector",
            "Filtered candidate",
            [1.0, 0.0],
        );
        document.metadata.insert(
            "space_id".to_string(),
            if id == 0 { "selected" } else { "other" }.to_string(),
        );
        index.upsert(document).unwrap();
    }
    let options = SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([("space_id".to_string(), "selected".to_string())]),
        policy_epoch: None,
    };

    let filtered = index
        .search_with_options_adaptive_vector_projection(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options.clone(),
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred)
                .with_backend_policy(AdaptiveVectorBackendPolicy {
                    flat_scan_max_documents: 2,
                    high_filter_selectivity_per_million: u32::MAX,
                    flat_scan_memory_budget_bytes: 0,
                }),
        )
        .unwrap();
    let recall_probe = index
        .search_with_options_adaptive_vector_projection(
            "",
            Some(&[1.0, 0.0]),
            SearchMode::Vector,
            options,
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Preferred)
                .with_backend_policy(force_quantized_policy())
                .as_recall_validation_probe(),
        )
        .unwrap();

    assert_eq!(filtered.hits.len(), 1);
    assert_eq!(
        filtered.retrievers[0].backend_selection_reason,
        Some(VectorBackendSelectionReason::SmallFilteredCandidateSet)
    );
    assert_eq!(filtered.retrievers[0].estimated_raw_vector_bytes, Some(8));
    assert_eq!(
        filtered.retrievers[0].filter_selectivity_per_million,
        Some(875_000)
    );
    assert_eq!(recall_probe.hits.len(), 1);
    assert_eq!(
        recall_probe.retrievers[0].backend_selection_reason,
        Some(VectorBackendSelectionReason::RecallValidationProbe)
    );
    assert_eq!(recall_probe.retrievers[0].backend, "scalar_vector_scan");
}

#[test]
#[cfg(feature = "vector-search")]
fn sampled_vector_recall_validates_filtered_persisted_projection() {
    let path = unique_test_dir("sampled_vector_recall");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_rabitq_projection_build_options(Default::default());
        for (id, embedding, space_id) in [
            (
                "memory:a",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "selected",
            ),
            (
                "memory:b",
                [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "selected",
            ),
            (
                "memory:c",
                [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "other",
            ),
            (
                "memory:d",
                [0.1, 0.9, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "other",
            ),
        ] {
            let mut document = doc(id, "Recall validation", "Bounded sampled query", embedding);
            document
                .metadata
                .insert("space_id".to_string(), space_id.to_string());
            index.upsert(document).unwrap();
        }
        index.checkpoint().unwrap();
    }
    let index = SearchIndex::open(&path).unwrap();

    let report = index.validate_sampled_compressed_vector_recall(VectorRecallValidationOptions {
        max_samples: 2,
        top_k: 1,
        candidate_limit: 1,
        minimum_recall_per_million: 1_000_000,
        metadata_filters: BTreeMap::from([("space_id".to_string(), "selected".to_string())]),
    });

    assert!(report.ready, "{:?}", report.blocker_codes);
    assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
    assert_eq!(report.sample_candidate_count, 2);
    assert_eq!(report.executed_sample_count, 2);
    assert_eq!(report.exact_hit_count, 2);
    assert_eq!(report.candidate_hit_count, 2);
    assert_eq!(report.candidate_overlap_count, 2);
    assert_eq!(report.candidate_recall_at_k_per_million, 1_000_000);
    assert_eq!(report.approximate_hit_count, 2);
    assert_eq!(report.recall_at_k_per_million, 1_000_000);
    assert_eq!(report.overlap_at_k_per_million, 1_000_000);
    assert_eq!(report.average_filter_selectivity_per_million, 500_000);
    assert_eq!(report.fallback_count, 0);
    assert_eq!(report.index_coverage_incomplete_count, 0);
    let json = report.json().to_string();
    assert!(!json.contains("memory:a"));
    assert!(!json.contains("Bounded sampled query"));

    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sampled_vector_recall_reports_probe_memory_exhaustion() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:a",
            "Recall A",
            "Budgeted recall validation",
            [1.0, 0.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "memory:b",
            "Recall B",
            "Budgeted recall validation",
            [0.9, 0.1],
        ))
        .unwrap();
    let mut exact_controls = AdaptiveVectorExecutionControls::IN_MEMORY;
    exact_controls.vector_execution_options.max_working_bytes = 0;

    let report = index.validate_sampled_vector_recall_with_controls(
        VectorRecallValidationOptions {
            max_samples: 1,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: BTreeMap::new(),
        },
        exact_controls,
        AdaptiveVectorExecutionControls::RECALL_CANDIDATES,
    );

    assert!(!report.ready);
    assert_eq!(report.protocol, VECTOR_RECALL_VALIDATION_PROTOCOL);
    assert_eq!(report.requested_sample_count, 1);
    assert_eq!(report.executed_sample_count, 0);
    assert_eq!(
        report.blocker_codes,
        vec![VectorRecallValidationBlocker::ProbeExecutionFailed]
    );
    assert_eq!(
        report.json()["blocker_codes"],
        serde_json::json!(["probe_execution_failed"])
    );
}

#[test]
#[cfg(feature = "vector-search")]
fn sampled_vector_recall_validates_rabitq_candidate_projection() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:a",
            "Recall A",
            "Missing projection",
            [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ))
        .unwrap();
    index
        .upsert(doc(
            "memory:b",
            "Recall B",
            "Missing projection",
            [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ))
        .unwrap();

    let report = index.validate_sampled_compressed_vector_recall(VectorRecallValidationOptions {
        max_samples: 2,
        top_k: 1,
        candidate_limit: 1,
        minimum_recall_per_million: 1_000_000,
        metadata_filters: BTreeMap::new(),
    });

    assert!(report.ready, "{:?}", report.blocker_codes);
    assert_eq!(report.executed_sample_count, 2);
    assert_eq!(report.exact_hit_count, 2);
    assert_eq!(report.candidate_hit_count, 2);
    assert_eq!(report.candidate_recall_at_k_per_million, 1_000_000);
    assert_eq!(report.approximate_hit_count, 2);
    assert_eq!(report.fallback_count, 0);
    assert!(report.blocker_codes.is_empty());
}

#[test]
#[cfg(feature = "vector-search")]
fn production_recall_identity_is_bound_to_reopened_file_projection() {
    let path = unique_test_dir("production_recall_identity");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_rabitq_projection_build_options(Default::default());
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "test-embedding".to_string(),
                version: Some("1".to_string()),
                dimension: 8,
            })
            .unwrap();
        index
            .upsert(doc(
                "memory:a",
                "Recall A",
                "Generation-bound projection",
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index
            .upsert(doc(
                "memory:b",
                "Recall B",
                "Generation-bound projection",
                [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ))
            .unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: Vec::new(),
                deletes: Vec::new(),
                max_operations: Some(1),
                source_graph_commit_epoch: Some(42),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    let index = SearchIndex::open(&path).unwrap();
    let identity = index.vector_projection_qualification_identity().unwrap();
    assert!(identity.file_backed);
    assert_eq!(identity.projection_generation, 1);
    assert_eq!(identity.source_graph_commit_epoch, Some(42));
    assert_eq!(identity.document_count, 2);
    assert_eq!(identity.embedding_model.as_deref(), Some("test-embedding"));
    assert_eq!(identity.embedding_version.as_deref(), Some("1"));
    assert_eq!(identity.dimension, 8);
    assert!(identity.source_digest > 0);

    let expected = crate::ProductionQualificationIdentity {
        source_revision: "test-revision".to_string(),
        rust_toolchain: "test-toolchain".to_string(),
        target_os: "linux".to_string(),
        target_arch: "x86_64".to_string(),
        enabled_features: vec!["vector-search".to_string()],
        durable_format_version: 1,
        schema_version: 1,
        configuration_digest: "test-config".to_string(),
        deployment_profile: "production-replica".to_string(),
        dataset_fingerprint: "test-dataset".to_string(),
        canonical_graph_commit_epoch: 42,
        policy_version: crate::PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
    let qualification = index.qualify_sampled_compressed_vector_recall_for_production(
        VectorRecallValidationOptions {
            max_samples: 2,
            top_k: 1,
            candidate_limit: 1,
            minimum_recall_per_million: 1_000_000,
            metadata_filters: BTreeMap::new(),
        },
        crate::ProductionEvidenceBinding {
            identity: expected.clone(),
            generated_at_unix_seconds: 1,
        },
        expected,
    );
    assert!(!qualification.ready);
    assert_eq!(
        qualification.blocker_codes,
        vec!["dataset_too_small".to_string()]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn sampled_vector_recall_redacts_invalid_metadata_filter() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:a",
            "Recall A",
            "Invalid filter must not leak",
            [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ))
        .unwrap();

    let report = index.validate_sampled_vector_recall(VectorRecallValidationOptions {
        metadata_filters: BTreeMap::from([(
            "lifecycle_state__not_in".to_string(),
            "sensitive malformed filter".to_string(),
        )]),
        ..VectorRecallValidationOptions::default()
    });

    assert!(!report.ready);
    assert!(report
        .blocker_codes
        .contains(&VectorRecallValidationBlocker::MetadataFilterInvalid));
    assert!(!report
        .json()
        .to_string()
        .contains("sensitive malformed filter"));
}

#[test]
#[cfg(feature = "full-text-search")]
fn projection_checkpoint_publishes_segment_descriptor() {
    let path = unique_test_dir("search_segment_descriptor_publish");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_0".to_string(),
                title: "Graph memory".to_string(),
                content: "segment descriptor retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_1".to_string(),
                title: "Graph memory".to_string(),
                content: "segment descriptor retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            })
            .unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_2".to_string(),
                title: "Graph memory".to_string(),
                content: "segment descriptor retrieval".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_2".to_string())]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }

    let descriptor_path = path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let descriptor = std::fs::read_to_string(&descriptor_path).unwrap();
    assert!(descriptor.contains("HAWDB_SEARCH_SEGMENTS_V3\n"));
    assert!(descriptor.contains("segment\t"));
    assert!(descriptor.contains("field\t"));
    assert!(descriptor.contains("checksum\t"));

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.filtered_document_count, 1);
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_prunes_numeric_range_filters() {
    let path = unique_test_dir("search_segment_descriptor_numeric_range");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, created_at) in [
            ("memory:0_old_0", "1"),
            ("memory:0_old_1", "2"),
            ("memory:1_new_0", "10"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor range retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("created_at".to_string(), created_at.to_string())]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    assert!(descriptor.contains("HAWDB_SEARCH_SEGMENTS_V3\n"));
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    assert_eq!(
        descriptor.segments[0]
            .metadata
            .get("created_at")
            .and_then(|summary| summary.numeric_range),
        Some(SearchNumericRange { min: 1.0, max: 2.0 })
    );
    assert_eq!(
        descriptor.segments[1]
            .metadata
            .get("created_at")
            .and_then(|summary| summary.numeric_range),
        Some(SearchNumericRange {
            min: 10.0,
            max: 10.0
        })
    );

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor range retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "10".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "memory:1_new_0");
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_pruning_candidate_document_count,
        3
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_pruned_document_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_scanned_document_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "created_at".to_string(),
            value_kind: "numeric_or_string".to_string(),
            operation_kinds: vec!["gte".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: true,
            timestamp_range_summary_used: false,
            value_summary_used: false,
        }]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn pinned_range_reads_survive_capacity_pressure_and_new_open_is_retryable() {
    use hawdb_core::error::FileDescriptorError;
    use hawdb_storage::file_descriptors::ProjectFileDescriptors;

    let path = unique_test_dir("search_range_fd_retry");
    let project = ProjectFileDescriptors::acquire_with_capacity_limit(&path, 32, true).unwrap();
    let mut index = SearchIndex::open(&path).unwrap();
    for (id, space) in [
        ("memory:0", "space-a"),
        ("memory:1", "space-a"),
        ("memory:2", "space-b"),
    ] {
        index
            .upsert(SearchDocument {
                id: id.to_string(),
                title: id.to_string(),
                content: "bounded physical range".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("space_id".to_string(), space.to_string())]),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let index = SearchIndex::open(&path).unwrap();
    let payload = path.join(SEARCH_SEGMENT_PAYLOAD_FILE);
    let original_payload = std::fs::read(&payload).unwrap();
    let options = SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([("space_id".to_string(), "space-b".to_string())]),
        policy_epoch: None,
    };
    let before = index
        .try_search_with_options(
            "bounded physical range",
            None,
            SearchMode::Text,
            options.clone(),
        )
        .unwrap();
    assert_eq!(
        before
            .candidate_set
            .metadata_predicate_pushdown
            .physical_range_read_count,
        1
    );
    let baseline_open = project.metrics().open;
    let mut held = Vec::new();
    for attempt in 0..=32 {
        assert!(
            attempt < 32,
            "strict capacity did not reject foreground opens"
        );
        match File::create(path.join(format!("held-{}", held.len()))) {
            Ok(file) => held.push(file),
            Err(error) => {
                assert!(matches!(
                    HawDBError::from(error),
                    HawDBError::FileDescriptors(FileDescriptorError::BudgetExceeded { .. })
                ));
                break;
            }
        }
    }
    assert_eq!(project.metrics().open, 32);
    let pinned = index
        .try_search_with_options(
            "bounded physical range",
            None,
            SearchMode::Text,
            options.clone(),
        )
        .unwrap();
    assert_eq!(pinned.hits.len(), 1);
    assert_eq!(
        pinned
            .candidate_set
            .metadata_predicate_pushdown
            .physical_range_read_count,
        1
    );
    let error = SearchIndex::open(&path).unwrap_err();
    assert!(
        matches!(
            error,
            HawDBError::FileDescriptors(FileDescriptorError::BudgetExceeded { .. })
        ),
        "generation-open rejection lost its resource cause: {error:?}"
    );
    assert_eq!(project.metrics().open, 32);
    assert_eq!(project.metrics().reserved, 0);
    drop(held);
    assert_eq!(std::fs::read(&payload).unwrap(), original_payload);
    drop(SearchIndex::open(&path).unwrap());
    let result = index
        .try_search_with_options("bounded physical range", None, SearchMode::Text, options)
        .unwrap();
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["memory:2"]
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_range_read_count,
        1
    );
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_bytes_read
            > 0
    );
    assert!(project.metrics().open <= baseline_open);
    assert_eq!(project.metrics().reserved, 0);
    drop(index);
    assert_eq!(project.metrics().open, 0);
    drop(project);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_ranges_execute_bounded_physical_reads() {
    let path = unique_test_dir("search_segment_physical_ranges");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, space_id) in [
            ("memory:0", "space-a"),
            ("memory:1", "space-a"),
            ("memory:2", "space-b"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: format!("Title {id}"),
                    content: "Physical segment range".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([
                        ("unit_type".to_string(), "memory".to_string()),
                        ("space_id".to_string(), space_id.to_string()),
                    ]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = read_search_segment_descriptor(&path)
        .unwrap()
        .expect("expected persisted segment descriptor");
    assert_eq!(descriptor.segments.len(), 2);
    assert!(descriptor.payload_artifact_is_available(&path).unwrap());
    let ranges = descriptor.physical_read_ranges();
    assert_eq!(ranges.len(), descriptor.segments.len());
    let scheduled_bytes = ranges.iter().map(|range| range.length.get()).sum::<u64>();
    let schedule =
        SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(1).unwrap())
            .schedule(ranges);
    let mut reader = FileSegmentRangeReader::new();
    reader.register(
        SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
        path.join(SEARCH_SEGMENT_PAYLOAD_FILE),
    );
    let mut document_ids = Vec::new();

    let report = SegmentReadExecutor::new(NonZeroU64::new(scheduled_bytes).unwrap())
        .execute(&reader, &schedule, |payload| {
            let segment_id = payload.range.segment_ids[0] as usize;
            let expected = descriptor.segments[segment_id]
                .payload_range
                .expect("expected physical payload range");
            assert_eq!(checksum_bytes(&payload.bytes), expected.checksum);
            let documents = decode_search_segment_documents(&payload.bytes)?;
            assert_eq!(
                documents.len(),
                descriptor.segments[segment_id].document_count
            );
            document_ids.extend(documents.into_iter().map(|document| document.id));
            Ok::<(), HawDBError>(())
        })
        .unwrap();

    assert_eq!(report.range_count, 2);
    assert_eq!(report.bytes_read, scheduled_bytes);
    assert_eq!(
        document_ids,
        vec![
            "memory:0".to_string(),
            "memory:1".to_string(),
            "memory:2".to_string(),
        ]
    );

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .try_search_with_options(
            "physical segment range",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "space-b".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["memory:2"]
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_range_read_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_bytes_read,
        descriptor.segments[1].payload_range.unwrap().length
    );

    let mut artifact = std::fs::read(path.join(SEARCH_SEGMENT_PAYLOAD_FILE)).unwrap();
    let corrupt_offset = descriptor.segments[1].payload_range.unwrap().offset as usize;
    artifact[corrupt_offset] ^= 0xff;
    std::fs::write(path.join(SEARCH_SEGMENT_PAYLOAD_FILE), artifact).unwrap();
    let error = index
        .try_search_with_options(
            "physical segment range",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "space-b".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("payload checksum mismatch"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_ranges_prune_label_in_filters_before_payload_reads() {
    let path = unique_test_dir("search_segment_label_physical_ranges");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, labels) in [
            ("memory:0", r#"["database"]"#),
            ("memory:1", r#"["database","rust"]"#),
            ("memory:2", r#"["planning"]"#),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: format!("Title {id}"),
                    content: "Physical label segment range".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("labels".to_string(), labels.to_string())]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = read_search_segment_descriptor(&path)
        .unwrap()
        .expect("expected persisted segment descriptor");
    assert_eq!(descriptor.segments.len(), 2);
    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .try_search_with_options(
            "physical label segment range",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "labels__in".to_string(),
                    r#"["Planning"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["memory:2"]
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_range_read_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .physical_bytes_read,
        descriptor.segments[1].payload_range.unwrap().length
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "labels".to_string(),
            value_kind: "enum".to_string(),
            operation_kinds: vec!["in".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: false,
            timestamp_range_summary_used: false,
            value_summary_used: true,
        }]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn fallible_search_rejects_pruned_reads_without_physical_ranges() {
    let path = unique_test_dir("search_segment_physical_ranges_missing");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, space_id) in [
            ("memory:0", "space-a"),
            ("memory:1", "space-a"),
            ("memory:2", "space-b"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: format!("Title {id}"),
                    content: "Physical segment range".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("space_id".to_string(), space_id.to_string())]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }
    let mut index = SearchIndex::open(&path).unwrap();
    for segment in &mut Arc::make_mut(
        index
            .segment_descriptor
            .as_mut()
            .expect("persisted segment descriptor"),
    )
    .segments
    {
        segment.payload_range = None;
    }

    let error = index
        .try_search_with_options(
            "physical segment range",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("space_id".to_string(), "space-b".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap_err();

    assert!(error
        .to_string()
        .contains("physical ranges are unavailable"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_prunes_timestamp_range_filters() {
    let path = unique_test_dir("search_segment_descriptor_timestamp_range");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, created_at) in [
            ("memory:0_old_0", "2026-01-01T00:00:00Z"),
            ("memory:0_old_1", "2026-01-02T00:00:00Z"),
            ("memory:1_new_0", "2026-02-01T12:00:00Z"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor timestamp retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([("created_at".to_string(), created_at.to_string())]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    assert!(descriptor.contains("HAWDB_SEARCH_SEGMENTS_V3\n"));
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    assert_eq!(
        descriptor.segments[0]
            .metadata
            .get("created_at")
            .and_then(|summary| summary.timestamp_range),
        Some(SearchTimestampRange {
            min_epoch_millis: metadata_timestamp_value("2026-01-01T00:00:00Z").unwrap(),
            max_epoch_millis: metadata_timestamp_value("2026-01-02T00:00:00Z").unwrap(),
        })
    );
    assert_eq!(
        descriptor.segments[1]
            .metadata
            .get("created_at")
            .and_then(|summary| summary.timestamp_range),
        Some(SearchTimestampRange {
            min_epoch_millis: metadata_timestamp_value("2026-02-01T12:00:00Z").unwrap(),
            max_epoch_millis: metadata_timestamp_value("2026-02-01T12:00:00Z").unwrap(),
        })
    );

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor timestamp retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "created_at__gte".to_string(),
                    "2026-02-01T00:00:00Z".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "memory:1_new_0");
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "created_at".to_string(),
            value_kind: "numeric_or_string".to_string(),
            operation_kinds: vec!["gte".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: true,
            timestamp_range_summary_used: true,
            value_summary_used: false,
        }]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_preserves_default_space_filter() {
    let path = unique_test_dir("search_segment_descriptor_default_space");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for id in ["memory:default_0", "memory:default_1", "memory:default_2"] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Default scoped graph".to_string(),
                    content: "segment descriptor default space retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    assert!(descriptor
        .segments
        .iter()
        .all(|segment| segment.metadata.contains_key("space_id")));

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor default space retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "space_id".to_string(),
                    DEFAULT_SPACE_ID.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 3);
    assert_eq!(result.filtered_document_count, 3);
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        0
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_tracks_missing_nowledge_scan_filter_fields() {
    let path = unique_test_dir("search_segment_descriptor_missing_nowledge_fields");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for id in ["memory:0_without_type", "memory:1_without_type"] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor missing field retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    for segment in &descriptor.segments {
        for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
            assert!(
                segment.metadata.contains_key(*field),
                "missing summary for {field}"
            );
        }
        assert_eq!(
            segment
                .metadata
                .get("unit_type")
                .map(|summary| summary.present_count),
            Some(0)
        );
    }

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor missing field retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([("unit_type".to_string(), "fact".to_string())]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 0);
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        0
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_projection_probe_reports_required_scan_filter_descriptor_fields() {
    let path = unique_test_dir("search_projection_probe_scan_filter_fields");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for row in nowledge_probe_rows() {
            index.upsert_projection_row(row).unwrap();
        }
        index.checkpoint().unwrap();
    }

    let index = SearchIndex::open(&path).unwrap();
    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: None,
        active_embedding_dimension: Some(2),
    });
    let summaries = probe["predicate_pushdown"]["segment_descriptor_field_summaries"]
        .as_array()
        .expect("expected descriptor summaries");
    let fields = summaries
        .iter()
        .filter_map(|summary| summary["field"].as_str())
        .collect::<BTreeSet<_>>();

    assert!(
        probe["predicate_pushdown"]["persisted_segment_descriptor_ready"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_document_pruning_ready"],
        true
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_pruning_candidate_document_count"],
        6
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_pruned_document_count"],
        4
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_scanned_document_count"],
        2
    );
    for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
        assert!(fields.contains(field), "missing descriptor field {field}");
    }
    assert!(fields.contains(SEARCH_DOCUMENT_ID_FIELD));
    for field in ["importance", "confidence"] {
        assert!(
            summaries.iter().any(|summary| summary["field"] == field
                && summary["numeric_range_summary_used"] == true),
            "missing numeric range summary for {field}"
        );
    }
    for field in ["created_at", "updated_at", "event_start", "event_end"] {
        assert!(
            summaries.iter().any(|summary| summary["field"] == field
                && summary["timestamp_range_summary_used"] == true),
            "missing timestamp range summary for {field}"
        );
    }
    assert!(summaries
        .iter()
        .any(|summary| summary["field"] == SEARCH_DOCUMENT_ID_FIELD
            && summary["unique_key_summary_used"] == true
            && summary["unique_key_summary_segment_count"] == 3));
    let production_filter_pruning = &probe["production_filter_pruning"];
    assert_eq!(production_filter_pruning["ready"], true);
    assert_eq!(
        production_filter_pruning["persisted_segment_descriptor_used"],
        true
    );
    assert_eq!(
        production_filter_pruning["payload_read_avoidance_ready"],
        true
    );
    assert_eq!(production_filter_pruning["explain_analyze_ready"], true);
    assert_eq!(
        production_filter_pruning["sample_count"],
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len() + 1
    );
    assert_eq!(
        production_filter_pruning["ready_field_count"],
        NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS.len()
    );
    let samples = production_filter_pruning["samples"]
        .as_array()
        .expect("expected production filter pruning samples");
    for field in NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS {
        assert!(
            samples
                .iter()
                .any(|sample| sample["field"] == *field && sample["ready"] == true),
            "missing production filter pruning sample for {field}"
        );
    }
    assert!(samples.iter().any(|sample| {
        sample["segment_pruned_document_count"]
            .as_u64()
            .unwrap_or(0)
            > 0
    }));
    assert!(samples
        .iter()
        .all(|sample| sample["explain_analyze"]["ready"] == true));
    assert!(samples
        .iter()
        .all(|sample| sample["explain_analyze"]["operator"] == "search_projection_segment_scan"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_prunes_document_id_filters() {
    let path = unique_test_dir("search_segment_descriptor_document_id");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for id in ["memory:0_old_0", "memory:0_old_1", "memory:1_target"] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor document id retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    assert_eq!(
        descriptor.segments[0]
            .metadata
            .get(SEARCH_DOCUMENT_ID_FIELD)
            .map(|summary| summary.values.clone()),
        Some(BTreeSet::from([
            "memory:0_old_0".to_string(),
            "memory:0_old_1".to_string()
        ]))
    );

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor document id retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "document_id__in".to_string(),
                    r#"["memory:1_target"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "memory:1_target");
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .segment_count,
        2
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .scanned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: SEARCH_DOCUMENT_ID_FIELD.to_string(),
            value_kind: "numeric_or_string".to_string(),
            operation_kinds: vec!["in".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: false,
            timestamp_range_summary_used: false,
            value_summary_used: true,
        }]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn persisted_segment_descriptor_prunes_enum_not_in_filters() {
    let path = unique_test_dir("search_segment_descriptor_enum_not_in");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, lifecycle_state) in [
            ("memory:0_deleted", "deleted"),
            ("memory:0_forgotten", " forgotten "),
            ("memory:1_active", "ACTIVE"),
        ] {
            index
                .upsert(SearchDocument {
                    id: id.to_string(),
                    title: "Graph memory".to_string(),
                    content: "segment descriptor enum retrieval".to_string(),
                    embedding: None,
                    metadata: BTreeMap::from([(
                        "lifecycle_state".to_string(),
                        lifecycle_state.to_string(),
                    )]),
                })
                .unwrap();
        }
        index.checkpoint().unwrap();
    }

    let descriptor = std::fs::read_to_string(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE)).unwrap();
    let descriptor = decode_search_segment_descriptor_text(&descriptor).unwrap();
    assert_eq!(
        descriptor.segments[0]
            .metadata
            .get("lifecycle_state")
            .map(|summary| summary.values.clone()),
        Some(BTreeSet::from([
            "deleted".to_string(),
            "forgotten".to_string()
        ]))
    );

    let index = SearchIndex::open(&path).unwrap();
    let result = index
        .search_with_options(
            "segment descriptor enum retrieval",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "lifecycle_state__not_in".to_string(),
                    r#"["deleted","forgotten"]"#.to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert_eq!(result.hits[0].id, "memory:1_active");
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .pruned_segment_count,
        1
    );
    assert_eq!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .field_summaries,
        vec![SearchPredicateFieldPruningReport {
            field: "lifecycle_state".to_string(),
            value_kind: "enum".to_string(),
            operation_kinds: vec!["not_in".to_string()],
            segment_count: 2,
            pruned_segment_count: 1,
            scanned_segment_count: 1,
            numeric_range_summary_used: false,
            timestamp_range_summary_used: false,
            value_summary_used: true,
        }]
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn corrupt_segment_descriptor_rebuilds_without_blocking_snapshot_load() {
    let path = unique_test_dir("search_segment_descriptor_rebuild");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .upsert(SearchDocument {
                id: "memory:thread_1".to_string(),
                title: "Graph memory".to_string(),
                content: "segment descriptor recovery".to_string(),
                embedding: None,
                metadata: BTreeMap::from([("source_id".to_string(), "thread_1".to_string())]),
            })
            .unwrap();
        index.checkpoint().unwrap();
    }
    std::fs::write(path.join(SEARCH_SEGMENT_DESCRIPTOR_FILE), "corrupt").unwrap();

    let index = SearchIndex::open(&path).unwrap();
    assert!(std::fs::read_dir(&path).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("search_projection_segments.hawdb.corrupt.")
    }));
    let result = index
        .search_with_options(
            "segment descriptor recovery",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(result.total_hits, 1);
    assert!(
        result
            .candidate_set
            .metadata_predicate_pushdown
            .persisted_segment_descriptor_used
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn embedding_manifest_round_trips_through_snapshot() {
    let path = unique_test_dir("embedding_manifest_snapshot");
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index
            .apply_embedding_manifest(SearchEmbeddingManifest {
                model: "text-embedding-3-small".to_string(),
                version: Some("2026-07-15".to_string()),
                dimension: 2,
            })
            .unwrap();
        index
            .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
            .unwrap();
        index.checkpoint().unwrap();
    }
    {
        let index = SearchIndex::open(&path).unwrap();
        assert_eq!(
            index.embedding_manifest(),
            Some(&SearchEmbeddingManifest {
                model: "text-embedding-3-small".to_string(),
                version: Some("2026-07-15".to_string()),
                dimension: 2,
            })
        );
        let hits = index
            .search("graph", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10)
            .unwrap();
        assert_eq!(hits[0].id, "a");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn embedding_manifest_change_marks_full_reindex_needed() {
    let path = unique_test_dir("embedding_manifest_change");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "model-a".to_string(),
            version: Some("1".to_string()),
            dimension: 2,
        })
        .unwrap();
    index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "model-b".to_string(),
            version: Some("1".to_string()),
            dimension: 2,
        })
        .unwrap();

    assert!(index.full_reindex_needed());
    let marker = std::fs::read_to_string(path.join(FULL_REINDEX_MARKER)).unwrap();
    assert!(marker.contains("embedding manifest changed"));
    assert!(marker.contains("model-a@1:2"));
    assert!(marker.contains("model-b@1:2"));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn embedding_manifest_dimension_rejects_mismatched_rows() {
    let mut index = SearchIndex::in_memory();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "model-a".to_string(),
            version: None,
            dimension: 3,
        })
        .unwrap();

    let error = index
        .upsert(doc("a", "Graph storage", "Native adjacency", [1.0, 0.0]))
        .unwrap_err();

    assert!(error.to_string().contains("manifest expects 3"));
}

#[test]
#[cfg(feature = "vector-search")]
fn nowledge_search_projection_probe_reports_ready_shape() {
    let path = unique_test_dir("nowledge_search_projection_probe_ready");
    let mut index = SearchIndex::open(&path).unwrap();
    index.set_rabitq_projection_build_options(Default::default());
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "bge-m3".to_string(),
            version: Some("local".to_string()),
            dimension: 2,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: nowledge_probe_rows(),
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(7),
        })
        .unwrap();
    index.checkpoint().unwrap();

    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: Some("bge-m3".to_string()),
        active_embedding_dimension: Some(2),
    });

    assert_eq!(probe["derived_projection"], true);
    assert_eq!(probe["document_count"], 6);
    assert_eq!(probe["lifecycle"]["generation_cleanup_ready"], true);
    assert_eq!(
        probe["generation_cleanup"]["protocol"],
        SEARCH_PROJECTION_CLEANUP_PROTOCOL
    );
    assert_eq!(probe["generation_cleanup"]["retry_required"], false);
    assert_eq!(probe["document_identity"]["ready"], true);
    assert_eq!(
        probe["document_identity"]["id_space"],
        "search_projection_document_id"
    );
    assert_eq!(
        probe["document_identity"]["representation"],
        "sorted_document_ids"
    );
    assert_eq!(probe["document_identity"]["document_count"], 6);
    assert!(probe["document_identity"].get("document_ids").is_none());
    assert_eq!(probe["tables"].as_array().unwrap().len(), 6);
    assert_eq!(
        probe["predicate_pushdown"]["segment_document_pruning_ready"],
        true
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_pruning_candidate_document_count"],
        6
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_pruned_document_count"],
        4
    );
    assert_eq!(
        probe["predicate_pushdown"]["segment_scanned_document_count"],
        2
    );
    assert!(probe["tables"].as_array().unwrap().iter().all(|table| {
        table["present"] == true && table["fts_ready"] == true && table["vector_ready"] == true
    }));
    assert_eq!(probe["embedding_manifest"]["model_matches"], true);
    assert_eq!(probe["embedding_manifest"]["dimension_matches"], true);
    assert_eq!(probe["fail_soft"]["fts_to_vector_ready"], true);
    assert_eq!(probe["fail_soft"]["vector_to_fts_ready"], true);
    assert_eq!(probe["lifecycle"]["rebuild_marker_ready"], true);
    assert_eq!(probe["lifecycle"]["metadata_repair_marker_ready"], true);
    assert_eq!(probe["incremental_update"]["ready"], true);
    assert_eq!(
        probe["compressed_vector_projection"]["engine"],
        "hawdb_rabitq_scan"
    );
    assert_eq!(probe["compressed_vector_projection"]["compiled"], true);
    assert_eq!(
        probe["compressed_vector_projection"]["supports_runtime_simd_dispatch"],
        cfg!(any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "aarch64"
        ))
    );
    assert_eq!(probe["compressed_vector_projection"]["ready"], true);
    assert_eq!(
        probe["compressed_vector_projection"]["bit_width"],
        hawdb_vector_projection::PROJECTION_BIT_WIDTH
    );
    assert_eq!(probe["compressed_vector_projection"]["dimension"], 2);
    assert_eq!(
        probe["compressed_vector_projection"]["persisted_artifact_used"],
        true
    );
    assert_eq!(probe["blocker_codes"], serde_json::json!([]));
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_projection_probe_blocks_on_invalid_cleanup_generation_identity() {
    let path = unique_test_dir("search_projection_cleanup_invalid_manifest");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "bge-m3".to_string(),
            version: Some("local".to_string()),
            dimension: 2,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: nowledge_probe_rows(),
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(7),
        })
        .unwrap();
    index.checkpoint().unwrap();
    index.checkpoint().unwrap();
    index.checkpoint().unwrap();
    let stale_artifact = path.join("search_projection_segments.1.hawdb");
    std::fs::write(&stale_artifact, b"stale generation").unwrap();
    std::fs::write(
        path.join("search_projection.out_of_core.manifest.hawdb"),
        b"invalid manifest",
    )
    .unwrap();

    assert!(SearchIndex::open(&path).is_err());
    let report = index.retry_projection_cleanup(SearchProjectionCleanupOptions::default());
    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: Some("bge-m3".to_string()),
        active_embedding_dimension: Some(2),
    });

    assert_eq!(report.generation_discovery_failures, 1);
    assert!(report.retry_required);
    assert!(stale_artifact.exists());
    assert_eq!(probe["lifecycle"]["generation_cleanup_ready"], false);
    assert!(probe["blocker_codes"]
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("projection_generation_cleanup_pending")));

    index.checkpoint().unwrap();
    assert!(!index.projection_cleanup_report().retry_required);
    assert!(!stale_artifact.exists());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn nowledge_search_projection_probe_reports_rabitq_projection_without_vectors() {
    let index = SearchIndex::in_memory();

    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: None,
        active_embedding_dimension: None,
    });

    assert_eq!(
        probe["compressed_vector_projection"]["engine"],
        "hawdb_rabitq_scan"
    );
    assert_eq!(
        probe["compressed_vector_projection"]["compiled"],
        cfg!(feature = "vector-search")
    );
    assert_eq!(probe["compressed_vector_projection"]["ready"], false);
    assert_eq!(
        probe["compressed_vector_projection"]["blocker_codes"],
        serde_json::json!([if cfg!(feature = "vector-search") {
            "rabitq_projection_unavailable"
        } else {
            "vector_search_feature_disabled"
        }])
    );
}

#[test]
#[cfg(feature = "qualification")]
fn external_vector_candidates_use_hawdb_filter_and_raw_rerank() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:a".to_string(),
            title: "Vector A".to_string(),
            content: String::new(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("space_id".to_string(), "allowed".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:b".to_string(),
            title: "Vector B".to_string(),
            content: String::new(),
            embedding: Some(vec![0.0, 1.0]),
            metadata: BTreeMap::from([("space_id".to_string(), "blocked".to_string())]),
        })
        .unwrap();
    let filters = BTreeMap::from([("space_id".to_string(), "allowed".to_string())]);
    let allowlist = index.vector_document_ids_matching_filters_for_validation(&filters);
    assert_eq!(allowlist, BTreeSet::from(["memory:a".to_string()]));

    let candidates = vec![("memory:b".to_string(), 0.9), ("memory:a".to_string(), 0.8)];
    let result = index
        .search_with_external_vector_candidates_for_validation(
            &candidates,
            &BTreeSet::from(["memory:a".to_string(), "memory:b".to_string()]),
            "",
            Some(&[0.8, 0.6]),
            SearchMode::Vector,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: filters,
                policy_epoch: Some(42),
            },
        )
        .unwrap();

    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].id, "memory:a");
    assert_eq!(result.retrievers[0].name, "vector");
    assert_eq!(
        result.retrievers[0].backend,
        "external_validation_candidate_projection"
    );
    assert_eq!(
        result.retrievers[0].candidate_score_source,
        "quantized_approximate"
    );
    assert_eq!(result.retrievers[0].final_score_source, "raw_vector");
    assert_eq!(
        result.retrievers[0].candidate_top_ids,
        vec!["memory:a".to_string()]
    );
    assert_eq!(result.filtered_document_count, 1);
    assert_eq!(result.candidate_set.policy_epoch, Some(42));
}

#[test]
fn nowledge_search_projection_probe_reports_manifest_and_marker_blockers() {
    let path = unique_test_dir("search_projection_probe_blockers");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_embedding_manifest(SearchEmbeddingManifest {
            model: "bge-m3".to_string(),
            version: None,
            dimension: 2,
        })
        .unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: nowledge_probe_rows(),
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(7),
        })
        .unwrap();
    index
        .mark_full_reindex_needed("embedding model changed")
        .unwrap();

    let probe = index.nowledge_search_projection_probe_json(SearchProjectionProbeOptions {
        active_embedding_model: Some("text-embedding-3-small".to_string()),
        active_embedding_dimension: Some(1536),
    });

    assert_eq!(probe["embedding_manifest"]["model_matches"], false);
    assert_eq!(probe["embedding_manifest"]["dimension_matches"], false);
    assert_eq!(probe["lifecycle"]["rebuild_marker_ready"], false);
    assert_eq!(
        probe["blocker_codes"],
        serde_json::json!([
            "embedding_dimension_mismatch",
            "embedding_model_mismatch",
            "full_reindex_needed"
        ])
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projection_markers_round_trip() {
    let path = unique_test_dir("search_markers");
    let index = SearchIndex::open(&path).unwrap();
    index
        .mark_full_reindex_needed("embedding model changed")
        .unwrap();
    index
        .mark_full_reindex_needed("embedding model changed")
        .unwrap();
    index
        .mark_metadata_repair_needed("missing metadata columns")
        .unwrap();

    assert!(index.full_reindex_needed());
    assert!(index.metadata_repair_needed());
    assert_eq!(
        std::fs::read_to_string(path.join(FULL_REINDEX_MARKER)).unwrap(),
        "embedding model changed"
    );
    std::fs::remove_dir_all(path).unwrap();
}

fn nowledge_probe_rows() -> Vec<SearchProjectionRow> {
    vec![
        nowledge_probe_row(SearchProjectionKind::Memory, "mem_1"),
        nowledge_probe_row_without_embedding(SearchProjectionKind::Message, "msg_1"),
        nowledge_probe_row(SearchProjectionKind::Community, "community_1"),
        nowledge_probe_row(SearchProjectionKind::Entity, "entity_1"),
        nowledge_probe_row(SearchProjectionKind::Source, "source_1"),
        nowledge_probe_row(SearchProjectionKind::SourceChunk, "chunk_1"),
    ]
}

fn nowledge_probe_row(kind: SearchProjectionKind, external_id: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind,
        external_id: external_id.to_string(),
        title: format!("{external_id} title"),
        body: format!("{external_id} body"),
        embedding: Some(vec![1.0, 0.0]),
        source_id: Some("source_1".to_string()),
        metadata: BTreeMap::from([
            ("space_id".to_string(), "default".to_string()),
            ("unit_type".to_string(), "memory".to_string()),
            ("lifecycle_state".to_string(), "active".to_string()),
            ("importance".to_string(), "0.8".to_string()),
            ("confidence".to_string(), "0.9".to_string()),
            ("created_at".to_string(), "2026-07-01T00:00:00Z".to_string()),
            ("updated_at".to_string(), "2026-07-02T00:00:00Z".to_string()),
            (
                "event_start".to_string(),
                "2026-07-01T12:00:00Z".to_string(),
            ),
            ("event_end".to_string(), "2026-07-01T13:00:00Z".to_string()),
            ("is_latest".to_string(), "true".to_string()),
        ]),
    }
}

fn nowledge_probe_row_without_embedding(
    kind: SearchProjectionKind,
    external_id: &str,
) -> SearchProjectionRow {
    SearchProjectionRow {
        embedding: None,
        ..nowledge_probe_row(kind, external_id)
    }
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn projection_rows_encode_nowledge_shapes() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert_projection_row(SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "mem_1".to_string(),
            title: "Graph storage".to_string(),
            body: "Native adjacency and WAL".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            source_id: Some("thread_1".to_string()),
            metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
        })
        .unwrap();

    let hits = index
        .search("wal", Some(&[1.0, 0.0]), SearchMode::Hybrid, 10)
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "memory:mem_1");
    let document = index.document("memory:mem_1").unwrap();
    assert_eq!(
        document.metadata.get("kind").map(String::as_str),
        Some("memory")
    );
    assert_eq!(
        document.metadata.get("external_id").map(String::as_str),
        Some("mem_1")
    );
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("thread_1")
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn graph_rebuild_falls_back_for_empty_node_ids() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String(String::new())),
                (
                    "title".to_string(),
                    Value::String("Empty id graph".to_string()),
                ),
                (
                    "content".to_string(),
                    Value::String("fallback identity projection".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    let hits = index
        .search("fallback identity projection", None, SearchMode::Text, 10)
        .unwrap();
    let document = index.document("memory:0").expect("projected document");

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "memory:0");
    assert_eq!(hits[0].external_id.as_deref(), Some("0"));
    assert!(index.document("memory:").is_none());
    assert_eq!(
        document.metadata.get("external_id").map(String::as_str),
        Some("0")
    );
}

#[test]
#[cfg(feature = "full-text-search")]
fn graph_rebuild_source_id_fallback_skips_empty_values() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Thread scoped graph".to_string()),
                ),
                (
                    "content".to_string(),
                    Value::String("source fallback projection".to_string()),
                ),
                ("source_id".to_string(), Value::String(String::new())),
                (
                    "thread_id".to_string(),
                    Value::String("thread_1".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    let document = index.document("memory:mem_1").expect("projected document");
    let hits = index
        .search_with_options(
            "source fallback projection",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::from([(
                    "source_id".to_string(),
                    "thread_1".to_string(),
                )]),
                policy_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("thread_1")
    );
    assert_eq!(hits.total_hits, 1);
    assert_eq!(hits.hits[0].source_id.as_deref(), Some("thread_1"));
}

#[test]
fn graph_rebuild_and_metadata_repair_collect_business_labels_once() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    let memory_id = store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Label projection".to_string()),
                ),
            ]),
        )
        .unwrap();
    store
        .business_labels
        .insert(memory_id, vec!["database".to_string(), "rust".to_string()]);

    let mut index = SearchIndex::in_memory();
    index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(store.business_label_bulk_scans.get(), 1);
    assert_eq!(store.business_label_point_lookups.get(), 0);
    assert_eq!(
        index
            .document("memory:mem_1")
            .and_then(|document| document.metadata.get("labels"))
            .map(String::as_str),
        Some(r#"["database","rust"]"#)
    );

    index
        .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(store.business_label_bulk_scans.get(), 2);
    assert_eq!(store.business_label_point_lookups.get(), 0);
}

#[test]
#[cfg(feature = "full-text-search")]
fn full_rebuild_projects_graph_nodes() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
                (
                    "content".to_string(),
                    Value::String("Native adjacency and WAL".to_string()),
                ),
                (
                    "source_id".to_string(),
                    Value::String("thread_1".to_string()),
                ),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("entity_1".to_string())),
                ("name".to_string(), Value::String("HawDB".to_string())),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    let summary = index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(summary.scanned_nodes, 2);
    assert_eq!(summary.indexed_documents, 2);
    assert!(index.document("memory:mem_1").is_some());
    assert_eq!(
        index
            .document("entity:entity_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("entity")
    );
    let hits = index
        .search("adjacency", None, SearchMode::Text, 10)
        .unwrap();
    assert_eq!(hits[0].id, "memory:mem_1");
    index.segment_descriptor = Some(Arc::new(SearchSegmentDescriptor::build(&index.documents)));
    let old = index.read_snapshot();
    index
        .rebuild_from_graph(
            &catalog,
            &TestProjectionSource::in_memory(),
            SearchRebuildOptions::default(),
        )
        .unwrap();
    assert!(index.segment_descriptor.is_none());
    assert_eq!(index.document_count(), 0);
    assert_eq!(old.document_count(), 2);
}

#[test]
fn full_rebuild_limit_failure_keeps_existing_projection() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Entity",
            BTreeMap::from([
                ("id".to_string(), Value::String("entity_1".to_string())),
                ("name".to_string(), Value::String("HawDB".to_string())),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
        .unwrap();
    let error = index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions { max_rows: Some(1) })
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(index.document_count(), 1);
    assert!(index.document("old").is_some());
    let freshness = index.projection_freshness();
    assert!(freshness.full_reindex_needed);
    assert_eq!(
        freshness.full_reindex_reasons,
        vec!["full rebuild exceeded configured row limit".to_string()]
    );
}

#[test]
fn full_rebuild_clears_projection_markers_on_success() {
    let path = unique_test_dir("search_rebuild_markers");
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::open(&path).unwrap();
    index.mark_full_reindex_needed("stale projection").unwrap();
    index
        .mark_metadata_repair_needed("missing metadata")
        .unwrap();
    index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    assert!(!index.full_reindex_needed());
    assert!(!index.metadata_repair_needed());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_derived_artifact_rebuild_reports_projection_refresh() {
    let path = unique_test_dir("search_derived_artifact_rebuild");
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
                (
                    "body".to_string(),
                    Value::String("Native adjacency".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::open(&path).unwrap();
    index
        .upsert(doc(
            "old",
            "Old projection",
            "Should be replaced",
            [1.0, 0.0],
        ))
        .unwrap();
    index.mark_full_reindex_needed("stale projection").unwrap();
    index
        .mark_metadata_repair_needed("missing metadata")
        .unwrap();

    let report = index
        .rebuild_derived_artifacts(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(report.artifact_type, "search_projection");
    assert_eq!(report.name, "search_projection");
    assert_eq!(report.action, "rebuilt");
    assert_eq!(report.before_document_count, 1);
    assert_eq!(report.after_document_count, 1);
    assert_eq!(report.scanned_nodes, 1);
    assert_eq!(report.indexed_documents, 1);
    assert!(!report.full_reindex_needed);
    assert!(report.full_reindex_reasons.is_empty());
    assert!(!report.metadata_repair_needed);
    assert!(report.metadata_repair_reasons.is_empty());
    assert!(index.document("old").is_none());
    assert!(index.document("memory:mem_1").is_some());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn search_derived_artifact_rebuild_limit_failure_keeps_existing_projection() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }

    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
        .unwrap();

    let error = index
        .rebuild_derived_artifacts(&catalog, &store, SearchRebuildOptions { max_rows: Some(1) })
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(index.document_count(), 1);
    assert!(index.document("old").is_some());
}

#[test]
fn search_rebuild_background_work_plan_uses_projection_lane() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }
    let index = SearchIndex::in_memory();

    let plan = index
        .rebuild_background_work_plan(
            &store,
            BackgroundWorkHint {
                active_topic: true,
                query_probability_per_million: 200_000,
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);
    let ranked = LocalQosPolicy::default().rank_background_work(&LocalQosState::default(), &[plan]);
    assert_eq!(ranked[0].index, 0);
    assert!(ranked[0]
        .decision
        .reasons
        .iter()
        .any(|reason| reason == "active topic"));
}

#[test]
#[cfg(feature = "background-maintenance")]
fn background_search_projection_rebuild_uses_qos_admission() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
        .unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };

    let error = index
        .rebuild_background_derived_artifacts(
            &policy,
            &LocalQosState::default(),
            &catalog,
            &store,
            SearchRebuildOptions::default(),
        )
        .unwrap_err();

    // A request above the per-work limit can never be admitted as is.
    assert_background_refusal(&error, 2, 1, false);
    assert_eq!(index.document_count(), 1);
    assert!(index.document("old").is_some());

    // Running work holding the budget defers it until that work ends.
    let busy = LocalQosPolicy {
        max_total_background_operations: Some(3),
        ..LocalQosPolicy::default()
    };
    let running = LocalQosState {
        running_background_operations: 2,
        ..LocalQosState::default()
    };
    let error = index
        .rebuild_background_derived_artifacts(
            &busy,
            &running,
            &catalog,
            &store,
            SearchRebuildOptions::default(),
        )
        .unwrap_err();
    assert_background_refusal(&error, 2, 1, true);
    assert!(index.document("old").is_some());
    index
        .rebuild_background_derived_artifacts(
            &busy,
            &LocalQosState::default(),
            &catalog,
            &store,
            SearchRebuildOptions::default(),
        )
        .unwrap();
    assert!(index.document("old").is_none());
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_search_projection_rebuild_releases_budget_on_rebuild_error() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("old", "Old projection", "Should stay", [1.0, 0.0]))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = index
        .rebuild_scheduled_background_derived_artifacts(
            &scheduler,
            &catalog,
            &store,
            SearchRebuildOptions { max_rows: Some(1) },
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(index.document_count(), 1);
    assert!(index.document("old").is_some());
}

#[test]
#[cfg(feature = "full-text-search")]
fn projection_delta_incrementally_updates_search_rows() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
        .unwrap();

    let report = index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Incremental FTS".to_string(),
                body: "Small batches keep embedded search cheap".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: vec!["memory:old".to_string()],
            max_operations: Some(2),
            source_graph_commit_epoch: None,
        })
        .unwrap();

    assert_eq!(report.action, "incremental_update");
    assert_eq!(report.before_document_count, 1);
    assert_eq!(report.after_document_count, 1);
    assert_eq!(report.upserted_documents, 1);
    assert_eq!(report.deleted_documents, 1);
    assert_eq!(report.operation_count, 2);
    assert_eq!(report.source_graph_commit_epoch_before, None);
    assert_eq!(report.source_graph_commit_epoch_after, None);
    assert!(!report.source_graph_commit_epoch_updated);
    assert!(index.document("memory:old").is_none());
    assert!(index.document("memory:new").is_some());
    let hits = index
        .search("embedded search", None, SearchMode::Text, 10)
        .unwrap();
    assert_eq!(hits[0].id, "memory:new");
}

#[test]
fn projection_delta_updates_source_graph_commit_epoch_when_provided() {
    let mut index = SearchIndex::in_memory();
    let report = index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Fresh graph delta".to_string(),
                body: "Graph derived projection delta advances freshness".to_string(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: Vec::new(),
            max_operations: Some(1),
            source_graph_commit_epoch: Some(7),
        })
        .unwrap();

    assert_eq!(report.source_graph_commit_epoch_before, None);
    assert_eq!(report.source_graph_commit_epoch_after, Some(7));
    assert!(report.source_graph_commit_epoch_updated);
    let freshness = index.projection_freshness();
    assert_eq!(freshness.source_graph_commit_epoch, Some(7));
    assert_eq!(freshness.durable_source_graph_commit_epoch, None);
    assert!(freshness.has_uncheckpointed_changes);
}

#[test]
fn import_provenance_is_independent_from_the_local_projection_cursor() {
    let root = unique_test_dir("import_provenance_is_independent_from_local_cursor");
    let mut index = SearchIndex::open(&root).unwrap();
    index.record_import_source_graph_commit_epoch(7).unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            source_graph_commit_epoch: Some(1),
            ..Default::default()
        })
        .unwrap();
    index.checkpoint().unwrap();
    let reopened = SearchIndex::open(&root).unwrap();
    let freshness = reopened.projection_freshness();
    assert_eq!(freshness.import_source_graph_commit_epoch, Some(7));
    assert_eq!(freshness.source_graph_commit_epoch, Some(1));
    assert_eq!(freshness.durable_source_graph_commit_epoch, Some(1));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn projection_checkpoint_advances_durable_watermark() {
    let path = unique_test_dir("durable_projection_watermark");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Durable graph delta".to_string(),
                body: "Checkpoint publishes the durable projection watermark".to_string(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: Vec::new(),
            max_operations: Some(1),
            source_graph_commit_epoch: Some(9),
        })
        .unwrap();

    assert_eq!(
        index
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        None
    );
    index.checkpoint().unwrap();
    let freshness = index.projection_freshness();
    assert_eq!(freshness.durable_source_graph_commit_epoch, Some(9));
    assert!(!freshness.has_uncheckpointed_changes);

    let reopened = SearchIndex::open(&path).unwrap();
    assert_eq!(
        reopened
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        Some(9)
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn projection_delta_background_work_plan_is_absent_without_operations() {
    let delta = SearchProjectionDelta::default();

    assert!(delta
        .background_work_plan(BackgroundWorkHint::default())
        .is_none());
}

#[test]
fn projection_delta_background_work_plan_uses_projection_lane() {
    let delta = SearchProjectionDelta {
        upserts: vec![SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "new".to_string(),
            title: "Incremental FTS".to_string(),
            body: "Small batches keep embedded search cheap".to_string(),
            embedding: Some(vec![0.0, 1.0]),
            source_id: None,
            metadata: BTreeMap::new(),
        }],
        deletes: vec!["memory:old".to_string()],
        max_operations: Some(2),
        source_graph_commit_epoch: None,
    };

    let plan = delta
        .background_work_plan(BackgroundWorkHint {
            recent_delta_operations: 2,
            ..BackgroundWorkHint::default()
        })
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 2);
    assert_eq!(plan.hint.recent_delta_operations, 2);
}

#[test]
fn projection_delta_budget_failure_keeps_existing_projection() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:old",
            "Old projection",
            "Should stay",
            [1.0, 0.0],
        ))
        .unwrap();

    let error = index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Too much work".to_string(),
                body: "Should not be applied".to_string(),
                embedding: Some(vec![0.0, 1.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: vec!["memory:old".to_string()],
            max_operations: Some(1),
            source_graph_commit_epoch: None,
        })
        .unwrap_err();

    assert!(error.to_string().contains("operation count 2"));
    assert_eq!(index.document_count(), 1);
    assert!(index.document("memory:old").is_some());
    assert!(index.document("memory:new").is_none());
}

#[test]
fn projection_delta_dimension_failure_keeps_existing_projection() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:old",
            "Old projection",
            "Should stay",
            [1.0, 0.0],
        ))
        .unwrap();

    let error = index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "new".to_string(),
                title: "Bad dimension".to_string(),
                body: "Should not be applied".to_string(),
                embedding: Some(vec![1.0, 0.0, 0.0]),
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            deletes: vec!["memory:old".to_string()],
            max_operations: Some(2),
            source_graph_commit_epoch: None,
        })
        .unwrap_err();

    assert!(error.to_string().contains("embedding dimension mismatch"));
    assert_eq!(index.document_count(), 1);
    assert!(index.document("memory:old").is_some());
    assert!(index.document("memory:new").is_none());
}

#[test]
#[cfg(feature = "background-maintenance")]
fn background_projection_delta_uses_qos_admission() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:old",
            "Old projection",
            "Should stay",
            [1.0, 0.0],
        ))
        .unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(1),
        ..LocalQosPolicy::default()
    };

    let error = index
        .apply_background_projection_delta(
            &policy,
            &LocalQosState::default(),
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Deferred projection".to_string(),
                    body: "Should not be applied".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap_err();

    assert_background_refusal(&error, 2, 1, false);
    assert_eq!(index.document_count(), 1);
    assert!(index.document("memory:old").is_some());
    assert!(index.document("memory:new").is_none());
}

#[test]
#[cfg(feature = "background-maintenance")]
fn background_projection_delta_applies_when_qos_admits() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
        .unwrap();

    let report = index
        .apply_background_projection_delta(
            &LocalQosPolicy::default(),
            &LocalQosState::default(),
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Admitted projection".to_string(),
                    body: "QoS admitted background work".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(report.operation_count, 2);
    assert!(index.document("memory:old").is_none());
    assert!(index.document("memory:new").is_some());
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_projection_delta_tracks_running_budget() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc("memory:old", "Old projection", "Remove me", [1.0, 0.0]))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(2),
        max_total_background_operations: Some(2),
        ..LocalQosPolicy::default()
    });

    let report = index
        .apply_scheduled_background_projection_delta(
            &scheduler,
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Scheduled projection".to_string(),
                    body: "QoS tracked background work".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap();

    assert_eq!(report.operation_count, 2);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(index.document("memory:old").is_none());
    assert!(index.document("memory:new").is_some());
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_projection_delta_defers_when_scheduler_is_full() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:old",
            "Old projection",
            "Should stay",
            [1.0, 0.0],
        ))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(4),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let running = scheduler
        .try_start(WorkRequest::background(WorkClass::Analytics, 3))
        .unwrap();

    let error = index
        .apply_scheduled_background_projection_delta(
            &scheduler,
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Deferred projection".to_string(),
                    body: "Should not be applied".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap_err();

    assert_background_refusal(&error, 2, 1, true);
    assert_eq!(scheduler.state().running_background_operations, 3);
    assert!(index.document("memory:old").is_some());
    assert!(index.document("memory:new").is_none());

    running.finish();
    assert_eq!(scheduler.state().running_background_operations, 0);
    index
        .apply_scheduled_background_projection_delta(
            &scheduler,
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Retried projection".to_string(),
                    body: "Applied once capacity frees".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(2),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap();
    assert!(index.document("memory:old").is_none());
    assert!(index.document("memory:new").is_some());
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_projection_delta_releases_budget_on_delta_error() {
    let mut index = SearchIndex::in_memory();
    index
        .upsert(doc(
            "memory:old",
            "Old projection",
            "Should stay",
            [1.0, 0.0],
        ))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = index
        .apply_scheduled_background_projection_delta(
            &scheduler,
            SearchProjectionDelta {
                upserts: vec![SearchProjectionRow {
                    kind: SearchProjectionKind::Memory,
                    external_id: "new".to_string(),
                    title: "Rejected by delta budget".to_string(),
                    body: "Should not be applied".to_string(),
                    embedding: Some(vec![0.0, 1.0]),
                    source_id: None,
                    metadata: BTreeMap::new(),
                }],
                deletes: vec!["memory:old".to_string()],
                max_operations: Some(1),
                source_graph_commit_epoch: None,
            },
        )
        .unwrap_err();

    assert_eq!(
        error,
        HawDBError::SearchProjectionBatchBudgetExceeded {
            budget: hawdb_core::SearchProjectionBatchBudget::ProjectionDelta,
            commit_epoch: None,
            required_operations: 2,
            limit: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "storage error: incremental projection update operation count 2 exceeded configured limit 1"
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(index.document("memory:old").is_some());
    assert!(index.document("memory:new").is_none());
}

#[test]
fn metadata_repair_updates_only_metadata() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
                ("space_id".to_string(), Value::String("default".to_string())),
                (
                    "source_id".to_string(),
                    Value::String("thread_1".to_string()),
                ),
            ]),
        )
        .unwrap();

    let path = unique_test_dir("metadata_repair_read_generation");
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: Some(vec![1.0, 0.0]),
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let mut index = SearchIndex::open(&path).unwrap();
    let old = index.read_snapshot();
    assert!(index.segment_descriptor.is_some());
    let summary = index
        .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.repaired_documents, 1);
    assert!(index.segment_descriptor.is_none());
    assert!(index.segment_payload_reader.is_none());
    assert_eq!(
        old.document("memory:mem_1").unwrap().metadata["kind"],
        "stale"
    );
    let document = index.document("memory:mem_1").unwrap();
    assert_eq!(document.title, "Old title");
    assert_eq!(document.content, "Old body should stay");
    assert_eq!(document.embedding, Some(vec![1.0, 0.0]));
    assert_eq!(
        document.metadata.get("kind").map(String::as_str),
        Some("memory")
    );
    assert_eq!(
        document.metadata.get("source_id").map(String::as_str),
        Some("thread_1")
    );
    assert_eq!(
        document.metadata.get("space_id").map(String::as_str),
        Some("default")
    );
    drop(old);
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn metadata_repair_corrects_segmented_lexical_statistics_from_original_documents() {
    let path = unique_test_dir("metadata_repair_lexical_statistics");
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }

    let mut index = SearchIndex::open(&path).unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "First document".to_string(),
            content: "Body".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:mem_2".to_string(),
            title: "Stale document".to_string(),
            content: "Body".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    index.checkpoint().unwrap();

    index
        .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
        .unwrap();
    let mut reference = SearchIndex::in_memory();
    for document in index.documents.values().cloned() {
        reference.upsert(document).unwrap();
    }

    let actual = index.search("stale", None, SearchMode::Text, 10).unwrap();
    let expected = reference
        .search("stale", None, SearchMode::Text, 10)
        .unwrap();
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].id, "memory:mem_2");
    assert_eq!(actual[0].text_score, expected[0].text_score);

    fs::remove_dir_all(path).unwrap();
}

#[test]
fn metadata_repair_background_work_plan_uses_projection_lane() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();
    let index = SearchIndex::in_memory();

    let plan = index
        .metadata_repair_background_work_plan(
            &store,
            BackgroundWorkHint {
                staleness_millis: 10_000,
                staleness_ttl_millis: Some(1_000),
                ..BackgroundWorkHint::default()
            },
        )
        .unwrap();

    assert_eq!(plan.request.class, WorkClass::Projection);
    assert_eq!(plan.request.estimated_operations, 1);
    assert_eq!(plan.hint.staleness_millis, 10_000);
}

#[test]
#[cfg(feature = "background-maintenance")]
fn background_metadata_repair_uses_qos_admission() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    let policy = LocalQosPolicy {
        max_background_operations: Some(0),
        ..LocalQosPolicy::default()
    };

    let error = index
        .repair_background_metadata_from_graph(
            &policy,
            &LocalQosState::default(),
            &catalog,
            &store,
            MetadataRepairOptions::default(),
            1,
        )
        .unwrap_err();

    assert_background_refusal(&error, 1, 0, false);
    assert_eq!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_metadata_repair_tracks_running_budget() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
                ("space_id".to_string(), Value::String("default".to_string())),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });

    let summary = index
        .repair_scheduled_background_metadata_from_graph(
            &scheduler,
            &catalog,
            &store,
            MetadataRepairOptions::default(),
            1,
        )
        .unwrap();

    assert_eq!(summary.repaired_documents, 1);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("memory")
    );
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_metadata_repair_defers_when_scheduler_is_full() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .upsert(SearchDocument {
            id: "memory:mem_1".to_string(),
            title: "Old title".to_string(),
            content: "Old body should stay".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
        })
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(4),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    });
    let running = scheduler
        .try_start(WorkRequest::background(WorkClass::Analytics, 3))
        .unwrap();

    let error = index
        .repair_scheduled_background_metadata_from_graph(
            &scheduler,
            &catalog,
            &store,
            MetadataRepairOptions::default(),
            2,
        )
        .unwrap_err();

    assert_background_refusal(&error, 2, 1, true);
    assert_eq!(scheduler.state().running_background_operations, 3);
    assert_eq!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );

    running.finish();
    assert_eq!(scheduler.state().running_background_operations, 0);
    index
        .repair_scheduled_background_metadata_from_graph(
            &scheduler,
            &catalog,
            &store,
            MetadataRepairOptions::default(),
            2,
        )
        .unwrap();
    assert_ne!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
    assert_eq!(scheduler.state().running_background_operations, 0);
}

#[test]
#[cfg(feature = "background-maintenance")]
fn scheduled_background_metadata_repair_releases_budget_on_repair_error() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }

    let mut index = SearchIndex::in_memory();
    for id in ["mem_1", "mem_2"] {
        index
            .upsert(SearchDocument {
                id: format!("memory:{id}"),
                title: id.to_string(),
                content: id.to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
    }
    let scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

    let error = index
        .repair_scheduled_background_metadata_from_graph(
            &scheduler,
            &catalog,
            &store,
            MetadataRepairOptions { max_rows: Some(1) },
            2,
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert_eq!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
}

#[test]
fn graph_projection_normalizes_default_space_metadata() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("missing_space".to_string())),
                (
                    "title".to_string(),
                    Value::String("Missing space".to_string()),
                ),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("empty_space".to_string())),
                (
                    "title".to_string(),
                    Value::String("Empty space".to_string()),
                ),
                ("space_id".to_string(), Value::String(String::new())),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::in_memory();
    index
        .rebuild_from_graph(&catalog, &store, SearchRebuildOptions::default())
        .unwrap();

    assert_eq!(
        index
            .document("memory:missing_space")
            .and_then(|document| document.metadata.get("space_id"))
            .map(String::as_str),
        Some(DEFAULT_SPACE_ID)
    );
    assert_eq!(
        index
            .document("memory:empty_space")
            .and_then(|document| document.metadata.get("space_id"))
            .map(String::as_str),
        Some(DEFAULT_SPACE_ID)
    );
}

#[test]
fn metadata_repair_limit_failure_keeps_existing_metadata() {
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    for id in ["mem_1", "mem_2"] {
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".to_string(), Value::String(id.to_string())),
                    ("title".to_string(), Value::String(id.to_string())),
                ]),
            )
            .unwrap();
    }

    let mut index = SearchIndex::in_memory();
    for id in ["mem_1", "mem_2"] {
        index
            .upsert(SearchDocument {
                id: format!("memory:{id}"),
                title: id.to_string(),
                content: id.to_string(),
                embedding: None,
                metadata: BTreeMap::from([("kind".to_string(), "stale".to_string())]),
            })
            .unwrap();
    }
    let error = index
        .repair_metadata_from_graph(
            &catalog,
            &store,
            MetadataRepairOptions { max_rows: Some(1) },
        )
        .unwrap_err();

    assert!(error.to_string().contains("row limit"));
    assert_eq!(
        index
            .document("memory:mem_1")
            .unwrap()
            .metadata
            .get("kind")
            .map(String::as_str),
        Some("stale")
    );
    let freshness = index.projection_freshness();
    assert!(freshness.metadata_repair_needed);
    assert_eq!(
        freshness.metadata_repair_reasons,
        vec!["metadata repair exceeded configured row limit".to_string()]
    );
}

#[test]
fn metadata_repair_marks_full_reindex_when_rows_are_missing() {
    let path = unique_test_dir("metadata_repair_missing_rows");
    let mut catalog = Catalog::default();
    let mut store = TestProjectionSource::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".to_string(), Value::String("mem_1".to_string())),
                (
                    "title".to_string(),
                    Value::String("Graph storage".to_string()),
                ),
            ]),
        )
        .unwrap();

    let mut index = SearchIndex::open(&path).unwrap();
    index
        .mark_metadata_repair_needed("missing metadata")
        .unwrap();
    let summary = index
        .repair_metadata_from_graph(&catalog, &store, MetadataRepairOptions::default())
        .unwrap();

    assert_eq!(summary.missing_documents, 1);
    assert!(index.full_reindex_needed());
    assert!(!index.metadata_repair_needed());
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn search_index_mutations_preserve_retained_lexical_delta_snapshots() {
    let path = unique_test_dir("retained_lexical_delta");
    let mut index = SearchIndex::open(&path).unwrap();
    let document = |content: &str| SearchDocument {
        id: "a".into(),
        title: String::new(),
        content: content.into(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    index.upsert(document("base graph")).unwrap();
    index.checkpoint().unwrap();
    index.upsert(document("alpha")).unwrap();
    let reader = index.lexical_projection.lock().unwrap().clone().unwrap();
    let snapshot = index.lexical_delta.lock().unwrap().clone();
    let terms = BTreeSet::from(["alpha".to_string()]);
    let expected = reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap();
    assert_eq!(expected.matching_document_count, 1);

    for content in [Some("beta"), None, Some("gamma")] {
        if let Some(content) = content {
            index.upsert(document(content)).unwrap();
        } else {
            index.delete("a");
        }
        assert_eq!(
            reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
            expected
        );
        let current = index.lexical_delta.lock().unwrap().clone();
        assert_eq!(
            reader
                .score(&terms, &current, None, |_| Ok(true))
                .unwrap()
                .matching_document_count,
            0
        );
    }
    index.checkpoint().unwrap();
    assert_eq!(
        reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
        expected
    );
    index.set_analyzer_lexicon(SearchAnalyzerLexicon::empty());
    assert_eq!(
        reader.score(&terms, &snapshot, None, |_| Ok(true)).unwrap(),
        expected
    );
    drop(reader);
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn segmented_lexical_projection_matches_reference_across_delta_and_reopen() {
    use std::io::{Read, Seek, SeekFrom, Write};

    let path = unique_test_dir("segmented_lexical_projection");
    let options = SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: Some(10),
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        policy_epoch: None,
    };
    let mut index = SearchIndex::open(&path).unwrap();
    let documents = [
        SearchDocument {
            id: "a".to_string(),
            title: "Graph Graph".to_string(),
            content: "storage engine".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchDocument {
            id: "b".to_string(),
            title: "Graph memory".to_string(),
            content: "retrieval".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchDocument {
            id: "hidden".to_string(),
            title: "Graph Graph Graph".to_string(),
            content: "private".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "private".to_string())]),
        },
    ];
    for document in documents.clone() {
        index.upsert(document).unwrap();
    }
    let reference = index
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    index.checkpoint().unwrap();
    let segmented = index
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    assert_eq!(
        segmented
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>(),
        reference
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>()
    );
    let text = segmented
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert!(text.segmented_lexical_projection_used);
    assert_eq!(text.backend, "segmented_bm25_text");
    assert!(text.posting_bytes_read > 0);
    assert!(text.candidate_postings_visited > 0);

    index
        .upsert(SearchDocument {
            id: "b".to_string(),
            title: "Vector memory".to_string(),
            content: "embedding".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();
    index.delete("a");
    index
        .upsert(SearchDocument {
            id: "c".to_string(),
            title: "Graph query".to_string(),
            content: "optimizer".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();
    let mut delta_reference = SearchIndex::in_memory();
    for document in index.documents.values().cloned() {
        delta_reference.upsert(document).unwrap();
    }
    let expected = delta_reference
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    let actual = index
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    assert_eq!(
        actual
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>()
    );

    index.checkpoint().unwrap();
    let generation = index
        .lexical_projection
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .generation();
    drop(index);
    let reopened = SearchIndex::open(&path).unwrap();
    let reopened_result = reopened
        .try_search_with_options("graph", None, SearchMode::Text, options)
        .unwrap();
    assert_eq!(
        reopened_result
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>()
    );
    drop(reopened);

    let artifact = path.join(lexical_projection::artifact_file(generation));
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&artifact)
        .unwrap();
    file.seek(SeekFrom::Start(32)).unwrap();
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(32)).unwrap();
    file.write_all(&[byte[0] ^ 0xff]).unwrap();
    file.sync_all().unwrap();
    let error = SearchIndex::open(&path).unwrap_err();
    assert!(error.to_string().contains("artifact checksum mismatch"));
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn multi_segment_bm25_scores_match_a_single_segment_baseline() {
    let baseline_path = unique_test_dir("single_segment_bm25_baseline");
    let segmented_path = unique_test_dir("multi_segment_bm25_differential");
    let documents = [
        SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "a-graph-storage".to_string(),
            title: "Graph Graph storage engine".to_string(),
            body: "durable graph storage".to_string(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "b-graph-query".to_string(),
            title: "Graph query planner".to_string(),
            body: "query graph graph optimization".to_string(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "c-storage-reader".to_string(),
            title: "Storage reader".to_string(),
            body: "bounded storage cache".to_string(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "d-private-graph".to_string(),
            title: "Graph Graph private".to_string(),
            body: "private graph material".to_string(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::from([("space_id".to_string(), "private".to_string())]),
        },
    ];
    let replacement = SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: "b-graph-query".to_string(),
        title: "Graph storage planner".to_string(),
        body: "durable graph storage update".to_string(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
    };
    let deleted_id = documents[3].clone().into_document().id;
    let options = SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: Some(10),
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let mut baseline_writer =
        SearchOutOfCoreGenerationWriter::create(&baseline_path, Default::default()).unwrap();
    for document in [
        documents[0].clone(),
        replacement.clone(),
        documents[2].clone(),
    ] {
        baseline_writer.push(document.into_document()).unwrap();
    }
    baseline_writer.finish().unwrap();

    let mut initial_writer =
        SearchOutOfCoreGenerationWriter::create(&segmented_path, Default::default()).unwrap();
    initial_writer
        .push(documents[0].clone().into_document())
        .unwrap();
    initial_writer.finish().unwrap();
    for document in documents.iter().skip(1).cloned() {
        let reader = SearchOutOfCoreReader::open(&segmented_path).unwrap();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts: vec![document],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
        update.finish().unwrap();
    }
    let reader = SearchOutOfCoreReader::open(&segmented_path).unwrap();
    assert_eq!(reader.artifact_count(), documents.len());
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![replacement],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    update.finish().unwrap();

    let reader = SearchOutOfCoreReader::open(&segmented_path).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            deletes: vec![deleted_id],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    update.finish().unwrap();

    let baseline = SearchOutOfCoreReader::open(&baseline_path).unwrap();
    let segmented = SearchOutOfCoreReader::open(&segmented_path).unwrap();
    assert_eq!(segmented.document_count(), 3);

    let filtered_options = SearchQueryOptions {
        metadata_filters: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        ..options.clone()
    };
    for (query, options) in [
        ("graph", options.clone()),
        ("graph storage", filtered_options),
        ("absent", options),
    ] {
        let expected = baseline
            .search_with_options(query, None, SearchMode::Text, options.clone())
            .unwrap()
            .result;
        let actual = segmented
            .search_with_options(query, None, SearchMode::Text, options)
            .unwrap()
            .result;
        assert_eq!(actual.total_hits, expected.total_hits, "query: {query}");
        assert_eq!(actual.truncated, expected.truncated, "query: {query}");
        assert_eq!(
            actual
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score, hit.text_rank))
                .collect::<Vec<_>>(),
            expected
                .hits
                .iter()
                .map(|hit| (&hit.id, hit.score, hit.text_rank))
                .collect::<Vec<_>>(),
            "query: {query}"
        );
    }

    drop(segmented);
    drop(baseline);
    fs::remove_dir_all(segmented_path).unwrap();
    fs::remove_dir_all(baseline_path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn segmented_lexical_projection_reopens_with_chinese_search_terms() {
    let path = unique_test_dir("segmented_lexical_projection_chinese");
    let term = "\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}";
    let mut index = SearchIndex::open(&path).unwrap();
    index
            .upsert(SearchDocument {
                id: "distributed".to_string(),
                title: "\u{73b0}\u{4ee3}\u{5206}\u{5e03}\u{5f0f}\u{7cfb}\u{7edf}\u{6570}\u{636e}\u{5e93}\u{8bbe}\u{8ba1}".to_string(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    index.checkpoint().unwrap();
    drop(index);

    let reopened = SearchIndex::open(&path).unwrap();
    let output = reopened
        .try_search_with_options(
            term,
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: SearchFusionWeights::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    let text = output
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert_eq!(output.hits[0].id, "distributed");
    assert!(output.hits[0]
        .matched_terms
        .iter()
        .any(|token| token == term));
    assert!(text.segmented_lexical_projection_used);
    drop(reopened);
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn segmented_lexical_topk_preserves_total_hits_and_page() {
    let path = unique_test_dir("segmented_lexical_topk");
    let mut reference = SearchIndex::in_memory();
    let mut index = SearchIndex::open(&path).unwrap();
    for number in 0..64 {
        let document = SearchDocument {
            id: format!("doc-{number:03}"),
            title: std::iter::repeat_n("graph", number % 4 + 1)
                .collect::<Vec<_>>()
                .join(" "),
            content: format!("storage document {number}"),
            embedding: None,
            metadata: BTreeMap::new(),
        };
        reference.upsert(document.clone()).unwrap();
        index.upsert(document).unwrap();
    }
    let options = SearchQueryOptions {
        limit: 3,
        offset: 2,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let expected = reference
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    index.checkpoint().unwrap();
    let actual = index
        .try_search_with_options("graph", None, SearchMode::Text, options)
        .unwrap();

    assert_eq!(actual.total_hits, 64);
    assert_eq!(actual.total_hits, expected.total_hits);
    assert_eq!(actual.hits.len(), 3);
    assert!(actual.truncated);
    assert_eq!(
        actual
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.score))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.score))
            .collect::<Vec<_>>()
    );
    let text = actual
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert_eq!(text.generated_candidate_count, 64);
    assert_eq!(text.candidate_count, 64);
    assert_eq!(text.candidate_set.cardinality, 5);
    assert!(text.segmented_lexical_projection_used);
    fs::remove_dir_all(path).unwrap();
}

#[test]
#[cfg(all(feature = "full-text-search", feature = "vector-search"))]
fn segmented_lexical_hybrid_rank_window_matches_reference() {
    let path = unique_test_dir("segmented_lexical_hybrid");
    let mut reference = SearchIndex::in_memory();
    let mut index = SearchIndex::open(&path).unwrap();
    for number in 0..12 {
        let document = SearchDocument {
            id: format!("doc-{number:02}"),
            title: std::iter::repeat_n("graph", number % 3 + 1)
                .collect::<Vec<_>>()
                .join(" "),
            content: format!("hybrid storage {number}"),
            embedding: Some(vec![number as f32, (12 - number) as f32]),
            metadata: BTreeMap::new(),
        };
        reference.upsert(document.clone()).unwrap();
        index.upsert(document).unwrap();
    }
    let options = SearchQueryOptions {
        limit: 5,
        offset: 0,
        rank_window: Some(3),
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let query_embedding = [1.0, 0.0];
    let expected = reference
        .try_search_with_options(
            "graph",
            Some(&query_embedding),
            SearchMode::Hybrid,
            options.clone(),
        )
        .unwrap();
    index.checkpoint().unwrap();
    let actual = index
        .try_search_with_options("graph", Some(&query_embedding), SearchMode::Hybrid, options)
        .unwrap();

    assert_eq!(actual.total_hits, expected.total_hits);
    assert_eq!(
        actual
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.score, hit.vector_rank, hit.text_rank,))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.score, hit.vector_rank, hit.text_rank,))
            .collect::<Vec<_>>()
    );
    let text = actual
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert_eq!(text.candidate_count, 12);
    assert_eq!(text.candidate_set.cardinality, 3);
    assert!(text.segmented_lexical_projection_used);
    fs::remove_dir_all(path).unwrap();
}

#[cfg(all(feature = "acl", feature = "full-text-search"))]
#[test]
fn resident_access_control_caller_mistakes_are_semantic_errors() {
    let mut index = SearchIndex::in_memory();
    index.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, true),
    );
    index
        .upsert(SearchDocument {
            id: "allowed".to_string(),
            title: "Graph".to_string(),
            content: "storage".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        })
        .unwrap();
    let options = |policy_epoch| SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch,
    };
    // A matching epoch and a valid context still search.
    index
        .try_search_with_options_access_control(
            "graph",
            None,
            SearchMode::Text,
            options(Some(7)),
            SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]),
        )
        .unwrap();
    // A mismatched epoch or a malformed context is the caller's mistake,
    // not a storage fault (#1767).
    for (policy_epoch, access_control) in SearchAccessControlContext::invalid_requests_for_test() {
        let error = index
            .try_search_with_options_access_control(
                "graph",
                None,
                SearchMode::Text,
                options(policy_epoch),
                access_control.clone(),
            )
            .unwrap_err();
        assert!(
            matches!(error, HawDBError::Semantic(_)),
            "{access_control:?}: {error:?}"
        );
    }
}

#[cfg(all(feature = "acl", feature = "full-text-search"))]
#[test]
fn segmented_lexical_acl_excludes_unauthorized_documents() {
    let path = unique_test_dir("segmented_lexical_acl");
    let mut expected_index = SearchIndex::in_memory();
    let mut index = SearchIndex::open(&path).unwrap();
    index.set_runtime_capabilities(
        RuntimeCapabilities::default().with(RuntimeCapability::AccessControl, true),
    );
    let allowed = [
        SearchDocument {
            id: "allowed-a".to_string(),
            title: "Graph graph".to_string(),
            content: "storage".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
        SearchDocument {
            id: "allowed-b".to_string(),
            title: "Graph".to_string(),
            content: "memory".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "team".to_string())]),
        },
    ];
    for document in allowed.clone() {
        expected_index.upsert(document.clone()).unwrap();
        index.upsert(document).unwrap();
    }
    index
        .upsert(SearchDocument {
            id: "hidden".to_string(),
            title: "Graph graph graph graph".to_string(),
            content: "private".to_string(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".to_string(), "private".to_string())]),
        })
        .unwrap();
    let options = SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: None,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let expected = expected_index
        .try_search_with_options("graph", None, SearchMode::Text, options.clone())
        .unwrap();
    index.checkpoint().unwrap();
    let actual = index
        .try_search_with_options_access_control(
            "graph",
            None,
            SearchMode::Text,
            options,
            SearchAccessControlContext::visibility_scopes(7, "space_id", ["team"]),
        )
        .unwrap();

    assert_eq!(actual.total_hits, 2);
    assert_eq!(actual.candidate_set.filtered_out_count, 1);
    assert_eq!(
        actual
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>(),
        expected
            .hits
            .iter()
            .map(|hit| (&hit.id, hit.text_rank))
            .collect::<Vec<_>>()
    );
    let text = actual
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert_eq!(text.candidate_count, 2);
    assert!(text.segmented_lexical_projection_used);
    fs::remove_dir_all(path).unwrap();
}

fn doc<const N: usize>(
    id: &str,
    title: &str,
    content: &str,
    embedding: [f32; N],
) -> SearchDocument {
    SearchDocument {
        id: id.to_string(),
        title: title.to_string(),
        content: content.to_string(),
        embedding: Some(embedding.to_vec()),
        metadata: BTreeMap::new(),
    }
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hawdb_search_{name}_{nanos}"))
}
