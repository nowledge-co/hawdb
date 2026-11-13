// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Embedded out-of-core candidate reads charge every eligible-set-scaled
//! allocation to the caller's admitted working memory and refuse, with a typed
//! budget error, a request that does not fit (#1722). A hybrid request without
//! a rank window ranks the complete eligible set in both legs.

use super::exact_candidate_tests::{
    embedding, handle, memory_filter, read_config, snapshot_search, Root,
};
use super::*;
use hawdb_core::ResourceBudgetKind;
use std::num::NonZeroUsize;

const LIMIT: usize = 10;
const LARGE_BUDGET: usize = 256 << 20;

fn requests() -> Vec<(&'static str, SearchCandidateRequest)> {
    let query = embedding(5_005);
    let text = SearchCandidateRequest::text("storage", LIMIT);
    let vector = SearchCandidateRequest::vector(query.clone(), LIMIT);
    let hybrid = SearchCandidateRequest::hybrid("storage", query, LIMIT);
    [
        ("text", text),
        ("vector", vector.clone()),
        ("vector exact", vector.with_exact_vector_search()),
        ("hybrid", hybrid.clone()),
        ("hybrid exact", hybrid.with_exact_vector_search()),
    ]
    .into_iter()
    .map(|(name, request)| (name, request.with_metadata_filters(memory_filter())))
    .collect()
}

fn store(name: &str, budget: usize) -> (Root, EmbeddedStoreHandle) {
    let root = Root::new(name);
    let mut config = read_config();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(budget).unwrap();
    let handle = handle(&root, true, config);
    (root, handle)
}

fn charged_bytes(output: &SearchCandidateOutput) -> u64 {
    output
        .out_of_core_metrics()
        .expect("out-of-core metrics")
        .query_memory_peak_bytes
}

#[test]
fn snapshot_candidate_reads_charge_admitted_working_memory() {
    let (_root, handle) = store("ooc-working-large", LARGE_BUDGET);
    let mut hybrid_exact = 0;
    for (name, request) in requests() {
        let output = snapshot_search(&handle, &request).unwrap();
        assert_eq!(output.accept_bounded().hits.len(), LIMIT, "{name}");
        let charged = charged_bytes(&output);
        assert!(
            charged > 0 && charged <= LARGE_BUDGET as u64,
            "{name}: {charged}"
        );
        if name == "hybrid exact" {
            hybrid_exact = charged;
        }
        // The admitted handle read charges the same account kind.
        let output = handle.search_candidates_with_report(&request).unwrap();
        assert!(charged_bytes(&output) > 0, "{name}");
    }
    // The unwindowed hybrid working set exceeds every page-bounded read's.
    assert!(
        hybrid_exact > page_bounded_admission(&handle) as u64,
        "{hybrid_exact}"
    );
}

/// The smallest admission serving text, exact vector-only and windowed exact
/// hybrid reads. Default vector stages size themselves to the remaining bytes.
fn page_bounded_admission(handle: &EmbeddedStoreHandle) -> usize {
    requests()
        .into_iter()
        .filter(|(name, _)| matches!(*name, "text" | "vector exact" | "hybrid exact"))
        .map(|(name, request)| {
            let request = if name == "hybrid exact" {
                request.with_rank_window(Some(LIMIT))
            } else {
                request
            };
            charged_bytes(&snapshot_search(handle, &request).unwrap()) as usize
        })
        .max()
        .unwrap()
}

#[test]
fn snapshot_hybrid_without_rank_window_is_refused_above_admitted_working_memory() {
    let small = {
        let (_root, handle) = store("ooc-working-measure", LARGE_BUDGET);
        page_bounded_admission(&handle)
    };
    let (_root, handle) = store("ooc-working-small", small);
    for (name, request) in requests() {
        let result = snapshot_search(&handle, &request);
        if name == "hybrid exact" {
            match result {
                Err(HawDBError::ResourceBudgetExceeded(error)) => {
                    assert_eq!(error.resource, ResourceBudgetKind::MemoryBytes, "{error}");
                    assert_eq!(error.budget, small as u64, "{error}");
                }
                other => panic!("hybrid exact served above its admission: {other:?}"),
            }
            // A rank window bounds both rankings and fits the same admission.
            let windowed =
                snapshot_search(&handle, &request.with_rank_window(Some(LIMIT))).unwrap();
            assert_eq!(windowed.accept_bounded().hits.len(), LIMIT);
            assert!(charged_bytes(&windowed) <= small as u64);
        } else if name != "hybrid" {
            // Text and vector-only reads are unaffected at this admission.
            let output = result.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(output.accept_bounded().hits.len(), LIMIT, "{name}");
            assert!(charged_bytes(&output) <= small as u64, "{name}");
        } else if let Ok(output) = result {
            // Default hybrid retains only the bounded graph window in its vector leg.
            assert!(charged_bytes(&output) <= small as u64);
        }
    }
}

/// Exact vector-only capacity at an 8 MiB snapshot admission: the duplicate-ID
/// cap (`8 MiB / 256`) binds, independently of the embedding dimension. The
/// capacity is served; a larger eligible set is refused with a typed error.
#[test]
fn snapshot_exact_vector_capacity_is_served_and_refused_above() {
    use crate::search::{SearchProjectionKind, SearchProjectionRow};
    const BUDGET: usize = 8 << 20;
    const CAPACITY: usize = BUDGET / 256;
    const ABOVE: usize = CAPACITY + 1;
    for dimension in [384, 1024] {
        let root = Root::new("ooc-exact-capacity");
        let path = root.0.join("out-of-core");
        let mut writer = crate::SearchOutOfCoreGenerationWriter::create(
            &path,
            crate::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(1),
                embedding_manifest: Some(crate::SearchEmbeddingManifest {
                    model: "capacity".into(),
                    version: Some("1".into()),
                    dimension,
                }),
                vector_backend: None,
                ..Default::default()
            },
        )
        .unwrap();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for number in 0..ABOVE {
            let embedding = (0..dimension)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 40) as f32 / (1u64 << 23) as f32 - 0.6
                })
                .collect();
            let tier = if number < CAPACITY {
                "capacity"
            } else {
                "above"
            };
            writer
                .push(
                    SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: format!("{number:06}"),
                        title: String::new(),
                        body: "vectors".into(),
                        embedding: Some(embedding),
                        source_id: None,
                        metadata: BTreeMap::from([("tier".to_string(), tier.to_string())]),
                    }
                    .into_document(),
                )
                .unwrap();
        }
        writer.finish().unwrap();
        let mut config = read_config();
        config.execution_memory.query_memory_bytes = NonZeroUsize::new(BUDGET).unwrap();
        let mut database = crate::Database::new_with_config(config);
        database.query("CREATE (:Memory {id: '000001'})").unwrap();
        let graph = EmbeddedGraph::from_database_with_runtime_governor(
            database,
            GraphMode::ShadowReadOnly,
            super::exact_candidate_tests::governor(),
        );
        let handle = EmbeddedStoreHandle::new(EmbeddedStore::new_with_out_of_core_search(
            graph,
            OutOfCoreSearchProjection::open(&path).unwrap(),
            Default::default(),
        ))
        .unwrap();
        let id_bytes = "memory:000000".len();
        let capacity = handle
            .with_bounded_read_snapshot(
                EmbeddedReadSnapshotBudget {
                    max_rows: 32,
                    max_payload_bytes: 128 * 1024,
                },
                |snapshot| snapshot.exact_vector_search_capacity(id_bytes),
            )
            .unwrap();
        assert_eq!(capacity, CAPACITY, "dimension {dimension}");
        let query = (0..dimension)
            .map(|index| (index as f32 * 0.37).sin())
            .collect::<Vec<_>>();
        let request = |tier: Option<&str>| {
            let mut filters = memory_filter();
            if let Some(tier) = tier {
                filters.insert("tier".into(), tier.into());
            }
            SearchCandidateRequest::vector(query.clone(), LIMIT)
                .with_metadata_filters(filters)
                .with_exact_vector_search()
        };
        let served = snapshot_search(&handle, &request(Some("capacity"))).unwrap();
        assert_eq!(
            served.accept_bounded().hits.len(),
            LIMIT,
            "dimension {dimension}"
        );
        assert!(charged_bytes(&served) <= BUDGET as u64);
        match snapshot_search(&handle, &request(None)) {
            Err(HawDBError::ResourceBudgetExceeded(error)) => {
                assert_eq!(
                    error.resource,
                    ResourceBudgetKind::MetadataEntries,
                    "{error}"
                );
                assert_eq!(error.budget, CAPACITY as u64, "{error}");
            }
            other => panic!("dimension {dimension}: {ABOVE} eligible served: {other:?}"),
        }
    }
}

/// The resident path has its own exact charges; the same snapshot method
/// reports them. At an admission whose capacity is exactly the eligible set,
/// the read is served untruncated; one more eligible document is refused with
/// a typed `MemoryBytes` budget error (#1748).
#[test]
fn snapshot_exact_vector_capacity_is_served_and_refused_above_for_resident_reads() {
    use super::exact_candidate_tests::{
        assert_budget_refusal, graph, resident_projection_with_rows,
    };
    use crate::search::{SearchProjectionKind, SearchProjectionRow};
    const CAPACITY: usize = 1_500;
    let id_bytes = "memory:000000".len();
    // `tier=capacity` selects exactly CAPACITY of the CAPACITY + 1 memories.
    let rows = (0..=CAPACITY)
        .map(|number| SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: format!("{number:06}"),
            title: String::new(),
            body: "vectors".into(),
            embedding: Some(embedding(number)),
            source_id: None,
            metadata: BTreeMap::from([(
                "tier".to_string(),
                if number < CAPACITY {
                    "capacity"
                } else {
                    "above"
                }
                .to_string(),
            )]),
        })
        .collect::<Vec<_>>();
    let root = Root::new("resident-capacity");
    let projection = resident_projection_with_rows(&root, 1, rows);
    // The smallest admission whose capacity covers CAPACITY eligible documents.
    let capacity = |bytes: usize| {
        projection
            .index()
            .exact_vector_search_capacity(bytes, id_bytes)
    };
    let (mut low, mut high) = (1usize, 1usize << 30);
    while low < high {
        let middle = low + (high - low) / 2;
        if capacity(middle) >= CAPACITY {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    let budget = low;
    assert_eq!(capacity(budget), CAPACITY);
    let mut config = read_config();
    config.execution_memory.query_memory_bytes = NonZeroUsize::new(budget).unwrap();
    let (graph, _) = graph(config);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(graph, Some(projection))).unwrap();
    let request = |tier: Option<&str>| {
        let mut filters = memory_filter();
        if let Some(tier) = tier {
            filters.insert("tier".into(), tier.into());
        }
        SearchCandidateRequest::vector(embedding(6_006), LIMIT)
            .with_metadata_filters(filters)
            .with_exact_vector_search()
    };
    let (snapshot_capacity, served, refused) = handle
        .with_bounded_read_snapshot(
            EmbeddedReadSnapshotBudget {
                max_rows: 32,
                max_payload_bytes: 128 * 1024,
            },
            |snapshot| {
                Ok((
                    snapshot.exact_vector_search_capacity(id_bytes)?,
                    snapshot.search_candidates_with_report(&request(Some("capacity"))),
                    snapshot.search_candidates_with_report(&request(None)),
                ))
            },
        )
        .unwrap();
    assert_eq!(snapshot_capacity, CAPACITY);
    let served = served.unwrap();
    assert_eq!(served.accept_bounded().hits.len(), LIMIT);
    assert_eq!(served.accept_bounded().candidate_set.cardinality, CAPACITY);
    assert!(served.report().exact_vector_search);
    assert_budget_refusal(
        &refused.unwrap_err(),
        ResourceBudgetKind::MemoryBytes,
        budget as u64,
    );
}
