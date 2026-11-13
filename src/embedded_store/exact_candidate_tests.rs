// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-level exact vector search (#1692): snapshot, handle and projection
//! entry points score the complete filtered eligible set, match a brute-force
//! top-k bit for bit, report it, and refuse an inadmissible working budget.

use super::*;
use crate::search::{SearchProjectionKind, SearchProjectionRow};
use crate::{Database, DatabaseConfig, SearchIndex};
use std::num::NonZeroUsize;

pub(super) const DIMENSION: usize = 8;
const ROWS: usize = 480;
const LIMIT: usize = 10;

pub(super) struct Root(pub(super) PathBuf);

impl Root {
    pub(super) fn new(name: &str) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hawdb_exact_candidates_{name}_{}_{sequence}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn embedding(number: usize) -> Vec<f32> {
    let mut state = (number as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..DIMENSION)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 0.6
        })
        .collect()
}

/// Memory rows interleaved with source rows that a `kind=memory` filter must
/// exclude before top-k, plus a few memories without embeddings.
fn rows() -> Vec<SearchProjectionRow> {
    (0..ROWS)
        .map(|number| SearchProjectionRow {
            kind: if number.is_multiple_of(4) {
                SearchProjectionKind::Source
            } else {
                SearchProjectionKind::Memory
            },
            external_id: format!("{number:04}"),
            title: format!("note {number}"),
            body: if number.is_multiple_of(3) {
                "storage vectors".into()
            } else {
                "other words".into()
            },
            embedding: (number % 17 != 5).then(|| embedding(number)),
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .collect()
}

/// The facade's positive cosine relevance for sub-16-dimension vectors,
/// computed independently of the engine.
fn cosine(query: &[f32], embedding: &[f32]) -> Option<f64> {
    let dot = |left: &[f32], right: &[f32]| {
        left.iter().zip(right).fold(0.0f64, |sum, (left, right)| {
            sum + f64::from(*left) * f64::from(*right)
        })
    };
    let (left, right) = (dot(query, query), dot(embedding, embedding));
    if left == 0.0 || right == 0.0 {
        return None;
    }
    Some((dot(query, embedding) / (left.sqrt() * right.sqrt())).clamp(0.0, 1.0))
}

fn memory_scores(query: &[f32]) -> BTreeMap<String, f64> {
    rows()
        .into_iter()
        .filter(|row| row.kind == SearchProjectionKind::Memory)
        .filter_map(|row| {
            let score = cosine(query, row.embedding.as_deref()?)?;
            (score > 0.0).then(|| (format!("memory:{}", row.external_id), score))
        })
        .collect()
}

fn brute_force_top_k(query: &[f32], limit: usize) -> Vec<(String, u64)> {
    let mut scores = memory_scores(query).into_iter().collect::<Vec<_>>();
    scores.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    scores
        .into_iter()
        .take(limit)
        .map(|(id, score)| (id, score.to_bits()))
        .collect()
}

fn hits(output: &SearchCandidateOutput) -> Vec<(String, u64)> {
    output
        .accept_bounded()
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.vector_score.to_bits()))
        .collect()
}

pub(super) fn memory_filter() -> BTreeMap<String, String> {
    BTreeMap::from([("kind".into(), "memory".into())])
}

pub(super) fn governor() -> hawdb_qos::RuntimeGovernor {
    use hawdb_qos::{RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot};
    hawdb_qos::RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(8 << 30), None, None, None),
        ),
        hawdb_qos::IoConcurrencyBudget::new(8, 2),
    )
}

fn resident_projection(root: &Root, commit_epoch: u64) -> SearchProjection {
    let mut index = SearchIndex::open(root.0.join("resident")).unwrap();
    index
        .apply_projection_delta(crate::SearchProjectionDelta {
            upserts: rows(),
            source_graph_commit_epoch: Some(commit_epoch),
            ..Default::default()
        })
        .unwrap();
    index.checkpoint().unwrap();
    SearchProjection::from_index(index)
}

fn out_of_core_projection(root: &Root, commit_epoch: u64) -> OutOfCoreSearchProjection {
    let path = root.0.join("out-of-core");
    let mut writer = crate::SearchOutOfCoreGenerationWriter::create(
        &path,
        crate::SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(commit_epoch),
            embedding_manifest: Some(crate::SearchEmbeddingManifest {
                model: "exact-candidate-fixture".into(),
                version: Some("1".into()),
                dimension: DIMENSION,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let mut documents = rows()
        .into_iter()
        .map(SearchProjectionRow::into_document)
        .collect::<Vec<_>>();
    documents.sort_by(|left, right| left.id.cmp(&right.id));
    for document in documents {
        writer.push(document).unwrap();
    }
    writer.finish().unwrap();
    OutOfCoreSearchProjection::open(&path).unwrap()
}

pub(super) fn handle(
    root: &Root,
    out_of_core: bool,
    config: DatabaseConfig,
) -> EmbeddedStoreHandle {
    let mut database = Database::new_with_config(config);
    database
        .query("CREATE (:Memory {id: '0001', title: 'Canonical memory'})")
        .unwrap();
    let commit_epoch = database.commit_epoch().unwrap();
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        database,
        GraphMode::ShadowReadOnly,
        governor(),
    );
    let store = if out_of_core {
        EmbeddedStore::new_with_out_of_core_search(
            graph,
            out_of_core_projection(root, commit_epoch),
            Default::default(),
        )
    } else {
        EmbeddedStore::new(graph, Some(resident_projection(root, commit_epoch)))
    };
    EmbeddedStoreHandle::new(store).unwrap()
}

pub(super) fn read_config() -> DatabaseConfig {
    DatabaseConfig {
        max_read_result_rows: Some(64),
        max_read_result_payload_bytes: Some(256 * 1024),
        ..Default::default()
    }
}

pub(super) fn snapshot_search(
    handle: &EmbeddedStoreHandle,
    request: &SearchCandidateRequest,
) -> Result<SearchCandidateOutput> {
    handle.with_bounded_read_snapshot(
        EmbeddedReadSnapshotBudget {
            max_rows: 32,
            max_payload_bytes: 128 * 1024,
        },
        |snapshot| snapshot.search_candidates_with_report(request),
    )
}

fn vector_source(output: &SearchCandidateOutput) -> &str {
    output.report().retriever_candidate_score_sources["vector"].as_str()
}

fn assert_exact_report(output: &SearchCandidateOutput, scalar_backend: &str) {
    assert!(output.report().exact_vector_search_requested);
    assert!(output.report().exact_vector_search);
    assert_eq!(vector_source(output), "raw_vector");
    assert_eq!(output.report().retriever_backends["vector"], scalar_backend);
    assert_eq!(output.report().metadata_filter_count, 1);
    let json = output.report().json();
    assert_eq!(json["exact_vector_search_requested"], true);
    assert_eq!(json["exact_vector_search"], true);
}

fn queries() -> [Vec<f32>; 4] {
    [
        embedding(1_001),
        embedding(1_077),
        embedding(1_150),
        embedding(1_299),
    ]
}

#[test]
fn snapshot_exact_vector_candidates_match_brute_force_bits_over_graph_projections() {
    for (out_of_core, default_source, scalar_backend) in [
        (false, "raw_vector", "scalar_vector_scan"),
        (true, "graph_distance", "scalar_vector_segment_scan"),
    ] {
        let root = Root::new(if out_of_core { "ooc" } else { "resident" });
        let handle = handle(&root, out_of_core, read_config());
        for query in queries() {
            let expected = brute_force_top_k(&query, LIMIT);
            assert_eq!(expected.len(), LIMIT);
            let default_request = SearchCandidateRequest::vector(query.clone(), LIMIT)
                .with_metadata_filters(memory_filter());
            let default = snapshot_search(&handle, &default_request).unwrap();
            assert!(!default.report().exact_vector_search_requested);
            assert_eq!(vector_source(&default), default_source, "{out_of_core}");
            if out_of_core {
                // Out-of-core default execution uses approximate graph candidates.
                assert!(!default.report().exact_vector_search);
            } else {
                // Resident candidate reads with compression disabled already
                // select the adaptive scalar scan, and the report says so.
                assert!(default.report().exact_vector_search);
                assert_eq!(
                    default.report().retriever_backend_selection_reasons["vector"],
                    "compression_disabled"
                );
                assert_eq!(hits(&default), expected);
            }

            let exact_request = default_request.clone().with_exact_vector_search();
            let exact = snapshot_search(&handle, &exact_request).unwrap();
            assert_eq!(hits(&exact), expected, "out_of_core={out_of_core}");
            assert!(exact
                .accept_bounded()
                .hits
                .iter()
                .all(|hit| hit.kind.as_deref() == Some("memory")));
            assert_exact_report(&exact, scalar_backend);

            // The handle-level admitted read applies the same request flag.
            let handle_exact = handle
                .search_candidates_with_report(&exact_request)
                .unwrap();
            assert_eq!(hits(&handle_exact), expected);
            assert_exact_report(&handle_exact, scalar_backend);
        }
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn snapshot_exact_hybrid_search_uses_the_exact_vector_leg() {
    for (out_of_core, default_source, scalar_backend) in [
        (false, "raw_vector", "scalar_vector_scan"),
        (true, "graph_distance", "scalar_vector_segment_scan"),
    ] {
        let root = Root::new(if out_of_core { "ooc-hybrid" } else { "hybrid" });
        let handle = handle(&root, out_of_core, read_config());
        for query in queries() {
            let request = SearchCandidateRequest::hybrid("storage", query.clone(), 20)
                .with_metadata_filters(memory_filter());
            let default = snapshot_search(&handle, &request).unwrap();
            assert_eq!(vector_source(&default), default_source);
            assert_eq!(default.report().exact_vector_search, !out_of_core);
            let exact = snapshot_search(&handle, &request.with_exact_vector_search()).unwrap();
            assert_exact_report(&exact, scalar_backend);
            assert!(!exact.accept_bounded().hits.is_empty());
            let scores = memory_scores(&query);
            for hit in &exact.accept_bounded().hits {
                assert_eq!(hit.kind.as_deref(), Some("memory"));
                let expected = scores.get(&hit.id).copied().unwrap_or(0.0);
                assert_eq!(hit.vector_score.to_bits(), expected.to_bits(), "{}", hit.id);
            }
            // Vector ranks are global ranks in the exact brute-force order.
            let order = brute_force_top_k(&query, usize::MAX)
                .into_iter()
                .enumerate()
                .map(|(index, (id, _))| (id, index + 1))
                .collect::<BTreeMap<_, _>>();
            let mut ranked = 0;
            for hit in &exact.accept_bounded().hits {
                if let Some(rank) = hit.vector_rank {
                    assert_eq!(Some(&rank), order.get(&hit.id), "{}", hit.id);
                    ranked += 1;
                }
            }
            assert!(ranked > 0);
        }
    }
}

#[test]
fn projection_exact_entry_points_match_brute_force_and_refuse_small_budgets() {
    let root = Root::new("projection");
    let query = embedding(2_024);
    let expected = brute_force_top_k(&query, LIMIT);
    let request = SearchCandidateRequest::vector(query, LIMIT)
        .with_metadata_filters(memory_filter())
        .with_exact_vector_search();
    let tiny = VectorSearchExecutionOptions::bounded(NonZeroUsize::MIN, 4096, None);

    let resident = resident_projection(&root, 1);
    for output in [
        resident.search_candidates_with_report(&request).unwrap(),
        resident
            .try_search_candidates_with_report(&request)
            .unwrap(),
        resident
            .try_search_candidates_with_report_context(
                &request,
                VectorSearchExecutionOptions::default(),
            )
            .unwrap(),
    ] {
        assert_eq!(hits(&output), expected);
        assert_exact_report(&output, "scalar_vector_scan");
    }
    let refused = resident
        .try_search_candidates_with_report_context(&request, tiny)
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("exceeding admitted working memory 4096"),
        "{refused}"
    );

    let out_of_core = out_of_core_projection(&root, 1);
    for output in [
        out_of_core.search_candidates_with_report(&request).unwrap(),
        out_of_core
            .search_candidates_with_report_context(
                &request,
                VectorSearchExecutionOptions::default(),
            )
            .unwrap(),
    ] {
        assert_eq!(hits(&output), expected);
        assert_exact_report(&output, "scalar_vector_segment_scan");
    }
    let refused = out_of_core
        .search_candidates_with_report_context(&request, tiny)
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("search vector uniqueness check requires more than 16 document ids"),
        "{refused}"
    );
}

#[test]
fn snapshot_exact_search_refuses_when_admitted_memory_cannot_hold_the_scan() {
    for out_of_core in [false, true] {
        let root = Root::new(if out_of_core { "ooc-budget" } else { "budget" });
        let mut config = read_config();
        config.execution_memory.query_memory_bytes = NonZeroUsize::new(4096).unwrap();
        let handle = handle(&root, out_of_core, config);
        let request = SearchCandidateRequest::vector(embedding(3_003), LIMIT)
            .with_metadata_filters(memory_filter())
            .with_exact_vector_search();
        let error = snapshot_search(&handle, &request).unwrap_err().to_string();
        // Out of core, the query-memory account refuses first (#1722).
        assert!(
            error.contains("exceeding admitted working memory")
                || (out_of_core && error.contains("exceeding its 4096-byte budget")),
            "out_of_core={out_of_core}: {error}"
        );
    }
}

#[test]
fn exact_search_refuses_an_effective_compressed_mode() {
    let root = Root::new("compressed");
    let handle = handle(&root, false, read_config());
    let ready = RetrievalProjectionAdvisor {
        recall_evidence_ready: true,
        recall_evidence_protocol: Some(crate::VECTOR_RECALL_VALIDATION_PROTOCOL.to_string()),
        recall_sample_count: 1,
        recall_at_k_per_million: Some(1_000_000),
        parity_evidence_ready: true,
        cold_or_constrained_local_segment: true,
    };
    let request = SearchCandidateRequest::vector(embedding(4_004), LIMIT)
        .with_compressed_vector_search_mode(CompressedVectorSearchMode::Required)
        .with_retrieval_projection_advisor(ready)
        .with_exact_vector_search();
    let error = snapshot_search(&handle, &request).unwrap_err().to_string();
    assert!(error.contains("exact vector search cannot use"), "{error}");
}
