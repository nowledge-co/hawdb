// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Ordinary Search ranks every vector path by the projection's own metric.
//!
//! The fixture mixes vector norms so cosine order and Euclidean order differ,
//! and it places near-origin vectors opposite the query: they are Euclidean
//! nearest neighbors with negative cosine similarity.

use super::*;
use hawdb_core::{
    VectorDimension, VectorDistance, VectorIndexBackend, VectorIndexDescriptor, VectorIndexTarget,
};

const DIMENSION: usize = 24;
const DOCUMENTS: usize = 400;
const OPPOSITE: usize = 6;
const TOP_K: usize = 10;

struct Directory(PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn directory(name: &str) -> Directory {
    Directory(test_temp_dir().join(format!(
        "hawdb-search-vector-metric-{name}-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )))
}

struct Random(u64);

impl Random {
    fn unit(&mut self) -> f32 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }

    fn direction(&mut self) -> Vec<f32> {
        let vector = (0..DIMENSION).map(|_| self.unit()).collect::<Vec<_>>();
        let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        vector.into_iter().map(|value| value / norm).collect()
    }
}

struct Fixture {
    query: Vec<f32>,
    documents: Vec<(String, Vec<f32>)>,
}

fn fixture() -> Fixture {
    let mut random = Random(0x1467);
    let query = random
        .direction()
        .into_iter()
        .map(|value| value * 0.5)
        .collect::<Vec<_>>();
    let mut documents = Vec::with_capacity(DOCUMENTS);
    for number in 0..DOCUMENTS {
        let embedding = if number < OPPOSITE {
            // Tiny vectors pointing away from the query: L2-near, cosine < 0.
            query
                .iter()
                .map(|value| -value * 0.04 + random.unit() * 0.001)
                .collect()
        } else {
            let norm = 0.2 + 4.8 * (random.unit() + 1.0) / 2.0;
            random
                .direction()
                .into_iter()
                .map(|value| value * norm)
                .collect()
        };
        documents.push((format!("doc:{number:04}"), embedding));
    }
    Fixture { query, documents }
}

fn l2(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            delta * delta
        })
        .sum::<f64>()
        .sqrt()
}

fn cosine(left: &[f32], right: &[f32]) -> f64 {
    let dot = left
        .iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum::<f64>();
    let norm = |vector: &[f32]| {
        vector
            .iter()
            .map(|value| f64::from(*value) * f64::from(*value))
            .sum::<f64>()
            .sqrt()
    };
    dot / (norm(left) * norm(right))
}

impl Fixture {
    fn l2_top(&self, k: usize) -> Vec<String> {
        let mut ranked = self
            .documents
            .iter()
            .map(|(id, embedding)| (l2(&self.query, embedding), id.clone()))
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
        ranked.into_iter().take(k).map(|(_, id)| id).collect()
    }

    fn cosine_top(&self, k: usize) -> Vec<String> {
        let mut ranked = self
            .documents
            .iter()
            .map(|(id, embedding)| (cosine(&self.query, embedding), id.clone()))
            .filter(|(score, _)| *score > 0.0)
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| right.0.total_cmp(&left.0).then(left.1.cmp(&right.1)));
        ranked.into_iter().take(k).map(|(_, id)| id).collect()
    }

    fn embedding(&self, id: &str) -> &[f32] {
        &self
            .documents
            .iter()
            .find(|(candidate, _)| candidate == id)
            .unwrap()
            .1
    }

    fn document(&self, (id, embedding): &(String, Vec<f32>)) -> SearchDocument {
        SearchDocument {
            id: id.clone(),
            title: String::new(),
            content: String::new(),
            embedding: Some(embedding.clone()),
            metadata: BTreeMap::new(),
        }
    }

    fn assert_valid(&self) {
        let l2 = self.l2_top(TOP_K);
        assert_ne!(l2, self.cosine_top(TOP_K), "fixture must separate metrics");
        assert!(
            l2.iter()
                .any(|id| cosine(&self.query, self.embedding(id)) < 0.0),
            "fixture must place a negative-cosine vector in the L2 top-k"
        );
    }
}

fn descriptor(distance: VectorDistance) -> VectorIndexDescriptor {
    VectorIndexDescriptor::new(
        "vectors.embedding".into(),
        VectorIndexTarget::Relational {
            table: "vectors".into(),
            column: "embedding".into(),
            column_ordinal: 1,
        },
        VectorDimension::new(DIMENSION as u32).unwrap(),
        distance,
        VectorIndexBackend::default(),
        [3; 32],
    )
    .unwrap()
}

fn options(limit: usize, rank_window: Option<usize>) -> SearchQueryOptions {
    SearchQueryOptions {
        limit,
        offset: 0,
        rank_window,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

fn ids(hits: &[SearchHit]) -> Vec<String> {
    hits.iter().map(|hit| hit.id.clone()).collect()
}

/// Euclidean relevance is `1 / (1 + distance)`: positive, at most one and
/// strictly decreasing in distance, so descending score is ascending distance.
fn assert_l2_hits(fixture: &Fixture, hits: &[SearchHit], path: &str) {
    assert_eq!(ids(hits), fixture.l2_top(TOP_K), "{path}");
    for hit in hits {
        let expected = 1.0 / (1.0 + l2(&fixture.query, fixture.embedding(&hit.id)));
        assert!((hit.score - expected).abs() < 1e-9, "{path} {}", hit.id);
        assert!(
            (hit.vector_score - expected).abs() < 1e-9,
            "{path} {}",
            hit.id
        );
    }
}

fn assert_cosine_hits(fixture: &Fixture, hits: &[SearchHit], path: &str) {
    assert_eq!(ids(hits), fixture.cosine_top(TOP_K), "{path}");
    for hit in hits {
        let expected = cosine(&fixture.query, fixture.embedding(&hit.id));
        assert!(expected > 0.0, "{path} {}", hit.id);
        assert!((hit.score - expected).abs() < 1e-6, "{path} {}", hit.id);
        assert!(
            (hit.vector_score - expected).abs() < 1e-6,
            "{path} {}",
            hit.id
        );
    }
}

fn write_generation(root: &Path, fixture: &Fixture, distance: Option<VectorDistance>) {
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(1),
            vector_backend: Some(VectorIndexBackend::default()),
            vector_index_descriptor: distance.map(descriptor),
            ..Default::default()
        },
    )
    .unwrap();
    for document in &fixture.documents {
        writer.push(fixture.document(document)).unwrap();
    }
    writer.finish().unwrap();
}

#[test]
fn out_of_core_euclidean_projection_matches_brute_force_l2() {
    let fixture = fixture();
    fixture.assert_valid();
    let root = directory("out-of-core-l2");
    write_generation(&root.0, &fixture, Some(VectorDistance::Euclidean));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let query = Some(fixture.query.as_slice());

    let exact = reader
        .search_with_options_exact("", query, SearchMode::Vector, options(TOP_K, None))
        .unwrap();
    assert_l2_hits(&fixture, &exact.result.hits, "out-of-core exact");

    // A window covering the corpus makes graph navigation exhaustive, so any
    // mismatch is a final-ranking error rather than ANN recall.
    let graph = reader
        .search_with_options(
            "",
            query,
            SearchMode::Vector,
            options(TOP_K, Some(DOCUMENTS)),
        )
        .unwrap();
    let vector = graph
        .result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "vector")
        .unwrap();
    assert_eq!(vector.backend, "hawdb_diskann_out_of_core_candidate_graph");
    assert_l2_hits(&fixture, &graph.result.hits, "out-of-core diskann");

    // Hybrid fusion consumes the same Euclidean vector ranks. Hybrid needs
    // full-text search, which the vector-only lanes build without.
    #[cfg(feature = "full-text-search")]
    {
        let hybrid = reader
            .search_with_options(
                "",
                query,
                SearchMode::Hybrid,
                options(TOP_K, Some(DOCUMENTS)),
            )
            .unwrap();
        assert_eq!(ids(&hybrid.result.hits), fixture.l2_top(TOP_K));
    }
}

#[test]
fn resident_euclidean_index_matches_brute_force_l2() {
    let fixture = fixture();
    let mut index = SearchIndex::in_memory();
    index
        .bind_vector_index(descriptor(VectorDistance::Euclidean))
        .unwrap();
    for document in &fixture.documents {
        index.upsert(fixture.document(document)).unwrap();
    }
    let query = Some(fixture.query.as_slice());
    let exact = index
        .search_with_options_exact("", query, SearchMode::Vector, options(TOP_K, None))
        .unwrap();
    assert_l2_hits(&fixture, &exact.hits, "resident exact");
    let graph = index
        .search_with_options(
            "",
            query,
            SearchMode::Vector,
            options(TOP_K, Some(DOCUMENTS)),
        )
        .unwrap();
    assert_l2_hits(&fixture, &graph.hits, "resident default");
}

#[test]
fn cosine_projections_keep_positive_only_similarity() {
    let fixture = fixture();
    let query = Some(fixture.query.as_slice());
    let positive = fixture
        .documents
        .iter()
        .filter(|(_, embedding)| cosine(&fixture.query, embedding) > 0.0)
        .count();
    assert!(positive < DOCUMENTS);
    for (name, distance) in [("unbound", None), ("bound", Some(VectorDistance::Cosine))] {
        let root = directory(name);
        write_generation(&root.0, &fixture, distance);
        let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
        let exact = reader
            .search_with_options_exact("", query, SearchMode::Vector, options(TOP_K, None))
            .unwrap();
        assert_cosine_hits(&fixture, &exact.result.hits, name);
        let graph = reader
            .search_with_options(
                "",
                query,
                SearchMode::Vector,
                options(TOP_K, Some(DOCUMENTS)),
            )
            .unwrap();
        assert_cosine_hits(&fixture, &graph.result.hits, name);
        // Nonpositive cosine similarity is not a vector match.
        let all = reader
            .search_with_options_exact("", query, SearchMode::Vector, options(DOCUMENTS, None))
            .unwrap();
        assert_eq!(all.result.total_hits, positive, "{name}");
        assert!(all.result.hits.iter().all(|hit| hit.score > 0.0));
    }
}

fn squared_l2_fixture() -> Fixture {
    let vector = |first, second| {
        let mut vector = vec![0.0; 1536];
        vector[0] = first;
        vector[1] = second;
        vector
    };
    Fixture {
        query: vector(1.0, 0.0),
        documents: vec![
            ("a-farthest".into(), vector(10.0, 0.0)),
            ("b-far".into(), vector(4.0, 0.0)),
            ("c-less-far".into(), vector(3.0, 0.0)),
            ("u-zero".into(), vector(0.0, 0.0)),
            ("v-opposite".into(), vector(-0.1, 0.0)),
            ("w-border-far".into(), vector(1.0, 0.632_455_6)),
            ("x-border-near".into(), vector(1.0, 0.632_455_47)),
            ("y-scaled".into(), vector(2.0, 0.0)),
            ("z-identical".into(), vector(1.0, 0.0)),
        ],
    }
}

fn assert_squared_l2_hits(fixture: &Fixture, hits: &[SearchHit]) {
    // Independent oracle: these fixtures have at most two nonzero coordinates,
    // so this Float32 reference has no reduction-order ambiguity.
    let distance = |embedding: &[f32]| {
        fixture
            .query
            .iter()
            .zip(embedding)
            .map(|(left, right)| {
                let difference = left - right;
                difference * difference
            })
            .sum::<f32>()
    };
    let mut expected = fixture.documents.iter().collect::<Vec<_>>();
    expected.sort_by(|left, right| {
        distance(&left.1)
            .total_cmp(&distance(&right.1))
            .then(left.0.cmp(&right.0))
    });
    assert_eq!(
        ids(hits),
        expected
            .iter()
            .take(hits.len())
            .map(|row| row.0.clone())
            .collect::<Vec<_>>()
    );
    for hit in hits {
        let expected = (1.0 - f64::from(distance(fixture.embedding(&hit.id))) / 2.0).max(0.0);
        assert_eq!(hit.score, expected, "{}", hit.id);
        assert_eq!(hit.vector_score, expected, "{}", hit.id);
    }
    if hits.len() == fixture.documents.len() {
        assert!(
            hits.iter()
                .find(|hit| hit.id == "x-border-near")
                .unwrap()
                .score
                >= 0.80
        );
        assert!(
            hits.iter()
                .find(|hit| hit.id == "w-border-far")
                .unwrap()
                .score
                < 0.80
        );
        assert_eq!(hits.iter().filter(|hit| hit.score == 0.0).count(), 3);
    }
}

#[test]
fn ordinary_squared_l2_preserves_scores_zero_neighbors_and_pinned_checkpoint() {
    let root = directory("ordinary-squared-l2");
    let fixture = squared_l2_fixture();
    let query = Some(fixture.query.as_slice());
    let count = fixture.documents.len();
    let mut index = SearchIndex::open(&root.0).unwrap();
    index
        .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
        .unwrap();
    for row in &fixture.documents {
        index.upsert(fixture.document(row)).unwrap();
    }
    assert!(index
        .set_vector_scoring(SearchVectorScoring::ProjectionMetric)
        .is_err());
    for exact in [false, true] {
        let result = index
            .try_search_with_options_adaptive_vector_projection_context(
                "",
                query,
                SearchMode::Vector,
                options(count, Some(count)),
                AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Disabled),
                execution(exact),
            )
            .unwrap();
        assert_squared_l2_hits(&fixture, &result.hits);
    }
    index.checkpoint().unwrap();
    let pinned = index.read_snapshot();
    index.delete("z-identical");
    index.checkpoint().unwrap();
    let old = pinned
        .try_search_with_options_adaptive_vector_projection_context(
            "",
            query,
            SearchMode::Vector,
            options(count, Some(count)),
            AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Disabled),
            VectorSearchExecutionOptions::default().with_exact_search(),
        )
        .unwrap();
    assert_squared_l2_hits(&fixture, &old.hits);
    drop(index);
    let reopened = SearchIndex::open(&root.0).unwrap();
    assert_eq!(
        reopened.vector_scoring(),
        SearchVectorScoring::LinearSquaredL2
    );
    assert_eq!(reopened.vector_metric(), VectorDistance::Euclidean);
    let current = reopened
        .search_with_options_exact("", query, SearchMode::Vector, options(count, None))
        .unwrap();
    assert!(!ids(&current.hits).contains(&"z-identical".to_string()));
}

#[test]
fn out_of_core_squared_l2_retains_profile_and_scores_without_hydration() {
    let root = directory("streamed-squared-l2");
    let fixture = squared_l2_fixture();
    let count = fixture.documents.len();
    let query = Some(fixture.query.as_slice());
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root.0,
        SearchOutOfCoreGenerationBuildOptions {
            vector_scoring: SearchVectorScoring::LinearSquaredL2,
            ..Default::default()
        },
    )
    .unwrap();
    for row in &fixture.documents {
        writer.push(fixture.document(row)).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reader.vector_scoring(),
        SearchVectorScoring::LinearSquaredL2
    );
    for exact in [false, true] {
        for hydration in [
            SearchHitHydration::Documents,
            SearchHitHydration::ScoresOnly,
        ] {
            let result = reader
                .search_with_hydration(
                    "",
                    query,
                    SearchMode::Vector,
                    options(count, Some(count)),
                    SearchOutOfCoreExecutionContext {
                        vector_execution_options: execution(exact),
                        ..Default::default()
                    },
                    hydration,
                )
                .unwrap();
            assert_squared_l2_hits(&fixture, &result.result.hits);
        }
    }
    for exact in [false, true] {
        let cancellation = hawdb_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let task = hawdb_core::RuntimeTaskContext::without_deadline(cancellation);
        let cancelled = VectorSearchExecutionOptions::admitted(64 << 20, &task);
        let bounded = VectorSearchExecutionOptions::bounded(NonZeroUsize::new(1).unwrap(), 1, None);
        for mut control in [cancelled, bounded] {
            if exact {
                control = control.with_exact_search();
            }
            assert!(reader
                .search_with_hydration(
                    "",
                    query,
                    SearchMode::Vector,
                    options(count, None),
                    SearchOutOfCoreExecutionContext {
                        vector_execution_options: control,
                        ..Default::default()
                    },
                    SearchHitHydration::ScoresOnly
                )
                .is_err());
        }
    }
    // An epoch-only update must inherit the base policy with ordinary defaults.
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: Vec::new(),
            deletes: Vec::new(),
            max_operations: None,
            source_graph_commit_epoch: Some(1),
        },
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reopened.vector_scoring(),
        SearchVectorScoring::LinearSquaredL2
    );
    let result = reopened
        .search_with_options_exact("", query, SearchMode::Vector, options(count, None))
        .unwrap();
    assert_squared_l2_hits(&fixture, &result.result.hits);
    // The original pinned reader still owns the old immutable score policy.
    let result = reader
        .search_with_options_exact("", query, SearchMode::Vector, options(count, None))
        .unwrap();
    assert_squared_l2_hits(&fixture, &result.result.hits);
}

#[test]
fn squared_l2_refuses_conflicting_bindings_compression_and_corrupt_metadata() {
    let mut index = SearchIndex::in_memory();
    index
        .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
        .unwrap();
    assert!(index
        .bind_vector_index(descriptor(VectorDistance::Euclidean))
        .is_err());
    let mut bound = SearchIndex::in_memory();
    bound
        .bind_vector_index(descriptor(VectorDistance::Euclidean))
        .unwrap();
    assert!(bound
        .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
        .is_err());
    let root = directory("refused-compressed-squared-l2");
    assert!(SearchOutOfCoreGenerationWriter::create(
        &root.0,
        SearchOutOfCoreGenerationBuildOptions {
            vector_scoring: SearchVectorScoring::LinearSquaredL2,
            vector_backend: None,
            ..Default::default()
        }
    )
    .is_err());
    let snapshot = |header: &str| {
        let body = format!("HAWDB_SEARCH_PROJECTION_V1\n{header}");
        format!("{body}checksum\t{}\n", checksum_bytes(body.as_bytes()))
    };
    let mut valid = SearchIndex::in_memory();
    valid
        .parse_snapshot_text(&snapshot("vector_scoring\tlinear_squared_l2\n"))
        .unwrap();
    assert_eq!(valid.vector_scoring(), SearchVectorScoring::LinearSquaredL2);
    for (header, reason) in [
        (
            "vector_scoring\tlinear_squared_l2\nvector_scoring\tlinear_squared_l2\n",
            "duplicate or misplaced vector scoring",
        ),
        (
            "vector_scoring\tunknown\n",
            "invalid search projection line",
        ),
        (
            "vector_scoring\tlinear_squared_l2\nrabitq_checkpoint\t4\t16\t1\n",
            "ordinary exact/DiskANN projection",
        ),
    ] {
        let mut restored = SearchIndex::in_memory();
        let error = restored.parse_snapshot_text(&snapshot(header)).unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
    }
}

#[test]
fn squared_l2_checkpoint_refuses_conflicting_options_before_replacing_snapshot() {
    let root = directory("squared-l2-checkpoint-refusal");
    let mut index = SearchIndex::open(&root.0).unwrap();
    index
        .set_vector_scoring(SearchVectorScoring::LinearSquaredL2)
        .unwrap();
    index
        .upsert(SearchDocument {
            id: "memory:one".into(),
            title: String::new(),
            content: String::new(),
            embedding: Some(vec![1.0, 0.5]),
            metadata: BTreeMap::new(),
        })
        .unwrap();
    index.checkpoint().unwrap();
    let snapshot = root.0.join(SEARCH_SNAPSHOT_FILE);
    let before = std::fs::read(&snapshot).unwrap();
    index.set_rabitq_projection_build_options(Default::default());
    let error = index.checkpoint().unwrap_err();
    assert!(error
        .to_string()
        .contains("ordinary exact/DiskANN projection"));
    assert_eq!(std::fs::read(&snapshot).unwrap(), before);
    drop(index);
    let reopened = SearchIndex::open(&root.0).unwrap();
    assert_eq!(
        reopened.vector_scoring(),
        SearchVectorScoring::LinearSquaredL2
    );
    assert_eq!(reopened.document_count(), 1);
    let hits = reopened
        .search_with_options_exact("", Some(&[1.0, 0.0]), SearchMode::Vector, options(1, None))
        .unwrap()
        .hits;
    assert_eq!(hits[0].score, 0.875);
}

fn execution(exact: bool) -> VectorSearchExecutionOptions<'static> {
    if exact {
        VectorSearchExecutionOptions::default().with_exact_search()
    } else {
        VectorSearchExecutionOptions::default()
    }
}
