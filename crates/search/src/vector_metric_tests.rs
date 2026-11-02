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
